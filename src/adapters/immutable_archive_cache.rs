use crate::{ArchiveStore, BucketId, Record, SharedArchiveStore, StoreError};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    mem::size_of,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

/// A bounded process-local cache for successful reads of immutable archive
/// artifacts.
///
/// The caller must only wrap an archive store whose published object keys are
/// immutable. This is true for durable archive routes, which publish a
/// generation-addressed manifest key. Failures are never cached, so a missing
/// object or transient S3 failure is retried on the next request.
pub struct ImmutableArchiveCache {
    inner: SharedArchiveStore,
    entries: Mutex<CacheState>,
    max_bytes: usize,
    max_entries: usize,
    next_access: AtomicU64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    used_bytes: usize,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct CacheKey {
    bucket: BucketId,
    object_key: String,
}

impl CacheKey {
    fn new(bucket: &BucketId, object_key: &str) -> Self {
        Self {
            bucket: bucket.clone(),
            object_key: object_key.to_owned(),
        }
    }
}

struct CacheEntry {
    records: Arc<Vec<Record>>,
    estimated_bytes: usize,
    last_access: u64,
}

impl ImmutableArchiveCache {
    /// Wraps `inner` with an LRU cache bounded by estimated record bytes and
    /// entry count. Set either limit to zero to disable caching.
    pub fn new(inner: SharedArchiveStore, max_bytes: usize, max_entries: usize) -> Self {
        Self {
            inner,
            entries: Mutex::new(CacheState::default()),
            max_bytes,
            max_entries,
            next_access: AtomicU64::new(0),
        }
    }

    fn get_cached(&self, bucket: &BucketId, object_key: &str) -> Option<Arc<Vec<Record>>> {
        let access = self.next_access.fetch_add(1, Ordering::Relaxed);
        let mut state = lock_unpoisoned(&self.entries);
        let entry = state.entries.get_mut(&CacheKey::new(bucket, object_key))?;
        entry.last_access = access;
        Some(Arc::clone(&entry.records))
    }

    fn cache(&self, bucket: &BucketId, object_key: String, records: Arc<Vec<Record>>) {
        if self.max_bytes == 0 || self.max_entries == 0 {
            return;
        }

        let estimated_bytes = estimated_records_size(&records);
        if estimated_bytes > self.max_bytes {
            return;
        }

        let access = self.next_access.fetch_add(1, Ordering::Relaxed);
        let mut state = lock_unpoisoned(&self.entries);
        let cache_key = CacheKey::new(bucket, &object_key);
        remove_entry(&mut state, &cache_key);
        while state.entries.len() >= self.max_entries
            || state.used_bytes.saturating_add(estimated_bytes) > self.max_bytes
        {
            let Some(eviction_key) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            remove_entry(&mut state, &eviction_key);
        }
        state.used_bytes = state.used_bytes.saturating_add(estimated_bytes);
        state.entries.insert(
            cache_key,
            CacheEntry {
                records,
                estimated_bytes,
                last_access: access,
            },
        );
    }

    fn invalidate(&self, bucket: &BucketId, object_key: &str) {
        remove_entry(
            &mut lock_unpoisoned(&self.entries),
            &CacheKey::new(bucket, object_key),
        );
    }
}

#[async_trait]
impl ArchiveStore for ImmutableArchiveCache {
    async fn put_bucket(
        &self,
        bucket: &BucketId,
        records: &[Record],
    ) -> Result<String, StoreError> {
        let object_key = self.inner.put_bucket(bucket, records).await?;
        self.invalidate(bucket, &object_key);
        Ok(object_key)
    }

    async fn put_bucket_generation(
        &self,
        bucket: &BucketId,
        generation: u64,
        records: &[Record],
    ) -> Result<String, StoreError> {
        let object_key = self
            .inner
            .put_bucket_generation(bucket, generation, records)
            .await?;
        self.invalidate(bucket, &object_key);
        Ok(object_key)
    }

    async fn get_bucket(
        &self,
        bucket: &BucketId,
        object_key: &str,
    ) -> Result<Vec<Record>, StoreError> {
        if let Some(records) = self.get_cached(bucket, object_key) {
            return Ok(records.as_ref().clone());
        }

        let records = Arc::new(self.inner.get_bucket(bucket, object_key).await?);
        self.cache(bucket, object_key.to_owned(), Arc::clone(&records));
        Ok(records.as_ref().clone())
    }
}

fn estimated_records_size(records: &[Record]) -> usize {
    records.iter().fold(0_usize, |size, record| {
        size.saturating_add(size_of::<Record>())
            .saturating_add(record.partition_key.as_bytes().len())
            .saturating_add(record.sort_key.as_bytes().len())
            .saturating_add(record.value.len())
    })
}

fn remove_entry(state: &mut CacheState, key: &CacheKey) {
    if let Some(entry) = state.entries.remove(key) {
        state.used_bytes = state.used_bytes.saturating_sub(entry.estimated_bytes);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BucketStrategy, PartitionKey, SortKey, TableId};
    use chrono::{TimeZone, Utc};
    use std::sync::atomic::AtomicUsize;

    struct CountingArchiveStore {
        reads: AtomicUsize,
        result: Result<Vec<Record>, StoreError>,
    }

    #[async_trait]
    impl ArchiveStore for CountingArchiveStore {
        async fn put_bucket(&self, _: &BucketId, _: &[Record]) -> Result<String, StoreError> {
            unreachable!("test store is read-only")
        }

        async fn get_bucket(&self, _: &BucketId, _: &str) -> Result<Vec<Record>, StoreError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            match &self.result {
                Ok(records) => Ok(records.clone()),
                Err(error) => Err(StoreError::Other(error.to_string())),
            }
        }
    }

    fn bucket() -> BucketId {
        BucketId::for_event_time_with_strategy(
            TableId::new("test-table").expect("test table ID is valid"),
            PartitionKey::from(b"general".to_vec()),
            BucketStrategy::CalendarYearUtc,
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single().unwrap(),
        )
    }

    fn record() -> Record {
        Record {
            partition_key: PartitionKey::from(b"general".to_vec()),
            event_time: Utc.with_ymd_and_hms(2026, 8, 6, 0, 0, 0).single().unwrap(),
            sort_key: SortKey::from(b"first".to_vec()),
            value: b"cached".to_vec(),
        }
    }

    #[test]
    fn estimates_opaque_key_bytes() {
        let record = record();
        assert_eq!(
            estimated_records_size(std::slice::from_ref(&record)),
            size_of::<Record>()
                + record.partition_key.as_bytes().len()
                + record.sort_key.as_bytes().len()
                + record.value.len()
        );
    }

    #[tokio::test]
    async fn caches_successful_immutable_reads() {
        let inner = Arc::new(CountingArchiveStore {
            reads: AtomicUsize::new(0),
            result: Ok(vec![record()]),
        });
        let cache = ImmutableArchiveCache::new(inner.clone(), 1024, 1);
        let bucket = bucket();

        assert_eq!(
            cache
                .get_bucket(&bucket, "generation-1/manifest.json")
                .await
                .unwrap(),
            vec![record()]
        );
        assert_eq!(
            cache
                .get_bucket(&bucket, "generation-1/manifest.json")
                .await
                .unwrap(),
            vec![record()]
        );
        assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn does_not_cache_failed_reads() {
        let inner = Arc::new(CountingArchiveStore {
            reads: AtomicUsize::new(0),
            result: Err(StoreError::Other("not found".into())),
        });
        let cache = ImmutableArchiveCache::new(inner.clone(), 1024, 1);
        let bucket = bucket();

        assert!(cache.get_bucket(&bucket, "missing").await.is_err());
        assert!(cache.get_bucket(&bucket, "missing").await.is_err());
        assert_eq!(inner.reads.load(Ordering::Relaxed), 2);
    }
}
