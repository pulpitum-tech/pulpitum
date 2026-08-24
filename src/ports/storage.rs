use crate::model::{BucketId, Cursor, Record, TimeRange};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use uuid::Uuid;

/// Durable routing state for a bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BucketState {
    /// Reads and writes go to the hot store.
    Hot,
    /// New writes are rejected while a fenced archive session is active.
    Archiving,
    /// Reads go to the immutable archive. `hot_deleted` records cleanup progress.
    Archived {
        object_key: String,
        hot_deleted: bool,
    },
}

#[derive(Debug, Error, Clone)]
pub enum StoreError {
    #[error("bucket not found: {0:?}")]
    MissingBucket(BucketId),
    #[error("store failure: {0}")]
    Other(String),
}

/// Failure from a durable bucket operation.
///
/// Implementations must not expose connection strings, query values, owner
/// tokens, or bucket identities through this error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableBucketStoreError {
    #[error("durable bucket store is unavailable")]
    Unavailable,
    #[error("durable bucket store operation failed")]
    OperationFailed,
    #[error("durable bucket transaction commit outcome is unknown")]
    CommitOutcomeUnknown,
    #[error("bucket cannot accept writes while in state {0:?}")]
    BucketReadOnly(BucketState),
    #[error("bucket cannot begin archival while in state {0:?}")]
    ArchiveNotAllowed(BucketState),
    #[error("bucket strategy does not match existing table metadata")]
    BucketStrategyMismatch,
    #[error("archive session no longer owns the bucket")]
    StaleArchiveSession,
}

/// Result of [`DurableBucketStore::read_range`].
///
/// This exposes the routed read result rather than a bucket's internal metadata
/// state. `Hot` records are filtered and sorted by `(event_time, sort_key)`; `Archive`
/// supplies the durable object key for the caller to read from the archive tier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableBucketRead {
    Hot(Vec<Record>),
    Archive(String),
}

/// Credential returned by [`DurableBucketStore::begin_archive`].
///
/// The bucket, owner token, and fencing generation are deliberately opaque:
/// callers can identify the bucket and observe the generation, but cannot mint,
/// inspect, or modify the ownership token. Every archive mutation must receive
/// this session so an implementation can condition the physical operation and
/// metadata transition on the same owner and generation.
pub struct ArchiveSession {
    bucket: BucketId,
    owner_token: String,
    generation: u64,
}

impl ArchiveSession {
    pub(crate) fn new(bucket: BucketId, owner_token: String, generation: u64) -> Self {
        Self {
            bucket,
            owner_token,
            generation,
        }
    }

    /// Bucket exclusively owned by this archival attempt.
    pub fn bucket(&self) -> &BucketId {
        &self.bucket
    }

    /// Monotonically increasing fence for this archival attempt.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn owner_token(&self) -> &str {
        &self.owner_token
    }
}

impl fmt::Debug for ArchiveSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArchiveSession")
            .field("bucket", &self.bucket)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// A unit of coordinator-owned archival work discovered from durable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveWork {
    Cutover(BucketId),
    Cleanup(BucketId),
}

/// Bounded discovery request for archival work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveScan {
    /// Buckets whose exclusive end is at or before this cutoff are eligible.
    pub eligible_before: DateTime<Utc>,
    pub limit: u32,
}

pub(crate) mod durable_bucket_store_sealed {
    pub trait Sealed {}
}

/// Atomic boundary for a production bucket's metadata and mutable records.
///
/// An implementation must validate a write fence and append in one durable
/// transaction. Archive methods must similarly condition every physical
/// snapshot/delete and metadata transition on the [`ArchiveSession`] owner
/// token and generation. This prevents the unsafe split in which metadata and
/// the hot store independently claim to coordinate a cutover.
///
/// The trait is sealed because only a Pulpitum adapter can mint the opaque
/// session credential.
#[async_trait]
pub trait DurableBucketStore: Send + Sync + durable_bucket_store_sealed::Sealed {
    /// Atomically verifies that the namespaced bucket is writable and appends the record.
    async fn append(
        &self,
        bucket: &BucketId,
        record: Record,
    ) -> Result<(), DurableBucketStoreError>;

    /// Returns the durable routing state for a bucket.
    async fn state(&self, bucket: &BucketId) -> Result<BucketState, DurableBucketStoreError>;

    /// Discovers bounded coordinator work. Returned work is advisory; callers
    /// must claim it before performing any physical operation.
    async fn discover_archive_work(
        &self,
        scan: ArchiveScan,
    ) -> Result<Vec<ArchiveWork>, DurableBucketStoreError>;

    /// Claims a hot or expired archiving bucket for a new cutover attempt.
    async fn claim_archive(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError>;

    /// Claims cleanup for a published archive whose earlier owner expired.
    async fn claim_cleanup(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError>;

    /// Extends the current worker's ownership lease.
    async fn renew_archive_lease(
        &self,
        session: &ArchiveSession,
        lease_for: Duration,
    ) -> Result<(), DurableBucketStoreError>;

    /// Reopens a pre-publication attempt and schedules a bounded retry.
    async fn defer_archive(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError>;

    /// Retains a published archive and schedules its hot-data cleanup retry.
    async fn defer_cleanup(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError>;

    /// Routes and reads a bounded page from one bucket range.
    ///
    /// Implementations must select mutable route metadata and bounded hot rows
    /// atomically, from one lock or one database statement/MVCC snapshot. A
    /// previously observed immutable archive route may be served from cache, but
    /// a cached hot observation must never select the tier. Thus a read racing
    /// publication and cleanup returns either the pre-publication hot page or the
    /// published archive key, never a stale hot route combined with post-cleanup
    /// rows. Hot and archiving buckets return at most `limit` matching records
    /// strictly after `after`, in `(event_time, sort_key)` order. Archived buckets
    /// return only their durable archive object key, including while hot-data
    /// cleanup remains pending. Separate bucket reads do not share a snapshot.
    async fn read_range(
        &self,
        bucket: &BucketId,
        range: &TimeRange,
        after: Option<&Cursor>,
        limit: usize,
    ) -> Result<DurableBucketRead, DurableBucketStoreError>;

    /// Closes the write fence and returns credentials for one archival owner.
    async fn begin_archive(
        &self,
        bucket: &BucketId,
    ) -> Result<ArchiveSession, DurableBucketStoreError>;

    /// Returns a stable hot-bucket snapshot only for the current archive owner.
    async fn snapshot(
        &self,
        session: &ArchiveSession,
    ) -> Result<Vec<Record>, DurableBucketStoreError>;

    /// Publishes an archive pointer only for the current archive owner.
    async fn publish_archive(
        &self,
        session: &ArchiveSession,
        object_key: String,
    ) -> Result<(), DurableBucketStoreError>;

    /// Deletes the hot bucket and records that cleanup atomically for the owner.
    async fn delete_hot_bucket(
        &self,
        session: &ArchiveSession,
    ) -> Result<(), DurableBucketStoreError>;

    /// Reopens a bucket only if the current archive owner has not published it.
    async fn abort_archive(&self, session: &ArchiveSession) -> Result<(), DurableBucketStoreError>;
}

/// Object-store adapter boundary. Implementations map `put_bucket`/`get_bucket`
/// to immutable archive artifacts, such as a versioned manifest and payload in
/// S3, GCS, Azure, or a local filesystem. The returned key is the published
/// read artifact, not necessarily the underlying record payload.
#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn put_bucket(&self, bucket: &BucketId, records: &[Record])
    -> Result<String, StoreError>;

    /// Writes a payload for one fenced archive generation so stale workers
    /// cannot overwrite a replacement worker's object.
    async fn put_bucket_generation(
        &self,
        _bucket: &BucketId,
        _generation: u64,
        _records: &[Record],
    ) -> Result<String, StoreError> {
        Err(StoreError::Other(
            "archive store does not support generation-addressed uploads".into(),
        ))
    }

    /// Reads an archive object only when it declares the expected bucket identity.
    async fn get_bucket(
        &self,
        bucket: &BucketId,
        object_key: &str,
    ) -> Result<Vec<Record>, StoreError>;
}

struct DurableBucketEntry {
    state: BucketState,
    generation: u64,
    archive_owner_token: Option<String>,
    archive_owner_expires_at: Option<Instant>,
    next_attempt_at: Option<Instant>,
    records: Vec<Record>,
}

impl Default for DurableBucketEntry {
    fn default() -> Self {
        Self {
            state: BucketState::Hot,
            generation: 0,
            archive_owner_token: None,
            archive_owner_expires_at: None,
            next_attempt_at: None,
            records: Vec::new(),
        }
    }
}

/// Process-local coupled bucket store for tests and single-process demos.
///
/// This is not durable across process restarts. It does, however, model the
/// coupled API by changing routing state and mutable records while holding one
/// lock, and by checking the opaque session on every archive mutation.
#[derive(Default)]
pub struct InMemoryDurableBucketStore {
    buckets: Mutex<HashMap<BucketId, DurableBucketEntry>>,
}

impl durable_bucket_store_sealed::Sealed for InMemoryDurableBucketStore {}

#[async_trait]
impl DurableBucketStore for InMemoryDurableBucketStore {
    async fn append(
        &self,
        bucket: &BucketId,
        record: Record,
    ) -> Result<(), DurableBucketStoreError> {
        let mut buckets = self.buckets.lock().await;
        let entry = buckets.entry(bucket.clone()).or_default();
        if entry.state != BucketState::Hot {
            return Err(DurableBucketStoreError::BucketReadOnly(entry.state.clone()));
        }
        entry.records.push(record);
        entry
            .records
            .sort_by(|a, b| (&a.event_time, &a.sort_key).cmp(&(&b.event_time, &b.sort_key)));
        Ok(())
    }

    async fn state(&self, bucket: &BucketId) -> Result<BucketState, DurableBucketStoreError> {
        Ok(self
            .buckets
            .lock()
            .await
            .get(bucket)
            .map(|entry| entry.state.clone())
            .unwrap_or(BucketState::Hot))
    }

    async fn discover_archive_work(
        &self,
        scan: ArchiveScan,
    ) -> Result<Vec<ArchiveWork>, DurableBucketStoreError> {
        let now = Instant::now();
        let buckets = self.buckets.lock().await;
        let mut cleanup = Vec::new();
        let mut cutover = Vec::new();
        for (bucket, entry) in buckets.iter() {
            if !attempt_due(entry, now) {
                continue;
            }
            match &entry.state {
                BucketState::Archived {
                    hot_deleted: false, ..
                } if lease_expired(entry, now) => {
                    cleanup.push(ArchiveWork::Cleanup(bucket.clone()))
                }
                BucketState::Hot if bucket.end <= scan.eligible_before => {
                    cutover.push(ArchiveWork::Cutover(bucket.clone()))
                }
                BucketState::Archiving
                    if bucket.end <= scan.eligible_before && lease_expired(entry, now) =>
                {
                    cutover.push(ArchiveWork::Cutover(bucket.clone()))
                }
                _ => {}
            }
        }
        cleanup.sort_by(|left, right| work_bucket(left).cmp(work_bucket(right)));
        cutover.sort_by(|left, right| work_bucket(left).cmp(work_bucket(right)));
        cleanup.extend(cutover);
        cleanup.truncate(scan.limit as usize);
        Ok(cleanup)
    }

    async fn claim_archive(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        let now = Instant::now();
        let expires_at = lease_deadline(lease_for)?;
        let mut buckets = self.buckets.lock().await;
        let entry = buckets.entry(bucket.clone()).or_default();
        let claimable = attempt_due(entry, now)
            && (entry.state == BucketState::Hot
                || (entry.state == BucketState::Archiving && lease_expired(entry, now)));
        if !claimable {
            return Ok(None);
        }
        entry.generation = entry
            .generation
            .checked_add(1)
            .ok_or(DurableBucketStoreError::OperationFailed)?;
        let owner_token = Uuid::new_v4().to_string();
        entry.state = BucketState::Archiving;
        entry.archive_owner_token = Some(owner_token.clone());
        entry.archive_owner_expires_at = Some(expires_at);
        entry.next_attempt_at = None;
        Ok(Some(ArchiveSession::new(
            bucket.clone(),
            owner_token,
            entry.generation,
        )))
    }

    async fn claim_cleanup(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        let now = Instant::now();
        let expires_at = lease_deadline(lease_for)?;
        let mut buckets = self.buckets.lock().await;
        let Some(entry) = buckets.get_mut(bucket) else {
            return Ok(None);
        };
        let claimable = matches!(
            entry.state,
            BucketState::Archived {
                hot_deleted: false,
                ..
            }
        ) && attempt_due(entry, now)
            && lease_expired(entry, now);
        if !claimable {
            return Ok(None);
        }
        entry.generation = entry
            .generation
            .checked_add(1)
            .ok_or(DurableBucketStoreError::OperationFailed)?;
        let owner_token = Uuid::new_v4().to_string();
        entry.archive_owner_token = Some(owner_token.clone());
        entry.archive_owner_expires_at = Some(expires_at);
        entry.next_attempt_at = None;
        Ok(Some(ArchiveSession::new(
            bucket.clone(),
            owner_token,
            entry.generation,
        )))
    }

    async fn renew_archive_lease(
        &self,
        session: &ArchiveSession,
        lease_for: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let expires_at = lease_deadline(lease_for)?;
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(session.bucket())
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_active_session(entry, session)?;
        entry.archive_owner_expires_at = Some(expires_at);
        Ok(())
    }

    async fn defer_archive(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let retry_at = lease_deadline(retry_after)?;
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(session.bucket())
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_archive_session(entry, session, &BucketState::Archiving)?;
        entry.state = BucketState::Hot;
        entry.archive_owner_token = None;
        entry.archive_owner_expires_at = None;
        entry.next_attempt_at = Some(retry_at);
        Ok(())
    }

    async fn defer_cleanup(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let retry_at = lease_deadline(retry_after)?;
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(session.bucket())
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_active_session(entry, session)?;
        if !matches!(
            entry.state,
            BucketState::Archived {
                hot_deleted: false,
                ..
            }
        ) {
            return Err(DurableBucketStoreError::StaleArchiveSession);
        }
        entry.archive_owner_expires_at = Some(retry_at);
        entry.next_attempt_at = Some(retry_at);
        Ok(())
    }

    async fn read_range(
        &self,
        bucket: &BucketId,
        range: &TimeRange,
        after: Option<&Cursor>,
        limit: usize,
    ) -> Result<DurableBucketRead, DurableBucketStoreError> {
        let buckets = self.buckets.lock().await;
        match buckets.get(bucket) {
            Some(DurableBucketEntry {
                state: BucketState::Archived { object_key, .. },
                ..
            }) => Ok(DurableBucketRead::Archive(object_key.clone())),
            Some(DurableBucketEntry {
                state: BucketState::Hot | BucketState::Archiving,
                records,
                ..
            }) => Ok(DurableBucketRead::Hot(
                records
                    .iter()
                    .filter(|record| range.contains(record.event_time))
                    .filter(|record| {
                        after.is_none_or(|cursor| {
                            (&record.event_time, &record.sort_key)
                                > (&cursor.event_time, &cursor.sort_key)
                        })
                    })
                    .take(limit)
                    .cloned()
                    .collect(),
            )),
            None => Ok(DurableBucketRead::Hot(Vec::new())),
        }
    }

    async fn begin_archive(
        &self,
        bucket: &BucketId,
    ) -> Result<ArchiveSession, DurableBucketStoreError> {
        let mut buckets = self.buckets.lock().await;
        let entry = buckets.entry(bucket.clone()).or_default();
        if entry.state != BucketState::Hot {
            return Err(DurableBucketStoreError::ArchiveNotAllowed(
                entry.state.clone(),
            ));
        }
        entry.generation = entry
            .generation
            .checked_add(1)
            .ok_or(DurableBucketStoreError::OperationFailed)?;
        let owner_token = Uuid::new_v4().to_string();
        entry.state = BucketState::Archiving;
        entry.archive_owner_token = Some(owner_token.clone());
        entry.archive_owner_expires_at = Some(lease_deadline(Duration::from_secs(300))?);
        entry.next_attempt_at = None;
        Ok(ArchiveSession::new(
            bucket.clone(),
            owner_token,
            entry.generation,
        ))
    }

    async fn snapshot(
        &self,
        session: &ArchiveSession,
    ) -> Result<Vec<Record>, DurableBucketStoreError> {
        let buckets = self.buckets.lock().await;
        let entry = buckets
            .get(&session.bucket)
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_archive_session(entry, session, &BucketState::Archiving)?;
        Ok(entry.records.clone())
    }

    async fn publish_archive(
        &self,
        session: &ArchiveSession,
        object_key: String,
    ) -> Result<(), DurableBucketStoreError> {
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(&session.bucket)
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_archive_session(entry, session, &BucketState::Archiving)?;
        entry.state = BucketState::Archived {
            object_key,
            hot_deleted: false,
        };
        Ok(())
    }

    async fn delete_hot_bucket(
        &self,
        session: &ArchiveSession,
    ) -> Result<(), DurableBucketStoreError> {
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(&session.bucket)
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        let BucketState::Archived {
            object_key,
            hot_deleted: false,
        } = &entry.state
        else {
            return Err(DurableBucketStoreError::StaleArchiveSession);
        };
        validate_active_session(entry, session)?;
        entry.records.clear();
        entry.state = BucketState::Archived {
            object_key: object_key.clone(),
            hot_deleted: true,
        };
        Ok(())
    }

    async fn abort_archive(&self, session: &ArchiveSession) -> Result<(), DurableBucketStoreError> {
        let mut buckets = self.buckets.lock().await;
        let entry = buckets
            .get_mut(&session.bucket)
            .ok_or(DurableBucketStoreError::StaleArchiveSession)?;
        validate_archive_session(entry, session, &BucketState::Archiving)?;
        entry.state = BucketState::Hot;
        entry.archive_owner_token = None;
        entry.archive_owner_expires_at = None;
        entry.next_attempt_at = None;
        Ok(())
    }
}

fn validate_archive_session(
    entry: &DurableBucketEntry,
    session: &ArchiveSession,
    expected_state: &BucketState,
) -> Result<(), DurableBucketStoreError> {
    if entry.state != *expected_state {
        return Err(DurableBucketStoreError::StaleArchiveSession);
    }
    validate_active_session(entry, session)
}

fn validate_active_session(
    entry: &DurableBucketEntry,
    session: &ArchiveSession,
) -> Result<(), DurableBucketStoreError> {
    if entry.generation != session.generation
        || entry.archive_owner_token.as_deref() != Some(session.owner_token.as_str())
        || lease_expired(entry, Instant::now())
    {
        return Err(DurableBucketStoreError::StaleArchiveSession);
    }
    Ok(())
}

fn attempt_due(entry: &DurableBucketEntry, now: Instant) -> bool {
    entry.next_attempt_at.is_none_or(|retry_at| retry_at <= now)
}

fn lease_expired(entry: &DurableBucketEntry, now: Instant) -> bool {
    entry
        .archive_owner_expires_at
        .is_none_or(|expires_at| expires_at <= now)
}

fn lease_deadline(duration: Duration) -> Result<Instant, DurableBucketStoreError> {
    if duration.is_zero() {
        return Err(DurableBucketStoreError::OperationFailed);
    }
    Instant::now()
        .checked_add(duration)
        .ok_or(DurableBucketStoreError::OperationFailed)
}

fn work_bucket(work: &ArchiveWork) -> &BucketId {
    match work {
        ArchiveWork::Cutover(bucket) | ArchiveWork::Cleanup(bucket) => bucket,
    }
}

fn partition_key_path_component(partition_key: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(partition_key.len() * 2);
    for byte in partition_key {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[derive(Default)]
pub struct InMemoryArchiveStore {
    objects: RwLock<HashMap<String, Vec<Record>>>,
}

#[async_trait]
impl ArchiveStore for InMemoryArchiveStore {
    async fn put_bucket(
        &self,
        bucket: &BucketId,
        records: &[Record],
    ) -> Result<String, StoreError> {
        let key = format!(
            "archives/{}/{}/{}/data.json",
            bucket.table_id.as_str(),
            partition_key_path_component(bucket.partition_key.as_bytes()),
            bucket.key.as_str()
        );
        self.objects
            .write()
            .await
            .insert(key.clone(), records.to_vec());
        Ok(key)
    }

    async fn put_bucket_generation(
        &self,
        bucket: &BucketId,
        generation: u64,
        records: &[Record],
    ) -> Result<String, StoreError> {
        let key = format!(
            "archives/{}/{}/{}/generation-{generation}/data.json",
            bucket.table_id.as_str(),
            partition_key_path_component(bucket.partition_key.as_bytes()),
            bucket.key.as_str()
        );
        self.objects
            .write()
            .await
            .insert(key.clone(), records.to_vec());
        Ok(key)
    }

    async fn get_bucket(
        &self,
        bucket: &BucketId,
        object_key: &str,
    ) -> Result<Vec<Record>, StoreError> {
        let records = self
            .objects
            .read()
            .await
            .get(object_key)
            .cloned()
            .ok_or_else(|| StoreError::Other(format!("archive object not found: {object_key}")))?;
        if records.iter().any(|record| {
            record.partition_key != bucket.partition_key || !bucket.contains(record.event_time)
        }) {
            return Err(StoreError::Other(
                "archive records do not match the requested bucket".into(),
            ));
        }
        Ok(records)
    }
}

pub type SharedArchiveStore = Arc<dyn ArchiveStore>;
pub type SharedDurableBucketStore = Arc<dyn DurableBucketStore>;
