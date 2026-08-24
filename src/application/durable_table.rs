use crate::{
    BucketId, BucketKey, Cursor, DefinitionError, DurableBucketRead, DurableBucketStoreError,
    NoopTelemetry, PartitionKey, ReadTier, Record, SharedArchiveStore, SharedDurableBucketStore,
    SharedTelemetry, StoreError, TableDefinition, TableId, TimeRange,
};
use chrono::Utc;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::Instrument;

const MAX_PARALLEL_BUCKET_READS: usize = 4;

/// A bounded logical-table query.
#[derive(Clone, Debug)]
pub struct Query {
    pub partition_key: PartitionKey,
    pub range: TimeRange,
    pub after: Option<Cursor>,
    pub limit: usize,
}

/// One page of records in logical `(event_time, sort_key)` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPage {
    pub records: Vec<Record>,
    pub next: Option<Cursor>,
}

#[derive(Debug, Error)]
pub enum DurableTableError {
    #[error("bucket {bucket:?} is not writable: {state:?}")]
    BucketReadOnly {
        bucket: Box<BucketId>,
        state: crate::BucketState,
    },
    #[error("writes are only allowed in buckets from {oldest} through {newest}; got {bucket:?}")]
    OutsideWriteWindow {
        bucket: Box<BucketId>,
        oldest: BucketKey,
        newest: BucketKey,
    },
    #[error("query range must have start before end")]
    InvalidTimeRange,
    #[error(transparent)]
    Definition(#[from] DefinitionError),
    #[error(transparent)]
    DurableStore(#[from] DurableBucketStoreError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Fenced table router backed by a coupled [`crate::DurableBucketStore`].
///
/// Every append and per-bucket read goes through the durable store's
/// transactional fence before optionally reading an archive object returned by
/// that store.
pub struct DurableTable {
    definition: TableDefinition,
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    telemetry: SharedTelemetry,
}

impl DurableTable {
    pub fn new(store: SharedDurableBucketStore, archive: SharedArchiveStore) -> Self {
        Self::with_definition_and_telemetry(
            TableDefinition::chat_messages(
                "records",
                TableId::new("pulpitum.default.records").expect("built-in table ID is valid"),
            ),
            store,
            archive,
            Arc::new(NoopTelemetry),
        )
        .expect("built-in definition is valid")
    }

    pub fn with_definition(
        definition: TableDefinition,
        store: SharedDurableBucketStore,
        archive: SharedArchiveStore,
    ) -> Result<Self, DurableTableError> {
        Self::with_definition_and_telemetry(definition, store, archive, Arc::new(NoopTelemetry))
    }

    pub fn with_definition_and_telemetry(
        definition: TableDefinition,
        store: SharedDurableBucketStore,
        archive: SharedArchiveStore,
        telemetry: SharedTelemetry,
    ) -> Result<Self, DurableTableError> {
        definition.validate()?;
        Ok(Self {
            definition,
            store,
            archive,
            telemetry,
        })
    }

    pub fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    #[tracing::instrument(name = "pulpitum.durable_table.append", skip(self, record), err, fields(pulpitum.table = %self.definition.name, pulpitum.operation = "append"))]
    pub async fn append(&self, record: Record) -> Result<(), DurableTableError> {
        let bucket = self.definition.bucket_for(&record);
        let writable = self
            .definition
            .writable_buckets_at(bucket.partition_key.clone(), Utc::now());
        let oldest = writable
            .first()
            .expect("validated definitions have a nonempty write window");
        let newest = writable
            .last()
            .expect("validated definitions have a nonempty write window");
        if bucket.start < oldest.start || bucket.start > newest.start {
            return Err(DurableTableError::OutsideWriteWindow {
                bucket: Box::new(bucket),
                oldest: oldest.key.clone(),
                newest: newest.key.clone(),
            });
        }
        self.store
            .append(&bucket, record)
            .await
            .map_err(|error| match error {
                DurableBucketStoreError::BucketReadOnly(state) => {
                    DurableTableError::BucketReadOnly {
                        bucket: Box::new(bucket),
                        state,
                    }
                }
                error => DurableTableError::DurableStore(error),
            })
    }

    /// Routes every strategy bucket intersecting the range and returns the logical sort order.
    /// Prefer [`Self::query_page`] for unbounded user-facing histories.
    pub async fn query(
        &self,
        partition_key: impl Into<PartitionKey>,
        range: TimeRange,
    ) -> Result<Vec<Record>, DurableTableError> {
        self.query_page(Query {
            partition_key: partition_key.into(),
            range,
            after: None,
            limit: usize::MAX,
        })
        .await
        .map(|page| page.records)
    }

    /// A cursor never includes a physical bucket ID, so it remains valid across
    /// hot/archive boundaries and strategy buckets.
    #[tracing::instrument(name = "pulpitum.durable_table.query", skip(self, query), err, fields(pulpitum.table = %self.definition.name, pulpitum.operation = "query"))]
    pub async fn query_page(&self, query: Query) -> Result<QueryPage, DurableTableError> {
        if query.range.start >= query.range.end {
            return Err(DurableTableError::InvalidTimeRange);
        }
        if query.limit == 0 {
            return Ok(QueryPage {
                records: Vec::new(),
                next: query.after,
            });
        }

        let page_capacity = query.limit.saturating_add(1);
        let buckets = self
            .definition
            .buckets_for_range(query.partition_key.clone(), &query.range);
        let mut result = read_buckets(
            buckets,
            Arc::clone(&self.store),
            Arc::clone(&self.archive),
            Arc::clone(&self.telemetry),
            query.range.clone(),
            query.after.clone(),
            page_capacity,
        )
        .await?;
        result.sort_by(|a, b| (&a.event_time, &a.sort_key).cmp(&(&b.event_time, &b.sort_key)));
        let has_more = result.len() > query.limit;
        result.truncate(query.limit);
        let next =
            has_more.then(|| Cursor::from(result.last().expect("nonempty page with more results")));
        Ok(QueryPage {
            records: result,
            next,
        })
    }
}

async fn read_buckets(
    buckets: impl IntoIterator<Item = BucketId>,
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    telemetry: SharedTelemetry,
    range: TimeRange,
    after: Option<Cursor>,
    limit: usize,
) -> Result<Vec<Record>, DurableTableError> {
    let mut buckets = buckets.into_iter();
    let Some(first) = buckets.next() else {
        return Ok(Vec::new());
    };
    let Some(second) = buckets.next() else {
        return read_bucket(store, archive, telemetry, first, range, after, limit).await;
    };

    let mut reads = JoinSet::new();
    spawn_bucket_read(
        &mut reads,
        store.clone(),
        archive.clone(),
        telemetry.clone(),
        first,
        range.clone(),
        after.clone(),
        limit,
    );
    spawn_bucket_read(
        &mut reads,
        store.clone(),
        archive.clone(),
        telemetry.clone(),
        second,
        range.clone(),
        after.clone(),
        limit,
    );
    for bucket in buckets.by_ref().take(MAX_PARALLEL_BUCKET_READS - 2) {
        spawn_bucket_read(
            &mut reads,
            store.clone(),
            archive.clone(),
            telemetry.clone(),
            bucket,
            range.clone(),
            after.clone(),
            limit,
        );
    }

    let mut result = Vec::new();
    while let Some(read) = reads.join_next().await {
        let records = read.map_err(|_| {
            DurableTableError::Store(StoreError::Other("parallel bucket read task failed".into()))
        })??;
        result.extend(records);
        if let Some(bucket) = buckets.next() {
            spawn_bucket_read(
                &mut reads,
                store.clone(),
                archive.clone(),
                telemetry.clone(),
                bucket,
                range.clone(),
                after.clone(),
                limit,
            );
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn spawn_bucket_read(
    reads: &mut JoinSet<Result<Vec<Record>, DurableTableError>>,
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    telemetry: SharedTelemetry,
    bucket: BucketId,
    range: TimeRange,
    after: Option<Cursor>,
    limit: usize,
) {
    let span = tracing::info_span!(
        "pulpitum.durable_table.bucket_read",
        pulpitum.bucket_key = %bucket.key,
        pulpitum.bucket_strategy = %bucket.strategy,
        pulpitum.query.limit = limit,
        pulpitum.records.returned = tracing::field::Empty,
    );
    reads.spawn(
        read_bucket(store, archive, telemetry, bucket, range, after, limit).instrument(span),
    );
}

async fn read_bucket(
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    telemetry: SharedTelemetry,
    bucket: BucketId,
    range: TimeRange,
    after: Option<Cursor>,
    limit: usize,
) -> Result<Vec<Record>, DurableTableError> {
    let span = tracing::Span::current();
    match store
        .read_range(&bucket, &range, after.as_ref(), limit)
        .await?
    {
        DurableBucketRead::Hot(records) => {
            span.record("pulpitum.records.returned", records.len());
            telemetry.read_routed(&bucket, ReadTier::Hot);
            Ok(records)
        }
        DurableBucketRead::Archive(object_key) => {
            telemetry.read_routed(&bucket, ReadTier::Archive);
            let mut records: Vec<_> = archive
                .get_bucket(&bucket, &object_key)
                .await?
                .into_iter()
                .filter(|record| range.contains(record.event_time))
                .filter(|record| {
                    after.as_ref().is_none_or(|cursor| {
                        (&record.event_time, &record.sort_key)
                            > (&cursor.event_time, &cursor.sort_key)
                    })
                })
                .collect();
            records.sort_by(|a, b| (&a.event_time, &a.sort_key).cmp(&(&b.event_time, &b.sort_key)));
            records.truncate(limit);
            span.record("pulpitum.records.returned", records.len());
            Ok(records)
        }
    }
}
