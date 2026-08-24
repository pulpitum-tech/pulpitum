use crate::cockroach_durable::routed_read_from_snapshot;
use crate::{
    ArchiveRecoveryConfig, ArchiveScan, ArchiveSession, ArchiveStore, ArchiveWork, BucketId,
    BucketState, BucketStrategy, ClusteringColumn, Cursor, DurableArchiveCoordinator,
    DurableArchiveError, DurableArchiveRecoveryRunner, DurableBucketRead, DurableBucketStore,
    DurableBucketStoreError, DurableTable, DurableTableError, InMemoryArchiveStore,
    InMemoryDurableBucketStore, Query, Record, SortDirection, StoreError, TableDefinition, TableId,
    TimeRange,
};
use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Utc};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Barrier, Notify};

fn record(sort_key: &str, year: i32) -> Record {
    Record {
        partition_key: "general".into(),
        event_time: Utc.with_ymd_and_hms(year, 6, 1, 12, 0, 0).single().unwrap(),
        sort_key: sort_key.into(),
        value: sort_key.as_bytes().to_vec(),
    }
}

fn test_table_id() -> TableId {
    TableId::new("pulpitum.tests.records").unwrap()
}

fn bucket(record: &Record) -> BucketId {
    BucketId::for_event_time_with_strategy(
        test_table_id(),
        record.partition_key.clone(),
        BucketStrategy::CalendarYearUtc,
        record.event_time,
    )
}

struct FailingArchiveStore;

#[derive(Default)]
struct BlockingArchiveStore {
    inner: InMemoryArchiveStore,
    started: Notify,
    release: Notify,
}

struct FailingPublishStore {
    inner: Arc<InMemoryDurableBucketStore>,
    read_barrier: Option<Arc<Barrier>>,
}

impl crate::storage::durable_bucket_store_sealed::Sealed for FailingPublishStore {}

#[async_trait]
impl DurableBucketStore for FailingPublishStore {
    async fn append(
        &self,
        bucket: &BucketId,
        record: Record,
    ) -> Result<(), DurableBucketStoreError> {
        self.inner.append(bucket, record).await
    }

    async fn state(&self, bucket: &BucketId) -> Result<BucketState, DurableBucketStoreError> {
        self.inner.state(bucket).await
    }

    async fn read_range(
        &self,
        bucket: &BucketId,
        range: &TimeRange,
        after: Option<&Cursor>,
        limit: usize,
    ) -> Result<DurableBucketRead, DurableBucketStoreError> {
        if let Some(barrier) = &self.read_barrier {
            barrier.wait().await;
        }
        self.inner.read_range(bucket, range, after, limit).await
    }

    async fn discover_archive_work(
        &self,
        scan: ArchiveScan,
    ) -> Result<Vec<ArchiveWork>, DurableBucketStoreError> {
        self.inner.discover_archive_work(scan).await
    }

    async fn claim_archive(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        self.inner.claim_archive(bucket, lease_for).await
    }

    async fn claim_cleanup(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        self.inner.claim_cleanup(bucket, lease_for).await
    }

    async fn renew_archive_lease(
        &self,
        session: &ArchiveSession,
        lease_for: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        self.inner.renew_archive_lease(session, lease_for).await
    }

    async fn defer_archive(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        self.inner.defer_archive(session, retry_after).await
    }

    async fn defer_cleanup(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        self.inner.defer_cleanup(session, retry_after).await
    }

    async fn begin_archive(
        &self,
        bucket: &BucketId,
    ) -> Result<ArchiveSession, DurableBucketStoreError> {
        self.inner.begin_archive(bucket).await
    }

    async fn snapshot(
        &self,
        session: &ArchiveSession,
    ) -> Result<Vec<Record>, DurableBucketStoreError> {
        self.inner.snapshot(session).await
    }

    async fn publish_archive(
        &self,
        _: &ArchiveSession,
        _: String,
    ) -> Result<(), DurableBucketStoreError> {
        Err(DurableBucketStoreError::OperationFailed)
    }

    async fn delete_hot_bucket(
        &self,
        session: &ArchiveSession,
    ) -> Result<(), DurableBucketStoreError> {
        self.inner.delete_hot_bucket(session).await
    }

    async fn abort_archive(&self, session: &ArchiveSession) -> Result<(), DurableBucketStoreError> {
        self.inner.abort_archive(session).await
    }
}

#[async_trait]
impl ArchiveStore for FailingArchiveStore {
    async fn put_bucket(&self, _: &BucketId, _: &[Record]) -> Result<String, StoreError> {
        Err(StoreError::Other("upload failed".into()))
    }

    async fn get_bucket(&self, _: &BucketId, _: &str) -> Result<Vec<Record>, StoreError> {
        Err(StoreError::Other("archive unavailable".into()))
    }
}

#[async_trait]
impl ArchiveStore for BlockingArchiveStore {
    async fn put_bucket(
        &self,
        bucket: &BucketId,
        records: &[Record],
    ) -> Result<String, StoreError> {
        self.inner.put_bucket(bucket, records).await
    }

    async fn put_bucket_generation(
        &self,
        bucket: &BucketId,
        generation: u64,
        records: &[Record],
    ) -> Result<String, StoreError> {
        self.started.notify_one();
        self.release.notified().await;
        self.inner
            .put_bucket_generation(bucket, generation, records)
            .await
    }

    async fn get_bucket(
        &self,
        bucket: &BucketId,
        object_key: &str,
    ) -> Result<Vec<Record>, StoreError> {
        self.inner.get_bucket(bucket, object_key).await
    }
}

#[tokio::test]
async fn durable_bucket_store_fences_appends_and_archive_mutations() {
    let store = InMemoryDurableBucketStore::default();
    let first = record("durable-first", 2023);
    let bucket = bucket(&first);

    store.append(&bucket, first.clone()).await.unwrap();
    let first_session = store.begin_archive(&bucket).await.unwrap();
    assert_eq!(first_session.bucket(), &bucket);
    assert_eq!(first_session.generation(), 1);
    assert_eq!(store.snapshot(&first_session).await.unwrap(), vec![first]);
    assert!(matches!(
        store
            .append(&bucket, record("durable-rejected", 2023))
            .await,
        Err(DurableBucketStoreError::BucketReadOnly(
            BucketState::Archiving
        ))
    ));

    store.abort_archive(&first_session).await.unwrap();
    let second_session = store.begin_archive(&bucket).await.unwrap();
    assert_eq!(second_session.generation(), 2);
    assert!(matches!(
        store.snapshot(&first_session).await,
        Err(DurableBucketStoreError::StaleArchiveSession)
    ));

    store
        .publish_archive(&second_session, "archives/general/2023/data.json".into())
        .await
        .unwrap();
    store.delete_hot_bucket(&second_session).await.unwrap();
    assert_eq!(
        store.state(&bucket).await.unwrap(),
        BucketState::Archived {
            object_key: "archives/general/2023/data.json".into(),
            hot_deleted: true,
        }
    );
}

#[tokio::test]
async fn durable_bucket_range_reads_return_a_statement_consistent_route() {
    let store = InMemoryDurableBucketStore::default();
    let mut before = record("before", 2023);
    before.event_time = Utc
        .with_ymd_and_hms(2023, 5, 31, 23, 59, 59)
        .single()
        .unwrap();
    let later = record("b", 2023);
    let earlier = record("a", 2023);
    let bucket = bucket(&earlier);
    let range = TimeRange {
        start: Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).single().unwrap(),
        end: Utc.with_ymd_and_hms(2023, 6, 2, 0, 0, 0).single().unwrap(),
    };

    store.append(&bucket, later.clone()).await.unwrap();
    store.append(&bucket, before).await.unwrap();
    store.append(&bucket, earlier.clone()).await.unwrap();
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![earlier.clone(), later.clone()]),
    );
    assert_eq!(
        store
            .read_range(&bucket, &range, Some(&Cursor::from(&earlier)), 1)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![later.clone()]),
    );
    assert_eq!(
        store.read_range(&bucket, &range, None, 0).await.unwrap(),
        DurableBucketRead::Hot(Vec::new()),
    );

    let session = store.begin_archive(&bucket).await.unwrap();
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![earlier.clone(), later.clone()]),
    );

    let object_key = "archives/general/2023/data.json".to_owned();
    store
        .publish_archive(&session, object_key.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Archive(object_key.clone()),
    );
    assert_eq!(
        store.read_range(&bucket, &range, None, 0).await.unwrap(),
        DurableBucketRead::Archive(object_key.clone()),
    );

    store.delete_hot_bucket(&session).await.unwrap();
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Archive(object_key),
    );
}

#[test]
fn routed_read_snapshot_rejects_mixed_or_invalid_tier_results() {
    let visible = record("visible", 2023);
    assert_eq!(
        routed_read_from_snapshot("hot", None, vec![visible.clone()]).unwrap(),
        DurableBucketRead::Hot(vec![visible.clone()])
    );
    assert_eq!(
        routed_read_from_snapshot("archiving", None, vec![visible.clone()]).unwrap(),
        DurableBucketRead::Hot(vec![visible.clone()])
    );
    assert_eq!(
        routed_read_from_snapshot(
            "archived",
            Some("archives/general/2023/manifest.json".into()),
            Vec::new(),
        )
        .unwrap(),
        DurableBucketRead::Archive("archives/general/2023/manifest.json".into())
    );
    assert!(
        routed_read_from_snapshot(
            "archived",
            Some("archives/general/2023/manifest.json".into()),
            vec![visible],
        )
        .is_err(),
        "an archived route must not be combined with hot rows"
    );
    assert!(routed_read_from_snapshot("archived", None, Vec::new()).is_err());
}

#[tokio::test]
async fn durable_table_routes_hot_and_archived_buckets_with_contiguous_cursors() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(InMemoryArchiveStore::default());
    let table = DurableTable::with_definition(
        TableDefinition::chat_messages("messages", test_table_id()),
        store.clone(),
        archive.clone(),
    )
    .unwrap();
    let current_year = Utc::now().year();
    let archived = record("archived", current_year - 1);
    let hot = record("hot", current_year);

    table.append(archived.clone()).await.unwrap();
    table.append(hot.clone()).await.unwrap();
    let outcome = DurableArchiveCoordinator::new(store.clone(), archive)
        .archive_bucket(bucket(&archived))
        .await
        .unwrap();
    assert_eq!(outcome.records_archived, 1);

    let range = TimeRange {
        start: Utc
            .with_ymd_and_hms(current_year - 1, 1, 1, 0, 0, 0)
            .single()
            .unwrap(),
        end: Utc
            .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
            .single()
            .unwrap(),
    };
    let first_page = table
        .query_page(Query {
            partition_key: "general".into(),
            range: range.clone(),
            after: None,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_page.records, vec![archived]);
    let second_page = table
        .query_page(Query {
            partition_key: "general".into(),
            range,
            after: first_page.next,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(second_page.records, vec![hot]);
    assert_eq!(second_page.next, None);
}

#[tokio::test]
async fn durable_table_reads_yearly_buckets_in_parallel() {
    let inner = Arc::new(InMemoryDurableBucketStore::default());
    let store = Arc::new(FailingPublishStore {
        inner: inner.clone(),
        read_barrier: Some(Arc::new(Barrier::new(2))),
    });
    let archive = Arc::new(InMemoryArchiveStore::default());
    let table = DurableTable::with_definition(
        TableDefinition::chat_messages("messages", test_table_id()),
        store,
        archive,
    )
    .unwrap();
    let current_year = Utc::now().year();
    let earlier = record("earlier", current_year - 1);
    let later = record("later", current_year);
    inner
        .append(&bucket(&earlier), earlier.clone())
        .await
        .unwrap();
    inner.append(&bucket(&later), later.clone()).await.unwrap();

    let page = tokio::time::timeout(
        Duration::from_millis(100),
        table.query_page(Query {
            partition_key: "general".into(),
            range: TimeRange {
                start: Utc
                    .with_ymd_and_hms(current_year - 1, 1, 1, 0, 0, 0)
                    .single()
                    .unwrap(),
                end: Utc
                    .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
                    .single()
                    .unwrap(),
            },
            after: None,
            limit: 10,
        }),
    )
    .await
    .expect("yearly reads should be in flight together")
    .unwrap();

    assert_eq!(page.records, vec![earlier, later]);
}

#[tokio::test]
async fn durable_table_validates_definition_and_write_window() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(InMemoryArchiveStore::default());
    let invalid = TableDefinition {
        name: " ".into(),
        table_id: test_table_id(),
        partition_key: "channel_id",
        bucket_time_key: "event_time",
        bucket_strategy: BucketStrategy::CalendarYearUtc,
        clustering_key: vec![
            ClusteringColumn {
                field: "event_time",
                direction: SortDirection::Ascending,
            },
            ClusteringColumn {
                field: "sort_key",
                direction: SortDirection::Ascending,
            },
        ],
        writable_buckets: 2,
    };
    assert!(matches!(
        DurableTable::with_definition(invalid, store.clone(), archive.clone()),
        Err(DurableTableError::Definition(_))
    ));

    let table = DurableTable::new(store, archive);
    let old = record("too-old", Utc::now().year() - 2);
    assert!(matches!(
        table.append(old).await,
        Err(DurableTableError::OutsideWriteWindow { .. })
    ));
}

#[test]
fn archive_recovery_config_rejects_an_invalid_renewal_interval() {
    let base = ArchiveRecoveryConfig {
        eligible_before: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
        scan_limit: 8,
        lease_for: Duration::from_secs(30),
        lease_renewal_interval: Duration::from_secs(10),
        retry_backoff: Duration::from_secs(5),
    };
    assert!(base.validate().is_ok());

    let mut zero = base.clone();
    zero.lease_renewal_interval = Duration::ZERO;
    assert!(zero.validate().is_err());

    let mut equal = base;
    equal.lease_renewal_interval = equal.lease_for;
    assert!(equal.validate().is_err());
}

#[tokio::test(start_paused = true)]
async fn durable_recovery_runner_renews_its_lease_during_a_slow_upload() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(BlockingArchiveStore::default());
    let staged = record("renewed-upload", 2023);
    let bucket = bucket(&staged);
    store.append(&bucket, staged).await.unwrap();

    let runner = DurableArchiveRecoveryRunner::new(
        store.clone(),
        archive.clone(),
        ArchiveRecoveryConfig {
            eligible_before: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
            scan_limit: 8,
            lease_for: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(250),
            retry_backoff: Duration::from_millis(100),
        },
    )
    .unwrap();

    let cycle = tokio::spawn(async move { runner.run_once().await });
    archive.started.notified().await;
    tokio::task::yield_now().await;
    for _ in 0..7 {
        tokio::time::advance(Duration::from_millis(300)).await;
        tokio::task::yield_now().await;
    }
    archive.release.notify_one();

    let outcome = cycle.await.unwrap().unwrap();
    assert_eq!(outcome.completed, 1);
    assert!(matches!(
        store.state(&bucket).await.unwrap(),
        BucketState::Archived {
            hot_deleted: true,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn direct_archiver_renews_its_lease_during_a_slow_upload() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(BlockingArchiveStore::default());
    let staged = record("renewed-direct-upload", 2023);
    let bucket = bucket(&staged);
    store.append(&bucket, staged).await.unwrap();

    let coordinator = DurableArchiveCoordinator::new(store.clone(), archive.clone());
    let cutover = tokio::spawn(async move { coordinator.archive_bucket(bucket).await });
    archive.started.notified().await;
    tokio::task::yield_now().await;
    for _ in 0..6 {
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
    }
    archive.release.notify_one();

    let outcome = cutover.await.unwrap().unwrap();
    assert_eq!(outcome.records_archived, 1);
}

#[tokio::test]
async fn durable_recovery_runner_archives_discovered_bucket_without_an_application_trigger() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(InMemoryArchiveStore::default());
    let record = record("recovery-runner", 2023);
    let bucket = bucket(&record);
    store.append(&bucket, record.clone()).await.unwrap();

    let runner = DurableArchiveRecoveryRunner::new(
        store.clone(),
        archive.clone(),
        ArchiveRecoveryConfig {
            eligible_before: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
            scan_limit: 8,
            lease_for: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(250),
            retry_backoff: Duration::from_millis(10),
        },
    )
    .unwrap();
    let outcome = runner.run_once().await.unwrap();

    assert_eq!(outcome.discovered, 1);
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.completed, 1);
    let BucketState::Archived {
        object_key,
        hot_deleted,
    } = store.state(&bucket).await.unwrap()
    else {
        panic!("runner must publish an archived route");
    };
    assert!(hot_deleted);
    assert!(object_key.contains("generation-1"));
    assert_eq!(
        archive.get_bucket(&bucket, &object_key).await.unwrap(),
        vec![record]
    );
}

#[tokio::test]
async fn durable_recovery_runner_claims_and_finishes_published_cleanup_after_owner_loss() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(InMemoryArchiveStore::default());
    let staged = record("recovery-cleanup", 2023);
    let bucket = bucket(&staged);
    store.append(&bucket, staged.clone()).await.unwrap();

    let first = store.begin_archive(&bucket).await.unwrap();
    let object_key = archive
        .put_bucket_generation(&bucket, first.generation(), std::slice::from_ref(&staged))
        .await
        .unwrap();
    store.publish_archive(&first, object_key).await.unwrap();
    store
        .defer_cleanup(&first, Duration::from_millis(5))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let runner = DurableArchiveRecoveryRunner::new(
        store.clone(),
        archive,
        ArchiveRecoveryConfig {
            eligible_before: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
            scan_limit: 8,
            lease_for: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(250),
            retry_backoff: Duration::from_millis(10),
        },
    )
    .unwrap();
    let outcome = runner.run_once().await.unwrap();

    assert_eq!(outcome.discovered, 1);
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.completed, 1);
    assert!(matches!(
        store.state(&bucket).await.unwrap(),
        BucketState::Archived {
            hot_deleted: true,
            ..
        }
    ));
    assert!(matches!(
        store
            .append(&bucket, record("recovery-cleanup-rejected", 2023))
            .await,
        Err(DurableBucketStoreError::BucketReadOnly(_))
    ));
}

#[tokio::test]
async fn durable_archiver_aborts_only_the_prepublication_upload_failure() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let bucket_record = record("upload-failure", Utc::now().year() - 1);
    let bucket = bucket(&bucket_record);
    store.append(&bucket, bucket_record).await.unwrap();

    let error = DurableArchiveCoordinator::new(store.clone(), Arc::new(FailingArchiveStore))
        .archive_bucket(bucket.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DurableArchiveError::Store(StoreError::Other(_))
    ));
    assert_eq!(store.state(&bucket).await.unwrap(), BucketState::Hot);
    assert!(
        store
            .append(
                &bucket,
                record("append-after-upload-failure", Utc::now().year() - 1),
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn durable_archiver_does_not_abort_after_a_publication_failure() {
    let inner = Arc::new(InMemoryDurableBucketStore::default());
    let store = Arc::new(FailingPublishStore {
        inner: inner.clone(),
        read_barrier: None,
    });
    let archive = Arc::new(InMemoryArchiveStore::default());
    let bucket_record = record("publish-failure", Utc::now().year() - 1);
    let bucket = bucket(&bucket_record);
    inner.append(&bucket, bucket_record).await.unwrap();

    let error = DurableArchiveCoordinator::new(store, archive)
        .archive_bucket(bucket.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DurableArchiveError::DurableStore(DurableBucketStoreError::OperationFailed)
    ));
    assert_eq!(inner.state(&bucket).await.unwrap(), BucketState::Archiving);
}

#[test]
fn table_definition_makes_the_cassandra_style_physical_clustering_key_explicit() {
    let definition = TableDefinition::chat_messages("messages", test_table_id());
    assert_eq!(definition.partition_key, "channel_id");
    assert_eq!(definition.bucket_time_key, "event_time");
    assert_eq!(
        definition.clustering_key,
        vec![
            ClusteringColumn {
                field: "event_time",
                direction: SortDirection::Ascending
            },
            ClusteringColumn {
                field: "sort_key",
                direction: SortDirection::Ascending
            },
        ]
    );
    assert!(
        TableDefinition::new(
            "bad",
            test_table_id(),
            "channel_id",
            "event_time",
            vec![ClusteringColumn {
                field: "sort_key",
                direction: SortDirection::Ascending
            },],
            2
        )
        .is_err()
    );
}
