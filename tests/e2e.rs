//! Jepsen-style, single-machine fault probes.
//!
//! These are intentionally ignored by default. Start the bounded harness with
//! `./docker/scripts/run-e2e.sh`, or run `cargo test --test e2e -- --ignored
//! --test-threads=1`. They require Docker Compose services, not credentials.

use chrono::{Datelike, TimeZone, Utc};
use pulpitum::{
    ArchiveRecoveryConfig, ArchiveStore, BucketId, BucketState, BucketStrategy,
    CockroachDurableBucketStore, CockroachPoolConfig, DurableArchiveCoordinator,
    DurableArchiveRecoveryRunner, DurableBucketRead, DurableBucketStore, DurableBucketStoreError,
    DurableTable, OpenDalArchiveStore, PartitionKey, Query, Record, SpikySqlLoadProfile,
    TableDefinition, TableId, TimeRange,
};
use std::{
    collections::HashMap,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::MissedTickBehavior,
};

// These host ports are Toxiproxy listeners, not direct service ports. Keeping
// all application traffic on this path makes the fault scripts meaningful.
const CRDB_URL: &str = "postgresql://root@127.0.0.1:26267/defaultdb?sslmode=disable";
const MINIO_ENDPOINT: &str = "http://127.0.0.1:19000";
const DEFAULT_TABLE_ID: &str = "pulpitum.default.records";
const DURABLE_SCHEDULED_TABLE_ID: &str = "pulpitum.e2e.durable-scheduled";

fn test_bucket(
    table_id: &str,
    partition_key: impl Into<PartitionKey>,
    timestamp: chrono::DateTime<Utc>,
) -> BucketId {
    BucketId::for_event_time_with_strategy(
        TableId::new(table_id).expect("test table ID is valid"),
        partition_key,
        BucketStrategy::CalendarYearUtc,
        timestamp,
    )
}

fn compose(args: &[&str]) {
    let status = Command::new("docker")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["compose"])
        .args(args)
        .status()
        .expect("Docker must be installed for E2E tests");
    assert!(status.success(), "docker compose {args:?} failed");
}

async fn wait_for_minio() {
    for _ in 0..30 {
        let store = OpenDalArchiveStore::s3(
            MINIO_ENDPOINT,
            "pulpitum",
            "minioadmin",
            "minioadmin",
            "healthcheck",
        )
        .unwrap();
        let bucket = test_bucket("pulpitum.e2e.healthcheck", "healthcheck", Utc::now());
        if store.get_bucket(&bucket, "does-not-exist").await.is_err() {
            // A missing object proves the service answered an S3 request.
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("MinIO did not become reachable within 30 seconds");
}

async fn wait_for_durable_archive_route(
    store: &CockroachDurableBucketStore,
    bucket: &BucketId,
    range: &TimeRange,
    object_key: &str,
) {
    for _ in 0..60 {
        if let DurableBucketRead::Archive(key) = store
            .read_range(bucket, range, None, usize::MAX)
            .await
            .unwrap()
            && key == object_key
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("archive route did not converge within six seconds");
}

struct LoadRequest {
    sequence: u64,
    shard_index: usize,
    is_query: bool,
}

#[derive(Default)]
struct LoadCounters {
    enqueued: AtomicU64,
    append_failures: AtomicU64,
    query_failures: AtomicU64,
}

struct LoadOutcome {
    started_at: chrono::DateTime<Utc>,
    expected_records: Vec<Record>,
    enqueued: u64,
    append_failures: u64,
    query_failures: u64,
}

#[async_trait::async_trait]
trait LoadTable: Send + Sync {
    async fn append_for_load(&self, record: Record) -> bool;
    async fn query_for_load(&self, query: Query) -> bool;
    async fn read_for_load(&self, shard: &str, range: TimeRange) -> Option<Vec<Record>>;
}

#[async_trait::async_trait]
impl LoadTable for DurableTable {
    async fn append_for_load(&self, record: Record) -> bool {
        self.append(record).await.is_ok()
    }

    async fn query_for_load(&self, query: Query) -> bool {
        self.query_page(query).await.is_ok()
    }

    async fn read_for_load(&self, shard: &str, range: TimeRange) -> Option<Vec<Record>> {
        self.query(shard, range).await.ok()
    }
}

struct SpikyLoad {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<LoadOutcome>,
}

impl SpikyLoad {
    fn start<T>(table: Arc<T>, shards: Vec<String>) -> Self
    where
        T: LoadTable + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let table: Arc<dyn LoadTable> = table;
        let task = tokio::spawn(run_spiky_load(table, shards, stop.clone()));
        Self { stop, task }
    }

    async fn stop(self) -> LoadOutcome {
        self.stop.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(15), self.task)
            .await
            .expect("spiky load workers must stop within 15 seconds")
            .expect("spiky load task must not panic")
    }
}

async fn run_spiky_load_worker(
    table: Arc<dyn LoadTable>,
    shards: Arc<Vec<String>>,
    receiver: Arc<Mutex<mpsc::Receiver<LoadRequest>>>,
    expected_records: Arc<Mutex<Vec<Record>>>,
    counters: Arc<LoadCounters>,
) {
    loop {
        let request = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };
        let Some(request) = request else {
            return;
        };
        let shard = &shards[request.shard_index];
        if request.is_query {
            let now = Utc::now();
            if !table
                .query_for_load(Query {
                    partition_key: shard.into(),
                    range: TimeRange {
                        start: now - chrono::Duration::minutes(5),
                        end: now + chrono::Duration::seconds(1),
                    },
                    after: None,
                    limit: 25,
                })
                .await
            {
                counters.query_failures.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }

        let record = Record {
            partition_key: shard.into(),
            event_time: Utc::now(),
            sort_key: format!("load-{sequence:020}", sequence = request.sequence).into(),
            value: request.sequence.to_be_bytes().to_vec(),
        };
        if table.append_for_load(record.clone()).await {
            expected_records.lock().await.push(record);
        } else {
            counters.append_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run_spiky_load(
    table: Arc<dyn LoadTable>,
    shards: Vec<String>,
    stop: Arc<AtomicBool>,
) -> LoadOutcome {
    let profile = SpikySqlLoadProfile::default();
    let started_at = Utc::now();
    let (sender, receiver) = mpsc::channel(SpikySqlLoadProfile::QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    let shards = Arc::new(shards);
    let expected_records = Arc::new(Mutex::new(Vec::new()));
    let counters = Arc::new(LoadCounters::default());
    let mut workers = JoinSet::new();
    for _ in 0..SpikySqlLoadProfile::WORKERS {
        workers.spawn(run_spiky_load_worker(
            table.clone(),
            shards.clone(),
            receiver.clone(),
            expected_records.clone(),
            counters.clone(),
        ));
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut second = 0_usize;
    let mut sequence = 0_u64;
    while !stop.load(Ordering::Acquire) {
        ticker.tick().await;
        if stop.load(Ordering::Acquire) {
            break;
        }
        let offered = profile.operations_for_second(second);

        let mut enqueued = 0_u64;
        for _ in 0..offered {
            let request = LoadRequest {
                sequence,
                shard_index: sequence as usize % shards.len(),
                is_query: profile.is_query(sequence),
            };
            sequence = sequence.wrapping_add(1);
            match sender.try_send(request) {
                Ok(()) => enqueued += 1,
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
        counters.enqueued.fetch_add(enqueued, Ordering::Relaxed);

        second = second.wrapping_add(1);
    }
    drop(sender);
    while let Some(result) = workers.join_next().await {
        result.expect("spiky load worker must not panic");
    }

    LoadOutcome {
        started_at,
        expected_records: std::mem::take(&mut *expected_records.lock().await),
        enqueued: counters.enqueued.load(Ordering::Relaxed),
        append_failures: counters.append_failures.load(Ordering::Relaxed),
        query_failures: counters.query_failures.load(Ordering::Relaxed),
    }
}

async fn assert_successful_live_records_are_readable<T>(table: &T, outcome: &LoadOutcome)
where
    T: LoadTable + ?Sized,
{
    let mut expected_by_shard: HashMap<&str, Vec<&Record>> = HashMap::new();
    for record in &outcome.expected_records {
        expected_by_shard
            .entry(
                record
                    .partition_key
                    .as_utf8()
                    .expect("load partition keys are UTF-8"),
            )
            .or_default()
            .push(record);
    }
    for (shard, expected) in expected_by_shard {
        let actual = table
            .read_for_load(
                shard,
                TimeRange {
                    start: outcome.started_at - chrono::Duration::seconds(1),
                    end: Utc::now() + chrono::Duration::seconds(1),
                },
            )
            .await
            .expect("live load records must remain readable after faults");
        let actual_by_id: HashMap<_, _> = actual
            .into_iter()
            .map(|record| (record.sort_key.clone(), record))
            .collect();
        for record in expected {
            assert_eq!(
                actual_by_id.get(&record.sort_key),
                Some(record),
                "successful live write {} is missing or changed",
                record.sort_key.as_utf8().expect("load sort keys are UTF-8")
            );
        }
    }
}

fn closed_bucket_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(Utc::now().year() - 1, 6, 1, 12, 0, 0)
        .single()
        .expect("a fixed UTC timestamp is valid")
}

fn closed_bucket_range(timestamp: chrono::DateTime<Utc>) -> TimeRange {
    TimeRange {
        start: Utc
            .with_ymd_and_hms(timestamp.year(), 1, 1, 0, 0, 0)
            .single()
            .expect("start of a UTC year is valid"),
        end: Utc
            .with_ymd_and_hms(timestamp.year() + 1, 1, 1, 0, 0, 0)
            .single()
            .expect("start of the next UTC year is valid"),
    }
}

#[tokio::test]
#[ignore = "requires docker compose E2E environment"]
async fn durable_tables_with_overlapping_buckets_are_isolated_through_archive_cleanup() {
    let store = Arc::new(
        CockroachDurableBucketStore::connect_insecure_dev(CRDB_URL)
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();

    let run = Utc::now().timestamp_nanos_opt().unwrap();
    let archive = Arc::new(
        OpenDalArchiveStore::s3(
            MINIO_ENDPOINT,
            "pulpitum",
            "minioadmin",
            "minioadmin",
            &format!("namespace-isolation-{run}"),
        )
        .unwrap(),
    );
    let messages_id = TableId::new("pulpitum.e2e.namespace.messages").unwrap();
    let reactions_id = TableId::new("pulpitum.e2e.namespace.reactions").unwrap();
    let messages = DurableTable::with_definition(
        TableDefinition::chat_messages("messages", messages_id.clone()),
        store.clone(),
        archive.clone(),
    )
    .unwrap();
    let reactions = DurableTable::with_definition(
        TableDefinition::chat_messages("reactions", reactions_id.clone()),
        store.clone(),
        archive.clone(),
    )
    .unwrap();
    let timestamp = closed_bucket_timestamp();
    let range = closed_bucket_range(timestamp);
    let shard = format!("namespace-isolation-{run}");
    let message = Record {
        partition_key: shard.clone().into(),
        event_time: timestamp,
        sort_key: "message-1".into(),
        value: b"message".to_vec(),
    };
    let reaction = Record {
        partition_key: shard.clone().into(),
        event_time: timestamp,
        sort_key: "reaction-1".into(),
        value: b"reaction".to_vec(),
    };
    let messages_bucket = test_bucket(messages_id.as_str(), &shard, timestamp);
    let reactions_bucket = test_bucket(reactions_id.as_str(), &shard, timestamp);

    messages.append(message.clone()).await.unwrap();
    reactions.append(reaction.clone()).await.unwrap();
    assert_eq!(
        messages.query(&shard, range.clone()).await.unwrap(),
        vec![message.clone()]
    );
    assert_eq!(
        reactions.query(&shard, range.clone()).await.unwrap(),
        vec![reaction.clone()]
    );

    let outcome = DurableArchiveCoordinator::new(store.clone(), archive.clone())
        .archive_bucket(messages_bucket.clone())
        .await
        .unwrap();
    wait_for_durable_archive_route(&store, &messages_bucket, &range, &outcome.object_key).await;

    assert_eq!(
        archive
            .get_bucket(&messages_bucket, &outcome.object_key)
            .await
            .unwrap(),
        vec![message.clone()],
        "the published manifest must identify and return only the messages bucket"
    );
    assert_eq!(
        messages.query(&shard, range.clone()).await.unwrap(),
        vec![message]
    );
    assert_eq!(
        reactions.query(&shard, range).await.unwrap(),
        vec![reaction.clone()]
    );
    assert!(
        matches!(
            store
                .read_range(
                    &messages_bucket,
                    &closed_bucket_range(timestamp),
                    None,
                    usize::MAX,
                )
                .await
                .unwrap(),
            DurableBucketRead::Archive(key) if key == outcome.object_key
        ),
        "archiving messages must route only the messages bucket to its archive"
    );
    assert_eq!(
        store
            .read_range(
                &reactions_bucket,
                &closed_bucket_range(timestamp),
                None,
                usize::MAX,
            )
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![reaction]),
        "cleanup must not delete or archive the overlapping reactions bucket"
    );
}

async fn stage_closed_bucket(
    table: &DurableTable,
    run: i64,
    sequence: usize,
) -> (BucketId, Vec<Record>) {
    let shard = format!("durable-scheduled-{run}-{sequence}");
    let timestamp = closed_bucket_timestamp();
    let records: Vec<_> = (0..8)
        .map(|offset| Record {
            partition_key: shard.clone().into(),
            event_time: timestamp,
            sort_key: format!("scheduled-{sequence:03}-{offset:03}").into(),
            value: format!("closed bucket {sequence}, record {offset}").into_bytes(),
        })
        .collect();
    let bucket = test_bucket(DURABLE_SCHEDULED_TABLE_ID, &shard, timestamp);
    for record in records.clone() {
        table.append(record).await.unwrap();
    }
    (bucket, records)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires docker compose E2E environment"]
async fn durable_scheduled_archives_survive_faults_while_hot_load_continues() {
    // CockroachDB's unlicensed test image enforces a low concurrent-transaction
    // ceiling. Keep this fault scenario below it while still exercising pool
    // queueing and concurrent append/archive behavior.
    let store = Arc::new(
        CockroachDurableBucketStore::connect_insecure_dev_with_pool_config(
            CRDB_URL,
            CockroachPoolConfig {
                max_connections: 4,
                ..CockroachPoolConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    store.migrate().await.unwrap();

    let run = Utc::now().timestamp_nanos_opt().unwrap();
    let prefix = format!("durable-scheduled-{run}");
    let archive = Arc::new(
        OpenDalArchiveStore::s3(
            MINIO_ENDPOINT,
            "pulpitum",
            "minioadmin",
            "minioadmin",
            &prefix,
        )
        .unwrap(),
    );
    let table = Arc::new(
        DurableTable::with_definition(
            TableDefinition::chat_messages(
                "messages",
                TableId::new(DURABLE_SCHEDULED_TABLE_ID).expect("test table ID is valid"),
            ),
            store.clone(),
            archive.clone(),
        )
        .unwrap(),
    );
    let coordinator = DurableArchiveCoordinator::new(store.clone(), archive);

    // The archive candidates are distinct closed prior-year buckets. The load
    // stays on current-year buckets, mirroring the production rule that an
    // archived `(shard, year)` must never receive another append.
    let load = SpikyLoad::start(
        table.clone(),
        (0..3)
            .map(|index| format!("durable-live-{run}-{index}"))
            .collect(),
    );
    tokio::time::sleep(Duration::from_secs(4)).await;

    let mut archived = Vec::new();
    for sequence in 0..3 {
        let (bucket, records) = stage_closed_bucket(&table, run, sequence).await;
        if sequence == 1 {
            // This is the accelerated equivalent of one scheduled run finding
            // its archive tier down: the bucket must reopen and the next
            // attempt must archive that same bucket, not a later one.
            compose(&["pause", "minio"]);
            assert!(
                coordinator.archive_bucket(bucket.clone()).await.is_err(),
                "an unavailable archive tier must fail the scheduled cutover"
            );
            compose(&["unpause", "minio"]);
            wait_for_minio().await;
            assert_eq!(store.state(&bucket).await.unwrap(), BucketState::Hot);
            assert_eq!(
                table
                    .query(
                        bucket.partition_key.clone(),
                        closed_bucket_range(records[0].event_time),
                    )
                    .await
                    .unwrap(),
                records
            );
        }

        let outcome = coordinator.archive_bucket(bucket.clone()).await.unwrap();
        assert_eq!(outcome.bucket, bucket);
        assert_eq!(outcome.records_archived, records.len());
        assert_eq!(
            store.state(&bucket).await.unwrap(),
            BucketState::Archived {
                object_key: outcome.object_key,
                hot_deleted: true,
            }
        );
        assert_eq!(
            table
                .query(
                    bucket.partition_key.clone(),
                    closed_bucket_range(records[0].event_time),
                )
                .await
                .unwrap(),
            records
        );
        assert!(
            table.append(records[0].clone()).await.is_err(),
            "an archived (shard, year) bucket must reject later appends"
        );
        archived.push((bucket, records));
    }

    // Archive reads must remain routable while one Cockroach node is absent.
    compose(&["stop", "cockroach-2"]);
    for (bucket, records) in &archived {
        assert_eq!(
            table
                .query(
                    bucket.partition_key.clone(),
                    closed_bucket_range(records[0].event_time),
                )
                .await
                .unwrap(),
            *records
        );
    }

    let load_outcome = load.stop().await;
    assert!(
        load_outcome.enqueued > 0 && !load_outcome.expected_records.is_empty(),
        "the durable spiky load must exercise current hot buckets"
    );
    assert_eq!(
        load_outcome.append_failures, 0,
        "hot writes must not fail while scheduled archiving and faults run"
    );
    assert_eq!(
        load_outcome.query_failures, 0,
        "hot reads must remain available while scheduled archiving and faults run"
    );
    assert_successful_live_records_are_readable(table.as_ref(), &load_outcome).await;
    compose(&["start", "cockroach-2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose E2E environment"]
async fn durable_cockroach_store_fences_and_archives_a_bucket() {
    let store = CockroachDurableBucketStore::connect_insecure_dev(CRDB_URL)
        .await
        .unwrap();
    store.migrate().await.unwrap();
    store.validate_schema().await.unwrap();

    let shard = format!("durable-{}", Utc::now().timestamp_nanos_opt().unwrap());
    let timestamp = Utc.with_ymd_and_hms(2023, 6, 1, 12, 0, 0).single().unwrap();
    let first = Record {
        partition_key: shard.clone().into(),
        event_time: timestamp,
        sort_key: "a".into(),
        value: b"first".to_vec(),
    };
    let second = Record {
        partition_key: shard.into(),
        event_time: timestamp,
        sort_key: "b".into(),
        value: b"second".to_vec(),
    };
    let bucket = test_bucket(
        "pulpitum.e2e.durable-fencing",
        first.partition_key.clone(),
        first.event_time,
    );

    let range = TimeRange {
        start: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).single().unwrap(),
        end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
    };
    store.append(&bucket, first.clone()).await.unwrap();
    store.append(&bucket, second.clone()).await.unwrap();
    assert_eq!(store.state(&bucket).await.unwrap(), BucketState::Hot);
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![first.clone(), second.clone()]),
    );

    let first_session = store.begin_archive(&bucket).await.unwrap();
    assert_eq!(first_session.generation(), 1);
    assert_eq!(
        store.snapshot(&first_session).await.unwrap(),
        vec![first.clone(), second.clone()]
    );
    assert_eq!(
        store
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![first.clone(), second.clone()]),
    );
    assert!(matches!(
        store.append(&bucket, first.clone()).await,
        Err(DurableBucketStoreError::BucketReadOnly(
            BucketState::Archiving
        ))
    ));

    store.abort_archive(&first_session).await.unwrap();
    let session = store.begin_archive(&bucket).await.unwrap();
    assert_eq!(session.generation(), 2);
    assert!(matches!(
        store.snapshot(&first_session).await,
        Err(DurableBucketStoreError::StaleArchiveSession)
    ));

    let object_key = "archives/durable/2023/records.json".to_owned();
    store
        .publish_archive(&session, object_key.clone())
        .await
        .unwrap();
    wait_for_durable_archive_route(&store, &bucket, &range, &object_key).await;
    assert!(matches!(
        store.abort_archive(&session).await,
        Err(DurableBucketStoreError::StaleArchiveSession)
    ));
    store.delete_hot_bucket(&session).await.unwrap();
    assert_eq!(
        store.state(&bucket).await.unwrap(),
        BucketState::Archived {
            object_key: object_key.clone(),
            hot_deleted: true,
        }
    );
    wait_for_durable_archive_route(&store, &bucket, &range, &object_key).await;
    assert!(matches!(
        store.delete_hot_bucket(&session).await,
        Err(DurableBucketStoreError::StaleArchiveSession)
    ));
}

/// Regression for a reader process that observed Hot before another process
/// published and cleaned up the bucket. The final read must select metadata and
/// rows from one statement snapshot rather than reuse the old Hot observation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose E2E environment"]
async fn durable_routed_read_is_statement_consistent_across_independent_stores() {
    let writer = CockroachDurableBucketStore::connect_insecure_dev(CRDB_URL)
        .await
        .unwrap();
    writer.migrate().await.unwrap();
    let reader = CockroachDurableBucketStore::connect_insecure_dev(CRDB_URL)
        .await
        .unwrap();

    let run = Utc::now().timestamp_nanos_opt().unwrap();
    let timestamp = Utc.with_ymd_and_hms(2023, 6, 1, 12, 0, 0).single().unwrap();
    let record = Record {
        partition_key: format!("statement-consistent-read-{run}").into(),
        event_time: timestamp,
        sort_key: "visible-before-cutover".into(),
        value: b"must route to the archive after cleanup".to_vec(),
    };
    let bucket = test_bucket(
        "pulpitum.e2e.statement-consistent-read",
        record.partition_key.clone(),
        timestamp,
    );
    let range = TimeRange {
        start: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).single().unwrap(),
        end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
    };

    writer.append(&bucket, record.clone()).await.unwrap();
    assert_eq!(
        reader
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Hot(vec![record.clone()]),
        "the independent reader first observes the hot route"
    );

    let session = writer.begin_archive(&bucket).await.unwrap();
    assert_eq!(writer.snapshot(&session).await.unwrap(), vec![record]);
    let object_key = format!("archives/statement-consistent-read/{run}/manifest.json");
    writer
        .publish_archive(&session, object_key.clone())
        .await
        .unwrap();
    writer.delete_hot_bucket(&session).await.unwrap();

    assert_eq!(
        reader
            .read_range(&bucket, &range, None, usize::MAX)
            .await
            .unwrap(),
        DurableBucketRead::Archive(object_key),
        "a prior Hot observation must not select the tier after remote cleanup"
    );
}

/// Jepsen-style recovery probe: worker A publishes an object and disappears
/// before cleanup; independently constructed worker B discovers and completes
/// the pending cleanup using a new durable lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose E2E environment"]
async fn durable_recovery_runner_takes_over_published_cleanup_after_worker_loss() {
    let store = Arc::new(
        CockroachDurableBucketStore::connect_insecure_dev(CRDB_URL)
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    let run = Utc::now().timestamp_nanos_opt().unwrap();
    let archive = Arc::new(
        OpenDalArchiveStore::s3(
            MINIO_ENDPOINT,
            "pulpitum",
            "minioadmin",
            "minioadmin",
            &format!("recovery-runner-{run}"),
        )
        .unwrap(),
    );
    let table = DurableTable::new(store.clone(), archive.clone());
    let archive_year = Utc::now().year() - 1;
    let record = Record {
        partition_key: format!("recovery-worker-loss-{run}").into(),
        event_time: Utc
            .with_ymd_and_hms(archive_year, 6, 1, 12, 0, 0)
            .single()
            .unwrap(),
        sort_key: "survives-takeover".into(),
        value: b"must survive coordinator restart".to_vec(),
    };
    let bucket = test_bucket(
        DEFAULT_TABLE_ID,
        record.partition_key.clone(),
        record.event_time,
    );
    table.append(record.clone()).await.unwrap();

    // Worker A has durably published but does not clean up. Deferring its
    // lease models process loss without relying on Tokio task cancellation.
    let worker_a = store.begin_archive(&bucket).await.unwrap();
    let records = store.snapshot(&worker_a).await.unwrap();
    let object_key = archive
        .put_bucket_generation(&bucket, worker_a.generation(), &records)
        .await
        .unwrap();
    store.publish_archive(&worker_a, object_key).await.unwrap();
    store
        .defer_cleanup(&worker_a, Duration::from_millis(25))
        .await
        .unwrap();
    assert!(matches!(
        store.state(&bucket).await.unwrap(),
        BucketState::Archived {
            hot_deleted: false,
            ..
        }
    ));

    let worker_b = DurableArchiveRecoveryRunner::new(
        store.clone(),
        archive.clone(),
        ArchiveRecoveryConfig {
            eligible_before: Utc
                .with_ymd_and_hms(archive_year + 1, 1, 1, 0, 0, 0)
                .single()
                .unwrap(),
            scan_limit: 8,
            lease_for: Duration::from_secs(2),
            lease_renewal_interval: Duration::from_millis(500),
            retry_backoff: Duration::from_millis(25),
        },
    )
    .unwrap();
    // CockroachDB evaluates `now()` at the statement's timestamp. Poll boundedly
    // instead of relying on a fixed local sleep to cross the persisted retry
    // boundary under scheduler or CI contention.
    let mut completed_outcome = None;
    for _ in 0..30 {
        let outcome = worker_b.run_once().await.unwrap();
        if matches!(
            store.state(&bucket).await.unwrap(),
            BucketState::Archived {
                hot_deleted: true,
                ..
            }
        ) {
            completed_outcome = Some(outcome);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let outcome =
        completed_outcome.expect("recovery worker must clean this bucket within three seconds");

    // The local harness intentionally preserves volumes between runs, so an
    // earlier interrupted run may contribute additional work to this cycle.
    // The unique bucket state above establishes this worker completed its
    // cleanup; the aggregate counters must therefore be non-zero.
    assert!(outcome.discovered >= 1);
    assert!(outcome.claimed >= 1);
    assert!(outcome.completed >= 1);
    assert_eq!(
        table
            .query(
                bucket.partition_key.clone(),
                TimeRange {
                    start: Utc
                        .with_ymd_and_hms(archive_year, 1, 1, 0, 0, 0)
                        .single()
                        .unwrap(),
                    end: Utc
                        .with_ymd_and_hms(archive_year + 1, 1, 1, 0, 0, 0)
                        .single()
                        .unwrap(),
                },
            )
            .await
            .unwrap(),
        vec![record]
    );
}
