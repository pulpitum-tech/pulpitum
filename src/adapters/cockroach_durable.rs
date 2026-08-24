use crate::cockroach_schema::{
    AppliedMigration, EXPECTED_V4_TABLES, ExpectedTable, MIGRATIONS, plan_migrations,
};
use crate::{
    ArchiveScan, ArchiveSession, ArchiveWork, BucketId, BucketKey, BucketState, BucketStrategy,
    CockroachPool, CockroachPoolConfig, CockroachSchemaError, CockroachTlsConfig, Cursor,
    DurableBucketRead, DurableBucketStore, DurableBucketStoreError, Record, StoreError, TableId,
    TimeRange,
};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_postgres::{Client, Row, error::SqlState};
use tracing::Instrument;
use uuid::Uuid;

const MAX_TRANSACTION_ATTEMPTS: usize = 5;
const ROUTE_CACHE_MAX_ENTRIES: usize = 4_096;

/// Coupled CockroachDB implementation of [`DurableBucketStore`].
///
/// State-changing operations hold one exclusive pooled connection and use a
/// serializable transaction for both bucket metadata and mutable records. An
/// uncached routed read selects metadata and bounded hot rows in one CockroachDB
/// statement, so both come from the same MVCC snapshot. The store owns its
/// physical tables so all runtime operations use the same transactional fence.
pub struct CockroachDurableBucketStore {
    pool: CockroachPool,
    route_cache: RouteCache,
}

/// A bounded cache of immutable published archive routes.
///
/// Hot and archiving observations are deliberately never cached: only a route
/// selected from the same statement snapshot as its hot rows may choose the hot
/// tier. Published archive manifest keys cannot revert or change, so they retain
/// the pool-free historical-read fast path.
#[derive(Clone, Default)]
struct RouteCache {
    entries: Arc<Mutex<HashMap<BucketId, CachedArchiveEntry>>>,
}

struct CachedArchiveEntry {
    object_key: String,
    observed_at: Instant,
}

impl RouteCache {
    fn archived(&self, bucket: &BucketId) -> Option<String> {
        lock_unpoisoned(&self.entries)
            .get(bucket)
            .map(|entry| entry.object_key.clone())
    }

    fn cache_archive(&self, bucket: BucketId, object_key: String) {
        self.cache_archive_at(bucket, object_key, Instant::now());
    }

    fn cache_archive_at(&self, bucket: BucketId, object_key: String, observed_at: Instant) {
        let mut entries = lock_unpoisoned(&self.entries);
        if !entries.contains_key(&bucket)
            && entries.len() >= ROUTE_CACHE_MAX_ENTRIES
            && let Some(oldest_bucket) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.observed_at)
                .map(|(bucket, _)| bucket.clone())
        {
            entries.remove(&oldest_bucket);
        }
        entries.insert(
            bucket,
            CachedArchiveEntry {
                object_key,
                observed_at,
            },
        );
    }

    fn invalidate(&self, bucket: &BucketId) {
        lock_unpoisoned(&self.entries).remove(bucket);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl CockroachDurableBucketStore {
    /// Opens a durable bucket store with the default bounded CockroachDB pool.
    #[tracing::instrument(
        name = "CONNECT defaultdb",
        skip(database_url),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.operation.name = "CONNECT",
        )
    )]
    #[deprecated(note = "use connect_rustls or connect_insecure_dev explicitly")]
    #[allow(deprecated)]
    pub async fn connect(database_url: &str) -> Result<Self, DurableBucketStoreError> {
        Self::connect_with_pool_config(database_url, CockroachPoolConfig::default()).await
    }

    /// Legacy insecure constructor retained for compatibility.
    #[deprecated(
        note = "use connect_rustls_with_pool_config or connect_insecure_dev_with_pool_config explicitly"
    )]
    #[allow(deprecated)]
    pub async fn connect_with_pool_config(
        database_url: &str,
        config: CockroachPoolConfig,
    ) -> Result<Self, DurableBucketStoreError> {
        Ok(Self::from_pool(
            CockroachPool::connect(database_url, config)
                .await
                .map_err(pool_error)?,
        ))
    }

    pub async fn connect_insecure_dev(database_url: &str) -> Result<Self, DurableBucketStoreError> {
        Self::connect_insecure_dev_with_pool_config(database_url, CockroachPoolConfig::default())
            .await
    }

    pub async fn connect_insecure_dev_with_pool_config(
        database_url: &str,
        config: CockroachPoolConfig,
    ) -> Result<Self, DurableBucketStoreError> {
        Ok(Self::from_pool(
            CockroachPool::connect_insecure_dev(database_url, config)
                .await
                .map_err(pool_error)?,
        ))
    }

    pub async fn connect_rustls(
        database_url: &str,
        tls: CockroachTlsConfig,
    ) -> Result<Self, DurableBucketStoreError> {
        Self::connect_rustls_with_pool_config(database_url, CockroachPoolConfig::default(), tls)
            .await
    }

    pub async fn connect_rustls_with_pool_config(
        database_url: &str,
        config: CockroachPoolConfig,
        tls: CockroachTlsConfig,
    ) -> Result<Self, DurableBucketStoreError> {
        Ok(Self::from_pool(
            CockroachPool::connect_rustls(database_url, config, tls)
                .await
                .map_err(pool_error)?,
        ))
    }

    /// Builds a durable bucket store using a shared CockroachDB pool.
    ///
    /// The pool must provide exclusive checkouts. State-changing operations keep
    /// their checkout for the complete serializable transaction.
    pub fn from_pool(pool: CockroachPool) -> Self {
        Self {
            pool,
            route_cache: RouteCache::default(),
        }
    }

    /// Returns the bounded pool used by this store.
    pub fn pool(&self) -> CockroachPool {
        self.pool.clone()
    }

    /// Applies the append-only, numbered CockroachDB schema migrations.
    ///
    /// This method is for a short-lived privileged deployment job. Runtime
    /// services should call [`Self::validate_schema`] with their DML-only role.
    #[tracing::instrument(name = "pulpitum.schema.migrate", skip(self), err)]
    pub async fn migrate(&self) -> Result<(), CockroachSchemaError> {
        let history_connection = self.pool.acquire().await.map_err(schema_pool_error)?;
        history_connection
            .client()
            .batch_execute(CREATE_MIGRATION_HISTORY_SQL)
            .await
            .map_err(schema_database_error)?;
        drop(history_connection);

        for attempt in 0..MAX_TRANSACTION_ATTEMPTS {
            let mut connection = self.pool.acquire().await.map_err(schema_pool_error)?;
            connection.mark_uncertain();
            let client = connection.client_arc();
            if let Err(error) = client
                .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                .await
            {
                if is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS {
                    continue;
                }
                return Err(schema_database_error(error));
            }

            let applied = match read_applied_migrations(client.as_ref()).await {
                Ok(applied) => applied,
                Err(error) => {
                    if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                        connection.mark_reusable();
                    }
                    return Err(error);
                }
            };
            let pending = match plan_migrations(MIGRATIONS, &applied) {
                Ok(pending) => pending,
                Err(error) => {
                    if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                        connection.mark_reusable();
                    }
                    return Err(error);
                }
            };

            let mut retry = false;
            for migration in pending {
                let result = async {
                    client.batch_execute(migration.sql).await?;
                    client
                        .execute(
                            "INSERT INTO pulpitum_schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
                            &[&migration.version, &migration.name, &migration.checksum()],
                        )
                        .await?;
                    Ok::<(), tokio_postgres::Error>(())
                }
                .await;
                if let Err(error) = result {
                    retry = is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS;
                    if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                        connection.mark_reusable();
                    }
                    if retry {
                        break;
                    }
                    return Err(schema_database_error(error));
                }
            }
            if retry {
                tokio::time::sleep(transaction_retry_delay(attempt)).await;
                continue;
            }

            match client.batch_execute("COMMIT").await {
                Ok(()) => {
                    connection.mark_reusable();
                    return Ok(());
                }
                Err(error) if is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS => {
                    let _ = rollback(client.as_ref(), self.pool.rollback_timeout()).await;
                    tokio::time::sleep(transaction_retry_delay(attempt)).await;
                }
                Err(error) => return Err(schema_database_error(error)),
            }
        }

        Err(CockroachSchemaError::Database(
            "migration transaction exhausted its retry budget".into(),
        ))
    }

    /// Read-only verification for runtime startup.
    ///
    /// It rejects missing, changed, future, or gapped migration history and
    /// checks every required v4 table column and primary-key order. It never
    /// issues DDL or writes migration history.
    #[tracing::instrument(name = "pulpitum.schema.validate", skip(self), err)]
    pub async fn validate_schema(&self) -> Result<(), CockroachSchemaError> {
        let connection = self.pool.acquire().await.map_err(schema_pool_error)?;
        let applied = read_applied_migrations(connection.client()).await?;
        let pending = plan_migrations(MIGRATIONS, &applied)?;
        if !pending.is_empty() {
            return Err(CockroachSchemaError::PendingMigrations {
                versions: pending.iter().map(|migration| migration.version).collect(),
            });
        }
        for table in EXPECTED_V4_TABLES {
            validate_table(connection.client(), table).await?;
        }
        Ok(())
    }

    async fn transaction<T, F>(&self, operation: F) -> Result<T, DurableBucketStoreError>
    where
        T: Send,
        F: for<'client> Fn(
            &'client Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, TransactionFailure>> + Send + 'client>,
        >,
    {
        for attempt in 0..MAX_TRANSACTION_ATTEMPTS {
            let acquire_span = tracing::info_span!("pulpitum.db.pool.acquire");
            let mut connection = self
                .pool
                .acquire()
                .instrument(acquire_span)
                .await
                .map_err(pool_error)?;
            // From this point until a complete COMMIT or ROLLBACK response, a
            // dropped future must evict the checkout rather than pool it.
            connection.mark_uncertain();
            let client = connection.client_arc();
            let begin_span = tracing::info_span!(
                "BEGIN",
                otel.kind = "client",
                db.system.name = "cockroachdb",
                db.namespace = "defaultdb",
                db.operation.name = "BEGIN",
                db.query.summary = "BEGIN",
                db.query.text = "BEGIN ISOLATION LEVEL SERIALIZABLE",
            );
            let begin = tokio::time::timeout(
                self.pool.transaction_timeout(),
                client
                    .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                    .instrument(begin_span),
            )
            .await;
            match begin {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let retry = is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS;
                    drop(connection);
                    if retry {
                        tokio::time::sleep(transaction_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(database_error(error));
                }
                Err(_) => return Err(DurableBucketStoreError::OperationFailed),
            }

            let operation_result =
                tokio::time::timeout(self.pool.transaction_timeout(), operation(client.as_ref()))
                    .await
                    .map_err(|_| DurableBucketStoreError::OperationFailed)?;
            match operation_result {
                Ok(value) => {
                    let commit_span = tracing::info_span!(
                        "COMMIT",
                        otel.kind = "client",
                        db.system.name = "cockroachdb",
                        db.namespace = "defaultdb",
                        db.operation.name = "COMMIT",
                        db.query.summary = "COMMIT",
                        db.query.text = "COMMIT",
                    );
                    let commit = tokio::time::timeout(
                        self.pool.commit_timeout(),
                        client.batch_execute("COMMIT").instrument(commit_span),
                    )
                    .await;
                    match commit {
                        Ok(Ok(())) => {
                            connection.mark_reusable();
                            return Ok(value);
                        }
                        Ok(Err(error)) => {
                            let definite_database_error = error.as_db_error().is_some();
                            let retry =
                                is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS;
                            if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                                connection.mark_reusable();
                            }
                            drop(connection);
                            if retry {
                                tokio::time::sleep(transaction_retry_delay(attempt)).await;
                                continue;
                            }
                            if definite_database_error {
                                return Err(database_error(error));
                            }
                            return Err(DurableBucketStoreError::CommitOutcomeUnknown);
                        }
                        Err(_) => return Err(DurableBucketStoreError::CommitOutcomeUnknown),
                    }
                }
                Err(TransactionFailure::Database(error))
                    if is_retryable(&error) && attempt + 1 < MAX_TRANSACTION_ATTEMPTS =>
                {
                    if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                        connection.mark_reusable();
                        drop(connection);
                        tokio::time::sleep(transaction_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(DurableBucketStoreError::OperationFailed);
                }
                Err(error) => {
                    if rollback(client.as_ref(), self.pool.rollback_timeout()).await {
                        connection.mark_reusable();
                    }
                    return Err(error.into_store_error());
                }
            }
        }

        Err(DurableBucketStoreError::OperationFailed)
    }

    #[tracing::instrument(
        name = "SELECT pulpitum_v4_bucket_metadata",
        skip(client, bucket),
        err(Debug),
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "SELECT",
            db.query.summary = "SELECT pulpitum_v4_bucket_metadata",
            db.query.text = "SELECT state, archive_owner_token, archive_owner_expires_at, archive_object_key, hot_deleted FROM pulpitum_v4_bucket_metadata WHERE table_id = $1 AND partition_key = $2 AND bucket_key = $3",
        )
    )]
    async fn state_from_metadata(
        client: &Client,
        bucket: &BucketId,
    ) -> Result<Option<BucketState>, TransactionFailure> {
        client
            .query_opt(
                "SELECT state, archive_owner_token, archive_owner_expires_at,
                        archive_object_key, hot_deleted
                 FROM pulpitum_v4_bucket_metadata
                 WHERE table_id = $1 AND partition_key = $2 AND bucket_key = $3",
                &[
                    &bucket.table_id.as_str(),
                    &bucket.partition_key.as_bytes(),
                    &bucket.key.as_str(),
                ],
            )
            .await
            .map_err(TransactionFailure::Database)?
            .map(metadata_state)
            .transpose()
            .map_err(TransactionFailure::Store)
    }
}

impl crate::storage::durable_bucket_store_sealed::Sealed for CockroachDurableBucketStore {}

#[async_trait]
impl DurableBucketStore for CockroachDurableBucketStore {
    #[tracing::instrument(
        name = "APPEND durable bucket",
        skip(self, bucket, record),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.operation.name = "APPEND",
            db.query.summary = "APPEND durable bucket",
        )
    )]
    async fn append(
        &self,
        bucket: &BucketId,
        record: Record,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = bucket.clone();
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let record = record.clone();
            Box::pin(async move {
                let strategy_conflict = client
                    .query_opt(
                        "SELECT 1
                         FROM pulpitum_v4_bucket_metadata
                         WHERE table_id = $1 AND bucket_strategy != $2
                         LIMIT 1",
                        &[&bucket.table_id.as_str(), &bucket.strategy.as_str()],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if strategy_conflict.is_some() {
                    return Err(TransactionFailure::Store(
                        DurableBucketStoreError::BucketStrategyMismatch,
                    ));
                }

                client
                    .execute(
                        "INSERT INTO pulpitum_v4_bucket_metadata
                            (table_id, partition_key, bucket_key, bucket_strategy, bucket_start, bucket_end)
                         VALUES ($1, $2, $3, $4, $5, $6)
                         ON CONFLICT (table_id, partition_key, bucket_key) DO NOTHING",
                        &[
                            &bucket.table_id.as_str(),
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &bucket.strategy.as_str(),
                            &bucket.start,
                            &bucket.end,
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;

                let state = Self::state_from_metadata(client, &bucket).await?.ok_or(
                    TransactionFailure::Store(DurableBucketStoreError::OperationFailed),
                )?;
                if state != BucketState::Hot {
                    return Err(TransactionFailure::Store(
                        DurableBucketStoreError::BucketReadOnly(state),
                    ));
                }

                client
                    .execute(
                        "INSERT INTO pulpitum_v4_records
                            (table_id, partition_key, bucket_key, event_time, sort_key, value)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &bucket.table_id.as_str(),
                            &record.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &record.event_time,
                            &record.sort_key.as_bytes(),
                            &record.value,
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                Ok(())
            })
        })
        .await
    }

    #[tracing::instrument(
        name = "SELECT pulpitum_v4_bucket_metadata",
        skip(self, bucket),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "SELECT",
            db.query.summary = "SELECT pulpitum_v4_bucket_metadata",
        )
    )]
    async fn state(&self, bucket: &BucketId) -> Result<BucketState, DurableBucketStoreError> {
        let connection = self.pool.acquire().await.map_err(pool_error)?;
        let row = connection
            .client()
            .query_opt(
                "SELECT state, archive_owner_token, archive_owner_expires_at,
                        archive_object_key, hot_deleted
                 FROM pulpitum_v4_bucket_metadata
                 WHERE table_id = $1 AND partition_key = $2 AND bucket_key = $3",
                &[
                    &bucket.table_id.as_str(),
                    &bucket.partition_key.as_bytes(),
                    &bucket.key.as_str(),
                ],
            )
            .await
            .map_err(database_error)?;
        row.map(metadata_state)
            .transpose()?
            .map_or_else(|| Ok(BucketState::Hot), Ok)
    }

    #[tracing::instrument(
        name = "SELECT pulpitum_v4_bucket_metadata",
        skip(self, scan),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "SELECT",
            db.query.summary = "SELECT pulpitum_v4_bucket_metadata",
        )
    )]
    async fn discover_archive_work(
        &self,
        scan: ArchiveScan,
    ) -> Result<Vec<ArchiveWork>, DurableBucketStoreError> {
        let connection = self.pool.acquire().await.map_err(pool_error)?;
        let client = connection.client();
        let limit = i64::from(scan.limit);
        let cleanup_rows = client
            .query(
                "SELECT table_id, partition_key, bucket_strategy, bucket_key, bucket_start, bucket_end
                 FROM pulpitum_v4_bucket_metadata
                 WHERE state = 'archived'
                   AND hot_deleted = false
                   AND (archive_next_attempt_at IS NULL OR archive_next_attempt_at <= now())
                   AND (archive_owner_expires_at IS NULL OR archive_owner_expires_at <= now())
                 ORDER BY bucket_end, table_id, partition_key, bucket_key
                 LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(database_error)?;
        let mut work = cleanup_rows
            .into_iter()
            .map(|row| bucket_from_row(row).map(ArchiveWork::Cleanup))
            .collect::<Result<Vec<_>, DurableBucketStoreError>>()?;
        let remaining = limit.saturating_sub(work.len() as i64);
        if remaining == 0 {
            return Ok(work);
        }
        let cutover_rows = client
            .query(
                "SELECT table_id, partition_key, bucket_strategy, bucket_key, bucket_start, bucket_end
                 FROM pulpitum_v4_bucket_metadata
                 WHERE bucket_end <= $1
                   AND (archive_next_attempt_at IS NULL OR archive_next_attempt_at <= now())
                   AND (state = 'hot'
                        OR (state = 'archiving'
                            AND archive_owner_expires_at <= now()))
                 ORDER BY bucket_end, table_id, partition_key, bucket_key
                 LIMIT $2",
                &[&scan.eligible_before, &remaining],
            )
            .await
            .map_err(database_error)?;
        work.extend(
            cutover_rows
                .into_iter()
                .map(|row| bucket_from_row(row).map(ArchiveWork::Cutover))
                .collect::<Result<Vec<_>, DurableBucketStoreError>>()?,
        );
        Ok(work)
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, bucket, lease_for),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn claim_archive(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        let bucket = bucket.clone();
        let owner_token = Uuid::new_v4().to_string();
        let expires_at = archive_lease_expiry(lease_for)?;
        let session = self
            .transaction(move |client| {
                let bucket = bucket.clone();
                let owner_token = owner_token.clone();
                Box::pin(async move {
                    let row = client
                        .query_opt(
                            "UPDATE pulpitum_v4_bucket_metadata
                         SET state = 'archiving',
                             generation = generation + 1,
                             archive_owner_token = $3,
                             archive_owner_expires_at = $4,
                             archive_next_attempt_at = NULL,
                             archive_object_key = NULL,
                             hot_deleted = false
                         WHERE partition_key = $1 AND bucket_key = $2
                           AND table_id = $5
                           AND (archive_next_attempt_at IS NULL OR archive_next_attempt_at <= now())
                           AND (state = 'hot'
                                OR (state = 'archiving'
                                    AND archive_owner_expires_at <= now()))
                         RETURNING generation",
                            &[
                                &bucket.partition_key.as_bytes(),
                                &bucket.key.as_str(),
                                &owner_token,
                                &expires_at,
                                &bucket.table_id.as_str(),
                            ],
                        )
                        .await
                        .map_err(TransactionFailure::Database)?;
                    row.map(|row| {
                        let generation: i64 = row.try_get("generation").map_err(|_| {
                            TransactionFailure::Store(DurableBucketStoreError::OperationFailed)
                        })?;
                        Ok(ArchiveSession::new(bucket, owner_token, generation as u64))
                    })
                    .transpose()
                })
            })
            .await?;
        if let Some(session) = &session {
            self.route_cache.invalidate(session.bucket());
        }
        Ok(session)
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, bucket, lease_for),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn claim_cleanup(
        &self,
        bucket: &BucketId,
        lease_for: Duration,
    ) -> Result<Option<ArchiveSession>, DurableBucketStoreError> {
        let bucket = bucket.clone();
        let owner_token = Uuid::new_v4().to_string();
        let expires_at = archive_lease_expiry(lease_for)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let row = client
                    .query_opt(
                        "UPDATE pulpitum_v4_bucket_metadata
                         SET generation = generation + 1,
                             archive_owner_token = $3,
                             archive_owner_expires_at = $4,
                             archive_next_attempt_at = NULL
                         WHERE partition_key = $1 AND bucket_key = $2
                           AND table_id = $5
                           AND state = 'archived' AND hot_deleted = false
                           AND (archive_next_attempt_at IS NULL OR archive_next_attempt_at <= now())
                           AND (archive_owner_expires_at IS NULL OR archive_owner_expires_at <= now())
                         RETURNING generation",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &owner_token,
                            &expires_at,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                row.map(|row| {
                    let generation: i64 = row.try_get("generation").map_err(|_| {
                        TransactionFailure::Store(DurableBucketStoreError::OperationFailed)
                    })?;
                    Ok(ArchiveSession::new(bucket, owner_token, generation as u64))
                })
                .transpose()
            })
        })
        .await
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, session, lease_for),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn renew_archive_lease(
        &self,
        session: &ArchiveSession,
        lease_for: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        let expires_at = archive_lease_expiry(lease_for)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let updated = client
                    .execute(
                        "UPDATE pulpitum_v4_bucket_metadata
                 SET archive_owner_expires_at = $5
                 WHERE partition_key = $1 AND bucket_key = $2 AND generation = $3
                   AND archive_owner_token = $4 AND archive_owner_expires_at > now()
                   AND table_id = $6
                   AND (state = 'archiving' OR (state = 'archived' AND hot_deleted = false))",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &expires_at,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if updated == 1 {
                    Ok(())
                } else {
                    Err(TransactionFailure::Store(
                        DurableBucketStoreError::StaleArchiveSession,
                    ))
                }
            })
        })
        .await
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, session, retry_after),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn defer_archive(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        let retry_at = archive_lease_expiry(retry_after)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let updated = client.execute(
                    "UPDATE pulpitum_v4_bucket_metadata
                     SET state = 'hot', archive_owner_token = NULL, archive_owner_expires_at = NULL,
                         archive_object_key = NULL, hot_deleted = false,
                         archive_attempts = archive_attempts + 1, archive_next_attempt_at = $5
                     WHERE partition_key = $1 AND bucket_key = $2 AND state = 'archiving'
                       AND generation = $3 AND archive_owner_token = $4 AND archive_owner_expires_at > now()
                       AND table_id = $6",
                    &[
                        &bucket.partition_key.as_bytes(),
                        &bucket.key.as_str(),
                        &generation,
                        &owner_token,
                        &retry_at,
                        &bucket.table_id.as_str(),
                    ],
                ).await.map_err(TransactionFailure::Database)?;
                if updated == 1 { Ok(()) } else { Err(TransactionFailure::Store(DurableBucketStoreError::StaleArchiveSession)) }
            })
        }).await?;
        self.route_cache.invalidate(session.bucket());
        Ok(())
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, session, retry_after),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn defer_cleanup(
        &self,
        session: &ArchiveSession,
        retry_after: Duration,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        let retry_at = archive_lease_expiry(retry_after)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let updated = client.execute(
                    "UPDATE pulpitum_v4_bucket_metadata
                     SET archive_owner_expires_at = $5,
                         archive_attempts = archive_attempts + 1, archive_next_attempt_at = $5
                     WHERE partition_key = $1 AND bucket_key = $2 AND state = 'archived' AND hot_deleted = false
                       AND generation = $3 AND archive_owner_token = $4 AND archive_owner_expires_at > now()
                       AND table_id = $6",
                    &[
                        &bucket.partition_key.as_bytes(),
                        &bucket.key.as_str(),
                        &generation,
                        &owner_token,
                        &retry_at,
                        &bucket.table_id.as_str(),
                    ],
                ).await.map_err(TransactionFailure::Database)?;
                if updated == 1 { Ok(()) } else { Err(TransactionFailure::Store(DurableBucketStoreError::StaleArchiveSession)) }
            })
        }).await
    }

    #[tracing::instrument(
        name = "READ durable bucket range",
        skip(self, bucket, range, after),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.operation.name = "READ",
            db.query.summary = "READ durable bucket range",
            pulpitum.query.limit = limit,
            pulpitum.route.cache = tracing::field::Empty,
            pulpitum.read.tier = tracing::field::Empty,
            server.address = tracing::field::Empty,
            server.port = tracing::field::Empty,
        )
    )]
    async fn read_range(
        &self,
        bucket: &BucketId,
        range: &TimeRange,
        after: Option<&Cursor>,
        limit: usize,
    ) -> Result<DurableBucketRead, DurableBucketStoreError> {
        let span = tracing::Span::current();
        let bucket = bucket.clone();
        let range = range.clone();
        let after = after.cloned();
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        if let Some(object_key) = self.route_cache.archived(&bucket) {
            span.record("pulpitum.route.cache", "archive");
            span.record("pulpitum.read.tier", "archive");
            return Ok(DurableBucketRead::Archive(object_key));
        }
        span.record("pulpitum.route.cache", "miss");

        self.pool.record_database_endpoint(&span);
        let acquire_span = tracing::info_span!("pulpitum.db.pool.acquire");
        let connection = self
            .pool
            .acquire()
            .instrument(acquire_span)
            .await
            .map_err(pool_error)?;
        let client = connection.client();
        let query_span = tracing::info_span!(
            "SELECT routed durable bucket range",
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata,pulpitum_v4_records",
            db.operation.name = "SELECT",
            db.query.summary = "SELECT routed durable bucket range",
            db.query.text = "WITH route AS (SELECT state, archive_object_key FROM pulpitum_v4_bucket_metadata WHERE table_id = $8 AND partition_key = $1 AND bucket_key = $2), effective_route AS (SELECT state, archive_object_key FROM route UNION ALL SELECT 'hot', NULL::STRING WHERE NOT EXISTS (SELECT 1 FROM route)) SELECT route.state AS route_state, route.archive_object_key AS route_object_key, hot.partition_key AS record_partition_key, hot.event_time AS record_event_time, hot.sort_key AS record_sort_key, hot.value AS record_value FROM effective_route AS route LEFT JOIN LATERAL (SELECT partition_key, event_time, sort_key, value FROM pulpitum_v4_records WHERE route.state != 'archived' AND table_id = $8 AND partition_key = $1 AND bucket_key = $2 AND event_time >= $3 AND event_time < $4 AND (event_time, sort_key) > ($5, $6) ORDER BY event_time, sort_key LIMIT $7) AS hot ON true ORDER BY hot.event_time, hot.sort_key",
            db.response.returned_rows = tracing::field::Empty,
            server.address = tracing::field::Empty,
            server.port = tracing::field::Empty,
        );
        self.pool.record_database_endpoint(&query_span);
        let rows = match after {
            Some(cursor) => {
                client
                    .query(
                        "WITH route AS (
                             SELECT state, archive_object_key
                             FROM pulpitum_v4_bucket_metadata
                             WHERE table_id = $8 AND partition_key = $1 AND bucket_key = $2
                         ), effective_route AS (
                             SELECT state, archive_object_key FROM route
                             UNION ALL
                             SELECT 'hot', NULL::STRING
                             WHERE NOT EXISTS (SELECT 1 FROM route)
                         )
                         SELECT route.state AS route_state,
                                route.archive_object_key AS route_object_key,
                                hot.partition_key AS record_partition_key,
                                hot.event_time AS record_event_time,
                                hot.sort_key AS record_sort_key,
                                hot.value AS record_value
                         FROM effective_route AS route
                         LEFT JOIN LATERAL (
                             SELECT partition_key, event_time, sort_key, value
                             FROM pulpitum_v4_records
                             WHERE route.state != 'archived'
                               AND table_id = $8
                               AND partition_key = $1
                               AND bucket_key = $2
                               AND event_time >= $3
                               AND event_time < $4
                               AND (event_time, sort_key) > ($5, $6)
                             ORDER BY event_time, sort_key
                             LIMIT $7
                         ) AS hot ON true
                         ORDER BY hot.event_time, hot.sort_key",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &range.start,
                            &range.end,
                            &cursor.event_time,
                            &cursor.sort_key.as_bytes(),
                            &limit,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .instrument(query_span.clone())
                    .await
            }
            None => {
                client
                    .query(
                        "WITH route AS (
                             SELECT state, archive_object_key
                             FROM pulpitum_v4_bucket_metadata
                             WHERE table_id = $6 AND partition_key = $1 AND bucket_key = $2
                         ), effective_route AS (
                             SELECT state, archive_object_key FROM route
                             UNION ALL
                             SELECT 'hot', NULL::STRING
                             WHERE NOT EXISTS (SELECT 1 FROM route)
                         )
                         SELECT route.state AS route_state,
                                route.archive_object_key AS route_object_key,
                                hot.partition_key AS record_partition_key,
                                hot.event_time AS record_event_time,
                                hot.sort_key AS record_sort_key,
                                hot.value AS record_value
                         FROM effective_route AS route
                         LEFT JOIN LATERAL (
                             SELECT partition_key, event_time, sort_key, value
                             FROM pulpitum_v4_records
                             WHERE route.state != 'archived'
                               AND table_id = $6
                               AND partition_key = $1
                               AND bucket_key = $2
                               AND event_time >= $3
                               AND event_time < $4
                             ORDER BY event_time, sort_key
                             LIMIT $5
                         ) AS hot ON true
                         ORDER BY hot.event_time, hot.sort_key",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &range.start,
                            &range.end,
                            &limit,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .instrument(query_span.clone())
                    .await
            }
        }
        .map_err(database_error)?;
        query_span.record("db.response.returned_rows", rows.len());

        let first = rows
            .first()
            .ok_or(DurableBucketStoreError::OperationFailed)?;
        let state = first
            .try_get::<_, String>("route_state")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        let object_key = first
            .try_get::<_, Option<String>>("route_object_key")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let partition_key = row
                .try_get::<_, Option<Vec<u8>>>("record_partition_key")
                .map_err(|_| DurableBucketStoreError::OperationFailed)?;
            let Some(partition_key) = partition_key else {
                continue;
            };
            records.push(Record {
                partition_key: partition_key.into(),
                event_time: row
                    .try_get::<_, Option<_>>("record_event_time")
                    .map_err(|_| DurableBucketStoreError::OperationFailed)?
                    .ok_or(DurableBucketStoreError::OperationFailed)?,
                sort_key: row
                    .try_get::<_, Option<Vec<u8>>>("record_sort_key")
                    .map_err(|_| DurableBucketStoreError::OperationFailed)?
                    .ok_or(DurableBucketStoreError::OperationFailed)?
                    .into(),
                value: row
                    .try_get::<_, Option<Vec<u8>>>("record_value")
                    .map_err(|_| DurableBucketStoreError::OperationFailed)?
                    .ok_or(DurableBucketStoreError::OperationFailed)?,
            });
        }

        let read = routed_read_from_snapshot(&state, object_key, records)?;
        match &read {
            DurableBucketRead::Archive(object_key) => {
                span.record("pulpitum.read.tier", "archive");
                self.route_cache.cache_archive(bucket, object_key.clone());
            }
            DurableBucketRead::Hot(_) => {
                span.record("pulpitum.read.tier", "hot");
            }
        }
        Ok(read)
    }

    #[tracing::instrument(
        name = "BEGIN durable archive",
        skip(self, bucket),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "BEGIN ARCHIVE",
            db.query.summary = "BEGIN durable archive",
        )
    )]
    async fn begin_archive(
        &self,
        bucket: &BucketId,
    ) -> Result<ArchiveSession, DurableBucketStoreError> {
        let bucket = bucket.clone();
        let owner_token = Uuid::new_v4().to_string();
        let session = self
            .transaction(move |client| {
                let bucket = bucket.clone();
                let owner_token = owner_token.clone();
                Box::pin(async move {
                    client
                        .execute(
                            "INSERT INTO pulpitum_v4_bucket_metadata
                                (table_id, partition_key, bucket_key, bucket_strategy, bucket_start, bucket_end)
                             VALUES ($1, $2, $3, $4, $5, $6)
                             ON CONFLICT (table_id, partition_key, bucket_key) DO NOTHING",
                            &[
                                &bucket.table_id.as_str(),
                                &bucket.partition_key.as_bytes(),
                                &bucket.key.as_str(),
                                &bucket.strategy.as_str(),
                                &bucket.start,
                                &bucket.end,
                            ],
                        )
                        .await
                        .map_err(TransactionFailure::Database)?;

                    let row = client
                        .query_opt(
                            "UPDATE pulpitum_v4_bucket_metadata
                         SET state = 'archiving',
                             generation = generation + 1,
                             archive_owner_token = $3,
                             archive_owner_expires_at = now() + INTERVAL '5 minutes',
                             archive_object_key = NULL,
                             hot_deleted = false
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND table_id = $4
                           AND (state = 'hot'
                                OR (state = 'archiving'
                                    AND archive_owner_expires_at <= now()))
                         RETURNING generation",
                            &[
                                &bucket.partition_key.as_bytes(),
                                &bucket.key.as_str(),
                                &owner_token,
                                &bucket.table_id.as_str(),
                            ],
                        )
                        .await
                        .map_err(TransactionFailure::Database)?;

                    if let Some(row) = row {
                        let generation: i64 = row.try_get("generation").map_err(|_| {
                            TransactionFailure::Store(DurableBucketStoreError::OperationFailed)
                        })?;
                        let generation = u64::try_from(generation).map_err(|_| {
                            TransactionFailure::Store(DurableBucketStoreError::OperationFailed)
                        })?;
                        return Ok(ArchiveSession::new(bucket, owner_token, generation));
                    }

                    let state = Self::state_from_metadata(client, &bucket).await?.ok_or(
                        TransactionFailure::Store(DurableBucketStoreError::OperationFailed),
                    )?;
                    Err(TransactionFailure::Store(
                        DurableBucketStoreError::ArchiveNotAllowed(state),
                    ))
                })
            })
            .await?;
        self.route_cache.invalidate(session.bucket());
        Ok(session)
    }

    #[tracing::instrument(
        name = "SNAPSHOT durable bucket",
        skip(self, session),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.operation.name = "SNAPSHOT",
            db.query.summary = "SNAPSHOT durable bucket",
        )
    )]
    async fn snapshot(
        &self,
        session: &ArchiveSession,
    ) -> Result<Vec<Record>, DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let current_owner = client
                    .query_opt(
                        "SELECT 1
                         FROM pulpitum_v4_bucket_metadata
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND state = 'archiving'
                           AND generation = $3
                           AND archive_owner_token = $4
                           AND archive_owner_expires_at > now()
                           AND table_id = $5",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if current_owner.is_none() {
                    return Err(TransactionFailure::Store(
                        DurableBucketStoreError::StaleArchiveSession,
                    ));
                }

                let rows = client
                    .query(
                        "SELECT partition_key, event_time, sort_key, value
                         FROM pulpitum_v4_records
                         WHERE partition_key = $1 AND bucket_key = $2 AND table_id = $3
                         ORDER BY event_time, sort_key",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                Ok(rows
                    .into_iter()
                    .map(|row| Record {
                        partition_key: row.get::<_, Vec<u8>>("partition_key").into(),
                        event_time: row.get("event_time"),
                        sort_key: row.get::<_, Vec<u8>>("sort_key").into(),
                        value: row.get("value"),
                    })
                    .collect())
            })
        })
        .await
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, session, object_key),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn publish_archive(
        &self,
        session: &ArchiveSession,
        object_key: String,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            let object_key = object_key.clone();
            Box::pin(async move {
                let updated = client
                    .execute(
                        "UPDATE pulpitum_v4_bucket_metadata
                         SET state = 'archived',
                             archive_object_key = $5,
                             hot_deleted = false
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND state = 'archiving'
                           AND generation = $3
                           AND archive_owner_token = $4
                           AND archive_owner_expires_at > now()
                           AND table_id = $6",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &object_key,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if updated == 1 {
                    Ok(())
                } else {
                    Err(TransactionFailure::Store(
                        DurableBucketStoreError::StaleArchiveSession,
                    ))
                }
            })
        })
        .await?;
        self.route_cache.invalidate(session.bucket());
        Ok(())
    }

    #[tracing::instrument(
        name = "DELETE pulpitum_v4_records",
        skip(self, session),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_records",
            db.operation.name = "DELETE",
            db.query.summary = "DELETE pulpitum_v4_records",
        )
    )]
    async fn delete_hot_bucket(
        &self,
        session: &ArchiveSession,
    ) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                client
                    .execute(
                        "DELETE FROM pulpitum_v4_records
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND table_id = $5
                           AND EXISTS (
                               SELECT 1
                               FROM pulpitum_v4_bucket_metadata
                               WHERE partition_key = $1
                                 AND bucket_key = $2
                                 AND table_id = $5
                                 AND state = 'archived'
                                 AND generation = $3
                                 AND archive_owner_token = $4
                                 AND archive_owner_expires_at > now()
                                 AND hot_deleted = false
                           )",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;

                let updated = client
                    .execute(
                        "UPDATE pulpitum_v4_bucket_metadata
                         SET hot_deleted = true
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND state = 'archived'
                           AND generation = $3
                           AND archive_owner_token = $4
                           AND archive_owner_expires_at > now()
                           AND hot_deleted = false
                           AND table_id = $5",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if updated == 1 {
                    Ok(())
                } else {
                    Err(TransactionFailure::Store(
                        DurableBucketStoreError::StaleArchiveSession,
                    ))
                }
            })
        })
        .await
    }

    #[tracing::instrument(
        name = "UPDATE pulpitum_v4_bucket_metadata",
        skip(self, session),
        err,
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.collection.name = "pulpitum_v4_bucket_metadata",
            db.operation.name = "UPDATE",
            db.query.summary = "UPDATE pulpitum_v4_bucket_metadata",
        )
    )]
    async fn abort_archive(&self, session: &ArchiveSession) -> Result<(), DurableBucketStoreError> {
        let bucket = session.bucket().clone();
        let owner_token = session.owner_token().to_owned();
        let generation = i64::try_from(session.generation())
            .map_err(|_| DurableBucketStoreError::OperationFailed)?;
        self.transaction(move |client| {
            let bucket = bucket.clone();
            let owner_token = owner_token.clone();
            Box::pin(async move {
                let updated = client
                    .execute(
                        "UPDATE pulpitum_v4_bucket_metadata
                         SET state = 'hot',
                             archive_owner_token = NULL,
                             archive_owner_expires_at = NULL,
                             archive_object_key = NULL,
                             hot_deleted = false
                         WHERE partition_key = $1
                           AND bucket_key = $2
                           AND state = 'archiving'
                           AND generation = $3
                           AND archive_owner_token = $4
                           AND archive_owner_expires_at > now()
                           AND table_id = $5",
                        &[
                            &bucket.partition_key.as_bytes(),
                            &bucket.key.as_str(),
                            &generation,
                            &owner_token,
                            &bucket.table_id.as_str(),
                        ],
                    )
                    .await
                    .map_err(TransactionFailure::Database)?;
                if updated == 1 {
                    Ok(())
                } else {
                    Err(TransactionFailure::Store(
                        DurableBucketStoreError::StaleArchiveSession,
                    ))
                }
            })
        })
        .await?;
        self.route_cache.invalidate(session.bucket());
        Ok(())
    }
}

const CREATE_MIGRATION_HISTORY_SQL: &str = "CREATE TABLE IF NOT EXISTS pulpitum_schema_migrations (
    version INT8 NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    checksum STRING NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pulpitum_schema_migrations_version_check CHECK (version >= 0),
    CONSTRAINT pulpitum_schema_migrations_name_unique UNIQUE (name)
);";

async fn read_applied_migrations(
    client: &Client,
) -> Result<Vec<AppliedMigration>, CockroachSchemaError> {
    let rows = client
        .query(
            "SELECT version, name, checksum
             FROM pulpitum_schema_migrations
             ORDER BY version",
            &[],
        )
        .await
        .map_err(schema_database_error)?;
    Ok(rows
        .into_iter()
        .map(|row| AppliedMigration {
            version: row.get("version"),
            name: row.get("name"),
            checksum: row.get("checksum"),
        })
        .collect())
}

async fn validate_table(
    client: &Client,
    expected: &ExpectedTable,
) -> Result<(), CockroachSchemaError> {
    let columns = client
        .query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = $1",
            &[&expected.name],
        )
        .await
        .map_err(schema_database_error)?
        .into_iter()
        .map(|row| row.get::<_, String>("column_name"))
        .collect::<HashSet<_>>();
    for column in expected.columns {
        if !columns.contains(*column) {
            return Err(CockroachSchemaError::MissingColumn {
                table: expected.name,
                column,
            });
        }
    }

    let primary_key = client
        .query(
            "SELECT key_column_usage.column_name
             FROM information_schema.table_constraints
             JOIN information_schema.key_column_usage
               USING (constraint_catalog, constraint_schema, constraint_name,
                      table_catalog, table_schema, table_name)
             WHERE table_constraints.table_schema = current_schema()
               AND table_constraints.table_name = $1
               AND table_constraints.constraint_type = 'PRIMARY KEY'
             ORDER BY key_column_usage.ordinal_position",
            &[&expected.name],
        )
        .await
        .map_err(schema_database_error)?
        .into_iter()
        .map(|row| row.get::<_, String>("column_name"))
        .collect::<Vec<_>>();
    if primary_key != expected.primary_key {
        return Err(CockroachSchemaError::PrimaryKeyMismatch {
            table: expected.name,
            expected: expected.primary_key,
            actual: primary_key,
        });
    }
    Ok(())
}

fn schema_pool_error(error: StoreError) -> CockroachSchemaError {
    CockroachSchemaError::Database(error.to_string())
}

fn schema_database_error(error: tokio_postgres::Error) -> CockroachSchemaError {
    CockroachSchemaError::Database(error.to_string())
}

#[derive(Debug)]
enum TransactionFailure {
    Database(tokio_postgres::Error),
    Store(DurableBucketStoreError),
}

impl TransactionFailure {
    fn into_store_error(self) -> DurableBucketStoreError {
        match self {
            Self::Database(error) => database_error(error),
            Self::Store(error) => error,
        }
    }
}

async fn rollback(client: &Client, timeout: Duration) -> bool {
    matches!(
        tokio::time::timeout(timeout, client.batch_execute("ROLLBACK")).await,
        Ok(Ok(()))
    )
}

fn is_retryable(error: &tokio_postgres::Error) -> bool {
    error.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE)
}

fn transaction_retry_delay(attempt: usize) -> Duration {
    let multiplier = 1_u64 << attempt.min(5);
    let cap_millis = 25_u64.saturating_mul(multiplier).min(1_000);
    let half = cap_millis / 2;
    let random = Uuid::new_v4();
    let bytes = random.as_bytes();
    let sample = u16::from_le_bytes([bytes[0], bytes[1]]) as u64;
    Duration::from_millis(half + sample % (half + 1))
}

fn pool_error(_: StoreError) -> DurableBucketStoreError {
    DurableBucketStoreError::Unavailable
}

fn database_error(error: tokio_postgres::Error) -> DurableBucketStoreError {
    tracing::debug!(?error, "CockroachDB durable operation failed");
    DurableBucketStoreError::OperationFailed
}

fn archive_lease_expiry(
    lease_for: Duration,
) -> Result<chrono::DateTime<Utc>, DurableBucketStoreError> {
    if lease_for.is_zero() {
        return Err(DurableBucketStoreError::OperationFailed);
    }
    let duration = ChronoDuration::from_std(lease_for)
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    Utc::now()
        .checked_add_signed(duration)
        .ok_or(DurableBucketStoreError::OperationFailed)
}

fn bucket_from_row(row: Row) -> Result<BucketId, DurableBucketStoreError> {
    let table_id = TableId::new(
        row.try_get::<_, String>("table_id")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?,
    )
    .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let strategy = row
        .try_get::<_, String>("bucket_strategy")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?
        .parse::<BucketStrategy>()
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let key = BucketKey::new(
        row.try_get::<_, String>("bucket_key")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?,
    )
    .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    Ok(BucketId {
        table_id,
        partition_key: row
            .try_get::<_, Vec<u8>>("partition_key")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?
            .into(),
        strategy,
        key,
        start: row
            .try_get("bucket_start")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?,
        end: row
            .try_get("bucket_end")
            .map_err(|_| DurableBucketStoreError::OperationFailed)?,
    })
}

pub(crate) fn routed_read_from_snapshot(
    state: &str,
    object_key: Option<String>,
    records: Vec<Record>,
) -> Result<DurableBucketRead, DurableBucketStoreError> {
    match state {
        "hot" | "archiving" if object_key.is_none() => Ok(DurableBucketRead::Hot(records)),
        "archived" if records.is_empty() => Ok(DurableBucketRead::Archive(
            object_key.ok_or(DurableBucketStoreError::OperationFailed)?,
        )),
        _ => Err(DurableBucketStoreError::OperationFailed),
    }
}

fn metadata_state(row: Row) -> Result<BucketState, DurableBucketStoreError> {
    let state: String = row
        .try_get("state")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let owner_token: Option<String> = row
        .try_get("archive_owner_token")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let owner_expires_at: Option<chrono::DateTime<chrono::Utc>> = row
        .try_get("archive_owner_expires_at")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let object_key: Option<String> = row
        .try_get("archive_object_key")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;
    let hot_deleted: bool = row
        .try_get("hot_deleted")
        .map_err(|_| DurableBucketStoreError::OperationFailed)?;

    match state.as_str() {
        "hot"
            if owner_token.is_none()
                && owner_expires_at.is_none()
                && object_key.is_none()
                && !hot_deleted =>
        {
            Ok(BucketState::Hot)
        }
        "archiving"
            if owner_token.is_some()
                && owner_expires_at.is_some()
                && object_key.is_none()
                && !hot_deleted =>
        {
            Ok(BucketState::Archiving)
        }
        "archived" if owner_token.is_some() => Ok(BucketState::Archived {
            object_key: object_key.ok_or(DurableBucketStoreError::OperationFailed)?,
            hot_deleted,
        }),
        _ => Err(DurableBucketStoreError::OperationFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket() -> BucketId {
        BucketId::for_event_time_with_strategy(
            TableId::new("test-table").expect("test table ID is valid"),
            b"general".to_vec(),
            BucketStrategy::CalendarYearUtc,
            chrono::DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
                .expect("test timestamp is valid")
                .with_timezone(&Utc),
        )
    }

    #[test]
    fn transaction_retry_delay_is_bounded_exponential_jitter() {
        for attempt in 0..8 {
            let multiplier = 1_u64 << attempt.min(5);
            let cap = 25_u64.saturating_mul(multiplier).min(1_000);
            for _ in 0..16 {
                let delay = transaction_retry_delay(attempt).as_millis() as u64;
                assert!(delay >= cap / 2);
                assert!(delay <= cap);
            }
        }
    }

    #[test]
    fn immutable_archive_routes_are_cached_until_invalidated() {
        let cache = RouteCache::default();
        let bucket = bucket();
        cache.cache_archive(bucket.clone(), "archives/general/2026/manifest.json".into());

        assert_eq!(
            cache.archived(&bucket).as_deref(),
            Some("archives/general/2026/manifest.json")
        );

        cache.invalidate(&bucket);
        assert_eq!(cache.archived(&bucket), None);
    }

    #[test]
    fn archive_route_cache_has_a_fixed_entry_limit() {
        let cache = RouteCache::default();
        let observed_at = Instant::now();
        let mut oldest = None;
        let mut newest = None;
        for index in 0..=ROUTE_CACHE_MAX_ENTRIES {
            let mut bucket = bucket();
            bucket.partition_key = format!("partition-key-{index}").into_bytes().into();
            if index == 0 {
                oldest = Some(bucket.clone());
            }
            if index == ROUTE_CACHE_MAX_ENTRIES {
                newest = Some(bucket.clone());
            }
            cache.cache_archive_at(
                bucket,
                format!("archives/{index}/manifest.json"),
                observed_at + Duration::from_millis(index as u64),
            );
        }

        assert_eq!(
            lock_unpoisoned(&cache.entries).len(),
            ROUTE_CACHE_MAX_ENTRIES
        );
        assert_eq!(cache.archived(&oldest.unwrap()), None);
        assert!(cache.archived(&newest.unwrap()).is_some());
    }
}
