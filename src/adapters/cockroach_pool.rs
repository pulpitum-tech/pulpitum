use crate::{CockroachTlsConfig, StoreError};
use std::{
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{
    Client, NoTls,
    config::{Config, Host, SslMode},
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::Instrument;

const CHECKED_OUT: u8 = 0;
const IDLE: u8 = 1;
const CLOSED: u8 = 2;

/// Settings for the CockroachDB connection pool.
///
/// A connection is checked out for one Pulpitum database operation and returned
/// immediately afterward. Keep `max_connections` below the database's per-node
/// connection budget after accounting for every application replica.
#[derive(Clone, Copy, Debug)]
pub struct CockroachPoolConfig {
    pub max_connections: usize,
    pub acquire_timeout: Duration,
    pub connect_timeout: Duration,
    pub transaction_timeout: Duration,
    pub commit_timeout: Duration,
    pub rollback_timeout: Duration,
}

impl Default for CockroachPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            acquire_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
            transaction_timeout: Duration::from_secs(30),
            commit_timeout: Duration::from_secs(10),
            rollback_timeout: Duration::from_secs(5),
        }
    }
}

/// Bounded pool of PostgreSQL protocol connections to CockroachDB.
///
/// `tokio-postgres` can pipeline work on one client, but a bounded pool provides
/// backpressure and isolates a slow request to the checked-out connection. The
/// pool opens connections lazily (after validating the first connection) and
/// exposes pool pressure through OpenTelemetry when that feature is enabled.
#[derive(Clone)]
pub struct CockroachPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    database_url: String,
    database_target: Option<DatabaseTarget>,
    transport: CockroachTransport,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    transaction_timeout: Duration,
    commit_timeout: Duration,
    rollback_timeout: Duration,
    available: Mutex<Vec<Arc<PooledClient>>>,
    permits: Arc<Semaphore>,
    state: PoolState,
    metrics: PoolMetrics,
}

struct PoolState {
    idle: AtomicU64,
    in_use: AtomicU64,
    open: AtomicU64,
    waiters: AtomicU64,
    max: u64,
}

impl PoolState {
    fn new(max_connections: usize) -> Self {
        Self {
            idle: AtomicU64::new(0),
            in_use: AtomicU64::new(0),
            open: AtomicU64::new(0),
            waiters: AtomicU64::new(0),
            max: max_connections as u64,
        }
    }
}

struct PooledClient {
    client: Arc<Client>,
    state: AtomicU8,
}

#[derive(Clone)]
enum CockroachTransport {
    InsecureDev,
    Rustls(MakeRustlsConnect),
}

#[derive(Clone, Debug)]
struct DatabaseTarget {
    address: String,
    port: u16,
}

fn require_ssl_mode(
    database_url: &str,
    required: SslMode,
    transport_name: &str,
) -> Result<(), StoreError> {
    let config: Config = database_url
        .parse()
        .map_err(|_| StoreError::Other("CockroachDB connection URL is invalid".into()))?;
    if config.get_ssl_mode() != required {
        let expected = match required {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            _ => "the required value",
        };
        return Err(StoreError::Other(format!(
            "CockroachDB {transport_name} connections require sslmode={expected}"
        )));
    }
    Ok(())
}

impl DatabaseTarget {
    fn from_database_url(database_url: &str) -> Option<Self> {
        let config: Config = database_url.parse().ok()?;
        let address = config
            .get_hostaddrs()
            .first()
            .map(ToString::to_string)
            .or_else(|| {
                config.get_hosts().first().map(|host| match host {
                    Host::Tcp(host) => host.clone(),
                    #[cfg(unix)]
                    Host::Unix(path) => path.display().to_string(),
                })
            })?;
        let port = config.get_ports().first().copied().unwrap_or(5432);
        Some(Self { address, port })
    }
}

/// A database connection checked out from [`CockroachPool`].
///
/// Dropping this value returns a healthy connection to the pool. It should be
/// kept only for the duration of one logical database operation.
pub struct PooledConnection {
    inner: Arc<PoolInner>,
    pooled: Arc<PooledClient>,
    reusable: bool,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    pub fn client(&self) -> &Client {
        &self.pooled.client
    }

    pub(crate) fn client_arc(&self) -> Arc<Client> {
        Arc::clone(&self.pooled.client)
    }

    /// Prevents a checkout with an in-flight or ambiguously completed operation
    /// from returning to the idle pool if its future is cancelled.
    pub(crate) fn mark_uncertain(&mut self) {
        self.reusable = false;
    }

    /// Marks the checkout reusable only after a complete COMMIT or ROLLBACK
    /// response proves that no transaction remains active.
    pub(crate) fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        self.inner.state.in_use.fetch_sub(1, Ordering::AcqRel);

        if self.reusable {
            if self
                .pooled
                .state
                .compare_exchange(CHECKED_OUT, IDLE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inner.state.idle.fetch_add(1, Ordering::AcqRel);
                lock_unpoisoned(&self.inner.available).push(self.pooled.clone());
            }
        } else if self
            .pooled
            .state
            .compare_exchange(CHECKED_OUT, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner.state.open.fetch_sub(1, Ordering::AcqRel);
            self.inner.metrics.connection_closed();
        }
        record_pool_state(&self.inner);
    }
}

impl CockroachPool {
    /// Creates a fixed-size pool and establishes every configured connection
    /// during startup. This avoids connection-establishment latency under load
    /// and fails fast when the database cannot satisfy the configured capacity.
    #[deprecated(note = "use connect_rustls or connect_insecure_dev explicitly")]
    pub async fn connect(
        database_url: impl Into<String>,
        config: CockroachPoolConfig,
    ) -> Result<Self, StoreError> {
        Self::connect_with_transport(database_url.into(), config, CockroachTransport::InsecureDev)
            .await
    }

    pub async fn connect_insecure_dev(
        database_url: impl Into<String>,
        config: CockroachPoolConfig,
    ) -> Result<Self, StoreError> {
        let database_url = database_url.into();
        require_ssl_mode(&database_url, SslMode::Disable, "insecure development")?;
        Self::connect_with_transport(database_url, config, CockroachTransport::InsecureDev).await
    }

    pub async fn connect_rustls(
        database_url: impl Into<String>,
        config: CockroachPoolConfig,
        tls: CockroachTlsConfig,
    ) -> Result<Self, StoreError> {
        let database_url = database_url.into();
        require_ssl_mode(&database_url, SslMode::Require, "verified TLS")?;
        let connector = tls
            .connector()
            .map_err(|error| StoreError::Other(error.to_string()))?;
        Self::connect_with_transport(database_url, config, CockroachTransport::Rustls(connector))
            .await
    }

    async fn connect_with_transport(
        database_url: String,
        config: CockroachPoolConfig,
        transport: CockroachTransport,
    ) -> Result<Self, StoreError> {
        if config.max_connections == 0 {
            return Err(StoreError::Other(
                "CockroachDB pool max_connections must be greater than zero".into(),
            ));
        }
        if config.acquire_timeout.is_zero()
            || config.connect_timeout.is_zero()
            || config.transaction_timeout.is_zero()
            || config.commit_timeout.is_zero()
            || config.rollback_timeout.is_zero()
        {
            return Err(StoreError::Other(
                "CockroachDB pool timeouts must be greater than zero".into(),
            ));
        }

        let database_target = DatabaseTarget::from_database_url(&database_url);
        let pool = Self {
            inner: Arc::new(PoolInner {
                database_url,
                database_target,
                transport,
                acquire_timeout: config.acquire_timeout,
                connect_timeout: config.connect_timeout,
                transaction_timeout: config.transaction_timeout,
                commit_timeout: config.commit_timeout,
                rollback_timeout: config.rollback_timeout,
                available: Mutex::new(Vec::with_capacity(config.max_connections)),
                permits: Arc::new(Semaphore::new(config.max_connections)),
                state: PoolState::new(config.max_connections),
                metrics: PoolMetrics::new(),
            }),
        };
        record_pool_state(&pool.inner);

        pool.prewarm().await?;
        Ok(pool)
    }

    async fn prewarm(&self) -> Result<(), StoreError> {
        for _ in 0..self.inner.state.max {
            let pooled = self.open_connection().await?;
            pooled.state.store(IDLE, Ordering::Release);
            self.inner.state.idle.fetch_add(1, Ordering::AcqRel);
            lock_unpoisoned(&self.inner.available).push(pooled);
        }
        record_pool_state(&self.inner);
        Ok(())
    }

    /// Wraps a caller-owned, already-driven client in a single-connection pool.
    ///
    /// This is primarily a compatibility path for applications that manage the
    /// `tokio-postgres` connection future themselves. New applications should
    /// use [`Self::connect`] so Pulpitum owns the pool lifecycle.
    pub fn from_client(client: Arc<Client>) -> Self {
        let inner = Arc::new(PoolInner {
            database_url: String::new(),
            database_target: None,
            transport: CockroachTransport::InsecureDev,
            acquire_timeout: CockroachPoolConfig::default().acquire_timeout,
            connect_timeout: CockroachPoolConfig::default().connect_timeout,
            transaction_timeout: CockroachPoolConfig::default().transaction_timeout,
            commit_timeout: CockroachPoolConfig::default().commit_timeout,
            rollback_timeout: CockroachPoolConfig::default().rollback_timeout,
            available: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(1)),
            state: PoolState::new(1),
            metrics: PoolMetrics::new(),
        });
        let pooled = Arc::new(PooledClient {
            client,
            state: AtomicU8::new(IDLE),
        });
        lock_unpoisoned(&inner.available).push(pooled);
        inner.state.idle.store(1, Ordering::Release);
        inner.state.open.store(1, Ordering::Release);
        record_pool_state(&inner);
        Self { inner }
    }

    /// Waits for a connection until the configured acquisition timeout expires.
    pub async fn acquire(&self) -> Result<PooledConnection, StoreError> {
        let started = Instant::now();
        self.inner.state.waiters.fetch_add(1, Ordering::AcqRel);
        record_pool_state(&self.inner);

        let permit_span = tracing::info_span!("pulpitum.db.pool.wait");
        let permit = tokio::time::timeout(
            self.inner.acquire_timeout,
            self.inner.permits.clone().acquire_owned(),
        )
        .instrument(permit_span)
        .await;
        self.inner.state.waiters.fetch_sub(1, Ordering::AcqRel);

        let permit = match permit {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.inner.metrics.acquire("closed", started.elapsed());
                record_pool_state(&self.inner);
                return Err(StoreError::Other("CockroachDB pool is closed".into()));
            }
            Err(_) => {
                self.inner.metrics.acquire("timeout", started.elapsed());
                record_pool_state(&self.inner);
                return Err(StoreError::Other(
                    "CockroachDB pool acquisition timed out".into(),
                ));
            }
        };
        self.inner.state.in_use.fetch_add(1, Ordering::AcqRel);

        let pooled = match self.take_idle_connection() {
            Some(pooled) => pooled,
            None => match self.open_connection().await {
                Ok(pooled) => pooled,
                Err(error) => {
                    self.inner.state.in_use.fetch_sub(1, Ordering::AcqRel);
                    self.inner
                        .metrics
                        .acquire("connect_error", started.elapsed());
                    record_pool_state(&self.inner);
                    return Err(error);
                }
            },
        };

        self.inner.metrics.acquire("success", started.elapsed());
        record_pool_state(&self.inner);
        Ok(PooledConnection {
            inner: self.inner.clone(),
            pooled,
            reusable: true,
            _permit: permit,
        })
    }

    fn take_idle_connection(&self) -> Option<Arc<PooledClient>> {
        let checkout_span = tracing::info_span!("pulpitum.db.pool.checkout");
        let _checkout = checkout_span.enter();
        let mut available = lock_unpoisoned(&self.inner.available);
        while let Some(pooled) = available.pop() {
            match pooled.state.compare_exchange(
                IDLE,
                CHECKED_OUT,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.state.idle.fetch_sub(1, Ordering::AcqRel);
                    if pooled.client.is_closed() {
                        if pooled.state.swap(CLOSED, Ordering::AcqRel) != CLOSED {
                            self.inner.state.open.fetch_sub(1, Ordering::AcqRel);
                            self.inner.metrics.connection_closed();
                        }
                        continue;
                    }
                    return Some(pooled);
                }
                Err(CLOSED) => continue,
                Err(_) => continue,
            }
        }
        None
    }

    #[tracing::instrument(
        name = "CONNECT defaultdb",
        skip(self),
        fields(
            otel.kind = "client",
            db.system.name = "cockroachdb",
            db.namespace = "defaultdb",
            db.operation.name = "CONNECT",
            server.address = tracing::field::Empty,
            server.port = tracing::field::Empty,
        )
    )]
    async fn open_connection(&self) -> Result<Arc<PooledClient>, StoreError> {
        self.record_database_endpoint(&tracing::Span::current());
        let config: Config = self
            .inner
            .database_url
            .parse()
            .map_err(|_| StoreError::Other("CockroachDB connection URL is invalid".into()))?;
        match &self.inner.transport {
            CockroachTransport::InsecureDev => {
                let connection =
                    tokio::time::timeout(self.inner.connect_timeout, config.connect(NoTls))
                        .await
                        .map_err(|_| StoreError::Other("CockroachDB connection timed out".into()))?
                        .map_err(sql_error)?;
                Ok(self.register_connection(connection.0, connection.1))
            }
            CockroachTransport::Rustls(connector) => {
                let connection = tokio::time::timeout(
                    self.inner.connect_timeout,
                    config.connect(connector.clone()),
                )
                .await
                .map_err(|_| StoreError::Other("CockroachDB connection timed out".into()))?
                .map_err(sql_error)?;
                Ok(self.register_connection(connection.0, connection.1))
            }
        }
    }

    fn register_connection<F>(&self, client: Client, connection: F) -> Arc<PooledClient>
    where
        F: Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
    {
        let pooled = Arc::new(PooledClient {
            client: Arc::new(client),
            state: AtomicU8::new(CHECKED_OUT),
        });
        self.inner.state.open.fetch_add(1, Ordering::AcqRel);
        self.inner.metrics.connection_created("success");
        let pool = Arc::downgrade(&self.inner);
        let connection_client = Arc::downgrade(&pooled);
        tokio::spawn(async move {
            if connection.await.is_err() {
                tracing::warn!("CockroachDB pool connection terminated");
            }
            if let Some(connection_client) = connection_client.upgrade() {
                close_connection(pool, connection_client);
            }
        });
        pooled
    }

    pub(crate) fn transaction_timeout(&self) -> Duration {
        self.inner.transaction_timeout
    }

    pub(crate) fn commit_timeout(&self) -> Duration {
        self.inner.commit_timeout
    }

    pub(crate) fn rollback_timeout(&self) -> Duration {
        self.inner.rollback_timeout
    }

    pub(crate) fn record_database_endpoint(&self, span: &tracing::Span) {
        let Some(target) = &self.inner.database_target else {
            return;
        };
        span.record("server.address", target.address.as_str());
        span.record("server.port", i64::from(target.port));
    }
}

fn close_connection(pool: Weak<PoolInner>, pooled: Arc<PooledClient>) {
    let previous = pooled.state.swap(CLOSED, Ordering::AcqRel);
    if previous == CLOSED {
        return;
    }
    let Some(pool) = pool.upgrade() else {
        return;
    };
    pool.state.open.fetch_sub(1, Ordering::AcqRel);
    if previous == IDLE {
        pool.state.idle.fetch_sub(1, Ordering::AcqRel);
    }
    pool.metrics.connection_closed();
    record_pool_state(&pool);
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn record_pool_state(pool: &PoolInner) {
    pool.metrics.connections(
        pool.state.idle.load(Ordering::Acquire),
        pool.state.in_use.load(Ordering::Acquire),
        pool.state.open.load(Ordering::Acquire),
        pool.state.max,
        pool.state.waiters.load(Ordering::Acquire),
    );
}

fn sql_error(error: tokio_postgres::Error) -> StoreError {
    StoreError::Other(format!("CockroachDB: {error}"))
}

#[derive(Clone)]
struct PoolMetrics {
    #[cfg(feature = "opentelemetry")]
    connections: opentelemetry::metrics::Gauge<u64>,
    #[cfg(feature = "opentelemetry")]
    acquires: opentelemetry::metrics::Counter<u64>,
    #[cfg(feature = "opentelemetry")]
    acquire_duration: opentelemetry::metrics::Histogram<f64>,
    #[cfg(feature = "opentelemetry")]
    connections_created: opentelemetry::metrics::Counter<u64>,
    #[cfg(feature = "opentelemetry")]
    connections_closed: opentelemetry::metrics::Counter<u64>,
}

impl PoolMetrics {
    fn new() -> Self {
        #[cfg(feature = "opentelemetry")]
        {
            use opentelemetry::global;
            let meter = global::meter("pulpitum");
            Self {
                connections: meter.u64_gauge("pulpitum.db.pool.connections").build(),
                acquires: meter.u64_counter("pulpitum.db.pool.acquires").build(),
                acquire_duration: meter
                    .f64_histogram("pulpitum.db.pool.acquire.duration")
                    .with_unit("s")
                    .build(),
                connections_created: meter
                    .u64_counter("pulpitum.db.pool.connections.created")
                    .build(),
                connections_closed: meter
                    .u64_counter("pulpitum.db.pool.connections.closed")
                    .build(),
            }
        }
        #[cfg(not(feature = "opentelemetry"))]
        {
            Self {}
        }
    }

    fn connections(&self, idle: u64, in_use: u64, open: u64, max: u64, waiters: u64) {
        #[cfg(feature = "opentelemetry")]
        {
            use opentelemetry::KeyValue;
            for (state, count) in [
                ("idle", idle),
                ("in_use", in_use),
                ("open", open),
                ("max", max),
                ("waiters", waiters),
            ] {
                self.connections
                    .record(count, &[KeyValue::new("pulpitum.db.pool.state", state)]);
            }
        }
        #[cfg(not(feature = "opentelemetry"))]
        let _ = (idle, in_use, open, max, waiters);
    }

    fn acquire(&self, outcome: &'static str, elapsed: Duration) {
        #[cfg(feature = "opentelemetry")]
        {
            use opentelemetry::KeyValue;
            let attributes = [KeyValue::new("pulpitum.db.pool.outcome", outcome)];
            self.acquires.add(1, &attributes);
            self.acquire_duration
                .record(elapsed.as_secs_f64(), &attributes);
        }
        #[cfg(not(feature = "opentelemetry"))]
        let _ = (outcome, elapsed);
    }

    fn connection_created(&self, outcome: &'static str) {
        #[cfg(feature = "opentelemetry")]
        self.connections_created.add(
            1,
            &[opentelemetry::KeyValue::new(
                "pulpitum.db.pool.outcome",
                outcome,
            )],
        );
        #[cfg(not(feature = "opentelemetry"))]
        let _ = outcome;
    }

    fn connection_closed(&self) {
        #[cfg(feature = "opentelemetry")]
        self.connections_closed.add(1, &[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_constructors_require_an_explicit_matching_ssl_mode() {
        let insecure = "postgresql://localhost/defaultdb?sslmode=disable";
        let secure = "postgresql://localhost/defaultdb?sslmode=require";
        let downgradeable = "postgresql://localhost/defaultdb?sslmode=prefer";

        assert!(require_ssl_mode(insecure, SslMode::Disable, "test").is_ok());
        assert!(require_ssl_mode(secure, SslMode::Require, "test").is_ok());
        assert!(require_ssl_mode(secure, SslMode::Disable, "test").is_err());
        assert!(require_ssl_mode(insecure, SslMode::Require, "test").is_err());
        assert!(require_ssl_mode(downgradeable, SslMode::Require, "test").is_err());
    }

    #[tokio::test]
    async fn pool_config_rejects_zero_timeouts_before_connecting() {
        let config = CockroachPoolConfig {
            connect_timeout: Duration::ZERO,
            ..CockroachPoolConfig::default()
        };
        let error = CockroachPool::connect_insecure_dev(
            "postgresql://localhost/defaultdb?sslmode=disable",
            config,
        )
        .await
        .err()
        .expect("zero timeout must fail before connecting");
        assert!(error.to_string().contains("timeouts"));
    }
}
