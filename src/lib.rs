//! `pulpitum` is a routing layer for append-oriented, bucketed KV tables.
//!
//! A logical key is `(partition_key, bucket, event_time, sort_key)`. The router keeps recent
//! buckets in a hot store and reads archived buckets from an object store.
//! Archival is a cutover protocol: block new writes, copy a stable snapshot,
//! publish the archive location, drain old hot reads, then delete hot data.

mod adapters;
mod application;
mod dev_support;
mod domain;
mod integrations;
mod ports;

// Internal compatibility aliases keep implementation imports stable while the
// public crate-root facade below preserves downstream API paths.
pub(crate) use adapters::{
    cockroach_durable, cockroach_pool, cockroach_schema, cockroach_tls, immutable_archive_cache,
    opendal_store,
};
pub(crate) use application::{durable_archive, durable_archive_recovery, durable_table};
pub(crate) use dev_support::load_profile;
pub(crate) use domain::{model, schema};
#[cfg(feature = "datafusion")]
pub(crate) use integrations::datafusion;
pub(crate) use integrations::observability;
pub(crate) use ports::storage;

#[cfg(test)]
mod tests;

pub use cockroach_durable::CockroachDurableBucketStore;
pub use cockroach_pool::{CockroachPool, CockroachPoolConfig, PooledConnection};
pub use cockroach_schema::CockroachSchemaError;
pub use cockroach_tls::{CockroachTlsConfig, CockroachTlsConfigError};
#[cfg(feature = "datafusion")]
pub use datafusion::{PulpitumTableProvider, PulpitumTableProviderError};
pub use durable_archive::{ArchiveOutcome, DurableArchiveCoordinator, DurableArchiveError};
pub use durable_archive_recovery::{
    ArchiveRecoveryConfig, ArchiveRecoveryError, ArchiveRecoveryOutcome,
    DurableArchiveRecoveryRunner,
};
pub use durable_table::{DurableTable, DurableTableError, Query, QueryPage};
pub use immutable_archive_cache::ImmutableArchiveCache;
pub use load_profile::SpikySqlLoadProfile;
pub use model::{
    BucketId, BucketKey, BucketKeyError, BucketStrategy, BucketStrategyParseError, Cursor,
    PartitionKey, Record, SortKey, TimeRange,
};
#[cfg(feature = "opentelemetry")]
pub use observability::otel::OtelTelemetry;
pub use observability::{
    ArchiveStage, CoordinatorPhase, NoopTelemetry, ReadTier, SharedTelemetry, Telemetry,
};
pub use opendal_store::{
    ArchiveFormat, ArchiveFormatParseError, OpenDalArchiveStore, S3ArchiveConfig,
    S3ServerSideEncryption,
};
pub use schema::{ClusteringColumn, DefinitionError, SortDirection, TableDefinition, TableId};
pub use storage::BucketState;
pub use storage::{
    ArchiveScan, ArchiveSession, ArchiveStore, ArchiveWork, DurableBucketRead, DurableBucketStore,
    DurableBucketStoreError, InMemoryArchiveStore, InMemoryDurableBucketStore, SharedArchiveStore,
    SharedDurableBucketStore, StoreError,
};
