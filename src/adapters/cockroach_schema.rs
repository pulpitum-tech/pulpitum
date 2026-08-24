//! Versioned CockroachDB schema definitions and database-free migration planning.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub const V4_METADATA_TABLE: &str = "pulpitum_v4_bucket_metadata";
pub const V4_RECORDS_TABLE: &str = "pulpitum_v4_records";

/// The first migration managed by this lifecycle.
///
/// It intentionally creates a new v4 namespace rather than attempting to infer
/// a v3 key mapping. See the migration command documentation for the required
/// explicit v3-to-v4 cutover process.
pub const V4_BASELINE_SQL: &str = "CREATE TABLE IF NOT EXISTS pulpitum_v4_bucket_metadata (
    table_id STRING NOT NULL,
    partition_key BYTES NOT NULL,
    bucket_key STRING NOT NULL,
    bucket_strategy STRING NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_end TIMESTAMPTZ NOT NULL,
    state STRING NOT NULL DEFAULT 'hot',
    generation INT8 NOT NULL DEFAULT 0,
    archive_owner_token STRING NULL,
    archive_owner_expires_at TIMESTAMPTZ NULL,
    archive_object_key STRING NULL,
    hot_deleted BOOL NOT NULL DEFAULT false,
    archive_attempts INT8 NOT NULL DEFAULT 0,
    archive_next_attempt_at TIMESTAMPTZ NULL,
    PRIMARY KEY (table_id, partition_key, bucket_key),
    CONSTRAINT pulpitum_v4_metadata_state_check
        CHECK (state IN ('hot', 'archiving', 'archived')),
    CONSTRAINT pulpitum_v4_metadata_generation_check
        CHECK (generation >= 0),
    CONSTRAINT pulpitum_v4_metadata_bounds_check
        CHECK (bucket_start < bucket_end),
    CONSTRAINT pulpitum_v4_metadata_invariants CHECK (
        (state = 'hot'
            AND archive_owner_token IS NULL
            AND archive_owner_expires_at IS NULL
            AND archive_object_key IS NULL
            AND hot_deleted = false)
        OR (state = 'archiving'
            AND archive_owner_token IS NOT NULL
            AND archive_owner_expires_at IS NOT NULL
            AND archive_object_key IS NULL
            AND hot_deleted = false)
        OR (state = 'archived'
            AND archive_owner_token IS NOT NULL
            AND archive_object_key IS NOT NULL)
    )
);
CREATE TABLE IF NOT EXISTS pulpitum_v4_records (
    table_id STRING NOT NULL,
    partition_key BYTES NOT NULL,
    bucket_key STRING NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    sort_key BYTES NOT NULL,
    value BYTES NOT NULL,
    PRIMARY KEY (table_id, partition_key, bucket_key, event_time, sort_key),
    CONSTRAINT pulpitum_v4_records_bucket_fk
        FOREIGN KEY (table_id, partition_key, bucket_key)
        REFERENCES pulpitum_v4_bucket_metadata (table_id, partition_key, bucket_key)
);
CREATE INDEX IF NOT EXISTS pulpitum_v4_archive_discovery_idx
    ON pulpitum_v4_bucket_metadata
    (state, hot_deleted, archive_next_attempt_at, archive_owner_expires_at,
     bucket_end, table_id, partition_key, bucket_key);";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SchemaMigration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

impl SchemaMigration {
    pub fn checksum(self) -> String {
        migration_checksum(self.sql)
    }
}

pub(crate) const MIGRATIONS: &[SchemaMigration] = &[SchemaMigration {
    version: 4,
    name: "v4_durable_bucket_store_baseline",
    sql: V4_BASELINE_SQL,
}];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AppliedMigration {
    pub version: i64,
    pub name: String,
    pub checksum: String,
}

#[derive(Debug, Error)]
pub enum CockroachSchemaError {
    #[error("CockroachDB schema operation failed: {0}")]
    Database(String),
    #[error(
        "migration {version} has drifted: expected name {expected_name:?} and checksum {expected_checksum}, found name {actual_name:?} and checksum {actual_checksum}"
    )]
    ChecksumDrift {
        version: i64,
        expected_name: &'static str,
        expected_checksum: String,
        actual_name: String,
        actual_checksum: String,
    },
    #[error(
        "database has unsupported historical migration version {version}; this binary starts its managed history at version {first_supported}"
    )]
    UnsupportedHistoricalVersion { version: i64, first_supported: i64 },
    #[error(
        "database migration version {version} is newer than this binary supports (latest is {latest_supported})"
    )]
    FutureVersion { version: i64, latest_supported: i64 },
    #[error(
        "database migration history has a gap: version {missing} is required before version {present}"
    )]
    MigrationGap { missing: i64, present: i64 },
    #[error("migration history contains duplicate version {version}")]
    DuplicateVersion { version: i64 },
    #[error("database is missing required migration versions {versions:?}")]
    PendingMigrations { versions: Vec<i64> },
    #[error("schema validation failed: table {table} is missing required column {column}")]
    MissingColumn {
        table: &'static str,
        column: &'static str,
    },
    #[error(
        "schema validation failed: table {table} has primary key {actual:?}, expected {expected:?}"
    )]
    PrimaryKeyMismatch {
        table: &'static str,
        expected: &'static [&'static str],
        actual: Vec<String>,
    },
}

pub(crate) fn migration_checksum(sql: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(sql.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Validates the append-only history and returns the missing migrations in
/// order. The first managed migration is version 4, so an empty history is a
/// valid pre-v4 database rather than a 1..3 gap.
pub(crate) fn plan_migrations(
    migrations: &[SchemaMigration],
    applied: &[AppliedMigration],
) -> Result<Vec<SchemaMigration>, CockroachSchemaError> {
    let first = migrations
        .first()
        .expect("the binary must ship at least one schema migration");
    let latest = migrations
        .last()
        .expect("the binary must ship at least one schema migration");
    let expected = migrations
        .iter()
        .map(|migration| (migration.version, migration))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeMap::new();

    for migration in applied {
        if seen.insert(migration.version, ()).is_some() {
            return Err(CockroachSchemaError::DuplicateVersion {
                version: migration.version,
            });
        }
        if migration.version < first.version {
            return Err(CockroachSchemaError::UnsupportedHistoricalVersion {
                version: migration.version,
                first_supported: first.version,
            });
        }
        if migration.version > latest.version {
            return Err(CockroachSchemaError::FutureVersion {
                version: migration.version,
                latest_supported: latest.version,
            });
        }
        let Some(expected_migration) = expected.get(&migration.version) else {
            let missing = migrations
                .iter()
                .find(|candidate| candidate.version > migration.version)
                .map(|candidate| candidate.version)
                .unwrap_or(migration.version);
            return Err(CockroachSchemaError::MigrationGap {
                missing,
                present: migration.version,
            });
        };
        let expected_checksum = expected_migration.checksum();
        if migration.name != expected_migration.name || migration.checksum != expected_checksum {
            return Err(CockroachSchemaError::ChecksumDrift {
                version: migration.version,
                expected_name: expected_migration.name,
                expected_checksum,
                actual_name: migration.name.clone(),
                actual_checksum: migration.checksum.clone(),
            });
        }
    }

    let applied_versions = seen.keys().copied().collect::<Vec<_>>();
    for (index, version) in applied_versions.iter().enumerate() {
        let expected_version = migrations[index].version;
        if *version != expected_version {
            return Err(CockroachSchemaError::MigrationGap {
                missing: expected_version,
                present: *version,
            });
        }
    }

    Ok(migrations
        .iter()
        .filter(|migration| !seen.contains_key(&migration.version))
        .copied()
        .collect())
}

pub(crate) struct ExpectedTable {
    pub name: &'static str,
    pub columns: &'static [&'static str],
    pub primary_key: &'static [&'static str],
}

pub(crate) const EXPECTED_V4_TABLES: &[ExpectedTable] = &[
    ExpectedTable {
        name: V4_METADATA_TABLE,
        columns: &[
            "table_id",
            "partition_key",
            "bucket_key",
            "bucket_strategy",
            "bucket_start",
            "bucket_end",
            "state",
            "generation",
            "archive_owner_token",
            "archive_owner_expires_at",
            "archive_object_key",
            "hot_deleted",
            "archive_attempts",
            "archive_next_attempt_at",
        ],
        primary_key: &["table_id", "partition_key", "bucket_key"],
    },
    ExpectedTable {
        name: V4_RECORDS_TABLE,
        columns: &[
            "table_id",
            "partition_key",
            "bucket_key",
            "event_time",
            "sort_key",
            "value",
        ],
        primary_key: &[
            "table_id",
            "partition_key",
            "bucket_key",
            "event_time",
            "sort_key",
        ],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    fn applied(migration: SchemaMigration) -> AppliedMigration {
        AppliedMigration {
            version: migration.version,
            name: migration.name.to_owned(),
            checksum: migration.checksum(),
        }
    }

    #[test]
    fn v4_baseline_checksum_is_sha256_and_plans_from_an_empty_database() {
        let checksum = MIGRATIONS[0].checksum();
        assert_eq!(checksum.len(), 64);
        assert!(
            checksum
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        );
        assert_eq!(plan_migrations(MIGRATIONS, &[]).unwrap(), MIGRATIONS);
    }

    #[test]
    fn planner_rejects_checksum_drift_and_future_versions() {
        let mut drifted = applied(MIGRATIONS[0]);
        drifted.checksum = "0".repeat(64);
        assert!(matches!(
            plan_migrations(MIGRATIONS, &[drifted]),
            Err(CockroachSchemaError::ChecksumDrift { .. })
        ));
        assert!(matches!(
            plan_migrations(
                MIGRATIONS,
                &[AppliedMigration {
                    version: 5,
                    name: "future".into(),
                    checksum: "0".repeat(64),
                }],
            ),
            Err(CockroachSchemaError::FutureVersion { .. })
        ));
    }

    #[test]
    fn planner_rejects_gaps_in_a_multi_migration_history() {
        const FIRST: SchemaMigration = SchemaMigration {
            version: 4,
            name: "first",
            sql: "first",
        };
        const SECOND: SchemaMigration = SchemaMigration {
            version: 5,
            name: "second",
            sql: "second",
        };
        assert!(matches!(
            plan_migrations(&[FIRST, SECOND], &[applied(SECOND)]),
            Err(CockroachSchemaError::MigrationGap {
                missing: 4,
                present: 5
            })
        ));
    }
}
