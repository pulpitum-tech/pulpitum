use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BucketId, BucketStrategy, PartitionKey, Record, TimeRange};

/// Stable physical namespace for one logical table.
///
/// This is deliberately separate from a table's display name so renaming a
/// table does not change its storage identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TableId(String);

impl TableId {
    pub fn new(value: impl Into<String>) -> Result<Self, DefinitionError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DefinitionError::EmptyTableId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TableId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// The direction is part of the physical layout contract, not merely a query
/// preference. The current storage adapters implement ascending keys only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// One field in the ordered clustering key of a physical partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusteringColumn {
    pub field: &'static str,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDefinition {
    pub name: String,
    /// Immutable physical namespace used in database keys and archive paths.
    pub table_id: TableId,
    /// Logical partition key, e.g. `channel_id`.
    pub partition_key: &'static str,
    /// Event-time source field used to derive the physical bucket.
    pub bucket_time_key: &'static str,
    /// Calendar strategy used to derive physical bucket descriptors.
    pub bucket_strategy: BucketStrategy,
    /// Ordered fields inside each `(partition_key, bucket)` partition.
    pub clustering_key: Vec<ClusteringColumn>,
    /// The ingestion policy may accept only this many recent strategy buckets.
    pub writable_buckets: u8,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DefinitionError {
    #[error("table name cannot be empty")]
    EmptyName,
    #[error("table ID cannot be empty")]
    EmptyTableId,
    #[error("this record model supports bucketing on `event_time`, not `{0}`")]
    UnsupportedBucketTimeKey(&'static str),
    #[error("this record model requires clustering key `(event_time ASC, sort_key ASC)`")]
    UnsupportedClusteringKey,
    #[error("writable_buckets must be at least one")]
    InvalidWriteWindow,
}

impl TableDefinition {
    /// Creates a table with yearly UTC buckets.
    pub fn new(
        name: impl Into<String>,
        table_id: TableId,
        partition_key: &'static str,
        bucket_time_key: &'static str,
        clustering_key: Vec<ClusteringColumn>,
        writable_buckets: u8,
    ) -> Result<Self, DefinitionError> {
        Self::new_with_bucket_strategy(
            name,
            table_id,
            partition_key,
            bucket_time_key,
            BucketStrategy::default(),
            clustering_key,
            writable_buckets,
        )
    }

    pub fn new_with_bucket_strategy(
        name: impl Into<String>,
        table_id: TableId,
        partition_key: &'static str,
        bucket_time_key: &'static str,
        bucket_strategy: BucketStrategy,
        clustering_key: Vec<ClusteringColumn>,
        writable_buckets: u8,
    ) -> Result<Self, DefinitionError> {
        let definition = Self {
            name: name.into(),
            table_id,
            partition_key,
            bucket_time_key,
            bucket_strategy,
            clustering_key,
            writable_buckets,
        };
        definition.validate()?;
        Ok(definition)
    }

    /// A suitable definition for `(channel_id, event_time, sort_key)` chat messages.
    pub fn chat_messages(name: impl Into<String>, table_id: TableId) -> Self {
        Self::new(
            name,
            table_id,
            "channel_id",
            "event_time",
            vec![
                ClusteringColumn {
                    field: "event_time",
                    direction: SortDirection::Ascending,
                },
                ClusteringColumn {
                    field: "sort_key",
                    direction: SortDirection::Ascending,
                },
            ],
            2,
        )
        .expect("built-in chat definition is valid")
    }

    pub fn validate(&self) -> Result<(), DefinitionError> {
        if self.name.trim().is_empty() {
            return Err(DefinitionError::EmptyName);
        }
        if self.table_id.as_str().trim().is_empty() {
            return Err(DefinitionError::EmptyTableId);
        }
        if self.bucket_time_key != "event_time" {
            return Err(DefinitionError::UnsupportedBucketTimeKey(
                self.bucket_time_key,
            ));
        }
        if self.writable_buckets == 0 {
            return Err(DefinitionError::InvalidWriteWindow);
        }
        let required = [
            ClusteringColumn {
                field: "event_time",
                direction: SortDirection::Ascending,
            },
            ClusteringColumn {
                field: "sort_key",
                direction: SortDirection::Ascending,
            },
        ];
        if self.clustering_key != required {
            return Err(DefinitionError::UnsupportedClusteringKey);
        }
        Ok(())
    }

    pub fn bucket_for(&self, record: &Record) -> BucketId {
        self.bucket_for_event_time(record.partition_key.clone(), record.event_time)
    }

    pub fn bucket_for_event_time(
        &self,
        partition_key: impl Into<PartitionKey>,
        event_time: DateTime<Utc>,
    ) -> BucketId {
        BucketId::for_event_time_with_strategy(
            self.table_id.clone(),
            partition_key,
            self.bucket_strategy,
            event_time,
        )
    }

    /// Returns every strategy bucket intersecting the finite, half-open range.
    pub fn buckets_for_range(
        &self,
        partition_key: impl Into<PartitionKey>,
        range: &TimeRange,
    ) -> Vec<BucketId> {
        if range.start >= range.end {
            return Vec::new();
        }

        let partition_key = partition_key.into();
        let mut bucket = self.bucket_for_event_time(partition_key.clone(), range.start);
        let mut buckets = Vec::new();
        while bucket.start < range.end {
            let next = bucket.end;
            buckets.push(bucket);
            bucket = self.bucket_for_event_time(partition_key.clone(), next);
        }
        buckets
    }

    /// Returns `count` strategy buckets ending with the bucket containing `event_time`,
    /// ordered from oldest to newest.
    pub fn recent_buckets(
        &self,
        partition_key: impl Into<PartitionKey>,
        event_time: DateTime<Utc>,
        count: usize,
    ) -> Vec<BucketId> {
        if count == 0 {
            return Vec::new();
        }

        let partition_key = partition_key.into();
        let mut bucket = self.bucket_for_event_time(partition_key.clone(), event_time);
        let mut buckets = Vec::with_capacity(count);
        buckets.push(bucket.clone());
        for _ in 1..count {
            let previous_event_time = bucket
                .start
                .checked_sub_signed(Duration::nanoseconds(1))
                .expect("previous UTC calendar bucket is representable");
            bucket = self.bucket_for_event_time(partition_key.clone(), previous_event_time);
            buckets.push(bucket.clone());
        }
        buckets.reverse();
        buckets
    }

    pub fn writable_buckets_at(
        &self,
        partition_key: impl Into<PartitionKey>,
        event_time: DateTime<Utc>,
    ) -> Vec<BucketId> {
        self.recent_buckets(
            partition_key,
            event_time,
            usize::from(self.writable_buckets),
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn definition(strategy: BucketStrategy) -> TableDefinition {
        TableDefinition::new_with_bucket_strategy(
            "events",
            TableId::new("events").unwrap(),
            "tenant_id",
            "event_time",
            strategy,
            vec![
                ClusteringColumn {
                    field: "event_time",
                    direction: SortDirection::Ascending,
                },
                ClusteringColumn {
                    field: "sort_key",
                    direction: SortDirection::Ascending,
                },
            ],
            3,
        )
        .unwrap()
    }

    fn timestamp(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn chat_messages_declares_the_cassandra_key_model() {
        let definition =
            TableDefinition::chat_messages("messages", TableId::new("messages").unwrap());

        assert_eq!(definition.partition_key, "channel_id");
        assert_eq!(definition.bucket_time_key, "event_time");
        assert_eq!(
            definition.clustering_key,
            [
                ClusteringColumn {
                    field: "event_time",
                    direction: SortDirection::Ascending,
                },
                ClusteringColumn {
                    field: "sort_key",
                    direction: SortDirection::Ascending,
                },
            ]
        );
    }

    #[test]
    fn rejects_any_other_bucket_or_clustering_definition() {
        let mut definition = definition(BucketStrategy::CalendarYearUtc);
        definition.bucket_time_key = "created_at";
        assert_eq!(
            definition.validate(),
            Err(DefinitionError::UnsupportedBucketTimeKey("created_at"))
        );

        definition.bucket_time_key = "event_time";
        definition.clustering_key.reverse();
        assert_eq!(
            definition.validate(),
            Err(DefinitionError::UnsupportedClusteringKey)
        );
    }

    #[test]
    fn bucket_for_uses_record_partition_and_event_time() {
        let definition = definition(BucketStrategy::CalendarMonthUtc);
        let record = Record {
            partition_key: vec![0, 255].into(),
            event_time: timestamp(2024, 2, 29),
            sort_key: "message".into(),
            value: Vec::new(),
        };

        let bucket = definition.bucket_for(&record);
        assert_eq!(bucket.partition_key, record.partition_key);
        assert_eq!(bucket.key.as_str(), "month:2024-02");
    }

    #[test]
    fn range_routing_uses_the_configured_strategy_and_exclusive_end() {
        let buckets = definition(BucketStrategy::CalendarMonthUtc).buckets_for_range(
            "tenant",
            &TimeRange {
                start: timestamp(2024, 1, 31),
                end: timestamp(2024, 3, 1),
            },
        );

        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.key.as_str())
                .collect::<Vec<_>>(),
            ["month:2024-01", "month:2024-02"]
        );
        assert!(
            buckets
                .iter()
                .all(|bucket| bucket.partition_key == PartitionKey::from("tenant"))
        );
    }

    #[test]
    fn writable_window_counts_recent_strategy_buckets() {
        let buckets = definition(BucketStrategy::CalendarDayUtc)
            .writable_buckets_at("tenant", timestamp(2024, 3, 1));

        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.key.as_str())
                .collect::<Vec<_>>(),
            ["day:2024-02-28", "day:2024-02-29", "day:2024-03-01"]
        );
    }
}
