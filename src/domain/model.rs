use std::{fmt, str::FromStr, str::Utf8Error};

use chrono::{DateTime, Datelike, Days, Months, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use super::schema::TableId;

macro_rules! opaque_bytes_key {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Vec<u8>);

        impl $name {
            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            pub fn as_utf8(&self) -> Result<&str, Utf8Error> {
                std::str::from_utf8(self.as_bytes())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value.into_bytes())
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.as_bytes().to_vec())
            }
        }

        impl From<Vec<u8>> for $name {
            fn from(value: Vec<u8>) -> Self {
                Self(value)
            }
        }

        impl From<&[u8]> for $name {
            fn from(value: &[u8]) -> Self {
                Self(value.to_vec())
            }
        }

        impl From<&String> for $name {
            fn from(value: &String) -> Self {
                Self(value.as_bytes().to_vec())
            }
        }
    };
}

opaque_bytes_key!(PartitionKey, "Opaque logical partition key bytes.");

opaque_bytes_key!(
    SortKey,
    "Opaque clustering key bytes used to disambiguate equal event times."
);

/// Stable, opaque identity for one time bucket.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BucketKey(String);

impl BucketKey {
    pub fn new(value: impl Into<String>) -> Result<Self, BucketKeyError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(BucketKeyError::Empty);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for BucketKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for BucketKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BucketKey {
    type Err = BucketKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for BucketKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BucketKeyError {
    #[error("bucket key cannot be empty")]
    Empty,
}

/// Calendar granularity used to derive UTC bucket boundaries.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BucketStrategy {
    #[default]
    CalendarYearUtc,
    CalendarMonthUtc,
    CalendarDayUtc,
}

impl BucketStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CalendarYearUtc => "calendar_year_utc",
            Self::CalendarMonthUtc => "calendar_month_utc",
            Self::CalendarDayUtc => "calendar_day_utc",
        }
    }

    pub fn bucket_key(self, event_time: DateTime<Utc>) -> BucketKey {
        let value = match self {
            Self::CalendarYearUtc => format!("year:{:04}", event_time.year()),
            Self::CalendarMonthUtc => {
                format!("month:{:04}-{:02}", event_time.year(), event_time.month())
            }
            Self::CalendarDayUtc => format!(
                "day:{:04}-{:02}-{:02}",
                event_time.year(),
                event_time.month(),
                event_time.day()
            ),
        };
        BucketKey::new(value).expect("calendar bucket keys are nonempty")
    }

    pub fn bucket_bounds(self, event_time: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        let start = match self {
            Self::CalendarYearUtc => utc_date(event_time.year(), 1, 1),
            Self::CalendarMonthUtc => utc_date(event_time.year(), event_time.month(), 1),
            Self::CalendarDayUtc => {
                utc_date(event_time.year(), event_time.month(), event_time.day())
            }
        };
        let end = match self {
            Self::CalendarYearUtc => start.checked_add_months(Months::new(12)),
            Self::CalendarMonthUtc => start.checked_add_months(Months::new(1)),
            Self::CalendarDayUtc => start.checked_add_days(Days::new(1)),
        }
        .expect("UTC calendar bucket end is representable");
        (start, end)
    }
}

impl fmt::Display for BucketStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for BucketStrategy {
    type Err = BucketStrategyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "calendar_year_utc" => Ok(Self::CalendarYearUtc),
            "calendar_month_utc" => Ok(Self::CalendarMonthUtc),
            "calendar_day_utc" => Ok(Self::CalendarDayUtc),
            _ => Err(BucketStrategyParseError(value.to_owned())),
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("unsupported bucket strategy `{0}`")]
pub struct BucketStrategyParseError(String);

/// A physical partition. `table_id` scopes the partition to one logical table;
/// `partition_key` identifies its logical Cassandra-style partition; and the
/// strategy, canonical key, and UTC bounds make the physical bucket self-describing.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BucketId {
    pub table_id: TableId,
    pub partition_key: PartitionKey,
    pub strategy: BucketStrategy,
    pub key: BucketKey,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl BucketId {
    pub fn for_event_time_with_strategy(
        table_id: TableId,
        partition_key: impl Into<PartitionKey>,
        strategy: BucketStrategy,
        event_time: DateTime<Utc>,
    ) -> Self {
        let (start, end) = strategy.bucket_bounds(event_time);
        Self {
            table_id,
            partition_key: partition_key.into(),
            strategy,
            key: strategy.bucket_key(event_time),
            start,
            end,
        }
    }

    pub fn contains(&self, event_time: DateTime<Utc>) -> bool {
        self.start <= event_time && event_time < self.end
    }
}

fn utc_date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .expect("valid UTC calendar bucket boundary")
}

/// Values are sorted by `(event_time, sort_key)` inside a bucket.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub partition_key: PartitionKey,
    pub event_time: DateTime<Utc>,
    pub sort_key: SortKey,
    pub value: Vec<u8>,
}

/// A stable continuation token for the declared `(event_time, sort_key)` order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub event_time: DateTime<Utc>,
    pub sort_key: SortKey,
}

impl From<&Record> for Cursor {
    fn from(record: &Record) -> Self {
        Self {
            event_time: record.event_time,
            sort_key: record.sort_key.clone(),
        }
    }
}

/// Inclusive start and exclusive end, which composes cleanly for pagination.
#[derive(Clone, Debug)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
    pub fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        self.start <= timestamp && timestamp < self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 12, 30, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn opaque_keys_accept_text_and_arbitrary_bytes() {
        let text = String::from("channel-42");
        let partition_key = PartitionKey::from(&text);
        assert_eq!(partition_key.as_bytes(), b"channel-42");
        assert_eq!(partition_key.len(), 10);
        assert!(!partition_key.is_empty());
        assert_eq!(partition_key.as_utf8().unwrap(), "channel-42");

        let sort_key = SortKey::from(&[0xff, 0x00][..]);
        assert_eq!(sort_key.as_bytes(), &[0xff, 0x00]);
        assert!(sort_key.as_utf8().is_err());
        assert!(SortKey::from(Vec::new()).is_empty());
    }

    #[test]
    fn opaque_keys_use_transparent_serde() {
        let key = PartitionKey::from(vec![0, 1, 255]);
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "[0,1,255]");
        assert_eq!(serde_json::from_str::<PartitionKey>(&json).unwrap(), key);
    }

    #[test]
    fn cursor_uses_the_record_clustering_values() {
        let record = Record {
            partition_key: "tenant".into(),
            event_time: timestamp(2024, 2, 29),
            sort_key: b"message-1".as_slice().into(),
            value: Vec::new(),
        };

        assert_eq!(
            Cursor::from(&record),
            Cursor {
                event_time: record.event_time,
                sort_key: record.sort_key.clone(),
            }
        );
    }

    #[test]
    fn bucket_key_rejects_empty_values_during_construction_and_deserialization() {
        assert_eq!(BucketKey::new("  "), Err(BucketKeyError::Empty));
        assert!(serde_json::from_str::<BucketKey>(r#"""#).is_err());
    }

    #[test]
    fn strategies_derive_canonical_keys_and_utc_bounds() {
        let event_time = timestamp(2024, 2, 29);
        let cases = [
            (
                BucketStrategy::CalendarYearUtc,
                "year:2024",
                (2024, 1, 1),
                (2025, 1, 1),
            ),
            (
                BucketStrategy::CalendarMonthUtc,
                "month:2024-02",
                (2024, 2, 1),
                (2024, 3, 1),
            ),
            (
                BucketStrategy::CalendarDayUtc,
                "day:2024-02-29",
                (2024, 2, 29),
                (2024, 3, 1),
            ),
        ];

        for (strategy, key, start, end) in cases {
            let bucket = BucketId::for_event_time_with_strategy(
                TableId::new("events").unwrap(),
                "tenant",
                strategy,
                event_time,
            );
            assert_eq!(bucket.partition_key, PartitionKey::from("tenant"));
            assert_eq!(bucket.key.as_str(), key);
            assert_eq!(bucket.start, utc_date(start.0, start.1, start.2));
            assert_eq!(bucket.end, utc_date(end.0, end.1, end.2));
            assert!(bucket.contains(event_time));
            assert!(!bucket.contains(bucket.end));
        }
    }
}
