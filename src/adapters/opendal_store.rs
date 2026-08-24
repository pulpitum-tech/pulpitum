use crate::{ArchiveStore, BucketId, PartitionKey, Record, SortKey, StoreError};
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use bytes::Bytes;

use opendal::{Operator, services::S3};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr, sync::Arc, time::Duration};
use thiserror::Error;

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const ARCHIVE_MANIFEST_VERSION: u32 = 4;
const RECORD_SCHEMA_VERSION: u32 = 2;
const PARQUET_ROW_GROUP_SIZE: usize = 16 * 1024;
const PARQUET_READ_BATCH_SIZE: usize = 1_024;

/// Encoding for newly written archive payloads.
///
/// Every newly written payload is described by a versioned archive manifest.
/// Readers select the codec recorded in that manifest, so changing the write
/// setting does not make existing archives unreadable.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveFormat {
    /// A UTF-8 JSON array of [`Record`] values.
    #[default]
    Json,
    /// A Zstandard-compressed Parquet payload using the built-in record schema.
    Parquet,
}

impl ArchiveFormat {
    fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Parquet => "parquet",
        }
    }

    fn serialize(self, records: &[Record]) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Json => serde_json::to_vec(records)
                .map_err(|error| StoreError::Other(format!("archive serialization: {error}"))),
            Self::Parquet => serialize_parquet(records),
        }
    }

    fn deserialize(self, body: &[u8]) -> Result<Vec<Record>, StoreError> {
        match self {
            Self::Json => serde_json::from_slice(body)
                .map_err(|error| StoreError::Other(format!("archive deserialization: {error}"))),
            Self::Parquet => deserialize_parquet(body),
        }
    }
}

impl fmt::Display for ArchiveFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json => formatter.write_str("json"),
            Self::Parquet => formatter.write_str("parquet"),
        }
    }
}

/// Error returned when a configured archive payload format is unsupported.
#[derive(Debug, Error, Eq, PartialEq)]
#[error("unsupported archive format {value:?}; supported formats: json, parquet")]
pub struct ArchiveFormatParseError {
    value: String,
}

impl FromStr for ArchiveFormat {
    type Err = ArchiveFormatParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "parquet" => Ok(Self::Parquet),
            _ => Err(ArchiveFormatParseError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3ServerSideEncryption {
    S3Managed,
    AwsManagedKms,
    CustomerManagedKms(String),
}

/// Production S3 configuration. Omit static credentials to use OpenDAL's
/// environment, shared-config, web-identity, and instance-metadata chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ArchiveConfig {
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub prefix: String,
    pub allow_http: bool,
    pub server_side_encryption: Option<S3ServerSideEncryption>,
}

impl S3ArchiveConfig {
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            endpoint: None,
            bucket: bucket.into(),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            prefix: prefix.into(),
            allow_http: false,
            server_side_encryption: None,
        }
    }
}

/// Metadata describing one immutable archive payload.
///
/// A manifest is written only after its payload is present. Its key—not the
/// payload key—is published through the durable bucket store, providing the
/// read path with an immutable format, schema, count, and integrity contract.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ArchiveManifest {
    version: u32,
    format: ArchiveFormat,
    schema_version: u32,
    bucket: BucketId,
    generation: Option<u64>,
    payload_key: String,
    record_count: u64,
    payload_bytes: u64,
    sha256: String,
    clustering_key: Vec<String>,
}

impl ArchiveManifest {
    fn new(
        bucket: &BucketId,
        generation: Option<u64>,
        format: ArchiveFormat,
        payload_key: String,
        records: &[Record],
        payload: &[u8],
    ) -> Result<Self, StoreError> {
        Ok(Self {
            version: ARCHIVE_MANIFEST_VERSION,
            format,
            schema_version: RECORD_SCHEMA_VERSION,
            bucket: bucket.clone(),
            generation,
            payload_key,
            record_count: u64::try_from(records.len())
                .map_err(|_| StoreError::Other("archive record count exceeds u64".into()))?,
            payload_bytes: u64::try_from(payload.len())
                .map_err(|_| StoreError::Other("archive payload exceeds u64 bytes".into()))?,
            sha256: sha256(payload),
            clustering_key: vec!["event_time".into(), "sort_key".into()],
        })
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.version != ARCHIVE_MANIFEST_VERSION {
            return Err(StoreError::Other(format!(
                "unsupported archive manifest version {}",
                self.version
            )));
        }
        if self.schema_version != RECORD_SCHEMA_VERSION {
            return Err(StoreError::Other(format!(
                "unsupported archive record schema version {}",
                self.schema_version
            )));
        }
        if self.payload_key.is_empty() {
            return Err(StoreError::Other(
                "archive manifest payload key is empty".into(),
            ));
        }
        if self.clustering_key != ["event_time", "sort_key"] {
            return Err(StoreError::Other(
                "archive manifest has an unsupported clustering key".into(),
            ));
        }
        Ok(())
    }

    fn verify_payload(&self, payload: &[u8]) -> Result<(), StoreError> {
        let payload_bytes = u64::try_from(payload.len())
            .map_err(|_| StoreError::Other("archive payload exceeds u64 bytes".into()))?;
        if payload_bytes != self.payload_bytes {
            return Err(StoreError::Other(
                "archive payload length does not match manifest".into(),
            ));
        }
        if sha256(payload) != self.sha256 {
            return Err(StoreError::Other(
                "archive payload checksum does not match manifest".into(),
            ));
        }
        Ok(())
    }
}

/// S3-compatible immutable archive adapter. It works with AWS S3 and MinIO;
/// OpenDAL supplies the object-storage transport and request signing.
pub struct OpenDalArchiveStore {
    operator: Operator,
    bucket: String,
    prefix: String,
    format: ArchiveFormat,
    operation_timeout: Duration,
}

impl OpenDalArchiveStore {
    pub fn s3(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        prefix: &str,
    ) -> Result<Self, StoreError> {
        let mut config = S3ArchiveConfig::new(bucket, prefix);
        config.endpoint = Some(endpoint.to_owned());
        config.region = Some("us-east-1".into());
        config.access_key_id = Some(access_key.to_owned());
        config.secret_access_key = Some(secret_key.to_owned());
        config.allow_http = true;
        Self::s3_config(config)
    }

    pub fn s3_config(config: S3ArchiveConfig) -> Result<Self, StoreError> {
        if config.bucket.trim().is_empty() {
            return Err(StoreError::Other("S3 archive bucket is required".into()));
        }
        if config
            .endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.starts_with("http://"))
            && !config.allow_http
        {
            return Err(StoreError::Other(
                "S3 archive endpoint must use HTTPS unless allow_http is explicitly enabled for development"
                    .into(),
            ));
        }
        if config.access_key_id.is_some() != config.secret_access_key.is_some() {
            return Err(StoreError::Other(
                "S3 access key and secret key must be configured together".into(),
            ));
        }
        if config.session_token.is_some() && config.access_key_id.is_none() {
            return Err(StoreError::Other(
                "an explicit S3 session token requires explicit access and secret keys".into(),
            ));
        }

        let mut service = S3::default().bucket(&config.bucket);
        if let Some(endpoint) = &config.endpoint {
            service = service.endpoint(endpoint);
        }
        if let Some(region) = &config.region {
            service = service.region(region);
        }
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            service = service
                .access_key_id(access_key)
                .secret_access_key(secret_key);
        }
        if let Some(session_token) = &config.session_token {
            service = service.session_token(session_token);
        }
        if let Some(encryption) = &config.server_side_encryption {
            service = match encryption {
                S3ServerSideEncryption::S3Managed => service.server_side_encryption_with_s3_key(),
                S3ServerSideEncryption::AwsManagedKms => {
                    service.server_side_encryption_with_aws_managed_kms_key()
                }
                S3ServerSideEncryption::CustomerManagedKms(key_id) => {
                    service.server_side_encryption_with_customer_managed_kms_key(key_id)
                }
            };
        }

        let operator = Operator::new(service).map_err(object_error)?;
        Ok(Self::with_operator(
            operator,
            &config.bucket,
            &config.prefix,
        ))
    }

    fn with_operator(operator: Operator, bucket: &str, prefix: &str) -> Self {
        Self {
            operator,
            bucket: bucket.to_owned(),
            prefix: prefix.trim_matches('/').to_owned(),
            format: ArchiveFormat::default(),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }

    /// Sets the payload encoding used for newly written archive objects.
    ///
    /// Reads use the format recorded in each manifest instead. The default is
    /// [`ArchiveFormat::Json`].
    pub fn with_format(mut self, format: ArchiveFormat) -> Self {
        self.format = format;
        self
    }

    /// Sets the deadline applied independently to each object-store operation.
    ///
    /// A deadline is required because a connected but unresponsive endpoint can
    /// otherwise leave reads and archival cutovers pending indefinitely.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, StoreError> {
        if timeout.is_zero() {
            return Err(StoreError::Other(
                "object-store operation timeout must be greater than zero".into(),
            ));
        }
        self.operation_timeout = timeout;
        Ok(self)
    }

    fn bucket_prefix(&self, bucket: &BucketId) -> String {
        format!(
            "{}/{}/{}/{}",
            self.prefix,
            encode_path_segment(bucket.table_id.as_str().as_bytes()),
            encode_path_segment(bucket.partition_key.as_bytes()),
            encode_path_segment(bucket.key.as_str().as_bytes()),
        )
    }

    fn object_directory(&self, bucket: &BucketId, generation: Option<u64>) -> String {
        match generation {
            Some(generation) => format!("{}/generation-{generation}", self.bucket_prefix(bucket)),
            None => self.bucket_prefix(bucket),
        }
    }

    fn content_payload_key(
        &self,
        bucket: &BucketId,
        generation: Option<u64>,
        digest: &str,
    ) -> String {
        format!(
            "{}/records-{digest}.{}",
            self.object_directory(bucket, generation),
            self.format.extension(),
        )
    }

    fn content_manifest_key(
        &self,
        bucket: &BucketId,
        generation: Option<u64>,
        digest: &str,
    ) -> String {
        format!(
            "{}/manifest-{digest}.json",
            self.object_directory(bucket, generation)
        )
    }

    async fn create_object_verified(
        &self,
        key: &str,
        expected: Vec<u8>,
    ) -> Result<Vec<u8>, StoreError> {
        let write = tokio::time::timeout(
            self.operation_timeout,
            self.operator
                .write_with(key, expected.clone())
                .if_not_exists(true),
        )
        .await;
        let write_error = match write {
            Ok(Ok(_)) => None,
            Ok(Err(error)) if error.kind() == opendal::ErrorKind::ConditionNotMatch => None,
            Ok(Err(error)) => Some(object_error(error)),
            Err(_) => Some(operation_timeout_error()),
        };

        match self.read_object(key).await {
            Ok(uploaded) if uploaded == expected => Ok(uploaded),
            Ok(_) => Err(StoreError::Other(
                "immutable archive object already exists with different content".into(),
            )),
            Err(_) if write_error.is_some() => Err(write_error.expect("checked as present")),
            Err(error) => Err(error),
        }
    }

    async fn read_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let body = tokio::time::timeout(self.operation_timeout, self.operator.read(key))
            .await
            .map_err(|_| operation_timeout_error())?
            .map_err(object_error)?;
        Ok(body.to_bytes().to_vec())
    }

    #[tracing::instrument(
        name = "pulpitum.archive.write",
        skip(self, bucket, records),
        err,
        fields(
            otel.kind = "client",
            rpc.system.name = "aws-api",
            cloud.region = "us-east-1",
            aws.s3.bucket = %self.bucket,
            pulpitum.archive.records = records.len(),
            pulpitum.archive.format = %self.format,
        )
    )]
    async fn write_records(
        &self,
        bucket: &BucketId,
        generation: Option<u64>,
        records: &[Record],
    ) -> Result<String, StoreError> {
        validate_records(bucket, records)?;
        let payload = self.format.serialize(records)?;
        let payload_key = self.content_payload_key(bucket, generation, &sha256(payload.as_slice()));
        let manifest = ArchiveManifest::new(
            bucket,
            generation,
            self.format,
            payload_key.clone(),
            records,
            &payload,
        )?;
        let manifest_body = serde_json::to_vec(&manifest).map_err(|error| {
            StoreError::Other(format!("archive manifest serialization: {error}"))
        })?;
        let manifest_key =
            self.content_manifest_key(bucket, generation, &sha256(manifest_body.as_slice()));

        let uploaded_payload = self.create_object_verified(&payload_key, payload).await?;
        manifest.verify_payload(&uploaded_payload)?;
        if u64::try_from(self.format.deserialize(&uploaded_payload)?.len())
            .map_err(|_| StoreError::Other("archive record count exceeds u64".into()))?
            != manifest.record_count
        {
            return Err(StoreError::Other(
                "uploaded archive record count does not match manifest".into(),
            ));
        }

        let uploaded_manifest = self
            .create_object_verified(&manifest_key, manifest_body)
            .await?;
        let verified_manifest: ArchiveManifest = serde_json::from_slice(&uploaded_manifest)
            .map_err(|error| {
                StoreError::Other(format!("archive manifest deserialization: {error}"))
            })?;
        verified_manifest.validate()?;
        if verified_manifest != manifest {
            return Err(StoreError::Other(
                "uploaded archive manifest does not match the intended manifest".into(),
            ));
        }
        Ok(manifest_key)
    }

    async fn read_manifest_records(
        &self,
        expected_bucket: &BucketId,
        manifest: ArchiveManifest,
    ) -> Result<Vec<Record>, StoreError> {
        manifest.validate()?;
        if manifest.bucket != *expected_bucket {
            return Err(StoreError::Other(
                "archive manifest does not match the requested bucket".into(),
            ));
        }
        let payload = self.read_object(&manifest.payload_key).await?;
        manifest.verify_payload(&payload)?;
        let records = manifest.format.deserialize(&payload)?;
        if u64::try_from(records.len())
            .map_err(|_| StoreError::Other("archive record count exceeds u64".into()))?
            != manifest.record_count
        {
            return Err(StoreError::Other(
                "archive record count does not match manifest".into(),
            ));
        }
        validate_records(expected_bucket, &records)?;
        Ok(records)
    }
}

#[async_trait]
impl ArchiveStore for OpenDalArchiveStore {
    async fn put_bucket(
        &self,
        bucket: &BucketId,
        records: &[Record],
    ) -> Result<String, StoreError> {
        self.write_records(bucket, None, records).await
    }

    async fn put_bucket_generation(
        &self,
        bucket: &BucketId,
        generation: u64,
        records: &[Record],
    ) -> Result<String, StoreError> {
        self.write_records(bucket, Some(generation), records).await
    }

    #[tracing::instrument(
        name = "pulpitum.archive.read",
        skip(self, object_key),
        err,
        fields(
            otel.kind = "client",
            rpc.system.name = "aws-api",
            cloud.region = "us-east-1",
            aws.s3.bucket = %self.bucket,
            pulpitum.archive.bytes_read = tracing::field::Empty,
            pulpitum.archive.format = tracing::field::Empty,
            pulpitum.archive.object_reads = tracing::field::Empty,
            pulpitum.archive.records = tracing::field::Empty,
        )
    )]
    async fn get_bucket(
        &self,
        bucket: &BucketId,
        object_key: &str,
    ) -> Result<Vec<Record>, StoreError> {
        let span = tracing::Span::current();
        let object = self.read_object(object_key).await?;
        let manifest_bytes = object.len();
        if let Some(expected_digest) = manifest_digest(object_key)?
            && sha256(&object) != expected_digest
        {
            return Err(StoreError::Other(
                "archive manifest checksum does not match its object key".into(),
            ));
        }

        let manifest = serde_json::from_slice::<ArchiveManifest>(&object).map_err(|error| {
            StoreError::Other(format!("archive manifest deserialization: {error}"))
        })?;
        let payload_bytes = manifest.payload_bytes;
        let format = manifest.format;
        let records = self.read_manifest_records(bucket, manifest).await?;
        span.record(
            "pulpitum.archive.bytes_read",
            u64::try_from(manifest_bytes)
                .map_err(|_| StoreError::Other("archive manifest exceeds u64 bytes".into()))?
                .saturating_add(payload_bytes),
        );
        span.record("pulpitum.archive.format", format.to_string());
        span.record("pulpitum.archive.object_reads", 2_u64);
        span.record("pulpitum.archive.records", records.len());
        Ok(records)
    }
}

fn archive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Binary, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
            false,
        ),
        Field::new("sort_key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ]))
}

fn serialize_parquet(records: &[Record]) -> Result<Vec<u8>, StoreError> {
    let event_times = records
        .iter()
        .map(|record| {
            record.event_time.timestamp_nanos_opt().ok_or_else(|| {
                StoreError::Other("archive event time cannot be represented as nanoseconds".into())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(
        archive_schema(),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                records.iter().map(|record| record.partition_key.as_bytes()),
            )) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(event_times).with_timezone("UTC")) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(
                records.iter().map(|record| record.sort_key.as_bytes()),
            )) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(
                records.iter().map(|record| record.value.as_slice()),
            )) as ArrayRef,
        ],
    )
    .map_err(|error| StoreError::Other(format!("archive parquet serialization: {error}")))?;
    let zstd_level = ZstdLevel::try_new(3)
        .map_err(|error| StoreError::Other(format!("archive parquet compression: {error}")))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_SIZE))
        .build();

    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, archive_schema(), Some(properties))
        .map_err(|error| StoreError::Other(format!("archive parquet serialization: {error}")))?;
    writer
        .write(&batch)
        .map_err(|error| StoreError::Other(format!("archive parquet serialization: {error}")))?;
    writer
        .close()
        .map_err(|error| StoreError::Other(format!("archive parquet serialization: {error}")))?;
    Ok(output)
}

fn deserialize_parquet(body: &[u8]) -> Result<Vec<Record>, StoreError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(body))
        .map_err(|error| StoreError::Other(format!("archive parquet deserialization: {error}")))?
        .with_batch_size(PARQUET_READ_BATCH_SIZE)
        .build()
        .map_err(|error| StoreError::Other(format!("archive parquet deserialization: {error}")))?;
    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| {
            StoreError::Other(format!("archive parquet deserialization: {error}"))
        })?;
        records.extend(records_from_batch(&batch)?);
    }
    Ok(records)
}

fn records_from_batch(batch: &RecordBatch) -> Result<Vec<Record>, StoreError> {
    let schema = batch.schema();
    if schema.fields() != archive_schema().fields() {
        return Err(StoreError::Other(
            "archive parquet schema is unsupported; expected partition_key Binary, event_time Timestamp(ns UTC), sort_key Binary, value Binary"
                .into(),
        ));
    }
    let partition_key = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            StoreError::Other("archive parquet partition key column is invalid".into())
        })?;
    let event_time = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| StoreError::Other("archive parquet event time column is invalid".into()))?;
    let sort_key = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| StoreError::Other("archive parquet sort key column is invalid".into()))?;
    let value = batch
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| StoreError::Other("archive parquet value column is invalid".into()))?;

    let mut records = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if partition_key.is_null(row)
            || event_time.is_null(row)
            || sort_key.is_null(row)
            || value.is_null(row)
        {
            return Err(StoreError::Other(
                "archive parquet contains null record fields".into(),
            ));
        }
        records.push(Record {
            partition_key: PartitionKey::from(partition_key.value(row).to_vec()),
            event_time: chrono::DateTime::from_timestamp_nanos(event_time.value(row)),
            sort_key: SortKey::from(sort_key.value(row).to_vec()),
            value: value.value(row).to_vec(),
        });
    }
    Ok(records)
}

fn validate_records(bucket: &BucketId, records: &[Record]) -> Result<(), StoreError> {
    if records.iter().any(|record| {
        record.partition_key != bucket.partition_key || !bucket.contains(record.event_time)
    }) {
        return Err(StoreError::Other(
            "archive records do not all belong to the requested bucket".into(),
        ));
    }
    if records.windows(2).any(|records| {
        (&records[0].event_time, &records[0].sort_key)
            > (&records[1].event_time, &records[1].sort_key)
    }) {
        return Err(StoreError::Other(
            "archive records are not sorted by event time and sort key".into(),
        ));
    }
    Ok(())
}

fn encode_path_segment(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha256(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn manifest_digest(object_key: &str) -> Result<Option<&str>, StoreError> {
    let Some(filename) = object_key.rsplit('/').next() else {
        return Ok(None);
    };
    let Some(digest) = filename
        .strip_prefix("manifest-")
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return Ok(None);
    };
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StoreError::Other(
            "archive manifest object key contains an invalid checksum".into(),
        ));
    }
    Ok(Some(digest))
}

fn operation_timeout_error() -> StoreError {
    StoreError::Other("object-store operation timed out".into())
}

fn object_error(error: opendal::Error) -> StoreError {
    StoreError::Other(format!("object store: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BucketStrategy, TableId};
    use chrono::{TimeZone, Utc};
    use opendal::services::Memory;

    fn records() -> Vec<Record> {
        vec![
            Record {
                partition_key: PartitionKey::from(b"general".to_vec()),
                event_time: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
                sort_key: SortKey::from(vec![0, 0xff]),
                value: vec![0, 1, 2],
            },
            Record {
                partition_key: PartitionKey::from(b"general".to_vec()),
                event_time: Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).single().unwrap(),
                sort_key: SortKey::from(b"second".to_vec()),
                value: b"value".to_vec(),
            },
        ]
    }

    #[test]
    fn archive_formats_round_trip_records() {
        let records = records();
        for format in [ArchiveFormat::Json, ArchiveFormat::Parquet] {
            let payload = format.serialize(&records).unwrap();
            assert_eq!(format.deserialize(&payload).unwrap(), records);
        }
    }

    #[test]
    fn json_format_uses_the_new_record_fields() {
        let payload = ArchiveFormat::Json.serialize(&records()).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let record = json[0].as_object().unwrap();

        assert_eq!(record.len(), 4);
        assert!(record.contains_key("partition_key"));
        assert!(record.contains_key("event_time"));
        assert!(record.contains_key("sort_key"));
        assert!(record.contains_key("value"));
    }

    #[test]
    fn parquet_schema_uses_binary_keys_and_utc_event_time() {
        let schema = archive_schema();
        assert_eq!(
            schema.field(0),
            &Field::new("partition_key", DataType::Binary, false)
        );
        assert_eq!(
            schema.field(1),
            &Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
                false,
            )
        );
        assert_eq!(
            schema.field(2),
            &Field::new("sort_key", DataType::Binary, false)
        );
        assert_eq!(
            schema.field(3),
            &Field::new("value", DataType::Binary, false)
        );
    }

    fn bucket() -> BucketId {
        BucketId::for_event_time_with_strategy(
            TableId::new("test-table").expect("test table ID is valid"),
            PartitionKey::from(b"general".to_vec()),
            BucketStrategy::CalendarYearUtc,
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap(),
        )
    }

    fn memory_store(format: ArchiveFormat) -> OpenDalArchiveStore {
        let operator = Operator::new(Memory::default()).unwrap();
        OpenDalArchiveStore::with_operator(operator, "test", "archives").with_format(format)
    }

    #[test]
    fn production_s3_config_rejects_plaintext_and_incomplete_credentials() {
        let mut plaintext = S3ArchiveConfig::new("bucket", "archives");
        plaintext.endpoint = Some("http://s3.example.test".into());
        assert!(OpenDalArchiveStore::s3_config(plaintext).is_err());

        let mut incomplete = S3ArchiveConfig::new("bucket", "archives");
        incomplete.endpoint = Some("https://s3.example.test".into());
        incomplete.access_key_id = Some("access".into());
        assert!(OpenDalArchiveStore::s3_config(incomplete).is_err());
    }

    #[test]
    fn production_s3_config_supports_the_standard_credential_chain() {
        let mut config = S3ArchiveConfig::new("bucket", "archives");
        config.endpoint = Some("https://s3.example.test".into());
        config.region = Some("us-west-2".into());
        config.server_side_encryption = Some(S3ServerSideEncryption::S3Managed);
        assert!(OpenDalArchiveStore::s3_config(config).is_ok());
    }

    #[test]
    fn json_format_writes_content_addressed_payload_and_manifest_keys() {
        let store = OpenDalArchiveStore::s3(
            "http://127.0.0.1:9000",
            "pulpitum",
            "access-key",
            "secret-key",
            "archives",
        )
        .unwrap()
        .with_format(ArchiveFormat::Json);
        let bucket = bucket();
        let digest = "a".repeat(64);

        assert_eq!(
            store.content_payload_key(&bucket, None, &digest),
            format!(
                "archives/746573742d7461626c65/67656e6572616c/796561723a32303234/records-{digest}.json"
            )
        );
        assert_eq!(
            store.content_manifest_key(&bucket, None, &digest),
            format!(
                "archives/746573742d7461626c65/67656e6572616c/796561723a32303234/manifest-{digest}.json"
            )
        );
        assert_eq!(
            store.content_payload_key(&bucket, Some(7), &digest),
            format!(
                "archives/746573742d7461626c65/67656e6572616c/796561723a32303234/generation-7/records-{digest}.json"
            )
        );
        assert_eq!(
            store.content_manifest_key(&bucket, Some(7), &digest),
            format!(
                "archives/746573742d7461626c65/67656e6572616c/796561723a32303234/generation-7/manifest-{digest}.json"
            )
        );

        let mut binary_bucket = bucket;
        binary_bucket.partition_key = PartitionKey::from(vec![0x00, 0xff, b'/']);
        assert_eq!(
            store.content_payload_key(&binary_bucket, None, &digest),
            format!(
                "archives/746573742d7461626c65/00ff2f/796561723a32303234/records-{digest}.json"
            )
        );
    }

    #[test]
    fn manifest_verifies_payload_integrity() {
        let records = records();
        let payload = ArchiveFormat::Json.serialize(&records).unwrap();
        let manifest = ArchiveManifest::new(
            &bucket(),
            Some(1),
            ArchiveFormat::Json,
            "archives/746573742d7461626c65/67656e6572616c/796561723a32303234/generation-1/records.json"
                .into(),
            &records,
            &payload,
        )
        .unwrap();

        manifest.validate().unwrap();
        assert_eq!(manifest.version, 4);
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.clustering_key, ["event_time", "sort_key"]);
        let manifest_json = serde_json::to_value(&manifest).unwrap();
        assert_eq!(
            manifest_json["clustering_key"],
            serde_json::json!(["event_time", "sort_key"])
        );
        assert!(manifest_json.get("sort_key").is_none());
        manifest.verify_payload(&payload).unwrap();
        assert!(manifest.verify_payload(b"changed").is_err());
    }

    #[tokio::test]
    async fn immutable_archive_writes_are_idempotent_and_round_trip() {
        for format in [ArchiveFormat::Json, ArchiveFormat::Parquet] {
            let store = memory_store(format);
            let bucket = bucket();
            let records = records();

            let first = store
                .put_bucket_generation(&bucket, 7, &records)
                .await
                .unwrap();
            let second = store
                .put_bucket_generation(&bucket, 7, &records)
                .await
                .unwrap();

            assert_eq!(first, second);
            assert!(first.contains("generation-7/manifest-"));
            assert_eq!(store.get_bucket(&bucket, &first).await.unwrap(), records);
        }
    }

    #[tokio::test]
    async fn immutable_archive_write_rejects_existing_different_content() {
        let store = memory_store(ArchiveFormat::Json);
        let bucket = bucket();
        let records = records();
        let payload = ArchiveFormat::Json.serialize(&records).unwrap();
        let payload_key = store.content_payload_key(&bucket, Some(3), &sha256(payload.as_slice()));
        store
            .operator
            .write(&payload_key, b"different".to_vec())
            .await
            .unwrap();

        let error = store
            .put_bucket_generation(&bucket, 3, &records)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("different content"));
        assert_eq!(
            store.operator.read(&payload_key).await.unwrap().to_bytes(),
            Bytes::from_static(b"different")
        );
    }

    #[tokio::test]
    async fn archive_reads_reject_a_tampered_content_addressed_manifest() {
        let store = memory_store(ArchiveFormat::Json);
        let bucket = bucket();
        let key = store
            .put_bucket_generation(&bucket, 4, &records())
            .await
            .unwrap();
        store.operator.write(&key, b"{}".to_vec()).await.unwrap();

        let error = store.get_bucket(&bucket, &key).await.unwrap_err();
        assert!(error.to_string().contains("manifest checksum"));
    }

    #[test]
    fn archive_format_parses_supported_values_case_insensitively() {
        assert_eq!("json".parse(), Ok(ArchiveFormat::Json));
        assert_eq!(" PARQUET ".parse(), Ok(ArchiveFormat::Parquet));
        assert!("ndjson".parse::<ArchiveFormat>().is_err());
    }

    #[test]
    fn archive_records_must_match_the_bucket_and_clustering_key() {
        let bucket = bucket();
        let mut wrong_bucket = records();
        wrong_bucket[0].partition_key = PartitionKey::from(b"other".to_vec());
        assert!(validate_records(&bucket, &wrong_bucket).is_err());

        let mut outside_bucket_bounds = records();
        outside_bucket_bounds[0].event_time =
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).single().unwrap();
        assert!(validate_records(&bucket, &outside_bucket_bounds).is_err());

        let mut unsorted_event_time = records();
        unsorted_event_time.reverse();
        assert!(validate_records(&bucket, &unsorted_event_time).is_err());

        let event_time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).single().unwrap();
        let mut unsorted_sort_key = records();
        unsorted_sort_key[0].event_time = event_time;
        unsorted_sort_key[0].sort_key = SortKey::from(b"second".to_vec());
        unsorted_sort_key[1].event_time = event_time;
        unsorted_sort_key[1].sort_key = SortKey::from(b"first".to_vec());
        assert!(validate_records(&bucket, &unsorted_sort_key).is_err());

        validate_records(&bucket, &records()).unwrap();
    }
}
