use crate::{DurableTable, PartitionKey, Query, Record, TimeRange};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryArray, StringArray, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::{Session, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use futures::TryStreamExt;
use std::{fmt, sync::Arc};
use thiserror::Error;
use tracing::Instrument;

const BATCH_SIZE: usize = 1_024;
const ROUTED_SQL_TEMPLATE: &str = "SELECT channel_id, timestamp, id, value FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC, id ASC LIMIT $4";

/// Planning errors for the bounded, routed DataFusion adapter.
///
/// The messages intentionally do not contain partition values, record IDs, archive
/// object keys, or backend error details.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PulpitumTableProviderError {
    #[error("DataFusion scan requires equality on the configured shard key")]
    MissingShardEquality,
    #[error("DataFusion scan requires an inclusive timestamp lower bound")]
    MissingTimestampStart,
    #[error("DataFusion scan requires an exclusive timestamp upper bound")]
    MissingTimestampEnd,
    #[error("DataFusion scan timestamp range must have start before end")]
    InvalidTimestampRange,
    #[error("DataFusion scan contains an unsupported predicate")]
    UnsupportedPredicate,
    #[error("DataFusion scan has conflicting shard equality predicates")]
    ConflictingShardEquality,
    #[error("DataFusion scan contains conflicting timestamp bounds")]
    ConflictingTimestampBounds,
}

/// A read-only DataFusion table over one durable Pulpitum logical table.
///
/// Every scan requires a single partition equality and a finite `[start, end)`
/// timestamp range. Data is read exclusively through [`DurableTable`], which
/// preserves its durable hot/archive routing fence.
pub struct PulpitumTableProvider {
    table: Arc<DurableTable>,
    schema: SchemaRef,
}

impl fmt::Debug for PulpitumTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PulpitumTableProvider")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PulpitumTableProvider {
    pub fn new(table: Arc<DurableTable>) -> Self {
        let partition_key = table.definition().partition_key;
        let schema = Arc::new(Schema::new(vec![
            Field::new(partition_key, DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
                false,
            ),
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Binary, false),
        ]));
        Self { table, schema }
    }

    pub fn table(&self) -> &Arc<DurableTable> {
        &self.table
    }

    fn extract_query(&self, filters: &[Expr]) -> Result<RoutedQuery, PulpitumTableProviderError> {
        let mut query = RoutedQuery::default();
        for filter in filters {
            collect_predicate(filter, self.table.definition().partition_key, &mut query)?;
        }
        query.finish()
    }

    fn source_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait]
impl TableProvider for PulpitumTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let query = self.extract_query(filters).map_err(planning_error)?;
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let output_schema = Arc::new(self.schema.project(&projection).map_err(|_| {
            DataFusionError::Plan("DataFusion scan has an invalid projection".into())
        })?);

        Ok(Arc::new(RoutedScanExec::new(
            Arc::clone(&self.table),
            self.source_schema(),
            output_schema,
            projection,
            query,
            limit,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                validate_predicate_shape(filter, self.table.definition().partition_key)
                    .map_err(planning_error)?;
                Ok(TableProviderFilterPushDown::Exact)
            })
            .collect()
    }
}

#[derive(Default)]
struct RoutedQuery {
    partition_key: Option<PartitionKey>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl RoutedQuery {
    fn finish(self) -> Result<Self, PulpitumTableProviderError> {
        let start = self
            .start
            .ok_or(PulpitumTableProviderError::MissingTimestampStart)?;
        let end = self
            .end
            .ok_or(PulpitumTableProviderError::MissingTimestampEnd)?;
        if start >= end {
            return Err(PulpitumTableProviderError::InvalidTimestampRange);
        }
        if self.partition_key.is_none() {
            return Err(PulpitumTableProviderError::MissingShardEquality);
        }
        Ok(self)
    }
}

fn validate_predicate_shape(
    expression: &Expr,
    partition_column: &str,
) -> Result<(), PulpitumTableProviderError> {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            validate_predicate_shape(&binary.left, partition_column)?;
            validate_predicate_shape(&binary.right, partition_column)
        }
        Expr::BinaryExpr(binary) => {
            let left_column = column_name(&binary.left);
            let right_column = column_name(&binary.right);
            let left_literal = literal(&binary.left);
            let right_literal = literal(&binary.right);
            let supported = match binary.op {
                Operator::Eq => {
                    (left_column == Some(partition_column)
                        && string_literal(right_literal).is_some())
                        || (right_column == Some(partition_column)
                            && string_literal(left_literal).is_some())
                }
                Operator::GtEq | Operator::Lt => {
                    (left_column == Some("timestamp") && timestamp_literal(right_literal).is_some())
                        || (right_column == Some("timestamp")
                            && timestamp_literal(left_literal).is_some())
                }
                _ => false,
            };
            supported
                .then_some(())
                .ok_or(PulpitumTableProviderError::UnsupportedPredicate)
        }
        _ => Err(PulpitumTableProviderError::UnsupportedPredicate),
    }
}

fn collect_predicate(
    expression: &Expr,
    partition_column: &str,
    query: &mut RoutedQuery,
) -> Result<(), PulpitumTableProviderError> {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_predicate(&binary.left, partition_column, query)?;
            collect_predicate(&binary.right, partition_column, query)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let partition = if column_name(&binary.left) == Some(partition_column) {
                string_literal(literal(&binary.right))
            } else if column_name(&binary.right) == Some(partition_column) {
                string_literal(literal(&binary.left))
            } else {
                None
            }
            .ok_or(PulpitumTableProviderError::UnsupportedPredicate)?;
            let partition_key = PartitionKey::from(partition);
            if let Some(existing) = &query.partition_key
                && existing != &partition_key
            {
                return Err(PulpitumTableProviderError::ConflictingShardEquality);
            }
            query.partition_key = Some(partition_key);
            Ok(())
        }
        Expr::BinaryExpr(binary) => collect_timestamp_bound(binary, query),
        _ => Err(PulpitumTableProviderError::UnsupportedPredicate),
    }
}

fn collect_timestamp_bound(
    binary: &datafusion::logical_expr::BinaryExpr,
    query: &mut RoutedQuery,
) -> Result<(), PulpitumTableProviderError> {
    let left_column = column_name(&binary.left);
    let right_column = column_name(&binary.right);
    let left_timestamp = timestamp_literal(literal(&binary.left));
    let right_timestamp = timestamp_literal(literal(&binary.right));

    let (bound, is_start) = match (binary.op, left_column, right_column) {
        (Operator::GtEq, Some("timestamp"), _) => (right_timestamp, true),
        (Operator::Lt, Some("timestamp"), _) => (right_timestamp, false),
        (Operator::Lt, _, Some("timestamp")) => (left_timestamp, true),
        (Operator::GtEq, _, Some("timestamp")) => (left_timestamp, false),
        _ => return Err(PulpitumTableProviderError::UnsupportedPredicate),
    };
    let bound = bound.ok_or(PulpitumTableProviderError::UnsupportedPredicate)?;
    let slot = if is_start {
        &mut query.start
    } else {
        &mut query.end
    };
    if slot.replace(bound).is_some() {
        return Err(PulpitumTableProviderError::ConflictingTimestampBounds);
    }
    Ok(())
}

fn column_name(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::Column(column) => Some(column.name.as_str()),
        _ => None,
    }
}

fn literal(expression: &Expr) -> Option<&ScalarValue> {
    match expression {
        Expr::Literal(value, _) => Some(value),
        Expr::Cast(cast) => literal(&cast.expr),
        Expr::TryCast(cast) => literal(&cast.expr),
        _ => None,
    }
}

fn string_literal(value: Option<&ScalarValue>) -> Option<&str> {
    match value? {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn timestamp_literal(value: Option<&ScalarValue>) -> Option<DateTime<Utc>> {
    let nanos = match value? {
        ScalarValue::TimestampNanosecond(Some(value), _) => *value,
        ScalarValue::TimestampMicrosecond(Some(value), _) => value.checked_mul(1_000)?,
        ScalarValue::TimestampMillisecond(Some(value), _) => value.checked_mul(1_000_000)?,
        ScalarValue::TimestampSecond(Some(value), _) => value.checked_mul(1_000_000_000)?,
        _ => return None,
    };
    Some(DateTime::from_timestamp_nanos(nanos))
}

fn planning_error(error: PulpitumTableProviderError) -> DataFusionError {
    DataFusionError::Plan(error.to_string())
}

struct RoutedScanExec {
    table: Arc<DurableTable>,
    source_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: Vec<usize>,
    query: RoutedQuery,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for RoutedScanExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutedScanExec")
            .field("source_schema", &self.source_schema)
            .field("output_schema", &self.output_schema)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl RoutedScanExec {
    fn new(
        table: Arc<DurableTable>,
        source_schema: SchemaRef,
        output_schema: SchemaRef,
        projection: Vec<usize>,
        query: RoutedQuery,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            source_schema,
            output_schema,
            projection,
            query,
            limit,
            properties,
        }
    }
}

impl DisplayAs for RoutedScanExec {
    fn fmt_as(&self, format: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "PulpitumRoutedScanExec: bounded_partition_range=true, limit={:?}",
                self.limit
            ),
            DisplayFormatType::TreeRender => write!(formatter, "PulpitumRoutedScanExec"),
        }
    }
}

impl ExecutionPlan for RoutedScanExec {
    fn name(&self) -> &str {
        "PulpitumRoutedScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "Pulpitum routed scans do not accept children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "Pulpitum routed scan partition is unavailable".into(),
            ));
        }
        let table = Arc::clone(&self.table);
        let source_schema = Arc::clone(&self.source_schema);
        let output_schema = Arc::clone(&self.output_schema);
        let projection = self.projection.clone();
        let partition_key = self.query.partition_key.clone().ok_or_else(|| {
            DataFusionError::Internal("Pulpitum routed scan was not validated".into())
        })?;
        let range = TimeRange {
            start: self.query.start.ok_or_else(|| {
                DataFusionError::Internal("Pulpitum routed scan was not validated".into())
            })?,
            end: self.query.end.ok_or_else(|| {
                DataFusionError::Internal("Pulpitum routed scan was not validated".into())
            })?,
        };
        let limit = self.limit.unwrap_or(usize::MAX);

        let sql_span = tracing::info_span!(
            "SELECT messages",
            otel.kind = "internal",
            db.system.name = "datafusion",
            db.collection.name = "messages",
            db.operation.name = "SELECT",
            db.query.summary = "SELECT messages",
            db.query.text = ROUTED_SQL_TEMPLATE,
            pulpitum.sql.mode = "routed",
        );
        let stream = futures::stream::once(
            async move {
                let page = table
                    .query_page(Query {
                        partition_key,
                        range,
                        after: None,
                        limit,
                    })
                    .await
                    .map_err(|_| {
                        DataFusionError::Execution("Pulpitum routed read failed".into())
                    })?;
                record_batches(page.records, source_schema, output_schema, &projection)
            }
            .instrument(sql_span),
        )
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)))
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }
}

fn record_batches(
    records: Vec<Record>,
    source_schema: SchemaRef,
    _output_schema: SchemaRef,
    projection: &[usize],
) -> DataFusionResult<Vec<RecordBatch>> {
    records
        .chunks(BATCH_SIZE)
        .map(|records| {
            let partition_keys = records
                .iter()
                .map(|record| record.partition_key.as_utf8())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    DataFusionError::Execution(
                        "Pulpitum partition key is not valid UTF-8 for the SQL adapter".into(),
                    )
                })?;
            let partition_key: ArrayRef = Arc::new(StringArray::from_iter_values(partition_keys));
            let timestamps = records
                .iter()
                .map(|record| record.event_time.timestamp_nanos_opt())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Pulpitum timestamp is outside Arrow nanosecond range".into(),
                    )
                })?;
            let timestamp: ArrayRef = Arc::new(
                TimestampNanosecondArray::from_iter_values(timestamps).with_timezone("UTC"),
            );
            let sort_keys = records
                .iter()
                .map(|record| record.sort_key.as_utf8())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    DataFusionError::Execution(
                        "Pulpitum sort key is not valid UTF-8 for the SQL adapter".into(),
                    )
                })?;
            let id: ArrayRef = Arc::new(StringArray::from_iter_values(sort_keys));
            let value: ArrayRef = Arc::new(BinaryArray::from_iter_values(
                records.iter().map(|record| record.value.as_slice()),
            ));
            RecordBatch::try_new(
                source_schema.clone(),
                vec![partition_key, timestamp, id, value],
            )
            .and_then(|batch| batch.project(projection))
            .map_err(|_| {
                DataFusionError::Execution("Pulpitum could not build an Arrow batch".into())
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DurableArchiveCoordinator, InMemoryArchiveStore, InMemoryDurableBucketStore, Record,
        SortKey, TableDefinition, TableId,
    };
    use chrono::{Datelike, TimeZone};
    use datafusion::{
        arrow::util::display::array_value_to_string, execution::context::SessionContext,
    };

    #[test]
    fn records_map_core_keys_to_the_utf8_sql_surface() {
        let provider = test_provider();
        let schema = provider.schema();
        assert_eq!(
            schema.field(0),
            &Field::new("channel_id", DataType::Utf8, false)
        );
        assert_eq!(
            schema.field(1),
            &Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC"))),
                false,
            )
        );
        assert_eq!(schema.field(2), &Field::new("id", DataType::Utf8, false));
        assert_eq!(
            schema.field(3),
            &Field::new("value", DataType::Binary, false)
        );

        let event_time = Utc.with_ymd_and_hms(2026, 8, 11, 12, 0, 0).unwrap();
        let batches = record_batches(
            vec![Record {
                partition_key: PartitionKey::from("general"),
                event_time,
                sort_key: SortKey::from("message-1"),
                value: b"hello".to_vec(),
            }],
            Arc::clone(&schema),
            Arc::clone(&schema),
            &[0, 1, 2, 3],
        )
        .unwrap();
        let batch = &batches[0];
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "general"
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            event_time.timestamp_nanos_opt().unwrap()
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "message-1"
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"hello"
        );
    }

    #[test]
    fn non_utf8_core_keys_return_clear_execution_errors() {
        let provider = test_provider();
        let schema = provider.schema();
        let event_time = Utc.with_ymd_and_hms(2026, 8, 11, 12, 0, 0).unwrap();

        let partition_error = record_batches(
            vec![Record {
                partition_key: PartitionKey::from(vec![0xff]),
                event_time,
                sort_key: SortKey::from("message-1"),
                value: Vec::new(),
            }],
            Arc::clone(&schema),
            Arc::clone(&schema),
            &[0, 1, 2, 3],
        )
        .unwrap_err();
        assert!(matches!(
            partition_error,
            DataFusionError::Execution(message)
                if message == "Pulpitum partition key is not valid UTF-8 for the SQL adapter"
        ));

        let sort_error = record_batches(
            vec![Record {
                partition_key: PartitionKey::from("general"),
                event_time,
                sort_key: SortKey::from(vec![0xff]),
                value: Vec::new(),
            }],
            Arc::clone(&schema),
            schema,
            &[0, 1, 2, 3],
        )
        .unwrap_err();
        assert!(matches!(
            sort_error,
            DataFusionError::Execution(message)
                if message == "Pulpitum sort key is not valid UTF-8 for the SQL adapter"
        ));
    }

    #[tokio::test]
    async fn sql_reads_hot_and_archived_years_through_the_durable_table() {
        let store = Arc::new(InMemoryDurableBucketStore::default());
        let archive = Arc::new(InMemoryArchiveStore::default());
        let table = Arc::new(
            DurableTable::with_definition(
                TableDefinition::chat_messages(
                    "messages",
                    TableId::new("pulpitum.datafusion.messages").unwrap(),
                ),
                store.clone(),
                archive.clone(),
            )
            .unwrap(),
        );
        let year = Utc::now().year();
        let archived = Record {
            partition_key: PartitionKey::from("general"),
            event_time: Utc
                .with_ymd_and_hms(year - 1, 6, 1, 12, 0, 0)
                .single()
                .unwrap(),
            sort_key: SortKey::from("archived"),
            value: b"cold".to_vec(),
        };
        let hot = Record {
            partition_key: PartitionKey::from("general"),
            event_time: Utc.with_ymd_and_hms(year, 6, 1, 12, 0, 0).single().unwrap(),
            sort_key: SortKey::from("hot"),
            value: b"hot".to_vec(),
        };
        table.append(archived.clone()).await.unwrap();
        table.append(hot).await.unwrap();
        DurableArchiveCoordinator::new(store, archive)
            .archive_bucket(table.definition().bucket_for(&archived))
            .await
            .unwrap();

        let context = SessionContext::new();
        context
            .register_table("messages", Arc::new(PulpitumTableProvider::new(table)))
            .unwrap();
        let sql = format!(
            "SELECT id FROM messages WHERE channel_id = 'general' \
             AND timestamp >= TIMESTAMP '{}-01-01T00:00:00Z' \
             AND timestamp < TIMESTAMP '{}-01-01T00:00:00Z' ORDER BY timestamp, id",
            year - 1,
            year + 1
        );
        let batches = context.sql(&sql).await.unwrap().collect().await.unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(move |row| array_value_to_string(batch.column(0).as_ref(), row).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, ["archived", "hot"]);

        let count_sql = format!(
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = 'general' \
             AND timestamp >= TIMESTAMP '{}-01-01T00:00:00Z' \
             AND timestamp < TIMESTAMP '{}-01-01T00:00:00Z'",
            year - 1,
            year + 1
        );
        let count_batches = context
            .sql(&count_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            array_value_to_string(count_batches[0].column(0).as_ref(), 0).unwrap(),
            "2"
        );
    }

    #[tokio::test]
    async fn sql_rejects_missing_bounds_before_executing() {
        let context = bounded_test_context();
        let error = context
            .sql("SELECT id FROM messages WHERE channel_id = 'general' AND timestamp >= TIMESTAMP '2025-01-01T00:00:00Z'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exclusive timestamp upper bound")
        );
    }

    #[tokio::test]
    async fn sql_rejects_non_routing_predicates_before_executing() {
        let context = bounded_test_context();
        let error = context
            .sql("SELECT id FROM messages WHERE channel_id = 'general' AND timestamp >= TIMESTAMP '2025-01-01T00:00:00Z' AND timestamp < TIMESTAMP '2026-01-01T00:00:00Z' AND id = 'not-pushable'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unsupported predicate"));
    }

    fn test_provider() -> PulpitumTableProvider {
        PulpitumTableProvider::new(Arc::new(DurableTable::new(
            Arc::new(InMemoryDurableBucketStore::default()),
            Arc::new(InMemoryArchiveStore::default()),
        )))
    }

    fn bounded_test_context() -> SessionContext {
        let context = SessionContext::new();
        context
            .register_table("messages", Arc::new(test_provider()))
            .unwrap();
        context
    }
}
