mod archive_connection;
mod cockroach_connection;
mod sql_sidecar_security;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use datafusion::{
    arrow::{
        array::{
            BinaryArray, LargeBinaryArray, LargeStringArray, StringArray, TimestampNanosecondArray,
        },
        datatypes::{DataType, Schema, TimeUnit},
        record_batch::RecordBatch,
        util::display::array_value_to_string,
    },
    execution::context::SessionContext,
    scalar::ScalarValue,
};
use futures::stream;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use pgwire::{
    api::auth::StartupHandler,
    api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type},
    api::{
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        stmt::{NoopQueryParser, StoredStatement},
    },
    api::{
        results::{
            DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat,
            FieldInfo, QueryResponse, Response, Tag,
        },
        store::PortalStore,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::data::DataRow,
    tokio::process_socket,
};
use pulpitum::{
    ArchiveFormat, CockroachPoolConfig, DurableTable, ImmutableArchiveCache, OtelTelemetry,
    PartitionKey, PulpitumTableProvider, Record, SortKey, TableDefinition, TableId,
};
use sql_sidecar_security::SidecarSecurity;
use sqlparser::{
    ast::{
        DataType as SqlDataType, Expr, SetExpr, Statement, TableObject, TimezoneInfo,
        Value as SqlValue,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{collections::BTreeMap, env, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing_subscriber::prelude::*;

// Gateway spans are SERVER spans. Query telemetry is restricted to a small set
// of fixed, parameterized shapes so that every gateway request is visible
// without leaking SQL literals or creating unbounded metric cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GatewayQueryShape {
    Insert,
    Select,
    Count,
    Other,
}

impl GatewayQueryShape {
    fn from_statement(statement: &str) -> Self {
        let statement = statement.trim_start();
        let operation_end = statement
            .find(|character: char| !character.is_ascii_alphabetic())
            .unwrap_or(statement.len());
        let operation = &statement[..operation_end];

        if operation.eq_ignore_ascii_case("INSERT") {
            return Self::Insert;
        }
        if !operation.eq_ignore_ascii_case("SELECT") {
            return Self::Other;
        }

        let projection = statement[operation_end..].trim_start();
        let count_end = projection
            .find(|character: char| !character.is_ascii_alphabetic())
            .unwrap_or(projection.len());
        if projection[..count_end].eq_ignore_ascii_case("COUNT") {
            Self::Count
        } else {
            Self::Select
        }
    }

    fn operation_name(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Select | Self::Count => "SELECT",
            Self::Other => "OTHER",
        }
    }

    fn summary(self, table_name: &str) -> String {
        match self {
            Self::Insert => format!("INSERT {table_name}"),
            Self::Select => format!("SELECT {table_name}"),
            Self::Count => format!("SELECT COUNT {table_name}"),
            Self::Other => format!("OTHER {table_name}"),
        }
    }

    fn template(self, table_name: &str) -> String {
        match self {
            Self::Insert => format!(
                "INSERT INTO {table_name} (channel_id, timestamp, id, value) \
                 VALUES ($1, $2, $3, $4)"
            ),
            Self::Select => format!(
                "SELECT timestamp, id, value FROM {table_name} WHERE channel_id = $1 \
                 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC, id ASC LIMIT $4"
            ),
            Self::Count => format!(
                "SELECT COUNT(*) AS message_count FROM {table_name} WHERE channel_id = $1 \
                 AND timestamp >= $2 AND timestamp < $3"
            ),
            Self::Other => format!("OTHER SQL statement against {table_name}"),
        }
    }
}

fn gateway_query_shape(statement: &str) -> GatewayQueryShape {
    GatewayQueryShape::from_statement(statement)
}

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn archive_cache_config() -> Result<(usize, usize), Box<dyn std::error::Error>> {
    Ok((
        setting("PULPITUM_SQL_ARCHIVE_CACHE_MAX_BYTES", "268435456").parse()?,
        setting("PULPITUM_SQL_ARCHIVE_CACHE_MAX_ENTRIES", "512").parse()?,
    ))
}

fn capture_query_text() -> Result<bool, Box<dyn std::error::Error>> {
    Ok(setting("PULPITUM_SQL_CAPTURE_QUERY_TEXT", "false").parse()?)
}

fn validate_listen_address(
    address: SocketAddr,
    secure: bool,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    if !secure && !address.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "insecure SQL sidecar mode is restricted to loopback; configure TLS and SCRAM to listen on a non-loopback address",
        )
        .into());
    }
    Ok(address)
}

fn listen_address(security: &SidecarSecurity) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    validate_listen_address(
        setting("PULPITUM_SQL_LISTEN_ADDR", "127.0.0.1:5433").parse()?,
        security.is_secure(),
    )
}

fn telemetry_query_template(statement: &str, table_name: &str, capture_query_text: bool) -> String {
    if capture_query_text {
        statement.trim().to_owned()
    } else {
        gateway_query_shape(statement).template(table_name)
    }
}

fn pool_config() -> Result<CockroachPoolConfig, Box<dyn std::error::Error>> {
    let mut config = CockroachPoolConfig::default();
    if let Ok(value) = env::var("COCKROACH_POOL_MAX_CONNECTIONS") {
        config.max_connections = value.parse()?;
    }
    if config.max_connections == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "COCKROACH_POOL_MAX_CONNECTIONS must be greater than zero",
        )
        .into());
    }
    Ok(config)
}

fn install_telemetry(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resource = Resource::builder_empty()
        .with_service_name("pulpitum-sql-sidecar")
        .build();
    let metrics = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(metrics)
        .build();
    global::set_meter_provider(meter_provider);
    let traces = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let trace_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(traces)
        .build();
    let tracer = trace_provider.tracer("pulpitum-sql-sidecar");
    global::set_tracer_provider(trace_provider);
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Ok(())
}

struct PulpitumPgWireServer {
    sql: Arc<SessionContext>,
    table: Arc<DurableTable>,
    table_name: String,
    capture_query_text: bool,
    listen_address: SocketAddr,
    query_parser: Arc<NoopQueryParser>,
}

impl PulpitumPgWireServer {
    async fn dataframe(&self, statement: &str) -> PgWireResult<datafusion::dataframe::DataFrame> {
        self.sql.sql(statement).await.map_err(|_| {
            tracing::debug!("sidecar SQL planning failed");
            query_error("query could not be planned")
        })
    }

    async fn dataframe_with_parameters(
        &self,
        statement: &str,
        parameters: Vec<ScalarValue>,
    ) -> PgWireResult<datafusion::dataframe::DataFrame> {
        self.dataframe(statement)
            .await?
            .with_param_values(parameters)
            .map_err(|_| {
                tracing::debug!("sidecar SQL parameter binding failed");
                query_error("query parameters could not be bound")
            })
    }

    #[tracing::instrument(
        name = "pulpitum.sql_gateway.request",
        skip(self, statement, format, client_address),
        fields(
            otel.kind = "server",
            server.address = %self.listen_address.ip(),
            server.port = i64::from(self.listen_address.port()),
            client.address = %client_address.ip(),
            client.port = i64::from(client_address.port()),
            network.transport = "tcp",
            network.protocol.name = "postgresql",
            pulpitum.sql.origin = "gateway",
            db.system.name = "postgresql",
            db.collection.name = %self.table_name,
            db.operation.name = gateway_query_shape(statement).operation_name(),
            db.query.summary = tracing::field::display(
                gateway_query_shape(statement).summary(&self.table_name)
            ),
            // This remains redacted even when the explicit query-text capture option is enabled.
            db.query.text = tracing::field::display(
                gateway_query_shape(statement).template(&self.table_name)
            ),
            pulpitum.sql.template = tracing::field::display(telemetry_query_template(
                statement,
                &self.table_name,
                self.capture_query_text,
            )),
        )
    )]
    async fn execute(
        &self,
        statement: &str,
        format: &Format,
        client_address: SocketAddr,
    ) -> PgWireResult<Response> {
        self.execute_with_parameters(statement, format, None).await
    }

    async fn execute_with_parameters(
        &self,
        statement: &str,
        format: &Format,
        portal: Option<&Portal<String>>,
    ) -> PgWireResult<Response> {
        if gateway_statement_kind(statement)? == GatewayStatementKind::Insert {
            let record = match portal {
                Some(portal) => parse_bound_insert(statement, &self.table_name, portal)?,
                None => parse_insert(statement, &self.table_name)?,
            };
            self.table.append(record).await.map_err(|_| {
                tracing::debug!("sidecar SQL insert failed");
                query_error("record could not be appended")
            })?;
            return Ok(Response::Execution(Tag::new("INSERT 0").with_rows(1)));
        }

        let dataframe = match portal {
            Some(portal) => {
                self.dataframe_with_parameters(statement, read_parameter_values(portal, statement)?)
                    .await?
            }
            None => self.dataframe(statement).await?,
        };
        let fields = Arc::new(fields_from_schema(dataframe.schema().as_arrow(), format)?);
        let batches = dataframe.collect().await.map_err(|_| {
            tracing::debug!("sidecar SQL execution failed");
            query_error("query could not be executed")
        })?;
        let rows = encode_rows(&batches, fields.clone())?;
        Ok(Response::Query(QueryResponse::new(
            fields,
            stream::iter(rows),
        )))
    }
}

#[async_trait]
impl ExtendedQueryHandler for PulpitumPgWireServer {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parameter_types =
            parameter_types_for_statement(&portal.statement.statement, &self.table_name)?;
        validate_parameter_count(portal, parameter_types.len())?;
        self.execute_with_parameters(
            &portal.statement.statement,
            &portal.result_column_format,
            Some(portal),
        )
        .await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields =
            if gateway_statement_kind(&statement.statement)? == GatewayStatementKind::Insert {
                parse_insert_values(&statement.statement, &self.table_name)?;
                Vec::new()
            } else {
                let dataframe = self.dataframe(&statement.statement).await?;
                fields_from_schema(dataframe.schema().as_arrow(), &Format::UnifiedText)?
            };
        let parameter_types =
            parameter_types_for_statement(&statement.statement, &self.table_name)?;
        Ok(DescribeStatementResponse::new(parameter_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if gateway_statement_kind(&portal.statement.statement)? == GatewayStatementKind::Insert {
            parse_insert_values(&portal.statement.statement, &self.table_name)?;
            return Ok(DescribePortalResponse::new(Vec::new()));
        }
        let dataframe = self.dataframe(&portal.statement.statement).await?;
        fields_from_schema(dataframe.schema().as_arrow(), &portal.result_column_format)
            .map(DescribePortalResponse::new)
    }
}

#[async_trait]
impl SimpleQueryHandler for PulpitumPgWireServer {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        self.execute(query, &Format::UnifiedText, client.socket_addr())
            .await
            .map(|response| vec![response])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GatewayStatementKind {
    Insert,
    Read,
}

/// The gateway is deliberately not a general-purpose SQL endpoint. Parsing
/// before DataFusion sees a statement makes the no-DDL boundary explicit and
/// stable even if DataFusion later adds support for additional statement types.
fn gateway_statement_kind(statement: &str) -> PgWireResult<GatewayStatementKind> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, statement)
        .map_err(|_| query_error("unsupported SQL statement"))?;
    if statements.len() != 1 {
        return Err(query_error("exactly one SQL statement is required"));
    }
    match statements.pop().expect("statement count was checked") {
        Statement::Insert(_) => Ok(GatewayStatementKind::Insert),
        Statement::Query(_) => Ok(GatewayStatementKind::Read),
        _ => Err(query_error(
            "only read-only queries and supported INSERT statements are allowed",
        )),
    }
}

fn parse_insert_values(
    statement: &str,
    expected_table: &str,
) -> PgWireResult<BTreeMap<String, Expr>> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, statement)
        .map_err(|_| query_error("unsupported INSERT statement"))?;
    if statements.len() != 1 {
        return Err(query_error("exactly one INSERT statement is required"));
    }
    let Statement::Insert(insert) = statements.pop().expect("one statement was checked") else {
        return Err(query_error("unsupported INSERT statement"));
    };
    if insert.table_alias.is_some()
        || insert.overwrite
        || insert.returning.is_some()
        || !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || insert.has_table_keyword
        || insert.replace_into
        || insert.priority.is_some()
        || insert.insert_alias.is_some()
        || insert.settings.is_some()
        || insert.format_clause.is_some()
        || insert.multi_table_insert_type.is_some()
        || !insert.multi_table_into_clauses.is_empty()
        || !insert.multi_table_when_clauses.is_empty()
        || insert.multi_table_else_clause.is_some()
    {
        return Err(query_error("unsupported INSERT statement"));
    }
    let TableObject::TableName(table_name) = insert.table else {
        return Err(query_error("INSERT target must be the messages table"));
    };
    if !table_name.to_string().eq_ignore_ascii_case(expected_table) {
        return Err(query_error("INSERT target must be the messages table"));
    }
    if insert.columns.len() != 4 {
        return Err(query_error("INSERT must provide every messages column"));
    }

    let source = insert
        .source
        .ok_or_else(|| query_error("INSERT must use a single VALUES row"))?;
    if source.with.is_some()
        || source.order_by.is_some()
        || source.limit_clause.is_some()
        || source.fetch.is_some()
        || !source.locks.is_empty()
        || source.for_clause.is_some()
        || source.settings.is_some()
        || source.format_clause.is_some()
        || !source.pipe_operators.is_empty()
    {
        return Err(query_error("INSERT must use a single VALUES row"));
    }
    let SetExpr::Values(values) = *source.body else {
        return Err(query_error("INSERT must use a single VALUES row"));
    };
    if values.rows.len() != 1 {
        return Err(query_error("INSERT must contain exactly one row"));
    }
    let expressions = values
        .rows
        .into_iter()
        .next()
        .expect("one row was checked")
        .content;
    if expressions.len() != insert.columns.len() {
        return Err(query_error("INSERT column and value counts must match"));
    }

    let mut values = BTreeMap::new();
    for (column, expression) in insert.columns.into_iter().zip(expressions) {
        let column = column.to_string().to_ascii_lowercase();
        if values.insert(column, expression).is_some() {
            return Err(query_error("INSERT contains duplicate columns"));
        }
    }
    for column in ["channel_id", "timestamp", "id", "value"] {
        if !values.contains_key(column) {
            return Err(query_error("INSERT must provide every messages column"));
        }
    }
    if values.len() != 4 {
        return Err(query_error("INSERT must provide only messages columns"));
    }
    Ok(values)
}

fn parse_insert(statement: &str, expected_table: &str) -> PgWireResult<Record> {
    let mut values = parse_insert_values(statement, expected_table)?;
    Ok(Record {
        partition_key: PartitionKey::from(string_literal(
            values.remove("channel_id").expect("columns were validated"),
        )?),
        event_time: timestamp_literal(values.remove("timestamp").expect("columns were validated"))?,
        sort_key: SortKey::from(string_literal(
            values.remove("id").expect("columns were validated"),
        )?),
        value: bytea_literal(values.remove("value").expect("columns were validated"))?,
    })
}

fn string_literal(expression: Expr) -> PgWireResult<String> {
    match expression {
        Expr::Value(value) => value
            .into_string()
            .ok_or_else(|| query_error("INSERT values must be string literals")),
        _ => Err(query_error("INSERT values must be string literals")),
    }
}

fn timestamp_literal(expression: Expr) -> PgWireResult<DateTime<Utc>> {
    let Expr::TypedString(timestamp) = expression else {
        return Err(query_error(
            "INSERT timestamp must be a TIMESTAMPTZ literal",
        ));
    };
    if !matches!(
        timestamp.data_type,
        SqlDataType::Timestamp(_, TimezoneInfo::Tz | TimezoneInfo::WithTimeZone)
    ) {
        return Err(query_error(
            "INSERT timestamp must be a TIMESTAMPTZ literal",
        ));
    }
    let value = timestamp
        .value
        .into_string()
        .ok_or_else(|| query_error("INSERT timestamp must be a TIMESTAMPTZ literal"))?;
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| query_error("INSERT timestamp must be an RFC 3339 value"))
}

fn bytea_literal(expression: Expr) -> PgWireResult<Vec<u8>> {
    let value = string_literal(expression)?;
    if let Some(value) = value.strip_prefix("\\x") {
        if !value.len().is_multiple_of(2) {
            return Err(query_error("INSERT bytea hexadecimal value is invalid"));
        }
        return (0..value.len())
            .step_by(2)
            .map(|index| {
                u8::from_str_radix(&value[index..index + 2], 16)
                    .map_err(|_| query_error("INSERT bytea hexadecimal value is invalid"))
            })
            .collect();
    }
    Ok(value.into_bytes())
}

fn parameter_types_for_statement(statement: &str, expected_table: &str) -> PgWireResult<Vec<Type>> {
    match gateway_statement_kind(statement)? {
        GatewayStatementKind::Insert => {
            insert_parameter_types(&parse_insert_values(statement, expected_table)?)
        }
        GatewayStatementKind::Read => read_parameter_types(statement),
    }
}

fn read_parameter_types(statement: &str) -> PgWireResult<Vec<Type>> {
    parameter_types_from_indices(
        &placeholder_indices(statement)?,
        &[Type::TEXT, Type::TIMESTAMPTZ, Type::TIMESTAMPTZ, Type::INT8],
    )
}

fn insert_parameter_types(values: &BTreeMap<String, Expr>) -> PgWireResult<Vec<Type>> {
    let mut types = BTreeMap::new();
    for (column, parameter_type) in [
        ("channel_id", Type::TEXT),
        ("timestamp", Type::TIMESTAMPTZ),
        ("id", Type::TEXT),
        ("value", Type::BYTEA),
    ] {
        let expression = values.get(column).expect("columns were validated");
        if let Some(index) = placeholder_index(expression)?
            && let Some(existing) = types.insert(index, parameter_type.clone())
            && existing != parameter_type
        {
            return Err(query_error(
                "a parameter cannot be used for different column types",
            ));
        }
    }
    let highest = types.keys().next_back().copied().unwrap_or(0);
    (1..=highest)
        .map(|index| {
            types
                .remove(&index)
                .ok_or_else(|| query_error("SQL parameters must be consecutively numbered"))
        })
        .collect()
}

fn parameter_types_from_indices(indices: &[usize], supported: &[Type]) -> PgWireResult<Vec<Type>> {
    let highest = indices.iter().copied().max().unwrap_or(0);
    if highest > supported.len() {
        return Err(query_error(
            "SQL statement contains an unsupported parameter",
        ));
    }
    for index in 1..=highest {
        if !indices.contains(&index) {
            return Err(query_error("SQL parameters must be consecutively numbered"));
        }
    }
    Ok(supported[..highest].to_vec())
}

fn placeholder_indices(statement: &str) -> PgWireResult<Vec<usize>> {
    let bytes = statement.as_bytes();
    let mut indices = Vec::new();
    let mut offset = 0;
    let mut in_string = false;
    while offset < bytes.len() {
        match bytes[offset] {
            b'\'' if in_string && bytes.get(offset + 1) == Some(&b'\'') => offset += 2,
            b'\'' => {
                in_string = !in_string;
                offset += 1;
            }
            b'$' if !in_string => {
                let start = offset + 1;
                let mut end = start;
                while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                    end += 1;
                }
                if end == start {
                    return Err(query_error(
                        "SQL parameters must use positional $n placeholders",
                    ));
                }
                let index = statement[start..end]
                    .parse::<usize>()
                    .ok()
                    .filter(|index| *index > 0)
                    .ok_or_else(|| query_error("SQL parameter indexes must start at $1"))?;
                indices.push(index);
                offset = end;
            }
            _ => offset += 1,
        }
    }
    Ok(indices)
}

fn placeholder_index(expression: &Expr) -> PgWireResult<Option<usize>> {
    let Expr::Value(value) = expression else {
        return Ok(None);
    };
    let SqlValue::Placeholder(value) = &value.value else {
        return Ok(None);
    };
    let index = value
        .strip_prefix('$')
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|index| *index > 0)
        .ok_or_else(|| query_error("SQL parameters must use positional $n placeholders"))?;
    Ok(Some(index))
}

fn validate_parameter_count(portal: &Portal<String>, expected: usize) -> PgWireResult<()> {
    (portal.parameter_len() == expected)
        .then_some(())
        .ok_or_else(|| query_error("SQL bind parameter count does not match the statement"))
}

fn required_parameter<T>(value: Option<T>) -> PgWireResult<T> {
    value.ok_or_else(|| query_error("SQL bind parameters cannot be NULL"))
}

fn read_parameter_values(
    portal: &Portal<String>,
    statement: &str,
) -> PgWireResult<Vec<ScalarValue>> {
    let parameter_types = read_parameter_types(statement)?;
    validate_parameter_count(portal, parameter_types.len())?;
    parameter_types
        .iter()
        .enumerate()
        .map(|(index, parameter_type)| match *parameter_type {
            Type::TEXT => required_parameter(portal.parameter::<String>(index, parameter_type)?)
                .map(|value| ScalarValue::Utf8(Some(value))),
            Type::TIMESTAMPTZ => {
                let value = required_parameter(
                    portal.parameter::<DateTime<FixedOffset>>(index, parameter_type)?,
                )?
                .with_timezone(&Utc);
                let nanos = value.timestamp_nanos_opt().ok_or_else(|| {
                    query_error("timestamp parameter is outside Arrow nanosecond range")
                })?;
                Ok(ScalarValue::TimestampNanosecond(
                    Some(nanos),
                    Some(Arc::<str>::from("UTC")),
                ))
            }
            Type::INT8 => {
                let value = required_parameter(portal.parameter::<i64>(index, parameter_type)?)?;
                Ok(ScalarValue::Int64(Some(value)))
            }
            _ => Err(query_error(
                "SQL statement contains an unsupported parameter",
            )),
        })
        .collect()
}

fn parse_bound_insert(
    statement: &str,
    expected_table: &str,
    portal: &Portal<String>,
) -> PgWireResult<Record> {
    let mut values = parse_insert_values(statement, expected_table)?;
    let parameter_types = insert_parameter_types(&values)?;
    validate_parameter_count(portal, parameter_types.len())?;
    Ok(Record {
        partition_key: PartitionKey::from(string_value(
            values.remove("channel_id").expect("columns were validated"),
            portal,
        )?),
        event_time: timestamp_value(
            values.remove("timestamp").expect("columns were validated"),
            portal,
        )?,
        sort_key: SortKey::from(string_value(
            values.remove("id").expect("columns were validated"),
            portal,
        )?),
        value: bytea_value(
            values.remove("value").expect("columns were validated"),
            portal,
        )?,
    })
}

fn string_value(expression: Expr, portal: &Portal<String>) -> PgWireResult<String> {
    let Some(index) = placeholder_index(&expression)? else {
        return string_literal(expression);
    };
    required_parameter(portal.parameter::<String>(index - 1, &Type::TEXT)?)
}

fn timestamp_value(expression: Expr, portal: &Portal<String>) -> PgWireResult<DateTime<Utc>> {
    let Some(index) = placeholder_index(&expression)? else {
        return timestamp_literal(expression);
    };
    required_parameter(portal.parameter::<DateTime<FixedOffset>>(index - 1, &Type::TIMESTAMPTZ)?)
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn bytea_value(expression: Expr, portal: &Portal<String>) -> PgWireResult<Vec<u8>> {
    let Some(index) = placeholder_index(&expression)? else {
        return bytea_literal(expression);
    };
    required_parameter(portal.parameter::<Vec<u8>>(index - 1, &Type::BYTEA)?)
}

fn fields_from_schema(schema: &Schema, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok(FieldInfo::new(
                field.name().clone(),
                None,
                None,
                pg_type(field.data_type())?,
                format.format_for(index),
            ))
        })
        .collect()
}

fn pg_type(data_type: &DataType) -> PgWireResult<Type> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::TEXT),
        DataType::Binary | DataType::LargeBinary => Ok(Type::BYTEA),
        DataType::Timestamp(_, _) => Ok(Type::TIMESTAMPTZ),
        DataType::Boolean => Ok(Type::BOOL),
        DataType::Int8 | DataType::Int16 => Ok(Type::INT2),
        DataType::Int32 => Ok(Type::INT4),
        DataType::Int64 => Ok(Type::INT8),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(Type::INT8),
        DataType::UInt64 => Ok(Type::NUMERIC),
        DataType::Float16 | DataType::Float32 => Ok(Type::FLOAT4),
        DataType::Float64 => Ok(Type::FLOAT8),
        _ => Err(query_error(
            "query result contains an unsupported data type",
        )),
    }
}

fn encode_rows(
    batches: &[RecordBatch],
    fields: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Vec<PgWireResult<DataRow>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for row_index in 0..batch.num_rows() {
            for (column_index, column) in batch.columns().iter().enumerate() {
                if column.is_null(row_index) {
                    encoder.encode_field(&None::<String>)?;
                    continue;
                }
                match column.data_type() {
                    DataType::Utf8 => {
                        let values = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| query_error("query result could not be encoded"))?;
                        encoder.encode_field(&values.value(row_index))?;
                    }
                    DataType::LargeUtf8 => {
                        let values = column
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| query_error("query result could not be encoded"))?;
                        encoder.encode_field(&values.value(row_index))?;
                    }
                    DataType::Binary => {
                        let values = column
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| query_error("query result could not be encoded"))?;
                        encoder.encode_field(&values.value(row_index))?;
                    }
                    DataType::LargeBinary => {
                        let values = column
                            .as_any()
                            .downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| query_error("query result could not be encoded"))?;
                        encoder.encode_field(&values.value(row_index))?;
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        let values = column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| query_error("query result could not be encoded"))?;
                        let timestamp =
                            DateTime::<Utc>::from_timestamp_nanos(values.value(row_index));
                        encoder.encode_field(&timestamp)?;
                    }
                    _ if fields[column_index].format() == FieldFormat::Text => {
                        let value = array_value_to_string(column.as_ref(), row_index)
                            .map_err(|_| query_error("query result could not be encoded"))?;
                        encoder.encode_field(&value)?;
                    }
                    _ => return Err(query_error("query result could not be encoded")),
                }
            }
            rows.push(Ok(encoder.take_row()));
        }
    }
    Ok(rows)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        GatewayQueryShape, GatewayStatementKind, gateway_statement_kind,
        parameter_types_for_statement, parse_insert, validate_listen_address,
    };
    use pgwire::api::Type;

    const TABLE: &str = "messages";

    #[test]
    fn insecure_listener_requires_loopback_while_secure_listener_allows_non_loopback() {
        let loopback = "127.0.0.1:5433".parse().unwrap();
        let public = "0.0.0.0:5433".parse().unwrap();
        assert!(validate_listen_address(loopback, false).is_ok());
        assert!(validate_listen_address(public, false).is_err());
        assert!(validate_listen_address(public, true).is_ok());
    }

    #[test]
    fn gateway_query_templates_require_an_explicit_literal_capture_opt_in() {
        let count = GatewayQueryShape::from_statement(
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = 'private'",
        );
        assert_eq!(count, GatewayQueryShape::Count);
        assert_eq!(count.operation_name(), "SELECT");
        assert_eq!(count.summary(TABLE), "SELECT COUNT messages");
        assert_eq!(
            count.template(TABLE),
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3"
        );
        let statement =
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = 'general'";
        assert_eq!(
            super::telemetry_query_template(statement, TABLE, false),
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3"
        );
        assert_eq!(
            super::telemetry_query_template(statement, TABLE, true),
            statement
        );

        assert_eq!(
            GatewayQueryShape::from_statement("SELECT timestamp FROM messages"),
            GatewayQueryShape::Select
        );
        assert_eq!(
            GatewayQueryShape::from_statement("INSERT INTO messages VALUES ('private')"),
            GatewayQueryShape::Insert
        );
        let other = GatewayQueryShape::from_statement("UPDATE messages SET value = 'private'");
        assert_eq!(other, GatewayQueryShape::Other);
        assert_eq!(other.operation_name(), "OTHER");
        assert_eq!(other.summary(TABLE), "OTHER messages");
        assert_eq!(
            other.template(TABLE),
            "OTHER SQL statement against messages"
        );
    }

    #[test]
    fn gateway_rejects_ddl_and_multi_statement_requests() {
        assert_eq!(
            gateway_statement_kind("SELECT timestamp FROM messages").unwrap(),
            GatewayStatementKind::Read
        );
        assert_eq!(
            gateway_statement_kind(
                "INSERT INTO messages (channel_id, timestamp, id, value) \
                 VALUES ('general', TIMESTAMPTZ '2026-08-06T12:00:00Z', 'one', 'value')"
            )
            .unwrap(),
            GatewayStatementKind::Insert
        );
        for statement in [
            "CREATE TABLE unexpected (id INT PRIMARY KEY)",
            "ALTER TABLE messages ADD COLUMN unsafe STRING",
            "DROP TABLE messages",
            "GRANT ALL ON DATABASE defaultdb TO public",
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "SELECT 1; DROP TABLE messages",
        ] {
            assert!(gateway_statement_kind(statement).is_err(), "{statement}");
        }
    }

    #[test]
    fn infers_typed_positional_parameters() {
        assert_eq!(
            parameter_types_for_statement(
                "SELECT timestamp, id, value FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC, id ASC LIMIT $4",
                TABLE,
            )
            .expect("read parameter types are inferred"),
            vec![Type::TEXT, Type::TIMESTAMPTZ, Type::TIMESTAMPTZ, Type::INT8]
        );
        assert_eq!(
            parameter_types_for_statement(
                "INSERT INTO messages (value, id, channel_id, timestamp) VALUES ($4, $3, $1, $2)",
                TABLE,
            )
            .expect("insert parameter types are inferred"),
            vec![Type::TEXT, Type::TIMESTAMPTZ, Type::TEXT, Type::BYTEA]
        );
        assert_eq!(
            parameter_types_for_statement(
                "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3",
                TABLE,
            )
            .expect("count parameter types are inferred"),
            vec![Type::TEXT, Type::TIMESTAMPTZ, Type::TIMESTAMPTZ]
        );
        assert!(parameter_types_for_statement(
            "SELECT timestamp FROM messages WHERE channel_id = $1 AND timestamp >= $3 AND timestamp < $4",
            TABLE,
        )
        .is_err());
    }

    #[test]
    fn insert_parser_accepts_a_single_literal_messages_row() {
        let record = parse_insert(
            "INSERT INTO messages (id, value, channel_id, timestamp) \
             VALUES ('message-1', '\\x4869', 'general', TIMESTAMPTZ '2026-08-06T12:00:00Z')",
            TABLE,
        )
        .expect("a valid messages INSERT is accepted");

        assert_eq!(record.partition_key.as_bytes(), b"general");
        assert_eq!(record.partition_key.as_utf8().unwrap(), "general");
        assert_eq!(record.sort_key.as_bytes(), b"message-1");
        assert_eq!(record.sort_key.as_utf8().unwrap(), "message-1");
        assert_eq!(record.value, b"Hi");
        assert_eq!(record.event_time.to_rfc3339(), "2026-08-06T12:00:00+00:00");
    }

    #[test]
    fn insert_parser_rejects_multiple_rows_and_nonliteral_timestamps() {
        assert!(
            parse_insert(
                "INSERT INTO messages (channel_id, timestamp, id, value) \
                 VALUES ('general', TIMESTAMPTZ '2026-08-06T12:00:00Z', 'one', 'first'), \
                        ('general', TIMESTAMPTZ '2026-08-06T12:00:01Z', 'two', 'second')",
                TABLE,
            )
            .is_err()
        );
        assert!(
            parse_insert(
                "INSERT INTO messages (channel_id, timestamp, id, value) \
                 VALUES ('general', NOW(), 'one', 'first')",
                TABLE,
            )
            .is_err()
        );
    }
}

fn query_error(message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        message.to_owned(),
    )))
}

struct PulpitumPgWireFactory {
    handler: Arc<PulpitumPgWireServer>,
    security: SidecarSecurity,
}

impl PgWireServerHandlers for PulpitumPgWireFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(self.security.startup_handler())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_telemetry(&setting(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://127.0.0.1:4317",
    ))?;
    let security = SidecarSecurity::from_environment()?;
    let database_url = setting(
        "COCKROACH_URL",
        "postgresql://pulpitum_runtime@127.0.0.1:26257/defaultdb?sslmode=disable",
    );
    let durable = Arc::new(cockroach_connection::connect(&database_url, pool_config()?).await?);
    let table_name = setting("PULPITUM_SQL_TABLE", "messages");
    let table_id = TableId::new(setting("PULPITUM_SQL_TABLE_ID", "pulpitum.sql.messages"))?;
    let archive_format: ArchiveFormat = setting("ARCHIVE_FORMAT", "json").parse()?;
    let (archive_cache_max_bytes, archive_cache_max_entries) = archive_cache_config()?;
    let capture_query_text = capture_query_text()?;
    let archive = Arc::new(
        archive_connection::connect("PULPITUM_SQL_ARCHIVE_PREFIX", "pulpitum")?
            .with_format(archive_format),
    );
    let archive = Arc::new(ImmutableArchiveCache::new(
        archive,
        archive_cache_max_bytes,
        archive_cache_max_entries,
    ));
    let table = Arc::new(DurableTable::with_definition_and_telemetry(
        TableDefinition::chat_messages(table_name.clone(), table_id),
        durable,
        archive,
        Arc::new(OtelTelemetry::new()),
    )?);
    let sql = Arc::new(SessionContext::new());
    sql.register_table(
        table_name.clone(),
        Arc::new(PulpitumTableProvider::new(table.clone())),
    )?;

    let address = listen_address(&security)?;
    let listener = TcpListener::bind(address).await?;
    let listen_address = listener.local_addr()?;
    tracing::info!(%listen_address, "PostgreSQL SQL sidecar listening");
    let factory = Arc::new(PulpitumPgWireFactory {
        handler: Arc::new(PulpitumPgWireServer {
            sql,
            table,
            table_name,
            capture_query_text,
            listen_address,
            query_parser: Arc::new(NoopQueryParser::new()),
        }),
        security: security.clone(),
    });
    loop {
        let (socket, _) = listener.accept().await?;
        let factory = factory.clone();
        let tls_acceptor = security.tls_acceptor();
        tokio::spawn(async move {
            if process_socket(socket, tls_acceptor, factory).await.is_err() {
                tracing::debug!("SQL client connection ended with an error");
            }
        });
    }
}
