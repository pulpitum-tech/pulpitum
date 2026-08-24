use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::get,
};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use pulpitum::{
    ArchiveFormat, CockroachDurableBucketStore, CockroachPoolConfig, DurableTable,
    OpenDalArchiveStore, OtelTelemetry, Record, SpikySqlLoadProfile, TableDefinition, TableId,
};
use serde::Serialize;
use std::{env, sync::Arc, time::Duration as StdDuration};
use tokio::{
    sync::{Mutex, mpsc},
    time::MissedTickBehavior,
};
use tokio_postgres::{Client, NoTls};
use tracing_subscriber::prelude::*;

const ACTORS: &[&str] = &["Ada", "Bea", "Cy", "Dee", "Eli"];
const CHANNELS: &[&str] = &["general", "engineering", "random"];
const CHANNEL_MESSAGE_COUNT_INTERVAL: u64 = 1_000;
const SELECT_MESSAGES_SQL: &str = "SELECT timestamp, id, value FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC, id ASC LIMIT $4";
const SELECT_MESSAGE_COUNT_SQL: &str = "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3";
const INSERT_MESSAGE_SQL: &str =
    "INSERT INTO messages (channel_id, timestamp, id, value) VALUES ($1, $2, $3, $4)";

const APP_HTML: &str = include_str!("showcase/app.html");

#[derive(Clone)]
struct UiState {
    sql_url: String,
    channels: Arc<Vec<String>>,
    history_start: DateTime<Utc>,
}

#[derive(Serialize)]
struct ChannelResponse {
    id: String,
    name: String,
}

#[derive(Serialize)]
struct MessageResponse {
    id: String,
    actor: String,
    text: String,
    timestamp: DateTime<Utc>,
}

async fn serve_ui(state: UiState) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/", get(|| async { Html(APP_HTML) }))
        .route("/api/channels", get(list_channels))
        .route("/api/channels/{channel}/messages", get(list_messages))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("chat UI available at http://localhost:18080");
    axum::serve(listener, app).await
}

async fn list_channels(State(state): State<UiState>) -> Json<Vec<ChannelResponse>> {
    Json(
        state
            .channels
            .iter()
            .map(|id| ChannelResponse {
                id: id.clone(),
                name: id.rsplit('-').next().unwrap_or(id).to_owned(),
            })
            .collect(),
    )
}

async fn list_messages(
    State(state): State<UiState>,
    Path(channel): Path<String>,
) -> Result<Json<Vec<MessageResponse>>, (StatusCode, String)> {
    let known_channel = state.channels.contains(&channel);
    if !known_channel {
        return Err((StatusCode::NOT_FOUND, "unknown channel".to_owned()));
    }
    let end = Utc::now() + Duration::days(1);
    let limit = 100_i64;
    let (client, connection) = tokio_postgres::connect(&state.sql_url, NoTls)
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "SQL sidecar is unavailable".to_owned(),
            )
        })?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::debug!("SQL sidecar connection ended with an error");
        }
    });
    let rows = client
        .query(
            SELECT_MESSAGES_SQL,
            &[&channel, &state.history_start, &end, &limit],
        )
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "message query failed".to_owned(),
            )
        })?;
    let mut messages = Vec::new();
    for row in rows {
        let timestamp = row.try_get(0).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected message schema".to_owned(),
            )
        })?;
        let id: String = row.try_get(1).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected message schema".to_owned(),
            )
        })?;
        let value: Vec<u8> = row.try_get(2).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected message schema".to_owned(),
            )
        })?;
        let text = String::from_utf8_lossy(&value).into_owned();
        let (actor, text) = match text.split_once(": ") {
            Some((actor, body)) => (actor.to_owned(), body.to_owned()),
            None => ("Pulpitum".to_owned(), text),
        };
        messages.push(MessageResponse {
            id,
            actor,
            text,
            timestamp,
        });
    }
    Ok(Json(messages))
}

struct LoadRequest {
    sequence: u64,
    channel_index: usize,
    operation: LoadOperation,
}

#[derive(Clone, Copy, Debug)]
enum LoadOperation {
    Append,
    RecentRead,
    CountChannelMessages,
}

fn load_operation(profile: SpikySqlLoadProfile, sequence: u64) -> LoadOperation {
    if !profile.is_query(sequence) {
        return LoadOperation::Append;
    }
    if sequence.is_multiple_of(CHANNEL_MESSAGE_COUNT_INTERVAL) {
        LoadOperation::CountChannelMessages
    } else {
        LoadOperation::RecentRead
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INSERT_MESSAGE_SQL, LoadOperation, SELECT_MESSAGE_COUNT_SQL, SELECT_MESSAGES_SQL,
        SpikySqlLoadProfile, load_operation,
    };

    #[test]
    fn channel_message_counts_are_infrequent_routed_reads() {
        let profile = SpikySqlLoadProfile::default();
        assert!(matches!(
            load_operation(profile, 0),
            LoadOperation::CountChannelMessages
        ));
        assert!(matches!(
            load_operation(profile, 2),
            LoadOperation::RecentRead
        ));
        assert!(matches!(load_operation(profile, 1), LoadOperation::Append));
        assert!(matches!(
            load_operation(profile, 1_000),
            LoadOperation::CountChannelMessages
        ));
    }

    #[test]
    fn uses_positional_parameters_for_every_sidecar_operation() {
        assert_eq!(
            SELECT_MESSAGES_SQL,
            "SELECT timestamp, id, value FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC, id ASC LIMIT $4"
        );
        assert_eq!(
            SELECT_MESSAGE_COUNT_SQL,
            "SELECT COUNT(*) AS message_count FROM messages WHERE channel_id = $1 AND timestamp >= $2 AND timestamp < $3"
        );
        assert_eq!(
            INSERT_MESSAGE_SQL,
            "INSERT INTO messages (channel_id, timestamp, id, value) VALUES ($1, $2, $3, $4)"
        );
    }
}

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
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

fn load_target_rps() -> usize {
    env::var("SHOWCASE_LOAD_TARGET_RPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(SpikySqlLoadProfile::DEFAULT_TARGET_RPS)
}

fn prior_year_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(Utc::now().year() - 1, 6, 1, 12, 0, 0)
        .single()
        .expect("a fixed UTC timestamp is valid")
}

fn install_telemetry(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resource = Resource::builder_empty()
        .with_service_name("pulpitum-showcase")
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
    let tracer = trace_provider.tracer("pulpitum-showcase");
    global::set_tracer_provider(trace_provider);
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Ok(())
}

async fn run_load_worker(
    channels: Arc<Vec<String>>,
    receiver: Arc<Mutex<mpsc::Receiver<LoadRequest>>>,
    sql_url: String,
    history_start: DateTime<Utc>,
) {
    let mut sql_client = None;
    loop {
        let request = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };
        let Some(request) = request else {
            return;
        };
        let channel = &channels[request.channel_index];
        let succeeded = match request.operation {
            LoadOperation::RecentRead => {
                let now = Utc::now();
                query_via_sql_sidecar(
                    &mut sql_client,
                    &sql_url,
                    channel,
                    now - Duration::minutes(5),
                    now + Duration::seconds(1),
                    25,
                )
                .await
                .is_some()
            }
            LoadOperation::CountChannelMessages => {
                let end = Utc::now() + Duration::seconds(1);
                match count_messages_via_sql_sidecar(
                    &mut sql_client,
                    &sql_url,
                    channel,
                    history_start,
                    end,
                )
                .await
                {
                    Some(messages) => {
                        tracing::info!(
                            channel,
                            messages,
                            routed_buckets = end.year() - history_start.year() + 1,
                            "counted all channel messages across history buckets"
                        );
                        true
                    }
                    None => false,
                }
            }
            LoadOperation::Append => {
                let actor = ACTORS[request.sequence as usize % ACTORS.len()];
                append_via_sql_sidecar(
                    &mut sql_client,
                    &sql_url,
                    channel,
                    Utc::now(),
                    &format!("load-{actor}-{sequence:020}", sequence = request.sequence),
                    &format!("{actor}: load simulation message {}", request.sequence),
                )
                .await
            }
        };
        if !succeeded {
            tracing::debug!(operation = ?request.operation, "showcase load request failed");
        }
    }
}

async fn append_via_sql_sidecar(
    client: &mut Option<Client>,
    sql_url: &str,
    channel: &str,
    timestamp: DateTime<Utc>,
    id: &str,
    value: &str,
) -> bool {
    let Some(client) = sql_client(client, sql_url).await else {
        return false;
    };
    client
        .execute(
            INSERT_MESSAGE_SQL,
            &[&channel, &timestamp, &id, &value.as_bytes()],
        )
        .await
        .map(|rows| rows == 1)
        .unwrap_or(false)
}

async fn query_via_sql_sidecar(
    client: &mut Option<Client>,
    sql_url: &str,
    channel: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: i64,
) -> Option<usize> {
    sql_client(client, sql_url)
        .await?
        .query(SELECT_MESSAGES_SQL, &[&channel, &start, &end, &limit])
        .await
        .ok()
        .map(|rows| rows.len())
}

async fn count_messages_via_sql_sidecar(
    client: &mut Option<Client>,
    sql_url: &str,
    channel: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Option<u64> {
    let row = sql_client(client, sql_url)
        .await?
        .query_one(SELECT_MESSAGE_COUNT_SQL, &[&channel, &start, &end])
        .await
        .ok()?;
    row.try_get::<_, i64>(0)
        .ok()
        .and_then(|count| count.try_into().ok())
}

async fn sql_client<'a>(client: &'a mut Option<Client>, sql_url: &str) -> Option<&'a Client> {
    if client.is_none() {
        let Ok((new_client, connection)) = tokio_postgres::connect(sql_url, NoTls).await else {
            return None;
        };
        tokio::spawn(async move {
            if connection.await.is_err() {
                tracing::debug!("SQL sidecar connection ended with an error");
            }
        });
        *client = Some(new_client);
    }
    client.as_ref()
}

async fn run_load_simulation(
    channels: Arc<Vec<String>>,
    sql_url: String,
    history_start: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let profile = SpikySqlLoadProfile::new(load_target_rps());
    let (sender, receiver) = mpsc::channel(SpikySqlLoadProfile::QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    for _ in 0..SpikySqlLoadProfile::WORKERS {
        tokio::spawn(run_load_worker(
            channels.clone(),
            receiver.clone(),
            sql_url.clone(),
            history_start,
        ));
    }

    tracing::info!(
        target_rps = profile.target_rps(),
        workers = SpikySqlLoadProfile::WORKERS,
        queue_capacity = SpikySqlLoadProfile::QUEUE_CAPACITY,
        shape_per_mille = ?SpikySqlLoadProfile::SHAPE_PERMILLE,
        "starting spiky SQL pool load simulation"
    );
    let mut ticker = tokio::time::interval(StdDuration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut second = 0_usize;
    let mut sequence = 0_u64;
    loop {
        ticker.tick().await;
        let offered = profile.operations_for_second(second);
        let mut enqueued = 0;
        for _ in 0..offered {
            let request = LoadRequest {
                sequence,
                channel_index: sequence as usize % channels.len(),
                operation: load_operation(profile, sequence),
            };
            sequence = sequence.wrapping_add(1);
            match sender.try_send(request) {
                Ok(()) => enqueued += 1,
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
            }
        }
        let dropped = offered - enqueued;
        if dropped > 0 {
            tracing::warn!(
                offered,
                enqueued,
                dropped,
                "load queue saturated; requests dropped"
            );
        } else if second.is_multiple_of(SpikySqlLoadProfile::SHAPE_PERMILLE.len()) {
            tracing::info!(
                offered,
                target_rps = profile.target_rps(),
                "load simulation interval enqueued"
            );
        }
        second = second.wrapping_add(1);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_telemetry(&setting(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector:4317",
    ))?;
    let durable = Arc::new(
        CockroachDurableBucketStore::connect_insecure_dev_with_pool_config(
            &setting(
                "COCKROACH_URL",
                "postgresql://pulpitum_runtime@cockroach-1:26257/defaultdb?sslmode=disable",
            ),
            pool_config()?,
        )
        .await?,
    );
    let archive_format: ArchiveFormat = setting("ARCHIVE_FORMAT", "json").parse()?;
    let archive = Arc::new(
        OpenDalArchiveStore::s3(
            &setting("S3_ENDPOINT", "http://minio:9000"),
            "pulpitum",
            &setting("S3_ACCESS_KEY", "minioadmin"),
            &setting("S3_SECRET_KEY", "minioadmin"),
            "showcase",
        )?
        .with_format(archive_format),
    );
    let telemetry = Arc::new(OtelTelemetry::new());
    let table = Arc::new(DurableTable::with_definition_and_telemetry(
        TableDefinition::chat_messages(
            "messages",
            TableId::new(setting("SHOWCASE_TABLE_ID", "pulpitum.showcase.messages"))?,
        ),
        durable.clone(),
        archive.clone(),
        telemetry.clone(),
    )?);
    let run = Utc::now().timestamp();
    let channels: Vec<String> = CHANNELS
        .iter()
        .map(|channel| format!("showcase-{run}-{channel}"))
        .collect();
    let old_timestamp = prior_year_timestamp();
    let history_start = old_timestamp - Duration::days(1);

    for channel in &channels {
        for sequence in 0..8 {
            table
                .append(Record {
                    partition_key: channel.clone().into(),
                    event_time: old_timestamp,
                    sort_key: format!("seed-{sequence:03}").into(),
                    value: format!("Ada: archived conversation {sequence} in #{channel}")
                        .into_bytes(),
                })
                .await?;
        }
    }
    tracing::info!(
        actors = ACTORS.len(),
        channels = channels.len(),
        "staged prior-year chat histories; the archival coordinator will recover and archive them"
    );

    let sql_url = setting(
        "SHOWCASE_SQL_URL",
        "postgresql://pulpitum@sql-sidecar:5433/pulpitum?sslmode=disable",
    );
    let channels = Arc::new(channels);
    tokio::spawn(serve_ui(UiState {
        sql_url: sql_url.clone(),
        channels: channels.clone(),
        history_start,
    }));

    run_load_simulation(channels, sql_url, history_start).await
}
