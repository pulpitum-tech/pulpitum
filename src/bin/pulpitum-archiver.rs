mod archive_connection;
mod cockroach_connection;

use chrono::{DateTime, Datelike, TimeZone, Utc};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use pulpitum::{
    ArchiveFormat, ArchiveRecoveryConfig, CockroachPoolConfig, DurableArchiveRecoveryRunner,
    OtelTelemetry,
};
use std::{env, sync::Arc, time::Duration};
use tracing_subscriber::prelude::*;

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn archival_enabled() -> Result<(), Box<dyn std::error::Error>> {
    if setting("PULPITUM_ARCHIVAL_ENABLED", "false").parse::<bool>()? {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "archival is disabled by default because it irreversibly deletes hot data; set PULPITUM_ARCHIVAL_ENABLED=true only after completing the archival fault-test acceptance criteria",
    )
    .into())
}

fn pool_config() -> Result<CockroachPoolConfig, Box<dyn std::error::Error>> {
    let mut config = CockroachPoolConfig::default();
    if let Ok(value) = env::var("COCKROACH_POOL_MAX_CONNECTIONS") {
        config.max_connections = value.parse()?;
    }
    if config.max_connections < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "COCKROACH_POOL_MAX_CONNECTIONS must be at least 2 so lease renewal cannot be blocked by archival work",
        )
        .into());
    }
    Ok(config)
}

fn seconds(name: &str, default: u64) -> Result<Duration, Box<dyn std::error::Error>> {
    let value = match env::var(name) {
        Ok(value) => value.parse::<u64>()?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} must be greater than zero"),
        )
        .into());
    }
    Ok(Duration::from_secs(value))
}

fn eligible_before() -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    if let Ok(value) = env::var("ARCHIVER_ELIGIBLE_BEFORE") {
        return Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc));
    }

    Ok(Utc
        .with_ymd_and_hms(Utc::now().year() - 1, 1, 1, 0, 0, 0)
        .single()
        .expect("January 1 is always a valid UTC timestamp"))
}

fn install_telemetry(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resource = Resource::builder_empty()
        .with_service_name("pulpitum-archiver")
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
    let tracer = trace_provider.tracer("pulpitum-archiver");
    global::set_tracer_provider(trace_provider);
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    archival_enabled()?;
    install_telemetry(&setting(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://127.0.0.1:4317",
    ))?;

    let database_url = setting(
        "COCKROACH_URL",
        "postgresql://pulpitum_runtime@127.0.0.1:26257/defaultdb?sslmode=disable",
    );
    let store = Arc::new(cockroach_connection::connect(&database_url, pool_config()?).await?);
    let archive_format: ArchiveFormat = setting("ARCHIVE_FORMAT", "json").parse()?;
    let archive = Arc::new(
        archive_connection::connect("ARCHIVER_PREFIX", "showcase")?.with_format(archive_format),
    );
    let interval = seconds("ARCHIVER_INTERVAL_SECONDS", 15)?;
    let lease_for = seconds("ARCHIVER_LEASE_SECONDS", 60)?;
    let renewal_default = (lease_for.as_secs() / 3).max(1);
    let lease_renewal_interval = seconds("ARCHIVER_LEASE_RENEWAL_SECONDS", renewal_default)?;
    let eligible_before = eligible_before()?;
    let runner = DurableArchiveRecoveryRunner::with_telemetry(
        store,
        archive,
        ArchiveRecoveryConfig {
            eligible_before,
            scan_limit: 64,
            lease_for,
            lease_renewal_interval,
            retry_backoff: seconds("ARCHIVER_RETRY_SECONDS", 15)?,
        },
        Arc::new(OtelTelemetry::new()),
    )?;

    tracing::info!(
        eligible_before = %eligible_before,
        interval_seconds = interval.as_secs(),
        "durable archival coordinator started"
    );
    loop {
        match runner.run_once().await {
            Ok(outcome) => tracing::info!(?outcome, "durable archival coordinator cycle completed"),
            Err(error) => tracing::warn!(?error, "durable archival coordinator cycle failed"),
        }
        tokio::time::sleep(interval).await;
    }
}
