# OpenTelemetry, dashboards, and alerting

Pulpitum does **not** serve a dashboard. A sidecar emits OTLP to a colocated OpenTelemetry Collector; Prometheus scrapes the Collector and Grafana queries Prometheus. This keeps the data plane independent from any central Pulpitum service.

## Rust sidecar instrumentation

Enable the optional metrics implementation:

```toml
pulpitum = { version = "…", features = ["opentelemetry"] }
```

Construct the table and archival coordinator with the same telemetry instance:

```rust
let telemetry = Arc::new(pulpitum::OtelTelemetry::new());
let table = Table::with_definition_and_telemetry(definition, metadata.clone(), hot.clone(), archive.clone(), telemetry.clone())?;
let archiver = ArchiveCoordinator::with_telemetry(metadata, hot, archive, telemetry);
```

## CockroachDB pool

`CockroachHotStore::connect` and `CockroachMetadataStore::connect` each create a bounded pool (16 connections, five-second acquisition timeout by default). When an application uses both adapters, create one `CockroachPool` and pass clones to `from_pool` so their work shares one connection budget and one set of pool metrics:

```rust
use pulpitum::{
    CockroachHotStore, CockroachMetadataStore, CockroachPool, CockroachPoolConfig,
};
use std::time::Duration;

let pool = CockroachPool::connect(
    database_url,
    CockroachPoolConfig {
        max_connections: 16,
        acquire_timeout: Duration::from_secs(5),
    },
).await?;
let hot = CockroachHotStore::from_pool(pool.clone());
let metadata = CockroachMetadataStore::from_pool(pool);
```

The pool checks out a connection only while a single Pulpitum SQL operation is in progress, supplies backpressure when all connections are busy, opens additional connections lazily, and discards connections once their driver ends. Size the pool per process within the CockroachDB cluster's total connection budget; raising it cannot improve a database-bound workload and can make contention worse.

`OtelTelemetry` uses the application’s globally configured OpenTelemetry meter provider. The sidecar executable—not this library—must configure an OTLP metrics exporter to `http://127.0.0.1:4318/v1/metrics` and a tracing subscriber/exporter to `http://127.0.0.1:4318/v1/traces` before creating Pulpitum objects.

`Telemetry` also has default no-op coordinator-cycle and recovery-phase hooks, so existing implementations stay source-compatible. A recovery runner should record one bounded discovery/recovery cycle with `coordinator_cycle_started`, then `coordinator_cycle_completed` or `coordinator_cycle_failed`, emit `coordinator_phase_entered` for each durable job transition, and periodically emit `coordinator_phase_count` for every phase in its job snapshot. The phase vocabulary is finite: `queued`, `claimed`, `uploading`, `uploaded_verified`, `published_cleanup_pending`, `completed`, `retry_scheduled`, and `failed_needs_attention`.

The library creates `tracing` spans for Pulpitum operations and Cockroach queries. Configure `tracing-opentelemetry` in the executable to export those spans. The Node service can use `examples/node-service/observability.mjs`; it must start before web/database imports.

## Semantic conventions and privacy

Cockroach spans use the stable SQL database conventions:

- span kind: `CLIENT` (`otel.kind = "client"`)
- `db.system.name = "cockroachdb"`
- `db.namespace = "defaultdb"` (make this configurable when the adapter accepts a database name)
- `db.collection.name = "pulpitum_records"`
- `db.operation.name`: `CONNECT`, `CREATE`, `INSERT`, `SELECT`, or `DELETE`
- parameter-free `db.query.summary`

The PostgreSQL sidecar emits a `SERVER` span for every inbound wire-protocol request. It uses `pulpitum.sql.origin = "gateway"` together with standard database attributes: `db.system.name = "postgresql"`, `db.collection.name`, `db.operation.name` (`SELECT`, `INSERT`, or `OTHER`), a low-cardinality `db.query.summary`, and fixed parameterized `db.query.text`. The gateway distinguishes routed row reads, `SELECT COUNT(*)`, and inserts; unsupported statements are recorded as `OTHER`. These standard attributes always represent values with `$1`–`$4`, never application data. The query-performance dashboard groups by the parameterized `db.query.text` attribute. With the explicit `PULPITUM_SQL_CAPTURE_QUERY_TEXT=true` opt-in, the sidecar additionally exports the literal inbound statement as `pulpitum.sql.template`; this can expose sensitive values and creates one metric series per distinct statement, so it must not be enabled without explicit privacy and cardinality approval.

The query attributes follow the [OpenTelemetry SQL database semantic conventions](https://opentelemetry.io/docs/specs/semconv/db/sql/). Although this is a gateway `SERVER` span rather than the specification's database-client span, it uses the standard database attribute names to describe its inbound SQL operation. We intentionally do **not** emit query parameter values, partition-key/channel values, bucket IDs/keys or bounds for coordinator jobs, record IDs, values, object keys, lease tokens, or raw storage/database error messages as telemetry attributes. Those frequently contain PII or create unbounded cardinality.

## Metrics

| Metric | Attributes | Use |
|---|---|---|
| `pulpitum.archive.runs` | `pulpitum.bucket.strategy`, `pulpitum.archive.outcome` | started/successful cutovers |
| `pulpitum.archive.failures` | `pulpitum.bucket.strategy`, `pulpitum.archive.stage` | failures in snapshot/upload/delete |
| `pulpitum.archive.records` | `pulpitum.bucket.strategy`, `pulpitum.archive.outcome` | copied records |
| `pulpitum.archive.duration` | `pulpitum.bucket.strategy`, `pulpitum.archive.outcome` | end-to-end cutover duration |
| `pulpitum.query.routes` | `pulpitum.bucket.strategy`, `pulpitum.query.tier` | hot vs archive routing |
| `pulpitum.buckets` | `pulpitum.bucket.tier` | currently known buckets in hot, archiving, and archive tiers |
| `pulpitum.archive.coordinator.cycles` | `pulpitum.archive.coordinator.outcome`: `started`, `success`, `failure` | bounded discovery/recovery-cycle lifecycle |
| `pulpitum.archive.coordinator.duration` | `pulpitum.archive.coordinator.outcome`: `success`, `failure` | discovery/recovery-cycle elapsed time |
| `pulpitum.archive.coordinator.phase.transitions` | `pulpitum.archive.coordinator.phase` | durable archival-job transitions into a finite recovery phase |
| `pulpitum.archive.coordinator.jobs` | `pulpitum.archive.coordinator.phase` | periodic snapshot of jobs in each durable recovery phase |
| `pulpitum_db_pool_connections` | `pulpitum_db_pool_state`: `idle`, `in_use`, `open`, `max`, `waiters` | SQL connection-pool gauges; compare `open` with `max` for capacity and use `waiters` to identify queueing |
| `pulpitum_db_pool_acquires_total` | `pulpitum_db_pool_outcome`: `success`, `timeout`, `connect_error`, `closed` | completed connection-acquire attempts |
| `pulpitum_db_pool_acquire_duration_seconds_bucket` | standard histogram labels | time spent acquiring a connection; use histogram quantiles for latency |
| `pulpitum_db_pool_connections_created_total` | `pulpitum_db_pool_outcome`: `success`, `error` | connection creation attempts |
| `pulpitum_db_pool_connections_closed_total` | none | closed connections; compare its rate with creation to identify churn |

The Prometheus exporter converts dots in names/attributes to underscores and appends standard counter/histogram suffixes. For example, the coordinator metrics appear as `pulpitum_archive_coordinator_cycles_total`, `pulpitum_archive_coordinator_duration_seconds_bucket`, `pulpitum_archive_coordinator_phase_transitions_total`, and `pulpitum_archive_coordinator_jobs`. `pulpitum.buckets` is a per-process gauge of buckets known to that process's in-memory metadata registry; aggregate it only when each bucket is owned by one process. The SQL-pool names above are Prometheus metric names as exposed by the exporter. Do not add `table`, `partition_key`, `channel`, `bucket`, `job`, `lease`, `sort_key`, or `object_key` labels.

The Collector's `spanmetrics` connector turns the Cockroach and gateway span attributes into Prometheus metrics. It retains `pulpitum.sql.origin`, `db.operation.name`, `db.query.summary`, `db.query.text`, `pulpitum.sql.template`, `pulpitum.archive.operation`, and `pulpitum.archive.coordinator.phase`. `pulpitum.sql.template` is dynamic only when `PULPITUM_SQL_CAPTURE_QUERY_TEXT=true`; otherwise it contains a bounded redacted template. The query-performance dashboard filters on `pulpitum_sql_origin="gateway"`, while the archival dashboard uses its archival dimensions.

## Database connection-pool operations

Use `grafana/sql-pool.json` to investigate SQL pool pressure. It links to the overview, query-performance, and archiving dashboards and provides checked-out connection utilization, connection states, waiters, acquire p95/p99, acquire outcomes, connection creation/churn, and CockroachDB statement p95 panels. Its trace-derived pool-phase panel separates permit waiting, idle-queue checkout, and connection opening.

- Sustained high checked-out utilization together with increasing `waiters`, acquire p95/p99, or `POOL_PERMIT` span duration means requests are queueing for connections. Check database/query latency and concurrency first; then tune fixed pool capacity only within CockroachDB and host connection limits.
- A high `POOL_CHECKOUT` duration isolates idle-queue or mutex work, while a high `POOL_CONNECT` duration identifies connection establishment or replacement. These should remain negligible during normal steady-state operation.
- `timeout` acquire outcomes identify pool exhaustion or an acquire timeout that is too short for the observed load. `connect_error` outcomes point to database reachability, TLS/authentication, DNS, or connection-establishment failures rather than queueing alone.
- A rising query p95 with waiters or acquire p95 suggests slow database work is holding connections. Use the query-performance dashboard and traces to identify the database operation before increasing the pool size.
- High connection-created and connection-closed rates, especially with creation `error`, indicate connection churn. Check process restarts, database/network interruptions, connection lifetime/idle policies, and deployment rollouts; avoid masking churn by raising `max`.

## Kubernetes assets

- `otel-collector.yaml`: base Collector configuration.
- `kubernetes/otel-sidecar.yaml`: ConfigMap and strategic-merge sidecar fragment for a Node deployment.
- `kubernetes/prometheus-rule.k8s.yaml`: archive failure, stalled-cutover, and p95-latency alerts for Prometheus Operator.
- `grafana/overview.json`, `grafana/core-metrics.json`, `grafana/query-performance.json`, `grafana/archiving.json`, and `grafana/sql-pool.json`: provisioned Grafana dashboards. They expect a Prometheus datasource. The overview summarizes SQL-pool capacity, workload status, storage tiers, and monitored targets. Core metrics provides SQL throughput, p99 statement latency, connection-pool pressure, SQL error rate, and p99 acquisition latency. The archiving dashboard adds coordinator-cycle rate/duration, jobs by recovery phase, phase-entry rate, and archival trace activity with `$coordinator_phase`, `$coordinator_outcome`, and `$archive_operation` selectors. The dashboards use the `$job` selector, Prometheus datasource UID `prometheus`, and links to the related dashboards.
- `grafana/provisioning/datasources/prometheus.yml`: also provisions a Jaeger datasource. Set `JAEGER_URL` to the Jaeger query endpoint (the showcase uses `http://jaeger:16686`), then use Grafana **Explore** and select **Jaeger** to search and inspect traces.

The included Collector writes traces to `debug` only to remain vendor-neutral. Replace it with a protected Tempo/Jaeger OTLP exporter in the production overlay. Protect collector egress with NetworkPolicies, use TLS for remote collectors, and set `OTEL_SERVICE_NAME`, `service.version`, and deployment environment resource attributes consistently for both processes.
