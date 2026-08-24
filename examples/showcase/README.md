# Pulpitum full-stack showcase

This stack runs a real Pulpitum workload against CockroachDB and MinIO. On startup it:

1. creates five named chat actors across `general`, `engineering`, and `random` channels;
2. backfills eight messages per channel into a prior-year bucket;
3. runs the separate `pulpitum-archiver` coordinator, which discovers and archives those initial prior-year buckets through the fenced `CockroachDurableBucketStore` → MinIO cutover;
4. starts a bounded, spiky SQL load generator against the current hot bucket;
5. starts a separate PostgreSQL-wire SQL sidecar that registers the feature-gated DataFusion table and serves bounded routed queries;
6. serves chat history by connecting to that sidecar using the same single-channel, finite timestamp-range query;
7. alternates writes and short-range reads, and periodically counts a channel's complete history across the prior-year and current buckets, while preserving a usable chat UI;
8. exports Pulpitum metrics and tracing spans with OTLP.

The default load target is **50 Pulpitum operations per second on average**. It repeats a four-second burst shape of **12 → 25 → 113 → 50 RPS**, which averages to 50 RPS without masking queueing behind a flat workload. The generator has 128 workers and a bounded queue of 4,096 requests; when that queue fills, it logs dropped offered requests rather than accumulating unbounded work. Operations alternate between sidecar reads and writes, so the peak batch sends at most 57 operations to the SQL-sidecar connection pool. The showcase config therefore uses fixed, prewarmed 64-connection pools for the workload and SQL sidecar, and 2 connections for the low-throughput archiver. Every 1,000th offered operation is a channel-wide `COUNT(*)` through the SQL sidecar over the finite history range from the prior-year seed through the current bucket, exercising a routed multi-bucket read.

Set `SHOWCASE_LOAD_TARGET_RPS` in `docker-compose.showcase.yml` to change the average target. The burst profile scales proportionally. Open Grafana's **Pulpitum SQL pool** dashboard to correlate those bursts with pool saturation, waiters, acquisition p95, churn, and SQL query latency.

## Multi-AZ network emulation

Each CockroachDB node applies a `tc netem` egress delay before cluster initialization. The default is `SHOWCASE_COCKROACH_AZ_DELAY=1ms` with `SHOWCASE_COCKROACH_AZ_JITTER=200us`, representing a same-region cross-AZ hop. Because the delay is applied by every node, node-to-node traffic sees approximately 2 ms base RTT plus jitter. This affects CockroachDB replication and SQL responses originating from a node, not MinIO or the other showcase services.

Override the values when starting the stack; values use `tc` time units:

```sh
SHOWCASE_COCKROACH_AZ_DELAY=2ms \
SHOWCASE_COCKROACH_AZ_JITTER=500us \
docker compose -f docker-compose.showcase.yml up --build
```

## Inspect archival

The independent `archiver` Compose service owns the archival loop. It discovers the prior-year buckets seeded when the workload starts, then claims eligible buckets, performs the fenced snapshot → generation-addressed MinIO upload → conditional publication → hot-data deletion cutover, and retries/reclaims interrupted work from durable metadata. Restarting the workload does not stop or forget archival work.

`ARCHIVER_INTERVAL_SECONDS` defaults to `60`; set it to a small positive value (for example, `5`) for a quicker local loop. The `archiver` service logs coordinator cycles and recovery outcomes. Inspect the generation-addressed objects in MinIO, use Grafana's **Pulpitum archiving** dashboard for coordinator cycles, job phases, and archival routing, and open Jaeger's `pulpitum.archive.coordinator.cycle` spans for drill-down.

Start it from the repository root:

```sh
docker compose -f docker-compose.showcase.yml up --build
```

Open:

| Service | URL | Credentials |
|---|---|---|
| Chat UI | http://localhost:18080 | none |
| Grafana | http://localhost:13000 | `admin` / `admin` |
| Jaeger | http://localhost:16696 | none |
| Prometheus | http://localhost:19090 | none |
| MinIO Console | http://localhost:19011 | `minioadmin` / `minioadmin` |
| Cockroach SQL | `postgresql://root@localhost:26277/defaultdb?sslmode=disable` | insecure local demo |
| Pulpitum SQL sidecar | `postgresql://pulpitum@localhost:15432/pulpitum?sslmode=disable` | no password; insecure local demo |

Grafana provisions the **Pulpitum overview**, **Pulpitum core metrics**, **Pulpitum SQL pool**, **Pulpitum query performance**, and **Pulpitum archival health** dashboards automatically. In Jaeger, search the `pulpitum-showcase` and `pulpitum-sql-sidecar` services and inspect `pulpitum.db.*`, `pulpitum.archive.*`, and `pulpitum.table.*` spans.

## Query the SQL sidecar

The `sql-sidecar` is a separate process that owns a DataFusion `SessionContext` and exposes its registered `messages` table over the PostgreSQL wire protocol. The chat UI uses this endpoint instead of embedding DataFusion in the workload process.

For example, obtain a channel identifier with `curl http://localhost:18080/api/channels`, then connect with `psql` using the URL in the table above and run a bounded query:

```sql
SELECT timestamp, id, value
FROM messages
WHERE channel_id = '<channel-id>'
  AND timestamp >= TIMESTAMPTZ '2025-01-01T00:00:00Z'
  AND timestamp < TIMESTAMPTZ '2027-01-01T00:00:00Z'
ORDER BY timestamp ASC, id ASC
LIMIT 100;
```

The sidecar implements PostgreSQL simple and extended query flows. Reads accept only the bounded query shape described in [`docs/datafusion.md`](../../docs/datafusion.md); extended queries bind `channel_id`, the timestamp range, and an optional limit before DataFusion plans the routed scan. Writes accept one `INSERT` row with typed `TEXT`, `TIMESTAMPTZ`, `TEXT`, and `BYTEA` values and forward it through `DurableTable::append`, preserving the normal write/archive fence. The showcase UI and load generator use PostgreSQL bound parameters on this endpoint. The listener has no TLS or authentication and is published only for this local demo; do not expose it outside a trusted development environment.

```sql
INSERT INTO messages (channel_id, timestamp, id, value)
VALUES (
  '<channel-id>',
  TIMESTAMPTZ '2026-08-06T12:00:00Z',
  'manual-message-001',
  'Ada: inserted through the SQL sidecar'
);
```

An insert must name exactly `channel_id`, `timestamp`, `id`, and `value`, contain one `VALUES` row, and use string literals plus an RFC 3339 `TIMESTAMPTZ` literal. The timestamp must also be in a bucket that `DurableTable` still permits for writes (normally the current or prior UTC year); historical archived buckets remain immutable. `value` is encoded as UTF-8 by default; a PostgreSQL hex `bytea` string such as `'\\x4869'` is also accepted.

Stop and reset all showcase data:

```sh
docker compose -f docker-compose.showcase.yml down --volumes
```

This is a local, insecure demonstration stack. It uses the durable coupled Cockroach routing path and publishes checksum-verified archive manifests, but it is not a production deployment: durable read leases and multi-worker crash/takeover validation remain pending.
