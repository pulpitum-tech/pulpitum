# Pulpitum

[Documentation](https://www.pulpitum.tech/) · [Source](https://github.com/pulpitum-tech/pulpitum) · [Security](SECURITY.md)

A Rust foundation for an append-oriented logical table with a Cassandra-style v4 core model:

```text
((table_id, partition_key, bucket_key), (event_time, sort_key))
```

The physical partition key is `(table_id, partition_key, bucket_key)`, and records cluster by `(event_time ASC, sort_key ASC)`. `PartitionKey` and `SortKey` are opaque byte strings. `event_time` is the mandatory leading clustering component because bucket routing is time-based and pagination across buckets must remain chronological.

`bucket_key` is an opaque canonical identifier derived from the configured UTC calendar strategy. The built-in strategies are `CalendarYearUtc` (the default), `CalendarMonthUtc`, and `CalendarDayUtc`, which produce `year:YYYY`, `month:YYYY-MM`, and `day:YYYY-MM-DD` respectively. Do not infer chronology from lexical bucket-key order; use the persisted `[bucket_start, bucket_end)` descriptor. For the built-in chat workload, logical `channel_id` maps to `partition_key`, `timestamp` maps to `event_time`, and `id` maps to `sort_key`; the default bucket strategy is yearly.
Recent data stays in a database; immutable older buckets are served from an object store. Use `DurableTable` as the read/write entry point with a `DurableBucketStore`, so callers do not need to know which tier owns a bucket and every hot/archive decision is fenced durably.

## Archival protocol

`DurableArchiveCoordinator::archive_bucket` performs this fenced order:

1. Begin a durable archive session, atomically changing `Hot` to `Archiving` and rejecting writes.
2. Take a snapshot that is conditioned on that session's owner token and fencing generation.
3. Upload the snapshot as JSON or Parquet, read it back to verify its checksum and row count, then write a versioned manifest.
4. Conditionally publish the manifest key as `Archived { object_key }` with the same session. New reads now use the archive.
5. Atomically delete hot records and mark `hot_deleted: true`, again only for that session.

The object pointer publication is the commit point. Snapshot or upload failure aborts the active session and returns the bucket to `Hot`. Failures after upload do not abort: publication or cleanup may already have committed, and recovery must remain session-fenced.

For deployment-owned archival, run the `pulpitum-archiver` binary with `PULPITUM_ARCHIVAL_ENABLED=true`. Archival is disabled by default because it eventually deletes hot data; leave it disabled until the environment has completed the archival fault-test acceptance criteria. It uses `DurableArchiveRecoveryRunner` to discover eligible buckets, claim expiring leases, retry pre-publication failures, and resume `Archived { hot_deleted: false }` cleanup after a prior worker disappears. It archives buckets whose persisted `bucket_end` is at or before `ARCHIVER_ELIGIBLE_BEFORE`, an RFC 3339 timestamp. The default cutoff is January 1 of the previous UTC year. See [`docs/hot-cold-archival.md`](docs/hot-cold-archival.md) for the lifecycle, cutover point, schema limits, PeerDB comparison, and prioritized next steps.

## Boundaries

- `DurableBucketStore`: `CockroachDurableBucketStore` couples mutable records with bucket state in serializable CockroachDB transactions for writes and archival fencing. Each uncached bounded read selects route metadata and hot rows in one CockroachDB statement/MVCC snapshot. Hot and archiving observations are never cached, so they cannot select a stale tier after remote cleanup. Published archive routes point to immutable manifests and retain a process-local fast path bounded to 4,096 entries. A read racing publication returns either the pre-publication hot page or the archive route; separate bucket reads still use independent snapshots. `DurableTable` and `DurableArchiveCoordinator` use it for fenced routing and cutover.
- `ArchiveStore`: `OpenDalArchiveStore` is included for S3-compatible endpoints (AWS S3 and MinIO) via [Apache OpenDAL](https://github.com/apache/opendal). OpenDAL provides transport and signing; Pulpitum owns archive encoding, metadata, and cutover semantics.

## Full-stack showcase

Run [`examples/showcase/README.md`](examples/showcase/README.md) to start a CockroachDB, MinIO, OpenTelemetry Collector, Prometheus, Grafana, and Jaeger workload. It backfills and archives older chat buckets for the UI, then applies a bounded spiky load to the current hot bucket so the SQL-pool dashboards show realistic saturation and queueing behavior.

## PostgreSQL SQL sidecar

`pulpitum-sql-sidecar` is a reusable PostgreSQL-wire server for the built-in chat-record schema. It registers a `PulpitumTableProvider` for bounded reads and maps its narrow insert form directly to `DurableTable::append`, preserving the durable write/archive fence rather than writing to CockroachDB directly.

Provision the schema with a privileged, short-lived deployment job, then run the sidecar with a separate runtime database role:

```sh
COCKROACH_CA_CERT_PATH='/var/run/secrets/cockroach/ca.crt' \
COCKROACH_MIGRATION_URL='postgresql://migration-role@db.example/defaultdb?sslmode=require' \
  cargo run --bin pulpitum-migrate

COCKROACH_CA_CERT_PATH='/var/run/secrets/cockroach/ca.crt' \
COCKROACH_URL='postgresql://pulpitum_runtime@db.example/defaultdb?sslmode=require' \
  cargo run --bin pulpitum-sql-sidecar --features sql-sidecar
```

`pulpitum-migrate` applies the numbered, append-only schema history in `pulpitum_schema_migrations`, verifies the recorded migration name/checksum and required v4 table columns/primary keys, then grants `pulpitum_runtime` only `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on Pulpitum tables. It revokes database/schema `CREATE` from the runtime and `public` roles. Migration history is written only by the privileged migration job; runtime roles receive no history-table privileges. Do not run a long-lived runtime service as `root` or the migration role.

The current baseline is migration **v4** for `pulpitum_v4_bucket_metadata` and `pulpitum_v4_records`, plus archive manifest v4/record schema v2. Startup validation rejects changed migration identities, future versions, and non-contiguous history rather than silently accepting drift. Migration intentionally leaves existing v3 tables and archive objects untouched; it does not reinterpret or copy them automatically. An authoritative v3 key/backfill schema is not present in this repository, so a safe automatic v3-to-v4 backfill is deliberately unsupported. An existing deployment must use a new `TableId` and archive namespace or perform an explicit, verified v3-to-v4 data migration before switching readers.

It listens on `127.0.0.1:5433` by default. Without a complete sidecar TLS/SCRAM file configuration it remains an intentionally unauthenticated loopback-only development endpoint. Configure the backing durable table and listener with:

| Variable | Default | Purpose |
|---|---|---|
| `COCKROACH_URL` | `postgresql://pulpitum_runtime@127.0.0.1:26257/defaultdb?sslmode=disable` | Durable CockroachDB store. Production URLs must use `sslmode=require`; `sslmode=prefer` is rejected to prevent plaintext downgrade. |
| `COCKROACH_CA_CERT_PATH` | none | PEM CA bundle required with `sslmode=require`; rustls validates the server chain and hostname. |
| `COCKROACH_CLIENT_CERT_PATH` / `COCKROACH_CLIENT_KEY_PATH` | none | Optional PEM client certificate and private key pair for CockroachDB mTLS. Configure both or neither. |
| `S3_ENDPOINT` | `http://127.0.0.1:9000` | S3-compatible archive endpoint. Production endpoints must use HTTPS. |
| `S3_BUCKET` | `pulpitum` | Archive bucket. |
| `S3_REGION` | provider/config chain | Signing region. |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` only for the default local endpoint | Optional static credentials. Omit both in production to use OpenDAL's environment/shared-config/web-identity/instance-metadata chain. |
| `S3_SESSION_TOKEN` | none | Optional temporary token paired with explicit static credentials. |
| `S3_ALLOW_HTTP` | local loopback only | Explicit development override for non-loopback plaintext endpoints such as Compose MinIO. |
| `S3_SERVER_SIDE_ENCRYPTION` | `none` | `none`, `s3`, or `kms`; set `S3_KMS_KEY_ID` for a customer-managed KMS key. |
| `PULPITUM_SQL_ARCHIVE_PREFIX` | `pulpitum` | Archive prefix for this logical table. |
| `PULPITUM_SQL_ARCHIVE_CACHE_MAX_BYTES` | `268435456` | Maximum estimated bytes retained for successful immutable archive reads (256 MiB). Set to `0` to disable the cache. |
| `PULPITUM_SQL_ARCHIVE_CACHE_MAX_ENTRIES` | `512` | Maximum immutable archive objects retained. Set to `0` to disable the cache. |
| `ARCHIVE_FORMAT` | `json` | Encoding for newly written archive payloads: `json` or `parquet`. Reads select the codec from each published manifest. |
| `PULPITUM_SQL_TABLE` | `messages` | Registered logical table name. |
| `PULPITUM_SQL_TABLE_ID` | `pulpitum.sql.messages` | Stable physical namespace. Do not change it when renaming the logical table or changing its configured bucket strategy. |
| `PULPITUM_SQL_LISTEN_ADDR` | `127.0.0.1:5433` | PostgreSQL-wire listener address. Insecure mode is strictly loopback-only; secure mode may listen on a non-loopback address. |
| `PULPITUM_SQL_TLS_CERT_PATH` | none | PEM server certificate or chain. Required together with `PULPITUM_SQL_TLS_KEY_PATH` and `PULPITUM_SQL_PASSWORD_FILE` to enable secure mode. The certificate also enables `SCRAM-SHA-256-PLUS` channel binding. |
| `PULPITUM_SQL_TLS_KEY_PATH` | none | PEM private key for the server certificate. Required with the certificate and password-file settings. Mount read-only. |
| `PULPITUM_SQL_PASSWORD_FILE` | none | Read-only file containing exactly one non-empty UTF-8 password line (a trailing newline is allowed). Required with the TLS certificate and key. No password environment variable is supported. |
| `PULPITUM_SQL_USER` | `pulpitum` | The single PostgreSQL user accepted in secure mode. |
| `PULPITUM_SQL_DATABASE` | `pulpitum` | The single PostgreSQL database accepted in secure mode. |
| `PULPITUM_SQL_CAPTURE_QUERY_TEXT` | `false` | Export each literal SQL statement as the `pulpitum.sql.template` trace/metric attribute. Enabling this can expose query values and creates one Prometheus series per distinct statement; use only with explicit privacy and cardinality approval. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://127.0.0.1:4317` | OTLP metrics and trace exporter. |

Cockroach certificates are loaded when the pool starts. Rotate them with overlapping certificate validity and a rolling process restart; private keys should be mounted read-only with workload-specific permissions.

### Sidecar TLS and SCRAM deployment

For production, mount a server certificate, its private key, and a password secret as files, then configure all three file paths. The configuration is all-or-nothing: setting any one or two of the TLS certificate, key, and password-file variables makes startup fail. The password is read at startup, converted into a salted SCRAM credential in memory, and is never accepted from an environment variable.

```sh
PULPITUM_SQL_LISTEN_ADDR='0.0.0.0:5433' \
PULPITUM_SQL_TLS_CERT_PATH='/var/run/secrets/sql-sidecar/tls.crt' \
PULPITUM_SQL_TLS_KEY_PATH='/var/run/secrets/sql-sidecar/tls.key' \
PULPITUM_SQL_PASSWORD_FILE='/var/run/secrets/sql-sidecar/password' \
PULPITUM_SQL_USER='pulpitum' \
PULPITUM_SQL_DATABASE='pulpitum' \
  cargo run --bin pulpitum-sql-sidecar --features sql-sidecar
```

Clients must validate the server certificate and should require channel binding, for example: `postgresql://pulpitum@sql.example:5433/pulpitum?sslmode=verify-full&sslrootcert=/path/to/ca.crt&channel_binding=require`. The server advertises `SCRAM-SHA-256-PLUS` when TLS is configured. Unknown users, databases, and wrong passwords follow the same generic PostgreSQL password-authentication failure path.

For local development only, omit all three secure-mode settings and keep the default loopback listener. There is no insecure non-loopback override.

The sidecar caches only successful reads addressed by an immutable published archive key. Durable archival publishes generation-addressed manifest keys, so an entry remains valid until it is evicted for the configured byte or entry limit. Failed reads, including missing S3 objects, are never cached.

The table keeps the logical SQL columns `channel_id`, `timestamp`, `id`, and `value`. The adapter maps them to the core fields `partition_key`, `event_time`, `sort_key`, and `value`; SQL predicates remain one `channel_id` equality plus a finite `timestamp` range. Writes support exactly one literal row:

```sql
INSERT INTO messages (channel_id, timestamp, id, value)
VALUES (
  'general',
  TIMESTAMPTZ '2026-08-06T12:00:00Z',
  'message-001',
  'Ada: appended through PostgreSQL wire protocol'
);
```

The insert must name all four columns, contain one `VALUES` row, and use string literals plus an RFC 3339 `TIMESTAMPTZ` literal. A `value` prefixed with `\\x` is decoded as hexadecimal `bytea`; other values are stored as UTF-8 bytes. The timestamp must be in `TableDefinition`'s writable window, so an archived historical bucket remains immutable. The gateway parses and rejects DDL, grants/revokes, transactions, multi-statement requests, and every non-query/non-supported-insert statement before it reaches DataFusion.

The server supports PostgreSQL simple and extended query flows. Secure mode requires TLS plus SCRAM authentication; it advertises channel binding from the configured server certificate. Insecure mode remains loopback-only for local development. Connection limits, cancellation, parameter-aware routing, batching/transaction semantics, and idempotency remain deployment considerations. See [`docs/datafusion.md`](docs/datafusion.md) for the exact routed read and sidecar-write contracts.

## Observability

The sidecar exposes metrics and traces through OpenTelemetry rather than serving a dashboard. See [`observability/README.md`](observability/README.md) for the Collector, Kubernetes sidecar patch, Grafana dashboard, Prometheus alerts, Node.js bootstrap, privacy boundaries, and CockroachDB semantic-convention mapping.

## Developer experience

Set up a `DurableTable` with `TableDefinition`; its key fields are `partition_key`, `bucket_time_key`, and `clustering_key: Vec<ClusteringColumn>`, in addition to the stable `TableId` and bucket strategy. The built-in chat definition requires `bucket_time_key = "event_time"` and clustering columns `(event_time ASC, sort_key ASC)`. A strategy is immutable for a `TableId`: changing strategy requires a new `TableId` and data migration. `DurableTable::query_page(Query { ... })` returns a cursor `(event_time, sort_key)`, so consumers page chronologically across hot and archived buckets without learning the bucket layout.

Enable the optional bounded SQL adapter with `cargo test --features datafusion`. It exposes `PulpitumTableProvider` for `SessionContext` registration and requires one logical `channel_id` equality plus literal `timestamp >=` and `timestamp <` bounds. See [`docs/datafusion.md`](docs/datafusion.md) for the exact supported query shape and limitations, and [`docs/testing.md`](docs/testing.md) for the executable fault-test inventory.

To run specifications for known, unimplemented distributed guarantees (and intentionally reproduce their failure):

```sh
./docker/scripts/run-known-failures.sh
```

## How Pulpitum compares

Pulpitum occupies a narrow middle ground between keeping all application data in the primary database and building a separate historical-data system. Its differentiator is not writing Parquet to S3: it is the coordinated lifecycle around that object. Pulpitum fences writes to an immutable bucket, verifies the uploaded snapshot, atomically changes the application's read route, and only then drains the hot rows. `DurableTable` preserves one bounded, partition-local pagination model across that cutover.

| Approach | Optimized for | Difference from Pulpitum |
|---|---|---|
| Keep all history in CockroachDB | The simplest operational model and full database query semantics | No cold-storage savings; historical data remains part of the primary database's storage and operational footprint. Prefer this until that footprint is a demonstrated problem. |
| Database-native hardware tiering | Preserve one database query surface while moving older partitions or key ranges to cheaper disks or nodes | PostgreSQL [partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) with [tablespaces](https://www.postgresql.org/docs/current/manage-ag-tablespaces.html), CockroachDB [replication zones](https://www.cockroachlabs.com/docs/stable/configure-replication-zones), YugabyteDB [tablespaces](https://docs.yugabyte.com/stable/explore/going-beyond-sql/tablespaces/), ClickHouse [storage policies](https://clickhouse.com/docs/guides/developer/ttl), and MongoDB [zones](https://www.mongodb.com/docs/manual/tutorial/sharding-tiered-hardware-for-varying-slas/) can place colder data on different hardware. Historical data remains part of the live database cluster and retains its replication, balancing, upgrade, backup, and capacity-planning costs. Pulpitum instead drains immutable buckets into independently addressed objects. |
| Tiger Cloud/Timescale tiered storage | Transparent PostgreSQL queries over hypertable chunks moved from high-performance storage to a managed Parquet object tier | This is the closest packaged SQL alternative: it provides broader PostgreSQL semantics plus chunk, row-group, and column pruning. It is a managed Timescale service with hypertable-specific constraints, including immutable tiered chunks; Pulpitum is a narrower Rust/CockroachDB/S3-compatible building block with an explicit application-level cutover protocol. See the [Timescale tiered-storage documentation](https://docs.timescale.com/use-timescale/latest/data-tiering/). |
| Export plus TTL or scheduled deletion | Cheap retention when archived data rarely needs to be served | An export job and a deletion policy do not by themselves provide a write fence, verified handoff, atomic read cutover, or cross-tier pagination. A custom implementation must supply those guarantees. Database backups are for recovery, not an application-serving archive. |
| Application-managed dual reads | Maximum control over schemas, storage formats, and routing | This is the closest architectural alternative. The application owns cutover races, partial uploads, stale workers, cleanup recovery, cursor stability, and observability; Pulpitum packages those concerns behind a narrow table contract. |
| CDC/ETL, such as PeerDB or Debezium | Continuously maintaining a downstream replica while source rows remain writable | CDC moves changes; it does not switch application reads per bucket or make deletion from the source safe. Choose it for replication, integration, or warehouse ingestion. Pulpitum and CDC can be complementary. See the detailed [PeerDB comparison](docs/hot-cold-archival.md#comparison-with-peerdb). |
| An analytical database, such as ClickHouse | High-volume aggregations, broad scans, and analytical indexing | Usually introduces a second query and consistency model rather than preserving the application's existing bounded table semantics. Choose it when historical analytics, not transparent application reads, is the product requirement. |
| Apache Iceberg, Apache Hudi, or Delta Lake | Lakehouse table management, large analytical scans, schema evolution, and interoperability | These formats manage analytical tables over object storage; they do not provide Pulpitum's CockroachDB write fence and per-bucket application read cutover. Pulpitum's Parquet output is an archive encoding, not an Iceberg/Hudi/Delta table. |

Pulpitum is a reasonable fit when all of the following are true:

- the workload is append-oriented and old time buckets can become permanently immutable;
- normal reads are bounded to one logical partition and a finite time range;
- recent writes should stay in CockroachDB while older records must remain readable from cheaper object storage; and
- callers should not need to know which tier currently owns a bucket.

Choose an alternative when historical rows must remain mutable, arbitrary SQL or large cross-partition analytics are primary requirements, a continuously updated replica is the goal, simple retention/deletion is sufficient, or keeping cold data on cheaper nodes inside the live database is acceptable. Pulpitum deliberately trades generality for an explicit archival fence, removal of old rows from CockroachDB, and a tier-transparent application read path.

OpenDAL and DataFusion are components used by Pulpitum, not competing storage systems. OpenDAL supplies object-store transport and signing; DataFusion supplies the optional bounded SQL planning and execution surface. Pulpitum retains ownership of archive metadata, publication, routing, and cleanup.

## Capability posture

The safe default is a v4 hot-store deployment: archival is disabled, automatic v3-to-v4 conversion is unavailable, and the SQL gateway rejects transactions, DDL, grants/revokes, multi-statement requests, batching, and unsupported SQL before DataFusion. Enable archival only with `PULPITUM_ARCHIVAL_ENABLED=true` after its environment-specific fault acceptance criteria are complete. This intentionally trades cold-tier cost savings for safety until the remaining distributed-archival evidence exists.

## Production readiness

This is an experimental `0.1.0` foundation, not a production-ready distributed storage system. See [`docs/production-readiness.md`](docs/production-readiness.md) for the audited release blockers, test evidence, architecture map, and acceptance criteria. The proposed deployment-owned archival control plane is documented in [`docs/archival-coordinator.md`](docs/archival-coordinator.md).

## Status

The durable CockroachDB route is available through `CockroachDurableBucketStore`, `DurableTable`, and `DurableArchiveCoordinator`; the showcase uses this path with MinIO/OpenDAL and a feature-gated DataFusion `SessionContext` read. OpenDAL archives publish verified manifests over JSON or Parquet payloads. Archive predicate/projection pushdown, durable read leases, cursor predicates, and multi-worker crash/takeover integration coverage remain pending; see `docs/datafusion.md` and `tests/README.md`.
