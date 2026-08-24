# DataFusion SQL adapter

## Status and decision

Pulpitum provides an **optional, read-only Apache DataFusion 54.1 adapter** behind the `datafusion` Cargo feature. It provides a familiar SQL surface and Arrow batches while retaining Pulpitum as the routing authority for hot and archived configured-strategy buckets.

`PulpitumTableProvider` is intentionally narrow: it accepts one logical `channel_id` equality plus a finite half-open `timestamp` range, then executes through `DurableTable`. The `showcase` feature runs the adapter in a separate PostgreSQL-wire SQL sidecar; the chat UI issues that bounded query over the sidecar connection.

DataFusion is an execution and planning layer, not the source of truth. It must never bypass:

- `DurableTable`'s logical range and cursor semantics;
- durable bucket metadata and fencing checks;
- the archive cutover publication protocol; or
- the Cockroach hot-store transaction that enforces a write/archive fence.

## Goals

- Expose bounded SQL reads for a single logical Pulpitum table, plus a sidecar-owned fenced append command.
- Produce Arrow `RecordBatch` output for bounded, partition-local range queries across hot and archived buckets.
- Keep the implementation on the durable routed table API rather than exposing backend-specific reads.
- Preserve user-visible `ORDER BY timestamp ASC, id ASC`, which maps to the physical `(event_time ASC, sort_key ASC)` clustering order and cursor.
- Return actionable planning errors before a query fans out across unbounded archive data.
- Keep query diagnostics in traces and metrics rather than result rows.

## Non-goals

The first adapter does not provide:

- Generic SQL writes, DDL, transactions, joins, arbitrary aggregations, cursor predicates, or arbitrary global scans. The sidecar accepts PostgreSQL extended-query binds only for the bounded routed read predicates (`channel_id`, range start, range end, and optional limit) and the four columns of one `INSERT` row. `COUNT(*)` is supported for a bounded single-partition scan, but it is not pushed down to the underlying stores. The proposed aggregate and bounded parallel-bucket design is documented in [`aggregate-pushdown-proposal.md`](aggregate-pushdown-proposal.md).
- a replacement for Iceberg, Hudi, or Delta Lake analytical-table management;
- predicate or projection pushdown into CockroachDB or archive objects;
- Parquet predicate or projection pushdown into archive objects. OpenDAL can write and verify Parquet payloads and manifests, but this routed adapter still materializes the bucket through `DurableTable`; or
- source streaming: a bounded `DurableTable::query_page` result is materialized per scan and emitted as Arrow batches of at most 1,024 rows.

## Logical and physical table contract

The user-facing definition names each part of the physical key separately:

```rust
let messages = TableDefinition::new_with_bucket_strategy(
    "messages",
    TableId::new("com.example.messages")?,
    "channel_id", // logical partition-key column
    "event_time", // mandatory time-bucket source
    BucketStrategy::CalendarYearUtc,
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
    2, // two recent strategy buckets are writable
)?;
```

For the built-in record model, v4 CockroachDB records use:

```sql
PRIMARY KEY (table_id, partition_key, bucket_key, event_time, sort_key)
```

This is the Cassandra-style identity `((table_id, partition_key, bucket_key), (event_time, sort_key))`: the first tuple is the physical partition key and records cluster by `(event_time ASC, sort_key ASC)`. `partition_key` and `sort_key` are CockroachDB `BYTES` columns backed by opaque `PartitionKey` and `SortKey` values. `event_time` is mandatory and first because the table derives time buckets from it and merges pagination chronologically across buckets.

Metadata for every `(table_id, partition_key, bucket_key)` stores `bucket_strategy` and UTC `[bucket_start, bucket_end)` bounds. `CalendarYearUtc` (the default), `CalendarMonthUtc`, and `CalendarDayUtc` derive canonical opaque keys `year:YYYY`, `month:YYYY-MM`, and `day:YYYY-MM-DD`; do not depend on lexical key order. The strategy is immutable for a `TableId`; create a new `TableId` and migrate data to change it.

| SQL column | Core field | Meaning |
|---|---|---|
| `channel_id` | `partition_key` | Logical partition selector, encoded as opaque bytes internally. |
| `timestamp` | `event_time` | Mandatory leading clustering component and time-bucket source. |
| `id` | `sort_key` | Opaque byte-string tiebreaker within one event time. |
| `value` | `value` | Opaque record payload, exposed as Arrow `Binary`. |
| — | `bucket_key` | Derived opaque physical routing key; SQL callers do not supply it. |

The public provider exposes `channel_id`, `timestamp`, `id`, and `value`. The physical `table_id`, `bucket_key`, strategy, and bounds remain routing metadata and are not required in a SQL query.

## SQL contract

### Supported low-latency query shape

A query must contain both:

1. equality on the configured logical partition column (`channel_id` for chat); and
2. a finite half-open predicate on the logical `timestamp` column.

```sql
SELECT timestamp, id, value
FROM messages
WHERE channel_id = 'general'
  AND timestamp >= TIMESTAMPTZ '2023-11-01 00:00:00Z'
  AND timestamp <  TIMESTAMPTZ '2025-01-01 00:00:00Z'
ORDER BY timestamp ASC, id ASC
LIMIT 100;
```

The provider does **not** emit Cockroach SQL. It maps `channel_id` to `partition_key`, maps the `timestamp` bounds to `event_time`, and extracts the resulting partition and `[start, end)` range, derives relevant configured-strategy buckets inside `DurableTable::query_page`, and uses that same fenced routing path.

### Bounds and predicate normalization

The provider accepts only conjunctions of literal predicates after the sidecar binds any PostgreSQL extended-query parameters into the DataFusion logical plan. It recognizes equivalent timestamp bounds in either order:

```sql
-- Accepted
channel_id = 'general'
  AND timestamp >= TIMESTAMP '2023-11-01T00:00:00Z'
  AND timestamp < TIMESTAMP '2025-01-01T00:00:00Z'

-- Accepted after normalization
TIMESTAMP '2023-11-01T00:00:00Z' <= timestamp
  AND TIMESTAMP '2025-01-01T00:00:00Z' > timestamp
```

It rejects or returns a planning error for:

```sql
-- Missing logical partition equality
WHERE timestamp >= $start AND timestamp < $end

-- Unbounded end
WHERE channel_id = 'general' AND timestamp >= $start

-- Disjunction cannot be represented by one routed Query
WHERE channel_id = 'general' OR channel_id = 'random'

-- Conflicting logical partition constraints
WHERE channel_id = 'general' AND channel_id = 'random'
```

`BETWEEN`, unbound placeholders, row-value cursor predicates, and non-literal expressions are rejected because they cannot preserve the explicit end-exclusive routed contract in this implementation. The sidecar resolves positional `$1`–`$4` binds before the provider plans a scan; the provider never receives raw bind values or an unbound routed scan.

### Sidecar insert shape

The PostgreSQL-wire showcase sidecar accepts exactly one append row. It supports either a literal SQL row or a PostgreSQL extended-query statement with positional binds:

```sql
INSERT INTO messages (channel_id, timestamp, id, value)
VALUES (
  'general',
  TIMESTAMPTZ '2026-08-06T12:00:00Z',
  'message-001',
  'Ada: a UTF-8 message'
);
```

The statement must target `messages`, name exactly `channel_id`, `timestamp`, `id`, and `value` (in any order), and contain exactly one `VALUES` row. For extended queries, those four columns accept `$n` binds decoded as `TEXT`, `TIMESTAMPTZ`, `TEXT`, and `BYTEA`, respectively; NULLs and skipped or conflicting parameter indexes are rejected. Literal SQL preserves the existing semantics: `channel_id`, `id`, and `value` are string literals, `timestamp` must be an RFC 3339 `TIMESTAMPTZ` literal, and a `value` beginning with `\\x` is decoded as PostgreSQL hex `bytea`. `RETURNING`, upserts, expressions, defaults, `INSERT ... SELECT`, and transactions are rejected.

The sidecar maps the validated row to `DurableTable::append`; it never writes directly to CockroachDB, so it retains the durable bucket-state and archive-fencing checks. The timestamp must fall in the table definition's writable window; an archived historical bucket remains immutable. The DataFusion table provider remains read-only.

### Ordering, limits, and pagination

- `DurableTable::query_page` supplies physical `(event_time, sort_key)` order across routed buckets. The SQL mapping exposes this as `(timestamp, id)`, so `ORDER BY timestamp ASC, id ASC` returns the core order; other `ORDER BY` clauses use DataFusion's normal sort and are not pushed down.
- A SQL `LIMIT` is forwarded as the `DurableTable::query_page` limit. It is global for this one routed partition/range scan, not a per-bucket limit.
- SQL cursor predicates are not implemented. Applications needing stable cursor pagination should call `DurableTable::query_page` directly.

## TableProvider design

The adapter lives behind a `datafusion` Cargo feature. The default crate must not depend on DataFusion, Arrow, or Parquet.

```rust
#[cfg(feature = "datafusion")]
pub struct PulpitumTableProvider {
    table: Arc<DurableTable>,
    schema: SchemaRef,
}
```

`PulpitumTableProvider` implements `TableProvider` as follows:

1. `schema()` returns the logical Arrow schema using the configured partition-key name, plus `timestamp`, `id`, and `value`.
2. `supports_filters_pushdown()` reports exact handling only for conjunctions of partition-column equality and `timestamp >=` / `timestamp <` literal bounds. Any other predicate is rejected during planning.
3. `scan()` extracts and validates a `RoutedQuery`, applies the DataFusion projection, and forwards a pushed `LIMIT` to `DurableTable::query_page`.
4. The one-partition execution plan invokes `DurableTable::query_page`; it never calls `CockroachHotStore`, `DurableBucketStore`, or `ArchiveStore` directly.
5. The bounded records are emitted in `DurableTable`'s physical `(event_time, sort_key)` order, exposed as logical `(timestamp, id)` Arrow columns. Backend and archive failures become non-sensitive DataFusion execution errors.

The current DataFusion `TableProvider` scan API supplies filters, projection, and limit but no `ORDER BY` request. Therefore `ORDER BY timestamp ASC, id ASC` is safe and returns the durable order, while any other `ORDER BY` is performed by DataFusion's normal sort operator rather than being pushed down or rejected by this provider.

### Provider registration

An application owns the DataFusion session and registers each Pulpitum table explicitly:

```rust
#[cfg(feature = "datafusion")]
let context = SessionContext::new();
context.register_table(
    "messages",
    Arc::new(PulpitumTableProvider::new(messages.clone())), 
)?;
```

The provider is read-only. DataFusion `INSERT`, `UPDATE`, `DELETE`, and `CREATE TABLE` are rejected. The showcase sidecar handles its narrow `INSERT` contract before DataFusion planning and forwards the resulting `Record` to `DurableTable::append`.

## Planning errors

Planning errors are deterministic, safe to expose to callers, and do not contain partition-key values, object keys, or raw database errors.

| Error | Condition |
|---|---|
| `MissingShardEquality` | No exact equality predicate for the configured logical partition-key column. The variant name is retained by the current API. |
| `MissingTimestampStart` | No finite inclusive lower timestamp bound. |
| `MissingTimestampEnd` | No finite exclusive upper timestamp bound. |
| `InvalidTimestampRange` | Start is greater than or equal to end. |
| `UnsupportedPredicate` | A filter cannot be represented by the routed execution contract. |
| `ConflictingShardEquality` | More than one distinct logical partition-key value was supplied. The variant name is retained by the current API. |
| `ConflictingTimestampBounds` | More than one lower or upper bound was supplied. |

A SQL client can distinguish planning errors from runtime storage failures, but neither error class leaks raw SQL parameter values or archive paths.

## Durable routing foundation

`CockroachDurableBucketStore` now supplies a separate coupled CockroachDB metadata/record schema for `DurableTable` and `DurableArchiveCoordinator`. It provides the essential data-plane invariants:

- `Hot → Archiving` is a Cockroach compare-and-set transition that creates an archive-owner token and increments `generation`.
- A Cockroach append validates `Hot` state in the **same serializable transaction** that inserts the record.
- Snapshot and hot-bucket deletion validate the archive-owner token and generation in their physical-store transaction.
- Publication of the archive object pointer is conditional on the active archive-owner token. A superseded worker cannot publish a stale object or delete hot data.
- Cockroach route metadata uses `AS OF SYSTEM TIME with_max_staleness('5 seconds')`; the separately current record query can therefore temporarily return no hot rows after archive cleanup. Callers needing the newly published archive route must retry after the five-second bound.

`DurableBucketStore` exposes claim, cleanup-claim, renewal, defer, and takeover operations. The recovery runner uses claims and durable retry state, but does not yet renew a lease during a long snapshot or upload. Durable read leases and independent-process crash/partition coverage also remain pending, so the SQL provider cannot claim an end-to-end distributed safety guarantee. Persisting the legacy `MetadataRegistry` counters remains insufficient for multi-worker safety.

## Archive format and execution phases

OpenDAL archives use archive manifest v4 and record schema v2 over either a JSON or Zstandard-compressed Parquet payload. The manifest carries the bucket, generation, schema/format version, row count, byte length, SHA-256 checksum, payload key, and clustering declaration `(event_time, sort_key)`. The Parquet envelope is exactly `partition_key Binary`, `event_time Timestamp(ns, UTC)`, `sort_key Binary`, and `value Binary`. The adapter ships in phases:

1. **Durable routing:** Cockroach coupled metadata/records, owner fencing generations, conditional publication, cleanup reclaim, and durable retry are implemented. Durable read leases, lease renewal wired into long-running runner work, and independent-process restart/partition tests remain.
2. **Verified archive artifacts:** JSON and Parquet payloads are written, read back for checksum and row-count verification, then exposed through a versioned manifest. Existing raw JSON payloads remain readable for compatibility. Conditional object creation, manifest read-back verification, and orphan cleanup remain.
3. **Routed SQL:** feature-gated `TableProvider`, bounded predicate validation, a forwarded global `LIMIT`, and Arrow output over the archive format. This is suitable for narrow, partition-local histories only; it does not push fields or predicates into storage.
4. **Streaming execution:** an `ExecutionPlan` that reads Parquet row groups and Cockroach rows as Arrow batches, pushes projections and predicates into both stores, and merges batches by physical `(event_time, sort_key)` order.
5. **Optional analytical mode:** a separately configured distributed execution implementation for explicit multi-partition or unbounded scans. It must not silently run when routed-mode guardrails fail.

## Observability and privacy

Future SQL-plan observability should emit traces/metrics with bounded attributes:

- `pulpitum.table`
- `pulpitum.sql.mode` (`routed` or `analytical`)
- `pulpitum.sql.origin` (`gateway` for the PostgreSQL boundary span)
- `pulpitum.query.buckets_scanned`
- `pulpitum.query.hot_buckets`
- `pulpitum.query.archive_buckets`
- `pulpitum.query.rows_returned`
- `pulpitum.query.bytes_read`
- `db.system.name`
- `db.collection.name`
- `db.operation.name`
- `db.query.summary`
- `db.query.text`

The PostgreSQL sidecar uses stable parameterized query text in `db.query.text` on its `pulpitum.sql.origin = gateway` server spans, which lets the query-performance dashboard identify the query shape while excluding nested durable-store/Cockroach spans. For the default `messages` table, the bounded shapes are routed row reads, `SELECT COUNT(*)`, inserts, and an `OTHER SQL statement` fallback; a configured logical-table name replaces `messages`. The dashboard groups these parameterized texts. `PULPITUM_SQL_CAPTURE_QUERY_TEXT=true` additionally emits the literal inbound statement as `pulpitum.sql.template`; it is never the default dashboard dimension because it can expose values and creates an unbounded Prometheus label. No metrics label may contain partition-key/channel values, sort keys, archive keys, SQL parameter values, or unbounded SQL text unless that explicit capture option has been approved.

## Showcase integration

The reusable `pulpitum-sql-sidecar` binary registers `PulpitumTableProvider` in its `SessionContext` and exposes the configured logical table through PostgreSQL wire protocol. The full-stack showcase runs it on port `5433` (`15432` on the host Compose port); the UI connects to that sidecar, escapes the known channel value before constructing SQL text, and returns only generic HTTP errors if the connection, planning, or execution fails. The binary's listener, table name, bucket, and archive prefix are configured with the `PULPITUM_SQL_*` and `S3_*` variables documented in the top-level README.

The sidecar supports PostgreSQL simple and extended query flows. Extended queries can bind the bounded routed read parameters and the four values of the single-row `INSERT` contract; it decodes them by PostgreSQL type and binds reads into the DataFusion logical plan without SQL string interpolation. Its reads reach the provider only after those values become typed literals, preserving the logical `channel_id`/`timestamp` contract. It has no TLS or authentication and is intentionally limited to the local showcase network. A production-facing server must add authentication, TLS, connection limits, query cancellation, batching/transaction semantics, and idempotency handling before exposure.

The adapter does not issue Cockroach SQL itself, expose statement templates, or add DataFusion query metrics. Those observability additions remain future work; query values, partition-key values, sort keys, archive keys, and raw SQL errors must not be exported as telemetry labels.

## Implementation checklist

### Durable fenced routing

- [x] Introduce the `MetadataStore` abstraction while retaining the in-memory `MetadataRegistry` for unit tests.
- [x] Make metadata methods fallible and add typed, non-sensitive metadata errors.
- [x] Define the sealed `DurableBucketStore` contract and opaque `ArchiveSession` for coupled append and archive operations; legacy `Table` and `ArchiveCoordinator` intentionally retain the split path.
- [x] Add fenced durable bucket range reads that atomically return filtered hot records or a durable archive object key, and route `DurableTable` through them.
- [x] Add Cockroach migrations for coupled bucket metadata, archive ownership, fencing generation, and mutable records. Active read/write leases remain pending; OpenDAL archives publish verified manifests.
- [ ] Add a one-shot MikroORM v7 migration service to the Compose showcase that applies the Pulpitum Cockroach schema before the workload starts; make it the sole schema owner rather than issuing runtime `CREATE TABLE` statements.
- [ ] Add durable read leases and wire the existing archive-owner renewal API into long-running runner work.
- [x] Couple Cockroach append, snapshot, and hot-bucket deletion to metadata fence validation in the same serializable transaction, including bounded retries for `40001` serialization errors.
- [x] Make archive publication, abort, cleanup reclaim, and retry conditional on the archive-owner token and generation through `DurableArchiveCoordinator` and `DurableArchiveRecoveryRunner`.
- [ ] Add independent-process Cockroach integration tests for worker restart, write/cutover races, archive-owner takeover, idempotent post-publication cleanup, and partitions.
- [ ] Replace the two currently ignored distributed-safety failure probes with passing Cockroach integration coverage.

### Routed DataFusion SQL

- [x] Add a non-default `datafusion` Cargo feature and keep DataFusion/Arrow out of the default dependency graph.
- [x] Implement the logical Arrow schema and read-only `PulpitumTableProvider`.
- [x] Extract literal `channel_id` equality and finite `[start, end)` `timestamp` bounds; pass DataFusion projection and `LIMIT` through the scan.
- [x] Reject unbounded, multi-partition, disjunctive, conflicting, or otherwise unsupported predicates before routed execution.
- [x] Implement a one-partition routed `ExecutionPlan` that preserves physical `(event_time, sort_key)` order across configured-strategy hot/archive sources.
- [x] Add a `SessionContext` provider test across a hot/archive boundary.
- [ ] Add a DataFusion analyzer or provider API support for rejecting unsupported `ORDER BY`; current DataFusion 54 `TableProvider::scan` does not receive ordering information.
- [ ] Add `(event_time, sort_key)` cursor predicates, source streaming, and storage-level projection/predicate pushdown.

### Archive and showcase

- [x] Version the archive manifest and support JSON and Parquet payloads; retain legacy raw JSON reads.
- [ ] Add conditional archive-object creation, manifest read-back verification, and delayed orphan cleanup.
- [ ] Push projection and timestamp predicates into Cockroach and Parquet reads; stream Arrow batches rather than collecting `Vec<Record>`.
- [x] Change the showcase to construct durable Cockroach routing with `DurableTable` and `DurableArchiveCoordinator`, and issue the UI history read through the PostgreSQL-wire SQL sidecar.
- [ ] Export bounded routing metrics and parameterized statement templates (never values) to the query-performance dashboard.
- [ ] Run the Compose showcase and verify the SQL dashboard, hot/archive traces, pagination, and reset flow.

## Acceptance criteria

The adapter is enabled in the local showcase for bounded routed reads. It is ready to claim broader production readiness only when all of the following are true:

- A restarted worker routes an already archived bucket to its durable archive pointer.
- A stale archive owner cannot publish or delete after lease takeover.
- A write racing archive cutover either commits before the fenced snapshot or is rejected/retried; it never disappears.
- A routed SQL query spanning multiple buckets returns the same records and user-visible `(timestamp, id)` order as `Table::query_page`, mapped from the physical `(event_time, sort_key)` order.
- Missing `channel_id` equality or `timestamp` bound is rejected before opening hot/archive reads.
- Timestamp bounds and a global `LIMIT` are routed correctly; cursor predicates and storage-level projection/predicate pushdown are implemented or explicitly remain unsupported.
- The showcase emits only privacy-safe diagnostics, without parameter values, partition-key values, sort keys, or object keys.
- The default Cargo build does not include DataFusion, Arrow, or Parquet.
