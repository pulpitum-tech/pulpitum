# Aggregate pushdown proposal

## Problem

`PulpitumTableProvider` currently exposes routed records to DataFusion through
[`RoutedScanExec`](../src/integrations/datafusion.rs). A SQL aggregate such as:

```sql
SELECT COUNT(*)
FROM messages
WHERE channel_id = 'general'
  AND timestamp >= TIMESTAMPTZ '2026-01-01T00:00:00Z'
  AND timestamp < TIMESTAMPTZ '2027-01-01T00:00:00Z';
```

is therefore planned as an unbounded record scan. `RoutedScanExec` turns an
absent `LIMIT` into `usize::MAX`, fetches `timestamp`, `id`, and `value` from
each routed bucket, builds Arrow record batches, and lets DataFusion count the
rows. This preserves the current routing semantics but unnecessarily transfers
and decodes record values for an aggregate that does not need them.

The showcase trace that motivated this proposal scans about 1,179 rows and 166
KiB on average. CockroachDB planning and CPU are small; reducing this work is
still worthwhile, but it must not bypass Pulpitum's hot/archive routing fence.

## Goals

- Push supported aggregates to the physical store selected by durable routing.
- Preserve the existing single-partition, finite-time-range safety contract.
- Avoid materializing `Record` values and Arrow batches for aggregate-only
  queries.
- Keep results consistent with the current routed-read contract during archive
  transitions; do not silently claim a stronger cross-bucket snapshot.
- Retain a correct fallback for formats or aggregate shapes that are not yet
  supported.

## Non-goals

- General SQL pushdown or arbitrary DataFusion expression pushdown.
- Changing the v4 physical partition key `(table_id, partition_key, bucket_key)`, clustering key `(event_time ASC, sort_key ASC)`, or configured bucket strategy.
- Replacing the durable archive fence with direct CockroachDB access.
- Maintaining a write-time counter as the first implementation. A shared
  counter would add a new write hotspot and has different recovery semantics.

## Proposed design

### 1. Introduce a narrow aggregate port

Add a durable aggregate operation that is explicitly bucket-routed rather than
exposing a raw Cockroach client. Its first shape should be:

```rust
struct CountQuery {
    partition_key: PartitionKey,
    range: TimeRange,
}

async fn count(&self, query: CountQuery) -> Result<u64, DurableTableError>;
```

`DurableTable::count` partitions the finite time range into the relevant configured-strategy buckets and asks the durable bucket store for each bucket's contribution. It
uses the same hot/archive decision path as `query_page`, including the existing
hot-route cache and archive publication fence.

The new port should return the count only. It must not construct `Record`s or
accept a general SQL fragment.

### 2. Implement store-specific bucket counts

For a **hot** bucket, `CockroachDurableBucketStore` runs a parameterized query:

```sql
SELECT count(*)
FROM pulpitum_v4_records
WHERE table_id = $1
  AND partition_key = $2
  AND bucket_key = $3
  AND event_time >= $4
  AND event_time < $5;
```

The primary key already provides the required constrained scan. The count needs
only key columns, so CockroachDB does not need to return `sort_key` or `value` to the
sidecar. The SQL adapter still accepts logical `channel_id` equality and `timestamp`
bounds, mapping them to `partition_key` and `event_time` before this physical query.

For an **archived** bucket:

- Parquet archives should use Parquet metadata and row-group predicate
  filtering where the requested range permits it.
- JSON archives initially use the existing archive decoder to count matching
  records, without Arrow materialization. This is a correctness-first fallback,
  not a performance claim.
- A manifest may be used directly only when its row count exactly covers the
  requested bucket/range. A whole-bucket count cannot answer an arbitrary
  partial time range.

The result is the sum of bucket contributions. As with today's multi-bucket
routed read, a concurrent cutover may be observed independently per bucket;
the feature must document that it preserves existing semantics rather than
providing a new global snapshot guarantee.

### 3. Push down only recognized DataFusion aggregates

The sidecar should recognize a physical plan that is exactly `COUNT(*)` above a
single `PulpitumTableProvider` scan with:

- one exact logical `channel_id` equality;
- finite inclusive/exclusive timestamp bounds;
- no `GROUP BY`, `DISTINCT`, joins, window functions, or extra predicates.

It should execute a dedicated `PulpitumCountExec` that calls
`DurableTable::count` and emits one Arrow batch containing the `UInt64` result.

All other aggregate plans continue through the existing record-scan path. This
keeps the first release small and avoids incorrectly accepting SQL that the
provider cannot route safely.

Once `COUNT(*)` is established, the next candidates are `MIN(timestamp)` and
`MAX(timestamp)`. `COUNT(column)`, `SUM`, and grouped aggregates require
explicit null/value semantics and should not be included implicitly.

## Bounded parallel bucket fan-out

This is independently useful even before aggregate pushdown. Today
`DurableTable::query_page` processes relevant buckets serially: it awaits each
`read_range` and any archive-object fetch before starting the next bucket. A
finite range can instead process independent `(table_id, partition_key, bucket_key)` physical partitions
concurrently because their persisted UTC intervals do not overlap.

The fan-out must be bounded, not `join_all` over an arbitrary history range:

- derive buckets from the query range using the configured strategy and order them by persisted bounds, not lexical bucket key;
- execute route lookup plus hot/archive read through a `FuturesUnordered` or
  stream buffer with a small configurable limit (initially 4);
- cap the limit at available pool capacity after reserving connections for
  writes and archive coordination;
- retain each bucket's descriptor and merge completed results by `(event_time, sort_key)`
  before constructing the page; and
- cancel outstanding work when the caller disconnects, while relying on normal
  pool-drop behavior to release local capacity.

For an unbounded aggregate scan, each bucket can contribute independently and
parallelism reduces wall-clock time toward the slowest bucket. For a paged
record query with global `LIMIT n`, each worker may need to fetch up to `n + 1`
records to prove whether the merged page has another cursor. This intentionally
trades bounded over-fetch for latency; it must be measured and exposed in
metrics. A serial path remains preferable for a one-year range, a very small
limit, or when the pool is already under pressure.

Parallel fan-out does **not** make one hot CockroachDB range faster. Splitting a
single ordered range into concurrent fragments would create extra reads,
merging, and write contention. It applies only to independent strategy buckets or
archive objects.

## Observability

Create an internal span named `COUNT messages` with:

- `db.system.name = "datafusion"`;
- `db.operation.name = "COUNT"`;
- `pulpitum.sql.mode = "routed_aggregate"`;
- `pulpitum.aggregate = "count"`;
- a bucket-count field, but no partition-key value or raw SQL literals.

Each hot-store count query receives its own CockroachDB client span, analogous
to the current record-read span. Add a counter for pushed-down aggregates and a
histogram for aggregate duration, separated by `source = hot | archive | mixed`
and `outcome = success | fallback | error`.

This allows CockroachDB service latency, aggregate routing time, and archive
fallback cost to be compared without treating the DataFusion span as database
CPU time.

## Test plan

1. Unit-test aggregate-plan recognition and rejection of unsupported logical
   shapes.
2. Verify a hot-only `COUNT(*)` uses one count query and does not request
   `value`.
3. Verify counts across two strategy buckets and a range that crosses the applicable UTC bucket boundary.
4. Verify hot, archiving, and archived routes return the same count as a
   materialized reference scan.
5. Add fault tests around archive publication and cleanup, preserving the
   current routed-read consistency behavior.
6. Add a showcase trace assertion/documented check that a count has aggregate
   spans and no `usize::MAX` record scan.

## Rollout

1. Land the durable-table count port and Cockroach hot-bucket implementation.
2. Add the DataFusion `COUNT(*)` execution path behind an opt-in feature or
   sidecar configuration flag.
3. Add archive implementations and parity tests.
4. Enable it in the showcase, compare CockroachDB service latency and bytes
   read against the existing scan path, then make it the default.

The first rollout should retain the existing scan fallback for every unsupported
condition. Correct durable routing takes priority over pushdown coverage.
