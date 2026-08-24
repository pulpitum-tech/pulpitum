# Platform-owned archival coordinator

## Current v4 implementation

`DurableArchiveRecoveryRunner` and the standalone `pulpitum-archiver` discover eligible v4 buckets, claim fenced leases, retry pre-publication failures, and resume `Archived { hot_deleted: false }` cleanup after a worker expires.

The runner operates directly on v4 bucket metadata. There is **no** implemented table registry, per-table retention registry, or separate durable archive-job table. Do not assume a registry or a dual v3/v4 reader exists.

## v4 bucket scope

The v4 physical record identity can be written compactly as:

```text
((table_id, partition_key, bucket_key), (event_time, sort_key))
```

The physical partition key is `(table_id, partition_key, bucket_key)`, and the clustering key is `(event_time ASC, sort_key ASC)`. `PartitionKey` and `SortKey` are opaque byte strings; `event_time` remains first so time-bucket routing and cross-bucket pagination stay chronological.

Each metadata row is keyed by `(table_id, partition_key, bucket_key)` and stores:

- `bucket_strategy`;
- UTC `bucket_start` and `bucket_end` bounds, interpreted as `[bucket_start, bucket_end)`;
- routing state, generation, lease owner/expiry, retry state, published object key, and cleanup state.

Built-in strategies are `CalendarYearUtc` (default), `CalendarMonthUtc`, and `CalendarDayUtc`. Their canonical opaque keys are `year:YYYY`, `month:YYYY-MM`, and `day:YYYY-MM-DD`. The archiver uses bounds rather than lexical bucket-key ordering.

A bucket strategy is immutable for a `TableId`. Changing it requires a new `TableId` and an explicit data migration.

## Candidate selection

The archiver receives one generic cutoff:

```text
ARCHIVER_ELIGIBLE_BEFORE=<RFC3339 timestamp>
```

A hot bucket is eligible when:

```text
bucket_end <= ARCHIVER_ELIGIBLE_BEFORE
```

If unset, the cutoff defaults to January 1 of the previous UTC year. This works for yearly, monthly, and daily buckets because each descriptor has an explicit end timestamp.

`ARCHIVER_KEEP_HOT_BUCKETS` and `ARCHIVER_ELIGIBLE_THROUGH_YEAR` were year-specific controls; they are obsolete and unsupported by the v4 archiver. `TableDefinition::writable_buckets` is an ingestion guard measured in the configured strategy's buckets, not the archival cutoff policy.

## Recovery workflow

For every eligible or recoverable v4 metadata row, the runner:

1. claims the bucket with an opaque owner token, lease expiry, and fencing generation;
2. changes `Hot` to `Archiving`, which fences Pulpitum appends;
3. takes an owner- and generation-checked snapshot;
4. conditionally creates content-addressed JSON or Parquet payloads, reads them back, and verifies checksum and row count;
5. conditionally creates and reads back a content-addressed archive manifest v4 before publishing its route as `Archived { hot_deleted: false }`; and
6. deletes hot rows in a separately fenced transaction, setting `hot_deleted: true`.

A supervised heartbeat renews the fenced ownership lease throughout snapshot, upload, publication reconciliation, cleanup, and retry deferral. If renewal fails, the runner drains the current operation but starts no subsequent destructive phase; durable expiry and takeover decide recovery.

Publication is the read-cutover point. A failure before publication is deferred and the bucket becomes retryable. A failure after upload is recovered by rereading metadata rather than assuming publication did or did not commit. A replacement worker can claim pending cleanup after the prior lease expires.

## Operational configuration

| Setting | Default | Purpose |
|---|---:|---|
| `ARCHIVER_ELIGIBLE_BEFORE` | January 1 of the previous UTC year | RFC3339 cutoff applied to `bucket_end` |
| `ARCHIVER_INTERVAL_SECONDS` | `15` | Discovery/recovery scan interval |
| `ARCHIVER_LEASE_SECONDS` | `60` | Claim lease duration |
| `ARCHIVER_LEASE_RENEWAL_SECONDS` | one third of the lease | Renewal heartbeat; must be non-zero and shorter than the lease |
| `ARCHIVER_RETRY_SECONDS` | `15` | Deferred-work retry delay |
| scan limit | `64` | Maximum work items discovered per cycle |

Run the archiver as a separately supervised process. Stop it during metadata/record backfills so a partially copied bucket cannot be claimed.

## Known limits

The implementation provides fenced routing, active lease renewal, content-addressed conditional object creation, and payload/manifest read-back verification, but it is still experimental. Durable read leases, orphan-object cleanup, and independent multi-process crash/partition evidence remain incomplete. See [`hot-cold-archival.md`](hot-cold-archival.md) and [`production-readiness.md`](production-readiness.md) for the broader readiness assessment.
