# Jepsen-style local E2E harness

This is a **single-machine Jepsen-style fault harness**, not Jepsen itself and not a proof of distributed correctness.

## Stack

`docker-compose.yml` provisions:

- CockroachDB `v25.4.10`, with three nodes;
- MinIO and a `pulpitum` bucket;
- Toxiproxy, preloaded with CockroachDB and MinIO client listeners;\n- an opt-in `e2e` runner that verifies proxy-routed client connectivity;
- an idempotent one-shot `cockroach-init` job.

Host ports are isolated from other local stacks: Cockroach SQL `26267` and MinIO S3 `19000` are **Toxiproxy listeners**; MinIO Console is `19001`; the Toxiproxy API is `18474`. The underlying CockroachDB and MinIO service ports are private to the Compose network.

## Run

```sh
./docker/scripts/run-e2e.sh
```

The script has a 90-second Cockroach readiness limit, a 120-second Compose startup limit, a 100-second Cockroach initializer limit, a 30-second MinIO initializer limit, a 180-second proxy-environment check limit, and a 300-second test-process limit. It also restores MinIO and the faulted Cockroach node when interrupted. It runs the proxy-routed `e2e` service first, then invokes the ignored durable `tests/e2e.rs` scenarios serially.

The storage-fault scenarios start the same shared SQL workload profile as the Grafana showcase before inducing any failure: 128 workers, a 4,096-request bounded queue, a 50/50 hot-bucket read/write mix, and a four-second **12 → 25 → 113 → 50 RPS** burst cycle (50 RPS average). They complete one full cycle before fault injection and continue through every fault and recovery step.

`durable_scheduled_archives_survive_faults_while_hot_load_continues` is the fast, deterministic counterpart to the showcase's one-minute recurrence. It stages and archives three **distinct, closed prior-year** `(partition_key, bucket_key)` physical partitions back-to-back rather than sleeping for real minutes. The second cutover runs while MinIO is paused, verifies that the bucket returns to `Hot` and remains readable, then retries that exact bucket after recovery. Finally it stops one Cockroach node and verifies every archived bucket still routes through MinIO while the current-year hot workload continues.

Together, the tests verify these invariants against the real CockroachDB and MinIO adapters:

1. A MinIO outage during archive upload returns an error, preserves `Hot` metadata, and leaves all records queryable from CockroachDB while live SQL traffic continues.
2. A retry after MinIO recovery archives every record, routes through S3, and removes the hot bucket only after the cutover.
3. Scheduled cutovers never skip a failed bucket or write again to an archived `(partition_key, bucket_key)` physical partition.
4. Reads of archived buckets remain available after stopping one Cockroach node while live traffic continues.
5. The spiky load completes SQL reads without errors, records no append failure, and every successful live append is still readable with its original value after recovery.

Successful enqueued writes remain subject to the integrity checks.

## Important boundary

The harness couples Cockroach metadata and records through `CockroachDurableBucketStore`, but it still runs from one test process. Archive-owner takeover, durable read leases, independently crashed archival workers, and client-path partitions remain pending. It does not establish Jepsen-level distributed safety yet.
