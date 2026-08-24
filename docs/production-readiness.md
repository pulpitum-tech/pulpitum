# Production readiness audit

**Audit date:** 2026-08-10  
**Version reviewed:** `0.1.0`  
**Decision:** **No-go for production**

Pulpitum is a credible experimental foundation with a useful local showcase, a fenced CockroachDB data path, recoverable published-cleanup workflow, and good unit-level coverage of routing semantics. It is not yet a production distributed-storage system. The remaining gaps include possible cross-table data collisions, incomplete immutable-object publication guarantees, lease renewal during long-running work, insecure CockroachDB transport, unbounded query materialization, and missing independent multi-worker fault evidence.

## Readiness estimate

| Use case | Readiness | Assessment |
|---|---:|---|
| Local development and architecture evaluation | 8/10 | Appropriate today. |
| Single-process prototype with disposable data | 6/10 | Usable with explicit limits and monitoring. |
| Production library for one trusted logical table | 3/10 | No-go until the P0 items below are resolved. |
| Multi-tenant or multi-table production service | 2/10 | Physical keys do not include a table namespace. |
| Distributed archival with crash/partition guarantees | 2/10 | Published-cleanup recovery exists, but long-work lease renewal, independent crash/partition evidence, and full multi-worker validation remain incomplete. |

The overall production-readiness estimate is **about 30%**. This is not a measure of code quality; it reflects how much distributed-systems and operational evidence remains before irreversible hot-data deletion is safe to operate.

## Test evidence

### Docker E2E harness

Command:

```sh
./docker/scripts/run-e2e.sh
```

The first audited run timed out after 300 seconds while MinIO was paused. `OpenDalArchiveStore::put_bucket` had no operation deadline, so a connected but unresponsive S3 endpoint could leave the archival future pending indefinitely. The test process was killed before it could unpause MinIO.

The audit added a default 30-second object-operation deadline, a configurable `OpenDalArchiveStore::with_operation_timeout`, and service-restoration traps in the runner. The repaired Compose/proxy harness was rerun and passed all four ignored scenarios:

- `durable_cockroach_store_fences_and_archives_a_bucket`: passed;
- `durable_recovery_runner_takes_over_published_cleanup_after_worker_loss`: passed;
- `durable_scheduled_archives_survive_faults_while_hot_load_continues`: passed;
- `storage_outage_cannot_lose_or_cut_over_a_bucket`: passed;
- runtime: 76.82 seconds;
- load: 1,837 offered, 1,837 enqueued, 0 dropped, 918 successful writes.

The runner first verifies CockroachDB and MinIO connectivity from the `e2e` service through Toxiproxy. The legacy storage-outage scenario still exercises `Table` + `MetadataRegistry`; although the durable scenarios cover CockroachDB, MinIO, recovery, and a one-node loss, they do not yet provide independent multi-worker, stale-owner, or client-gateway fault evidence.

### Other validation

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed. |
| `cargo test --locked --all-targets --all-features` | Passed: 29 library/DataFusion tests, 4 SQL-sidecar tests, 2 showcase tests, and 2 environment tests; 4 opt-in Docker E2E tests are ignored by this command. |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | Passed after three style fixes. |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps` | Passed. |
| `cargo package --locked --allow-dirty` | Passed; warns that documentation/homepage/repository metadata is absent. |
| `cargo audit` | No known vulnerability; warns that transitive `paste 1.0.15` is unmaintained (`RUSTSEC-2024-0436`). |
| Compose config validation and `sh -n` for shell scripts | Passed. |
| `./docker/scripts/run-known-failures.sh` | Reproduced both deliberately failing legacy safety specifications. |

The workspace is not a Git checkout, so commit history, ignored-file behavior, tags, branch protection, CODEOWNERS, release provenance, and working-tree cleanliness could not be audited.

## Current implementation checklist

This is the canonical, executable backlog for the findings above. The P0/P1 sections below provide the rationale and acceptance criteria for these items.

### Test and delivery unblockers

- [x] **Repair the Cockroach Compose topology.** Each Cockroach node has a distinct advertised address; `./docker/scripts/run-e2e.sh` waits for three live nodes and executes all four ignored Rust E2E tests through the proxy-routed stack.
- [x] **Restore the all-features build.** `cargo test --locked --all-targets --all-features` compiles the SQL sidecar and passes locally; make it a required CI validation command.
- [ ] **Add a coverage baseline and gate.** Adopt `cargo-llvm-cov` (or an equivalent Rust coverage tool), publish line and branch coverage for routine tests, and set an initial threshold that can be raised deliberately. No numeric coverage report exists today.
- [ ] **Add CI.** On pull requests, run format, strict Clippy, default tests, supported feature combinations, documentation, and coverage. Run the Docker E2E/fault matrix on a scheduled or manually approved job until its runtime is suitable for every pull request.
- [x] **Make the Toxiproxy scenarios executable.** The authoritative `e2e` runner loads `docker/toxiproxy/toxiproxy.json`; Cockroach and S3 test clients use proxy listeners; and the harness runs `tests/e2e_environment.rs`. The Cockroach outage, S3 outage, and process-restart scripts are cleanup-safe and pass locally.
- [ ] **Run the fault matrix in CI.** Execute `run-e2e.sh` and each Toxiproxy script in a scheduled or manually approved CI job, retaining logs and duration evidence.
- [ ] **Add SQL-sidecar integration coverage.** Exercise simple and extended PostgreSQL-wire query flows, supported inserts and bounded reads, invalid SQL/input handling, and sidecar-to-Cockroach/MinIO routing.

### Distributed-safety evidence

- [ ] **Test independent worker crash recovery.** Run archival workers in separate processes or containers; kill/restart at each archival phase and prove that a successor resumes or safely retries without data loss or permanent write fencing.
- [ ] **Test multi-worker coordination.** Add concurrent archiver, lease-expiry/renewal, stale-owner, and takeover cases against the real Cockroach durable store.
- [ ] **Test client-path and database partitions.** Inject Cockroach gateway loss and metadata/client partitions through Toxiproxy, with bounded recovery-time and availability assertions.
- [ ] **Test archive integrity failures.** Cover partial uploads, corrupted payloads/manifests, wrong-bucket objects, stale uploads, and orphan-object cleanup with the real OpenDAL/S3 adapter.
- [ ] **Test cutover/read races.** Add concurrent append-versus-cutover and query-drain tests once durable read leases and deterministic clock/lease controls exist.

## P0: release blockers

### 1. Logical tables require a completed namespace migration

**Implemented:** `TableDefinition` requires a stable `TableId`, and the table router derives a namespaced `BucketId` for every write and read. The v4 Cockroach adapter uses `pulpitum_v4_bucket_metadata` and `pulpitum_v4_records`; its physical partition key is `(table_id, partition_key, bucket_key)`, with `partition_key BYTES`, and records cluster by `(event_time ASC, sort_key ASC)`, with `sort_key BYTES`. Archive manifests and object paths are namespaced; reads verify the manifest bucket against the requested bucket. The ordinary regression test `tables_with_overlapping_bucket_ids_are_isolated` proves that two tables sharing a durable adapter and bucket coordinates cannot read each other's data.

**Still required before release:**

- provide and exercise an operational v3-to-v4 backfill that authoritatively converts legacy key fields to `partition_key`, `event_time`, and `sort_key`; never assign an implicit namespace or reinterpret a bucket key;
- [x] execute a real Cockroach/MinIO E2E case for two table IDs with overlapping partition keys and buckets, including archival and cleanup (`durable_tables_with_overlapping_buckets_are_isolated_through_archive_cleanup`); it passed locally through the Docker/Toxiproxy stack;
- document and enforce a rolling-deployment cutover so v3 and v4 writers cannot operate on the same logical dataset concurrently. The v4 bootstrap creates only `pulpitum_v4_bucket_metadata` and `pulpitum_v4_records`, leaves v3 tables untouched, and performs no automatic data migration. Existing deployments must use a new `TableId` and archive namespace/prefix or complete an explicit, verified migration before v4 traffic.

### 2. Archive publication still lacks a complete immutable identity boundary

OpenDAL now writes a JSON or Parquet payload, reads it back to verify its SHA-256 checksum and row count, then publishes a versioned manifest key. Archive manifest v4 records the bucket, generation, format, record schema v2, payload length, checksum, payload key, and clustering key `(event_time, sort_key)`; writes also reject records from another bucket or out of clustering order. The Parquet envelope is `partition_key Binary`, `event_time Timestamp(ns, UTC)`, `sort_key Binary`, and `value Binary`. Generation-addressed durable uploads prevent a replacement owner from overwriting an earlier generation.

Implemented since the audit: both legacy and generation-addressed writes now use SHA-256 content-addressed payload and manifest names, OpenDAL conditional creation, idempotent read-after-ambiguous-write verification, and manifest read-back validation. Unit tests cover JSON/Parquet idempotency, existing-object collision rejection, and manifest tampering.

Remaining required work:

- define orphan-payload garbage collection and object-retention policy;
- add real S3/MinIO partial-write, stale-owner, concurrent-create, and wrong-bucket integration tests.

### 3. Long-running archival recovery and evidence are incomplete

`DurableArchiveRecoveryRunner` can discover an expired `Archived { hot_deleted: false }` bucket, claim a new fenced session, and finish cleanup. It also reopens deferred pre-publication work. This closes the original in-memory-session-only cleanup gap.

The runner now starts a supervised fenced heartbeat for every claimed cutover and cleanup session. It renews through snapshot, upload, publication reconciliation, cleanup, and retry deferral. A renewal failure drains the active operation but prevents the next destructive phase. A paused-time test proves a slow upload can exceed the original lease and still complete under renewal. The remaining behavior has not yet been proven through independent process kills, partitions, or ambiguous commit outcomes. The current bucket metadata is also only an interim job record, not the target registry/job schema.

Required change:

- persist the planned table registry and explicit archival job/phase model;
- extend renewal coverage to the direct `DurableArchiveCoordinator` compatibility path and add a database-authoritative/injectable clock;
- add cancellation-safe takeover and idempotent recovery tests at every await boundary;
- test independent worker-process/container kill, restart, and partition recovery;
- clean orphaned immutable objects created by stale attempts; and
- retain the current durable scan/claim/cleanup recovery path as the migration baseline.

### 4. CockroachDB secure transport needs live certificate evidence

Implemented since the audit: `CockroachTlsConfig` builds a rustls connector from an explicit CA bundle and optional mTLS identity. Secure constructors require `sslmode=require`; downgradeable `sslmode=prefer` is rejected. Local stacks use explicitly named `connect_insecure_dev*` constructors, and legacy ambiguous constructors are deprecated. Connection establishment is bounded by a configured timeout, and certificate rotation through rolling restart is documented.

Remaining required change:

- add a TLS-enabled Cockroach integration harness proving valid startup, untrusted-CA rejection, hostname mismatch rejection, and required-client-certificate behavior;
- remove the deprecated ambiguous constructors at the next compatibility boundary.

### 5. Transaction cancellation needs protocol-level fault evidence

Implemented since the audit: a transaction checkout is marked uncertain before `BEGIN` and is returned to the idle pool only after a complete `COMMIT` or `ROLLBACK` response. Cancellation, phase timeout, failed rollback, and ambiguous commit evict the connection. Connect, transaction, commit, and rollback phases have explicit deadlines; ambiguous commits return `CommitOutcomeUnknown`; and `40001` retries use bounded exponential jitter.

Remaining required change:

- add protocol-stub and real Cockroach tests that cancel or disconnect at every transaction await and verify the next checkout uses a clean replacement connection;
- apply the same timeout/eviction wrapper to every remaining non-transactional SQL statement.

### 6. Queries and archives materialize unbounded data

A page query loads all matching hot rows and complete JSON archives, globally sorts them, then applies the cursor and limit. Snapshots and JSON encoding also allocate a complete bucket in memory.

Required change:

- enforce maximum strategy buckets, rows, bytes, value size, and execution time;
- push `(event_time, sort_key)` cursor predicates and limits into Cockroach;
- replace JSON blobs with Parquet plus row-group metadata or another streaming format;
- stream sorted batches and merge sources incrementally;
- define behavior for `query()` rather than using an effective unlimited page.

### 7. The production durable stack is not tested end to end

No test combines:

```text
DurableTable
+ CockroachDurableBucketStore
+ DurableArchiveCoordinator
+ OpenDalArchiveStore
+ S3 fault
+ concurrent load
```

The current node-loss test stops Cockroach node 2 while clients remain connected through node 1. It tests quorum tolerance, not client gateway loss/reconnection. Load runs on separate shards from the bucket being archived.

Required change:

- add concurrent append-versus-cutover and two-archiver tests;
- route clients through Toxiproxy and fault the active gateway;
- add worker kill/restart, lease expiry/takeover, and post-publication cleanup tests;
- set throughput, drop-rate, latency, and recovery-time assertions;
- run the fault matrix in CI.

### 8. Toxiproxy fault scripts are executable locally but not yet in CI

Compose now defines an `e2e` runner, loads `docker/toxiproxy/toxiproxy.json`, publishes proxy listeners for CockroachDB and MinIO, and directs host and container E2E clients through those listeners. The scripts use the published Toxiproxy API port `18474`, remove injected toxics on exit, and have passed locally.

Remaining required change:

- validate `run-e2e.sh` and every fault script in CI rather than only shell syntax; retain logs, fault timing, and recovery evidence.

## P1: required engineering and operational work

### API and architecture

- Split the public data-plane interface from crate-private archive-control mutations. A consumer can currently call low-level publication/deletion methods without the coordinator.
- Deprecate or feature-gate the legacy `Table`, `ArchiveCoordinator`, `CockroachHotStore`, and `CockroachMetadataStore` path so the safer durable route is unambiguous.
- Extract shared write-window, bucket planning, merge, cursor, and pagination policy from the duplicated legacy and durable table implementations.
- Move in-memory implementations out of `ports/storage.rs` into `adapters/memory` in a later compatibility-preserving cleanup.
- Add input limits for partition keys, sort keys, values, ranges, and object keys.
- Document duplicate append/idempotency semantics.

### Database and migrations

- [x] Remove runtime schema migration. `pulpitum-migrate` is the dedicated, short-lived schema/bootstrap command; sidecar, archiver, and showcase workload startup no longer run `migrate()`.
- [x] Use separate migration and runtime roles with least privilege. `pulpitum-migrate` grants the runtime role only DML privileges and revokes schema/database `CREATE`; the showcase composes migration as a job and runs services as `pulpitum_runtime`.
- [ ] Replace the bootstrap `CREATE TABLE IF NOT EXISTS` implementation with numbered, reviewable migrations and version tracking.
- Define compatibility, rolling-upgrade, rollback/forward-fix, backup, and restore procedures.
- Add schema-upgrade tests from every supported version.
- Review Cockroach locality/multi-region requirements before choosing regional table settings; no topology is currently declared.

### S3 and credentials

Implemented since the audit: `S3ArchiveConfig` supports the standard OpenDAL/AWS credential chain, optional temporary static credentials and session tokens, region selection, HTTPS-by-default endpoint validation, and SSE-S3/SSE-KMS. The binaries expose these controls through environment settings; non-loopback HTTP requires an explicit development override.

Remaining work:

- document and test least-privilege IAM, workload-identity refresh, bucket versioning, retention/lifecycle, orphan cleanup, and disaster recovery;
- classify retryable object errors and add bounded backoff/jitter in addition to the operation deadline;
- test SSE and temporary-credential behavior against real S3.

### Observability

- Instrument durable SQL operations; current durable-path visibility does not match the dashboards.
- Export current archive phase and phase start time so stalled-archive alerts are reliable.
- Emit durable bucket-tier and pending-cleanup metrics.
- Sanitize raw Cockroach/OpenDAL errors before telemetry export.
- Add cardinality/privacy tests for labels and span fields.
- Harden the Collector sidecar with loopback binding where appropriate, probes, security contexts, NetworkPolicy, queue/retry storage, and a real trace backend.
- Validate Prometheus rules with `promtool` and dashboards against representative durable traffic.

### Delivery and supply chain

- Add CI for format, strict Clippy, tests, docs, MSRV/current Rust, feature combinations, package verification, E2E/fault tests, RustSec, licenses, and Compose validation.
- Add a protected release pipeline with SBOMs, signatures, provenance attestations, and image scanning.
- Add `repository`, `documentation`, and `homepage` once authoritative URLs are known.
- Track or remove unmaintained `paste 1.0.15` through a DataFusion/Parquet upgrade; document any temporary advisory exception with an owner and expiry.
- Add `SECURITY.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, support policy, compatibility policy, and release process.

### Deployment

- Keep `docker-compose.showcase.yml` explicitly local-only. It uses insecure CockroachDB, default MinIO/Grafana credentials, anonymous Grafana, and privileged cAdvisor access.
- Create a separate hardened production image: non-root runtime, read-only filesystem, dropped capabilities, health/readiness endpoints, graceful shutdown, resource limits, immutable base/image digests, OCI metadata, and secret injection.
- Provide Helm/Kustomize or equivalent with ServiceAccounts/workload identity, NetworkPolicy, PDB, probes, resources, and migration jobs.
- Define SLOs, RPO/RTO, capacity limits, alert ownership, incident response, safe rolling upgrades, and credential/certificate rotation runbooks.

## Reorganized source architecture

The audit reorganized the previously flat `src/` directory into layers while preserving existing crate-root exports:

```text
src/
├── lib.rs                    # public compatibility facade
├── domain/                   # records, buckets, queries, table definitions
├── application/              # durable table and archive workflows
├── ports/                    # storage contracts
├── adapters/                 # CockroachDB and OpenDAL implementations
├── integrations/             # DataFusion and OpenTelemetry
├── legacy/                   # original split-store compatibility path
├── dev_support/              # shared load-generation support
└── tests.rs                  # crate-level behavioral tests
```

This is an organizational improvement, not a correctness claim. The most important next structural work is to separate in-memory adapters from ports, split the large durable Cockroach adapter into migration/transaction/repository modules, and isolate archive-control capabilities from the public data plane.

## Recommended release sequence

### Milestone 1: safety model

1. Harden v4 `TableId` namespacing with numbered schema migrations and an explicit v3-to-v4 backfill.
2. Add immutable payloads and verified manifests.
3. Add durable archival jobs, lease renewal/takeover, and resumable cleanup.
4. Make transaction pooling cancellation-safe and add operation deadlines.
5. Add secure Cockroach TLS and production S3 credentials.

### Milestone 2: executable evidence

1. Build the full durable Cockroach + S3 E2E scenario.
2. Repair Toxiproxy routing and fault scripts.
3. Test concurrent writes/archivers and crash at every phase.
4. Test corruption, stale uploads, gateway loss, and ambiguous database outcomes.
5. Establish performance and recovery thresholds.

### Milestone 3: scale and operations

1. Add streaming storage/query execution and hard resource budgets.
2. Move to versioned migrations and least-privilege roles.
3. Complete durable observability and alert validation.
4. Add production deployment, CI/release, supply-chain, backup/restore, and incident artifacts.

## Production acceptance criteria

A production release should not be declared until all of these are true:

- logical tables cannot collide in SQL or object storage;
- every published archive is immutable, versioned, and checksum-verified;
- a crash at any archive phase converges automatically without data loss or permanent write unavailability;
- stale owners cannot publish, overwrite, or delete after takeover;
- pooled Cockroach sessions are clean after cancellation and failed rollback;
- all external operations are bounded and retry policy is explicit;
- Cockroach and S3 credentials/transports meet production security requirements;
- queries and snapshots enforce tested memory/time/row/byte budgets;
- the full durable stack passes multi-worker fault tests in CI;
- migrations, backups, restores, rolling upgrades, and rollback/forward-fix are exercised;
- production dashboards, alerts, privacy controls, and runbooks are validated;
- release artifacts are reproducible, scanned, signed, and supported by a documented policy.
