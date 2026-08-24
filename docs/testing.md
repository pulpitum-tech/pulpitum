# Correctness and fault-test inventory

Run routine tests with `cargo test`. Run real-adapter fault tests with `./docker/scripts/run-e2e.sh`. Run the known-failure inventory with `./docker/scripts/run-known-failures.sh`; that command succeeds only when the listed specifications fail.

| Scenario | Test | Status | Required production change |
|---|---|---:|---|
| Cassandra-style clustering-key declaration and validation | `src/tests.rs::table_definition_makes_the_cassandra_style_physical_clustering_key_explicit` | passing | none |
| Cursor crosses archived/hot bucket boundary | `src/tests.rs::cursor_pagination_is_contiguous_across_archived_and_hot_years` | passing | predicate/limit pushdown for scale |
| Durable table cursor crosses archive/hot boundary | `src/tests.rs::durable_table_routes_hot_and_archived_buckets_with_contiguous_cursors` | passing | Cockroach integration coverage |
| Durable upload failure reopens the bucket | `src/tests.rs::durable_archiver_aborts_only_the_prepublication_upload_failure` | passing | Cockroach integration coverage |
| Durable coordinator discovers and archives eligible work | `src/tests.rs::durable_recovery_runner_archives_discovered_bucket_without_an_application_trigger` | passing | Cockroach integration coverage |
| Durable coordinator recovers published cleanup after worker loss | `src/tests.rs::durable_recovery_runner_claims_and_finishes_published_cleanup_after_owner_loss` | passing | independent-process coverage |
| Durable coordinator renews ownership through a slow upload | `src/tests.rs::durable_recovery_runner_renews_its_lease_during_a_slow_upload` | passing | real Cockroach/MinIO slow-operation coverage |
| Content-addressed conditional archive writes and manifest tamper detection | `src/adapters/opendal_store.rs` unit tests | passing | concurrent MinIO conditional-create coverage |
| Real Cockroach/MinIO published-cleanup takeover after a modeled worker loss | `tests/e2e.rs::durable_recovery_runner_takes_over_published_cleanup_after_worker_loss` | opt-in | independent container kill/restart coverage |
| Upload outage never publishes archive routing (legacy split path) | `tests/e2e.rs::storage_outage_cannot_lose_or_cut_over_a_bucket` | opt-in | run through Compose/MinIO |
| Legacy worker restart after hot deletion | `tests/known_failures.rs::worker_restart_after_hot_deletion_loses_the_archive_route` | deliberately failing | legacy split-store limitation; replace with durable restart/cleanup coverage |
| Legacy worker cancellation during upload | `tests/known_failures.rs::cancelled_archival_worker_does_not_leave_a_bucket_unavailable` | deliberately failing | durable job ownership, TTL leases, resumable state machine |
| Concurrent archivers | not implemented | blocked | fencing token and CAS transition test harness |
| Metadata/database partition | not implemented | blocked | independent worker processes and Toxiproxy routes |
| Object-store partial upload/corruption | unit coverage only | partial | real MinIO partial-write and corruption fault injection |
| Query drain while cutover commits | not implemented | blocked | durable read leases plus clock/lease-expiry test control |

The `blocked` cases are intentionally documented rather than represented by misleading passing tests. The durable Cockroach route has moved archive fencing and records into one store, but durable read leases and multi-worker crash/takeover coverage are still needed before those scenarios can become executable.
