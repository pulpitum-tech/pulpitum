# Capability and safety posture

Pulpitum intentionally exposes a narrow contract. Unsupported behavior is rejected rather than approximated.

## Supported now

| Capability | Status | Boundary |
|---|---|---|
| v4 logical-table namespacing | Supported | Stable `TableId` plus namespaced SQL/object keys. |
| Append-oriented writes | Supported | Writes are fenced by durable CockroachDB metadata. |
| Bounded partition-local reads | Supported | One partition plus a finite time range. |
| Secure CockroachDB transport | Supported | `sslmode=require`, CA validation, optional mTLS. |
| Secure PgWire sidecar | Supported | TLS + SCRAM, one configured user/database, narrow SQL surface. |
| Schema lifecycle | Supported | Append-only migration history with checksum and catalog validation. |
| Hot-only operation | Supported safe default | No hot data is deleted. |

## Explicitly blocked or opt-in

| Capability | Policy | Reason |
|---|---|---|
| Deployment-owned archival | Disabled by default | It eventually deletes the hot copy. Set `PULPITUM_ARCHIVAL_ENABLED=true` only after fault acceptance. |
| SQL transactions and batching | Rejected | The sidecar is autocommit-only; it will not emulate transaction semantics. |
| DDL, permissions SQL, and multi-statements | Rejected | Schema and privileges belong to the dedicated migrator. |
| v3-to-v4 automatic conversion | Unsupported | No authoritative legacy schema/mapping is available to convert safely. |
| Orphan archive-object deletion | Unsupported | Objects may leak; automated deletion is intentionally fail-closed. |
| Arbitrary SQL and cross-partition analytics | Unsupported | Use a dedicated analytical system where that is the product requirement. |

## Before enabling archival

Archival requires more than valid object credentials. Confirm all of the following for the target environment:

1. CockroachDB and object-store traffic are observable and alertable.
2. The exact object-storage retention/versioning policy is approved.
3. The Docker fault matrix has passed for the deployed versions.
4. Recovery ownership, operator escalation, and backup/restore runbooks are approved.
5. A rollback/forward-fix plan exists for the specific schema and deployment version.

The detailed unresolved evidence is tracked in the [production-readiness audit](production-readiness.md).

## Important distinction: client and internal transactions

The PgWire gateway rejects client-issued transaction statements. Pulpitum still uses internal serializable CockroachDB transactions for its write and archive fences. Those internal transactions are necessary for correctness and are not exposed as client transaction semantics.
