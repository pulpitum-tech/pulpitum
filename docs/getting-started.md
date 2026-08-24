# Getting started

## Prerequisites

Pulpitum targets Rust **1.94** and uses CockroachDB plus an S3-compatible object store for the complete hot/cold workflow.

For a local demonstration, use the full showcase:

```sh
cargo run --example showcase --features showcase
```

The [showcase guide](https://github.com/pulpitum-tech/pulpitum/tree/main/examples/showcase) provisions CockroachDB, MinIO, OpenTelemetry, Prometheus, Grafana, and Jaeger.

## Create the v4 schema

Run the privileged migration job separately from runtime services:

```sh
COCKROACH_CA_CERT_PATH=/var/run/secrets/cockroach/ca.crt \
COCKROACH_MIGRATION_URL='postgresql://migration-role@db.example/defaultdb?sslmode=require' \
  cargo run --bin pulpitum-migrate
```

The migrator applies the append-only schema history, validates its checksums and v4 catalog shape, then grants a dedicated runtime role DML-only permissions.

## Run the SQL sidecar securely

The optional sidecar exposes the built-in chat schema through PostgreSQL wire protocol. Production mode requires TLS and a password file:

```sh
COCKROACH_CA_CERT_PATH=/var/run/secrets/cockroach/ca.crt \
COCKROACH_URL='postgresql://pulpitum_runtime@db.example/defaultdb?sslmode=require' \
PULPITUM_SQL_LISTEN_ADDR='0.0.0.0:5433' \
PULPITUM_SQL_TLS_CERT_PATH=/var/run/secrets/sql-sidecar/tls.crt \
PULPITUM_SQL_TLS_KEY_PATH=/var/run/secrets/sql-sidecar/tls.key \
PULPITUM_SQL_PASSWORD_FILE=/var/run/secrets/sql-sidecar/password \
  cargo run --bin pulpitum-sql-sidecar --features sql-sidecar
```

Clients should validate the server certificate and require channel binding:

```text
postgresql://pulpitum@sql.example:5433/pulpitum?sslmode=verify-full&sslrootcert=/path/to/ca.crt&channel_binding=require
```

## Keep archival disabled initially

The safe default is to retain all data in CockroachDB. Do **not** start the archiver unless the deployment has completed its archival fault acceptance criteria:

```sh
PULPITUM_ARCHIVAL_ENABLED=true \
  cargo run --bin pulpitum-archiver --features showcase
```

See [Capabilities](capabilities.md) and the [archival coordinator guide](archival-coordinator.md) before enabling it.

## Validate a checkout

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

The real CockroachDB/MinIO/Toxiproxy matrix is opt-in:

```sh
./docker/scripts/run-e2e.sh
```
