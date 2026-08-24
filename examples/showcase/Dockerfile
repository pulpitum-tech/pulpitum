# syntax=docker/dockerfile:1
FROM rust:1.94-bookworm AS build
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples/showcase.rs ./examples/showcase.rs

COPY examples/showcase ./examples/showcase
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release --bin pulpitum-migrate --bin pulpitum-archiver --bin pulpitum-sql-sidecar --example showcase --features showcase \
    && cp target/release/pulpitum-migrate /pulpitum-migrate \
    && cp target/release/pulpitum-archiver /pulpitum-archiver \
    && cp target/release/examples/showcase /pulpitum-showcase \
    && cp target/release/pulpitum-sql-sidecar /pulpitum-sql-sidecar

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /pulpitum-migrate /usr/local/bin/pulpitum-migrate
COPY --from=build /pulpitum-archiver /usr/local/bin/pulpitum-archiver
COPY --from=build /pulpitum-showcase /usr/local/bin/pulpitum-showcase
COPY --from=build /pulpitum-sql-sidecar /usr/local/bin/pulpitum-sql-sidecar
ENV RUST_LOG=pulpitum=info
ENTRYPOINT ["/usr/local/bin/pulpitum-showcase"]
