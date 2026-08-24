# syntax=docker/dockerfile:1

FROM rust:1.94-bookworm AS build
ARG CARGO_BUILD_JOBS=2
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Cargo validates the example declared in Cargo.toml while parsing the manifest.
COPY examples/showcase.rs ./examples/showcase.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release --jobs "$CARGO_BUILD_JOBS" --features sql-sidecar \
      --bin pulpitum-migrate --bin pulpitum-sql-sidecar \
    && cp target/release/pulpitum-migrate /pulpitum-migrate \
    && cp target/release/pulpitum-sql-sidecar /pulpitum-sql-sidecar

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 pulpitum \
    && useradd --uid 10001 --gid pulpitum --create-home --shell /usr/sbin/nologin pulpitum

COPY --from=build --chown=pulpitum:pulpitum /pulpitum-migrate /usr/local/bin/pulpitum-migrate
COPY --from=build --chown=pulpitum:pulpitum /pulpitum-sql-sidecar /usr/local/bin/pulpitum-sql-sidecar

USER 10001:10001
WORKDIR /home/pulpitum

ENV RUST_LOG=pulpitum=info
EXPOSE 5433
ENTRYPOINT ["/usr/local/bin/pulpitum-sql-sidecar"]
