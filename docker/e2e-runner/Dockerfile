FROM rust:1.94-bookworm

WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
# Cargo validates declared examples while parsing the manifest, even though this
# runner executes only the transport-level E2E integration target.
COPY examples ./examples

# The default command is intentionally a cargo integration-test invocation so
# `docker compose run e2e` runs only the opt-in infrastructure checks.
CMD ["cargo", "test", "--locked", "--test", "e2e_environment", "--", "--nocapture"]
