#!/bin/sh
# Bounded local fault-harness bootstrap. Run from any directory.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root" || exit 1

restore_faulted_services() {
  docker compose unpause minio >/dev/null 2>&1 || true
  docker compose start cockroach-2 minio >/dev/null 2>&1 || true
}
trap restore_faulted_services EXIT INT TERM

# Repair state left by a previously interrupted fault test before readiness checks.
restore_faulted_services

run_bounded() {
  seconds=$1
  shift
  "$@" &
  pid=$!
  deadline=$(( $(date +%s) + seconds ))
  while kill -0 "$pid" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "Timed out after ${seconds}s: $*" >&2
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" || true
      return 124
    fi
    sleep 1
  done
  wait "$pid"
}

# Do not use `docker compose up --wait`: one-shot init services correctly exit
# after succeeding, while --wait may treat that state differently by version.
run_bounded 120 docker compose up -d cockroach-1 cockroach-2 cockroach-3 minio toxiproxy

run_bounded 100 docker compose run --rm cockroach-init

deadline=$(( $(date +%s) + 90 ))
while ! docker compose exec -T cockroach-1 cockroach node status --insecure --host=localhost:26257 >/dev/null 2>&1; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo 'Timed out waiting for CockroachDB after 90 seconds.' >&2
    docker compose ps --all >&2
    docker compose logs --tail=80 cockroach-1 >&2
    exit 1
  fi
  sleep 2
done

run_bounded 30 docker compose run --rm minio-init

# Validate that the E2E client network reaches both dependencies only through
# the configured Toxiproxy listeners before running application scenarios.
run_bounded 180 docker compose --profile e2e run --build --rm e2e

# These intentionally inject outages; the hard bound prevents a failed network
# request or a stuck container from holding a developer or CI runner forever.
# Host-published CockroachDB and MinIO ports are the Toxiproxy listeners, so
# every application request in this test target uses the same faultable path.
run_bounded 300 cargo test --locked --test e2e -- --ignored --nocapture --test-threads=1
