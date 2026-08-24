#!/bin/sh
set -eu

compose='docker compose -f docker-compose.sidecar-smoke.yml'

cleanup() {
  $compose down --volumes --remove-orphans
}

trap cleanup EXIT INT TERM
docker build --file docker/sidecar.Dockerfile --tag pulpitum/sql-sidecar:smoke .
if ! $compose up --detach; then
  $compose logs --no-color
  exit 1
fi

container_id="$($compose ps --all --quiet sql-smoke)"
if [ -z "$container_id" ]; then
  $compose logs --no-color
  exit 1
fi

set +e
docker wait "$container_id"
status=$?
set -e
$compose logs --no-color
exit "$status"
