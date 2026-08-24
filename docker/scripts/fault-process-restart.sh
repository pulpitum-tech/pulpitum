#!/usr/bin/env sh
set -eu

./docker/scripts/run-e2e.sh

echo 'Restarting CockroachDB node 1 (the node behind cockroach-client)...'
docker compose restart cockroach-1

attempt=0
until docker compose exec -T cockroach-1 cockroach sql --insecure --host=localhost:26257 --execute='SELECT 1' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    echo 'CockroachDB node 1 did not become ready after restart.' >&2
    exit 1
  fi
  sleep 1
done

docker compose --profile e2e run --rm e2e
