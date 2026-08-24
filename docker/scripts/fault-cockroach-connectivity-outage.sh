#!/usr/bin/env sh
set -eu

./docker/scripts/run-e2e.sh

api=http://localhost:18474
cleanup() {
  curl --fail --silent --show-error --request DELETE "$api/proxies/cockroach-client/toxics/cockroach-outage-upstream" >/dev/null || true
  curl --fail --silent --show-error --request DELETE "$api/proxies/cockroach-client/toxics/cockroach-outage-downstream" >/dev/null || true
}
trap cleanup EXIT INT TERM

# Block the PostgreSQL request and response directions through the client proxy.
curl --fail --silent --show-error --request POST "$api/proxies/cockroach-client/toxics" \
  --header 'Content-Type: application/json' \
  --data '{"name":"cockroach-outage-upstream","type":"timeout","stream":"upstream","attributes":{"timeout":0}}' >/dev/null
curl --fail --silent --show-error --request POST "$api/proxies/cockroach-client/toxics" \
  --header 'Content-Type: application/json' \
  --data '{"name":"cockroach-outage-downstream","type":"timeout","stream":"downstream","attributes":{"timeout":0}}' >/dev/null

docker compose --profile e2e run --rm -e E2E_EXPECT_COCKROACH=down e2e
