#!/usr/bin/env sh
set -eu

./docker/scripts/run-e2e.sh

api=http://localhost:18474
cleanup() {
  curl --fail --silent --show-error --request DELETE "$api/proxies/minio-s3/toxics/s3-outage-upstream" >/dev/null || true
  curl --fail --silent --show-error --request DELETE "$api/proxies/minio-s3/toxics/s3-outage-downstream" >/dev/null || true
}
trap cleanup EXIT INT TERM

# Block both request and response flow. The Rust check performs an HTTP read, so
# a Toxiproxy listener accepting TCP cannot be mistaken for a healthy S3 path.
curl --fail --silent --show-error --request POST "$api/proxies/minio-s3/toxics" \
  --header 'Content-Type: application/json' \
  --data '{"name":"s3-outage-upstream","type":"timeout","stream":"upstream","attributes":{"timeout":0}}' >/dev/null
curl --fail --silent --show-error --request POST "$api/proxies/minio-s3/toxics" \
  --header 'Content-Type: application/json' \
  --data '{"name":"s3-outage-downstream","type":"timeout","stream":"downstream","attributes":{"timeout":0}}' >/dev/null

docker compose --profile e2e run --rm -e E2E_EXPECT_S3=down e2e
