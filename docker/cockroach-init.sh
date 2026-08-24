#!/bin/sh
set -eu

# `cockroach node status` requires an initialized cluster, so it cannot be used
# as the readiness gate for first initialization. Retry init itself instead.
attempt=0
while [ "$attempt" -lt 45 ]; do
  set +e
  output="$(cockroach init --insecure --host=cockroach-1:26257 2>&1)"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    echo 'CockroachDB cluster initialized'
    break
  fi
  case "$output" in
    *"cluster has already been initialized"*)
      echo 'CockroachDB cluster was already initialized; continuing'
      break
      ;;
  esac

  attempt=$((attempt + 1))
  echo "CockroachDB init is not ready (attempt ${attempt}/45): ${output}" >&2
  sleep 2
done

if [ "$attempt" -eq 45 ]; then
  echo 'Timed out waiting to initialize CockroachDB after 90 seconds.' >&2
  exit 1
fi

attempt=0
while [ "$attempt" -lt 45 ]; do
  live_nodes="$(
    cockroach sql --insecure --host=cockroach-1:26257 --format=csv \
      -e 'SELECT count(*) FROM crdb_internal.gossip_nodes WHERE is_live' 2>/dev/null \
      | awk -F, 'NR == 2 { print $1 }'
  )"
  if [ "$live_nodes" = 3 ]; then
    echo 'CockroachDB cluster has three live nodes'
    exit 0
  fi

  attempt=$((attempt + 1))
  echo "CockroachDB is waiting for three live nodes (attempt ${attempt}/45; found ${live_nodes:-0})" >&2
  sleep 2
done

echo 'Timed out waiting for three live CockroachDB nodes after 90 seconds.' >&2
exit 1
