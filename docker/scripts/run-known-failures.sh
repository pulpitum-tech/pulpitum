#!/bin/sh
# Executes adversarial specifications that are expected to fail today.
set -u

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root" || exit 1

cargo test --test known_failures -- --ignored --nocapture
status=$?
if [ "$status" -eq 0 ]; then
  echo 'WARNING: no known-failure test failed; update the expected-failure inventory.' >&2
  exit 1
fi

echo 'Known distributed-safety failure reproduced as expected.'
exit 0
