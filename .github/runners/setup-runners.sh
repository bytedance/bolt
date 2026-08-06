#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

# shellcheck disable=SC2086
docker compose build ${BUILD_ARGS}
docker compose up -d bolt-registry

for attempt in $(seq 1 45); do
  if docker compose exec -T bolt-registry \
    wget -q --spider http://localhost:5000/v2/; then
    break
  fi
  if [ "$attempt" -eq 45 ]; then
    echo "Registry API did not become ready" >&2
    exit 1
  fi
  sleep 2
done

docker compose push ci-image
docker compose push conan_server
docker compose up -d --wait --wait-timeout 90 bolt-registry
docker compose up -d conan_server runner-medium runner-small
exec docker compose ps
