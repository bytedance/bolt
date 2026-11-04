#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

with_arrow_cache=false
case ${1:-} in
  --with-arrow-cache)
    with_arrow_cache=true
    export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}:compose.arrow-cache.yml:compose.arrow-runners.yml"
    export BOLT_ARROW_CACHE_DIR="${BOLT_ARROW_CACHE_DIR:-/data/conan-source-cache}"
    python3 arrow-cache.py warm
    ;;
  "") ;;
  *)
    echo "Usage: $0 [--with-arrow-cache]" >&2
    exit 2
    ;;
esac

# shellcheck disable=SC2086
docker compose build ${BUILD_ARGS:-}
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
if [[ "$with_arrow_cache" == true ]]; then
  docker compose up -d --wait bolt-source-cache
fi
docker compose up -d conan_server runner-medium runner-small
exec docker compose ps
