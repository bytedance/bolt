#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

docker compose build ${BUILD_ARGS}
docker compose up -d
# wait a few seconds for the registry to start up
wait 5
docker compose push ci-image
exec docker compose ps
