# Celeborn test runtime

This directory provides a self-contained Celeborn runtime harness for shuffle tests
inside the development container.

## Prerequisites

- `java` in PATH (JDK 11+)
- network access to clone Celeborn source from GitHub
- Spark shaded client jar available under `/tmp/bolt-celeborn-runtime-$USER/celeborn-bin/spark`

## Scripts

- `scripts/run_e2e.sh [--build-type Debug|Release]`: start services, run tests, and cleanup.

## Environment variables

- `BOLT_CELEBORN_GIT_REPO` (default `https://github.com/apache/celeborn.git`)
- `BOLT_CELEBORN_GIT_REF` (default `81d89f3`, aligned with cpp-client recipe)
- `BOLT_CELEBORN_MASTER_HOST` (default `127.0.0.1`)
- `BOLT_CELEBORN_MASTER_PORT` (default `19097`)
- `BOLT_CELEBORN_NUM_WORKERS` (default `$(nproc)`, number of worker instances on localhost)
- `BOLT_CELEBORN_WORKER_BASE_PORT` (default `19098`)
- `BOLT_CELEBORN_TEST_PATTERNS` (optional comma-separated ctest patterns)
- `BOLT_CELEBORN_CTEST_TIMEOUT_SECONDS` (default `7200`)

`run_e2e.sh` sets `BOLT_CELEBORN_E2E=1` and `BOLT_SHUFFLE_TEST_REAL_CELEBORN=1` automatically.
The script uses `_build/<build-type>` as the build directory.
