# Celeborn test runtime

This directory provides a self-contained Celeborn runtime harness for shuffle tests
inside the development container.

## Prerequisites

- `java` in PATH (JDK 11+)
- network access to clone Celeborn source from GitHub
- Spark shaded client jar available in `${CELEBORN_HOME}/spark` or provided via `BOLT_CELEBORN_LM_HELPER_JAR_PATH`

## Scripts

- `scripts/run_e2e.sh [--build-type Debug|Release] [build_dir]`: start services, run tests, and cleanup.

## Environment variables

- `BOLT_CELEBORN_RUNTIME_DIR` (default `/tmp/bolt-celeborn-runtime-$USER`)
- `BOLT_CELEBORN_SOURCE_HOME` (default `$BOLT_CELEBORN_RUNTIME_DIR/celeborn-src`)
- `BOLT_CELEBORN_GIT_REF` (default `81d89f3`, aligned with cpp-client recipe)
- `BOLT_CELEBORN_HOME` (default `$BOLT_CELEBORN_RUNTIME_DIR/celeborn-bin`)

`run_e2e.sh` sets `BOLT_CELEBORN_E2E=1` and `BOLT_SHUFFLE_TEST_REAL_CELEBORN=1` automatically.
Without `build_dir`, it defaults to `_build/<build-type>`.
