# `scripts/`

Developer tooling for the Bolt repo.

| File | What it does |
|---|---|
| `setup-dev-env.sh` | One-shot host setup: JDK, Conan, system packages. Run once on a fresh box. |
| `install-bolt-deps.sh` | Pulls Bolt's pinned Conan deps into the local Conan cache. |
| `install-gcc.sh` | Installs the GCC toolchain Bolt builds against (root required). |
| `run-clang-tidy.py` | Runs clang-tidy on changed lines only; used by pre-commit + CI. |
| `launch-spark.sh` | One-click Spark + Gluten + Bolt launcher with a long-lived Connect Server. |
| `connect.py` | pyspark Connect client used by `launch-spark.sh sql` / `shell`. |
| `test-queries.sql` | Smoke-test SQL covering scan/agg/join paths for Bolt offload verification. |
| `tpcds.sh`, `tpcds.py` | TPC-DS benchmark runner; data gen via dsdgen, queries via the Connect Server. |
| `conan/` | Conan profiles consumed by Bolt's build. |

## `launch-spark.sh`

One-click Spark + Gluten + Bolt launcher. Builds Bolt, auto-clones the Gluten
fork into `$GLUTEN_HOME` (default `/tmp/spark-bolt/gluten`) if missing,
locates / builds the Gluten bundle JAR, then starts a long-lived Spark
Connect Server in standalone mode (one master + one worker JVM). Exposes:

```bash
scripts/launch-spark.sh start                            # start server (reuses cached Bolt + Gluten JAR)
scripts/launch-spark.sh start --build                    # rebuild Bolt + Gluten first, then start
scripts/launch-spark.sh sql -e "select count(*) from range(1e7)"
scripts/launch-spark.sh shell                            # Python REPL
scripts/launch-spark.sh stop                             # stop server
scripts/launch-spark.sh status                           # show state
```

See `scripts/launch-spark.sh --help` for env vars (ports, JAVA_HOME, etc.).

## Dependencies

### JDK 17

Auto-detected by `launch-spark.sh` under `/usr/lib/jvm/{java-17-*,temurin-17-*}`,
or set `JAVA_HOME`.

### Python (only for `launch-spark.sh sql` / `shell` / TPC-DS)

- **uv** — manages a private Python 3.11 venv that the launcher uses for
  pyspark. No sudo needed:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
  pyspark itself is installed automatically into the venv on first
  `launch-spark.sh start`.

### Optional but recommended

- **aria2c** — parallel download, ~10× faster than curl for Spark's
  `archive.apache.org` mirror. `launch-spark.sh` falls back to curl if
  missing.
