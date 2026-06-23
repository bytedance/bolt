# `scripts/`

Developer tooling for the Bolt repo.

| File | What it does |
|---|---|
| `setup-dev-env.sh` | One-shot host setup: JDK, Conan, system packages. Run once on a fresh box. |
| `install-bolt-deps.sh` | Pulls Bolt's pinned Conan deps into the local Conan cache. |
| `install-gcc.sh` | Installs the GCC toolchain Bolt builds against (root required). |
| `run-clang-tidy.py` | Runs clang-tidy on changed lines only; used by pre-commit + CI. |
| `launch-spark.sh` | One-click Spark + Gluten + Bolt launcher with a long-lived Connect Server. |
| `launch-spark/connect.py` | pyspark Connect client used by `launch-spark.sh sql` / `shell`. |
| `launch-spark/test-queries.sql` | Smoke-test SQL covering scan/agg/join paths for Bolt offload verification. |
| `conan/` | Conan profiles consumed by Bolt's build. |

## `launch-spark.sh`

One-click Spark + Gluten + Bolt launcher. Builds Bolt, auto-clones the Gluten
fork into `$GLUTEN_HOME` (default `/tmp/spark-bolt/gluten`) if missing,
locates / builds the Gluten bundle JAR, then starts a long-lived Spark
Connect Server in standalone mode (one master + one worker JVM). Exposes:

```bash
# run SQL, which automatically starts a Spark server if one isn't already running.
scripts/launch-spark.sh sql -e "select count(*) from range(1e7)"

# start/stop/status spark server
scripts/launch-spark.sh start                            # start server (reuses cached Bolt + Gluten JAR)
scripts/launch-spark.sh start --build                    # rebuild Bolt + Gluten first, then start
scripts/launch-spark.sh stop                             # stop server
scripts/launch-spark.sh status                           # show state

scripts/launch-spark.sh shell                            # Python REPL
```

See `scripts/launch-spark.sh --help` for env vars (ports, JAVA_HOME, etc.).

### Custom `GLUTEN_HOME`

Point at an existing Gluten checkout instead of letting the launcher clone
into `/tmp/spark-bolt/gluten`. If the checkout already has a built bundle
JAR (`<dir>/package/target/gluten-bolt-bundle-spark3.5*.jar`), `start`
reuses it; otherwise `start --build` will build inside that directory.

```bash
export GLUTEN_HOME=$HOME/work/gluten         # your existing checkout
scripts/launch-spark.sh start                # reuse cached JAR
scripts/launch-spark.sh start --build        # rebuild Bolt + Gluten in $GLUTEN_HOME
```

`GLUTEN_FORK_URL` / `GLUTEN_FORK_BRANCH` override the auto-clone source
when `$GLUTEN_HOME` is empty (defaults: `WangGuangxin/gluten` @
`add_bolt_backend`).

The launcher prefers a Bolt-backend bundle JAR
(`gluten-*bolt*bundle*spark3.5*.jar`) but falls back to a Velox bundle
(`gluten-*velox*bundle*spark3.5*.jar`) if no Bolt one is present. The
detected backend is printed on `start`:

```
OK: Gluten JAR: .../gluten-velox-bundle-spark3.5_*.jar (backend=velox)
```

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
