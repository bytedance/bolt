# Gluten UT timing comparison

`run.sh` builds with `mvn clean install -DskipTests`, discovers ScalaTest and
JUnit classes, then dispatches isolated JVM jobs in parallel. It applies
`blacklist.txt` to the results.

`run_maven.sh` is an experimental baseline using the `clean test` lifecycle
from Gluten's `.github/workflows/velox_backend_x86.yml`: a serial Maven reactor
and the POM's Surefire / ScalaTest plugins. It uses the same Bolt profiles,
tag exclusions and JVM limits as `run.sh`. It does not shard suites. Unlike
upstream Velox's three normal-test runners and two slow-test runners, this
baseline runs all tags allowed by the Bolt runner on one machine.

Build the native backend and patched Arrow jars first, and prepare the Spark
binary distribution plus source SQL test resources as in `bolt_gluten_ut.yml`.
Run both methods sequentially so they do not compete for CPU or memory:

```bash
export GLUTEN_HOME=/path/to/gluten
export SPARK_HOME=/path/to/spark_home
export JAVA_HOME=/path/to/jdk17
export PATH="$JAVA_HOME/bin:$PATH"
export PARALLEL_LOG_DIR="$PWD/scripts/gluten_ut/logs/parallel"

LOG_DIR="$PARALLEL_LOG_DIR" bash scripts/gluten_ut/run.sh
# Run this even if the tests above failed, provided the JVM build succeeded.
LOG_DIR="$PWD/scripts/gluten_ut/logs/maven" bash scripts/gluten_ut/run_maven.sh
```

For local test-only iteration, `SKIP_INSTALL=1` lets `run.sh` reuse compiled
classes. Label such measurements separately from full build-and-test timings.
Both scripts accept `MVN_PROFILES`, `TAGS_TO_EXCLUDE` and `JOB_JVM_OPTS` overrides;
apply identical overrides when comparing them. The defaults reproduce the CI
JDK and build profiles, without `fast-build`.

The Maven script records its exact command and Gluten commit in `_command.txt`,
raw output in `_maven.log`, and measurements in `_summary.json` / `_summary.md`.
It archives fresh XML and text reports under `reports/`. `_phases.tsv` records
the parallel runner's cumulative and previous-phase wall time in seconds.
Pull requests run the parallel gate. To also measure Maven in CI, manually run
the workflow with `compare_maven=true`. The comparison repeats the tests on the
same runner and can take much longer than the parallel gate. Its job Summary
includes both timings, and both runners' logs are uploaded in
`bolt-gluten-ut-reports`.
GitHub requires the workflow file to exist on the default branch before
[manual dispatch is available](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).

Interpret the measurements with these differences in mind:

- Total time includes different build work: `clean install -DskipTests` plus
  discovery for the parallel runner, versus `clean test` for Maven. Maven test
  time sums the serial test-plugin intervals, including discovery and JVM
  startup. Parallel test time is the dispatch interval.
- Both runs reuse downloaded dependencies; the second may benefit from cache
  entries populated by the first. Neither number includes the native build.
- Maven's POM-based discovery can cover fewer suites. For example, the Gluten
  revision used locally does not enable the ScalaTest plugin in `gluten-arrow`.
  The summary records executed and skipped tests separately and lists classes
  discovered by `run.sh` that have no Maven XML report. Excluded and aborted
  suites can also appear in this list.
- Maven continues after failures to measure later modules. The report checker
  returns failure for failed tests, suite aborts, missing reports or incomplete
  runs. It reports raw failures without the parallel runner's blacklist.
  The experimental CI step uses `continue-on-error`; the parallel runner
  remains the required test gate.

Validate the scripts with:

```bash
python3 -m unittest discover -s scripts/gluten_ut/tests -v
pre-commit run --files .github/workflows/bolt_gluten_ut.yml \
  scripts/gluten_ut/{run.sh,run_maven.sh,summarize_maven.py,README.md} \
  scripts/gluten_ut/tests/test_maven.py
```
