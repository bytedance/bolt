# Gluten CI tasks

Bolt owns this workflow implementation and its deployment. Apache Gluten supplies
backend source and native/Spark test compatibility fixes, not CI helper scripts.

The Bolt PR tier compiles native libraries, runs the native CTest suite and
verifies the selected Bolt package, linkage, archive identity and nonempty backend
output. Failed or missing test evidence fails the gate. It does not run benchmarks
or Spark/JVM tasks.
The nightly tier runs 39 tasks: one native CTest task, 12 package configurations
and 26 Spark/JVM/connector tasks. Spark 3.4 is intentionally outside this scope.

## Coverage

- Spark 3.5, 4.0, 4.1 and 4.2: three standard groups, extended SQL and slow Hive.
- Spark 3.5 Scala 2.13: three extra groups; local UDF, JVM/Delta and TPC checks
  use the dedicated Spark 3.5 cells.
- Packaging covers supported Scala/JDK combinations and connector profiles.
- Spark 4 uses per-version inventories and shared Bolt settings. Explicit
  exclusions cover backend-specific planner contracts and missing Bolt golden
  fixtures. The selection tests compare the actual Apache checkout to Velox.
- Cluster UDF, distributed shuffle, remote storage, chaos, GPU and separate
  ANSI/enhanced workflows are outside this local x86 matrix.

## Entry points

The callable/manual workflow is `.github/workflows/bolt_backend_x86.yml`, called
locally by Bolt's PR and nightly workflows. PRs use the same candidate commit for
the Bolt engine, composite action, helpers and Spark driver. Nightly uses the
main commit for all four. There is no fork workflow dependency or runtime
patching of Apache Gluten source. These workflows follow Bolt's existing PR
review model.

PRs resolve Apache main before checking merge-queue reuse. Reuse requires matching
Gluten SHA evidence from the trusted gate; old records without it are misses.
The gate begins preserving this evidence after this change reaches main.

The CLI uses the selected driver checkout; `GLUTEN_CI_DRIVER_SHA` can require an
exact revision when reproducing a run. It requires clean Gluten, Bolt and driver
checkouts before building or consuming commit-keyed artifacts, including no
untracked source files.
Each task uses separate writable caches and report directories. Native artifacts
are checked before downstream reuse. Test mode requires nonzero passing evidence;
compile mode explicitly records zero tests. PR artifacts cannot substitute for
full nightly artifacts. Use `--native-only` for the native PR tier and
`--compile-only` for compilation without UT.

## Validation

```bash
GLUTEN_TEST_SOURCE=/path/to/apache-gluten python3 -m unittest discover -s scripts/gluten-ci -v
```

The Apache checkout must include the prerequisite source/test changes. Without
`GLUTEN_TEST_SOURCE`, only the cross-repository Spark selection tests are skipped.
Fixture tests do not establish that the full native/Spark matrix has executed.
