# Gluten CI tasks

Bolt owns this workflow implementation and its deployment. Apache Gluten supplies
backend source and native/Spark test compatibility fixes, not CI helper scripts.

The Bolt PR tier compiles native libraries, runs the native CTest suite and
verifies the selected Bolt package, linkage, archive identity and nonempty backend
output. Failed or missing test evidence fails the gate. It does not run benchmarks
or Spark/JVM tasks. The external ECS controller retains its compile-only tier.
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

The callable/manual workflow is `.github/workflows/bolt_backend_x86.yml`.
Its composite action is `.github/actions/bolt-ci-task/action.yml`. Both select
helpers from the exact Bolt caller revision; source comes from Apache Gluten.
The external controller exports the same scripts from its reviewed Bolt bundle.
There is no fork fallback and no runtime source patching.

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
