# Gluten CI

PR checks build the candidate Bolt as a local Conan package, compile
`apache/gluten` main with it, and run native CTest. Nightly runs at 02:00
Asia/Shanghai with Bolt main and Apache Gluten main. Each run resolves the
Gluten revision once. Gluten native checks run again in the merge queue, even
when Bolt-only results are reused.

`.github/workflows/gluten-ci.yaml` owns checkouts, matrices, caches and artifacts.
`scripts/gluten-ci.sh` adapts Bolt packaging/runtime setup and checks fresh test
reports. Native builds use the existing Conan/CMake entry points with matching
profiles; tests use CTest and Gluten's Maven lifecycle. `make test_spark35` is
not used because it ignores test failures. No Gluten fork or source patch is needed.

Nightly preserves 39 tasks: native UT, 12 package configurations and 26
Spark/JVM/connector groups for Spark 3.5, 4.0, 4.1 and 4.2. Spark 3.4 is excluded.
The loader and Delta backend suites use separate JVMs to avoid native conflicts.
Conan server packages and shared read-only ccache reuse the existing ECS setup.
[Cache deployment](../.github/runners/arrow-cache.md) can add verified source archives
and trusted Maven/Spark/pip seeds to the CI image. Jobs use private copies
automatically; without seeds, dependencies download normally. Only ccache has
explicit cache configuration in the workflow. No cache service address is required.

Use the reusable workflow's manual `full` mode to validate the complete matrix.
CI helpers and the engine use the same selected Bolt revision. Native artifacts
are downloaded only from the current run and checked against both source SHAs.

```bash
python3 -m unittest discover -s .github/scripts/tests -p 'test_gluten*.py' -v
```

Suite selection and Maven source-profile checks belong to the Gluten repository.
Fixture tests do not establish successful native or full Spark execution.
