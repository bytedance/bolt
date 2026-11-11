# Apache Gluten prerequisites

Bolt owns all CI scripts, workflows, matrix and deployment. Apache Gluten needs
only the native and test compatibility changes listed below. Spark 3.4 is not
part of this scope. The nightly matrix has 39 tasks: 1 native, 12 packaging and
26 Spark/JVM/connector tasks for Spark 3.5, 4.0, 4.1 and 4.2.

`APACHE_GLUTEN_COMMIT_REQUIRED` blocks the Bolt caller until a suitable Apache
source commit lands. Bolt helpers use the caller revision or the deployment
bundle revision; no Gluten helper pin, workflow mirror, or fork fallback is used.

Baseline: Apache main `0447c1aa45f3a5c96cf921490bbea8d1f1dd1bfe`.
These 29 files are the source/test prerequisite set, not 29 newly discovered
changes: most are already proposed in the open PRs below. Full execution after
upstream integration remains unverified.

## Existing Apache PRs

| PR | Relation to this prerequisite |
| --- | --- |
| [#13486](https://github.com/apache/gluten/pull/13486) | Contains the previous CI/helper implementation and all 29 source/test paths. Trim it to necessary source/test changes, omit Spark 3.4 and keep CI in Bolt. Two additional Spark 4.1/4.2 NTZ suite selections are included in the local branch. |
| [#13377](https://github.com/apache/gluten/pull/13377) | Covers the seven native benchmark/test-support paths. Five files match exactly; CMakeLists and GenericBenchmark differ. Reconcile, rather than overwrite, its additional linker/runtime fixes and the listener lifetime guard. |
| [#13379](https://github.com/apache/gluten/pull/13379) | Overlaps Delta/Hudi tests, but its current head does not contain the same final fixes: retain the Delta 4 logPath implementation and remove the Hudi resourcePath override. |
| [#13367](https://github.com/apache/gluten/pull/13367) | Related memory lifecycle and shared UDF runtime fixes. Not part of the minimal source diff; evaluate runtime failures separately. |
| [#13378](https://github.com/apache/gluten/pull/13378) | Related Arrow/Python/write correctness fixes. Not an unconditional CI bootstrap dependency. |
| [#13380](https://github.com/apache/gluten/pull/13380) | Related expression/generator semantics fixes. Not an unconditional CI bootstrap dependency. |

At inspection, #13486 was mergeable and the other five PRs had conflicts.
An open PR is not an accepted Apache dependency. Do not require or merge all six
solely because they are related to Bolt.

## Native build and tests

Current runtime/shuffle APIs and local RSS test support:

- `cpp/bolt/benchmarks/CMakeLists.txt`
- `cpp/bolt/benchmarks/GenericBenchmark.cc`
- `cpp/bolt/benchmarks/PlanValidatorUtil.cc`
- `cpp/bolt/tests/utils/LocalRssClient.cc`
- `cpp/bolt/tests/utils/LocalRssClient.h`
- `cpp/bolt/tests/utils/TestAllocationListener.cc`
- `cpp/bolt/tests/utils/TestAllocationListener.h`

## Spark and connector tests

Version-specific Bolt source roots, selected suites, backend-neutral assertions,
connector test dependencies and Delta/Hudi compatibility:

- `gluten-ut/pom.xml`
- `gluten-ut/common/src/test/scala/org/apache/gluten/utils/BackendTestUtils.scala`
- `gluten-ut/spark35/src/test/scala/org/apache/gluten/utils/bolt/BoltTestSettings.scala`
- `gluten-ut/spark40/pom.xml`
- `gluten-ut/spark40/src/test/backends-bolt/org/apache/gluten/utils/bolt/BoltTestSettings.scala`
- `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenLogicalPlanTagInSparkPlanSuite.scala`
- `gluten-ut/spark40/src/test/scala/org/apache/spark/sql/execution/GlutenWholeStageCodegenSuite.scala`
- `gluten-ut/spark41/pom.xml`
- `gluten-ut/spark41/src/test/backends-bolt/org/apache/gluten/utils/bolt/BoltTestSettings.scala`
- `gluten-ut/spark41/src/test/scala/org/apache/spark/sql/execution/GlutenLogicalPlanTagInSparkPlanSuite.scala`
- `gluten-ut/spark41/src/test/scala/org/apache/spark/sql/execution/GlutenWholeStageCodegenSuite.scala`
- `gluten-ut/spark42/pom.xml`
- `gluten-ut/spark42/src/test/backends-bolt/org/apache/gluten/utils/bolt/BoltTestSettings.scala`
- `gluten-ut/spark42/src/test/scala/org/apache/spark/sql/execution/GlutenLogicalPlanTagInSparkPlanSuite.scala`
- `gluten-ut/spark42/src/test/scala/org/apache/spark/sql/execution/GlutenWholeStageCodegenSuite.scala`
- `gluten-ut/spark40/src/test/bolt-shared/org/apache/gluten/GlutenColumnarWriteTestSupport.scala`
- `gluten-ut/spark40/src/test/bolt-shared/org/apache/gluten/utils/bolt/BoltSpark4TestSettings.scala`
- `gluten-ut/spark40/src/test/bolt-shared/org/apache/gluten/utils/bolt/BoltTestSettingsSuite.scala`
- `gluten-ut/spark40/src/test/bolt-shared/org/apache/spark/sql/GlutenBoltBackendSmokeSuite.scala`
- `gluten-ut/spark40/src/test/bolt-shared/org/apache/spark/sql/execution/GlutenBroadcastNestedLoopJoinAssertionSuite.scala`
- `backends-bolt/src-delta/test/scala/org/apache/gluten/execution/BoltDeltaSuite.scala`
- `backends-bolt/src-hudi/test/scala/org/apache/gluten/execution/BoltTPCHHudiSuite.scala`

## Integration order

1. Reconcile the overlapping native changes in #13377 and the Delta/Hudi changes
   in #13379 or the trimmed #13486; preserve newer Apache main changes.
2. Merge the remaining Spark 3.5/4.x test support upstream. No CI script or
   automatic PR/schedule workflow needs to be added to Gluten.
3. Pin the accepted Apache source commit in both Bolt callers and their tests.
   Verify that it is an ancestor of Apache main.
4. Run real backend compilation and the full nightly with Bolt-owned helpers.
   Previous fork-backed results do not validate this new source/helper combination.
5. Rebuild the external deployment from the reviewed Bolt commit. Its version 2
   bundle includes Bolt helpers; older fork-helper preview bundles are obsolete.
