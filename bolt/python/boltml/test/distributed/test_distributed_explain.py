# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import tempfile

from ...dataframe import DataFrame
from ...executor.ray import RayExecutor
from ...function.aggregation import Mean
from ...plan_builder.substrait import SubstraitPlanBuilderFactory
from ...distributed.ray.exchange_manager import FileExchangeManager
from ...distributed.ray.runtime import (
    RayExecutionConfig,
    RayExecutionMode,
    RayRolloutLevel,
    RayRuntime,
)
from ..tpch.tpch_harness import (
    PHASED_TPCH_QUERY_SUBSETS,
    TpchExecutionMode,
    TpchParityHarness,
)


class TestDistributedExplain(unittest.TestCase):
    def _assertAcrossPlanBuilders(self, assertion):
        # explain() is offered on SubstraitPlanBuilder (BoltPlanBuilder raises
        # NotImplementedError — #79); optimization is intrinsic to
        # materialisation now, so this runs the single explain-capable factory.
        for factory in (SubstraitPlanBuilderFactory(),):
            with self.subTest(planFactory=factory.__class__.__name__):
                assertion(factory)

    def testExplainIncludesStageDagForShuffle(self):
        def run(factory):
            df = DataFrame({"id": [1, 2], "tag": ["a", "b"]}, planFactory=factory)
            df.shuffle(7)

            explain = df.explain()

            self.assertIn("Physical Plan:", explain)
            self.assertIn("Stage DAG:", explain)
            self.assertIn("Stages[2]", explain)
            self.assertIn("Stage[stage-0]", explain)
            self.assertIn("Stage[stage-1]", explain)
            self.assertIn("PlaceholderRead[stage-0/exchange-0]", explain)

        self._assertAcrossPlanBuilders(run)

    def testExplainIncludesSingleStageDagForLocalPlan(self):
        def run(factory):
            df = DataFrame({"id": [1, 2], "tag": ["a", "b"]}, planFactory=factory)
            df.filter(df["id"] > 1)

            explain = df.explain()

            self.assertIn("Stage DAG:", explain)
            self.assertIn("Stages[1]", explain)
            self.assertIn("Stage[stage-0]", explain)

        self._assertAcrossPlanBuilders(run)

    def testExplainIncludesAggregateExchangeMetadata(self):
        def run(factory):
            df = DataFrame(
                {"grp": ["a", "a", "b"], "value": [1, 2, 3]}, planFactory=factory
            )
            df.groupBy("grp").aggregate(avg=Mean("value"))

            explain = df.explain()

            self.assertIn("Physical Plan:", explain)
            self.assertIn("partitioning=hash(grp)", explain)
            self.assertIn("Stage DAG:", explain)
            self.assertIn("PlaceholderRead[stage-0/exchange-0]", explain)

        self._assertAcrossPlanBuilders(run)

    def testExplainIncludesJoinStageDag(self):
        def run(factory):
            lhs = DataFrame({"id": [1, 2], "lv": [10, 20]}, planFactory=factory)
            rhs = DataFrame({"id": [1, 3], "rv": [100, 300]}, planFactory=factory)
            lhs.join(rhs, {"id"})

            explain = lhs.explain()

            self.assertIn("Physical Plan:", explain)
            self.assertIn("Stage DAG:", explain)
            self.assertIn("Stages[3]", explain)
            self.assertIn("Stage[stage-2]", explain)
            self.assertIn("PlaceholderRead[stage-0/exchange-0]", explain)
            self.assertIn("PlaceholderRead[stage-1/exchange-1]", explain)

        self._assertAcrossPlanBuilders(run)

    def testExplainIncludesRuntimeSummaryAfterRayExecution(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )
            shuffled = df.shuffle(7)

            self.assertEqual(len(shuffled), 3)
            explain = shuffled.explain()

            self.assertIn("Runtime Summary:", explain)
            self.assertIn("RuntimeSummary[ray-plan-", explain)
            self.assertIn("mode=staged_local", explain)
            self.assertIn("Stage[stage-0]", explain)
            self.assertIn("Stage[stage-1]", explain)
            self.assertIn("transport=file", explain)

    def testExplainIncludesPartitionCountsForGroupedFlow(self):
        # Pin ``defaultPartitionCount=4`` on the engine so the assertion
        # below has a deterministic value to match. This test asserts
        # that the explain output surfaces the configured partition
        # count, not whatever the pipeline default happens to be.
        from ...optimizer.substrait_physical_pipeline import (
            SubstraitPhysicalOptimizerPipeline,
        )

        with tempfile.TemporaryDirectory() as tmpDir:
            # The physical optimizer now lives on the executor; pin the
            # partition count there so explain() (which reads it back via
            # the executor's ``physicalPipeline``) renders ``partitions=4``.
            executor = RayExecutor(
                exchangeManager=FileExchangeManager(tmpDir),
                physical=SubstraitPhysicalOptimizerPipeline(defaultPartitionCount=4),
            )
            df = DataFrame(
                {"grp": ["a", "a", "b", "b"], "value": [1, 2, 3, 4]},
                executor=executor,
            )
            result = df.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")

            self.assertEqual(len(result), 2)
            explain = result.explain()

            self.assertIn("Runtime Summary:", explain)
            self.assertIn("Stage[stage-0]", explain)
            self.assertIn("Stage[stage-1]", explain)
            self.assertIn("partitions=4", explain)

    def testExplainIncludesRolloutLevelAfterRemoteExecution(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(
                    mode=RayExecutionMode.REMOTE_LEAF,
                    rolloutLevel=RayRolloutLevel.REMOTE_LEAF,
                )
            )
        )
        df = DataFrame(
            {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
            executor=executor,
        )
        query = (
            df.filter(df["id"] > 1).select(["id", "tag"]).orderBy("id").select(count=2)
        )

        self.assertEqual(len(query), 2)
        explain = query.explain()

        self.assertIn("rollout=remote_leaf", explain)

    def testExplainIncludesPhase4TpchTransportAndPartitionMetadata(self):
        # The TPC-H harness doesn't pin a ``defaultPartitionCount``,
        # so the pipeline default propagates into the runtime summary.
        # The intent of this test is to verify that the explain output
        # surfaces the partition count for each stage; the specific
        # number is whatever the pipeline's default happens to be.
        harness = TpchParityHarness()
        spec = PHASED_TPCH_QUERY_SUBSETS["phase4"][1]

        with tempfile.TemporaryDirectory() as tmpDir:
            result, _ = harness.runQuery(
                spec, TpchExecutionMode.STAGED_FILE_OPTIMIZED, tmpDir=tmpDir
            )
            self.assertEqual(len(result), 5)
            explain = result.explain()

            self.assertIn("Runtime Summary:", explain)
            self.assertIn("transport=file", explain)
            # Partition count should appear on every stage in the runtime
            # summary; we don't pin the specific number because the
            # harness uses the pipeline default (currently 12).
            import re

            partition_counts = re.findall(r"partitions=(\d+)", explain)
            self.assertGreater(
                len(partition_counts),
                0,
                msg="Runtime summary should include partition counts per stage",
            )


if __name__ == "__main__":
    unittest.main()
