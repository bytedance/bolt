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

from ..dataframe import DataFrame
from ..optimizer.substrait_pipeline import SubstraitOptimizerPipeline
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


class TestOptimizer(unittest.TestCase):
    # Optimization is now intrinsic to materialisation: ``explain()`` /
    # ``execute()`` run the logical Substrait optimizer via ``plan.optimize()``.
    # These tests call ``explain()``, which is offered on
    # ``SubstraitPlanBuilder`` (``BoltPlanBuilder`` raises NotImplementedError —
    # #79), so the loop runs the single explain-capable plan-builder factory.
    def _assertAcrossPlanBuilders(self, assertion):
        for factory in (SubstraitPlanBuilderFactory(),):
            with self.subTest(planFactory=factory.__class__.__name__):
                assertion(factory)

    def _explainSection(self, explain: str, header: str) -> str:
        token = f"{header}:\n"
        _, tail = explain.split(token, 1)
        if "\n\n" in tail:
            return tail.split("\n\n", 1)[0]
        return tail

    def testPredicatePushdownMovesFilterBelowProject(self):
        def run(factory):
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]}, planFactory=factory
            )
            df.select({"id"}).filter(df["id"] > 1)

            explain = self._explainSection(df.explain(), "Optimized Logical Plan")
            self.assertLess(explain.index("Filter["), explain.index("Project["))

            optimized = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                planFactory=SubstraitPlanBuilderFactory(),
            )
            optimized.select({"id"}).filter(optimized["id"] > 1)
            baseline = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                planFactory=factory,
            )
            baseline.select({"id"}).filter(baseline["id"] > 1)
            self.assertEqual(baseline, optimized)

        self._assertAcrossPlanBuilders(run)

    def testProjectionPruningMergesStackedProjects(self):
        def run(factory):
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]}, planFactory=factory
            )
            df.rename(id="key").select({"key"})

            explain = self._explainSection(df.explain(), "Optimized Logical Plan")
            self.assertEqual(explain.count("Project["), 1)

            optimized = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                planFactory=SubstraitPlanBuilderFactory(),
            )
            optimized.rename(id="key").select({"key"})
            baseline = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                planFactory=factory,
            )
            baseline.rename(id="key").select({"key"})
            self.assertEqual(baseline, optimized)

        self._assertAcrossPlanBuilders(run)

    def testLimitPushdownAndMergeSimplifyLogicalPlan(self):
        def run(factory):
            df = DataFrame({"id": [1, 2, 3, 4]}, planFactory=factory)
            df.select({"id"}).select(offset=1, count=3).select(offset=1, count=1)

            explain = self._explainSection(df.explain(), "Optimized Logical Plan")
            self.assertEqual(explain.count("Limit["), 1)
            self.assertLess(explain.index("Limit["), explain.index("Project["))

            optimized = DataFrame(
                {"id": [1, 2, 3, 4]}, planFactory=SubstraitPlanBuilderFactory()
            )
            optimized.select({"id"}).select(offset=1, count=3).select(offset=1, count=1)
            baseline = DataFrame({"id": [1, 2, 3, 4]}, planFactory=factory)
            baseline.select({"id"}).select(offset=1, count=3).select(offset=1, count=1)
            self.assertEqual(baseline, optimized)

        self._assertAcrossPlanBuilders(run)

    def testOptimizerIsIdempotent(self):
        # Idempotence on the canonical Substrait pipeline: the second
        # pass over the rule schedule should produce a byte-identical
        # Plan (or at least an explain-identical one). Exercises the
        # Substrait pipeline that ``LocalEngine`` actually runs.
        def run(factory):
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]}, planFactory=factory
            )
            df.select({"id"}).filter(df["id"] > 1)

            pipeline = SubstraitOptimizerPipeline()
            lowered = df._planBuilder_.toSubstraitPlan()
            optimized = pipeline.optimize(lowered)
            reoptimized = pipeline.optimize(optimized)

            self.assertEqual(
                optimized.SerializeToString(),
                reoptimized.SerializeToString(),
            )

        self._assertAcrossPlanBuilders(run)

    def testJoinActsAsOptimizerBarrier(self):
        def run(factory):
            lhs = DataFrame({"id": [1, 2, 3], "lv": [10, 20, 30]}, planFactory=factory)
            rhs = DataFrame(
                {"id": [1, 2, 4], "rv": [100, 200, 400]}, planFactory=factory
            )

            joined = lhs.join(rhs, {"id"}).select({"id", "lv"}).filter(lhs["id"] > 1)
            explain = self._explainSection(joined.explain(), "Optimized Logical Plan")

            self.assertLess(explain.index("HashJoin"), explain.index("Filter["))

        self._assertAcrossPlanBuilders(run)


if __name__ == "__main__":
    unittest.main()
