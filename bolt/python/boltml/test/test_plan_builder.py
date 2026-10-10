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

"""Tests for the generic ``PlanBuilder.copy()`` API.

The base implementation does a structural clone via
``cloneWithLogicalPlan(self.logicalPlan())`` — both shipping
builders (Substrait, Bolt) inherit this directly. Subclasses that
need a materialise-fallback can override.
"""

import unittest

from ..dataframe import DataFrame
from ..plan_builder.bolt import BoltPlanBuilderFactory
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


class TestPlanBuilderCopy(unittest.TestCase):
    def _assertAcrossPlanBuilders(self, assertion):
        for factory in (BoltPlanBuilderFactory(), SubstraitPlanBuilderFactory()):
            with self.subTest(planFactory=factory.__class__.__name__):
                assertion(factory)

    def testCopyReturnsIndependentBuilder(self):
        """``copy()`` returns a builder whose state is decoupled from
        the original — mutating one's logical plan via subsequent
        operations does not perturb the other."""

        def run(factory):
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]}, planFactory=factory
            )
            df.select({"id"})  # one operation on the original

            originalBuilder = df._planBuilder_
            clonedBuilder = originalBuilder.copy()

            self.assertIsNot(clonedBuilder, originalBuilder)
            self.assertEqual(
                originalBuilder.logicalPlan(),
                clonedBuilder.logicalPlan(),
            )

            # Mutate the clone with another operation; original stays put.
            clonedBuilder.limit(2, 0)
            self.assertNotEqual(
                originalBuilder.logicalPlan(),
                clonedBuilder.logicalPlan(),
            )
            originalOps = originalBuilder.logicalPlan().operations
            clonedOps = clonedBuilder.logicalPlan().operations
            self.assertEqual(len(originalOps) + 1, len(clonedOps))

        self._assertAcrossPlanBuilders(run)

    def testCopyPreservesLogicalPlanForFreshBuilder(self):
        """Copying a freshly-constructed builder (just a source, no
        ops) yields a builder with the same source and an empty
        operation log."""

        def run(factory):
            df = DataFrame({"id": [1, 2, 3]}, planFactory=factory)
            originalBuilder = df._planBuilder_
            clonedBuilder = originalBuilder.copy()

            self.assertIsNot(clonedBuilder, originalBuilder)
            self.assertEqual(
                originalBuilder.logicalPlan().source,
                clonedBuilder.logicalPlan().source,
            )
            self.assertEqual(
                originalBuilder.logicalPlan().operations,
                clonedBuilder.logicalPlan().operations,
            )

        self._assertAcrossPlanBuilders(run)

    def testCopyDtypeMatchesOriginal(self):
        """The cloned builder reports the same output schema as the
        original — exercises that ``cloneWithLogicalPlan`` rebuilds
        the same shape of plan node rather than just the logical-op
        log."""

        def run(factory):
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]}, planFactory=factory
            )
            df.select({"id"})

            originalBuilder = df._planBuilder_
            clonedBuilder = originalBuilder.copy()
            self.assertEqual(originalBuilder.dtype(), clonedBuilder.dtype())

        self._assertAcrossPlanBuilders(run)


if __name__ == "__main__":
    unittest.main()
