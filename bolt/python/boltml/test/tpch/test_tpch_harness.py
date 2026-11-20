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

import tempfile
import unittest

from ...dataframe import DataFrame
from ...executor import LocalExecutor
from ...executor.ray import RayExecutor
from ...distributed.ray.runtime import RayExecutionMode
from .tpch_harness import PHASED_TPCH_QUERY_SUBSETS
from .tpch_harness import TpchExecutionMode
from .tpch_harness import TpchParityHarness


class TestTpchHarness(unittest.TestCase):
    def testEarlyPhaseSubsetsAreExplicit(self):
        self.assertEqual(
            tuple(PHASED_TPCH_QUERY_SUBSETS.keys()),
            ("phase1", "phase2", "phase3", "phase4"),
        )
        self.assertGreaterEqual(len(PHASED_TPCH_QUERY_SUBSETS["phase1"]), 2)
        self.assertGreaterEqual(len(PHASED_TPCH_QUERY_SUBSETS["phase2"]), 2)
        self.assertGreaterEqual(len(PHASED_TPCH_QUERY_SUBSETS["phase3"]), 1)
        self.assertGreaterEqual(len(PHASED_TPCH_QUERY_SUBSETS["phase4"]), 2)

    def testNormalizeResultTreatsRowPermutationsAsEqualWhenOrderNotRequired(self):
        lhs = DataFrame({"id": [2, 1], "tag": ["b", "a"]})
        rhs = DataFrame({"id": [1, 2], "tag": ["a", "b"]})

        self.assertEqual(
            TpchParityHarness.normalizeResult(lhs),
            TpchParityHarness.normalizeResult(rhs),
        )

    def testNormalizeResultPreservesOrderingWhenRequired(self):
        lhs = DataFrame({"id": [2, 1], "tag": ["b", "a"]})
        rhs = DataFrame({"id": [1, 2], "tag": ["a", "b"]})

        self.assertNotEqual(
            TpchParityHarness.normalizeResult(lhs, ordered=True),
            TpchParityHarness.normalizeResult(rhs, ordered=True),
        )

    def testClassifyFailureBucketsRepresentativeErrors(self):
        self.assertEqual(
            TpchParityHarness.classifyFailure(KeyError("Unknown exchange placeholder")),
            "transport",
        )
        self.assertEqual(
            TpchParityHarness.classifyFailure(
                NotImplementedError(
                    "only 1-input or 2-input join consumers are supported"
                )
            ),
            "binding",
        )
        self.assertEqual(
            TpchParityHarness.classifyFailure(
                NotImplementedError(
                    "only single-partition exchange descriptors are supported"
                )
            ),
            "unsupported-shape",
        )
        self.assertEqual(
            TpchParityHarness.classifyFailure(
                RuntimeError("retry exhausted after transient stage error")
            ),
            "retry",
        )

    def testBuildEngineSupportsLocalAndStagedBaselineModes(self):
        harness = TpchParityHarness()

        localEngine = harness.engine(TpchExecutionMode.LOCAL_OPTIMIZED)
        self.assertIsInstance(localEngine.executor, LocalExecutor)

        with tempfile.TemporaryDirectory() as tmpDir:
            stagedEngine = harness.engine(
                TpchExecutionMode.STAGED_FILE_OPTIMIZED, tmpDir=tmpDir
            )
            self.assertIsInstance(stagedEngine.executor, RayExecutor)

        remoteLeafEngine = harness.engine(TpchExecutionMode.REMOTE_LEAF_OPTIMIZED)
        self.assertIsInstance(remoteLeafEngine.executor, RayExecutor)
        self.assertEqual(
            remoteLeafEngine.executor.executionMode, RayExecutionMode.REMOTE_LEAF
        )


if __name__ == "__main__":
    unittest.main()
