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

from ...distributed.ray.heuristics import RuntimeHeuristics


class TestTransportPolicy(unittest.TestCase):
    def testNormalizedPartitionCountHonorsRequestedCount(self):
        heuristics = RuntimeHeuristics(defaultPartitionCount=4, maxPartitionCount=16)

        self.assertEqual(
            heuristics.normalizedPartitionCount(100, 2, requestedCount=8), 8
        )
        self.assertEqual(
            heuristics.normalizedPartitionCount(100, 2, requestedCount=32), 16
        )

    def testNormalizedPartitionCountUsesRowsAndFilesHeuristics(self):
        heuristics = RuntimeHeuristics(
            defaultPartitionCount=2,
            maxPartitionCount=16,
            targetRowsPerPartition=100,
        )

        self.assertEqual(heuristics.normalizedPartitionCount(50, 1), 2)
        self.assertEqual(heuristics.normalizedPartitionCount(450, 2), 5)
        self.assertEqual(heuristics.normalizedPartitionCount(50, 7), 7)

    def testCoalesceFilesUsesDeterministicBatching(self):
        heuristics = RuntimeHeuristics(maxFilesPerTask=3)

        groups = heuristics.coalesceFiles([f"f{i}" for i in range(7)])

        self.assertEqual(groups, [["f0", "f1", "f2"], ["f3", "f4", "f5"], ["f6"]])


if __name__ == "__main__":
    unittest.main()
