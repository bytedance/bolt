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

from ..distributed.dispatcher.placeholders import PlaceholderRead
from ..distributed.ray.exchange_manager import ExchangePartitionDescriptor


class TestPhysicalOptimizer(unittest.TestCase):
    """``PlaceholderRead`` invariants. The Substrait analogue of the
    physical-optimizer surface is covered by
    ``test_substrait_physical_optimizer.py``; this file keeps only the
    executor-agnostic ``PlaceholderRead`` tests, which the Substrait
    dispatcher uses for its placeholder rewriting at exchange cuts."""

    def testPlaceholderReadIsTransportAgnostic(self):
        placeholder = PlaceholderRead(
            sourceStageId="stage-0",
            exchangeId="exchange-0",
            outputNames=("id", "tag"),
            outputTypes=("BIGINT", "VARCHAR"),
            partitioning="hash(id)",
        )

        self.assertIsNone(placeholder.transport)
        self.assertEqual(placeholder.outputNames, ("id", "tag"))
        self.assertEqual(placeholder.outputTypes, ("BIGINT", "VARCHAR"))
        self.assertIn("PlaceholderRead[stage-0/exchange-0]", placeholder.explain())
        self.assertIn("partitioning=hash(id)", placeholder.explain())
        self.assertIn("partitions=1", placeholder.explain())

    def testPlaceholderReadValidatesResolvedDescriptorMetadata(self):
        placeholder = PlaceholderRead(
            sourceStageId="stage-0",
            exchangeId="exchange-0",
            outputNames=("id", "tag"),
            outputTypes=("BIGINT", "VARCHAR"),
            partitioning="hash(id)",
        )
        descriptor = ExchangePartitionDescriptor(
            executionId="",
            stageId="stage-0",
            exchangeId="exchange-0",
            attemptId=1,
            partitionCount=1,
            partitionId=0,
            path="/tmp/part-0.parquet",
            outputNames=("id", "tag"),
            outputTypes=("BIGINT", "VARCHAR"),
            partitioning="hash(id)",
        )

        placeholder.validateDescriptor(descriptor)

        with self.assertRaisesRegex(ValueError, "partitioning"):
            placeholder.validateDescriptor(
                ExchangePartitionDescriptor(
                    executionId="",
                    stageId="stage-0",
                    exchangeId="exchange-0",
                    attemptId=1,
                    partitionCount=1,
                    partitionId=0,
                    path="/tmp/part-0.parquet",
                    outputNames=("id", "tag"),
                    outputTypes=("BIGINT", "VARCHAR"),
                    partitioning="singleton",
                )
            )


if __name__ == "__main__":
    unittest.main()
