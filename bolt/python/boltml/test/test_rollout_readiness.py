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
import time
import unittest
from pathlib import Path

from ..dataframe import DataFrame
from ..executor.ray import RayExecutor
from ..function.aggregation import Mean
from ..distributed.ray.exchange_manager import AdaptiveExchangeManager
from ..distributed.ray.exchange_manager import ExchangeTransportPolicy
from ..distributed.ray.exchange_manager import FileExchangeManager
from ..distributed.ray.runtime import (
    RayExecutionConfig,
    RayExecutionMode,
    RayRolloutLevel,
    RayRuntime,
)


class TestRolloutReadiness(unittest.TestCase):
    def testRolloutGateDefaultsToStagedLocalMode(self):
        executor = RayExecutor()

        self.assertEqual(executor.executionMode, RayExecutionMode.STAGED_LOCAL)

    def testRolloutLevelCanForceLocalExecution(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(
                    mode=RayExecutionMode.REMOTE_LEAF,
                    rolloutLevel=RayRolloutLevel.STAGED_LOCAL,
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
        summary = executor.lastExecutionSummary
        self.assertFalse(summary.stages[0].remote)
        self.assertEqual(summary.stages[0].fallbackReason, "rollout_gate")

    def testLeafOnlyRolloutKeepsGroupedConsumerLocal(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(
                    mode=RayExecutionMode.REMOTE_LEAF,
                    rolloutLevel=RayRolloutLevel.REMOTE_LEAF,
                )
            )
        )
        distributed = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
            executor=executor,
        )
        local = DataFrame({"grp": ["a", "a", "b"], "value": [1, 2, 3]})

        self.assertEqual(
            local.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp"),
            distributed.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp"),
        )
        summary = executor.lastExecutionSummary
        self.assertTrue(summary.stages[0].remote)
        self.assertFalse(summary.stages[1].remote)
        self.assertEqual(summary.stages[1].fallbackReason, "rollout_gate")

    def testRemoteLeafModeRunsValuesLeafAsSingleRemoteTask(self):
        # Values-source leaves (DataFrame from a literal dict) carry
        # their data in the plan literal, not as splits. The runtime's
        # ``_plan_has_shardable_input`` gate refuses to fan out such
        # leaves -- with ``leafShardCount=N`` the splits map is empty
        # so each shard would re-execute the full Values plan and emit
        # N copies of the same data. The gate forces ``shardCount=1``
        # but still runs the single shard remotely (Values sources are
        # tiny -- IN-list filter tables, scalar subquery results --
        # so a single remote task is fine and avoids a local fallback).
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
        )
        df = DataFrame(
            {"id": [1, 2, 3]},
            executor=executor,
        )
        df.transform(augmented=lambda frame: frame["id"] + 1)

        self.assertEqual(len(df), 3)
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        # Values leaf still runs remotely, just not fanned out.
        self.assertTrue(summary.stages[0].remote)
        self.assertIsNone(summary.stages[0].fallbackReason)

    def testRemoteLeafModeSupportsSimpleValuesLeafQueries(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
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
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)

    def testRemoteLeafModeSupportsGroupedAggregateFlow(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
        )
        distributed = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
            executor=executor,
        )
        local = DataFrame({"grp": ["a", "a", "b"], "value": [1, 2, 3]})

        expected = local.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")
        result = distributed.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")

        self.assertEqual(expected, result)
        summary = executor.lastExecutionSummary
        self.assertTrue(summary.stages[1].remote)
        self.assertEqual(summary.stages[1].partitionCount, 12)

    def testRemoteLeafModeSupportsRepresentativeJoinFlow(self):
        executor = RayExecutor(
            runtime=RayRuntime(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
        )
        lhs = DataFrame(
            {"id": [1, 2, 3], "lv": [10, 20, 30]},
            executor=executor,
        )
        rhs = DataFrame(
            {"id": [1, 3, 4], "rv": [100, 300, 400]},
            executor=executor,
        )
        localLhs = DataFrame({"id": [1, 2, 3], "lv": [10, 20, 30]})
        localRhs = DataFrame({"id": [1, 3, 4], "rv": [100, 300, 400]})

        expected = (
            localLhs.join(localRhs, {"id"}).select(["id", "lv", "rv"]).orderBy("id")
        )
        result = lhs.join(rhs, {"id"}).select(["id", "lv", "rv"]).orderBy("id")

        self.assertEqual(expected, result)
        summary = executor.lastExecutionSummary
        self.assertTrue(summary.stages[-1].remote)
        self.assertIsNone(summary.stages[-1].fallbackReason)

    def testRepresentativeJoinLimitFlowMatchesAcrossTransports(self):
        orders = {
            "order_id": [1, 2, 3, 4, 5, 6],
            "cust_id": [10, 20, 10, 30, 20, 10],
            "amount": [40, 15, 60, 10, 50, 25],
        }
        customers = {
            "cust_id": [10, 20, 30],
            "region": ["west", "east", "north"],
        }

        localOrders = DataFrame(orders)
        localCustomers = DataFrame(customers)

        expected = (
            localOrders.join(localCustomers, {"cust_id"})
            .select(["cust_id", "region", "amount"])
            .orderBy("cust_id")
            .select(count=2)
        )

        with (
            tempfile.TemporaryDirectory() as objectTmpDir,
            tempfile.TemporaryDirectory() as fileTmpDir,
            tempfile.TemporaryDirectory() as adaptiveTmpDir,
        ):
            objectOrders = DataFrame(
                orders,
                executor=RayExecutor(
                    exchangeManager=AdaptiveExchangeManager(
                        objectTmpDir,
                        policy=ExchangeTransportPolicy(
                            preferredTransport="object",
                            maxRowsForObject=100,
                        ),
                    )
                ),
            )
            objectCustomers = DataFrame(
                customers,
                executor=RayExecutor(
                    exchangeManager=AdaptiveExchangeManager(
                        objectTmpDir,
                        policy=ExchangeTransportPolicy(
                            preferredTransport="object",
                            maxRowsForObject=100,
                        ),
                    )
                ),
            )
            fileOrders = DataFrame(
                orders,
                executor=RayExecutor(exchangeManager=FileExchangeManager(fileTmpDir)),
            )
            fileCustomers = DataFrame(
                customers,
                executor=RayExecutor(exchangeManager=FileExchangeManager(fileTmpDir)),
            )
            adaptiveOrders = DataFrame(
                orders,
                executor=RayExecutor(
                    exchangeManager=AdaptiveExchangeManager(
                        adaptiveTmpDir,
                        policy=ExchangeTransportPolicy(
                            preferredTransport="object",
                            maxRowsForObject=100,
                        ),
                    )
                ),
            )
            adaptiveCustomers = DataFrame(
                customers,
                executor=RayExecutor(
                    exchangeManager=AdaptiveExchangeManager(
                        adaptiveTmpDir,
                        policy=ExchangeTransportPolicy(
                            preferredTransport="object",
                            maxRowsForObject=100,
                        ),
                    )
                ),
            )

            objectResult = (
                objectOrders.join(objectCustomers, {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )
            fileResult = (
                fileOrders.join(fileCustomers, {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )
            adaptiveResult = (
                adaptiveOrders.join(adaptiveCustomers, {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )

            self.assertEqual(expected, objectResult)
            self.assertEqual(expected, fileResult)
            self.assertEqual(expected, adaptiveResult)
            self.assertEqual(
                objectOrders._executor_.lastExecutionSummary.stages[0].transport,
                "object",
            )
            self.assertEqual(
                objectOrders._executor_.lastExecutionSummary.stages[-1].transport,
                "object",
            )
            self.assertEqual(
                fileOrders._executor_.lastExecutionSummary.stages[0].transport,
                "file",
            )
            self.assertEqual(
                fileOrders._executor_.lastExecutionSummary.stages[-1].transport,
                "file",
            )
            self.assertEqual(
                adaptiveOrders._executor_.lastExecutionSummary.stages[0].transport,
                "object",
            )
            self.assertEqual(
                adaptiveOrders._executor_.lastExecutionSummary.stages[-1].transport,
                "object",
            )

    def testRemoteRepresentativeJoinFlowMatchesAcrossTransports(self):
        orders = {
            "order_id": [1, 2, 3, 4, 5, 6],
            "cust_id": [10, 20, 10, 30, 20, 10],
            "amount": [40, 15, 60, 10, 50, 25],
        }
        customers = {
            "cust_id": [10, 20, 30],
            "region": ["west", "east", "north"],
        }
        expected = (
            DataFrame(orders)
            .join(DataFrame(customers), {"cust_id"})
            .select(["cust_id", "region", "amount"])
            .orderBy("cust_id")
            .select(count=2)
        )

        with (
            tempfile.TemporaryDirectory() as objectTmpDir,
            tempfile.TemporaryDirectory() as fileTmpDir,
            tempfile.TemporaryDirectory() as adaptiveTmpDir,
        ):

            def build_df(data, executor):
                return DataFrame(data, executor=executor)

            objectExecutor = RayExecutor(
                runtime=RayRuntime(
                    config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
                ),
                exchangeManager=AdaptiveExchangeManager(
                    objectTmpDir,
                    policy=ExchangeTransportPolicy(
                        preferredTransport="object", maxRowsForObject=100
                    ),
                ),
            )
            fileExecutor = RayExecutor(
                runtime=RayRuntime(
                    config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
                ),
                exchangeManager=FileExchangeManager(fileTmpDir),
            )
            adaptiveExecutor = RayExecutor(
                runtime=RayRuntime(
                    config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
                ),
                exchangeManager=AdaptiveExchangeManager(
                    adaptiveTmpDir,
                    policy=ExchangeTransportPolicy(
                        preferredTransport="object", maxRowsForObject=100
                    ),
                ),
            )

            objectResult = (
                build_df(orders, objectExecutor)
                .join(build_df(customers, objectExecutor), {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )
            fileResult = (
                build_df(orders, fileExecutor)
                .join(build_df(customers, fileExecutor), {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )
            adaptiveResult = (
                build_df(orders, adaptiveExecutor)
                .join(build_df(customers, adaptiveExecutor), {"cust_id"})
                .select(["cust_id", "region", "amount"])
                .orderBy("cust_id")
                .select(count=2)
            )

            self.assertEqual(expected, objectResult)
            self.assertEqual(expected, fileResult)
            self.assertEqual(expected, adaptiveResult)
            self.assertTrue(objectExecutor.lastExecutionSummary.stages[-1].remote)
            self.assertTrue(fileExecutor.lastExecutionSummary.stages[-1].remote)
            self.assertTrue(adaptiveExecutor.lastExecutionSummary.stages[-1].remote)
            self.assertEqual(
                objectExecutor.lastExecutionSummary.stages[-1].transport, "object"
            )
            self.assertEqual(
                fileExecutor.lastExecutionSummary.stages[-1].transport, "file"
            )
            self.assertEqual(
                adaptiveExecutor.lastExecutionSummary.stages[-1].transport, "object"
            )

    def testObjectTransportPublishLoadSmokeAvoidsFileArtifacts(self):
        data = {
            "id": list(range(3000)),
            "tag": [f"v{i % 7}" for i in range(3000)],
        }
        dataframe = DataFrame(data)

        with (
            tempfile.TemporaryDirectory() as objectTmpDir,
            tempfile.TemporaryDirectory() as fileTmpDir,
        ):
            objectManager = AdaptiveExchangeManager(
                objectTmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10000
                ),
            )
            fileManager = FileExchangeManager(fileTmpDir)

            objectTimes = []
            fileTimes = []
            for _ in range(3):
                start = time.perf_counter()
                descriptor = objectManager.publish(
                    "stage-0", "exchange-0", dataframe, "hash(id)"
                )[0]
                loaded = objectManager.load(descriptor)
                objectManager.cleanup()
                objectTimes.append(time.perf_counter() - start)
                self.assertEqual(dataframe, loaded)
                self.assertEqual(list(Path(objectTmpDir).rglob("*.parquet")), [])

                start = time.perf_counter()
                descriptor = fileManager.publish(
                    "stage-0", "exchange-0", dataframe, "hash(id)"
                )[0]
                loaded = fileManager.load(descriptor)
                fileManager.cleanup()
                fileTimes.append(time.perf_counter() - start)
                self.assertEqual(dataframe, loaded)


if __name__ == "__main__":
    unittest.main()
