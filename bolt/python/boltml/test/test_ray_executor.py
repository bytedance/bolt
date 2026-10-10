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
from pathlib import Path

from ..dataframe import DataFrame
from ..executor.ray import RayExecutor
from ..function.aggregation import Mean
from ..plan_builder.bolt import BoltPlanBuilderFactory
from ..tpch import Customer
from ..distributed.ray.exchange_manager import AdaptiveExchangeManager
from ..distributed.ray.exchange_manager import ExchangePartitionDescriptor
from ..distributed.ray.exchange_manager import ExchangeTransportPolicy
from ..distributed.ray.exchange_manager import FileExchangeManager
from ..distributed.ray.runtime import RayExecutionConfig, RayExecutionMode
from ..distributed.ray.runtime import (
    RayRuntime,
    RayWorkerMode,
    TransientStageError,
    _splitProducedResult,
    _group_descriptors_by_partition,
)
from ..distributed.ray.task_spec import StageTaskResult


class _SpyRuntime:
    def __init__(self):
        self.calls = []
        self.stageDagCalls = []

    def runStage(self, plan, taskName, executor):
        self.calls.append((taskName, tuple(plan.dtype().names())))
        return executor.eval(plan)

    def runStageSubstraitBytes(self, substraitPlanBytes, taskName, outputNames=()):
        from boltml.distributed.ray.remote_worker import _execute_substrait_bytes

        self.calls.append((taskName, tuple(outputNames)))
        return _execute_substrait_bytes(substraitPlanBytes)._data_

    def runStageDag(self, stageDag, exchangeManager=None, udfRegistry=None):
        self.stageDagCalls.append(tuple(stage.stageId for stage in stageDag.stages))
        result = None
        for stage in stageDag.stages:
            if stage.producerSubstraitPlanBytes is not None:
                result = self.runStageSubstraitBytes(
                    stage.producerSubstraitPlanBytes,
                    stage.stageId,
                    stage.outputNames,
                )
        return result


class _FailOnceRuntime(RayRuntime):
    def __init__(self, stageId: str, maxStageRetries: int = 1):
        super().__init__(maxStageRetries=maxStageRetries)
        self._stageId = stageId
        self._failed = False

    def runStageSubstraitBytes(self, substraitPlanBytes, taskName, *loadedInputs):
        if taskName == self._stageId and not self._failed:
            self._failed = True
            raise TransientStageError(f"transient failure on {taskName}")
        return super().runStageSubstraitBytes(
            substraitPlanBytes, taskName, *loadedInputs
        )


class _AlwaysFailConsumerRuntime(RayRuntime):
    def runStageSubstraitBytes(self, substraitPlanBytes, taskName, *loadedInputs):
        if taskName == "stage-1":
            raise RuntimeError("consumer stage failed")
        return super().runStageSubstraitBytes(
            substraitPlanBytes, taskName, *loadedInputs
        )


class _FailOnceRemoteGroupedRuntime(RayRuntime):
    def __init__(self):
        super().__init__(
            maxStageRetries=1,
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
        )
        self._failed = False

    def _runSingleInputConsumerStageRemotely(
        self, taskSpec, loadedInput, *args, **kwargs
    ):
        if not self._failed:
            self._failed = True
            raise TransientStageError(
                f"transient remote grouped failure on {taskSpec.stageId}"
            )
        return super()._runSingleInputConsumerStageRemotely(
            taskSpec, loadedInput, *args, **kwargs
        )


class _FailOnceRemoteJoinRuntime(RayRuntime):
    def __init__(self):
        super().__init__(
            maxStageRetries=1,
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
        )
        self._failed = False

    def _runMultiInputConsumerStageRemotely(
        self, taskSpec, loadedInputs, *args, **kwargs
    ):
        if not self._failed:
            self._failed = True
            raise TransientStageError(
                f"transient remote join failure on {taskSpec.stageId}"
            )
        return super()._runMultiInputConsumerStageRemotely(
            taskSpec, loadedInputs, *args, **kwargs
        )


class _PublishFailExchangeManager(FileExchangeManager):
    def publish(
        self, stageId, exchangeId, dataframe, partitioning, executionId="", attemptId=1
    ):
        super().publish(
            stageId,
            exchangeId,
            dataframe,
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )
        raise RuntimeError("publish failed")

    def publishPartitions(
        self, stageId, exchangeId, partitions, partitioning, executionId="", attemptId=1
    ):
        super().publishPartitions(
            stageId,
            exchangeId,
            partitions,
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )
        raise RuntimeError("publish failed")


class _ResolveFailExchangeManager(FileExchangeManager):
    def resolve(self, placeholder):
        raise RuntimeError("resolve failed")


class _DescriptorMismatchExchangeManager(FileExchangeManager):
    def resolve(self, placeholder):
        descriptor = super().resolve(placeholder)[0]
        return (
            type(descriptor)(
                executionId=descriptor.executionId,
                stageId=descriptor.stageId,
                exchangeId=descriptor.exchangeId,
                attemptId=descriptor.attemptId,
                partitionCount=descriptor.partitionCount,
                partitionId=descriptor.partitionId,
                path=descriptor.path,
                outputNames=descriptor.outputNames,
                outputTypes=descriptor.outputTypes,
                partitioning="singleton",
                transport=descriptor.transport,
                objectKey=descriptor.objectKey,
                shardId=descriptor.shardId,
            ),
        )


class _LoadFailExchangeManager(FileExchangeManager):
    def load(self, descriptor):
        raise RuntimeError("driver load is forbidden")


class _StagePublishFailExchangeManager(FileExchangeManager):
    def __init__(self, rootDir: str, blockedStages: tuple[str, ...]):
        super().__init__(rootDir)
        self._blockedStages = set(blockedStages)

    def publish(
        self, stageId, exchangeId, dataframe, partitioning, executionId="", attemptId=1
    ):
        if stageId in self._blockedStages:
            raise RuntimeError(f"driver publish is forbidden for {stageId}")
        return super().publish(
            stageId,
            exchangeId,
            dataframe,
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )

    def publishPartitions(
        self, stageId, exchangeId, partitions, partitioning, executionId="", attemptId=1
    ):
        if stageId in self._blockedStages:
            raise RuntimeError(f"driver publish is forbidden for {stageId}")
        return super().publishPartitions(
            stageId,
            exchangeId,
            partitions,
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )


class TestRayExecutor(unittest.TestCase):
    def testRayExecutorDefaultsToStagedLocalExecutionMode(self):
        executor = RayExecutor()

        self.assertEqual(executor.executionMode, RayExecutionMode.STAGED_LOCAL)

    def testRayExecutorDelegatesSingleStagePlanToRuntime(self):
        runtime = _SpyRuntime()
        executor = RayExecutor(runtime=runtime)
        df = DataFrame({"id": [1, 2, 3]}, executor=executor)
        df.filter(df["id"] > 1)

        self.assertEqual(len(df), 2)
        self.assertEqual(runtime.stageDagCalls, [("stage-0",)])
        self.assertEqual(len(runtime.calls), 1)
        self.assertEqual(runtime.calls[0][0], "stage-0")
        self.assertEqual(runtime.calls[0][1], ("id",))

    def testRayExecutorPreservesLocalResults(self):
        local = DataFrame({"id": [1, 2, 3], "tag": ["a", "b", "c"]})
        distributed = DataFrame(
            {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
            executor=RayExecutor(),
        )

        expected = local.filter(local["id"] > 1).select({"id", "tag"}).orderBy("id")
        result = (
            distributed.filter(distributed["id"] > 1)
            .select({"id", "tag"})
            .orderBy("id")
        )

        self.assertEqual(expected, result)

    def testRayExecutorRunsSingleStageDagThroughRuntime(self):
        runtime = _SpyRuntime()
        executor = RayExecutor(runtime=runtime)
        df = DataFrame(
            {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
            executor=executor,
        )
        df.filter(df["id"] > 1)

        self.assertEqual(len(df), 2)
        self.assertEqual(runtime.stageDagCalls, [("stage-0",)])
        self.assertEqual(runtime.calls[0][0], "stage-0")

    def testRayExecutorExposesSingleStageExecutionSummary(self):
        executor = RayExecutor()
        df = DataFrame({"id": [1, 2, 3]}, executor=executor)
        df.filter(df["id"] > 1)

        self.assertEqual(len(df), 2)
        summary = executor.lastExecutionSummary

        self.assertIsNotNone(summary)
        self.assertEqual(summary.executionMode, RayExecutionMode.STAGED_LOCAL)
        self.assertEqual(summary.stageIds, ("stage-0",))
        self.assertEqual(summary.stages[0].stageId, "stage-0")
        self.assertEqual(summary.stages[0].attempt, 1)
        self.assertEqual(summary.stages[0].inputCount, 0)
        self.assertEqual(summary.stages[0].outputNames, ("id",))

    def testRayExecutorExposesTwoStageTransportSummary(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )
            shuffled = df.shuffle(7)

            self.assertEqual(len(shuffled), 3)
            summary = executor.lastExecutionSummary

            self.assertEqual(summary.stageIds, ("stage-0", "stage-1"))
            self.assertEqual(summary.stages[0].partitionCount, 12)
            self.assertEqual(summary.stages[1].stageId, "stage-1")
            self.assertEqual(summary.stages[1].inputCount, 1)
            self.assertEqual(summary.stages[1].partitionCount, 12)
            self.assertEqual(summary.stages[1].exchangeIds, ("exchange-0",))
            self.assertEqual(summary.stages[1].transport, "file")
            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testSplitProducedResultHashesRowsByDeclaredKey(self):
        result = DataFrame({"grp": ["a", "b", "a", "b"], "value": [1, 2, 3, 4]})._data_

        partitions = _splitProducedResult(result, "hash(grp)")
        placement = {}
        for index, partition in enumerate(partitions):
            for row in partition.toArrow().to_pylist():
                placement.setdefault(row["grp"], index)
                self.assertEqual(placement[row["grp"]], index)
        self.assertEqual(set(placement.keys()), {"a", "b"})

    def testSplitProducedResultIsDeterministicForShuffleSeed(self):
        result = DataFrame({"id": [1, 2, 3, 4], "tag": ["a", "b", "c", "d"]})._data_

        lhs = _splitProducedResult(result, "shuffle(seed=7)")
        rhs = _splitProducedResult(result, "shuffle(seed=7)")
        self.assertEqual(
            [partition.toArrow().to_pylist() for partition in lhs],
            [partition.toArrow().to_pylist() for partition in rhs],
        )

    def testGroupDescriptorsByPartitionRejectsMissingPartitions(self):
        descriptors = (
            ExchangePartitionDescriptor(
                executionId="",
                stageId="stage-0",
                exchangeId="exchange-0",
                attemptId=1,
                partitionCount=4,
                partitionId=0,
                path="/tmp/part-0.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            ),
            ExchangePartitionDescriptor(
                executionId="",
                stageId="stage-0",
                exchangeId="exchange-0",
                attemptId=1,
                partitionCount=4,
                partitionId=2,
                path="/tmp/part-2.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "missing partition ids"):
            _group_descriptors_by_partition(descriptors, 4)

    def testRayExecutorUsesSingleRemoteTaskForNonLocalAggregateSuffix(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_LoadFailExchangeManager(tmpDir),
            )
            distributed = DataFrame(
                {"grp": ["a", "a", "b", "c", "c", "d"], "value": [1, 2, 3, 4, 5, 6]},
                executor=executor,
            )

            result = (
                distributed.groupBy("grp").aggregate(avg=Mean("value")).select(count=2)
            )

            self.assertEqual(len(result), 2)
            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[1].remote)
            self.assertEqual(summary.stages[1].remoteTaskCount, 1)

    def testRayExecutorUsesObjectTransportWhenPolicyAllows(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                exchangeManager=AdaptiveExchangeManager(
                    tmpDir,
                    policy=ExchangeTransportPolicy(
                        preferredTransport="object", maxRowsForObject=10
                    ),
                )
            )
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            self.assertEqual(len(df.shuffle(7)), 3)
            summary = executor.lastExecutionSummary

            self.assertEqual(summary.stages[1].transport, "object")
            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testRayExecutorObjectAndFileTransportsRemainEquivalent(self):
        local = DataFrame({"id": [1, 2, 3], "tag": ["a", "b", "c"]})
        with (
            tempfile.TemporaryDirectory() as objectTmpDir,
            tempfile.TemporaryDirectory() as fileTmpDir,
        ):
            objectDf = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=RayExecutor(
                    exchangeManager=AdaptiveExchangeManager(
                        objectTmpDir,
                        policy=ExchangeTransportPolicy(
                            preferredTransport="object", maxRowsForObject=10
                        ),
                    )
                ),
            )
            fileDf = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=RayExecutor(exchangeManager=FileExchangeManager(fileTmpDir)),
            )

            expected = local.shuffle(7).orderBy("id")
            self.assertEqual(expected, objectDf.shuffle(7).orderBy("id"))
            self.assertEqual(expected, fileDf.shuffle(7).orderBy("id"))

    def testRayExecutorRunsTwoStageFileBackedFlow(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            runtime = _SpyRuntime()
            executor = RayExecutor(
                runtime=runtime, exchangeManager=FileExchangeManager(tmpDir)
            )
            distributed = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )
            local = DataFrame({"id": [1, 2, 3], "tag": ["a", "b", "c"]})

            expected = local.shuffle(7).orderBy("id")
            result = distributed.shuffle(7).orderBy("id")

            self.assertEqual(expected, result)
            self.assertEqual(runtime.stageDagCalls, [("stage-0", "stage-1")])
            self.assertEqual(runtime.calls[0][0], "stage-0")

    def testRayExecutorRunsGroupedAggregateFileBackedFlow(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
            distributed = DataFrame(
                {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
                executor=executor,
            )
            local = DataFrame({"grp": ["a", "a", "b"], "value": [1, 2, 3]})

            expected = local.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")
            result = (
                distributed.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")
            )

            self.assertEqual(expected, result)
            summary = executor.lastExecutionSummary
            self.assertEqual(summary.stages[0].partitionCount, 12)
            self.assertEqual(summary.stages[1].partitionCount, 12)

    def testRayExecutorRunsGroupedAggregateRemotelyInRemoteLeafMode(self):
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
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
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)
        self.assertTrue(summary.stages[1].remote)
        self.assertIsNone(summary.stages[1].fallbackReason)

    def testRayExecutorRunsRemoteGroupedConsumerWithoutDriverExchangeLoad(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_LoadFailExchangeManager(tmpDir),
            )
            distributed = DataFrame(
                {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
                executor=executor,
            )
            local = DataFrame({"grp": ["a", "a", "b"], "value": [1, 2, 3]})

            expected = local.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")
            result = (
                distributed.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp")
            )

            self.assertEqual(expected, result)
            self.assertTrue(executor.lastExecutionSummary.stages[1].remote)

    def testRayExecutorRunsPartitionParallelRemoteGroupedConsumer(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_LoadFailExchangeManager(tmpDir),
            )
            distributed = DataFrame(
                {"grp": ["a", "a", "b", "c", "c", "d"], "value": [1, 2, 3, 4, 5, 6]},
                executor=executor,
            )
            result = (
                distributed.groupBy("grp")
                .aggregate(avg=Mean("value"))
                .select(["grp", "avg"])
            )
            self.assertEqual(len(result), 4)

            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[1].remote)
            self.assertEqual(summary.stages[1].remoteTaskCount, 12)
            self.assertEqual(len(summary.stages[1].workerIds), 12)
            self.assertIn("remote_tasks=12", summary.explain())
            self.assertEqual(
                sorted(result.toArrow().to_pylist(), key=lambda row: row["grp"]),
                [
                    {"grp": "a", "avg": 1.5},
                    {"grp": "b", "avg": 3.0},
                    {"grp": "c", "avg": 4.5},
                    {"grp": "d", "avg": 6.0},
                ],
            )

    def testRayExecutorRepublishesRemoteAggregateOutputWithoutDriverGather(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_StagePublishFailExchangeManager(tmpDir, ("stage-1",)),
            )
            distributed = DataFrame(
                {"grp": ["a", "a", "b", "c", "c", "d"], "value": [1, 2, 3, 4, 5, 6]},
                executor=executor,
            )
            local = DataFrame(
                {"grp": ["a", "a", "b", "c", "c", "d"], "value": [1, 2, 3, 4, 5, 6]}
            )

            expected = (
                local.groupBy("grp")
                .aggregate(avg=Mean("value"))
                .shuffle(7)
                .select(["grp", "avg"])
            )
            result = (
                distributed.groupBy("grp")
                .aggregate(avg=Mean("value"))
                .shuffle(7)
                .select(["grp", "avg"])
            )

            self.assertEqual(
                sorted(
                    result.toArrow().to_pylist(),
                    key=lambda row: (row["grp"], row["avg"]),
                ),
                sorted(
                    expected.toArrow().to_pylist(),
                    key=lambda row: (row["grp"], row["avg"]),
                ),
            )
            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[1].remote)
            self.assertEqual(summary.stages[1].remoteTaskCount, 12)
            self.assertEqual(summary.stages[1].transport, "file")

    def testRayExecutorRunsJoinFlowRemotelyInRemoteLeafMode(self):
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
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
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)
        self.assertTrue(summary.stages[1].remote)
        self.assertTrue(summary.stages[2].remote)
        self.assertIsNone(summary.stages[2].fallbackReason)

    def testRayExecutorRunsRemoteJoinConsumerWithoutDriverExchangeLoad(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_LoadFailExchangeManager(tmpDir),
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
            self.assertTrue(executor.lastExecutionSummary.stages[2].remote)

    def testRayExecutorRunsPartitionParallelRemoteJoinConsumer(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_LoadFailExchangeManager(tmpDir),
            )
            lhs = DataFrame(
                {"id": [1, 2, 3, 4], "lv": [10, 20, 30, 40]},
                executor=executor,
            )
            rhs = DataFrame(
                {"id": [1, 3, 4, 5], "rv": [100, 300, 400, 500]},
                executor=executor,
            )

            result = lhs.join(rhs, {"id"}).select(["id", "lv", "rv"])
            self.assertEqual(len(result), 3)

            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[2].remote)
            self.assertEqual(summary.stages[2].remoteTaskCount, 12)
            self.assertEqual(len(summary.stages[2].nodeIds), 12)
            self.assertEqual(
                sorted(result.toArrow().to_pylist(), key=lambda row: row["id"]),
                [
                    {"id": 1, "lv": 10, "rv": 100},
                    {"id": 3, "lv": 30, "rv": 300},
                    {"id": 4, "lv": 40, "rv": 400},
                ],
            )

    def testRayExecutorRepublishesRemoteJoinOutputWithoutDriverGather(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                exchangeManager=_StagePublishFailExchangeManager(tmpDir, ("stage-2",)),
            )
            lhs = DataFrame(
                {"id": [1, 2, 3, 4], "lv": [10, 20, 30, 40]},
                executor=executor,
            )
            rhs = DataFrame(
                {"id": [1, 3, 4, 5], "rv": [100, 300, 400, 500]},
                executor=executor,
            )
            localLhs = DataFrame({"id": [1, 2, 3, 4], "lv": [10, 20, 30, 40]})
            localRhs = DataFrame({"id": [1, 3, 4, 5], "rv": [100, 300, 400, 500]})

            expected = (
                localLhs.join(localRhs, {"id"}).shuffle(7).select(["id", "lv", "rv"])
            )
            result = lhs.join(rhs, {"id"}).shuffle(7).select(["id", "lv", "rv"])

            self.assertEqual(
                sorted(result.toArrow().to_pylist(), key=lambda row: row["id"]),
                sorted(expected.toArrow().to_pylist(), key=lambda row: row["id"]),
            )
            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[2].remote)
            self.assertEqual(summary.stages[2].remoteTaskCount, 12)
            self.assertEqual(summary.stages[2].transport, "file")

    def testActorModeMatchesTaskModeForGroupedFlow(self):
        taskExecutor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        actorExecutor = RayExecutor(
            config=RayExecutionConfig(
                mode=RayExecutionMode.REMOTE_LEAF, workerMode=RayWorkerMode.ACTOR
            )
        )
        taskDf = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
            executor=taskExecutor,
        )
        actorDf = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]},
            executor=actorExecutor,
        )

        self.assertEqual(
            taskDf.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp"),
            actorDf.groupBy("grp").aggregate(avg=Mean("value")).orderBy("grp"),
        )
        self.assertTrue(actorExecutor.lastExecutionSummary.stages[1].remote)

    def testActorModeMatchesTaskModeForJoinFlow(self):
        taskExecutor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        actorExecutor = RayExecutor(
            config=RayExecutionConfig(
                mode=RayExecutionMode.REMOTE_LEAF, workerMode=RayWorkerMode.ACTOR
            )
        )
        taskLhs = DataFrame(
            {"id": [1, 2, 3], "lv": [10, 20, 30]},
            executor=taskExecutor,
        )
        taskRhs = DataFrame(
            {"id": [1, 3, 4], "rv": [100, 300, 400]},
            executor=taskExecutor,
        )
        actorLhs = DataFrame(
            {"id": [1, 2, 3], "lv": [10, 20, 30]},
            executor=actorExecutor,
        )
        actorRhs = DataFrame(
            {"id": [1, 3, 4], "rv": [100, 300, 400]},
            executor=actorExecutor,
        )

        self.assertEqual(
            taskLhs.join(taskRhs, {"id"}).select(["id", "lv", "rv"]).orderBy("id"),
            actorLhs.join(actorRhs, {"id"}).select(["id", "lv", "rv"]).orderBy("id"),
        )
        self.assertTrue(actorExecutor.lastExecutionSummary.stages[2].remote)

    def testRayExecutorRunsJoinFileBackedFlow(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
            lhs = DataFrame(
                {"id": [1, 2], "lv": [10, 20]},
                executor=executor,
            )
            rhs = DataFrame(
                {"id": [1, 3], "rv": [100, 300]},
                executor=executor,
            )

            localLhs = DataFrame({"id": [1, 2], "lv": [10, 20]})
            localRhs = DataFrame({"id": [1, 3], "rv": [100, 300]})

            expected = localLhs.join(localRhs, {"id"}).orderBy("id")
            result = lhs.join(rhs, {"id"}).orderBy("id")

            self.assertEqual(expected, result)

    def testRayExecutorReportsMissingExchangeManagerForConsumerStages(self):
        executor = RayExecutor()
        df = DataFrame(
            {"id": [1, 2], "tag": ["a", "b"]},
            executor=executor,
        )

        shuffled = df.shuffle(7)

        with self.assertRaisesRegex(RuntimeError, "stage-1.*exchange manager"):
            len(shuffled)

    def testRayExecutorRetriesTransientStageFailure(self):
        executor = RayExecutor(runtime=_FailOnceRuntime("stage-0", maxStageRetries=1))
        df = DataFrame({"id": [1, 2, 3]}, executor=executor)
        df.filter(df["id"] > 1)

        self.assertEqual(len(df), 2)
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.stages[0].attempt, 2)
        self.assertEqual(summary.stages[0].retryReason, "TransientStageError")

    def testRayExecutorRetriesTransientRemoteGroupedConsumerFailure(self):
        executor = RayExecutor(runtime=_FailOnceRemoteGroupedRuntime())
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
        self.assertEqual(summary.stages[1].attempt, 2)
        self.assertEqual(summary.stages[1].retryReason, "TransientStageError")

    def testRayExecutorRetriesTransientRemoteJoinFailure(self):
        executor = RayExecutor(runtime=_FailOnceRemoteJoinRuntime())
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
        self.assertTrue(summary.stages[2].remote)
        self.assertEqual(summary.stages[2].attempt, 2)
        self.assertEqual(summary.stages[2].retryReason, "TransientStageError")

    def testRayExecutorCleansUpArtifactsAfterFailure(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                runtime=_AlwaysFailConsumerRuntime(),
                exchangeManager=FileExchangeManager(tmpDir),
            )
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            with self.assertRaisesRegex(RuntimeError, "consumer stage failed"):
                len(df.shuffle(7))

            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testRayExecutorReportsPublishFailureWithStageAndExchange(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                exchangeManager=_PublishFailExchangeManager(tmpDir),
            )
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            with self.assertRaisesRegex(
                RuntimeError, "stage-0.*exchange-0.*publish failed"
            ):
                len(df.shuffle(7))

            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testRayExecutorReportsResolveFailureWithStageAndExchange(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                exchangeManager=_ResolveFailExchangeManager(tmpDir),
            )
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            with self.assertRaisesRegex(
                RuntimeError, "stage-1.*exchange-0.*resolve failed"
            ):
                len(df.shuffle(7))

            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testRayExecutorReportsDescriptorMismatchWithStageAndExchange(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                exchangeManager=_DescriptorMismatchExchangeManager(tmpDir),
            )
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            with self.assertRaisesRegex(
                RuntimeError, "stage-1.*exchange-0.*partitioning"
            ):
                len(df.shuffle(7))

            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testRayExecutorFallsBackToLocalForRemoteLeafConsumerStages(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(
                runtime=RayRuntime(
                    config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
                ),
                exchangeManager=FileExchangeManager(tmpDir),
            )
            factory = BoltPlanBuilderFactory()
            df = DataFrame(
                factory.tpchGenerator(Customer, scaleFactor=0.01, numSplits=2),
                executor=executor,
                planFactory=factory,
            )
            shuffled = df.shuffle(7)

            self.assertGreater(len(shuffled), 0)
            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[0].remote)
            self.assertFalse(summary.stages[1].remote)
            self.assertEqual(summary.stages[1].fallbackReason, "consumer_stage")

    def testRayExecutorRunsValuesWithPythonUdfRemotely(self):
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        df = DataFrame({"id": [1, 2, 3]}, executor=executor)
        df.transform(avg_id=lambda frame: frame["id"] + 1)

        self.assertEqual(len(df), 3)
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)
        self.assertIsNone(summary.stages[0].fallbackReason)

    def testRayExecutorPropagatesRegisteredVectorUdfToRemoteWorker(self):
        """A vector UDF registered on ``RayExecutor`` must be visible to
        the remote worker that executes the Substrait plan referencing
        it. Before the propagation fix the registration only landed on
        the driver's pybolt singleton; the remote worker had no entry
        and execution failed at function-name lookup in C++."""
        import pyarrow.compute as pc
        import pybolt
        from ..function.vector import vectorFunction

        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )

        # Register on the Ray executor (not on a default LocalExecutor)
        # so the registration goes through ``RayExecutor.registerVectorFunction``
        # → captured into ``executor.udfRegistry`` → shipped on every
        # ``runStageDag`` call.
        @vectorFunction(pybolt.BigintType(), executors=executor)
        def square_plus_one(value):
            arr = pybolt.exportToArrow(value)
            return pybolt.importFromArrow(pc.add(pc.multiply(arr, arr), 1))

        # Cross-check the registry before execution.
        self.assertEqual(len(executor.udfRegistry), 1)
        self.assertEqual(executor.udfRegistry.specs[0].functionName, "square_plus_one")

        df = DataFrame({"id": [1, 2, 3, 4]}, executor=executor)
        df.transform(squared_plus=lambda frame: frame["id"].map(square_plus_one))

        self.assertEqual(list(df["squared_plus"]), [2, 5, 10, 17])
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)
        self.assertIsNone(summary.stages[0].fallbackReason)

    def testRayExecutorRunsValuesLeafStageRemotely(self):
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
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
        self.assertIsNotNone(summary.stages[0].workerId)

    def testRayExecutorRunsPhaseOneTpchLeafStageRemotely(self):
        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        factory = BoltPlanBuilderFactory()
        df = DataFrame(
            factory.tpchGenerator(Customer, scaleFactor=0.01, numSplits=2),
            executor=executor,
            planFactory=factory,
        )
        query = df.select(["custkey", "nationkey"]).orderBy("custkey").select(count=5)

        self.assertEqual(len(query), 5)
        summary = executor.lastExecutionSummary
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertTrue(summary.stages[0].remote)
        self.assertIsNotNone(summary.stages[0].workerId)

    def testRayExecutorExposesExplicitStageTaskResults(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            executor = RayExecutor(exchangeManager=FileExchangeManager(tmpDir))
            df = DataFrame(
                {"id": [1, 2, 3], "tag": ["a", "b", "c"]},
                executor=executor,
            )

            self.assertEqual(len(df.shuffle(7)), 3)
            taskResults = executor.lastStageTaskResults

            self.assertEqual(len(taskResults), 2)
            self.assertIsInstance(taskResults[0], StageTaskResult)
            self.assertEqual(taskResults[0].stageId, "stage-0")
            self.assertEqual(taskResults[0].attempt, 1)
            self.assertEqual(taskResults[0].inputCount, 0)
            self.assertEqual(taskResults[1].stageId, "stage-1")
            self.assertEqual(taskResults[1].exchangeIds, ("exchange-0",))
            self.assertEqual(taskResults[1].transport, "file")


# ---------------------------------------------------------------------------
# BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES driver-collect
# ---------------------------------------------------------------------------


class _ThresholdEnv:
    """Context manager: set ``BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES``
    to the given value on entry; restore prior state on exit.

    The runtime reads the env var on every dispatch decision, so a
    test that wraps its work in this context sees the threshold-driven
    behaviour without leaking it to other tests.
    """

    def __init__(self, value: int | None) -> None:
        self.__value = value
        self.__prior: str | None = None

    def __enter__(self) -> "_ThresholdEnv":
        import os

        self.__prior = os.environ.get("BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES")
        if self.__value is None:
            os.environ.pop("BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES", None)
        else:
            os.environ["BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES"] = str(self.__value)
        return self

    def __exit__(self, *exc_info) -> None:
        import os

        if self.__prior is None:
            os.environ.pop("BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES", None)
        else:
            os.environ["BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES"] = self.__prior


class TestSmallConsumerDriverCollect(unittest.TestCase):
    """``BOLTML_LOCAL_CONSUMER_THRESHOLD_BYTES`` overrides remote
    dispatch when the consumer's resolved input bytes are below the
    threshold. Default is 0 (off). With a high threshold every
    consumer routes to the driver via the local-fallback path.
    """

    def testDefaultOffPreservesRemoteDispatch(self):
        # Threshold is unset → no small-consumer override fires;
        # a small grouped aggregate runs remotely under REMOTE_LEAF.
        with _ThresholdEnv(None):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            # Consumer stage[1] is remote.
            self.assertTrue(summary.stages[1].remote)
            self.assertIsNone(summary.stages[1].fallbackReason)

    def testHighThresholdRoutesConsumerToDriver(self):
        # Threshold is 1 GiB (way above the few-bytes test data) →
        # the consumer stage routes to the driver and is marked with
        # the new ``small_consumer_input`` fallback reason.
        with _ThresholdEnv(1 << 30):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            # Result must still be correct.
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            # The consumer stage is now LOCAL despite REMOTE_LEAF mode.
            self.assertFalse(summary.stages[1].remote)
            self.assertEqual(summary.stages[1].fallbackReason, "small_consumer_input")
            # The producer leaf is still remote (the gate only applies
            # to consumer stages).
            self.assertTrue(summary.stages[0].remote)

    def testThresholdEqualsZeroPreservesRemoteDispatch(self):
        # Explicit 0 disables the gate.
        with _ThresholdEnv(0):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            self.assertTrue(summary.stages[1].remote)


# ---------------------------------------------------------------------------
# BOLTML_LEAF_PARALLELISM leaf-stage fan-out
# ---------------------------------------------------------------------------


class _LeafParallelismEnv:
    """Context manager: set ``BOLTML_LEAF_PARALLELISM`` on entry,
    restore prior state on exit.
    """

    def __init__(self, value: int | None) -> None:
        self.__value = value
        self.__prior: str | None = None

    def __enter__(self) -> "_LeafParallelismEnv":
        import os

        self.__prior = os.environ.get("BOLTML_LEAF_PARALLELISM")
        if self.__value is None:
            os.environ.pop("BOLTML_LEAF_PARALLELISM", None)
        else:
            os.environ["BOLTML_LEAF_PARALLELISM"] = str(self.__value)
        return self

    def __exit__(self, *exc_info) -> None:
        import os

        if self.__prior is None:
            os.environ.pop("BOLTML_LEAF_PARALLELISM", None)
        else:
            os.environ["BOLTML_LEAF_PARALLELISM"] = self.__prior


class TestLeafParallelism(unittest.TestCase):
    """``BOLTML_LEAF_PARALLELISM=N`` (default 1 = off) fans a single
    leaf-producer stage out across N parallel Ray tasks. Each task
    processes ``splits[i::N]`` of the plan's input splits; shards that
    end up with zero splits short-circuit to an empty Arrow table.
    """

    def testDefaultOffPreservesSingleLeafTask(self):
        # Threshold unset → single leaf task, prior behaviour.
        with _LeafParallelismEnv(None):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            self.assertEqual(summary.stages[0].remoteTaskCount, 1)

    def testHighParallelismRefusesFanOutForUnshardedValuesSource(self):
        # N=4 would fan the leaf out into 4 Ray tasks for a shardable
        # input (e.g. a multi-file local_files source). The runtime's
        # ``_plan_has_shardable_input`` gate refuses fan-out for
        # non-shardable leaves: a Values source carries its data in
        # the plan literal, not as splits, so all N shards would
        # re-execute the full plan and emit N copies of the same data
        # (and downstream joins would amplify by N). The gate forces
        # shardCount=1 in that case. The unit-test environment doesn't
        # have a multi-file local_files source wired up, so we assert
        # the gate's correct single-task refusal here. End-to-end
        # multi-task fan-out is exercised by the SF=10 TPC-H sweep.
        with _LeafParallelismEnv(4):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            # Result is correct (single-task execution).
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            # Single task -- the gate forces shardCount=1 because
            # Values isn't shardable.
            self.assertEqual(summary.stages[0].remoteTaskCount, 1)

    def testParallelismOnePreservesSingleLeafTask(self):
        # Explicit 1 disables the fan-out.
        with _LeafParallelismEnv(1):
            executor = RayExecutor(
                config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
            )
            df = DataFrame(
                {"grp": ["a", "a", "b"], "v": [1, 2, 3]},
                executor=executor,
            )
            df.groupBy("grp").aggregate(avg=Mean("v"))
            self.assertEqual(len(df), 2)
            summary = executor.lastExecutionSummary
            self.assertEqual(summary.stages[0].remoteTaskCount, 1)


if __name__ == "__main__":
    unittest.main()
