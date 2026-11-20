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
from pathlib import Path

import ray

from ...dataframe import DataFrame
from ...distributed.dispatcher.placeholders import PlaceholderRead
from ...distributed.ray.exchange_manager import AdaptiveExchangeManager
from ...distributed.ray.exchange_manager import ExchangeTransportPolicy
from ...distributed.ray.exchange_manager import ExchangePartitionDescriptor
from ...distributed.ray.exchange_manager import FileExchangeManager
from ...distributed.ray.exchange_manager import publishExchangePartition
from ...distributed.ray.exchange_registry import getExchangeRegistry


class TestExchangeManager(unittest.TestCase):
    def testExchangeDescriptorExposesIdentityAndExplain(self):
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

        self.assertEqual(descriptor.identity, ("stage-0", "exchange-0"))
        self.assertIn(
            "ExchangePartitionDescriptor[stage-0/exchange-0/part-0]",
            descriptor.explain(),
        )
        self.assertIn("partitions=1", descriptor.explain())
        self.assertIn("partitioning=hash(id)", descriptor.explain())
        self.assertIn("shard=0", descriptor.explain())

    def testFileExchangeManagerPublishesAndResolvesDescriptor(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            df = DataFrame({"id": [1, 2], "tag": ["a", "b"]})

            descriptors = manager.publish("stage-0", "exchange-0", df, "hash(id)")

            self.assertEqual(len(descriptors), 1)
            descriptor = descriptors[0]
            self.assertIsInstance(descriptor, ExchangePartitionDescriptor)
            self.assertEqual(descriptor.transport, "file")
            self.assertEqual(descriptor.partitioning, "hash(id)")
            self.assertEqual(descriptor.outputNames, ("id", "tag"))
            self.assertTrue(Path(descriptor.path).exists())

            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-0",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
            )
            resolved = manager.resolve(placeholder)

            self.assertEqual(resolved, descriptors)
            placeholder.validateDescriptor(descriptor)

    def testPlaceholderReadRejectsDescriptorOutputMismatch(self):
        placeholder = PlaceholderRead(
            sourceStageId="stage-0",
            exchangeId="exchange-0",
            outputNames=("id", "tag"),
            outputTypes=("BIGINT", "VARCHAR"),
            partitioning="hash(id)",
        )
        with self.assertRaisesRegex(ValueError, "output names mismatch"):
            placeholder.validateDescriptor(
                ExchangePartitionDescriptor(
                    executionId="",
                    stageId="stage-0",
                    exchangeId="exchange-0",
                    attemptId=1,
                    partitionCount=1,
                    partitionId=0,
                    path="/tmp/part-0.parquet",
                    outputNames=("id", "value"),
                    outputTypes=("BIGINT", "VARCHAR"),
                    partitioning="hash(id)",
                )
            )
        with self.assertRaisesRegex(ValueError, "output types mismatch"):
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
                    outputTypes=("BIGINT", "BIGINT"),
                    partitioning="hash(id)",
                )
            )

    def testFileExchangeManagerLoadsPublishedPartition(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})

            descriptor = manager.publish("stage-0", "exchange-0", expected, "hash(id)")[
                0
            ]
            result = manager.load(descriptor)

            self.assertEqual(expected, result)

    def testFileExchangeManagerPublishesMultiplePartitions(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            partitions = (
                DataFrame({"id": [1, 3], "tag": ["a", "c"]}),
                DataFrame({"id": [2], "tag": ["b"]}),
            )

            descriptors = manager.publishPartitions(
                "stage-0",
                "exchange-0",
                partitions,
                "hash(id)",
            )

            self.assertEqual(tuple(d.partitionId for d in descriptors), (0, 1))
            self.assertEqual(
                manager.resolve(
                    PlaceholderRead("stage-0", "exchange-0", ("id", "tag"))
                ),
                descriptors,
            )
            self.assertEqual(partitions[0], manager.load(descriptors[0]))
            self.assertEqual(partitions[1], manager.load(descriptors[1]))

    def testFileExchangeManagerCleanupRemovesPublishedArtifacts(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            df = DataFrame({"id": [1, 2], "tag": ["a", "b"]})

            descriptor = manager.publish("stage-0", "exchange-0", df, "hash(id)")[0]
            self.assertTrue(Path(descriptor.path).exists())

            manager.cleanup()

            self.assertFalse(Path(descriptor.path).exists())
            with self.assertRaises(KeyError):
                manager.resolve(
                    PlaceholderRead(
                        sourceStageId="stage-0",
                        exchangeId="exchange-0",
                        outputNames=("id", "tag"),
                    )
                )

    def testAdaptiveExchangeManagerUsesObjectTransportForSmallInputs(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-object-small"

            descriptor = manager.publish(
                "stage-0", "exchange-0", expected, "hash(id)", executionId=executionId
            )[0]

            self.assertEqual(descriptor.transport, "object")
            self.assertEqual(
                manager.resolve(
                    PlaceholderRead(
                        "stage-0", "exchange-0", ("id", "tag"), executionId=executionId
                    )
                ),
                (descriptor,),
            )
            self.assertEqual(expected, manager.load(descriptor))
            self.assertEqual(list(Path(tmpDir).rglob("*.parquet")), [])

    def testAdaptiveExchangeManagerPublishesMultipleObjectPartitions(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            executionId = "exec-object-multi"
            partitions = (
                DataFrame({"id": [1, 3], "tag": ["a", "c"]}),
                DataFrame({"id": [2], "tag": ["b"]}),
            )

            descriptors = manager.publishPartitions(
                "stage-0",
                "exchange-0",
                partitions,
                "hash(id)",
                executionId=executionId,
            )

            self.assertEqual(
                tuple(d.transport for d in descriptors), ("object", "object")
            )
            self.assertEqual(tuple(d.partitionId for d in descriptors), (0, 1))
            self.assertEqual(partitions[0], manager.load(descriptors[0]))
            self.assertEqual(partitions[1], manager.load(descriptors[1]))

    def testAdaptiveExchangeManagerResolvesObjectDescriptorsAcrossManagers(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            producer = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            resolver = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-cross-manager-object"

            descriptor = producer.publish(
                "stage-0", "exchange-0", expected, "hash(id)", executionId=executionId
            )[0]
            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-0",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
                executionId=executionId,
            )

            self.assertEqual(resolver.resolve(placeholder), (descriptor,))
            self.assertEqual(expected, resolver.load(descriptor))

    def testAdaptiveExchangeManagerCleanupRemovesRegistryEntries(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            producer = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            resolver = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-cleanup-object"
            descriptor = producer.publish(
                "stage-0", "exchange-0", expected, "hash(id)", executionId=executionId
            )[0]
            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-0",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
                executionId=executionId,
            )

            producer.cleanup()

            with self.assertRaises(KeyError):
                resolver.resolve(placeholder)
            with self.assertRaises(KeyError):
                resolver.load(descriptor)

    def testAdaptiveExchangeManagerCleanupRemovesIngestedObjectDescriptors(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            descriptor = publishExchangePartition(
                manager.publishConfig(),
                "",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1, 2], "tag": ["a", "b"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            manager.ingestDescriptors((descriptor,), owned=True)
            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-0",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
            )

            self.assertEqual(manager.resolve(placeholder), (descriptor,))

            manager.cleanup()

            with self.assertRaises(KeyError):
                manager.resolve(placeholder)
            with self.assertRaises(KeyError):
                manager.load(descriptor)

    def testFileExchangeManagerIngestDescriptorsPreservesDistinctShards(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            config = manager.publishConfig()
            descriptors = (
                publishExchangePartition(
                    config,
                    "",
                    "stage-1",
                    "exchange-0",
                    DataFrame({"id": [1], "tag": ["a"]}),
                    "hash(id)",
                    attemptId=1,
                    partitionId=0,
                    partitionCount=1,
                    shardId="0",
                ),
                publishExchangePartition(
                    config,
                    "",
                    "stage-1",
                    "exchange-0",
                    DataFrame({"id": [2], "tag": ["b"]}),
                    "hash(id)",
                    attemptId=1,
                    partitionId=0,
                    partitionCount=1,
                    shardId="1",
                ),
            )

            manager.ingestDescriptors(descriptors)
            resolved = manager.resolve(
                PlaceholderRead("stage-1", "exchange-0", ("id", "tag"))
            )

            self.assertEqual(
                tuple(descriptor.shardId for descriptor in resolved), ("0", "1")
            )

    def testAdaptiveExchangeManagerReplacesOlderAttemptDescriptors(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            executionId = "exec-replace-attempt"
            first = publishExchangePartition(
                manager.publishConfig(),
                executionId,
                "stage-1",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            manager.ingestDescriptors((first,), owned=True)

            second = publishExchangePartition(
                manager.publishConfig(),
                executionId,
                "stage-1",
                "exchange-0",
                DataFrame({"id": [2], "tag": ["b"]}),
                "hash(id)",
                attemptId=2,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            manager.ingestDescriptors((second,), owned=True)

            resolved = manager.resolve(
                PlaceholderRead(
                    "stage-1", "exchange-0", ("id", "tag"), executionId=executionId
                )
            )

            self.assertEqual(resolved, (second,))
            self.assertEqual(manager.load(second), DataFrame({"id": [2], "tag": ["b"]}))
            with self.assertRaises(KeyError):
                manager.load(first)

    def testFileExchangeManagerUsesAttemptScopedPaths(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = FileExchangeManager(tmpDir)
            first = manager.publish(
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1]}),
                "hash(id)",
                executionId="exec-file-attempt",
                attemptId=1,
            )[0]
            second = manager.publish(
                "stage-0",
                "exchange-0",
                DataFrame({"id": [2]}),
                "hash(id)",
                executionId="exec-file-attempt",
                attemptId=2,
            )[0]

            self.assertIn("attempt-1", first.path)
            self.assertIn("attempt-2", second.path)
            self.assertNotEqual(first.path, second.path)
            self.assertTrue(Path(first.path).exists())
            self.assertTrue(Path(second.path).exists())

    def testAdaptiveExchangeManagerUsesAttemptScopedFilePaths(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=1
                ),
            )
            first = manager.publish(
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1, 2]}),
                "hash(id)",
                executionId="exec-adaptive-file-attempt",
                attemptId=1,
            )[0]
            second = manager.publish(
                "stage-0",
                "exchange-0",
                DataFrame({"id": [3, 4]}),
                "hash(id)",
                executionId="exec-adaptive-file-attempt",
                attemptId=2,
            )[0]

            self.assertEqual(first.transport, "file")
            self.assertEqual(second.transport, "file")
            self.assertIn("attempt-1", first.path)
            self.assertIn("attempt-2", second.path)
            self.assertNotEqual(first.path, second.path)

    def testAdaptiveExchangeManagerIgnoresStaleAttemptDescriptors(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=10
                ),
            )
            executionId = "exec-ignore-stale"
            latest = publishExchangePartition(
                manager.publishConfig(),
                executionId,
                "stage-1",
                "exchange-0",
                DataFrame({"id": [2], "tag": ["b"]}),
                "hash(id)",
                attemptId=2,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            manager.ingestDescriptors((latest,), owned=True)

            stale = publishExchangePartition(
                manager.publishConfig(),
                executionId,
                "stage-1",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            manager.ingestDescriptors((stale,), owned=True)

            resolved = manager.resolve(
                PlaceholderRead(
                    "stage-1", "exchange-0", ("id", "tag"), executionId=executionId
                )
            )

            self.assertEqual(resolved, (latest,))
            self.assertEqual(manager.load(latest), DataFrame({"id": [2], "tag": ["b"]}))

    def testAdaptiveExchangeManagerResolveRefreshesNewerRegistryAttempt(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(tmpDir)
            registry = getExchangeRegistry()
            stageId = "stage-cache"
            exchangeId = "exchange-cache"
            placeholder = PlaceholderRead(
                stageId, exchangeId, ("id",), executionId="exec-cache"
            )

            first = ExchangePartitionDescriptor(
                executionId="exec-cache",
                stageId=stageId,
                exchangeId=exchangeId,
                attemptId=1,
                partitionCount=1,
                partitionId=0,
                path="/tmp/cache-attempt-1.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            )
            second = ExchangePartitionDescriptor(
                executionId="exec-cache",
                stageId=stageId,
                exchangeId=exchangeId,
                attemptId=2,
                partitionCount=1,
                partitionId=0,
                path="/tmp/cache-attempt-2.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            )

            ray.get(
                registry.registerExchange.remote(
                    "owner-cache", "exec-cache", stageId, exchangeId, (first,)
                )
            )
            self.assertEqual(manager.resolve(placeholder), (first,))

            ray.get(
                registry.registerExchange.remote(
                    "owner-cache", "exec-cache", stageId, exchangeId, (second,)
                )
            )
            self.assertEqual(manager.resolve(placeholder), (second,))

            ray.get(
                registry.deleteExchange.remote(
                    "owner-cache", "exec-cache", stageId, exchangeId
                )
            )

    def testAdaptiveExchangeManagerResolveIsExecutionScoped(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(tmpDir)
            registry = getExchangeRegistry()
            stageId = "stage-shared"
            exchangeId = "exchange-shared"
            execA = ExchangePartitionDescriptor(
                executionId="exec-a",
                stageId=stageId,
                exchangeId=exchangeId,
                attemptId=1,
                partitionCount=1,
                partitionId=0,
                path="/tmp/exec-a.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            )
            execB = ExchangePartitionDescriptor(
                executionId="exec-b",
                stageId=stageId,
                exchangeId=exchangeId,
                attemptId=1,
                partitionCount=1,
                partitionId=0,
                path="/tmp/exec-b.parquet",
                outputNames=("id",),
                outputTypes=("BIGINT",),
                partitioning="hash(id)",
            )

            ray.get(
                registry.registerExchange.remote(
                    "owner-a", "exec-a", stageId, exchangeId, (execA,)
                )
            )
            ray.get(
                registry.registerExchange.remote(
                    "owner-b", "exec-b", stageId, exchangeId, (execB,)
                )
            )

            self.assertEqual(
                manager.resolve(
                    PlaceholderRead(stageId, exchangeId, ("id",), executionId="exec-a")
                ),
                (execA,),
            )
            self.assertEqual(
                manager.resolve(
                    PlaceholderRead(stageId, exchangeId, ("id",), executionId="exec-b")
                ),
                (execB,),
            )

            ray.get(
                registry.deleteExchange.remote("owner-a", "exec-a", stageId, exchangeId)
            )
            ray.get(
                registry.deleteExchange.remote("owner-b", "exec-b", stageId, exchangeId)
            )

    def testExchangeRegistryRejectsForeignExchangeDeletion(self):
        registry = getExchangeRegistry()
        descriptor = ExchangePartitionDescriptor(
            executionId="exec-owner-check",
            stageId="stage-0",
            exchangeId="exchange-0",
            attemptId=1,
            partitionCount=1,
            partitionId=0,
            path="/tmp/owner-check.parquet",
            outputNames=("id",),
            outputTypes=("BIGINT",),
            partitioning="hash(id)",
        )

        try:
            ray.get(
                registry.registerExchange.remote(
                    "owner-a",
                    "exec-owner-check",
                    "stage-0",
                    "exchange-0",
                    (descriptor,),
                )
            )
            deleted = ray.get(
                registry.deleteExchange.remote(
                    "owner-b", "exec-owner-check", "stage-0", "exchange-0"
                )
            )

            self.assertFalse(deleted)
            self.assertEqual(
                ray.get(
                    registry.resolve.remote("exec-owner-check", "stage-0", "exchange-0")
                ),
                (descriptor,),
            )
        finally:
            ray.get(
                registry.deleteExchange.remote(
                    "owner-a", "exec-owner-check", "stage-0", "exchange-0"
                )
            )

    def testExchangeRegistryRejectsForeignObjectDeletion(self):
        registry = getExchangeRegistry()
        objectKey = "owner-check-object"

        try:
            self.assertTrue(
                ray.get(
                    registry.registerObject.remote("owner-a", objectKey, "payload-a")
                )
            )
            self.assertFalse(
                ray.get(registry.deleteObject.remote("owner-b", objectKey))
            )
            self.assertEqual(ray.get(registry.getObject.remote(objectKey)), "payload-a")
        finally:
            ray.get(registry.deleteObject.remote("owner-a", objectKey))

    def testExchangeRegistryPrepareCommitAndAbortLifecycle(self):
        registry = getExchangeRegistry()
        descriptor = ExchangePartitionDescriptor(
            executionId="exec-prepare",
            stageId="stage-0",
            exchangeId="exchange-0",
            attemptId=1,
            partitionCount=1,
            partitionId=0,
            path="/tmp/prepare.parquet",
            outputNames=("id",),
            outputTypes=("BIGINT",),
            partitioning="hash(id)",
        )
        try:
            reservation = ray.get(
                registry.prepareExchange.remote(
                    "owner-a", "exec-prepare", "stage-0", "exchange-0", 1
                )
            )
            self.assertTrue(reservation["accepted"])
            self.assertIn("token", reservation)
            self.assertIsNone(
                ray.get(
                    registry.resolve.remote("exec-prepare", "stage-0", "exchange-0")
                )
            )

            committed = ray.get(
                registry.commitExchange.remote(
                    "owner-a",
                    "exec-prepare",
                    "stage-0",
                    "exchange-0",
                    reservation["token"],
                    (descriptor,),
                )
            )
            self.assertTrue(committed["accepted"])
            self.assertEqual(
                ray.get(
                    registry.resolve.remote("exec-prepare", "stage-0", "exchange-0")
                ),
                (descriptor,),
            )

            reservation = ray.get(
                registry.prepareExchange.remote(
                    "owner-a", "exec-prepare", "stage-0", "exchange-1", 1
                )
            )
            self.assertTrue(reservation["accepted"])
            self.assertTrue(
                ray.get(
                    registry.abortExchange.remote(
                        "owner-a",
                        "exec-prepare",
                        "stage-0",
                        "exchange-1",
                        reservation["token"],
                    )
                )
            )
            self.assertIsNone(
                ray.get(
                    registry.resolve.remote("exec-prepare", "stage-0", "exchange-1")
                )
            )
        finally:
            ray.get(
                registry.deleteExchange.remote(
                    "owner-a", "exec-prepare", "stage-0", "exchange-0"
                )
            )

    def testExchangeRegistryPrepareRejectsForeignPendingOwner(self):
        registry = getExchangeRegistry()
        try:
            reservation = ray.get(
                registry.prepareExchange.remote(
                    "owner-a", "exec-pending", "stage-0", "exchange-0", 1
                )
            )
            self.assertTrue(reservation["accepted"])
            foreign = ray.get(
                registry.prepareExchange.remote(
                    "owner-b", "exec-pending", "stage-0", "exchange-0", 1
                )
            )
            self.assertFalse(foreign["accepted"])
        finally:
            ray.get(
                registry.abortExchange.remote(
                    "owner-a",
                    "exec-pending",
                    "stage-0",
                    "exchange-0",
                    reservation["token"],
                )
            )

    def testAdaptiveExchangeManagerRejectsForeignExchangeRegistration(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            ownerA = AdaptiveExchangeManager(tmpDir)
            ownerB = AdaptiveExchangeManager(tmpDir)
            placeholder = PlaceholderRead(
                "stage-0", "exchange-0", ("id",), executionId="exec-owned"
            )
            descriptorA = publishExchangePartition(
                ownerA.publishConfig(),
                "exec-owned",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )
            ownerA.ingestDescriptors((descriptorA,), owned=True)

            descriptorB = publishExchangePartition(
                ownerB.publishConfig(),
                "exec-owned",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [2]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )

            with self.assertRaisesRegex(
                RuntimeError, "Registry rejected exchange registration"
            ):
                ownerB.ingestDescriptors((descriptorB,), owned=True)

            self.assertEqual(ownerB.resolve(placeholder), (descriptorA,))
            ownerB.cleanup()
            self.assertEqual(ownerA.resolve(placeholder), (descriptorA,))

            ownerA.cleanup()

    def testAdaptiveExchangeManagerCleanupDoesNotDeleteResolvedForeignExchange(self):
        with (
            tempfile.TemporaryDirectory() as producerDir,
            tempfile.TemporaryDirectory() as resolverDir,
        ):
            producer = AdaptiveExchangeManager(producerDir)
            resolver = AdaptiveExchangeManager(resolverDir)
            descriptor = publishExchangePartition(
                producer.publishConfig(),
                "exec-owner",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="remote-0",
            )
            producer.ingestDescriptors((descriptor,), owned=True)
            placeholder = PlaceholderRead(
                "stage-0", "exchange-0", ("id", "tag"), executionId="exec-owner"
            )

            self.assertEqual(resolver.resolve(placeholder), (descriptor,))

            resolver.cleanup()

            self.assertEqual(producer.resolve(placeholder), (descriptor,))
            self.assertEqual(
                producer.load(descriptor), DataFrame({"id": [1], "tag": ["a"]})
            )
            producer.cleanup()

    def testAdaptiveExchangeManagerCleanupCanTargetSingleExecution(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(tmpDir)
            first = publishExchangePartition(
                manager.publishConfig(),
                "exec-a",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )
            second = publishExchangePartition(
                manager.publishConfig(),
                "exec-b",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [2], "tag": ["b"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )
            manager.ingestDescriptors((first,), owned=True)
            manager.ingestDescriptors((second,), owned=True)

            manager.cleanup(executionId="exec-a")

            with self.assertRaises(KeyError):
                manager.resolve(
                    PlaceholderRead(
                        "stage-0", "exchange-0", ("id", "tag"), executionId="exec-a"
                    )
                )
            self.assertEqual(
                manager.resolve(
                    PlaceholderRead(
                        "stage-0", "exchange-0", ("id", "tag"), executionId="exec-b"
                    )
                ),
                (second,),
            )
            self.assertEqual(manager.load(second), DataFrame({"id": [2], "tag": ["b"]}))
            manager.cleanup()

    def testAdaptiveExchangeManagerCleanupDoesNotDeleteForeignIngestedExchange(self):
        with (
            tempfile.TemporaryDirectory() as producerDir,
            tempfile.TemporaryDirectory() as observerDir,
        ):
            producer = AdaptiveExchangeManager(producerDir)
            observer = AdaptiveExchangeManager(observerDir)
            descriptor = publishExchangePartition(
                producer.publishConfig(),
                "exec-foreign",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )
            producer.ingestDescriptors((descriptor,), owned=True)
            observer.ingestDescriptors((descriptor,), owned=False)

            observer.cleanup()

            placeholder = PlaceholderRead(
                "stage-0", "exchange-0", ("id", "tag"), executionId="exec-foreign"
            )
            self.assertEqual(producer.resolve(placeholder), (descriptor,))
            self.assertEqual(
                producer.load(descriptor), DataFrame({"id": [1], "tag": ["a"]})
            )
            producer.cleanup()

    def testAdaptiveExchangeManagerResolveDropsStaleForeignCache(self):
        with (
            tempfile.TemporaryDirectory() as producerDir,
            tempfile.TemporaryDirectory() as observerDir,
        ):
            producer = AdaptiveExchangeManager(producerDir)
            observer = AdaptiveExchangeManager(observerDir)
            descriptor = publishExchangePartition(
                producer.publishConfig(),
                "exec-stale",
                "stage-0",
                "exchange-0",
                DataFrame({"id": [1], "tag": ["a"]}),
                "hash(id)",
                attemptId=1,
                partitionId=0,
                partitionCount=1,
                shardId="0",
            )
            producer.ingestDescriptors((descriptor,), owned=True)
            placeholder = PlaceholderRead(
                "stage-0", "exchange-0", ("id", "tag"), executionId="exec-stale"
            )

            self.assertEqual(observer.resolve(placeholder), (descriptor,))

            producer.cleanup()

            with self.assertRaises(KeyError):
                observer.resolve(placeholder)

    def testAdaptiveExchangeManagerFallsBackToFileTransport(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            manager = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object",
                    maxRowsForObject=1,
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-file-fallback"

            descriptor = manager.publish(
                "stage-0", "exchange-0", expected, "hash(id)", executionId=executionId
            )[0]

            self.assertEqual(descriptor.transport, "file")
            self.assertTrue(Path(descriptor.path).exists())

    def testAdaptiveExchangeManagerResolvesFileDescriptorsAcrossManagers(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            producer = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=1
                ),
            )
            resolver = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=1
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-file-cross-manager"

            descriptor = producer.publish(
                "stage-0", "exchange-1", expected, "hash(id)", executionId=executionId
            )[0]
            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-1",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
                executionId=executionId,
            )

            self.assertEqual(descriptor.transport, "file")
            self.assertEqual(resolver.resolve(placeholder), (descriptor,))
            self.assertEqual(expected, resolver.load(descriptor))

    def testAdaptiveExchangeManagerCleanupRemovesFileRegistryEntries(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            producer = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=1
                ),
            )
            resolver = AdaptiveExchangeManager(
                tmpDir,
                policy=ExchangeTransportPolicy(
                    preferredTransport="object", maxRowsForObject=1
                ),
            )
            expected = DataFrame({"id": [1, 2], "tag": ["a", "b"]})
            executionId = "exec-file-cleanup"

            descriptor = producer.publish(
                "stage-0", "exchange-1", expected, "hash(id)", executionId=executionId
            )[0]
            placeholder = PlaceholderRead(
                sourceStageId="stage-0",
                exchangeId="exchange-1",
                outputNames=("id", "tag"),
                partitioning="hash(id)",
                executionId=executionId,
            )

            producer.cleanup()

            with self.assertRaises(KeyError):
                resolver.resolve(placeholder)
            with self.assertRaises(FileNotFoundError):
                resolver.load(descriptor)


if __name__ == "__main__":
    unittest.main()
