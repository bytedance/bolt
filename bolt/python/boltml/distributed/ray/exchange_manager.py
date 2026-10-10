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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import ray

from ...dataframe import DataFrame
from ..dispatcher.placeholders import PlaceholderRead
from ...logging import boltmlDebugLog, boltmlTraceLog
from ._typed_arrow import (
    arrow_table_with_bolt_types,
    dataframe_from_typed_arrow,
    empty_dataframe_with_bolt_types,
)
from .exchange_registry import getExchangeRegistry


def _empty_dataframe_for_descriptor(
    descriptor: "ExchangePartitionDescriptor", arrow_schema
) -> DataFrame:
    """Build a schema-bearing empty ``DataFrame`` for *descriptor*.

    Prefer the bolt-type information persisted in
    ``ExchangePartitionDescriptor.outputTypes`` to construct columns with
    their declared types. Falls back to the Arrow schema when
    no bolt types are available (legacy descriptors written before this
    fix) or when parsing the bolt types fails.
    """
    if (
        descriptor.outputTypes
        and descriptor.outputNames
        and len(descriptor.outputTypes) == len(descriptor.outputNames)
    ):
        try:
            return empty_dataframe_with_bolt_types(
                descriptor.outputNames, descriptor.outputTypes
            )
        except Exception:  # noqa: BLE001
            # Type parse / construction failure — fall through to the
            # Arrow-default empty form. The warning helper in
            # ``_typed_arrow`` would have already logged the parse
            # failure on a non-empty partition with the same type.
            pass
    return DataFrame(
        pa.StructArray.from_arrays(
            [pa.array([], type=field.type) for field in arrow_schema],
            fields=list(arrow_schema),
        )
    )


@dataclass(frozen=True)
class ExchangePartitionDescriptor:
    executionId: str
    stageId: str
    exchangeId: str
    attemptId: int
    partitionCount: int
    partitionId: int
    path: str
    outputNames: tuple[str, ...]
    outputTypes: tuple[str, ...]
    partitioning: str
    transport: str = "file"
    objectKey: str | None = None
    shardId: str = "0"
    # Ray node id of the producer for file transport. File transport
    # writes producer-local absolute paths under the driver tmp dir; a
    # consumer scheduled on a different node cannot read them. The
    # load path compares this against the consumer's current node id
    # and raises a clear error on mismatch. ``None`` for object
    # transport (paths are not node-local) or for legacy descriptors
    # written before this field was added.
    producerNodeId: str | None = None

    @property
    def identity(self) -> tuple[str, str]:
        return (self.stageId, self.exchangeId)

    def explain(self) -> str:
        parts = [
            f"ExchangePartitionDescriptor[{self.stageId}/{self.exchangeId}/part-{self.partitionId}]",
            f"execution={self.executionId}",
            f"attempt={self.attemptId}",
            f"output={','.join(self.outputNames)}",
            f"partitioning={self.partitioning}",
            f"partitions={self.partitionCount}",
            f"transport={self.transport}",
        ]
        if self.outputTypes:
            parts.append(f"types={','.join(self.outputTypes)}")
        parts.append(f"shard={self.shardId}")
        if self.objectKey is not None:
            parts.append(f"object_key={self.objectKey}")
        return " ".join(parts)


@dataclass(frozen=True)
class ExchangePublishConfig:
    rootDir: str
    ownerId: str = ""
    preferredTransport: str = "file"
    # ``maxRowsForObject`` semantics:
    #   < 0  → always object (used when runtime forces object transport in
    #          multi-node mode because file is unsafe across the cluster)
    #   == 0 → object disabled, always file (default)
    #   > N  → object when partition has <= N rows, file otherwise
    maxRowsForObject: int = 0
    # Declares whether ``rootDir`` lives on a filesystem mounted on
    # every Ray node (set when the operator provides
    # ``BOLTML_EXCHANGE_ROOT``). When True, file-transport descriptors
    # publish with ``producerNodeId=None`` so the consumer-side
    # cross-node check accepts reads from any node — without this,
    # the shared-root wiring is half-fixed: writes land on the shared
    # mount but reads from a different node still get refused by
    # ``loadExchangeDescriptor``.
    sharedFilesystem: bool = False

    def chooseTransport(self, dataframe: DataFrame) -> str:
        if self.preferredTransport != "object":
            return "file"
        if self.maxRowsForObject < 0:
            return "object"
        if self.maxRowsForObject == 0:
            return "file"
        if len(dataframe) > self.maxRowsForObject:
            return "file"
        return "object"


@dataclass(frozen=True)
class ExchangeTransportPolicy:
    preferredTransport: str = "file"
    maxRowsForObject: int = 0
    sharedFilesystem: bool = False

    def chooseTransport(self, dataframe: DataFrame) -> str:
        if self.preferredTransport != "object":
            return "file"
        if self.maxRowsForObject < 0:
            return "object"
        if self.maxRowsForObject == 0:
            return "file"
        if len(dataframe) > self.maxRowsForObject:
            return "file"
        return "object"


def loadExchangeDescriptor(descriptor: ExchangePartitionDescriptor) -> DataFrame:
    import time

    t0 = time.perf_counter()
    boltmlDebugLog(
        "exchange:load",
        f"load descriptor={descriptor.stageId}/{descriptor.exchangeId}/part-{descriptor.partitionId} transport={descriptor.transport}",
    )
    if descriptor.transport == "object":
        if descriptor.objectKey is None:
            raise KeyError(f"Unknown object transport payload: {descriptor.objectKey}")
        registry = getExchangeRegistry()
        objectPayload = ray.get(registry.getObject.remote(descriptor.objectKey))
        if objectPayload is None:
            raise KeyError(f"Unknown object transport payload: {descriptor.objectKey}")
        if isinstance(objectPayload, ray.ObjectRef):
            objectPayload = ray.get(objectPayload)
        rows = len(objectPayload)
        nbytes = objectPayload.nbytes if hasattr(objectPayload, "nbytes") else 0
        cols = (
            list(objectPayload.schema.names) if hasattr(objectPayload, "schema") else []
        )
        boltmlDebugLog(
            "exchange:load:done",
            f"loaded {descriptor.stageId}/{descriptor.exchangeId}/part-{descriptor.partitionId} "
            f"transport=object rows={rows} bytes={nbytes:,} cols={len(cols)}={cols[:6]}{'...' if len(cols) > 6 else ''} "
            f"elapsed_ms={int((time.perf_counter() - t0) * 1000)}",
        )
        if rows == 0:
            # Construct an empty vector with explicit Bolt types so its
            # schema matches the corresponding non-empty partitions.
            return _empty_dataframe_for_descriptor(descriptor, objectPayload.schema)
        # Rebuild any custom-typed columns from the schema-metadata bolt
        # types (set by ``arrow_table_with_bolt_types`` at publish).
        # ``descriptor.outputTypes`` is a fallback for legacy descriptors
        # written before the metadata tagging landed.
        return dataframe_from_typed_arrow(
            objectPayload, fallback_types=descriptor.outputTypes
        )

    # File transport: validate that we are on the same node as the producer.
    # The file path is a producer-local absolute path; if the consumer has
    # been scheduled on a different node we would otherwise hit a confusing
    # FileNotFoundError. Belt-and-suspenders for the auto-switch in
    # ``RayRuntime.runStageDag`` (which routes multi-node clusters without a
    # shared ``BOLTML_EXCHANGE_ROOT`` to object transport up front).
    if descriptor.producerNodeId is not None:
        try:
            current_node_id = ray.get_runtime_context().get_node_id()
        except Exception:  # noqa: BLE001
            current_node_id = None
        if current_node_id and current_node_id != descriptor.producerNodeId:
            raise RuntimeError(
                f"File-transport exchange descriptor for "
                f"{descriptor.stageId}/{descriptor.exchangeId}/"
                f"part-{descriptor.partitionId} was produced on node "
                f"{descriptor.producerNodeId} but the consumer is running "
                f"on node {current_node_id}. File transport writes "
                f"producer-local paths; cross-node reads require a shared "
                f"filesystem. Either set ``BOLTML_EXCHANGE_ROOT`` to a "
                f"path mounted on every Ray node, or set "
                f"``BOLTML_EXCHANGE_TRANSPORT=object`` to use the Ray "
                f"object store instead."
            )
    table = pq.read_table(descriptor.path)
    rows = len(table)
    nbytes = table.nbytes
    cols = list(table.schema.names)
    try:
        on_disk_bytes = Path(descriptor.path).stat().st_size
    except Exception:  # noqa: BLE001
        on_disk_bytes = -1
    boltmlDebugLog(
        "exchange:load:done",
        f"loaded {descriptor.stageId}/{descriptor.exchangeId}/part-{descriptor.partitionId} "
        f"transport=file rows={rows} arrow_bytes={nbytes:,} parquet_bytes={on_disk_bytes:,} "
        f"cols={len(cols)}={cols[:6]}{'...' if len(cols) > 6 else ''} "
        f"elapsed_ms={int((time.perf_counter() - t0) * 1000)}",
    )
    if rows == 0:
        return _empty_dataframe_for_descriptor(descriptor, table.schema)
    return dataframe_from_typed_arrow(table, fallback_types=descriptor.outputTypes)


def publishExchangePartition(
    config: ExchangePublishConfig,
    executionId: str,
    stageId: str,
    exchangeId: str,
    dataframe: DataFrame,
    partitioning: str,
    attemptId: int,
    partitionId: int,
    partitionCount: int,
    shardId: str | None = None,
) -> ExchangePartitionDescriptor:
    import time

    t0 = time.perf_counter()
    resolvedShardId = shardId or "0"
    transport = config.chooseTransport(dataframe)
    if transport == "object":
        objectKey = (
            f"{executionId}-{stageId}-{exchangeId}-attempt-{attemptId}-"
            f"part-{partitionId}-{resolvedShardId}-{uuid4().hex}"
        )
        # Tag the Arrow table with bolt-type metadata so the consumer
        # can restore each column's declared type on load.
        table = arrow_table_with_bolt_types(dataframe)
        objectRef = ray.put(table)
        registered = ray.get(
            getExchangeRegistry().registerObject.remote(
                config.ownerId, objectKey, objectRef
            )
        )
        if not registered:
            raise RuntimeError(
                f"Registry rejected object registration for {objectKey}."
            )
        boltmlDebugLog(
            "exchange:publish",
            f"published {stageId}/{exchangeId}/part-{partitionId} transport=object "
            f"rows={table.num_rows} arrow_bytes={table.nbytes:,} cols={len(table.schema.names)}="
            f"{list(table.schema.names)[:6]}{'...' if len(table.schema.names) > 6 else ''} "
            f"elapsed_ms={int((time.perf_counter() - t0) * 1000)}",
        )
        return ExchangePartitionDescriptor(
            executionId=executionId,
            stageId=stageId,
            exchangeId=exchangeId,
            attemptId=attemptId,
            partitionCount=partitionCount,
            partitionId=partitionId,
            path="",
            outputNames=tuple(dataframe.dtype.names()),
            outputTypes=tuple(
                str(dataframe.dtype.childAt(i)) for i in range(len(dataframe.dtype))
            ),
            partitioning=partitioning,
            transport="object",
            objectKey=objectKey,
            shardId=resolvedShardId,
        )

    exchangeDir = (
        Path(config.rootDir)
        / executionId
        / stageId
        / exchangeId
        / f"attempt-{attemptId}"
    )
    exchangeDir.mkdir(parents=True, exist_ok=True)
    fileSuffix = f"part-{partitionId}"
    if shardId is not None:
        fileSuffix = f"{fileSuffix}-shard-{resolvedShardId}"
    path = exchangeDir / f"{fileSuffix}.parquet"
    # Tag the Arrow table with bolt-type metadata so the consumer can
    # restore the declared column types on load. Parquet
    # preserves Arrow schema metadata round-trips, so the tag survives
    # the write/read cycle.
    table = arrow_table_with_bolt_types(dataframe)
    pq.write_table(table, path)
    parquet_bytes = path.stat().st_size
    boltmlDebugLog(
        "exchange:publish",
        f"published {stageId}/{exchangeId}/part-{partitionId} transport=file "
        f"rows={table.num_rows} arrow_bytes={table.nbytes:,} parquet_bytes={parquet_bytes:,} "
        f"cols={len(table.schema.names)}={list(table.schema.names)[:6]}{'...' if len(table.schema.names) > 6 else ''} "
        f"path={path.name} elapsed_ms={int((time.perf_counter() - t0) * 1000)}",
    )
    dtype = dataframe.dtype
    # Stamp the producer node id so the consumer can refuse to read a
    # file-transport descriptor that was written on a different node
    # (see ``loadExchangeDescriptor`` for the symmetric check). Best
    # effort — if Ray runtime context is unavailable (e.g. unit tests
    # running outside a Ray cluster) we leave the field None and the
    # consumer-side check no-ops.
    #
    # When the publish config declares a shared filesystem
    # (``BOLTML_EXCHANGE_ROOT`` set), every Ray node can read the
    # same path — leave ``producerNodeId=None`` so the consumer-side
    # cross-node guard accepts the read. Without this, the shared-root
    # wiring is half-fixed: writes land on the shared mount but reads
    # from a different node still get refused.
    if config.sharedFilesystem:
        producerNodeId = None
    else:
        try:
            producerNodeId = ray.get_runtime_context().get_node_id()
        except Exception:  # noqa: BLE001
            producerNodeId = None
    return ExchangePartitionDescriptor(
        executionId=executionId,
        stageId=stageId,
        exchangeId=exchangeId,
        attemptId=attemptId,
        partitionCount=partitionCount,
        partitionId=partitionId,
        path=str(path),
        outputNames=tuple(dtype.names()),
        outputTypes=tuple(str(dtype.childAt(i)) for i in range(len(dtype))),
        partitioning=partitioning,
        shardId=resolvedShardId,
        producerNodeId=producerNodeId,
    )


def _descriptor_slot(descriptor: ExchangePartitionDescriptor) -> tuple[int, str]:
    return descriptor.partitionId, descriptor.shardId


def _descriptor_attempt(descriptors: tuple[ExchangePartitionDescriptor, ...]) -> int:
    if not descriptors:
        return 0
    attempts = {descriptor.attemptId for descriptor in descriptors}
    if len(attempts) != 1:
        raise ValueError(f"Inconsistent descriptor attempts: {sorted(attempts)}")
    return next(iter(attempts))


class ExchangeManager(ABC):
    @abstractmethod
    def publishConfig(self) -> ExchangePublishConfig: ...

    def publishPartitions(
        self,
        stageId: str,
        exchangeId: str,
        partitions: tuple[DataFrame, ...],
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        if len(partitions) != 1:
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support multi-partition publish yet."
            )
        return self.publish(
            stageId,
            exchangeId,
            partitions[0],
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )

    @abstractmethod
    def publish(
        self,
        stageId: str,
        exchangeId: str,
        dataframe: DataFrame,
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]: ...

    @abstractmethod
    def resolve(
        self, placeholder: PlaceholderRead
    ) -> tuple[ExchangePartitionDescriptor, ...]: ...

    @abstractmethod
    def load(self, descriptor: ExchangePartitionDescriptor) -> DataFrame: ...

    @abstractmethod
    def ingestDescriptors(
        self, descriptors: tuple[ExchangePartitionDescriptor, ...], owned: bool = False
    ) -> None: ...

    @abstractmethod
    def cleanup(self, executionId: str | None = None) -> None: ...


class FileExchangeManager(ExchangeManager):
    def __init__(self, rootDir: str, sharedFilesystem: bool = False):
        self.__root = Path(rootDir)
        self.__root.mkdir(parents=True, exist_ok=True)
        self.__published: dict[
            tuple[str, str, str], tuple[ExchangePartitionDescriptor, ...]
        ] = {}
        self.__ownedExchangeKeys: set[tuple[str, str, str]] = set()
        self.__ownerId = f"file-manager-{uuid4().hex}"
        # When ``sharedFilesystem`` is True, this manager skips
        # stamping ``producerNodeId`` on the descriptors it produces —
        # files written here live on a filesystem mounted on every
        # Ray node, so the consumer-side cross-node guard should
        # accept reads from any node. Without this, descriptors
        # falsely fail the cross-node check even on a properly
        # configured shared mount.
        self.__sharedFilesystem = bool(sharedFilesystem)

    def publishConfig(self) -> ExchangePublishConfig:
        return ExchangePublishConfig(
            rootDir=str(self.__root),
            ownerId=self.__ownerId,
            sharedFilesystem=self.__sharedFilesystem,
        )

    def _removeDescriptorArtifact(
        self, descriptor: ExchangePartitionDescriptor
    ) -> None:
        if not descriptor.path:
            return
        path = Path(descriptor.path)
        if path.exists():
            path.unlink()
        exchangeDir = path.parent
        if exchangeDir.exists() and not any(exchangeDir.iterdir()):
            exchangeDir.rmdir()
        stageDir = exchangeDir.parent
        if stageDir.exists() and not any(stageDir.iterdir()):
            stageDir.rmdir()

    def _dropPublishedKey(self, key: tuple[str, str, str]) -> None:
        self.__published.pop(key, None)
        self.__ownedExchangeKeys.discard(key)

    def publishPartitions(
        self,
        stageId: str,
        exchangeId: str,
        partitions: tuple[DataFrame, ...],
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        exchangeDir = (
            self.__root / executionId / stageId / exchangeId / f"attempt-{attemptId}"
        )
        exchangeDir.mkdir(parents=True, exist_ok=True)
        boltmlDebugLog(
            "exchange:file",
            f"publish stage={stageId} exchange={exchangeId} partitions={len(partitions)} partitioning={partitioning}",
        )
        descriptors = []
        partitionCount = len(partitions)
        # Stamp producer node id once for the whole batch — every
        # partition in this call publishes from the same process. The
        # standalone ``publishExchangePartition`` helper has matching
        # logic; without it here, the cross-node guard in
        # ``loadExchangeDescriptor`` couldn't catch a mis-routed
        # cross-node read of files this manager produced.
        #
        # When the manager was constructed with
        # ``sharedFilesystem=True`` (the AdaptiveExchangeManager
        # passes this through from the runtime's
        # ``BOLTML_EXCHANGE_ROOT`` detection), files are on a mount
        # readable from every Ray node and the cross-node guard
        # would falsely reject them — leave ``producerNodeId=None``
        # so the load path accepts reads from any node.
        #
        # Best-effort: if Ray runtime context is unavailable we
        # leave the field None and the consumer-side check no-ops
        # (single-node / unit-test cases).
        if self.__sharedFilesystem:
            producerNodeId = None
        else:
            try:
                producerNodeId = ray.get_runtime_context().get_node_id()
            except Exception:  # noqa: BLE001
                producerNodeId = None
        for partitionId, dataframe in enumerate(partitions):
            path = exchangeDir / f"part-{partitionId}.parquet"
            # Preserve the declared Bolt types through the parquet round-trip.
            table = arrow_table_with_bolt_types(dataframe)
            pq.write_table(table, path)

            dtype = dataframe.dtype
            descriptors.append(
                ExchangePartitionDescriptor(
                    executionId=executionId,
                    stageId=stageId,
                    exchangeId=exchangeId,
                    attemptId=attemptId,
                    partitionCount=partitionCount,
                    partitionId=partitionId,
                    path=str(path),
                    outputNames=tuple(dtype.names()),
                    outputTypes=tuple(str(dtype.childAt(i)) for i in range(len(dtype))),
                    partitioning=partitioning,
                    producerNodeId=producerNodeId,
                )
            )
        key = (executionId, stageId, exchangeId)
        self.__published[key] = tuple(descriptors)
        self.__ownedExchangeKeys.add(key)
        boltmlTraceLog(
            "exchange:file",
            "published descriptors:\n" + "\n".join(d.explain() for d in descriptors),
        )
        return tuple(descriptors)

    def publish(
        self,
        stageId: str,
        exchangeId: str,
        dataframe: DataFrame,
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        return self.publishPartitions(
            stageId,
            exchangeId,
            (dataframe,),
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )

    def resolve(
        self, placeholder: PlaceholderRead
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        key = (
            placeholder.executionId,
            placeholder.sourceStageId,
            placeholder.exchangeId,
        )
        if key not in self.__published:
            raise KeyError(f"Unknown exchange placeholder: {key}")
        boltmlDebugLog(
            "exchange:file",
            f"resolve placeholder={key} partitions={len(self.__published[key])}",
        )
        return self.__published[key]

    def load(self, descriptor: ExchangePartitionDescriptor) -> DataFrame:
        return loadExchangeDescriptor(descriptor)

    def ingestDescriptors(
        self, descriptors: tuple[ExchangePartitionDescriptor, ...], owned: bool = False
    ) -> None:
        grouped: dict[tuple[str, str, str], list[ExchangePartitionDescriptor]] = {}
        for descriptor in descriptors:
            grouped.setdefault(
                (descriptor.executionId, descriptor.stageId, descriptor.exchangeId), []
            ).append(descriptor)
        for key, newDescriptors in grouped.items():
            existingDescriptors = self.__published.get(key, ())
            existingAttempt = _descriptor_attempt(existingDescriptors)
            incomingAttempt = _descriptor_attempt(tuple(newDescriptors))
            if incomingAttempt < existingAttempt:
                continue
            if incomingAttempt > existingAttempt:
                for descriptor in existingDescriptors:
                    self._removeDescriptorArtifact(descriptor)
                merged = {}
            else:
                merged = {
                    _descriptor_slot(descriptor): descriptor
                    for descriptor in existingDescriptors
                }
            for descriptor in newDescriptors:
                slot = _descriptor_slot(descriptor)
                previous = merged.get(slot)
                if previous is not None and previous != descriptor:
                    self._removeDescriptorArtifact(previous)
                merged[slot] = descriptor
            self.__published[key] = tuple(
                descriptor
                for _, descriptor in sorted(merged.items(), key=lambda item: item[0])
            )
            if owned:
                self.__ownedExchangeKeys.add(key)

    def cleanup(self, executionId: str | None = None) -> None:
        targetKeys = tuple(
            key
            for key in self.__ownedExchangeKeys
            if executionId is None or key[0] == executionId
        )
        boltmlDebugLog(
            "exchange:file",
            f"cleanup published={len(targetKeys)} execution={executionId or '*'}",
        )
        for key in targetKeys:
            descriptors = self.__published.pop(key, ())
            for descriptor in descriptors:
                self._removeDescriptorArtifact(descriptor)
            self.__ownedExchangeKeys.discard(key)
        if executionId is not None:
            for key in tuple(self.__published.keys()):
                if key[0] == executionId and key not in self.__ownedExchangeKeys:
                    self.__published.pop(key, None)


class AdaptiveExchangeManager(ExchangeManager):
    def __init__(self, rootDir: str, policy: ExchangeTransportPolicy | None = None):
        self.__policy = policy or ExchangeTransportPolicy()
        # Pass the policy's sharedFilesystem flag through to the
        # underlying file manager so descriptors it produces drop
        # ``producerNodeId`` when the runtime declared a shared
        # mount. Without this, file-publish via the manager path
        # would falsely fail the consumer-side cross-node guard
        # even on a properly configured shared filesystem.
        self.__file = FileExchangeManager(
            rootDir, sharedFilesystem=self.__policy.sharedFilesystem
        )
        self.__published: dict[
            tuple[str, str, str], tuple[ExchangePartitionDescriptor, ...]
        ] = {}
        self.__objects: dict[str, ray.ObjectRef | None] = {}
        self.__ownedExchangeKeys: set[tuple[str, str, str]] = set()
        self.__ownedObjectKeys: set[str] = set()
        self.__ownerId = f"adaptive-manager-{uuid4().hex}"
        self.__registry = getExchangeRegistry()

    def publishConfig(self) -> ExchangePublishConfig:
        return ExchangePublishConfig(
            rootDir=str(self.__file._FileExchangeManager__root),
            ownerId=self.__ownerId,
            preferredTransport=self.__policy.preferredTransport,
            maxRowsForObject=self.__policy.maxRowsForObject,
            sharedFilesystem=self.__policy.sharedFilesystem,
        )

    def _removeDescriptorArtifacts(
        self, descriptor: ExchangePartitionDescriptor
    ) -> None:
        if descriptor.transport == "object" and descriptor.objectKey is not None:
            ray.get(
                self.__registry.deleteObject.remote(
                    self.__ownerId, descriptor.objectKey
                )
            )
            self.__objects.pop(descriptor.objectKey, None)
            self.__ownedObjectKeys.discard(descriptor.objectKey)
            return
        self.__file._removeDescriptorArtifact(descriptor)

    def _dropLocalExchangeState(
        self,
        key: tuple[str, str, str],
        descriptors: tuple[ExchangePartitionDescriptor, ...],
    ) -> None:
        self.__published.pop(key, None)
        self.__ownedExchangeKeys.discard(key)
        self.__file._dropPublishedKey(key)
        for descriptor in descriptors:
            if descriptor.transport == "object" and descriptor.objectKey is not None:
                ray.get(
                    self.__registry.deleteObject.remote(
                        self.__ownerId, descriptor.objectKey
                    )
                )
                self.__objects.pop(descriptor.objectKey, None)
                self.__ownedObjectKeys.discard(descriptor.objectKey)
            else:
                self.__file._removeDescriptorArtifact(descriptor)

    def _prepareExchangeReservation(
        self,
        executionId: str,
        stageId: str,
        exchangeId: str,
        attemptId: int,
    ) -> str:
        reservation = ray.get(
            self.__registry.prepareExchange.remote(
                self.__ownerId,
                executionId,
                stageId,
                exchangeId,
                attemptId,
            )
        )
        if not reservation.get("accepted"):
            raise RuntimeError(
                f"Registry rejected exchange registration for {executionId}/{stageId}/{exchangeId}."
            )
        return reservation["token"]

    def _commitExchangeReservation(
        self,
        executionId: str,
        stageId: str,
        exchangeId: str,
        token: str,
        descriptors: tuple[ExchangePartitionDescriptor, ...],
    ) -> None:
        registration = ray.get(
            self.__registry.commitExchange.remote(
                self.__ownerId,
                executionId,
                stageId,
                exchangeId,
                token,
                descriptors,
            )
        )
        if (
            not registration.get("accepted")
            or tuple(registration.get("descriptors", ())) != descriptors
        ):
            raise RuntimeError(
                f"Registry rejected exchange registration for {executionId}/{stageId}/{exchangeId}."
            )

    def _abortExchangeReservation(
        self,
        executionId: str,
        stageId: str,
        exchangeId: str,
        token: str,
    ) -> None:
        ray.get(
            self.__registry.abortExchange.remote(
                self.__ownerId,
                executionId,
                stageId,
                exchangeId,
                token,
            )
        )

    def publishPartitions(
        self,
        stageId: str,
        exchangeId: str,
        partitions: tuple[DataFrame, ...],
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        transports = {
            self.__policy.chooseTransport(partition) for partition in partitions
        }
        boltmlDebugLog(
            "exchange:adaptive",
            f"publish stage={stageId} exchange={exchangeId} partitions={len(partitions)} transports={sorted(transports)}",
        )
        reservationToken = self._prepareExchangeReservation(
            executionId, stageId, exchangeId, attemptId
        )
        if transports == {"object"}:
            descriptors = []
            try:
                partitionCount = len(partitions)
                for partitionId, dataframe in enumerate(partitions):
                    objectKey = (
                        f"{executionId}-{stageId}-{exchangeId}-attempt-{attemptId}-"
                        f"part-{partitionId}-{uuid4().hex}"
                    )
                    # Tag the Arrow table with bolt-type metadata so the
                    # consumer can restore the declared column types on load.
                    table = arrow_table_with_bolt_types(dataframe)
                    objectRef = ray.put(table)
                    descriptor = ExchangePartitionDescriptor(
                        executionId=executionId,
                        stageId=stageId,
                        exchangeId=exchangeId,
                        attemptId=attemptId,
                        partitionCount=partitionCount,
                        partitionId=partitionId,
                        path="",
                        outputNames=tuple(dataframe.dtype.names()),
                        outputTypes=tuple(
                            str(dataframe.dtype.childAt(i))
                            for i in range(len(dataframe.dtype))
                        ),
                        partitioning=partitioning,
                        transport="object",
                        objectKey=objectKey,
                    )
                    registered = ray.get(
                        self.__registry.registerObject.remote(
                            self.__ownerId, objectKey, objectRef
                        )
                    )
                    if not registered:
                        raise RuntimeError(
                            f"Registry rejected object registration for {objectKey}."
                        )
                    self.__objects[objectKey] = objectRef
                    self.__ownedObjectKeys.add(objectKey)
                    descriptors.append(descriptor)
                result = tuple(descriptors)
                self._commitExchangeReservation(
                    executionId, stageId, exchangeId, reservationToken, result
                )
                key = (executionId, stageId, exchangeId)
                self.__published[key] = result
                self.__ownedExchangeKeys.add(key)
                boltmlTraceLog(
                    "exchange:adaptive",
                    "published descriptors:\n" + "\n".join(d.explain() for d in result),
                )
                return result
            except Exception:
                self._abortExchangeReservation(
                    executionId, stageId, exchangeId, reservationToken
                )
                self._dropLocalExchangeState(
                    (executionId, stageId, exchangeId), tuple(descriptors)
                )
                raise

        descriptors = ()
        try:
            descriptors = self.__file.publishPartitions(
                stageId,
                exchangeId,
                partitions,
                partitioning,
                executionId=executionId,
                attemptId=attemptId,
            )
            self._commitExchangeReservation(
                executionId, stageId, exchangeId, reservationToken, descriptors
            )
            key = (executionId, stageId, exchangeId)
            self.__published[key] = descriptors
            self.__ownedExchangeKeys.add(key)
            boltmlTraceLog(
                "exchange:adaptive",
                "published descriptors:\n"
                + "\n".join(d.explain() for d in descriptors),
            )
            return descriptors
        except Exception:
            self._abortExchangeReservation(
                executionId, stageId, exchangeId, reservationToken
            )
            self._dropLocalExchangeState(
                (executionId, stageId, exchangeId), descriptors
            )
            raise

    def publish(
        self,
        stageId: str,
        exchangeId: str,
        dataframe: DataFrame,
        partitioning: str,
        executionId: str = "",
        attemptId: int = 1,
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        return self.publishPartitions(
            stageId,
            exchangeId,
            (dataframe,),
            partitioning,
            executionId=executionId,
            attemptId=attemptId,
        )

    def resolve(
        self, placeholder: PlaceholderRead
    ) -> tuple[ExchangePartitionDescriptor, ...]:
        key = (
            placeholder.executionId,
            placeholder.sourceStageId,
            placeholder.exchangeId,
        )
        registryDescriptors = ray.get(self.__registry.resolve.remote(*key))
        cachedDescriptors = self.__published.get(key)
        if registryDescriptors is None and cachedDescriptors is None:
            raise KeyError(f"Unknown exchange placeholder: {key}")
        if registryDescriptors is not None:
            registryAttempt = _descriptor_attempt(registryDescriptors)
            cachedAttempt = _descriptor_attempt(cachedDescriptors or ())
            if cachedDescriptors is None or registryAttempt >= cachedAttempt:
                self.__published[key] = registryDescriptors
        elif cachedDescriptors is not None:
            if key not in self.__ownedExchangeKeys:
                self.__published.pop(key, None)
                raise KeyError(f"Unknown exchange placeholder: {key}")
            self.__published[key] = cachedDescriptors
        boltmlDebugLog(
            "exchange:adaptive",
            f"resolve placeholder={key} partitions={len(self.__published[key])}",
        )
        return self.__published[key]

    def load(self, descriptor: ExchangePartitionDescriptor) -> DataFrame:
        if descriptor.transport == "object" and descriptor.objectKey is not None:
            objectPayload = self.__objects.get(descriptor.objectKey)
            if objectPayload is not None:
                if isinstance(objectPayload, ray.ObjectRef):
                    objectPayload = ray.get(objectPayload)
                if len(objectPayload) == 0:
                    return _empty_dataframe_for_descriptor(
                        descriptor, objectPayload.schema
                    )
                # Rebuild any custom-typed columns from the schema-metadata
                # bolt types (set by ``arrow_table_with_bolt_types`` at
                # publish). ``descriptor.outputTypes`` is a fallback for
                # legacy descriptors written before the metadata tagging
                # landed.
                return dataframe_from_typed_arrow(
                    objectPayload, fallback_types=descriptor.outputTypes
                )
        return loadExchangeDescriptor(descriptor)

    def ingestDescriptors(
        self, descriptors: tuple[ExchangePartitionDescriptor, ...], owned: bool = False
    ) -> None:
        self.__file.ingestDescriptors(descriptors, owned=owned)
        grouped: dict[tuple[str, str, str], list[ExchangePartitionDescriptor]] = {}
        for descriptor in descriptors:
            grouped.setdefault(
                (descriptor.executionId, descriptor.stageId, descriptor.exchangeId), []
            ).append(descriptor)
        for key, newDescriptors in grouped.items():
            existingDescriptors = self.__published.get(key, ())
            existingAttempt = _descriptor_attempt(existingDescriptors)
            incomingAttempt = _descriptor_attempt(tuple(newDescriptors))
            if incomingAttempt < existingAttempt:
                continue
            for descriptor in newDescriptors:
                if (
                    descriptor.transport == "object"
                    and descriptor.objectKey is not None
                ):
                    self.__objects.setdefault(descriptor.objectKey, None)
                    if owned:
                        self.__ownedObjectKeys.add(descriptor.objectKey)
            if incomingAttempt > existingAttempt:
                for descriptor in existingDescriptors:
                    self._removeDescriptorArtifacts(descriptor)
                merged = {}
            else:
                merged = {
                    _descriptor_slot(descriptor): descriptor
                    for descriptor in existingDescriptors
                }
            for descriptor in newDescriptors:
                slot = _descriptor_slot(descriptor)
                previous = merged.get(slot)
                if previous is not None and previous != descriptor:
                    self._removeDescriptorArtifacts(previous)
                merged[slot] = descriptor
            resolved = tuple(
                descriptor
                for _, descriptor in sorted(merged.items(), key=lambda item: item[0])
            )
            if owned:
                reservationToken = self._prepareExchangeReservation(
                    key[0], key[1], key[2], _descriptor_attempt(resolved)
                )
                try:
                    self._commitExchangeReservation(
                        key[0], key[1], key[2], reservationToken, resolved
                    )
                except Exception:
                    self._abortExchangeReservation(
                        key[0], key[1], key[2], reservationToken
                    )
                    self._dropLocalExchangeState(key, resolved)
                    raise
                self.__ownedExchangeKeys.add(key)
            self.__published[key] = resolved

    def cleanup(self, executionId: str | None = None) -> None:
        targetExchangeKeys = tuple(
            key
            for key in self.__ownedExchangeKeys
            if executionId is None or key[0] == executionId
        )
        targetObjectKeys = tuple(
            objectKey
            for objectKey in self.__ownedObjectKeys
            if executionId is None or objectKey.startswith(f"{executionId}-")
        )
        boltmlDebugLog(
            "exchange:adaptive",
            f"cleanup published={len(targetExchangeKeys)} objects={len(targetObjectKeys)} execution={executionId or '*'}",
        )
        for scopedExecutionId, stageId, exchangeId in targetExchangeKeys:
            ray.get(
                self.__registry.deleteExchange.remote(
                    self.__ownerId, scopedExecutionId, stageId, exchangeId
                )
            )
            self.__ownedExchangeKeys.discard((scopedExecutionId, stageId, exchangeId))
            self.__published.pop((scopedExecutionId, stageId, exchangeId), None)
        for objectKey in targetObjectKeys:
            ray.get(self.__registry.deleteObject.remote(self.__ownerId, objectKey))
            self.__ownedObjectKeys.discard(objectKey)
            self.__objects.pop(objectKey, None)
        if executionId is None:
            self.__published.clear()
            self.__objects.clear()
            self.__ownedExchangeKeys.clear()
            self.__ownedObjectKeys.clear()
        else:
            for key in tuple(self.__published.keys()):
                if key[0] == executionId and key not in self.__ownedExchangeKeys:
                    self.__published.pop(key, None)
        self.__file.cleanup(executionId=executionId)

    def cleanupDescriptors(self, descriptors) -> None:
        """Cleanup artifacts associated with *descriptors* only, not
        every owned descriptor in their execution.

        Used by partial-publish failure handling. The standard
        ``cleanup(executionId=...)`` deletes ALL owned exchanges
        for the execution — including valid upstream ones the retry
        path still needs to read — so it's the wrong tool when a
        single stage's batch fails partway through. This method
        scopes deletion to:
          * the exchange(s) the failed batch was publishing into
            (typically one (executionId, stageId, exchangeId)),
          * the specific objectKeys those descriptors carry for
            object-transport buckets,
          * the specific file paths those descriptors carry for
            file-transport buckets (via FileExchangeManager).

        Upstream / sibling-stage exchanges are untouched. Missing /
        not-owned descriptors are silently ignored — best-effort
        cleanup must not throw on top of an already-failed attempt.
        """
        exchangeKeys = set()
        objectKeys = set()
        for d in descriptors:
            if d is None:
                continue
            exchangeKeys.add((d.executionId, d.stageId, d.exchangeId))
            if d.transport == "object" and d.objectKey:
                objectKeys.add(d.objectKey)
        boltmlDebugLog(
            "exchange:adaptive",
            f"cleanupDescriptors exchanges={len(exchangeKeys)} objects={len(objectKeys)}",
        )
        for key in exchangeKeys:
            if key not in self.__ownedExchangeKeys:
                continue
            try:
                ray.get(
                    self.__registry.deleteExchange.remote(
                        self.__ownerId, key[0], key[1], key[2]
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            self.__ownedExchangeKeys.discard(key)
            self.__published.pop(key, None)
        for objectKey in objectKeys:
            if objectKey not in self.__ownedObjectKeys:
                continue
            try:
                ray.get(self.__registry.deleteObject.remote(self.__ownerId, objectKey))
            except Exception:  # noqa: BLE001
                pass
            self.__ownedObjectKeys.discard(objectKey)
            self.__objects.pop(objectKey, None)
        # Unlink any file-transport parquet artifacts the worker wrote.
        for d in descriptors:
            if d is None or d.transport != "file":
                continue
            try:
                self.__file._removeDescriptorArtifact(d)
            except Exception:  # noqa: BLE001
                pass
