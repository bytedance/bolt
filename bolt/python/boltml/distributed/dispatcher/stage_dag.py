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

from dataclasses import dataclass

from .placeholders import PlaceholderRead
from ..ray.task_spec import StageTaskSpec


@dataclass(frozen=True)
class StageNode:
    """A stage in the StageDAG.

    Every stage is Substrait-native: leaf stages carry
    ``producerSubstraitPlanBytes`` (no inputs) and consumer stages
    carry ``consumerSubstraitPlanBytes`` (with placeholder ``ReadRel``
    inputs that the runtime substitutes from upstream output).
    """

    stageId: str
    dependencies: tuple[str, ...] = ()
    inputPlaceholders: tuple[PlaceholderRead, ...] = ()
    outputNames: tuple[str, ...] = ()
    partitioning: str = "singleton"
    partitionCount: int = 1
    outputRowType: object | None = None
    consumerSubstraitPlanBytes: bytes | None = None
    producerSubstraitPlanBytes: bytes | None = None

    @property
    def taskSpec(self) -> StageTaskSpec:
        return StageTaskSpec.fromStageNode(self)

    def explain(self) -> str:
        lines = [
            f"Stage[{self.stageId}]",
            f"  dependencies={','.join(self.dependencies) if self.dependencies else '-'}",
            f"  inputs={len(self.inputPlaceholders)}",
            f"  partitioning={self.partitioning}",
            f"  partitions={self.partitionCount}",
        ]
        if self.producerSubstraitPlanBytes is not None:
            lines.append(
                f"  plan=producer-substrait-bytes({len(self.producerSubstraitPlanBytes)} bytes)"
            )
        elif self.consumerSubstraitPlanBytes is not None:
            lines.append(
                f"  plan=consumer-substrait-bytes({len(self.consumerSubstraitPlanBytes)} bytes)"
            )
        else:
            lines.append("  plan=placeholder-only")
        for placeholder in self.inputPlaceholders:
            lines.append(f"  {placeholder.explain()}")
        return "\n".join(lines)

    def validate(self) -> None:
        if self.partitionCount < 1:
            raise ValueError(
                f"Stage {self.stageId} has an invalid partition count {self.partitionCount}."
            )
        if (
            self.producerSubstraitPlanBytes is None
            and self.consumerSubstraitPlanBytes is None
            and not self.inputPlaceholders
        ):
            raise ValueError(
                f"Stage {self.stageId} has neither a Substrait-native producer plan, "
                "consumer plan, nor placeholder inputs."
            )
        if self.inputPlaceholders:
            if self.consumerSubstraitPlanBytes is None:
                raise ValueError(
                    f"Stage {self.stageId} has placeholder inputs but no "
                    "consumerSubstraitPlanBytes."
                )
            expectedDependencies = tuple(
                dict.fromkeys(p.sourceStageId for p in self.inputPlaceholders)
            )
            if tuple(self.dependencies) != expectedDependencies:
                raise ValueError(
                    f"Stage {self.stageId} dependency mismatch: expected {expectedDependencies}, got {self.dependencies}."
                )
            # Broadcast placeholders are allowed to have a
            # partitionCount different from the stage's own partition
            # count. A broadcast input has partitionCount=1 (one
            # broadcast output) but flows into every consumer task
            # regardless of the stage's hash-shuffled partition count.
            # Skip such placeholders when checking consistency.
            non_broadcast = tuple(
                p
                for p in self.inputPlaceholders
                if not (
                    p.partitionCount == 1
                    and (p.partitioning or "").startswith("broadcast")
                )
            )
            inputPartitionCounts = {
                placeholder.partitionCount for placeholder in non_broadcast
            }
            if any(count < 1 for count in inputPartitionCounts):
                raise ValueError(
                    f"Stage {self.stageId} received an invalid placeholder partition count."
                )
            if len(inputPartitionCounts) > 1:
                raise ValueError(
                    f"Stage {self.stageId} has inconsistent placeholder partition counts: {sorted(inputPartitionCounts)}."
                )
            if inputPartitionCounts and self.partitionCount != next(
                iter(inputPartitionCounts)
            ):
                raise ValueError(
                    f"Stage {self.stageId} partition count mismatch: expected {next(iter(inputPartitionCounts))}, got {self.partitionCount}."
                )


@dataclass(frozen=True)
class StageDAG:
    stages: tuple[StageNode, ...]

    @property
    def taskSpecs(self) -> tuple[StageTaskSpec, ...]:
        return tuple(stage.taskSpec for stage in self.stages)

    def explain(self) -> str:
        lines = [f"Stages[{len(self.stages)}]"]
        for stage in self.stages:
            lines.append(stage.explain())
        return "\n".join(lines)

    def validate(self) -> None:
        stageById = {stage.stageId: stage for stage in self.stages}
        for stage in self.stages:
            stage.validate()
            for placeholder in stage.inputPlaceholders:
                sourceStage = stageById.get(placeholder.sourceStageId)
                if sourceStage is None:
                    raise ValueError(
                        f"Stage {stage.stageId} references an unknown source stage {placeholder.sourceStageId}."
                    )
                # A stage that ENDS in a singleton/broadcast
                # exchange (output partition_count=1) has a different
                # output shape than its dispatch fan-out. The
                # ``sourceStage.partitionCount`` describes how the source
                # stage is dispatched (e.g. 12 partition-parallel tasks);
                # the placeholder's partitionCount=1 reflects the
                # downstream exchange shape (every task writes to
                # partition 0). The runtime handles this: each source
                # task writes 1 output file, all consumer tasks read
                # them via the broadcast / singleton routing in
                # ``_group_descriptors_by_partition``. Allow the
                # mismatch when the placeholder is a singleton
                # (partitionCount=1).
                if (
                    sourceStage.partitionCount != placeholder.partitionCount
                    and placeholder.partitionCount != 1
                ):
                    raise ValueError(
                        f"Stage {stage.stageId} placeholder partition count mismatch for source {placeholder.sourceStageId}: "
                        f"expected {sourceStage.partitionCount}, got {placeholder.partitionCount}."
                    )
