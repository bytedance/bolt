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


@dataclass(frozen=True)
class PlaceholderRead:
    sourceStageId: str
    exchangeId: str
    outputNames: tuple[str, ...]
    executionId: str = ""
    outputTypes: tuple[str, ...] = ()
    partitioning: str = "unknown"
    partitionCount: int = 1
    transport: str | None = None

    @property
    def identity(self) -> tuple[str, str]:
        return (self.sourceStageId, self.exchangeId)

    @property
    def scopedIdentity(self) -> tuple[str, str, str]:
        return (self.executionId, self.sourceStageId, self.exchangeId)

    def validateDescriptor(self, descriptor) -> None:
        if (descriptor.stageId, descriptor.exchangeId) != self.identity:
            raise ValueError(
                f"Descriptor identity mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if (
            self.executionId
            and getattr(descriptor, "executionId", self.executionId) != self.executionId
        ):
            raise ValueError(
                f"Descriptor execution mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if self.outputNames and tuple(descriptor.outputNames) != tuple(
            self.outputNames
        ):
            raise ValueError(
                f"Descriptor output names mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if (
            self.outputTypes
            and descriptor.outputTypes
            and tuple(descriptor.outputTypes) != tuple(self.outputTypes)
        ):
            raise ValueError(
                f"Descriptor output types mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if (
            self.partitioning != "unknown"
            and descriptor.partitioning != self.partitioning
        ):
            raise ValueError(
                f"Descriptor partitioning mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if (
            self.partitionCount > 0
            and getattr(descriptor, "partitionCount", self.partitionCount)
            != self.partitionCount
        ):
            raise ValueError(
                f"Descriptor partition count mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )
        if self.transport is not None and descriptor.transport != self.transport:
            raise ValueError(
                f"Descriptor transport mismatch for placeholder {self.sourceStageId}/{self.exchangeId}."
            )

    def explain(self) -> str:
        parts = [
            f"PlaceholderRead[{self.sourceStageId}/{self.exchangeId}]",
            f"output={','.join(self.outputNames)}",
            f"partitioning={self.partitioning}",
            f"partitions={self.partitionCount}",
        ]
        if self.executionId:
            parts.append(f"execution={self.executionId}")
        if self.outputTypes:
            parts.append(f"types={','.join(self.outputTypes)}")
        if self.transport is not None:
            parts.append(f"transport={self.transport}")
        return " ".join(parts)
