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

"""Per-stage runtime summary types surfaced by ``executor.lastExecutionSummary``.

``RuntimeExecutionStage`` captures one stage's bookkeeping (input
counts, partitioning, transport, retry/fallback reason, optional
driver-observed wall timestamps). ``RuntimeExecutionSummary`` bundles
the per-plan list.

These are runtime types — they describe what *happened* during a
``boltml.execute.execute`` invocation. The richer profiling /
observability layer that consumes them (web viewer, profile-mode
wrapper, env-gated debug logs) lives on the
``boltml-followup/observability-infra`` branch and will land as its
own MR.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeExecutionStage:
    stageId: str
    attempt: int
    inputCount: int
    outputNames: tuple[str, ...]
    partitionCount: int = 1
    exchangeIds: tuple[str, ...] = ()
    transport: str | None = None
    retryReason: str | None = None
    remote: bool = False
    remoteTaskCount: int = 0
    workerId: str | None = None
    nodeId: str | None = None
    workerIds: tuple[str, ...] = ()
    nodeIds: tuple[str, ...] = ()
    fallbackReason: str | None = None
    # Driver-observed monotonic timestamps. None means the stage was
    # skipped or its timing wasn't captured.
    startNs: int | None = None
    endNs: int | None = None
    publishStartNs: int | None = None
    publishEndNs: int | None = None
    publishWallNsExplicit: int | None = None
    """Sum of per-task publish durations across all tasks of this stage.
    Populated for multi-worker partitioned-remote stages where the
    timestamp pair above is deliberately None (worker monotonic clocks
    aren't comparable across workers). Safe to aggregate as a
    duration; not a timeline timestamp."""

    @property
    def wallNs(self) -> int | None:
        if self.startNs is None or self.endNs is None:
            return None
        return self.endNs - self.startNs

    @property
    def publishWallNs(self) -> int | None:
        if self.publishWallNsExplicit is not None:
            return self.publishWallNsExplicit
        if self.publishStartNs is None or self.publishEndNs is None:
            return None
        return self.publishEndNs - self.publishStartNs


@dataclass(frozen=True)
class RuntimeExecutionSummary:
    planId: str
    stages: tuple[RuntimeExecutionStage, ...]
    executionMode: str = "staged_local"
    rolloutLevel: str = "staged_local"
    startNs: int | None = None
    endNs: int | None = None

    @property
    def stageIds(self) -> tuple[str, ...]:
        return tuple(stage.stageId for stage in self.stages)

    @property
    def wallNs(self) -> int | None:
        if self.startNs is None or self.endNs is None:
            return None
        return self.endNs - self.startNs

    def explain(self) -> str:
        lines = [
            f"RuntimeSummary[{self.planId}] mode={self.executionMode} rollout={self.rolloutLevel}",
            f"Stages[{len(self.stages)}]",
        ]
        for stage in self.stages:
            parts = [
                f"Stage[{stage.stageId}]",
                f"attempt={stage.attempt}",
                f"inputs={stage.inputCount}",
                f"output={','.join(stage.outputNames)}",
                f"partitions={stage.partitionCount}",
            ]
            if stage.exchangeIds:
                parts.append(f"exchange={','.join(stage.exchangeIds)}")
            if stage.transport is not None:
                parts.append(f"transport={stage.transport}")
            if stage.retryReason is not None:
                parts.append(f"retry_reason={stage.retryReason}")
            parts.append(f"remote={str(stage.remote).lower()}")
            if stage.remoteTaskCount > 0:
                parts.append(f"remote_tasks={stage.remoteTaskCount}")
            if stage.workerId is not None:
                parts.append(f"worker_id={stage.workerId}")
            if stage.nodeId is not None:
                parts.append(f"node_id={stage.nodeId}")
            if stage.workerIds:
                parts.append(f"worker_ids={','.join(stage.workerIds)}")
            if stage.nodeIds:
                parts.append(f"node_ids={','.join(stage.nodeIds)}")
            if stage.fallbackReason is not None:
                parts.append(f"fallback_reason={stage.fallbackReason}")
            lines.append(" ".join(parts))
        return "\n".join(lines)
