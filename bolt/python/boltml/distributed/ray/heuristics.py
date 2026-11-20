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
import math


@dataclass(frozen=True)
class RuntimeHeuristics:
    defaultPartitionCount: int = 12
    maxPartitionCount: int = 64
    targetRowsPerPartition: int = 1000
    maxFilesPerTask: int = 4

    def normalizedPartitionCount(
        self,
        rowCount: int,
        fileCount: int,
        requestedCount: int | None = None,
    ) -> int:
        if requestedCount is not None:
            return max(1, min(requestedCount, self.maxPartitionCount))

        byRows = max(1, math.ceil(max(rowCount, 1) / self.targetRowsPerPartition))
        byFiles = max(1, fileCount)
        return max(
            1,
            min(
                self.maxPartitionCount, max(self.defaultPartitionCount, byRows, byFiles)
            ),
        )

    def coalesceFiles(self, filePaths: list[str]) -> list[list[str]]:
        if self.maxFilesPerTask <= 1:
            return [[path] for path in filePaths]
        return [
            filePaths[i : i + self.maxFilesPerTask]
            for i in range(0, len(filePaths), self.maxFilesPerTask)
        ]
