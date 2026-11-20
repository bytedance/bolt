# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

from pybolt import FileFormat

from ..dataframe import DataFrame
from ..plan_builder.base import PlanBuilder
from .base import TableWriter


class LocalTableWriter(TableWriter):
    """
    Default implementation to write a DataFrame to hive
    a storage system.

    The input dataframe is consumed in the `write()` process.
    If you need to use the dataframe after the write, you should pass
    a copy.

    * directoryPath: Path to a directory to write data to.
    * fileFormat: File format to use for the written data.
    * partitionBy: Specifies the partition key columns. If not set,
    the resulting stored table won't be partitioned.
    * bucketedBy: Specifies the bucket by columns and the number of
    buckets. If not set, the resulting stored table won't be bucketed.
    * aggregates: Aggregations for column statistics collection during write.
    """

    def __init__(
        self,
        directoryPath: str,
        fileFormat: FileFormat = FileFormat.PARQUET,
        partitionBy: list[str] = [],
        bucketBy: tuple[list[str], int] = ([], 0),
        aggregations: list[str] = [],
    ):
        self.__directoryPath = directoryPath
        self.__fileFormat = fileFormat
        self.__partitionBy = partitionBy
        self.__bucketBy = bucketBy[0]
        self.__numBuckets = bucketBy[1]
        self.__aggregations = aggregations

    def write(self, dataframe: DataFrame):
        pb: PlanBuilder = dataframe._planBuilder_
        pb.catalogWrite(
            self.__directoryPath,
            self.__fileFormat,
            self.__partitionBy,
            self.__numBuckets,
            self.__bucketBy,
            self.__aggregations,
        )
        # Route through the free ``execute`` function instead of calling
        # ``executor.eval(pb)`` directly so writers share one materialisation
        # entry point with ``DataFrame`` (legacy kill-switch guard + debug
        # logging) and let each executor own its strategy: ``LocalExecutor``
        # lowers + runs in-process while ``RayExecutor`` physical-optimizes +
        # dispatches into a stage DAG. Both implement ``eval(PlanBuilder)``.
        from ..execute import execute as _execute

        _execute(pb, dataframe._executor_)
