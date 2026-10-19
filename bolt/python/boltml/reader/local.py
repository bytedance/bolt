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

from pybolt import FileFormat, RowType

from ..dataframe import DataFrame
from ..executor.base import Executor
from ..executor.local import LocalExecutor
from ..plan_builder.base import PlanBuilderFactory
from ..plan_builder.substrait import SubstraitPlanBuilderFactory
from .base import TableReader


class LocalTableReader(TableReader):
    """
    Default lazy table reader from local file using bolt hive table reader.
    """

    def __init__(
        self,
        filePath: str,
        outputType: RowType,
        fileFormat: FileFormat,
        planFactory: PlanBuilderFactory = SubstraitPlanBuilderFactory(),
    ):
        hasScheme = filePath.startswith("file:") or "://" in filePath
        self.__filePath = filePath if hasScheme else f"file:{filePath}"
        self.__fileFormat = fileFormat
        self.__outputType = outputType
        self.__planFactory = planFactory

    def read(self, executor: Executor = LocalExecutor()) -> DataFrame:
        pb = self.__planFactory.fromSingleFileRead(
            self.__filePath, self.__outputType, self.__fileFormat
        )
        return DataFrame(pb, executor, planFactory=self.__planFactory)
