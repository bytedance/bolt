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

from abc import ABC, abstractmethod

from ..dataframe import DataFrame
from ..executor.base import Executor
from ..executor.local import LocalExecutor


class TableReader(ABC):
    """
    Base interface for classes that can generate a new DataFrame from a table
    storage.
    For instance, a table can be stored:
    * In a parquet file on the local file system
    * In a hive catalog on a different file system.
    """

    @abstractmethod
    def read(self, executor: Executor = LocalExecutor()) -> DataFrame:
        """
        Read the associated table through the input executor.
        """
        ...
