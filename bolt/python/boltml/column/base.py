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

from abc import abstractmethod
from typing import Any, Iterator, Union

from pybolt import BaseVector, BoltType, rowVector

from ..details.txt_formatter import TxtFormatter


class Column:
    @property
    @abstractmethod
    def data(self) -> BaseVector:
        """
        Get column inner bolt data.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Get column name.
        """
        ...

    @property
    @abstractmethod
    def formatter(self) -> TxtFormatter: ...

    @property
    def dtype(self) -> BoltType:
        return self.data.dtype()

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: Union[int, slice]) -> Any:
        imax = None
        if isinstance(idx, int):
            imax = idx
        elif isinstance(idx, slice):
            imax = idx.stop
        if imax is not None and imax > len(self):
            raise IndexError(f"Index {imax} is larger than column length: {len(self)}")
        return self.data[idx]

    def __setitem__(self, idx: int, value: Any):
        if idx >= len(self):
            raise IndexError(f"Index {idx} is larger than column length: {len(self)}")
        self.data[idx] = value

    def __iter__(self) -> Iterator:
        data = self.data
        return (data[i] for i in range(len(self)))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}: {self.dtype})"

    def __str__(self) -> str:
        from ..dataframe import DataFrame

        return str(
            DataFrame(rowVector([self.name], [self.data]), formatter=self.formatter)
        )

    def equals(self, rhs: "Column") -> bool:
        """
        Operator for equality comparson between two columns.
        """
        # Cheap checks without evaluation
        if not isinstance(rhs, Column):
            return False
        if self is rhs:
            return True
        if self.dtype != rhs.dtype:
            return False
        # Cheap checks with evaluation
        if self.data is rhs.data:
            return True
        if len(self) != len(rhs):
            return False
        # Expensive element wise check.
        return rowVector([self.name], [self.data]) == rowVector([self.name], [rhs.data])
