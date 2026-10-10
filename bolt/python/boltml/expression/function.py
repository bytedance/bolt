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

from typing import Optional

from pybolt import BoltType

from .base import Expression


class FunctionExpression(Expression):
    def __init__(
        self,
        fnName: str,
        outputType: BoltType,
        arguments: list[Expression],
        dataframe: Optional["DataFrame"] = None,  # noqa: F821
    ):
        self.__fnName = fnName
        self.__dtype = outputType
        self.__args = arguments
        self.__dataframe = dataframe

    @property
    def dtype(self) -> BoltType:
        return self.__dtype

    def expr(self) -> str:
        return f"{self.__fnName}({', '.join([e.expr() for e in self.__args])})"

    @property
    def dataframe(self) -> Optional["DataFrame"]:  # noqa: F821
        return self.__dataframe

    @property
    def arguments(self) -> list[Expression]:
        return self.__args

    @property
    def name(self) -> str:
        return self.__fnName

    def asFunction(self) -> Optional["FunctionExpression"]:
        return self
