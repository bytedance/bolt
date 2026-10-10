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


class Expression:
    def __init__(
        self,
        expr: str,
        dtype: BoltType,
        dataframe: Optional["DataFrame"],  # noqa: F821
    ):
        self.__expr = expr
        self.__dtype = dtype
        self.__dataframe = dataframe

    def expr(self) -> str:
        """
        Convert object instance to an expression.
        """
        return self.__expr

    @property
    def dtype(self) -> BoltType:
        """
        Get the expression output type.
        """
        return self.__dtype

    @property
    def dataframe(self) -> Optional["DataFrame"]:  # noqa: F821
        """
        Return the dataframe attached to this expression if any.
        """
        return self.__dataframe

    def __str__(self) -> str:
        return f"({self.dtype})({self.expr()})"

    def asLiteral(self) -> Optional["LiteralExpression"]:  # noqa: F821
        """
        If the class instance is a literal expression, then cast it to a
        literal expression
        """
        return None

    def asFunction(self) -> Optional["FunctionExpression"]:  # noqa: F821
        """
        If the class instance is a function expression, then cast it to a
        function expression
        """
        return None
