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

from pybolt import BooleanType


from .base import Expression
from .boolean import BooleanExpression
from .function import FunctionExpression


class ProjectExpression(Expression):
    def __init__(
        self,
        expr: Expression,
    ):
        self.__expr = expr
        super().__init__(expr.expr(), expr.dtype, expr.dataframe)

    def inner(self) -> Expression:
        """
        Return the inner expression wrapped by this ProjectExpression.
        """
        return self.__expr

    def isNotNone(self) -> BooleanExpression:
        expr = FunctionExpression(
            "NOT is_null", BooleanType(), [self.inner()], self.dataframe
        )
        return BooleanExpression(expr)

    def isNone(self) -> BooleanExpression:
        expr = FunctionExpression(
            "is_null", BooleanType(), [self.inner()], self.dataframe
        )
        return BooleanExpression(expr)

    def expr(self) -> str:
        return self.inner().expr()

    @property
    def dataframe(self) -> "DataFrame":  # noqa: F821
        return self.inner().dataframe

    def asLiteral(self) -> Optional["LiteralExpression"]:  # noqa: F821
        return self.inner().asLiteral()

    def asFunction(self) -> Optional["FunctionExpression"]:  # noqa: F821
        return self.inner().asFunction()
