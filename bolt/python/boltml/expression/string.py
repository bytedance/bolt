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


from pybolt import VarcharType

from .base import Expression
from .boolean import BooleanExpression, BooleanOp
from .function import FunctionExpression
from .literal import LiteralExpression
from .project import ProjectExpression


class StringProjectExpression(ProjectExpression):
    def __init__(self, arg: Expression):
        if not isinstance(arg, Expression):
            raise TypeError(f"Invalid expression type {type(arg)}")
        if arg.dtype != VarcharType():
            raise TypeError(f"Invalid expression dtype {arg.dtype}")
        super().__init__(arg)

    def concat(self, rhsStr: str) -> "Self":  # noqa: F821
        return StringProjectExpression(
            FunctionExpression(
                "concat",
                VarcharType(),
                [self, LiteralExpression(rhsStr)],
                self.dataframe,
            )
        )

    def searchReplace(self, search: str, replace: str = "") -> "Self":  # noqa: F821
        return StringProjectExpression(
            FunctionExpression(
                "replace",
                VarcharType(),
                [self, LiteralExpression(search), LiteralExpression(replace)],
                self.dataframe,
            )
        )

    def lowercase(self) -> "Self":  # noqa: F821
        return StringProjectExpression(
            FunctionExpression("lower", VarcharType(), [self], self.dataframe)
        )

    def uppercase(self) -> "Self":  # noqa: F821
        return StringProjectExpression(
            FunctionExpression("upper", VarcharType(), [self], self.dataframe)
        )

    def __eq__(self, literal: str) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.EQ, LiteralExpression(literal))

    def __ne__(self, literal: str) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.NE, LiteralExpression(literal))

    def like(self, pattern: str) -> BooleanExpression:
        from pybolt import BooleanType

        fn = FunctionExpression(
            "like", BooleanType(), [self, LiteralExpression(pattern)], self.dataframe
        )
        return BooleanExpression(fn)

    def startswith(self, prefix: str) -> BooleanExpression:
        from pybolt import BooleanType

        fn = FunctionExpression(
            "starts_with",
            BooleanType(),
            [self, LiteralExpression(prefix)],
            self.dataframe,
        )
        return BooleanExpression(fn)

    def endswith(self, suffix: str) -> BooleanExpression:
        from pybolt import BooleanType

        fn = FunctionExpression(
            "ends_with",
            BooleanType(),
            [self, LiteralExpression(suffix)],
            self.dataframe,
        )
        return BooleanExpression(fn)

    def substr(self, start: int, length: int) -> "Self":  # noqa: F821
        return StringProjectExpression(
            FunctionExpression(
                "substr",
                VarcharType(),
                [self, LiteralExpression(int(start)), LiteralExpression(int(length))],
                self.dataframe,
            )
        )

    def contains(self, needle: str) -> BooleanExpression:
        return self.like(f"%{needle}%")

    def isin(self, values) -> BooleanExpression:
        if not values:
            raise ValueError("isin requires at least one value")
        # Chain OR of equality tests — avoids needing a variadic `in` fn.
        expr = self == values[0]
        for v in values[1:]:
            expr = expr | (self == v)
        return expr
