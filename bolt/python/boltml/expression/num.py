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

from typing import Optional, Union

from pybolt import (
    BooleanType,
    BoltType,
)

from ..details import _isArithmeticType_

from .base import Expression
from .boolean import BooleanExpression, BooleanOp
from .function import FunctionExpression
from .literal import LiteralExpression
from .project import ProjectExpression
from .cast import CastExpression


def binopCheck(fn):
    def wrapper(lhs, rhs: Union[Expression, int, float], *args):
        if not isinstance(rhs, Expression):
            rhs = NumProjectExpression(rhs)

        # Implicit cast to matching types. This is done to match bolt behavior.
        # Without this, the fuzzer test comparing exact plan match between bolt
        # and boltml will fail.
        # Bolt expression parser does implicit type casting in the following
        # situation: parsing the expression: `multiply("c_real", 6.38)`.
        # `6.38` literal is mapped as a `DOUBLE` type. Bolt then chooses to
        # promote `c_real` column from `REAL` type to `DOUBLE` type with an
        # implicit cast from the right to the left. The cast is added to the
        # expression so that it matches the registered
        # `multiply(DOUBLE, DOUBLE)` signature.
        if lhs.dtype != rhs.dtype:
            lhs = CastExpression(lhs, rhs.dtype)
        if (
            lhs.dataframe is not None
            and rhs.dataframe is not None
            and rhs.dataframe is not lhs.dataframe
        ):
            raise RuntimeError(
                "Cannot perform binary operations on expressions from different"
                " dataframes"
            )
        return fn(lhs, rhs, *args)

    return wrapper


class NumProjectExpression(ProjectExpression):
    def __init__(
        self, arg: Union[Expression, int, float], dtype: Optional[BoltType] = None
    ):
        if isinstance(arg, int) or isinstance(arg, float):
            arg = LiteralExpression(arg, dtype)
        if not isinstance(arg, Expression):
            raise TypeError(f"Invalid expression type: {type(arg)}")
        if not _isArithmeticType_(arg.dtype):
            raise TypeError(f"Invalid expression dtype {arg.dtype}")
        super().__init__(arg)

    @binopCheck
    def __arith(self, rhs: Union[Expression, int, float], name: str) -> "Self":  # noqa: F821
        expr = FunctionExpression(name, self.dtype, [self, rhs], self.dataframe)
        return NumProjectExpression(expr)

    def __add__(self, rhs: Union[Expression, int, float]) -> "Self":  # noqa: F821
        return self.__arith(rhs, "plus")

    def __sub__(self, rhs: Union[Expression, int, float]) -> "Self":  # noqa: F821
        return self.__arith(rhs, "minus")

    def __mul__(self, rhs: Union[Expression, int, float]) -> "Self":  # noqa: F821
        return self.__arith(rhs, "multiply")

    def __truediv__(self, rhs: Union["NumProjectExpression", int, float]) -> "Self":  # noqa: F821
        return self.__arith(rhs, "divide")

    # ``__radd__`` and ``__rmul__`` can delegate to their forward forms
    # because ``+`` and ``*`` are commutative — ``lhs + self`` produces
    # the same value as ``self + lhs``. ``__rsub__`` cannot: ``lhs -
    # self`` is not ``self - lhs``, so it builds the ``minus`` function
    # call explicitly with the operands in the right order. The
    # downstream ``minus`` extension treats its arg list as ordered.
    def __radd__(self, lhs):
        return self.__add__(lhs)

    def __rmul__(self, lhs):
        return self.__mul__(lhs)

    @binopCheck
    def __rsub__(self, lhs) -> "Self":  # noqa: F821
        expr = FunctionExpression("minus", self.dtype, [lhs, self], self.dataframe)
        return NumProjectExpression(expr)

    @binopCheck
    def __lt__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.LT, rhs)

    @binopCheck
    def __gt__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.GT, rhs)

    @binopCheck
    def __le__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.LE, rhs)

    @binopCheck
    def __ge__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.GE, rhs)

    @binopCheck
    def __eq__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.EQ, rhs)

    @binopCheck
    def __ne__(self, rhs: Union[Expression, int, float]) -> BooleanExpression:
        return BooleanExpression(self, BooleanOp.NE, rhs)

    def isNan(self) -> "Self":  # noqa: F821
        expr = FunctionExpression("isnan", BooleanType(), [self], self.dataframe)
        return BooleanExpression(expr)

    def isin(self, values) -> BooleanExpression:
        if not values:
            raise ValueError("isin requires at least one value")
        expr = self == values[0]
        for v in values[1:]:
            expr = expr | (self == v)
        return expr

    def between(self, lo, hi) -> BooleanExpression:
        return (self >= lo) & (self <= hi)
