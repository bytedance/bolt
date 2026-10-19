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

from enum import Enum
from typing import Optional

from pybolt import BoltType, BooleanType

from .base import Expression
from .function import FunctionExpression


class BooleanOp(Enum):
    AND = "and"
    EQ = "eq"
    GE = "gte"
    GT = "gt"
    LE = "lte"
    LT = "lt"
    NE = "neq"
    OR = "or"
    UDF = ""


class BooleanExpression(FunctionExpression):
    """
    A predicate defining how to filter rows.
    """

    def __init__(
        self,
        lhs: Expression,
        op: BooleanOp = BooleanOp.UDF,
        rhs: Optional[Expression] = None,
    ):
        if not isinstance(lhs, Expression):
            raise TypeError(f"Invalid boolean expression type {type(lhs)} for lhs.")
        if op is BooleanOp.UDF and lhs.asFunction() is None:
            raise ValueError(
                "lhs expression must be a function for UDF boolean expressions."
            )
        if (
            rhs is not None
            and rhs.dataframe is not None
            and lhs.dataframe is not rhs.dataframe
        ):
            raise RuntimeError(
                "BooleanExpression cannot combine expressions from different "
                "dataframes."
            )
        if rhs is not None and not isinstance(rhs, Expression):
            raise TypeError(f"Invalid boolean expression type {type(rhs)} for rhs.")
        if op is not BooleanOp.UDF and rhs is None:
            raise ValueError("Expected rhs argument for non UDF boolean op.")

        if op == BooleanOp.UDF:
            lhs = lhs.asFunction()
            super().__init__(lhs.name, lhs.dtype, lhs.arguments, lhs.dataframe)
        else:
            dataframe = lhs.dataframe if lhs.dataframe is not None else rhs.dataframe
            super().__init__(op.value, BooleanType(), [lhs, rhs], dataframe)

        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    @property
    def dtype(self) -> BoltType:
        return BooleanType()

    def expr(self) -> str:
        if self.op in (BooleanOp.AND, BooleanOp.OR):
            return f"({self.lhs.expr()}) {self.op.value} ({self.rhs.expr()})"
        return super().expr()

    def __and__(self, rhs: "BooleanExpression") -> "BooleanExpression":
        if not isinstance(rhs, BooleanExpression):
            raise TypeError(
                "BooleanExpression AND requires a BooleanExpression right hand "
                f"side operand. rhs: {type(rhs)}"
            )
        return BooleanExpression(self, BooleanOp.AND, rhs)

    def __or__(self, rhs: "BooleanExpression") -> "BooleanExpression":
        if not isinstance(rhs, BooleanExpression):
            raise TypeError(
                "BooleanExpression OR requires a BooleanExpression right hand "
                f"side operand. rhs: {type(rhs)}"
            )
        return BooleanExpression(self, BooleanOp.OR, rhs)

    def __invert__(self) -> "BooleanExpression":
        """Logical NOT — wraps ``self`` in a Substrait ``not`` scalar function.

        Lets users write ``~df["col"].like("%foo%")`` for negated predicates,
        symmetric with ``&`` (AND) and ``|`` (OR) on top of the existing
        comparison operators. The C++ engine resolves ``not`` via the
        Substrait functions registry the same way it resolves ``like``,
        ``starts_with``, etc.
        """
        # Local import: ``boolean`` is imported by ``function`` at module load,
        # importing ``function`` here would create a cycle.
        from .function import FunctionExpression

        fn = FunctionExpression("not", BooleanType(), [self], self.dataframe)
        return BooleanExpression(fn)

    def __bool__(self) -> bool:
        raise RuntimeError(
            "BooleanExpression is not meant to be directly evaluated. "
            "It is intended to be used as a filter expression. "
            "This expression may have been evaluated by being combined with "
            "boolean keywords such as `and`, `or`, `is True` etc. In that case "
            "you may have meant to use the boolean operator overloads instead, "
            "i.e `&` and `|`."
        )
