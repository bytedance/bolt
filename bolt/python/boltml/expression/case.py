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

from typing import Any, Callable, Optional

from pybolt import BoltType

from .base import Expression
from .boolean import BooleanExpression
from .literal import LiteralExpression


class CaseExpressionArm(Expression):
    """
    A single arm of a case expression.
    """

    def __init__(self, predicate: BooleanExpression, value: Expression):
        """
        Initialize a CaseExpressionArm.

        Args:
            predicate: The boolean condition for this case arm
            value: The expression to return if the predicate is true
        """
        if not isinstance(predicate, BooleanExpression):
            raise TypeError(
                f"predicate must be a BooleanExpression, got {type(predicate)}"
            )
        if not isinstance(value, Expression):
            raise TypeError(f"value must be an Expression, got {type(value)}")
        self.__predicate = predicate
        self.__value = value

        expr = f"WHEN {predicate.expr()} THEN {value.expr()}"
        super().__init__(expr, value.dtype, value.dataframe)

    @property
    def predicate(self) -> BooleanExpression:
        return self.__predicate

    @property
    def value(self) -> Expression:
        return self.__value


class CaseExpression(Expression):
    """
    An expression similar to SQL case expression.
    """

    def __init__(
        self,
        cases: list[CaseExpressionArm],
        defaultValue: Optional[Expression],
    ):
        if len(cases) == 0:
            raise ValueError("Cannot make a case expression with no cases.")

        dtype = (
            defaultValue.dtype
            if defaultValue is not None
            else next(c.dtype for c in cases)
        )
        if any((caseExpr.dtype != dtype for caseExpr in cases)):
            raise ValueError("Inconsistent expressions data types in case expression.")

        # Pick the first non-None dataframe. ``defaultValue`` is allowed to
        # be a literal (LiteralExpression with ``dataframe=None``); when it
        # is, we fall back to the cases. Without this fallback,
        # ``If(df["x"] > 0).then(df["y"]).otherwise(0)`` would pick
        # ``dataframe=None`` from the literal default and then complain
        # the cases (bound to ``df``) are inconsistent.
        dataframe = None
        if defaultValue is not None and defaultValue.dataframe is not None:
            dataframe = defaultValue.dataframe
        if dataframe is None:
            dataframe = next(
                (c.dataframe for c in cases if c.dataframe is not None), None
            )
        if any(
            (
                caseExpr.dataframe is not None and caseExpr.dataframe is not dataframe
                for caseExpr in cases
            )
        ):
            raise ValueError("Inconsistent expressions dataframes in case expression.")

        self.__cases = cases
        self.__defaultValue = defaultValue

        expr = "CASE"
        for c in cases:
            expr = expr + "\n\t" + c.expr()
        if defaultValue is not None:
            expr = expr + "\n\tELSE " + defaultValue.expr()
        expr += "\nEND"
        super().__init__(expr, dtype, dataframe)

    @property
    def defaultValue(self) -> Optional[Expression]:
        return self.__defaultValue

    @property
    def cases(self) -> list[CaseExpressionArm]:
        return self.__cases


class CaseExpressionBuilder:
    """
    A builder for creating CaseExpression instances.
    """

    def __init__(self):
        """
        Initialize a CaseExpressionBuilder.
        """
        self.__defaultValue: Optional[Expression] = None
        self.__cases: list[CaseExpressionArm] = []

    @property
    def dtype(self) -> Optional[BoltType]:
        if self.__defaultValue is not None:
            return self.__defaultValue.dtype
        return next((c.dtype for c in self.__cases), None)

    @property
    def dataframe(self) -> Optional["DataFrame"]:  # noqa: F821
        if self.__defaultValue is not None:
            return self.__defaultValue.dataframe
        return next((c.value.dataframe for c in self.__cases), None)

    def __checkExpr(self, expr: Expression, checkType=True):
        if (
            self.dataframe is not None
            and expr.dataframe is not None
            and expr.dataframe != self.dataframe
        ):
            raise ValueError("DataFrame mismatch in case expression.")
        if not checkType:
            return
        if self.dtype is not None and expr.dtype != self.dtype:
            raise ValueError(
                "Inconsistent expressions data types in case expression."
                f"Expression data type is {self.dtype}. New case arm data type "
                f"is {expr.dtype}"
            )

    def case(
        self, predicate: BooleanExpression, value: Expression
    ) -> "CaseExpressionBuilder":
        self.__checkExpr(predicate, checkType=False)
        self.__checkExpr(value)
        self.__cases.append(CaseExpressionArm(predicate, value))
        return self

    def default(self, value: Expression) -> "CaseExpressionBuilder":
        if self.__defaultValue is not None:
            raise RuntimeError("Cannot set default value of a case expression twice.")
        self.__checkExpr(value)
        self.__defaultValue = value
        return self

    def build(self) -> CaseExpression:
        if self.__defaultValue is None:
            raise RuntimeError("Case expression requires a default value.")
        return CaseExpression(self.__cases, self.__defaultValue)


class Switch:
    """
    A builder for creating case expressions using a switch-like syntax.

    Example:
        Switch(df).case(lambda df: df["c0"] < 4).then(LiteralExpression(0))\
                  .case(lambda df: df["c0"] > 4).then(LiteralExpression(8))\
                  .default(LiteralExpression(2))
    """

    def __init__(self, value: Any, builder: Optional[CaseExpressionBuilder] = None):
        """
        Initialize a Switch builder.

        Args:
            value: The value to use in case conditions
            builder: Optional existing CaseExpressionBuilder to use
        """
        self.__value = value
        self.__builder = builder or CaseExpressionBuilder()

    class Then:
        """
        Intermediate class for building switch cases.
        """

        def __init__(
            self,
            value: Any,
            builder: CaseExpressionBuilder,
            cond: BooleanExpression,
        ):
            """
            Initialize a Then builder.

            Args:
                value: The value to use in case conditions
                builder: The CaseExpressionBuilder to use
                cond: The boolean condition for this case
            """
            self.__value = value
            self.__builder = builder
            self.__cond = cond

        def then(self, value: Expression) -> "Switch":
            """
            Specify the value to return if the condition is true.

            Args:
                value: The expression to return if the condition is true

            Returns:
                A Switch builder to continue building cases
            """
            if not isinstance(value, Expression):
                value = LiteralExpression(value)
            return Switch(self.__value, self.__builder.case(self.__cond, value))

    def case(self, cond: Callable[[Any], BooleanExpression]) -> Then:
        """
        Add a case condition.

        Args:
            cond: A function that takes the switch value and returns a BooleanExpression

        Returns:
            A Then builder to specify the value for this case
        """
        return Switch.Then(self.__value, self.__builder, cond(self.__value))

    def default(self, value: Expression) -> CaseExpression:
        """
        Add a default case.

        Args:
            value: The expression to return if no conditions are met

        Returns:
            A CaseExpression representing the complete switch expression
        """
        if not isinstance(value, Expression):
            value = LiteralExpression(value)
        return self.__builder.default(value).build()


class If:
    """
    A builder for creating if-else expressions.

    Example:
        If(df["c0"] < 4).then(LiteralExpression(0)).otherwise(LiteralExpression(2))
    """

    class Else:
        """
        Intermediate class for building if-else expressions.
        """

        def __init__(self, caseBuilder: CaseExpressionBuilder):
            """
            Initialize an Else builder.

            Args:
                caseBuilder: The CaseExpressionBuilder to use
            """
            self.__case = caseBuilder

        def otherwise(self, value: Expression) -> CaseExpression:
            """
            Specify the value to return if the condition is false.

            Args:
                value: The expression to return if the condition is false

            Returns:
                A CaseExpression representing the complete if-else expression
            """
            if not isinstance(value, Expression):
                value = LiteralExpression(value)
            self.__case.default(value)
            return self.__case.build()

    def __init__(self, cond: BooleanExpression):
        """
        Initialize an If builder.

        Args:
            cond: The boolean condition for the if expression
        """
        if not isinstance(cond, BooleanExpression):
            raise TypeError(f"cond must be a BooleanExpression, got {type(cond)}")
        self.__case = CaseExpressionBuilder()
        self.__cond = cond

    def then(self, value: Expression) -> Else:
        """
        Specify the value to return if the condition is true.

        Args:
            value: The expression to return if the condition is true

        Returns:
            An Else builder to specify the value for the else case
        """
        if not isinstance(value, Expression):
            value = LiteralExpression(value)
        return If.Else(self.__case.case(self.__cond, value))
