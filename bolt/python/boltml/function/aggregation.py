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

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Callable, Optional, Union
import inspect

from pybolt import BaseVector, BoltType, BooleanType, DoubleType, BigintType, RowVector

from ..executor.base import Executor
from ..expression.base import Expression
from ..expression.literal import LiteralExpression
from ..expression.function import FunctionExpression

# The register of user custom aggregation functions used to collect the
# return type and name of the function.
functionRegister = defaultdict(list)


class AggregationFunction:
    """
    Base class for aggregation functions accepted by grouped dataframe
    aggregate() method.
    """

    def __init__(
        self,
        name: str,
        outputType: BoltType,
        arguments: list[Expression],
        checkExpr: Optional[Callable[[FunctionExpression], None]] = None,
        registrationHook: Optional[Callable[[Executor], None]] = None,
    ):
        self.__name = name
        self.__outputType = outputType
        self.__arguments = arguments
        self.__checkExpr = checkExpr
        self.__register = registrationHook

    def register(self, executor: Executor):
        """
        Use the constructor registration hook to register the function if needed.
        For function that don't need registration, `registrationHook` should be
        `None`.
        """
        if self.__register is None:
            return
        # Avoid multiple registrations.
        if executor in functionRegister[self.name]:
            return
        self.__register(executor)
        functionRegister[self.name].append(executor)

    def _checkExpr(self, expr: FunctionExpression):
        """
        Method used by GroupedDataFrame to check inputs of the
        AggregationFunction when it is converted into an AggregationExpression.
        """
        if self.__checkExpr is not None:
            self.__checkExpr(expr)

    @property
    def outputType(self) -> BoltType:
        return self.__outputType

    @property
    def name(self) -> str:
        return self.__name

    @property
    def arguments(self) -> list[Expression]:
        return self.__arguments

    class FieldType:
        """
        A temporary type shadowing the type of a dataframe column.
        The type is to be set later when the type is matched with a
        dataframe.
        """

        def __init__(self, fieldName: str):
            self.__fieldName = fieldName

        @property
        def fieldName(self) -> str:
            return self.__fieldName

    class Field(Expression):
        """
        A temporary expression with a FieldType representing a dataframe
        column
        """

        def __init__(self, name: str):
            self.__name = name
            super().__init__(name, AggregationFunction.FieldType(name), None)

        @property
        def fieldName(self) -> str:
            return self.__name

    class AnyField(Expression):
        """
        A temporary expression meant to be replaced with any dataframe column.
        """

        def __init__(self):
            super().__init__("any", None, None)

    @staticmethod
    def _checkCol(
        fn: Callable[["ColumnView"], bool],  # noqa: F821
    ) -> Callable[[FunctionExpression], None]:
        def checkFn(fnExpr: FunctionExpression):
            if len(fnExpr.arguments) == 0:
                raise RuntimeError(
                    f"Aggregation function {fnExpr.name} expected at least one argument."
                )
            if not fn(fnExpr.arguments[0]):
                raise RuntimeError(
                    f"Invalid argument {fnExpr.arguments[0].expr()} for aggregation "
                    f"function '{fnExpr.name}'."
                )

        return checkFn


class Mean(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            name="avg",
            outputType=DoubleType(),
            arguments=[AggregationFunction.Field(col)],
            checkExpr=AggregationFunction._checkCol(lambda c: c.isNum()),
        )


class Sum(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            "sum",
            AggregationFunction.FieldType(col),
            [AggregationFunction.Field(col)],
            AggregationFunction._checkCol(lambda c: c.isNum()),
        )


class Min(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            "min",
            AggregationFunction.FieldType(col),
            [AggregationFunction.Field(col)],
            AggregationFunction._checkCol(lambda c: c.isNum()),
        )


class Max(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            "max",
            AggregationFunction.FieldType(col),
            [AggregationFunction.Field(col)],
            AggregationFunction._checkCol(lambda c: c.isNum()),
        )


class Count(AggregationFunction):
    """SQL ``COUNT`` aggregation.

    * ``Count()`` -- ``COUNT(*)``: counts every row in the group, never
      skipping. Internally uses an ``AnyField`` placeholder so the C++
      side knows it can pick any column to walk row-by-row.
    * ``Count(col)`` -- ``COUNT(col)``: counts non-NULL values of *col*
      only. This is the SQL-spec null-skipping count, the form needed by
      LEFT-outer-join + group-by patterns like TPC-H Q13 where customers
      with no matching orders should reach the group-by stage with a
      NULL ``o_orderkey`` and contribute 0 to the count.

    Both variants resolve to Substrait's ``count`` scalar function; the
    difference is solely whether the argument is a placeholder or a
    bound field reference. Bolt's C++ aggregation layer reads the
    function arity at run time and dispatches to ``count_star`` or
    ``count(col)`` accordingly.
    """

    def __init__(self, col: Optional[str] = None):
        if col is None:
            super().__init__("count", BigintType(), [AggregationFunction.AnyField()])
        else:
            super().__init__(
                "count",
                BigintType(),
                [AggregationFunction.Field(col)],
            )


class All(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            "bool_and",
            BooleanType(),
            [AggregationFunction.Field(col)],
            AggregationFunction._checkCol(lambda c: c.isBool()),
        )


class Any(AggregationFunction):
    def __init__(self, col: str):
        super().__init__(
            "bool_or",
            BooleanType(),
            [AggregationFunction.Field(col)],
            AggregationFunction._checkCol(lambda c: c.isBool()),
        )


class Aggregator(ABC):
    """
    Base class for user custom aggregation functions.

    This class has a dual purpose:
    1. To generate an `AggregationFunction` to be used as part of
    a dataframe groupby/aggregate call.
    ```
    avg = MyAggregator.initializer()
    df.groupBy("c1").aggregate(result=avg(df["c0"]))
    ```

    2. To store and accumulate aggregates in the executor.
    """

    def __call__(self, *args) -> AggregationFunction:
        """
        The __call__ method is used on an instance of aggregator to yield an
        aggregation expression.
        """
        args = [
            arg if isinstance(arg, Expression) else LiteralExpression(arg)
            for arg in args
        ]
        return AggregationFunction(
            self.__class__.__name__,
            self.outputType(),
            args,
            registrationHook=lambda executor: self.__register(executor),
        )

    @abstractmethod
    def outputType(self) -> BoltType:
        """
        The output type of the "reduce()" method.
        """
        ...

    @staticmethod
    @abstractmethod
    def initializer() -> "Self":  # noqa: F821
        """
        Create a new aggregator initialized with a neutral value.
        For instance this would be `0` for `+` or `1` for `*`.
        """
        ...

    @staticmethod
    @abstractmethod
    def aggregate(*columns: list[BaseVector]) -> "Self":  # noqa: F821
        """
        Create intermediate aggregate values from columns.
        `columns` is a list of bolt vectors.
        The method must take as many arguments as there are expected columns.

        For instance, for a `mean` aggregator and a single column, this would be
        two scalar values:
        * sum containing the sum of elements,
        * count containing the length if the column.
        """
        ...

    @abstractmethod
    def accumulate(self, other: "Self"):  # noqa: F821
        """
        Merge multiple aggregates together into this aggregate.

        Because grouped data is processed in batches, multiple aggregates
        of the same group need to be merged.
        For instance, for a `mean` aggregator, the `sum` and `count` would
        see their content incremented respectively by `other.sum` and
        `other.count`
        """
        ...

    @abstractmethod
    def reduce(self) -> Union[int, float, str, RowVector]:
        """
        Compute the final scalar value for the aggregation.

        The reduce result represents a single column and single row element
        scalar value in the final vector matching the type of `outputType()`.
        The type must be a native python type.

        For instance, for `mean` aggregator this would be computing
        `sum / count` and returning the results.
        """
        ...

    @property
    def name(self) -> str:
        """
        A unique name for the aggregation function instance represented by this
        aggregator.
        `Aggregator`s are registered in the backend based on this name.
        """
        return self.__class__.__name__

    def __register(self, executor: Executor):
        """
        Registers an Aggregator child class to an executor to make it usable.
        This should not be directly used by the user.
        """

        def checkFn(
            self,
            name: str,
            isStaticmethod: bool = False,
            isClassmethod: bool = False,
            mayHaveVoidSignature: bool = True,
        ):
            if not hasattr(self, name):
                raise TypeError(f"{self} needs to implement '{name}()' method.")

            method = inspect.getattr_static(self, name)

            if isStaticmethod != isinstance(method, staticmethod):
                raise TypeError(
                    f"{self} method '{name}()' must "
                    f"{'' if isStaticmethod else 'not '}be a "
                    "staticmethod."
                )

            if isClassmethod != isinstance(method, classmethod):
                raise TypeError(
                    f"{self} method '{name}()' must "
                    f"{'' if isClassmethod else 'not '}be a "
                    "classmethod."
                )

            signature = inspect.signature(getattr(self, name))
            parameters = list(iter(signature.parameters.values()))
            if not isStaticmethod and not isClassmethod:
                parameters = [p for p in parameters if p.name != "self"]

            hasMandatoryArgs = any(
                (p.default is inspect.Parameter.empty for p in parameters)
            )

            if mayHaveVoidSignature != (not hasMandatoryArgs):
                raise TypeError(
                    f"{self} method '{name}()' must "
                    f"{'not ' if mayHaveVoidSignature else ''}have "
                    "mandatory arguments."
                )

        checkFn(self, "initializer", mayHaveVoidSignature=True, isStaticmethod=True)
        checkFn(
            self,
            "aggregate",
            isStaticmethod=True,
            mayHaveVoidSignature=False,
        )
        checkFn(self, "accumulate", mayHaveVoidSignature=False)
        checkFn(self, "reduce", mayHaveVoidSignature=True)
        outputType = self.initializer().outputType()
        numArgs = self.aggregate.__code__.co_argcount
        executor.registerAggregationFunction(self, self.name, outputType, numArgs)
