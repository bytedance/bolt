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

from typing import Union

from ..expression.field import FieldExpression
from ..function.aggregation import (
    AggregationFunction,
    Mean,
    Sum,
    Min,
    Max,
    Count,
    All,
    Any,
)
from .function import FunctionExpression


class AggregationExpression(FunctionExpression):
    """
    Aggregation assignable to a dataframe with transform() method into a single
    aggregation node.
    """

    def __init__(
        self,
        expr: FunctionExpression,
        groupingKeys: list[str],
    ):
        self.__groupingKeys: list[FieldExpression] = [
            FieldExpression(g, expr.dataframe) for g in groupingKeys
        ]
        super().__init__(expr.name, expr.dtype, expr.arguments, expr.dataframe)

    @property
    def groupingKeys(self) -> list[str]:
        return [g.name for g in self.__groupingKeys]

    @property
    def groupExpressions(self) -> list[FieldExpression]:
        return self.__groupingKeys


class GroupedDataFrame:
    def __init__(
        self,
        groupingKeys: list[str],
        dataframe: "DataFrame",  # noqa: F821
    ):
        self.__groupingKeys = groupingKeys
        self.__dataframe = dataframe

    def __toAggregationExpression(
        self,
        fn: AggregationFunction,
    ) -> AggregationExpression:
        fn.register(self.__dataframe._executor_)
        if isinstance(fn.outputType, AggregationFunction.FieldType):
            outputType = self.__dataframe[fn.outputType.fieldName].dtype
        else:
            outputType = fn.outputType
        arguments = [
            self.__dataframe[arg.fieldName]
            if isinstance(arg, AggregationFunction.Field)
            else self.__dataframe[0]
            if isinstance(arg, AggregationFunction.AnyField)
            else arg
            for arg in fn.arguments
        ]
        fnExpr = FunctionExpression(fn.name, outputType, arguments, self.__dataframe)
        fn._checkExpr(fnExpr)
        return AggregationExpression(fnExpr, self.__groupingKeys)

    def __singleColumnAggregation(self, fn: AggregationFunction) -> Any:
        colName = f"__boltml_aggregation_col_{len(self.__dataframe.names)}"
        expr = self.__toAggregationExpression(fn)
        self.__dataframe.transform(**{colName: expr})
        return self.__dataframe[colName]

    def aggregate(
        self,
        *directFn: list[AggregationFunction],
        **assignments: dict[str, AggregationFunction],
    ) -> "DataFrame":  # noqa: F821
        if len(directFn) > 1:
            raise ValueError("Can only aggregate one anonymous aggregation function.")
        if len(directFn) == 1:
            return self.__singleColumnAggregation(directFn[0])
        for col, fn in assignments.items():
            assignments[col] = self.__toAggregationExpression(fn)
        self.__dataframe.transform(**assignments)
        return self.__dataframe

    def mean(self, col: str) -> float:
        return self.__singleColumnAggregation(Mean(col))

    def sum(self, col: str) -> Union[int, float]:
        return self.__singleColumnAggregation(Sum(col))

    def min(self, col: str) -> Union[int, float]:
        return self.__singleColumnAggregation(Min(col))

    def max(self, col: str) -> Union[int, float]:
        return self.__singleColumnAggregation(Max(col))

    def count(self) -> int:
        return self.__singleColumnAggregation(Count())

    def all(self, col: str) -> bool:
        return self.__singleColumnAggregation(All(col))

    def any(self, col: str) -> bool:
        return self.__singleColumnAggregation(Any(col))
