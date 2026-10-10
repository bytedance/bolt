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

from pybolt import rowVector

from ..expression.field import FieldExpression
from ..expression.string import StringProjectExpression

from .view import ColumnView


class StringColumn(ColumnView, StringProjectExpression):
    def __init__(self, df: "DataFrame", column: str):  # noqa: F821
        expr = FieldExpression(column, df)
        ColumnView.__init__(self, df, column)
        StringProjectExpression.__init__(self, expr)

    def __deepcopy__(self, memo) -> "StringColumn":
        from ..dataframe import DataFrame

        data = rowVector([self.name], [self.data.copy()])
        return StringColumn(DataFrame(data), self.name)

    def __eq__(self, rhs: Union[StringProjectExpression, str]):
        """
        If the column is compared to an Expression or a str,
        then the intent is to build an expression and we return an expression.
        Otherwise, if the right hand side `rhs` is a column, then we use
        the comparison operator.
        """
        return StringProjectExpression.__eq__(self, rhs)

    def __ne__(self, rhs: Union[StringProjectExpression, str]):
        """
        If the column is compared to an Expression or a str,
        then the intent is to build an expression and we return an expression.
        Otherwise, if the right hand side `rhs` is a column, then we use
        the comparison operator.
        """
        return StringProjectExpression.__ne__(self, rhs)
