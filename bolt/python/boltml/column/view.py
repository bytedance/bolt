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

import copy
from typing import Callable, Union

from pybolt import (
    BooleanType,
    BoltType,
    rowVector,
    TinyintType,
    SmallintType,
    IntegerType,
    BigintType,
    HugeintType,
    RealType,
    DoubleType,
    VarcharType,
)

from ..expression.base import Expression
from ..expression.cast import CastExpression
from ..expression.literal import LiteralExpression
from ..expression.field import FieldExpression
from ..expression.function import FunctionExpression
from ..expression.project import ProjectExpression
from ..function.vector import functionRegister

from .base import Column


class ColumnView(Column):
    @staticmethod
    def _captureDtype(df: "DataFrame", column: str):  # noqa: F821
        """Resolve ``column``'s dtype against *df*'s current schema and
        return it as a fallback snapshot, or ``None`` if the column
        isn't bound yet (empty DataFrame, lazy upstream not yet
        materialised with a matching field).

        This is *not* the value ``dtype`` normally returns — that
        property reads the live schema first (see ``dtype``). The
        snapshot is only consulted as a fallback for when the column
        later leaves the live schema, which happens when a view is
        captured as an expression argument (``Sum(df["c0"])``) and the
        owning dataframe is then mutated so ``c0`` is gone by lowering
        time (e.g. consumed by a ``groupBy().aggregate()``). The
        snapshot carries the type the column had when referenced;
        without it that lowering path raises ``Field not found: c0``
        (this regressed 13 aggregation tests when the fallback was
        absent). Refreshed at ``__init__``, ``__deepcopy__``, and after
        ``map`` reassigns the column, so the fallback always reflects
        the column's type as of its last presence in the schema.
        Centralised here so the try/except lives in one place.
        """
        try:
            return df.dtype.findChild(column)
        except Exception:  # noqa: BLE001
            return None

    def __init__(self, df: "DataFrame", column: str):  # noqa: F821
        self.__dataframe = df
        self.__name = column
        self.__dtype = self._captureDtype(df, column)

    @property
    def data(self):
        """
        Materializes the dataframe and retrieves the Bolt vector
        associated with this column.
        """
        return self.__dataframe._data_.childAt(self.name)

    @property
    def dtype(self) -> BoltType:
        # A ColumnView is a *live* window onto its dataframe: resolve the
        # column's dtype against the dataframe's current schema so an
        # in-place type change (e.g. ``cast`` reprojecting the column) is
        # reflected rather than shadowed by a stale capture-time value.
        #
        # Fall back to the capture-time snapshot only when the column is
        # no longer present in the live schema. That happens when the view
        # was captured as an expression argument (``Sum(df["c0"])``) and
        # the owning dataframe was then mutated so ``c0`` left the schema
        # (e.g. consumed by a ``groupBy().aggregate()``); the snapshot
        # carries the type the column had when it was referenced, which
        # the lowering needs. Without the fallback that path raises
        # ``Field not found: c0``.
        try:
            return self.__dataframe.dtype.findChild(self.name)
        except Exception:  # noqa: BLE001
            if self.__dtype is not None:
                return self.__dtype
            raise

    @property
    def name(self) -> str:
        return self.__name

    @property
    def formatter(self):
        return self.__dataframe._formatter_

    @property
    def dataframe(self):
        """
        Get the dataframe this column view originates from.
        """
        return self.__dataframe

    def isNum(self) -> bool:
        return self.dtype in (
            TinyintType(),
            SmallintType(),
            IntegerType(),
            BigintType(),
            HugeintType(),
            RealType(),
            DoubleType(),
        )

    def isBool(self) -> bool:
        return self.dtype == BooleanType()

    def isString(self) -> bool:
        return self.dtype == VarcharType()

    @staticmethod
    def make(df: "DataFrame", column: str) -> "Self":  # noqa: F821
        """
        Make a column view from a dataframe column with the proper child
        class.
        """
        col = ColumnView(df, column)
        if col.isString():
            from .string import StringColumn

            return StringColumn(df, column)
        if col.isNum():
            from .num import NumColumn

            return NumColumn(df, column)

        from .project import ColumnProject

        return ColumnProject(df, column)

    def __makeDataframe(self, *args):
        """
        Create a dataframe instance.
        This method breaks the circular dependency between the dataframe and
        the columnView modules.
        """
        from ..dataframe import DataFrame

        return DataFrame(*args, **self.__dataframe._derivedKwargs_())

    def __deepcopy__(self, memo) -> "ColumnView":
        newInstance = self.__class__.__new__(self.__class__)
        memo[id(self)] = newInstance
        data = rowVector([self.name], [self.data.copy()])
        newInstance.__dataframe = self.__makeDataframe(data)
        newInstance.__name = self.name
        # ``__class__.__new__`` bypasses ``__init__`` so the dtype
        # snapshot must be re-established explicitly here; otherwise
        # ``newInstance.dtype`` would crash on AttributeError when
        # the property accesses the unset ``__dtype`` attribute.
        newInstance.__dtype = self._captureDtype(newInstance.__dataframe, self.name)
        return newInstance

    def copy(self) -> "ColumnView":
        """
        Equivalent to copy.deepcopy(self)
        """
        return copy.deepcopy(self)

    def rename(self, replacement: str) -> "ColumnView":
        """
        Rename the column view and the associated column in the underlying
        dataframe.
        """
        self.__dataframe.rename(**{self.name: replacement})
        self.__name = replacement
        return self

    def cast(self, dtype: BoltType) -> "Self":  # noqa: F821
        castExpr = {
            self.name: ProjectExpression(
                CastExpression(FieldExpression(self.name, self.__dataframe), dtype)
            )
        }
        self.__dataframe.transform(**castExpr)
        return ColumnView.make(self.dataframe, self.name)

    def map(
        self,
        rhs: Union[
            Expression, Callable, Callable[["DataFrame", ...], Expression]  # noqa: F821
        ],
        *args,
    ) -> "Self":  # noqa: F821
        """
        Assign `rhs` to this column.
        `rhs` can be one of the following:

        * A registered function callable. If `rhs` is a registered
        `vectorFunction`, `scalarFunction` or `mapBatchFunction`, then
        an expression calling the callable with the stringified `args`
        argument preceded by this column name is created and assigned to
        the column for evaluation as part of the owning dataframe. In this case,
        the callable arguments need to be an Expression type or cohersible to
        the latter (i.e bool, int, float and str literal).
        ```
        @scalarFunction(pybolt.BigintType())
        def add(lhs, rhs):
            return lhs + rhs
        col.map(add, 2)
        ```

        * A callable that receives this column as its first argument, followed
        by `args` and returning an `Expression` object or a scalar value
        that can be assigned to a column. In that case, the callable is
        called to generate the expression and the latter is assign to the
        column for evaluation as part of the owning dataframe.
        For instance:
        ```
        # Logically equivalent to the example above.
        col.map(lambda c, v: c + v, 2)
        # Or simpler:
        col.map(lambda c: c + 2)
        ```

        * An `Expression` or scalar value that can be assigned to a column.
        See `DataFrame.__transform__` for more information.
        ```
        df = DataFrame({"c0": [0, 2] })
        df["c0"].map(df["c0"] + 2)
        ```
        """
        if (
            not callable(rhs)
            and not isinstance(rhs, ProjectExpression)
            and not isinstance(rhs, FunctionExpression)
        ):
            raise TypeError(
                "ColumnView.map should be callable (lambda column, ... -> column)"
                f"or a ProjectExpression, not: {type(rhs)}"
            )

        if isinstance(rhs, ProjectExpression) or isinstance(rhs, FunctionExpression):
            pass
        elif rhs in functionRegister:
            fn = functionRegister[rhs]
            rhs = FunctionExpression(
                fn.name,
                fn.outputType,
                [self]
                + [
                    arg if isinstance(arg, Expression) else LiteralExpression(arg)
                    for arg in args
                ],
                self.dataframe,
            )
            rhs = ProjectExpression(rhs)
        else:
            rexpr = rhs(self, *args)
            if not isinstance(rexpr, Expression):
                raise TypeError(f"{rhs} did not yield an expression.")
            if not isinstance(rexpr, ProjectExpression):
                rhs = ProjectExpression(rexpr)
            else:
                rhs = rexpr

        self.dataframe[self.name] = rhs
        # The assignment may have rewritten this column's type — e.g.
        # ``col.map(lambda c: c.isNone())`` turns a numeric column
        # into a BOOLEAN one. Re-capture so ``dtype`` reflects the
        # post-assignment type.
        self.__dtype = self._captureDtype(self.__dataframe, self.__name)
        return self
