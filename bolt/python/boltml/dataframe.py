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
from functools import wraps
from random import getrandbits
from typing import Any, Callable, Iterator, Optional, Union

import pyarrow as pa
from pybolt import (
    BaseVector,
    BoltType,
    JoinType,
    RowType,
    RowVector,
    SortOrder,
    constantVector,
    exportToArrow,
    fromList,
    importFromArrow,
    rowVector,
)

from .expression import (
    AggregationExpression,
    GroupedDataFrame,
    BooleanExpression,
    CaseExpression,
    Expression,
    FieldExpression,
    LiteralExpression,
    NamedExpression,
    ProjectExpression,
)

from .column.view import ColumnView
from .function.dataframe import functionRegister as dataframeFunctions
from .details.txt_formatter import TxtFormatter
from .details import (
    _isConstantType_,
    _isVectorType_,
    _orderByArg_,
)
from .execute import execute as _execute
from .executor import Executor, LocalExecutor
from .plan_builder.substrait import SubstraitPlanBuilderFactory
from .plan_builder.base import (
    LogicalPlanBuilderFactory,
    PlanBuilder,
    PlanBuilderFactory,
)

_localExecutor_ = LocalExecutor()


class DataFrame:
    """
    `DataFrame` Object for bolt-ml

    This object represents a table with column names and columns of various
    types and associated methods.
    The data inside of a DataFrame is backed by bolt `RowVector` with an
    allocation that spans the lifetime of the `pybolt` module.
    Some `DataFrame` operations will turn it into a "lazy" `DataFrame`, i.e
    with an internal representation using a bolt `PlanBuilder` instead of a
    `RowVector`. Whenever a materialized form of the dataframe is required
    (albeit by invoking some of its methods) the lazy representation is
    evaluated using an `Executor` instance stored in the `DataFrame`.
    """

    def __init__(
        self,
        arg=None,
        executor: Optional[Executor] = None,
        formatter=TxtFormatter.formatDataframe,
        planFactory: LogicalPlanBuilderFactory = SubstraitPlanBuilderFactory(),
    ):
        """
        DataFrame constructor.

        * `arg`: one of:
         - dict[str, list[Any]]: This is the main constructor for a dataframe
        from existing data. Dictionary keys will be used as column names and
        dictionary values will be used as column values.
         - None: Creates an empty dataframe. Beware that most operation are not
        possible on en empty dataframe. This is mostly used internally.
         - pybolt RowVector: this is the internal representation of in memory
        data. The dataframe object will directly use this row vector as its
        internal representation. This is mostly used internally.
         - pybolt PlanBuilder: this is the internal representation of a lazy
        dataframe. The dataframe object will directly use this plan builder as
        its internal representation. This is mostly used internally.
        * `executor`: An `Executor` instance to execute a chain of operations on
        a DataFrame. The default value executes operations on the local machine.
        * `planFactory`: a ``LogicalPlanBuilderFactory`` that builds the lazy
        plan. Defaults to ``SubstraitPlanBuilderFactory``. Optimization is no
        longer baked into the builder: the executor materialisation path calls
        ``plan.optimize().boltPlan()``, so the logical Substrait optimizer runs
        once at execution time (not in ``boltPlan()``).
        """
        self.__executor: Executor = executor or _localExecutor_
        self.__data: Optional[Union[RowVector, PlanBuilder]] = None
        self.__planFactory = planFactory
        self.__formatter = formatter

        if arg is None:
            pass
        elif isinstance(arg, dict):
            self.fromDict(arg)
        elif isinstance(arg, RowVector):
            self.fromRowVector(arg)
        elif isinstance(arg, PlanBuilder):
            self.fromPlanBuilder(arg, planFactory)
        elif any(
            (
                isinstance(arg, t)
                for t in (
                    pa.Table,
                    pa.Array,
                    pa.StructArray,
                    pa.RecordBatch,
                    pa.ChunkedArray,
                )
            )
        ):
            self.fromArrow(arg)
        else:
            raise TypeError(
                f"Cannot make {self.__class__.__name__} from type {type(arg)}."
            )

    def fromArrow(
        self,
        arg: Union[pa.Table, pa.Array, pa.ChunkedArray, pa.StructArray, pa.RecordBatch],
    ):
        if isinstance(arg, pa.ChunkedArray):
            return self.fromArrow(arg.combine_chunks())
        if isinstance(arg, pa.StructArray):
            data = importFromArrow(arg)
            if not isinstance(data, BaseVector):
                raise TypeError(
                    "DataFrame conversion from arrow did not yield a BaseVector"
                )
            return self.fromRowVector(data)
        if hasattr(arg, "to_struct_array") and callable(arg.to_struct_array):
            # ``Table.to_struct_array()`` / ``RecordBatch.to_struct_array()``
            # raise ``ArrowInvalid: cannot construct ChunkedArray from empty
            # vector and omitted type`` when the input has zero rows, because
            # pyarrow can't infer the chunk type from an empty list of chunks.
            # Build the empty StructArray explicitly from the schema instead so
            # the column types stay intact and downstream consumers see the
            # right row schema.
            if (
                hasattr(arg, "num_rows")
                and arg.num_rows == 0
                and hasattr(arg, "schema")
            ):
                struct_array = pa.StructArray.from_arrays(
                    [pa.array([], type=f.type) for f in arg.schema],
                    fields=list(arg.schema),
                )
                return self.fromArrow(struct_array)
            return self.fromArrow(arg.to_struct_array())
        raise TypeError(f"Unsupported DataFrame conversion from type {type(arg)}")

    def fromRowVector(self, rowVector: RowVector):
        self.__data = rowVector
        return self

    def fromPlanBuilder(self, planBuilder: PlanBuilder, factory: PlanBuilderFactory):
        self.__data = planBuilder
        self.__planFactory = factory
        return self

    def fromDict(self, dictionary: dict):
        def _makeField_(arg, length=None):
            if _isVectorType_(arg):
                return arg
            if not isinstance(arg, list) and hasattr(arg, "__iter__"):
                arg = list(arg)
            if isinstance(arg, list):
                return fromList(arg)
            if _isConstantType_(arg):
                if length is not None:
                    return constantVector(arg, length)
                else:
                    raise ValueError(
                        "Internal error: _makeField_() called on constant "
                        "field without a length."
                    )
            raise TypeError(f"Cannot make field type {type(arg)}.")

        fields = {}
        # Constant field requires a length for initialization.
        # If the first dictionary element is a constant fields, we would
        # not know its length yet. We will do a second pass through constant
        # fields once we set the other fields.
        constantFields = {}

        for k, v in dictionary.items():
            if _isConstantType_(v):
                constantFields[k] = v
            else:
                fields[k] = _makeField_(v)

        # Pass for constant fields.
        # The size of the constant fields are either the size of the first
        # fields or 1 if there are only constant fields.
        length = next((len(v) for v in fields.values() if hasattr(v, "__len__")), 1)
        for k, v in constantFields.items():
            fields[k] = _makeField_(v, length)

        names = list(fields.keys())
        values = [fields[k] for k in names]
        return self.fromRowVector(rowVector(names, values))

    @property
    def _data_(self) -> RowVector:
        """
        Safe internal bolt row vector accessor.
        """
        if isinstance(self.__data, PlanBuilder):
            self.__eval()
        return self.__data

    @property
    def _planBuilder_(self) -> PlanBuilder:
        """
        Safe internal bolt plan builder accessor.

        This method will move the inner RowVector data to a new plan
        if needed before returning the plan stored internally.
        This is intended to be used as a 'friend' method by classes that
        need access to the plan builder.
        """
        if isinstance(self.__data, RowVector):
            self.__lazy()
        return self.__data

    @property
    def _executor_(self) -> Executor:
        """
        Get a reference of the underlying executor.

        This is intended to be used as a 'friend' method by classes that
        need to get a reference to the same executor.
        """
        return self.__executor

    @property
    def _formatter_(self) -> TxtFormatter:
        return self.__formatter

    @property
    def _planFactory_(self) -> PlanBuilderFactory:
        """
        Get a reference of the underlying plan builder factory.

        This is intended to be used as a 'friend' method by classes that
        need to get a reference to the same plan builder factory.
        """
        return self.__planFactory

    def _derivedKwargs_(self) -> dict:
        """Return the constructor kwargs needed to build a derived
        DataFrame that shares this one's executor / plan-builder factory.
        Friend method for ColumnView and any future helper that
        materialises a child DataFrame from a column-level operation.
        """
        return {
            "executor": self.__executor,
            "planFactory": self.__planFactory,
        }

    def __eval(self):
        """
        Evaluate existing plan and store the result in the `data_` field.
        """
        assert isinstance(self.__data, PlanBuilder)
        self.__data = _execute(self.__data, self.__executor)

    def __lazy(self):
        """
        Move existing data (if any) into a plan.
        """
        assert self.__data is None or isinstance(self.__data, RowVector)
        if isinstance(self.__data, RowVector):
            self.__data: PlanBuilder = self.__planFactory.fromData(self.__data)
        else:
            self.__data: PlanBuilder = self.__planFactory.empty()

    def __evalmethod(fn):
        """
        Decorator for `DataFrame` methods that require to evaluate an existing
        plan before accessing the internal data.
        """

        @wraps(fn)
        def wrapper(self: "DataFrame", *args, **kwargs):
            if isinstance(self.__data, PlanBuilder):
                self.__eval()
            return fn(self, *args, **kwargs)

        return wrapper

    def __lazymethod(fn):
        """
        Decorator for `DataFrame` lazily evaluated methods.
        Lazy methods require a PlanBuilder. This decorator will create a plan if
        it does not exist and move the `DataFrame`'s data inside of the plan.
        """

        @wraps(fn)
        def wrapper(self: "DataFrame", *args, **kwargs):
            if self.__data is None or isinstance(self.__data, RowVector):
                self.__lazy()
            return fn(self, *args, **kwargs)

        return wrapper

    def __eq__(self, rhs: "DataFrame") -> bool:
        # First check elements that don't require evaluation.
        if self is rhs:
            return True

        if not isinstance(rhs, DataFrame):
            return False

        if len(self.names) != len(rhs.names):
            return False

        if any(n not in rhs.names for n in self.names):
            return False

        if any(self[c].dtype != rhs[c].dtype for c in self.names):
            return False

        # Next check elements that require dataframe evaluation.
        if self._data_ is rhs._data_:
            return True

        # Check columns equality. Columns might have different order
        return all(self[c].equals(rhs[c]) for c in self.names)

    @__evalmethod
    def __len__(self) -> int:
        if self.__data is None:
            return 0
        return self.__data.__len__()

    def __getitem__(self, index) -> Any:
        """
        Subscript operator for DataFrame

        This method supports different types of arguments and will return
        different types of return value accordingly. If `index` type is:
        * `str`: It is considered as a column accessor and will return
        a `ColumnView` object of the same name if the column exists.
        * `int`: It is considered as a column accessor and will return
        a `ColumnView` object at the same index position in the list of
        columns.
        * `slice`: It is considered as a `DataFrame` subsetting operation
        and will return a `DataFrame` view that contains only the selected
        column indices.
        * `list[str] | list[int]`: It is considered as a `DataFrame`
        subsetting operation and will return a `DataFrame` view that
        contains only the selected columns.
        * `tuple[Any, Any]`: the first element is the row selector while the
        second element is the column selector. See details below:
        * `tuple[int, str]`: It is considered as an element access where
        the `str` type selects the column and the `int` type selects the row.
        The type of return value in that case matches the type the selected
        column elements.
        * `tuple[slice, str]`: It is considered as a column accessor
        and subsetting. Slice stepping is not supported. It will return
        a `ColumnView` object of the same name if the column exists.
        The `ColumnView` will only contain elements from the slice.
        * `tuple[int|slice, list[str|int]]`: `DataFrame`
        subsetting operation and will return a `DataFrame` view that
        contains only the selected columns and rows.
        """
        if self.__data is None:
            return None

        if not any((isinstance(index, t) for t in [tuple, int, str, list, slice])):
            raise TypeError(f"Invalid DataFrame indexing type: {type(index)}")

        columns = index
        rows = None

        if isinstance(index, tuple):
            if len(index) != 2:
                raise ValueError(
                    "Invalid DataFrame indexing tuple size. Tuple must have"
                    " exactly two elements, one for the row selector and one"
                    " for the columns selector."
                )
            columns = index[1]
            rows = index[0]

        # Convert columns into a list of string of the DataFrame field names.
        def asColnames(df, columns):
            if isinstance(columns, int):
                return [df.names[columns]]
            if isinstance(columns, str):
                if columns not in df.names:
                    raise ValueError(
                        f"Invalid DataFrame column: {columns}. Existing columns are: {df.names}"
                    )
                return [columns]
            if isinstance(columns, slice):
                return df.names[columns]
            if isinstance(columns, list):
                return [asColnames(df, c)[0] for c in columns]
            raise TypeError(f"Invalid dataframe columns index type {type(columns)}")

        columns = asColnames(self, columns)
        if len(columns) == 0:
            return None

        # Case for single column selector.
        if rows is None and len(columns) == 1:
            return ColumnView.make(self, columns[0])

        # Case for multiple columns selector.
        if rows is None:
            return DataFrame(
                {n: self._data_.childAt(n) for n in columns},
                executor=self.__executor,
            )

        # Case for single element access.
        if (
            isinstance(rows, int)
            and len(columns) == 1
            and not isinstance(index[1], slice)
        ):
            col = self._data_.childAt(columns[0])
            if type(col) is RowVector:
                return DataFrame(col)[rows, :]._data_
            else:
                return col[rows]

        # Case for multiple columns selection.
        if isinstance(rows, int):
            if rows == -1:
                rows = slice(rows, None)
            else:
                rows = slice(rows, rows + 1)
        if not isinstance(rows, slice):
            raise TypeError(f"Invalid row indexing type: {type(rows)}")
        cols = []
        for n in columns:
            col = self._data_.childAt(n)
            if type(col) is RowVector:
                cols.append(DataFrame(col)[rows, :]._data_)
            else:
                cols.append(col[rows])
        data = rowVector(columns, cols)
        return DataFrame(
            executor=self.__executor,
        ).fromRowVector(data)

    @__evalmethod
    def __iter__(self) -> Iterator:
        """
        `DataFrame` columns iterator.
        """
        return (self[col] for col in self.names)

    @__lazymethod
    def __assignProjectExpressions(self, expressions: dict[str, ProjectExpression]):
        plan: PlanBuilder = self.__data
        projections: list[NamedExpression] = []

        # There are two kind of projections (order matters):
        # * projections that modify an existing column (standard).
        # * projections that append a new column (append),
        for col in self.names:
            if col in expressions:
                projections.append(NamedExpression(col, expressions[col]))
            else:
                projections.append(NamedExpression(col, FieldExpression(col, self)))
        for col, expr in expressions.items():
            if col not in self.names:
                projections.append(NamedExpression(col, expr))
        plan.project(projections)

    @__lazymethod
    def __assignAggregationExpressions(
        self, expressions: dict[str, AggregationExpression]
    ):
        plan: PlanBuilder = self.__data
        # Check that all aggregation expressions use the same set of keys
        groups = set(
            ["_-:boltml:-_".join(expr.groupingKeys) for expr in expressions.values()]
        )
        if len(groups) > 1:
            raise ValueError(
                "Cannot assign multiple aggregations at once when they have "
                "different keys."
            )
        keys = next(iter(expressions.values())).groupExpressions
        expressions = [NamedExpression(col, expr) for col, expr in expressions.items()]
        plan.aggregate(keys, expressions)

    def __setitem__(
        self, index: Union[tuple[int, Union[str, int]], str, int], value: Any
    ):
        """
        Sets a single element in a dataframe.

        Indexing supports different types:

        * `str|int`: Will set an entire column of the dataframe. The `value`
        argument needs to be a coercible into a single DataFrame column i.e:
          * a constant value
          * a bolt vector
          * a ColumnView (from the same dataframe),
          * an Expression
        If the column already exists, it will be overridden, otherwise,
        it will be created and added to the dataframe.
        If `index` is of type `int`, the index must be within the range
        of the DataFrame number of columns and will be converted into the
        actual column name.
        * `tuple[int, str|int]`: Will set a single element.
        The first `int` type element selects the row while the second element
        selects the column, i.e by name if its type is `str` else by index.
        In that variant, the `value` type must be coercible into the selected
        column element types. The column and row must exist in the dataframe.
        """
        if self.__data is None:
            raise ValueError("Cannot set item of empty DataFrame.")

        row = None
        col = index

        # Check types of row and index selectors
        if isinstance(index, tuple):
            row, col = index
        if isinstance(col, int):
            col = self.names[col]
        if not isinstance(col, str):
            raise TypeError(f"Invalid argument type ({type(col)}) for column selector.")
        if row is not None and not isinstance(row, int):
            raise TypeError(f"Invalid argument type ({type(row)}) for row selector.")

        # Case for single (entire) column setter.
        if row is None:
            if isinstance(value, ColumnView):
                if value.dataframe is self:
                    # Redundant assignment from a column to itself.
                    # This can happen in an unexpected way as follow:
                    # when using __imul__ and friend overload they may run
                    # assignment to dataframe column in that method.
                    # For instance if we want to support both:
                    # 1. `df["c"] *= df["d"]` and
                    # 2. `c = df["c"]; c *= df["d"]`
                    # In case 2., only one assignment `df["c"] = expression`
                    # happens in __imul__ and the column reference returned is
                    # assign to `c`. If the assignment was not happening in
                    # __imul__, then the dataframe would not be updated.
                    # In case 1., one assignment happens in __imul__ and another
                    # one when assigning to df["c"], therefore calling
                    # DataFrame.__setitem__ a second time here but with the
                    # column instead of the expression that has been assigned
                    # in __imul__.
                    # Note that if we let python default __imul__
                    # implementation run, we end up not supporting case 2. where
                    # the `Expression` reference would be stored in c instead
                    # without updating the dataframe.
                    if value.name == col:
                        return
                    self.transform(**{col: value})
                else:
                    data = {c: self._data_.childAt(c) for c in self.names}
                    data[col] = value._data_.copy()
                    self.fromDict(data)
            elif isinstance(value, Expression):
                self.transform(**{col: value})
            elif type(value) in (int, float, str, bool):
                self.transform(**{col: value})
            elif isinstance(value, BaseVector):
                # len(self) materializes self.__data to a RowVector.
                if len(value) != len(self):
                    raise ValueError(
                        "Cannot set DataFrame column with column of different length."
                    )
                data = {c: self.__data.childAt(c) for c in self.names}
                data[col] = value
                self.fromDict(data)
            else:
                raise TypeError(
                    f"Unsupported value type: {type(value)} as DataFrame column."
                )
            return

        # Case for single value within a column setter.

        # We can only set scalar value if the column exists.
        if col not in self.names:
            raise IndexError(f"Dataframe column {col} does not exits.")

        # Get the materialized column.
        column = self._data_.childAt(col)

        # If we attempt to modify a single value in a constant vector column,
        # then we need to transform the column into a value column first.
        if "ConstantVector" in column.__class__.__name__:
            data = {c: self.__data.childAt(c) for c in self.names}
            values = [column[row]] * len(self)
            values[row] = value
            data[col] = values
            self.fromDict(data)
        else:
            column.__setitem__(row, value)

    def __deepcopy__(self, memo) -> "DataFrame":
        """Lazy deep-copy. Lazy DataFrames clone their ``PlanBuilder`` via
        ``cloneWithLogicalPlan`` so the two branches can diverge without
        cross-mutation; eager DataFrames copy their backing ``RowVector``.

        Lazy here means the upstream plan is *not* evaluated at copy
        time — without that, every ``.copy()`` on a mid-pipeline
        DataFrame would force a driver-side materialization and
        subsequent operations would re-source data through a Substrait
        ``virtual_table`` literal embedding.
        """
        newInstance = self.__class__.__new__(self.__class__)
        memo[id(self)] = newInstance
        if isinstance(self.__data, RowVector):
            newInstance.__data = self.__data.copy()
        elif isinstance(self.__data, PlanBuilder):
            newInstance.__data = self.__data.copy()
        else:
            newInstance.__data = None
        newInstance.__executor = self.__executor
        newInstance.__planFactory = self.__planFactory
        newInstance.__formatter = self.__formatter

        return newInstance

    def copy(self) -> "DataFrame":
        """
        Create a deep copy of the existing dataframe.
        """
        return copy.deepcopy(self)

    @property
    def dtype(self) -> RowType:
        if isinstance(self.__data, PlanBuilder):
            return self.__data.dtype()
        elif isinstance(self.__data, RowVector):
            return self.__data.dtype()
        else:
            return None

    @property
    def types(self) -> list[BoltType]:
        """
        Get the bolt type of each individual column.
        """
        rowType = self.dtype
        if rowType is None:
            return []
        return [rowType.childAt(i) for i in range(len(rowType))]

    @property
    def names(self) -> list[str]:
        """
        Get the column names.
        """
        if self.__data is not None:
            return self.dtype.names()
        else:
            return []

    @__evalmethod
    def append(self, rhs: "DataFrame") -> "DataFrame":
        """
        Append (row-wise) a ``DataFrame`` at the end of this ``DataFrame``.
        """
        if rhs.__data is None:
            return self

        if self.__data is None:
            self.__data = rhs.__data
            return self

        self.__checkSchemaCompatibility(rhs, "append")

        if isinstance(rhs.__data, PlanBuilder):
            rhs.__eval()

        self.__data.append(rhs.__data)
        return self

    def __checkSchemaCompatibility(self, rhs: "DataFrame", op: str):
        if self.names != rhs.names:
            raise ValueError(
                f"Cannot {op} `DataFrame` with a different list of names or"
                " different name order."
            )
        if any(lhs != rhs for lhs, rhs in zip(self.types, rhs.types)):
            raise TypeError(f"Cannot {op} `DataFrame` with different column types.")

    @__evalmethod
    def toArrow(self, dtype=pa.StructArray):
        """
        Convert this DataFrame to a pyarrow.Table type.
        """
        result = exportToArrow(self.__data)

        if dtype is pa.StructArray or dtype is pa.Array:
            return result
        if hasattr(dtype, "from_struct_array"):
            return dtype.from_struct_array(result)
        raise TypeError(f"Unsupported DataFrame to arrow type: {dtype}")

    def __repr__(self) -> str:
        typeList: str = ", ".join(
            [n + ": " + str(t) for n, t in zip(self.names, self.types)]
        )
        return f"DataFrame({typeList})"

    def __str__(self) -> str:
        return "\n".join(self.__formatter(self))

    def explain(self) -> str:
        """
        Return a deterministic textual rendering of the current plan,
        including optimizer and dispatcher output.

        Thin wrapper around :meth:`PlanBuilder.explain` that forwards
        this DataFrame's executor.
        """
        return self._planBuilder_.explain(executor=self.__executor)

    @__lazymethod
    def select(
        self,
        columns: Optional[set[str]] = None,
        offset: int = 0,
        count: Optional[int] = None,
    ) -> "DataFrame":
        """
        Subset the `DataFrame` at the current pipeline stage.

        If `columns` is not set, all columns are selected. Otherwise,
        only the columns listed will be selected.

        `offset` is the number of rows to skip at the start of the `DataFrame`.
        `offset` can be a negative number. In that case, it is interpreted
        as the starting point from the last row of the table (before
        limiting to `count` rows).

        `count` is the number of rows to output. If not set, all rows
        after `offset` will be output.
        """
        plan: PlanBuilder = self.__data
        if columns is not None:
            plan.project(
                [NamedExpression(c, FieldExpression(c, self)) for c in list(columns)]
            )
        if offset != 0 or count is not None:
            plan.limit(count, offset)
        return self

    @__lazymethod
    def transform(self, **expressions) -> "DataFrame":
        """
        Update existing columns or create new columns using functions of
        existing columns.

        Keyword arguments **expressions are expected to be as follow:
        The key is the name of the new (or existing) column and the value is
        an object that is assignable to a column.
        Objects assignable to columns are:
        * `int`, `float`, or `str`: will create a column with a constant value.
        * `Expression`: will assign the expression result to the column.
        * `Callable[[DataFrame], Expression]` will call the callable with this
          dataframe and assign the resulting expression to the column.
        """
        # Generate expressions if they were lambdas
        for col, expr in expressions.items():
            if callable(expr):
                e = expr(self)
                if not isinstance(e, Expression):
                    raise TypeError(
                        "Invalid transform function return type."
                        f"Expected an Expression. Got: {type(e)}"
                    )
                expressions[col] = e
        for expr in expressions.values():
            if (
                isinstance(expr, Expression)
                and expr.dataframe is not None
                and expr.dataframe is not self
            ):
                raise ValueError(
                    "Cannot assign an expression from another dataframe."
                    "Maybe use a lambda `lambda df: expr` to capture the "
                    " same dataframe in the expression"
                )
        projectExprs = {
            k: v
            for k, v in expressions.items()
            if (
                isinstance(v, ProjectExpression)
                or isinstance(v, BooleanExpression)
                or isinstance(v, CaseExpression)
            )
        }
        projectExprs |= {
            k: ProjectExpression(LiteralExpression(v))
            for k, v in expressions.items()
            if any((isinstance(v, t) for t in (bool, int, float, str, bytes)))
        }
        if len(projectExprs) > 0:
            self.__assignProjectExpressions(projectExprs)
        aggExprs = {
            k: v for k, v in expressions.items() if isinstance(v, AggregationExpression)
        }
        if len(aggExprs) > 0:
            self.__assignAggregationExpressions(aggExprs)
        return self

    @__lazymethod
    def map(
        self,
        fn: Callable[["DataFrame", ...], "DataFrame"],
        *args,
        **kwargs,
    ) -> "DataFrame":
        """
        Map execute a dataframe function on this dataframe.

        `fn` must be registered with the decorator `@dataframeFunction`.
        `fn` is a Callable function that takes a DataFrame as first argument and
        returns a dataframe.
        Additional arguments in `*args` and `**kwargs` will be forwarded to `fn`
        at execution time.

        This method returns the current dataframe after applying fn.
        """
        if fn not in dataframeFunctions:
            raise ValueError(f"function {fn} is not a registered dataframe function")
        outputType = dataframeFunctions[fn].outputType

        @wraps(fn)
        def userFunctionAdapter(vec: RowVector, *args, **kwargs):
            """
            This function implement the adapter between the user function that
            expects a DataFrame as input (and returns a dataframe) and
            Bolt python Operator that provides a RowVector as input and expects
            the same object as output.
            """
            # The dataframe provided to the user will run locally, i.e where the
            # user function is executed.
            df = DataFrame(vec, executor=LocalExecutor())
            ret = fn(df, *args, **kwargs)
            if not isinstance(ret, DataFrame):
                raise TypeError(
                    f"Function: '{fn.__name__}' must return a"
                    f"DataFrame instance but returned: {type(ret)}"
                )
            # This will materialize the data and return the RowVector that
            # contains it.
            return ret._data_

        self.__data.python(outputType, userFunctionAdapter, *args, **kwargs)
        return self

    def groupBy(self, *columns: list[str]) -> GroupedDataFrame:
        """
        Create an expression on a group of rows.
        `columns` is the list of column name to use for groups.
        """
        return GroupedDataFrame(columns, self)

    @__lazymethod
    def orderBy(
        self, columns: Union[str, list[str], list[tuple[str, SortOrder]]]
    ) -> "DataFrame":
        """
        Order the `DataFrame` rows by `columns` keys.

        `columns` argument supports multiple types:
        * `str`: The `DataFrame` is sorted by the corresponding column name
        in ascending order with null values last.
        * `list[str]`: The `DataFrame` is sorted by the first column name. Ties
        are ordered by the next column name and so on. Rows will be sorted
        in ascending order with null values last.
        * `list[tuple(str, SortOrder)]`: Same as `list[str]` except that you can
        specify the sort order for each column.
        """
        plan: PlanBuilder = self.__data
        columns: list[tuple[str, SortOrder]] = _orderByArg_(columns)
        plan.orderBy([(FieldExpression(c, self), o) for c, o in columns])
        return self

    @__lazymethod
    def shuffle(self, seed: Optional[int] = None) -> "DataFrame":
        """
        Shuffle `DataFrame` rows.
        """
        if seed is None:
            seed = getrandbits(32)
        plan: PlanBuilder = self.__data
        plan.shuffle(seed)
        return self

    @__lazymethod
    def filter(self, expression: BooleanExpression) -> "DataFrame":
        """
        Filter rows based on a predicate.
        """
        if not isinstance(expression, BooleanExpression):
            raise TypeError(f"Invalid filter expression type: {type(expression)}")
        plan: PlanBuilder = self.__data
        plan.filter(expression)
        return self

    @__lazymethod
    def rename(self, **replacement: dict[str, str]) -> "DataFrame":
        """
        Rename columns with name in `replacement` keys with their associated
        values.
        """
        plan: PlanBuilder = self.__data
        proj = [
            NamedExpression(
                n if n not in replacement else replacement[n],
                FieldExpression(n, self),
            )
            for n in self.names
        ]
        plan.project(proj)
        return self

    @__lazymethod
    def join(
        self,
        rhs: "DataFrame",
        keys: Optional[set[str]] = None,
        joinType: JoinType = JoinType.kInner,
    ) -> "DataFrame":
        """
        Join two `DataFrame` objects.

        The join algorithm is a hash join and it will hash the columns matching
        `keys` on each sides of the node to compute the match.
        If no keys are provided, the matching columns names from each
        `DataFrame`s are used.
        """
        if joinType in (JoinType.kLeftSemiFilter, JoinType.kRightSemiFilter):
            raise ValueError(
                f"DataFrame.join does not support {joinType!r} on the public "
                "join API. Express semi/anti semantics via a boolean filter "
                "over the matching key set instead. The substrait optimizer "
                "may still emit SemiFilter internally."
            )
        lhsPlan: PlanBuilder = self.__data
        if not isinstance(rhs.__data, PlanBuilder):
            rhs.__lazy()
        rhsPlan: PlanBuilder = rhs.__data

        # If the user does not provide keys, we use matching keys.
        if keys is None:
            keys = {k for k in self.names if k in rhs.names}

        # Materialize ``keys`` into a deterministic order. The public API
        # accepts a ``set`` for ergonomic reasons, but iteration order on a
        # ``set`` depends on Python's per-process ``PYTHONHASHSEED``. Every
        # downstream artifact derived from this iteration — the join's
        # ``outputLayout``, the per-stage Substrait plans materialized by
        # the dispatcher, the column ordering observed by Bolt's C++ hash
        # join scheduler — would otherwise vary across processes for the
        # same logical query. That nondeterminism is invisible at the row
        # level (joins are set-semantic) but it perturbs the order in which
        # downstream floating-point aggregations accumulate values, so the
        # final ``Sum(...)`` of a wide TPC-H join (e.g. Q5's per-nation
        # revenue) lands on slightly different doubles run-to-run. Sorting
        # the keys here pins the iteration order to a stable schedule that
        # is independent of the hash seed and of the order in which the
        # caller happened to construct the set literal.
        #
        # Iterate ``keysOrder`` everywhere a deterministic sequence is
        # required; ``keys`` itself is preserved as a ``set`` for the
        # subsequent membership tests on column names.
        keysOrder = sorted(keys)

        # Rename columns with prefix to avoid conflicts
        lhsNames = {n: "l_" + n for n in self.names if n not in keys}
        rhsNames = {n: "r_" + n for n in rhs.names if n not in keys}
        lhsKeys = {k: "l_" + k for k in keysOrder}
        rhsKeys = {k: "r_" + k for k in keysOrder}
        self.rename(**(lhsNames | lhsKeys))
        rhs.rename(**(rhsNames | rhsKeys))

        internalJoinType = joinType

        if internalJoinType in (JoinType.kLeftSemiProject, JoinType.kAnti):
            outputLayout = list(lhsKeys.values()) + list(lhsNames.values())
        elif internalJoinType == JoinType.kRightSemiProject:
            outputLayout = list(rhsKeys.values()) + list(rhsNames.values())
        else:
            outputLayout = (
                list(lhsKeys.values())
                + list(lhsNames.values())
                + list(rhsNames.values())
            )

        if internalJoinType in (JoinType.kLeftSemiProject, JoinType.kRightSemiProject):
            outputLayout = [*outputLayout, "match"]

        # Queue Hash Join in the plan:
        lhsPlan.join(
            rhsPlan,
            [FieldExpression(k, self) for k in lhsKeys.values()],
            [FieldExpression(k, rhs) for k in rhsKeys.values()],
            outputLayout=outputLayout,
            joinType=internalJoinType,
        )

        # Remove prefix from columns
        # We only remove the prefix if columns would have different names.
        if internalJoinType == JoinType.kRightSemiProject:
            newNames = {v: k for k, v in rhsKeys.items()}
            newNames |= {v: k for k, v in rhsNames.items()}
        else:
            newNames = {v: k for k, v in lhsKeys.items()}
            newNames |= {v: k for k, v in lhsNames.items() if k not in rhsNames}
            if internalJoinType not in (
                JoinType.kLeftSemiProject,
                JoinType.kAnti,
            ):
                newNames |= {v: k for k, v in rhsNames.items() if k not in lhsNames}
        self.rename(**newNames)
        return self
