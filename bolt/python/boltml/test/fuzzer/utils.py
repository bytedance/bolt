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

from itertools import chain
import random
from string import ascii_letters, digits

from boltml.dataframe import DataFrame
from pybolt import (
    ArrayType,
    BigintType,
    BooleanType,
    BoltType,
    DoubleType,
    HugeintType,
    IntegerType,
    MapType,
    RealType,
    RowType,
    SmallintType,
    TimestampType,
    TinyintType,
    VarbinaryType,
    VarcharType,
)
from pybolt.test.fuzzer.utils import (
    ColumnKind,
)

from pybolt.test.utils import ColumnGenerator, StringGenerator

_BOLTML_ALL_TYPE_SCHEMA = [
    ("c_bool", BooleanType(), ColumnKind.BOOLEAN),
    ("c_tinyint", TinyintType(), ColumnKind.NUMERIC),
    ("c_smallint", SmallintType(), ColumnKind.NUMERIC),
    ("c_int", IntegerType(), ColumnKind.NUMERIC),
    ("c_bigint", BigintType(), ColumnKind.NUMERIC),
    ("c_real", RealType(), ColumnKind.NUMERIC),
    ("c_double", DoubleType(), ColumnKind.NUMERIC),
    ("c_timestamp", TimestampType(), "timestamp"),
    ("c_varchar", VarcharType(), ColumnKind.STRING),
    ("c_varbinary", VarbinaryType(), ColumnKind.BINARY),
    ("c_array_int", ArrayType(IntegerType()), ColumnKind.COMPLEX),
    ("c_array_bool", ArrayType(BooleanType()), ColumnKind.COMPLEX),
    ("c_map_str_int", MapType(VarcharType(), IntegerType()), ColumnKind.COMPLEX),
    ("c_map_int_double", MapType(IntegerType(), DoubleType()), ColumnKind.COMPLEX),
]


def _make_all_type_kinds():
    return {name: kind for name, _, kind in _BOLTML_ALL_TYPE_SCHEMA}


def _make_all_types_dataframe(rng: random.Random, num_rows: int, runtime=None):
    names, types, _ = tuple(map(list, zip(*_BOLTML_ALL_TYPE_SCHEMA)))
    dtype = RowType(names, types)
    generator = ColumnGenerator.from_dtype(dtype, rng).with_none_values()
    vector = generator(num_rows)
    data = {name: list(vector.childAt(name)) for name in names}
    if runtime:
        df = DataFrame(
            vector, executor=runtime.executor, planFactory=runtime.planFactory
        )
    else:
        df = DataFrame(vector)
    return df, vector, data


def _dataframe_column_groups(df: DataFrame, cols: list[str]):
    groups = {
        "numeric": [],
        "boolean": [],
        "string": [],
        "binary": [],
        "complex": [],
        "scalar": [],
        "groupable": [],
        "orderable": [],
        "joinable": [],
    }
    for col in cols:
        dtype = df[col].dtype
        is_numeric = isinstance(
            dtype,
            (
                IntegerType,
                BigintType,
                HugeintType,
                DoubleType,
                RealType,
                SmallintType,
                TinyintType,
            ),
        )
        is_boolean = isinstance(dtype, BooleanType)
        is_string = isinstance(dtype, VarcharType)
        is_binary = isinstance(dtype, VarbinaryType)
        is_orderable = (
            is_numeric or is_boolean or is_string or isinstance(dtype, TimestampType)
        )
        if is_numeric:
            groups["numeric"].append(col)
        elif is_boolean:
            groups["boolean"].append(col)
        elif is_string:
            groups["string"].append(col)
        elif is_binary:
            groups["binary"].append(col)
        else:
            groups["complex"].append(col)
        if is_numeric or is_boolean or is_string or is_binary:
            groups["scalar"].append(col)
        if is_orderable:
            groups["groupable"].append(col)
            groups["orderable"].append(col)
            groups["joinable"].append(col)
    return groups


def _numeric_literal_for_dtype(rng: random.Random, dtype):
    if isinstance(dtype, (DoubleType, RealType)):
        return round(rng.uniform(-10, 10), 2)
    return rng.randint(-10, 10)


def _random_string(rng: random.Random, max_len: int = 8):
    return StringGenerator(
        length=max_len, characters=ascii_letters + digits, rng=rng
    ).generate_one()


def _make_dataframe(rng: random.Random, num_rows: int, runtime=None):
    from pybolt.test.fuzzer.utils import _COMPLEX_TYPES, _PRIMARY_TYPES

    names = []
    types = []
    for col_name, col_type in chain(_PRIMARY_TYPES, _COMPLEX_TYPES):
        names.append(col_name)
        try:
            if issubclass(col_type, BoltType):
                col_type = col_type()
        except TypeError:
            pass
        if not isinstance(col_type, BoltType):
            raise ValueError(f"col_type is not a BoltType instance: {type(col_type)}")
        types.append(col_type)

    dtype = RowType(names, types)
    generator = ColumnGenerator.from_dtype(dtype, rng)
    vec = generator(num_rows)
    data = {name: list(vec.childAt(name)) for name in names}
    if runtime:
        df = DataFrame(data, executor=runtime.executor, planFactory=runtime.planFactory)
    else:
        df = DataFrame(data)
    return df, data


def _make_dataframe_project_expr(df: DataFrame, rng: random.Random, cols: list[str]):
    groups = _dataframe_column_groups(df, cols)
    projectable = []
    if groups["numeric"]:
        projectable.append(ColumnKind.NUMERIC)
    if groups["string"]:
        projectable.append(ColumnKind.STRING)

    kind = rng.choice(projectable)
    if kind is ColumnKind.STRING:
        col = rng.choice(groups["string"])
        op = rng.choice(["concat", "replace", "lower", "upper"])
        if op == "concat":
            suffix = _random_string(rng, max_len=4)
            expr = df[col].concat(suffix)

            def fn(row):
                return row[col] + suffix

        elif op == "replace":
            search = rng.choice(["a", "e", "0", "1"])
            replace = rng.choice(["", "x", "Z"])
            expr = df[col].searchReplace(search, replace)

            def fn(row):
                return row[col].replace(search, replace)

        elif op == "lower":
            expr = df[col].lowercase()

            def fn(row):
                return row[col].lower()

        else:
            expr = df[col].uppercase()

            def fn(row):
                return row[col].upper()

        return expr, fn, ColumnKind.STRING

    lhs = rng.choice(groups["numeric"])
    numeric_ops = ["plus", "minus", "multiply"]
    if isinstance(df[lhs].dtype, (DoubleType, RealType)):
        numeric_ops.append("divide")
        numeric_ops.append("isnan")
    op = rng.choice(numeric_ops)
    const = _numeric_literal_for_dtype(rng, df[lhs].dtype)
    lhs_dtype = df[lhs].dtype
    # Bolt's integer ``plus``/``minus``/``multiply`` are two-s-complement
    # operations on the fixed-width native type; ``c_bigint * 7`` for a
    # ``c_bigint`` value near 2^61 wraps modulo 2^64 in Bolt but produces
    # an arbitrary-precision Python ``int`` here. Without aligning the
    # two, the fuzzer's ``expected_proj`` (built from ``fn(row)`` output
    # via ``pybolt.fromList``) rejects the out-of-range Python int with
    # ``Unable to cast Python instance of type <class 'int'> to C++ type
    # '?'`` and the assertion against Bolt's actual result never even
    # gets to run. Wrap each integer ``fn`` result to the host bolt-int
    # width so the expected vector matches Bolt's actual computation.
    _int_widths = {
        TinyintType: 8,
        SmallintType: 16,
        IntegerType: 32,
        BigintType: 64,
    }
    _int_width = None
    for _int_type, _w in _int_widths.items():
        if isinstance(lhs_dtype, _int_type):
            _int_width = _w
            break

    def _wrap_int_if_needed(value):
        if _int_width is None or not isinstance(value, int) or isinstance(value, bool):
            return value
        modulo = 1 << _int_width
        wrapped = ((value + (modulo >> 1)) % modulo) - (modulo >> 1)
        return wrapped

    if op == "plus":
        expr = df[lhs] + const

        def fn(row):
            return _wrap_int_if_needed(row[lhs] + const)

    elif op == "minus":
        expr = df[lhs] - const

        def fn(row):
            return _wrap_int_if_needed(row[lhs] - const)

    elif op == "multiply":
        expr = df[lhs] * const

        def fn(row):
            return _wrap_int_if_needed(row[lhs] * const)

    elif op == "divide":
        if const == 0:
            const = 1.0 if isinstance(df[lhs].dtype, (DoubleType, RealType)) else 1
        expr = df[lhs] / const

        def fn(row):
            return row[lhs] / const

    else:
        expr = df[lhs].isNan()

        def fn(row):
            return row[lhs] != row[lhs]

        return expr, fn, ColumnKind.BOOLEAN

    return expr, fn, ColumnKind.NUMERIC


def _make_atomic_dataframe_predicate_expr(
    df: DataFrame,
    rng: random.Random,
    cols: list[str],
):
    groups = _dataframe_column_groups(df, cols)
    predicate_types = []
    if groups["numeric"]:
        predicate_types.append(ColumnKind.NUMERIC)
    if groups["string"]:
        predicate_types.append(ColumnKind.STRING)

    kind = rng.choice(predicate_types)
    if kind is ColumnKind.STRING:
        col = rng.choice(groups["string"])
        literal = _random_string(rng, max_len=4)
        if rng.random() < 0.5:
            expr = df[col] == literal

            def fn(row):
                return row[col] == literal

        else:
            expr = df[col] != literal

            def fn(row):
                return row[col] != literal
    else:
        op = rng.choice(["gt", "lt", "ge", "le", "eq", "ne"])
        col = rng.choice(groups["numeric"])
        threshold = _numeric_literal_for_dtype(rng, df[col].dtype)
        if op == "lt":
            expr = df[col] < threshold

            def fn(row):
                return row[col] < threshold

        elif op == "gt":
            expr = df[col] > threshold

            def fn(row):
                return row[col] > threshold

        elif op == "le":
            expr = df[col] <= threshold

            def fn(row):
                return row[col] <= threshold

        elif op == "ge":
            expr = df[col] >= threshold

            def fn(row):
                return row[col] >= threshold

        elif op == "ne":
            expr = df[col] != threshold

            def fn(row):
                return row[col] != threshold

        else:
            expr = df[col] == threshold

            def fn(row):
                return row[col] == threshold

    return expr, fn


def _make_dataframe_predicate_expr(
    df: DataFrame,
    rng: random.Random,
    cols: list[str],
    allow_compound: bool = True,
):
    expr, fn = _make_atomic_dataframe_predicate_expr(df, rng, cols)

    while allow_compound and rng.random() >= 0.5:
        rhs_expr, rhs_fn = _make_atomic_dataframe_predicate_expr(df, rng, cols)
        lhs_fn = fn
        if rng.random() < 0.5:
            expr = expr & rhs_expr

            def fn(row, lhs_fn=lhs_fn, rhs_fn=rhs_fn):
                return lhs_fn(row) and rhs_fn(row)
        else:
            expr = expr | rhs_expr

            def fn(row, lhs_fn=lhs_fn, rhs_fn=rhs_fn):
                return lhs_fn(row) or rhs_fn(row)

    return expr, fn
