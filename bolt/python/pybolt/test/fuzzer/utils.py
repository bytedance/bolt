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

import os
import random
from enum import Enum
from string import ascii_letters, digits
from itertools import chain

from pybolt import (
    ArrayType,
    BigintType,
    BoltType,
    BooleanType,
    DoubleType,
    HugeintType,
    IntegerType,
    MapType,
    RealType,
    RowType,
    SmallintType,
    TinyintType,
    VarbinaryType,
    VarcharType,
)

from ..utils import ColumnGenerator, StringGenerator

_PRIMARY_TYPES = [
    ("c_bool", BooleanType),
    ("c_int", IntegerType),
    ("c_bigint", BigintType),
    ("c_double", DoubleType),
    ("c_varchar", VarcharType),
    ("c_varbinary", VarbinaryType),
]

_COMPLEX_TYPES = [
    ("c_array_int", ArrayType(IntegerType())),
    ("c_array_bool", ArrayType(BooleanType())),
    ("c_map_str_int", MapType(VarcharType(), IntegerType())),
    ("c_map_int_double", MapType(IntegerType(), DoubleType())),
]

_NUMERIC_COLUMNS = ["c_int", "c_bigint", "c_double"]
_BOOLEAN_COLUMNS = ["c_bool"]
_STRING_COLUMNS = ["c_varchar"]
_BINARY_COLUMNS = ["c_varbinary"]
_COMPLEX_COLUMNS = [name for name, _ in _COMPLEX_TYPES]
_SCALAR_COLUMNS = [name for name, _ in _PRIMARY_TYPES]
_GROUPABLE_COLUMNS = ["c_bool", "c_int", "c_bigint", "c_double", "c_varchar"]
_ORDERABLE_COLUMNS = list(_GROUPABLE_COLUMNS)
_JOINABLE_COLUMNS = ["c_bool", "c_int", "c_bigint", "c_double", "c_varchar"]


class ColumnKind(Enum):
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    STRING = "string"
    BINARY = "binary"
    COMPLEX = "complex"

    def isPredicate(self) -> bool:
        return self in (ColumnKind.NUMERIC, ColumnKind.BOOLEAN, ColumnKind.STRING)

    def isScalar(self) -> bool:
        return self in (
            ColumnKind.NUMERIC,
            ColumnKind.BOOLEAN,
            ColumnKind.STRING,
            ColumnKind.BINARY,
        )

    def isGroupable(self) -> bool:
        return self in (ColumnKind.NUMERIC, ColumnKind.BOOLEAN, ColumnKind.STRING)

    def isOrderable(self) -> bool:
        return self.isGroupable()

    def isJoinable(self) -> bool:
        return self.isGroupable()

    @staticmethod
    def getAll() -> list["ColumnKind"]:
        return list(ColumnKind)


def getBaseKind(dtype: BoltType) -> ColumnKind:
    if isinstance(
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
    ):
        return ColumnKind.NUMERIC
    if isinstance(dtype, BooleanType):
        return ColumnKind.BOOLEAN
    if isinstance(dtype, VarcharType):
        return ColumnKind.STRING
    if isinstance(dtype, VarbinaryType):
        return ColumnKind.BINARY
    return ColumnKind.COMPLEX


def _make_base_kinds():
    return {
        name: getBaseKind(dtype() if isinstance(dtype, type) else dtype)
        for name, dtype in [*_PRIMARY_TYPES, *_COMPLEX_TYPES]
    }


def _fuzzer_config():
    seed_env = os.getenv("BOLT_FUZZ_SEED")
    if seed_env is None:
        seed = 1
    else:
        seed = int(seed_env)
        if seed == 0:
            seed = random.SystemRandom().randint(1, (1 << 63) - 1)
    iterations = int(os.getenv("BOLT_FUZZ_ITERS", "1"))
    max_rows = int(os.getenv("BOLT_FUZZ_MAX_ROWS", "25"))
    return seed, iterations, max_rows


def _column_groups(cols: list[str]):
    return {
        "numeric": [c for c in _NUMERIC_COLUMNS if c in cols],
        "boolean": [c for c in _BOOLEAN_COLUMNS if c in cols],
        "string": [c for c in _STRING_COLUMNS if c in cols],
        "binary": [c for c in _BINARY_COLUMNS if c in cols],
        "complex": [c for c in _COMPLEX_COLUMNS if c in cols],
        "scalar": [c for c in _SCALAR_COLUMNS if c in cols],
        "groupable": [c for c in _GROUPABLE_COLUMNS if c in cols],
        "orderable": [c for c in _ORDERABLE_COLUMNS if c in cols],
        "joinable": [c for c in _JOINABLE_COLUMNS if c in cols],
    }


def _single_column_groups(col: str, kind: ColumnKind):
    return {
        "numeric": [col] if kind is ColumnKind.NUMERIC else [],
        "boolean": [col] if kind is ColumnKind.BOOLEAN else [],
        "string": [col] if kind is ColumnKind.STRING else [],
        "binary": [col] if kind is ColumnKind.BINARY else [],
        "complex": [col] if kind is ColumnKind.COMPLEX else [],
        "scalar": [col] if kind.isScalar() else [],
        "groupable": [col] if kind.isGroupable() else [],
        "orderable": [col] if kind.isOrderable() else [],
        "joinable": [col] if kind.isJoinable() else [],
    }


def _kind_groups(column_kinds: dict[str, ColumnKind]):
    return {
        "numeric": [
            name for name, kind in column_kinds.items() if kind is ColumnKind.NUMERIC
        ],
        "boolean": [
            name for name, kind in column_kinds.items() if kind is ColumnKind.BOOLEAN
        ],
        "string": [
            name for name, kind in column_kinds.items() if kind is ColumnKind.STRING
        ],
        "binary": [
            name for name, kind in column_kinds.items() if kind is ColumnKind.BINARY
        ],
        "complex": [
            name for name, kind in column_kinds.items() if kind is ColumnKind.COMPLEX
        ],
        "scalar": [name for name, kind in column_kinds.items() if kind.isScalar()],
        "groupable": [
            name for name, kind in column_kinds.items() if kind.isGroupable()
        ],
        "orderable": [
            name for name, kind in column_kinds.items() if kind.isOrderable()
        ],
        "joinable": [name for name, kind in column_kinds.items() if kind.isJoinable()],
        "predicate": [
            name for name, kind in column_kinds.items() if kind.isPredicate()
        ],
        "all": list(column_kinds.keys()),
    }


def _weighted_choice(rng: random.Random, choices: list[tuple[object, int]]):
    values = [value for value, _ in choices]
    weights = [weight for _, weight in choices]
    return rng.choices(values, weights=weights, k=1)[0]


def _column_output_type(col: str):
    if col == "c_int":
        return IntegerType()
    if col == "c_bigint":
        return BigintType()
    if col == "c_double":
        return DoubleType()
    if col == "c_varchar":
        return VarcharType()
    if col == "c_varbinary":
        return VarbinaryType()
    if col == "c_bool":
        return BooleanType()
    return None


def _numeric_sql_constant(col: str, value):
    if col == "c_double":
        return f"CAST({value} AS DOUBLE)"
    if col == "c_bigint":
        return f"CAST({value} AS BIGINT)"
    return f"CAST({value} AS INTEGER)"


_DEFAULT_STRING_PROJECT_TEMPLATES = (
    "upper({col}) as {alias}",
    "lower({col}) as {alias}",
)


def _build_random_projection(
    rng: random.Random,
    column_kinds: dict[str, ColumnKind],
    alias_prefix: str,
    *,
    numeric_ops: tuple[str, ...] = ("+",),
    string_op_templates: tuple[str, ...] = _DEFAULT_STRING_PROJECT_TEMPLATES,
    boolean_prob: float = 0.5,
    include_complex_fallback: bool = False,
    complex_fallback_prob: float = 0.5,
):
    """Build a random SQL projection list + the resulting column kinds.

    The structure (passthrough → optional numeric → optional string → optional
    boolean → optional complex) is identical for both the aggregation and
    expression fuzzers; only the knobs below differ.

    * ``numeric_ops`` — pool of arithmetic operators for numeric expressions.
      If it has one element, no RNG draw is consumed choosing one.
    * ``string_op_templates`` — ``str.format``-style templates with ``{col}``,
      ``{alias}``, and optionally ``{alias_prefix}`` placeholders.
    * ``boolean_prob`` — probability of adding a boolean passthrough column.
    * ``include_complex_fallback`` — when True, with probability
      ``complex_fallback_prob`` add a complex-typed column as ``{prefix}_complex``.
    """
    groups = _kind_groups(column_kinds)
    projection = []
    new_kinds = {}

    passthrough_count = rng.randint(1, len(column_kinds))
    for name in rng.sample(list(column_kinds.keys()), passthrough_count):
        projection.append(name)
        new_kinds[name] = column_kinds[name]

    if groups["numeric"] and rng.random() < 0.8:
        col = rng.choice(groups["numeric"])
        alias = f"{alias_prefix}_num"
        const = (
            round(rng.uniform(-5, 5), 2) if col == "c_double" else rng.randint(-5, 5)
        )
        op = numeric_ops[0] if len(numeric_ops) == 1 else rng.choice(list(numeric_ops))
        projection.append(f"{col} {op} {_numeric_sql_constant(col, const)} as {alias}")
        new_kinds[alias] = ColumnKind.NUMERIC

    if groups["string"] and rng.random() < 0.6:
        col = rng.choice(groups["string"])
        alias = f"{alias_prefix}_str"
        template = rng.choice(list(string_op_templates))
        projection.append(
            template.format(col=col, alias=alias, alias_prefix=alias_prefix)
        )
        new_kinds[alias] = ColumnKind.STRING

    if groups["boolean"] and rng.random() < boolean_prob:
        col = rng.choice(groups["boolean"])
        alias = f"{alias_prefix}_bool"
        projection.append(f"{col} as {alias}")
        new_kinds[alias] = ColumnKind.BOOLEAN

    if include_complex_fallback and rng.random() < complex_fallback_prob:
        complex_cols = [
            name for name, kind in column_kinds.items() if kind is ColumnKind.COMPLEX
        ]
        if complex_cols:
            col = rng.choice(complex_cols)
            alias = f"{alias_prefix}_complex"
            projection.append(f"{col} as {alias}")
            new_kinds[alias] = ColumnKind.COMPLEX

    return projection, new_kinds


def _random_order_by_keys(
    rng: random.Random,
    column_kinds: dict[str, ColumnKind],
    max_keys: int = 2,
):
    orderable = _kind_groups(column_kinds)["orderable"]
    count = rng.randint(1, min(max_keys, len(orderable)))
    return rng.sample(orderable, count)


def _make_string_project_expr(rng: random.Random, col: str):
    op = rng.choice(["concat", "replace", "lower", "upper"])
    if op == "concat":
        suffix = _random_string(rng, max_len=4)
        expr = f"concat({col}, '{suffix}')"

        def fn(row):
            return row[col] + suffix

    elif op == "replace":
        search = rng.choice(["a", "e", "0", "1"])
        replace = rng.choice(["", "x", "Z"])
        expr = f"replace({col}, '{search}', '{replace}')"

        def fn(row):
            return row[col].replace(search, replace)

    elif op == "lower":
        expr = f"lower({col})"

        def fn(row):
            return row[col].lower()

    else:
        expr = f"upper({col})"

        def fn(row):
            return row[col].upper()

    return expr, fn, VarcharType(), ColumnKind.STRING


def _make_project_expr(rng: random.Random, cols: list[str]):
    groups = _column_groups(cols)
    projectable = []
    if groups["numeric"]:
        projectable.append(ColumnKind.NUMERIC)
    if groups["string"]:
        projectable.append(ColumnKind.STRING)

    kind = rng.choice(projectable)
    if kind is ColumnKind.STRING:
        return _make_string_project_expr(rng, rng.choice(groups["string"]))

    lhs = rng.choice(groups["numeric"])
    op = rng.choice(["plus", "minus", "multiply"])
    const = round(rng.uniform(-5, 5), 2) if lhs == "c_double" else rng.randint(-5, 5)
    const_sql = _numeric_sql_constant(lhs, const)
    if op == "plus":
        expr = f"{lhs} + {const_sql}"

        def fn(row):
            return row[lhs] + const

    elif op == "minus":
        expr = f"{lhs} - {const_sql}"

        def fn(row):
            return row[lhs] - const

    else:
        expr = f"{lhs} * {const_sql}"

        def fn(row):
            return row[lhs] * const

    return expr, fn, _column_output_type(lhs), ColumnKind.NUMERIC


def _make_simple_predicate_expr(rng: random.Random, cols: list[str] | dict[str, str]):
    groups = _kind_groups(cols) if isinstance(cols, dict) else _column_groups(cols)
    predicate_types = []
    if groups["numeric"]:
        predicate_types.append(ColumnKind.NUMERIC)
    if groups["string"]:
        predicate_types.append(ColumnKind.STRING)
    if groups["boolean"]:
        predicate_types.append(ColumnKind.BOOLEAN)

    kind = rng.choice(predicate_types)
    if kind is ColumnKind.NUMERIC:
        ops = ["gt", "lt", "ge", "le", "eq", "ne"]
        op = rng.choice(ops)
        col = rng.choice(groups["numeric"])
        threshold = round(rng.uniform(-10, 10), 2)
        lhs = f"CAST({col} AS DOUBLE)"
        rhs = f"CAST({threshold} AS DOUBLE)"
        if op == "gt":
            expr = f"{lhs} > {rhs}"

            def fn(row):
                return row[col] > threshold

        elif op == "lt":
            expr = f"{lhs} < {rhs}"

            def fn(row):
                return row[col] < threshold

        elif op == "ge":
            expr = f"{lhs} >= {rhs}"

            def fn(row):
                return row[col] >= threshold

        elif op == "le":
            expr = f"{lhs} <= {rhs}"

            def fn(row):
                return row[col] <= threshold

        elif op == "ne":
            expr = f"{lhs} != {rhs}"

            def fn(row):
                return row[col] != threshold

        else:
            expr = f"{lhs} = {rhs}"

            def fn(row):
                return row[col] == threshold

        return expr, fn

    if kind is ColumnKind.STRING:
        col = rng.choice(groups["string"])
        literal = _random_string(rng, max_len=4)
        if rng.random() < 0.5:
            expr = f"{col} = '{literal}'"

            def fn(row):
                return row[col] == literal

        else:
            expr = f"{col} != '{literal}'"

            def fn(row):
                return row[col] != literal

        return expr, fn

    col = rng.choice(groups["boolean"])
    literal = rng.choice([True, False])
    literal_sql = "true" if literal else "false"
    if rng.random() < 0.5:
        expr = f"{col} = {literal_sql}"

        def fn(row):
            return row[col] == literal

    else:
        expr = f"{col} != {literal_sql}"

        def fn(row):
            return row[col] != literal

    return expr, fn


def _make_predicate_expr(rng: random.Random, cols: list[str]):
    expr, fn = _make_simple_predicate_expr(rng, cols)
    if rng.random() < 0.5:
        return expr, fn

    rhs_expr, rhs_fn = _make_simple_predicate_expr(rng, cols)
    if rng.random() < 0.5:
        return (
            f"({expr}) AND ({rhs_expr})",
            lambda row: fn(row) and rhs_fn(row),
        )
    return (
        f"({expr}) OR ({rhs_expr})",
        lambda row: fn(row) or rhs_fn(row),
    )


def _random_string(rng: random.Random, max_len: int = 8):
    return StringGenerator(
        length=max_len, characters=ascii_letters + digits, rng=rng
    ).generate_one()


def _make_random_row_vector(rng: random.Random, num_rows: int, include_extra: bool):
    del include_extra
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
    generator = ColumnGenerator.from_dtype(dtype, rng).with_none_values()
    vec = generator(num_rows)
    data = {name: vec.childAt(name) for name in names}
    return vec, data
