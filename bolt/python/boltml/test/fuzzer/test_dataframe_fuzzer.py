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

import random
import unittest

import pyarrow as pa

from ...dataframe import DataFrame
from ...function.aggregation import All, Any, Count, Max, Mean, Min, Sum
from ...function.dataframe import dataframeFunction

from pybolt import (
    BigintType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    PlanBuilder,
    SortOrder,
)

from pybolt.test.utils import ColumnGenerator

from pybolt.test.fuzzer.utils import (
    _column_groups,
    _fuzzer_config,
    _make_base_kinds,
    _weighted_choice,
)

from .groups import (
    _joined_output_kinds,
    _pipeline_groups,
)
from .plan import (
    _aggregation_sql,
    _assert_same_plan,
    _base_plan,
    _expected_join_plan_from_builders,
    _expected_join_plan_from_frames,
    _rename_projection,
)
from .states import (
    _ChainableOperatorsMixin,
    _DataframePipelineContext,
    _DataframePipelineState,
    _PostAggregationDataframeState,
    _StandardPipelineStateMixin,
    _state_operator,
    _TerminalDataframeState,
    _TerminalTypedState,
    _TypedPipelineContext,
    _TypedPipelineState,
    _TypedPostAggregationState,
)
from .utils import (
    _make_all_type_kinds,
    _make_all_types_dataframe,
    _make_dataframe,
    _make_dataframe_predicate_expr,
    _make_dataframe_project_expr,
)
from ..utils import Runtime, WithDataFrame


def _make_extended_logical_dataframe(
    rng: random.Random,
    num_rows: int,
    runtime: Runtime | None = None,
):
    df, vector, _ = _make_all_types_dataframe(rng, num_rows, runtime)
    return df, vector


_EXTENDED_KINDS = _make_all_type_kinds()


def _random_literal_value(rng: random.Random):
    kind = rng.choice(["bool", "int", "float", "string"])
    if kind == "bool":
        return rng.choice([True, False])
    if kind == "int":
        return rng.randint(-20, 20)
    if kind == "float":
        return round(rng.uniform(-20, 20), 2)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 6)))


_STANDARD_BOLTML_FUZZ_DTYPE = _make_dataframe(random.Random(0), 1)[0].dtype
_FUZZ_RUNTIMES = Runtime.all()


@dataframeFunction(_STANDARD_BOLTML_FUZZ_DTYPE)
def _map_add_numeric(frame, col, delta):
    return frame.transform(**{col: frame[col] + delta})


@dataframeFunction(_STANDARD_BOLTML_FUZZ_DTYPE)
def _map_uppercase_string(frame, col):
    return frame.transform(**{col: frame[col].uppercase()})


class _DataframeChainState(
    _StandardPipelineStateMixin, _ChainableOperatorsMixin, _DataframePipelineState
):
    _terminal_cls = _TerminalDataframeState
    _rename_prefix = "renamed"
    _order_by_max_keys = 1
    _transform_alias_prefix = "out"
    _post_agg_state_cls = _PostAggregationDataframeState

    @_state_operator()
    def _shuffle(self):
        seed_value = self.ctx.rng.randint(1, 1000)
        self.ctx.dataframe.shuffle(seed=seed_value)
        self.ctx.expected_plan.localShuffle(seed_value)
        self.ctx.steps += 1
        return self

    @_state_operator(guard="_can_join")
    def _join(self):
        rhs_df = DataFrame(
            self.ctx.join_data.copy(),
            executor=self.ctx.dataframe._executor_,
            planFactory=self.ctx.dataframe._planFactory_,
        )
        expected_rhs_df = rhs_df.copy()
        rhs_kinds = dict(_make_base_kinds())
        joinable = sorted(
            set(_pipeline_groups(self.ctx.column_kinds)["joinable"]) & set(rhs_df.names)
        )
        join_key = self.ctx.rng.choice(joinable)
        self.ctx.dataframe = self.ctx.dataframe.join(rhs_df, keys={join_key})
        self.ctx.expected_plan = _expected_join_plan_from_builders(
            self.ctx.current_names,
            expected_rhs_df.names,
            self.ctx.expected_plan,
            expected_rhs_df._planBuilder_,
            join_key,
        )
        self.ctx.column_kinds = _joined_output_kinds(
            self.ctx.column_kinds, rhs_kinds, join_key
        )
        self.ctx.current_names = list(self.ctx.dataframe.names)
        self.ctx.joined_once = True
        self.ctx.steps += 1
        return self

    def _can_join(self):
        if self.ctx.joined_once or self.ctx.join_data is None:
            return False
        return bool(
            set(_pipeline_groups(self.ctx.column_kinds)["joinable"])
            & set(self.ctx.join_data.keys())
        )


class _TypedSourceState(
    _StandardPipelineStateMixin, _ChainableOperatorsMixin, _TypedPipelineState
):
    _terminal_cls = _TerminalTypedState
    _rename_prefix = "typed"
    _select_cap = 6
    _select_ensures_groupable = True
    _transform_alias_prefix = "typed_out"
    _post_agg_state_cls = _TypedPostAggregationState


class _TypedJoinState(_StandardPipelineStateMixin, _TypedPipelineState):
    _terminal_cls = _TerminalTypedState
    _rename_prefix = "join"
    _select_cap = 6

    @_state_operator()
    def _limit(self):
        offset = self.ctx.rng.randint(0, max(1, len(self.ctx.current_names)))
        count = self.ctx.rng.randint(1, max(1, len(self.ctx.current_names)))
        self.ctx.dataframe.select(offset=offset, count=count)
        self.ctx.expected_plan.limit(count, offset)
        self.ctx.steps += 1
        return self


class TestDataframeFuzzer(unittest.TestCase):
    @WithDataFrame.factory(
        lambda pl, ex: _make_extended_logical_dataframe(
            random.Random(0), 3, Runtime(pl, ex)
        )[0]
    )
    def test_all_type_registry_coverage(self, df):
        self.assertEqual(set(df.names), set(_EXTENDED_KINDS.keys()))
        self.assertIn("c_varbinary", df.names)
        self.assertIn("c_timestamp", df.names)
        self.assertIn("c_array_int", df.names)
        self.assertIn("c_map_int_double", df.names)

    def test_random_map_with_finite_key_space(self):
        rng = random.Random(0)
        value = ColumnGenerator.from_dtype(
            MapType(BooleanType(), IntegerType()), rng
        ).generate_one()
        self.assertIsNotNone(value)
        self.assertLessEqual(len(value), 2)

    def test_random_transform_literal_and_callable(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 15)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)
            variant = rng.choice(["literal", "callable_numeric", "callable_string"])

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                if variant == "literal":
                    literal = _random_literal_value(rng)
                    df.transform(out=literal)
                    test.assertEqual(list(df["out"]), [literal] * num_rows)
                    return

                if variant == "callable_numeric":
                    col = rng.choice(["c_int", "c_bigint", "c_double"])
                    if col == "c_double":
                        delta = round(rng.uniform(-5, 5), 2)
                    else:
                        delta = rng.randint(-5, 5)
                    df.transform(
                        out=lambda frame, col=col, delta=delta: frame[col] + delta
                    )
                    test.assertEqual(
                        list(df["out"]),
                        [row + delta for row in data[col]],
                    )
                    return

                col = "c_varchar"
                suffix = _random_literal_value(rng)
                if not isinstance(suffix, str):
                    suffix = str(suffix)
                df.transform(
                    out=lambda frame, col=col, suffix=suffix: frame[col].concat(suffix)
                )
                test.assertEqual(
                    list(df["out"]),
                    [row + suffix for row in data[col]],
                )

            run(self)

    def test_random_select_negative_offset_and_sort_order(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 16)
        sort_orders = [
            SortOrder.ASC_NULLS_FIRST,
            SortOrder.ASC_NULLS_LAST,
            SortOrder.DESC_NULLS_FIRST,
            SortOrder.DESC_NULLS_LAST,
        ]
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                df_limited = df.copy()
                orderable_cols = _column_groups(df.names)["orderable"]
                if orderable_cols:
                    selected = rng.sample(
                        orderable_cols, rng.randint(1, min(2, len(orderable_cols)))
                    )
                    sort_spec = [(name, rng.choice(sort_orders)) for name in selected]
                    df.orderBy(sort_spec)
                    expected_plan = _base_plan(data).orderBy(
                        [f"{name} {order}" for name, order in sort_spec], False
                    )
                    _assert_same_plan(test, df, expected_plan)

                offset = -rng.randint(1, num_rows)
                count = rng.randint(1, num_rows)
                df_limited.select(offset=offset, count=count)
                expected_limit_plan = _base_plan(data).limit(offset, count, False)
                _assert_same_plan(test, df_limited, expected_limit_plan)
                test.assertLessEqual(len(df_limited), count)

            run(self)

    def test_random_append(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 17)
        for _ in range(iterations):
            lhs_rows = rng.randint(1, max_rows)
            rhs_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(
                _STANDARD_BOLTML_FUZZ_DTYPE, length=lhs_rows + rhs_rows
            )
            def run(test, df):
                lhs_df = df.copy().select(offset=0, count=lhs_rows)
                rhs_df = df.copy().select(offset=lhs_rows, count=rhs_rows)
                expected = df.copy()
                appended = lhs_df.copy().append(rhs_df)
                test.assertEqual(appended, expected)

            run(self)

    def test_random_map(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 18)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                expected = df.copy()
                choices = [("add_numeric", 1), ("uppercase_string", 1)]
                selected = _weighted_choice(rng, choices)
                if selected == "add_numeric":
                    col = rng.choice(["c_int", "c_bigint", "c_double"])
                    if col == "c_double":
                        delta = round(rng.uniform(-5, 5), 2)
                    else:
                        delta = rng.randint(-5, 5)
                    df.map(_map_add_numeric, col, delta)
                    expected.transform(**{col: expected[col] + delta})
                elif selected == "uppercase_string":
                    col = "c_varchar"
                    df.map(_map_uppercase_string, col)
                    expected.transform(**{col: expected[col].uppercase()})
                test.assertEqual(df, expected)

            run(self)

    def test_random_direct_aggregation(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 19)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                groupable_cols = _column_groups(df.names)["groupable"]
                group_keys = rng.sample(
                    groupable_cols, rng.randint(1, min(2, len(groupable_cols)))
                )
                choices = [("count", lambda: Count(), "agg_value")]
                boolean_cols = _column_groups(df.names)["boolean"]
                numeric_cols = _column_groups(df.names)["numeric"]
                if boolean_cols:
                    choices.extend(
                        [
                            (
                                "all",
                                lambda col=rng.choice(boolean_cols): All(col),
                                "agg_value",
                            ),
                            (
                                "any",
                                lambda col=rng.choice(boolean_cols): Any(col),
                                "agg_value",
                            ),
                        ]
                    )
                if numeric_cols:
                    choices.extend(
                        [
                            (
                                "sum",
                                lambda col=rng.choice(numeric_cols): Sum(col),
                                "agg_value",
                            ),
                            (
                                "mean",
                                lambda col=rng.choice(numeric_cols): Mean(col),
                                "agg_value",
                            ),
                            (
                                "min",
                                lambda col=rng.choice(numeric_cols): Min(col),
                                "agg_value",
                            ),
                            (
                                "max",
                                lambda col=rng.choice(numeric_cols): Max(col),
                                "agg_value",
                            ),
                        ]
                    )
                _, factory, alias = _weighted_choice(
                    rng, [(choice, 1) for choice in choices]
                )
                direct_fn = factory()
                named_fn = factory()
                direct = df.copy().groupBy(*group_keys).aggregate(direct_fn)
                named = df.copy().groupBy(*group_keys).aggregate(**{alias: named_fn})
                test.assertEqual(list(direct), list(named[alias]))

            run(self)

    def test_random_copy_arrow_and_column_ops(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 20)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                choice = _weighted_choice(
                    rng,
                    [
                        ("copy", 1),
                        ("arrow", 1),
                        ("column_rename", 1),
                        ("column_map_expr", 1),
                        ("column_map_callable", 1),
                        ("column_cast", 1),
                    ],
                )
                if choice == "copy":
                    copied = df.copy()
                    test.assertEqual(copied, df)
                    test.assertIsNot(copied, df)
                elif choice == "arrow":
                    table = df.toArrow(pa.Table)
                    roundtrip = DataFrame(
                        table,
                        executor=df._executor_,
                        planFactory=df._planFactory_,
                    )
                    test.assertEqual(roundtrip, df)
                elif choice == "column_rename":
                    original = rng.choice(df.names)
                    renamed = f"col_{original}_{rng.randint(0, 99)}"
                    col = df[original]
                    col.rename(renamed)
                    test.assertEqual(col.name, renamed)
                    test.assertIn(renamed, df.names)
                elif choice == "column_map_expr":
                    numeric_col = rng.choice(["c_int", "c_bigint", "c_double"])
                    delta = (
                        round(rng.uniform(-5, 5), 2)
                        if numeric_col == "c_double"
                        else rng.randint(-5, 5)
                    )
                    df[numeric_col].map(df[numeric_col] + delta)
                    test.assertEqual(
                        list(df[numeric_col]),
                        [value + delta for value in data[numeric_col]],
                    )
                elif choice == "column_map_callable":
                    string_col = "c_varchar"
                    suffix = f"_{rng.randint(0, 9)}"
                    df[string_col].map(lambda col, suffix=suffix: col.concat(suffix))
                    test.assertEqual(
                        list(df[string_col]),
                        [value + suffix for value in data[string_col]],
                    )
                else:
                    cast_choices = [
                        ("c_int", DoubleType()),
                        ("c_int", BigintType()),
                        ("c_bigint", DoubleType()),
                        ("c_bigint", BigintType()),
                        ("c_double", DoubleType()),
                    ]
                    cast_col, target_type = rng.choice(cast_choices)
                    result_col = df[cast_col].cast(target_type)
                    test.assertEqual(result_col.dtype, target_type)

            run(self)

    def test_random_misc_dataframe_apis(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 21)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                choices = [
                    ("from_arrow", 1),
                    ("from_row_vector", 1),
                    ("from_plan_builder", 1),
                    ("getitem", 2),
                    ("setitem", 2),
                    ("metadata", 1),
                ]
                choice = _weighted_choice(rng, choices)

                if choice == "from_arrow":
                    dtype = rng.choice(
                        [pa.Table, pa.RecordBatch, pa.Array, pa.StructArray]
                    )
                    arrow_value = df.toArrow(dtype)
                    rebuilt = DataFrame(
                        executor=df._executor_,
                        planFactory=df._planFactory_,
                    ).fromArrow(arrow_value)
                    test.assertEqual(rebuilt, df)
                    return

                if choice == "from_row_vector":
                    rebuilt = DataFrame(
                        executor=df._executor_,
                        planFactory=df._planFactory_,
                    ).fromRowVector(df._data_)
                    test.assertEqual(rebuilt, df)
                    return

                if choice == "from_plan_builder":
                    lazy_df = DataFrame(
                        executor=df._executor_,
                        planFactory=df._planFactory_,
                    ).fromPlanBuilder(_base_plan(data), df._planFactory_)
                    test.assertEqual(lazy_df, df)
                    return

                if choice == "getitem":
                    variant = _weighted_choice(
                        rng,
                        [
                            ("col_name", 1),
                            ("col_index", 1),
                            ("col_slice", 1),
                            ("row_col_scalar", 1),
                            ("row_slice_cols", 1),
                        ],
                    )
                    if variant == "col_name":
                        name = rng.choice(df.names)
                        test.assertTrue(df[name].equals(data[name]))
                    elif variant == "col_index":
                        idx = rng.randrange(len(df.names))
                        name = df.names[idx]
                        test.assertTrue(df[idx].equals(data[name]))
                    elif variant == "col_slice":
                        start = rng.randint(0, max(0, len(df.names) - 1))
                        stop = rng.randint(start + 1, len(df.names))
                        subset = df[start:stop]
                        expected_names = df.names[start:stop]
                        if len(expected_names) == 1:
                            test.assertEqual(subset.name, expected_names[0])
                        else:
                            test.assertEqual(subset.names, expected_names)
                    elif variant == "row_col_scalar":
                        row = rng.randrange(num_rows)
                        name = rng.choice(df.names)
                        test.assertEqual(df[row, name], data[name][row])
                    else:
                        start = rng.randint(0, max(0, num_rows - 1))
                        stop = rng.randint(start + 1, num_rows)
                        selectable = _column_groups(df.names)["scalar"]
                        selected = rng.sample(
                            selectable, rng.randint(1, min(3, len(selectable)))
                        )
                        subset = df[start:stop, selected]
                        test.assertEqual(subset.names, selected)
                        test.assertEqual(len(subset), stop - start)
                    return

                if choice == "setitem":
                    variant = _weighted_choice(
                        rng,
                        [
                            ("constant", 1),
                            ("expression", 1),
                            ("column_copy", 1),
                            ("element", 1),
                        ],
                    )
                    target = df.copy()
                    if variant == "constant":
                        value = _random_literal_value(rng)
                        target["new_col"] = value
                        test.assertEqual(list(target["new_col"]), [value] * num_rows)
                    elif variant == "expression":
                        col = rng.choice(["c_int", "c_bigint", "c_double"])
                        delta = (
                            round(rng.uniform(-5, 5), 2)
                            if col == "c_double"
                            else rng.randint(-5, 5)
                        )
                        target["expr_col"] = target[col] + delta
                        test.assertEqual(
                            list(target["expr_col"]),
                            [value + delta for value in data[col]],
                        )
                    elif variant == "column_copy":
                        col = rng.choice(target.names)
                        target["copied_col"] = target[col]
                        test.assertTrue(target["copied_col"].equals(data[col]))
                    else:
                        row = rng.randrange(num_rows)
                        col = rng.choice(["c_int", "c_bigint"])
                        value = rng.randint(-100, 100)
                        target[row, col] = value
                        expected = list(data[col])
                        expected[row] = value
                        test.assertEqual(list(target[col]), expected)
                    return

                if choice == "metadata":
                    test.assertEqual(df.names, list(data.names))
                    test.assertEqual(len(df.types), len(df.names))
                    test.assertEqual(df.dtype.names(), df.names)
                    test.assertEqual(len(list(df)), len(df.names))
                    test.assertIn("DataFrame(", repr(df))
                    test.assertIsInstance(str(df), str)
                    return

            run(self)

    def test_random_transform_and_filter(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 1)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)
            for cols in (["c_int"], ["c_bigint"], ["c_double"], ["c_varchar"]):

                @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
                def run(test, df_proj):
                    data = DataFrame(df_proj.copy()._data_)
                    df_filter = df_proj.copy()
                    expr, expr_fn, _ = _make_dataframe_project_expr(df_proj, rng, cols)
                    df_proj.transform(out=expr)
                    expected_proj_plan = _base_plan(data).project(
                        [f"{name} AS {name}" for name in data.names]
                        + [f"{expr.expr()} AS out"]
                    )
                    _assert_same_plan(test, df_proj, expected_proj_plan)
                    expected_out = [
                        expr_fn({c: data[c][idx] for c in data.names})
                        for idx in range(num_rows)
                    ]
                    for name in data.names:
                        test.assertTrue(df_proj[name].equals(data[name]))
                    test.assertEqual(list(df_proj["out"]), expected_out)

                    pred_expr, pred_fn = _make_dataframe_predicate_expr(
                        df_filter, rng, cols
                    )
                    df_filter.filter(pred_expr)
                    expected_filter_plan = _base_plan(data).filter(pred_expr.expr())
                    _assert_same_plan(test, df_filter, expected_filter_plan)
                    expected_filtered = {c: [] for c in data.names}
                    for idx in range(num_rows):
                        row = {c: data[c][idx] for c in data.names}
                        if pred_fn(row):
                            for c in data.names:
                                expected_filtered[c].append(data[c][idx])
                    if all(len(v) == 0 for v in expected_filtered.values()):
                        test.assertEqual(len(df_filter), 0)
                    else:
                        for c in data.names:
                            test.assertEqual(list(df_filter[c]), expected_filtered[c])

                run(self)

    def test_random_select(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 2)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                all_cols = list(data.names)
                df_limited = df.copy()
                if len(all_cols) > 1:
                    num_cols = rng.randint(1, len(all_cols) - 1)
                    selected_cols = rng.sample(all_cols, num_cols)
                    df_selected = df.select(selected_cols)
                    expected_selected_plan = _base_plan(data).project(selected_cols)
                    _assert_same_plan(test, df_selected, expected_selected_plan)
                    test.assertEqual(set(df_selected.names), set(selected_cols))

                offset = rng.randint(0, num_rows)
                count = rng.randint(1, max(1, num_rows - offset))
                df_limited.select(offset=offset, count=count)
                expected_limit_plan = _base_plan(data).limit(offset, count, False)
                _assert_same_plan(test, df_limited, expected_limit_plan)
                test.assertLessEqual(len(df_limited), count)

            run(self)

    def test_random_order_by(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 3)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                orderable_cols = _column_groups(df.names)["orderable"]
                if orderable_cols:
                    sort_col = rng.choice(orderable_cols)
                    df_sorted = df.orderBy(sort_col)
                    expected_sorted_plan = _base_plan(data).orderBy(
                        [f"{sort_col} ASC NULLS LAST"], False
                    )
                    _assert_same_plan(test, df_sorted, expected_sorted_plan)
                    test.assertIsNotNone(df_sorted)

            run(self)

    def test_random_shuffle(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 4)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                seed_value = rng.randint(1, 1000)
                df_shuffled = df.shuffle(seed=seed_value)
                expected_shuffle_plan = _base_plan(data).localShuffle(seed_value)
                _assert_same_plan(test, df_shuffled, expected_shuffle_plan)
                test.assertIsNotNone(df_shuffled)

            run(self)

    def test_random_rename(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 5)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                if len(df.names) > 0:
                    rename_col = rng.choice(df.names)
                    new_name = f"new_{rename_col}"
                    df_renamed = df.rename(**{rename_col: new_name})

                    plan = _base_plan(data)
                    proj = _rename_projection(
                        list(data.names), {rename_col: new_name}, plan
                    )
                    expected_rename_plan = plan.project([p.expr() for p in proj])
                    _assert_same_plan(test, df_renamed, expected_rename_plan)
                    test.assertIn(new_name, df_renamed.names)
                    test.assertNotIn(rename_col, df_renamed.names)

            run(self)

    def test_random_join(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 6)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows * 2)
            def run(test, df):
                df1 = df.copy().select(offset=0, count=num_rows)
                df2 = df.copy().select(offset=num_rows, count=num_rows)
                common_cols = list(
                    set(_column_groups(df1.names)["joinable"])
                    & set(_column_groups(df2.names)["joinable"])
                )
                if common_cols:
                    join_col = rng.choice(common_cols)
                    expected_join_plan = _expected_join_plan_from_frames(
                        df1, df2, join_col
                    )
                    df_joined = df1.join(df2, keys={join_col})
                    _assert_same_plan(test, df_joined, expected_join_plan)
                    test.assertIsNotNone(df_joined)

            run(self)

    def test_random_group_by(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 7)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                groupable_cols = _column_groups(df.names)["groupable"]
                if groupable_cols:
                    group_keys = rng.sample(
                        groupable_cols, rng.randint(1, min(2, len(groupable_cols)))
                    )
                    assignments = {}
                    aggregate_factories = [
                        ("sum_int", lambda: Sum("c_int")),
                        ("mean_double", lambda: Mean("c_double")),
                        ("min_int", lambda: Min("c_int")),
                        ("max_bigint", lambda: Max("c_bigint")),
                        ("count_rows", lambda: Count()),
                        ("all_bool", lambda: All("c_bool")),
                        ("any_bool", lambda: Any("c_bool")),
                    ]
                    for name, factory in rng.sample(
                        aggregate_factories,
                        rng.randint(1, len(aggregate_factories)),
                    ):
                        assignments[name] = factory()
                    fallback_col = data.names[0]
                    grouped_df = df.groupBy(*group_keys).aggregate(**assignments)
                    expected_group_plan = _base_plan(data).singleAggregation(
                        group_keys,
                        [
                            _aggregation_sql(fn, alias, fallback_col)
                            for alias, fn in assignments.items()
                        ],
                        [],
                    )
                    _assert_same_plan(test, grouped_df, expected_group_plan)
                    test.assertIsNotNone(grouped_df)

            run(self)

    def test_random_operator_combination(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 8)
        for _ in range(iterations):
            num_rows = rng.randint(1, max_rows)

            @WithDataFrame.dtype(_STANDARD_BOLTML_FUZZ_DTYPE, length=num_rows)
            def run(test, df):
                data = df.copy()
                join_data = {name: data[name] for name in data.names}
                context = _DataframePipelineContext(
                    rng=rng,
                    dataframe=df,
                    expected_plan=_base_plan(data),
                    current_names=list(data.names),
                    column_kinds=dict(_make_base_kinds()),
                    suffix=str(num_rows),
                    max_steps=rng.randint(3, 7),
                    join_data=join_data,
                )
                state: _DataframePipelineState = _DataframeChainState(context)
                while not state.is_terminal():
                    state = state.next_state()

                _assert_same_plan(test, context.dataframe, context.expected_plan)
                test.assertIsNotNone(context.dataframe)

            run(self)

    def test_random_typed_operator_combination(self):
        seed, iterations, max_rows = _fuzzer_config()
        for runtime in _FUZZ_RUNTIMES:
            with self.subTest(str(runtime)):
                rng = random.Random(seed + 9)
                for _ in range(iterations):
                    num_rows = rng.randint(1, max_rows)
                    df, vector = _make_extended_logical_dataframe(
                        rng, num_rows, runtime
                    )
                    context = _TypedPipelineContext(
                        rng=rng,
                        dataframe=df,
                        expected_plan=PlanBuilder().values([vector]),
                        current_names=list(df.names),
                        column_kinds=dict(_EXTENDED_KINDS),
                        suffix=str(num_rows),
                        max_steps=rng.randint(3, 7),
                    )
                    state: _TypedPipelineState = _TypedSourceState(context)
                    while not state.is_terminal():
                        state = state.next_state()

                    _assert_same_plan(self, context.dataframe, context.expected_plan)

    def test_random_typed_join_operator_combination(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 10)
        for runtime in _FUZZ_RUNTIMES:
            with self.subTest(str(runtime)):
                for _ in range(iterations):
                    num_rows = rng.randint(1, max_rows)
                    lhs_df, lhs_vector = _make_extended_logical_dataframe(
                        rng, num_rows, runtime
                    )
                    rhs_df, rhs_vector = _make_extended_logical_dataframe(
                        rng, num_rows, runtime
                    )

                    join_key = rng.choice(
                        _pipeline_groups(_EXTENDED_KINDS)["groupable"]
                    )
                    expected_plan = _expected_join_plan_from_frames(
                        lhs_df,
                        rhs_df,
                        join_key,
                    )
                    joined_df = lhs_df.join(rhs_df, keys={join_key})

                    current_names = list(joined_df.names)
                    column_kinds = {join_key: _EXTENDED_KINDS[join_key]}
                    column_kinds |= {
                        f"l_{name}": kind
                        for name, kind in _EXTENDED_KINDS.items()
                        if name != join_key
                    }
                    column_kinds |= {
                        f"r_{name}": kind
                        for name, kind in _EXTENDED_KINDS.items()
                        if name != join_key
                    }
                    context = _TypedPipelineContext(
                        rng=rng,
                        dataframe=joined_df,
                        expected_plan=expected_plan,
                        current_names=current_names,
                        column_kinds=column_kinds,
                        suffix=f"join_{num_rows}",
                        max_steps=rng.randint(2, 5),
                    )
                    state: _TypedPipelineState = _TypedJoinState(context)
                    while not state.is_terminal():
                        state = state.next_state()

                    _assert_same_plan(self, context.dataframe, context.expected_plan)


if __name__ == "__main__":
    unittest.main()
