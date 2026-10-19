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
from dataclasses import dataclass

from pybolt import (
    ArrayType,
    BooleanType,
    BoltTaskExecutor,
    IntegerType,
    MapType,
    PlanBuilder,
    VarbinaryType,
    VarcharType,
)

from .plan import _assert_plan_node_names
from .states import _PipelineOperatorsMixin, _PipelineState
from .utils import (
    ColumnKind,
    _build_random_projection,
    _fuzzer_config,
    _kind_groups as _groups,
    _make_base_kinds,
    _make_predicate_expr,
    _make_random_row_vector,
    _weighted_choice,
    getBaseKind,
)


_BASE_KINDS = _make_base_kinds()


def _random_grouping_keys(rng: random.Random, column_kinds: dict[str, ColumnKind]):
    groupable = _groups(column_kinds)["groupable"]
    if not groupable:
        return []
    if not _weighted_choice(rng, [(False, 1), (True, 1)]):
        return []

    max_group_keys = min(2, len(groupable))
    num_keys = _weighted_choice(
        rng, [(count, 1) for count in range(1, max_group_keys + 1)]
    )
    return rng.sample(groupable, num_keys)


_AGGREGATION_PROJECTION_HOOKS = dict(
    numeric_ops=("+",),
    boolean_prob=0.5,
    include_complex_fallback=True,
    complex_fallback_prob=0.5,
)


def _random_projection(
    rng: random.Random, column_kinds: dict[str, ColumnKind], prefix: str
):
    return _build_random_projection(
        rng, column_kinds, prefix, **_AGGREGATION_PROJECTION_HOOKS
    )


def _random_aggregate_exprs(rng: random.Random, column_kinds: dict[str, str]):
    groups = _groups(column_kinds)
    choices = []
    if groups["numeric"]:
        choices.extend(["sum", "min", "max", "avg"])
    if groups["boolean"]:
        choices.extend(["bool_and", "bool_or"])
    if groups["all"]:
        choices.append("count")

    aggregates = []
    for idx in range(rng.randint(1, min(6, max(1, len(choices)) + 1))):
        op = rng.choice(choices)
        if op in {"sum", "min", "max", "avg"}:
            col = rng.choice(groups["numeric"])
            alias = f"a_{idx}_{op}_{col}"
            aggregates.append(f"{op}({col}) as {alias}")
        elif op in {"bool_and", "bool_or"}:
            col = rng.choice(groups["boolean"])
            alias = f"a_{idx}_{op}_{col}"
            aggregates.append(f"{op}({col}) as {alias}")
        else:
            col = rng.choice(groups["all"])
            alias = f"a_{idx}_count_{col}"
            aggregates.append(f"count({col}) as {alias}")
    return aggregates


def _aggregate_output_kinds(
    grouping_keys: list[str],
    aggregates: list[str],
    input_kinds: dict[str, ColumnKind] | None = None,
):
    source_kinds = _BASE_KINDS if input_kinds is None else input_kinds
    kinds = {name: source_kinds[name] for name in grouping_keys}
    for expr in aggregates:
        alias = expr.split(" as ")[-1]
        if expr.startswith(("sum(", "count(", "min(", "max(")):
            kinds[alias] = ColumnKind.NUMERIC
        elif expr.startswith("avg("):
            kinds[alias] = ColumnKind.NUMERIC
        else:
            kinds[alias] = ColumnKind.BOOLEAN
    return kinds


@dataclass
class _AggregationPipelineContext:
    rng: random.Random
    plan_builder: PlanBuilder
    current_kinds: dict[str, ColumnKind]
    expected_names: list[str]
    suffix: str
    steps: int = 0


class _PreAggregationState(_PipelineOperatorsMixin, _PipelineState):
    _project_alias_kind = "pre"
    _project_include_complex = True

    def _aggregate(self):
        grouping_keys = _random_grouping_keys(self.ctx.rng, self.ctx.current_kinds)
        aggregates = _random_aggregate_exprs(self.ctx.rng, self.ctx.current_kinds)
        self.ctx.plan_builder.singleAggregation(grouping_keys, aggregates)
        self.ctx.current_kinds = _aggregate_output_kinds(
            grouping_keys, aggregates, self.ctx.current_kinds
        )
        self.ctx.expected_names = ["Aggregation", *self.ctx.expected_names]
        self.ctx.steps = 0
        return _PostAggregationState(self.ctx)

    def transitions(self):
        if self.ctx.steps >= 2:
            return [(self._aggregate, 1)]
        transitions = [(self._aggregate, 2)]
        if any(kind.isPredicate() for kind in self.ctx.current_kinds.values()):
            transitions.append((self._add_filter, 1))
        if self.ctx.current_kinds:
            transitions.append((self._add_project, 1))
        return transitions


class _PostAggregationState(_PipelineOperatorsMixin, _PipelineState):
    def _project_outputs(self):
        current_names = list(self.ctx.current_kinds.keys())
        passthrough = self.ctx.rng.sample(
            current_names, self.ctx.rng.randint(1, len(current_names))
        )
        projection = [f"{name} as {name}" for name in passthrough]
        projected_kinds = {name: self.ctx.current_kinds[name] for name in passthrough}
        numeric_outputs = _groups(self.ctx.current_kinds)["numeric"]
        if numeric_outputs and self.ctx.rng.random() < 0.7:
            col = self.ctx.rng.choice(numeric_outputs)
            alias = f"post_{self.ctx.suffix}_{self.ctx.steps}"
            projection.append(f"CAST({col} AS DOUBLE) + CAST(1 AS DOUBLE) as {alias}")
            projected_kinds[alias] = ColumnKind.NUMERIC
        self.ctx.plan_builder.project(projection)
        self.ctx.current_kinds = projected_kinds
        self.ctx.expected_names = ["Project", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self

    def _limit_outputs(self):
        self.ctx.plan_builder.limit(0, self.ctx.rng.randint(1, 10), False)
        self.ctx.expected_names = ["Limit", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self._finish()

    def transitions(self):
        transitions = [(self._finish, 1)]
        if self.ctx.steps >= 3:
            return transitions
        if self.ctx.current_kinds:
            transitions.append((self._project_outputs, 1))
        if any(kind.isOrderable() for kind in self.ctx.current_kinds.values()):
            transitions.append((self._add_order_by, 1))
        transitions.append((self._limit_outputs, 1))
        return transitions


class TestAggregationFuzzer(unittest.TestCase):
    def setUp(self):
        self.executor = BoltTaskExecutor()

    def test_base_kind_inference(self):
        self.assertEqual(getBaseKind(BooleanType()), ColumnKind.BOOLEAN)
        self.assertEqual(getBaseKind(IntegerType()), ColumnKind.NUMERIC)
        self.assertEqual(getBaseKind(VarcharType()), ColumnKind.STRING)
        self.assertEqual(getBaseKind(VarbinaryType()), ColumnKind.BINARY)
        self.assertEqual(getBaseKind(ArrayType(IntegerType())), ColumnKind.COMPLEX)
        self.assertEqual(
            getBaseKind(MapType(VarcharType(), IntegerType())), ColumnKind.COMPLEX
        )

    def test_random_aggregation(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 3)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            grouping_keys = _random_grouping_keys(rng, dict(_BASE_KINDS))
            aggregates = _random_aggregate_exprs(rng, dict(_BASE_KINDS))

            plan = (
                PlanBuilder()
                .values([input_vec])
                .singleAggregation(grouping_keys, aggregates)
            )
            _assert_plan_node_names(self, plan, ["Aggregation", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            self.assertIsNotNone(result)

    def test_aggregation_with_filter(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 4)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, data = _make_random_row_vector(rng, num_rows, False)
            grouping_keys = _random_grouping_keys(rng, dict(_BASE_KINDS))
            predicate_expr, predicate_fn = _make_predicate_expr(
                rng, ["c_bool", "c_int", "c_double", "c_varchar"]
            )
            aggregates = _random_aggregate_exprs(rng, dict(_BASE_KINDS))

            plan = (
                PlanBuilder()
                .values([input_vec])
                .filter(predicate_expr)
                .singleAggregation(grouping_keys, aggregates)
            )
            _assert_plan_node_names(self, plan, ["Aggregation", "Filter", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            has_matching_rows = any(
                predicate_fn({name: data[name][idx] for name in data.keys()})
                for idx in range(num_rows)
            )
            if not has_matching_rows and grouping_keys:
                self.assertIsNone(result)
            else:
                self.assertIsNotNone(result)

    def test_aggregation_with_project(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 5)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            projection, projected_kinds = _random_projection(
                rng, dict(_BASE_KINDS), f"p{i}"
            )
            grouping_keys = _random_grouping_keys(rng, projected_kinds)
            aggregates = _random_aggregate_exprs(rng, projected_kinds)

            plan = (
                PlanBuilder()
                .values([input_vec])
                .project(projection)
                .singleAggregation(grouping_keys, aggregates)
            )
            _assert_plan_node_names(self, plan, ["Aggregation", "Project", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            self.assertIsNotNone(result)

    def test_aggregation_with_order_by(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 6)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            grouping_keys = _random_grouping_keys(rng, dict(_BASE_KINDS))
            if not grouping_keys:
                grouping_keys = [rng.choice(_groups(dict(_BASE_KINDS))["groupable"])]
            aggregates = _random_aggregate_exprs(rng, dict(_BASE_KINDS))
            plan = (
                PlanBuilder()
                .values([input_vec])
                .singleAggregation(grouping_keys, aggregates)
                .orderBy(
                    rng.sample(grouping_keys, rng.randint(1, len(grouping_keys))), False
                )
            )
            _assert_plan_node_names(self, plan, ["OrderBy", "Aggregation", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            self.assertIsNotNone(result)

    def test_aggregation_operator_combination(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 7)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            context = _AggregationPipelineContext(
                rng=rng,
                plan_builder=PlanBuilder().values([input_vec]),
                current_kinds=dict(_BASE_KINDS),
                expected_names=["Values"],
                suffix=str(i),
            )
            state: _PipelineState = _PreAggregationState(context)
            while not state.is_terminal():
                state = state.next_state()

            _assert_plan_node_names(self, context.plan_builder, context.expected_names)
            result = self.executor.execute(
                context.plan_builder.planFragment(), f"{self.id()}_{i}"
            )
            self.assertFalse(isinstance(result, Exception))

    def test_deep_aggregation_operator_combination(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 8)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            context = _AggregationPipelineContext(
                rng=rng,
                plan_builder=PlanBuilder().values([input_vec]),
                current_kinds=dict(_BASE_KINDS),
                expected_names=["Values"],
                suffix=f"deep_{i}",
            )
            state: _PipelineState = _PreAggregationState(context)
            while not state.is_terminal():
                state = state.next_state()

            _assert_plan_node_names(self, context.plan_builder, context.expected_names)
            result = self.executor.execute(
                context.plan_builder.planFragment(), f"{self.id()}_{i}"
            )
            self.assertFalse(isinstance(result, Exception))


if __name__ == "__main__":
    unittest.main()
