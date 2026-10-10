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
    BigintType,
    BoltTaskExecutor,
    DoubleType,
    IntegerType,
    PlanBuilder,
    fromList,
    rowVector,
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
    _make_project_expr,
    _make_simple_predicate_expr,
    _make_random_row_vector,
    _random_order_by_keys,
)


_BASE_KINDS = _make_base_kinds()


_EXPRESSION_PROJECTION_HOOKS = dict(
    numeric_ops=("+", "-", "*"),
    string_op_templates=(
        "upper({col}) as {alias}",
        "lower({col}) as {alias}",
        "concat({col}, '{alias_prefix}') as {alias}",
    ),
    boolean_prob=0.4,
)


def _random_project_columns(
    rng: random.Random, column_kinds: dict[str, ColumnKind], alias_prefix: str
):
    return _build_random_projection(
        rng, column_kinds, alias_prefix, **_EXPRESSION_PROJECTION_HOOKS
    )


@dataclass
class _ExpressionPipelineContext:
    rng: random.Random
    plan_builder: PlanBuilder
    current_kinds: dict[str, ColumnKind]
    expected_names: list[str]
    num_rows: int
    suffix: str
    max_steps: int
    steps: int = 0


class _ExpressionChainState(_PipelineOperatorsMixin, _PipelineState):
    _project_alias_kind = "state"
    _numeric_project_ops = ("+", "-", "*")
    _string_project_templates = (
        "upper({col}) as {alias}",
        "lower({col}) as {alias}",
        "concat({col}, '{alias_prefix}') as {alias}",
    )
    _project_boolean_prob = 0.4

    def _add_limit(self):
        offset = self.ctx.rng.randint(0, self.ctx.num_rows)
        count = self.ctx.rng.randint(1, max(1, self.ctx.num_rows - offset))
        self.ctx.plan_builder.limit(offset, count, False)
        self.ctx.expected_names = ["Limit", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self

    def _add_shuffle(self):
        self.ctx.plan_builder.localShuffle(self.ctx.rng.randint(1, 1000))
        self.ctx.expected_names = ["LocalShuffle", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self

    def transitions(self):
        if self.ctx.steps >= self.ctx.max_steps:
            return [(self._finish, 1)]

        transitions = [
            (self._finish, 1),
            (self._add_project, 2),
            (self._add_limit, 1),
            (self._add_shuffle, 1),
        ]
        groups = _groups(self.ctx.current_kinds)
        if groups["predicate"]:
            transitions.append((self._add_filter, 1))
        if groups["orderable"]:
            transitions.append((self._add_order_by, 1))
        return transitions


class TestExpressionFuzzer(unittest.TestCase):
    def setUp(self):
        self.executor = BoltTaskExecutor()

    def test_random_project_and_filter(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, data = _make_random_row_vector(rng, num_rows, False)
            col = rng.choice(["c_int", "c_bigint", "c_double"])
            output_type = (
                IntegerType()
                if col == "c_int"
                else BigintType()
                if col == "c_bigint"
                else DoubleType()
            )
            project_expr, project_fn, _, _ = _make_project_expr(rng, [col])
            predicate_expr, predicate_fn = _make_simple_predicate_expr(rng, [col])
            plan = (
                PlanBuilder()
                .values([input_vec])
                .filter(predicate_expr)
                .project([f"{project_expr} as out"])
            )
            _assert_plan_node_names(self, plan, ["Project", "Filter", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            expected = []
            for idx in range(num_rows):
                row = {name: data[name][idx] for name in data.keys()}
                if predicate_fn(row):
                    expected.append(project_fn(row))
            expected_vec = rowVector(["out"], [fromList(expected, output_type)])
            if not expected:
                self.assertIsNone(result)
            else:
                self.assertEqual(result, expected_vec)

    def test_boolean_filter_combination(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 1)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            predicate_expr, _ = _make_predicate_expr(
                rng, ["c_bool", "c_int", "c_double", "c_varchar"]
            )
            plan = (
                PlanBuilder()
                .values([input_vec])
                .filter(predicate_expr)
                .project(["c_bool"])
            )
            _assert_plan_node_names(self, plan, ["Project", "Filter", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            self.assertFalse(isinstance(result, Exception))

    def test_random_order_by(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 4)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            sort_keys = _random_order_by_keys(rng, dict(_BASE_KINDS))
            plan = PlanBuilder().values([input_vec]).orderBy(sort_keys, False)
            _assert_plan_node_names(self, plan, ["OrderBy", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            self.assertIsNotNone(result)

    def test_random_limit(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 5)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            offset = rng.randint(0, num_rows)
            count = rng.randint(1, max(1, num_rows - offset))

            plan = PlanBuilder().values([input_vec]).limit(offset, count, False)
            _assert_plan_node_names(self, plan, ["Limit", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            # Just verify the plan executes without errors
            self.assertFalse(isinstance(result, Exception))

    def test_random_local_shuffle(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 6)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            shuffle_seed = rng.randint(1, 1000)

            plan = PlanBuilder().values([input_vec]).localShuffle(shuffle_seed)
            _assert_plan_node_names(self, plan, ["LocalShuffle", "Values"])
            result = self.executor.execute(plan.planFragment(), f"{self.id()}_{i}")
            # Just verify the plan executes without errors
            self.assertIsNotNone(result)

    def test_random_operator_combination(self):
        (
            seed,
            iterations,
            max_rows,
        ) = _fuzzer_config()
        rng = random.Random(seed + 7)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            context = _ExpressionPipelineContext(
                rng=rng,
                plan_builder=PlanBuilder().values([input_vec]),
                current_kinds=dict(_BASE_KINDS),
                expected_names=["Values"],
                num_rows=num_rows,
                suffix=str(i),
                max_steps=rng.randint(2, 5),
            )
            state: _PipelineState = _ExpressionChainState(context)
            while not state.is_terminal():
                state = state.next_state()

            _assert_plan_node_names(self, context.plan_builder, context.expected_names)
            result = self.executor.execute(
                context.plan_builder.planFragment(), f"{self.id()}_{i}"
            )
            self.assertFalse(isinstance(result, Exception))

    def test_random_deep_operator_combination(self):
        seed, iterations, max_rows = _fuzzer_config()
        rng = random.Random(seed + 8)
        for i in range(iterations):
            num_rows = rng.randint(1, max_rows)
            input_vec, _ = _make_random_row_vector(rng, num_rows, False)
            context = _ExpressionPipelineContext(
                rng=rng,
                plan_builder=PlanBuilder().values([input_vec]),
                current_kinds=dict(_BASE_KINDS),
                expected_names=["Values"],
                num_rows=num_rows,
                suffix=f"deep_{i}",
                max_steps=rng.randint(4, 7),
            )
            state: _PipelineState = _ExpressionChainState(context)
            while not state.is_terminal():
                state = state.next_state()

            _assert_plan_node_names(self, context.plan_builder, context.expected_names)
            result = self.executor.execute(
                context.plan_builder.planFragment(), f"{self.id()}_{i}"
            )
            self.assertFalse(isinstance(result, Exception))


if __name__ == "__main__":
    unittest.main()
