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
from dataclasses import dataclass
from typing import Callable

from boltml.dataframe import DataFrame
from pybolt import PlanBuilder, SortOrder
from pybolt.test.fuzzer.utils import ColumnKind, _weighted_choice, getBaseKind

from .groups import (
    _aggregation_assignments,
    _aggregation_output_kinds,
    _pipeline_groups,
)
from .plan import _aggregation_sql, _rename_projection
from .utils import _make_dataframe_predicate_expr, _make_dataframe_project_expr


def _state_operator(weight: int = 1, guard: str | None = None):
    def decorator(fn):
        fn._boltml_state_operator = {"weight": weight, "guard": guard}
        return fn

    return decorator


def _collect_state_transitions(state) -> list[tuple[Callable, int]]:
    if state.ctx.steps >= state.ctx.max_steps:
        return [(state._finish, 1)]

    transitions = []
    for name in dir(state):
        method = getattr(state, name)
        meta = getattr(
            getattr(method, "__func__", method), "_boltml_state_operator", None
        )
        if meta is None:
            continue
        guard_name = meta["guard"]
        if guard_name is not None and not getattr(state, guard_name)():
            continue
        transitions.append((method, meta["weight"]))
    transitions.sort(key=lambda item: item[0].__name__)
    return transitions


@dataclass
class _DataframePipelineContext:
    rng: random.Random
    dataframe: DataFrame
    expected_plan: PlanBuilder
    current_names: list[str]
    column_kinds: dict[str, ColumnKind]
    suffix: str
    max_steps: int
    join_data: dict | None = None
    joined_once: bool = False
    steps: int = 0


class _DataframePipelineState:
    def __init__(self, ctx: _DataframePipelineContext):
        self.ctx = ctx

    def transitions(self) -> list[tuple[Callable[[], "_DataframePipelineState"], int]]:
        raise NotImplementedError()

    def next_state(self) -> "_DataframePipelineState":
        return _weighted_choice(self.ctx.rng, self.transitions())()

    def is_terminal(self) -> bool:
        return False


class _TerminalDataframeState(_DataframePipelineState):
    def transitions(self):
        return []

    def next_state(self):
        return self

    def is_terminal(self) -> bool:
        return True


@dataclass
class _TypedPipelineContext:
    rng: random.Random
    dataframe: DataFrame
    expected_plan: PlanBuilder
    current_names: list[str]
    column_kinds: dict[str, ColumnKind | str]
    suffix: str
    max_steps: int
    steps: int = 0


class _TypedPipelineState:
    def __init__(self, ctx: _TypedPipelineContext):
        self.ctx = ctx

    def transitions(self) -> list[tuple[Callable[[], "_TypedPipelineState"], int]]:
        raise NotImplementedError()

    def next_state(self) -> "_TypedPipelineState":
        return _weighted_choice(self.ctx.rng, self.transitions())()

    def is_terminal(self) -> bool:
        return False


class _TerminalTypedState(_TypedPipelineState):
    def transitions(self):
        return []

    def next_state(self):
        return self

    def is_terminal(self) -> bool:
        return True


class _StandardPipelineStateMixin:
    """Shared ``_finish`` / ``_select`` / ``_rename`` / ``_order_by`` operators.

    Subclasses set the following class-level hooks:

    * ``_terminal_cls`` — terminal-state class returned from ``_finish``
    * ``_rename_prefix`` — prefix used when synthesizing renamed columns
    * ``_select_cap`` — upper bound on retained columns (``None`` = no cap)
    * ``_select_ensures_groupable`` — if ``True``, appends a groupable column
      when the random sample happened to exclude all of them
    * ``_order_by_max_keys`` — 1 for a single sort column, 2 for up to two
    """

    _terminal_cls: type = None
    _rename_prefix: str = ""
    _select_cap: int | None = None
    _select_ensures_groupable: bool = False
    _order_by_max_keys: int = 2

    @_state_operator()
    def _finish(self):
        return self._terminal_cls(self.ctx)

    @_state_operator()
    def _select(self):
        total = len(self.ctx.current_names)
        cap = total if self._select_cap is None else min(self._select_cap, total)
        selected = self.ctx.rng.sample(
            self.ctx.current_names, self.ctx.rng.randint(1, cap)
        )
        if self._select_ensures_groupable:
            groupable = _pipeline_groups(self.ctx.column_kinds)["groupable"]
            if groupable and not set(selected) & set(groupable):
                selected.append(self.ctx.rng.choice(groupable))
                selected = list(dict.fromkeys(selected))
        self.ctx.dataframe.select(selected)
        expected_projection = (
            selected
            if isinstance(self.ctx.expected_plan, PlanBuilder)
            else _rename_projection(selected, {}, self.ctx.expected_plan)
        )
        self.ctx.expected_plan.project(expected_projection)
        self.ctx.current_names = selected
        self.ctx.column_kinds = {
            name: kind
            for name, kind in self.ctx.column_kinds.items()
            if name in selected
        }
        self.ctx.steps += 1
        return self

    @_state_operator()
    def _rename(self):
        rename_col = self.ctx.rng.choice(self.ctx.current_names)
        new_name = (
            f"{self._rename_prefix}_{self.ctx.suffix}_{self.ctx.steps}_{rename_col}"
        )
        rename_kind = self.ctx.column_kinds.get(
            rename_col, getBaseKind(self.ctx.dataframe[rename_col].dtype)
        )
        self.ctx.dataframe.rename(**{rename_col: new_name})
        rename_projection = _rename_projection(
            self.ctx.current_names, {rename_col: new_name}, self.ctx.expected_plan
        )
        if isinstance(self.ctx.expected_plan, PlanBuilder):
            rename_projection = [proj.expr() for proj in rename_projection]
        self.ctx.expected_plan.project(rename_projection)
        self.ctx.current_names = [
            new_name if name == rename_col else name for name in self.ctx.current_names
        ]
        self.ctx.column_kinds.pop(rename_col, None)
        self.ctx.column_kinds[new_name] = rename_kind
        self.ctx.steps += 1
        return self

    @_state_operator(guard="_can_order_by")
    def _order_by(self):
        orderable = _pipeline_groups(self.ctx.column_kinds)["orderable"]
        if self._order_by_max_keys == 1:
            sort_col = self.ctx.rng.choice(orderable)
            self.ctx.dataframe.orderBy(sort_col)
            self.ctx.expected_plan.orderBy([f"{sort_col} ASC NULLS LAST"], False)
        else:
            count = self.ctx.rng.randint(
                1, min(self._order_by_max_keys, len(orderable))
            )
            order_keys = self.ctx.rng.sample(orderable, count)
            self.ctx.dataframe.orderBy(
                order_keys if len(order_keys) > 1 else order_keys[0]
            )
            if isinstance(self.ctx.expected_plan, PlanBuilder):
                self.ctx.expected_plan.orderBy(
                    [f"{name} {SortOrder.ASC_NULLS_LAST}" for name in order_keys],
                    False,
                )
            else:
                self.ctx.expected_plan.orderBy(
                    [
                        (self.ctx.dataframe[name], SortOrder.ASC_NULLS_LAST)
                        for name in order_keys
                    ]
                )
        self.ctx.steps += 1
        return self

    def _can_order_by(self):
        return bool(_pipeline_groups(self.ctx.column_kinds)["orderable"])

    def transitions(self):
        return _collect_state_transitions(self)


class _ChainableOperatorsMixin:
    """Shared ``_transform`` / ``_filter`` / ``_group_by`` operators.

    Subclasses set the following class-level hooks:

    * ``_transform_alias_prefix`` — prefix for the alias emitted by ``_transform``
    * ``_post_agg_state_cls`` — state class returned after ``_group_by``
    """

    _transform_alias_prefix: str = ""
    _post_agg_state_cls: type = None

    @_state_operator(guard="_can_transform")
    def _transform(self):
        expr, _, kind = _make_dataframe_project_expr(
            self.ctx.dataframe,
            self.ctx.rng,
            _pipeline_groups(self.ctx.column_kinds)["projectable"],
        )
        alias = f"{self._transform_alias_prefix}_{self.ctx.suffix}_{self.ctx.steps}"
        self.ctx.dataframe.transform(**{alias: expr})
        self.ctx.expected_plan.project(
            [f"{name} AS {name}" for name in self.ctx.current_names]
            + [f"{expr.expr()} AS {alias}"]
        )
        self.ctx.current_names = self.ctx.current_names + [alias]
        self.ctx.column_kinds[alias] = kind
        self.ctx.steps += 1
        return self

    @_state_operator(guard="_can_filter")
    def _filter(self):
        pred_expr, _ = _make_dataframe_predicate_expr(
            self.ctx.dataframe,
            self.ctx.rng,
            _pipeline_groups(self.ctx.column_kinds)["predicate"],
        )
        self.ctx.dataframe.filter(pred_expr)
        self.ctx.expected_plan.filter(pred_expr.expr())
        self.ctx.steps += 1
        return self

    @_state_operator(guard="_can_group_by")
    def _group_by(self):
        live_kinds = {
            name: self.ctx.column_kinds[name]
            for name in self.ctx.dataframe.names
            if name in self.ctx.column_kinds
        }
        live_groups = _pipeline_groups(live_kinds)
        groupable = live_groups["groupable"]
        group_keys = self.ctx.rng.sample(
            groupable, self.ctx.rng.randint(1, min(2, len(groupable)))
        )
        assignments = _aggregation_assignments(self.ctx.rng, live_groups)
        self.ctx.dataframe = self.ctx.dataframe.groupBy(*group_keys).aggregate(
            **assignments
        )
        self.ctx.expected_plan.singleAggregation(
            group_keys,
            [
                _aggregation_sql(fn, alias, self.ctx.current_names[0])
                for alias, fn in assignments.items()
            ],
            [],
        )
        self.ctx.column_kinds = _aggregation_output_kinds(
            group_keys, assignments, live_kinds
        )
        self.ctx.current_names = list(self.ctx.dataframe.names)
        self.ctx.steps += 1
        return self._post_agg_state_cls(self.ctx)

    def _can_transform(self):
        return bool(_pipeline_groups(self.ctx.column_kinds)["projectable"])

    def _can_filter(self):
        return bool(_pipeline_groups(self.ctx.column_kinds)["predicate"])

    def _can_group_by(self):
        return bool(_pipeline_groups(self.ctx.column_kinds)["groupable"])


class _PostAggregationStateBase(_StandardPipelineStateMixin):
    """Standard operators plus ``_limit`` for states that run after an
    aggregation has produced the active row shape.
    """

    @_state_operator()
    def _limit(self):
        count = self.ctx.rng.randint(1, 5)
        self.ctx.dataframe.select(offset=0, count=count)
        self.ctx.expected_plan.limit(0, count, False)
        self.ctx.steps += 1
        return self


class _PostAggregationDataframeState(
    _PostAggregationStateBase, _DataframePipelineState
):
    _terminal_cls = _TerminalDataframeState
    _rename_prefix = "agg"
    _select_cap = None


class _TypedPostAggregationState(_PostAggregationStateBase, _TypedPipelineState):
    _terminal_cls = _TerminalTypedState
    _rename_prefix = "post"
    _select_cap = 4
