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

from typing import Callable

from .utils import (
    _build_random_projection,
    _kind_groups,
    _make_simple_predicate_expr,
    _weighted_choice,
)


class _PipelineState:
    def __init__(self, ctx):
        self.ctx = ctx

    def transitions(self) -> list[tuple[Callable[[], "_PipelineState"], int]]:
        raise NotImplementedError()

    def next_state(self) -> "_PipelineState":
        transition = _weighted_choice(self.ctx.rng, self.transitions())
        return transition()

    def is_terminal(self) -> bool:
        return False


class _TerminalState(_PipelineState):
    def transitions(self):
        return []

    def next_state(self):
        return self

    def is_terminal(self) -> bool:
        return True


class _PipelineOperatorsMixin:
    """Shared transition operators.

    Subclasses set the following class-level hooks:

    * ``_terminal_cls`` — terminal-state class returned from ``_finish``
    * ``_project_alias_kind`` — short label used in projected-column aliases
      (``f"{alias_kind}_{suffix}_{steps}"``)
    * ``_numeric_project_ops`` — pool of arithmetic operators for numeric
      columns in ``_add_project``; one-element tuples skip the op RNG draw
    * ``_string_project_templates`` — ``str.format`` templates for string
      projections
    * ``_project_boolean_prob`` — probability of emitting a boolean column
    * ``_project_include_complex`` — whether ``_add_project`` may emit a
      complex passthrough column
    """

    _terminal_cls: type = _TerminalState
    _project_alias_kind: str = "project"
    _numeric_project_ops: tuple[str, ...] = ("+",)
    _string_project_templates: tuple[str, ...] = (
        "upper({col}) as {alias}",
        "lower({col}) as {alias}",
    )
    _project_boolean_prob: float = 0.5
    _project_include_complex: bool = False

    def _finish(self):
        return self._terminal_cls(self.ctx)

    def _add_filter(self):
        predicate_expr, _ = _make_simple_predicate_expr(
            self.ctx.rng,
            {
                name: kind
                for name, kind in self.ctx.current_kinds.items()
                if kind.isPredicate()
            },
        )
        self.ctx.plan_builder.filter(predicate_expr)
        self.ctx.expected_names = ["Filter", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self

    def _add_project(self):
        projection, projected_kinds = _build_random_projection(
            self.ctx.rng,
            self.ctx.current_kinds,
            f"{self._project_alias_kind}_{self.ctx.suffix}_{self.ctx.steps}",
            numeric_ops=self._numeric_project_ops,
            string_op_templates=self._string_project_templates,
            boolean_prob=self._project_boolean_prob,
            include_complex_fallback=self._project_include_complex,
        )
        self.ctx.plan_builder.project(projection)
        self.ctx.current_kinds = projected_kinds
        self.ctx.expected_names = ["Project", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self

    def _add_order_by(self):
        orderable = _kind_groups(self.ctx.current_kinds)["orderable"]
        if not orderable:
            return self
        count = self.ctx.rng.randint(1, min(2, len(orderable)))
        self.ctx.plan_builder.orderBy(self.ctx.rng.sample(orderable, count), False)
        self.ctx.expected_names = ["OrderBy", *self.ctx.expected_names]
        self.ctx.steps += 1
        return self
