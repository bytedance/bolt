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

"""Push ``FilterRel`` below an immediate ``ProjectRel`` when safe."""

from __future__ import annotations

from typing import Optional

from substrait.proto import algebra, plan

from ._expression import collect_field_indices, substitute_field_refs
from .base import (
    Rule,
    get_rel_kind,
    rewrite_plan_root,
)


class PushFilterThroughProjects(Rule):
    """Rewrite ``FilterRel(ProjectRel(X))`` to ``ProjectRel(FilterRel(X))``.

    BoltML's ``ProjectRel`` is replacement-style: its output column ``i``
    is exactly ``project.expressions[i]``. The filter sits *above* that
    project, so its field-references address project-output positions. To
    push the filter below the project, every direct field-reference ``i``
    in the filter condition is substituted with ``project.expressions[i]``,
    putting the condition into the project's *input* column space.

    Safety invariant: if any outer-filter field-reference index is outside
    ``range(len(project.expressions))``, the rule skips this site (returns
    ``None``); merging anyway would yield an out-of-range field-ref.

    The rewrite recurses bottom-up, so chains of ``Filter(Project(...))``
    nested below other rels are pushed at every depth in a single pass.
    """

    name: str = "push_filter_through_projects"

    def apply(self, p: plan.Plan) -> plan.Plan:
        """Apply the pushdown bottom-up. Returns a fresh ``Plan``."""
        return rewrite_plan_root(p, _push_one, bottom_up=True)


def _push_one(rel: algebra.Rel) -> Optional[algebra.Rel]:
    if get_rel_kind(rel) != "filter":
        return None
    filt = rel.filter
    if get_rel_kind(filt.input) != "project":
        return None
    proj = filt.input.project

    proj_width = len(proj.expressions)
    valid_range = range(proj_width)

    # Refuse to push if the filter touches a column outside the project's
    # output. (Replacement-style projects make this an alignment violation.)
    for idx in collect_field_indices(filt.condition):
        if idx not in valid_range:
            return None

    def _lookup(idx: int) -> Optional[algebra.Expression]:
        if idx < 0 or idx >= proj_width:
            return None
        return proj.expressions[idx]

    rewritten_cond = substitute_field_refs(filt.condition, _lookup)

    new_filter = algebra.FilterRel()
    new_filter.CopyFrom(filt)
    new_filter.input.CopyFrom(proj.input)
    new_filter.condition.CopyFrom(rewritten_cond)

    new_project = algebra.ProjectRel()
    new_project.CopyFrom(proj)
    new_project.input.CopyFrom(algebra.Rel(filter=new_filter))

    return algebra.Rel(project=new_project)
