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

"""Shared protocol and ``Rel``-tree traversal for Substrait optimizer rules.

The transform helper here is the only place that knows how to recurse into
``Rel`` children. Every concrete rule expresses a *local* rewrite (a function
from one ``Rel`` to either ``None`` or a replacement ``Rel``) and lets this
module drive the walk. This keeps tree-walking logic in exactly one file —
no copy-paste recursion in each rule.
"""

from __future__ import annotations

from typing import Callable, Optional, Protocol

from substrait.proto import algebra, plan


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class Rule(Protocol):
    """A single Substrait ``Plan -> Plan`` rewrite rule.

    Invariant: ``apply`` must return a fresh ``Plan``. The input ``Plan``
    must not be mutated. Rules that do not match anywhere return a deep
    copy of the input unchanged.
    """

    name: str

    def apply(self, p: plan.Plan) -> plan.Plan:
        """Rewrite *p* and return the new ``Plan``. Pure function: no mutation."""
        ...


# ---------------------------------------------------------------------------
# Rel taxonomy
# ---------------------------------------------------------------------------

# Rel kinds whose proto message has a singular ``input`` field.
_SINGLE_INPUT_KINDS: frozenset[str] = frozenset(
    {
        "filter",
        "project",
        "fetch",
        "aggregate",
        "sort",
        "extension_single",
        "exchange",
        "expand",
        "write",
    }
)

# Rel kinds whose proto message has ``left``/``right`` input fields.
#
# Verified against ``bolt/substrait/proto/substrait/algebra.proto`` (the
# Bolt-vendored Substrait protos) — the ``Rel.rel_type`` oneof declares
# exactly these dual-input rel kinds: ``join`` (logical), ``cross``
# (logical cartesian), and the three physical join variants
# ``hash_join``, ``merge_join``, ``nested_loop_join`` (each defined as
# its own ``*JoinRel`` message with ``Rel left = 2; Rel right = 3``).
# Add new entries only after confirming the proto declares the expected
# ``left``/``right`` fields.
_DUAL_INPUT_KINDS: frozenset[str] = frozenset(
    {"join", "cross", "hash_join", "merge_join", "nested_loop_join"}
)

# Rel kinds whose proto message has a repeated ``inputs`` field.
_MULTI_INPUT_KINDS: frozenset[str] = frozenset({"set"})


def get_rel_kind(rel: algebra.Rel) -> Optional[str]:
    """Return the active oneof field name on a ``Rel`` (e.g. ``"filter"``).

    Returns ``None`` for an empty ``Rel``.
    """
    return rel.WhichOneof("rel_type")


def get_root_rel(p: plan.Plan) -> algebra.Rel:
    """Return the top-level ``Rel`` of the first ``PlanRel`` in *p*.

    Invariant: the caller must have verified ``p.relations`` is non-empty.
    """
    return p.relations[0].root.input


def set_root_rel(p: plan.Plan, new_rel: algebra.Rel) -> None:
    """Replace the top-level ``Rel`` of the first ``PlanRel`` in *p* in place.

    Invariant: ``p.relations`` must be non-empty.
    """
    p.relations[0].root.input.CopyFrom(new_rel)


# ---------------------------------------------------------------------------
# Tree transform
# ---------------------------------------------------------------------------


RelTransformer = Callable[[algebra.Rel], Optional[algebra.Rel]]


def transform_rels(
    rel: algebra.Rel,
    fn: RelTransformer,
    *,
    bottom_up: bool = True,
) -> algebra.Rel:
    """Walk *rel*'s tree, applying *fn* at each node, returning a new ``Rel``.

    *fn* receives the current ``Rel`` and returns either ``None`` (keep this
    node) or a replacement ``Rel``. Children are recursed into first when
    ``bottom_up`` is ``True`` (the default), so a rule sees a tree whose
    children have already been rewritten. Top-down (``bottom_up=False``) is
    used for rules that may rewrite a child *and then* re-examine the parent
    in the same pass.

    Replacement-child contract
    --------------------------
    When *fn* returns a replacement ``Rel``, the replacement is treated as
    already-rewritten and **its children are NOT visited** by this call.
    This is true in both ``bottom_up`` and top-down modes:

    * ``bottom_up=True``: children of the original ``rel`` are visited
      before *fn* runs; if *fn* then returns a replacement, that
      replacement's own children are not re-visited.
    * ``bottom_up=False``: *fn* runs first; if it returns a replacement,
      the children that get visited afterwards are the *replacement's*
      children — but only one level (the recursion does not re-enter the
      replacement node itself).

    A rule that needs the entire replacement subtree re-walked must call
    ``transform_rels`` recursively on the replacement inside its handler
    before returning it. Rules in this package (``MergeProjects``,
    ``PushFilterThroughProjects``, ``RewriteLimits``) all satisfy the
    "replacement is already-rewritten" contract by construction: they
    splice locally-rewritten parents over already-walked children.

    Invariant: the input ``rel`` is never mutated. The returned ``Rel`` is
    always a fresh proto.
    """
    out = algebra.Rel()
    out.CopyFrom(rel)

    if bottom_up:
        _recurse_children(out, fn, bottom_up=bottom_up)

    replacement = fn(out)
    if replacement is not None:
        out = algebra.Rel()
        out.CopyFrom(replacement)

    if not bottom_up:
        _recurse_children(out, fn, bottom_up=bottom_up)

    return out


def _recurse_children(
    rel: algebra.Rel,
    fn: RelTransformer,
    *,
    bottom_up: bool,
) -> None:
    """Mutate *rel* in place by transforming each child via ``transform_rels``.

    *rel* is always a freshly-copied node owned by ``transform_rels``.
    """
    kind = get_rel_kind(rel)
    if kind is None:
        return
    if kind in _SINGLE_INPUT_KINDS:
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            new_child = transform_rels(inner.input, fn, bottom_up=bottom_up)
            inner.input.CopyFrom(new_child)
    elif kind in _DUAL_INPUT_KINDS:
        inner = getattr(rel, kind)
        if inner.HasField("left"):
            new_left = transform_rels(inner.left, fn, bottom_up=bottom_up)
            inner.left.CopyFrom(new_left)
        if inner.HasField("right"):
            new_right = transform_rels(inner.right, fn, bottom_up=bottom_up)
            inner.right.CopyFrom(new_right)
    elif kind in _MULTI_INPUT_KINDS:
        inner = getattr(rel, kind)
        for i in range(len(inner.inputs)):
            new_child = transform_rels(inner.inputs[i], fn, bottom_up=bottom_up)
            inner.inputs[i].CopyFrom(new_child)
    elif kind == "read":
        # Leaf rel: no children to recurse into.
        return
    else:
        # Defensive: a Rel kind we have not classified must surface, not
        # silently pass through. New Rel kinds added to the proto must be
        # categorised in the taxonomy above.
        raise NotImplementedError(
            f"transform_rels: unhandled Rel kind {kind!r}; "
            "add it to the SINGLE/DUAL/MULTI/leaf taxonomy in base.py"
        )


# ---------------------------------------------------------------------------
# Plan-level helper used by every rule.apply
# ---------------------------------------------------------------------------


def rewrite_plan_root(
    p: plan.Plan,
    fn: RelTransformer,
    *,
    bottom_up: bool = True,
) -> plan.Plan:
    """Apply *fn* over the root ``Rel`` of every ``PlanRel`` in *p*.

    Returns a fresh ``Plan``; *p* is not mutated.

    Invariant: at least one of the relations in *p* must be a root relation
    with a populated ``root.input``; otherwise the returned plan is a deep
    copy of *p*.
    """
    out = plan.Plan()
    out.CopyFrom(p)
    for plan_rel in out.relations:
        if not plan_rel.HasField("root"):
            continue
        if not plan_rel.root.HasField("input"):
            continue
        new_root = transform_rels(plan_rel.root.input, fn, bottom_up=bottom_up)
        plan_rel.root.input.CopyFrom(new_root)
    return out
