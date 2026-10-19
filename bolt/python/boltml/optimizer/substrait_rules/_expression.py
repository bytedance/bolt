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

"""Expression-level helpers shared by Substrait optimizer rules.

These helpers walk a Substrait ``Expression`` proto and either:
  * collect every top-level direct field reference into a set, or
  * substitute each direct field reference with a caller-supplied
    replacement expression.

Field-reference handling is centralised here so no rule does ad-hoc
integer arithmetic on field indices.
"""

from __future__ import annotations

from typing import Callable, Optional

from substrait.proto import algebra


Expression = algebra.Expression


def is_direct_field_ref(expr: Expression) -> bool:
    """Return True iff *expr* is a plain ``selection.direct_reference.struct_field``.

    This is the *only* expression shape the rules treat as "passes a column
    through unchanged". Anything else (cast, scalar function, literal,
    if-then, ...) is considered a computed expression and blocks rules that
    require pure pass-through projections.
    """
    if expr.WhichOneof("rex_type") != "selection":
        return False
    sel = expr.selection
    if not sel.HasField("direct_reference"):
        return False
    ref = sel.direct_reference
    if ref.WhichOneof("reference_type") != "struct_field":
        return False
    # Only top-level (non-nested) struct field references count as a plain
    # pass-through.  A nested ``child`` indicates we are addressing into a
    # struct value.
    return not ref.struct_field.HasField("child")


def direct_field_index(expr: Expression) -> Optional[int]:
    """Return the field index for a plain direct field reference, else ``None``."""
    if not is_direct_field_ref(expr):
        return None
    return expr.selection.direct_reference.struct_field.field


def collect_field_indices(expr: Expression) -> set[int]:
    """Collect every top-level direct-field-reference index referenced by *expr*.

    Walks scalar functions, casts, if-then arms, and singular_or_list /
    nested expressions. Indices appearing inside a *nested* struct child
    reference are **not** collected — those are intra-row addressing and
    irrelevant to the column-flow rewrites.
    """
    indices: set[int] = set()
    _collect(expr, indices)
    return indices


def _collect(expr: Expression, out: set[int]) -> None:
    which = expr.WhichOneof("rex_type")
    if which is None:
        return
    if which == "selection":
        idx = direct_field_index(expr)
        if idx is not None:
            out.add(idx)
        return
    if which == "scalar_function":
        for arg in expr.scalar_function.arguments:
            if arg.WhichOneof("arg_type") == "value":
                _collect(arg.value, out)
        return
    if which == "window_function":
        for arg in expr.window_function.arguments:
            if arg.WhichOneof("arg_type") == "value":
                _collect(arg.value, out)
        return
    if which == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if"):
                _collect(getattr(clause, "if"), out)
            if clause.HasField("then"):
                _collect(clause.then, out)
        if expr.if_then.HasField("else"):
            _collect(getattr(expr.if_then, "else"), out)
        return
    if which == "switch_expression":
        _collect(expr.switch_expression.match, out)
        for clause in expr.switch_expression.ifs:
            if clause.HasField("then"):
                _collect(clause.then, out)
        if expr.switch_expression.HasField("else"):
            _collect(getattr(expr.switch_expression, "else"), out)
        return
    if which == "singular_or_list":
        _collect(expr.singular_or_list.value, out)
        for opt in expr.singular_or_list.options:
            _collect(opt, out)
        return
    if which == "multi_or_list":
        for v in expr.multi_or_list.value:
            _collect(v, out)
        for record in expr.multi_or_list.options:
            for f in record.fields:
                _collect(f, out)
        return
    if which == "cast":
        _collect(expr.cast.input, out)
        return
    if which == "subquery":
        # Subquery scalars/values are independent rels; their field indices
        # are not in our column space.
        return
    if which == "nested":
        nested = expr.nested
        nkind = nested.WhichOneof("nested_type")
        if nkind == "struct":
            for f in nested.struct.fields:
                _collect(f, out)
        elif nkind == "list":
            for v in nested.list.values:
                _collect(v, out)
        elif nkind == "map":
            for kv in nested.map.key_values:
                if kv.HasField("key"):
                    _collect(kv.key, out)
                if kv.HasField("value"):
                    _collect(kv.value, out)
        return
    # literal / enum / dynamic_parameter / etc. — no field refs
    return


# ---------------------------------------------------------------------------
# Substitution
# ---------------------------------------------------------------------------


FieldSubstitutor = Callable[[int], Optional[Expression]]


def substitute_field_refs(
    expr: Expression,
    substitute: FieldSubstitutor,
) -> Expression:
    """Return a new ``Expression`` with every plain field-ref index ``i``
    replaced by ``substitute(i)``.

    Invariants:
      * The input ``expr`` is not mutated; a deep copy is produced.
      * If ``substitute(i)`` returns ``None`` for a referenced index, the
        original field-ref is preserved unchanged at that site.
      * Replacement is non-recursive: replacement expressions are spliced
        in verbatim. Callers that need recursive substitution must compose
        substitutors themselves.
    """
    out = Expression()
    out.CopyFrom(expr)
    _substitute_in_place(out, substitute)
    return out


def _substitute_in_place(expr: Expression, substitute: FieldSubstitutor) -> None:
    which = expr.WhichOneof("rex_type")
    if which is None:
        return
    if which == "selection":
        idx = direct_field_index(expr)
        if idx is None:
            return
        replacement = substitute(idx)
        if replacement is not None:
            expr.Clear()
            expr.MergeFrom(replacement)
        return
    if which == "scalar_function":
        for arg in expr.scalar_function.arguments:
            if arg.WhichOneof("arg_type") == "value":
                _substitute_in_place(arg.value, substitute)
        return
    if which == "window_function":
        for arg in expr.window_function.arguments:
            if arg.WhichOneof("arg_type") == "value":
                _substitute_in_place(arg.value, substitute)
        return
    if which == "if_then":
        for clause in expr.if_then.ifs:
            if clause.HasField("if"):
                _substitute_in_place(getattr(clause, "if"), substitute)
            if clause.HasField("then"):
                _substitute_in_place(clause.then, substitute)
        if expr.if_then.HasField("else"):
            _substitute_in_place(getattr(expr.if_then, "else"), substitute)
        return
    if which == "switch_expression":
        _substitute_in_place(expr.switch_expression.match, substitute)
        for clause in expr.switch_expression.ifs:
            if clause.HasField("then"):
                _substitute_in_place(clause.then, substitute)
        if expr.switch_expression.HasField("else"):
            _substitute_in_place(getattr(expr.switch_expression, "else"), substitute)
        return
    if which == "singular_or_list":
        _substitute_in_place(expr.singular_or_list.value, substitute)
        for opt in expr.singular_or_list.options:
            _substitute_in_place(opt, substitute)
        return
    if which == "cast":
        _substitute_in_place(expr.cast.input, substitute)
        return
    if which == "nested":
        nested = expr.nested
        nkind = nested.WhichOneof("nested_type")
        if nkind == "struct":
            for f in nested.struct.fields:
                _substitute_in_place(f, substitute)
        elif nkind == "list":
            for v in nested.list.values:
                _substitute_in_place(v, substitute)
        elif nkind == "map":
            for kv in nested.map.key_values:
                if kv.HasField("key"):
                    _substitute_in_place(kv.key, substitute)
                if kv.HasField("value"):
                    _substitute_in_place(kv.value, substitute)
        return
    # literal / enum / subquery / etc. — nothing to rewrite
    return
