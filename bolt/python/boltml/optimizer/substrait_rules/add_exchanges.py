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

"""``AddExchanges`` rule — Substrait → Substrait, inserts ``ExchangeRel`` nodes.

This is the Substrait analogue of ``optimizer.physical.PhysicalOptimizerPipeline``.
The legacy pipeline iterates over the **top-level operations chain** of the
``LogicalPlan`` and emits a ``PhysicalExchange`` for every chain entry whose
op kind triggers a repartition; the right-hand subplan of a join is treated
as opaque (its own internal joins/aggregates do not contribute additional
exchanges). The dispatcher consumes that flat exchange list to cut
stages, so the right-hand subplan becomes a single locally-executed stage.

To preserve byte-for-byte structural parity, this rule walks the Substrait
``Rel`` tree along the same notion of a chain: from the root, recurse into
the single input of every single-input rel; when a ``hash_join`` is
encountered, recurse into its **left** input only — the left input is the
chain continuation, while the right input represents the rhs subplan that
the legacy pipeline does not iterate. Both inputs of every chain-encountered
join are wrapped with an ``ExchangeRel``, but the right side's wrapped
subtree is not re-walked; this matches the legacy semantics exactly.

The rules of the chain walk emit one of three exchange kinds:

* ``aggregate`` with at least one grouping expression → wrap its ``input``
  with an ``ExchangeRel`` whose ``scatter_by_fields`` are the grouping
  expressions.
* ``hash_join`` (Bolt's physical join rel kind) → wrap ``left`` and
  ``right`` independently, each with an ``ExchangeRel`` whose
  ``scatter_by_fields`` are the corresponding side's join keys.
* ``extension_single`` whose ``detail.type_url`` identifies the Bolt
  ``LocalShuffleExtension`` (emitted by ``DataFrame.shuffle``) → wrap its
  ``input`` with a ``round_robin`` ``ExchangeRel``.

Aggregates without grouping keys are global aggregates and need no
repartition (single output partition); they are deliberately skipped, just
like ``PhysicalOptimizerPipeline`` does.

Proto representation
--------------------
The rule uses the **native** ``substrait.algebra.ExchangeRel`` (already
present in Bolt's vendored ``algebra.proto`` at field 15 of the
``Rel.rel_type`` oneof). For hash repartitions we populate
``ExchangeRel.scatter_by_fields`` with one ``Expression.FieldReference``
per partitioning column. For shuffles we populate
``ExchangeRel.round_robin`` (the closest spec-defined kind for an
unkeyed re-distribution; the seed is carried out-of-band, see below).

The existing scattering grammar (``"hash(col,col)"`` /
``"shuffle(seed=N)"``) is preserved verbatim by stamping it into
``ExchangeRel.common.hint.alias`` so the dispatcher can read it as the
canonical scattering string. The proto's ``scatter_by_fields`` /
``round_robin`` payload is the spec-conformant duplicate of that
information for downstream Substrait consumers; once the legacy string
grammar is retired the dispatcher can switch to reading the typed
payload directly.

Partition counts come from the rule's ``defaultPartitionCount``
constructor argument, which is plumbed from the
``SubstraitPhysicalOptimizerPipeline`` constructor (mirroring the
``PhysicalOptimizerPipeline(defaultPartitionCount=...)`` knob the engine
already exposes). No partition count is hardcoded inside this module.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from substrait.proto import algebra, plan

from .base import (
    Rule,
    get_rel_kind,
)
from ...substrait.rel import rel_output_names as _output_names


# Single-input ``Rel`` kinds the chain walk forwards through verbatim.
# ``hash_join``, ``aggregate``, ``extension_single`` are handled separately
# because they may insert an ``ExchangeRel``; ``read`` is a leaf; everything
# else listed here just plumbs ``input`` through untouched. Keep aligned
# with the corresponding taxonomy in ``substrait_rules.base``.
_SINGLE_INPUT_CHAIN_KINDS: frozenset[str] = frozenset(
    {
        "filter",
        "project",
        "fetch",
        "sort",
        "expand",
        "write",
    }
)


# Type URL stamped onto the ``ExtensionSingleRel.detail`` ``Any`` payload
# by ``pybolt.createShuffleRel`` (the only producer of this rel kind in
# the BoltML lowering). Verified empirically on the round-trip output of
# ``createShuffleRel`` — the C++ binding sets exactly this string. Used
# here to distinguish a shuffle ``extension_single`` from a Python UDF
# ``extension_single`` (which uses a different type URL).
SHUFFLE_EXTENSION_TYPE_URL: str = (
    "type.googleapis.com/substrait.bolt.LocalShuffleExtension"
)


def _decode_shuffle_seed(payload: bytes) -> int:
    """Extract the ``seed`` int field (proto field 1, varint) from a
    ``LocalShuffleExtension`` Any payload.

    The Bolt extension proto encodes the shuffle seed as a single varint
    field at tag 1. Decoding inline avoids importing the C++ binding from
    a pure-Python optimizer rule and avoids round-tripping through the
    binding for a single integer. An empty payload represents seed ``0``
    (the proto3 default).

    Raises ``ValueError`` if the payload contains a tag other than 1 or
    a wire type other than varint — surfacing any future schema drift in
    the Bolt extension instead of silently ignoring it.
    """
    if not payload:
        return 0
    tag = payload[0]
    field_number = tag >> 3
    wire_type = tag & 0x07
    if field_number != 1 or wire_type != 0:
        raise ValueError(
            "LocalShuffleExtension payload has unexpected leading tag "
            f"0x{tag:02x} (field={field_number}, wire={wire_type}); "
            "expected field 1, varint"
        )
    value = 0
    shift = 0
    i = 1
    while i < len(payload):
        b = payload[i]
        value |= (b & 0x7F) << shift
        i += 1
        if (b & 0x80) == 0:
            return value
        shift += 7
        if shift >= 64:
            raise ValueError("LocalShuffleExtension seed varint exceeds 64 bits")
    raise ValueError("LocalShuffleExtension seed varint truncated")


def _is_shuffle_extension(rel: algebra.Rel) -> bool:
    """Return True iff *rel* is the Bolt ``LocalShuffleExtension`` rel."""
    if get_rel_kind(rel) != "extension_single":
        return False
    return rel.extension_single.detail.type_url == SHUFFLE_EXTENSION_TYPE_URL


def _direct_field_ref(field_index: int) -> algebra.Expression.FieldReference:
    """Build a ``FieldReference`` selecting ``field_index`` of the root struct.

    Mirrors the field-reference shape every other BoltML rule emits
    (root reference + ``DirectReference`` with a ``StructField``). Kept
    in one place here so the rule never reaches into expression-builder
    helpers from other modules.
    """
    seg = algebra.Expression.ReferenceSegment(
        struct_field=algebra.Expression.ReferenceSegment.StructField(
            field=field_index,
        )
    )
    return algebra.Expression.FieldReference(
        direct_reference=seg,
        root_reference=algebra.Expression.FieldReference.RootReference(),
    )


def _grouping_field_indices(agg: algebra.AggregateRel) -> List[int]:
    """Return the field indices of *agg*'s grouping expressions.

    BoltML's ``aggregateRel`` always produces grouping expressions that are
    direct ``FieldReference``s into the input (see ``substrait.rel.aggregateRel``).
    Any non-field grouping expression is currently outside the BoltML
    surface area and would require a richer scattering encoding than the
    legacy ``hash(col,col)`` grammar supports — surfaced as a
    ``NotImplementedError`` rather than silently dropped.
    """
    indices: List[int] = []
    for expr in agg.grouping_expressions:
        idx = _expression_field_index(expr)
        if idx is None:
            raise NotImplementedError(
                "AddExchanges: grouping expression is not a direct field "
                "reference; the legacy hash(col,col) scattering grammar "
                "cannot encode computed grouping keys"
            )
        indices.append(idx)
    return indices


def _expression_field_index(expr: algebra.Expression) -> Optional[int]:
    """Return the struct-field index of *expr* if it is a direct field ref.

    Returns ``None`` for any other expression shape (literal, function
    call, etc.). The ``selection.direct_reference.struct_field.field``
    path mirrors the inverse of ``_direct_field_ref`` above.
    """
    if expr.WhichOneof("rex_type") != "selection":
        return None
    sel = expr.selection
    if not sel.HasField("direct_reference"):
        return None
    if sel.direct_reference.WhichOneof("reference_type") != "struct_field":
        return None
    return sel.direct_reference.struct_field.field


def _key_name(names: Tuple[str, ...], index: int) -> str:
    """Return the column name at *index* (or a stable ``$idx`` fallback).

    The ``$N`` fallback is engaged only when the producer rel did not
    populate ``RelCommon.Hint.output_names`` *and* is not a leaf
    ``ReadRel`` — a shape no part of BoltML's lowering currently emits
    above an aggregate or join input. The fallback exists so the rule is
    total over every Substrait ``Plan`` rather than crashing on a
    plan shape it has never seen; the dispatcher's hash partitioner
    matches on the alias string verbatim, so any stable encoding suffices
    for routing equivalence.
    """
    if 0 <= index < len(names):
        return names[index]
    return f"${index}"


def _make_exchange(
    *,
    input_rel: algebra.Rel,
    field_indices: List[int],
    scattering_alias: str,
    partition_count: int,
    round_robin: bool,
) -> algebra.Rel:
    """Build an ``ExchangeRel`` wrapping *input_rel*.

    Populates the proto-spec-conformant payload (``scatter_by_fields`` for
    a hash repartition, ``round_robin`` for a shuffle) **and** stamps the
    legacy-grammar scattering string into ``RelCommon.Hint.alias`` so the
    dispatcher can read it without re-parsing the proto payload.
    """
    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(alias=scattering_alias),
    )
    exchange = algebra.ExchangeRel(
        common=common,
        input=input_rel,
        partition_count=partition_count,
    )
    if round_robin:
        if field_indices:
            raise ValueError(
                "round-robin exchange must not carry scatter field indices"
            )
        exchange.round_robin.CopyFrom(algebra.ExchangeRel.RoundRobin(exact=False))
    else:
        if not field_indices:
            raise ValueError("hash exchange requires at least one scatter field index")
        exchange.scatter_by_fields.CopyFrom(
            algebra.ExchangeRel.ScatterFields(
                fields=[_direct_field_ref(i) for i in field_indices],
            )
        )
    return algebra.Rel(exchange=exchange)


def _hash_alias(key_names: List[str]) -> str:
    """Render the legacy ``hash(col,col)`` scattering string."""
    return f"hash({','.join(key_names)})"


def _shuffle_alias(seed: int) -> str:
    """Render the legacy ``shuffle(seed=N)`` scattering string."""
    return f"shuffle(seed={seed})"


def _join_key_field_indices(
    join: algebra.HashJoinRel,
) -> Tuple[List[int], List[int]]:
    """Return ``(left_indices, right_indices)`` for a Bolt ``HashJoinRel``.

    BoltML's ``joinRel`` always populates ``ComparisonJoinKey.left`` and
    ``ComparisonJoinKey.right`` as direct ``FieldReference``s on the
    respective sides. Anything else (e.g. an expression-valued comparison
    key) is outside the surface area we lower and is surfaced as a
    ``NotImplementedError`` so the dispatcher does not silently
    receive a partitioning string with wrong column names.
    """
    left_indices: List[int] = []
    right_indices: List[int] = []
    for k in join.keys:
        l_idx = _field_reference_field_index(k.left)
        r_idx = _field_reference_field_index(k.right)
        if l_idx is None or r_idx is None:
            raise NotImplementedError(
                "AddExchanges: HashJoinRel comparison key is not a pair of "
                "direct field references; cannot encode a hash-partitioning "
                "scattering string"
            )
        left_indices.append(l_idx)
        right_indices.append(r_idx)
    return left_indices, right_indices


def _field_reference_field_index(
    ref: algebra.Expression.FieldReference,
) -> Optional[int]:
    """Return the struct-field index of a direct ``FieldReference``."""
    if not ref.HasField("direct_reference"):
        return None
    if ref.direct_reference.WhichOneof("reference_type") != "struct_field":
        return None
    return ref.direct_reference.struct_field.field


class AddExchanges(Rule):
    """Insert ``ExchangeRel`` nodes above repartition-requiring inputs.

    Parameters
    ----------
    defaultPartitionCount:
        Number of output partitions every inserted ``ExchangeRel`` is
        configured with. Mirrors the ``PhysicalOptimizerPipeline``
        constructor argument of the same name; the rule never invents a
        partition count of its own. Coerced to ``max(1, ...)`` so a
        zero/negative count cannot reach the dispatcher.
    """

    name: str = "add_exchanges"

    def __init__(self, defaultPartitionCount: int = 12) -> None:
        self.__partitionCount = max(1, defaultPartitionCount)

    @property
    def partitionCount(self) -> int:
        """Return the partition count this rule stamps onto each exchange."""
        return self.__partitionCount

    def apply(self, p: plan.Plan) -> plan.Plan:
        """Return a fresh ``Plan`` with ``ExchangeRel`` nodes inserted.

        Pure: *p* is not mutated. The walk follows the legacy
        ``PhysicalOptimizerPipeline``'s "iterate the top-level operations
        chain" semantics rather than a generic tree walk — see
        ``_rewrite_chain`` for the precise traversal contract.
        """
        out = plan.Plan()
        out.CopyFrom(p)
        for plan_rel in out.relations:
            if not plan_rel.HasField("root"):
                continue
            if not plan_rel.root.HasField("input"):
                continue
            new_root = self._rewrite_chain(plan_rel.root.input)
            plan_rel.root.input.CopyFrom(new_root)
        return out

    def _rewrite_chain(self, rel: algebra.Rel) -> algebra.Rel:
        """Walk *rel* as a chain root and return a fresh rewritten ``Rel``.

        Chain semantics
        ---------------
        From *rel*, recurse only along the single-input chain (the input
        field of every single-input rel kind, or the ``left`` input of a
        ``hash_join``). The right input of a ``hash_join`` is treated as
        an opaque subplan — it is not re-walked, which mirrors the legacy
        pipeline's iterating-the-top-level-ops-only behaviour. Reads (no
        input) terminate the recursion.

        At each chain node, ``_match`` decides whether the node triggers
        an ``ExchangeRel`` insertion above one or both of its inputs.
        Insertions are spliced over the already-rewritten child(ren), so
        every pre-existing parent-child relationship is preserved while
        the new ``ExchangeRel`` slots in between.
        """
        kind = get_rel_kind(rel)
        if kind is None:
            return rel
        if kind == "read":
            return rel
        if kind == "exchange":
            # Idempotency: an existing ExchangeRel is treated as opaque
            # (its input is whatever the previous rewrite installed).
            new_input = self._rewrite_chain(rel.exchange.input)
            new_exchange = algebra.ExchangeRel()
            new_exchange.CopyFrom(rel.exchange)
            new_exchange.input.CopyFrom(new_input)
            return algebra.Rel(exchange=new_exchange)
        if kind == "hash_join":
            return self._rewrite_join_node(rel)
        if kind == "aggregate":
            return self._rewrite_aggregate_node(rel)
        if kind == "extension_single":
            return self._rewrite_extension_single_node(rel)
        # Generic single-input rel: recurse into input, splice replacement
        # back in. Falls through to the standard taxonomy here so any new
        # single-input Rel kind added to Substrait is forwarded into the
        # chain walk by default; multi-input kinds outside hash_join must
        # be added explicitly so they cannot silently truncate the chain.
        return self._rewrite_single_input_node(rel, kind)

    def _rewrite_single_input_node(
        self,
        rel: algebra.Rel,
        kind: str,
    ) -> algebra.Rel:
        """Recurse into the ``input`` field of a generic single-input rel."""
        if kind not in _SINGLE_INPUT_CHAIN_KINDS:
            raise NotImplementedError(
                f"AddExchanges: unhandled Rel kind {kind!r}; add it to "
                "the SINGLE/JOIN taxonomy in add_exchanges.py"
            )
        inner = getattr(rel, kind)
        if not inner.HasField("input"):
            return rel
        new_input = self._rewrite_chain(inner.input)
        out = algebra.Rel()
        out.CopyFrom(rel)
        getattr(out, kind).input.CopyFrom(new_input)
        return out

    def _rewrite_extension_single_node(
        self,
        rel: algebra.Rel,
    ) -> algebra.Rel:
        """Handle ``extension_single``: shuffle wraps; others just recurse."""
        ext = rel.extension_single
        new_input = self._rewrite_chain(ext.input) if ext.HasField("input") else None
        if not _is_shuffle_extension(rel):
            if new_input is None:
                return rel
            out = algebra.Rel()
            out.CopyFrom(rel)
            out.extension_single.input.CopyFrom(new_input)
            return out
        return self._rewrite_shuffle_with_input(rel, new_input)

    def _rewrite_aggregate_node(self, rel: algebra.Rel) -> algebra.Rel:
        """Handle ``aggregate``: recurse, then wrap if it has grouping keys."""
        agg = rel.aggregate
        new_input = self._rewrite_chain(agg.input) if agg.HasField("input") else None
        if not agg.grouping_expressions:
            if new_input is None:
                return rel
            out_agg = algebra.AggregateRel()
            out_agg.CopyFrom(agg)
            out_agg.input.CopyFrom(new_input)
            return algebra.Rel(aggregate=out_agg)
        if new_input is not None and get_rel_kind(new_input) == "exchange":
            # Idempotency: the chain walk has already wrapped the input.
            out_agg = algebra.AggregateRel()
            out_agg.CopyFrom(agg)
            out_agg.input.CopyFrom(new_input)
            return algebra.Rel(aggregate=out_agg)
        if new_input is None:
            new_input = agg.input
        return self._wrap_aggregate(agg, new_input)

    def _rewrite_join_node(self, rel: algebra.Rel) -> algebra.Rel:
        """Handle ``hash_join``: recurse into left only, then wrap both."""
        join = rel.hash_join
        new_left = (
            self._rewrite_chain(join.left) if join.HasField("left") else join.left
        )
        # The right subplan is intentionally not re-walked — the legacy
        # pipeline does not iterate the rhs's operations, so any nested
        # repartition-inducing op inside the rhs is absorbed into the
        # rhs's single-stage execution.
        right_subplan = join.right
        if (
            get_rel_kind(new_left) == "exchange"
            and get_rel_kind(right_subplan) == "exchange"
        ):
            # Idempotency: another pass already wrapped both sides.
            out_join = algebra.HashJoinRel()
            out_join.CopyFrom(join)
            out_join.left.CopyFrom(new_left)
            return algebra.Rel(hash_join=out_join)
        return self._wrap_hash_join(join, new_left, right_subplan)

    def _wrap_aggregate(
        self,
        agg: algebra.AggregateRel,
        chain_input: algebra.Rel,
    ) -> algebra.Rel:
        """Build a new ``AggregateRel`` whose ``input`` is *chain_input*
        wrapped in a hash ``ExchangeRel`` keyed by *agg*'s grouping fields.

        Caller has already rewritten *chain_input* (the chain continuation
        below the aggregate) and verified *agg* has at least one grouping
        expression and *chain_input* is not itself an ``ExchangeRel``.
        """
        field_indices = _grouping_field_indices(agg)
        input_names = _output_names(chain_input)
        key_names = [_key_name(input_names, i) for i in field_indices]
        new_input = _make_exchange(
            input_rel=chain_input,
            field_indices=field_indices,
            scattering_alias=_hash_alias(key_names),
            partition_count=self.__partitionCount,
            round_robin=False,
        )
        new_agg = algebra.AggregateRel()
        new_agg.CopyFrom(agg)
        new_agg.input.CopyFrom(new_input)
        return algebra.Rel(aggregate=new_agg)

    def _wrap_hash_join(
        self,
        join: algebra.HashJoinRel,
        chain_left: algebra.Rel,
        opaque_right: algebra.Rel,
    ) -> algebra.Rel:
        """Build a new ``HashJoinRel`` whose ``left``/``right`` are wrapped
        in hash ``ExchangeRel``s keyed by the join's left/right keys.

        Caller has rewritten *chain_left* (the chain continuation) and
        passes *opaque_right* (the rhs subplan) verbatim — the rhs is
        intentionally not chain-walked, mirroring legacy semantics.
        """
        left_indices, right_indices = _join_key_field_indices(join)
        if not left_indices:
            # No equality keys → this isn't actually a hash-join, it's
            # a cross product. The previous code claimed to emit two
            # empty ``hash()`` exchanges for byte-equal parity with the
            # legacy pipeline, but ``_make_exchange`` rejects an empty
            # field-index list (``hash exchange requires at least one
            # scatter field index``), so this branch always crashed
            # before reaching the consumer — a latent contradiction
            # the prior review flagged.
            #
            # Reject the construction explicitly here so the planner
            # is forced to route keyless joins through
            # ``NestedLoopJoinRel`` (which doesn't go through this
            # exchange-rewrite at all). If we ever need to support a
            # broadcast-then-cross-product strategy via exchanges,
            # implement it deliberately rather than papering over
            # this code path.
            raise ValueError(
                "_wrap_hash_join: keyless HashJoinRel cannot be partitioned via "
                "hash exchanges (a keyless join is a cross product; route it "
                "through NestedLoopJoinRel or add a broadcast exchange "
                "explicitly). This shape would have crashed downstream in "
                "_make_exchange anyway — the planner should not emit a "
                "HashJoinRel without join keys."
            )
        left_names = _output_names(chain_left)
        right_names = _output_names(opaque_right)
        left_key_names = [_key_name(left_names, i) for i in left_indices]
        right_key_names = [_key_name(right_names, i) for i in right_indices]
        left_alias = _hash_alias(left_key_names)
        right_alias = _hash_alias(right_key_names)
        new_left = _make_exchange(
            input_rel=chain_left,
            field_indices=left_indices,
            scattering_alias=left_alias,
            partition_count=self.__partitionCount,
            round_robin=False,
        )
        new_right = _make_exchange(
            input_rel=opaque_right,
            field_indices=right_indices,
            scattering_alias=right_alias,
            partition_count=self.__partitionCount,
            round_robin=False,
        )
        new_join = algebra.HashJoinRel()
        new_join.CopyFrom(join)
        new_join.left.CopyFrom(new_left)
        new_join.right.CopyFrom(new_right)
        return algebra.Rel(hash_join=new_join)

    def _rewrite_shuffle_with_input(
        self,
        rel: algebra.Rel,
        chain_input: Optional[algebra.Rel],
    ) -> algebra.Rel:
        """Wrap a shuffle ``extension_single`` rel's *chain_input* in a
        round-robin ``ExchangeRel``.

        The shuffle extension itself remains as a (now no-op) pass-through
        whose input is the new ExchangeRel; the C++ converter still treats
        the extension payload as the operator marker, so we cannot drop
        the extension wrapper.
        """
        ext = rel.extension_single
        if chain_input is None:
            return rel
        if get_rel_kind(chain_input) == "exchange":
            new_ext = algebra.ExtensionSingleRel()
            new_ext.CopyFrom(ext)
            new_ext.input.CopyFrom(chain_input)
            return algebra.Rel(extension_single=new_ext)
        seed = _decode_shuffle_seed(ext.detail.value)
        new_input = _make_exchange(
            input_rel=chain_input,
            field_indices=[],
            scattering_alias=_shuffle_alias(seed),
            partition_count=self.__partitionCount,
            round_robin=True,
        )
        new_ext = algebra.ExtensionSingleRel()
        new_ext.CopyFrom(ext)
        new_ext.input.CopyFrom(new_input)
        return algebra.Rel(extension_single=new_ext)
