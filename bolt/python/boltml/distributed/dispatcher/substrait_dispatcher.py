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

"""Substrait-Plan-consuming dispatcher.

Walks a ``substrait.plan.Plan`` whose physical optimizer pipeline has
already inserted ``ExchangeRel`` nodes (see
``optimizer.substrait_rules.add_exchanges``) and produces the same
``StageDAG`` shape that ``PhysicalPlanDispatcher`` produces from a
legacy ``PhysicalPlan``.

Each ``ExchangeRel`` becomes a stage cut: the subtree below the exchange
is serialized as the producer stage's Substrait plan; the parent rel's
reference to the ``ExchangeRel`` is replaced with a placeholder
``ReadRel`` that the remote worker re-reads from the spilled exchange
parquet at execution time.

The encoding contract for each ``ExchangeRel`` matches the
``AddExchanges`` rule's output:

* ``exchange.common.hint.alias`` carries the legacy scattering string
  (``"hash(col,col)"`` or ``"shuffle(seed=N)"``) verbatim.
* ``exchange.partition_count`` is the partition count.
* ``exchange.input`` is the producer-side subtree.

Output stage IDs follow the same ``stage-{idx}`` convention as the
legacy dispatcher and are sorted by index for stable test parity.
"""

from __future__ import annotations

from typing import Callable, List, Tuple

from substrait.proto import algebra, plan as plan_pb2, type as type_pb2

from ...substrait.rel import BOLTML_HINT_PROVIDING_REL_KINDS
from ...substrait.rel import rel_output_names

from .base import Dispatcher
from .placeholders import PlaceholderRead
from .stage_dag import StageDAG, StageNode


def _producer_output_names(rel: algebra.Rel) -> Tuple[str, ...]:
    """Return ``rel_output_names(rel)`` and assert non-emptiness for
    rel kinds the BoltML lowering is expected to populate.

    The fail-loud set is ``BOLTML_HINT_PROVIDING_REL_KINDS`` from
    ``substrait.rel`` — the canonical registry of which rel-builder
    functions populate ``RelCommon.Hint.output_names``, declared
    next to the builders themselves so it can't drift from the
    lowering. An empty result for one of those kinds means an
    optimizer rule dropped the hint; surface it rather than silently
    emitting a ``PlaceholderRead`` with unverifiable names.

    The transparent-recurse kinds (``filter`` / ``fetch`` / ``sort``
    / ``extension_single``) are not in the set because their output
    schema delegates to their input, which ``rel_output_names``
    resolves. ``read`` is also not in the set: the dispatcher's own
    placeholder ``ReadRel`` (substituted in place of an exchange
    cut) intentionally carries an empty ``base_schema`` and must
    not trip the check.
    """
    names = rel_output_names(rel)
    if names:
        return names
    kind = rel.WhichOneof("rel_type")
    if kind in BOLTML_HINT_PROVIDING_REL_KINDS:
        raise RuntimeError(
            f"SubstraitPlanDispatcher: producer subtree of kind {kind!r} "
            "has no output names. Every BoltML-emitted "
            f"{kind} rel populates RelCommon.Hint.output_names; an "
            "empty result here means an optimizer rule dropped the "
            "hint. Surface it rather than silently emitting a "
            "PlaceholderRead with unknown names."
        )
    return names


# ``Rel`` kinds whose input(s) the cut walk recurses into. ``read`` and
# ``exchange`` are handled out of band (read = leaf, exchange = boundary).
# Multi-input kinds outside this set must be added explicitly so the cut
# walk cannot silently truncate a chain.
_SINGLE_INPUT_RECURSE_KINDS: frozenset[str] = frozenset(
    {
        "filter",
        "project",
        "aggregate",
        "fetch",
        "sort",
        "expand",
        "write",
        "extension_single",
    }
)

_TWO_INPUT_RECURSE_KINDS: frozenset[str] = frozenset(
    {
        "join",
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "cross",
    }
)


def _build_placeholder_read(exchange_id: str) -> algebra.Rel:
    """Build the placeholder ``ReadRel`` substituted in place of an exchange.

    Uses ``named_table`` with the exchange identifier so the substituted
    rel is a well-formed ``ReadRel`` (not a no-input rel that would fail
    Substrait validation in downstream consumers). The remote worker's
    ``_traverse_and_replace_read`` is keyed off ``rel_type == "read"``
    and does not inspect the read kind, so the named-table content is
    irrelevant at execution time — it is overwritten with a
    ``localFilesRel`` pointing at the spilled exchange parquet.
    """
    return algebra.Rel(
        read=algebra.ReadRel(
            named_table=algebra.ReadRel.NamedTable(names=[exchange_id]),
            base_schema=type_pb2.NamedStruct(),
        )
    )


def _serialize_subplan(
    subtree: algebra.Rel,
    extensions,
) -> bytes:
    """Wrap *subtree* in a one-relation ``Plan`` and return the bytes.

    Carries the parent plan's *extensions* through unchanged so the C++
    converter on the remote worker resolves every function reference
    used inside *subtree*.
    """
    rel_root = algebra.RelRoot(input=subtree)
    plan_rel = plan_pb2.PlanRel(root=rel_root)
    out_plan = plan_pb2.Plan(relations=[plan_rel], extensions=extensions)
    return out_plan.SerializeToString()


CutResult = Tuple[algebra.Rel, List[PlaceholderRead], List[str]]


class SubstraitPlanDispatcher(Dispatcher):
    """Cut a Substrait ``Plan`` into stages at every ``ExchangeRel`` boundary.

    The output ``StageDAG`` shape (stage IDs, dependency edges, partitioning
    strings, partition counts) is identical to what
    ``PhysicalPlanDispatcher`` emits for the same logical plan. The
    per-stage substrait bytes are carried on the new
    ``StageNode.producerSubstraitPlanBytes`` /
    ``StageNode.consumerSubstraitPlanBytes`` fields; the executor reads
    one or the other depending on whether the stage has placeholder
    inputs.
    """

    def split(self, physicalPlan: plan_pb2.Plan) -> StageDAG:  # noqa: F821
        """Return the ``StageDAG`` for *physicalPlan*.

        *physicalPlan* must be a Substrait ``plan.Plan`` with exactly one
        ``PlanRel`` whose ``root`` carries the top-level ``input``.
        Anything else raises ``ValueError`` rather than silently returning
        an empty DAG.
        """
        if len(physicalPlan.relations) != 1:
            raise ValueError(
                "SubstraitPlanDispatcher requires exactly one PlanRel; "
                f"got {len(physicalPlan.relations)}."
            )
        plan_rel = physicalPlan.relations[0]
        if not plan_rel.HasField("root"):
            raise ValueError("SubstraitPlanDispatcher requires a PlanRel with a root.")
        root = plan_rel.root

        stages: List[StageNode] = []
        counter = [0]
        exchange_counter = [0]

        def _new_stage_id() -> str:
            sid = f"stage-{counter[0]}"
            counter[0] += 1
            return sid

        def _new_exchange_id() -> str:
            eid = f"exchange-{exchange_counter[0]}"
            exchange_counter[0] += 1
            return eid

        # Bind the cut walker to this invocation's mutable stage list +
        # counter so the recursive helper does not need to thread them.
        def _cut(rel: algebra.Rel) -> CutResult:
            return _cut_rel(
                rel, _new_stage_id, _new_exchange_id, stages, physicalPlan.extensions
            )

        rewritten_root_input, top_placeholders, top_deps = _cut(root.input)

        consumer_stage_id = _new_stage_id()
        consumer_root = algebra.RelRoot(
            input=rewritten_root_input,
            names=list(root.names),
        )
        consumer_plan_rel = plan_pb2.PlanRel(root=consumer_root)
        consumer_plan = plan_pb2.Plan(
            relations=[consumer_plan_rel],
            extensions=physicalPlan.extensions,
        )
        consumer_bytes = consumer_plan.SerializeToString()

        # The top stage carries its substrait bytes on the producer or
        # consumer field depending on whether it has placeholder inputs;
        # the StageTaskSpec maps each correctly to substraitPlanBytes.
        if top_placeholders:
            input_partition_counts = {
                placeholder.partitionCount for placeholder in top_placeholders
            }
            stage_partition_count = (
                max(input_partition_counts) if input_partition_counts else 1
            )
            unique_partitionings = tuple(
                dict.fromkeys(
                    placeholder.partitioning for placeholder in top_placeholders
                )
            )
            stage_partitioning = (
                unique_partitionings[0] if len(unique_partitionings) == 1 else "mixed"
            )
            top_stage = StageNode(
                stageId=consumer_stage_id,
                dependencies=tuple(dict.fromkeys(top_deps)),
                inputPlaceholders=tuple(top_placeholders),
                outputNames=tuple(root.names),
                partitioning=stage_partitioning,
                partitionCount=stage_partition_count,
                outputRowType=None,
                consumerSubstraitPlanBytes=consumer_bytes,
                producerSubstraitPlanBytes=None,
            )
        else:
            # Single-stage plan: no exchanges anywhere. The whole plan is
            # one Substrait-native producer stage.
            top_stage = StageNode(
                stageId=consumer_stage_id,
                dependencies=(),
                inputPlaceholders=(),
                outputNames=tuple(root.names),
                partitioning="singleton",
                partitionCount=1,
                outputRowType=None,
                consumerSubstraitPlanBytes=None,
                producerSubstraitPlanBytes=consumer_bytes,
            )
        stages.append(top_stage)

        def _stage_sort_key(stage: StageNode) -> int | str:
            try:
                return int(stage.stageId.split("-", 1)[1])
            except (IndexError, ValueError):
                return stage.stageId

        return StageDAG(tuple(sorted(stages, key=_stage_sort_key)))


def _cut_rel(
    rel: algebra.Rel,
    new_stage_id: Callable[[], str],
    new_exchange_id: Callable[[], str],
    stages: List[StageNode],
    extensions,
) -> CutResult:
    """Walk *rel*, cutting at each ``ExchangeRel``.

    Returns ``(rewritten_subtree, placeholders_for_parent,
    dependency_stage_ids)``. The rewritten subtree has each direct-child
    ``ExchangeRel`` replaced with a placeholder ``ReadRel``; each cut
    subtree is appended to *stages* as a producer (which may itself
    have been recursively cut into multiple stages).
    """
    kind = rel.WhichOneof("rel_type")
    if kind is None:
        return rel, [], []
    if kind == "read":
        return rel, [], []
    if kind == "exchange":
        return _cut_exchange(rel, new_stage_id, new_exchange_id, stages, extensions)
    if kind in _SINGLE_INPUT_RECURSE_KINDS:
        return _cut_single_input(
            rel, kind, new_stage_id, new_exchange_id, stages, extensions
        )
    if kind in _TWO_INPUT_RECURSE_KINDS:
        return _cut_two_input(
            rel, kind, new_stage_id, new_exchange_id, stages, extensions
        )
    if kind == "set":
        return _cut_set(rel, new_stage_id, new_exchange_id, stages, extensions)
    raise NotImplementedError(
        f"SubstraitPlanDispatcher: unhandled Rel kind {kind!r}; "
        "extend the cut walk taxonomy in substrait_dispatcher.py."
    )


def _cut_exchange(
    rel: algebra.Rel,
    new_stage_id: Callable[[], str],
    new_exchange_id: Callable[[], str],
    stages: List[StageNode],
    extensions,
) -> CutResult:
    """Handle a stage-boundary ``ExchangeRel``.

    Recurses into the exchange's ``input`` first (so nested exchanges
    below this one are emitted as producers before this exchange's own
    producer stage), then emits a producer ``StageNode`` carrying the
    rewritten input as its Substrait bytes, and finally returns a
    placeholder ``ReadRel`` for the parent rel to splice over the
    ExchangeRel position.
    """
    inner_rewritten, inner_placeholders, inner_deps = _cut_rel(
        rel.exchange.input, new_stage_id, new_exchange_id, stages, extensions
    )
    producer_stage_id = new_stage_id()
    producer_bytes = _serialize_subplan(inner_rewritten, extensions)
    producer_output_names = _producer_output_names(inner_rewritten)

    # The legacy dispatcher derives the partitioning string + partition
    # count from the PhysicalExchange dataclass; here both are stamped
    # onto the ExchangeRel by the AddExchanges rule.
    if not rel.exchange.HasField("common"):
        partitioning = "unknown"
    else:
        partitioning = rel.exchange.common.hint.alias or "unknown"
    partition_count = (
        rel.exchange.partition_count if rel.exchange.partition_count > 0 else 1
    )
    exchange_id = new_exchange_id()

    # An intermediate stage is *both* a consumer (its inner subtree had
    # placeholders) and a producer (its output feeds an exchange).
    # Carry the bytes on the consumer field in that case so the task
    # spec routes through the consumer execution path; otherwise it is
    # a leaf producer.
    is_consumer = bool(inner_placeholders)
    if is_consumer:
        input_partition_counts = {
            placeholder.partitionCount for placeholder in inner_placeholders
        }
        stage_partition_count = (
            max(input_partition_counts) if input_partition_counts else partition_count
        )
        unique_partitionings = tuple(
            dict.fromkeys(p.partitioning for p in inner_placeholders)
        )
        consumer_input_partitioning = (
            unique_partitionings[0] if len(unique_partitionings) == 1 else "mixed"
        )
        # ``partitioning`` on an intermediate (consumer-style) stage
        # records how its placeholder inputs are distributed: the runtime
        # uses it together with ``inputPartitionCounts`` to validate that
        # every placeholder partitions consistently and to size the
        # downstream task fan-out. ``partitionCount`` is set from the
        # parent ExchangeRel that triggered this stage cut. Per-placeholder
        # partitioning is captured separately on each ``PlaceholderRead``
        # and used by ``validate()``.
        producer_stage = StageNode(
            stageId=producer_stage_id,
            dependencies=tuple(dict.fromkeys(inner_deps)),
            inputPlaceholders=tuple(inner_placeholders),
            outputNames=producer_output_names,
            partitioning=consumer_input_partitioning,
            partitionCount=stage_partition_count,
            outputRowType=None,
            consumerSubstraitPlanBytes=producer_bytes,
            producerSubstraitPlanBytes=None,
        )
    else:
        producer_stage = StageNode(
            stageId=producer_stage_id,
            dependencies=(),
            inputPlaceholders=(),
            outputNames=producer_output_names,
            partitioning=partitioning,
            partitionCount=partition_count,
            outputRowType=None,
            consumerSubstraitPlanBytes=None,
            producerSubstraitPlanBytes=producer_bytes,
        )
    stages.append(producer_stage)

    placeholder = PlaceholderRead(
        sourceStageId=producer_stage_id,
        exchangeId=exchange_id,
        outputNames=producer_output_names,
        partitioning=partitioning,
        partitionCount=partition_count,
    )
    return _build_placeholder_read(exchange_id), [placeholder], [producer_stage_id]


def _cut_single_input(
    rel: algebra.Rel,
    kind: str,
    new_stage_id: Callable[[], str],
    new_exchange_id: Callable[[], str],
    stages: List[StageNode],
    extensions,
) -> CutResult:
    """Recurse into the ``input`` field of a generic single-input rel."""
    out = algebra.Rel()
    out.CopyFrom(rel)
    inner = getattr(out, kind)
    if not inner.HasField("input"):
        return out, [], []
    new_input, placeholders, deps = _cut_rel(
        inner.input, new_stage_id, new_exchange_id, stages, extensions
    )
    inner.input.CopyFrom(new_input)
    return out, list(placeholders), list(deps)


def _cut_two_input(
    rel: algebra.Rel,
    kind: str,
    new_stage_id: Callable[[], str],
    new_exchange_id: Callable[[], str],
    stages: List[StageNode],
    extensions,
) -> CutResult:
    """Recurse into the ``left`` and ``right`` inputs of a two-input rel."""
    out = algebra.Rel()
    out.CopyFrom(rel)
    inner = getattr(out, kind)
    placeholders: List[PlaceholderRead] = []
    deps: List[str] = []
    if inner.HasField("left"):
        new_left, p_l, d_l = _cut_rel(
            inner.left, new_stage_id, new_exchange_id, stages, extensions
        )
        inner.left.CopyFrom(new_left)
        placeholders.extend(p_l)
        deps.extend(d_l)
    if inner.HasField("right"):
        new_right, p_r, d_r = _cut_rel(
            inner.right, new_stage_id, new_exchange_id, stages, extensions
        )
        inner.right.CopyFrom(new_right)
        placeholders.extend(p_r)
        deps.extend(d_r)
    return out, placeholders, deps


def _cut_set(
    rel: algebra.Rel,
    new_stage_id: Callable[[], str],
    new_exchange_id: Callable[[], str],
    stages: List[StageNode],
    extensions,
) -> CutResult:
    """Recurse into every input of a ``SetRel``."""
    out = algebra.Rel()
    out.CopyFrom(rel)
    placeholders: List[PlaceholderRead] = []
    deps: List[str] = []
    for idx in range(len(out.set.inputs)):
        new_input, p, d = _cut_rel(
            out.set.inputs[idx], new_stage_id, new_exchange_id, stages, extensions
        )
        out.set.inputs[idx].CopyFrom(new_input)
        placeholders.extend(p)
        deps.extend(d)
    return out, placeholders, deps
