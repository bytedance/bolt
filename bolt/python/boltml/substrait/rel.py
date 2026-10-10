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

from typing import Callable, Tuple

from substrait.proto import algebra
from substrait.proto import type as stype

from pybolt import (
    FileFormat,
    JoinType,
    RowType,
    RowVector,
    SortOrder,
    createPythonRel,
    createWriteRel,
    createShuffleRel,
    makeTpchExtensionTable,
)

from ..expression.base import Expression as BoltExpression
from ..expression.named import NamedExpression
from ..expression.field import FieldExpression

from .expression import substraitExpression
from .types import (
    substraitType,
    substraitFile,
    substraitJoinType,
    substraitSortDirection,
)
from .literal import substraitVectorLiteral
from .extensions import extensionRegister, extensionIndex


Expression = algebra.Expression


# Substrait-Rel kinds that the BoltML lowering populates with
# ``RelCommon.Hint.output_names``. Imported by
# ``dispatcher.substrait_dispatcher`` (whose ``_producer_output_names``
# raises when one of these kinds returns an empty tuple). Declared
# here, next to the rel-builder functions that actually populate the
# hint, so adding a new ``output_names`` call site means updating one
# set in one place.
#
# Today the hint-emitting builders are: ``projectRel`` (``project``),
# ``aggregateRel`` (``aggregate``), ``joinRel`` (``hash_join``).
# ``virtualTable`` and ``localFilesRel`` also populate
# ``Hint.output_names`` (and ``ReadRel.base_schema``), but ``read``
# is intentionally excluded from this fail-loud set because the
# dispatcher synthesizes its own placeholder ``ReadRel`` at exchange
# cuts — that placeholder is built directly via ``algebra.ReadRel``
# (not via ``virtualTable`` or ``localFilesRel``) and carries an
# empty ``base_schema`` and no ``Hint.output_names`` by design;
# fail-louding on ``read`` would trip on every consumer-stage cut.
# If ``rel.py`` grows a new builder that calls
# ``algebra.RelCommon.Hint(output_names=...)``, add the rel kind here.
BOLTML_HINT_PROVIDING_REL_KINDS: frozenset[str] = frozenset(
    {
        "project",
        "aggregate",
        "hash_join",
    }
)


def hint_output_names(rel: algebra.Rel) -> Tuple[str, ...]:
    """Return ``RelCommon.Hint.output_names`` if present, else ``()``."""
    kind = rel.WhichOneof("rel_type")
    if kind is None:
        return ()
    inner = getattr(rel, kind)
    if not inner.HasField("common"):
        return ()
    if not inner.common.HasField("hint"):
        return ()
    return tuple(inner.common.hint.output_names)


def read_schema_names(read: algebra.ReadRel) -> Tuple[str, ...]:
    """Return the column names of a ``ReadRel`` from its ``base_schema``."""
    if not read.HasField("base_schema"):
        return ()
    return tuple(read.base_schema.names)


def rel_output_names(rel: algebra.Rel) -> Tuple[str, ...]:
    """Return the output column names of *rel* when derivable from the proto."""
    kind = rel.WhichOneof("rel_type")
    if kind is None:
        return ()
    names = hint_output_names(rel)
    if names:
        return names
    if kind == "read":
        return read_schema_names(rel.read)
    if kind in ("filter", "fetch", "sort"):
        return rel_output_names(getattr(rel, kind).input)
    if kind == "extension_single":
        return rel_output_names(rel.extension_single.input)
    return ()


def virtualTable(data: RowVector):
    types = [data.dtype().childAt(i) for i in range(len(data.dtype()))]
    names = data.dtype().names()
    nullRow = [Expression.Literal(null=substraitType(t)) for t in types]

    values = []
    for i in range(len(data)):
        if data.isNullAt(i):
            values.append(Expression.Literal.Struct(fields=nullRow))
        else:
            values.append(substraitVectorLiteral(data, i).struct)
    if len(data) == 0:
        # A typed dummy row plus Filter(false) keeps an empty input's schema
        # without constructing native vectors for nested or binary columns.
        values.append(Expression.Literal.Struct(fields=nullRow))
    virtual_table = algebra.ReadRel.VirtualTable(values=values)

    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(
            output_names=names, stats=algebra.RelCommon.Hint.Stats(row_count=len(data))
        ),
    )
    base_schema = stype.NamedStruct(
        names=names, struct=substraitType(data.dtype()).struct
    )
    read = algebra.ReadRel(
        common=common, base_schema=base_schema, virtual_table=virtual_table
    )
    if len(data) == 0:
        condition = Expression(literal=Expression.Literal(boolean=False))
        return algebra.Rel(
            filter=algebra.FilterRel(input=algebra.Rel(read=read), condition=condition)
        )
    return read


def localFilesRel(
    filePaths: list[str],
    outputType: RowType,
    fileFormat: FileFormat,
) -> algebra.ReadRel:
    names = outputType.names()
    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=names),
    )
    base_schema = stype.NamedStruct(
        names=names, struct=substraitType(outputType).struct
    )
    items = [substraitFile(fpath, fileFormat) for fpath in filePaths]
    local_files = algebra.ReadRel.LocalFiles(items=items)
    return algebra.ReadRel(
        common=common,
        base_schema=base_schema,
        local_files=local_files,
    )


def tpchExtensionTableRel(
    tableName: str,
    columnNames: list[str],
    scaleFactor: float,
    numSplits: int,
) -> algebra.ReadRel:
    """Build a Substrait ``ReadRel`` whose ``extension_table`` carries a
    ``TpchExtensionTable`` payload.

    The C++ side (``SubstraitToBoltPlan.cpp``) recognizes the
    extension via ``readRel.extension_table().detail().Is<
    substrait::bolt::TpchExtensionTable>()`` and constructs the
    corresponding TPC-H connector splits + table handle. The Python
    side delegates the proto packing to ``pybolt.makeTpchExtensionTable``
    — the proto is defined in
    ``bolt/substrait/proto/substrait/bolt/extensions.proto`` and is
    not compiled into a Python module today, so the C++ binding is
    the lowest-friction way to produce the byte-correct Any payload.

    Carries ``RelCommon.Hint.output_names`` populated from
    ``columnNames`` so it shows up in the hint-providing-rel registry
    for the dispatcher's name-validation; ``base_schema`` is left
    empty because the C++ side derives the row type from the TPC-H
    table handle, not from the proto schema.
    """
    relBytes = makeTpchExtensionTable(tableName, columnNames, scaleFactor, numSplits)
    rel = algebra.ReadRel()
    rel.ParseFromString(bytes(relBytes))
    if not rel.HasField("common"):
        rel.common.MergeFrom(
            algebra.RelCommon(
                direct=algebra.RelCommon.Direct(),
                hint=algebra.RelCommon.Hint(output_names=list(columnNames)),
            )
        )
    return rel


def projectRel(
    input: algebra.Rel,
    expressions: list[NamedExpression],
) -> algebra.ProjectRel:
    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=[e.name for e in expressions]),
    )
    expressions = [substraitExpression(e, input) for e in expressions]
    return algebra.ProjectRel(common=common, input=input, expressions=expressions)


def aggregateRel(
    input: algebra.Rel,
    keys: list[FieldExpression],
    expressions: list[NamedExpression],
) -> algebra.AggregateRel:
    # Output names: grouping keys followed by aggregate aliases
    output_names = [k.name for k in keys] + [e.name for e in expressions]
    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=output_names),
    )

    # Build grouping expressions from key names
    # We need to use a struct field reference instead of map_key.
    grouping_exprs = [substraitExpression(k) for k in keys]

    # Single grouping set referencing all grouping expressions by index
    groupings = []
    if len(grouping_exprs) > 0:
        groupings.append(
            algebra.AggregateRel.Grouping(
                expression_references=list(range(len(grouping_exprs)))
            )
        )

    # Convert AggregationExpressions to Substrait measures
    measures = []
    for agg in expressions:
        agg = agg.asFunction()
        function_reference = extensionIndex(extensionRegister[agg])
        args = [
            algebra.FunctionArgument(value=substraitExpression(arg))
            for arg in agg.arguments
        ]
        output_type = substraitType(agg.dtype)
        aggregate_fn = algebra.AggregateFunction(
            function_reference=function_reference,
            arguments=args,
            output_type=output_type,
            phase=algebra.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT,
        )
        measures.append(algebra.AggregateRel.Measure(measure=aggregate_fn))

    rel = algebra.AggregateRel(
        common=common,
        input=input,
        groupings=groupings,
        measures=measures,
        grouping_expressions=grouping_exprs,
    )
    return rel


def fetchRel(input: algebra.Rel, count: int, offset: int) -> algebra.FetchRel:
    common = algebra.RelCommon(direct=algebra.RelCommon.Direct())

    # Build literals for offset and count as expressions.
    # Unset count means ALL; if count < 0, omit count_expr.
    fetch = algebra.FetchRel(common=common, input=input)

    # Offset: treat None as 0; explicitly set 0 for clarity
    off_val = 0 if offset is None else offset
    fetch.offset_expr.CopyFrom(Expression(literal=Expression.Literal(i64=off_val)))

    if count is not None and count >= 0:
        fetch.count_expr.CopyFrom(Expression(literal=Expression.Literal(i64=count)))

    return fetch


def sortRel(
    input: algebra.Rel, order: list[tuple[FieldExpression, SortOrder]]
) -> algebra.SortRel:
    common = algebra.RelCommon(direct=algebra.RelCommon.Direct())
    sorts = []
    for e, so in order:
        expr = substraitExpression(e)
        sorts.append(algebra.SortField(expr=expr, direction=substraitSortDirection(so)))

    return algebra.SortRel(common=common, input=input, sorts=sorts)


def filterRel(input: algebra.Rel, condition: BoltExpression) -> algebra.FilterRel:
    common = algebra.RelCommon(direct=algebra.RelCommon.Direct())
    cond = substraitExpression(condition, input)
    return algebra.FilterRel(common=common, input=input, condition=cond)


def joinRel(
    left: algebra.Rel,
    right: algebra.Rel,
    lhsKeys: list[FieldExpression],
    rhsKeys: list[FieldExpression],
    outputLayout: list[str],
    joinType: JoinType,
) -> algebra.HashJoinRel:
    # Output layout corresponds to the alias/names of the resulting fields
    common = algebra.RelCommon(
        direct=algebra.RelCommon.Direct(),
        hint=algebra.RelCommon.Hint(output_names=outputLayout),
    )
    # ``SimpleComparisonType`` is an enum nested directly under
    # ``ComparisonJoinKey`` (not under the inner ``ComparisonType``
    # message); enum values are exposed as attributes on the parent
    # message in google-protobuf's generated Python bindings.
    eq_val = algebra.ComparisonJoinKey.SIMPLE_COMPARISON_TYPE_EQ
    # Build comparison keys for equality on corresponding fields
    keys = []
    for lk, rk in zip(lhsKeys, rhsKeys):
        lref = substraitExpression(lk)
        rref = substraitExpression(rk)
        comp = algebra.ComparisonJoinKey.ComparisonType(simple=eq_val)
        keys.append(
            algebra.ComparisonJoinKey(
                left=lref.selection,
                right=rref.selection,
                comparison=comp,
            )
        )

    return algebra.HashJoinRel(
        common=common,
        left=left,
        right=right,
        keys=keys,
        type=substraitJoinType(joinType),
    )


def pythonRel(
    input: algebra.Rel,
    func: Callable,
    outputType: RowType,
    *args,
    **kwargs,
) -> algebra.ExtensionSingleRel:
    ext_list: list[bytes] = [e.SerializeToString() for e in extensionRegister.getAll()]

    ext_bytes = createPythonRel(
        bytes(input.SerializeToString()),
        func,
        outputType,
        ext_list,
        *args,
        **kwargs,
    )
    ext_rel = algebra.ExtensionSingleRel()
    ext_rel.ParseFromString(bytes(ext_bytes))
    return ext_rel


def shuffleRel(input: algebra.Rel, seed: int) -> algebra.ExtensionSingleRel:
    ext_bytes = createShuffleRel(input.SerializeToString(), seed)
    ext_rel = algebra.ExtensionSingleRel()
    ext_rel.ParseFromString(bytes(ext_bytes))
    return ext_rel


def writeRel(
    input: algebra.Rel,
    directoryPath: str,
    fileFormat: FileFormat,
    partitionKeys: list[str],
    numBuckets: int,
    bucketKeys: list[str],
) -> algebra.WriteRel:
    # Use native binding to create a WriteRel skeleton, then attach input.
    wbytes = createWriteRel(
        directoryPath, fileFormat, partitionKeys, numBuckets, bucketKeys
    )
    wrel = algebra.WriteRel()
    wrel.ParseFromString(bytes(wbytes))
    wrel.input.CopyFrom(input)
    # Mark direct to simplify emit handling.
    wrel.common.direct.SetInParent()
    return wrel
