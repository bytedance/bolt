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

"""Preserve native Bolt type metadata across Arrow transport.

Remote-task returns and exchange publish/load paths share these helpers.
Exchange descriptors can supply fallback types when a producer did not attach
metadata to its Arrow table. Explicit schemas also preserve decimal and
container types when constructing empty partitions.
"""

from __future__ import annotations

import logging
from typing import Sequence

import pyarrow as pa

from ...dataframe import DataFrame


_BOLT_TYPE_METADATA_KEY = b"boltml_column_types"
_BOLT_TYPE_METADATA_SEP = "\x00"

_logger = logging.getLogger(__name__)

# Type strings we've already warned about in this process. The set is
# unbounded in principle but bounded in practice by the (small) number of
# distinct bolt types a workload uses; one-warning-per-type avoids log
# spam when the same column flows through hundreds of exchange tasks.
_unparseable_warned: set[str] = set()


def arrow_table_with_bolt_types(dataframe: DataFrame) -> "pa.Table":
    """Convert *dataframe* to Arrow ``Table`` with bolt-type metadata.

    Encodes each column's canonical ``str(boltType)`` in the schema
    metadata under :data:`_BOLT_TYPE_METADATA_KEY`, null-byte-separated
    so a column count + per-column type string round-trips through
    Arrow without ambiguity. Used by every producer-side path that
    serialises a bolt DataFrame to Arrow for transport across the
    Python ↔ Ray boundary.
    """
    arrow_table = pa.Table.from_struct_array(dataframe.toArrow())
    return _attach_type_metadata(
        arrow_table,
        [str(dataframe.dtype.childAt(i)) for i in range(len(dataframe.dtype))],
    )


def _attach_type_metadata(table: "pa.Table", type_strs: Sequence[str]) -> "pa.Table":
    """Return *table* with bolt-type metadata attached.

    Idempotent: replaces any existing entry under the key. Preserves
    every other metadata entry already on the schema (Arrow's own
    metadata, user-supplied keys).
    """
    metadata = dict(table.schema.metadata or {})
    metadata[_BOLT_TYPE_METADATA_KEY] = _BOLT_TYPE_METADATA_SEP.join(type_strs).encode()
    return table.replace_schema_metadata(metadata)


def empty_dataframe_with_bolt_types(
    names: Sequence[str],
    type_strs: Sequence[str],
) -> "DataFrame":
    """Construct a zero-row DataFrame with the specified native Bolt types.

    The single-RowType overload of pybolt.rowVector creates every column with
    its declared type, without inferring types from an empty value list.

    Raises ``ValueError`` if ``names`` and ``type_strs`` differ in
    length, or if any type string can't be parsed; callers that don't
    have authoritative type info should fall back to the Arrow-default
    path.
    """
    if len(names) != len(type_strs):
        raise ValueError(
            f"names and type_strs must have the same length: "
            f"{len(names)} vs {len(type_strs)}"
        )

    import pybolt

    from .udf_spec import _parseBoltType

    types = [_parseBoltType(ts) for ts in type_strs]
    rowType = pybolt.RowType(list(names), types)
    return DataFrame(pybolt.rowVector(rowType))


def dataframe_from_typed_arrow(
    arrow_table: "pa.Table",
    fallback_types: Sequence[str] | None = None,
) -> "DataFrame":
    """Rebuild a bolt ``DataFrame`` from a metadata-tagged Arrow Table.

    Resolution order for per-column bolt types:

    1. The :data:`_BOLT_TYPE_METADATA_KEY` entry in the table's schema
       metadata (set by :func:`arrow_table_with_bolt_types`). Wins
       whenever present.
    2. The explicit *fallback_types* sequence, used when the caller has
       authoritative type info from elsewhere (e.g. an
       ``ExchangePartitionDescriptor.outputTypes`` tuple persisted in
       the descriptor itself, for legacy descriptors written before the
       metadata path was added).
    3. Default Arrow decoding via plain ``DataFrame(arrow_table)``.

    For each column the resolved type is compared against the default
    Arrow decoding's bolt type. Matches take the fast path (no rebuild).
    Mismatches are rebuilt via ``pybolt.fromList(values, target_type)``.
    Empty tables use the explicit-schema construction path below.
    """
    metadata = arrow_table.schema.metadata or {}
    type_strs: list[str] | None = None
    if _BOLT_TYPE_METADATA_KEY in metadata:
        candidate = (
            metadata[_BOLT_TYPE_METADATA_KEY].decode().split(_BOLT_TYPE_METADATA_SEP)
        )
        if len(candidate) == len(arrow_table.column_names):
            type_strs = candidate
    if type_strs is None and fallback_types is not None:
        if len(fallback_types) == len(arrow_table.column_names):
            type_strs = list(fallback_types)
    if type_strs is None:
        return DataFrame(arrow_table)

    # Rebuild zero-row tables from the declared types rather than values.
    if arrow_table.num_rows == 0:
        try:
            return empty_dataframe_with_bolt_types(arrow_table.column_names, type_strs)
        except Exception:  # noqa: BLE001
            # Parse / construction failure — fall through to default
            # Arrow decoding. The per-column warning path below would
            # otherwise log the parse failure on a non-empty partition
            # with the same type.
            return DataFrame(arrow_table)

    import pybolt

    from .udf_spec import _parseBoltType

    children = []
    needs_recast = False
    for i, ts in enumerate(type_strs):
        col_chunked = arrow_table.column(i).combine_chunks()
        default_vec = pybolt.importFromArrow(col_chunked)
        if str(default_vec.dtype()) == ts:
            children.append(default_vec)
            continue
        try:
            target_type = _parseBoltType(ts)
        except Exception as exc:  # noqa: BLE001
            # Unknown type string — leave the column on default decoding
            # rather than crash, but make the degradation visible.
            # Report metadata that cannot be restored so callers can detect
            # when default Arrow decoding was used instead.
            if ts not in _unparseable_warned:
                _unparseable_warned.add(ts)
                _logger.warning(
                    "boltml exchange: cannot parse bolt type %r "
                    "(column %d, default decoded as %s); type tag "
                    "lost on the receive side. Extend "
                    "boltml.distributed.ray.udf_spec._parseBoltType to cover "
                    "this type. Underlying parser error: %s",
                    ts,
                    i,
                    default_vec.dtype(),
                    exc,
                )
            children.append(default_vec)
            continue
        if len(col_chunked) == 0:
            children.append(default_vec)
            continue
        values = col_chunked.to_pylist()
        children.append(pybolt.fromList(values, target_type))
        needs_recast = True

    if not needs_recast:
        return DataFrame(arrow_table)
    rv = pybolt.rowVector(list(arrow_table.column_names), children)
    return DataFrame(rv)
