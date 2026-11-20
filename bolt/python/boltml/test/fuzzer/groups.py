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

from boltml.function.aggregation import Any, Count, Max
from pybolt.test.fuzzer.utils import ColumnKind


_PROJECTABLE_KINDS = {ColumnKind.NUMERIC, ColumnKind.STRING}


def _typed_is_orderable(kind: ColumnKind | str):
    return kind == "timestamp" or (isinstance(kind, ColumnKind) and kind.isOrderable())


def _typed_is_groupable(kind: ColumnKind | str):
    return kind == "timestamp" or (isinstance(kind, ColumnKind) and kind.isGroupable())


def _pipeline_groups(column_kinds: dict[str, ColumnKind | str]):
    """Groups column names by capability.

    Accepts both pure-ColumnKind maps (dataframe pipelines) and maps that
    additionally tag extended types with string sentinels like ``"timestamp"``
    (typed pipelines). Keys that aren't applicable for a given kind-domain are
    safely empty.
    """
    items = column_kinds.items()
    return {
        "numeric": [n for n, k in items if k is ColumnKind.NUMERIC],
        "boolean": [n for n, k in items if k is ColumnKind.BOOLEAN],
        "string": [n for n, k in items if k is ColumnKind.STRING],
        "projectable": [n for n, k in items if k in _PROJECTABLE_KINDS],
        "predicate": [n for n, k in items if k in _PROJECTABLE_KINDS],
        "orderable": [n for n, k in items if _typed_is_orderable(k)],
        "groupable": [n for n, k in items if _typed_is_groupable(k)],
        "joinable": [
            n for n, k in items if isinstance(k, ColumnKind) and k.isJoinable()
        ],
        "all": list(column_kinds.keys()),
    }


def _aggregation_assignments(rng: random.Random, groups: dict[str, list[str]]):
    assignments = {"row_count": Count()}
    if groups["boolean"]:
        bool_col = rng.choice(groups["boolean"])
        assignments[f"any_{bool_col}"] = Any(bool_col)
    if groups["numeric"]:
        numeric_col = rng.choice(groups["numeric"])
        assignments[f"max_{numeric_col}"] = Max(numeric_col)
    return assignments


def _aggregation_output_kinds(
    grouping_keys: list[str],
    assignments: dict,
    column_kinds: dict[str, ColumnKind | str],
):
    output = {key: column_kinds[key] for key in grouping_keys}
    for alias, fn in assignments.items():
        if type(fn).__name__ == "Any":
            output[alias] = ColumnKind.BOOLEAN
        else:
            output[alias] = ColumnKind.NUMERIC
    return output


def _joined_output_kinds(
    lhs_kinds: dict[str, ColumnKind], rhs_kinds: dict[str, ColumnKind], join_key: str
):
    output = {join_key: lhs_kinds[join_key]}
    output |= {
        (f"l_{name}" if name in rhs_kinds else name): kind
        for name, kind in lhs_kinds.items()
        if name != join_key
    }
    output |= {
        (f"r_{name}" if name in lhs_kinds else name): kind
        for name, kind in rhs_kinds.items()
        if name != join_key
    }
    return output
