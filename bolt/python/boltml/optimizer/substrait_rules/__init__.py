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

"""Substrait-Plan rewrite rules for the BoltML logical optimizer.

Each rule is a pure ``Plan -> Plan`` transformation that does not mutate
its input. Rules share a single Rel-tree visitor (``base.transform_rels``)
so recursion logic lives in exactly one place.
"""

from .base import (
    Rule as Rule,
    get_rel_kind as get_rel_kind,
    get_root_rel as get_root_rel,
    set_root_rel as set_root_rel,
    transform_rels as transform_rels,
)
from .add_exchanges import AddExchanges as AddExchanges
from .broadcast_small_build_side import (
    BroadcastSmallBuildSide as BroadcastSmallBuildSide,
)
from .ensure_smaller_on_right import EnsureSmallerOnRight as EnsureSmallerOnRight
from .inner_dedup_to_semi import InnerDedupToSemi as InnerDedupToSemi
from .join_reorder import JoinReorder as JoinReorder
from .merge_projects import MergeProjects as MergeProjects
from .partial_agg_before_exchange import (
    PartialAggBeforeExchange as PartialAggBeforeExchange,
)
from .prune_unused_columns import PruneUnusedColumns as PruneUnusedColumns
from .push_agg_through_join import PushAggThroughJoin as PushAggThroughJoin
from .push_semi_join_into_aggregate import (
    PushSemiJoinIntoAggregate as PushSemiJoinIntoAggregate,
)
from .remove_redundant_exchanges import (
    RemoveRedundantExchanges as RemoveRedundantExchanges,
)
from .push_filter_through_projects import (
    PushFilterThroughProjects as PushFilterThroughProjects,
)
from .rewrite_limits import RewriteLimits as RewriteLimits

__all__ = [
    "Rule",
    "get_rel_kind",
    "get_root_rel",
    "set_root_rel",
    "transform_rels",
    "AddExchanges",
    "BroadcastSmallBuildSide",
    "EnsureSmallerOnRight",
    "InnerDedupToSemi",
    "JoinReorder",
    "MergeProjects",
    "PartialAggBeforeExchange",
    "PruneUnusedColumns",
    "PushAggThroughJoin",
    "PushSemiJoinIntoAggregate",
    "RemoveRedundantExchanges",
    "PushFilterThroughProjects",
    "RewriteLimits",
]
