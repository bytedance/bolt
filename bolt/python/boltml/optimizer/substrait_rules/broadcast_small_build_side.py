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

"""Broadcast a HashJoinRel's right-side scan when it's small enough.

Pattern: after ``AddExchanges`` materializes one ``ExchangeRel`` over
each side of a hash join, a small build side (the right-hand scan) is
better broadcast to every probe-side partition than hash-shuffled.

Why broadcast helps
-------------------

For ``lineitem (60M) ⨝ small_dim``:

* Default (hash on both sides):
  - lineitem is shuffled by join key (~960 MB at SF=10)
  - small_dim is shuffled by join key (~5 MB)
  - Total shuffle bytes = ~965 MB

* Broadcast right side:
  - lineitem stays at scan partitioning (NO shuffle on the big side)
  - small_dim is broadcast to all N consumer partitions
    (= ~5 MB × N partitions)
  - Total shuffle bytes = ~5 MB × N (e.g. 60 MB at N=12)

The trade depends on the build side's bytes vs ``N × right_bytes`` vs
``left_bytes_shuffled``. For TPC-H ``region`` (5 rows) and ``nation``
(25 rows), broadcast is unconditionally better. For ``part`` (~50 MB
at SF=10), broadcast at N=12 is 600 MB which beats the original
~1010 MB total. The threshold is configurable via
``BOLTML_BROADCAST_BUILD_BYTES`` env var (default 0 = off, opt-in).

Build-side size estimation
--------------------------

The optimizer reads the right-side scan's ``ReadRel.local_files`` URIs
and stats each file. This requires filesystem access from the optimizer
rule -- a small but real coupling that's only safe when:

* The plan's leaf scans use ``localFilesRel`` with concrete file URIs
  (boltml's TPC-H lowering does this).
* The optimizer runs on a host that can stat those URIs (driver does;
  Ray workers may not, but the optimizer runs on the driver).

When the right side isn't a simple scan (joins, aggregates, etc.) the
rule conservatively skips it. When file size can't be determined
(``uri_path_glob``, missing file, permission error) the rule also
skips.

Substrait broadcast exchange kind
---------------------------------

Substrait's ``ExchangeRel`` natively supports a ``broadcast`` kind
(in addition to ``scatter_by_fields``, ``round_robin``, etc.). The
rewrite swaps ``scatter_by_fields -> broadcast``, sets
``partition_count = 1`` (single broadcast output), and stamps a
``broadcast`` alias into ``RelCommon.Hint.alias`` so the dispatcher
can route it correctly.

Idempotency
-----------

After rewriting, the right-side ExchangeRel's ``exchange_kind`` is
``broadcast`` (not ``scatter_by_fields``). A second rule pass finds no
``scatter_by_fields`` to rewrite and is a no-op. Output bytes are
byte-equal under repeated application.
"""

from __future__ import annotations

import os
from typing import Optional
from urllib.parse import urlparse

from substrait.proto import algebra, plan

from ...logging import boltmlDebugLog
from .base import Rule, get_rel_kind, rewrite_plan_root


_DEFAULT_THRESHOLD_BYTES: int = 0  # opt-in: off by default


def _broadcast_threshold_bytes() -> int:
    """Read ``BOLTML_BROADCAST_BUILD_BYTES`` env var (default 0)."""
    try:
        return max(0, int(os.environ.get("BOLTML_BROADCAST_BUILD_BYTES", "0")))
    except ValueError:
        return _DEFAULT_THRESHOLD_BYTES


class BroadcastSmallBuildSide(Rule):
    """Replace a hash exchange over a small right-side scan with a broadcast
    exchange. Pure: input ``Plan`` is not mutated. Idempotent.
    """

    name: str = "broadcast_small_build_side"

    def apply(self, p: plan.Plan) -> plan.Plan:
        threshold = _broadcast_threshold_bytes()
        if threshold <= 0:
            # Opt-in only -- env var unset / 0 means leave the plan unchanged.
            out = plan.Plan()
            out.CopyFrom(p)
            return out

        def _try(rel: algebra.Rel) -> Optional[algebra.Rel]:
            return _try_broadcast_join(rel, threshold)

        return rewrite_plan_root(p, _try, bottom_up=True)


def _try_broadcast_join(rel: algebra.Rel, threshold: int) -> Optional[algebra.Rel]:
    """If *rel* is a hash join whose right-side scan total bytes < threshold,
    return a rewritten join with the right-side exchange flipped from
    ``scatter_by_fields`` to ``broadcast``.
    """
    if get_rel_kind(rel) != "hash_join":
        return None
    join = rel.hash_join
    if not join.HasField("right"):
        return None
    right = join.right
    # Right side must already be wrapped in an Exchange (this rule runs
    # after AddExchanges). If it's not, the upstream pipeline shape is
    # unexpected -- bail.
    if get_rel_kind(right) != "exchange":
        return None
    right_exchange = right.exchange
    if right_exchange.WhichOneof("exchange_kind") != "scatter_by_fields":
        return None  # already broadcast or other kind
    if not right_exchange.HasField("input"):
        return None
    # Walk the exchange's input down to find leaf reads. Sum their bytes.
    total = _estimate_subtree_bytes(right_exchange.input)
    if total is None:
        return None  # can't determine size, conservative skip
    if total >= threshold:
        return None  # too big to broadcast, leave as hash shuffle

    # All good -- rewrite the right-side exchange to broadcast.
    new_right_exchange = algebra.ExchangeRel()
    new_right_exchange.CopyFrom(right_exchange)
    # Clear the scatter_by_fields oneof and set broadcast instead.
    new_right_exchange.ClearField("scatter_by_fields")
    new_right_exchange.broadcast.CopyFrom(algebra.ExchangeRel.Broadcast())
    # Single broadcast output (vs N hash buckets).
    new_right_exchange.partition_count = 1
    # Stamp the alias so the dispatcher's per-stage partitioning string
    # picks up "broadcast" (the dispatcher reads RelCommon.Hint.alias as
    # the canonical scattering string).
    if not new_right_exchange.HasField("common"):
        new_right_exchange.common.CopyFrom(algebra.RelCommon())
    new_right_exchange.common.hint.alias = "broadcast"

    new_join = algebra.HashJoinRel()
    new_join.CopyFrom(join)
    new_join.right.CopyFrom(algebra.Rel(exchange=new_right_exchange))

    boltmlDebugLog(
        "broadcast_small_build_side",
        f"broadcasting right-side scan: total_bytes={total} < threshold={threshold}",
    )
    return algebra.Rel(hash_join=new_join)


def _estimate_subtree_bytes(rel: algebra.Rel) -> Optional[int]:
    """Walk *rel* to find leaf ``ReadRel`` instances and sum their on-disk
    file sizes. Returns ``None`` if the subtree can't be sized
    confidently (non-file source, missing file, permission error).
    """
    kind = get_rel_kind(rel)
    if kind == "read":
        return _read_rel_bytes(rel.read)
    # Single-input rels: recurse.
    if kind in (
        "filter",
        "project",
        "fetch",
        "aggregate",
        "sort",
        "exchange",
        "expand",
        "write",
        "extension_single",
    ):
        inner = getattr(rel, kind)
        if inner.HasField("input"):
            return _estimate_subtree_bytes(inner.input)
        return None
    # Dual-input rels: sum both sides.
    if kind in (
        "hash_join",
        "merge_join",
        "nested_loop_join",
        "join",
        "cross",
    ):
        inner = getattr(rel, kind)
        left_bytes = (
            _estimate_subtree_bytes(inner.left) if inner.HasField("left") else None
        )
        right_bytes = (
            _estimate_subtree_bytes(inner.right) if inner.HasField("right") else None
        )
        if left_bytes is None or right_bytes is None:
            return None
        return left_bytes + right_bytes
    # Unknown leaf or set op -- conservative bail.
    return None


def _read_rel_bytes(read: algebra.ReadRel) -> Optional[int]:
    """Sum on-disk file size for a ReadRel. Only handles ``local_files``
    with concrete URIs (uri_path / uri_file). Globs and named tables
    return ``None`` (can't size confidently at plan time).
    """
    if read.WhichOneof("read_type") != "local_files":
        return None
    total = 0
    for item in read.local_files.items:
        path = None
        kind = item.WhichOneof("path_type")
        if kind == "uri_file":
            path = item.uri_file
        elif kind == "uri_path":
            path = item.uri_path
        elif kind == "uri_folder":
            path = item.uri_folder
        else:
            return None  # uri_path_glob -- can't easily enumerate
        # Strip file:// prefix.
        parsed = urlparse(path)
        local_path = parsed.path if parsed.scheme in ("file", "") else None
        if local_path is None:
            return None
        try:
            if os.path.isdir(local_path):
                # Directory: sum all parquet shards under it.
                for fname in os.listdir(local_path):
                    full = os.path.join(local_path, fname)
                    if os.path.isfile(full):
                        total += os.path.getsize(full)
            elif os.path.isfile(local_path):
                total += os.path.getsize(local_path)
            else:
                return None
        except OSError:
            return None
    return total
