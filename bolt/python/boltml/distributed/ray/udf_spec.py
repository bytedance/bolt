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

"""Pickleable description of a user-defined function registration.

The driver records every UDF registered on an ``Executor`` as a
``UdfSpec`` and ships the registry to remote workers as part of the
``StageTaskSpec``. Each worker re-registers the UDFs on its own
``pybolt`` singleton before executing the Substrait plan, so the
plan's function-extension references resolve to a real callable.

Without this, ``RayExecutor.registerVectorFunction`` silently stored
the registration on the driver only; the worker's pybolt singleton
had no entry for the function name and execution failed with a
"function not registered" error deep in C++.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Tuple


# ``pybolt.BoltType`` instances are pybind objects that don't ship with
# pickle support, so a ``UdfSpec`` carrying one would fail Ray's
# cloudpickle check on the driver before the task ever reached a worker.
# We sidestep the problem by serialising the type to its canonical
# string form (e.g. ``"BIGINT"``, ``"ARRAY<BIGINT>"``,
# ``"MAP<VARCHAR,DOUBLE>"``) on the driver and reconstructing it on the
# worker via ``_parseBoltType``. Substrait already uses the same string
# vocabulary in its function-extension references, so there's no novel
# vocabulary to maintain — we mirror what ``str(boltType)`` produces.
_PRIMITIVE_BOLT_TYPES: dict[str, str] = {
    "BIGINT": "BigintType",
    "INTEGER": "IntegerType",
    "DOUBLE": "DoubleType",
    "REAL": "RealType",
    "BOOLEAN": "BooleanType",
    "VARCHAR": "VarcharType",
    "VARBINARY": "VarbinaryType",
    "TINYINT": "TinyintType",
    "SMALLINT": "SmallintType",
    "TIMESTAMP": "TimestampType",
    "HUGEINT": "HugeintType",
    "DATE": "DateType",
    "UNKNOWN": "UnknownType",
}


def _splitTopLevel(s: str, sep: str = ",") -> list[str]:
    """Split ``s`` on ``sep`` only at top-level <...> nesting depth.

    Used to break ``MAP<VARCHAR,BIGINT>`` into ``["VARCHAR", "BIGINT"]``
    and ``ROW<a:BIGINT,b:DOUBLE>`` into ``["a:BIGINT", "b:DOUBLE"]``
    without splitting inside a nested complex type's brackets.
    """
    parts: list[str] = []
    depth = 0
    start = 0
    for i, c in enumerate(s):
        if c == "<":
            depth += 1
        elif c == ">":
            depth -= 1
        elif c == sep and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    parts.append(s[start:].strip())
    return parts


def _parseBoltType(s: str):
    """Parse a Bolt type string back into a ``pybolt.BoltType`` instance.

    Inverse of ``str(boltType)`` for the surface that ``str`` produces:
    primitives (``BIGINT`` etc.), ``ARRAY<X>``, ``MAP<K,V>``,
    ``ROW<name1:T1,name2:T2,...>``, and ``DECIMAL(p,s)`` (Short for
    p<=18, Long for p>18). Timestamp-with-tz variants are not
    currently handled — UDFs that need them can extend this helper.
    Local import of ``pybolt`` so this module stays cheap to import
    on the driver before pybolt is loaded.
    """
    import pybolt

    s = s.strip()
    if not s:
        raise ValueError("empty type string")
    primitive = _PRIMITIVE_BOLT_TYPES.get(s)
    if primitive is not None:
        return getattr(pybolt, primitive)()
    if s.startswith("ARRAY<") and s.endswith(">"):
        return pybolt.ArrayType(_parseBoltType(s[len("ARRAY<") : -1]))
    if s.startswith("MAP<") and s.endswith(">"):
        inner = s[len("MAP<") : -1]
        kv = _splitTopLevel(inner, ",")
        if len(kv) != 2:
            raise ValueError(f"MAP requires exactly 2 type args, got {kv!r}")
        return pybolt.MapType(_parseBoltType(kv[0]), _parseBoltType(kv[1]))
    if s.startswith("ROW<") and s.endswith(">"):
        inner = s[len("ROW<") : -1]
        fields = _splitTopLevel(inner, ",")
        names: list[str] = []
        types: list = []
        for f in fields:
            # Field is ``name:TYPE`` — the colon is at depth-0 too.
            m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+)$", f)
            if not m:
                raise ValueError(f"ROW field {f!r} not in 'name:TYPE' form")
            names.append(m.group(1))
            types.append(_parseBoltType(m.group(2)))
        return pybolt.RowType(names, types)
    # ``DECIMAL(p, s)`` — Short for p<=18, Long for p>18. The canonical
    # form ``str(BoltType)`` emits matches this regex.
    decimal_match = re.match(r"^DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)$", s)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision <= 18:
            return pybolt.ShortDecimalType(precision, scale)
        return pybolt.LongDecimalType(precision, scale)
    raise ValueError(
        f"Cannot parse Bolt type from string {s!r} — extend "
        "boltml.distributed.ray.udf_spec._parseBoltType to support it. "
        "Known gaps: timestamp with time zone."
    )


def _serializeBoltType(t) -> str:
    """Canonical string form of ``t`` for ``UdfSpec.outputTypeStr``."""
    return str(t)


@dataclass(frozen=True)
class UdfSpec:
    """A single UDF registration.

    Attributes:
        kind: ``"vector"`` (scalar / map-batch / vector functions registered
            via ``Executor.registerVectorFunction``) or ``"aggregation"``
            (registered via ``Executor.registerAggregationFunction``). The
            two go to different pybolt entry points
            (``registerPythonFunction`` vs ``registerPythonAggregator``).
        function: The Python callable. Pickled by Ray (cloudpickle) when
            shipped in a task argument; closures over module-level state
            are fine, closures over driver-local state are not.
        functionName: The name the Substrait plan references. Must match
            the function-extension reference in the plan.
        outputTypeStr: Canonical string form of the Bolt return type
            (``str(pybolt.BoltType)``). Stored as a string instead of
            a live ``BoltType`` because pybind's BoltType bindings
            don't pickle, and Ray cloudpickles task arguments before
            shipping. Reconstructed on the worker via
            ``_parseBoltType``.
        numArgs: Argument count (matches what the registration entry
            point on pybolt expects).
        defaultArgs: Tail of trailing default values in declaration
            order; the caller passes the same list it inferred from
            ``inspect.signature``. ``()`` for aggregations (no defaults
            currently surfaced through the aggregation register API).
        mapBatch: Whether the function is a map-batch function. Only
            meaningful for ``kind="vector"``.
    """

    kind: str
    function: Callable
    functionName: str
    outputTypeStr: str
    numArgs: int
    defaultArgs: Tuple[Any, ...] = ()
    mapBatch: bool = False

    def __post_init__(self):
        if self.kind not in ("vector", "aggregation"):
            raise ValueError(
                f"UdfSpec.kind must be 'vector' or 'aggregation', got {self.kind!r}"
            )


@dataclass(frozen=True)
class UdfRegistry:
    """Ordered, immutable collection of ``UdfSpec`` entries.

    Used as a single field on ``StageTaskSpec`` so the registry survives
    Ray's cloudpickle round-trip into remote workers. The order of
    ``specs`` is the insertion order from the driver — preserved so
    re-registration on the worker matches what the user did on the
    driver (relevant when one UDF closes over another that was
    registered earlier).
    """

    specs: Tuple[UdfSpec, ...] = field(default_factory=tuple)

    def __len__(self) -> int:
        return len(self.specs)

    def __bool__(self) -> bool:
        return bool(self.specs)

    def __iter__(self):
        return iter(self.specs)

    def appended(self, spec: UdfSpec) -> "UdfRegistry":
        """Return a new registry with ``spec`` appended.

        Returns a fresh instance so the original (and any taskSpec that
        already captured it) stays untouched. Frozen-dataclass-friendly.
        """
        return UdfRegistry(specs=self.specs + (spec,))


def reregisterUdfsOnPybolt(registry: UdfRegistry) -> None:
    """Re-register every UDF in ``registry`` on the current process's
    ``pybolt`` singleton.

    Called by remote workers at the top of every task entry point so
    that the worker's pybolt registry mirrors the driver's. Idempotent
    at the pybolt level: re-registering an existing name overwrites
    the previous registration, so the same registry can flow through
    multiple consecutive tasks on the same worker without leaking
    duplicates.
    """
    if not registry:
        return
    # Local import: ``pybolt`` is a heavy native module; deferring keeps
    # the driver-side import (e.g. ``from .udf_spec import UdfSpec`` in
    # ``RayExecutor``) cheap.
    from pybolt import registerPythonAggregator, registerPythonFunction

    for spec in registry:
        outputType = _parseBoltType(spec.outputTypeStr)
        if spec.kind == "vector":
            registerPythonFunction(
                spec.function,
                spec.functionName,
                outputType,
                spec.numArgs,
                list(spec.defaultArgs),
                spec.mapBatch,
            )
        else:  # "aggregation" (validated in __post_init__)
            registerPythonAggregator(
                spec.function,
                spec.functionName,
                outputType,
                spec.numArgs,
            )
