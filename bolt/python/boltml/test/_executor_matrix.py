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

"""Cross-executor / cross-plan-builder test harness.

Most boltml tests construct DataFrames via the bare ``DataFrame(...)``
constructor, which uses ``LocalExecutor`` + ``SubstraitPlanBuilderFactory``
(the module-level defaults in ``boltml/dataframe.py``). That means the
tests cover one (executor, plan-builder) pair only; nothing forces
``RayExecutor`` or the legacy ``BoltPlanBuilderFactory`` to behave
identically.

This module provides ``cross_executor_tests``, a class decorator that
takes a ``unittest.TestCase`` subclass and emits one new subclass per
configured combination. Each generated subclass monkey-patches
``boltml.dataframe`` module-level defaults inside ``setUp`` (and
restores them in ``tearDown``) so any test that builds DataFrames
through the default constructor automatically picks up the configured
combo. Tests that explicitly pass an executor / planFactory keep
their explicit choice — the harness only affects the implicit path.

Usage
-----

    import unittest
    from boltml.dataframe import DataFrame
    from boltml.test._executor_matrix import cross_executor_tests

    @cross_executor_tests
    class TestMyOps(unittest.TestCase):
        def test_simple(self):
            df = DataFrame({"a": [1, 2, 3]})
            self.assertEqual(len(df), 3)

After decoration, ``TestMyOps`` is replaced by a marker that unittest
discovery ignores; the real test classes
``TestMyOps_Local_Substrait``, ``TestMyOps_Ray_STAGED_LOCAL_Bolt``,
… are emitted into the calling module's namespace. ``unittest discover``
picks all of them up; each runs the same tests under a different combo.

Selecting a single combo for a focused run::

    python -m unittest boltml.test.test_my_ops.TestMyOps_Local_Substrait

Adding a new combo
------------------

Edit ``DEFAULT_MATRIX`` below or pass a custom ``matrix=`` to the
decorator. Each entry is a ``MatrixCombo`` triple of
``(label, executor_factory, plan_factory_factory)`` — both factories
are zero-arg callables so the harness creates a fresh instance per
test class (avoids state bleed between tests).

Skipping costly combos in CI
----------------------------

Set ``BOLTML_TEST_MATRIX`` to a comma-separated list of combo labels
to whitelist; the harness skips everything else::

    BOLTML_TEST_MATRIX=Local_Substrait python -m unittest discover -s bolt/python

(Empty / unset → run all combos.)
"""

from __future__ import annotations

import os
import sys
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterable

from ..executor.base import Executor
from ..executor.local import LocalExecutor
from ..plan_builder.base import PlanBuilderFactory
from ..plan_builder.bolt import BoltPlanBuilderFactory
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


@dataclass(frozen=True)
class MatrixCombo:
    """One row of the cross-test matrix.

    ``executor_factory`` and ``plan_factory_factory`` are zero-arg
    callables so each generated test class instantiates fresh objects;
    sharing a single ``RayExecutor`` across test classes leaks Ray
    cluster state, plan caches, and last-execution-summary fields
    between unrelated tests.
    """

    label: str
    executor_factory: Callable[[], Executor]
    plan_factory_factory: Callable[[], PlanBuilderFactory]


def _local_executor() -> Executor:
    return LocalExecutor()


def _ephemeral_exchange_manager():
    """Build a per-test ``FileExchangeManager`` rooted at a fresh
    ``tempfile.TemporaryDirectory``.

    Each Ray-flavoured executor needs its own exchange manager for
    multi-stage queries (the runtime auto-creates one for
    ``REMOTE_LEAF`` mode, but ``STAGED_LOCAL`` does not — without an
    exchange manager any query that produces a ``stage-1`` raises
    ``RuntimeError: Stage stage-1 requires an exchange manager``).
    Wrapping the manager in a closure that holds the ``TemporaryDirectory``
    keeps the temp-dir alive for the lifetime of the executor; the
    test harness drops both at tearDown.
    """
    import tempfile
    from ..distributed.ray.exchange_manager import FileExchangeManager

    tmp = tempfile.TemporaryDirectory(prefix="boltml-matrix-exchange-")
    manager = FileExchangeManager(tmp.name)
    # Pin the TemporaryDirectory to the manager so its lifetime
    # tracks the manager's. Without this, Python would gc the
    # ``TemporaryDirectory`` immediately and rmtree the rootDir
    # while the executor still held the path string.
    manager._owned_tempdir = tmp  # noqa: SLF001
    return manager


def _ray_executor_staged_local() -> Executor:
    """Ray executor in STAGED_LOCAL mode — Ray task scheduling, no
    cross-stage exchange. Cheapest of the Ray modes; no exchange
    manager / file I/O. Tests that don't exercise distributed
    exchanges still benefit from running here because they catch
    serialization regressions across the Ray task boundary."""
    # Local import: ``RayExecutor`` pulls Ray, which is heavy and
    # not always installed; deferring keeps the bare module import
    # cheap for collectors that don't use Ray combos.
    from ..executor.ray import RayExecutor
    from ..distributed.ray import RayExecutionConfig, RayExecutionMode

    return RayExecutor(
        config=RayExecutionConfig(mode=RayExecutionMode.STAGED_LOCAL),
        exchangeManager=_ephemeral_exchange_manager(),
    )


def _ray_executor_remote_leaf() -> Executor:
    """Ray executor in REMOTE_LEAF mode — leaf stages run as Ray
    tasks, consumer stages may fall back to local depending on
    rolloutLevel. Catches the Ray-task ↔ exchange-manager round-trip
    in addition to scheduling."""
    from ..executor.ray import RayExecutor
    from ..distributed.ray import RayExecutionConfig, RayExecutionMode

    return RayExecutor(
        config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF),
        exchangeManager=_ephemeral_exchange_manager(),
    )


# Default matrix — six combos covering the cross product of three
# executor flavours and two plan builders. Keep this small enough that
# a full run is tolerable on a developer laptop; opt out of Ray combos
# via ``BOLTML_TEST_MATRIX=Local_Substrait,Local_Bolt`` when iterating.
DEFAULT_MATRIX: tuple[MatrixCombo, ...] = (
    MatrixCombo("Local_Substrait", _local_executor, SubstraitPlanBuilderFactory),
    MatrixCombo("Local_Bolt", _local_executor, BoltPlanBuilderFactory),
    MatrixCombo(
        "Ray_StagedLocal_Substrait",
        _ray_executor_staged_local,
        SubstraitPlanBuilderFactory,
    ),
    MatrixCombo(
        "Ray_StagedLocal_Bolt", _ray_executor_staged_local, BoltPlanBuilderFactory
    ),
    MatrixCombo(
        "Ray_RemoteLeaf_Substrait",
        _ray_executor_remote_leaf,
        SubstraitPlanBuilderFactory,
    ),
    MatrixCombo(
        "Ray_RemoteLeaf_Bolt", _ray_executor_remote_leaf, BoltPlanBuilderFactory
    ),
)


def _selected_combos(matrix: Iterable[MatrixCombo]) -> tuple[MatrixCombo, ...]:
    """Filter ``matrix`` by ``BOLTML_TEST_MATRIX`` env var (whitelist)."""
    raw = os.environ.get("BOLTML_TEST_MATRIX", "").strip()
    if not raw:
        return tuple(matrix)
    allowed = {label.strip() for label in raw.split(",") if label.strip()}
    return tuple(combo for combo in matrix if combo.label in allowed)


@contextmanager
def _override_dataframe_defaults(executor: Executor, planFactory: PlanBuilderFactory):
    """Temporarily replace ``boltml.dataframe`` module-level executor /
    engine / planFactory defaults so the bare ``DataFrame(...)``
    constructor picks up the test combo.

    The ``planFactory=SubstraitPlanBuilderFactory()`` default in
    ``DataFrame.__init__`` is evaluated **at function-definition time**,
    so we also patch the ``__defaults__`` tuple on the constructor.
    Restore everything in the ``finally`` so unrelated tests
    (e.g. tests that explicitly assert on the default executor /
    planFactory) aren't affected.
    """
    from .. import dataframe as _df_mod

    old_local_executor = _df_mod._localExecutor_
    init_defaults = _df_mod.DataFrame.__init__.__defaults__
    # ``DataFrame.__init__`` signature (after ``self``):
    #   arg=None, executor=None, formatter=..., planFactory=...
    # (the substrait optimizer/physical/dispatcher kwargs were removed when
    # the logical optimizer moved onto the plan-builder factory).
    new_defaults = tuple(
        planFactory if isinstance(default, PlanBuilderFactory) else default
        for default in init_defaults
    )

    _df_mod._localExecutor_ = executor
    _df_mod.DataFrame.__init__.__defaults__ = new_defaults
    try:
        yield
    finally:
        _df_mod._localExecutor_ = old_local_executor
        _df_mod.DataFrame.__init__.__defaults__ = init_defaults


def _make_subclass(parent: type, combo: MatrixCombo) -> type:
    """Build a per-combo TestCase subclass that wraps every test method
    in the ``_override_dataframe_defaults`` context manager.

    We override ``run`` rather than ``setUp``/``tearDown`` because
    ``setUp`` runs after the test method is selected but BEFORE the
    test body executes — the patch-in-setup approach works, but
    ``run`` is the documented hook for "wrap the entire test" and
    survives ``setUp``-level errors (the patch revert always fires).
    """
    name = f"{parent.__name__}_{combo.label}"

    def run(self, result=None):
        executor = combo.executor_factory()
        plan_factory = combo.plan_factory_factory()
        # Stash on the instance so test bodies that need to assert on
        # the active combo (e.g. for skip-by-combo logic) can see it.
        self.matrix_combo = combo
        self.matrix_executor = executor
        self.matrix_plan_factory = plan_factory
        with _override_dataframe_defaults(executor, plan_factory):
            return super(generated, self).run(result)

    # Build the class dynamically so ``run`` can refer to ``generated``
    # via closure (super() wants the explicit class for clarity).
    generated = type(name, (parent,), {"run": run, "__module__": parent.__module__})
    return generated


class _CrossExecutorTestsHidden:
    """Non-TestCase placeholder that replaces the decorated class.

    ``unittest.TestLoader`` discovers test classes by walking module
    attributes for ``TestCase`` subclasses; ``__test__ = False`` is a
    *pytest* convention and unittest ignores it. So to keep unittest
    from running the bare un-combo'd parent (which would execute against
    whatever happened to be the module-level default executor and produce
    a ghost test pair under the original name), we have to rebind the
    name in the module to something that simply *isn't* a ``TestCase``.

    The real per-combo subclasses don't suffer because they were created
    via ``type(name, (parent,), ...)``: their ``__mro__`` already points
    at the original ``cls`` object, so this rebinding doesn't disturb
    them. Tests can still ``import`` the original class to introspect it
    (it lives on the placeholder under ``original``) without unittest
    picking it up.
    """

    pass


def cross_executor_tests(parent: type | None = None, *, matrix=None):
    """Class decorator. Replace a TestCase with N per-combo subclasses.

    Can be used bare or with arguments::

        @cross_executor_tests                      # default matrix
        class TestX(unittest.TestCase): ...

        @cross_executor_tests(matrix=(custom,))    # custom matrix
        class TestY(unittest.TestCase): ...

    After decoration:

    * ``TestX`` (the original name) is rebound in the module to a
      non-``TestCase`` placeholder so unittest discovery skips it. The
      placeholder exposes the original class as ``TestX.original`` for
      introspection.
    * ``TestX_<combo-label>`` (one per matrix entry) are emitted into
      the module namespace; these are the real test classes unittest
      runs.
    """

    def _decorate(cls: type) -> type:
        if not issubclass(cls, unittest.TestCase):
            raise TypeError(
                f"@cross_executor_tests must wrap unittest.TestCase, got {cls!r}"
            )
        chosen = _selected_combos(matrix or DEFAULT_MATRIX)
        if not chosen:
            raise RuntimeError(
                "BOLTML_TEST_MATRIX whitelist excluded every combo; "
                "remove the env var or include at least one valid label "
                f"({[c.label for c in (matrix or DEFAULT_MATRIX)]})."
            )
        module = sys.modules[cls.__module__]
        for combo in chosen:
            sub = _make_subclass(cls, combo)
            setattr(module, sub.__name__, sub)
        # Build a non-TestCase placeholder so unittest discovery skips
        # the bare parent. We have to *return* the placeholder (rather
        # than mutate ``cls``) because Python's class-decorator semantics
        # rebind the class name to the decorator's return value: any
        # attribute we set on ``cls`` would still leave a TestCase
        # subclass under the original name in the module namespace.
        placeholder = type(
            cls.__name__,
            (_CrossExecutorTestsHidden,),
            {
                "__doc__": cls.__doc__,
                "__module__": cls.__module__,
                "original": cls,
            },
        )
        return placeholder

    # Bare ``@cross_executor_tests`` (no parens).
    if parent is not None and isinstance(parent, type):
        return _decorate(parent)
    # ``@cross_executor_tests(matrix=...)`` — return the actual decorator.
    return _decorate
