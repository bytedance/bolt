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

"""Regression test locking the ``BOLTML_LEAF_REMOTE`` env-var gate.

The gate exists because TPC-H Q10 produces wrong rows when
``single_file_read`` leaves are dispatched to Ray workers. The off-
by-something has not been root-caused. Default-off keeps the leaves
driver-local where they are correct; ``BOLTML_LEAF_REMOTE=1`` opts in
on shapes validated by the operator.

This test fails immediately if someone removes the gate without
fixing Q10. See ``docs/boltml_pipeline.md`` § "Known correctness
gates" and the comment at ``ray/task_spec.py:_encode_source``.
"""

from __future__ import annotations

import os
import unittest

from pybolt import BigintType, FileFormat, RowType, VarcharType

from ..plan_builder.base import PlanSource
from ..distributed.ray.task_spec import _encode_source


class _UnsetEnv:
    """Context manager that ensures ``BOLTML_LEAF_REMOTE`` is unset
    on entry and restores the prior value on exit."""

    def __init__(self, value: str | None) -> None:
        self._value = value
        self._prior: str | None = None
        self._had_prior = False

    def __enter__(self) -> "_UnsetEnv":
        self._had_prior = "BOLTML_LEAF_REMOTE" in os.environ
        if self._had_prior:
            self._prior = os.environ["BOLTML_LEAF_REMOTE"]
        if self._value is None:
            os.environ.pop("BOLTML_LEAF_REMOTE", None)
        else:
            os.environ["BOLTML_LEAF_REMOTE"] = self._value
        return self

    def __exit__(self, *exc_info) -> None:
        if self._had_prior:
            os.environ["BOLTML_LEAF_REMOTE"] = self._prior  # type: ignore[assignment]
        else:
            os.environ.pop("BOLTML_LEAF_REMOTE", None)


class TestLeafRemoteGate(unittest.TestCase):
    def _single_file_source(self) -> PlanSource:
        rowType = RowType(["a", "b"], [BigintType(), VarcharType()])
        return PlanSource(
            "single_file_read",
            ("/nonexistent/leaf_remote_gate.parquet", rowType, FileFormat.PARQUET),
        )

    def test_unset_keeps_single_file_read_local(self) -> None:
        """Default (unset env var) → ``_encode_source`` returns ``None``,
        keeping leaf stages driver-local where Q10 is correct."""
        with _UnsetEnv(None):
            self.assertIsNone(_encode_source(self._single_file_source()))

    def test_zero_keeps_single_file_read_local(self) -> None:
        """Explicit ``0`` (any non-``"1"``) reaches the legacy branch."""
        with _UnsetEnv("0"):
            self.assertIsNone(_encode_source(self._single_file_source()))

    def test_one_opts_into_remote_dispatch(self) -> None:
        """``BOLTML_LEAF_REMOTE=1`` returns the encoded source dict so
        the leaf stage flips to ``remoteCapable=True``. Operators
        opting in are responsible for verifying their query shapes do
        not hit the Q10 bug; do not flip this default without fixing
        Q10."""
        with _UnsetEnv("1"):
            encoded = _encode_source(self._single_file_source())
        self.assertIsNotNone(encoded)
        assert encoded is not None  # type-narrowing for the linter
        self.assertEqual(encoded["kind"], "single_file_read")
        self.assertEqual(encoded["file_path"], "/nonexistent/leaf_remote_gate.parquet")


if __name__ == "__main__":
    unittest.main()
