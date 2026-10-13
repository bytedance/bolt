# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

"""Exercise distributed safety decisions without starting a Ray cluster."""

from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pybolt
import ray
from substrait.proto import algebra, plan

from boltml import DataFrame
from boltml.distributed.ray.remote_worker import _traverse_and_replace_read
from boltml.distributed.ray.runtime import (
    RayRuntime,
    _merge_remote_results,
    _partition_parallel_reason,
)
from boltml.distributed.ray.task_spec import StageTaskSpec
from boltml.distributed.ray.udf_spec import _parseBoltType
from boltml.distributed.ray import exchange_registry
from boltml.distributed.ray.scheduler import RayLeafScheduler
from boltml.optimizer.substrait_rules.join_reorder import _make_field_ref
from boltml.optimizer.substrait_rules.add_exchanges import _hash_alias
from boltml.optimizer.substrait_rules.remove_redundant_exchanges import (
    RemoveRedundantExchanges,
)


def planBytes(rel):
    return plan.Plan(
        relations=[plan.PlanRel(root=algebra.RelRoot(input=rel))]
    ).SerializeToString()


def exchange(rel, indices):
    return algebra.Rel(
        exchange=algebra.ExchangeRel(
            input=rel,
            partition_count=2,
            scatter_by_fields=algebra.ExchangeRel.ScatterFields(
                fields=[_make_field_ref(i).selection for i in indices]
            ),
        )
    )


class RayCorrectnessTests(unittest.TestCase):
    def testRegistryRejectedDeletionPreservesPendingCommit(self):
        for hasCommitted in (False, True):
            with self.subTest(hasCommitted=hasCommitted):
                registry = exchange_registry._ExchangeRegistryActor.__ray_metadata__.modified_class()
                key = ("execution", "stage", "exchange")
                if hasCommitted:
                    registry.registerExchange(
                        "owner", *key, (SimpleNamespace(attemptId=0),)
                    )
                reservation = registry.prepareExchange("owner", *key, 1)
                self.assertTrue(reservation["accepted"])
                timestamps = dict(registry._pendingTimestamps)

                self.assertFalse(registry.deleteExchange("other", *key))
                self.assertEqual(registry._pendingTimestamps, timestamps)

                descriptors = (SimpleNamespace(attemptId=1),)
                committed = registry.commitExchange(
                    "owner", *key, reservation["token"], descriptors
                )
                self.assertTrue(committed["accepted"])
                self.assertEqual(registry.resolve(*key), descriptors)

    def testRegistryDeletionChecksBothOwnersBeforeChangingState(self):
        registry = (
            exchange_registry._ExchangeRegistryActor.__ray_metadata__.modified_class()
        )
        key = ("execution", "stage", "exchange")
        reservation = registry.prepareExchange("pending-owner", *key, 1)
        descriptors = (SimpleNamespace(attemptId=0),)
        registry.registerExchange("committed-owner", *key, descriptors)
        for owner in ("pending-owner", "committed-owner"):
            self.assertFalse(registry.deleteExchange(owner, *key))
        self.assertEqual(registry.resolve(*key), descriptors)
        self.assertTrue(
            registry.abortExchange("pending-owner", *key, reservation["token"])
        )
        self.assertTrue(registry.deleteExchange("committed-owner", *key))

    def testRegistryOwnerDeletionClearsPendingAndCommittedState(self):
        for hasCommitted in (False, True):
            with self.subTest(hasCommitted=hasCommitted):
                registry = exchange_registry._ExchangeRegistryActor.__ray_metadata__.modified_class()
                key = ("execution", "stage", "exchange")
                descriptors = (SimpleNamespace(attemptId=1),)
                if hasCommitted:
                    registry.registerExchange("owner", *key, descriptors)
                reservation = registry.prepareExchange("owner", *key, 1)

                self.assertEqual(registry.deleteExchange("owner", *key), hasCommitted)
                self.assertFalse(
                    registry.commitExchange(
                        "owner", *key, reservation["token"], descriptors
                    )["accepted"]
                )
                self.assertIsNone(registry.resolve(*key))
                self.assertFalse(registry.deleteExchange("owner", *key))
                self.assertEqual(registry._pendingTimestamps, {})
                self.assertEqual(registry._descriptorTimestamps, {})
                self.assertTrue(registry.prepareExchange("other", *key, 1)["accepted"])

    def testAmbiguousHashNamesAreRejectedBeforeDispatch(self):
        for name in ("", "a,b", " a", "a "):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "rename the columns"):
                    _hash_alias([name])

    def testRegistryCacheIsScopedToRaySession(self):
        context = MagicMock()
        context.get_job_id.return_value = "job-1"
        context.get_node_id.side_effect = ["node-1", "node-1", "node-2"]
        handles = [MagicMock(), MagicMock()]
        with (
            patch.object(exchange_registry, "ensureRayInitialized"),
            patch.object(exchange_registry, "_REGISTRY_HANDLE", None),
            patch.object(exchange_registry, "_REGISTRY_SESSION", None),
            patch.object(ray, "get_runtime_context", return_value=context),
            patch.object(ray, "get_actor", side_effect=handles) as lookup,
            patch.object(ray, "get"),
        ):
            self.assertIs(exchange_registry.getExchangeRegistry(), handles[0])
            self.assertIs(exchange_registry.getExchangeRegistry(), handles[0])
            self.assertIs(exchange_registry.getExchangeRegistry(), handles[1])
            self.assertEqual(lookup.call_count, 2)

    def testDeadActorHandleIsNotReusedForRetry(self):
        scheduler = RayLeafScheduler(workerMode="actor")
        scheduler._actor = object()
        with patch.object(ray, "get", side_effect=ray.exceptions.ActorDiedError()):
            with self.assertRaises(ray.exceptions.ActorDiedError):
                scheduler._get(object())
        self.assertIsNone(scheduler._actor)

    def testActorHandleIsScopedToRaySession(self):
        scheduler = RayLeafScheduler(workerMode="actor")
        context = MagicMock()
        context.get_job_id.return_value = "job-1"
        context.get_node_id.side_effect = ["node-1", "node-1", "node-2"]
        actors = [object(), object()]
        with (
            patch.object(scheduler, "ensureInitialized"),
            patch.object(ray, "get_runtime_context", return_value=context),
            patch(
                "boltml.distributed.ray.scheduler.RemoteExecutionActor.remote",
                side_effect=actors,
            ) as create,
        ):
            self.assertIs(scheduler._ensureActor(), actors[0])
            self.assertIs(scheduler._ensureActor(), actors[0])
            self.assertIs(scheduler._ensureActor(), actors[1])
            self.assertEqual(create.call_count, 2)

    def testRowTypeSerializationPreservesQuotedNamesAndDecimals(self):
        names = ["space name", "a,b", "a:b", 'a"b', "a<b", ""]
        nested = pybolt.RowType(names, [pybolt.BigintType() for _ in names])
        dtype = pybolt.RowType(
            ["amount", "nested"],
            [pybolt.ShortDecimalType(5, 2), pybolt.ArrayType(nested)],
        )
        self.assertEqual(str(_parseBoltType(str(dtype))), str(dtype))
        self.assertEqual(str(_parseBoltType("ROW<>")), "ROW<>")

    def testReversedHashKeysKeepBothExchanges(self):
        inner = exchange(algebra.Rel(read=algebra.ReadRel()), [0, 1])
        for keys, removable in (([0, 1], True), ([1, 0], False), ([0, 0, 1], False)):
            with self.subTest(keys=keys):
                before = plan.Plan.FromString(planBytes(exchange(inner, keys)))
                after = RemoveRedundantExchanges().apply(before)
                self.assertEqual(
                    after.relations[0].root.input,
                    inner if removable else before.relations[0].root.input,
                )

    def testJoinPartitionGateRequiresOrderedHashKeys(self):
        joined = algebra.Rel(
            hash_join=algebra.HashJoinRel(
                keys=[
                    algebra.ComparisonJoinKey(
                        left=_make_field_ref(i).selection,
                        right=_make_field_ref(i).selection,
                    )
                    for i in (0, 1)
                ]
            )
        )
        for rhs, expected in (
            ("hash(c,d)", None),
            ("hash(d,c)", "join_partitioning_key_mismatch"),
        ):
            with self.subTest(rhs=rhs):
                spec = StageTaskSpec(
                    stageId="join",
                    executionKind="consumer",
                    taskKind="multi_input_consumer",
                    remoteCapable=True,
                    partitionCount=2,
                    inputPartitionCounts=(2, 2),
                    inputPartitionings=("hash(a,b)", rhs),
                    inputPlaceholders=(
                        SimpleNamespace(outputNames=("a", "b")),
                        SimpleNamespace(outputNames=("c", "d")),
                    ),
                    substraitPlanBytes=planBytes(joined),
                )
                self.assertEqual(_partition_parallel_reason(spec), expected)

    def testWritePlaceholderReplacedWithoutReplacingOrdinaryRead(self):
        ordinary = algebra.Rel(
            read=algebra.ReadRel(virtual_table=algebra.ReadRel.VirtualTable())
        )
        placeholder = algebra.Rel(
            read=algebra.ReadRel(
                named_table=algebra.ReadRel.NamedTable(names=["exchange-0"])
            )
        )
        root = algebra.Rel(
            write=algebra.WriteRel(
                input=algebra.Rel(
                    cross=algebra.CrossRel(left=ordinary, right=placeholder)
                )
            )
        )
        replacement = algebra.Rel(
            read=algebra.ReadRel(local_files=algebra.ReadRel.LocalFiles())
        )
        remaining = [replacement]
        _traverse_and_replace_read(root, remaining)
        self.assertEqual(remaining, [])
        self.assertEqual(root.write.input.cross.left, ordinary)
        self.assertEqual(root.write.input.cross.right, replacement)

    def testMissingPlaceholderInputFailsClearly(self):
        placeholder = algebra.Rel(
            read=algebra.ReadRel(
                named_table=algebra.ReadRel.NamedTable(names=["exchange-0"])
            )
        )
        with self.assertRaisesRegex(ValueError, "Missing input"):
            _traverse_and_replace_read(placeholder, [])

    def testSortAndLimitLeavesNeverFanOut(self):
        runtime = RayRuntime()
        for rel in (
            algebra.Rel(sort=algebra.SortRel()),
            algebra.Rel(fetch=algebra.FetchRel(count=2)),
            algebra.Rel(
                project=algebra.ProjectRel(input=algebra.Rel(sort=algebra.SortRel()))
            ),
        ):
            with self.subTest(kind=rel.WhichOneof("rel_type")):
                spec = StageTaskSpec(
                    stageId="leaf",
                    executionKind="producer",
                    substraitPlanBytes=planBytes(rel),
                )
                with patch.object(
                    runtime, "_runLeafStageRemotely", return_value="single"
                ) as single:
                    self.assertEqual(
                        runtime._runPartitionedLeafStageRemotely(spec, 4), "single"
                    )
                single.assert_called_once()

    def testMergeDoesNotSortArrayKeysOrGuessProjectedKeys(self):
        grouped = algebra.Rel(
            aggregate=algebra.AggregateRel(
                grouping_expressions=[_make_field_ref(0), _make_field_ref(1)]
            )
        )
        projected = algebra.Rel(
            project=algebra.ProjectRel(input=grouped, expressions=[_make_field_ref(0)])
        )
        for tables, root in (
            (
                [
                    pa.table({"key": [[2]], "total": [3]}),
                    pa.table({"key": [[1]], "total": [2]}),
                ],
                grouped,
            ),
            ([pa.table({"key": [2]}), pa.table({"key": [1]})], projected),
        ):
            with self.subTest(schema=tables[0].schema):
                results = [SimpleNamespace(table=table) for table in tables]
                output = _merge_remote_results(
                    results, SimpleNamespace(substraitPlanBytes=planBytes(root))
                )
                self.assertEqual(
                    DataFrame(output).toArrow(pa.Table).to_pylist(),
                    pa.concat_tables(tables).to_pylist(),
                )


if __name__ == "__main__":
    unittest.main()
