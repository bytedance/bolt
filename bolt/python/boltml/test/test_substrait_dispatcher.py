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

"""Per-construct + structural-parity tests for ``SubstraitPlanDispatcher``.

The dispatcher consumes a Substrait ``Plan`` whose physical optimizer
pipeline has inserted ``ExchangeRel`` nodes and produces a ``StageDAG``
whose shape matches what ``PhysicalPlanDispatcher`` produces from the
legacy ``PhysicalPlan``.

Tests stay in-process (no Ray, no harness, no compile_parity); each
fixture is a small DataFrame whose logical plan is fully Substrait-
lowerable.
"""

import unittest
from typing import List, Tuple

from ..dataframe import DataFrame
from ..distributed.dispatcher.stage_dag import StageDAG
from ..distributed.dispatcher.substrait_dispatcher import SubstraitPlanDispatcher
from ..function.aggregation import Mean, Sum
from ..optimizer.substrait_physical_pipeline import (
    SubstraitPhysicalOptimizerPipeline,
)
from ..plan_builder.substrait import SubstraitPlanBuilderFactory


def _build_substrait_dag(df: DataFrame) -> StageDAG:
    """Lower *df*, run the Substrait physical pipeline, then dispatch."""
    plan = df._planBuilder_.toSubstraitPlan()
    physical = SubstraitPhysicalOptimizerPipeline().optimize(plan)
    return SubstraitPlanDispatcher().split(physical)


def _stage_signatures(
    dag: StageDAG,
) -> List[Tuple[str, str, int, Tuple[str, ...]]]:
    """Return ``(stageId, partitioning, partitionCount, dependencies)`` per stage."""
    return [
        (s.stageId, s.partitioning, s.partitionCount, tuple(s.dependencies))
        for s in dag.stages
    ]


class TestSubstraitDispatcherPerConstruct(unittest.TestCase):
    """Each test exercises one Substrait rel kind that triggers an exchange."""

    def setUp(self) -> None:
        self.factory = SubstraitPlanBuilderFactory()

    def test_no_exchanges_yields_single_producer_stage(self) -> None:
        df = DataFrame({"id": [1, 2, 3]}, planFactory=self.factory)
        df.filter(df["id"] > 1)

        dag = _build_substrait_dag(df)
        self.assertEqual(len(dag.stages), 1)
        sole = dag.stages[0]
        self.assertEqual(sole.stageId, "stage-0")
        self.assertEqual(sole.dependencies, ())
        self.assertEqual(sole.inputPlaceholders, ())
        # Substrait-native producer: bytes live on the producer field.
        self.assertIsNone(sole.consumerSubstraitPlanBytes)
        self.assertIsNotNone(sole.producerSubstraitPlanBytes)

    def test_grouped_aggregate_yields_producer_then_consumer(self) -> None:
        df = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(avg=Mean("value"))

        dag = _build_substrait_dag(df)
        self.assertEqual(len(dag.stages), 2)
        producer, consumer = dag.stages
        self.assertEqual(producer.stageId, "stage-0")
        self.assertEqual(consumer.stageId, "stage-1")
        self.assertEqual(producer.dependencies, ())
        self.assertEqual(consumer.dependencies, ("stage-0",))
        self.assertEqual(len(consumer.inputPlaceholders), 1)
        self.assertEqual(consumer.inputPlaceholders[0].partitioning, "hash(grp)")
        self.assertEqual(producer.partitioning, "hash(grp)")
        self.assertEqual(producer.partitionCount, 12)
        self.assertEqual(consumer.partitionCount, 12)
        # Producer carries its own (full producer-subtree) bytes; consumer
        # carries the placeholder-bearing rewritten root subtree.
        self.assertIsNotNone(producer.producerSubstraitPlanBytes)
        self.assertIsNone(producer.consumerSubstraitPlanBytes)
        self.assertIsNotNone(consumer.consumerSubstraitPlanBytes)

    def test_hash_join_yields_two_producers_one_consumer(self) -> None:
        lhs = DataFrame({"id": [1, 2], "lv": [10, 20]}, planFactory=self.factory)
        rhs = DataFrame({"id": [1, 3], "rv": [100, 300]}, planFactory=self.factory)
        lhs.join(rhs, {"id"})

        dag = _build_substrait_dag(lhs)
        self.assertEqual(len(dag.stages), 3)
        # Walk-order: producer for hash_join.left (the chain continuation),
        # then producer for hash_join.right (opaque rhs), then the
        # join-consumer at the top.
        leftProd, rightProd, consumer = dag.stages
        self.assertEqual(leftProd.dependencies, ())
        self.assertEqual(rightProd.dependencies, ())
        self.assertEqual(consumer.dependencies, (leftProd.stageId, rightProd.stageId))
        self.assertEqual(len(consumer.inputPlaceholders), 2)
        self.assertEqual(consumer.inputPlaceholders[0].partitioning, "hash(l_id)")
        self.assertEqual(consumer.inputPlaceholders[1].partitioning, "hash(r_id)")
        self.assertEqual(leftProd.partitionCount, 12)
        self.assertEqual(rightProd.partitionCount, 12)
        self.assertEqual(consumer.partitionCount, 12)

    def test_shuffle_yields_two_stages_with_round_robin(self) -> None:
        df = DataFrame({"id": [1, 2], "tag": ["a", "b"]}, planFactory=self.factory)
        df.shuffle(7)

        dag = _build_substrait_dag(df)
        self.assertEqual(len(dag.stages), 2)
        producer, consumer = dag.stages
        self.assertEqual(producer.partitioning, "shuffle(seed=7)")
        self.assertEqual(consumer.dependencies, ("stage-0",))
        self.assertEqual(consumer.inputPlaceholders[0].partitioning, "shuffle(seed=7)")

    def test_global_aggregate_yields_single_stage(self) -> None:
        # A global (no-grouping) aggregate stays single-phase: with no
        # grouping keys ``AddExchanges`` inserts no exchange, so there's
        # no stage cut and the dispatcher emits a single stage.
        # Distributed correctness is preserved by the leaf-fan-out gate
        # (``_plan_contains_aggregate``), which runs an aggregate-bearing
        # leaf as a single task so there are no per-shard partials left
        # un-merged.
        df = DataFrame({"value": [1, 2, 3]}, planFactory=self.factory)
        df.groupBy().aggregate(total=Sum("value"))
        dag = _build_substrait_dag(df)
        self.assertEqual(len(dag.stages), 1)
        (stage,) = dag.stages
        self.assertEqual(stage.dependencies, ())
        self.assertIsNotNone(stage.producerSubstraitPlanBytes)


class TestStructuralParityWithLegacy(unittest.TestCase):
    """Per-construct structural assertions for the Substrait dispatcher.

    The original legacy ``PhysicalPlanDispatcher`` parity test has been
    converted to inline-expected stage signatures: the legacy outputs
    were well-known and stable, so the test still checks the same
    shape without depending on a legacy dispatcher.
    """

    def setUp(self) -> None:
        self.factory = SubstraitPlanBuilderFactory()

    def _assert_signatures(self, df: DataFrame, expected: list) -> None:
        substrait = _build_substrait_dag(df)
        self.assertEqual(sorted(_stage_signatures(substrait)), sorted(expected))

    def test_parity_filter_only(self) -> None:
        df = DataFrame({"id": [1, 2, 3]}, planFactory=self.factory)
        df.filter(df["id"] > 1)
        self._assert_signatures(df, [("stage-0", "singleton", 1, ())])

    def test_parity_grouped_aggregate(self) -> None:
        df = DataFrame(
            {"grp": ["a", "a", "b"], "value": [1, 2, 3]}, planFactory=self.factory
        )
        df.groupBy("grp").aggregate(avg=Mean("value"))
        self._assert_signatures(
            df,
            [
                ("stage-0", "hash(grp)", 12, ()),
                ("stage-1", "hash(grp)", 12, ("stage-0",)),
            ],
        )

    def test_parity_hash_join(self) -> None:
        lhs = DataFrame({"id": [1, 2], "lv": [10, 20]}, planFactory=self.factory)
        rhs = DataFrame({"id": [1, 3], "rv": [100, 300]}, planFactory=self.factory)
        lhs.join(rhs, {"id"})
        self._assert_signatures(
            lhs,
            [
                ("stage-0", "hash(l_id)", 12, ()),
                ("stage-1", "hash(r_id)", 12, ()),
                ("stage-2", "mixed", 12, ("stage-0", "stage-1")),
            ],
        )

    def test_parity_shuffle(self) -> None:
        df = DataFrame({"id": [1, 2], "tag": ["a", "b"]}, planFactory=self.factory)
        df.shuffle(7)
        self._assert_signatures(
            df,
            [
                ("stage-0", "shuffle(seed=7)", 12, ()),
                ("stage-1", "shuffle(seed=7)", 12, ("stage-0",)),
            ],
        )


if __name__ == "__main__":
    unittest.main()
