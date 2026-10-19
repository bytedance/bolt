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

import unittest
import os
import shutil

from pybolt import (
    BoltTaskExecutor,
    DoubleType,
    PlanBuilder,
    rowVector,
    fromList,
    JoinType,
    FileFormat,
    Split,
    TpchTable,
)

from .utils import (
    ALL_BOLT_TYPES_ROW_TYPE,
    ColumnGenerator,
    IntGenerator,
    RowVectorGenerator,
    RepeatGenerator,
)


class TestPlanExecution(unittest.TestCase):
    def setUp(self):
        self.executor = BoltTaskExecutor()
        self.testDir = "/tmp/bolt_test_plan"
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        os.makedirs(self.testDir)

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def testValuesNode(self):
        values = ColumnGenerator.from_dtype(ALL_BOLT_TYPES_ROW_TYPE)(8)
        planBuilder = PlanBuilder().values([values])
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(values, result)

    def testFilterNode(self):
        expectedValues = RowVectorGenerator(c0=RepeatGenerator(5))(4)
        filteredValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        planBuilder = (
            PlanBuilder().values([expectedValues, filteredValues]).filter("c0 != 4")
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testProjectNode(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        expectedValues = RowVectorGenerator(c0_x_2=RepeatGenerator(8))(4)
        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .project(["c0 * CAST(2 AS INTEGER) as c0_x_2"])
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testAggregationNode(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        expectedValues = RowVectorGenerator(
            c0=RepeatGenerator(4),
            avg_c0=RepeatGenerator(4.0, DoubleType()),
        )(1)
        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .singleAggregation(
                groupingKeys=["c0"],
                aggregates=["avg(c0) as avg_c0"],
            )
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testLimitNode(self):
        inputValues = RowVectorGenerator(c0=IntGenerator().repeat(4))(4)
        expectedValues = RowVectorGenerator(c0=IntGenerator().repeat(4))(2)
        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .limit(offset=1, count=2, isPartial=False)
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testOrderByNode(self):
        inputValues = RowVectorGenerator(c0=IntGenerator().setMin(0).setMax(100))(4)
        # Extract values and sort them manually to create expected result
        orderedValues = sorted(
            [inputValues.childAt("c0")[i] for i in range(len(inputValues))]
        )
        expectedValues = rowVector(
            inputValues.dtype().names(),
            [fromList(orderedValues, inputValues.childAt("c0").dtype())],
        )
        planBuilder = (
            PlanBuilder().values([inputValues]).orderBy(["c0 ASC"], isPartial=False)
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testHashJoinNode(self):
        leftValues = RowVectorGenerator(c0=RepeatGenerator(1))(2)
        rightValues = RowVectorGenerator(
            c00=RepeatGenerator(1), c11=RepeatGenerator(2)
        )(2)

        # Cross product of 2 rows with '1' and 2 rows with '1' should give 4 rows
        expectedValues = RowVectorGenerator(
            c0=RepeatGenerator(1), c11=RepeatGenerator(2)
        )(4)

        rightPlan = PlanBuilder(0).values([rightValues]).planNode()

        planBuilder = (
            PlanBuilder(200)
            .values([leftValues])
            .hashJoin(
                leftKeys=["c0"],
                rightKeys=["c00"],
                build=rightPlan,
                filter="",
                outputLayout=["c0", "c11"],
                joinType=JoinType.kInner,
                nullAware=False,
            )
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testAppendColumns(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        expectedValues = RowVectorGenerator(
            c0=RepeatGenerator(4), c1=RepeatGenerator(5)
        )(4)
        planBuilder = (
            PlanBuilder().values([inputValues]).appendColumns(["c0 + 1 as c1"])
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testLocalShuffleNode(self):
        inputValues = RowVectorGenerator(c0=IntGenerator())(10)
        planBuilder = PlanBuilder().values([inputValues]).localShuffle(42)
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        # Verify row equivalence
        colNames = inputValues.dtype().names()

        inputRows = []
        for i in range(len(inputValues)):
            inputRows.append(tuple(inputValues.childAt(c)[i] for c in colNames))

        resultRows = []
        for i in range(len(result)):
            resultRows.append(tuple(result.childAt(c)[i] for c in colNames))

        # Compare as sorted lists to handle shuffling
        self.assertEqual(sorted(inputRows), sorted(resultRows))

    def testTableWriteReadNode(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .tableWrite(outputDirectoryPath=self.testDir, fileFormat=FileFormat.PARQUET)
        )
        planFragment = planBuilder.planFragment()
        self.executor.execute(planFragment, "TestExecutorWrite")

        # Read values from the parquet file found in testDir.
        files = [
            f
            for f in os.listdir(self.testDir)
            if os.path.isfile(os.path.join(self.testDir, f))
        ]
        self.assertTrue(len(files) == 1)

        filePath = os.path.join(self.testDir, files[0])
        readBuilder = PlanBuilder().tableRead(filePath, inputValues.dtype())

        # add hive split with the input file to executor.
        planNode = readBuilder.planNode()
        split = Split.hive(filePath, FileFormat.PARQUET)
        self.executor.addSplits(planNode.id(), [split])

        # Read the table.
        result = self.executor.execute(readBuilder.planFragment(), "TestExecutorRead")

        # Check that the table read from the file is the same as the initial
        # input values.
        self.assertEqual(result, inputValues)

    def testPythonPlanNode(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)

        def myFunc(vector):
            return vector

        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .pythonPlanNode(
                functionName="myFunc",
                outputType=inputValues.dtype(),
                function=myFunc,
            )
        )
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, inputValues)

    def testCreateComplexPlan(self):
        inputValues = RowVectorGenerator(c0=RepeatGenerator(4))(4)
        # Expected: c0 -> filter(c0>0) -> project(c0 as id) -> orderBy(id) -> limit(1)
        # Result should be 1 row with id=4
        expectedValues = rowVector(["id"], [fromList([4])])

        planBuilder = (
            PlanBuilder()
            .values([inputValues])
            .filter("c0 > 0")
            .project(["c0 as id"])
            .orderBy(["id ASC"], isPartial=False)
            .limit(offset=0, count=1, isPartial=False)
        )

        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result, expectedValues)

    def testTpchTableScanNode(self):
        columnNames = ["nationkey", "comment"]
        planBuilder = PlanBuilder().tpchTableScan(
            table=TpchTable.nation, columnNames=columnNames, scaleFactor=0.01
        )
        splits = Split.tpch(1)
        self.executor.addSplits(planBuilder.planNode().id(), splits)
        planFragment = planBuilder.planFragment()
        result = self.executor.execute(planFragment, "TestExecutor")
        self.assertEqual(result.dtype().names(), columnNames)
