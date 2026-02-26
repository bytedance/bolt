/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/connectors/arrow/ArrowMemoryConnector.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/type/Type.h"
#include "gtest/gtest.h"

#include <folly/init/Init.h>

namespace {
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::connector::arrow;
using bytedance::bolt::exec::test::PlanBuilder;

class ArrowConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kArrowConnectorId = "test-arrow";

  std::shared_ptr<FlatVector<int64_t>> sampleBigIntVector_;
  std::shared_ptr<FlatVector<int32_t>> sampleIntegerVector_;
  std::shared_ptr<FlatVector<double>> sampleDoubleVector_;
  std::shared_ptr<FlatVector<StringView>> sampleVarCharVector__;

  std::vector<int64_t> bigIntVec_ = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<int32_t> integerVec_ = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<double> doubleVec_ =
      {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
  std::vector<StringView> stringViewVec_ = {
      "ALPHA",
      "BRAVO",
      "CHARLIE",
      "DELTA",
      "Echo",
      "FOXTROT",
      "GOLF",
      "INDIA",
      "JULIET",
      "KILO"};

  void SetUp() override {
    bytedance::bolt::connector::arrow::CheckArrowMemoryConnectorFactoryInit<
        bytedance::bolt::connector::arrow::ArrowMemoryConnectorFactory>();
    std::shared_ptr<const config::ConfigBase> config;
    auto arrowConnector =
        connector::getConnectorFactory(connector::kArrowMemoryConnectorName)
            ->newConnector(kArrowConnectorId, config);
    connector::registerConnector(arrowConnector);

    sampleBigIntVector_ = createSampleVectorColumn<int64_t>(bigIntVec_);
    sampleIntegerVector_ = createSampleVectorColumn<int32_t>(integerVec_);
    sampleDoubleVector_ = createSampleVectorColumn<double>(doubleVec_);
    sampleVarCharVector__ =
        createSampleVectorColumn<StringView>(stringViewVec_);
  }

  void TearDown() override {
    connector::unregisterConnector(kArrowConnectorId);
    sampleBigIntVector_ = nullptr;
    sampleIntegerVector_ = nullptr;
    sampleDoubleVector_ = nullptr;
    sampleVarCharVector__ = nullptr;
    OperatorTestBase::TearDown();
  }

  template <class T>
  std::shared_ptr<FlatVector<T>> createSampleVectorColumn(std::vector<T>& vec) {
    return makeFlatVector<T>(vec);
  }

  template <class T>
  std::shared_ptr<FlatVector<T>>
  createSampleVectorColumn(std::vector<T>& vec, int start, int end) {
    BOLT_CHECK_LT(end, vec.size());
    BOLT_CHECK_LE(start, end);
    std::vector<T> res;
    for (int i = start; i <= end; i++) {
      res.push_back(vec[i]);
    }
    return makeFlatVector<T>(res);
  }

  std::shared_ptr<RowType> createRowType(
      std::vector<std::string> columnNames,
      std::vector<std::shared_ptr<const Type>> columnTypes) {
    return std::make_shared<RowType>(
        std::move(columnNames), std::move(columnTypes));
  }

  // Create one pair of struct-typed Arrow Array and Schema, representing the
  // whole table.
  exec::Split makeArrowSplits(ArrowSchema arrowSchema, ArrowArray arrowArray)
      const {
    auto arrowSplit =
        std::make_shared<ArrowMemoryConnectorSplit>(kArrowConnectorId);
    arrowSplit->addArrowSplitData({arrowSchema}, {arrowArray}, false);
    return exec::Split(arrowSplit);
  }

  // Create an In-columns Arrow table. Each column is represented by one pair of
  // Arrow Array and Schema.
  exec::Split makeArrowSplits(
      std::vector<ArrowSchema> arrowSchemas,
      std::vector<ArrowArray> arrowArrays) const {
    auto arrowSplit =
        std::make_shared<ArrowMemoryConnectorSplit>(kArrowConnectorId);
    arrowSplit->addArrowSplitData(arrowSchemas, arrowArrays, true);
    return exec::Split(arrowSplit);
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }

  std::pair<RowVectorPtr, std::shared_ptr<RowType>> generateArrowData(
      const std::vector<std::string>& childrenNames,
      const std::vector<std::shared_ptr<const Type>>& childrenTypes) {
    BOLT_CHECK_EQ(
        childrenNames.size(),
        childrenTypes.size(),
        "childrenNames and childrenTypes should have the equal length.");
    std::vector<VectorPtr> childrenVecs;
    for (auto type : childrenTypes) {
      if (type->isBigint()) {
        childrenVecs.push_back(createSampleVectorColumn<int64_t>(bigIntVec_));
      } else if (type->isInteger()) {
        childrenVecs.push_back(createSampleVectorColumn<int32_t>(integerVec_));
      } else if (type->isDouble()) {
        childrenVecs.push_back(createSampleVectorColumn<double>(doubleVec_));
      } else if (type->isVarchar()) {
        childrenVecs.push_back(
            createSampleVectorColumn<StringView>(stringViewVec_));
      } else {
        BOLT_FAIL("{} is not supported in this unit test.", type->toString());
      }
    }
    auto rowVectorPtr = makeRowVector(childrenNames, childrenVecs);
    auto rowTypePtr = createRowType(childrenNames, childrenTypes);
    return {rowVectorPtr, rowTypePtr};
  }
};

// Simple scan of a table of 4 columns in different type
TEST_F(ArrowConnectorTest, simple) {
  // Create the Arrow table

  auto [rowVec, rowTypePtr] = generateArrowData(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto plan = PlanBuilder()
                  .tableScan(std::move(rowTypePtr), tableHandler, assignments)
                  .planNode();
  auto output = getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)});
  auto expected = makeRowVector(
      {sampleBigIntVector_,
       sampleIntegerVector_,
       sampleDoubleVector_,
       sampleVarCharVector__});
  test::assertEqualVectors(expected, output);
}

// Simple Arrow Connector Test on the in-column arrow table
TEST_F(ArrowConnectorTest, simpleInColumn) {
  // Create the in-column Arrow table
  int coloumnSize = 4;

  auto rowVecC0 = createSampleVectorColumn<int64_t>(bigIntVec_);
  auto rowVecC1 = createSampleVectorColumn<int32_t>(integerVec_);
  auto rowVecC2 = createSampleVectorColumn<double>(doubleVec_);
  auto rowVecC3 = createSampleVectorColumn<StringView>(stringViewVec_);

  std::vector<ArrowSchema> arrowSchemas(coloumnSize);
  std::vector<ArrowArray> arrowArrays(coloumnSize);

  exportToArrow(rowVecC0, arrowSchemas[0], {});
  exportToArrow(rowVecC0, arrowArrays[0], pool(), {});

  exportToArrow(rowVecC1, arrowSchemas[1], {});
  exportToArrow(rowVecC1, arrowArrays[1], pool(), {});

  exportToArrow(rowVecC2, arrowSchemas[2], {});
  exportToArrow(rowVecC2, arrowArrays[2], pool(), {});

  exportToArrow(rowVecC3, arrowSchemas[3], {});
  exportToArrow(rowVecC3, arrowArrays[3], pool(), {});

  auto rowTypePtr = createRowType(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});
  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto plan = PlanBuilder()
                  .tableScan(std::move(rowTypePtr), tableHandler, assignments)
                  .planNode();
  auto output = getResults(plan, {makeArrowSplits(arrowSchemas, arrowArrays)});
  auto expected = makeRowVector(
      {sampleBigIntVector_,
       sampleIntegerVector_,
       sampleDoubleVector_,
       sampleVarCharVector__});
  test::assertEqualVectors(expected, output);
}

TEST_F(ArrowConnectorTest, simpleFilter) {
  auto [rowVec, rowTypePtr] = generateArrowData({"c0"}, {BIGINT()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto plan = PlanBuilder()
                  .tableScan(std::move(rowTypePtr), tableHandler, assignments)
                  .filter("c0 > 5")
                  .orderBy({"c0"}, false)
                  .planNode();
  auto output = getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)});
  auto expected = makeRowVector({makeFlatVector<int64_t>({6, 7, 8, 9, 10})});
  test::assertEqualVectors(expected, output);
}

// Check that aliases are correctly resolved.
TEST_F(ArrowConnectorTest, singleColumnWithAlias) {
  const std::string aliasedName = "my_aliased_column_name";

  auto [rowVec, rowTypePtr] = generateArrowData({"c3"}, {VARCHAR()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto outputType = ROW({aliasedName}, {VARCHAR()});
  auto plan =
      PlanBuilder()
          .tableScan(
              outputType,
              tableHandler,
              {
                  {aliasedName,
                   std::make_shared<ArrowMemoryColumnHandle>("c3", VARCHAR())},
                  {"other_name",
                   std::make_shared<ArrowMemoryColumnHandle>("c3", VARCHAR())},
                  {"third_column",
                   std::make_shared<ArrowMemoryColumnHandle>("c1", INTEGER())},
              })
          .limit(0, 1, false)
          .planNode();

  auto output = getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALPHA",
  })});
  test::assertEqualVectors(expected, output);

  EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
  EXPECT_EQ(1, output->childrenSize());
}

// Ensures that splits broken down using different configurations return the
// same dataset in the end.
TEST_F(ArrowConnectorTest, multipleSplits) {
  // Create split1
  auto [firstRowVec, firstRowTypePtr] =
      generateArrowData({"c0", "c1"}, {BIGINT(), INTEGER()});
  auto [secondRowVec, secondRowTypePtr] =
      generateArrowData({"c0", "c1"}, {BIGINT(), INTEGER()});

  ArrowSchema firstArrowSchema, secondArrowSchema;
  ArrowArray firstArrowArray, secondArrowArray;
  exportToArrow(firstRowVec, firstArrowSchema, {});
  exportToArrow(firstRowVec, firstArrowArray, pool(), {});
  exportToArrow(secondRowVec, secondArrowSchema, {});
  exportToArrow(secondRowVec, secondArrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto plan =
      PlanBuilder()
          .tableScan(std::move(firstRowTypePtr), tableHandler, assignments)
          .orderBy({"c0"}, false)
          .planNode();

  std::vector<uint64_t> expectedBigIntVec;
  std::vector<uint32_t> expectedIntegerVec;

  for (int i = 0; i < bigIntVec_.size(); i++) {
    expectedBigIntVec.push_back(i + 1);
    expectedBigIntVec.push_back(i + 1);
    expectedIntegerVec.push_back(i + 1);
    expectedIntegerVec.push_back(i + 1);
  }

  auto expected = makeRowVector(
      {makeFlatVector<uint64_t>(expectedBigIntVec),
       makeFlatVector<uint32_t>(expectedIntegerVec)});

  auto output = getResults(
      plan,
      {makeArrowSplits(firstArrowSchema, firstArrowArray),
       makeArrowSplits(secondArrowSchema, secondArrowArray)});
  for (int i = 0; i < expected->size(); i++) {
    BOLT_CHECK_EQ(expected->toString(i), output->toString(i))
  }
}

// input data type different from data type in table scan node
TEST_F(ArrowConnectorTest, testErrorType) {
  // Create the Arrow table

  auto [rowVec, rowTypePtr] =
      generateArrowData({"c0", "c1"}, {BIGINT(), INTEGER()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto errorType = createRowType({"c0", "c1"}, {INTEGER(), BIGINT()});
  auto plan = PlanBuilder()
                  .tableScan(std::move(errorType), tableHandler, assignments)
                  .planNode();
  EXPECT_THROW(
      getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)}),
      BoltRuntimeError);
}

TEST_F(ArrowConnectorTest, unknownColumn) {
  auto [rowVec, rowTypePtr] =
      generateArrowData({"c0", "c1"}, {BIGINT(), INTEGER()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto unexistedColumn = createRowType({"does_not_exist"}, {INTEGER()});
  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  EXPECT_THROW(
      {
        auto plan =
            PlanBuilder()
                .tableScan(
                    std::move(unexistedColumn), tableHandler, assignments)
                .planNode();
        auto output =
            getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)});
      },
      BoltRuntimeError);
}

// Testing aggregation on Arrow Table
TEST_F(ArrowConnectorTest, simpleAggregation) {
  // Create the Arrow table
  auto [rowVec, rowTypePtr] = generateArrowData(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});

  ArrowSchema arrowSchema;
  ArrowArray arrowArray;
  exportToArrow(rowVec, arrowSchema, {});
  exportToArrow(rowVec, arrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;

  auto plan = PlanBuilder()
                  .tableScan(std::move(rowTypePtr), tableHandler, assignments)
                  .singleAggregation({}, {"count(1)"})
                  .planNode();
  auto output = getResults(plan, {makeArrowSplits(arrowSchema, arrowArray)});
  auto expected = makeRowVector({makeFlatVector<int64_t>(
      std::vector<int64_t>{(int64_t)bigIntVec_.size()})});
  test::assertEqualVectors(expected, output);
}

// Join two Arrow Tables
TEST_F(ArrowConnectorTest, join) {
  // Create left table
  auto [leftRowVec, leftTableRowTypePtr] =
      generateArrowData({"left_c0", "left_c1"}, {BIGINT(), INTEGER()});

  ArrowSchema leftArrowSchema, rightArrowSchema;
  ArrowArray leftArrowArray, rightArrowArray;
  exportToArrow(leftRowVec, leftArrowSchema, {});
  exportToArrow(leftRowVec, leftArrowArray, pool(), {});

  // Create right table
  auto [rightRowVec, rightTableRowTypePtr] =
      generateArrowData({"right_c0", "right_c3"}, {BIGINT(), VARCHAR()});

  exportToArrow(rightRowVec, rightArrowSchema, {});
  exportToArrow(rightRowVec, rightArrowArray, pool(), {});

  auto tableHandler =
      std::make_shared<connector::arrow::ArrowMemoryTableHandle>(
          kArrowConnectorId);

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      leftAssignments, rightAssignments;

  auto planNodeIdGenerator =
      std::make_shared<bytedance::bolt::core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              std::move(leftTableRowTypePtr), tableHandler, leftAssignments)
          .capturePlanNodeId(leftScanId)
          .hashJoin(
              {"left_c0"},
              {"right_c0"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      std::move(rightTableRowTypePtr),
                      tableHandler,
                      rightAssignments)
                  .capturePlanNodeId(rightScanId)
                  .planNode(),
              "", // extra filter
              {"left_c0", "left_c1", "right_c3"})
          .orderBy({"left_c0"}, false)
          .planNode();

  auto output =
      exec::test::AssertQueryBuilder(plan)
          .split(leftScanId, makeArrowSplits(leftArrowSchema, leftArrowArray))
          .split(
              rightScanId, makeArrowSplits(rightArrowSchema, rightArrowArray))
          .copyResults(pool());

  auto expected = makeRowVector(
      {sampleBigIntVector_, sampleIntegerVector_, sampleVarCharVector__});
  test::assertEqualVectors(expected, output);
}
} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
