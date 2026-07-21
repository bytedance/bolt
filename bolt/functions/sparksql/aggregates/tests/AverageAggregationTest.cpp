/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

class AverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(AverageAggregationTest, avgAllNulls) {
  vector_size_t size = 1'000;
  // Have two row vectors a least as it triggers different code paths.
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({
          makeAllNullFlatVector<int64_t>(size),
      }),
      makeRowVector({
          makeAllNullFlatVector<int64_t>(size),
      }),
  };
  testAggregations(vectors, {}, {"spark_avg(c0)"}, "SELECT NULL");

  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"spark_avg(c0)"})
                  .planNode();
  assertQuery(plan, "SELECT row(0, 0)");

  // Average with grouping key.
  // Have at least two row vectors as it triggers different code paths.
  vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullableFlatVector<int64_t>(
              {std::nullopt,
               std::nullopt,
               2,
               std::nullopt,
               10,
               9,
               std::nullopt,
               25,
               12,
               std::nullopt}),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullableFlatVector<int64_t>(
              {std::nullopt,
               10,
               20,
               std::nullopt,
               std::nullopt,
               25,
               std::nullopt,
               16,
               21,
               std::nullopt}),
      }),
  };
  createDuckDbTable(vectors);
  testAggregations(
      vectors,
      {"c0"},
      {"spark_avg(c1)"},
      "SELECT c0, avg(c1) FROM tmp GROUP BY c0");

  plan = PlanBuilder()
             .values(vectors)
             .partialAggregation({"c0"}, {"spark_avg(c1)"})
             .planNode();
  auto expected = makeRowVector(
      {"c0", "c1"},
      {
          makeFlatVector<int64_t>({0, 1, 2}),
          makeRowVector(
              {"sum", "count"},
              {
                  makeFlatVector<double>({0, 61, 89}),
                  makeFlatVector<int64_t>({0, 4, 6}),
              }),
      });
  assertQuery(plan, expected);
}

// column writer doesn't support HUGEINT
TEST_F(AverageAggregationTest, avgDecimal) {
  int64_t kRescale = DecimalUtil::kPowersOfTen[4];
  // Short decimal aggregation
  auto shortDecimal = makeNullableFlatVector<int64_t>(
      {1'000, 2'000, 3'000, 4'000, 5'000, std::nullopt}, DECIMAL(10, 1));
  testAggregations(
      {makeRowVector({shortDecimal})},
      {},
      {"spark_avg(c0)"},
      {},
      {makeRowVector({makeNullableFlatVector<int64_t>(
          {3'000 * kRescale}, DECIMAL(14, 5))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Long decimal aggregation
  //   testAggregations(
  //       {makeRowVector({makeNullableFlatVector<int128_t>(
  //           {HugeInt::build(10, 100),
  //            HugeInt::build(10, 200),
  //            HugeInt::build(10, 300),
  //            HugeInt::build(10, 400),
  //            HugeInt::build(10, 500),
  //            std::nullopt},
  //           DECIMAL(23, 4))})},
  //       {},
  //       {"spark_avg(c0)"},
  //       {},
  //       {makeRowVector({makeFlatVector(
  //           std::vector<int128_t>{HugeInt::build(10, 300) * kRescale},
  //           DECIMAL(27, 8))})});

  // The total sum overflows the max int128_t limit.
  //   std::vector<int128_t> rawVector;
  //   for (int i = 0; i < 10; ++i) {
  //     rawVector.push_back(DecimalUtil::kLongDecimalMax);
  //   }
  //   testAggregations(
  //       {makeRowVector({makeFlatVector<int128_t>(rawVector, DECIMAL(38,
  //       0))})},
  //       {},
  //       {"spark_avg(c0)"},
  //       {},
  //       {makeRowVector({makeNullableFlatVector(
  //           std::vector<std::optional<int128_t>>{std::nullopt},
  //           DECIMAL(38, 4))})});

  // The total sum underflows the min int128_t limit.
  //   rawVector.clear();
  //   auto underFlowTestResult = makeNullableFlatVector(
  //       std::vector<std::optional<int128_t>>{std::nullopt}, DECIMAL(38, 4));
  //   for (int i = 0; i < 10; ++i) {
  //     rawVector.push_back(DecimalUtil::kLongDecimalMin);
  //   }
  //   testAggregations(
  //       {makeRowVector({makeFlatVector<int128_t>(rawVector, DECIMAL(38,
  //       0))})},
  //       {},
  //       {"spark_avg(c0)"},
  //       {},
  //       {makeRowVector({underFlowTestResult})});

  // Test constant vector.
  testAggregations(
      {makeRowVector({makeConstant<int64_t>(100, 10, DECIMAL(10, 2))})},
      {},
      {"spark_avg(c0)"},
      {},
      {makeRowVector({makeFlatVector(
          std::vector<int64_t>{100 * kRescale}, DECIMAL(14, 6))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  auto newSize = shortDecimal->size() * 2;
  auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
  auto dictVector =
      VectorTestBase::wrapInDictionary(indices, newSize, shortDecimal);

  testAggregations(
      {makeRowVector({dictVector})},
      {},
      {"spark_avg(c0)"},
      {},
      {makeRowVector({makeFlatVector(
          std::vector<int64_t>{3'000 * kRescale}, DECIMAL(14, 5))})},
      /*config*/ {},
      /*testWithTableScan*/ false);

  // Decimal average aggregation with multiple groups.
  auto inputRows = {
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1, 1}),
           makeFlatVector<int64_t>({37220, 53450}, DECIMAL(15, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2, 2}),
           makeFlatVector<int64_t>({10410, 9250}, DECIMAL(15, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({3, 3}),
           makeFlatVector<int64_t>({-12783, 0}, DECIMAL(15, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1, 2}),
           makeFlatVector<int64_t>({23178, 41093}, DECIMAL(15, 2))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2, 3}),
           makeFlatVector<int64_t>({-10023, 5290}, DECIMAL(15, 2))}),
  };

  auto expectedResult = {
      makeRowVector(
          {makeNullableFlatVector<int32_t>({1}),
           makeFlatVector(std::vector<int128_t>{379493333}, DECIMAL(19, 6))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({2}),
           makeFlatVector(std::vector<int128_t>{126825000}, DECIMAL(19, 6))}),
      makeRowVector(
          {makeNullableFlatVector<int32_t>({3}),
           makeFlatVector(std::vector<int128_t>{-24976667}, DECIMAL(19, 6))})};

  testAggregations(
      inputRows,
      {"c0"},
      {"spark_avg(c1)"},
      expectedResult,
      /*config*/ {},
      /*testWithTableScan*/ false);
}

TEST_F(AverageAggregationTest, avgDecimalWithMultipleRowVectors) {
  int64_t kRescale = DecimalUtil::kPowersOfTen[4];
  auto inputRows = {
      makeRowVector({makeFlatVector<int64_t>({100, 200}, DECIMAL(15, 2))}),
      makeRowVector({makeFlatVector<int64_t>({300, 400}, DECIMAL(15, 2))}),
      makeRowVector({makeFlatVector<int64_t>({500, 600}, DECIMAL(15, 2))}),
  };

  auto expectedResult = {makeRowVector(
      {makeFlatVector(std::vector<int128_t>{350 * kRescale}, DECIMAL(19, 6))})};

  testAggregations(
      inputRows,
      {},
      {"spark_avg(c0)"},
      expectedResult,
      /*config*/ {},
      /*testWithTableScan*/ false);
}

TEST_F(AverageAggregationTest, rowBasedSpillDecimalAvg) {
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < 100; ++i) {
    size_t rowCount = 1000;
    vectors.emplace_back(makeRowVector(
        {makeFlatVector<std::string>(
             rowCount,
             [&](auto row) {
               return "sssssssssssss" + std::to_string(i * rowCount + row);
             }),
         makeFlatVector<int128_t>(
             rowCount, [](auto row) { return 0; }, nullptr, DECIMAL(38, 18))}));
  }

  std::unordered_map<std::string, std::string> config = {
      {core::QueryConfig::kSpillEnabled, "true"},
      {core::QueryConfig::kAggregationSpillEnabled, "true"},
      {core::QueryConfig::kTestingSpillPct, "100"},
      {core::QueryConfig::kAggregationSpillMemoryThreshold, "100"}};

  allowInputShuffle();
  testAggregations(
      {vectors},
      {"c0"},
      {"spark_avg(c1)"},
      {vectors},
      {},
      /*testWithTableScan*/ false);
  disallowInputShuffle();
}

TEST_F(AverageAggregationTest, rowBasedSpillDecimalAvgPartialOutputType) {
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < 4; ++i) {
    constexpr vector_size_t rowCount = 1024;
    vectors.emplace_back(makeRowVector(
        {makeFlatVector<int32_t>(
             rowCount, [&](auto row) { return i * rowCount + row; }),
         makeFlatVector<int128_t>(
             rowCount,
             [](auto row) { return HugeInt::build(0, row + 1); },
             nullptr,
             DECIMAL(32, 12))}));
  }

  auto source = PlanBuilder().values(vectors).planNode();
  auto key =
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto input =
      std::make_shared<const core::FieldAccessTypedExpr>(DECIMAL(32, 12), "c1");
  auto outputType = ROW({DECIMAL(38, 12), BIGINT()});
  // Match Substrait plans where Spark partial avg exposes an expanded decimal
  // sum buffer. The companion signature must resolve the same intermediate
  // type.
  core::AggregationNode::Aggregate aggregate{
      std::make_shared<const core::CallTypedExpr>(
          outputType,
          std::vector<core::TypedExprPtr>{input},
          "spark_avg_partial"),
      {DECIMAL(32, 12)},
      nullptr,
      {},
      {}};
  auto plan = std::make_shared<core::AggregationNode>(
      "partial_avg_decimal_repro",
      core::AggregationNode::Step::kPartial,
      std::vector<core::FieldAccessTypedExprPtr>{key},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"avg"},
      std::vector<core::AggregationNode::Aggregate>{aggregate},
      false,
      source);

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::shared_ptr<exec::Task> task;
  auto result =
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kAggregationSpillEnabled, true)
          .config(core::QueryConfig::kTestingSpillPct, "100")
          .config(core::QueryConfig::kRowBasedSpillMode, "compression")
          .config(core::QueryConfig::kPartialAggregationSpillMaxPct, "100")
          .config(
              core::QueryConfig::kAbandonPartialAggregationMinFinalPct, "100")
          .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "100")
          .config(core::QueryConfig::kAggregationSpillMemoryThreshold, "1")
          .config(core::QueryConfig::kAbandonPartialAggregationMinRows, "50")
          .copyResults(pool(), task);

  ASSERT_EQ(result->size(), 4 * 1024);
  ASSERT_TRUE(
      result->type()->equivalent(*ROW({"c0", "avg"}, {INTEGER(), outputType})));

  int64_t spilledBytes = 0;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      spilledBytes += op.spilledBytes;
    }
  }
  ASSERT_GT(spilledBytes, 0);
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
