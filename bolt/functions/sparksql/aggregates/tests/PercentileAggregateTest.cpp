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

#include <algorithm>

#include "bolt/common/base/RandomUtil.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

// Return the argument types of an aggregation when the aggregation is
// constructed by `functionCall` with the given dataType, weighted, accuracy,
// and percentileCount.
std::vector<TypePtr>
getArgTypes(const TypePtr& dataType, bool weighted, int percentileCount) {
  std::vector<TypePtr> argTypes;
  argTypes.push_back(dataType);
  if (percentileCount == -1) {
    argTypes.push_back(DOUBLE());
  } else {
    argTypes.push_back(ARRAY(DOUBLE()));
  }
  if (weighted) {
    argTypes.push_back(BIGINT());
  }
  return argTypes;
}

std::string functionCall(
    bool keyed,
    bool weighted,
    double percentile,
    int percentileCount) {
  std::ostringstream buf;
  int columnIndex = keyed;
  buf << "percentile(c" << columnIndex++;
  buf << ", ";
  if (percentileCount == -1) {
    buf << percentile;
  } else {
    buf << "ARRAY[";
    for (int i = 0; i < percentileCount; ++i) {
      buf << (i == 0 ? "" : ",") << percentile;
    }
    buf << ']';
  }
  if (weighted) {
    buf << ", c" << columnIndex++;
  }
  buf << ')';
  return buf.str();
}

class PercentileTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    random::setSeed(0);
    allowInputShuffle();
    AggregationTestBase::disableTestIncremental();
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      std::optional<T> expectedResult) {
    SCOPED_TRACE(fmt::format(
        "weighted={} percentile={}", weights != nullptr, percentile));
    auto rows =
        weights ? makeRowVector({values, weights}) : makeRowVector({values});

    auto expected = expectedResult.has_value()
        ? fmt::format("SELECT {}", expectedResult.value())
        : "SELECT NULL";
    auto expectedArray = expectedResult.has_value()
        ? fmt::format("SELECT ARRAY[{0},{0},{0}]", expectedResult.value())
        : "SELECT NULL";

    enableTestStreaming();
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, -1)},
        expected);
    testAggregations(
        {rows},
        {},
        {functionCall(false, weights.get(), percentile, 3)},
        expectedArray);

    // Companion functions of percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, weights.get(), percentile, -1)},
        {getArgTypes(values->type(), weights.get(), -1)},
        {},
        expected);
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, weights.get(), percentile, 3)},
        {getArgTypes(values->type(), weights.get(), 3)},
        {},
        expectedArray);
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const VectorPtr& weights,
      double percentile,
      const RowVectorPtr& expectedResult) {
    auto rows = weights ? makeRowVector({keys, values, weights})
                        : makeRowVector({keys, values});
    enableTestStreaming();
    testAggregations(
        {rows},
        {"c0"},
        {functionCall(true, weights.get(), percentile, -1)},
        {expectedResult});

    // Companion functions of percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {"c0"},
        {functionCall(true, weights.get(), percentile, -1)},
        {getArgTypes(values->type(), weights.get(), -1)},
        {},
        {expectedResult});

    {
      SCOPED_TRACE("Percentile array");
      auto resultValues = expectedResult->childAt(1);
      RowVectorPtr expected = nullptr;
      auto size = resultValues->size();
      if (resultValues->nulls() &&
          bits::countNonNulls(resultValues->rawNulls(), 0, size) == 0) {
        expected = makeRowVector(
            {expectedResult->childAt(0),
             BaseVector::createNullConstant(
                 ARRAY(resultValues->type()), size, pool())});
      } else {
        auto elements = BaseVector::create(
            resultValues->type(), 3 * resultValues->size(), pool());
        auto offsets = allocateOffsets(resultValues->size(), pool());
        auto rawOffsets = offsets->asMutable<vector_size_t>();
        auto sizes = allocateSizes(resultValues->size(), pool());
        auto rawSizes = sizes->asMutable<vector_size_t>();
        for (int i = 0; i < resultValues->size(); ++i) {
          rawOffsets[i] = 3 * i;
          rawSizes[i] = 3;
          elements->copy(resultValues.get(), 3 * i + 0, i, 1);
          elements->copy(resultValues.get(), 3 * i + 1, i, 1);
          elements->copy(resultValues.get(), 3 * i + 2, i, 1);
        }
        expected = makeRowVector(
            {expectedResult->childAt(0),
             std::make_shared<ArrayVector>(
                 pool(),
                 ARRAY(elements->type()),
                 nullptr,
                 resultValues->size(),
                 offsets,
                 sizes,
                 elements)});
      }

      enableTestStreaming();
      testAggregations(
          {rows},
          {"c0"},
          {functionCall(true, weights.get(), percentile, 3)},
          {expected});

      // Companion functions of percentile do not support test streaming
      // because intermediate results are KLL that has non-deterministic
      // shape.
      disableTestStreaming();
      testAggregationsWithCompanion(
          {rows},
          [](auto& /*builder*/) {},
          {"c0"},
          {functionCall(true, weights.get(), percentile, 3)},
          {getArgTypes(values->type(), weights.get(), 3)},
          {},
          {expected});
    }
  }
};

TEST_F(PercentileTest, globalAgg) {
  vector_size_t size = 2;
  auto simpleValues =
      makeFlatVector<int32_t>(size, [](auto row) { return row * 10; });
  testGlobalAgg<double>(simpleValues, nullptr, 0.3, 3.0f);

  size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row + 1; });
  auto weights =
      makeFlatVector<int64_t>(size, [](auto row) { return row + 1; });

  testGlobalAgg<double>(values, nullptr, 0.25, 250.75);
  testGlobalAgg<double>(values, weights, 0.5, 707.0);
  testGlobalAgg<double>(values, weights, 0.75, 866.0);

  auto reverseValues =
      makeFlatVector<int32_t>(size, [](auto row) { return 1000 - row; });
  auto reverseWeights =
      makeFlatVector<int64_t>(size, [](auto row) { return 1000 - row; });

  testGlobalAgg<double>(reverseValues, nullptr, 0.25, 250.75);
  testGlobalAgg<double>(reverseValues, reverseWeights, 0.5, 707.0);
  testGlobalAgg<double>(reverseValues, reverseWeights, 0.75, 866.0);

  size = 1'250;
  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row / 5 * 4 + row % 5; }, nullEvery(5));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return row / 5 * 4 + row % 5; }, nullEvery(5));

  testGlobalAgg<double>(valuesWithNulls, nullptr, 0.25, 250.75);
  testGlobalAgg<double>(valuesWithNulls, weightsWithNulls, 0.5, 707.0);
  testGlobalAgg<double>(valuesWithNulls, weightsWithNulls, 0.75, 866.0);
}

TEST_F(PercentileTest, stringAgg) {
  vector_size_t size = 15;
  auto values = makeFlatVector<StringView>(
      {"1",
       "2",
       "3",
       "4",
       "5",
       "6",
       "7",
       "8",
       "9",
       "10",
       "A",
       "B",
       "C",
       "D",
       "R"});
  auto weights = makeFlatVector<int64_t>(size, [](auto row) { return 2; });

  testGlobalAgg<double>(values, nullptr, 0.25, 3.25);
  testGlobalAgg<double>(values, weights, 0.25, 3.0);

  auto valuesWithNulls = makeFlatVector<StringView>({"A", "B", "C"});
  auto weightsWithNulls = makeFlatVector<int64_t>({1, 2, 3});

  testGlobalAgg<double>(valuesWithNulls, nullptr, 0.25, std::nullopt);
  testGlobalAgg<double>(valuesWithNulls, weightsWithNulls, 0.25, std::nullopt);
}

TEST_F(PercentileTest, decimalAgg) {
  vector_size_t size = 10;
  auto shortValues = makeFlatVector<int64_t>(
      size,
      [](auto row) { return (row + 1) * 100000; },
      nullptr,
      DECIMAL(18, 5));
  auto weights = makeFlatVector<int64_t>(size, [](auto row) { return 2; });

  testGlobalAgg<double>(shortValues, nullptr, 0.25, 3.25);
  testGlobalAgg<double>(shortValues, weights, 0.25, 3.0);

  auto longValues = makeFlatVector<int128_t>(
      size,
      [](auto row) { return (row + 1) * 100000; },
      nullptr,
      DECIMAL(38, 5));
  testGlobalAgg<double>(shortValues, nullptr, 0.25, 3.25);
  testGlobalAgg<double>(shortValues, weights, 0.25, 3.0);
}

TEST_F(PercentileTest, doubleAgg) {
  const std::vector<double> rawData{
      8814.79, 7960.1,    8492.04,   6837.15,     8429.88, 6034.46, 4464.18,
      6628.85, 7465.74,   8335.13,   7883.56,     6673.07, 7160.19, 7303.34,
      6371.66, 7277.8,    4463.77,   8233.78,     8605.39, 8370.71, 6054.61,
      4520.48, 5278.29,   6743.67,   8697.81,     8828.3,  6975.8,  5624.5,
      5544.61, 6099.35,   6562.43,   6507.85,     4379.5,  7273.95, 6862.91,
      6767.65, 6664.36,   0.0,       5624.5,      7143.87, 6277.58, 5624.5,
      7123.34, 6662.73,   7083.62,   4863.93,     0.0,     6831.25, 6827.66,
      6179.52, 6111.47,   6430.53,   644.48,      7262.85, 6425.22, 6804.15,
      0.0,     0.0 / 0.0, 0.0 / 0.0, std::nan("")};

  const vector_size_t size = rawData.size();
  auto vec = makeFlatVector<double>(
      size, [rawData](auto row) { return rawData[row]; }, nullptr, DOUBLE());

  testGlobalAgg<double>(vec, nullptr, 0.9, 8614.632);
}

TEST_F(PercentileTest, groupByAgg) {
  vector_size_t size = 5'000;
  auto keys =
      makeFlatVector<int32_t>(size, [](auto row) { return row / 1000; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 1000 + 1; });
  auto weights =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 1000 + 1; });

  auto expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(
           std::vector<double>{250.75, 250.75, 250.75, 250.75, 250.75})});
  testGroupByAgg(keys, values, nullptr, 0.25, expectedResult);

  auto expectedResultWithWeight = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(std::vector<double>{707.0, 707.0, 707.0, 707.0, 707.0})});
  testGroupByAgg(keys, values, weights, 0.5, expectedResultWithWeight);

  auto expectedResultWithWeightSecond = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(std::vector<double>{866.0, 866.0, 866.0, 866.0, 866.0})});
  testGroupByAgg(keys, values, weights, 0.75, expectedResultWithWeightSecond);
  testGroupByAgg(keys, values, weights, 0.75, expectedResultWithWeightSecond);

  size = 1250 * 5;
  auto keysWithNulls =
      makeFlatVector<int32_t>(size, [](auto row) { return row / 1250; });
  auto valuesWithNulls = makeFlatVector<int32_t>(
      size,
      [](auto row) { return row % 1250 / 5 * 4 + row % 1250 % 5; },
      nullEvery(5));
  auto weightsWithNulls = makeFlatVector<int64_t>(
      size,
      [](auto row) { return row % 1250 / 5 * 4 + row % 1250 % 5; },
      nullEvery(5));

  auto expectedResultWithNulls = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(
           std::vector<double>{250.75, 250.75, 250.75, 250.75, 250.75})});
  testGroupByAgg(
      keysWithNulls, valuesWithNulls, nullptr, 0.25, expectedResultWithNulls);

  auto expectedResultWithWeightNulls = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(std::vector<double>{707.0, 707.0, 707.0, 707.0, 707.0})});
  testGroupByAgg(
      keysWithNulls,
      valuesWithNulls,
      weightsWithNulls,
      0.5,
      expectedResultWithWeightNulls);

  auto expectedResultWithWeightNullsSecond = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4}),
       makeFlatVector(std::vector<double>{866.0, 866.0, 866.0, 866.0, 866.0})});
  testGroupByAgg(
      keysWithNulls,
      valuesWithNulls,
      weightsWithNulls,
      0.75,
      expectedResultWithWeightNullsSecond);
}

TEST_F(PercentileTest, partialFull) {
  // Make sure partial aggregation runs out of memory after first batch.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe({
      {core::QueryConfig::kMaxPartialAggregationMemory, "300000"},
  });

  auto data = {
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 10; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 15; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 20; }),
      }),
  };

  params.planNode = PlanBuilder()
                        .values(data)
                        .project({"c0", "c1", "0.25", "1"})
                        .partialAggregation({"c0"}, {"percentile(c1, p2, p3)"})
                        .finalAggregation()
                        .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(128, [](auto row) { return row; }),
      makeFlatVector<double>(128, [](auto row) { return 10; }),
  });
  exec::test::assertQuery(params, {expected});
}

TEST_F(PercentileTest, invalidWeight) {
  constexpr int64_t kMinWeight = 1;
  auto makePlan = [&](int64_t weight, bool grouped) {
    auto rows = makeRowVector({
        makeConstant<int32_t>(0, 1),
        makeConstant<int64_t>(weight, 1),
        makeConstant<int32_t>(1, 1),
    });
    std::vector<std::string> groupingKeys;
    if (grouped) {
      groupingKeys.push_back("c2");
    }
    return PlanBuilder()
        .values({rows})
        .singleAggregation(groupingKeys, {"percentile(c0, 0.5, c1)"})
        .planNode();
  };
  assertQuery(makePlan(kMinWeight, false), "SELECT 0");
  assertQuery(makePlan(kMinWeight, true), "SELECT 1, 0");
  for (bool grouped : {false, true}) {
    AssertQueryBuilder badQuery(makePlan(kMinWeight - 1, grouped));
    BOLT_ASSERT_THROW(
        badQuery.copyResults(pool()), "weight must be positive, got 0");
  }
}

TEST_F(PercentileTest, noInput) {
  const int size = 1000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });
  auto weights =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; });
  auto nullValues = makeNullConstant(TypeKind::INTEGER, size);
  auto nullWeights = makeNullConstant(TypeKind::BIGINT, size);

  // Test global.
  {
    testGlobalAgg<int32_t>(nullValues, nullptr, 0.5, std::nullopt);
    testGlobalAgg<int32_t>(nullValues, weights, 0.5, std::nullopt);
    testGlobalAgg<int32_t>(values, nullWeights, 0.5, std::nullopt);
    testGlobalAgg<int32_t>(nullValues, nullWeights, 0.5, std::nullopt);
  }

  // Test group-by.
  {
    auto expected = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6}),
         makeNullConstant(TypeKind::DOUBLE, 7)});

    testGroupByAgg(keys, nullValues, nullptr, 0.5, expected);
    testGroupByAgg(keys, nullValues, weights, 0.5, expected);
    testGroupByAgg(keys, values, nullWeights, 0.5, expected);
    testGroupByAgg(keys, nullValues, nullWeights, 0.5, expected);
  }

  // Test when all inputs are masked out.
  {
    auto testWithMask = [&](bool groupBy, const RowVectorPtr& expected) {
      std::vector<std::string> groupingKeys;
      if (groupBy) {
        groupingKeys.push_back("c0");
      }
      auto plan = PlanBuilder()
                      .values({makeRowVector({keys, values, weights})})
                      .project(
                          {"c0",
                           "c1",
                           "c2",
                           "array_constructor(0.5) as pct",
                           "c1 > 6 as m1"})
                      .singleAggregation(
                          groupingKeys,
                          {"percentile(c1, 0.5)",
                           "percentile(c1, 0.5, c2)",
                           "percentile(c1, pct)",
                           "percentile(c1, pct, c2)"},
                          {"m1", "m1", "m1", "m1"})
                      .planNode();

      AssertQueryBuilder(plan).assertResults(expected);
    };

    // Global.
    std::vector<VectorPtr> children{4};
    std::fill_n(children.begin(), 2, makeNullConstant(TypeKind::DOUBLE, 1));
    std::fill_n(
        children.begin() + 2,
        2,
        BaseVector::createNullConstant(ARRAY(DOUBLE()), 1, pool()));
    auto expected1 = makeRowVector(children);
    testWithMask(false, expected1);

    // Group-by.
    children.resize(5);
    children[0] = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6});
    std::fill_n(children.begin() + 1, 2, makeNullConstant(TypeKind::DOUBLE, 7));
    std::fill_n(
        children.begin() + 3,
        2,
        BaseVector::createNullConstant(ARRAY(DOUBLE()), 7, pool()));
    auto expected2 = makeRowVector(children);
    testWithMask(true, expected2);
  }
}

TEST_F(PercentileTest, nullPercentile) {
  auto values = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto percentileOfDouble = makeConstant<double>(std::nullopt, 4);
  auto rows = makeRowVector({values, percentileOfDouble});

  // Test null percentile for percentile(value, percentile).
  BOLT_ASSERT_THROW(
      testAggregations({rows}, {}, {"percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");

  auto percentileOfArrayOfDouble = BaseVector::wrapInConstant(
      4,
      0,
      makeNullableArrayVector<double>(
          std::vector<std::vector<std::optional<double>>>{
              {std::nullopt, std::nullopt, std::nullopt, std::nullopt}}));
  rows = makeRowVector({values, percentileOfArrayOfDouble});

  // Test null percentile for percentile(value, percentiles).
  BOLT_ASSERT_THROW(
      testAggregations({rows}, {}, {"percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
