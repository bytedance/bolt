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
#include <cmath>
#include <limits>
#include <optional>

#include "bolt/common/base/RandomUtil.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

// Return the argument types of an aggregation when the aggregation is
// constructed by `functionCall` with the given dataType, accuracy,
// and percentileCount.
std::vector<TypePtr>
getArgTypes(const TypePtr& dataType, int64_t accuracy, int percentileCount) {
  std::vector<TypePtr> argTypes;
  argTypes.push_back(dataType);
  if (percentileCount == -1) {
    argTypes.push_back(DOUBLE());
  } else {
    argTypes.push_back(ARRAY(DOUBLE()));
  }
  if (accuracy > 0) {
    argTypes.push_back(BIGINT());
  }
  return argTypes;
}

std::string functionCall(
    bool keyed,
    double percentile,
    int64_t accuracy,
    int percentileCount) {
  std::ostringstream buf;
  int columnIndex = keyed;
  buf << "percentile_approx(c" << columnIndex++;
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
  if (accuracy > 0) {
    buf << ", " << accuracy;
  }
  buf << ')';
  return buf.str();
}

class PercentileApproxTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    random::setSeed(0);
    allowInputShuffle();
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      double percentile,
      int64_t accuracy,
      std::optional<T> expectedResult) {
    SCOPED_TRACE(
        fmt::format(" percentile={} accuracy={}", percentile, accuracy));
    auto rows = makeRowVector({values});

    auto expected = expectedResult.has_value()
        ? fmt::format("SELECT {}", expectedResult.value())
        : "SELECT NULL";
    auto expectedArray = expectedResult.has_value()
        ? fmt::format("SELECT ARRAY[{0},{0},{0}]", expectedResult.value())
        : "SELECT NULL";
    enableTestStreaming();
    testAggregations(
        {rows}, {}, {functionCall(false, percentile, accuracy, -1)}, expected);
    testAggregations(
        {rows},
        {},
        {functionCall(false, percentile, accuracy, 3)},
        expectedArray);
    // Companion functions of percentile_approx do not support test
    // streaming because intermediate results are KLL that has non-deterministic
    // shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, percentile, accuracy, -1)},
        {getArgTypes(values->type(), accuracy, -1)},
        {},
        expected);
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, percentile, accuracy, 3)},
        {getArgTypes(values->type(), accuracy, 3)},
        {},
        expectedArray);
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      double percentile,
      int64_t accuracy,
      const RowVectorPtr& expectedResult) {
    auto rows = makeRowVector({keys, values});
    enableTestStreaming();
    testAggregations(
        {rows},
        {"c0"},
        {functionCall(true, percentile, accuracy, -1)},
        {expectedResult});

    // Companion functions of percentile_approx do not support test
    // streaming because intermediate results are KLL that has non-deterministic
    // shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {"c0"},
        {functionCall(true, percentile, accuracy, -1)},
        {getArgTypes(values->type(), accuracy, -1)},
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
          {functionCall(true, percentile, accuracy, 3)},
          {expected});

      // Companion functions of percentile_approx do not support test
      // streaming because intermediate results are KLL that has
      // non-deterministic shape.
      disableTestStreaming();
      testAggregationsWithCompanion(
          {rows},
          [](auto& /*builder*/) {},
          {"c0"},
          {functionCall(true, percentile, accuracy, 3)},
          {getArgTypes(values->type(), accuracy, 3)},
          {},
          {expected});
    }
  }
};

TEST_F(PercentileApproxTest, globalAgg) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 23; });

  testGlobalAgg<int32_t>(values, 0.5, 10000, 11);
  testGlobalAgg<int32_t>(values, 0.5, 2000, 11);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 23; }, nullEvery(7));

  testGlobalAgg<int32_t>(valuesWithNulls, 0.5, 10000, 11);
  testGlobalAgg<int32_t>(valuesWithNulls, 0.5, 2000, 11);
}

TEST_F(PercentileApproxTest, groupByAgg) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; });

  auto expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{11, 12, 13, 14, 15, 16, 17})});
  testGroupByAgg(keys, values, 0.5, 1000, expectedResult);
  testGroupByAgg(keys, values, 0.5, 2000, expectedResult);

  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return (row / 7) % 23 + row % 7; }, nullEvery(11));

  expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}),
       makeFlatVector(std::vector<int32_t>{10, 11, 13, 14, 14, 15, 17})});
  testGroupByAgg(keys, valuesWithNulls, 0.5, 10000, expectedResult);
  testGroupByAgg(keys, valuesWithNulls, 0.5, 2000, expectedResult);
}

/// Repro of "decodedPercentile_.isConstantMapping() Percentile argument must be
/// constant for all input rows" error caused by (1) HashAggregation keeping a
/// reference to input vectors when partial aggregation ran out of memory; (2)
/// EvalCtx::moveOrCopyResult needlessly flattening constant vector result of a
/// constant expression.
TEST_F(PercentileApproxTest, partialFull) {
  // Make sure partial aggregation runs out of memory after first batch.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe({
      {core::QueryConfig::kMaxPartialAggregationMemory, "300000"},
  });

  auto data = {
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 117; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 10; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 15; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 7; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 20; }),
      }),
  };

  params.planNode =
      PlanBuilder()
          .values(data)
          .project({"c0", "c1", "0.9995", "1000"})
          .partialAggregation({"c0"}, {"percentile_approx(c1, p2, p3)"})
          .finalAggregation()
          .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(117, [](auto row) { return row; }),
      makeFlatVector<int32_t>(117, [](auto row) { return row < 7 ? 20 : 10; }),
  });
  exec::test::assertQuery(params, {expected});
}

TEST_F(PercentileApproxTest, percentileArrayWithNaNIsNonDecreasing) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  const std::vector<double> rawData{
      8814.79, 7960.1,  8492.04, 6837.15, 8429.88, 6034.46, 4464.18, 6628.85,
      7465.74, 8335.13, 7883.56, 6673.07, 7160.19, 7303.34, 6371.66, 7277.8,
      4463.77, 8233.78, 8605.39, 8370.71, 6054.61, 4520.48, 5278.29, 6743.67,
      8697.81, 8828.3,  6975.8,  5624.5,  5544.61, 6099.35, 6562.43, 6507.85,
      4379.5,  7273.95, 6862.91, 6767.65, 6664.36, 0.0,     5624.5,  7143.87,
      6277.58, 5624.5,  7123.34, 6662.73, 7083.62, 4863.93, 0.0,     6831.25,
      6827.66, 6179.52, 6111.47, 6430.53, 644.48,  7262.85, 6425.22, 6804.15,
      0.0,     nan,     nan,     nan};

  auto rows = makeRowVector({makeFlatVector<double>(rawData)});
  auto plan = PlanBuilder()
                  .values({rows})
                  .project(
                      {"c0",
                       "array_constructor(0.10, 0.20, 0.30, 0.40, 0.50, "
                       "0.60, 0.70, 0.80, 0.90, 0.91, 0.92, 0.93, "
                       "0.94, 0.95, 0.96, 0.97, 0.98, 0.99) as pct"})
                  .singleAggregation({}, {"percentile_approx(c0, pct)"})
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  ASSERT_EQ(result->size(), 1);

  auto arrayResult = result->childAt(0)->asUnchecked<ArrayVector>();
  ASSERT_FALSE(arrayResult->isNullAt(0));
  ASSERT_EQ(arrayResult->sizeAt(0), 18);

  auto elements = arrayResult->elements()->asUnchecked<FlatVector<double>>();
  const auto offset = arrayResult->offsetAt(0);
  bool seenNaN = false;
  std::optional<double> previous;
  for (vector_size_t i = 0; i < arrayResult->sizeAt(0); ++i) {
    auto current = elements->valueAt(offset + i);
    if (std::isnan(current)) {
      seenNaN = true;
      continue;
    }
    ASSERT_FALSE(seenNaN) << "finite value after NaN at index: " << i;
    if (previous.has_value()) {
      ASSERT_LE(previous.value(), current) << "index: " << i;
    }
    previous = current;
  }
}

TEST_F(PercentileApproxTest, finalAggregateAccuracy) {
  auto batch = makeRowVector(
      {makeFlatVector<int32_t>(1000, [](auto row) { return row; })});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<std::shared_ptr<const core::PlanNode>> sources;
  for (int i = 0; i < 10; ++i) {
    sources.push_back(
        PlanBuilder(planNodeIdGenerator)
            .values({batch})
            .partialAggregation({}, {"percentile_approx(c0, 0.005, 100000)"})
            .planNode());
  }
  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartitionRoundRobin(sources)
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT 4");
}

TEST_F(PercentileApproxTest, invalidEncoding) {
  auto indices = AlignedBuffer::allocate<vector_size_t>(3, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  std::iota(rawIndices, rawIndices + indices->size(), 0);
  auto percentiles = std::make_shared<ArrayVector>(
      pool(),
      ARRAY(DOUBLE()),
      nullptr,
      1,
      AlignedBuffer::allocate<vector_size_t>(1, pool(), 0),
      AlignedBuffer::allocate<vector_size_t>(1, pool(), 3),
      BaseVector::wrapInDictionary(
          nullptr, indices, 3, makeFlatVector<double>({0, 0.5, 1})));
  auto rows = makeRowVector({
      makeFlatVector<int32_t>(10, folly::identity),
      BaseVector::wrapInConstant(1, 0, percentiles),
  });
  auto plan = PlanBuilder()
                  .values({rows})
                  .singleAggregation({}, {"percentile_approx(c0, c1)"})
                  .planNode();
  AssertQueryBuilder assertQuery(plan);
  BOLT_ASSERT_THROW(
      assertQuery.copyResults(pool()),
      "Only flat encoding is allowed for percentile array elements");
}

TEST_F(PercentileApproxTest, noInput) {
  const int size = 1000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });
  auto nullValues = makeNullConstant(TypeKind::INTEGER, size);
  // Test global.
  { testGlobalAgg<int32_t>(nullValues, 0.5, 10000, std::nullopt); }

  // Test group-by.
  {
    auto expected = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6}),
         makeNullConstant(TypeKind::INTEGER, 7)});

    testGroupByAgg(keys, nullValues, 0.5, 10000, expected);
  }

  // Test when all inputs are masked out.
  {
    auto testWithMask = [&](bool groupBy, const RowVectorPtr& expected) {
      std::vector<std::string> groupingKeys;
      if (groupBy) {
        groupingKeys.push_back("c0");
      }
      auto plan =
          PlanBuilder()
              .values({makeRowVector({keys, values})})
              .project(
                  {"c0", "c1", "array_constructor(0.5) as pct", "c1 > 6 as m1"})
              .singleAggregation(
                  groupingKeys,
                  {"percentile_approx(c1, 0.5)",
                   "percentile_approx(c1, 0.5, 5)",
                   "percentile_approx(c1, pct)",
                   "percentile_approx(c1, pct, 5)"},
                  {"m1", "m1", "m1", "m1"})
              .planNode();

      AssertQueryBuilder(plan).assertResults(expected);
    };

    // Global.
    std::vector<VectorPtr> children{4};
    std::fill_n(children.begin(), 2, makeNullConstant(TypeKind::INTEGER, 1));
    std::fill_n(
        children.begin() + 2,
        2,
        BaseVector::createNullConstant(ARRAY(INTEGER()), 1, pool()));
    auto expected1 = makeRowVector(children);
    testWithMask(false, expected1);

    // Group-by.
    children.resize(5);
    children[0] = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6});
    std::fill_n(
        children.begin() + 1, 2, makeNullConstant(TypeKind::INTEGER, 7));
    std::fill_n(
        children.begin() + 3,
        2,
        BaseVector::createNullConstant(ARRAY(INTEGER()), 7, pool()));
    auto expected2 = makeRowVector(children);
    testWithMask(true, expected2);
  }
}

TEST_F(PercentileApproxTest, nullPercentile) {
  auto values = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto percentileOfDouble = makeConstant<double>(std::nullopt, 4);
  auto rows = makeRowVector({values, percentileOfDouble});

  // Test null percentile for percentile_approx(value, percentile).
  BOLT_ASSERT_THROW(
      testAggregations(
          {rows}, {}, {"percentile_approx(c0, c1)"}, "SELECT NULL"),
      "Percentage value must not be null");

  auto percentileOfArrayOfDouble = BaseVector::wrapInConstant(
      4,
      0,
      makeNullableArrayVector<double>(
          std::vector<std::vector<std::optional<double>>>{
              {std::nullopt, std::nullopt, std::nullopt, std::nullopt}}));
  rows = makeRowVector({values, percentileOfArrayOfDouble});

  // Test null percentile for percentile_approx(value, percentiles).
  BOLT_ASSERT_THROW(
      testAggregations(
          {rows}, {}, {"percentile_approx(c0, c1)"}, "SELECT NULL"),
      "Percentage value must not be null");
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
