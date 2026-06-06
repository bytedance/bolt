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

#include <vector/ComplexVector.h>
#include <limits>
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

namespace {

std::string min(const std::string& column) {
  return fmt::format("min({})", column);
}

std::string max(const std::string& column) {
  return fmt::format("max({})", column);
}

class MinMaxTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  std::vector<RowVectorPtr> fuzzData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    options.nullRatio = 0.1;
    VectorFuzzer fuzzer(options, pool());
    std::vector<RowVectorPtr> vectors(10);
    for (auto i = 0; i < 10; ++i) {
      vectors[i] = fuzzer.fuzzInputRow(rowType);
    }
    return vectors;
  }

  template <typename TAgg>
  void
  doTest(TAgg agg, const TypePtr& inputType, bool testWithTableScan = true) {
    auto rowType = ROW({"c0", "c1", "mask"}, {BIGINT(), inputType, BOOLEAN()});
    auto vectors = fuzzData(rowType);
    doTest(agg, vectors, testWithTableScan);
  }

  template <typename TAgg>
  void doTest(
      TAgg agg,
      std::vector<RowVectorPtr>& vectors,
      bool testWithTableScan = true) {
    createDuckDbTable(vectors);

    static const std::string c0 = "c0";
    static const std::string c1 = "c1";
    static const std::string a0 = "a0";

    // Global aggregation.
    testAggregations(
        vectors,
        {},
        {agg(c1)},
        fmt::format("SELECT {} FROM tmp", agg(c1)),
        /*config*/ {},
        testWithTableScan);

    // Group by aggregation.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1"});
        },
        {"p0"},
        {agg(c1)},
        fmt::format("SELECT c0 % 10, {} FROM tmp GROUP BY 1", agg(c1)),
        /*config*/ {},
        testWithTableScan);

    // Masked aggregations.
    auto maskedAgg = agg(c1) + " filter (where mask)";
    testAggregations(
        vectors,
        {},
        {maskedAgg},
        fmt::format("SELECT {} FROM tmp", maskedAgg),
        /*config*/ {},
        testWithTableScan);

    testAggregations(
        [&](auto& builder) {
          builder.values(vectors).project({"c0 % 10", "c1", "mask"});
        },
        {"p0"},
        {maskedAgg},
        fmt::format("SELECT c0 % 10, {} FROM tmp GROUP BY 1", maskedAgg),
        /*config*/ {},
        testWithTableScan);

    // Encodings: use filter to wrap aggregation inputs in a dictionary.
    testAggregations(
        [&](auto& builder) {
          builder.values(vectors)
              .filter("c0 % 2 = 0")
              .project({"c0 % 11", "c1"});
        },
        {"p0"},
        {agg(c1)},
        fmt::format(
            "SELECT c0 % 11, {} FROM tmp WHERE c0 % 2 = 0 GROUP BY 1", agg(c1)),
        /*config*/ {},
        testWithTableScan);

    testAggregations(
        [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
        {},
        {agg(c1)},
        fmt::format("SELECT {} FROM tmp WHERE c0 % 2 = 0", agg(c1)),
        /*config*/ {},
        testWithTableScan);
  }
};

TEST_F(MinMaxTest, maxTinyint) {
  doTest(max, TINYINT());
}

TEST_F(MinMaxTest, maxSmallint) {
  doTest(max, SMALLINT());
}

TEST_F(MinMaxTest, maxInteger) {
  doTest(max, INTEGER());
}

TEST_F(MinMaxTest, maxBigint) {
  doTest(max, BIGINT());
}

TEST_F(MinMaxTest, maxReal) {
  doTest(max, REAL());
}

TEST_F(MinMaxTest, maxDouble) {
  doTest(max, DOUBLE());
}

TEST_F(MinMaxTest, maxVarchar) {
  doTest(max, VARCHAR());
}

TEST_F(MinMaxTest, maxBoolean) {
  doTest(max, BOOLEAN());
}

TEST_F(MinMaxTest, maxInterval) {
  doTest(max, INTERVAL_DAY_TIME());
}

TEST_F(MinMaxTest, minTinyint) {
  doTest(min, TINYINT());
}

TEST_F(MinMaxTest, minSmallint) {
  doTest(min, SMALLINT());
}

TEST_F(MinMaxTest, minInteger) {
  doTest(min, INTEGER());
}

TEST_F(MinMaxTest, minBigint) {
  doTest(min, BIGINT());
}

TEST_F(MinMaxTest, minReal) {
  doTest(min, REAL());
}

TEST_F(MinMaxTest, minDouble) {
  doTest(min, DOUBLE());
}

TEST_F(MinMaxTest, minInterval) {
  doTest(min, INTERVAL_DAY_TIME());
}

TEST_F(MinMaxTest, minVarchar) {
  doTest(min, VARCHAR());
}

TEST_F(MinMaxTest, minBoolean) {
  doTest(min, BOOLEAN());
}

TEST_F(MinMaxTest, hashAggrJitBooleanMinMax) {
  auto data = makeRowVector(
      {makeFlatVector<int64_t>(256, [](auto row) { return row % 13; }),
       makeFlatVector<bool>(
           256,
           [](auto row) { return row % 5 < 2; },
           [](auto row) { return row % 11 == 0; })});

  auto plan = PlanBuilder()
                  .values({data})
                  .partialAggregation({"c0"}, {"min(c1)", "max(c1)"})
                  .finalAggregation()
                  .planNode();

  auto noJit = AssertQueryBuilder(plan, duckDbQueryRunner_)
                   .config(core::QueryConfig::kHashAggrJitEnabled, "false")
                   .copyResults(pool());
  auto jit = AssertQueryBuilder(plan, duckDbQueryRunner_)
                 .config(core::QueryConfig::kHashAggrJitEnabled, "true")
                 .config(core::QueryConfig::kHashAggrJitMinFuseWidth, "1")
                 .copyResults(pool());
  assertEqualResults({noJit}, {jit});
}

TEST_F(MinMaxTest, constVarchar) {
  // Create two batches of the source data for the aggregation:
  // Column c0 with 1K of "apple" and 1K of "banana".
  // Column c1 with 1K of nulls and 1K of nulls.
  auto constVectors = {
      makeRowVector(
          {makeConstant("apple", 1'000),
           makeNullConstant(TypeKind::VARCHAR, 1'000)}),
      makeRowVector({
          makeConstant("banana", 1'000),
          makeNullConstant(TypeKind::VARCHAR, 1'000),
      })};

  testAggregations(
      {constVectors},
      {},
      {"min(c0)", "max(c0)", "min(c1)", "max(c1)"},
      "SELECT 'apple', 'banana', null, null");
}

TEST_F(MinMaxTest, largeVarchar) {
  size_t size = 512 * 1024 * 1024;
  auto constVectors = {makeRowVector(
      {makeFlatVector<std::string>({std::string(size, 0), "1"})})};

  testAggregations({constVectors}, {}, {"max(c0)"}, "SELECT '1'");
}

TEST_F(MinMaxTest, varcharIncrease) {
  size_t size = HashStringAllocator::kMaxAlloc;
  auto rowVectors = {makeRowVector({makeFlatVector<std::string>(
      {std::string(size + 2, 0),
       std::string(size, '1'),
       std::string(1024, '2'),
       "3"})})};

  testAggregations({rowVectors}, {}, {"max(c0)"}, "SELECT '3'");
}

TEST_F(MinMaxTest, minMaxTimestamp) {
  auto rowType = ROW({"c0", "c1"}, {SMALLINT(), TIMESTAMP()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"min(c1)", "max(c1)"},
      "SELECT date_trunc('millisecond', min(c1)), "
      "date_trunc('millisecond', max(c1)) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 17 as k", "c1"});
      },
      {"k"},
      {"min(c1)", "max(c1)"},
      "SELECT c0 % 17, date_trunc('millisecond', min(c1)), "
      "date_trunc('millisecond', max(c1)) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxTest, constNull) {
  // Create two batches of the source data for the aggregation:
  // Column c0 with 1K of "apple" and 1K of "banana".
  // Column c1 with 1K of nulls and 1K of nulls.
  auto constVectors = {
      makeRowVector(
          {makeConstant("apple", 1), makeNullConstant(TypeKind::VARCHAR, 1)}),
      makeRowVector({
          makeConstant("banana", 1),
          makeNullConstant(TypeKind::VARCHAR, 1),
      })};

  testAggregations({constVectors}, {}, {"max(null)"}, "SELECT max(null)");
  testAggregations({constVectors}, {}, {"min(null)"}, "SELECT min(null)");
  testAggregations(
      {constVectors},
      {},
      {"min(c0)", "max(c0)", "min(c1)", "max(c1)", "min(null)"},
      "SELECT 'apple', 'banana', null, null, null");
}

TEST_F(MinMaxTest, largeValuesDate) {
  auto vectors = {makeRowVector(
      {makeConstant(60577, 100, DATE()), makeConstant(-57604, 100, DATE())})};
  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {},
      {"min(c0)", "max(c0)", "min(c1)", "max(c1)"},
      "SELECT min(c0), max(c0), min(c1), max(c1) FROM tmp");
}

TEST_F(MinMaxTest, minMaxDate) {
  auto rowType = ROW({"c0", "c1"}, {SMALLINT(), DATE()});
  auto vectors = makeVectors(rowType, 1'000, 10);
  createDuckDbTable(vectors);

  testAggregations(
      vectors, {}, {"min(c1)", "max(c1)"}, "SELECT min(c1), max(c1) FROM tmp");

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 17 as k", "c1"});
      },
      {"k"},
      {"min(c1)", "max(c1)"},
      "SELECT c0 % 17, min(c1), max(c1) FROM tmp GROUP BY 1");
}

TEST_F(MinMaxTest, initialValue) {
  // Ensures that no groups are default initialized (to 0) in
  // aggregate::SimpleNumericAggregate.
  auto row = makeRowVector({
      makeFlatVector<int8_t>({1, 1, 1, 1}),
      makeFlatVector<int8_t>({-1, -1, -1, -1}),
      makeFlatVector<double>({1, 2, 3, 4}),
      makeFlatVector<double>({-1, -2, -3, -4}),
  });
  testAggregations(
      {row},
      {},
      {"min(c0)", "max(c1)", "min(c2)", "max(c3)"},
      "SELECT 1, -1, 1, -1");
}

TEST_F(MinMaxTest, maxShortDecimal) {
  doTest(max, DECIMAL(18, 3), false);
}

TEST_F(MinMaxTest, minShortDecimal) {
  doTest(min, DECIMAL(3, 1), false);
}

TEST_F(MinMaxTest, maxLongDecimal) {
  doTest(max, DECIMAL(20, 3), false);
}

TEST_F(MinMaxTest, minLongDecimal) {
  doTest(min, DECIMAL(38, 19), false);
}

TEST_F(MinMaxTest, array) {
  auto data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {2, std::nullopt},
          {6, 7, 8},
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

#ifndef SPARK_COMPATIBLE
  BOLT_ASSERT_THROW(
      testAggregations({data}, {}, {"min(c0)", "max(c0)"}, {expected}),
      "ARRAY comparison not supported for values that contain nulls");
#endif

  data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {3, 2},
          {6, 7, 8},
      }),
  });
  testAggregations({data}, {}, {"min(c0)", "max(c0)"}, {expected});
}

TEST_F(MinMaxTest, row) {
  auto data = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              std::nullopt,
              "efg"_sv,
              "hij"_sv,
          }),
      }),
  });

  auto expected = makeRowVector({
      makeRowVector(
          {makeFlatVector<StringView>({"a"_sv}),
           makeFlatVector<StringView>({"abc"_sv})}),
      makeRowVector(
          {makeFlatVector<StringView>({"c"_sv}),
           makeFlatVector<StringView>({"hij"_sv})}),
  });

#ifndef SPARK_COMPATIBLE
  BOLT_ASSERT_THROW(
      testAggregations({data}, {}, {"min(c0)", "max(c0)"}, {expected}),
      "ROW comparison not supported for values that contain nulls");
#endif

  data = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "abc"_sv,
              "efg"_sv,
              "hij"_sv,
          }),
      }),
  });
  testAggregations({data}, {}, {"min(c0)", "max(c0)"}, {expected});
}

TEST_F(MinMaxTest, arrayCheckNulls) {
  auto batch = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[2, 3]",
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
  });

  auto batchWithNull = makeRowVector({
      makeArrayVectorFromJson<int32_t>({
          "[1, 2]",
          "[6, 7]",
          "[3, null]",
      }),
      makeFlatVector<int32_t>({
          1,
          2,
          3,
      }),
  });

#ifndef SPARK_COMPATIBLE
  for (const auto& expr : {"min(c0)", "max(c0)"}) {
    testFailingAggregations(
        {batch, batchWithNull},
        {},
        {expr},
        "ARRAY comparison not supported for values that contain nulls");
    testFailingAggregations(
        {batch, batchWithNull},
        {"c1"},
        {expr},
        "ARRAY comparison not supported for values that contain nulls");
  }
#endif
}

TEST_F(MinMaxTest, rowCheckNull) {
  auto batch = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "aa"_sv,
              "bb"_sv,
              "cc"_sv,
          }),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

  auto batchWithNull = makeRowVector({
      makeRowVector({
          makeFlatVector<StringView>({
              "a"_sv,
              "b"_sv,
              "c"_sv,
          }),
          makeNullableFlatVector<StringView>({
              "aa"_sv,
              std::nullopt,
              "cc"_sv,
          }),
      }),
      makeFlatVector<int8_t>({1, 2, 3}),
  });

#ifndef SPARK_COMPATIBLE
  for (const auto& expr : {"min(c0)", "max(c0)"}) {
    testFailingAggregations(
        {batch, batchWithNull},
        {},
        {expr},
        "ROW comparison not supported for values that contain nulls");
    testFailingAggregations(
        {batch, batchWithNull},
        {"c1"},
        {expr},
        "ROW comparison not supported for values that contain nulls");
  }
#endif
}

#ifdef SPARK_COMPATIBLE
TEST_F(MinMaxTest, map) {
  auto expected = makeRowVector({
      makeMapVector<StringView, StringView>({{
          {"cpu p96消耗"_sv, "95.4"_sv},
          {"cpu使用率"_sv, "0.48"_sv},
          {"cpu分配"_sv, "200.0"_sv},
          {"cpu可节约"_sv, "80.0"_sv},
          {"单tm 建议分配cpu"_sv, "2.4"_sv},
          {"单tm最大cpu使用率"_sv, "0.5"_sv},
      }}),
  });

  auto data = makeRowVector({
      makeNullableMapVector<StringView, StringView>(
          {{{{"queue_name"_sv,
              "root.feins_gl_flink_risk_data_bus_ordinary"_sv}}},
           {{
               {"cpu p96消耗"_sv, "95.4"_sv},
               {"cpu使用率"_sv, "0.48"_sv},
               {"cpu分配"_sv, "200.0"_sv},
               {"cpu可节约"_sv, "80.0"_sv},
               {"单tm 建议分配cpu"_sv, "2.4"_sv},
               {"单tm最大cpu使用率"_sv, "0.5"_sv},
           }},
           {{
               {"cpu p96消耗"_sv, "69.8"_sv},
               {"cpu使用率"_sv, "0.35"_sv},
               {"cpu分配"_sv, "200.0"_sv},
               {"cpu可节约"_sv, "94.4"_sv},
               {"单tm 建议分配cpu"_sv, "2.1"_sv},
               {"单tm最大cpu使用率"_sv, "0.5"_sv},
           }}}),
  });

  testAggregations({data}, {}, {"max(c0)"}, {expected});
}
#endif

TEST_F(MinMaxTest, failOnUnorderableType) {
  auto data = makeRowVector({
      makeAllNullMapVector(5, VARCHAR(), BIGINT()),
      makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
  });

  static const std::string kErrorMessage =
      "Aggregate function signature is not supported";
  for (const auto& expr : {"min(c0)", "max(c0)"}) {
    {
      auto builder = PlanBuilder().values({data});

#ifndef SPARK_COMPATIBLE
      BOLT_ASSERT_THROW(builder.singleAggregation({}, {expr}), kErrorMessage);
#endif
    }

    {
      auto builder = PlanBuilder().values({data});
#ifndef SPARK_COMPATIBLE
      BOLT_ASSERT_THROW(
          builder.singleAggregation({"c1"}, {expr}), kErrorMessage);
#endif
    }
  }
}

TEST_F(MinMaxTest, minMaxNaN) {
  {
    auto rowType = ROW({"c0", "c1", "mask"}, {BIGINT(), DOUBLE(), BOOLEAN()});
    constexpr int size = 1000;
    std::vector<VectorPtr> children = {
        makeFlatVector<int64_t>(size, [](auto row) { return row % 2; }),
        makeFlatVector<double>(
            size,
            [](auto row) {
              return row % 3 == 1 ? std::numeric_limits<double>::quiet_NaN()
                                  : row % 3 - 2;
            }),
        makeFlatVector<bool>(size, [](auto row) { return row % 7 != 0; }),
    };
    std::vector<RowVectorPtr> data = {std::make_shared<RowVector>(
        pool_.get(), rowType, nullptr, size, std::move(children))};
    doTest(max, data);
    doTest(min, data);
  }
  {
    auto rowType = ROW({"c0", "c1", "mask"}, {BIGINT(), DOUBLE(), BOOLEAN()});
    constexpr int size = 100;
    std::vector<VectorPtr> children = {
        makeFlatVector<int64_t>(size, [](auto row) { return row % 2; }),
        makeFlatVector<double>(
            size,
            [](auto row) { return std::numeric_limits<double>::quiet_NaN(); }),
        makeFlatVector<bool>(size, [](auto row) { return row % 7 != 0; }),
    };
    std::vector<RowVectorPtr> data = {std::make_shared<RowVector>(
        pool_.get(), rowType, nullptr, size, std::move(children))};
    doTest(max, data);
    doTest(min, data);
  }
}

class MinMaxNTest : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  template <typename T>
  void testNumericGlobal() {
    auto data =
        makeRowVector({makeFlatVector<T>({1, 10, 2, 9, 3, 8, 4, 7, 6, 5})});

    // DuckDB doesn't support min(x, n) or max(x, n) functions.

    auto expected = makeRowVector({
        makeArrayVector<T>({
            {1, 2},
        }),
        makeArrayVector<T>({
            {1, 2, 3, 4, 5},
        }),
        makeArrayVector<T>({
            {10, 9, 8},
        }),
        makeArrayVector<T>({
            {10, 9, 8, 7, 6, 5, 4},
        }),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});

    // Add some nulls. Expect these to be ignored.
    data = makeRowVector({
        makeNullableFlatVector<T>(
            {1,
             std::nullopt,
             10,
             2,
             9,
             std::nullopt,
             3,
             8,
             4,
             7,
             6,
             5,
             std::nullopt}),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});

    // Test all null input.
    data = makeRowVector({
        makeAllNullFlatVector<T>(100),
    });

    expected = makeRowVector({
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
        makeAllNullArrayVector(1, data->childAt(0)->type()),
    });

    testAggregations(
        {data},
        {},
        {"min(c0, 2)", "min(c0, 5)", "max(c0, 3)", "max(c0, 7)"},
        {expected});

    // Test the NULL handling in `N` param.
    data = makeRowVector({
        makeFlatVector<T>({1, 10, 2, 9, 3, 8, 4, 7, 6, 5}),
        // c1, used as the N of minN, with NULL in it.
        makeNullableFlatVector<int64_t>(
            {2, 2, std::nullopt, 2, 2, 2, 2, 2, 2, 2}),
        // c2, used as the N of maxN, with NULL in it.
        makeNullableFlatVector<int64_t>(
            {3, 3, 3, 3, 3, std::nullopt, 3, 3, 3, 3}),
        // c3, used as the N of minN/maxN, all NULL.
        makeAllNullFlatVector<int64_t>(10),
    });

    expected = makeRowVector(
        // min(c0, c1): Because of NULL N, 2 is ignored.
        {makeArrayVector<T>({
             {1, 3},
         }),
         // min(c0, c3): Since all N are NULL, the result is NULL.
         makeNullableArrayVector<T>({std::nullopt}),
         // max(c0, c2): Because of NULL N, 8 is ignored.
         makeArrayVector<T>({
             {10, 9, 7},
         }),
         // max(c0, c3): Since all N are NULL, the result is NULL.
         makeNullableArrayVector<T>({std::nullopt})});

    testAggregations(
        {data},
        {},
        {"min(c0, c1)", "min(c0, c3)", "max(c0, c2)", "max(c0, c3)"},
        {expected});

    // Second argument of max_n/min_n must be less than or equal to 10000.
    BOLT_ASSERT_THROW(
        testAggregations({data}, {}, {"min(c0, 10001)"}, {expected}),
        "second argument of max/min must be less than or equal to 10000");
    BOLT_ASSERT_THROW(
        testAggregations({data}, {}, {"max(c0, 10001)"}, {expected}),
        "second argument of max/min must be less than or equal to 10000");
  }

  template <typename T>
  void testNumericGroupBy() {
    auto data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 2, 2, 1, 2}),
        makeFlatVector<T>({1, 2, 4, 3, 6, 5, 7, 8}),
    });

    auto expected = makeRowVector({
        makeFlatVector<int16_t>({1, 2}),
        makeArrayVector<T>({
            {1, 3},
            {2, 5},
        }),
        makeArrayVector<T>({
            {1, 3, 4, 7},
            {2, 5, 6, 8},
        }),
        makeArrayVector<T>({
            {7, 4, 3},
            {8, 6, 5},
        }),
        makeArrayVector<T>({
            {7, 4, 3, 1},
            {8, 6, 5, 2},
        }),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});

    // Add some nulls. Expect these to be ignored.
    data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 1, 2, 2, 2, 1, 2}),
        makeNullableFlatVector<T>(
            {1, 2, std::nullopt, 4, 3, 6, std::nullopt, 5, 7, 8}),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});

    // Test all null input.
    data = makeRowVector({
        makeFlatVector<int16_t>({1, 2, 1, 1, 1, 2, 2, 2, 1, 2}),
        makeNullableFlatVector<T>(
            {std::nullopt,
             2,
             std::nullopt,
             std::nullopt,
             std::nullopt,
             6,
             std::nullopt,
             5,
             std::nullopt,
             8}),
    });

    expected = makeRowVector({
        makeFlatVector<int16_t>({1, 2}),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{2, 5}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{2, 5, 6, 8}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{8, 6, 5}}},
        }),
        makeNullableArrayVector<T>({
            std::nullopt,
            {{{8, 6, 5, 2}}},
        }),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, 2)", "min(c1, 5)", "max(c1, 3)", "max(c1, 7)"},
        {expected});

    // Test the NULL handling in `N` param.
    data = makeRowVector({
        // Group by column.
        makeFlatVector<int16_t>({1, 2, 1, 1, 2, 2, 1, 2}),
        // Values.
        makeFlatVector<T>({1, 2, 4, 3, 6, 5, 7, 8}),
        // c2: used as the N of min, with NULL in it.
        makeNullableFlatVector<int64_t>(
            {2, 2, 2, std::nullopt, 2, std::nullopt, 2, 2}),
        // c3: used as the N of max, with NULL in it.
        makeNullableFlatVector<int64_t>(
            {3, 3, 3, 3, 3, 3, std::nullopt, std::nullopt}),
        // c4: used as the N of minN/maxN, all NULL.
        makeAllNullFlatVector<int64_t>(8),
    });

    expected = makeRowVector({
        makeFlatVector<int16_t>({1, 2}),
        // min(c1, c2): 3, 5 are ignored because of NULL N.
        makeArrayVector<T>({
            {1, 4},
            {2, 6},
        }),
        // min(c1, c4): Since all N are NULL, the result is NULL.
        makeNullableArrayVector<T>({std::nullopt, std::nullopt}),
        // max(c1, c3): 7, 8 are ignored because of NULL N.
        makeArrayVector<T>({
            {4, 3, 1},
            {6, 5, 2},
        }),
        // max(c1, c4): Since all N are NULL, the result is NULL.
        makeNullableArrayVector<T>({std::nullopt, std::nullopt}),
    });

    testAggregations(
        {data},
        {"c0"},
        {"min(c1, c2)", "min(c1, c4)", "max(c1, c3)", "max(c1, c4)"},
        {expected});
  }
};

TEST_F(MinMaxNTest, tinyint) {
  testNumericGlobal<int8_t>();
  testNumericGroupBy<int8_t>();
}

TEST_F(MinMaxNTest, smallint) {
  testNumericGlobal<int16_t>();
  testNumericGroupBy<int16_t>();
}

TEST_F(MinMaxNTest, integer) {
  testNumericGlobal<int32_t>();
  testNumericGroupBy<int32_t>();
}

TEST_F(MinMaxNTest, bigint) {
  testNumericGlobal<int64_t>();
  testNumericGroupBy<int64_t>();
}

TEST_F(MinMaxNTest, real) {
  testNumericGlobal<float>();
  testNumericGroupBy<float>();
}

TEST_F(MinMaxNTest, double) {
  testNumericGlobal<double>();
  testNumericGroupBy<double>();
}

TEST_F(MinMaxNTest, incrementalWindow) {
  // SELECT
  //  c0, c1, c2, c3,
  //  max(c0, c1) over (partition by c2 order by c3 asc)
  // FROM (
  //  VALUES
  //      (1, 10, false, 0),
  //      (2, 10, false, 1)
  // ) AS t(c0, c1, c2, c3)
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<int64_t>({10, 10}),
      makeFlatVector<bool>({false, false}),
      makeFlatVector<int64_t>({0, 1}),
  });

  auto plan =
      PlanBuilder()
          .values({data})
          .window({"max(c0, c1) over (partition by c2 order by c3 asc)"})
          .planNode();

  // Expected result: {1, 10, false, 0, [1]}, {2, 10, false, 1, [2, 1]}.
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<int64_t>({10, 10}),
      makeFlatVector<bool>({false, false}),
      makeFlatVector<int64_t>({0, 1}),
      makeArrayVector<int64_t>({{1}, {2, 1}}),
  });
  AssertQueryBuilder(plan).assertResults(expected);
}

TEST_F(MinMaxTest, rowBasedSpillAgg) {
#ifdef __APPLE__
  struct rlimit rl;
  if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
    GTEST_SKIP() << "Cannot get file descriptor limit";
  }

  // Required file descriptor limit.
  constexpr rlim_t kRequired = 10000;

  if (rl.rlim_cur < kRequired) {
    rl.rlim_cur = std::min(rl.rlim_max, (rlim_t)65536);
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0 || rl.rlim_cur < kRequired) {
      GTEST_SKIP() << "Insufficient file descriptor limit on macOS"
                   << " (have: " << rl.rlim_cur << ", need: " << kRequired
                   << ")";
    }
  }
#endif
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < 100; ++i) {
    size_t rowCount = 10000;
    vectors.emplace_back(makeRowVector({
        makeFlatVector<int>(
            rowCount, [](auto row) { return row; }, nullptr, INTEGER()),
        makeFlatVector<std::string>(
            rowCount,
            [&](auto row) {
              return "sssssssssssss" + std::to_string(i * rowCount + row);
            }),
    }));
  }

  createDuckDbTable(vectors);

  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 170 as k", "c1"});
      },
      {"k"},
      {"min(c1)", "max(c1)"},
      "SELECT c0 % 170, min(c1), max(c1) FROM tmp GROUP BY 1");
}

} // namespace
