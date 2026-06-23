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
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"

using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;

namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

class ModeAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
    registerAggregateFunctions("spark_");
  }

  void testModeWithDuck(
      const VectorPtr& vectorKey,
      const VectorPtr& vectorInput) {
    auto vectors = makeRowVector({vectorKey, vectorInput});
    createDuckDbTable({vectors});

    testAggregations(
        {vectors},
        {"c0"},
        {"spark_mode(c1)"},
        "SELECT c0, mode(c1) FROM tmp GROUP BY c0");
  }

  void testGlobalModeWithDuck(const VectorPtr& vector) {
    auto num = vector->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors =
        makeRowVector({vector, wrapInDictionary(reverseIndices, num, vector)});

    createDuckDbTable({vectors});
    testAggregations(
        {vectors}, {}, {"spark_mode(c0)"}, "SELECT mode(c0) FROM tmp");

    testAggregations(
        {vectors}, {}, {"spark_mode(c1)"}, "SELECT mode(c1) FROM tmp");
  }

  void testGlobalModeWithDuckSkipTableScan(const VectorPtr& vector) {
    auto num = vector->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors =
        makeRowVector({vector, wrapInDictionary(reverseIndices, num, vector)});

    createDuckDbTable({vectors});
    testAggregations(
        {vectors},
        {},
        {"spark_mode(c0)"},
        "SELECT mode(c0) FROM tmp",
        /*config=*/{},
        /*testWithTableScan=*/false);

    testAggregations(
        {vectors},
        {},
        {"spark_mode(c1)"},
        "SELECT mode(c1) FROM tmp",
        /*config=*/{},
        /*testWithTableScan=*/false);
  }

  void testMode(
      const std::string& expression,
      const std::vector<std::string>& groupKeys,
      const VectorPtr& vectorKey,
      const VectorPtr& vectorInput,
      const RowVectorPtr& expected) {
    auto vectors = makeRowVector({vectorKey, vectorInput});
    testAggregations({vectors}, groupKeys, {expression}, {expected});
  }

  void testModeSkipTableScan(
      const std::string& expression,
      const std::vector<std::string>& groupKeys,
      const VectorPtr& vectorKey,
      const VectorPtr& vectorInput,
      const RowVectorPtr& expected) {
    auto vectors = makeRowVector({vectorKey, vectorInput});
    testAggregations(
        {vectors},
        groupKeys,
        {expression},
        {expected},
        /*config=*/{},
        /*testWithTableScan=*/false);
  }
};

TEST_F(ModeAggregateTest, groupByInteger) {
  vector_size_t num = 37;

  auto vectorKey = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vectorInput = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5));

  testModeWithDuck(vectorKey, vectorInput);

  // Test when some group-by keys have only null values.
  auto vectorKeyNull =
      makeNullableFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, std::nullopt});
  auto vectorInputNull = makeNullableFlatVector<int64_t>(
      {10, 10, 10, 20, std::nullopt, 20, std::nullopt, std::nullopt, 20});

  testModeWithDuck(vectorKeyNull, vectorInputNull);
}

TEST_F(ModeAggregateTest, groupByDouble) {
  vector_size_t num = 37;

  auto vectorKey = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vectorInput = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 2 + 0.05; }, nullEvery(5));

  testModeWithDuck(vectorKey, vectorInput);
}

TEST_F(ModeAggregateTest, groupByBoolean) {
  vector_size_t num = 37;

  auto vectorKey = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vectorInput = makeFlatVector<bool>(
      num, [](vector_size_t row) { return row % 2 == 0; }, nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<bool>({true, false, false, false})});

  testMode("spark_mode(c1)", {"c0"}, vectorKey, vectorInput, expected);
}

TEST_F(ModeAggregateTest, groupByTimestamp) {
  vector_size_t num = 10;

  auto vectorKey = makeNullableFlatVector<int32_t>(
      {std::nullopt, 1, 2, 0, std::nullopt, 2, 0, 1, std::nullopt, 0});
  auto vectorInput = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) {
        return Timestamp{row % 2, 17'123'456};
      },
      nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<Timestamp>(
           {Timestamp{0, 17'123'456},
            Timestamp{1, 17'123'456},
            Timestamp{1, 17'123'456},
            Timestamp{0, 17'123'456}})});

  testMode("spark_mode(c1)", {"c0"}, vectorKey, vectorInput, expected);
}

TEST_F(ModeAggregateTest, groupByDate) {
  vector_size_t num = 10;

  auto vectorKey = makeNullableFlatVector<int32_t>(
      {std::nullopt, 1, 2, 0, std::nullopt, 2, 0, 1, std::nullopt, 0});
  auto vectorInput = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE());

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<int32_t>({0, 1, 1, 0}, DATE())});

  testMode("spark_mode(c1)", {"c0"}, vectorKey, vectorInput, expected);
}

TEST_F(ModeAggregateTest, groupByInterval) {
  vector_size_t num = 30;

  auto vectorKey = makeFlatVector<int32_t>(num, [](auto row) { return row; });
  auto vectorInput = makeFlatVector<int64_t>(
      num,
      [](auto row) { return row % 5 == 0 ? 2 : row + 1; },
      nullEvery(5),
      INTERVAL_DAY_TIME());

  testModeWithDuck(vectorKey, vectorInput);
}

TEST_F(ModeAggregateTest, groupByString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto keys = makeFlatVector<int16_t>(
      1'002, [](auto row) { return row % 5; }, nullEvery(5));
  auto data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(5));
  testModeWithDuck(keys, data);
}

TEST_F(ModeAggregateTest, globalInteger) {
  vector_size_t num = 30;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 7; }, nullEvery(7));

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalDecimal) {
  vector_size_t num = 10;
  auto longDecimalType = DECIMAL(20, 2);
  auto vectorLongDecimal = makeFlatVector<int128_t>(
      num,
      [](vector_size_t row) { return row % 4; },
      nullEvery(7),
      longDecimalType);

  // Decimal types cannot be written as DWRF in our TableScan test path.
  // Keep DuckDB verification, but skip the TableScan (write-to-file) variant.
  testGlobalModeWithDuckSkipTableScan(vectorLongDecimal);

  auto shortDecimalType = DECIMAL(6, 2);
  auto vectorShortDecimal = makeFlatVector<int64_t>(
      num,
      [](vector_size_t row) { return row % 4; },
      nullEvery(7),
      shortDecimalType);

  testGlobalModeWithDuckSkipTableScan(vectorShortDecimal);
}

TEST_F(ModeAggregateTest, globalUnknown) {
  auto vector = makeAllNullFlatVector<UnknownValue>(6);

  auto expected = makeRowVector({
      BaseVector::createNullConstant(UNKNOWN(), 1, pool()),
  });

  // UNKNOWN type cannot be written in our TableScan (write-to-file) test path.
  testModeSkipTableScan("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalDouble) {
  vector_size_t num = 32;
  auto vector = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 5 + 0.05; }, nullEvery(5));

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalBoolean) {
  auto vector =
      makeNullableFlatVector<bool>({false, false, true, std::nullopt});

  auto expected =
      makeRowVector({makeFlatVector<bool>(std::vector<bool>{false})});

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalTimestamp) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) {
        return Timestamp{row % 4, 100};
      },
      nullEvery(7));

  auto expected = makeRowVector(
      {makeFlatVector<Timestamp>(std::vector<Timestamp>{Timestamp{1, 100}})});

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalDate) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 4; }, nullEvery(7), DATE());

  auto expected =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>{1}, DATE())});

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalInterval) {
  auto vector = makeFlatVector<int64_t>(
      51, [](auto row) { return row % 7; }, nullEvery(7), INTERVAL_DAY_TIME());

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalEmpty) {
  auto vector = makeFlatVector<int32_t>({});

  auto expected = makeRowVector({
      BaseVector::createNullConstant(INTEGER(), 1, pool()),
  });

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto data = makeFlatVector<StringView>(1'001, [&](auto row) {
    return StringView(strings[row % strings.size()]);
  });
  testGlobalModeWithDuck(data);

  // Some nulls.
  data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(strings.size()));
  testGlobalModeWithDuck(data);

  // All nulls.
  testGlobalModeWithDuck(makeAllNullFlatVector<StringView>(1'000));

  // Lots of unique strings.
  std::string scratch;
  data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) {
        scratch = std::string(50 + row % 1'000, 'A' + (row % 10));
        return StringView(scratch);
      },
      nullEvery(10));
  testGlobalModeWithDuck(data);
}

TEST_F(ModeAggregateTest, globalNaNs) {
  // Verify that NaNs with different binary representations are considered equal
  // and deduplicated.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto vector = makeFlatVector<double>({1, kNaN, kSNaN, 2, 3, kNaN, kSNaN, 3});

  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{kNaN})});

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, allNulls) {
  auto vector = makeAllNullFlatVector<int32_t>(3);

  auto expected = makeRowVector({makeAllNullFlatVector<int32_t>(1)});

  testMode("spark_mode(c1)", {}, vector, vector, expected);

  vector = makeAllNullFlatVector<int32_t>(0);

  testMode("spark_mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, arrays) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[1, 2]",
          "[]",
          "[1, 2]",
          "[]",
          "[1, null, 2, null]",
          "[1, null, 2, null]",
          "[]",
          "[1, null, 2, null]",
          "null",
          "[1, null, 2, null]",
          "null",
      }),
  });

  auto expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, null, 2, null]"}),
  });

  testAggregations({input}, {}, {"spark_mode(c1)"}, {expected});

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[1, null, 2, null]",
          "[1, 2]",
      }),
  });
  testAggregations({input}, {"c0"}, {"spark_mode(c1)"}, {expected});
}

TEST_F(ModeAggregateTest, maps) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}),
      makeMapVectorFromJson<int32_t, int32_t>({
          "{1: 10, 2: 20, 3: 30}",
          "{1: 10, 2: 20}",
          "{}",
          "{1: 10, 2: 20}",
          "{}",
          "{1: 10, 2: 20, 3: null}",
          "{1: 10, 2: 20, 3: null}",
          "{}",
          "{1: 10, 2: 20, 3: null}",
          "null",
          "{1: 10, 2: 20, 3: null}",
          "null",
      }),
  });

  auto expected = makeRowVector({
      makeMapVectorFromJson<int32_t, int32_t>({"{1: 10, 2: 20, 3: null}"}),
  });

  testAggregations({input}, {}, {"spark_mode(c1)"}, {expected});

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeMapVectorFromJson<int32_t, int32_t>({
          "{1: 10, 2: 20, 3: null}",
          "{1: 10, 2: 20}",
      }),
  });
  testAggregations({input}, {"c0"}, {"spark_mode(c1)"}, {expected});
}

TEST_F(ModeAggregateTest, rows) {
  auto input = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({
              1,
              1,
              2,
              1,
          }),
          makeNullableFlatVector<int32_t>({
              std::nullopt,
              1,
              2,
              1,
          }),
      }),
  });

  auto expected = makeRowVector({
      makeRowVector({
          makeConstant(1, 1),
          makeConstant(1, 1),
      }),
  });

  testAggregations({input}, {}, {"spark_mode(c0)"}, {expected});

  // Group by.
  auto inputGroupBy = makeRowVector({
      makeFlatVector<int64_t>({1, 0, 1, 0, 1, 1}),
      makeRowVector({
          makeFlatVector<int32_t>({
              1,
              1,
              2,
              1,
              2,
              2,
          }),
          makeNullableFlatVector<int32_t>({
              std::nullopt,
              1,
              2,
              1,
              1,
              2,
          }),
      }),
  });
  auto expectedGroupBy = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({1, 2}),
      }),
  });
  testAggregations(
      {inputGroupBy}, {"c0"}, {"spark_mode(c1)"}, {expectedGroupBy});
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
