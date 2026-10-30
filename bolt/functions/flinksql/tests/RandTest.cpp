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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "folly/Random.h"
namespace bytedance::bolt::functions::flinksql::test {

using bytedance::bolt::test::assertEqualVectors;

namespace {
class RandTest : public FlinkFunctionBaseTest {
 public:
  RandTest() {
    // Allow for parsing literal integers as INTEGER, not BIGINT.
    options_.parseIntegerAsBigint = false;
  }
};

TEST_F(RandTest, rand_integer) {
  auto check_result = [](std::optional<int32_t> max,
                         std::optional<int32_t> result) {
    if (max == std::nullopt) {
      EXPECT_EQ(result, std::nullopt);
    } else if (max.value() == 0) {
      EXPECT_NE(result, std::nullopt);
      EXPECT_EQ(result.value(), 0);
    } else {
      EXPECT_NE(result, std::nullopt);
      EXPECT_GE(result.value(), 0);
      EXPECT_LT(result.value(), max);
    }
  };

  auto rand_integer = [this, &check_result](std::optional<int32_t> max) {
    return evaluateOnce<int32_t>(
        fmt::format("rand_integer(c0)"),
        makeRowVector({makeNullableFlatVector<int32_t>({max})}));
  };

  auto rand_integer_seed = [this, &check_result](
                               std::optional<int32_t> max, int32_t seed) {
    return evaluateOnce<int32_t>(
        fmt::format("rand_integer(c0, {})", seed),
        makeRowVector({makeNullableFlatVector<int32_t>({max})}));
  };

  {
    auto result = rand_integer(0);
    check_result(0, result);
  }
  {
    auto result = rand_integer(10000000);
    check_result(10000000, result);
  }
  {
    auto result1 = rand_integer(10);
    check_result(10, result1);
  }
  {
    auto result = rand_integer_seed(0, 100);
    check_result(0, result);
  }
  {
    auto result = rand_integer_seed(10000000, 10);
    check_result(10000000, result);
  }
  {
    auto result1 = rand_integer_seed(100, 5);
    check_result(100, result1);
    auto result2 = rand_integer_seed(100, 5);
    check_result(100, result2);
    EXPECT_EQ(result1.value(), result2.value());
  }
}
TEST_F(RandTest, LiteralSeedMatchesJavaRandomSequence) {
  auto input = makeRowVector({makeFlatVector<int32_t>({0, 0, 0})});
  auto doubles =
      evaluate<SimpleVector<double>>("flink_rand(cast(1 as integer))", input);
  EXPECT_DOUBLE_EQ(doubles->valueAt(0), 0.7308781907032909);
  EXPECT_DOUBLE_EQ(doubles->valueAt(1), 0.41008081149220166);
  EXPECT_DOUBLE_EQ(doubles->valueAt(2), 0.20771484130971707);

  auto integers = evaluate<SimpleVector<int32_t>>(
      "flink_rand_integer(cast(1 as integer), cast(100 as integer))", input);
  EXPECT_EQ(integers->valueAt(0), 85);
  EXPECT_EQ(integers->valueAt(1), 88);
  EXPECT_EQ(integers->valueAt(2), 47);

  auto powersOfTwo = evaluate<SimpleVector<int32_t>>(
      "flink_rand_integer(cast(1 as integer), cast(128 as integer))", input);
  EXPECT_EQ(powersOfTwo->valueAt(0), 93);
  EXPECT_EQ(powersOfTwo->valueAt(1), 12);
  EXPECT_EQ(powersOfTwo->valueAt(2), 52);
}

TEST_F(RandTest, LegacySeededIntegerSequenceIsUnchanged) {
  auto input = makeRowVector({makeFlatVector<int32_t>({100, 100, 100})});
  auto result = evaluate<SimpleVector<int32_t>>("rand_integer(c0, 1)", input);
  std::mt19937 reference{1};
  for (vector_size_t row = 0; row < input->size(); ++row) {
    EXPECT_EQ(result->valueAt(row), folly::Random::rand32(100, reference));
  }
  EXPECT_EQ(evaluateOnce<int32_t>("rand_integer(0, 1)", input), 0);
}

TEST_F(RandTest, DynamicIntegerSeedAndBound) {
  auto input = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 1, 1}),
       makeNullableFlatVector<int32_t>({100, 100, 100, std::nullopt, 100})});
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({85, 8, std::nullopt, std::nullopt, 85}),
      evaluate("flink_rand_integer(c0, c1)", input));
}

TEST_F(RandTest, NullLiteralSeed) {
  auto input = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  assertEqualVectors(
      makeNullableFlatVector<double>(
          {std::nullopt, std::nullopt, std::nullopt}),
      evaluate("flink_rand(cast(null as integer))", input));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {std::nullopt, std::nullopt, std::nullopt}),
      evaluate("flink_rand_integer(cast(null as integer), c0)", input));
}

TEST_F(RandTest, LargeBoundMatchesJavaRejectionSampling) {
  auto input = makeRowVector({makeFlatVector<int32_t>({0, 0, 0, 0, 0})});
  assertEqualVectors(
      makeFlatVector<int32_t>(
          {215764588, 880641847, 874970313, 446064254, 77814904}),
      evaluate("flink_rand_integer(1, 1073741825)", input));
}

TEST_F(RandTest, BigintDynamicSeeds) {
  auto input =
      makeRowVector({makeFlatVector<int64_t>({0, INT64_MIN, INT64_MAX, -1})});
  assertEqualVectors(
      makeFlatVector<double>(
          {0.730967787376657,
           0.730967787376657,
           0.26894263088050496,
           0.26894263088050496}),
      evaluate("flink_rand(c0)", input));
}

TEST_F(RandTest, LiteralSeedContinuesAcrossBatches) {
  auto input = makeRowVector({makeFlatVector<int32_t>({0})});
  auto expression =
      compileExpression("flink_rand(1)", asRowType(input->type()));
  auto first =
      evaluate(*expression, input)->as<SimpleVector<double>>()->valueAt(0);
  auto second =
      evaluate(*expression, input)->as<SimpleVector<double>>()->valueAt(0);
  EXPECT_DOUBLE_EQ(first, 0.7308781907032909);
  EXPECT_DOUBLE_EQ(second, 0.41008081149220166);
}

TEST_F(RandTest, DynamicSeedStartsNewSequenceForEachRow) {
  auto input =
      makeRowVector({makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 1})});
  auto result = evaluate<SimpleVector<double>>("flink_rand(c0)", input);
  EXPECT_DOUBLE_EQ(result->valueAt(0), 0.7308781907032909);
  EXPECT_DOUBLE_EQ(result->valueAt(1), 0.7311469360199058);
  EXPECT_TRUE(result->isNullAt(2));
  EXPECT_DOUBLE_EQ(result->valueAt(3), 0.7308781907032909);
}

TEST_F(RandTest, UnseededResultsStayWithinRange) {
  auto input = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto doubles = evaluate<SimpleVector<double>>("flink_rand()", input);
  auto integers =
      evaluate<SimpleVector<int32_t>>("flink_rand_integer(c0)", input);
  for (vector_size_t row = 0; row < input->size(); ++row) {
    EXPECT_FALSE(doubles->isNullAt(row));
    EXPECT_GE(doubles->valueAt(row), 0.0);
    EXPECT_LT(doubles->valueAt(row), 1.0);
    EXPECT_FALSE(integers->isNullAt(row));
    EXPECT_GE(integers->valueAt(row), 0);
    EXPECT_LT(integers->valueAt(row), row + 1);
  }
}

TEST_F(RandTest, NullBoundDoesNotConsumeSeededSequence) {
  auto input = makeRowVector(
      {makeNullableFlatVector<int32_t>({100, std::nullopt, 100})});
  auto result = evaluate<SimpleVector<int32_t>>(
      "flink_rand_integer(cast(1 as integer), c0)", input);
  EXPECT_EQ(result->valueAt(0), 85);
  EXPECT_TRUE(result->isNullAt(1));
  EXPECT_EQ(result->valueAt(2), 88);
}

TEST_F(RandTest, IntegerBoundMustBePositive) {
  auto input =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>{0})});
  EXPECT_ANY_THROW(evaluate(
      "flink_rand_integer(cast(1 as integer), cast(0 as integer))", input));
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
