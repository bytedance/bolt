/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <cmath>
#include <limits>
#include <type_traits>

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class SparkRoundTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  void runRoundTest(
      const std::vector<std::tuple<T, T>>& data,
      const std::string& func = "round") {
    auto result = evaluate<SimpleVector<T>>(
        func + "(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(
      const std::vector<std::tuple<T, int32_t, T>>& data,
      const std::string& func = "round") {
    auto result = evaluate<SimpleVector<T>>(
        func + "(c0, c1)",
        makeRowVector(
            {makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatData() {
    std::vector<std::tuple<T, int32_t, T>> data = {
        {1.122112, 0, 1},
        {1.129, 1, 1.1},
        {1.129, 2, 1.13},
        {1.0 / 3, 0, 0.0},
        {1.0 / 3, 1, 0.3},
        {1.0 / 3, 2, 0.33},
        {1.0 / 3, 6, 0.333333},
        {-1.122112, 0, -1},
        {-1.129, 1, -1.1},
        {-1.129, 2, -1.13},
        {-1.129, 2, -1.13},
        {-1.0 / 3, 0, 0.0},
        {-1.0 / 3, 1, -0.3},
        {-1.0 / 3, 2, -0.33},
        {-1.0 / 3, 6, -0.333333},
        {1.0, -1, 0.0},
        {0.0, -2, 0.0},
        {-1.0, -3, 0.0},
        {11111.0, -1, 11110.0},
        {11111.0, -2, 11100.0},
        {11111.0, -3, 11000.0},
        {11111.0, -4, 10000.0},
        {0.575, 2, 0.58},
        {0.574, 2, 0.57},
        {-0.575, 2, -0.58},
        {-0.574, 2, -0.57},
        {102.825291, 6, 102.825291},
        {10.424817, 6, 10.424817},
        {-82.737209, 6, -82.737209},
        {-123.106541, 6, -123.106541}};

    // Matching Spark expectations for float
    if constexpr (std::is_same_v<T, float>) {
      data[22] = {0.575, 2, 0.57};
      data[24] = {-0.575, 2, -0.57};
    }

    return data;
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0},
        {12, -1, 10},
        {12, -2, 0},
        {123, -1, 120},
        {123, -2, 100},
        {123, -3, 0}};
  }

  // HALF_EVEN only differs from HALF_UP on exact ties, so most rows are ties;
  // a few non-ties confirm the shared path is unchanged.
  template <typename T>
  std::vector<std::tuple<T, T>> testBRoundFloatData() {
    return {
        {0.5, 0.0},
        {1.5, 2.0},
        {2.5, 2.0},
        {3.5, 4.0},
        {-0.5, 0.0},
        {-1.5, -2.0},
        {-2.5, -2.0},
        {-3.5, -4.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testBRoundWithDecFloatData() {
    std::vector<std::tuple<T, int32_t, T>> data = {
        {2.5, 0, 2.0},
        {-2.5, 0, -2.0},
        {0.125, 2, 0.12},
        {-0.125, 2, -0.12},
        {0.135, 2, 0.14},
        {0.585, 2, 0.58},
        {1.615, 2, 1.62},
        {1.129, 2, 1.13},
        {-1.129, 1, -1.1},
        {1.0 / 3, 2, 0.33},
        {1250.0, -2, 1200.0},
        {1350.0, -2, 1400.0},
        {-1250.0, -2, -1200.0},
        {11111.0, -1, 11110.0},
        {5.0, -1, 0.0},
        {15.0, -1, 20.0},
        {25.0, -1, 20.0},
        {1.0, -1, 0.0},
        {0.0, -2, 0.0}};

    // Spark widens float to double before taking the decimal string, so
    // 0.575f is 0.574999988079071 and 2.675f is 2.6749999523162842: below
    // the tie, rounded down under either mode. As doubles the strings are
    // exact ties on an odd digit and round up.
    if constexpr (std::is_same_v<T, float>) {
      data.insert(
          data.end(), {{0.575, 2, 0.57}, {-0.575, 2, -0.57}, {2.675, 2, 2.67}});
    } else {
      data.insert(
          data.end(), {{0.575, 2, 0.58}, {-0.575, 2, -0.58}, {2.675, 2, 2.68}});
    }
    return data;
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testBRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {-1, 0, -1},
        {1, 10, 1},
        {5, -1, 0},
        {15, -1, 20},
        {25, -1, 20},
        {35, -1, 40},
        {-25, -1, -20},
        {-35, -1, -40},
        {24, -1, 20},
        {26, -1, 30},
        {125, -1, 120},
        {123, -2, 100},
        {123, -3, 0},
        {12, -2, 0}};
  }
};

TEST_F(SparkRoundTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());

  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(SparkRoundTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatData<double>());

  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(SparkRoundTest, bround) {
  runRoundTest<float>(testBRoundFloatData<float>(), "bround");
  runRoundTest<double>(testBRoundFloatData<double>(), "bround");

  runRoundTest<int64_t>(testRoundIntegralData<int64_t>(), "bround");
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>(), "bround");
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>(), "bround");
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>(), "bround");
}

TEST_F(SparkRoundTest, broundWithDecimal) {
  runRoundWithDecimalTest<float>(testBRoundWithDecFloatData<float>(), "bround");
  runRoundWithDecimalTest<double>(
      testBRoundWithDecFloatData<double>(), "bround");

  runRoundWithDecimalTest<int64_t>(
      testBRoundWithDecIntegralData<int64_t>(), "bround");
  runRoundWithDecimalTest<int32_t>(
      testBRoundWithDecIntegralData<int32_t>(), "bround");
  runRoundWithDecimalTest<int16_t>(
      testBRoundWithDecIntegralData<int16_t>(), "bround");
  runRoundWithDecimalTest<int8_t>(
      testBRoundWithDecIntegralData<int8_t>(), "bround");
}

TEST_F(SparkRoundTest, broundNonFiniteAndNull) {
  const auto bround = [&](std::optional<double> a, std::optional<int32_t> b) {
    return evaluateOnce<double>("bround(c0, c1)", a, b);
  };
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  constexpr double kInf = std::numeric_limits<double>::infinity();
  EXPECT_TRUE(std::isnan(bround(kNaN, 2).value()));
  EXPECT_EQ(bround(kInf, 2), kInf);
  EXPECT_EQ(bround(-kInf, 2), -kInf);
  EXPECT_EQ(bround(std::nullopt, 2), std::nullopt);
  EXPECT_EQ(bround(1.5, std::nullopt), std::nullopt);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
