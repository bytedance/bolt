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

#include <gtest/gtest.h>
#include <tuple>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/type/BigDecimal.h"
namespace bytedance::bolt {

namespace {

inline std::vector<std::tuple<double, int128_t, std::string, int>>
doubleInput() {
  using M = std::tuple<double, int128_t, std::string, int>;
  return {
      M{7784e8, 7784, "7784", -8},
      M{9077e-8, 9077, "9077", 8},
      M{907784232501249e-12, 907784232501249, "907784232501249", 12},
      M{8.507059173023462e39, 8507059173023462, "8507059173023462", -24},
      M{0.0, -1, "0", 0},
      M{-1.25, 125, "125", 2}};
}

inline std::vector<std::tuple<float, int128_t, std::string, int>> floatInput() {
  using M = std::tuple<float, int128_t, std::string, int>;
  int128_t d2 = 9077842407226562LL;
  return {
      M{7784e8, 778399973376, "778399973376", 0},
      M{907784232501249e-12, d2, "9077842407226562", 13},
      M{0.0, -1, "0", 0},
      M{-1.25, 125, "125", 2}};
}

TEST(BigDecimalTest, basic) {
  {
    auto input = doubleInput();
    for (auto& data : input) {
      BigDecimal decimal(std::get<0>(data));
      EXPECT_TRUE(decimal.getCompactVal() == std::get<1>(data));
      EXPECT_EQ(decimal.toString(), std::get<2>(data));
      EXPECT_EQ(decimal.doubleValue(), std::get<0>(data));
      EXPECT_EQ(decimal.getScale(), std::get<3>(data));
    }
  }

  {
    auto input = floatInput();
    for (auto& data : input) {
      BigDecimal decimal(static_cast<double>(std::get<0>(data)));
      EXPECT_TRUE(std::get<1>(data) == decimal.getCompactVal());
      EXPECT_EQ(decimal.toString(), std::get<2>(data));
      EXPECT_EQ(decimal.floatValue(), std::get<0>(data));
      EXPECT_EQ(decimal.doubleValue(), std::get<0>(data));
      EXPECT_EQ(decimal.getScale(), std::get<3>(data));
    }
  }
}

inline std::vector<std::tuple<double, int, int, double>> doubleScaleInput() {
  return {{0.575, 3, 2, 0.58}, {1.8824999999999998, 16, 3, 1.882}};
}

inline std::vector<std::tuple<float, int, int, float>> floatScaleInput() {
  return {{0.575, 15, 2, 0.57}, {1.88249993, 16, 3, 1.882}};
}

TEST(BigDecimalTest, setScale) {
  auto input = doubleScaleInput();
  for (auto& data : input) {
    BigDecimal decimal(std::get<0>(data));
    EXPECT_EQ(decimal.getScale(), std::get<1>(data));
    decimal.setScale(std::get<2>(data));
    EXPECT_EQ(decimal.getScale(), std::get<2>(data));
    EXPECT_EQ(decimal.doubleValue(), std::get<3>(data));
  }

  auto smallInput = floatScaleInput();
  for (auto& data : smallInput) {
    BigDecimal decimal(std::get<0>(data));
    EXPECT_EQ(decimal.getScale(), std::get<1>(data));
    decimal.setScale(std::get<2>(data));
    EXPECT_EQ(decimal.getScale(), std::get<2>(data));
    EXPECT_EQ(decimal.floatValue(), std::get<3>(data));
  }
}

TEST(BigDecimalTest, setScaleHalfEven) {
  // (input, newScale, expected). All ties except the last two.
  std::vector<std::tuple<double, int, double>> testData = {
      {2.5, 0, 2.0},
      {3.5, 0, 4.0},
      {-2.5, 0, -2.0},
      {-3.5, 0, -4.0},
      {0.125, 2, 0.12},
      {0.135, 2, 0.14},
      {2.675, 2, 2.68},
      {1250.0, -2, 1200.0},
      {1350.0, -2, 1400.0},
      {2.674, 2, 2.67},
      {2.676, 2, 2.68}};
  for (const auto& data : testData) {
    BigDecimal decimal(std::get<0>(data));
    decimal.setScale(std::get<1>(data), BigDecimal::RoundingMode::kHalfEven);
    EXPECT_EQ(decimal.getScale(), std::get<1>(data));
    EXPECT_EQ(decimal.doubleValue(), std::get<2>(data));
  }

  // The default mode is still HALF_UP.
  BigDecimal decimal(2.5);
  decimal.setScale(0);
  EXPECT_EQ(decimal.doubleValue(), 3.0);
}

} // namespace

} // namespace bytedance::bolt
