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

#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/common/base/tests/GTestUtils.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class DecimalUtilTest : public testing::Test {
 protected:
  template <typename R, typename A, typename B>
  void testDivideWithRoundUp(
      A a,
      B b,
      int32_t aRescale,
      R expectedResult,
      bool expectedOverflow) {
    R r;
    bool overflow = false;
    DecimalUtil::divideWithRoundUp<R, A, B>(r, a, b, aRescale, overflow);
    ASSERT_EQ(overflow, expectedOverflow);
    ASSERT_EQ(r, expectedResult);
  }

  template <bool allowPrecisionLoss>
  void testComputeDivideResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale,
      std::pair<uint8_t, uint8_t> expected) {
    ASSERT_EQ(
        DecimalUtil::computeDivideResultPrecisionScale<allowPrecisionLoss>(
            aPrecision, aScale, bPrecision, bScale),
        expected);
  }
};
} // namespace

TEST_F(DecimalUtilTest, divideWithRoundUp) {
  testDivideWithRoundUp<int64_t, int64_t, int64_t>(60, 30, 3, 2000, false);
  testDivideWithRoundUp<int64_t, int64_t, int64_t>(
      6, bolt::DecimalUtil::kPowersOfTen[17], 20, 6000, false);
}

TEST_F(DecimalUtilTest, minLeadingZeros) {
  auto result =
      DecimalUtil::minLeadingZeros<int64_t, int64_t>(10000, 6000000, 10, 12);
  ASSERT_EQ(result, 1);

  result = DecimalUtil::minLeadingZeros<int64_t, int128_t>(
      10000, 6'000'000'000'000'000'000, 10, 12);
  ASSERT_EQ(result, 16);

  result = DecimalUtil::minLeadingZeros<int128_t, int128_t>(
      bolt::DecimalUtil::kLongDecimalMax,
      bolt::DecimalUtil::kLongDecimalMin,
      10,
      12);
  ASSERT_EQ(result, 0);
}

TEST_F(DecimalUtilTest, stripTrailingZeros) {
  int64_t shortDecimal = -12000;
  uint8_t shortScale = 5;
  DecimalUtil::stripTrailingZeros(shortDecimal, shortScale);
  EXPECT_EQ(shortDecimal, -12);
  EXPECT_EQ(shortScale, 2);

  int128_t longDecimal = 12000;
  uint8_t longScale = 5;
  DecimalUtil::stripTrailingZeros(longDecimal, longScale);
  EXPECT_EQ(longDecimal, 12);
  EXPECT_EQ(longScale, 2);

  shortDecimal = std::numeric_limits<int64_t>::min();
  shortScale = 5;
  DecimalUtil::stripTrailingZeros(shortDecimal, shortScale);
  EXPECT_EQ(shortDecimal, std::numeric_limits<int64_t>::min());
  EXPECT_EQ(shortScale, 5);

  longDecimal = std::numeric_limits<int128_t>::min();
  longScale = 5;
  DecimalUtil::stripTrailingZeros(longDecimal, longScale);
  EXPECT_EQ(longDecimal, std::numeric_limits<int128_t>::min());
  EXPECT_EQ(longScale, 5);
}

TEST_F(DecimalUtilTest, bounded) {
  // Both precision and scale below 38 should stay the same
  auto result = DecimalUtil::bounded(10, 5);
  ASSERT_EQ(result.first, 10);
  ASSERT_EQ(result.second, 5);

  // Precision and scale exactly 38 should stay the same
  result = DecimalUtil::bounded(38, 38);
  ASSERT_EQ(result.first, 38);
  ASSERT_EQ(result.second, 38);

  // Precision above 38 should be capped at 38
  result = DecimalUtil::bounded(40, 10);
  ASSERT_EQ(result.first, 38);
  ASSERT_EQ(result.second, 10);

  // Scale above 38 should be capped at 38
  result = DecimalUtil::bounded(20, 40);
  ASSERT_EQ(result.first, 20);
  ASSERT_EQ(result.second, 38);

  // Both precision and scale above 38 should be capped at 38
  result = DecimalUtil::bounded(40, 40);
  ASSERT_EQ(result.first, 38);
  ASSERT_EQ(result.second, 38);
}

TEST_F(DecimalUtilTest, computeDivideResultPrecisionScale) {
  // Test with allowPrecisionLoss = true.
  testComputeDivideResultPrecisionScale<true>(10, 2, 5, 1, {17, 8});
  testComputeDivideResultPrecisionScale<true>(38, 10, 10, 5, {38, 6});
  testComputeDivideResultPrecisionScale<true>(1, 0, 1, 0, {7, 6});
  testComputeDivideResultPrecisionScale<true>(20, 2, 20, 2, {38, 18});

  // Test with allowPrecisionLoss = false.
  testComputeDivideResultPrecisionScale<false>(10, 2, 5, 1, {17, 8});
  testComputeDivideResultPrecisionScale<false>(38, 10, 5, 3, {38, 11});
  testComputeDivideResultPrecisionScale<false>(1, 0, 1, 0, {7, 6});
  testComputeDivideResultPrecisionScale<false>(30, 5, 10, 5, {38, 11});
}
} // namespace bytedance::bolt::functions::sparksql::test
