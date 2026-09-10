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

#include <gmock/gmock.h>
#include <optional>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;

namespace {

static constexpr auto kMin8 = std::numeric_limits<int8_t>::min();
static constexpr auto kMax8 = std::numeric_limits<int8_t>::max();
static constexpr auto kMin16 = std::numeric_limits<int16_t>::min();
static constexpr auto kMax16 = std::numeric_limits<int16_t>::max();
static constexpr auto kMin32 = std::numeric_limits<int32_t>::min();
static constexpr auto kMax32 = std::numeric_limits<int32_t>::max();
static constexpr auto kMin64 = std::numeric_limits<int64_t>::min();
static constexpr auto kMax64 = std::numeric_limits<int64_t>::max();
static constexpr int kMaxBits = std::numeric_limits<uint64_t>::digits;

class BitwiseTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> bitwiseAnd(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_and(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> bitwiseOr(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_or(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int32_t> bitCount(std::optional<T> a) {
    return evaluateOnce<int32_t>("bit_count(c0)", a);
  }

  std::optional<int32_t> bitDaysCount(const std::vector<int64_t>& input) {
    auto rows = makeRowVector({makeArrayVector<int64_t>({input})});
    return evaluateOnce<int32_t>("bit_days_count(c0)", rows);
  }

  std::optional<int32_t> bitDaysCount(
      const std::vector<std::optional<int64_t>>& input,
      std::optional<int32_t> lastNDays) {
    std::vector<std::vector<std::optional<int64_t>>> inputWrapper{input};
    auto rows = makeRowVector(
        {makeNullableArrayVector<int64_t>(inputWrapper),
         makeNullableFlatVector(
             std::vector<std::optional<int32_t>>{lastNDays})});
    return evaluateOnce<int32_t>("bit_days_count(c0, c1)", rows);
  }

  template <typename T>
  std::optional<int8_t> bitGet(std::optional<T> a, std::optional<int32_t> b) {
    return evaluateOnce<int8_t>("bit_get(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> shiftLeft(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("shiftleft(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> shiftRight(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("shiftright(c0, c1)", a, b);
  }

  template <typename T1, typename T2>
  std::optional<T1> shiftLeft_twoTypes(
      std::optional<T1> a,
      std::optional<T2> b) {
    return evaluateOnce<T1>("shiftleft(c0, c1)", a, b);
  }

  template <typename T1, typename T2>
  std::optional<T1> shiftRight_twoTypes(
      std::optional<T1> a,
      std::optional<T2> b) {
    return evaluateOnce<T1>("shiftright(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> shiftRightUnsigned(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("shiftrightunsigned(c0, c1)", a, b);
  }

  template <typename T1, typename T2>
  std::optional<T1> shiftRightUnsigned_twoTypes(
      std::optional<T1> a,
      std::optional<T2> b) {
    return evaluateOnce<T1>("shiftrightunsigned(c0, c1)", a, b);
  }
};

TEST_F(BitwiseTest, bitwiseAnd) {
  EXPECT_EQ(bitwiseAnd<int32_t>(0, -1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(3, 8), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(-4, 12), 12);
  EXPECT_EQ(bitwiseAnd<int32_t>(60, 21), 20);

  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMax16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMin16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, 1), 1);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, -1), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, 1), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, -1), kMin16);

  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMin32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, 1), 1);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, -1), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, 1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, -1), kMin32);

  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMax64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMin64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, 1), 1);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, -1), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, 1), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, -1), kMin64);
}

TEST_F(BitwiseTest, bitwiseOr) {
  EXPECT_EQ(bitwiseOr<int32_t>(0, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(3, 8), 11);
  EXPECT_EQ(bitwiseOr<int32_t>(-4, 12), -4);
  EXPECT_EQ(bitwiseOr<int32_t>(60, 21), 61);

  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMin16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, 1), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, -1), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, 1), kMin16 + 1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, -1), -1);

  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMin32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, 1), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, 1), kMin32 + 1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, -1), -1);

  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMin64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, 1), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, -1), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, 1), kMin64 + 1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, -1), -1);
}

TEST_F(BitwiseTest, bitCount) {
  EXPECT_EQ(bitCount<int8_t>(std::nullopt), std::nullopt);
  EXPECT_EQ(bitCount<bool>(1), 1);
  EXPECT_EQ(bitCount<bool>(false), 0);
  EXPECT_EQ(bitCount<int8_t>(kMin8), 1);
  EXPECT_EQ(bitCount<int8_t>(kMax8), 7);
  EXPECT_EQ(bitCount<int16_t>(kMin16), 1);
  EXPECT_EQ(bitCount<int16_t>(kMax16), 15);
  EXPECT_EQ(bitCount<int32_t>(kMin32), 1);
  EXPECT_EQ(bitCount<int32_t>(kMax32), 31);
  EXPECT_EQ(bitCount<int64_t>(kMin64), 1);
  EXPECT_EQ(bitCount<int64_t>(kMax64), 63);
}

TEST_F(BitwiseTest, bitDaysCount) {
  EXPECT_EQ(bitDaysCount({}), 0);
  EXPECT_EQ(bitDaysCount({111111}), 7);
  EXPECT_EQ(bitDaysCount({4267520, 46582, 4252}), 19);
  EXPECT_EQ(bitDaysCount({73479242, 84764793, 44270444}), 32);
  EXPECT_EQ(bitDaysCount({23675021, 74297423, 39942444}), 33);
  EXPECT_EQ(bitDaysCount({24692444, 20427027, 17492489}), 34);
  EXPECT_EQ(bitDaysCount({47624720, 42692647, 26946294}), 35);

  // Normal Case
  EXPECT_EQ(bitDaysCount({22, 11, 26946294}, 2), 1);
  // Case: Check when binaryDays has only one element and valid lastNDays.
  EXPECT_EQ(bitDaysCount({4267520}, 4), 0);
  // Case: Check when lastNDays is below the minimum range.
  BOLT_ASSERT_THROW(
      bitDaysCount({12345}, 0), "lastNDays 0 should be within 1 and 60");
  // Case: Check when lastNDays exceeds the maximum range.
  BOLT_ASSERT_THROW(
      bitDaysCount({12345}, 61), "lastNDays 61 should be within 1 and 60");

  // Corner case: Check when the last element of binaryDays is NULL.
  BOLT_ASSERT_THROW(
      bitDaysCount({1698635493, 99, std::nullopt}, 1),
      "The last element of binaryDays should not be NULL.");
}

TEST_F(BitwiseTest, bitGet) {
  EXPECT_EQ(bitGet<int8_t>(std::nullopt, 1), std::nullopt);
  EXPECT_EQ(bitGet<int8_t>(kMin8, 0), 0);
  EXPECT_EQ(bitGet<int8_t>(kMin8, 7), 1);
  EXPECT_EQ(bitGet<int8_t>(kMax8, 0), 1);
  EXPECT_EQ(bitGet<int8_t>(kMax8, 7), 0);
  BOLT_ASSERT_THROW(
      bitGet<int8_t>(kMax8, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  BOLT_ASSERT_THROW(
      bitGet<int8_t>(kMax8, 8),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int16_t>(kMin16, 0), 0);
  EXPECT_EQ(bitGet<int16_t>(kMin16, 15), 1);
  EXPECT_EQ(bitGet<int16_t>(kMax16, 0), 1);
  EXPECT_EQ(bitGet<int16_t>(kMax16, 15), 0);
  BOLT_ASSERT_THROW(
      bitGet<int16_t>(kMax16, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  BOLT_ASSERT_THROW(
      bitGet<int16_t>(kMax16, 16),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int32_t>(kMin32, 0), 0);
  EXPECT_EQ(bitGet<int32_t>(kMin32, 31), 1);
  EXPECT_EQ(bitGet<int32_t>(kMax32, 0), 1);
  EXPECT_EQ(bitGet<int32_t>(kMax32, 31), 0);
  BOLT_ASSERT_THROW(
      bitGet<int32_t>(kMax32, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  BOLT_ASSERT_THROW(
      bitGet<int32_t>(kMax32, 32),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int64_t>(kMin64, 0), 0);
  EXPECT_EQ(bitGet<int64_t>(kMin64, 63), 1);
  EXPECT_EQ(bitGet<int64_t>(kMax64, 0), 1);
  EXPECT_EQ(bitGet<int64_t>(kMax64, 63), 0);
  BOLT_ASSERT_THROW(
      bitGet<int64_t>(kMax64, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  BOLT_ASSERT_THROW(
      bitGet<int64_t>(kMax64, 64),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");
}

TEST_F(BitwiseTest, shiftLeft) {
  EXPECT_EQ(shiftLeft<int32_t>(1, 1), 2);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 1), -2);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 3), -8);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 33), -2);
  EXPECT_EQ(shiftLeft<int32_t>(524287, 3), 4194296);

  EXPECT_EQ(shiftLeft<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(shiftLeft<int32_t>(kMax32, kMax32), -2147483648);
  EXPECT_EQ(shiftLeft<int32_t>(kMax32, 1), kMax32 << 1);
  EXPECT_EQ(shiftLeft<int32_t>(kMin32, 1), 0);

  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMin64, kMax64)), 0);
  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMax64, 1)), kMax64 << 1);
  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMin64, 1)), 0);
}

TEST_F(BitwiseTest, shiftRight) {
  EXPECT_EQ(shiftRight<int32_t>(-3, 1), -2);
  EXPECT_EQ(shiftRight<int32_t>(-1, 32), -1);
  EXPECT_EQ(shiftRight<int32_t>(-1, -1), -1);

  EXPECT_EQ(shiftRight<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(shiftRight<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(shiftRight<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(shiftRight<int32_t>(kMin32, 1), -1073741824);

  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMin64, kMax64)), -1);
  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMax64, kMax64)), 0);
  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMax64, 1)), kMax64 >> 1);
  EXPECT_EQ(
      (shiftRight_twoTypes<int64_t, int32_t>(kMin64, 1)), -4611686018427387904);
}

TEST_F(BitwiseTest, shiftRightUnsigned) {
  // Logical shift: vacated high bits are zero-filled, as with Java's >>>.
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-1, 1), kMax32);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-3, 1), 2147483646);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(kMin32, 1), 1073741824);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(kMin32, 31), 1);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(0, 5), 0);

  // Shift count is reduced modulo the type width, as in Java.
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-1, 32), -1);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-1, 33), kMax32);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-1, -1), 1);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(-1, kMin32), -1);

  EXPECT_EQ(shiftRightUnsigned<int32_t>(std::nullopt, 1), std::nullopt);
  EXPECT_EQ(shiftRightUnsigned<int32_t>(1, std::nullopt), std::nullopt);

  EXPECT_EQ((shiftRightUnsigned_twoTypes<int64_t, int32_t>(-1, 1)), kMax64);
  EXPECT_EQ(
      (shiftRightUnsigned_twoTypes<int64_t, int32_t>(kMin64, 1)),
      4611686018427387904);
  EXPECT_EQ((shiftRightUnsigned_twoTypes<int64_t, int32_t>(kMin64, 63)), 1);
  EXPECT_EQ(
      (shiftRightUnsigned_twoTypes<int64_t, int32_t>(kMax64, 1)), kMax64 >> 1);
  EXPECT_EQ((shiftRightUnsigned_twoTypes<int64_t, int32_t>(-1, 64)), -1);
  EXPECT_EQ((shiftRightUnsigned_twoTypes<int64_t, int32_t>(-1, 65)), kMax64);
  EXPECT_EQ((shiftRightUnsigned_twoTypes<int64_t, int32_t>(-1, -1)), 1);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
