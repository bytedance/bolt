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

#include "bolt/functions/lib/Repeat.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions {
namespace {

class RepeatTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    FunctionBaseTest::SetUpTestCase();
    exec::registerStatefulVectorFunction(
        "repeat", functions::repeatSignatures(), functions::makeRepeat);
    exec::registerStatefulVectorFunction(
        "repeat_allow_negative_count",
        functions::repeatSignatures(),
        functions::makeRepeatAllowNegativeCount);
  }

  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  void testExpressionWithError(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const std::string& expectedError) {
    BOLT_ASSERT_THROW(
        evaluate(expression, makeRowVector(input)), expectedError);
  }
};

TEST_F(RepeatTest, repeat) {
  const auto elementVector = makeNullableFlatVector<float>(
      {0.0, -2.0, 3.333333, 4.0004, std::nullopt, 5.12345});
  const auto countVector =
      makeNullableFlatVector<int32_t>({1, 2, 3, 0, 4, std::nullopt});
  VectorPtr expected;

  expected = makeNullableArrayVector<float>({
      {{0.0}},
      {{-2.0, -2.0}},
      {{3.333333, 3.333333, 3.333333}},
      emptyArray,
      {{std::nullopt, std::nullopt, std::nullopt, std::nullopt}},
      std::nullopt,
  });
  testExpression("repeat(C0, C1)", {elementVector, countVector}, expected);
  testExpression("try(repeat(C0, C1))", {elementVector, countVector}, expected);

  // Test using a null constant as the count argument.
  expected = BaseVector::createNullConstant(ARRAY(REAL()), 6, pool());
  testExpression("repeat(C0, null::INTEGER)", {elementVector}, expected);
  testExpression("try(repeat(C0, null::INTEGER))", {elementVector}, expected);

  // Test using a non-null constant as the count argument.
  expected = makeNullableArrayVector<float>({
      {0.0, 0.0, 0.0},
      {-2.0, -2.0, -2.0},
      {3.333333, 3.333333, 3.333333},
      {4.0004, 4.0004, 4.0004},
      {std::nullopt, std::nullopt, std::nullopt},
      {5.12345, 5.12345, 5.12345},
  });
  testExpression("repeat(C0, '3'::INTEGER)", {elementVector}, expected);
  testExpression("try(repeat(C0, '3'::INTEGER))", {elementVector}, expected);

  expected = makeArrayVector<float>({{}, {}, {}, {}, {}, {}});
  testExpression("repeat(C0, '0'::INTEGER)", {elementVector}, expected);
  testExpression("try(repeat(C0, '0'::INTEGER))", {elementVector}, expected);
}

TEST_F(RepeatTest, repeatWithInvalidCount) {
  const auto elementVector =
      makeNullableFlatVector<float>({0.0, 2.0, 3.333333});

  VectorPtr countVector;
  VectorPtr expected;

  expected = makeNullableArrayVector<float>({
      {{0.0}},
      std::nullopt,
      {{3.333333, 3.333333, 3.333333}},
  });
  countVector = makeNullableFlatVector<int32_t>({1, -2, 3});
  testExpression("try(repeat(C0, C1))", {elementVector, countVector}, expected);
  testExpressionWithError(
      "repeat(C0, C1)",
      {elementVector, countVector},
      "(-2 vs. 0) Count argument of repeat function must be greater than or equal to 0");

  countVector = makeNullableFlatVector<int32_t>({1, 123456, 3});
  testExpression("try(repeat(C0, C1))", {elementVector, countVector}, expected);
  testExpressionWithError(
      "repeat(C0, C1)",
      {elementVector, countVector},
      "(123456 vs. 10000) Count argument of repeat function must be less than or equal to 10000");

  // Test using a constant as the count argument.
  expected = BaseVector::createNullConstant(ARRAY(REAL()), 3, pool());
  testExpression("try(repeat(C0, '-5'::INTEGER))", {elementVector}, expected);
  testExpressionWithError(
      "repeat(C0, '-5'::INTEGER)",
      {elementVector},
      "(-5 vs. 0) Count argument of repeat function must be greater than or equal to 0");

  testExpression(
      "try(repeat(C0, '10001'::INTEGER))", {elementVector}, expected);
  testExpressionWithError(
      "repeat(C0, '10001'::INTEGER)",
      {elementVector},
      "(10001 vs. 10000) Count argument of repeat function must be less than or equal to 10000");
}

TEST_F(RepeatTest, repeatAllowNegativeCount) {
  const auto elementVector = makeNullableFlatVector<float>(
      {0.0, -2.0, 3.333333, 4.0004, std::nullopt, 5.12345});
  auto expected = makeArrayVector<float>({{}, {}, {}, {}, {}, {}});

  // Test negative count.
  auto countVector =
      makeNullableFlatVector<int32_t>({-1, -2, -3, -5, -10, -100});
  testExpression(
      "repeat_allow_negative_count(C0, C1)",
      {elementVector, countVector},
      expected);

  // Test using a constant as the count argument.
  testExpression(
      "repeat_allow_negative_count(C0, '-5'::INTEGER)",
      {elementVector},
      expected);

  // Test mixed case.
  expected = makeArrayVector<float>(
      {{0.0}, {-2.0, -2.0}, {}, {}, {}, {5.12345, 5.12345, 5.12345}});
  countVector = makeNullableFlatVector<int32_t>({1, 2, -1, 0, -10, 3});
  testExpression(
      "repeat_allow_negative_count(C0, C1)",
      {elementVector, countVector},
      expected);
}

TEST_F(RepeatTest, repeatAllowNegativeCountAbovePrestoLimit) {
  constexpr int32_t repeatCount = 10'452;
  const auto elementVector = makeFlatVector<int32_t>({repeatCount});
  const auto countVector = makeFlatVector<int32_t>({repeatCount});
  const auto expected = makeArrayVector<int32_t>(
      {std::vector<int32_t>(repeatCount, repeatCount)});

  testExpression(
      "repeat_allow_negative_count(C0, C1)",
      {elementVector, countVector},
      expected);
  testExpression(
      "repeat_allow_negative_count(C0, '10452'::INTEGER)",
      {elementVector},
      expected);
}
} // namespace
} // namespace bytedance::bolt::functions
