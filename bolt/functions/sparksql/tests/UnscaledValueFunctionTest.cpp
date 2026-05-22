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
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class UnscaledValueFunctionTest : public SparkFunctionBaseTest {};

TEST_F(UnscaledValueFunctionTest, unscaledValue) {
  auto testUnscaledValue = [&](const std::vector<int64_t>& unscaledValue,
                               const TypePtr& decimalType) {
    auto input = makeFlatVector<int64_t>(unscaledValue, decimalType);
    auto expected = makeFlatVector<int64_t>(unscaledValue);
    auto result = evaluate("unscaled_value(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  };

  testUnscaledValue({1000, 2000, -3000, -4000}, DECIMAL(18, 3));

  BOLT_ASSERT_THROW(
      testUnscaledValue({1000, 2000, -3000, -4000}, DECIMAL(20, 3)),
      "Expect short decimal type, but got: DECIMAL(20, 3)");
}

TEST_F(UnscaledValueFunctionTest, nullableShortDecimal) {
  auto input = makeNullableFlatVector<int64_t>(
      {1111111, std::nullopt, 10000}, DECIMAL(15, 10));
  auto expected =
      makeNullableFlatVector<int64_t>({1111111, std::nullopt, 10000});
  auto result = evaluate("unscaled_value(c0)", makeRowVector({input}));
  assertEqualVectors(expected, result);
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
