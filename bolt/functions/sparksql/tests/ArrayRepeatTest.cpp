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

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {

class ArrayRepeatTest : public SparkFunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
};

TEST_F(ArrayRepeatTest, supportsLargeRepeatCount) {
  const auto elementVector = makeFlatVector<int32_t>({1});
  const auto countVector = makeFlatVector<int32_t>({15'000});

  VectorPtr expected =
      makeArrayVector<int32_t>({std::vector<int32_t>(15'000, 1)});
  testExpression(
      "array_repeat(c0, c1)", {elementVector, countVector}, expected);
}

TEST_F(ArrayRepeatTest, nullAndNegativeCount) {
  const auto elementVector =
      makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt, 5});
  const auto countVector =
      makeNullableFlatVector<int32_t>({2, -1, std::nullopt, 3, 0});

  VectorPtr expected = makeNullableArrayVector<int32_t>({
      {{1, 1}},
      emptyArray,
      std::nullopt,
      {{std::nullopt, std::nullopt, std::nullopt}},
      emptyArray,
  });

  testExpression(
      "array_repeat(c0, c1)", {elementVector, countVector}, expected);
}

} // namespace bytedance::bolt::functions::sparksql::test
