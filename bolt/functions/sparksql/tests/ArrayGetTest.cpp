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

#include <limits>
#include <optional>
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::functions::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class ArrayGetTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> arrayGet(
      const ArrayVectorPtr& arrayVector,
      const std::optional<int32_t>& index) {
    auto input =
        makeRowVector({arrayVector, makeConstant(index, arrayVector->size())});
    return evaluateOnce<T>("get(c0, c1)", input);
  }

  template <typename T>
  std::optional<T> arrayGetBigint(
      const ArrayVectorPtr& arrayVector,
      const std::optional<int64_t>& index) {
    auto input =
        makeRowVector({arrayVector, makeConstant(index, arrayVector->size())});
    return evaluateOnce<T>("get(c0, c1)", input);
  }
};

TEST_F(ArrayGetTest, basic) {
  auto arrayVector = makeNullableArrayVector<int32_t>({{1, 2, std::nullopt}});
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, -1), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 0), 1);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 1), 2);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 2), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, 3), std::nullopt);
  EXPECT_EQ(arrayGet<int32_t>(arrayVector, std::nullopt), std::nullopt);
}

TEST_F(ArrayGetTest, bigintIndex) {
  auto arrayVector = makeNullableArrayVector<int32_t>({{1, 2, std::nullopt}});
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, -1), std::nullopt);
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, 0), 1);
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, 1), 2);
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, 2), std::nullopt);
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, 3), std::nullopt);
  EXPECT_EQ(
      arrayGetBigint<int32_t>(
          arrayVector, int64_t{std::numeric_limits<int32_t>::max()} + 1),
      std::nullopt);
  EXPECT_EQ(arrayGetBigint<int32_t>(arrayVector, std::nullopt), std::nullopt);
}

TEST_F(ArrayGetTest, bigintIndexVector) {
  auto arrayVector = makeNullableArrayVector<int32_t>({
      {1, 2, 3},
      {4, 5},
      {std::nullopt},
      {6},
  });
  auto input = makeRowVector({
      arrayVector,
      makeFlatVector<int64_t>(
          {0, 1, 0, int64_t{std::numeric_limits<int32_t>::max()} + 1}),
  });
  auto expected =
      makeNullableFlatVector<int32_t>({1, 5, std::nullopt, std::nullopt});
  auto result = evaluate("get(c0, c1)", input);
  assertEqualVectors(expected, result);
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
