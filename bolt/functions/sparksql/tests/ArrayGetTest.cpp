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

#include <optional>
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::functions::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class ArrayGetTest : public SparkFunctionBaseTest {
 protected:
  template <typename T, typename IndexType>
  std::optional<T> arrayGet(
      const ArrayVectorPtr& arrayVector,
      const std::optional<IndexType>& index) {
    auto input =
        makeRowVector({arrayVector, makeConstant(index, arrayVector->size())});
    return evaluateOnce<T>("get(c0, c1)", input);
  }

  template <typename IndexType>
  void testArrayGet() {
    auto arrayVector = makeNullableArrayVector<int32_t>({{1, std::nullopt, 2}});

    auto result = arrayGet<int32_t, IndexType>(arrayVector, 0);
    EXPECT_EQ(result, 1);
    result = arrayGet<int32_t, IndexType>(arrayVector, 1);
    EXPECT_EQ(result, std::nullopt);
    result = arrayGet<int32_t, IndexType>(arrayVector, 2);
    EXPECT_EQ(result, 2);

    result = arrayGet<int32_t, IndexType>(arrayVector, -1);
    EXPECT_EQ(result, std::nullopt);
    result = arrayGet<int32_t, IndexType>(arrayVector, 3);
    EXPECT_EQ(result, std::nullopt);
    result = arrayGet<int32_t, IndexType>(arrayVector, std::nullopt);
    EXPECT_EQ(result, std::nullopt);
  }
};

TEST_F(ArrayGetTest, basic) {
  testArrayGet<int8_t>();
  testArrayGet<int16_t>();
  testArrayGet<int32_t>();
  testArrayGet<int64_t>();
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
