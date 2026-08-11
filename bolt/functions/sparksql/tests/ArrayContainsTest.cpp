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
namespace {

class ArrayContainsTest : public SparkFunctionBaseTest {};

TEST_F(ArrayContainsTest, rowWithNestedNulls) {
  const auto rowType =
      ROW({"author_type", "role_type"}, {INTEGER(), INTEGER()});
  const auto nullInt = variant(TypeKind::INTEGER);
  const auto infoList = makeArrayOfRowVector(
      rowType,
      std::vector<std::vector<variant>>{
          {variant::row({2, nullInt})},
          {variant::row({22, nullInt})},
      });
  const auto targetInfo = makeRowVector({
      makeNullableFlatVector<int32_t>({2, 22}),
      makeNullableFlatVector<int32_t>({1, std::nullopt}),
  });

  const auto result =
      evaluate("array_contains(c0, c1)", makeRowVector({infoList, targetInfo}));

  assertEqualVectors(makeFlatVector<bool>({false, true}), result);
}

TEST_F(ArrayContainsTest, arrayWithNestedNulls) {
  const auto data = makeNullableNestedArrayVector<int32_t>({
      {{{{{3, std::nullopt}}}}},
      {{{{{3, std::nullopt}}}}},
  });
  const auto search = makeArrayVectorFromJson<int32_t>({"[3, 3]", "[3, null]"});

  const auto result =
      evaluate("array_contains(c0, c1)", makeRowVector({data, search}));

  assertEqualVectors(makeFlatVector<bool>({false, true}), result);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
