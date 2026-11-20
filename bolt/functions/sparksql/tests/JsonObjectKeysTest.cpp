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
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class JsonObjectKeysTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr jsonObjectKeys(const std::string& json) {
    auto varcharVector = makeFlatVector<std::string>({json});
    return evaluate("json_object_keys(c0)", makeRowVector({varcharVector}));
  }
};

TEST_F(JsonObjectKeysTest, basic) {
  auto expected =
      makeArrayVectorFromJson<std::string>({"[\"name\",\"age\",\"id\"]"});
  assertEqualVectors(
      jsonObjectKeys(R"({"name": "Alice", "age": 5, "id": "001"})"), expected);

  expected = makeArrayVectorFromJson<std::string>({"[]"});
  assertEqualVectors(jsonObjectKeys(R"({})"), expected);

  expected = makeNullableArrayVector<std::string>({std::nullopt});
  assertEqualVectors(jsonObjectKeys(R"(1)"), expected);
  assertEqualVectors(jsonObjectKeys(R"("hello")"), expected);
  assertEqualVectors(jsonObjectKeys(R"("")"), expected);
}

TEST_F(JsonObjectKeysTest, multipleRows) {
  auto input = makeFlatVector<std::string>({
      R"({"name": "Alice", "address": {"city": "Paris"}, "id": "001"})",
      R"({"k": 1})",
      R"({"k": "unterminated)",
      R"(1)",
      R"({"x": [1, 2], "y": null})",
  });
  auto expected = makeArrayVectorFromJson<std::string>({
      R"(["name", "address", "id"])",
      R"(["k"])",
      "null",
      "null",
      R"(["x", "y"])",
  });
  assertEqualVectors(
      expected, evaluate("json_object_keys(c0)", makeRowVector({input})));
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
