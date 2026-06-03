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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"

using bytedance::bolt::test::emptyArray;

namespace bytedance::bolt::functions::flinksql::test {

namespace {
class JsonFunctionsTest : public FlinkFunctionBaseTest {
 protected:
  void setFlinkCompatible(bool value) {
    auto config = queryCtx_->queryConfig().rawConfigsCopy();
    config[core::QueryConfig::kEnableFlinkCompatible] = std::to_string(value);
    queryCtx_->testingOverrideConfigUnsafe(std::move(config));
  }
};

TEST_F(JsonFunctionsTest, flinkCompatibleConfigEnabled) {
  setFlinkCompatible(true);
  EXPECT_TRUE(queryCtx_->queryConfig().enableFlinkCompatible());
}

TEST_F(JsonFunctionsTest, jsonStrToMap) {
  auto evaluateExpr = [&](std::optional<StringView> json,
                          std::optional<bool> logFailuresOnly = std::nullopt) {
    VectorPtr jsonVector = makeNullableFlatVector<StringView>({json});
    auto expr = logFailuresOnly.has_value()
        ? fmt::format("json_str_to_map(c0, {})", logFailuresOnly.value())
        : "json_str_to_map(c0)";
    return evaluate<MapVector>(
        expr,
        makeRowVector({jsonVector}),
        SelectivityVector{jsonVector->size()},
        MAP(VARCHAR(), VARCHAR()));
  };

  using T = std::optional<
      std::vector<std::pair<StringView, std::optional<StringView>>>>;
  auto testJsonStrToMap = [&](const VectorPtr& result, T expectedVec = {}) {
    auto expected = makeNullableMapVector(std::vector<T>{expectedVec});
    ::bytedance::bolt::test::assertEqualVectors(expected, result);
  };

  setFlinkCompatible(true);
  testJsonStrToMap(evaluateExpr("{\"a\": \"1\"}"), {{{"a", "1"}}});
  testJsonStrToMap(
      evaluateExpr(
          "{\"a\": \"1\", \"b\": {\"c\" : \"2\"}, \"d\": [\"3\", \"4\"]}"),
      {{{"a", "1"}, {"b", "{\"c\":\"2\"}"}, {"d", "[\"3\",\"4\"]"}}});
  testJsonStrToMap(evaluateExpr("{}"), emptyArray);
  testJsonStrToMap(evaluateExpr("{\"a\": null}"), {{{"a", std::nullopt}}});
  BOLT_ASSERT_THROW(
      evaluateExpr("{\"a\": \"1}"), "A string is opened, but never closed");
  BOLT_ASSERT_THROW(
      evaluateExpr("abc"), "The JSON element does not have the requested type");
  testJsonStrToMap(evaluateExpr("  "), std::nullopt);
  BOLT_ASSERT_THROW(evaluateExpr(std::nullopt), "input is null");

  testJsonStrToMap(evaluateExpr("{\"a\": \"1\"}", true), {{{"a", "1"}}});
  testJsonStrToMap(
      evaluateExpr(
          "{\"a\": \"1\", \"b\": {\"c\" : \"2\"}, \"d\": [\"3\", \"4\"]}",
          false),
      {{{"a", "1"}, {"b", "{\"c\":\"2\"}"}, {"d", "[\"3\",\"4\"]"}}});
  testJsonStrToMap(evaluateExpr("{}", true), emptyArray);
  testJsonStrToMap(
      evaluateExpr("{\"a\": null}", true), {{{"a", std::nullopt}}});
  testJsonStrToMap(evaluateExpr("{\"a\": \"1}", true), std::nullopt);
  BOLT_ASSERT_THROW(
      evaluateExpr("abc", false),
      "The JSON element does not have the requested type");
  testJsonStrToMap(evaluateExpr("abc", true), std::nullopt);
  testJsonStrToMap(evaluateExpr("  ", false), std::nullopt);
  testJsonStrToMap(evaluateExpr("  ", true), std::nullopt);
  testJsonStrToMap(evaluateExpr("", false), std::nullopt);
  BOLT_ASSERT_THROW(evaluateExpr(std::nullopt, false), "input is null");
  testJsonStrToMap(evaluateExpr(std::nullopt, true), std::nullopt);
}

TEST_F(JsonFunctionsTest, jsonStrToMapNestedEncoding) {
  auto testJsonStrToMap =
      [&](StringView json,
          std::vector<std::pair<StringView, std::optional<StringView>>>
              expectedVec) {
        auto jsonVector = makeNullableFlatVector<StringView>({json});
        auto jsonDictVector = wrapInDictionary(makeIndices({0}), jsonVector);
        auto logFailuresOnlyVector = makeConstant<bool>(true, 1);
        auto logFailuresOnlyDictVector = wrapInDictionary(
            makeIndices({0}), std::move(logFailuresOnlyVector));

        auto actual = evaluate<MapVector>(
            "json_str_to_map(c0, c1)",
            makeRowVector(
                {std::move(jsonDictVector),
                 std::move(logFailuresOnlyDictVector)}),
            SelectivityVector{jsonVector->size()},
            MAP(VARCHAR(), VARCHAR()));
        auto expected = makeMapVector<StringView, StringView>({expectedVec});
        ::bytedance::bolt::test::assertEqualVectors(expected, actual);
      };

  testJsonStrToMap("{\"a\": \"1\"}", {{"a", "1"}});
  testJsonStrToMap(
      "{\"a\": \"1\", \"b\": {\"c\" : \"2\"}, \"d\": [\"3\", \"4\"]}",
      {{"a", "1"}, {"b", "{\"c\":\"2\"}"}, {"d", "[\"3\",\"4\"]"}});
  testJsonStrToMap("{\"a\": null}", {{"a", std::nullopt}});
}

TEST_F(JsonFunctionsTest, jsonStrToArray) {
  auto evaluateExpr = [&](std::optional<StringView> json,
                          std::optional<bool> logFailuresOnly = std::nullopt) {
    VectorPtr jsonVector = makeNullableFlatVector<StringView>({json});
    auto expr = logFailuresOnly.has_value()
        ? fmt::format("json_str_to_array(c0, {})", logFailuresOnly.value())
        : "json_str_to_array(c0)";
    return evaluate<ArrayVector>(
        expr,
        makeRowVector({jsonVector}),
        SelectivityVector{jsonVector->size()},
        ARRAY(VARCHAR()));
  };

  using T = std::optional<std::vector<std::optional<StringView>>>;
  auto assertResult = [&](const VectorPtr& result, T expectedVec = {}) {
    auto expected = makeNullableArrayVector(std::vector<T>{expectedVec});
    ::bytedance::bolt::test::assertEqualVectors(expected, result);
  };

  setFlinkCompatible(true);
  assertResult(
      evaluateExpr("[\"测试组1\", \"测试组2\"]"), {{{"测试组1"}, {"测试组2"}}});
  BOLT_ASSERT_THROW(
      evaluateExpr("[\"测试组1\", \"测试组2]"),
      "A string is opened, but never closed");
  assertResult(evaluateExpr("[\"测试组1\", \"测试组2]", true), std::nullopt);
  assertResult(evaluateExpr("[]", true), emptyArray);
  assertResult(evaluateExpr("", true), std::nullopt);
  assertResult(
      evaluateExpr("[{\"c\":\"2\"}, {\"f\":2}]"),
      {{{"{\"c\":\"2\"}"}, {"{\"f\":2}"}}});
  assertResult(evaluateExpr(std::nullopt), std::nullopt);
  BOLT_ASSERT_THROW(
      evaluateExpr("test"),
      "The JSON element does not have the requested type");
  BOLT_ASSERT_THROW(
      evaluateExpr("{\"test\":\"map\"}"),
      "The JSON element does not have the requested type");
  assertResult(
      evaluateExpr("[[{\"c\":\"2\"},{\"f\":2}]]"),
      {{{"[{\"c\":\"2\"},{\"f\":2}]"}}});
  assertResult(evaluateExpr("  "), std::nullopt);
  assertResult(evaluateExpr("[\"a\", null]"), {{{"a"}, std::nullopt}});
}
} // namespace

} // namespace bytedance::bolt::functions::flinksql::test
