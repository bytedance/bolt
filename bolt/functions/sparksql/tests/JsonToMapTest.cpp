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
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class JsonToMapTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr evaluateJsonToMap(const std::vector<StringView>& inputs) {
    const std::string expr = "json_to_map(c0)";
    return evaluate<MapVector>(
        expr, makeRowVector({makeFlatVector<StringView>({inputs[0]})}));
  }

  void testJsonToMap(
      const std::vector<StringView>& inputs,
      const std::vector<std::pair<StringView, std::optional<StringView>>>&
          expect) {
    auto result = evaluateJsonToMap(inputs);
    auto expectVector = makeMapVector<StringView, StringView>({expect});
    assertEqualVectors(expectVector, result);
  }
};

TEST_F(JsonToMapTest, basic) {
  {
    testJsonToMap(
        {R"({"a":1,"b":2,"c":3})"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  }

  {
    StringView json = StringView(R"({
    "car":{"make":"Toyota","model":"Camry","year":2018,"tire_pressure":[40.1,39.9,37.7,40.4]}
  })");
    StringView value = StringView(
        R"({"make":"Toyota","model":"Camry","year":2018,"tire_pressure":[40.1,39.9,37.7,40.4]})");
    testJsonToMap({json}, {{"car", value}});
  }

  {
    testJsonToMap(
        {R"({"a":[{"b":-100},{"b":null}]})"},
        {{"a", R"([{"b":-100},{"b":null}])"}});
  }

  {
    StringView json = StringView(R"( {
    "作家":{"姓名":"金庸","$性别":"男","##出生年份":1924,"作品集":["笑傲江湖","鹿鼎記","倚天屠龍記"]}})");
    StringView value = StringView(
        R"({"姓名":"金庸","$性别":"男","##出生年份":1924,"作品集":["笑傲江湖","鹿鼎記","倚天屠龍記"]})");
    testJsonToMap({json}, {{"作家", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a": "asd\\f\""})");
    StringView value = StringView(R"(asd\f")");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     0.000001})");
    StringView value = StringView(R"(0.000001)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     null})");
    StringView value = StringView(R"(null)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     "null"})");
    StringView value = StringView(R"(null)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"\u26A0":  "\u231B"})");
    StringView value = StringView("⌛");
    testJsonToMap({json}, {{"⚠", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a": [{"asd\\f\"":123}]})");
    StringView value = StringView(R"([{"asd\\f\"":123}])");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    // duplicated keys, just overwrite it, same behavior as java's implement
    testJsonToMap({R"({"a": [1, 223,      23], "a": 1})"}, {{"a", "1"}});
  }

  {
    // Valid non-object JSON should return empty map, same as hiveudf.
    // JSON null
    testJsonToMap({"null"}, {});
    // JSON number
    testJsonToMap({"123"}, {});
    // JSON boolean
    testJsonToMap({"true"}, {});
    testJsonToMap({"false"}, {});
    // JSON string
    testJsonToMap({R"("hello")"}, {});
    // JSON array
    testJsonToMap({R"([1,2,3])"}, {});
  }

  {
    // json parse failed, return null
    auto result = evaluateJsonToMap({R"({"a": [1, 223,      23], "a" 1})"});
    auto expectVector =
        makeNullableMapVector<StringView, StringView>({std::nullopt});
    assertEqualVectors(expectVector, result);
  }

  {
    // input is null, return null
    auto result = evaluate<MapVector>(
        "json_to_map(c0)",
        makeRowVector({makeNullableFlatVector<StringView>(
            {std::nullopt, R"({"a":1,"b":2,"c":3})"})}));
    auto expectVector = makeNullableMapVector<StringView, StringView>(
        {std::nullopt, {{{"a", "1"}, {"b", "2"}, {"c", "3"}}}});
    assertEqualVectors(expectVector, result);
  }
}

TEST_F(JsonToMapTest, DISABLED_spaceAfterColon) {
  // int, double, [], {} keep the blank spaces after ':'
  {
    StringView json = StringView(R"({"1\\23\"a":     "null"})");
    StringView value = StringView(R"(null)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     123})");
    StringView value = StringView(R"(     123)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     0.000001})");
    StringView value = StringView(R"(     0.000001)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     "asdf"})");
    StringView value = StringView(R"(asdf)");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }

  {
    StringView json = StringView(R"({"1\\23\"a":     ["asdf"]})");
    StringView value = StringView(R"(     ["asdf"])");
    testJsonToMap({json}, {{"1\\23\"a", value}});
  }
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
