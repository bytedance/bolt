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

TEST_F(JsonToMapTest, unescapedControlChars) {
  // Strict JSON requires control chars (\x00-\x1f) inside strings to be
  // escaped, but the reference Hive UDF (backed by com.jsoniter) accepts raw
  // control chars and keeps them verbatim in the values. json_to_map must
  // match that lenient behavior instead of returning SQL NULL.
  {
    // Raw newlines embedded in several values; they must be preserved.
    StringView json =
        StringView("{\"a\":\"x\ny\",\"b\":\"plain\",\"c\":\"end\n\"}");
    testJsonToMap(
        {json},
        {{"a", StringView("x\ny")},
         {"b", "plain"},
         {"c", StringView("end\n")}});
  }

  {
    // Other raw control chars (tab, carriage return) are likewise preserved.
    StringView json = StringView("{\"k\":\"a\tb\rc\"}");
    testJsonToMap({json}, {{"k", StringView("a\tb\rc")}});
  }

  {
    // Backspace/form-feed and a generic control char (no named escape) are all
    // accepted and preserved verbatim.
    StringView json = StringView("{\"k\":\"\b\f\x01\x1f\"}");
    testJsonToMap({json}, {{"k", StringView("\b\f\x01\x1f")}});
  }

  {
    // A raw control char in the key is also accepted.
    StringView json = StringView("{\"key\nname\":\"v\"}");
    testJsonToMap({json}, {{StringView("key\nname"), "v"}});
  }
}

TEST_F(JsonToMapTest, numericValuesPreserveOriginalText) {
  // Numeric values must be returned as their original JSON token, matching the
  // Hive reference UDF (com.jsoniter). They must NOT be round-tripped through a
  // double (which loses precision and reformats) nor rejected when the value is
  // out of double range.
  {
    // Large integer that does not fit in a double: must keep all digits.
    testJsonToMap(
        {R"({"v":6222000000000000000001125652734})"},
        {{"v", "6222000000000000000001125652734"}});
  }
  {
    // High-precision decimal: must not be rounded to the nearest double.
    testJsonToMap(
        {R"({"v":221.5222225222225222})"}, {{"v", "221.5222225222225222"}});
  }
  {
    // Exponent form must be preserved, not expanded.
    testJsonToMap({R"({"v":1e10})"}, {{"v", "1e10"}});
  }
  {
    // Out-of-double-range exponent must not fail the whole parse.
    testJsonToMap({R"({"v":1e400})"}, {{"v", "1e400"}});
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
