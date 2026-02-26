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

#include "bolt/functions/prestosql/json/JsonExtractor.h"

#include <folly/Benchmark.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "bolt/common/base/BoltException.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"

using bytedance::bolt::functions::follyJsonExtractScalar;

using bytedance::bolt::BoltUserError;
using bytedance::bolt::functions::jsonExtract;
using bytedance::bolt::functions::jsonExtractScalar;
using bytedance::bolt::functions::jsonExtractSize;
using bytedance::bolt::functions::jsonExtractTuple;
using folly::json::parse_error;
using namespace std::string_literals;

#define EXPECT_SCALAR_VALUE_EQ(json, path, ret) \
  {                                             \
    auto val = jsonExtractScalar(json, path);   \
    EXPECT_TRUE(val.hasValue());                \
    EXPECT_EQ(val.value(), ret);                \
  }

#define EXPECT_JSON_SIZE_EQ(json, path, ret) \
  {                                          \
    auto val = jsonExtractSize(json, path);  \
    EXPECT_TRUE(val.hasValue());             \
    EXPECT_EQ(val.value(), ret);             \
  }

#define EXPECT_SCALAR_VALUE_NULL(json, path) \
  EXPECT_FALSE(jsonExtractScalar(json, path).hasValue())

#define EXPECT_JSON_VALUE_EQ(json, path, ret)        \
  {                                                  \
    auto val = json_format(jsonExtract(json, path)); \
    EXPECT_TRUE(val.hasValue());                     \
    EXPECT_EQ(val.value(), ret);                     \
  }

#define EXPECT_JSON_VALUE_NULL(json, path) \
  EXPECT_FALSE(json_format(jsonExtract(json, path)).hasValue())

#define EXPECT_THROW_INVALID_ARGUMENT(json, path) \
  EXPECT_THROW(jsonExtract(json, path), BoltUserError)

namespace {
folly::Optional<std::string> json_format(
    const folly::Optional<folly::dynamic>& json) {
  if (json.has_value()) {
    return folly::toJson(json.value());
  }
  return folly::none;
}
} // namespace

TEST(JsonExtractorTest, generalJsonTest) {
  // clang-format off
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  // clang-format on
  std::replace(json.begin(), json.end(), '\'', '\"');
  auto res = json_format(jsonExtract(json, "$.store.fruit[0].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("8", res.value());
  res = json_format(jsonExtract(json, "$.store.fruit[1].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("9", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract(json, "$.store.fruit[2].weight"s)).has_value());
  res = json_format(jsonExtract(json, "$.store.fruit[*].weight"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[8,9]", res.value());
  res = json_format(jsonExtract(json, "$.store.fruit[*].type"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[\"apple\",\"pear\"]", res.value());
  res = json_format(jsonExtract(json, "$.store.book[0].price"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("8.95", res.value());
  res = json_format(jsonExtract(json, "$.store.book[2].category"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"fiction\"", res.value());
  res = json_format(jsonExtract(json, "$.store.basket[1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[3,4]", res.value());
  res = json_format(jsonExtract(json, "$.store.basket[0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(
      folly::parseJson("[1,2,{\"a\":\"x\",\"b\":\"y\"}]"),
      folly::parseJson(res.value()));
  EXPECT_FALSE(
      json_format(jsonExtract(json, "$.store.baskets[1]"s)).has_value());
  res = json_format(jsonExtract(json, "$[\"e mail\"]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"amy@only_for_json_udf_test.net\"", res.value());
  res = json_format(jsonExtract(json, "$.owner"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("\"amy\"", res.value());
  res = json_format(
      jsonExtract("[[1.1,[2.1,2.2]],2,{\"a\":\"b\"}]"s, "$[0][1][1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2.2", res.value());
  res = json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2", res.value());
  res = json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[2]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract("[1,2,{\"a\":\"b\"}]"s, "$[3]"s)).has_value());
  res = json_format(jsonExtract("[{\"a\":\"b\"}]"s, "$[0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());
  EXPECT_FALSE(
      json_format(jsonExtract("[{\"a\":\"b\"}]"s, "$[2]"s)).has_value());
  res = json_format(jsonExtract("{\"a\":\"b\"}"s, " $ "s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("{\"a\":\"b\"}", res.value());

  std::string json2 =
      "[[{\"key\": 1, \"value\": 2},"
      "{\"key\": 2, \"value\": 4}],"
      "[{\"key\": 3, \"value\": 6},"
      "{\"key\": 4, \"value\": 8},"
      "{\"key\": 5, \"value\": 10}]]";

  // Key value pair order of a JsonMap may be changed after folly::toJson
  std::string expected("[[{\"key\":1,\"value\":2},{\"key\":2,\"value\":4}],");
  expected.append("[{\"key\":3,\"value\":6},{\"key\":4,\"value\":8},")
      .append("{\"key\":5,\"value\":10}]]");
  auto expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  expected.clear();
  expected.append("[{\"key\":1,\"value\":2},{\"key\":2,\"value\":4},")
      .append("{\"key\":3,\"value\":6},")
      .append("{\"key\":4,\"value\":8},{\"value\":10,\"key\":5}]");
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][*]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  res = json_format(jsonExtract(json2, "$[*][*].key"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[1,2,3,4,5]", res.value());

  expected = "[{\"key\":1,\"value\":2},{\"key\":3,\"value\":6}]";
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][0]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  expected = "{\"key\":5,\"value\":10}";
  expectedJson = folly::parseJson(expected);
  res = json_format(jsonExtract(json2, "$[*][2]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ(expectedJson, folly::parseJson(res.value()));

  // TEST whitespaces in Path and Json
  res = json_format(jsonExtract(json2, "$[*][*].key"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("[1,2,3,4,5]", res.value());
  res = json_format(
      jsonExtract(" [ [1.1,[2.1,2.2]],2, {\"a\": \"b\"}]"s, "$[0][1][1]"s));
  EXPECT_TRUE(res.has_value());
  EXPECT_EQ("2.2", res.value());
  EXPECT_SCALAR_VALUE_NULL(json2, "  \t\n "s);
}

// Test compatibility with Presto
// Reference: from https://github.com/prestodb/presto
// presto-main/src/test/java/com/facebook/presto/operator/scalar/TestJsonExtract.java
TEST(JsonExtractorTest, scalarValueTest) {
  EXPECT_SCALAR_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_SCALAR_VALUE_EQ("-1"s, "$"s, "-1"s);
  EXPECT_SCALAR_VALUE_EQ("\"abc\""s, "$"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("\"\""s, "$"s, ""s);
  EXPECT_SCALAR_VALUE_EQ("null"s, "$"s, "null"s);

  // Test character escaped values
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0001c\""s, "$"s, "ab\001c"s);
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0002c\""s, "$"s, "ab\002c"s);

  EXPECT_JSON_VALUE_EQ("[1,2,3]"s, "$"s, "[1,2,3]"s);
  EXPECT_JSON_VALUE_EQ("[1,   2,   3]"s, "$"s, "[1,2,3]"s);
  EXPECT_JSON_VALUE_EQ("{\"a\": 1}"s, "$"s, "{\"a\":1}"s);
  EXPECT_JSON_VALUE_EQ("{\"a\":        1}"s, "$"s, "{\"a\":1}"s);
}

TEST(JsonExtractorTest, jsonValueTest) {
  // Check scalar values
  EXPECT_JSON_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_JSON_VALUE_EQ("-1"s, "$"s, "-1"s);
  EXPECT_JSON_VALUE_EQ("0.01"s, "$"s, "0.01"s);
  EXPECT_JSON_VALUE_EQ("\"abc\""s, "$"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("\"\""s, "$"s, "\"\""s);
  EXPECT_JSON_VALUE_EQ("null"s, "$"s, "null"s);

  // Test character escaped values
  EXPECT_JSON_VALUE_EQ("\"ab\\u0001c\""s, "$"s, "\"ab\\u0001c\""s);
  EXPECT_JSON_VALUE_EQ("\"ab\\u0002c\""s, "$"s, "\"ab\\u0002c\""s);

  // Complex types should return json values
  EXPECT_JSON_VALUE_EQ("[1, 2, 3]"s, "$"s, "[1,2,3]"s);
  EXPECT_JSON_VALUE_EQ("{\"a\": 1}"s, "$"s, "{\"a\":1}"s);
}

TEST(JsonExtractorTest, arrayJsonValueTest) {
  EXPECT_JSON_VALUE_NULL("[]"s, "$[0]"s);
  EXPECT_JSON_VALUE_EQ("[1, 2, 3]"s, "$[0]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[1, 2]"s, "$[1]"s, "2"s);
  EXPECT_JSON_VALUE_EQ("[1, null]"s, "$[1]"s, "null"s);
  // Out of bounds
  EXPECT_JSON_VALUE_NULL("[1]"s, "$[1]"s);
  // Check skipping complex structures
  EXPECT_JSON_VALUE_EQ("[{\"a\": 1}, 2, 3]"s, "$[1]"s, "2"s);
}

TEST(JsonExtractorTest, objectJsonValueTest) {
  EXPECT_JSON_VALUE_NULL("{}"s, "$.fuu"s);
  EXPECT_JSON_VALUE_NULL("{\"a\": 1}"s, "$.fuu"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"a\": 0, \"fuu\": 1}"s, "$.fuu"s, "1"s);
  // Check skipping complex structures
  EXPECT_JSON_VALUE_EQ("{\"a\": [1, 2, 3], \"fuu\": 1}"s, "$.fuu"s, "1"s);
}

TEST(JsonExtractorTest, fullScalarTest) {
  EXPECT_JSON_VALUE_EQ("{}"s, "$"s, "{}");
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\":1}}"s, "$.fuu"s, "{\"bar\":1}");
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": {\"bar\":          1}}"s, "$.fuu"s, "{\"bar\":1}");
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$[fuu]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"ab\\\"cd\\\"ef\": 2}"s, "$[\"ab\\\"cd\\\"ef\"]"s, "2"s);
  EXPECT_SCALAR_VALUE_NULL("{\"fuu\": null}"s, "$.fuu"s);
  EXPECT_SCALAR_VALUE_NULL("{\"fuu\": 1}"s, "$.bar"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\001"s); // Test escaped characters
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": 1, \"bar\": \"abc\"}"s, "$.bar"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$.fuu[0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1]"s, "[100,101]");
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1][1]"s, "101"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}"s,
      "$.fuu[1].bar.key[0]"s,
      "value"s);

  // Test non-object extraction
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[0]"s, "0"s);
  EXPECT_SCALAR_VALUE_EQ("\"abc\""s, "$"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_SCALAR_VALUE_EQ("null"s, "$"s, "null"s);

  // Test numeric path expression matches arrays and objects
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$.1"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[1]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("[0, 1, 2]"s, "$[\"1\"]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$.1"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[1]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[\"1\"]"s, "1"s);

  // Test fields starting with a digit
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$.30day"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[30day]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[\"30day\"]"s, "1"s);
}

TEST(JsonExtractorTest, fullJsonValueTest) {
  EXPECT_JSON_VALUE_EQ("{}"s, "$"s, "{}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$.fuu"s, "{\"bar\":1}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$.fuu"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[fuu]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": null}"s, "$.fuu"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\": 1}"s, "$.bar"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1, \"bar\": \"abc\"}"s, "$.bar"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$.fuu[0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$.fuu[1][1]"s, "101"s);

  // Test non-object extraction
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[0]"s, "0"s);
  EXPECT_JSON_VALUE_EQ("\"abc\""s, "$"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_JSON_VALUE_EQ("null"s, "$"s, "null"s);

  // Test extraction using bracket json path
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"]"s, "{\"bar\":1}"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"][\"bar\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": 1}"s, "$[\"fuu\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": null}"s, "$[\"fuu\"]"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\": 1}"s, "$[\"bar\"]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$[\"fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": 1, \"bar\": \"abc\"}"s, "$[\"bar\"]"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": [0.1, 1, 2]}"s, "$[\"fuu\"][0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$[\"fuu\"][1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [0, [100, 101], 2]}"s, "$[\"fuu\"][1][1]"s, "101"s);

  // Test extraction using bracket json path with special json characters in
  // path
  EXPECT_JSON_VALUE_EQ(
      "{\"@$fuu\": {\".b.ar\": 1}}"s, "$[\"@$fuu\"]"s, "{\".b.ar\":1}"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu..\": 1}"s, "$[\"fuu..\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fu*u\": null}"s, "$[\"fu*u\"]"s, "null"s);
  EXPECT_JSON_VALUE_NULL("{\",fuu\": 1}"s, "$[\"bar\"]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\",fuu\": [\"\\u0001\"]}"s,
      "$[\",fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\":fu:u:\": 1, \":b:ar:\": \"abc\"}"s, "$[\":b:ar:\"]"s, "\"abc\""s);
  EXPECT_JSON_VALUE_EQ(
      "{\"?()fuu\": [0.1, 1, 2]}"s, "$[\"?()fuu\"][0]"s, "0.1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"f?uu\": [0, [100, 101], 2]}"s, "$[\"f?uu\"][1]"s, "[100,101]"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu()\": [0, [100, 101], 2]}"s, "$[\"fuu()\"][1][1]"s, "101"s);

  // Test extraction using mix of bracket and dot notation json path
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$[\"fuu\"].bar"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"fuu\": {\"bar\": 1}}"s, "$.fuu[\"bar\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$[\"fuu\"][0]"s,
      "\"\\u0001\""s); // Test escaped characters
  EXPECT_JSON_VALUE_EQ(
      "{\"fuu\": [\"\\u0001\"]}"s,
      "$.fuu[0]"s,
      "\"\\u0001\""s); // Test escaped characters

  // Test extraction using  mix of bracket and dot notation json path with
  // special json characters in path
  EXPECT_JSON_VALUE_EQ("{\"@$fuu\": {\"bar\": 1}}"s, "$[\"@$fuu\"].bar"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\",fuu\": {\"bar\": [\"\\u0001\"]}}"s,
      "$[\",fuu\"].bar[0]"s,
      "\"\\u0001\""s); // Test escaped characters

  // Test numeric path expression matches arrays and objects
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$.1"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[1]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("[0, 1, 2]"s, "$[\"1\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$.1"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[1]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"0\" : 0, \"1\" : 1, \"2\" : 2 }"s, "$[\"1\"]"s, "1"s);

  // Test fields starting with a digit
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$.30day"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[30day]"s, "1"s);
  EXPECT_JSON_VALUE_EQ(
      "{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2 }"s, "$[\"30day\"]"s, "1"s);
  EXPECT_JSON_VALUE_EQ("{\"a\\\\b\": 4}"s, "$[\"a\\\\b\"]"s, "4"s);
  EXPECT_JSON_VALUE_NULL("{\"fuu\" : null}"s, "$.a.b"s);
}

TEST(JsonExtractorTest, invalidJsonPathTest) {
  EXPECT_SCALAR_VALUE_NULL(""s, ""s);
  EXPECT_SCALAR_VALUE_NULL("{}"s, "$.bar[2][-1]"s);
  EXPECT_SCALAR_VALUE_NULL("{}"s, "$.fuu..bar"s);
  EXPECT_SCALAR_VALUE_NULL("{}"s, "$."s);
  EXPECT_SCALAR_VALUE_NULL(""s, "$$"s);
  EXPECT_SCALAR_VALUE_NULL(""s, " "s);
  EXPECT_SCALAR_VALUE_NULL(""s, "."s);
  EXPECT_SCALAR_VALUE_NULL(
      "{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }"s,
      "$.store.book["s);
}

TEST(JsonExtractorTest, reextractJsonTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  auto originalJsonObj = jsonExtract(json, "$");
  // extract the same json json by giving the root path
  auto reExtractedJsonObj = jsonExtract(originalJsonObj.value(), "$");
  ASSERT_TRUE(reExtractedJsonObj.hasValue());
  // expect the re-extracted json object to be the same as the original jsonObj
  EXPECT_EQ(originalJsonObj.value(), reExtractedJsonObj.value());
}

TEST(JsonExtractorTest, jsonMultipleExtractsTest) {
  std::string json = R"DELIM(
      {"store":
        {"fruit":[
          {"weight":8, "type":"apple"},
          {"weight":9, "type":"pear"}],
         "basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],
         "book":[
            {"author":"Nigel Rees",
             "title":"Sayings of the Century",
             "category":"reference",
             "price":8.95},
            {"author":"Herman Melville",
             "title":"Moby Dick",
             "category":"fiction",
             "price":8.99,
             "isbn":"0-553-21311-3"},
            {"author":"J. R. R. Tolkien",
             "title":"The Lord of the Rings",
             "category":"fiction",
             "reader":[
                {"age":25,
                 "name":"bob"},
                {"age":26,
                 "name":"jack"}],
             "price":22.99,
             "isbn":"0-395-19395-8"}],
          "bicycle":{"price":19.95, "color":"red"}},
        "e mail":"amy@only_for_json_udf_test.net",
        "owner":"amy"})DELIM";
  auto extract1 = jsonExtract(json, "$.store");
  ASSERT_TRUE(extract1.hasValue());
  auto extract2 = jsonExtract(extract1.value(), "$.fruit");
  ASSERT_TRUE(extract2.hasValue());
  EXPECT_EQ(jsonExtract(json, "$.store.fruit").value(), extract2.value());
}

TEST(JsonExtractorTest, simdJsonScalarValueTest) {
  // Check scalar values
  EXPECT_SCALAR_VALUE_EQ("123"s, "$"s, "123"s);
  EXPECT_SCALAR_VALUE_EQ("-1"s, "$"s, "-1"s);
  EXPECT_SCALAR_VALUE_EQ("0.01"s, "$"s, "0.01"s);

  EXPECT_SCALAR_VALUE_EQ("\"abc\""s, "$"s, "abc"s);
  EXPECT_SCALAR_VALUE_EQ("\"\""s, "$"s, ""s);

  EXPECT_SCALAR_VALUE_EQ("null"s, "$"s, "null"s); // null json
  EXPECT_SCALAR_VALUE_EQ("\"null\""s, "$"s, "null"s); // "null" string

  // Test character escaped values
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0001c\""s, "$"s, "ab\u0001c"s);
  EXPECT_SCALAR_VALUE_EQ("\"ab\\u0002c\""s, "$"s, "ab\u0002c"s);

  // utf-8
  EXPECT_SCALAR_VALUE_EQ("\"中文\""s, "$"s, "中文"s);
}

TEST(JsonExtractorTest, simdJsonSimpleArrayTest) {
  EXPECT_SCALAR_VALUE_EQ("[1, 2, 3]"s, "$[0]"s, "1"s);
  EXPECT_SCALAR_VALUE_EQ("[1, 2]"s, "$[1]"s, "2"s);

  EXPECT_SCALAR_VALUE_NULL("[1, null]"s, "$[1]"s);
  EXPECT_SCALAR_VALUE_NULL("[1, null]"s, "$[2]"s); // Out of bounds
  EXPECT_SCALAR_VALUE_NULL("[1]"s, "$[1]"s);
}

TEST(JsonExtractorTest, quotedUnicode) {
  // unicode control character quoted
  std::string json2 =
      R"( {"category":{"origin_string":"=\u0007mEs\u000fA���%חk�9ded06cd author: John Doe"},"server_uuid":"1234"} )";
  std::string path2 = "$.category";
  std::string expect2 =
      "{\"origin_string\":\"=\\u0007mEs\\u000FA���%חk�9ded06cd author: John Doe\"}";
  EXPECT_JSON_VALUE_EQ(json2, path2, expect2);
}

TEST(JsonExtractorTest, simdJsonTypesTest) {
  std::string json = R"json(
    {
      "arr" : [1, 2, 3],
      "str" : "你好",
      "boolean" : true,
      "int": 1,
      "negative" : -2,
      "double" : 3.1415926,
      "null" : null,
      "big_integer" : 22675263000000000000,
      "nested" : { "arr" : [1,2,4]}
    }
  )json";

  EXPECT_SCALAR_VALUE_EQ(json, "$.str", "你好");
  EXPECT_SCALAR_VALUE_EQ(json, "$.boolean", "true");
  EXPECT_SCALAR_VALUE_EQ(json, "$.int", "1");
  EXPECT_SCALAR_VALUE_EQ(json, "$.negative", "-2");
  EXPECT_SCALAR_VALUE_EQ(json, "$.double", "3.1415926");
  EXPECT_SCALAR_VALUE_NULL(json, "$.null");
  EXPECT_SCALAR_VALUE_EQ(json, "$.nested.arr[1]", "2");
  EXPECT_SCALAR_VALUE_EQ(json, "$.big_integer", "22675263000000000000");

  // Check skipping complex structures
  // int, double, string, boolean in array.
  EXPECT_SCALAR_VALUE_EQ("[{\"a\": 1}, 2, false, \"中文\", 4]"s, "$[1]"s, "2"s);
  EXPECT_SCALAR_VALUE_EQ(
      "[{\"a\": 1}, 2, true, \"中文\", -4.4]"s, "$[2]"s, "true"s);
  EXPECT_SCALAR_VALUE_EQ(
      "[{\"a\": 1}, 2, false, \"中文\", 4]"s, "$[3]"s, "中文"s);
  EXPECT_SCALAR_VALUE_EQ(
      "[{\"a\": 1}, 2, true, \"中文\", -4.4]"s, "$[4]"s, "-4.4"s);
}

TEST(JsonExtractorTest, simdJsonJsonSize) {
  std::string json{R"json(
        {"a" : {"b":1, "c":null, "d": "data"}, "arr": [1,2,3,4,5]}
      )json"};

  EXPECT_JSON_SIZE_EQ(json, "$"s, 2);
  EXPECT_JSON_SIZE_EQ(json, "$.a"s, 3);
  EXPECT_JSON_SIZE_EQ(json, "$.arr"s, 5);
}

TEST(JsonExtractorTest, benchmark) {
  // a json segment from
  // https://github.com/simdjson/simdjson/blob/master/jsonexamples/twitter.json
  std::string json = R"json(
    {
      "user": {
          "id": 903487807,
          "id_str": "903487807",
          "name": "RT&ファボ魔のむっつんさっm",
          "screen_name": "yuttari1998",
          "location": "関西    ↓詳しいプロ↓",
          "description": "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
          "url": "http://t.co/Yg9e1Fl8wd",
          "entities": {
            "url": {
              "urls": [
                {
                  "url": "http://t.co/Yg9e1Fl8wd",
                  "expanded_url": "http://twpf.jp/yuttari1998",
                  "display_url": "twpf.jp/yuttari1998",
                  "indices": [
                    0,
                    22
                  ]
                }
              ]
            },
            "description": {
              "urls": []
            }
          },
          "protected": false,
          "followers_count": 95,
          "friends_count": 158,
          "listed_count": 1,
          "created_at": "Thu Oct 25 08:27:13 +0000 2012",
          "favourites_count": 3652,
          "utc_offset": null,
          "time_zone": null,
          "geo_enabled": false,
          "verified": false,
          "statuses_count": 10276,
          "lang": "ja",
          "contributors_enabled": false,
          "is_translator": false,
          "is_translation_enabled": false,
          "profile_background_color": "C0DEED",
          "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
          "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
          "profile_background_tile": false,
          "profile_image_url": "http://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
          "profile_image_url_https": "https://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
          "profile_banner_url": "https://pbs.twimg.com/profile_banners/903487807/1409062272",
          "profile_link_color": "0084B4",
          "profile_sidebar_border_color": "C0DEED",
          "profile_sidebar_fill_color": "DDEEF6",
          "profile_text_color": "333333",
          "profile_use_background_image": true,
          "default_profile": true,
          "default_profile_image": false,
          "following": false,
          "follow_request_sent": false,
          "notifications": false
        }
    }
  )json";

  std::vector<std::string> paths{
      "$.user.id_str",
      "$.user.id",
      "$.user.location",
      "$.user.description",
      "$.user.entities.url.urls[0].indices[1]",
      "$.user.followers_count",
      "$.user.friends_count",
      "$.user.profile_background_image_url",
      "$.user.is_translation_enabled",
      "$.user.notifications",
  };

  //  "$.user.time_zone",

  for (size_t j = 0; j < paths.size(); j++) {
    auto val = jsonExtractScalar(json, paths[j]);
    EXPECT_TRUE(val.hasValue());
  }

  // std::random_device rd;
  // std::mt19937 mt(rd());
  // std::uniform_real_distribution<double> dist(1, 100);

  constexpr size_t rounds = 1024 * 256;
  size_t rnd = 0; // ((size_t)dist(mt)) % 2;   // To stop optimization

  // simdJSON
  // auto start = std::chrono::high_resolution_clock::now();

  // for (size_t i = 0; i < rounds; i++) {
  //   for (size_t j = 0; j < paths.size() - rnd; j++) {
  //     auto val = jsonExtractScalar(json, paths[j+rnd]);
  //     if (val.hasValue()) {
  //       sum++;
  //     }
  //   }
  // }

  // auto end = std::chrono::high_resolution_clock::now();
  // auto time_span =
  // std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
  // std::cout << "It took  " << time_span.count() << " seconds." << std::endl;

  // Folly
  // start = std::chrono::high_resolution_clock::now();
  // for (size_t i = 0; i < rounds; i++) {
  //   for (size_t j = 0; j < paths.size() - rnd; j++) {
  //     auto val = follyJsonExtractScalar(json, paths[j+rnd]);
  //     if (val.hasValue()) {
  //       sum++;
  //     }
  //   }
  // }

  // end = std::chrono::high_resolution_clock::now();
  // time_span = std::chrono::duration_cast<std::chrono::duration<double>>(end -
  // start); std::cout << "It took  " << time_span.count() << " seconds." <<
  // std::endl; std::cout << sum << std::endl;
}

TEST(JsonExtractorTest, JsonTupleExtractScalar) {
  std::string json = R"json(
  {
    "a": {
      "b": {
        "c": "value1",
        "d": "value2"
      }
    },
    "e.f": "value3",
    "g.h.i": "value4"
  }
  )json";
  {
    auto val = jsonExtractScalar(json, "$.a.b.c", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "a.b.c", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "$.a.b.d", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "a.b.d", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "$.e.f", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "e.f", false, false, true, false);
    EXPECT_TRUE(val.hasValue());
    EXPECT_EQ(val.value(), "value3");
  }
  {
    auto val = jsonExtractScalar(json, "$.g.h.i", false, false, true, false);
    EXPECT_FALSE(val.hasValue());
  }
  {
    auto val = jsonExtractScalar(json, "g.h.i", false, false, true, false);
    EXPECT_TRUE(val.hasValue());
    EXPECT_EQ(val.value(), "value4");
  }
}

TEST(JsonExtractTupleTest, normal) {
  // a json segment from
  // https://github.com/simdjson/simdjson/blob/master/jsonexamples/twitter.json
  std::string json = R"json(
    {
      "id": 903487807,
      "id_str": "903487807",
      "name": "RT&ファボ魔のむっつんさっm",
      "screen_name": "yuttari1998",
      "location": "関西    ↓詳しいプロ↓",
      "description": "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
      "url": "http://t.co/Yg9e1Fl8wd",
      "entities": {
        "url": {
          "urls": [
            {
              "url": "http://t.co/Yg9e1Fl8wd",
              "expanded_url": "http://twpf.jp/yuttari1998",
              "display_url": "twpf.jp/yuttari1998",
              "indices": [
                0,
                22
              ]
            }
          ]
        },
        "description": {
          "urls": []
        }
      },
      "protected": false,
      "followers_count": 95,
      "friends_count": 158,
      "listed_count": 1,
      "created_at": "Thu Oct 25 08:27:13 +0000 2012",
      "favourites_count": 3652,
      "utc_offset": null,
      "time_zone": null,
      "geo_enabled": false,
      "verified": false,
      "statuses_count": 10276,
      "lang": "ja",
      "contributors_enabled": false,
      "is_translator": false,
      "is_translation_enabled": false,
      "profile_background_color": "C0DEED",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_tile": false,
      "profile_image_url": "http://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_image_url_https": "https://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/903487807/1409062272",
      "profile_link_color": "0084B4",
      "profile_sidebar_border_color": "C0DEED",
      "profile_sidebar_fill_color": "DDEEF6",
      "profile_text_color": "333333",
      "profile_use_background_image": true,
      "default_profile": true,
      "default_profile_image": false,
      "following": false,
      "follow_request_sent": false,
      "notifications": false
    }
  )json";

  std::vector<std::string> paths{
      "id_str",
      "id",
      "location",
      "description",
      "entities.url.urls[0].indices[1]",
      "followers_count",
      "friends_count",
      "profile_background_image_url",
      "profile_use_background_image",
      "notifications",
      "entities"};
  std::vector<folly::Optional<folly::StringPiece>> stringPieces;

  for (const auto& path : paths) {
    stringPieces.emplace_back(path);
  }
  auto val = jsonExtractTuple(json, stringPieces);
  EXPECT_EQ(stringPieces.size(), val.size());
  EXPECT_TRUE(val[0].hasValue());
  EXPECT_EQ(val[0].value(), "903487807");
  EXPECT_TRUE(val[1].hasValue());
  EXPECT_EQ(val[1].value(), "903487807");
  EXPECT_TRUE(val[2].hasValue());
  EXPECT_EQ(val[2].value(), "関西    ↓詳しいプロ↓");
  EXPECT_TRUE(val[3].hasValue());
  EXPECT_EQ(
      val[3].value(),
      "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます");
  EXPECT_FALSE(val[4].hasValue());
  EXPECT_TRUE(val[5].hasValue());
  EXPECT_EQ(val[5].value(), "95");
  EXPECT_TRUE(val[6].hasValue());
  EXPECT_EQ(val[6].value(), "158");
  EXPECT_TRUE(val[7].hasValue());
  EXPECT_EQ(val[7].value(), "http://abs.twimg.com/images/themes/theme1/bg.png");
  EXPECT_TRUE(val[8].hasValue());
  EXPECT_EQ(val[8].value(), "true");
  EXPECT_TRUE(val[9].hasValue());
  EXPECT_EQ(val[9].value(), "false");
  EXPECT_TRUE(val[10].hasValue());
  EXPECT_EQ(
      std::string(val[10].value()),
      "{\"url\":{\"urls\":[{\"url\":\"http://t.co/Yg9e1Fl8wd\",\"expanded_url\":\"http://twpf.jp/yuttari1998\",\"display_url\":\"twpf.jp/yuttari1998\",\"indices\":[0,22]}]},\"description\":{\"urls\":[]}}");

  for (size_t i = 0; i < paths.size(); i++) {
    auto tmp = jsonExtractScalar(json, paths[i], false, false, true, false);
    EXPECT_EQ(val[i].has_value(), tmp.has_value());
    if (val[i].has_value()) {
      EXPECT_EQ(std::string(val[i].value()), std::string(tmp.value()));
    }
  }
}
TEST(JsonExtractTupleTest, malformed) {
  std::string json = "{\"a\":1,\"b\":2, c}";
  std::vector<std::string> paths{"a", "b", "c", "d"};
  std::vector<folly::Optional<folly::StringPiece>> stringPieces;

  for (const auto& path : paths) {
    stringPieces.emplace_back(path);
  }
  auto val = jsonExtractTuple(json, stringPieces);
  for (size_t i = 0; i < paths.size(); i++) {
    auto tmp = jsonExtractScalar(json, paths[i], false, false, true, false);
    EXPECT_EQ(val[i].has_value(), tmp.has_value());
    if (val[i].has_value()) {
      EXPECT_EQ(std::string(val[i].value()), std::string(tmp.value()));
    }
  }
}

TEST(JsonExtractTupleTest, invalidValue) {
  std::string json = "{\"a\":1,\"b\":2c}";
  std::vector<std::string> paths{"a", "b"};
  std::vector<folly::Optional<folly::StringPiece>> stringPieces;

  for (const auto& path : paths) {
    stringPieces.emplace_back(path);
  }
  auto val = jsonExtractTuple(json, stringPieces);
  EXPECT_EQ(val.size(), paths.size());
  EXPECT_TRUE(val[0].hasValue());
  EXPECT_EQ(val[0].value(), "1");
  EXPECT_FALSE(val[1].hasValue());
}

BENCHMARK(extractTupleScala) {
  std::string json = R"json(
    {
      "id": 903487807,
      "id_str": "903487807",
      "name": "RT&ファボ魔のむっつんさっm",
      "screen_name": "yuttari1998",
      "location": "関西    ↓詳しいプロ↓",
      "description": "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
      "url": "http://t.co/Yg9e1Fl8wd",
      "entities": {
        "url": {
          "urls": [
            {
              "url": "http://t.co/Yg9e1Fl8wd",
              "expanded_url": "http://twpf.jp/yuttari1998",
              "display_url": "twpf.jp/yuttari1998",
              "indices": [
                0,
                22
              ]
            }
          ]
        },
        "description": {
          "urls": []
        }
      },
      "protected": false,
      "followers_count": 95,
      "friends_count": 158,
      "listed_count": 1,
      "created_at": "Thu Oct 25 08:27:13 +0000 2012",
      "favourites_count": 3652,
      "utc_offset": null,
      "time_zone": null,
      "geo_enabled": false,
      "verified": false,
      "statuses_count": 10276,
      "lang": "ja",
      "contributors_enabled": false,
      "is_translator": false,
      "is_translation_enabled": false,
      "profile_background_color": "C0DEED",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_tile": false,
      "profile_image_url": "http://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_image_url_https": "https://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/903487807/1409062272",
      "profile_link_color": "0084B4",
      "profile_sidebar_border_color": "C0DEED",
      "profile_sidebar_fill_color": "DDEEF6",
      "profile_text_color": "333333",
      "profile_use_background_image": true,
      "default_profile": true,
      "default_profile_image": false,
      "following": false,
      "follow_request_sent": false,
      "notifications": false
    }
  )json";

  const std::vector<std::string> paths = {
      "id",
      "id_str",
      "name",
      "screen_name",
      "location",
      "description",
      "url",
      "entities",
      "protected",
      "followers_count",
      "friends_count",
      "listed_count",
      "created_at",
      "favourites_count",
      "utc_offset",
      "time_zone",
      "geo_enabled",
      "verified",
      "statuses_count",
      "lang",
      "contributors_enabled",
      "is_translator",
      "is_translation_enabled",
      "profile_background_color",
      "profile_background_image_url",
      "profile_background_image_url_https",
      "profile_background_tile",
      "profile_image_url",
      "profile_image_url_https",
      "profile_banner_url",
      "profile_link_color",
      "profile_sidebar_border_color",
      "profile_sidebar_fill_color",
      "profile_text_color",
      "profile_use_background_image",
      "default_profile",
      "default_profile_image",
      "following",
      "follow_request_sent",
      "notifications"};

  for (const auto& path : paths) {
    auto tmp = jsonExtractScalar(json, path, false, false, true, false);
    doNotOptimizeAway(tmp);
  }
}

BENCHMARK(extractTuple) {
  std::string json = R"json(
    {
      "id": 903487807,
      "id_str": "903487807",
      "name": "RT&ファボ魔のむっつんさっm",
      "screen_name": "yuttari1998",
      "location": "関西    ↓詳しいプロ↓",
      "description": "無言フォローはあまり好みません ゲームと動画が好きですシモ野郎ですがよろしく…最近はMGSとブレイブルー、音ゲーをプレイしてます",
      "url": "http://t.co/Yg9e1Fl8wd",
      "entities": {
        "url": {
          "urls": [
            {
              "url": "http://t.co/Yg9e1Fl8wd",
              "expanded_url": "http://twpf.jp/yuttari1998",
              "display_url": "twpf.jp/yuttari1998",
              "indices": [
                0,
                22
              ]
            }
          ]
        },
        "description": {
          "urls": []
        }
      },
      "protected": false,
      "followers_count": 95,
      "friends_count": 158,
      "listed_count": 1,
      "created_at": "Thu Oct 25 08:27:13 +0000 2012",
      "favourites_count": 3652,
      "utc_offset": null,
      "time_zone": null,
      "geo_enabled": false,
      "verified": false,
      "statuses_count": 10276,
      "lang": "ja",
      "contributors_enabled": false,
      "is_translator": false,
      "is_translation_enabled": false,
      "profile_background_color": "C0DEED",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
      "profile_background_tile": false,
      "profile_image_url": "http://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_image_url_https": "https://pbs.twimg.com/profile_images/500268849275494400/AoXHZ7Ij_normal.jpeg",
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/903487807/1409062272",
      "profile_link_color": "0084B4",
      "profile_sidebar_border_color": "C0DEED",
      "profile_sidebar_fill_color": "DDEEF6",
      "profile_text_color": "333333",
      "profile_use_background_image": true,
      "default_profile": true,
      "default_profile_image": false,
      "following": false,
      "follow_request_sent": false,
      "notifications": false
    }
  )json";

  const std::vector<std::string> paths = {
      "id",
      "id_str",
      "name",
      "screen_name",
      "location",
      "description",
      "url",
      "entities",
      "protected",
      "followers_count",
      "friends_count",
      "listed_count",
      "created_at",
      "favourites_count",
      "utc_offset",
      "time_zone",
      "geo_enabled",
      "verified",
      "statuses_count",
      "lang",
      "contributors_enabled",
      "is_translator",
      "is_translation_enabled",
      "profile_background_color",
      "profile_background_image_url",
      "profile_background_image_url_https",
      "profile_background_tile",
      "profile_image_url",
      "profile_image_url_https",
      "profile_banner_url",
      "profile_link_color",
      "profile_sidebar_border_color",
      "profile_sidebar_fill_color",
      "profile_text_color",
      "profile_use_background_image",
      "default_profile",
      "default_profile_image",
      "following",
      "follow_request_sent",
      "notifications"};

  std::vector<folly::Optional<folly::StringPiece>> stringPieces;
  BENCHMARK_SUSPEND {
    for (const auto& path : paths) {
      stringPieces.emplace_back(path);
    }
  }
  auto tmp = jsonExtractTuple(json, stringPieces);
  EXPECT_EQ(stringPieces.size(), tmp.size());
}

TEST(JsonExtractTupleTest, benchmark) {
  folly::runBenchmarks();
}
