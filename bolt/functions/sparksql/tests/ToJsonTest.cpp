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
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/sparksql/tests/JsonTestUtil.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

using JsonNativeType = StringView;

class ToJsonTest : public SparkFunctionBaseTest {
 protected:
  void testToJson(const VectorPtr& input, const VectorPtr& expected) {
    testToJson(input, std::nullopt, expected);
  }

  void testToJson(
      const VectorPtr& input,
      const std::optional<std::string>& timezone,
      const VectorPtr& expected) {
    auto expr = createToJson(input->type(), timezone);
    testEncodings(expr, {input}, expected);
  }

  void disableJsonIgnoreNullFields() {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSparkJsonIgnoreNullFields, "false"}});
  }

  void setTimezone(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, value},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }
};

TEST_F(ToJsonTest, basicStruct) {
  auto input = makeRowVector(
      {"a", "b"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt}),
       makeNullableFlatVector<double>({1.1, 2.2, std::nullopt, std::nullopt})});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":1,"b":1.1})", R"({"b":2.2})", R"({"a":3})", R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicArray) {
  auto input = makeNullableArrayVector<int64_t>(
      {{{1}}, {{2, std::nullopt, 3}}, {{}}, std::nullopt});
  auto expected = makeNullableFlatVector<std::string>(
      {R"([1])", R"([2,null,3])", R"([])", std::nullopt});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicMap) {
  // MAP(VARCHAR, BIGINT)
  auto input = makeNullableMapVector<std::string, int64_t>(
      {{{{"a", 1}, {"b", 2}, {"c", 3}}},
       std::nullopt,
       {{}},
       {{{"a", 1}, {"b", std::nullopt}}}});
  auto expected = makeNullableFlatVector<std::string>(
      {R"({"a":1,"b":2,"c":3})", std::nullopt, R"({})", R"({"a":1,"b":null})"});
  testToJson(input, expected);

  // MAP(BIGINT, VARCHAR)
  input = makeNullableMapVector<int64_t, std::string>(
      {{{{1, "a"}, {2, "b"}, {3, "c"}}},
       std::nullopt,
       {{}},
       {{{1, "a"}, {2, std::nullopt}}}});
  expected = makeNullableFlatVector<std::string>(
      {R"({"1":"a","2":"b","3":"c"})",
       std::nullopt,
       R"({})",
       R"({"1":"a","2":null})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicBool) {
  disableJsonIgnoreNullFields();
  auto data = makeNullableFlatVector<bool>({true, false, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":true})", R"({"a":false})", R"({"a":null})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicString) {
  disableJsonIgnoreNullFields();
  auto data = makeNullableFlatVector<std::string>(
      {"str1", "str2\\u", std::nullopt, "str\"3\"", std::nullopt, "a\"é你😄"});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":"str1"})",
       R"({"a":"str2\\u"})",
       R"({"a":null})",
       R"({"a":"str\"3\""})",
       R"({"a":null})",
       R"({"a":"a\"é你😄"})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicTinyInt) {
  auto data =
      makeNullableFlatVector<int8_t>({0, 127, 128, -128, -129, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":0})",
       R"({"a":127})",
       R"({"a":-128})",
       R"({"a":-128})",
       R"({"a":127})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicSmallInt) {
  auto data = makeNullableFlatVector<int16_t>({0, 32768, -32769, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":0})", R"({"a":-32768})", R"({"a":32767})", R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicInt) {
  auto data = makeNullableFlatVector<int32_t>(
      {0, 2147483648, -2147483649, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":0})", R"({"a":-2147483648})", R"({"a":2147483647})", R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicBigInt) {
  auto data = makeNullableFlatVector<int64_t>(
      {std::nullopt, 0, 1, INT64_MAX, INT64_MIN});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({})",
       R"({"a":0})",
       R"({"a":1})",
       R"({"a":9223372036854775807})",
       R"({"a":-9223372036854775808})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicFloat) {
  auto data = makeNullableFlatVector<float>(
      {1.0, kNaNFloat, kInfFloat, -kInfFloat, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":1.0})",
       R"({"a":"NaN"})",
       R"({"a":"Infinity"})",
       R"({"a":"-Infinity"})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicDouble) {
  auto data = makeNullableFlatVector<double>(
      {1.0, kNaNDouble, kInfDouble, -kInfDouble, std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":1.0})",
       R"({"a":"NaN"})",
       R"({"a":"Infinity"})",
       R"({"a":"-Infinity"})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicDecimal) {
  auto data = makeNullableFlatVector<int64_t>(
      {12345, 0, -67890, std::nullopt}, DECIMAL(10, 2));
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":123.45})", R"({"a":0.00})", R"({"a":-678.90})", R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, longDecimal) {
  using bytedance::bolt::HugeInt;
  auto data = makeNullableFlatVector<int128_t>(
      {HugeInt::build(0, 0x112210F47DE98115), // 123456789.0123456789
       HugeInt::build(0, 0), // 0.0000000000
       -HugeInt::build(
           0x35,
           0x8A750438F380F524), // -98765432109.8765432100
       HugeInt::build(
           0x4B3B4CA85A86C47A,
           0x098A223FFFFFFFFF), // max decimal(38, 10)
       -HugeInt::build(
           0x4B3B4CA85A86C47A,
           0x098A223FFFFFFFFF), // min decimal(38, 10)
       std::nullopt},
      DECIMAL(38, 10));
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":123456789.0123456789})",
#ifdef SPARK_COMPATIBLE
       R"({"a":0E-10})",
#else
       R"({"a":0.0000000000})",
#endif
       R"({"a":-98765432109.8765432100})",
       R"({"a":9999999999999999999999999999.9999999999})",
       R"({"a":-9999999999999999999999999999.9999999999})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicTimestamp) {
  auto data = makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 0),
       Timestamp(1582934400, 0),
       Timestamp(-2208988800, 0),
       std::nullopt});
  auto input = makeRowVector({"a"}, {data});
  // UTC time zone.
  auto expected = makeFlatVector<std::string>(
      {R"({"a":"1970-01-01T00:00:00.000Z"})",
       R"({"a":"2020-02-29T00:00:00.000Z"})",
       R"({"a":"1900-01-01T00:00:00.000Z"})",
       R"({})"});
  testToJson(input, "UTC", expected);
  setTimezone("America/Los_Angeles");
  expected = makeFlatVector<std::string>(
      {R"({"a":"1969-12-31T16:00:00.000-08:00"})",
       R"({"a":"2020-02-28T16:00:00.000-08:00"})",
       R"({"a":"1899-12-31T16:00:00.000-08:00"})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, basicDate) {
  auto data = makeNullableFlatVector<int32_t>(
      {0, 18321, -25567, 2932896, std::nullopt}, DateType::get());
  auto input = makeRowVector({"a"}, {data});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":"1970-01-01"})",
       R"({"a":"2020-02-29"})",
       R"({"a":"1900-01-01"})",
       R"({"a":"9999-12-31"})",
       R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, nestedComplexType) {
  // ROW(VARCHAR, ARRAY(INTEGER), MAP(VARCHAR, INTEGER),
  //     ROW(VARCHAR, ARRAY(INTEGER)))
  auto data1 = makeNullableFlatVector<std::string>({"str1", "str2", "str3"});
  auto data2 =
      makeNullableArrayVector<int64_t>({{1, 2, 3}, {}, {std::nullopt}});
  auto data3 = makeNullableMapVector<std::string, int64_t>(
      {{{{"key1", 1}, {"key2", 2}, {"key3", 3}}},
       std::nullopt,
       {{{"key4", 1}, {"key5", std::nullopt}}}});
  auto data4 = makeRowVector(
      {"d1", "d2"},
      {makeNullableFlatVector<std::string>(
           {"d1_str1", "d1_str2", std::nullopt}),
       makeNullableArrayVector<int64_t>({{1, 2, 3}, {4, 5}, {std::nullopt}})});
  auto input =
      makeRowVector({"a", "b", "c", "d"}, {data1, data2, data3, data4});
  auto expected = makeFlatVector<std::string>(
      {R"({"a":"str1","b":[1,2,3],"c":{"key1":1,"key2":2,"key3":3},"d":{"d1":"d1_str1","d2":[1,2,3]}})",
       R"({"a":"str2","b":[],"d":{"d1":"d1_str2","d2":[4,5]}})",
       R"({"a":"str3","b":[null],"c":{"key4":1,"key5":null},"d":{"d2":[null]}})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, nestedMap) {
  // MAP(ROW(DATE, ARRAY(VARCHAR), DOUBLE, TIMESTAMP), INTEGER)
  auto values = makeFlatVector<int32_t>({10, 20, 30, 40, 50});
  auto data1 = makeNullableFlatVector<int32_t>(
      {0, 18321, -25567, 2932896, std::nullopt}, DateType::get());
  auto data2 = makeNullableArrayVector<std::string>(
      {{{"a", "b", std::nullopt}},
       {{}},
       {{"d", "e"}},
       {{"f", std::nullopt, "h"}},
       std::nullopt});
  auto data3 = makeNullableFlatVector<double>(
      {1.0, kNaNDouble, kInfDouble, -kInfDouble, std::nullopt});
  auto data4 = makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 0),
       std::nullopt,
       Timestamp(1582934400, 0),
       Timestamp(-2208988800, 0),
       Timestamp(1735713000, 0)});
  auto rows = makeRowVector({"a", "b", "c", "d"}, {data1, data2, data3, data4});
  auto input = makeMapVector({0, 1, 2, 3, 4}, rows, values);
  auto expected = makeFlatVector<std::string>(
      {R"({"[0,[a,b,null],1.0,0]":10})",
       R"({"[18321,[],NaN,null]":20})",
       R"({"[-25567,[d,e],Infinity,1582934400000000]":30})",
       R"({"[2932896,[f,null,h],-Infinity,-2208988800000000]":40})",
       R"({"[null,null,null,1735713000000000]":50})"});
  testToJson(input, expected);
}

// Tests ported from prestosql/tests/ToJsonTest.cpp using testToJson.
TEST_F(ToJsonTest, fromArray){
    {// Array of JSON-like strings.
     std::vector<std::vector<std::optional<std::string>>>
         array{{"red", "blue"}, {std::nullopt, std::nullopt, "purple"}, {}};
auto arrayVector =
    makeNullableArrayVector<std::string>(array, ARRAY(VARCHAR()));
auto expected = makeNullableFlatVector<std::string>(
    {R"(["red","blue"])", R"([null,null,"purple"])", "[]"});
testToJson(arrayVector, expected);
} // namespace
{
  // Array with Unknown elements.
  auto arrayVector = makeArrayWithDictionaryElements<UnknownValue>(
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt},
      2,
      ARRAY(UNKNOWN()));
  auto expected = makeNullableFlatVector<std::string>(
      {"[null,null]", "[null,null]", "[null,null]"});
  testToJson(arrayVector, expected);
}
{
  // Array with dictionary-wrapped elements.
  auto arrayVector =
      makeArrayWithDictionaryElements<int64_t>({1, -2, 3, -4, 5, -6, 7}, 2);
  auto expected = makeNullableFlatVector<std::string>(
      {"[null,-6]", "[5,-4]", "[3,-2]", "[1]"});
  testToJson(arrayVector, expected);
}
{
  // Array with JSON-typed elements and dictionary.
  auto arrayVector = makeArrayWithDictionaryElements<StringView>(
      {"a", "b", "c", "d", "e", "f", "g"}, 2, ARRAY(JSON()));
  auto expected = makeNullableFlatVector<JsonNativeType>(
      {R"([null,"f"])", R"(["e","d"])", R"(["c","b"])", R"(["a"])"}, VARCHAR());
  testToJson(arrayVector, expected);
}
{
  // All-null array rows.
  auto arrayVector = makeAllNullArrayVector(5, BIGINT());
  auto expected = makeNullableFlatVector<std::string>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testToJson(arrayVector, expected);
}

#ifdef SPARK_COMPATIBLE
{
  // Array with short decimals.
  std::vector<std::vector<int64_t>> decimalArray{
      {0, 100}, {123456, 1234567890}, {84059812, 1234567800}};
  auto arrayVector = makeArrayVector<int64_t>(decimalArray, DECIMAL(10, 5));
  auto expected = makeNullableFlatVector<std::string>(
      {R"([0.00000,0.00100])",
       R"([1.23456,12345.67890])",
       R"([840.59812,12345.67800])"});
  testToJson(arrayVector, expected);
}
{
  // Array with long decimals.
  std::vector<std::vector<int128_t>> longDecimalArray{
      {0, 100}, {123456, 123456789112LL}, {84059812, 12345678000}};
  auto longArrayVector =
      makeArrayVector<int128_t>(longDecimalArray, DECIMAL(30, 10));
  auto expected = makeNullableFlatVector<std::string>(
      {R"([0E-10,1.00E-8])",
       R"([0.0000123456,12.3456789112])",
       R"([0.0084059812,1.2345678000])"});
  testToJson(longArrayVector, expected);
}
#endif
} // namespace bytedance::bolt::functions::sparksql::test

TEST_F(ToJsonTest, fromMap) {
  {
    // Map with string keys.
    auto mapVector = makeMapVector<std::string, int64_t>(
        {{{"blue", 1}, {"red", 2}},
         {{"purple", std::nullopt}, {"orange", -2}},
         {}},
        MAP(VARCHAR(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"blue":1,"red":2})", R"({"purple":null,"orange":-2})", "{}"});
    testToJson(mapVector, expected);
  }
  {
    // Map with integer keys.
    auto mapVector = makeMapVector<int16_t, int64_t>(
        {{{3, std::nullopt}, {4, 2}}, {}, {}}, MAP(SMALLINT(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"3":null,"4":2})", "{}", "{}"});
    testToJson(mapVector, expected);
  }
  {
    // Map with floating-point keys.
    auto mapVector = makeMapVector<double, int64_t>(
        {{{4.4, std::nullopt}, {3.3, 2}}, {}, {}}, MAP(DOUBLE(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"4.4":null,"3.3":2})", "{}", "{}"});
    testToJson(mapVector, expected);
  }
  {
    // Map with boolean keys.
    auto mapVector = makeMapVector<bool, int64_t>(
        {{{true, std::nullopt}, {false, 2}}, {}, {}}, MAP(BOOLEAN(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"true":null,"false":2})", "{}", "{}"});
    testToJson(mapVector, expected);
  }

#ifdef SPARK_COMPATIBLE
  {
    // Map with short decimals.
    auto mapVector = makeMapVector<std::string, int64_t>(
        {{{"a", 0}, {"b", 100}},
         {{"c", 123456}, {"d", 1234567890}},
         {{"e", 84059812}, {"f", 1234567800}}},
        MAP(VARCHAR(), DECIMAL(10, 5)));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"a":0.00000,"b":0.00100})",
         R"({"c":1.23456,"d":12345.67890})",
         R"({"e":840.59812,"f":12345.67800})"});
    testToJson(mapVector, expected);
  }
  {
    // Map with long decimals.
    auto mapVector = makeMapVector<std::string, int128_t>(
        {{{"a", 0}, {"b", 100}},
         {{"c", 123456}, {"d", 123456789112LL}},
         {{"e", 84059812}, {"f", 12345678000}}},
        MAP(VARCHAR(), DECIMAL(30, 10)));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"a":0E-10,"b":1.00E-8})",
         R"({"c":0.0000123456,"d":12.3456789112})",
         R"({"e":0.0084059812,"f":1.2345678000})"});
    testToJson(mapVector, expected);
  }
#endif

  {
    // Map with Unknown values.
    std::vector<std::optional<std::string>> keys{
        "a", "b", "c", "d", "e", "f", "g"};
    std::vector<std::optional<UnknownValue>> values{
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt};
    auto mapVector = makeMapWithDictionaryElements<std::string, UnknownValue>(
        keys, values, 2, MAP(VARCHAR(), UNKNOWN()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"g":null,"f":null})",
         R"({"e":null,"d":null})",
         R"({"c":null,"b":null})",
         R"({"a":null})"});
    testToJson(mapVector, expected);
  }
  {
    // Dictionary-wrapped elements.
    std::vector<std::optional<std::string>> keys{
        "a", "b", "c", "d", "e", "f", "g"};
    std::vector<std::optional<double>> values{
        1100.0, 2.2, 3.14, -4.4, std::nullopt, -6e-10, -7.7};
    auto mapVector = makeMapWithDictionaryElements(keys, values, 2);
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"g":null,"f":-6.0E-10})",
         R"({"e":null,"d":-4.4})",
         R"({"c":3.14,"b":2.2})",
         R"({"a":1100.0})"});
    testToJson(mapVector, expected);
  }
  {
    // JSON-typed keys.
    std::vector<std::optional<std::string>> keys{
        "a", "b", "c", "d", "e", "f", "g"};
    std::vector<std::optional<double>> values{
        1100.0, 2.2, 3.14, -4.4, std::nullopt, -6e-10, -7.7};
    auto mapVector =
        makeMapWithDictionaryElements(keys, values, 2, MAP(JSON(), DOUBLE()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"g":null,"f":-6.0E-10})",
         R"({"e":null,"d":-4.4})",
         R"({"c":3.14,"b":2.2})",
         R"({"a":1100.0})"});
    testToJson(mapVector, expected);
  }
  {
    // All-null map rows.
    auto mapVector = makeAllNullMapVector(5, VARCHAR(), BIGINT());
    auto expected = makeNullableFlatVector<std::string>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
    testToJson(mapVector, expected);
  }
}

TEST_F(ToJsonTest, fromRow) {
  {
    // Mixed row.
    auto c0 = makeNullableFlatVector<int64_t>(
        {std::nullopt, 2, 3, std::nullopt, 5, 6, std::nullopt, 8}, BIGINT());
    auto c1 = makeNullableFlatVector<std::string>(
        {"red",
         std::nullopt,
         "blue",
         std::nullopt,
         "yellow",
         std::nullopt,
         "yellow",
         "blue"},
        VARCHAR());
    auto c2 = makeNullableFlatVector<double>(
        {1.1,
         2.2,
         std::nullopt,
         std::nullopt,
         5.5,
         1.0,
         0.0008547008547008547,
         0.0009165902841429881},
        DOUBLE());
    auto input = makeRowVector({c0, c1, c2});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c1":"red","c2":1.1})",
         R"({"c0":2,"c2":2.2})",
         R"({"c0":3,"c1":"blue"})",
         R"({})",
         R"({"c0":5,"c1":"yellow","c2":5.5})",
         R"({"c0":6,"c2":1.0})",
         R"({"c1":"yellow","c2":8.547008547008547E-4})",
         R"({"c0":8,"c1":"blue","c2":9.165902841429881E-4})"});
    testToJson(input, expected);
  }
  {
    // Row with JSON-typed child.
    auto c0 = makeNullableFlatVector<int64_t>(
        {std::nullopt, 2, 3, std::nullopt, 5, 6, std::nullopt, 8}, BIGINT());
    auto c1 = makeNullableFlatVector<StringView>(
        {"red",
         std::nullopt,
         "blue",
         std::nullopt,
         "yellow",
         std::nullopt,
         "yellow",
         "blue"},
        JSON());
    auto c2 = makeNullableFlatVector<double>(
        {1.1,
         2.2,
         std::nullopt,
         std::nullopt,
         5.5,
         1.0,
         0.0008547008547008547,
         0.0009165902841429881},
        DOUBLE());
    auto input = makeRowVector({c0, c1, c2});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c1":"red","c2":1.1})",
         R"({"c0":2,"c2":2.2})",
         R"({"c0":3,"c1":"blue"})",
         R"({})",
         R"({"c0":5,"c1":"yellow","c2":5.5})",
         R"({"c0":6,"c2":1.0})",
         R"({"c1":"yellow","c2":8.547008547008547E-4})",
         R"({"c0":8,"c1":"blue","c2":9.165902841429881E-4})"});
    testToJson(input, expected);
  }

#ifdef SPARK_COMPATIBLE
  {
    // Row with short decimal.
    auto a =
        makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"}, VARCHAR());
    auto b = makeFlatVector<int64_t>(
        {0, 100, 123456, 1234567890, 84059812, 1234567800}, DECIMAL(10, 5));
    auto input = makeRowVector({a, b});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":"a","c1":0.00000})",
         R"({"c0":"b","c1":0.00100})",
         R"({"c0":"c","c1":1.23456})",
         R"({"c0":"d","c1":12345.67890})",
         R"({"c0":"e","c1":840.59812})",
         R"({"c0":"f","c1":12345.67800})"});
    testToJson(input, expected);
  }
  {
    // Row with long decimal.
    auto a =
        makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"}, VARCHAR());
    auto b = makeFlatVector<int128_t>(
        {0, 100, 123456, 123456789112LL, 84059812, 12345678000},
        DECIMAL(30, 10));
    auto input = makeRowVector({a, b});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":"a","c1":0E-10})",
         R"({"c0":"b","c1":1.00E-8})",
         R"({"c0":"c","c1":0.0000123456})",
         R"({"c0":"d","c1":12.3456789112})",
         R"({"c0":"e","c1":0.0084059812})",
         R"({"c0":"f","c1":1.2345678000})"});
    testToJson(input, expected);
  }
#endif

  {
    // Dorado case: nested row with strings and numbers.
    auto c0 = makeNullableFlatVector<int64_t>(
        {7306871280580460812, 7306871280580460812, 7306871280580460812},
        BIGINT());
    auto c1 = makeNullableFlatVector<std::string>(
        {"柯琳妮尔/奶盖水光精华水",
         "柯琳妮尔/奶盖水光精华水",
         "柯琳妮尔/奶盖水光精华水"},
        VARCHAR());
    auto c2 = makeNullableFlatVector<std::string>(
        {"https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image",
         "https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image",
         "https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image"},
        VARCHAR());
    auto c3 = makeNullableFlatVector<int64_t>({33800, 33800, 33800}, BIGINT());
    auto c4 = makeNullableFlatVector<int64_t>({27165, 27165, 27165}, BIGINT());
    auto c5 = makeNullableArrayVector<int64_t>(
        {std::nullopt, std::nullopt, std::nullopt}, ARRAY(BIGINT()));
    auto c6 = makeNullableFlatVector<int64_t>({277, 277, 277}, BIGINT());
    auto c7 = makeNullableFlatVector<std::string>(
        {"20240106", "20240106", "20240106"}, VARCHAR());
    auto input = makeRowVector({c0, c1, c2, c3, c4, c5, c6, c7});
    auto expectedJson =
        R"({"c0":7306871280580460812,"c1":"柯琳妮尔/奶盖水光精华水","c2":"https://p9-ecom-spu.byteimg.com/tos-cn-i-89jsre2ap7/a6abfd28f7bf4375a341a5a629f289df~tplv-89jsre2ap7-image.image","c3":33800,"c4":27165,"c6":277,"c7":"20240106"})";
    auto expected = makeNullableFlatVector<std::string>(
        {expectedJson, expectedJson, expectedJson});
    testToJson(input, expected);
  }
}

TEST_F(ToJsonTest, fromNested) {
  // Map of array vector.
  auto keyVector = makeNullableFlatVector<StringView>(
      {"blue", "red", "green", "yellow", "purple", "orange"}, JSON());
  auto valueVector = makeNullableArrayVector<int64_t>(
      {{1, 2},
       {std::nullopt, 4},
       {std::nullopt, std::nullopt},
       {7, 8},
       {9, std::nullopt},
       {11, 12}});

  auto offsets = allocateOffsets(3, pool());
  auto sizes = allocateSizes(3, pool());
  makeOffsetsAndSizes(6, 2, offsets, sizes);

  auto nulls = makeNulls({false, true, false});

  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(JSON(), ARRAY(BIGINT())),
      nulls,
      3,
      offsets,
      sizes,
      keyVector,
      valueVector);

  // Array of map vector.
  std::vector<std::pair<std::string, std::optional<int64_t>>> a{
      {"blue", 1}, {"red", 2}};
  std::vector<std::pair<std::string, std::optional<int64_t>>> b{
      {"green", std::nullopt}};
  std::vector<std::pair<std::string, std::optional<int64_t>>> c{
      {"yellow", 4}, {"purple", 5}};
  std::vector<
      std::vector<std::vector<std::pair<std::string, std::optional<int64_t>>>>>
      data{{a, b}, {b}, {c, a}};
  auto arrayVector = makeArrayOfMapVector<std::string, int64_t>(data);

  auto input = makeRowVector({mapVector, arrayVector});
  auto expected = makeNullableFlatVector<std::string>(
      {R"({"c0":{"blue":[1,2],"red":[null,4]},"c1":[{"blue":1,"red":2},{"green":null}]})",
       R"({"c1":[{"green":null}]})",
       R"({"c0":{"purple":[9,null],"orange":[11,12]},"c1":[{"yellow":4,"purple":5},{"blue":1,"red":2}]})"});
  testToJson(input, expected);
}

TEST_F(ToJsonTest, fromSparkParity) {
  {
    // Escaping in JSON-typed struct.
    auto child = makeNullableFlatVector<StringView>(
        {"\"quote", "\"quote", "\"quote"}, JSON());
    auto input = makeRowVector({child});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":"\"quote"})", R"({"c0":"\"quote"})", R"({"c0":"\"quote"})"});
    testToJson(input, expected);
  }
  {
    // Struct with BIGINT.
    auto child = makeNullableFlatVector<int64_t>({1, 2, 3}, BIGINT());
    auto input = makeRowVector({child});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":1})", R"({"c0":2})", R"({"c0":3})"});
    testToJson(input, expected);
  }
  {
    // Array of ROW.
    auto rowType = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
    std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
        data{
            {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
            {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
            {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}}};
    auto input = makeArrayOfRowVector(data, rowType);
    auto expected = makeNullableFlatVector<std::string>(
        {R"([{"a":1,"b":"red"},{"a":2,"b":"blue"},{"a":3,"b":"green"}])",
         R"([{"a":1,"b":"red"},{"a":2,"b":"blue"},{"a":3,"b":"green"}])",
         R"([{"a":1,"b":"red"},{"a":2,"b":"blue"},{"a":3,"b":"green"}])"});
    testToJson(input, expected);
  }
  {
    // Array with single empty ROW.
    auto rowType = ROW({"a", "b"}, {INTEGER(), VARCHAR()});
    std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
        data{{{std::nullopt}}, {{std::nullopt}}, {{std::nullopt}}};
    auto input = makeArrayOfRowVector(data, rowType);
    auto expected =
        makeNullableFlatVector<std::string>({R"([{}])", R"([{}])", R"([{}])"});
    testToJson(input, expected);
  }
  {
    // Empty array.
    std::vector<std::vector<std::optional<std::string>>> array{{}, {}, {}};
    auto input = makeNullableArrayVector<std::string>(array, ARRAY(VARCHAR()));
    auto expected = makeNullableFlatVector<std::string>({"[]", "[]", "[]"});
    testToJson(input, expected);
  }
  {
    // Null input column.
    auto child = makeAllNullFlatVector<StringView>(3);
    auto input = makeRowVector({child});
    input->setNull(0, true);
    input->setNull(1, true);
    input->setNull(2, true);
    // Expect null output.
    auto expected = makeNullableFlatVector<std::string>(
        {std::nullopt, std::nullopt, std::nullopt});
    testToJson(input, expected);
  }
  {
    // MAP<string, struct>.
    constexpr vector_size_t size = 3;
    auto values = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    auto keys = makeNullableFlatVector<std::string>({"a", "b", "c"}, VARCHAR());
    std::vector<vector_size_t> offsets{0, 1, 2};
    auto input = makeMapVector(offsets, keys, values);
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"a":{"c0":0,"c1":0}})",
         R"({"b":{"c0":1,"c1":1}})",
         R"({"c":{"c0":2,"c1":2}})"});
    testToJson(input, expected);
  }
  {
    // MAP<struct, struct> unsupported.
    constexpr vector_size_t size = 3;
    auto values = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    auto keys = makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
    });
    std::vector<vector_size_t> offsets{0, 1, 2};
    auto input = makeMapVector(offsets, keys, values);
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"[0,0]":{"c0":0,"c1":0}})",
         R"({"[1,1]":{"c0":1,"c1":1}})",
         R"({"[2,2]":{"c0":2,"c1":2}})"});
    testToJson(input, expected);
  }
  {
    // MAP<string, integer>.
    auto input = makeMapVector<std::string, int32_t>(
        {{{"a", 1}}, {}, {}}, MAP(VARCHAR(), INTEGER()));
    auto expected =
        makeNullableFlatVector<std::string>({R"({"a":1})", R"({})", R"({})"});
    testToJson(input, expected);
  }
  {
    // Array with maps.
    std::vector<std::pair<std::string, std::optional<int64_t>>> a{
        {"blue", 1}, {"red", 2}};
    std::vector<std::vector<
        std::vector<std::pair<std::string, std::optional<int64_t>>>>>
        data{{a}, {a}, {a}};
    auto input = makeArrayOfMapVector<std::string, int64_t>(data);
    auto expected = makeNullableFlatVector<std::string>(
        {R"([{"blue":1,"red":2}])",
         R"([{"blue":1,"red":2}])",
         R"([{"blue":1,"red":2}])"});
    testToJson(input, expected);
  }
  {
    // Array with single map.
    std::vector<std::pair<std::string, std::optional<int64_t>>> a{{"blue", 1}};
    std::vector<std::vector<
        std::vector<std::pair<std::string, std::optional<int64_t>>>>>
        data{{a}, {a}, {a}};
    auto input = makeArrayOfMapVector<std::string, int64_t>(data);
    auto expected = makeNullableFlatVector<std::string>(
        {R"([{"blue":1}])", R"([{"blue":1}])", R"([{"blue":1}])"});
    testToJson(input, expected);
  }
}

TEST_F(ToJsonTest, doesntSortKeyInMapParity) {
  {
    auto input = makeMapVector<std::string, int64_t>(
        {{{"cccc", 1}, {"aaaa", 2}},
         {{"zzzz", std::nullopt}, {"aaaa", -2}},
         {}},
        MAP(VARCHAR(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"cccc":1,"aaaa":2})", R"({"zzzz":null,"aaaa":-2})", "{}"});
    testToJson(input, expected);
  }
  {
    auto input = makeMapVector<int16_t, int64_t>(
        {{{4, std::nullopt}, {3, 2}}, {}, {}}, MAP(SMALLINT(), BIGINT()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"4":null,"3":2})", R"({})", R"({})"});
    testToJson(input, expected);
  }
}

TEST_F(ToJsonTest, NaNParity) {
  constexpr double nan = std::numeric_limits<double>::quiet_NaN();
  {
    auto input = makeMapVector<double, double>(
        {{{1, nan}, {2, 2}}, {{nan, nan}, {4, nan}}, {}},
        MAP(DOUBLE(), DOUBLE()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"1.0":"NaN","2.0":2.0})", R"({"NaN":"NaN","4.0":"NaN"})", "{}"});
    testToJson(input, expected);
  }
  {
    auto child = makeNullableFlatVector<double>({1, nan, 2.0}, DOUBLE());
    auto input = makeRowVector({child});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":1.0})", R"({"c0":"NaN"})", R"({"c0":2.0})"});
    testToJson(input, expected);
  }
  {
    auto rowType = ROW({"a", "b"}, {DOUBLE(), VARCHAR()});
    // Create 3 rows to satisfy testEncodings size >= 3 requirement.
    std::vector<std::vector<std::optional<std::tuple<double, std::string>>>>
        data{
            {{{1, "red"}}, {{nan, "blue"}}, {{nan, "green"}}},
            {{{1, "red"}}, {{nan, "blue"}}, {{nan, "green"}}},
            {{{1, "red"}}, {{nan, "blue"}}, {{nan, "green"}}},
        };
    auto input = makeArrayOfRowVector(data, rowType);
    auto expected = makeNullableFlatVector<std::string>({
        R"([{"a":1.0,"b":"red"},{"a":"NaN","b":"blue"},{"a":"NaN","b":"green"}])",
        R"([{"a":1.0,"b":"red"},{"a":"NaN","b":"blue"},{"a":"NaN","b":"green"}])",
        R"([{"a":1.0,"b":"red"},{"a":"NaN","b":"blue"},{"a":"NaN","b":"green"}])",
    });
    testToJson(input, expected);
  }
}

// Varbinary is not supported
TEST_F(ToJsonTest, varbinaryParity) {
  {
    auto input = makeNullableArrayVector<std::string>(
        {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}},
        ARRAY(VARBINARY()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"(["a2V5MQ==","dmFsdWUx"])",
         R"(["a2V5Mg==","dmFsdWUy"])",
         R"(["a2V5Mw==","dmFsdWUz"])"});
    testToJson(input, expected);
  }
  {
    auto input = makeMapVector<std::string, std::string>(
        {{{"key1", "value1"}}, {{"key2", "value2"}}, {{"key3", "value3"}}, {}},
        MAP(VARCHAR(), VARBINARY()));
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"key1":"dmFsdWUx"})",
         R"({"key2":"dmFsdWUy"})",
         R"({"key3":"dmFsdWUz"})",
         "{}"});
    testToJson(input, expected);
  }
  {
    auto a = makeNullableFlatVector<std::string>(
        {"key1", "key2", "key3"}, VARBINARY());
    auto b = makeNullableFlatVector<std::string>(
        {"value1", "value2", "value3"}, VARBINARY());
    auto input = makeRowVector({a, b});
    auto expected = makeNullableFlatVector<std::string>(
        {R"({"c0":"a2V5MQ==","c1":"dmFsdWUx"})",
         R"({"c0":"a2V5Mg==","c1":"dmFsdWUy"})",
         R"({"c0":"a2V5Mw==","c1":"dmFsdWUz"})"});
    testToJson(input, expected);
  }
}

TEST_F(ToJsonTest, allDataTypeParity) {
  auto c0 =
      makeFlatVector<int8_t>(std::vector<int8_t>{127, 127, 127}, TINYINT());
  auto c1 = makeFlatVector<int16_t>(
      std::vector<int16_t>{32767, 32767, 32767}, SMALLINT());
  auto c2 = makeFlatVector<int32_t>(
      std::vector<int32_t>{2147483647, 2147483647, 2147483647}, INTEGER());
  auto c3 = makeFlatVector<int64_t>(
      std::vector<int64_t>{
          9223372036854775807, 9223372036854775807, 9223372036854775807},
      BIGINT());
  auto c4 =
      makeFlatVector<float>(std::vector<float>{1.23E7, 1.23E7, 1.23E7}, REAL());
  auto c5 = makeFlatVector<double>(
      std::vector<double>{1.23456789, 1.23456789, 1.23456789}, DOUBLE());
  auto c6 = makeFlatVector<int64_t>(
      std::vector<int64_t>{123456, 123456, 123456}, DECIMAL(10, 2));
  auto c7 = makeFlatVector<int128_t>(
      std::vector<int128_t>{922337203685478, 922337203685478, 922337203685478},
      DECIMAL(38, 10));
  auto c8 = makeFlatVector<std::string>(
      std::vector<std::string>{
          "example string", "example string", "example string"},
      VARCHAR());
  auto c9 =
      makeFlatVector<bool>(std::vector<bool>{true, true, true}, BOOLEAN());
  auto c10 = makeArrayVector<int8_t>({{1, 2}, {1, 2}, {1, 2}}, TINYINT());
  auto c11 = makeArrayVector<int16_t>({{3, 4}, {3, 4}, {3, 4}}, SMALLINT());
  auto c12 = makeArrayVector<int32_t>({{5, 6}, {5, 6}, {5, 6}}, INTEGER());
  auto c13 = makeArrayVector<int64_t>({{7, 8}, {7, 8}, {7, 8}}, BIGINT());
  auto c14 =
      makeArrayVector<float>({{1.1, 2.2}, {1.1, 2.2}, {1.1, 2.2}}, REAL());
  auto c15 =
      makeArrayVector<double>({{3.3, 4.4}, {3.3, 4.4}, {3.3, 4.4}}, DOUBLE());
  auto c16 = makeArrayVector<int64_t>(
      {{5555, 6666}, {5555, 6666}, {5555, 6666}}, DECIMAL(10, 2));
  auto c17 = makeArrayVector<int128_t>(
      {{922337203685477, 922337203685478},
       {922337203685477, 922337203685478},
       {922337203685477, 922337203685478}},
      DECIMAL(38, 10));
  auto c18 = makeArrayVector<std::string>(
      {{"string1", "string2"}, {"string1", "string2"}, {"string1", "string2"}},
      VARCHAR());
  auto c19 = makeArrayVector<bool>(
      {{true, false}, {true, false}, {true, false}}, BOOLEAN());
  auto c20 = makeMapVector<std::string, int8_t>(
      {{{"key1", 1}, {"key2", 2}},
       {{"key1", 1}, {"key2", 2}},
       {{"key1", 1}, {"key2", 2}}},
      MAP(VARCHAR(), TINYINT()));
  auto c21 = makeMapVector<std::string, int16_t>(
      {{{"key1", 3}, {"key2", 4}},
       {{"key1", 3}, {"key2", 4}},
       {{"key1", 3}, {"key2", 4}}},
      MAP(VARCHAR(), SMALLINT()));
  auto c22 = makeMapVector<std::string, int32_t>(
      {{{"key1", 5}, {"key2", 6}},
       {{"key1", 5}, {"key2", 6}},
       {{"key1", 5}, {"key2", 6}}},
      MAP(VARCHAR(), INTEGER()));
  auto c23 = makeMapVector<std::string, int64_t>(
      {{{"key1", 9223372036854775806}, {"key2", 9223372036854775807}},
       {{"key1", 9223372036854775806}, {"key2", 9223372036854775807}},
       {{"key1", 9223372036854775806}, {"key2", 9223372036854775807}}},
      MAP(VARCHAR(), BIGINT()));
  auto c24 = makeMapVector<std::string, float>(
      {{{"key1", 1.1}, {"key2", 2.2}},
       {{"key1", 1.1}, {"key2", 2.2}},
       {{"key1", 1.1}, {"key2", 2.2}}},
      MAP(VARCHAR(), REAL()));
  auto c25 = makeMapVector<std::string, double>(
      {{{"key1", 3.3}, {"key2", 4.4}},
       {{"key1", 3.3}, {"key2", 4.4}},
       {{"key1", 3.3}, {"key2", 4.4}}},
      MAP(VARCHAR(), DOUBLE()));
  auto c26 = makeMapVector<std::string, int64_t>(
      {{{"key1", 7778}, {"key2", 8889}},
       {{"key1", 7778}, {"key2", 8889}},
       {{"key1", 7778}, {"key2", 8889}}},
      MAP(VARCHAR(), DECIMAL(10, 2)));
  auto c27 = makeMapVector<std::string, int128_t>(
      {{{"key1", 922337203685477}, {"key2", 922337203685478}},
       {{"key1", 922337203685477}, {"key2", 922337203685478}},
       {{"key1", 922337203685477}, {"key2", 922337203685478}}},
      MAP(VARCHAR(), DECIMAL(38, 10)));
  auto c28 = makeMapVector<std::string, std::string>(
      {{{"key1", "value1"}, {"key2", "value2"}},
       {{"key1", "value1"}, {"key2", "value2"}},
       {{"key1", "value1"}, {"key2", "value2"}}},
      MAP(VARCHAR(), VARCHAR()));
  auto c29 = makeMapVector<std::string, bool>(
      {{{"key1", true}, {"key2", false}},
       {{"key1", true}, {"key2", false}},
       {{"key1", true}, {"key2", false}}},
      MAP(VARCHAR(), BOOLEAN()));
  auto c30 = makeNullableFlatVector<int32_t>(
      {DATE()->toDays("2025-04-08"),
       DATE()->toDays("2025-04-08"),
       DATE()->toDays("2025-04-08")},
      DATE());
  auto c31 = makeFlatVector<std::string>(
      std::vector<std::string>{"abc", "abc", "abc"}, VARBINARY());
  auto c32 = makeArrayVector<std::string>(
      {{"abcd", "abcde"}, {"abcd", "abcde"}, {"abcd", "abcde"}}, VARBINARY());
  auto c33 = makeMapVector<std::string, std::string>(
      {{{"key1", "abcd"}, {"key2", "abcde"}},
       {{"key1", "abcd"}, {"key2", "abcde"}},
       {{"key1", "abcd"}, {"key2", "abcde"}}},
      MAP(VARCHAR(), VARBINARY()));
  auto input =
      makeRowVector({c0,  c1,  c2,  c3,  c4,  c5,  c6,  c7,  c8,  c9,  c10, c11,
                     c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23,
                     c24, c25, c26, c27, c28, c29, c30, c31, c32, c33});
  auto expectedJson =
      R"({"c0":127,"c1":32767,"c2":2147483647,"c3":9223372036854775807,"c4":1.23E7,"c5":1.23456789,"c6":1234.56,"c7":92233.7203685478,"c8":"example string","c9":true,"c10":[1,2],"c11":[3,4],"c12":[5,6],"c13":[7,8],"c14":[1.1,2.2],"c15":[3.3,4.4],"c16":[55.55,66.66],"c17":[92233.7203685477,92233.7203685478],"c18":["string1","string2"],"c19":[true,false],"c20":{"key1":1,"key2":2},"c21":{"key1":3,"key2":4},"c22":{"key1":5,"key2":6},"c23":{"key1":9223372036854775806,"key2":9223372036854775807},"c24":{"key1":1.1,"key2":2.2},"c25":{"key1":3.3,"key2":4.4},"c26":{"key1":77.78,"key2":88.89},"c27":{"key1":92233.7203685477,"key2":92233.7203685478},"c28":{"key1":"value1","key2":"value2"},"c29":{"key1":true,"key2":false},"c30":"2025-04-08","c31":"YWJj","c32":["YWJjZA==","YWJjZGU="],"c33":{"key1":"YWJjZA==","key2":"YWJjZGU="}})";
  auto expected = makeNullableFlatVector<std::string>(
      {expectedJson, expectedJson, expectedJson});
  testToJson(input, expected);
}
TEST_F(ToJsonTest, unsupportedType) {
  VectorFuzzer::Options opts;
  opts.vectorSize = 1000;
  opts.nullRatio = 0.1;

  VectorFuzzer fuzzer(opts, pool_.get());

#ifndef NDEBUG
  // ROW(HUGEINT)
  auto hugeIntInput =
      fuzzer.fuzzDictionary(fuzzer.fuzzFlat(ROW({"a"}, {HUGEINT()})));
  BOLT_ASSERT_THROW(
      testToJson(hugeIntInput, nullptr), "HUGEINT must be a decimal type.");
#endif

  // MAP(MAP)
  auto input = fuzzer.fuzzDictionary(
      fuzzer.fuzzFlat(MAP(MAP(BIGINT(), BIGINT()), INTEGER())));
  BOLT_ASSERT_THROW(
      testToJson(input, nullptr),
      "to_json function does not support type MAP<MAP<BIGINT,BIGINT>,INTEGER>.");

  // MAP(ARRAY(MAP))
  input = fuzzer.fuzzDictionary(
      fuzzer.fuzzFlat(MAP(ARRAY(MAP(VARCHAR(), INTEGER())), INTEGER())));
  BOLT_ASSERT_THROW(
      testToJson(input, nullptr),
      "to_json function does not support type MAP<ARRAY<MAP<VARCHAR,INTEGER>>,INTEGER>.");

  // ROW(MAP(ARRAY(MAP)))
  input = makeRowVector(
      {"a"},
      {fuzzer.fuzzDictionary(
          fuzzer.fuzzFlat(MAP(ARRAY(MAP(BIGINT(), BIGINT())), INTEGER())))});
  BOLT_ASSERT_THROW(
      testToJson(input, nullptr),
      "to_json function does not support type ROW<a:MAP<ARRAY<MAP<BIGINT,BIGINT>>,INTEGER>>.");
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
