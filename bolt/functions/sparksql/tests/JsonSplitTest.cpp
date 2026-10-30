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

#include <chrono>
#include <iostream>
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using bytedance::bolt::test::emptyArray;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class JsonSplitTest : public SparkFunctionBaseTest {
 protected:
  void JsonSplitSimple(
      const VectorPtr& parameter,
      const VectorPtr& expected = nullptr) {
    core::TypedExprPtr jsonField =
        std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c0");
    auto jsonSplitExpr = std::make_shared<const core::CallTypedExpr>(
        ARRAY(VARCHAR()),
        std::vector<core::TypedExprPtr>{jsonField},
        "json_split");

    bytedance::bolt::VectorPtr result =
        evaluate(jsonSplitExpr, makeRowVector({"c0"}, {parameter}));
    if (expected) {
      ::bytedance::bolt::test::assertEqualVectors(expected, result);
    }
  }

  void JsonSplitWithType(
      const VectorPtr& parameter,
      const TypePtr& elementType,
      const VectorPtr& expected = nullptr) {
    core::TypedExprPtr jsonField =
        std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c0");
    core::TypedExprPtr typeField =
        std::make_shared<const core::ConstantTypedExpr>(
            VARCHAR(), variant(elementType->toString()));
    auto jsonSplitExpr = std::make_shared<const core::CallTypedExpr>(
        ARRAY(elementType),
        std::vector<core::TypedExprPtr>{jsonField, typeField},
        "json_split");

    auto result = evaluate(jsonSplitExpr, makeRowVector({"c0"}, {parameter}));
    if (expected) {
      ::bytedance::bolt::test::assertEqualVectors(expected, result);
    }
  }

  template <typename IntType>
  void testIntType() {
    {
      auto values = makeNullableFlatVector<std::string>(
          {"[]", "[1]", "[1,2,3]", "[4,5,6]"});
      auto result = makeArrayVector<IntType>({
          {},
          {1},
          {1, 2, 3},
          {4, 5, 6},
      });
      JsonSplitWithType(values, CppToType<IntType>::create(), result);
    }
    {
      // invalid integer
      auto values = makeNullableFlatVector<std::string>(
          {"[]", "[\"a\"]", "[1,2,3]", "[4,5,6]"});
      auto result = makeArrayVector<IntType>({
          {},
          {0},
          {1, 2, 3},
          {4, 5, 6},
      });
      JsonSplitWithType(values, CppToType<IntType>::create(), result);
    }
  }

  template <typename FloatType>
  void testFloatType() {
    {
      auto values = makeNullableFlatVector<std::string>(
          {"[]", "[1.1]", "[1.1,2.2,3.3]", "[4.4,5.5,6.6]"});
      auto result = makeArrayVector<FloatType>({
          {},
          {1.1},
          {1.1, 2.2, 3.3},
          {4.4, 5.5, 6.6},
      });
      JsonSplitWithType(values, CppToType<FloatType>::create(), result);
    }
    {
      // invalid float
      auto values = makeNullableFlatVector<std::string>(
          {"[]", "[\"a\"]", "[1.1,2.2,3.3]", "[4.4,5.5,6.6]"});
      auto result = makeArrayVector<FloatType>({
          {},
          {0},
          {1.1, 2.2, 3.3},
          {4.4, 5.5, 6.6},
      });
      JsonSplitWithType(values, CppToType<FloatType>::create(), result);
    }
  }
};

TEST_F(JsonSplitTest, testString) {
  {
    auto values = makeNullableFlatVector<std::string>(
        {"[]", R"(["1"])", R"(["a"])", R"(["a1", "a2", "a3"])"});
    auto result = makeArrayVector<std::string>({
        {},
        {"1"},
        {"a"},
        {"a1", "a2", "a3"},
    });
    JsonSplitSimple(values, result);
  }
  {
    auto values = makeNullableFlatVector<std::string>(
        {"[]",
         R"([{"a":1,"b":2}])",
         R"([[1,2,3], ["a", "b", "c"]])",
         R"([null, 1.1, 2])"});
    auto result = makeNullableArrayVector<std::string>({
        {},
        {R"({"a":1,"b":2})"},
        {"[1,2,3]", R"(["a","b","c"])"},
        {std::nullopt, "1.1", "2"},
    });
    JsonSplitSimple(values, result);
  }
}

TEST_F(JsonSplitTest, testInteger) {
  testIntType<int64_t>();
  testIntType<int32_t>();
  testIntType<int16_t>();
  testIntType<int8_t>();
}

TEST_F(JsonSplitTest, testFloat) {
  testFloatType<float>();
  testFloatType<double>();
}

TEST_F(JsonSplitTest, testTimeStamp) {
  auto values = makeNullableFlatVector<std::string>(
      {"[]",
       R"(["2014-06-01T03:02:13+08"])",
       R"(["2014-06-01T03:02:13+00", "2014-06-01T03:02:13+08", "2014-06-01T03:02:13-08"])"});
  auto result = makeArrayVector<Timestamp>({
      {},
      {Timestamp(1401562933, 0)},
      {Timestamp(1401591733, 0),
       Timestamp(1401562933, 0),
       Timestamp(1401620533, 0)},
  });
  JsonSplitWithType(values, TIMESTAMP(), result);
}

TEST_F(JsonSplitTest, testArray) {
  auto values = makeNullableFlatVector<std::string>(
      {"[[1,2,3], [4,5]]", "[[6,7,8], [9, 10]]", "[[]]"});
  using innerArrayType = std::vector<std::optional<int64_t>>;
  using outerArrayType =
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>;

  innerArrayType a{1, 2, 3};
  innerArrayType b{4, 5};
  innerArrayType c{6, 7, 8};
  innerArrayType d{9, 10};
  outerArrayType row1{{a}, {b}};
  outerArrayType row2{{c}, {d}};
  outerArrayType row3{emptyArray};
  ArrayVectorPtr result =
      makeNullableNestedArrayVector<int64_t>({{row1}, {row2}, {row3}});
  JsonSplitWithType(values, ARRAY(BIGINT()), result);
}

TEST_F(JsonSplitTest, testMap) {
  auto values = makeNullableFlatVector<std::string>({
      R"([{"key1":"red", "key2":"blue", "key3":"green"}, {"key1":"yellow", "key2":"orange"}])",
      R"([{"key1":"yellow", "key2":"orange"}, {"key1":"red", "key2":"yellow", "key2": "purple"}])",
      R"([{"key1":"red", "key2":"yellow", "key2": "purple"}, {"key1":"red", "key2":"blue", "key3":"green"}])",
  });
  using S = StringView;
  using P = std::pair<S, std::optional<S>>;
  std::vector<P> a{
      P{"key1", S{"red"}}, P{"key2", S{"blue"}}, P{"key3", S{"green"}}};
  std::vector<P> b{P{"key1", S{"yellow"}}, P{"key2", S{"orange"}}};
  std::vector<P> c{
      P{"key1", S{"red"}}, P{"key2", S{"yellow"}}, P{"key2", S{"purple"}}};
  std::vector<std::vector<std::vector<P>>> data = {{a, b}, {b, c}, {c, a}};
  auto result = makeArrayOfMapVector<S, S>(data);
  JsonSplitWithType(values, MAP(VARCHAR(), VARCHAR()), result);
}

TEST_F(JsonSplitTest, testRow) {
  auto values = makeNullableFlatVector<std::string>({
      R"([{"a":1, "b": "red"}, {"a":1, "b":"blue"}])",
      "[]",
      R"([{"a":3, "b": "green"}])",
  });
  auto rowType = ROW({{"a", BIGINT()}, {"b", VARCHAR()}});
  std::vector<std::vector<std::optional<std::tuple<int64_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{1, "blue"}}},
          {},
          {{{3, "green"}}},
      };
  auto result = makeArrayOfRowVector(data, rowType);
  JsonSplitWithType(values, rowType, result);
}

TEST_F(JsonSplitTest, testObjectAsArray) {
  {
    auto values = makeNullableFlatVector<std::string>(
        {"123",
         R"("123")",
         R"({"key":"value"})",
         R"({"key1":"value1","key2":"value2"})"});
    auto result = makeArrayVector<std::string>({
        {},
        {},
        {"value"},
        {"value1", "value2"},
    });
    JsonSplitSimple(values, result);
  }
  {
    auto values = makeNullableFlatVector<std::string>(
        {R"([{"k1":1,"k2":2,"k3":3}, {"k4":4,"k5":5}])",
         R"([{"k1":6,"k2":7,"k3":8}, {"k4":9,"k5":10}])",
         "[1, 2]",
         "[[]]"});
    using innerArrayType = std::vector<std::optional<int64_t>>;
    using outerArrayType =
        std::vector<std::optional<std::vector<std::optional<int64_t>>>>;

    innerArrayType a{1, 2, 3};
    innerArrayType b{4, 5};
    innerArrayType c{6, 7, 8};
    innerArrayType d{9, 10};
    outerArrayType row1{{a}, {b}};
    outerArrayType row2{{c}, {d}};
    outerArrayType row3{emptyArray, emptyArray};
    outerArrayType row4{emptyArray};
    ArrayVectorPtr result = makeNullableNestedArrayVector<int64_t>(
        {{row1}, {row2}, {row3}, {row4}});
    JsonSplitWithType(values, ARRAY(BIGINT()), result);
  }
}

TEST_F(JsonSplitTest, testNull) {
  {
    auto values = makeNullableFlatVector<std::string>(
        {"null",
         R"([null, null])",
         R"(["1", null, "2"])",
         R"(["1", "2", "null", null])"});
    auto result = makeNullableArrayVector<std::string>({
        {},
        {std::nullopt, std::nullopt},
        {"1", std::nullopt, "2"},
        {"1", "2", "null", std::nullopt},
    });
    result->setNull(0, true);
    JsonSplitSimple(values, result);
  }
}

#define PERF_BATCH_NUMBER 1 // 1024

template <typename F>
void loop(int count, F f) {
  double total = 0;
  for (int i = 0; i < count; i++) {
    auto t0 = std::chrono::high_resolution_clock::now();
    f();
    auto t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> fs = t1 - t0;
    auto d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
    std::cout << d.count() << "ms" << std::endl;
    total += d.count();
  }
  std::cout << "avg: " << total / count << "ms" << std::endl;
}

TEST_F(JsonSplitTest, testPerfString) {
  std::vector<std::optional<std::string>> sample = {
      R"(["1", "hello", "world"])",
      R"(["a", "b", "c", "d", "e"])",
      R"(["a1", "a2", "a3", "a4"])"};
  std::vector<std::optional<std::string>> values;
  int batchRowCount = 8192, batchNumber = PERF_BATCH_NUMBER;
  for (int i = 0; i < batchRowCount; i++) {
    values.emplace_back(sample[i % sample.size()]);
  }
  auto flatVector = makeNullableFlatVector<std::string>(values);
  loop(3, [&]() {
    for (int i = 0; i < batchNumber; i++) {
      JsonSplitSimple(flatVector);
    }
  });
}

TEST_F(JsonSplitTest, testPerfLongString) {
  std::vector<std::optional<std::string>> sample = {
      R"(["9999999999999", "aaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbb"])",
      R"(["aaaaaaaaaaaaa", "bvvvvvvvvvvvvvv", "cssssssssssss", "dssssssssssss", "eaaaaaaaaaaaa"])",
      R"(["aaaaaaaaaaaaaa1", "assssssssssss2", "aaaaaaaaaaaa3", "assssssssssss4"])"};
  std::vector<std::optional<std::string>> values;
  int batchRowCount = 8192, batchNumber = PERF_BATCH_NUMBER;
  for (int i = 0; i < batchRowCount; i++) {
    values.emplace_back(sample[i % sample.size()]);
  }
  auto flatVector = makeNullableFlatVector<std::string>(values);
  loop(3, [&]() {
    for (int i = 0; i < batchNumber; i++) {
      JsonSplitSimple(flatVector);
    }
  });
}

TEST_F(JsonSplitTest, testPerfJsonString) {
  std::vector<std::optional<std::string>> sample = {
      R"([{"key1":"red", "key2":"blue", "key3":"green"}, {"key1":"yellow", "key2":"orange"}, {"key1":"red", "key2":"blue", "key3":"green"}])",
      R"([{"key1":"yellow", "key2":"orange"}, {"key1":"red", "key2":"yellow", "key2": "purple"}])",
      R"([{"key1":"red", "key2":"yellow", "key2": "purple"}, {"key1":"red", "key2":"blue", "key3":"green"}, {"key1":"red", "key2":"blue", "key3":"green"}])"};
  std::vector<std::optional<std::string>> values;
  int batchRowCount = 8192, batchNumber = PERF_BATCH_NUMBER;
  for (int i = 0; i < batchRowCount; i++) {
    values.emplace_back(sample[i % sample.size()]);
  }
  auto flatVector = makeNullableFlatVector<std::string>(values);
  loop(3, [&]() {
    for (int i = 0; i < batchNumber; i++) {
      JsonSplitSimple(flatVector);
    }
  });
}

TEST_F(JsonSplitTest, testPerfBigint) {
  std::vector<std::optional<std::string>> sample = {
      R"([1, 2, 3])",
      R"([1000000, 200000, 300000])",
      R"([99999999, 111111111, 12345678, 88888888, 23333333333])"};
  std::vector<std::optional<std::string>> values;
  int batchRowCount = 8192, batchNumber = PERF_BATCH_NUMBER;
  for (int i = 0; i < batchRowCount; i++) {
    values.emplace_back(sample[i % sample.size()]);
  }
  auto flatVector = makeNullableFlatVector<std::string>(values);
  loop(3, [&]() {
    for (int i = 0; i < batchNumber; i++) {
      JsonSplitWithType(flatVector, BIGINT());
    }
  });
}

TEST_F(JsonSplitTest, testPerfComplex) {
  std::vector<std::optional<std::string>> sample = {
      R"([{"key1":"red", "key2":"blue", "key3":"green"}, {"key1":"yellow", "key2":"orange"}, {"key1":"red", "key2":"blue", "key3":"green"}])",
      R"([{"key1":"yellow", "key2":"orange"}, {"key1":"red", "key2":"yellow", "key2": "purple"}])",
      R"([{"key1":"red", "key2":"yellow", "key2": "purple"}, {"key1":"red", "key2":"blue", "key3":"green"}, {"key1":"red", "key2":"blue", "key3":"green"}])"};
  std::vector<std::optional<std::string>> values;
  int batchRowCount = 8192, batchNumber = PERF_BATCH_NUMBER;
  for (int i = 0; i < batchRowCount; i++) {
    values.emplace_back(sample[i % sample.size()]);
  }
  auto flatVector = makeNullableFlatVector<std::string>(values);
  loop(3, [&]() {
    for (int i = 0; i < batchNumber; i++) {
      JsonSplitWithType(flatVector, MAP(VARCHAR(), VARCHAR()));
    }
  });
}

TEST_F(JsonSplitTest, testMalformedJson) {
  auto values = makeNullableFlatVector<std::string>({
      R"(["before"])",
      R"(["invalid",])",
      R"(["truncated","value")",
      R"(["truncated",{"value": 1)",
      R"(["after1","after2"])",
  });

  using OptionalArray = std::optional<std::vector<std::optional<std::string>>>;
  auto expected =
      makeNullableArrayVector<std::string>(std::vector<OptionalArray>{
          std::vector<std::optional<std::string>>{"before"},
          std::nullopt,
          std::nullopt,
          std::nullopt,
          std::vector<std::optional<std::string>>{"after1", "after2"},
      });

  for (const auto useSonic : {true, false}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kEnableSonicJsonParse,
          useSonic ? "true" : "false"}});
    JsonSplitSimple(values, expected);
  }
}

TEST_F(JsonSplitTest, testElementOffsetRestoredAfterRowFailure) {
  auto values = makeFlatVector<std::string>({
      R"([{"a":0}])",
      R"([{"a":1},{invalid}])",
      R"([{"a":2}])",
  });

  auto rowType = ROW({"a"}, {BIGINT()});
  auto elements = makeRowVector({"a"}, {makeFlatVector<int64_t>({0, 2})});
  std::vector<vector_size_t> offsets{0, 1, 1};
  auto expected = makeArrayVector(offsets, elements, {1});

  for (const auto useSonic : {true, false}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kEnableSonicJsonParse,
          useSonic ? "true" : "false"}});

    JsonSplitWithType(values, rowType, expected);
  }

  auto timestampValues = makeFlatVector<std::string>({
      R"(["2014-06-01T03:02:13+00"])",
      R"(["2014-06-01T03:02:13+08","invalid"])",
      R"(["2014-06-01T03:02:13-08"])",
  });
  auto timestampElements = makeFlatVector<Timestamp>(
      {Timestamp(1401591733, 0), Timestamp(1401620533, 0)});
  auto timestampExpected = makeArrayVector(offsets, timestampElements, {1});
  for (const auto useSonic : {true, false}) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kEnableSonicJsonParse,
          useSonic ? "true" : "false"}});

    JsonSplitWithType(timestampValues, TIMESTAMP(), timestampExpected);
  }
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
