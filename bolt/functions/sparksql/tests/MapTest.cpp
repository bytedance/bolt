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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class MapTest : public SparkFunctionBaseTest {
 protected:
  template <typename K = int64_t, typename V = std::string>
  void testMap(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const VectorPtr& expected) {
    auto result = evaluate<MapVector>(expression, makeRowVector(parameters));
    ::bytedance::bolt::test::assertEqualVectors(expected, result);
  }

  template <typename K = int64_t, typename V = std::string>
  void mapSimple(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      bool expectException = false,
      const VectorPtr& expected = nullptr) {
    if (expectException) {
      ASSERT_THROW(
          evaluate<MapVector>(expression, makeRowVector(parameters)),
          std::exception);
    } else {
      auto result = evaluate<MapVector>(expression, makeRowVector(parameters));
      ::bytedance::bolt::test::assertEqualVectors(result, expected);
    }
  }

  void testMapFails(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const std::string errorMsg) {
    BOLT_ASSERT_USER_THROW(
        evaluate<MapVector>(expression, makeRowVector(parameters)), errorMsg);
  }

  void setMapKeyDedupPolicy(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSparkMapKeyDedupPolicy, value}});
  }
};

static std::string POLICIES[3]{"EXCEPTION", "FIRST_WIN", "LAST_WIN"};

TEST_F(MapTest, Basics) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 = makeNullableFlatVector<int64_t>({4, 5, 6});
      auto mapVector =
          makeMapVector<int64_t, int64_t>({{{1, 4}}, {{2, 5}}, {{3, 6}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<StringView>("a"_sv, 3);
      auto inputVector2 = makeNullableFlatVector<int64_t>({4, 5, 6});
      auto mapVector = makeMapVector<StringView, int64_t>(
          {{{"a"_sv, 4}}, {{"a"_sv, 5}}, {{"a"_sv, 6}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }
  }
}

TEST_F(MapTest, Nulls) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 =
          makeNullableFlatVector<int64_t>({std::nullopt, 5, std::nullopt});
      auto mapVector = makeMapVector<int64_t, int64_t>(
          {{{1, std::nullopt}}, {{2, 5}}, {{3, std::nullopt}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<int64_t>(1, 3);
      auto inputVector2 =
          makeNullableFlatVector<int64_t>({std::nullopt, 5, std::nullopt});
      auto mapVector = makeMapVector<int64_t, int64_t>(
          {{{1, std::nullopt}}, {{1, 5}}, {{1, std::nullopt}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }
  }
}

TEST_F(MapTest, differentTypes) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto mapVector =
          makeMapVector<int64_t, double>({{{1, 4.0}}, {{2, 5.0}}, {{3, 6.0}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<int64_t>(1, 3);
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto mapVector =
          makeMapVector<int64_t, double>({{{1, 4.0}}, {{1, 5.0}}, {{1, 6.0}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }
  }
}

TEST_F(MapTest, boolType) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<bool>({1, 1, 0});
      auto inputVector2 = makeNullableFlatVector<bool>({0, 0, 1});
      auto mapVector =
          makeMapVector<bool, bool>({{{1, 0}}, {{1, 0}}, {{0, 1}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<bool>(1, 3);
      auto inputVector2 = makeNullableFlatVector<bool>({0, 0, 1});
      auto mapVector =
          makeMapVector<bool, bool>({{{1, 0}}, {{1, 0}}, {{1, 1}}});
      testMap("map(c0, c1)", {inputVector1, inputVector2}, mapVector);
    }
  }
}

TEST_F(MapTest, wide) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto inputVector11 = makeNullableFlatVector<int64_t>({10, 20, 30});
      auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
      auto mapVector = makeMapVector<int64_t, double>(
          {{{1, 4.0}, {10, 4.1}},
           {{2, 5.0}, {20, 5.1}},
           {{3, 6.0}, {30, 6.1}}});
      testMap(
          "map(c0, c1, c2, c3)",
          {inputVector1, inputVector2, inputVector11, inputVector22},
          mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<int64_t>(1, 3);
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto inputVector11 = makeConstant<int64_t>(2, 3);
      auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
      auto mapVector = makeMapVector<int64_t, double>(
          {{{1, 4.0}, {2, 4.1}}, {{1, 5.0}, {2, 5.1}}, {{1, 6.0}, {2, 6.1}}});
      testMap(
          "map(c0, c1, c2, c3)",
          {inputVector1, inputVector2, inputVector11, inputVector22},
          mapVector);
    }
  }
}

TEST_F(MapTest, MultipleColumnInput) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    {
      // vector key
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto inputVector11 = makeNullableFlatVector<int64_t>({10, 20, 30});
      auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
      auto inputVector111 = makeNullableFlatVector<int64_t>({100, 200, 300});
      auto inputVector222 = makeNullableFlatVector<double>({4.2, 5.2, 6.2});
      auto inputVector1111 =
          makeNullableFlatVector<int64_t>({1000, 2000, 3000});
      auto inputVector2222 = makeNullableFlatVector<double>({4.3, 5.3, 6.3});
      auto mapVector = makeMapVector<int64_t, double>(
          {{{1, 4.0}, {10, 4.1}, {100, 4.2}, {1000, 4.3}},
           {{2, 5.0}, {20, 5.1}, {200, 5.2}, {2000, 5.3}},
           {{3, 6.0}, {30, 6.1}, {300, 6.2}, {3000, 6.3}}});
      mapSimple(
          "map(c0, c1, c2, c3, c4, c5, c6, c7)",
          {inputVector1,
           inputVector2,
           inputVector11,
           inputVector22,
           inputVector111,
           inputVector222,
           inputVector1111,
           inputVector2222},
          false,
          mapVector);
    }

    {
      // constant key
      auto inputVector1 = makeConstant<int64_t>(1, 3);
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto inputVector11 = makeConstant<int64_t>(2, 3);
      auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
      auto inputVector111 = makeConstant<int64_t>(3, 3);
      auto inputVector222 = makeNullableFlatVector<double>({4.2, 5.2, 6.2});
      auto inputVector1111 = makeConstant<int64_t>(4, 3);
      auto inputVector2222 = makeNullableFlatVector<double>({4.3, 5.3, 6.3});
      auto mapVector = makeMapVector<int64_t, double>(
          {{{1, 4.0}, {2, 4.1}, {3, 4.2}, {4, 4.3}},
           {{1, 5.0}, {2, 5.1}, {3, 5.2}, {4, 5.3}},
           {{1, 6.0}, {2, 6.1}, {3, 6.2}, {4, 6.3}}});
      mapSimple(
          "map(c0, c1, c2, c3, c4, c5, c6, c7)",
          {inputVector1,
           inputVector2,
           inputVector11,
           inputVector22,
           inputVector111,
           inputVector222,
           inputVector1111,
           inputVector2222},
          false,
          mapVector);
    }
  }
}

TEST_F(MapTest, errorCases) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    auto inputVectorInt64 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVectorDouble = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto nullInputVector =
        makeNullableFlatVector<int64_t>({1, std::nullopt, 3});

    // Number of args
    testMapFails(
        "map(c0)",
        {inputVectorInt64},
        "Scalar function signature is not supported: map(BIGINT). Supported signatures: (K,V,any...) -> map(K,V).");
    testMapFails(
        "map(c0, c1, c2)",
        {inputVectorInt64, inputVectorDouble, inputVectorInt64},
        "Map function must take an even number of arguments");

    // Types of args
    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVectorInt64,
         inputVectorDouble,
         inputVectorDouble,
         inputVectorDouble},
        "All the key arguments in Map function must be the same!");

    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVectorDouble,
         inputVectorInt64,
         inputVectorDouble,
         inputVectorDouble},
        "All the value arguments in Map function must be the same!");

    testMapFails(
        "map(c0, c1)",
        {nullInputVector, inputVectorDouble},
        "Cannot use null as map key");

    {
      auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
      auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
      auto inputVector11 =
          makeNullableFlatVector<int64_t>({10, 20, std::nullopt});
      auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
      testMapFails(
          "map(c0, c1, c2, c3)",
          {inputVector1, inputVector2, inputVector11, inputVector22},
          "Cannot use null as map key!");
    }
  }
}

TEST_F(MapTest, String) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    auto inputVector1 = makeNullableFlatVector<StringView>({
        "looooooooooooooooooong_key1"_sv,
        "null_key"_sv,
        "short_key2"_sv,
    });

    auto inputVector2 = makeNullableFlatVector<StringView>(
        {"value1_looooooooooooooooooong"_sv, std::nullopt, "value2"_sv});

    auto inputVector3 = makeNullableFlatVector<StringView>({
        "looooooooooooooooooong_key3"_sv,
        "null_key3"_sv,
        "short_key3"_sv,
    });

    auto inputVector4 = makeNullableFlatVector<StringView>(
        {"value4_looooooooooooooooooong"_sv, std::nullopt, "value4"_sv});

    auto expected = makeMapVector<StringView, StringView>({
        {{"looooooooooooooooooong_key1"_sv, "value1_looooooooooooooooooong"_sv},
         {"looooooooooooooooooong_key3"_sv,
          "value4_looooooooooooooooooong"_sv}},
        {{"null_key"_sv, std::nullopt}, {"null_key3"_sv, std::nullopt}},
        {{"short_key2"_sv, "value2"_sv}, {"short_key3"_sv, "value4"_sv}},
    });
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector3, inputVector4},
        expected);

    // constant key
    auto inputConstant1 = makeConstant<StringView>("abc"_sv, 3);
    auto inputConstant2 = makeConstant<StringView>("abc1234567890abcdef"_sv, 3);
    auto expected2 = makeMapVector<StringView, StringView>({
        {{"abc"_sv, "value1_looooooooooooooooooong"_sv},
         {"abc1234567890abcdef"_sv, "value4_looooooooooooooooooong"_sv}},
        {{"abc"_sv, std::nullopt}, {"abc1234567890abcdef"_sv, std::nullopt}},
        {{"abc"_sv, "value2"_sv}, {"abc1234567890abcdef"_sv, "value4"_sv}},
    });

    testMap(
        "map(c0, c1, c2, c3)",
        {inputConstant1, inputVector2, inputConstant2, inputVector4},
        expected2);
  }
}

TEST_F(MapTest, complexTypes) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    auto makeSingleMapVector = [&](const VectorPtr& keyVector,
                                   const VectorPtr& valueVector) {
      return makeMapVector(
          {
              0,
          },
          keyVector,
          valueVector);
    };

    auto makeSingleRowVector = [&](vector_size_t size = 1,
                                   vector_size_t base = 0) {
      return makeRowVector({
          makeFlatVector<int64_t>(size, [&](auto row) { return row + base; }),
      });
    };

    auto testSingleMap = [&](const VectorPtr& keyVector,
                             const VectorPtr& valueVector) {
      testMap(
          "map(c0, c1)",
          {keyVector, valueVector},
          makeSingleMapVector(keyVector, valueVector));
    };

    auto arrayKey = makeArrayVectorFromJson<int64_t>({"[1, 2, 3]"});
    auto arrayValue = makeArrayVectorFromJson<int64_t>({"[1, 3, 5]"});
    auto nullArrayValue = makeArrayVectorFromJson<int64_t>({"null"});

    testSingleMap(makeSingleRowVector(), makeSingleRowVector(1, 2));

    testSingleMap(arrayKey, arrayValue);

    testSingleMap(
        makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector(1, 3)),
        makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector(1, 2)));

    testSingleMap(
        makeSingleMapVector(
            makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector()),
            makeSingleRowVector()),
        makeSingleMapVector(
            arrayKey,
            makeSingleMapVector(makeSingleRowVector(), makeSingleRowVector())));

    testSingleMap(arrayKey, nullArrayValue);

    auto mixedArrayKey1 = makeArrayVector<int64_t>({{1, 2, 3}});
    auto mixedRowValue1 = makeSingleRowVector();
    auto mixedArrayKey2 = makeArrayVector<int64_t>({{4, 5}});
    auto mixedRowValue2 = makeSingleRowVector(1, 1);
    auto mixedMapResult = makeSingleMapVector(
        makeArrayVector<int64_t>({{1, 2, 3}, {4, 5}}),
        makeSingleRowVector(2, 0));
    testMap(
        "map(c0, c1, c2, c3)",
        {mixedArrayKey1, mixedRowValue1, mixedArrayKey2, mixedRowValue2},
        mixedMapResult);

    auto arrayMapResult1 = makeMapVector(
        {
            0,
            1,
        },
        makeArrayVector<int64_t>({{1, 2, 3}, {7, 9}}),
        makeArrayVector<int64_t>({{1, 2}, {4, 6}}));
    testMap(
        "map(c0, c1)",
        {makeArrayVector<int64_t>({{1, 2, 3}, {7, 9}}),
         makeArrayVector<int64_t>({{1, 2}, {4, 6}})},
        arrayMapResult1);
  }
}

TEST_F(MapTest, resultSize) {
  for (const auto& policy : POLICIES) {
    setMapKeyDedupPolicy(policy);
    auto condition = makeFlatVector<int64_t>({1, 2, 3});
    auto keys = makeFlatVector<int64_t>({3, 2, 1});
    auto values = makeFlatVector<int64_t>({4, 5, 6});
    auto mapVector =
        makeMapVector<int64_t, int64_t>({{{4, 3}}, {{5, 2}}, {{1, 6}}});
    testMap(
        "if(greaterthan(c2, 2), map(c0, c1), map(c1, c0))",
        {keys, values, condition},
        mapVector);
  }
}

TEST_F(MapTest, mapDupKeyWithExceptionPolicy) {
  setMapKeyDedupPolicy("EXCEPTION");
  const std::string errorMsg =
      "Duplicate map key was found, please check the input data.";
  {
    // test bool key
    auto inputVector1 = makeNullableFlatVector<bool>({true, false, true});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<bool>({false, true, true});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        errorMsg);
  }

  {
    // test int key
    auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<int64_t>({10, 2, 30});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        errorMsg);
  }

  {
    // test double key
    auto inputVector1 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeNullableFlatVector<double>({4.1, 5.1, 6.0});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 2, 30});
    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        errorMsg);
  }

  {
    // test string key
    auto inputVector1 =
        makeNullableFlatVector<StringView>({"4.0"_sv, "5.0"_sv, "6.0"_sv});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 =
        makeNullableFlatVector<StringView>({"4.1"_sv, "5.0"_sv, "6.1"_sv});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    testMapFails(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        errorMsg);
  }

  {
    // test constant key
    auto inputVector1 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeConstant<StringView>("b"_sv, 3);
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    auto inputVector111 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector222 = makeNullableFlatVector<int64_t>({100, 200, 300});
    testMapFails(
        "map(c0, c1, c2, c3, c4, c5)",
        {inputVector1,
         inputVector2,
         inputVector11,
         inputVector22,
         inputVector111,
         inputVector222},
        errorMsg);
  }
}

TEST_F(MapTest, mapDupKeyWithFirstWinPolicy) {
  setMapKeyDedupPolicy("FIRST_WIN");
  {
    // test bool key
    auto inputVector1 = makeNullableFlatVector<bool>({true, false, true});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<bool>({false, true, true});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    auto mapVector = makeMapVector<bool, double>(
        {{{true, 4.0}, {false, 4.1}},
         {{false, 5.0}, {true, 5.1}},
         {{true, 6.0}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test int key
    auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<int64_t>({10, 2, 30});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    auto mapVector = makeMapVector<int64_t, double>(
        {{{1, 4.0}, {10, 4.1}}, {{2, 5.0}}, {{3, 6.0}, {30, 6.1}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test double key
    auto inputVector1 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeNullableFlatVector<double>({4.1, 5.1, 6.0});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 2, 30});
    auto mapVector = makeMapVector<double, int64_t>(
        {{{4.0, 1}, {4.1, 10}}, {{5.0, 2}, {5.1, 2}}, {{6.0, 3}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test string key
    auto inputVector1 =
        makeNullableFlatVector<StringView>({"4.0"_sv, "5.0"_sv, "6.0"_sv});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 =
        makeNullableFlatVector<StringView>({"4.1"_sv, "5.0"_sv, "6.1"_sv});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    auto mapVector = makeMapVector<StringView, int64_t>(
        {{{"4.0"_sv, 1}, {"4.1"_sv, 10}},
         {{"5.0"_sv, 2}},
         {{"6.0"_sv, 3}, {"6.1"_sv, 30}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test constant key
    auto inputVector1 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeConstant<StringView>("b"_sv, 3);
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    auto inputVector111 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector222 = makeNullableFlatVector<int64_t>({100, 200, 300});
    auto mapVector = makeMapVector<StringView, int64_t>(
        {{{"a"_sv, 1}, {"b"_sv, 10}},
         {{"a"_sv, 2}, {"b"_sv, 20}},
         {{"a"_sv, 3}, {"b"_sv, 30}}});
    testMap(
        "map(c0, c1, c2, c3, c4, c5)",
        {inputVector1,
         inputVector2,
         inputVector11,
         inputVector22,
         inputVector111,
         inputVector222},
        mapVector);
  }
}

TEST_F(MapTest, mapDupKeyWithLastWinPolicy) {
  setMapKeyDedupPolicy("LAST_WIN");
  {
    // test bool key
    auto inputVector1 = makeNullableFlatVector<bool>({true, false, true});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<bool>({false, true, true});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    auto mapVector = makeMapVector<bool, double>(
        {{{true, 4.0}, {false, 4.1}},
         {{false, 5.0}, {true, 5.1}},
         {{true, 6.1}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test int key
    auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector11 = makeNullableFlatVector<int64_t>({10, 2, 30});
    auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
    auto mapVector = makeMapVector<int64_t, double>(
        {{{1, 4.0}, {10, 4.1}}, {{2, 5.1}}, {{3, 6.0}, {30, 6.1}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test double key
    auto inputVector1 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeNullableFlatVector<double>({4.1, 5.1, 6.0});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 2, 30});
    auto mapVector = makeMapVector<double, int64_t>(
        {{{4.0, 1}, {4.1, 10}}, {{5.0, 2}, {5.1, 2}}, {{6.0, 30}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test string key
    auto inputVector1 =
        makeNullableFlatVector<StringView>({"4.0"_sv, "5.0"_sv, "6.0"_sv});
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 =
        makeNullableFlatVector<StringView>({"4.1"_sv, "5.0"_sv, "6.1"_sv});
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    auto mapVector = makeMapVector<StringView, int64_t>(
        {{{"4.0"_sv, 1}, {"4.1"_sv, 10}},
         {{"5.0"_sv, 20}},
         {{"6.0"_sv, 3}, {"6.1"_sv, 30}}});
    testMap(
        "map(c0, c1, c2, c3)",
        {inputVector1, inputVector2, inputVector11, inputVector22},
        mapVector);
  }

  {
    // test constant key
    auto inputVector1 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector2 = makeNullableFlatVector<int64_t>({1, 2, 3});
    auto inputVector11 = makeConstant<StringView>("b"_sv, 3);
    auto inputVector22 = makeNullableFlatVector<int64_t>({10, 20, 30});
    auto inputVector111 = makeConstant<StringView>("a"_sv, 3);
    auto inputVector222 = makeNullableFlatVector<int64_t>({100, 200, 300});
    auto mapVector = makeMapVector<StringView, int64_t>(
        {{{"a"_sv, 100}, {"b"_sv, 10}},
         {{"a"_sv, 200}, {"b"_sv, 20}},
         {{"a"_sv, 300}, {"b"_sv, 30}}});
    testMap(
        "map(c0, c1, c2, c3, c4, c5)",
        {inputVector1,
         inputVector2,
         inputVector11,
         inputVector22,
         inputVector111,
         inputVector222},
        mapVector);
  }
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
