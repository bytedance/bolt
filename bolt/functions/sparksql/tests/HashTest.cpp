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

#include <stdint.h>
#include <optional>
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class HashTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  int32_t hash(std::optional<T> arg) {
    return evaluateOnce<int32_t>("hash(c0)", arg).value();
  }
};

template <typename T>
using NestedVector = std::vector<std::vector<T>>;

inline NestedVector<std::vector<std::pair<int32_t, std::optional<int32_t>>>>
mapInput() {
  using M = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  return NestedVector<M>{
      // Empty.
      {},
      // Sort on normalized keys.
      {M{{1, 11}, {3, 10}}, M{{2, 11}, {0, 10}}, M{{1, 11}, {1, 10}}},
      // Sort on values when keys are same.
      {M{{1, 11}, {3, 10}}, M{{1, 13}, {3, 12}}},
      // Null values in map.
      {M{{0, std::nullopt}}, M{{0, 10}}},
  };
}

inline NestedVector<variant> rowInput() {
  variant nullRow = variant(TypeKind::ROW);
  variant nullInt = variant(TypeKind::INTEGER);
  return NestedVector<variant>{
      // Empty.
      {},
      // All nulls.
      {nullRow, nullRow},
      // Null row.
      {variant::row({2, "red"}), nullRow, variant::row({1, "blue"})},
      // Null values in row.
      {variant::row({1, "green"}), variant::row({nullInt, "red"})},
  };
}

TEST_F(HashTest, String) {
  EXPECT_EQ(hash<std::string>("Spark"), 228093765);
  EXPECT_EQ(hash<std::string>(""), 142593372);
  EXPECT_EQ(hash<std::string>("abcdefghijklmnopqrstuvwxyz"), -1990933474);
  // String that has a length that is a multiple of four.
  EXPECT_EQ(hash<std::string>("12345678"), 2036199019);
  EXPECT_EQ(hash<std::string>(std::nullopt), 42);
}

TEST_F(HashTest, longDecimal) {
  EXPECT_EQ(hash<int128_t>(12345678), -277285195);
  EXPECT_EQ(hash<int128_t>(0), -783713497);
  EXPECT_EQ(hash<int128_t>(DecimalUtil::kLongDecimalMin), 1400911110);
  EXPECT_EQ(hash<int128_t>(DecimalUtil::kLongDecimalMax), -817514053);
  EXPECT_EQ(hash<int128_t>(-12345678), -1198355617);
  EXPECT_EQ(hash<int128_t>(std::nullopt), 42);
}

// Spark CLI select timestamp_micros(12345678) to get the Timestamp.
// select hash(Timestamp("1970-01-01 00:00:12.345678")) to get the hash value.
TEST_F(HashTest, Timestamp) {
  EXPECT_EQ(hash<Timestamp>(Timestamp::fromMicros(12345678)), 1402875301);
}

TEST_F(HashTest, Int64) {
  EXPECT_EQ(hash<int64_t>(0xcafecafedeadbeef), -256235155);
  EXPECT_EQ(hash<int64_t>(0xdeadbeefcafecafe), 673261790);
  EXPECT_EQ(hash<int64_t>(INT64_MAX), -1604625029);
  EXPECT_EQ(hash<int64_t>(INT64_MIN), -853646085);
  EXPECT_EQ(hash<int64_t>(1), -1712319331);
  EXPECT_EQ(hash<int64_t>(0), -1670924195);
  EXPECT_EQ(hash<int64_t>(-1), -939490007);
  EXPECT_EQ(hash<int64_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int32) {
  EXPECT_EQ(hash<int32_t>(0xdeadbeef), 141248195);
  EXPECT_EQ(hash<int32_t>(0xcafecafe), 638354558);
  EXPECT_EQ(hash<int32_t>(1), -559580957);
  EXPECT_EQ(hash<int32_t>(0), 933211791);
  EXPECT_EQ(hash<int32_t>(-1), -1604776387);
  EXPECT_EQ(hash<int32_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int16) {
  EXPECT_EQ(hash<int16_t>(1), -559580957);
  EXPECT_EQ(hash<int16_t>(0), 933211791);
  EXPECT_EQ(hash<int16_t>(-1), -1604776387);
  EXPECT_EQ(hash<int16_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int8) {
  EXPECT_EQ(hash<int8_t>(1), -559580957);
  EXPECT_EQ(hash<int8_t>(0), 933211791);
  EXPECT_EQ(hash<int8_t>(-1), -1604776387);
  EXPECT_EQ(hash<int8_t>(std::nullopt), 42);
}

TEST_F(HashTest, Bool) {
  EXPECT_EQ(hash<bool>(false), 933211791);
  EXPECT_EQ(hash<bool>(true), -559580957);
  EXPECT_EQ(hash<bool>(std::nullopt), 42);
}

TEST_F(HashTest, StringInt32) {
  auto hash = [&](std::optional<std::string> a, std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("hash(c0, c1)", a, b);
  };

  EXPECT_EQ(hash(std::nullopt, std::nullopt), 42);
  EXPECT_EQ(hash("", std::nullopt), 142593372);
  EXPECT_EQ(hash(std::nullopt, 0), 933211791);
  EXPECT_EQ(hash("", 0), 1143746540);
}

TEST_F(HashTest, Double) {
  using limits = std::numeric_limits<double>;

  EXPECT_EQ(hash<double>(std::nullopt), 42);
  EXPECT_EQ(hash<double>(-0.0), -1670924195);
  EXPECT_EQ(hash<double>(0), -1670924195);
  EXPECT_EQ(hash<double>(1), -460888942);
  EXPECT_EQ(hash<double>(limits::quiet_NaN()), -1281358385);
  EXPECT_EQ(hash<double>(limits::infinity()), 833680482);
  EXPECT_EQ(hash<double>(-limits::infinity()), 461104036);
}

TEST_F(HashTest, Float) {
  using limits = std::numeric_limits<float>;

  EXPECT_EQ(hash<float>(std::nullopt), 42);
  EXPECT_EQ(hash<float>(-0.0f), 933211791);
  EXPECT_EQ(hash<float>(0), 933211791);
  EXPECT_EQ(hash<float>(1), -466301895);
  EXPECT_EQ(hash<float>(limits::quiet_NaN()), -349261430);
  EXPECT_EQ(hash<float>(limits::infinity()), 2026854605);
  EXPECT_EQ(hash<float>(-limits::infinity()), 427440766);
}

TEST_F(HashTest, Array) {
  auto arg = makeNullableArrayVector<int>({{0, 1}});
  EXPECT_EQ(
      evaluateOnce<int32_t>("hash(c0)", makeRowVector({arg})).value(),
      -1834803471);

  auto testArrayHash =
      [&](const VectorPtr& arrayVector,
          const std::vector<std::optional<int32_t>>& expected) {
        auto result = evaluate<SimpleVector<int32_t>>(
            "hash(c0)", makeRowVector({arrayVector}));

        assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
      };

  auto arrayOfMap = makeArrayOfMapVector(mapInput());
  testArrayHash(arrayOfMap, {42, 1711427742, 48282590, 311826655});

  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayOfRow = makeArrayOfRowVector(rowType, rowInput());
  // last one, 1958209746 -> -514935617
  testArrayHash(arrayOfRow, {42, 42, -1074044201, -514935617});

  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = emptyArray;
  array_type array3 = {{1, 100, 2}};

  // -1181176833, -1181176833, 1246926396
  auto nestedArray =
      makeNullableNestedArrayVector<int64_t>({{{array1, array2, array3}}});
  testArrayHash(nestedArray, {1246926396});

  auto unknownArray = makeArrayWithDictionaryElements<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      2,
      ARRAY(UNKNOWN()));
  testArrayHash(unknownArray, {42, 42});
}

TEST_F(HashTest, Map) {
  auto testMapHash = [&](const VectorPtr& mapVector,
                         const std::vector<std::optional<int32_t>>& expected) {
    auto result =
        evaluate<SimpleVector<int32_t>>("hash(c0)", makeRowVector({mapVector}));

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };
  auto arg =
      makeMapVector<int32_t, int32_t>({{{1, 1}}}, MAP(INTEGER(), INTEGER()));
  testMapHash(arg, {245521047});

  auto mapOfArray = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, {{2}}}}, {{3, {{4, 5, 6}}}}, {{7, {{8, 9, 10}}}}});
  testMapHash(mapOfArray, {3113790463, 3173539792, 1816103839});

  auto mapOfMap = createMapOfMapVector<int64_t, int64_t, int64_t>(
      {{{1, {{{2, 3}, {4, 5}}}}}, {{6, {{{7, 8}}}}}});
  testMapHash(mapOfMap, {2101165938, 1066583986});

  auto mapOfRow = createMapOfRowVector<int64_t, int64_t, int64_t>(
      {{{1, {{2, 3}}}, {3, {{4, 5}}}}, {{6, {{7, 8}}}}});
  testMapHash(mapOfRow, {3568381598, 1066583986});

  auto keys = makeFlatUnknownVector(0);
  auto values = makeFlatUnknownVector(0);
  auto mapOfUnknown = makeMapVector({0, 0, 0}, keys, values);
  testMapHash(mapOfUnknown, {42, 42, 42});
}

TEST_F(HashTest, Row) {
  auto row = makeRowVector({
      makeNullableFlatVector<int64_t>({11, 12}),
      makeNullableFlatVector<int64_t>({1, 2}),
  });
  auto result =
      evaluate<SimpleVector<int32_t>>("hash(c0)", makeRowVector({row}));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({3565597558, 78809581}), result);

  auto testRowHash = [&](const VectorPtr& rowVector,
                         const std::vector<std::optional<int32_t>>& expected) {
    auto result =
        evaluate<SimpleVector<int32_t>>("hash(c0)", makeRowVector({rowVector}));

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };

  auto boolRow = makeRowVector(
      {makeNullableFlatVector<int32_t>({11, 12}),
       makeNullableFlatVector<bool>({true, false})});
  testRowHash(boolRow, {-195158775, -1057720493});

  auto rowNested = makeRowVector(
      {makeMapVector<int32_t, int32_t>(
           {
               {{1, 10}, {2, 20}},
               {{3, 30}, {4, 40}},
           },
           MAP(INTEGER(), INTEGER())),
       makeNullableArrayVector<int32_t>({{0, 1}, {2, 3}}),
       makeRowVector({
           makeNullableFlatVector<int64_t>({11, 12}),
           makeNullableFlatVector<int64_t>({1, 2}),
       })});
  testRowHash(rowNested, {2387589476, 776268270});

  auto rowOfUnknownChildren = makeRowVector({
      makeFlatUnknownVector(2),
      makeFlatUnknownVector(2),
  });
  testRowHash(rowOfUnknownChildren, {42, 42});

  rowOfUnknownChildren = makeRowVector(
      {makeMapVector<int32_t, int32_t>(
           {
               {{1, 10}, {2, 20}},
               {{3, 30}, {4, 40}},
           },
           MAP(INTEGER(), INTEGER())),
       makeArrayWithDictionaryElements<UnknownValue>(
           {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
           2,
           ARRAY(UNKNOWN())),
       makeNullableArrayVector<int32_t>({{0, 1}, {2, 3}}),
       makeFlatUnknownVector(2),
       makeRowVector({
           makeNullableFlatVector<int64_t>({11, 12}),
           makeNullableFlatVector<int64_t>({1, 2}),
       })});
  testRowHash(rowOfUnknownChildren, {-1907377820, 776268270});
}

TEST_F(HashTest, Complex) {
  auto testHash = [&](const VectorPtr& vector,
                      const std::vector<std::optional<int32_t>>& expected) {
    auto result =
        evaluate<SimpleVector<int32_t>>("hash(c0)", makeRowVector({vector}));

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };

  // row(map, array, row)
  auto rowNested = makeRowVector(
      {makeMapVector<int32_t, int32_t>(
           {
               {{1, 10}, {2, 20}},
               {{3, 30}, {4, 40}, {9, 90}},
               {{5, std::nullopt}, {6, 60}},
               {{7, std::nullopt}},
           },
           MAP(INTEGER(), INTEGER())),
       makeNullableArrayVector<int32_t>(
           {{0, 1},
            {2, 3, 4},
            {6, std::nullopt, 7},
            {std::nullopt, std::nullopt}}),
       makeRowVector({
           makeNullableFlatVector<int64_t>({11, 12, std::nullopt, 13}),
           makeNullableFlatVector<int64_t>({1, 2, 4, 5}),
       })});

  // map(array)
  auto mapOfArray = createMapOfArraysVector<int64_t, int64_t>(
      {{{3, {{4, 5, 6, 1, 2}}}},
       {{5, {{4, std::nullopt, 1, 2}}}},
       {{0, {{4, 5, 6, 1, 2}}}},
       {{7, std::nullopt}}});
  // map(map)
  auto mapOfMap = createMapOfMapVector<int64_t, int64_t, int64_t>(
      {{{1, {{{2, 3}, {4, 5}}}}},
       {{0, {{{2, 3}, {4, 5}}}}},
       {{1, {{{2, 3}, {4, std::nullopt}}}}},
       {{6, std::nullopt}}});
  // map(row)
  auto mapOfRow = createMapOfRowVector<int64_t, int64_t, int64_t>(
      {{{1, {{2, 3}}}, {3, {{4, 5}}}},
       {{0, {{2, 3}}}, {3, std::nullopt}},
       {{1, {{2, std::nullopt}}}, {3, {{std::nullopt, 5}}}},
       {{6, {{7, 8}}}}});

  // array(map)
  auto arrayOfMap = makeArrayOfMapVector(mapInput());
  // array(row)
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayOfRow = makeArrayOfRowVector(rowType, rowInput());
  // array(array)
  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = std::nullopt;
  array_type array2 = {{1, 100, 2}};
  array_type array3 = {{1, 100, std::nullopt}};
  array_type array4 = {{315}};

  auto nestedArray = makeNullableNestedArrayVector<int64_t>(
      {{{array1, array2}}, {{array2, array4}}, {{array3}}, {{array4}}});

  // started with row
  // row(row(map, array, row), array(row), array(map), array(array), map(array),
  // map(row), map(map))
  auto finalRowVector = makeRowVector(
      {nestedArray,
       arrayOfMap,
       arrayOfRow,
       mapOfArray,
       mapOfMap,
       mapOfRow,
       rowNested});
  testHash(finalRowVector, {35206719, 336579516, -666873760, 1255129121});
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
