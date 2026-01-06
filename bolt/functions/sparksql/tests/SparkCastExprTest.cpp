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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/functions/prestosql/tests/CastBaseTest.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/parse/TypeResolver.h"
using namespace bytedance::bolt;
namespace bytedance::bolt::test {
namespace {

class SparkCastExprTest : public functions::test::CastBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    functions::sparksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  template <typename T>
  void testDecimalToIntegralCasts() {
    auto shortFlat = makeNullableFlatVector<int64_t>(
        {-300,
         -260,
         -230,
         -200,
         -100,
         0,
         5500,
         5749,
         5755,
         6900,
         7200,
         std::nullopt},
        DECIMAL(6, 2));
    testCast(
        shortFlat,
        makeNullableFlatVector<T>(
            {-3,
             -2 /*-2.6 truncated to -2*/,
             -2 /*-2.3 truncated to -2*/,
             -2,
             -1,
             0,
             55,
             57 /*57.49 truncated to 57*/,
             57 /*57.55 truncated to 57*/,
             69,
             72,
             std::nullopt}));
    auto longFlat = makeNullableFlatVector<int128_t>(
        {-30'000'000'000,
         -25'500'000'000,
         -24'500'000'000,
         -20'000'000'000,
         -10'000'000'000,
         0,
         550'000'000'000,
         554'900'000'000,
         559'900'000'000,
         690'000'000'000,
         720'000'000'000,
         std::nullopt},
        DECIMAL(20, 10));
    testCast(
        longFlat,
        makeNullableFlatVector<T>(
            {-3,
             -2 /*-2.55 truncated to -2*/,
             -2 /*-2.45 truncated to -2*/,
             -2,
             -1,
             0,
             55,
             55 /* 55.49 truncated to 55*/,
             55 /* 55.99 truncated to 55*/,
             69,
             72,
             std::nullopt}));
  }

  template <typename T>
  void testIntegralToTimestampCast() {
    testCast(
        makeNullableFlatVector<T>({
            0,
            1,
            std::numeric_limits<T>::max(),
            std::numeric_limits<T>::min(),
            std::nullopt,
        }),
        makeNullableFlatVector<Timestamp>(
            {Timestamp(0, 0),
             Timestamp(1, 0),
             Timestamp(std::numeric_limits<T>::max(), 0),
             Timestamp(std::numeric_limits<T>::min(), 0),
             std::nullopt}));
  }
};

#ifdef SPARK_COMPATIBLE

TEST_F(SparkCastExprTest, date) {
  testTryCast<std::string, int32_t>(
      "date",
      {"0",           "01",         "012",        "0123",        "-0",
       "-01",         "-012",       "-0123",      "1970-01-01",  "2020-01-01",
       "2135-11-09",  "1969-12-27", "1812-04-15", "1920-01-02",  "12345-12-18",
       "1970-1-2",    "1970-01-2",  "1970-1-02",  "+1970-01-02", "-1-1-1",
       " 1970-01-01", std::nullopt},
      {std::nullopt,
       std::nullopt,
       std::nullopt,
       -674603,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       -764452,
       0,
       18262,
       60577,
       -5,
       -57604,
       -18262,
       3789742,
       1,
       1,
       1,
       1,
       std::nullopt,
       0,
       std::nullopt},
      VARCHAR(),
      DATE());
  testTryCast<std::string, int32_t>(
      "date",
      {"12345",
       "2015",
       "2015-03",
       "2015-03-18T",
       "2015-03-18T123123",
       "2015-03-18 123142",
       "2015-03-18 (BC)"},
      {3789391, 16436, 16495, 16512, 16512, 16512, 16512},
      VARCHAR(),
      DATE());
}

TEST_F(SparkCastExprTest, decimalToIntegral) {
  testDecimalToIntegralCasts<int64_t>();
  testDecimalToIntegralCasts<int32_t>();
  testDecimalToIntegralCasts<int16_t>();
  testDecimalToIntegralCasts<int8_t>();
}

TEST_F(SparkCastExprTest, invalidDate) {
  testUnsupportedCast<int8_t>(
      "date", {12}, "Cast from TINYINT to DATE is not supported", TINYINT());
  testUnsupportedCast<int16_t>(
      "date",
      {1234},
      "Cast from SMALLINT to DATE is not supported",
      SMALLINT());
  testUnsupportedCast<int32_t>(
      "date", {1234}, "Cast from INTEGER to DATE is not supported", INTEGER());
  testUnsupportedCast<int64_t>(
      "date", {1234}, "Cast from BIGINT to DATE is not supported", BIGINT());

  testUnsupportedCast<float>(
      "date", {12.99}, "Cast from REAL to DATE is not supported", REAL());
  testUnsupportedCast<double>(
      "date", {12.99}, "Cast from DOUBLE to DATE is not supported", DOUBLE());

  // Parsing ill-formated dates.
  testTryCast<std::string, int32_t>(
      "date", {"2012-Oct-23"}, {std::nullopt}, VARCHAR(), DATE());
  testTryCast<std::string, int32_t>(
      "date", {"2015-03-18X"}, {std::nullopt}, VARCHAR(), DATE());
  testTryCast<std::string, int32_t>(
      "date", {"2015/03/18"}, {std::nullopt}, VARCHAR(), DATE());
  testTryCast<std::string, int32_t>(
      "date", {"2015.03.18"}, {std::nullopt}, VARCHAR(), DATE());
  testTryCast<std::string, int32_t>(
      "date", {"20150318"}, {std::nullopt}, VARCHAR(), DATE());
  testTryCast<std::string, int32_t>(
      "date", {"2015-031-8"}, {std::nullopt}, VARCHAR(), DATE());
}

TEST_F(SparkCastExprTest, stringToTimestamp) {
  std::vector<std::optional<std::string>> input{
      "1970-01-01",
      "2000-01-01",
      "1970-01-01 00:00:00",
      "1970-01-01 00:00:00+00:00",
      "1970-01-01 00:00:00-0000",
      "2000-01-01 12:21:56",
      "1970-01-01 00:00:00-02:00",
      "1970-01-01 00:00:00-02",
      "1970-01-01 00:00:00-2",
      std::nullopt,
      "2015-03-18T12:03:17",
      "2015-03-18 12:03:17Z",
      "2015-03-18T12:03:17Z",
      "2015-03-18T10:03:17-02",
      "2015-03-18 12:03:17.123",
      "2015-03-18T12:03:17.123",
      "2015-03-18T12:03:17.0123",
      "2015-03-18T12:03:17.000001",
      "2015-03-18T12:03:17.456Z",
      "2015-03-18 12:03:17.456Z",
  };
  std::vector<std::optional<Timestamp>> expected{
      Timestamp(0, 0),
      Timestamp(946684800, 0),
      Timestamp(0, 0),
      Timestamp(0, 0),
      Timestamp(0, 0),
      Timestamp(946729316, 0),
      Timestamp(7200, 0),
      Timestamp(7200, 0),
      Timestamp(7200, 0),
      std::nullopt,
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 12300000),
      Timestamp(1426680197, 1000),
      Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000),
  };
  testCast<std::string, Timestamp>("timestamp", input, expected);

  setQueryTimeZone("Asia/Shanghai");
  testCast<std::string, Timestamp>(
      "timestamp",
      {"1970-01-01 00:00:00",
       "1970-01-01 08:00:00",
       "1970-01-01 08:00:00.123000000",
       "1970-01-01 08:00:00.001020",
       "1970-01-01 08:00:59",
       "1970"},
      {Timestamp(-8 * 3600, 0),
       Timestamp(0, 0),
       Timestamp(0, 123000000),
       Timestamp(0, 1020000),
       Timestamp(59, 0),
       Timestamp(-8 * 3600, 0)});
}

TEST_F(SparkCastExprTest, stringToTimestampWithTimezone) {
  std::vector<std::optional<std::string>> input{
      "2015-03-18 20:03",
      "2015-03-18 20",
      "2015-03-18",
      "2015-03-18T",
      "2015-03-18 ",
      "2015-03-18T ",
      "2015-03-18  ",
      "2015-03-18T UTC+8",
      "2015-03-18  UTC+8",
      "2015-03",
      "2015",
      "2015-03-18T20:03:17.456",
      "2015-03-18 20:03:17.456",
      "2015-03-18T10:03:17.456-02:00",
      "2015-03-18 14:03:17.456+02:00",
      "2015-03-18 14:23:17.456+02:20",
      "2015-03-18T12:03:17.456UTC",
      "2015-03-18T12:03:17.456 UTC",
      "2015-03-18 12:03:17.456 UTC",
      "2015-03-18 12:03:17.456 UCT",
      "2015-03-18 12:03:17.456 GMT",
      "2015-03-18 12:03:17.456 GMT0",
      "2015-03-18 12:03:17.456 UT",
      "2015-03-18 10:03:17.456 UTC-02:00",
      "2015-03-18 10:03:17.456 UTC-02",
      "2015-03-18 10:03:17.456 UTC-2",
      "2015-03-18 10:03:17.456 UCT-02:00",
      "2015-03-18 10:03:17.456 GMT-02:00",
      "2015-03-18 10:03:17.456 GMT0-02:00",
      "2015-03-18 10:03:17.456 UT-02:00",
      "2015-03-18 15:03:17.456 UT+3",
      "2015-03-18T12:03:17.456Z",
      "2015-03-18 12:03:17.456Z",
      "2015-03-18T05:03:17.456 America/Los_Angeles",
      "2015-03-18T13:03:17.456 Europe/Paris",
      "2015-03-18T23:03:17.456 Australia/Sydney",
  };
  std::vector<std::optional<Timestamp>> expected{
      Timestamp(1426680180, 000000000), Timestamp(1426680000, 000000000),
      Timestamp(1426608000, 000000000), Timestamp(1426608000, 000000000),
      Timestamp(1426608000, 000000000), Timestamp(1426608000, 000000000),
      Timestamp(1426608000, 000000000), Timestamp(1426608000, 000000000),
      Timestamp(1426608000, 000000000), Timestamp(1425139200, 000000000),
      Timestamp(1420041600, 000000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000), Timestamp(1426680197, 456000000),
  };

  setQueryTimeZone("Asia/Shanghai");

  testCast<std::string, Timestamp>("timestamp", input, expected);
}

TEST_F(SparkCastExprTest, intToTimestamp) {
  // Cast bigint as timestamp.
  testCast(
      makeNullableFlatVector<int64_t>({
          0,
          1727181032,
          -1727181032,
          9223372036855,
          -9223372036856,
          std::numeric_limits<int64_t>::max(),
          std::numeric_limits<int64_t>::min(),
      }),
      makeNullableFlatVector<Timestamp>({
          Timestamp(0, 0),
          Timestamp(1727181032, 0),
          Timestamp(-1727181032, 0),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
      }));

  // Cast tinyint/smallint/integer as timestamp.
  testIntegralToTimestampCast<int8_t>();
  testIntegralToTimestampCast<int16_t>();
  testIntegralToTimestampCast<int32_t>();
}

TEST_F(SparkCastExprTest, primitiveInvalidCornerCases) {
  // To integer.
  { testInvalidCast<std::string>("tinyint", {""}, "Empty string"); }

  // To floating-point.
  {
    // Invalid strings.
    testInvalidCast<std::string>(
        "real",
        {"1.2a"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "real",
        {"1.2.3"},
        "Non-whitespace character found after end of conversion");
  }

  // To boolean.
  {
    testInvalidCast<std::string>(
        "boolean",
        {"1.7E308"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean",
        {"nan"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean", {"infinity"}, "Invalid value for bool");
    testInvalidCast<std::string>(
        "boolean", {"12"}, "Integer overflow when parsing bool");
    testInvalidCast<std::string>("boolean", {"-1"}, "Invalid value for bool");
    testInvalidCast<std::string>(
        "boolean",
        {"tr"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean",
        {"tru"},
        "Non-whitespace character found after end of conversion");

    testInvalidCast<std::string>("boolean", {"on"}, "");

    testInvalidCast<std::string>("boolean", {"off"}, "");
  }
}

TEST_F(SparkCastExprTest, primitiveValidCornerCases) {
  // To integer.
  {
    // Valid strings.
    testCast<std::string, int8_t>("tinyint", {"1.2"}, {1});
    testCast<std::string, int8_t>("tinyint", {"1.23444"}, {1});
    testCast<std::string, int8_t>("tinyint", {".2355"}, {0});
    testCast<std::string, int8_t>("tinyint", {"-1.8"}, {-1});
    testCast<std::string, int8_t>("tinyint", {"+1"}, {1});
    testCast<std::string, int8_t>("tinyint", {"1."}, {1});
    testCast<std::string, int8_t>("tinyint", {"-1"}, {-1});
    testCast<std::string, int8_t>("tinyint", {"-1."}, {-1});
    testCast<std::string, int8_t>("tinyint", {"0."}, {0});
    testCast<std::string, int8_t>("tinyint", {"."}, {0});
    testCast<std::string, int8_t>("tinyint", {"-."}, {0});

    testCast<int32_t, int8_t>("tinyint", {1234567}, {-121});
    testCast<int32_t, int8_t>("tinyint", {-1234567}, {121});
    testCast<double, int8_t>("tinyint", {12345.67}, {57});
    testCast<double, int8_t>("tinyint", {-12345.67}, {-57});
    testCast<double, int8_t>("tinyint", {127.1}, {127});
    testCast<float, int64_t>("bigint", {kInf}, {9223372036854775807});
    testCast<float, int64_t>("bigint", {kNan}, {0});
    testCast<float, int32_t>("integer", {kNan}, {0});
    testCast<float, int16_t>("smallint", {kNan}, {0});
    testCast<float, int8_t>("tinyint", {kNan}, {0});

    testCast<double, int64_t>("bigint", {12345.12}, {12345});
    testCast<double, int64_t>("bigint", {12345.67}, {12345});
  }

  // To floating-point.
  {
    testCast<double, float>("real", {1.7E308}, {kInf});

    testCast<std::string, float>("real", {"1.7E308"}, {kInf});
    testCast<std::string, float>("real", {"1."}, {1.0});
    testCast<std::string, float>("real", {"1"}, {1});
    testCast<std::string, float>("real", {"infinity"}, {kInf});
    testCast<std::string, float>("real", {"-infinity"}, {-kInf});
    testCast<std::string, float>("real", {"nan"}, {kNan});
    testCast<std::string, float>("real", {"InfiNiTy"}, {kInf});
    testCast<std::string, float>("real", {"-InfiNiTy"}, {-kInf});
    testCast<std::string, float>("real", {"nAn"}, {kNan});
  }

  // To boolean.
  {
    testCast<int8_t, bool>("boolean", {1}, {true});
    testCast<int8_t, bool>("boolean", {0}, {false});
    testCast<int8_t, bool>("boolean", {12}, {true});
    testCast<int8_t, bool>("boolean", {-1}, {true});
    testCast<double, bool>("boolean", {1.0}, {true});
    testCast<double, bool>("boolean", {1.1}, {true});
    testCast<double, bool>("boolean", {0.1}, {true});
    testCast<double, bool>("boolean", {-0.1}, {true});
    testCast<double, bool>("boolean", {-1.0}, {true});
    testCast<float, bool>("boolean", {kNan}, {true});
    testCast<float, bool>("boolean", {kInf}, {true});
    testCast<double, bool>("boolean", {0.0000000000001}, {true});

    testCast<std::string, bool>("boolean", {"1"}, {true});
    testCast<std::string, bool>("boolean", {"0"}, {false});
    testCast<std::string, bool>("boolean", {"t"}, {true});
    testCast<std::string, bool>("boolean", {"true"}, {true});

    testCast<std::string, bool>("boolean", {" true "}, {true});
    testCast<std::string, bool>("boolean", {" TRUE "}, {true});
    testCast<std::string, bool>("boolean", {" TrUe "}, {true});
  }

  // To string.
  {
    testCast<float, std::string>("varchar", {kInf}, {"Infinity"});
    testCast<float, std::string>("varchar", {kNan}, {"NaN"});
  }
}

TEST_F(SparkCastExprTest, truncate) {
  // Testing truncate cast from double to int.
  testCast<int32_t, int8_t>(
      "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5});
}

TEST_F(SparkCastExprTest, errorHandling) {
  testTryCast<std::string, int8_t>(
      "tinyint",
      {"-",
       "-0",
       " @w 123",
#ifdef SPARK_COMPATIBLE
       "123 ",
#endif
       "  122",
       "",
       "-12-3",
       "1234",
       "-129",
       "1.1.1",
       "1..",
       "1.abc",
       "..",
       "-..",
       "125.5",
       "127",
       "-128"},
      {std::nullopt,
       0,
       std::nullopt,
#ifdef SPARK_COMPATIBLE
       123,
#endif
       122,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       127,
       -128});

  testTryCast<double, int>(
      "integer",
      {1e12, 2.5, 3.6, 100.44, -100.101},
      {std::nullopt, 2, 3, 100, -100});
}

TEST_F(SparkCastExprTest, overflow) {
  testCast<int16_t, int8_t>("tinyint", {456}, {-56});
  testCast<int32_t, int8_t>("tinyint", {266}, {10});
  testCast<int64_t, int8_t>("tinyint", {1234}, {-46});
  testCast<int64_t, int16_t>("smallint", {1234567}, {-10617});
  testCast<double, int8_t>("tinyint", {127.8}, {127});
  testCast<double, int8_t>("tinyint", {129.9}, {-127});
  testCast<double, int16_t>("smallint", {1234567.89}, {-10617});
  testCast<double, int64_t>(
      "bigint", {std::numeric_limits<double>::max()}, {9223372036854775807});
  testCast<double, int64_t>(
      "bigint", {std::numeric_limits<double>::quiet_NaN()}, {0});
  auto shortFlat = makeNullableFlatVector<int64_t>(
      {-3000,
       -2600,
       -2300,
       -2000,
       -1000,
       0,
       55000,
       57490,
       5755,
       6900,
       7200,
       std::nullopt},
      DECIMAL(5, 1));
  testCast(
      shortFlat,
      makeNullableFlatVector<int8_t>(
          {-44, -4, 26, 56, -100, 0, 124, 117, 63, -78, -48, std::nullopt}),
      false);

  testCast(
      shortFlat,
      makeNullableFlatVector<int8_t>(
          {std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           -100,
           0,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt}),
      true);

  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int8_t>({0}),
      false);
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int32_t>({-2147483648}),
      false);
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int64_t>({2147483648}),
      false);
}

TEST_F(SparkCastExprTest, timestampToString) {
  testCast<Timestamp, std::string>(
      "string",
      {
          Timestamp(-946684800, 0),
          Timestamp(-7266, 0),
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(9466848000, 0),
          Timestamp(94668480000, 0),
          Timestamp(946729316, 0),
          Timestamp(946729316, 123),
          Timestamp(946729316, 100000000),
          Timestamp(946729316, 129900000),
          Timestamp(946729316, 123456789),

          Timestamp(7266, 0),
          Timestamp(-50049331200, 0),
          Timestamp(253405036800, 0),
          Timestamp(-62480037600, 0),
          std::nullopt,
      },
      {
          "1940-01-02 00:00:00",
          "1969-12-31 21:58:54",
          "1970-01-01 00:00:00",
          "2000-01-01 00:00:00",
          "2269-12-29 00:00:00",
          "4969-12-04 00:00:00",
          "2000-01-01 12:21:56",
          "2000-01-01 12:21:56",
          "2000-01-01 12:21:56.1",
          "2000-01-01 12:21:56.1299",
          "2000-01-01 12:21:56.123456",
          "1970-01-01 02:01:06",
          "0384-01-01 08:00:00",
          "+10000-02-01 16:00:00",
          "-0010-02-01 10:00:00",
          std::nullopt,
      });
  {
    std::vector<std::optional<Timestamp>> input = {
        Timestamp(-946684800, 0),
        Timestamp(-7266, 0),
        Timestamp(0, 0),
        Timestamp(61, 10),
        Timestamp(3600, 0),
        Timestamp(946684800, 0),
        Timestamp(946729316, 0),
        Timestamp(946729316, 123),
        Timestamp(946729316, 100000000),
        Timestamp(946729316, 129900000),
        Timestamp(946729316, 123456789),
        Timestamp(7266, 0),
        std::nullopt,
    };

    setQueryTimeZone("America/Los_Angeles");
    testCast<Timestamp, std::string>(
        "string",
        input,
        {
            "1940-01-01 16:00:00",
            "1969-12-31 13:58:54",
            "1969-12-31 16:00:00",
            "1969-12-31 16:01:01",
            "1969-12-31 17:00:00",
            "1999-12-31 16:00:00",
            "2000-01-01 04:21:56",
            "2000-01-01 04:21:56",
            "2000-01-01 04:21:56.1",
            "2000-01-01 04:21:56.1299",
            "2000-01-01 04:21:56.123456",
            "1969-12-31 18:01:06",
            std::nullopt,
        });
    setQueryTimeZone("Asia/Shanghai");
    testCast<Timestamp, std::string>(
        "string",
        input,
        {
            "1940-01-02 08:00:00",
            "1970-01-01 05:58:54",
            "1970-01-01 08:00:00",
            "1970-01-01 08:01:01",
            "1970-01-01 09:00:00",
            "2000-01-01 08:00:00",
            "2000-01-01 20:21:56",
            "2000-01-01 20:21:56",
            "2000-01-01 20:21:56.1",
            "2000-01-01 20:21:56.1299",
            "2000-01-01 20:21:56.123456",
            "1970-01-01 10:01:06",
            std::nullopt,
        });
  }
}

TEST_F(SparkCastExprTest, fromString) {
  // String with leading and trailing whitespaces.
  testCast(
      makeFlatVector<StringView>(
          {" 9999999999.99",
#ifdef SPARK_COMPATIBLE
           "9999999999.99 ",
           "-3E+2 ",
#endif
           " -3E+2"}),
      makeFlatVector<int64_t>(
          {999'999'999'999,
#ifdef SPARK_COMPATIBLE
           999'999'999'999,
           -30000,
#endif
           -30000},
          DECIMAL(12, 2)));
}

TEST_F(SparkCastExprTest, testInvalidDate) {
  testInvalidCast<std::string>("date", {"2025/05"}, "");
  testInvalidCast<std::string>("date", {"2025-05/01"}, "");
  testInvalidCast<std::string>("date", {"2025 05"}, "");
  testInvalidCast<std::string>("date", {"abc"}, "");

  testInvalidCast<std::string>("timestamp", {"2025/05"}, "");
  testInvalidCast<std::string>("timestamp", {"2025-05/01"}, "");
  testInvalidCast<std::string>("timestamp", {"2025 05"}, "");
  testInvalidCast<std::string>("timestamp", {"abc"}, "");
}

TEST_F(SparkCastExprTest, testBinary) {
  testCast<int8_t, std::string>(TINYINT(), VARBINARY(), {0x30}, {"0"});
  testCast<int16_t, std::string>(SMALLINT(), VARBINARY(), {0x3031}, {"01"});
  testCast<int32_t, std::string>(
      INTEGER(), VARBINARY(), {0x30313233}, {"0123"});
  testCast<int64_t, std::string>(
      BIGINT(), VARBINARY(), {0x3031323334353637}, {"01234567"});
  testCast<std::string, std::string>(
      VARCHAR(), VARBINARY(), {"76543210"}, {"76543210"});
  testUnsupportedCast<float>("binary", {1.2}, "");
}

#endif
} // namespace
} // namespace bytedance::bolt::test
