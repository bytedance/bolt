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

#include <array>
#include <limits>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/functions/flinksql/TemporalFunctions.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "bolt/type/TimestampConversion.h"

namespace bytedance::bolt::functions::flinksql::test {
namespace {

using bolt::test::assertEqualVectors;

class TemporalFunctionsTest : public FlinkFunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    exec::registerFunctionCallToSpecialForms();
    FlinkFunctionBaseTest::SetUpTestCase();
  }

  std::optional<Timestamp> evaluateTimestamp(
      const std::string& expression,
      const std::optional<Timestamp>& value) {
    return evaluateOnce<Timestamp>(expression, value);
  }

  std::optional<Timestamp> parseString(
      const std::string& expression,
      const std::optional<std::string>& value) {
    return evaluateOnce<Timestamp>(expression, value);
  }

  Timestamp timestamp(const std::string& text) {
    return util::fromTimestampStringNanos(text.data(), text.size(), nullptr);
  }
};

TEST_F(TemporalFunctionsTest, stringNanoseconds) {
  const std::vector<std::string> texts = {
      "1970-01-01",
      "1970-01-01 00:00:00.1",
      "1970-01-01 00:00:00.12",
      "1970-01-01 00:00:00.123",
      "1970-01-01 00:00:00.1234",
      "1970-01-01 00:00:00.12345",
      "1970-01-01 00:00:00.123456",
      "1970-01-01 00:00:00.1234567",
      "1970-01-01 00:00:00.12345678",
      "1970-01-01 00:00:00.123456789",
      "1969-12-31 23:59:59.999999999",
      "9999-12-31 23:59:59.123456789",
      "0001-01-01 00:00:00.000000001",
      "  1970-1-1 1:1:1.000000001\t"};
  const std::vector<Timestamp> expected = {
      Timestamp(0, 0),
      Timestamp(0, 100000000),
      Timestamp(0, 120000000),
      Timestamp(0, 123000000),
      Timestamp(0, 123400000),
      Timestamp(0, 123450000),
      Timestamp(0, 123456000),
      Timestamp(0, 123456700),
      Timestamp(0, 123456780),
      Timestamp(0, 123456789),
      Timestamp(-1, 999999999),
      Timestamp(253402300799, 123456789),
      Timestamp(-62135596800, 1),
      Timestamp(3661, 1)};
  auto input = makeRowVector({makeFlatVector<std::string>(texts)});
  for (const auto* expression :
       {"to_timestamp(c0)",
        "flink_string_to_timestamp(c0, true)",
        "flink_string_to_timestamp(c0, false)"}) {
    SCOPED_TRACE(expression);
    assertEqualVectors(
        makeFlatVector<Timestamp>(expected), evaluate(expression, input));
  }
}

TEST_F(TemporalFunctionsTest, stringJavaResolution) {
  // Flink first uses DateTimeFormatter's SMART resolver, then falls back to
  // java.sql parsing. Whitespace and variable-width fields select the latter.
  const std::vector<std::string> texts = {
      "2023-02-29",
      "2023-02-29 00:00:00",
      "2023-2-29 0:0:0",
      "2020-01-01 24:00:01",
      "2020-01-01 23:59:60",
      "0000-01-01",
      "0000-01-01 00:00:00.000000001",
      "1582-10-10 00:00:00",
      " 1582-10-10 00:00:00 "};
  const std::vector<Timestamp> expected = {
      Timestamp(1677542400, 0),
      Timestamp(1677542400, 0),
      Timestamp(1677628800, 0),
      Timestamp(1577923201, 0),
      Timestamp(1577923200, 0),
      Timestamp(-62135596800, 0),
      Timestamp(-62135596800, 1),
      Timestamp(-12219724800, 0),
      Timestamp(-12218860800, 0)};
  auto input = makeRowVector({makeFlatVector<std::string>(texts)});
  for (const auto* expression :
       {"to_timestamp(c0)", "flink_string_to_timestamp(c0, true)"}) {
    SCOPED_TRACE(expression);
    assertEqualVectors(
        makeFlatVector<Timestamp>(expected), evaluate(expression, input));
  }
}

TEST_F(TemporalFunctionsTest, stringErrorsAndNulls) {
  auto texts = makeNullableFlatVector<std::string>(
      {"",
       " ",
       "invalid",
       "1970",
       "1970/01/01",
       "1970-01-01T00:00:00",
       "1970-01-01 00:00",
       "1970-01-01 00:00:00Z",
       "1970-01-01 00:00:00+00:00",
       "1970-01-01 00:00:00.",
       "1970-01-01 00:00:00.1234567890",
       "1970-1-1 0:0:0.1234567890",
       "1970-01-01 00:00:00.1x",
       "1970-01-01 00:00:00.123456789x",
       "1970-13-01",
       "1970-01-32",
       std::nullopt});
  auto input = makeRowVector({texts});
  auto expected = makeNullConstant(TypeKind::TIMESTAMP, texts->size());
  for (const auto* expression :
       {"to_timestamp(c0)",
        "flink_string_to_timestamp(c0, true)",
        "try(flink_string_to_timestamp(c0, false))"}) {
    SCOPED_TRACE(expression);
    assertEqualVectors(expected, evaluate(expression, input));
  }
  BOLT_ASSERT_THROW(
      evaluate("flink_string_to_timestamp(c0, false)", input),
      "Invalid timestamp literal");
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<Timestamp>(
          "flink_string_to_timestamp(c0, c1)",
          std::optional<std::string>("1970-01-01"),
          std::optional<bool>()));
  EXPECT_EQ(
      std::nullopt,
      parseString(
          "flink_string_to_timestamp(c0, false)",
          std::optional<std::string>()));
  EXPECT_EQ(
      Timestamp(0, 0),
      parseString(
          "flink_string_to_timestamp(c0, true)",
          std::string(
              "\0"
              "1970-01-01\0",
              12)));
}

TEST_F(TemporalFunctionsTest, formattedTimestampFallback) {
  auto texts = makeNullableFlatVector<std::string>(
      {"2023-02-29",
       "1970-01-01 00:00:00.123456789",
       "1970-1-1 0:0:0.000000001",
       "１９７０-01-01",
       "1582-10-10",
       "2020-01-01 24:00:01",
       "0000-02-29",
       "invalid",
       std::nullopt});
  auto expected = makeNullableFlatVector<Timestamp>(
      {timestamp("2023-03-01 00:00:00"),
       Timestamp(0, 123456789),
       Timestamp(0, 1),
       Timestamp(0, 0),
       timestamp("1582-10-20 00:00:00"),
       timestamp("2020-01-02 00:00:01"),
       std::nullopt,
       std::nullopt,
       std::nullopt});
  assertEqualVectors(
      expected,
      evaluate("to_timestamp(c0, 'yyyy/MM/dd')", makeRowVector({texts})));
  auto formats = makeFlatVector<std::string>(texts->size(), [](auto row) {
    return row % 2 ? "yyyy/MM/dd" : "dd.MM.yyyy";
  });
  assertEqualVectors(
      expected,
      evaluate("to_timestamp(c0, c1)", makeRowVector({texts, formats})));
}

TEST_F(TemporalFunctionsTest, formattedTimestampDefaultJvmZone) {
  const std::string input = "2021-03-14 02:30:00.123";
  EXPECT_EQ(
      timestamp("2021-03-14 03:30:00.123"),
      parseString(
          "to_timestamp(c0, 'yyyy/MM/dd', 'America/New_York', cast(-18000 as integer))",
          input));
  EXPECT_EQ(
      timestamp(input),
      parseString(
          "to_timestamp(c0, 'yyyy-MM-dd HH:mm:ss.SSS', 'America/New_York', cast(-18000 as integer))",
          input));
  EXPECT_EQ(
      timestamp(input), parseString("to_timestamp(c0, 'yyyy/MM/dd')", input));
  EXPECT_EQ(
      std::nullopt,
      parseString(
          "to_timestamp(c0, 'yyyy/MM/dd', 'America/New_York', cast(-18000 as integer))",
          std::nullopt));
}

TEST_F(TemporalFunctionsTest, constantPrecision) {
  const std::array<uint64_t, 10> nanos = {
      0,
      100000000,
      120000000,
      123000000,
      123400000,
      123450000,
      123456000,
      123456700,
      123456780,
      123456789};
  auto values = makeNullableFlatVector<Timestamp>(
      {Timestamp(-1, 123456789),
       Timestamp(0, 123456789),
       Timestamp(253402300799, 123456789),
       std::nullopt});
  for (int32_t p = 0; p <= 9; ++p) {
    const auto expression =
        fmt::format("flink_timestamp_precision(c0, cast({} as integer))", p);
    auto expected = makeNullableFlatVector<Timestamp>(
        {Timestamp(-1, nanos[p]),
         Timestamp(0, nanos[p]),
         Timestamp(253402300799, nanos[p]),
         std::nullopt});
    assertEqualVectors(expected, evaluate(expression, makeRowVector({values})));
    auto indices = makeIndices(8, [](auto row) { return (7 - row) % 4; });
    assertEqualVectors(
        wrapInDictionary(indices, 8, expected),
        evaluate(
            expression, makeRowVector({wrapInDictionary(indices, 8, values)})));
    assertEqualVectors(
        BaseVector::wrapInConstant(8, 0, expected),
        evaluate(
            expression,
            makeRowVector({BaseVector::wrapInConstant(8, 0, values)})));
  }
}

TEST_F(TemporalFunctionsTest, dynamicPrecisionAndErrors) {
  const std::vector<std::optional<int32_t>> p = {
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, 10, std::nullopt};
  auto input = makeRowVector(
      {makeConstant<Timestamp>(Timestamp(-1, 999999999), p.size()),
       makeNullableFlatVector<int32_t>(p)});
  auto expected = makeNullableFlatVector<Timestamp>(
      {Timestamp(-1, 0),
       Timestamp(-1, 900000000),
       Timestamp(-1, 990000000),
       Timestamp(-1, 999000000),
       Timestamp(-1, 999900000),
       Timestamp(-1, 999990000),
       Timestamp(-1, 999999000),
       Timestamp(-1, 999999900),
       Timestamp(-1, 999999990),
       Timestamp(-1, 999999999),
       std::nullopt,
       std::nullopt,
       std::nullopt});
  assertEqualVectors(
      expected, evaluate("try(flink_timestamp_precision(c0, c1))", input));
  BOLT_ASSERT_THROW(
      evaluate("flink_timestamp_precision(c0, c1)", input),
      "Timestamp precision must be between 0 and 9");
  for (const auto precision : {-1, 10}) {
    const auto expression = fmt::format(
        "flink_timestamp_precision(c0, cast({} as integer))", precision);
    EXPECT_EQ(
        std::nullopt,
        evaluateTimestamp("try(" + expression + ")", Timestamp(0, 1)));
    EXPECT_EQ(
        std::nullopt,
        evaluateTimestamp(expression, std::optional<Timestamp>()));
  }
  EXPECT_EQ(
      std::nullopt,
      evaluateTimestamp(
          "flink_timestamp_precision(c0, cast(null as integer))",
          Timestamp(0, 1)));
}

TEST_F(TemporalFunctionsTest, precisionPartialSelection) {
  auto input = makeRowVector(
      {makeNullableFlatVector<Timestamp>(
           {Timestamp(0, 123456789),
            Timestamp(-1, 123456789),
            std::nullopt,
            Timestamp(1, 123456789)}),
       makeFlatVector<bool>({true, false, true, false})});
  assertEqualVectors(
      makeNullableFlatVector<Timestamp>(
          {Timestamp(0, 123456789),
           Timestamp(-1, 123000000),
           std::nullopt,
           Timestamp(1, 123000000)}),
      evaluate(
          "if(c1, flink_timestamp_precision(c0, cast(9 as integer)), flink_timestamp_precision(c0, cast(3 as integer)))",
          input));
}

TEST_F(TemporalFunctionsTest, timeCasts) {
  const std::vector<std::optional<int32_t>> millis = {
      0,
      1,
      86399999,
      -1,
      -86400000,
      std::numeric_limits<int32_t>::min(),
      std::numeric_limits<int32_t>::max(),
      std::nullopt};
  auto input = makeRowVector({makeNullableFlatVector<int32_t>(millis)});
  assertEqualVectors(
      makeNullableFlatVector<Timestamp>(
          {Timestamp(0, 0),
           Timestamp(0, 1000000),
           Timestamp(86399, 999000000),
           Timestamp(-1, 999000000),
           Timestamp(-86400, 0),
           Timestamp(-2147484, 352000000),
           Timestamp(2147483, 647000000),
           std::nullopt}),
      evaluate("flink_time_to_timestamp(c0)", input));
  auto timestamps = makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 999999),
       Timestamp(0, 1000000),
       Timestamp(86399, 999999999),
       Timestamp(-1, 999999999),
       Timestamp(-86400, 1000000),
       Timestamp(-86400, 0),
       Timestamp(-86401, 999999999),
       Timestamp(253402300799, 123456789),
       std::nullopt});
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {0, 1, 86399999, -1, -86399999, 0, -1, 86399123, std::nullopt}),
      evaluate("flink_timestamp_to_time(c0)", makeRowVector({timestamps})));
}

TEST_F(TemporalFunctionsTest, fixedTimeZones) {
  const std::vector<std::pair<std::string, int32_t>> offsets = {
      {"UTC", 0},
      {"GMT", 0},
      {"UT", 0},
      {"Z", 0},
      {"+18:00", 64800},
      {"-18:00", -64800},
      {"+05:30:45", 19845},
      {"UTC-00:00:01", -1},
      {"GMT+8", 28800},
      {"UT+0830", 30600},
      {"-023045", -9045}};
  for (const auto& [zone, offset] : offsets) {
    SCOPED_TRACE(zone);
    for (const auto seconds :
         {int64_t{-62167219200},
          int64_t{-1},
          int64_t{0},
          int64_t{253402300799}}) {
      EXPECT_EQ(
          Timestamp(seconds - offset, 123456789),
          evaluateTimestamp(
              fmt::format("flink_timestamp_to_utc(c0, '{}')", zone),
              Timestamp(seconds, 123456789)));
      EXPECT_EQ(
          Timestamp(seconds + offset, 123456789),
          evaluateTimestamp(
              fmt::format("flink_timestamp_from_utc(c0, '{}')", zone),
              Timestamp(seconds, 123456789)));
    }
  }
}

TEST_F(TemporalFunctionsTest, regionTimeZonesAndDst) {
  struct Case {
    std::string zone;
    std::string local;
    std::string utc;
    std::string resolvedLocal;
  };
  const std::vector<Case> cases = {
      {"Asia/Shanghai",
       "1970-01-01 00:00:00",
       "1969-12-31 16:00:00",
       "1970-01-01 00:00:00"},
      {"America/New_York",
       "2021-03-14 02:30:00",
       "2021-03-14 07:30:00",
       "2021-03-14 03:30:00"},
      {"America/New_York",
       "2021-11-07 01:30:00",
       "2021-11-07 05:30:00",
       "2021-11-07 01:30:00"},
      {"Australia/Lord_Howe",
       "2021-10-03 02:15:00",
       "2021-10-02 15:45:00",
       "2021-10-03 02:45:00"},
      {"Pacific/Apia",
       "2011-12-30 12:00:00",
       "2011-12-30 22:00:00",
       "2011-12-31 12:00:00"},
      {"America/Toronto",
       "1919-03-31 00:00:00",
       "1919-03-31 05:00:00",
       "1919-03-31 01:00:00"},
      {"America/New_York",
       "2037-03-08 02:30:00",
       "2037-03-08 07:30:00",
       "2037-03-08 03:30:00"},
      {"America/New_York",
       "2037-11-01 01:30:00",
       "2037-11-01 05:30:00",
       "2037-11-01 01:30:00"},
      {"Europe/Paris",
       "2037-03-29 02:30:00",
       "2037-03-29 01:30:00",
       "2037-03-29 03:30:00"},
      {"Australia/Lord_Howe",
       "2037-10-04 02:15:00",
       "2037-10-03 15:45:00",
       "2037-10-04 02:45:00"},
      {"America/New_York",
       "2037-07-01 12:00:00",
       "2037-07-01 16:00:00",
       "2037-07-01 12:00:00"}};
  for (const auto& c : cases) {
    SCOPED_TRACE(c.zone + " " + c.local);
    const auto local = timestamp(c.local + ".123456789");
    const auto utc = timestamp(c.utc + ".123456789");
    EXPECT_EQ(
        utc,
        evaluateTimestamp(
            fmt::format("flink_timestamp_to_utc(c0, '{}')", c.zone), local));
    EXPECT_EQ(
        timestamp(c.resolvedLocal + ".123456789"),
        evaluateTimestamp(
            fmt::format("flink_timestamp_from_utc(c0, '{}')", c.zone), utc));
  }
  EXPECT_EQ(
      timestamp("2037-03-29 01:59:59.999999999"),
      evaluateTimestamp(
          "flink_timestamp_from_utc(c0, 'Europe/Paris')",
          timestamp("2037-03-29 00:59:59.999999999")));
  EXPECT_EQ(
      timestamp("2037-03-29 03:00:00"),
      evaluateTimestamp(
          "flink_timestamp_from_utc(c0, 'Europe/Paris')",
          timestamp("2037-03-29 01:00:00")));
  EXPECT_EQ(
      std::nullopt,
      evaluateTimestamp(
          "flink_timestamp_to_utc(c0, 'Asia/Shanghai')",
          std::optional<Timestamp>()));
  EXPECT_EQ(
      std::nullopt,
      evaluateTimestamp(
          "flink_timestamp_from_utc(c0, cast(null as varchar))",
          Timestamp(0, 1)));
}

TEST_F(TemporalFunctionsTest, timeZoneErrors) {
  for (const auto* zone :
       {"+18:00:01",
        "-18:01",
        "+19:00",
        "+05:60",
        "+05:30:60",
        "+0x:00",
        "+1:00",
        "Unknown/Zone",
        ""}) {
    SCOPED_TRACE(zone);
    EXPECT_THROW(
        evaluateTimestamp(
            fmt::format("flink_timestamp_to_utc(c0, '{}')", zone),
            Timestamp(0, 1)),
        BoltUserError);
  }
  BOLT_ASSERT_THROW(
      evaluate(
          "flink_timestamp_to_utc(c0, c1)",
          makeRowVector(
              {makeFlatVector<Timestamp>({Timestamp(0, 1), Timestamp(0, 1)}),
               makeFlatVector<std::string>({"UTC", "Asia/Shanghai"})})),
      "Session time zone must be constant");
}

TEST_F(TemporalFunctionsTest, timeZoneRange) {
  for (const auto* function :
       {"flink_timestamp_to_utc", "flink_timestamp_from_utc"}) {
    const auto expression = fmt::format("{}(c0, 'America/New_York')", function);
    BOLT_ASSERT_THROW(
        evaluateTimestamp(expression, Timestamp::min()),
        "outside of supported timestamp seconds since epoch range");
    EXPECT_EQ(
        std::nullopt,
        evaluateTimestamp("try(" + expression + ")", Timestamp::min()));
  }
}

TEST_F(TemporalFunctionsTest, legacyCalendarBoundaries) {
  EXPECT_EQ(
      timestamp("1582-10-15 00:00:00.000000001"),
      parseString(
          "flink_string_to_timestamp(c0, true)",
          "1582-10-04 23:59:60.000000001"));
  EXPECT_EQ(
      timestamp("1582-10-21 00:00:00"),
      parseString(
          "flink_string_to_timestamp(c0, true)", "1582-10-10 23:59:60"));
  EXPECT_EQ(
      Timestamp(7606495872000, 0),
      parseString(
          "flink_string_to_timestamp(c0, true)", "1970-01-01 -2147483648:0:0"));
}

TEST_F(TemporalFunctionsTest, legacyCalendarInvalidDates) {
  for (const auto* input :
       {"0000-02-29", "0100-2-29 0:0:0", "1500-02-28 23:59:60"}) {
    SCOPED_TRACE(input);
    EXPECT_EQ(std::nullopt, parseString("to_timestamp(c0)", input));
    EXPECT_EQ(
        std::nullopt,
        parseString("flink_string_to_timestamp(c0, true)", input));
    BOLT_ASSERT_THROW(
        parseString("flink_string_to_timestamp(c0, false)", input),
        "Invalid timestamp literal");
  }
}

TEST_F(TemporalFunctionsTest, unicodeDigits) {
  for (const auto* expression :
       {"to_timestamp(c0)", "flink_string_to_timestamp(c0, true)"}) {
    SCOPED_TRACE(expression);
    for (const auto* input : {"１９７０-01-01", "1970-１-１", "١٩٧٠-1-1"}) {
      EXPECT_EQ(Timestamp(0, 0), parseString(expression, input));
    }
    EXPECT_EQ(
        Timestamp(0, 100000000),
        parseString(expression, "1970-01-01 00:00:00.١"));
    EXPECT_EQ(
        timestamp("2023-03-01 00:00:00"),
        parseString(expression, "２０２３-02-29"));
    EXPECT_EQ(std::nullopt, parseString(expression, "1970-01-01 00:00:00.𝟙"));
  }
}

TEST_F(TemporalFunctionsTest, defaultJvmTimeZone) {
  const auto expression =
      "flink_string_to_timestamp(c0, true, 'America/New_York', cast(-18000 as integer))";
  EXPECT_EQ(
      timestamp("2021-03-14 02:30:00.123456789"),
      parseString(expression, "2021-03-14 02:30:00.123456789"));
  for (const auto* text :
       {"2021-3-14 2:30:00.123456789", " 2021-03-14 02:30:00.123456789 "}) {
    EXPECT_EQ(
        timestamp("2021-03-14 03:30:00.123456789"),
        parseString(expression, text));
    EXPECT_EQ(
        timestamp("2021-03-14 02:30:00.123456789"),
        parseString("flink_string_to_timestamp(c0, true)", text));
  }
  EXPECT_EQ(
      timestamp("1844-12-31 12:00:00"),
      parseString(
          "flink_string_to_timestamp(c0, true, 'Asia/Manila', cast(28800 as integer))",
          " 1844-12-31 12:00:00 "));
  EXPECT_EQ(
      timestamp("2037-03-08 03:30:00.123456789"),
      parseString(expression, " 2037-03-08 02:30:00.123456789 "));
  EXPECT_EQ(std::nullopt, parseString(expression, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      parseString(
          "flink_string_to_timestamp(c0, true, cast(null as varchar), cast(0 as integer))",
          "1970-01-01"));
}

TEST_F(TemporalFunctionsTest, legacyTimeZoneBoundary) {
  EXPECT_EQ(
      timestamp("1900-01-01 08:05:43"),
      parseString(
          "flink_string_to_timestamp(c0, true, 'Asia/Shanghai', cast(28800 as integer))",
          " 1900-01-01 08:00:00 "));
  EXPECT_EQ(
      timestamp("1900-01-01 01:00:12"),
      parseString(
          "flink_string_to_timestamp(c0, true, 'Africa/Ndjamena', cast(3600 as integer))",
          " 1900-01-01 01:00:00 "));
  EXPECT_EQ(
      timestamp("2021-03-14 02:30:00"),
      parseString(
          "flink_string_to_timestamp(c0, true, 'America/New_York', cast(-18000 as integer))",
          "0000-01-01 -17705566:30:00"));
  EXPECT_EQ(
      timestamp("2021-01-01 00:00:00"),
      parseString(
          "flink_string_to_timestamp(c0, true, 'GMT+20:00', cast(72000 as integer))",
          "2021-01-01 00:00:00"));
  for (const auto* input :
       {"1970-01-01 00:00:00.෧", "1970-01-01 00:00:00.꧱"}) {
    EXPECT_EQ(
        std::nullopt,
        parseString("flink_string_to_timestamp(c0, true)", input));
  }
}

TEST_F(TemporalFunctionsTest, ltzTimeCast) {
  auto values = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp(-1, 999999999),
       Timestamp(-86400, 1000000),
       Timestamp(0, 123456789),
       std::nullopt})});
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({86399999, 1, 123, std::nullopt}),
      evaluate("flink_timestamp_to_time(c0, 'UTC')", values));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {28799999, 28800001, 28800123, std::nullopt}),
      evaluate("flink_timestamp_to_time(c0, '+08:00')", values));
  EXPECT_EQ(
      12600123,
      evaluateOnce<int32_t>(
          "flink_timestamp_to_time(c0, 'America/New_York')",
          std::optional<Timestamp>(
              timestamp("2021-03-14 07:30:00.123456789"))));
}

TEST_F(TemporalFunctionsTest, prefixedRegistration) {
  registerTemporalFunctions("test_");
  for (const auto* name :
       {"to_timestamp",
        "flink_string_to_timestamp",
        "flink_timestamp_precision",
        "flink_timestamp_to_utc",
        "flink_timestamp_from_utc",
        "flink_time_to_timestamp",
        "flink_timestamp_to_time"}) {
    EXPECT_FALSE(getSignatureStrings("test_" + std::string(name)).empty());
  }
  EXPECT_EQ(
      Timestamp(0, 123456789),
      parseString("test_to_timestamp(c0)", "1970-01-01 00:00:00.123456789"));
  EXPECT_EQ(
      Timestamp(0, 123000000),
      parseString(
          "test_to_timestamp(c0, 'yyyy/MM/dd HH:mm:ss.SSS')",
          "1970/01/01 00:00:00.123"));
  EXPECT_EQ(
      Timestamp(0, 123000000),
      parseString(
          "test_flink_timestamp_precision(test_flink_string_to_timestamp(c0, true), cast(3 as integer))",
          std::string("1970-01-01 00:00:00.123456789")));
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
