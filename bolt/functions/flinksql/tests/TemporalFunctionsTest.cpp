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
      for (const auto* function :
           {"flink_timestamp_to_utc", "flink_timestamp_to_utc_later_offset"}) {
        EXPECT_EQ(
            Timestamp(seconds - offset, 123456789),
            evaluateTimestamp(
                fmt::format("{}(c0, '{}')", function, zone),
                Timestamp(seconds, 123456789)));
      }
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

TEST_F(TemporalFunctionsTest, laterOverlapOffset) {
  struct Case {
    std::string zone;
    std::string local;
    std::string utc;
  };
  const std::vector<Case> cases = {
      {"America/New_York", "2024-11-03 01:30:00", "2024-11-03 06:30:00"},
      {"America/New_York", "2024-03-10 02:30:00", "2024-03-10 07:30:00"},
      {"America/New_York", "2024-01-01 12:00:00", "2024-01-01 17:00:00"},
      {"Europe/Berlin", "2024-10-27 02:30:00", "2024-10-27 01:30:00"},
      {"Australia/Lord_Howe", "2024-04-07 01:45:00", "2024-04-06 15:15:00"},
      {"Australia/Lord_Howe", "2024-10-06 02:15:00", "2024-10-05 15:45:00"}};
  for (const auto& c : cases) {
    SCOPED_TRACE(c.zone + " " + c.local);
    EXPECT_EQ(
        timestamp(c.utc + ".123456789"),
        evaluateTimestamp(
            fmt::format(
                "flink_timestamp_to_utc_later_offset(c0, '{}')", c.zone),
            timestamp(c.local + ".123456789")));
  }
  EXPECT_EQ(
      std::nullopt,
      evaluateTimestamp(
          "flink_timestamp_to_utc_later_offset(c0, 'America/New_York')",
          std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateTimestamp(
          "flink_timestamp_to_utc_later_offset(c0, cast(null as varchar))",
          Timestamp(0, 1)));
}

TEST_F(TemporalFunctionsTest, localDateBeforeEpoch) {
  const auto input = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 0),
       Timestamp(-1, 999999999),
       Timestamp(-86400, 0),
       Timestamp(-86401, 999999999),
       Timestamp(86400, 1),
       std::nullopt})});
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({0, -1, -1, -2, 1, std::nullopt}, DATE()),
      evaluate("flink_timestamp_to_local_date(c0)", input));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({0, 0, -1, -1, 1, std::nullopt}, DATE()),
      evaluate(
          "flink_timestamp_to_local_date(flink_timestamp_from_utc(c0, '+08:00'))",
          input));
}

TEST_F(TemporalFunctionsTest, localDateUsesFlinkCalendar) {
  const auto input = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp(-219946838400LL, 0),
       Timestamp(-219946752001LL, 999999999),
       Timestamp(-213667027200LL, 0),
       Timestamp(-213635577600LL, 0),
       Timestamp(-213635491200LL, 0),
       Timestamp(-213635404800LL, 0),
       Timestamp(-213608966400LL, 0),
       Timestamp::minMillis(),
       Timestamp::maxMillis(),
       std::nullopt})});
  // Expected day counts come from Flink's timestampWithLocalZoneToDate in UTC.
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {-2545680,
           -2545680,
           -2472997,
           -2472633,
           -2472632,
           -2472632,
           -2472326,
           622191233,
           -622191233,
           std::nullopt},
          DATE()),
      evaluate("flink_timestamp_to_local_date(c0)", input));
}

TEST_F(TemporalFunctionsTest, timeExtractionPreservesSign) {
  const auto input = makeRowVector({makeNullableFlatVector<int32_t>(
      {std::nullopt,
       std::numeric_limits<int32_t>::min(),
       -3726789,
       -1001,
       -1,
       0,
       45296789,
       std::numeric_limits<int32_t>::max()})});
  struct Case {
    int32_t modulus;
    int32_t divisor;
    std::vector<std::optional<int32_t>> expected;
  };
  const std::vector<Case> cases{
      {86400000, 3600000, {std::nullopt, -20, -1, 0, 0, 0, 12, 20}},
      {3600000, 60000, {std::nullopt, -31, -2, 0, 0, 0, 34, 31}},
      {60000, 1000, {std::nullopt, -23, -6, -1, 0, 0, 56, 23}},
      {1000, 1, {std::nullopt, -648, -789, -1, -1, 0, 789, 647}}};
  for (const auto& c : cases) {
    SCOPED_TRACE(c.divisor);
    assertEqualVectors(
        makeNullableFlatVector<int32_t>(c.expected),
        evaluate(
            fmt::format(
                "flink_time_extract(c0, cast({} as integer), cast({} as integer))",
                c.modulus,
                c.divisor),
            input));
  }
  for (const auto* expression :
       {"flink_time_extract(c0, cast(0 as integer), cast(1 as integer))",
        "flink_time_extract(c0, cast(1000 as integer), cast(0 as integer))"}) {
    EXPECT_THROW(evaluate(expression, input), BoltUserError);
  }
}

TEST_F(TemporalFunctionsTest, floatingSecondsToTimestamp) {
  const auto expected = makeNullableFlatVector<Timestamp>(
      {Timestamp(1, 789000000),
       Timestamp(-2, 211000000),
       Timestamp(0, 0),
       Timestamp(0, 0),
       Timestamp(0, 0),
       Timestamp::fromMillis(std::numeric_limits<int64_t>::max()),
       Timestamp::fromMillis(std::numeric_limits<int64_t>::min()),
       std::nullopt});
  const std::vector<VectorPtr> inputs{
      makeNullableFlatVector<float>(
          {1.789f,
           -1.789f,
           0.0009f,
           -0.0009f,
           std::numeric_limits<float>::quiet_NaN(),
           std::numeric_limits<float>::infinity(),
           -std::numeric_limits<float>::infinity(),
           std::nullopt}),
      makeNullableFlatVector<double>(
          {1.789,
           -1.789,
           0.0009,
           -0.0009,
           std::numeric_limits<double>::quiet_NaN(),
           std::numeric_limits<double>::infinity(),
           -std::numeric_limits<double>::infinity(),
           std::nullopt})};
  const std::array<const char*, 2> functions{
      "flink_float_seconds_to_timestamp", "flink_double_seconds_to_timestamp"};
  for (size_t i = 0; i < inputs.size(); ++i) {
    assertEqualVectors(
        expected,
        evaluate(
            fmt::format("{}(c0)", functions[i]), makeRowVector({inputs[i]})));
  }
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
       {"flink_timestamp_to_utc",
        "flink_timestamp_to_utc_later_offset",
        "flink_timestamp_from_utc"}) {
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

TEST_F(TemporalFunctionsTest, timestampNumericCasts) {
  auto values = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp(1, 789123456),
       Timestamp(-2, 211987654),
       Timestamp(-1, 999999999),
       Timestamp(0, 999999),
       std::nullopt})});
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({1, -1, 0, 0, std::nullopt}),
      evaluate("flink_timestamp_to_long_seconds(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({1, -1, 0, 0, std::nullopt}),
      evaluate("flink_timestamp_to_int_seconds(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int16_t>({1, -1, 0, 0, std::nullopt}),
      evaluate("flink_timestamp_to_smallint_seconds(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int8_t>({1, -1, 0, 0, std::nullopt}),
      evaluate("flink_timestamp_to_tinyint_seconds(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<float>(
          {1.789f, -1.789f, -0.001f, 0, std::nullopt}),
      evaluate("flink_timestamp_to_float_seconds(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<double>({1.789, -1.789, -0.001, 0, std::nullopt}),
      evaluate("flink_timestamp_to_double_seconds(c0)", values));

  auto indices = makeIndices(5, [](auto row) { return 4 - row; });
  auto dictionary =
      makeRowVector({wrapInDictionary(indices, 5, values->childAt(0))});
  assertEqualVectors(
      makeNullableFlatVector<double>({std::nullopt, 0, -0.001, -1.789, 1.789}),
      evaluate("flink_timestamp_to_double_seconds(c0)", dictionary));
  auto constant =
      makeRowVector({makeConstant<Timestamp>(Timestamp(-1, 999999999), 5)});
  assertEqualVectors(
      makeFlatVector<int64_t>({0, 0, 0, 0, 0}),
      evaluate("flink_timestamp_to_long_seconds(c0)", constant));
}

TEST_F(TemporalFunctionsTest, timestampNumericNarrowing) {
  const auto values = makeRowVector({makeFlatVector<Timestamp>(
      {Timestamp(128, 0),
       Timestamp(32768, 0),
       Timestamp(2147483648LL, 0),
       Timestamp(-2147483649LL, 999999999)})});
  assertEqualVectors(
      makeFlatVector<int8_t>({-128, 0, 0, 0}),
      evaluate("flink_timestamp_to_tinyint_seconds(c0)", values));
  assertEqualVectors(
      makeFlatVector<int16_t>({128, -32768, 0, 0}),
      evaluate("flink_timestamp_to_smallint_seconds(c0)", values));
  assertEqualVectors(
      makeFlatVector<int32_t>({128, 32768, INT32_MIN, INT32_MIN}),
      evaluate("flink_timestamp_to_int_seconds(c0)", values));

  const auto limits = makeRowVector({makeFlatVector<Timestamp>(
      {Timestamp::minMillis(), Timestamp::maxMillis()})});
  assertEqualVectors(
      makeFlatVector<int64_t>({-9223372036854775LL, 9223372036854775LL}),
      evaluate("flink_timestamp_to_long_seconds(c0)", limits));
  BOLT_ASSERT_THROW(
      evaluateOnce<int64_t>(
          "flink_timestamp_to_long_seconds(c0)",
          std::optional<Timestamp>(Timestamp::min())),
      "to milliseconds");
}

TEST_F(TemporalFunctionsTest, integerSecondsToTimestamp) {
  const auto expected = makeNullableFlatVector<Timestamp>(
      {Timestamp(-100, 0), Timestamp(0, 0), Timestamp(100, 0), std::nullopt});
  const std::vector<std::pair<std::string, VectorPtr>> inputs{
      {"long", makeNullableFlatVector<int64_t>({-100, 0, 100, std::nullopt})},
      {"int", makeNullableFlatVector<int32_t>({-100, 0, 100, std::nullopt})},
      {"short", makeNullableFlatVector<int16_t>({-100, 0, 100, std::nullopt})},
      {"byte", makeNullableFlatVector<int8_t>({-100, 0, 100, std::nullopt})}};
  for (const auto& [name, values] : inputs) {
    SCOPED_TRACE(name);
    assertEqualVectors(
        expected,
        evaluate(
            "flink_" + name + "_seconds_to_timestamp(c0)",
            makeRowVector({values})));
  }
  const auto values = makeRowVector({makeFlatVector<int64_t>(
      {INT64_MIN,
       INT64_MAX,
       9223372036854775LL,
       9223372036854776LL,
       -9223372036854776LL})});
  assertEqualVectors(
      makeFlatVector<Timestamp>(
          {Timestamp(0, 0),
           Timestamp(-1, 0),
           Timestamp::fromMillis(9223372036854775000LL),
           Timestamp::fromMillis(-9223372036854775616LL),
           Timestamp::fromMillis(9223372036854775616LL)}),
      evaluate("flink_long_seconds_to_timestamp(c0)", values));
}

TEST_F(TemporalFunctionsTest, timestampToNtzDate) {
  auto values = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp(-86401, 999999999),
       Timestamp(-86400, 0),
       Timestamp(-1, 999999999),
       Timestamp(86400, 123456789),
       std::nullopt})});
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({-1, -1, 0, 1, std::nullopt}, DATE()),
      evaluate("flink_timestamp_to_date(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>({-2, -1, -1, 1, std::nullopt}, DATE()),
      evaluate("flink_timestamp_to_local_date(c0)", values));
}

TEST_F(TemporalFunctionsTest, timestampToLongZones) {
  auto timestamps = makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 123456789),
       Timestamp(0, 123456789),
       Timestamp(-1, 999999999),
       std::nullopt});
  auto values = makeRowVector({timestamps});
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({123, 123, -1, std::nullopt}),
      evaluate("timestamp_to_long(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {-28799877, -28799877, -28800001, std::nullopt}),
      evaluate("timestamp_to_long(c0, 'Asia/Shanghai')", values));
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({123, -28799877, 17999999, std::nullopt}),
      evaluate(
          "timestamp_to_long(c0, c1)",
          makeRowVector(
              {timestamps,
               makeNullableFlatVector<std::string>(
                   {"UTC",
                    "Asia/Shanghai",
                    "America/New_York",
                    std::nullopt})})));
  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<int64_t>(
          "timestamp_to_long(c0, cast(null as varchar))",
          std::optional<Timestamp>(Timestamp(0, 0))));

  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "Asia/Shanghai"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {-28799877, -28799877, -28800001, std::nullopt}),
      evaluate("timestamp_to_long(c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int64_t>({123, 123, -1, std::nullopt}),
      evaluate("timestamp_to_long(c0, 'UTC')", values));
  for (const auto* text : {"2024-11-03 01:30:00", "2024-03-10 02:30:00"}) {
    EXPECT_EQ(
        std::nullopt,
        evaluateOnce<int64_t>(
            "try(timestamp_to_long(c0, 'America/New_York'))",
            std::optional<Timestamp>(timestamp(text))));
  }
}

TEST_F(TemporalFunctionsTest, timestampToLongExplicitZoneOverridesSession) {
  auto timestamps = makeNullableFlatVector<Timestamp>(
      {Timestamp(0, 123456789),
       Timestamp(0, 123456789),
       Timestamp(-1, 999999999),
       std::nullopt});
  for (const auto* sessionZone : {"SystemV/EST5EDT", "Invalid/Session"}) {
    SCOPED_TRACE(sessionZone);
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, sessionZone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    assertEqualVectors(
        makeNullableFlatVector<int64_t>({123, 123, -1, std::nullopt}),
        evaluate("timestamp_to_long(c0, 'UTC')", makeRowVector({timestamps})));
    assertEqualVectors(
        makeNullableFlatVector<int64_t>(
            {123, -19799877, -28800001, std::nullopt}),
        evaluate(
            "timestamp_to_long(c0, c1)",
            makeRowVector(
                {timestamps,
                 makeFlatVector<std::string>(
                     {"UTC", "GMT+05:30", "Asia/Shanghai", "UTC"})})));
    EXPECT_EQ(
        std::nullopt,
        evaluateOnce<int64_t>(
            "timestamp_to_long(c0, cast(null as varchar))",
            std::optional<Timestamp>(Timestamp(0, 0))));
  }
  BOLT_ASSERT_THROW(
      evaluateOnce<int64_t>(
          "timestamp_to_long(c0)", std::optional<Timestamp>(Timestamp(0, 0))),
      "Unknown time zone: 'Invalid/Session'");
}

TEST_F(TemporalFunctionsTest, timestampToLongUtcRange) {
  const auto values = makeRowVector({makeNullableFlatVector<Timestamp>(
      {Timestamp::minMillis(),
       Timestamp(-1, 999999999),
       Timestamp(0, 999999),
       Timestamp::maxMillis(),
       std::nullopt})});
  const auto expected = makeNullableFlatVector<int64_t>(
      {INT64_MIN, -1, 0, INT64_MAX, std::nullopt});
  for (const auto* zone : {"UTC", "Etc/UTC", "GMT", "+00:00", "-00:00"}) {
    SCOPED_TRACE(zone);
    assertEqualVectors(
        expected,
        evaluate(fmt::format("timestamp_to_long(c0, '{}')", zone), values));
  }
  assertEqualVectors(
      expected,
      evaluate(
          "timestamp_to_long(c0, c1)",
          makeRowVector(
              {values->childAt(0),
               makeNullableFlatVector<std::string>(
                   {"UTC", "Etc/UTC", "+00:00", "GMT", std::nullopt})})));
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "UTC"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  assertEqualVectors(expected, evaluate("timestamp_to_long(c0)", values));

  for (const auto& input : {Timestamp::min(), Timestamp::max()}) {
    BOLT_ASSERT_THROW(
        evaluateOnce<int64_t>(
            "timestamp_to_long(c0, 'UTC')", std::optional<Timestamp>(input)),
        "to milliseconds");
    EXPECT_EQ(
        std::nullopt,
        evaluateOnce<int64_t>(
            "try(timestamp_to_long(c0, 'UTC'))",
            std::optional<Timestamp>(input)));
  }
}

TEST_F(TemporalFunctionsTest, timestampToLongFixedOffsetRange) {
  struct Case {
    Timestamp input;
    std::string zone;
    int64_t expected;
  };
  const std::vector<Case> cases{
      {Timestamp::maxMillis(), "+00:01", INT64_MAX - 60'000},
      {Timestamp::fromMillis(INT64_MIN + 60'000), "+00:01", INT64_MIN},
      {Timestamp::minMillis(), "-00:01", INT64_MIN + 60'000},
      {Timestamp::fromMillis(INT64_MAX - 60'000), "-00:01", INT64_MAX},
      {Timestamp::maxMillis(), "+14:00", INT64_MAX - 50'400'000},
      {Timestamp::minMillis(), "-14:00", INT64_MIN + 50'400'000},
      {Timestamp::maxMillis(), "Etc/GMT-1", INT64_MAX - 3'600'000},
      {Timestamp::minMillis(), "Etc/GMT+1", INT64_MIN + 3'600'000},
      {Timestamp::maxMillis(), "GMT+05:30", INT64_MAX - 19'800'000},
      {Timestamp::minMillis(), "UTC-03:30", INT64_MIN + 12'600'000},
      {Timestamp::maxMillis(), "+05:30:45", INT64_MAX - 19'845'000},
      {Timestamp(-1, 999999999), "+01:30", -5'400'001},
      {Timestamp::max(), "+00:01", INT64_MAX - 59'808},
      {Timestamp::min(), "-00:01", INT64_MIN + 59'808}};
  std::vector<Timestamp> inputs;
  std::vector<std::string> zones;
  std::vector<int64_t> expected;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.zone);
    EXPECT_EQ(
        c.expected,
        evaluateOnce<int64_t>(
            fmt::format("timestamp_to_long(c0, '{}')", c.zone),
            std::optional<Timestamp>(c.input)));
    inputs.push_back(c.input);
    zones.push_back(c.zone);
    expected.push_back(c.expected);
  }
  assertEqualVectors(
      makeFlatVector<int64_t>(expected),
      evaluate(
          "timestamp_to_long(c0, c1)",
          makeRowVector(
              {makeFlatVector<Timestamp>(inputs),
               makeFlatVector<std::string>(zones)})));
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "+00:01"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  EXPECT_EQ(
      INT64_MAX - 60'000,
      evaluateOnce<int64_t>(
          "timestamp_to_long(c0)",
          std::optional<Timestamp>(Timestamp::maxMillis())));
  for (const auto& c :
       {Case{Timestamp::maxMillis(), "-00:01", 0},
        Case{Timestamp::minMillis(), "+00:01", 0}}) {
    SCOPED_TRACE(c.zone);
    const auto expression = fmt::format("timestamp_to_long(c0, '{}')", c.zone);
    BOLT_ASSERT_THROW(
        evaluateOnce<int64_t>(expression, std::optional<Timestamp>(c.input)),
        "to milliseconds");
    EXPECT_EQ(
        std::nullopt,
        evaluateOnce<int64_t>(
            "try(" + expression + ")", std::optional<Timestamp>(c.input)));
  }
}

TEST_F(TemporalFunctionsTest, timestampToLongJavaFixedOffsets) {
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "America/New_York"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  struct Case {
    std::string zone;
    int64_t millis;
  };
  const std::vector<Case> cases{
      {"GMT+05:30", -19'800'000},
      {"GMT+00:45", -2'700'000},
      {"UTC-03:30", 12'600'000},
      {"UT+05:30:45", -19'845'000},
      {"+05:30:45", -19'845'000},
      {"+18:00", -64'800'000},
      {"-18:00", 64'800'000},
      {"Z", 0},
      {"UTC", 0},
      {"GMT", 0},
      {"UT", 0},
      {"Etc/GMT+1", 3'600'000},
      {"Asia/Shanghai", -28'800'000}};
  const Timestamp input(0, 123456789);
  std::vector<std::string> zones;
  std::vector<int64_t> expected;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.zone);
    EXPECT_EQ(
        c.millis + 123,
        evaluateOnce<int64_t>(
            fmt::format("timestamp_to_long(c0, '{}')", c.zone),
            std::optional<Timestamp>(input)));
    zones.push_back(c.zone);
    expected.push_back(c.millis + 123);
  }
  assertEqualVectors(
      makeFlatVector<int64_t>(expected),
      evaluate(
          "timestamp_to_long(c0, c1)",
          makeRowVector(
              {makeFlatVector<Timestamp>(
                   std::vector<Timestamp>(cases.size(), input)),
               makeFlatVector<std::string>(zones)})));
  for (const auto& c : cases) {
    SCOPED_TRACE(c.zone);
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, c.zone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
    EXPECT_EQ(
        c.millis + 123,
        evaluateOnce<int64_t>(
            "timestamp_to_long(c0)", std::optional<Timestamp>(input)));
  }
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "GMT+05:30"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "false"}});
  EXPECT_EQ(
      123,
      evaluateOnce<int64_t>(
          "timestamp_to_long(c0)", std::optional<Timestamp>(input)));
}

TEST_F(TemporalFunctionsTest, timestampToLongInvalidFixedOffsets) {
  const std::vector<std::string> zones{
      "+18:01", "-18:00:01", "GMT+00:60", "UTC+19:00"};
  for (const auto& zone : zones) {
    SCOPED_TRACE(zone);
    const auto expression = fmt::format("timestamp_to_long(c0, '{}')", zone);
    BOLT_ASSERT_THROW(
        evaluateOnce<int64_t>(expression, std::optional<Timestamp>({0, 0})),
        "Invalid time zone offset");
    EXPECT_EQ(
        std::nullopt,
        evaluateOnce<int64_t>(
            "try(" + expression + ")", std::optional<Timestamp>({0, 0})));
  }
  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {std::nullopt, std::nullopt, std::nullopt, std::nullopt}),
      evaluate(
          "try(timestamp_to_long(c0, c1))",
          makeRowVector(
              {makeFlatVector<Timestamp>(
                   std::vector<Timestamp>(zones.size(), Timestamp(0, 0))),
               makeFlatVector<std::string>(zones)})));
}

TEST_F(TemporalFunctionsTest, intervalArithmeticWraps) {
  const auto check = [&](const auto& left,
                         const auto& right,
                         const auto& added,
                         const auto& subtracted,
                         const auto& negated) {
    auto values = makeRowVector({left, right});
    assertEqualVectors(added, evaluate("add(c0, c1)", values));
    assertEqualVectors(subtracted, evaluate("subtract(c0, c1)", values));
    assertEqualVectors(negated, evaluate("negative(c0)", values));
  };
  check(
      makeNullableFlatVector<int32_t>(
          {14, INT32_MAX, INT32_MIN, std::nullopt}, INTERVAL_YEAR_MONTH()),
      makeNullableFlatVector<int32_t>({2, 1, 1, 0}, INTERVAL_YEAR_MONTH()),
      makeNullableFlatVector<int32_t>(
          {16, INT32_MIN, INT32_MIN + 1, std::nullopt}, INTERVAL_YEAR_MONTH()),
      makeNullableFlatVector<int32_t>(
          {12, INT32_MAX - 1, INT32_MAX, std::nullopt}, INTERVAL_YEAR_MONTH()),
      makeNullableFlatVector<int32_t>(
          {-14, -INT32_MAX, INT32_MIN, std::nullopt}, INTERVAL_YEAR_MONTH()));
  check(
      makeNullableFlatVector<int64_t>(
          {1001, INT64_MAX, INT64_MIN, std::nullopt}, INTERVAL_DAY_TIME()),
      makeNullableFlatVector<int64_t>({2, 1, 1, 0}, INTERVAL_DAY_TIME()),
      makeNullableFlatVector<int64_t>(
          {1003, INT64_MIN, INT64_MIN + 1, std::nullopt}, INTERVAL_DAY_TIME()),
      makeNullableFlatVector<int64_t>(
          {999, INT64_MAX - 1, INT64_MAX, std::nullopt}, INTERVAL_DAY_TIME()),
      makeNullableFlatVector<int64_t>(
          {-1001, -INT64_MAX, INT64_MIN, std::nullopt}, INTERVAL_DAY_TIME()));
}

TEST_F(TemporalFunctionsTest, intervalStringCasts) {
  assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"+0-00",
           "+1-02",
           "-1-02",
           "+178956970-07",
           "--178956970-0-8",
           std::nullopt}),
      evaluate(
          "flink_interval_year_month_to_string(c0)",
          makeRowVector({makeNullableFlatVector<int32_t>(
              {0, 14, -14, INT32_MAX, INT32_MIN, std::nullopt},
              INTERVAL_YEAR_MONTH())})));
  // These boundary strings follow Calcite's Java narrowing and negation rules.
  assertEqualVectors(
      makeNullableFlatVector<std::string>(
          {"+0 00:00:00.000",
           "+1 01:01:01.002",
           "-1 01:01:01.002",
           "+-622191233 07:12:55.807",
           "-622191233 0-7:-12:-55.-808",
           std::nullopt}),
      evaluate(
          "flink_interval_day_time_to_string(c0)",
          makeRowVector({makeNullableFlatVector<int64_t>(
              {0, 90061002, -90061002, INT64_MAX, INT64_MIN, std::nullopt},
              INTERVAL_DAY_TIME())})));
}

TEST_F(TemporalFunctionsTest, dateDayIntervalArithmetic) {
  auto values = makeRowVector(
      {makeNullableFlatVector<int32_t>(
           {0, 0, 0, 0, INT32_MAX, INT32_MIN, std::nullopt}, DATE()),
       makeNullableFlatVector<int64_t>(
           {86399999, -86399999, 86400001, -86400001, 86400000, 86400000, 1},
           INTERVAL_DAY_TIME())});
  auto added = makeNullableFlatVector<int32_t>(
      {0, 0, 1, -1, INT32_MIN, INT32_MIN + 1, std::nullopt}, DATE());
  assertEqualVectors(added, evaluate("add(c0, c1)", values));
  assertEqualVectors(added, evaluate("add(c1, c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {0, 0, -1, 1, INT32_MAX - 1, INT32_MAX, std::nullopt}, DATE()),
      evaluate("subtract(c0, c1)", values));
  auto overflow = makeRowVector(
      {makeFlatVector<int32_t>({0}, DATE()),
       makeFlatVector<int64_t>({185542587187200000LL}, INTERVAL_DAY_TIME())});
  for (const auto* expression :
       {"add(c0, c1)", "subtract(c0, c1)", "add(c1, c0)"}) {
    BOLT_ASSERT_THROW(
        evaluate(expression, overflow), "Day interval exceeds integer range");
    EXPECT_TRUE(evaluate("try(" + std::string(expression) + ")", overflow)
                    ->isNullAt(0));
  }
}

TEST_F(TemporalFunctionsTest, dateMonthIntervalArithmetic) {
  auto values = makeRowVector(
      {makeNullableFlatVector<int32_t>(
           {-336, -1, 18292, 18321, -719132, 2932896, std::nullopt}, DATE()),
       makeNullableFlatVector<int32_t>(
           {1, 1, 1, 12, 1, 1, 0}, INTERVAL_YEAR_MONTH())});
  auto added = makeNullableFlatVector<int32_t>(
      {-307, 30, 18321, 18686, -719104, 2932927, std::nullopt}, DATE());
  assertEqualVectors(added, evaluate("add(c0, c1)", values));
  assertEqualVectors(added, evaluate("add(c1, c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<int32_t>(
          {-367, -32, 18261, 17955, -719163, 2932865, std::nullopt}, DATE()),
      evaluate("subtract(c0, c1)", values));
  auto limits = makeRowVector(
      {makeFlatVector<int32_t>({18292, 18292}, DATE()),
       makeFlatVector<int32_t>({INT32_MIN, INT32_MAX}, INTERVAL_YEAR_MONTH())});
  assertEqualVectors(
      makeFlatVector<int32_t>({-938163627, -938163658}, DATE()),
      evaluate("add(c0, c1)", limits));
}

TEST_F(TemporalFunctionsTest, timestampDayIntervalArithmetic) {
  auto values = makeRowVector(
      {makeNullableFlatVector<Timestamp>(
           {Timestamp(0, 123456789),
            Timestamp(-1, 999999999),
            Timestamp::maxMillis(),
            Timestamp::minMillis(),
            std::nullopt}),
       makeNullableFlatVector<int64_t>(
           {1001, 1, 1, 1, 0}, INTERVAL_DAY_TIME())});
  auto added = makeNullableFlatVector<Timestamp>(
      {Timestamp(1, 124456789),
       Timestamp(0, 999999),
       Timestamp::minMillis(),
       Timestamp::fromMillis(INT64_MIN + 1),
       std::nullopt});
  assertEqualVectors(added, evaluate("add(c0, c1)", values));
  assertEqualVectors(added, evaluate("add(c1, c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<Timestamp>(
          {Timestamp(-1, 122456789),
           Timestamp(-1, 998999999),
           Timestamp::fromMillis(INT64_MAX - 1),
           Timestamp::maxMillis(),
           std::nullopt}),
      evaluate("subtract(c0, c1)", values));
}

TEST_F(TemporalFunctionsTest, timestampMonthIntervalArithmetic) {
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kSessionTimezone, "America/New_York"},
       {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  auto values = makeRowVector(
      {makeNullableFlatVector<Timestamp>(
           {timestamp("1969-01-30 23:59:59.123456789"),
            timestamp("1969-12-31 23:59:59.999999999"),
            timestamp("2020-01-31 02:30:00.123456789"),
            timestamp("2020-02-29 02:30:00.123456789"),
            std::nullopt}),
       makeNullableFlatVector<int32_t>(
           {1, 1, 1, 12, 0}, INTERVAL_YEAR_MONTH())});
  auto added = makeNullableFlatVector<Timestamp>(
      {timestamp("1969-02-28 23:59:59.123456789"),
       timestamp("1970-01-31 23:59:59.999999999"),
       timestamp("2020-02-29 02:30:00.123456789"),
       timestamp("2021-02-28 02:30:00.123456789"),
       std::nullopt});
  assertEqualVectors(added, evaluate("add(c0, c1)", values));
  assertEqualVectors(added, evaluate("add(c1, c0)", values));
  assertEqualVectors(
      makeNullableFlatVector<Timestamp>(
          {timestamp("1968-12-30 23:59:59.123456789"),
           timestamp("1969-11-30 23:59:59.999999999"),
           timestamp("2019-12-31 02:30:00.123456789"),
           timestamp("2019-02-28 02:30:00.123456789"),
           std::nullopt}),
      evaluate("subtract(c0, c1)", values));
  auto limits = makeRowVector(
      {makeFlatVector<Timestamp>(
           {Timestamp::minMillis(), Timestamp::maxMillis()}),
       makeFlatVector<int32_t>({1, 1}, INTERVAL_YEAR_MONTH())});
  assertEqualVectors(
      makeFlatVector<Timestamp>(
          {Timestamp::fromMillis(-53757319273975808LL),
           Timestamp::fromMillis(-53757319222024193LL)}),
      evaluate("add(c0, c1)", limits));
}

TEST_F(TemporalFunctionsTest, prefixedRegistration) {
  registerTemporalFunctions("test_");
  for (const auto* name :
       {"to_timestamp",
        "flink_string_to_timestamp",
        "flink_timestamp_precision",
        "flink_timestamp_to_utc",
        "flink_timestamp_to_utc_later_offset",
        "flink_timestamp_from_utc",
        "flink_float_seconds_to_timestamp",
        "flink_double_seconds_to_timestamp",
        "flink_time_to_timestamp",
        "flink_timestamp_to_local_date",
        "flink_time_extract",
        "flink_timestamp_to_time",
        "timestamp_to_long",
        "flink_timestamp_to_long_seconds",
        "flink_timestamp_to_int_seconds",
        "flink_timestamp_to_smallint_seconds",
        "flink_timestamp_to_tinyint_seconds",
        "flink_timestamp_to_float_seconds",
        "flink_timestamp_to_double_seconds",
        "flink_long_seconds_to_timestamp",
        "flink_int_seconds_to_timestamp",
        "flink_short_seconds_to_timestamp",
        "flink_byte_seconds_to_timestamp",
        "flink_timestamp_to_date",
        "flink_interval_year_month_to_string",
        "flink_interval_day_time_to_string"}) {
    EXPECT_FALSE(getSignatureStrings("test_" + std::string(name)).empty());
  }
  EXPECT_EQ(10, getSignatureStrings("test_add").size());
  EXPECT_EQ(6, getSignatureStrings("test_subtract").size());
  EXPECT_EQ(2, getSignatureStrings("test_negative").size());
  EXPECT_EQ(2, getSignatureStrings("test_timestamp_to_long").size());
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
