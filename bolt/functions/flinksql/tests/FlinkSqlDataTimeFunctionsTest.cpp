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
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
#include "bolt/type/HugeInt.h"

#include <cstdint>
#include <limits>
namespace bytedance::bolt::functions::flinksql::test {

namespace {
const int64_t LongMinValue = std::numeric_limits<int64_t>::min();

class FlinkSqlDateTimeFunctionsTest : public FlinkFunctionBaseTest {};

TEST_F(FlinkSqlDateTimeFunctionsTest, currentTimestamp) {
  const auto currentTimestampWithTimeZone =
      [&](const std::optional<StringView> timeZone) {
        return evaluateOnce<Timestamp>("current_timestamp(c0)", timeZone);
      };

  EXPECT_TRUE(currentTimestampWithTimeZone("Asia/Shanghai").has_value());
}

TEST_F(FlinkSqlDateTimeFunctionsTest, unixTimestamp) {
  const auto unixTimestamp = [&](const std::optional<std::string>& dateString,
                                 const std::optional<std::string>& format,
                                 const std::string& timeZone) {
    return evaluateOnce<int64_t>(
        fmt::format("flink_unix_timestamp(c0, c1, '{}')", timeZone),
        makeRowVector(
            {makeNullableFlatVector(
                 std::vector<std::optional<std::string>>{dateString}),
             makeNullableFlatVector(
                 std::vector<std::optional<std::string>>{format})}));
  };

  EXPECT_EQ(
      1747843200, unixTimestamp("2025-05-22", "yyyy-MM-dd", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481944,
      unixTimestamp(
          "1900-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481600,
      unixTimestamp(
          "1901-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  // daylight saving time
  EXPECT_EQ(
      675702000,
      unixTimestamp(
          "1991-06-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));

  EXPECT_EQ(
      0,
      unixTimestamp(
          "1970-01-01 08:00:00.000",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "Asia/Shanghai"));
  EXPECT_EQ(
      -28800,
      unixTimestamp(
          "1970-01-01 00:00:00.000",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "Asia/Shanghai"));
  EXPECT_EQ(
      -62135625943,
      unixTimestamp(
          "0001-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481600,
      unixTimestamp(
          "1901-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
  EXPECT_EQ(
      -2177481944,
      unixTimestamp(
          "1900-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));

  const std::string ISO_8601_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSZZ";
  std::string isoTime = "1970-01-01 05:30:01.00+05:30";
  EXPECT_EQ(1, unixTimestamp(isoTime, ISO_8601_DATE_FORMAT, "Asia/Shanghai"));

  // Empty or malformed input returns null.
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp(
          std::nullopt, "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp("2025-05-22", std::nullopt, "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp(
          "1970-01-01", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp("00:00:00", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp("", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      LongMinValue,
      unixTimestamp(
          "malformed input", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));

  EXPECT_EQ(
      0,
      unixTimestamp(
          "1970-01-01 05:30:00", "yyyy-MM-dd HH:mm:ss", "Asia/Kolkata"));
  EXPECT_EQ(
      5 * 3600,
      unixTimestamp(
          "1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "America/Toronto"));
  EXPECT_EQ(
      0, unixTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "UTC"));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, fromUnixtimeWithTimezone) {
  const auto fromUnixtimeOnce = [&](std::optional<int64_t> unixtime,
                                    const std::string& format,
                                    const std::string& timezone) {
    return evaluateOnce<std::string>(
        fmt::format("flink_from_unixtime(c0, '{}', '{}')", format, timezone),
        unixtime);
  };
  EXPECT_EQ(
      "20201228", fromUnixtimeOnce(1609167953, "yyyyMMdd", "Asia/Shanghai"));
  EXPECT_EQ(
      "1970-01-02 11:46:40.000",
      fromUnixtimeOnce(100000, "yyyy-MM-dd HH:mm:ss.SSS", "Asia/Shanghai"));

  EXPECT_EQ(
      "2025-09-11 18:00:00",
      fromUnixtimeOnce(1757615001, "yyyy-MM-dd HH:00:00", "UTC"));

  EXPECT_EQ(
      "2025-09-11 18:00:00",
      fromUnixtimeOnce(1757615001, "yyyy-MM-dd HH:00:00", "Africa/Abidjan"));

  EXPECT_EQ(
      "1970-01-01 08:00:00",
      fromUnixtimeOnce(LongMinValue, "yyyy-MM-dd HH:mm:ss", "Asia/Shanghai"));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, timestampToStringV2) {
  const auto formatOnce = [&](Timestamp timestamp, int32_t precision) {
    return evaluateOnce<std::string>(
        "flink_timestamp_to_string_v2(c0, c1)",
        makeRowVector(
            {makeFlatVector<Timestamp>({timestamp}),
             makeFlatVector<int32_t>({precision})}));
  };

  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56"),
      formatOnce(Timestamp(946729316, 0), 0));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.123"),
      formatOnce(Timestamp(946729316, 123000000), 3));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.123456789"),
      formatOnce(Timestamp(946729316, 123456789), 3));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.000000123"),
      formatOnce(Timestamp(946729316, 123), 9));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.000000123"),
      formatOnce(Timestamp(946729316, 123), 12));
  EXPECT_EQ(
      std::optional<std::string>("2000-01-01 12:21:56.1"),
      formatOnce(Timestamp(946729316, 100000000), -1));
}
TEST_F(FlinkSqlDateTimeFunctionsTest, timestampdiff) {
  const auto timestampdiff = [&](const std::string& unit,
                                 std::optional<Timestamp> start,
                                 std::optional<Timestamp> end) {
    return evaluateOnce<int32_t>(
        fmt::format("timestampdiff('{}', c0, c1)", unit), start, end);
  };

  const auto parseTimestamp =
      [&](const std::string& str) -> std::optional<Timestamp> {
    bool isNull{false};
    auto result = util::fromTimestampString(str.data(), str.size(), &isNull);
    return isNull ? std::nullopt : std::optional<Timestamp>(result);
  };

  const auto parseDate = [&](const std::string& str) {
    return parseTimestamp(str + " 00:00:00");
  };

  // Fixed-ratio units. These also cover the registered timestamp/timestamp,
  // date/timestamp, timestamp/date, and date/date argument combinations.
  EXPECT_EQ(
      2,
      timestampdiff(
          "day",
          parseTimestamp("2018-07-03 11:11:11"),
          parseTimestamp("2018-07-05 11:11:11")));
  EXPECT_EQ(
      35,
      timestampdiff(
          "hour",
          parseDate("2016-06-15"),
          parseTimestamp("2016-06-16 11:11:11")));
  EXPECT_EQ(
      85,
      timestampdiff(
          "hour",
          parseTimestamp("2016-06-15 11:00:00"),
          parseDate("2016-06-19")));
  EXPECT_EQ(
      -72,
      timestampdiff("hour", parseDate("2016-06-15"), parseDate("2016-06-12")));
  EXPECT_EQ(
      2,
      timestampdiff(
          "minute",
          parseTimestamp("2023-12-31 23:59:00"),
          parseTimestamp("2024-01-01 00:01:00")));
  EXPECT_EQ(
      259200,
      timestampdiff(
          "second", parseDate("2016-06-15"), parseDate("2016-06-18")));
  EXPECT_EQ(
      8,
      timestampdiff(
          "week",
          parseTimestamp("2018-05-03 11:11:11"),
          parseTimestamp("2018-07-03 11:12:12")));
  EXPECT_EQ(
      1000, timestampdiff("millisecond", Timestamp(0, 0), Timestamp(1, 0)));

  // Month-like units use Flink/Calcite whole-month semantics, then divide for
  // quarter/year.
  EXPECT_EQ(
      2,
      timestampdiff(
          "month",
          parseTimestamp("2018-07-03 11:11:11"),
          parseTimestamp("2018-09-05 11:11:11")));
  EXPECT_EQ(
      2,
      timestampdiff(
          "quarter",
          parseTimestamp("2018-01-03 11:11:11"),
          parseTimestamp("2018-09-05 11:11:11")));
  EXPECT_EQ(
      1,
      timestampdiff(
          "year",
          parseTimestamp("2018-07-03 11:11:11"),
          parseTimestamp("2019-07-05 11:11:11")));

  // null inputs
  EXPECT_EQ(
      std::nullopt,
      timestampdiff(
          "day", std::nullopt, parseTimestamp("2016-02-24 12:42:25")));

  // case-insensitive unit (Flink emits upper-case keywords)
  EXPECT_EQ(
      2,
      timestampdiff(
          "DAY",
          parseTimestamp("2018-07-03 11:11:11"),
          parseTimestamp("2018-07-05 11:11:11")));

  // Calcite/Flink end-of-month semantics: the clamped month boundary lands on
  // the target day, so the time-of-day decides whether a full month elapsed.
  EXPECT_EQ(
      0,
      timestampdiff(
          "month",
          parseTimestamp("2021-01-31 23:00:00"),
          parseTimestamp("2021-02-28 12:00:00")));
  EXPECT_EQ(
      1,
      timestampdiff(
          "month",
          parseTimestamp("2021-01-31 11:00:00"),
          parseTimestamp("2021-02-28 12:00:00")));
  EXPECT_EQ(
      1,
      timestampdiff("month", parseDate("2021-01-31"), parseDate("2021-02-28")));
  EXPECT_EQ(
      0,
      timestampdiff(
          "year",
          parseTimestamp("2020-02-29 23:00:00"),
          parseTimestamp("2021-02-28 12:00:00")));

  // int32 overflow wraps like Flink's (int) cast (2^31 seconds -> INT_MIN).
  EXPECT_EQ(
      std::numeric_limits<int32_t>::min(),
      timestampdiff("second", Timestamp(0, 0), Timestamp(2147483648LL, 0)));

  // Non-constant unit column exercises the per-row resolveUnit path.
  {
    auto unitVec = makeFlatVector<std::string>({"day", "hour", "month"});
    auto startVec = makeFlatVector<Timestamp>({
        *parseTimestamp("2018-07-03 11:11:11"),
        *parseTimestamp("2018-07-03 11:11:11"),
        *parseTimestamp("2018-01-03 11:11:11"),
    });
    auto endVec = makeFlatVector<Timestamp>({
        *parseTimestamp("2018-07-05 11:11:11"),
        *parseTimestamp("2018-07-04 12:12:11"),
        *parseTimestamp("2018-09-05 11:11:11"),
    });
    auto result = evaluate<SimpleVector<int32_t>>(
        "timestampdiff(c0, c1, c2)",
        makeRowVector({unitVec, startVec, endVec}));
    EXPECT_EQ(2, result->valueAt(0));
    EXPECT_EQ(25, result->valueAt(1));
    EXPECT_EQ(8, result->valueAt(2));
  }

  // invalid unit returns SQL NULL
  EXPECT_EQ(
      std::nullopt,
      timestampdiff(
          "invalid",
          parseTimestamp("2018-07-03 11:11:11"),
          parseTimestamp("2018-07-05 11:11:11")));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, toTimestamp) {
  const auto toTimestamp1 = [&](std::optional<std::string> str) {
    return evaluateOnce<Timestamp>("to_timestamp(c0)", str);
  };
  const auto toTimestamp2 = [&](std::optional<std::string> str,
                                std::optional<std::string> fmt) {
    return evaluateOnce<Timestamp>("to_timestamp(c0, c1)", str, fmt);
  };

  // Single arg: date only (2016-12-31 00:00:00)
  auto ts = toTimestamp1("2016-12-31");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(1483142400, ts->getSeconds());

  // Single arg: datetime without fractional seconds
  ts = toTimestamp1("2016-12-31 00:12:00");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(1483143120, ts->getSeconds());

  // Single arg: fractional seconds (auto-detected)
  ts = toTimestamp1("2016-12-31 00:12:00.123");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(1483143120, ts->getSeconds());
  EXPECT_EQ(123000000, ts->getNanos());

  // Flink falls back to Timestamp.valueOf/Date.valueOf for non-zero-padded
  // month and day fields.
  ts = toTimestamp1("1999-9-10 05:20:10");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(936940810, ts->getSeconds());

  ts = toTimestamp1("1999-9-10");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(936921600, ts->getSeconds());

  // Two args: date-only format
  ts = toTimestamp2("2016-12-31", "yyyy-MM-dd");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(1483142400, ts->getSeconds());

  ts = toTimestamp2("1999-9-10", "yyyy-MM-dd");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(936921600, ts->getSeconds());

  // Two args: datetime with millisecond format
  ts = toTimestamp2("2016-12-31 00:12:00.123", "yyyy-MM-dd HH:mm:ss.SSS");
  ASSERT_TRUE(ts.has_value());
  EXPECT_EQ(1483143120, ts->getSeconds());
  EXPECT_EQ(123000000, ts->getNanos());

  // Two args: non-constant format column must use each row's own format,
  // not the first row's.
  {
    auto strVec = makeFlatVector<std::string>(
        {"2016-12-31", "2016/12/31 01:02:03", "31-12-2016"});
    auto fmtVec = makeFlatVector<std::string>(
        {"yyyy-MM-dd", "yyyy/MM/dd HH:mm:ss", "dd-MM-yyyy"});
    auto result = evaluate<SimpleVector<Timestamp>>(
        "to_timestamp(c0, c1)", makeRowVector({strVec, fmtVec}));
    EXPECT_EQ(1483142400, result->valueAt(0).getSeconds());
    EXPECT_EQ(1483146123, result->valueAt(1).getSeconds());
    EXPECT_EQ(1483142400, result->valueAt(2).getSeconds());
  }

  // null inputs
  EXPECT_EQ(std::nullopt, toTimestamp1(std::nullopt));
  EXPECT_EQ(std::nullopt, toTimestamp2("2016-12-31", std::nullopt));

  // invalid input returns null
  EXPECT_EQ(std::nullopt, toTimestamp1("not_a_date"));
  EXPECT_EQ(std::nullopt, toTimestamp1("2016-12-31 trailing"));
  EXPECT_EQ(std::nullopt, toTimestamp1("2016-12-31 00:12:00 trailing"));
  EXPECT_EQ(std::nullopt, toTimestamp2("2016-12-31 trailing", "yyyy-MM-dd"));
}

TEST_F(FlinkSqlDateTimeFunctionsTest, hashCode) {
  const auto hashBool = [&](std::optional<bool> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashInt = [&](std::optional<int32_t> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashBigint = [&](std::optional<int64_t> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashFloat = [&](std::optional<float> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashDouble = [&](std::optional<double> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashStr = [&](std::optional<std::string> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashTinyint = [&](std::optional<int8_t> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashSmallint = [&](std::optional<int16_t> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };
  const auto hashTs = [&](std::optional<Timestamp> v) {
    return evaluateOnce<int32_t>("hash_code(c0)", v);
  };

  // Boolean: Java Boolean.hashCode
  EXPECT_EQ(1231, hashBool(true));
  EXPECT_EQ(1237, hashBool(false));

  // Tinyint/Smallint: Java Byte/Short.hashCode (identity)
  EXPECT_EQ(42, hashTinyint(static_cast<int8_t>(42)));
  EXPECT_EQ(-1, hashTinyint(static_cast<int8_t>(-1)));
  EXPECT_EQ(42, hashSmallint(static_cast<int16_t>(42)));
  EXPECT_EQ(-1, hashSmallint(static_cast<int16_t>(-1)));

  // Integer: identity
  EXPECT_EQ(0, hashInt(0));
  EXPECT_EQ(42, hashInt(42));
  EXPECT_EQ(-1, hashInt(-1));

  // Bigint: Java Long.hashCode == (int)(v ^ (v >>> 32)).
  EXPECT_EQ(42, hashBigint(42));
  EXPECT_EQ(0, hashBigint(-1)); // (-1 ^ -1) low 32 bits == 0
  EXPECT_EQ(1, hashBigint(1L << 32)); // 0x1_00000000 -> high^low == 1

  // Float: Java Float.hashCode == floatToIntBits.
  EXPECT_EQ(0, hashFloat(0.0f));
  EXPECT_EQ(1109917696, hashFloat(42.0f)); // floatToIntBits(42.0f)==0x42280000
  // -0.0f keeps its sign bit (NOT normalized to 0).
  EXPECT_EQ(std::numeric_limits<int32_t>::min(), hashFloat(-0.0f));
  // All NaN payloads canonicalize to 0x7fc00000.
  EXPECT_EQ(2143289344, hashFloat(std::numeric_limits<float>::quiet_NaN()));

  // Double: Java Double.hashCode == (int)(bits ^ (bits >>> 32)).
  EXPECT_EQ(0, hashDouble(0.0));
  EXPECT_EQ(1078263808, hashDouble(42.0)); // 0x40450000
  EXPECT_EQ(std::numeric_limits<int32_t>::min(), hashDouble(-0.0));
  EXPECT_EQ(2146959360, hashDouble(std::numeric_limits<double>::quiet_NaN()));

  // Varchar: Flink uses Math.abs(Java String.hashCode) over UTF-16 code units.
  EXPECT_EQ(0, hashStr(""));
  EXPECT_EQ(97, hashStr("a"));
  EXPECT_EQ(99162322, hashStr("hello"));
  EXPECT_EQ(685785664, hashStr("zzzzzz")); // Java hash is negative.
  EXPECT_EQ(
      std::numeric_limits<int32_t>::min(),
      hashStr("polygenelubricants")); // Math.abs(Integer.MIN_VALUE)
  // Non-ASCII: a single BMP code unit, not its UTF-8 bytes.
  EXPECT_EQ(233, hashStr("é")); // "é" -> 0x00E9
  EXPECT_EQ(20013, hashStr("中")); // "中" -> 0x4E2D
  // Supplementary code point -> UTF-16 surrogate pair.
  EXPECT_EQ(1772899, hashStr("\U0001F600")); // "😀"

  // Timestamp: Flink TimestampData.hashCode (millis hash plus nanoOfMillis).
  EXPECT_EQ(0, hashTs(Timestamp(0, 0)));
  EXPECT_EQ(531000, hashTs(Timestamp(1, 500000)));
  // Sub-millisecond nanos affect the hash.
  EXPECT_NE(
      hashTs(Timestamp(946729316, 123000000)),
      hashTs(Timestamp(946729316, 123000456)));

  // Date: hash_code(DATE) resolves (own registration) and is the identity of
  // days-since-epoch.
  {
    auto dateVec = makeFlatVector<int32_t>({0, 100, -5}, DATE());
    auto result = evaluate<SimpleVector<int32_t>>(
        "hash_code(c0)", makeRowVector({dateVec}));
    EXPECT_EQ(0, result->valueAt(0));
    EXPECT_EQ(100, result->valueAt(1));
    EXPECT_EQ(-5, result->valueAt(2));
  }

  // Varbinary: Flink uses MurmurHashUtil.hashUnsafeBytes with seed 42.
  {
    auto binaryVec = makeFlatVector<std::string>(
        {"", "a", "abc", std::string("\0\xff\x10", 3)}, VARBINARY());
    auto result = evaluate<SimpleVector<int32_t>>(
        "hash_code(c0)", makeRowVector({binaryVec}));
    EXPECT_EQ(142593372, result->valueAt(0));
    EXPECT_EQ(1485273170, result->valueAt(1));
    EXPECT_EQ(1322437556, result->valueAt(2));
    EXPECT_EQ(-339173231, result->valueAt(3));
  }

  // Decimal: Flink delegates to java.math.BigDecimal.hashCode().
  {
    auto shortDecimalVec =
        makeFlatVector<int64_t>({12345, -12345, 0}, DECIMAL(10, 2));
    auto shortResult = evaluate<SimpleVector<int32_t>>(
        "hash_code(c0)", makeRowVector({shortDecimalVec}));
    EXPECT_EQ(382697, shortResult->valueAt(0));
    EXPECT_EQ(-382693, shortResult->valueAt(1));
    EXPECT_EQ(2, shortResult->valueAt(2));

    auto longDecimalVec = makeFlatVector<int128_t>(
        {HugeInt::parse("123456789012345678901"),
         HugeInt::parse("-123456789012345678901")},
        DECIMAL(21, 2));
    auto longResult = evaluate<SimpleVector<int32_t>>(
        "hash_code(c0)", makeRowVector({longDecimalVec}));
    EXPECT_EQ(1337890792, longResult->valueAt(0));
    EXPECT_EQ(-1337890788, longResult->valueAt(1));
  }

  // null returns null
  EXPECT_EQ(std::nullopt, hashInt(std::nullopt));
  EXPECT_EQ(std::nullopt, hashStr(std::nullopt));
}

} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
