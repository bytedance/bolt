/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include <common/base/BoltException.h>
#include <fmt/core.h>
#include <type/Type.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {

class SparkSqlDateTimeFunctionsTest : public SparkFunctionBaseTest {
 public:
  static constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
  static constexpr int32_t kMax = std::numeric_limits<int32_t>::max();
  static constexpr int16_t kMinSmallint = std::numeric_limits<int16_t>::min();
  static constexpr int16_t kMaxSmallint = std::numeric_limits<int16_t>::max();
  static constexpr int8_t kMinTinyint = std::numeric_limits<int8_t>::min();
  static constexpr int8_t kMaxTinyint = std::numeric_limits<int8_t>::max();
  static constexpr int64_t kMinBigint = std::numeric_limits<int64_t>::min();
  static constexpr int64_t kMaxBigint = std::numeric_limits<int64_t>::max();

  static constexpr float kLowestFloat = std::numeric_limits<float>::lowest();
  static constexpr float kMaxFloat = std::numeric_limits<float>::max();
  static constexpr double kLowestDouble = std::numeric_limits<double>::lowest();
  static constexpr double kMaxDouble = std::numeric_limits<double>::max();

 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  void setTimeParserPolicy(const std::string& timeParserPolicy) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kTimeParserPolicy, timeParserPolicy}});
  }

  void setPolicyAndTimeZone(
      const std::string& timeParserPolicy,
      const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, timeZone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
         {core::QueryConfig::kTimeParserPolicy, timeParserPolicy}});
  }

  int32_t parseDate(const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  }

  template <typename TOutput, typename TValue>
  std::optional<TOutput> evaluateDateFuncOnce(
      const std::string& expr,
      const std::optional<int32_t>& date,
      const std::optional<TValue>& value) {
    return evaluateOnce<TOutput>(
        expr,
        makeRowVector(
            {makeNullableFlatVector(
                 std::vector<std::optional<int32_t>>{date}, DATE()),
             makeNullableFlatVector(
                 std::vector<std::optional<TValue>>{value})}));
  }

  template <typename T>
  auto secondsToTimestamp(T seconds) {
    return evaluateOnce<Timestamp, T>("timestamp_seconds(c0)", seconds);
  }

  std::string timestampToString(Timestamp ts) {
    TimestampToStringOptions options;
    options.mode = TimestampToStringOptions::Mode::kFull;
    std::string result;
    result.resize(getMaxStringLength(options));
    const auto view = Timestamp::tsToStringView(ts, options, result.data());
    result.resize(view.size());
    return result;
  }
};

TEST_F(SparkSqlDateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 9000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1969, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(DATE()->toDays("1970-05-05")));
  EXPECT_EQ(1969, year(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(2020, year(DATE()->toDays("2020-01-01")));
  EXPECT_EQ(1920, year(DATE()->toDays("1920-01-01")));
}

TEST_F(SparkSqlDateTimeFunctionsTest, weekOfYear) {
  const auto weekOfYear = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    return evaluateOnce<int32_t, int32_t>("week_of_year(c0)", {date}, {DATE()})
        .value();
  };

  EXPECT_EQ(1, weekOfYear("1919-12-31"));
  EXPECT_EQ(1, weekOfYear("1920-01-01"));
  EXPECT_EQ(1, weekOfYear("1920-01-04"));
  EXPECT_EQ(2, weekOfYear("1920-01-05"));
  EXPECT_EQ(53, weekOfYear("1960-01-01"));
  EXPECT_EQ(53, weekOfYear("1960-01-03"));
  EXPECT_EQ(1, weekOfYear("1960-01-04"));
  EXPECT_EQ(1, weekOfYear("1969-12-31"));
  EXPECT_EQ(1, weekOfYear("1970-01-01"));
  EXPECT_EQ(1, weekOfYear("0001-01-01"));
  EXPECT_EQ(52, weekOfYear("9999-12-31"));
  EXPECT_EQ(8, weekOfYear("2008-02-20"));
  EXPECT_EQ(15, weekOfYear("2015-04-08"));
  EXPECT_EQ(15, weekOfYear("2013-04-08"));
  EXPECT_EQ(52, weekOfYear("2022-01-01"));
  EXPECT_EQ(52, weekOfYear("2022-01-02"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, weekOfYearLargeTimestamp) {
  const auto weeksInIsoYear = [](int32_t year) {
    const auto wdayJan1 =
        util::extractISODayOfTheWeek(util::daysSinceEpochFromDate(year, 1, 1));
    return (wdayJan1 == 4 || (wdayJan1 == 3 && util::isLeapYear(year))) ? 53
                                                                        : 52;
  };

  const auto isoWeekNumber = [&](const Timestamp& ts) {
    const auto civil = util::toCivilDateTime(ts, true, false);
    const auto isoWeekday = util::extractISODayOfTheWeek(civil.daysSinceEpoch);
    int32_t isoWeek =
        static_cast<int32_t>((10 + civil.dayOfYear - isoWeekday) / 7);
    int32_t isoYear = civil.date.year;
    if (isoWeek < 1) {
      isoYear -= 1;
      isoWeek = weeksInIsoYear(isoYear);
    } else {
      const auto weeksThisYear = weeksInIsoYear(isoYear);
      if (isoWeek > weeksThisYear) {
        isoYear += 1;
        isoWeek = 1;
      }
    }
    return isoWeek;
  };

  Timestamp ts(1'764'593'923'251, 0); // year ~57887, beyond chrono::time_point
  const auto week =
      evaluateOnce<int32_t>("week_of_year(c0)", std::optional<Timestamp>{ts});
  ASSERT_TRUE(week.has_value());
  EXPECT_EQ(week.value(), isoWeekNumber(ts));
}

TEST_F(SparkSqlDateTimeFunctionsTest, weekdayDate) {
  const auto weekday = [&](std::optional<int32_t> value) {
    return evaluateOnce<int32_t, int32_t>("weekday(c0)", {value}, {DATE()});
  };

  EXPECT_EQ(3, weekday(0));
  EXPECT_EQ(2, weekday(-1));
  EXPECT_EQ(5, weekday(-40));
  EXPECT_EQ(3, weekday(parseDate("2009-07-30")));
  EXPECT_EQ(6, weekday(parseDate("2023-08-20")));
  EXPECT_EQ(0, weekday(parseDate("2023-08-21")));
  EXPECT_EQ(1, weekday(parseDate("2023-08-22")));
  EXPECT_EQ(2, weekday(parseDate("2023-08-23")));
  EXPECT_EQ(3, weekday(parseDate("2023-08-24")));
  EXPECT_EQ(4, weekday(parseDate("2023-08-25")));
  EXPECT_EQ(5, weekday(parseDate("2023-08-26")));
  EXPECT_EQ(6, weekday(parseDate("2023-08-27")));
  EXPECT_EQ(5, weekday(parseDate("2017-05-27")));
  EXPECT_EQ(2, weekday(parseDate("2015-04-08")));
  EXPECT_EQ(4, weekday(parseDate("2013-11-08")));
  EXPECT_EQ(4, weekday(parseDate("2011-05-06")));
  EXPECT_EQ(4, weekday(parseDate("1582-10-15")));
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixSeconds) {
  const auto unixSeconds = [&](const StringView time) {
    return evaluateOnce<int64_t, Timestamp>(
        "unix_seconds(c0)", util::fromTimestampString(time, nullptr));
  };
  EXPECT_EQ(unixSeconds("1970-01-01 00:00:01"), 1);
  EXPECT_EQ(unixSeconds("1970-01-01 00:00:00.000127"), 0);
  EXPECT_EQ(unixSeconds("1969-12-31 23:59:59.999872"), -1);
  EXPECT_EQ(unixSeconds("1970-01-01 00:35:47.483647"), 2147);
  EXPECT_EQ(unixSeconds("1971-01-01 00:00:01.483647"), 31536001);
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixDate) {
  const auto unixDate = [&](std::string_view date) {
    return evaluateOnce<int32_t, int32_t>(
        "unix_date(c0)",
        {util::fromDateString(date.data(), date.length(), nullptr)},
        {DATE()});
  };

  EXPECT_EQ(unixDate("1970-01-01"), 0);
  EXPECT_EQ(unixDate("1970-01-02"), 1);
  EXPECT_EQ(unixDate("1969-12-31"), -1);
  EXPECT_EQ(unixDate("1970-02-01"), 31);
  EXPECT_EQ(unixDate("1971-01-31"), 395);
  EXPECT_EQ(unixDate("1971-01-01"), 365);
  EXPECT_EQ(unixDate("1972-02-29"), 365 + 365 + 30 + 29);
  EXPECT_EQ(unixDate("1971-03-01"), 365 + 30 + 28 + 1);
  EXPECT_EQ(unixDate("5881580-07-11"), kMax);
  EXPECT_EQ(unixDate("-5877641-06-23"), kMin);
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestamp) {
  const auto unixTimestamp = [&](std::optional<StringView> dateStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0)", dateStr);
  };

  const auto unixTimestampWithFormat = [&](const std::string& dateString,
                                           const std::string& format) {
    auto res1 = evaluateOnce<int64_t>(
        "unix_timestamp(c0)",
        makeRowVector({makeNullableFlatVector(
            std::vector<std::optional<int32_t>>{parseDate(dateString)},
            DATE())}));
    EXPECT_TRUE(res1.has_value());
    return res1.value();
  };

  const auto unixTimestampWithFormatAndTZ = [&](const std::string& dateString,
                                                const std::string& format) {
    auto date = parseDate(dateString);
    return evaluateOnce<int64_t>(
        "unix_timestamp(c0, c1, 'Asia/Shanghai')",
        makeRowVector(
            {makeNullableFlatVector(
                 std::vector<std::optional<int32_t>>{date}, DATE()),
             makeNullableFlatVector(
                 std::vector<std::optional<std::string>>{format})}));
  };

  EXPECT_EQ(
      1747843200, unixTimestampWithFormatAndTZ("2025-05-22", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, unixTimestamp("1970-01-01 00-00-00"));
  EXPECT_EQ(0, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(1, unixTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(61, unixTimestamp("1970-01-01 00:01:01"));

  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(28800, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(1670859931, unixTimestamp("2022-12-12 07:45:31"));
  // daylight saving time
  EXPECT_EQ(1437721200, unixTimestamp("2015-07-24 00:00:00"));

  // Empty or malformed input returns null.
  EXPECT_EQ(std::nullopt, unixTimestamp(std::nullopt));
  EXPECT_EQ(std::nullopt, unixTimestamp("1970-01-01"));
  EXPECT_EQ(std::nullopt, unixTimestamp("00:00:00"));
  EXPECT_EQ(std::nullopt, unixTimestamp(""));
  EXPECT_EQ(std::nullopt, unixTimestamp("malformed input"));

  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(-2177481944, unixTimestamp("1900-12-31 23:59:59"));
  EXPECT_EQ(-2177481600, unixTimestamp("1901-01-01 00:00:00"));
  // daylight saving time
  EXPECT_EQ(675702000, unixTimestamp("1991-06-01 00:00:00"));

  setQueryTimeZone("+01:00");
  EXPECT_EQ(-3600, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(1563922800, unixTimestamp("2019-07-24 00:00:00"));
  // unix_timestamp(date, format)
  EXPECT_EQ(1563922800, unixTimestampWithFormat("2019-07-24", "yyyy-MM-dd"));
  EXPECT_EQ(1563922800, unixTimestampWithFormat("2019-07-24", "yyyyMMdd"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestampDate) {
  const auto unixTimestamp = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>(
        "unix_timestamp(c0)", {date}, {DATE()});
  };

  EXPECT_EQ(0, unixTimestamp(DATE()->toDays("1970-01-01")));

  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(-31564800, unixTimestamp(DATE()->toDays("1969-01-01")));
  EXPECT_EQ(-28800, unixTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(1437667200, unixTimestamp(DATE()->toDays("2015-07-24")));
  EXPECT_EQ(1714492800, unixTimestamp(DATE()->toDays("2024-05-01")));
  EXPECT_EQ(-2209017943, unixTimestamp(DATE()->toDays("1900-01-01")));

  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(28800, unixTimestamp(DATE()->toDays("1970-01-01")));
  // daylight saving time
  EXPECT_EQ(1437721200, unixTimestamp(DATE()->toDays("2015-07-24")));

  setQueryTimeZone("+01:00");
  EXPECT_EQ(-3600, unixTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(1563922800, unixTimestamp(DATE()->toDays("2019-07-24")));

  // Empty input returns null.
  EXPECT_EQ(std::nullopt, unixTimestamp(std::nullopt));
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestampTimestamp) {
  const auto unixTimestamp = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("unix_timestamp(c0)", timestamp);
  };

  EXPECT_EQ(
      693678673609,
      unixTimestamp(bytedance::bolt::util::fromTimestampString(
          "23951-10-21 08:26:49.742814362", nullptr)));
  EXPECT_EQ(
      453573169803,
      unixTimestamp(bytedance::bolt::util::fromTimestampString(
          "16343-03-01 11:10:03.025739021", nullptr)));
  EXPECT_EQ(
      111180482824,
      unixTimestamp(bytedance::bolt::util::fromTimestampString(
          "5493-03-03 03:27:04.154041328", nullptr)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestampCurrent) {
  // Need a mock row vector so we can pump exactly one record out.
  auto mockRowVector =
      makeRowVector({BaseVector::createNullConstant(UNKNOWN(), 1, pool())});

  // Safe bet that unix epoch (in seconds) should be between 500M and 5B.
  auto epoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  EXPECT_GE(epoch, 500'000'000);
  EXPECT_LT(epoch, 5'000'000'000);

  // Spark doesn't seem to adjust based on timezones.
  auto gmtEpoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  setQueryTimeZone("America/Los_Angeles");
  auto laEpoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  EXPECT_EQ(gmtEpoch, laEpoch);
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestampCustomFormat) {
  const auto unixTimestamp = [&](std::optional<StringView> dateStr,
                                 std::optional<StringView> formatStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0, c1)", dateStr, formatStr);
  };

  EXPECT_EQ(0, unixTimestamp("1970-01-01", "yyyy-MM-dd"));
  EXPECT_EQ(0, unixTimestamp(" 1970-01-01 00:00:00", " yyyy-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp("1970-01-01 00-00-00", "yyyy-MM-dd HH:mm:ss"));
  EXPECT_EQ(-31536000, unixTimestamp("1969", "yyyy"));
  EXPECT_EQ(86400, unixTimestamp("1970-01-02", "yyyy-MM-dd"));
  EXPECT_EQ(86410, unixTimestamp("1970-01-02 00:00:10", "yyyy-MM-dd HH:mm:ss"));
  // The func unix_timestamp logic in the new version of Spark has changed. When
  // the input and format are incompatible, the content in the input that has
  // not been parsed by the format will be ignored. for example, query
  // `select unix_timestamp("1970-01-01 00:00", "yyyy-MM-dd")` returns -28800,
  //  which is equal to `select unix_timestamp("1970-01-01", "yyyy-MM-dd")`,
  // instead of NULL
  EXPECT_EQ(
      unixTimestamp("1970-01-02", "yyyy-MM-dd"),
      unixTimestamp("1970-01-02 00:00:10", "yyyy-MM-dd"));

  EXPECT_EQ(
      unixTimestamp("1970-01-02", "yyyy-MM-dd"),
      unixTimestamp("1970-01-02 00:00", "yyyy-MM-dd"));

  EXPECT_EQ(
      unixTimestamp("1970-01-01 00:00", "yyyy-MM-dd HH:mm"),
      unixTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH:mm"));

  EXPECT_EQ(
      unixTimestamp("1970-01-01 00", "yyyy-MM-dd HH"),
      unixTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH"));

  // Literal.
  EXPECT_EQ(
      1670831131,
      unixTimestamp("2022-12-12 asd 07:45:31", "yyyy-MM-dd 'asd' HH:mm:ss"));

  // Invalid format returns null (unclosed quoted literal).
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp("2022-12-12 asd 07:45:31", "yyyy-MM-dd 'asd HH:mm:ss"));
}

TEST_F(
    SparkSqlDateTimeFunctionsTest,
    unixTimestampCustomFormatWithoutConsumeAllCheck) {
  const auto UnixTimestampWithTimeZone =
      [&](std::optional<StringView> dateStr,
          std::optional<StringView> formatStr,
          std::optional<StringView> timeZone) {
        return evaluateOnce<int64_t>(
            fmt::format(
                "unix_timestamp(c0, c1, '{}')", timeZone.value_or("null")),
            dateStr,
            formatStr);
      };
  const auto shanghaiUnixTimestamp = [&](std::optional<StringView> dateStr,
                                         std::optional<StringView> formatStr) {
    return UnixTimestampWithTimeZone(dateStr, formatStr, "Asia/Shanghai");
  };
  const auto unixTimestamp = [&](std::optional<StringView> dateStr,
                                 std::optional<StringView> formatStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0, c1)", dateStr, formatStr);
  };

  EXPECT_EQ(57600, shanghaiUnixTimestamp("1970-01-02", "yyyy-MM-dd"));
  EXPECT_EQ(86400, unixTimestamp("1970-01-02", "yyyy-MM-dd"));
  EXPECT_EQ(-28800, shanghaiUnixTimestamp("1970-01", "yyyy-MM"));
  EXPECT_EQ(0, unixTimestamp("1970-01", "yyyy-MM"));
  EXPECT_EQ(-28800, shanghaiUnixTimestamp("1970", "yyyy"));
  EXPECT_EQ(0, unixTimestamp("1970", "yyyy"));
  EXPECT_EQ(-2209017943, shanghaiUnixTimestamp("1900", "yyyy"));
  EXPECT_EQ(
      1000,
      UnixTimestampWithTimeZone(
          "1969-12-31 16:16:40", "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles"));
  EXPECT_EQ(
      0, shanghaiUnixTimestamp("1970-01-01 08:00:00", "yyyy-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      1514779260,
      shanghaiUnixTimestamp("2018-01-01 12:01:00", "yyyy-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      0,
      shanghaiUnixTimestamp(
          "1970-01-01 08:00:00.000 +0800", "yyyy-MM-dd HH:mm:ss.SSS Z"));
  EXPECT_EQ(
      1514808060,
      shanghaiUnixTimestamp(
          "2018-01-01 12:01:00.123 +0000", "yyyy-MM-dd HH:mm:ss.SSS Z"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, unixTimestampCustomFormatTimeZone) {
  const auto unixTimestamp = [&](std::optional<std::string> ts,
                                 const std::string& format,
                                 const std::string& timezone) {
    return evaluateOnce<int64_t>(
        fmt::format("unix_timestamp(c0, '{}', '{}')", format, timezone), ts);
  };

  setQueryTimeZone("Asia/Shanghai");

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

  // format wiht different timezone
  // current yyyy-MM-ddTHH:mm:ss.SSZZ  is not support, since Specifier T is not
  // supported. // TODO to be supported
  const std::string ISO_8601_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSZZ";
  std::string isoTime = "1970-01-01 05:30:01.00+05:30";
  EXPECT_EQ(1, unixTimestamp(isoTime, ISO_8601_DATE_FORMAT, "Asia/Shanghai"));

  // Change timezone to "Asia/Kolkata"  +5:30
  setQueryTimeZone("Asia/Kolkata");
  EXPECT_EQ(
      0,
      unixTimestamp(
          "1970-01-01 05:30:00", "yyyy-MM-dd HH:mm:ss", "Asia/Kolkata"));

  // https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  // Change to 	America/Toronto  −05:00	−04:00	EST	EDT
  // but how about CDT ?
  setQueryTimeZone("America/Toronto");
  EXPECT_EQ(
      5 * 3600,
      unixTimestamp(
          "1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss", "America/Toronto"));
}

// unix_timestamp and to_unix_timestamp are aliases.
TEST_F(SparkSqlDateTimeFunctionsTest, toUnixTimestamp) {
  std::optional<StringView> dateStr = "1970-01-01 08:32:11"_sv;
  std::optional<StringView> formatStr = "yyyy-MM-dd HH:mm:ss"_sv;

  EXPECT_EQ(
      evaluateOnce<int64_t>("unix_timestamp(c0)", dateStr),
      evaluateOnce<int64_t>("to_unix_timestamp(c0)", dateStr));
  EXPECT_EQ(
      evaluateOnce<int64_t>("unix_timestamp(c0, c1)", dateStr, formatStr),
      evaluateOnce<int64_t>("to_unix_timestamp(c0, c1)", dateStr, formatStr));

  std::optional<Timestamp> timestamp =
      bytedance::bolt::util::fromTimestampString(
          "23951-10-21 08:26:49.742814362", nullptr);
  EXPECT_EQ(
      evaluateOnce<int64_t>("unix_timestamp(c0)", timestamp),
      evaluateOnce<int64_t>("to_unix_timestamp(c0)", timestamp));

  // to_unix_timestamp does not provide an overoaded without any parameters.
  EXPECT_THROW(evaluateOnce<int64_t>("to_unix_timestamp()"), BoltUserError);
}

TEST_F(SparkSqlDateTimeFunctionsTest, makeDate) {
  const auto makeDate = [&](std::optional<int32_t> year,
                            std::optional<int32_t> month,
                            std::optional<int32_t> day) {
    return evaluateOnce<int32_t>("make_date(c0, c1, c2)", year, month, day);
  };
  EXPECT_EQ(makeDate(1920, 1, 25), DATE()->toDays("1920-01-25"));
  EXPECT_EQ(makeDate(-10, 1, 30), DATE()->toDays("-0010-01-30"));

  EXPECT_EQ(makeDate(kMax, 12, 15), std::nullopt);

  constexpr const int32_t kJodaMaxYear{292278994};
  EXPECT_EQ(makeDate(kJodaMaxYear - 10, 12, 15), std::nullopt);

  EXPECT_EQ(makeDate(2021, 13, 1), std::nullopt);
  EXPECT_EQ(makeDate(2022, 3, 35), std::nullopt);

  EXPECT_EQ(makeDate(2023, 4, 31), std::nullopt);
  EXPECT_EQ(makeDate(2023, 3, 31), parseDate("2023-03-31"));

  EXPECT_EQ(makeDate(2023, 2, 29), std::nullopt);
  EXPECT_EQ(makeDate(2023, 3, 29), parseDate("2023-03-29"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, lastDay) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("last_day(c0)", {date}, {DATE()});
  };

  const auto lastDay = [&](const std::string& dateStr) {
    return lastDayFunc(DATE()->toDays(dateStr));
  };

  const auto parseDateStr = [&](const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  };

  EXPECT_EQ(lastDay("2015-02-28"), parseDateStr("2015-02-28"));
  EXPECT_EQ(lastDay("2015-03-27"), parseDateStr("2015-03-31"));
  EXPECT_EQ(lastDay("2015-04-26"), parseDateStr("2015-04-30"));
  EXPECT_EQ(lastDay("2015-05-25"), parseDateStr("2015-05-31"));
  EXPECT_EQ(lastDay("2015-06-24"), parseDateStr("2015-06-30"));
  EXPECT_EQ(lastDay("2015-07-23"), parseDateStr("2015-07-31"));
  EXPECT_EQ(lastDay("2015-08-01"), parseDateStr("2015-08-31"));
  EXPECT_EQ(lastDay("2015-09-02"), parseDateStr("2015-09-30"));
  EXPECT_EQ(lastDay("2015-10-03"), parseDateStr("2015-10-31"));
  EXPECT_EQ(lastDay("2015-11-04"), parseDateStr("2015-11-30"));
  EXPECT_EQ(lastDay("2015-12-05"), parseDateStr("2015-12-31"));
  EXPECT_EQ(lastDay("2016-01-06"), parseDateStr("2016-01-31"));
  EXPECT_EQ(lastDay("2016-02-07"), parseDateStr("2016-02-29"));
  EXPECT_EQ(lastDayFunc(std::nullopt), std::nullopt);
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateAdd) {
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int32_t> value) {
    return evaluateDateFuncOnce<int32_t, int32_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  // Account for the last day of a year-month
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));

  // Check for negative intervals
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("5881580-07-11"), dateAdd("1970-01-01", kMax));
  EXPECT_EQ(parseDate("1969-12-31"), dateAdd("-5877641-06-23", kMax));
  EXPECT_EQ(parseDate("-5877641-06-23"), dateAdd("1970-01-01", kMin));
  EXPECT_EQ(parseDate("1969-12-31"), dateAdd("5881580-07-11", kMin));
  EXPECT_EQ(parseDate("5881580-07-10"), dateAdd("1969-12-31", kMax));

  EXPECT_EQ(parseDate("-5877587-07-11"), dateAdd("2024-01-22", kMax - 1));
  EXPECT_EQ(parseDate("-5877587-07-12"), dateAdd("2024-01-22", kMax));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateAddSmallint) {
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int16_t> value) {
    return evaluateDateFuncOnce<int32_t, int16_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  // Account for the last day of a year-month
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));

  // Check for negative intervals
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("2059-09-17"), dateAdd("1969-12-31", kMaxSmallint));
  EXPECT_EQ(parseDate("1880-04-13"), dateAdd("1969-12-31", kMinSmallint));

  EXPECT_EQ(parseDate("2113-10-09"), dateAdd("2024-01-22", kMaxSmallint));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateAddTinyint) {
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int8_t> value) {
    return evaluateDateFuncOnce<int32_t, int8_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };
  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  EXPECT_EQ(parseDate("1970-05-07"), dateAdd("1969-12-31", kMaxTinyint));

  EXPECT_EQ(parseDate("1969-08-25"), dateAdd("1969-12-31", kMinTinyint));

  EXPECT_EQ(parseDate("2024-05-28"), dateAdd("2024-01-22", kMaxTinyint));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateSub) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int32_t> value) {
    return evaluateDateFuncOnce<int32_t, int32_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  // Account for the last day of a year-month.
  EXPECT_EQ(parseDate("2019-01-30"), dateSub("2020-02-29", 395));

  // Check for negative intervals.
  EXPECT_EQ(parseDate("2020-02-29"), dateSub("2019-02-28", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("-5877641-06-23"), dateSub("1969-12-31", kMax));
  EXPECT_EQ(parseDate("1970-01-01"), dateSub("5881580-07-11", kMax));
  EXPECT_EQ(parseDate("1970-01-01"), dateSub("-5877641-06-23", kMin));
  EXPECT_EQ(parseDate("5881580-07-11"), dateSub("1969-12-31", kMin));

  EXPECT_EQ(parseDate("-5877588-12-28"), dateSub("2023-07-10", kMin + 1));
  EXPECT_EQ(parseDate("-5877588-12-29"), dateSub("2023-07-10", kMin));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateSubSmallint) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int16_t> value) {
    return evaluateDateFuncOnce<int32_t, int16_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  // Account for the last day of a year-month.
  EXPECT_EQ(parseDate("2019-01-30"), dateSub("2020-02-29", 395));

  // Check for negative intervals.
  EXPECT_EQ(parseDate("2020-02-29"), dateSub("2019-02-28", -366));

  EXPECT_EQ(parseDate("1880-04-15"), dateSub("1970-01-01", kMaxSmallint));
  EXPECT_EQ(parseDate("2059-09-19"), dateSub("1970-01-01", kMinSmallint));

  EXPECT_EQ(parseDate("2113-03-28"), dateSub("2023-07-10", kMinSmallint));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateSubTinyint) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int8_t> value) {
    return evaluateDateFuncOnce<int32_t, int8_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  EXPECT_EQ(parseDate("1969-08-27"), dateSub("1970-01-01", kMaxTinyint));
  EXPECT_EQ(parseDate("1970-05-09"), dateSub("1970-01-01", kMinTinyint));

  EXPECT_EQ(parseDate("2023-11-15"), dateSub("2023-07-10", kMinTinyint));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dayOfYear) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("dayofyear(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(100, day(parseDate("2016-04-09")));
  EXPECT_EQ(235, day(parseDate("2023-08-23")));
  EXPECT_EQ(1, day(parseDate("1970-01-01")));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("dayofmonth(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(30, day(parseDate("2009-07-30")));
  EXPECT_EQ(23, day(parseDate("2023-08-23")));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dayOfWeekDate) {
  const auto dayOfWeek = [&](std::optional<int32_t> date,
                             const std::string& func) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("{}(c0)", func), {date}, {DATE()});
  };

  for (const auto& func : {"dayofweek", "dow"}) {
    EXPECT_EQ(std::nullopt, dayOfWeek(std::nullopt, func));
    EXPECT_EQ(5, dayOfWeek(0, func));
    EXPECT_EQ(4, dayOfWeek(-1, func));
    EXPECT_EQ(7, dayOfWeek(-40, func));
    EXPECT_EQ(5, dayOfWeek(parseDate("2009-07-30"), func));
    EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-20"), func));
    EXPECT_EQ(2, dayOfWeek(parseDate("2023-08-21"), func));
    EXPECT_EQ(3, dayOfWeek(parseDate("2023-08-22"), func));
    EXPECT_EQ(4, dayOfWeek(parseDate("2023-08-23"), func));
    EXPECT_EQ(5, dayOfWeek(parseDate("2023-08-24"), func));
    EXPECT_EQ(6, dayOfWeek(parseDate("2023-08-25"), func));
    EXPECT_EQ(7, dayOfWeek(parseDate("2023-08-26"), func));
    EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-27"), func));

    // test cases from spark's DateExpressionSuite.
    EXPECT_EQ(6, dayOfWeek(util::fromDateString("2011-05-06", nullptr), func));
  }
}

TEST_F(SparkSqlDateTimeFunctionsTest, dayofWeekTs) {
  const auto dayOfWeek = [&](std::optional<Timestamp> date,
                             const std::string& func) {
    return evaluateOnce<int32_t>(fmt::format("{}(c0)", func), date);
  };

  for (const auto& func : {"dayofweek", "dow"}) {
    EXPECT_EQ(5, dayOfWeek(Timestamp(0, 0), func));
    EXPECT_EQ(4, dayOfWeek(Timestamp(-1, 0), func));
    EXPECT_EQ(
        1,
        dayOfWeek(
            util::fromTimestampString("2023-08-20 20:23:00.001", nullptr),
            func));
    EXPECT_EQ(
        2,
        dayOfWeek(
            util::fromTimestampString("2023-08-21 21:23:00.030", nullptr),
            func));
    EXPECT_EQ(
        3,
        dayOfWeek(
            util::fromTimestampString("2023-08-22 11:23:00.100", nullptr),
            func));
    EXPECT_EQ(
        4,
        dayOfWeek(
            util::fromTimestampString("2023-08-23 22:23:00.030", nullptr),
            func));
    EXPECT_EQ(
        5,
        dayOfWeek(
            util::fromTimestampString("2023-08-24 15:23:00.000", nullptr),
            func));
    EXPECT_EQ(
        6,
        dayOfWeek(
            util::fromTimestampString("2023-08-25 03:23:04.000", nullptr),
            func));
    EXPECT_EQ(
        7,
        dayOfWeek(
            util::fromTimestampString("2023-08-26 01:03:00.300", nullptr),
            func));
    EXPECT_EQ(
        1,
        dayOfWeek(
            util::fromTimestampString("2023-08-27 01:13:00.000", nullptr),
            func));
    // test cases from spark's DateExpressionSuite.
    EXPECT_EQ(
        4,
        dayOfWeek(
            util::fromTimestampString("2015-04-08 13:10:15", nullptr), func));
    EXPECT_EQ(
        7,
        dayOfWeek(
            util::fromTimestampString("2017-05-27 13:10:15", nullptr), func));
    EXPECT_EQ(
        6,
        dayOfWeek(
            util::fromTimestampString("1582-10-15 13:10:15", nullptr), func));
  }
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](std::optional<int32_t> endDate,
                            std::optional<int32_t> startDate) {
    return evaluateOnce<int32_t, int32_t>(
        "datediff(c0, c1)", {endDate, startDate}, {DATE(), DATE()});
  };

  // Simple tests.
  EXPECT_EQ(-1, dateDiff(parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(-358, dateDiff(parseDate("2019-02-28"), parseDate("2020-02-21")));
  EXPECT_EQ(0, dateDiff(parseDate("1994-04-20"), parseDate("1994-04-20")));

  // Account for the last day of a year-month.
  EXPECT_EQ(395, dateDiff(parseDate("2020-02-29"), parseDate("2019-01-30")));

  // Check Large date.
  EXPECT_EQ(
      -737790, dateDiff(parseDate("2020-02-29"), parseDate("4040-02-29")));

  // Overflowed result, consistent with spark.
  EXPECT_EQ(
      2147474628,
      dateDiff(parseDate("-5877641-06-23"), parseDate("1994-09-12")));
}

TEST_F(SparkSqlDateTimeFunctionsTest, addMonths) {
  const auto addMonths = [&](const std::string& dateString, int32_t value) {
    return evaluateOnce<int32_t, int32_t>(
        "add_months(c0, c1)",
        {parseDate(dateString), value},
        {DATE(), INTEGER()});
  };

  EXPECT_EQ(addMonths("2015-01-30", 1), parseDate("2015-02-28"));
  EXPECT_EQ(addMonths("2015-01-30", 11), parseDate("2015-12-30"));
  EXPECT_EQ(addMonths("2015-01-01", 10), parseDate("2015-11-01"));
  EXPECT_EQ(addMonths("2015-01-31", 24), parseDate("2017-01-31"));
  EXPECT_EQ(addMonths("2015-01-31", 8), parseDate("2015-09-30"));
  EXPECT_EQ(addMonths("2015-01-30", 0), parseDate("2015-01-30"));
  // The last day of Feb. 2016 is 29th.
  EXPECT_EQ(addMonths("2016-03-30", -1), parseDate("2016-02-29"));
  // The last day of Feb. 2015 is 28th.
  EXPECT_EQ(addMonths("2015-03-31", -1), parseDate("2015-02-28"));
  EXPECT_EQ(addMonths("2015-01-30", -2), parseDate("2014-11-30"));
  EXPECT_EQ(addMonths("2015-04-20", -24), parseDate("2013-04-20"));

  BOLT_ASSERT_THROW(
      addMonths("2023-07-10", kMin),
      fmt::format("Integer overflow in add_months(2023-07-10, {})", kMin));
  BOLT_ASSERT_THROW(
      addMonths("2023-07-10", kMax),
      fmt::format("Integer overflow in add_months(2023-07-10, {})", kMax));
}

TEST_F(SparkSqlDateTimeFunctionsTest, monthDate) {
  const auto month = [&](const std::string& dateString) {
    return evaluateOnce<int32_t, int32_t>(
        "month(c0)", {parseDate(dateString)}, {DATE()});
  };

  EXPECT_EQ(4, month("2015-04-08"));
  EXPECT_EQ(11, month("2013-11-08"));
  EXPECT_EQ(1, month("1987-01-08"));
  EXPECT_EQ(8, month("1954-08-08"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, month) {
  const auto month = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(1, month(Timestamp(0, 0)));
  EXPECT_EQ(12, month(Timestamp(-1, 9000)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 0)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(8, month(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, month(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(12, month(Timestamp(0, 0)));
  EXPECT_EQ(12, month(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 0)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(8, month(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, month(Timestamp(998423705, 321000000)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](const std::string& dateString) {
    return evaluateOnce<int32_t, int32_t>(
        "quarter(c0)", {parseDate(dateString)}, {DATE()});
  };

  EXPECT_EQ(2, quarter("2015-04-08"));
  EXPECT_EQ(4, quarter("2013-11-08"));
  EXPECT_EQ(1, quarter("1987-01-08"));
  EXPECT_EQ(3, quarter("1954-08-08"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, quarter) {
  const auto quarter = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("quarter(c0)", date);
  };
  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(1, quarter(Timestamp(0, 0)));
  EXPECT_EQ(4, quarter(Timestamp(-1, 9000)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 0)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2, quarter(Timestamp(990000000, 321000000)));
  EXPECT_EQ(3, quarter(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(4, quarter(Timestamp(0, 0)));
  EXPECT_EQ(4, quarter(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 0)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2, quarter(Timestamp(990000000, 321000000)));
  EXPECT_EQ(3, quarter(Timestamp(998423705, 321000000)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, nextDay) {
  const auto nextDay = [&](const std::string& date,
                           const std::string& dayOfWeek) {
    auto startDates =
        makeNullableFlatVector<int32_t>({parseDate(date)}, DATE());
    auto dayOfWeeks = makeNullableFlatVector<std::string>({dayOfWeek});

    auto result = evaluateOnce<int32_t>(
        fmt::format("next_day(c0, '{}')", dayOfWeek),
        makeRowVector({startDates}));

    auto anotherResult = evaluateOnce<int32_t>(
        "next_day(c0, c1)", makeRowVector({startDates, dayOfWeeks}));

    EXPECT_EQ(result, anotherResult);
    std::optional<std::string> res;
    if (result.has_value()) {
      res = DATE()->toString(result.value());
    }
    return res;
  };

  EXPECT_EQ(nextDay("2015-07-23", "Mon"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "mo"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "monday"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "Tue"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "tu"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "tuesday"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "we"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "wed"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "wednesday"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "Thu"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "TH"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "thursday"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "Fri"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-23", "fr"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-23", "friday"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-31", "wed"), "2015-08-05");
  EXPECT_EQ(nextDay("2015-07-23", "saturday"), "2015-07-25");
  EXPECT_EQ(nextDay("2015-07-23", "sunday"), "2015-07-26");
  EXPECT_EQ(nextDay("2015-12-31", "Fri"), "2016-01-01");

  EXPECT_EQ(nextDay("2015-07-23", "xx"), std::nullopt);
  EXPECT_EQ(nextDay("2015-07-23", "\"quote"), std::nullopt);
  EXPECT_EQ(nextDay("2015-07-23", ""), std::nullopt);
}

TEST_F(SparkSqlDateTimeFunctionsTest, getTimestamp) {
  const auto getTimestamp = [&](const std::optional<StringView>& dateString,
                                const std::string& format) {
    return evaluateOnce<Timestamp>(
        fmt::format("get_timestamp(c0, '{}')", format), dateString);
  };

  const auto getTimestampString =
      [&](const std::optional<StringView>& dateString,
          const std::string& format) {
        auto ts = getTimestamp(dateString, format);
        return ts.has_value() ? ts.value().toString() : "null";
      };

  EXPECT_EQ(getTimestamp("1970-01-01", "yyyy-MM-dd"), Timestamp(0, 0));
  EXPECT_EQ(Timestamp(-31536000, 0), getTimestamp("1969", "yyyy"));
  EXPECT_EQ(Timestamp(86400, 0), getTimestamp("1970-01-02", "yyyy-MM-dd"));
  EXPECT_EQ(
      Timestamp(86410, 0),
      getTimestamp("1970-01-02 00:00:10", "yyyy-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      getTimestamp("1970-01-01 00:00:00.010", "yyyy-MM-dd HH:mm:ss.SSS")
          .value(),
      Timestamp::fromMillis(10));
  auto milliSeconds = (6 * 60 * 60 + 10 * 60 + 59) * 1000 + 19;
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "yyyy-MM-dd HH:mm:ss.SSS")
          .value(),
      Timestamp::fromMillis(milliSeconds));

  EXPECT_EQ(
      getTimestampString("1970-01-01", "yyyy-MM-dd"),
      "1970-01-01 00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1970-01-01 00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("08/27/2017", "MM/dd/yyy"),
      "2017-08-27 00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01 12:08:59", "yyyy/MM/dd HH:mm:ss"),
      "1970-01-01 12:08:59.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08 08:20:19.000000000");

  // 8 hours ahead UTC.
  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(
      getTimestampString("1970-01-01", "yyyy-MM-dd"),
      "1969-12-31 16:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1969-12-31 16:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08 00:20:19.000000000");

  // 8 hours behind UTC.
  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1970-01-01 08:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08 16:20:19.000000000");

  setQueryTimeZone("Asia/Shanghai");

  EXPECT_EQ(
      Timestamp(0, 0),
      getTimestamp("1970-01-01 08:00:00.000", "yyyy-MM-dd HH:mm:ss.SSS"));
  EXPECT_EQ(
      Timestamp(-28800, 0),
      getTimestamp("1970-01-01 00:00:00.000", "yyyy-MM-dd HH:mm:ss.SSS"));

  // Change timezone to "Asia/Kolkata"  +5:30
  setQueryTimeZone("Asia/Kolkata");
  EXPECT_EQ(
      Timestamp(0, 0),
      getTimestamp("1970-01-01 05:30:00", "yyyy-MM-dd HH:mm:ss"));

  // https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  // Change to 	America/Toronto  −05:00	−04:00	EST	EDT
  // but how about CDT ?
  setQueryTimeZone("America/Toronto");
  EXPECT_EQ(
      Timestamp(5 * 3600, 0),
      getTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss"));

  // to_timestamp exception mode
  setQueryTimeZone("America/Los_Angeles");
// legacy timeparser
#ifdef SPARK_COMPATIBLE
  EXPECT_EQ(
      Timestamp(
          1580184371,
          847000000), // Timestamp.valueOf("2020-01-27 20:06:11.847")
      getTimestamp("2020-01-27 20:06:11.847-0800", "yyyy-MM-dd HH:mm:ss.SSSz")
          .value());
#endif
  setQueryTimeZone("UTC");
#ifdef SPARK_COMPATIBLE
  EXPECT_EQ(
      "2020-01-28 01:06:11.847000000",
      getTimestampString(
          "2020-01-27 20:06:11.847est", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "2020-01-28 01:06:11.847000000",
      getTimestampString(
          "2020-01-27 20:06:11.847Est", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "null",
      getTimestampString(
          "2020-01-27 20:06:11.847UT", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.847000000",
      getTimestampString(
          "2020-01-27 20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSSS"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.847000000",
      getTimestampString("2020-01-27 20:06:11.847", "yyyy-MM-dd HH:mm:ss.SSS"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.847000000",
      getTimestampString("2020-01-27 20:06:11.847", "yyyy-MM-dd HH:mm:ss.SS"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.847000000",
      getTimestampString("2020-01-27 20:06:11.847", "yyyy-MM-dd HH:mm:ss.S"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.084000000",
      getTimestampString("2020-01-27 20:06:11.84", "yyyy-MM-dd HH:mm:ss.SS"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.008000000",
      getTimestampString("2020-01-27 20:06:11.8", "yyyy-MM-dd HH:mm:ss.S"));
  EXPECT_EQ(
      "2020-01-27 20:06:11.008000000",
      getTimestampString("2020-01-27 20:06:11.8", "yyyy-MM-dd HH:mm:ss.SSSS"));
#endif
  EXPECT_EQ(
      std::nullopt,
      getTimestamp("2020-01-27 20:06:11.1234", "yyyy-MM-dd HH:mm:ss.SSSS"));
  EXPECT_EQ(std::nullopt, getTimestamp("", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("abcd-01-01", "yyyy-MM-dd"));
  EXPECT_EQ(Timestamp(0, 0), getTimestamp("1970-01-01 00:00:00", "yyyy"));
  EXPECT_EQ(Timestamp(0, 0), getTimestamp("1970-01-01 00:00:00", "yyyy-MM"));
  EXPECT_EQ(Timestamp(0, 0), getTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd"));
  EXPECT_EQ(
      Timestamp(0, 0), getTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH"));
  EXPECT_EQ(
      "1400-12-30 00:00:00.000000000",
      getTimestampString("1400-365", "yyyy-DD"));
  EXPECT_EQ(
      "1400-12-31 00:00:00.000000000",
      getTimestampString("1400-366", "yyyy-DD"));
  EXPECT_EQ(
      "1400-03-01 00:00:00.000000000",
      getTimestampString("1400-60", "yyyy-DD"));
  EXPECT_EQ(
      "1400-03-01 00:00:00.000000000",
      getTimestampString("1400-61", "yyyy-DD"));

  // week of year
  EXPECT_EQ(
      "1400-03-01 00:00:00.000000000",
      getTimestampString("1400-10-01", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1400-03-02 00:00:00.000000000",
      getTimestampString("1400-10-02", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1400-03-03 00:00:00.000000000",
      getTimestampString("1400-10-03", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1400-03-01 00:00:00.000000000",
      getTimestampString("1400-10-07", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1582-09-30 00:00:00.000000000",
      getTimestampString("1582-40-07", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1582-10-01 00:00:00.000000000",
      getTimestampString("1582-40-01", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1582-10-04 00:00:00.000000000",
      getTimestampString("1582-40-04", "YYYY-ww-uu"));
  EXPECT_EQ(
      "1582-10-15 00:00:00.000000000",
      getTimestampString("1582-40-05", "YYYY-ww-uu"));
  ASSERT_EQ(
      "2021-01-04 00:00:00.000000000",
      getTimestampString("2021-02-01", "YYYY-ww-uu"));

  // corrected
  setTimeParserPolicy("corrected");
#ifdef SPARK_COMPATIBLE
  EXPECT_EQ(
      std::nullopt,
      getTimestamp("2020-01-27 20:06:11.847-0800", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "2020-01-28 04:06:11.847000000",
      getTimestampString(
          "2020-01-27 20:06:11.847-08:00", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "2020-01-28 01:06:11.847000000",
      getTimestampString(
          "2020-01-27 20:06:11.847est", "yyyy-MM-dd HH:mm:ss.SSSz"));
#endif
  EXPECT_EQ(std::nullopt, getTimestamp("", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("abcd-01-01", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01-01 00:00:00", "yyyy"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01-01 00:00:00", "yyyy-MM"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("1970-01-01 00:00:00", "yyyy-MM-dd HH"));
  EXPECT_EQ(std::nullopt, getTimestamp("2015-07-22 10:00:00", "yyyy-MM-dd"));
  EXPECT_EQ(std::nullopt, getTimestamp("2014-31-12", "yyyy-MM-dd"));
  EXPECT_EQ(
      std::nullopt,
      getTimestamp("2020-01-27 20:06:11.847-0800", "yyyy-MM-dd HH:mm:ss.SSSz"));
  EXPECT_EQ(
      "1400-12-31 00:00:00.000000000",
      getTimestampString("1400-365", "yyyy-DD"));
  EXPECT_EQ("null", getTimestampString("1400-366", "yyyy-DD"));
  EXPECT_EQ(
      "1400-03-01 00:00:00.000000000",
      getTimestampString("1400-60", "yyyy-DD"));
  EXPECT_EQ(
      "1400-03-02 00:00:00.000000000",
      getTimestampString("1400-61", "yyyy-DD"));
  EXPECT_EQ(
      "1400-03-02 00:00:00.000000000",
      getTimestampString("1400-61", "yyyy-DD"));

  EXPECT_EQ("null", getTimestampString("1400-12-29-30", "yyyy-MM-dd-dd"));
  EXPECT_EQ(
      "1400-12-30 00:00:00.000000000",
      getTimestampString("1400-12-30-30", "yyyy-MM-dd-dd"));
  // function to_date legacy
  EXPECT_EQ(std::nullopt, getTimestamp("2015-07-22 10:00:00", "yyyy-MM-dd"));

  const auto getTimestamp2 = [&](std::optional<Timestamp> timestamp,
                                 std::optional<StringView> formatStr) {
    return evaluateOnce<Timestamp>(
        "get_timestamp(c0, c1)", timestamp, formatStr);
  };
  setPolicyAndTimeZone("corrected", "Asia/Shanghai");
  EXPECT_EQ(
      Timestamp(
          1437703200, 500000000), // Timestamp.valueOf("2015-07-24 10:00:00.5")
      getTimestamp2(Timestamp(1437703200, 500000000), "yyyy/MM/dd HH:mm:ss.S"));
  EXPECT_EQ(
      Timestamp(
          1437703200, 500000000), // Timestamp.valueOf("2015-07-24 10:00:00.5")
      getTimestamp("2015/07/24 10:00:00.5", "yyyy/MM/dd HH:mm:ss.S"));

  setPolicyAndTimeZone("corrected", "+01:00");
  EXPECT_EQ(
      Timestamp(-3600, 0),
      getTimestamp("1970/01/01 00:00:00", "yyyy/MM/dd HH:mm:ss"));
  EXPECT_EQ(Timestamp(1563922800, 0), getTimestamp("2019-07-24", "yyyy-MM-dd"));

  setTimeParserPolicy("corrected");
  // Parsing error.
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "HH:mm:ss.SSS"), std::nullopt);
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "yyyy/MM/dd HH:mm:ss.SSS"),
      std::nullopt);

  setTimeParserPolicy("exception");
  // Invalid date format.
  BOLT_ASSERT_THROW(
      getTimestamp("2020/01/24", "AA/MM/dd"),
      "Fail to parse. You may get a different result due to the upgrading of Spark");
  BOLT_ASSERT_THROW(
      getTimestamp("2023-07-13 21:34", "yyyy-MM-dd HH:II"),
      "Fail to parse. You may get a different result due to the upgrading of Spark");
}

TEST_F(SparkSqlDateTimeFunctionsTest, longToTimestamp) {
  const auto longToTimestamp = [&](const std::optional<int64_t> seconds) {
    return evaluateOnce<Timestamp>("long_to_timestamp(c0)", seconds);
  };
  const auto longToTimestampWithTimeZone =
      [&](const std::optional<int64_t> seconds,
          const std::optional<StringView> timeZone) {
        return evaluateOnce<Timestamp>(
            "long_to_timestamp(c0, c1)", seconds, timeZone);
      };

  EXPECT_EQ(std::make_optional(Timestamp{0, 0}), longToTimestamp(0));
  EXPECT_EQ(
      std::make_optional(Timestamp{1743465661, 123000000}),
      longToTimestamp(1743465661123L));

  EXPECT_EQ(
      std::make_optional(
          util::fromTimestampWithTimezoneString("1970-01-01 08:00:00 +08:00")
              .value()
              .first),
      longToTimestampWithTimeZone(0, "Asia/Shanghai"));
  EXPECT_EQ(
      std::make_optional(util::fromTimestampWithTimezoneString(
                             "2025-04-01 08:01:01.123 +08:00")
                             .value()
                             .first),
      longToTimestampWithTimeZone(1743465661123L, "Asia/Shanghai"));

  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(
      std::make_optional(
          util::fromTimestampWithTimezoneString("1970-01-01 08:00:00 +08:00")
              .value()
              .first),
      longToTimestamp(0));
  EXPECT_EQ(
      std::make_optional(util::fromTimestampWithTimezoneString(
                             "2025-04-01 08:01:01.123 +08:00")
                             .value()
                             .first),
      longToTimestamp(1743465661123L));
}

TEST_F(SparkSqlDateTimeFunctionsTest, hour) {
  const auto hour = [&](const StringView timestampStr) {
    const auto timeStamp =
        std::make_optional(util::fromTimestampString(timestampStr, nullptr));
    return evaluateOnce<int32_t>("hour(c0)", timeStamp);
  };

  EXPECT_EQ(0, hour("2024-01-08 00:23:00.001"));
  EXPECT_EQ(0, hour("2024-01-08 00:59:59.999"));
  EXPECT_EQ(1, hour("2024-01-08 01:23:00.001"));
  EXPECT_EQ(13, hour("2024-01-20 13:23:00.001"));
  EXPECT_EQ(13, hour("1969-01-01 13:23:00.001"));

  // Set time zone to Pacific/Apia (13 hours ahead of UTC).
  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(13, hour("2024-01-08 00:23:00.001"));
  EXPECT_EQ(13, hour("2024-01-08 00:59:59.999"));
  EXPECT_EQ(14, hour("2024-01-08 01:23:00.001"));
  EXPECT_EQ(2, hour("2024-01-20 13:23:00.001"));
  EXPECT_EQ(2, hour("1969-01-01 13:23:00.001"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, minute) {
  const auto minute = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("minute(c0)", date);
  };

  EXPECT_EQ(
      typeid(int), typeid(minute(Timestamp(998423705, 321000000)).value()));

  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(0, minute(Timestamp(0, 0)));
  EXPECT_EQ(59, minute(Timestamp(-1, 9000)));
  EXPECT_EQ(6, minute(Timestamp(4000000000, 0)));
  EXPECT_EQ(6, minute(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(4, minute(Timestamp(998474645, 321000000)));
  EXPECT_EQ(55, minute(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(30, minute(Timestamp(0, 0)));
  EXPECT_EQ(29, minute(Timestamp(-1, 9000)));
  EXPECT_EQ(36, minute(Timestamp(4000000000, 0)));
  EXPECT_EQ(36, minute(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(34, minute(Timestamp(998474645, 321000000)));
  EXPECT_EQ(25, minute(Timestamp(998423705, 321000000)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, second) {
  const auto second = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int32_t>("second(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(Timestamp(0, 0)));
  EXPECT_EQ(40, second(Timestamp(4000000000, 0)));
  EXPECT_EQ(59, second(Timestamp(-1, 123000000)));
  EXPECT_EQ(59, second(Timestamp(-1, Timestamp::kMaxNanos)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, millisecond) {
  const auto millisecond = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int32_t>("millisecond(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(Timestamp(0, 0)));
  EXPECT_EQ(0, millisecond(Timestamp(4000000000, 0)));
  EXPECT_EQ(123, millisecond(Timestamp(-1, 123000000)));
  EXPECT_EQ(999, millisecond(Timestamp(-1, Timestamp::kMaxNanos)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, fromUnixtimeWithTimezone) {
  const auto fromUnixtimeOnce = [&](std::optional<int64_t> unixtime,
                                    const std::string& format,
                                    const std::string& timezone) {
    return evaluateOnce<std::string>(
        fmt::format("from_unixtime(c0, '{}', '{}')", format, timezone),
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
}

TEST_F(SparkSqlDateTimeFunctionsTest, fromUnixtime) {
  const auto getUnixTime = [&](const StringView& str) {
    Timestamp t = util::fromTimestampString(str, nullptr);
    return t.getSeconds();
  };

  const auto fromUnixTime = [&](const std::optional<int64_t>& unixTime,
                                const std::optional<std::string>& timeFormat) {
    return evaluateOnce<std::string>(
        "from_unixtime(c0, c1)", unixTime, timeFormat);
  };

  EXPECT_EQ(fromUnixTime(0, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 00:00:00");
  EXPECT_EQ(fromUnixTime(100, "yyyy-MM-dd"), "1970-01-01");
  EXPECT_EQ(fromUnixTime(120, "yyyy-MM-dd HH:mm"), "1970-01-01 00:02");
  EXPECT_EQ(fromUnixTime(100, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 00:01:40");
  EXPECT_EQ(fromUnixTime(-59, "yyyy-MM-dd HH:mm:ss"), "1969-12-31 23:59:01");
  EXPECT_EQ(fromUnixTime(3600, "yyyy"), "1970");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-06-30 11:29:59");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "yyyy-MM-dd"),
      "2020-06-30");
  EXPECT_EQ(fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "MM-dd"), "06-30");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "HH:mm:ss"), "11:29:59");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 23:59:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-06-30 23:59:59");

// In debug mode, Timestamp constructor will throw exception if range check
// fails.
#ifdef NDEBUG
  // Integer overflow in the internal conversion from seconds to milliseconds.
  EXPECT_EQ(
      fromUnixTime(std::numeric_limits<int64_t>::max(), "yyyy-MM-dd HH:mm:ss")
          .value(),
      "1969-12-31 23:59:59");
#endif

  // 8 hours ahead UTC.
  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(fromUnixTime(0, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 08:00:00");
  EXPECT_EQ(fromUnixTime(120, "yyyy-MM-dd HH:mm"), "1970-01-01 08:02");
  EXPECT_EQ(fromUnixTime(-59, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 07:59:01");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2014-07-21 16:00:00"), "yyyy-MM-dd"),
      "2014-07-22");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 23:59:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-07-01 07:59:59");

#ifdef SPARK_COMPATIBLE
  setPolicyAndTimeZone("corrected", "Asia/Shanghai");
  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss").value(),
      "-0001-11-28 00:05:43");

  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss G").value(),
      "0002-11-28 00:05:43 BC");

  setPolicyAndTimeZone("exception", "Asia/Shanghai");
  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss").value(),
      "-0001-11-28 00:05:43");

  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss G").value(),
      "0002-11-28 00:05:43 BC");

  setPolicyAndTimeZone("legacy", "Asia/Shanghai");
  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss").value(),
      "0002-11-28 00:05:43");

  EXPECT_EQ(
      fromUnixTime(-62170185600, "yyyy-MM-dd HH:mm:ss G").value(),
      "0002-11-28 00:05:43 BC");

  EXPECT_EQ(fromUnixTime(0, "FF/MM/dd").value(), "01/01/01");
#endif

  // Invalid format.
  BOLT_ASSERT_THROW(
      fromUnixTime(0, "yyyy-AA"), "Specifier A is not supported.");
  BOLT_ASSERT_THROW(
      fromUnixTime(0, "yyyy-MM-dd HH:II"), "Specifier I is not supported");
}

TEST_F(SparkSqlDateTimeFunctionsTest, fromUnixtimeIllegal) {
  const auto fromUnixTime = [&](const std::optional<std::string>& unixTime,
                                const std::optional<std::string>& timeFormat) {
    return evaluateOnce<std::string>(
        "from_unixtime(try_cast(c0 as bigint), c1)", unixTime, timeFormat);
  };

  queryCtx_->testingOverrideConfigUnsafe({
      {core::QueryConfig::kThrowExceptionWhenEncounterBadTimestamp, "true"},
  });
  // Spark returns a wrapped calendar value instead of throwing for extremely
  // large seconds input. We only assert that parsing succeeds.
  EXPECT_NO_THROW(fromUnixTime("20231228101858000", "yyyy-MM-dd"));
  EXPECT_NO_THROW(fromUnixTime("-20231228101858000", "yyyy-MM-dd"));
}

#ifdef SPARK_COMPATIBLE
TEST_F(SparkSqlDateTimeFunctionsTest, CastStringToLargeTimestamp) {
  using util::fromTimestampString;

  auto result = evaluateOnce<Timestamp>(
      "cast(c0 as timestamp)", std::optional<std::string>("32768"));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(
      result.value(),
      fromTimestampString(StringView("32768-01-01 00:00:00"), nullptr));
}
#endif

TEST_F(SparkSqlDateTimeFunctionsTest, FromUnixtimeLargeValuesSparkParity) {
#ifdef SPARK_COMPATIBLE
  const std::string prefix = "+";
#else
  const std::string prefix;
#endif
  auto fromUnixTime = [&](int64_t unixTime) {
    return evaluateOnce<std::string>(
               "from_unixtime(c0, 'yyyy-MM-dd HH:mm:ss')",
               std::optional<int64_t>{unixTime})
        .value();
  };

  std::vector<std::pair<int64_t, std::string>> cases = {
      {1764593923251, prefix + "57887-10-03 18:40:51"},
      {1764592804143, prefix + "57887-09-20 19:49:03"},
      {1764593758503, prefix + "57887-10-01 20:55:03"},
      {1764590772393, prefix + "57887-08-28 07:26:33"},
      {1764593528791, prefix + "57887-09-29 05:06:31"},
      {1764593955077, prefix + "57887-10-04 03:31:17"},
      {1764593666320, prefix + "57887-09-30 19:18:40"},
      {1764593144444, prefix + "57887-09-24 18:20:44"},
      {1764594190546, prefix + "57887-10-06 20:55:46"},
      {1764593098611, prefix + "57887-09-24 05:36:51"},
      {1764592074551, prefix + "57887-09-12 09:09:11"},
      {1764590721621, prefix + "57887-08-27 17:20:21"},
      {1764590914524, prefix + "57887-08-29 22:55:24"},
      {1764592158681, prefix + "57887-09-13 08:31:21"},
      {1764590816700, prefix + "57887-08-28 19:45:00"},
      {1764591795161, prefix + "57887-09-09 03:32:41"},
      {1764594152392, prefix + "57887-10-06 10:19:52"},
      {1764593373289, prefix + "57887-09-27 09:54:49"},
      {1764591622807, prefix + "57887-09-07 03:40:07"},
      {1764592069089, prefix + "57887-09-12 07:38:09"},
  };

  for (const auto& [input, expected] : cases) {
    EXPECT_EQ(fromUnixTime(input), expected) << input;
  }
}

TEST_F(SparkSqlDateTimeFunctionsTest, FromUnixtimeYYYYThrowError) {
  const auto fromUnixTime = [&](const std::optional<std::string>& unixTime,
                                const std::optional<std::string>& timeFormat) {
    return evaluateOnce<std::string>(
        "from_unixtime(try_cast(c0 as bigint), c1)", unixTime, timeFormat);
  };

  EXPECT_THROW(fromUnixTime("1609167953", "YYYY-MM-dd"), BoltUserError);

  const auto fromUnixTimeWithTimezone = [&](const std::optional<std::string>&
                                                unixTime,
                                            const std::optional<std::string>&
                                                timeFormat,
                                            const std::optional<std::string>&
                                                timezone) {
    return evaluateOnce<std::string>(
        "from_unixtime(try_cast(c0 as bigint), 'YYYY-MM-dd', 'Asia/Shanghai')",
        unixTime,
        timeFormat,
        timezone);
  };

  EXPECT_THROW(
      fromUnixTimeWithTimezone("1609167953", "YYYY-MM-dd", "Asia/Shanghai"),
      BoltUserError);
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateFormat) {
  using util::fromTimestampString;

  const auto dateFormat = [&](std::optional<Timestamp> timestamp,
                              const std::string& format) {
    auto resultVector = evaluate(
        "date_format(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));

    auto resultStringView =
        resultVector->as<SimpleVector<StringView>>()->valueAt(0);
    return resultStringView.getString();
  };
  EXPECT_THROW(
      dateFormat(fromTimestampString("1970-01-01", nullptr), "YYYY-MM-dd"),
      BoltUserError);
  EXPECT_EQ(
      "20180314 01:02:03.123",
      dateFormat(
          fromTimestampString("2018-03-14 01:02:03.123456", nullptr),
          "yyyMMdd HH:mm:ss.SSS"));
  EXPECT_EQ(
      "2018/03/14 01:02:03.123456",
      dateFormat(
          fromTimestampString("2018-03-14 01:02:03.123456", nullptr),
          "yyy/MM/dd HH:mm:ss.SSSSSS"));
  EXPECT_EQ(
      "2018-03-14 01:02:03.123456000",
      dateFormat(
          fromTimestampString("2018-03-14 01:02:03.123456", nullptr),
          "yyy-MM-dd HH:mm:ss.SSSSSSSSS"));
  EXPECT_EQ(
      "2018-03-14 01:02:03.123456789",
      dateFormat(
          Timestamp(1520989323L, 123456789L), "yyy-MM-dd HH:mm:ss.SSSSSSSSS"));
}

#ifdef SPARK_COMPATIBLE
TEST_F(SparkSqlDateTimeFunctionsTest, weekBased) {
  using util::fromTimestampString;

  const auto dateFormat = [&](const std::string& dateStr,
                              const std::string& format) {
    auto timestamp = fromTimestampString(StringView(dateStr), nullptr);
    auto resultVector = evaluate(
        "date_format(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));

    auto resultStringView =
        resultVector->as<SimpleVector<StringView>>()->valueAt(0);
    return resultStringView.getString();
  };

  setTimeParserPolicy("legacy");
  // YYYY should not used with MM
  EXPECT_THROW(dateFormat("1970-01-01", "YYYY-MM-dd"), BoltUserError);

  // YYYY-ww-uu
  EXPECT_EQ("2024-52-06", dateFormat("2024-12-28", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-07", dateFormat("2024-12-29", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-01", dateFormat("2024-12-30", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-02", dateFormat("2024-12-31", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-03", dateFormat("2025-01-01", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-04", dateFormat("2025-01-02", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-05", dateFormat("2025-01-03", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-06", dateFormat("2025-01-04", "YYYY-ww-uu"));
  EXPECT_EQ("2025-02-07", dateFormat("2025-01-05", "YYYY-ww-uu"));

  // yyyy-MM-WW-uu
  EXPECT_EQ("2024-12-04-06", dateFormat("2024-12-28", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-05-07", dateFormat("2024-12-29", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-05-01", dateFormat("2024-12-30", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-05-02", dateFormat("2024-12-31", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-01-03", dateFormat("2025-01-01", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-01-04", dateFormat("2025-01-02", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-01-05", dateFormat("2025-01-03", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-01-06", dateFormat("2025-01-04", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02-07", dateFormat("2025-01-05", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02-01", dateFormat("2025-01-06", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02-02", dateFormat("2025-01-07", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02-03", dateFormat("2025-01-08", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02-04", dateFormat("2025-01-09", "yyyy-MM-WW-uu"));

  // yyyy-MM-FF-uu
  EXPECT_EQ("2024-12-04-06", dateFormat("2024-12-28", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-05-07", dateFormat("2024-12-29", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-05-01", dateFormat("2024-12-30", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-05-02", dateFormat("2024-12-31", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-03", dateFormat("2025-01-01", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-04", dateFormat("2025-01-02", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-05", dateFormat("2025-01-03", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-06", dateFormat("2025-01-04", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-07", dateFormat("2025-01-05", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-01", dateFormat("2025-01-06", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01-02", dateFormat("2025-01-07", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-02-03", dateFormat("2025-01-08", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-02-04", dateFormat("2025-01-09", "yyyy-MM-FF-uu"));

  const auto getTimestamp = [&](const std::optional<StringView>& dateString,
                                const std::string& format) {
    auto options = TimestampToStringOptions{
        .mode = TimestampToStringOptions::Mode::kDateOnly};
    return evaluateOnce<Timestamp>(
               fmt::format("get_timestamp(c0, '{}')", format), dateString)
        ->toString(options);
  };

  // YYYY should not used with MM
  EXPECT_THROW(getTimestamp("1970-01-01", "YYYY-MM-dd"), BoltUserError);

  // YYYY-ww-uu
  EXPECT_EQ("2024-12-28", getTimestamp("2024-52-06", "YYYY-ww-uu"));
  EXPECT_EQ("2024-12-29", getTimestamp("2025-01-07", "YYYY-ww-uu"));
  EXPECT_EQ("2024-12-30", getTimestamp("2025-01-01", "YYYY-ww-uu"));
  EXPECT_EQ("2024-12-31", getTimestamp("2025-01-02", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-01", getTimestamp("2025-01-03", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-02", getTimestamp("2025-01-04", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-03", getTimestamp("2025-01-05", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-04", getTimestamp("2025-01-06", "YYYY-ww-uu"));
  EXPECT_EQ("2025-01-05", getTimestamp("2025-02-07", "YYYY-ww-uu"));

  // yyyy-MM-WW-uu
  EXPECT_EQ("2024-12-28", getTimestamp("2024-12-04-06", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-29", getTimestamp("2024-12-05-07", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-30", getTimestamp("2024-12-05-01", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2024-12-31", getTimestamp("2024-12-05-02", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-01", getTimestamp("2025-01-01-03", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-02", getTimestamp("2025-01-01-04", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-03", getTimestamp("2025-01-01-05", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-04", getTimestamp("2025-01-01-06", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-05", getTimestamp("2025-01-02-07", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-06", getTimestamp("2025-01-02-01", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-07", getTimestamp("2025-01-02-02", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-08", getTimestamp("2025-01-02-03", "yyyy-MM-WW-uu"));
  EXPECT_EQ("2025-01-09", getTimestamp("2025-01-02-04", "yyyy-MM-WW-uu"));

  // yyyy-MM-FF-uu
  EXPECT_EQ("2024-12-28", getTimestamp("2024-12-04-06", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-29", getTimestamp("2024-12-05-07", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-30", getTimestamp("2024-12-05-01", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2024-12-31", getTimestamp("2024-12-05-02", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-01", getTimestamp("2025-01-01-03", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-02", getTimestamp("2025-01-01-04", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-03", getTimestamp("2025-01-01-05", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-04", getTimestamp("2025-01-01-06", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-05", getTimestamp("2025-01-01-07", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-06", getTimestamp("2025-01-01-01", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-07", getTimestamp("2025-01-01-02", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-08", getTimestamp("2025-01-02-03", "yyyy-MM-FF-uu"));
  EXPECT_EQ("2025-01-09", getTimestamp("2025-01-02-04", "yyyy-MM-FF-uu"));
}
#endif

TEST_F(SparkSqlDateTimeFunctionsTest, formatPdate) {
  const auto formatPdate = [&](std::optional<StringView> dateStr) {
    return evaluateOnce<std::string>("format_pdate(c0)", dateStr);
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, formatPdate(std::nullopt));

  // Simple tests
  EXPECT_EQ("2023-07-01", formatPdate("20230701"));
  EXPECT_EQ("2023-07-01", formatPdate("2023-07-01"));

  // Input string with '-' are directly returned
  // to match the behavior in spark
  EXPECT_EQ("-", formatPdate("-"));
  EXPECT_EQ("2023-", formatPdate("2023-"));

  // Invalid date string
  EXPECT_THROW(formatPdate("2023"), BoltUserError);
  EXPECT_THROW(formatPdate("2023070a"), BoltUserError);
  EXPECT_THROW(formatPdate("20230701123"), BoltUserError);
  EXPECT_THROW(formatPdate("20230799"), BoltUserError);
  EXPECT_THROW(formatPdate("20231301"), BoltUserError);
}

TEST_F(SparkSqlDateTimeFunctionsTest, pdate2date) {
  {
    // varchar -> varchar
    const auto pdate2date = [&](const std::optional<std::string>& input) {
      return evaluateOnce<std::string>("pdate2date(c0)", input);
    };

    // Simple tests
    EXPECT_EQ("20230517", pdate2date("2023-05-17"));
    EXPECT_EQ("20240101", pdate2date("2024-01-01"));
    EXPECT_EQ("00010203", pdate2date("0001-02-03"));

    // Illegal input
    EXPECT_EQ(std::nullopt, pdate2date(std::nullopt));
    EXPECT_EQ(std::nullopt, pdate2date("20230517"));
  }

  {
    // date -> varchar
    const auto pdate2date = [&](const std::optional<int32_t>& input) {
      return evaluateOnce<std::string, int32_t>(
          "pdate2date(c0)", {input}, {DATE()});
    };

    // Simple tests
    EXPECT_EQ("20230517", pdate2date(DATE()->toDays("2023-05-17")));
    EXPECT_EQ("20240101", pdate2date(DATE()->toDays("2024-01-01")));
    EXPECT_EQ("20010203", pdate2date(DATE()->toDays("2001-02-03")));

    // Illegal input
    EXPECT_EQ(std::nullopt, pdate2date(std::nullopt));
  }
}

TEST_F(SparkSqlDateTimeFunctionsTest, date2pdateVarchar) {
  const auto date2pdate = [&](const std::optional<std::string>& input) {
    return evaluateOnce<std::string>("date2pdate(c0)", input);
  };

  // Simple tests
  EXPECT_EQ("2023-05-17", date2pdate("20230517"));
  EXPECT_EQ("0001-02-03", date2pdate("00010203"));
  EXPECT_EQ("2022-04-30", date2pdate("20220431"));
  EXPECT_EQ("2022-02-28", date2pdate("20220231"));
  EXPECT_EQ("2024-02-29", date2pdate("20240231"));

  // Illegal input
  EXPECT_EQ(std::nullopt, date2pdate("2023-05-17"));
  EXPECT_EQ(std::nullopt, date2pdate(""));
  EXPECT_EQ(std::nullopt, date2pdate("2"));
  EXPECT_EQ(std::nullopt, date2pdate("2023"));
  EXPECT_EQ(std::nullopt, date2pdate("202305170"));
  EXPECT_EQ(std::nullopt, date2pdate("20239999"));
  EXPECT_EQ(std::nullopt, date2pdate("2023-99-99"));
  EXPECT_EQ(std::nullopt, date2pdate("00000517"));
  EXPECT_EQ(std::nullopt, date2pdate("00010003"));
  EXPECT_EQ(std::nullopt, date2pdate("20220400"));
  EXPECT_EQ(std::nullopt, date2pdate("20220232"));
  EXPECT_EQ(std::nullopt, date2pdate("20241331"));
  EXPECT_EQ(std::nullopt, date2pdate("+0241231"));
  EXPECT_EQ(std::nullopt, date2pdate("a0241b31"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, date2pdateBigint) {
  const auto date2pdate = [&](const std::optional<int64_t>& input) {
    return evaluateOnce<std::string>("date2pdate(c0)", input);
  };

  // Simple tests
  EXPECT_EQ("2023-05-17", date2pdate(20230517));
  EXPECT_EQ("2021-02-03", date2pdate(20210203));

  // Illegal input
  EXPECT_EQ(std::nullopt, date2pdate(1));
  EXPECT_EQ(std::nullopt, date2pdate(123));
  EXPECT_EQ(std::nullopt, date2pdate(12345678));
  EXPECT_EQ(std::nullopt, date2pdate(20232301));
  EXPECT_EQ(std::nullopt, date2pdate(20231299));
  EXPECT_EQ(std::nullopt, date2pdate(20231200));
  EXPECT_EQ(std::nullopt, date2pdate(20230000));
}

TEST_F(SparkSqlDateTimeFunctionsTest, date2pdateInteger) {
  const auto date2pdate = [&](const std::optional<int32_t>& input) {
    return evaluateOnce<std::string>("date2pdate(c0)", input);
  };

  // Simple tests
  EXPECT_EQ("2023-05-17", date2pdate(20230517));
  EXPECT_EQ("2021-02-03", date2pdate(20210203));

  // Illegal input
  EXPECT_EQ(std::nullopt, date2pdate(1));
  EXPECT_EQ(std::nullopt, date2pdate(123));
  EXPECT_EQ(std::nullopt, date2pdate(12345678));
  EXPECT_EQ(std::nullopt, date2pdate(20232301));
  EXPECT_EQ(std::nullopt, date2pdate(20231299));
  EXPECT_EQ(std::nullopt, date2pdate(20231200));
  EXPECT_EQ(std::nullopt, date2pdate(20230000));
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateTrunc) {
  const auto dateTrunc = [&](const std::string& format,
                             std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_trunc('{}', c0)", format), timestamp);
  };

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(std::nullopt, dateTrunc("", Timestamp(0, 0)));
  EXPECT_EQ(std::nullopt, dateTrunc("y", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
  EXPECT_EQ(Timestamp(-1, 0), dateTrunc("second", Timestamp(-1, 0)));
  EXPECT_EQ(Timestamp(-1, 0), dateTrunc("second", Timestamp(-1, 123)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("day", Timestamp(0, 123)));
  EXPECT_EQ(
      Timestamp(998474645, 321'001'000),
      dateTrunc("microsecond", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474645, 321'000'000),
      dateTrunc("millisecond", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      dateTrunc("second", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      dateTrunc("minute", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474400, 0),
      dateTrunc("hour", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998438400, 0),
      dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998438400, 0),
      dateTrunc("dd", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998265600, 0),
      dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996624000, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996624000, 0),
      dateTrunc("mon", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996624000, 0),
      dateTrunc("mm", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993945600, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978307200, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978307200, 0),
      dateTrunc("yyyy", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978307200, 0),
      dateTrunc("yy", Timestamp(998'474'645, 321'001'234)));

  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));

  EXPECT_EQ(Timestamp(-57600, 0), dateTrunc("day", Timestamp(0, 0)));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      dateTrunc("second", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      dateTrunc("minute", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474400, 0),
      dateTrunc("hour", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998463600, 0),
      dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998290800, 0),
      dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996649200, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993970800, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978336000, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));
}

TEST_F(SparkSqlDateTimeFunctionsTest, monthsBetween) {
  const auto monthsBetween = [&](std::optional<Timestamp> timestamp1,
                                 std::optional<Timestamp> timestamp2,
                                 std::optional<std::string> timeZone) {
    return evaluateOnce<double>(
        "months_between(c0, c1, c2, c3)",
        timestamp1,
        timestamp2,
        std::optional<bool>(true),
        timeZone);
  };

  using util::fromTimestampString;

  // Simple tests
  EXPECT_FLOAT_EQ(
      -13.0,
      monthsBetween(
          fromTimestampString("2019-02-28 10:00:00.500", nullptr),
          fromTimestampString("2020-03-28 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      14.06451613,
      monthsBetween(
          fromTimestampString("2021-05-30 10:00:00.500", nullptr),
          fromTimestampString("2020-03-28 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      0.03225806,
      monthsBetween(
          fromTimestampString("2024-05-30 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      0.0,
      monthsBetween(
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      -0.03225806,
      monthsBetween(
          fromTimestampString("2024-05-28 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      0.96774194,
      monthsBetween(
          fromTimestampString("2024-06-28 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      1.0,
      monthsBetween(
          fromTimestampString("2024-06-29 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          "Asia/Shanghai")
          .value());

  // Test for daylight saving. Daylight saving in US starts at 2021-03-14
  // 02:00:00 PST.
  // When adjust_timestamp_to_timezone is off, Daylight saving occurs in UTC
  EXPECT_FLOAT_EQ(
      -1.0,
      monthsBetween(
          fromTimestampString("2021-03-14 01:00:00.000", nullptr),
          fromTimestampString("2021-04-14 01:00:00.000", nullptr),
          "Asia/Shanghai")
          .value());

  // When adjust_timestamp_to_timezone is on, respect Daylight saving in the
  // session time zone
  // setQueryTimeZone("America/Los_Angeles");

  EXPECT_FLOAT_EQ(
      -1.0,
      monthsBetween(
          fromTimestampString("2021-03-14 09:00:00.000", nullptr),
          fromTimestampString("2021-04-14 09:00:00.000", nullptr),
          "America/Los_Angeles")
          .value());
  // Test for respecting the last day of a year-month
  EXPECT_FLOAT_EQ(
      -12.96774194,
      monthsBetween(
          fromTimestampString("2019-01-30 10:00:00.500", nullptr),
          fromTimestampString("2020-02-29 10:00:00.500", nullptr),
          "America/Los_Angeles")
          .value());

  EXPECT_FLOAT_EQ(
      -13.0,
      monthsBetween(
          fromTimestampString("2019-01-31 10:00:00.500", nullptr),
          fromTimestampString("2020-02-29 10:00:00.500", nullptr),
          "America/Los_Angeles")
          .value());

  EXPECT_FLOAT_EQ(
      -12.90322581,
      monthsBetween(
          fromTimestampString("2019-01-31 10:00:00.500", nullptr),
          fromTimestampString("2020-02-28 10:00:00.500", nullptr),
          "America/Los_Angeles")
          .value());

  EXPECT_FLOAT_EQ(
      12.0,
      monthsBetween(
          fromTimestampString("2020-02-29 10:00:00.500", nullptr),
          fromTimestampString("2019-02-28 10:00:00.500", nullptr),
          "America/Los_Angeles")
          .value());

  // setQueryTimeZone("Asia/Shanghai");

  EXPECT_FLOAT_EQ(
      15.0,
      monthsBetween(
          fromTimestampString("2021-04-30", nullptr),
          fromTimestampString("2020-01-31", nullptr),
          "Asia/Shanghai")
          .value());

  EXPECT_FLOAT_EQ(
      26.967741,
      monthsBetween(
          fromTimestampString("2022-04-30", nullptr),
          fromTimestampString("2020-01-31", nullptr),
          "-08:00")
          .value());

  EXPECT_FLOAT_EQ(
      11.0,
      monthsBetween(
          fromTimestampString("2021-03-31", nullptr),
          fromTimestampString("2020-04-30", nullptr),
          "+00:00")
          .value());
}

TEST_F(SparkSqlDateTimeFunctionsTest, makeYMInterval) {
  const auto fromYearAndMonth = [&](const std::optional<int32_t>& year,
                                    const std::optional<std::int32_t>& month) {
    auto result = evaluateOnce<int32_t, int32_t>(
        "make_ym_interval(c0, c1)",
        {year, month},
        {INTEGER(), INTEGER()},
        std::nullopt,
        {INTERVAL_YEAR_MONTH()});
    BOLT_CHECK(result.has_value());
    return INTERVAL_YEAR_MONTH()->valueToString(result.value());
  };
  const auto fromYear = [&](const std::optional<int32_t>& year) {
    auto result = evaluateOnce<int32_t, int32_t>(
        "make_ym_interval(c0)",
        {year},
        {INTEGER()},
        std::nullopt,
        {INTERVAL_YEAR_MONTH()});
    BOLT_CHECK(result.has_value());
    return INTERVAL_YEAR_MONTH()->valueToString(result.value());
  };

  EXPECT_EQ(fromYearAndMonth(1, 2), "1-2");
  EXPECT_EQ(fromYearAndMonth(0, 1), "0-1");
  EXPECT_EQ(fromYearAndMonth(1, 100), "9-4");
  EXPECT_EQ(fromYear(0), "0-0");
  EXPECT_EQ(fromYear(178956970), "178956970-0");
  EXPECT_EQ(fromYear(-178956970), "-178956970-0");
  {
    // Test signature for no year and month.
    auto result = evaluateOnce<int32_t>(
        "make_ym_interval()",
        makeRowVector(ROW({}), 1),
        std::nullopt,
        {INTERVAL_YEAR_MONTH()});
    BOLT_CHECK(result.has_value());
    EXPECT_EQ(INTERVAL_YEAR_MONTH()->valueToString(result.value()), "0-0");
  }

  BOLT_ASSERT_THROW(
      fromYearAndMonth(178956970, 8),
      "Integer overflow in make_ym_interval(178956970, 8)");
  BOLT_ASSERT_THROW(
      fromYearAndMonth(-178956970, -9),
      "Integer overflow in make_ym_interval(-178956970, -9)");
  BOLT_ASSERT_THROW(
      fromYearAndMonth(178956971, 0),
      "Integer overflow in make_ym_interval(178956971, 0)");
  BOLT_ASSERT_THROW(
      fromYear(178956971), "Integer overflow in make_ym_interval(178956971)");
  BOLT_ASSERT_THROW(
      fromYear(-178956971), "Integer overflow in make_ym_interval(-178956971)");
}

TEST_F(SparkSqlDateTimeFunctionsTest, microsToTimestamp) {
  const auto microsToTimestamp = [&](int64_t micros) {
    return evaluateOnce<Timestamp, int64_t>("timestamp_micros(c0)", micros);
  };
  EXPECT_EQ(
      microsToTimestamp(1000000),
      util::fromTimestampString("1970-01-01 00:00:01", nullptr));
  EXPECT_EQ(
      microsToTimestamp(1230219000123123),
      util::fromTimestampString("2008-12-25 15:30:00.123123", nullptr));

  EXPECT_EQ(
      microsToTimestamp(kMaxTinyint),
      util::fromTimestampString("1970-01-01 00:00:00.000127", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMinTinyint),
      util::fromTimestampString("1969-12-31 23:59:59.999872", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMaxSmallint),
      util::fromTimestampString("1970-01-01 00:00:00.032767", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMinSmallint),
      util::fromTimestampString("1969-12-31 23:59:59.967232", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMax),
      util::fromTimestampString("1970-01-01 00:35:47.483647", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMin),
      util::fromTimestampString("1969-12-31 23:24:12.516352", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMaxBigint),
      util::fromTimestampString("294247-01-10 04:00:54.775807", nullptr));
  EXPECT_EQ(
      microsToTimestamp(kMinBigint),
      util::fromTimestampString("-290308-12-21 19:59:05.224192", nullptr));
}

TEST_F(SparkSqlDateTimeFunctionsTest, millisToTimestamp) {
  const auto millisToTimestamp = [&](int64_t millis) {
    return evaluateOnce<Timestamp, int64_t>("timestamp_millis(c0)", millis);
  };
  EXPECT_EQ(
      millisToTimestamp(1000),
      util::fromTimestampString("1970-01-01 00:00:01", nullptr));
  EXPECT_EQ(
      millisToTimestamp(1230219000123),
      util::fromTimestampString("2008-12-25 15:30:00.123", nullptr));

  EXPECT_EQ(
      millisToTimestamp(kMaxTinyint),
      util::fromTimestampString("1970-01-01 00:00:00.127", nullptr));
  EXPECT_EQ(
      millisToTimestamp(kMinTinyint),
      util::fromTimestampString("1969-12-31 23:59:59.872", nullptr));
  EXPECT_EQ(
      millisToTimestamp(kMaxSmallint),
      util::fromTimestampString("1970-01-01 00:00:32.767", nullptr));
  EXPECT_EQ(
      millisToTimestamp(kMinSmallint),
      util::fromTimestampString("1969-12-31 23:59:27.232", nullptr));
  EXPECT_EQ(
      millisToTimestamp(kMax),
      util::fromTimestampString("1970-01-25 20:31:23.647", nullptr));
  EXPECT_EQ(
      millisToTimestamp(kMin),
      util::fromTimestampString("1969-12-07 03:28:36.352", nullptr));
  // ZJ: Temporarily disable tests below when util::fromTimestampString returns
  // error "Unable to parse timestamp value" EXPECT_EQ(
  //     millisToTimestamp(kMaxBigint),
  //     util::fromTimestampString("292278994-08-17 07:12:55.807", nullptr));
  // EXPECT_EQ(
  //     millisToTimestamp(kMinBigint),
  //     util::fromTimestampString("-292275055-05-16 16:47:04.192", nullptr));
}

TEST_F(SparkSqlDateTimeFunctionsTest, timestampToMicros) {
  const auto timestampToMicros = [&](const StringView time) {
    return evaluateOnce<int64_t, Timestamp>(
        "unix_micros(c0)", util::fromTimestampString(time, nullptr));
  };
  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:01"), 1000000);
  EXPECT_EQ(timestampToMicros("2008-12-25 15:30:00.123123"), 1230219000123123);

  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:00.000127"), kMaxTinyint);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:59:59.999872"), kMinTinyint);
  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:00.032767"), kMaxSmallint);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:59:59.967232"), kMinSmallint);
  EXPECT_EQ(timestampToMicros("1970-01-01 00:35:47.483647"), kMax);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:24:12.516352"), kMin);
  // ZJ: Temporarily disable due to " Unable to parse timestamp value: "xxx",
  // expected format is (YYYY-MM-DD HH:MM:SS[.MS])"
  // EXPECT_EQ(timestampToMicros("294247-01-10 04:00:54.775807"), kMaxBigint);
  // EXPECT_EQ(
  //     timestampToMicros("-290308-12-21 19:59:06.224192"), kMinBigint +
  //     1000000);
}

TEST_F(SparkSqlDateTimeFunctionsTest, timestampToMillis) {
  const auto timestampToMillis = [&](const StringView time) {
    return evaluateOnce<int64_t, Timestamp>(
        "unix_millis(c0)", util::fromTimestampString(time, nullptr));
  };
  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:01"), 1000);
  EXPECT_EQ(timestampToMillis("2008-12-25 15:30:00.123"), 1230219000123);

  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:00.127"), kMaxTinyint);
  EXPECT_EQ(timestampToMillis("1969-12-31 23:59:59.872"), kMinTinyint);
  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:32.767"), kMaxSmallint);
  EXPECT_EQ(timestampToMillis("1969-12-31 23:59:27.232"), kMinSmallint);
  EXPECT_EQ(timestampToMillis("1970-01-25 20:31:23.647"), kMax);
  EXPECT_EQ(timestampToMillis("1969-12-07 03:28:36.352"), kMin);
  // ZJ: Temporarily disable due to " Unable to parse timestamp value: "xxx",
  // expected format is (YYYY-MM-DD HH:MM:SS[.MS])"
  // EXPECT_EQ(timestampToMillis("292278994-08-17T07:12:55.807"), kMaxBigint);
  // EXPECT_EQ(timestampToMillis("-292275055-05-16T16:47:04.192"), kMinBigint);
}

TEST_F(SparkSqlDateTimeFunctionsTest, civilDateTimeMicroRange) {
  const auto maxSparkRange = util::toCivilDateTime(
      Timestamp::fromMicros(kMaxBigint),
      /*allowOverflow*/ false,
      /*isPrecision*/ false);
  EXPECT_EQ(294247, maxSparkRange.date.year);
  EXPECT_EQ(1, maxSparkRange.date.month);
  EXPECT_EQ(10, maxSparkRange.date.day);

  const Timestamp overflowTs(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  BOLT_ASSERT_THROW(
      util::toCivilDateTime(
          overflowTs, /*allowOverflow*/ false, /*isPrecision*/ false),
      "Could not convert Timestamp(9223372036854775, 999999999) to microseconds");
}

TEST_F(SparkSqlDateTimeFunctionsTest, secondsToTimestamp) {
  // Tests using integer seconds as input.
  EXPECT_EQ(
      secondsToTimestamp<int8_t>(1), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<int8_t>(-1), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<int16_t>(1), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<int16_t>(-1), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<int32_t>(1), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<int32_t>(-1), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<int64_t>(1), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<int64_t>(-1), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<int64_t>(1230219000),
      parseTimestamp("2008-12-25 15:30:00"));
  EXPECT_EQ(
      secondsToTimestamp<int64_t>(253402300799),
      parseTimestamp("9999-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<int64_t>(-62167219200),
      parseTimestamp("0000-01-01 0:0:0"));
  EXPECT_EQ(
      secondsToTimestamp<int8_t>(kMaxTinyint),
      parseTimestamp("1970-01-01 00:02:07"));
  EXPECT_EQ(
      secondsToTimestamp<int8_t>(kMinTinyint),
      parseTimestamp("1969-12-31 23:57:52"));
  EXPECT_EQ(
      secondsToTimestamp<int16_t>(kMaxSmallint),
      parseTimestamp("1970-01-01 09:06:07"));
  EXPECT_EQ(
      secondsToTimestamp<int16_t>(kMinSmallint),
      parseTimestamp("1969-12-31 14:53:52"));
  EXPECT_EQ(
      secondsToTimestamp<int32_t>(kMax), parseTimestamp("2038-01-19 03:14:07"));
  EXPECT_EQ(
      secondsToTimestamp<int32_t>(kMin), parseTimestamp("1901-12-13 20:45:52"));

  // Tests using floating-point seconds as input.
  EXPECT_EQ(
      secondsToTimestamp<float>(1.0), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<float>(-1.0), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<float>(1.1234567),
      parseTimestamp("1970-01-01 00:00:01.123456"));
  EXPECT_EQ(
      secondsToTimestamp<float>(kMaxFloat),
      parseTimestamp("+294247-01-10 04:00:54.775807"));
  EXPECT_EQ(
      secondsToTimestamp<float>(kLowestFloat),
      parseTimestamp("-290308-12-21 19:59:05.224192"));

  // Tests using double seconds as input.
  EXPECT_EQ(
      secondsToTimestamp<double>(1.0), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      secondsToTimestamp<double>(-1.0), parseTimestamp("1969-12-31 23:59:59"));
  EXPECT_EQ(
      secondsToTimestamp<double>(1230219000.123),
      parseTimestamp("2008-12-25 15:30:00.123"));
  EXPECT_EQ(
      secondsToTimestamp<double>(0.127),
      parseTimestamp("1970-01-01 00:00:00.127"));
  EXPECT_EQ(
      secondsToTimestamp<double>(-0.128),
      parseTimestamp("1969-12-31 23:59:59.872"));
  EXPECT_EQ(
      secondsToTimestamp<double>(32.767),
      parseTimestamp("1970-01-01 00:00:32.767"));
  EXPECT_EQ(
      secondsToTimestamp<double>(-32.768),
      parseTimestamp("1969-12-31 23:59:27.232"));
  EXPECT_EQ(
      secondsToTimestamp<double>(20.523),
      parseTimestamp("1970-01-01 00:00:20.523"));
  EXPECT_EQ(
      secondsToTimestamp(-20.524), parseTimestamp("1969-12-31 23:59:39.476"));
  EXPECT_EQ(
      secondsToTimestamp<double>(2147483647.123),
      parseTimestamp("2038-01-19 03:14:07.123"));
  EXPECT_EQ(
      secondsToTimestamp<double>(-2147483648.567),
      parseTimestamp("1901-12-13 20:45:51.433"));
  EXPECT_EQ(
      secondsToTimestamp<double>(1.1234567),
      parseTimestamp("1970-01-01 00:00:01.123456"));
  EXPECT_EQ(
      secondsToTimestamp<double>(kLowestDouble),
      parseTimestamp("-290308-12-21 19:59:05.224192"));
  EXPECT_EQ(
      secondsToTimestamp<double>(kMaxDouble),
      parseTimestamp("+294247-01-10 04:00:54.775807"));
}

TEST_F(SparkSqlDateTimeFunctionsTest, toUtcTimestamp) {
  const auto toUtcTimestamp = [&](std::string_view ts, const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(
        util::fromTimestampString(ts.data(), ts.length(), nullptr));
    auto result = evaluateOnce<Timestamp>(
        "to_utc_timestamp(c0, c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  // Since TimestampToStringOptions::dateTimeSeparator is ' ' internally at
  // Bytedance instead of 'T', we modified the expected result.
  EXPECT_EQ(
      "2015-07-24 07:00:00.000000000",
      toUtcTimestamp("2015-07-24 00:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 08:00:00.000000000",
      toUtcTimestamp("2015-01-24 00:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      toUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      toUtcTimestamp("2015-01-24 05:30:00", "Asia/Kolkata"));
  BOLT_ASSERT_THROW(
      toUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty");
}

TEST_F(SparkSqlDateTimeFunctionsTest, fromUtcTimestamp) {
  const auto fromUtcTimestamp = [&](std::string_view ts,
                                    const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(
        util::fromTimestampString(ts.data(), ts.length(), nullptr));
    auto result = evaluateOnce<Timestamp>(
        "from_utc_timestamp(c0, c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  // Since TimestampToStringOptions::dateTimeSeparator is ' ' internally at
  // Bytedance instead of 'T', we modified the expected result.
  EXPECT_EQ(
      "2015-07-24 00:00:00.000000000",
      fromUtcTimestamp("2015-07-24 07:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      fromUtcTimestamp("2015-01-24 08:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      fromUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24 05:30:00.000000000",
      fromUtcTimestamp("2015-01-24 00:00:00", "Asia/Kolkata"));
  BOLT_ASSERT_THROW(
      fromUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty");
}

TEST_F(SparkSqlDateTimeFunctionsTest, toFromUtcTimestamp) {
  const auto toFromUtcTimestamp = [&](std::string_view ts,
                                      const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(
        util::fromTimestampString(ts.data(), ts.length(), nullptr));
    auto result = evaluateOnce<Timestamp>(
        "to_utc_timestamp(from_utc_timestamp(c0, c1), c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  // Since TimestampToStringOptions::dateTimeSeparator is ' ' internally at
  // Bytedance instead of 'T', we modified the expected result.
  EXPECT_EQ(
      "2015-07-24 07:00:00.000000000",
      toFromUtcTimestamp("2015-07-24 07:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 08:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 08:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24 00:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 00:00:00", "Asia/Kolkata"));
  BOLT_ASSERT_THROW(
      toFromUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty");
}

TEST_F(SparkSqlDateTimeFunctionsTest, dateFromUnixDate) {
  const auto dateFromUnixDate = [&](std::optional<int32_t> value) {
    return evaluateOnce<int32_t>("date_from_unix_date(c0)", value);
  };

  // Basic tests
  EXPECT_EQ(parseDate("1970-01-01"), dateFromUnixDate(0));
  EXPECT_EQ(parseDate("1970-01-02"), dateFromUnixDate(1));
  EXPECT_EQ(parseDate("1969-12-31"), dateFromUnixDate(-1));
  EXPECT_EQ(parseDate("1970-02-01"), dateFromUnixDate(31));
  EXPECT_EQ(parseDate("1971-01-31"), dateFromUnixDate(395));
  EXPECT_EQ(parseDate("1971-01-01"), dateFromUnixDate(365));

  // Leap year tests
  EXPECT_EQ(parseDate("1972-02-29"), dateFromUnixDate(365 + 365 + 30 + 29));
  EXPECT_EQ(parseDate("1971-03-01"), dateFromUnixDate(365 + 30 + 28 + 1));

  // Min and max value tests
  EXPECT_EQ(parseDate("5881580-07-11"), dateFromUnixDate(kMax));
  EXPECT_EQ(parseDate("-5877641-06-23"), dateFromUnixDate(kMin));
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
