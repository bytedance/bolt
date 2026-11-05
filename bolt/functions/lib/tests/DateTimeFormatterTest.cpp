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

#include "bolt/common/base/SparkCompatibility.h"

#include <bolt/common/base/BoltException.h>
#include <bolt/type/StringView.h>
#include <date/tz.h>
#include "bolt/functions/lib/DateTimeFormatter.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/lib/DateTimeFormatterBuilder.h"
#include "bolt/functions/lib/DateTimeFormatterCache.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"

#include <gtest/gtest.h>
#include <unicode/timezone.h>
#include <string>
using namespace bytedance::bolt;
namespace bytedance::bolt::functions {

class DateTimeFormatterTest : public testing::Test {
 protected:
  static constexpr std::string_view monthsFull[] = {
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December",
  };

  static constexpr std::string_view monthsShort[] = {
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
  };

  void testTokenRange(
      char specifier,
      int numTokenStart,
      int numTokenEnd,
      const DateTimeFormatSpecifier& token) {
    for (size_t i = numTokenStart; i <= numTokenEnd; i++) {
      std::string pattern(i, specifier);
      std::vector<DateTimeToken> expected;
      expected = {DateTimeToken(FormatPattern{token, i})};
      EXPECT_EQ(expected, buildJodaDateTimeFormatter(pattern)->tokens());
    }
  }

  DateTimeResultValue parseJoda(
      const std::string_view& input,
      const std::string_view& format) {
    return buildJodaDateTimeFormatter(format)->parse(input).value();
  }

  Timestamp parseMysql(
      const std::string_view& input,
      const std::string_view& format) {
    return buildMysqlDateTimeFormatter(format)->parse(input).value().timestamp;
  }

  // Parses and returns the timezone converted back to string, to ease
  // verifiability.
  std::string parseTZ(
      const std::string_view& input,
      const std::string_view& format) {
    auto result = buildJodaDateTimeFormatter(format)->parse(input).value();
    if (result.timezoneId == 0) {
      return "+00:00";
    }
    return tz::getTimeZoneName(result.timezoneId);
  }

  std::string formatMysqlDateTime(
      const std::string& format,
      const Timestamp& timestamp,
      const tz::TimeZone* timezone) const {
    auto formatter = buildMysqlDateTimeFormatter(format);
    const auto maxSize = formatter->maxResultSize(timezone);
    std::string result(maxSize, '\0');
    auto resultSize =
        formatter->format(timestamp, timezone, maxSize, result.data());
    result.resize(resultSize);
    return result;
  }
};

TEST_F(DateTimeFormatterTest, fixedLengthTokenBuilder) {
  DateTimeFormatterBuilder builder(100);
  std::vector<DateTimeToken> expectedTokens;

  // Test fixed length tokens
  builder.appendEra();
  builder.appendLiteral("-");
  auto formatter =
      builder.appendHalfDayOfDay().setType(DateTimeFormatterType::JODA).build();

  std::string_view actualLiterals(
      formatter->literalBuf().get(), formatter->bufSize());
  EXPECT_EQ(actualLiterals, "-");
  expectedTokens = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
  EXPECT_EQ(formatter->tokens(), expectedTokens);
}

TEST_F(DateTimeFormatterTest, variableLengthTokenBuilder) {
  // Test variable length tokens
  DateTimeFormatterBuilder builder(100);
  std::vector<DateTimeToken> expectedTokens;

  auto formatter = builder.appendCenturyOfEra(3)
                       .appendLiteral("-")
                       .appendYearOfEra(4)
                       .appendLiteral("/")
                       .appendWeekYear(3)
                       .appendLiteral("//")
                       .appendWeekOfWeekYear(3)
                       .appendLiteral("-00-")
                       .appendDayOfWeek0Based(3)
                       .appendDayOfWeek1Based(4)
                       .appendLiteral("--")
                       .appendDayOfWeekText(6)
                       .appendLiteral("---")
                       .appendYear(5)
                       .appendLiteral("///")
                       .appendDayOfYear(4)
                       .appendMonthOfYear(2)
                       .appendMonthOfYearText(4)
                       .appendDayOfMonth(4)
                       .appendHourOfHalfDay(2)
                       .appendClockHourOfHalfDay(3)
                       .appendClockHourOfDay(2)
                       .appendHourOfDay(2)
                       .appendMinuteOfHour(2)
                       .appendSecondOfMinute(1)
                       .appendFractionOfSecond(6)
                       .appendTimeZone(3)
                       .appendTimeZoneOffsetId(3)
                       .setType(DateTimeFormatterType::JODA)
                       .build();
  auto actualLiterals =
      std::string_view(formatter->literalBuf().get(), formatter->bufSize());
  EXPECT_EQ(actualLiterals, "-///-00------///");
  expectedTokens = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::CENTURY_OF_ERA, 3}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR_OF_ERA, 4}),
      DateTimeToken("/"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::WEEK_YEAR, 3}),
      DateTimeToken("//"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR, 3}),
      DateTimeToken("-00-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED, 4}),
      DateTimeToken("--"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT, 6}),
      DateTimeToken("---"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 5}),
      DateTimeToken("///"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_YEAR, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_HALFDAY, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::SECOND_OF_MINUTE, 1}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::FRACTION_OF_SECOND, 6}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::TIMEZONE, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID, 3})};
  EXPECT_EQ(formatter->tokens(), expectedTokens);
}

class JodaDateTimeFormatterTest : public DateTimeFormatterTest {};

TEST_F(JodaDateTimeFormatterTest, validJodaBuild) {
  std::vector<DateTimeToken> expected;

  // G specifier case
  expected = {DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("G")->tokens());
  // minRepresentDigits should be unchanged with higher number of specifier for
  // ERA
  expected = {DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2})};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("GGGG")->tokens());

  // C specifier case
  testTokenRange('C', 1, 3, DateTimeFormatSpecifier::CENTURY_OF_ERA);

  // Y specifier case
  testTokenRange('Y', 1, 4, DateTimeFormatSpecifier::YEAR_OF_ERA);

  // x specifier case
  testTokenRange('x', 1, 4, DateTimeFormatSpecifier::WEEK_YEAR);

  // w specifier case
  testTokenRange('w', 1, 4, DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR);

  // e specifier case
  testTokenRange('e', 1, 4, DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED);

  // E specifier case
  testTokenRange('E', 1, 4, DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT);

  // y specifier case
  testTokenRange('y', 1, 4, DateTimeFormatSpecifier::YEAR);

  // D specifier case
  testTokenRange('D', 1, 4, DateTimeFormatSpecifier::DAY_OF_YEAR);

  // M specifier case
  testTokenRange('M', 1, 2, DateTimeFormatSpecifier::MONTH_OF_YEAR);
  testTokenRange('M', 3, 4, DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT);

  // d specifier case
  testTokenRange('d', 1, 4, DateTimeFormatSpecifier::DAY_OF_MONTH);

  if (!::bytedance::bolt::kSparkCompatible) {
    // a specifier case
    expected = {DateTimeToken(
        FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
    EXPECT_EQ(expected, buildJodaDateTimeFormatter("a")->tokens());
    // minRepresentDigits should be unchanged with higher number of specifier
    // for HALFDAY_OF_DAY
    expected = {DateTimeToken(
        FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2})};
    EXPECT_EQ(expected, buildJodaDateTimeFormatter("aa")->tokens());
  }

  // K specifier case
  testTokenRange('K', 1, 4, DateTimeFormatSpecifier::HOUR_OF_HALFDAY);

  // h specifier case
  testTokenRange('h', 1, 4, DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY);

  // H specifier case
  testTokenRange('H', 1, 4, DateTimeFormatSpecifier::HOUR_OF_DAY);

  // k specifier case
  testTokenRange('k', 1, 4, DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY);

  // m specifier case
  testTokenRange('m', 1, 4, DateTimeFormatSpecifier::MINUTE_OF_HOUR);

  // s specifier
  testTokenRange('s', 1, 4, DateTimeFormatSpecifier::SECOND_OF_MINUTE);

  // S specifier
  testTokenRange('S', 1, 4, DateTimeFormatSpecifier::FRACTION_OF_SECOND);

  // z specifier
  testTokenRange('z', 1, 4, DateTimeFormatSpecifier::TIMEZONE);

  // Z specifier
  testTokenRange('Z', 1, 2, DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID);
  for (const auto* pattern : {"ZZZ", "ZZZZ"}) {
    EXPECT_EQ(
        buildJodaDateTimeFormatter(pattern)->tokens(),
        std::vector<DateTimeToken>{DateTimeToken(
            FormatPattern{DateTimeFormatSpecifier::TIMEZONE_ID, 3})});
  }

  // Literal case
  expected = {DateTimeToken(" ")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter(" ")->tokens());
  expected = {DateTimeToken("1234567890")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("1234567890")->tokens());
  expected = {DateTimeToken("'")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("''")->tokens());
  expected = {DateTimeToken("abcdefghijklmnopqrstuvwxyz")};
  EXPECT_EQ(
      expected,
      buildJodaDateTimeFormatter("'abcdefghijklmnopqrstuvwxyz'")->tokens());
  expected = {DateTimeToken("'abcdefg'hijklmnop'qrstuv'wxyz'")};
  EXPECT_EQ(
      expected,
      buildJodaDateTimeFormatter("'''abcdefg''hijklmnop''qrstuv''wxyz'''")
          ->tokens());
  expected = {DateTimeToken("'1234abcd")};
  EXPECT_EQ(expected, buildJodaDateTimeFormatter("''1234'abcd'")->tokens());

  // Specifier combinations
  expected = {
      DateTimeToken("'"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::CENTURY_OF_ERA, 3}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR_OF_ERA, 4}),
      DateTimeToken("/"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::WEEK_YEAR, 3}),
      DateTimeToken("//"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR, 3}),
      DateTimeToken("-00-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED, 4}),
      DateTimeToken("--"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT, 6}),
      DateTimeToken("---"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 5}),
      DateTimeToken("///"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_YEAR, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken("-"),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 4}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_HALFDAY, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY, 3}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HOUR_OF_DAY, 2}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::SECOND_OF_MINUTE, 1}),
      DateTimeToken(
          FormatPattern{DateTimeFormatSpecifier::FRACTION_OF_SECOND, 6}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::TIMEZONE, 3}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::TIMEZONE_ID, 3}),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::ERA, 2}),
      DateTimeToken("abcdefghijklmnopqrstuvwxyz"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::HALFDAY_OF_DAY, 2}),
  };

  if (!::bytedance::bolt::kSparkCompatible) {
    EXPECT_EQ(
        expected,
        buildJodaDateTimeFormatter(
            "''CCC-YYYY/xxx//www-00-eeee--EEEEEE---yyyyy///DDDDMM-MMMMddddKKhhhkkHHmmsSSSSSSzzzZZZGGGG'abcdefghijklmnopqrstuvwxyz'aaa")
            ->tokens());
  }
}

TEST_F(JodaDateTimeFormatterTest, invalidJodaBuild) {
  // Invalid specifiers
  EXPECT_THROW(buildJodaDateTimeFormatter("q"), BoltUserError);
  EXPECT_THROW(buildJodaDateTimeFormatter("r"), BoltUserError);
  EXPECT_THROW(buildJodaDateTimeFormatter("g"), BoltUserError);

  // Unclosed literal sequence
  EXPECT_THROW(buildJodaDateTimeFormatter("'abcd"), BoltUserError);

  // Empty format string
  EXPECT_THROW(buildJodaDateTimeFormatter(""), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, invalid) {
  // Parse:
  EXPECT_THROW(parseJoda("", ""), BoltUserError);
  EXPECT_THROW(parseJoda(" ", ""), BoltUserError);
  EXPECT_THROW(parseJoda("", " "), BoltUserError);
  EXPECT_THROW(parseJoda("", "Y '"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseJodaEra) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Normal era cases
  EXPECT_EQ(
      util::fromTimestampString("-100-01-01", nullptr),
      parseJoda("BC 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("101-01-01", nullptr),
      parseJoda("AD 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-100-01-01", nullptr),
      parseJoda("bc 101", "G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("101-01-01", nullptr),
      parseJoda("ad 101", "G Y").timestamp);

  // Era specifier with 'y' specifier
  EXPECT_EQ(
      util::fromTimestampString("101-01-01", nullptr),
      parseJoda("BC 101", "G y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01", nullptr),
      parseJoda("BC 2012", "G y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-101-01-01", nullptr),
      parseJoda("AD 2012 -101", "G Y y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01", nullptr),
      parseJoda("BC 101 2012", "G Y y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01", nullptr),
      parseJoda("BC 2000 2012", "G y Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01", nullptr),
      parseJoda("BC 2000 2012 BC", "G y Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2014-01-01", nullptr),
      parseJoda("BC 1 BC 2015", "G y G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01", nullptr),
      parseJoda("BC 0 BC 2015 AD", "G y G Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2015-01-01", nullptr),
      parseJoda("AD 0 AD 2015", "G y G Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-2011-01-01", nullptr),
      parseJoda("BC 0 BC 2015 2 2012 BC", "G y G Y y Y G").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01", nullptr),
      parseJoda("AD 0 AD 2015 2 2012 AD", "G y G Y y Y G").timestamp);

  // Invalid cases
  EXPECT_THROW(parseJoda("FG", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("AC", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("BD", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("aD", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("Ad", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("bC", "G"), BoltUserError);
  EXPECT_THROW(parseJoda("Bc", "G"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseYearOfEra) {
  // By the default, assume epoch.
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01", nullptr),
      parseJoda(" ", " ").timestamp);

  // Number of times the token is repeated doesn't change the parsing behavior.
  EXPECT_EQ(
      util::fromTimestampString("2134-01-01", nullptr),
      parseJoda("2134", "Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2134-01-01", nullptr),
      parseJoda("2134", "YYYYYYYY").timestamp);

  // Probe the year of era range. Joda only supports positive years.
  EXPECT_EQ(
      util::fromTimestampString("294247-01-01", nullptr),
      parseJoda("294247", "Y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01", nullptr),
      parseJoda("1", "Y").timestamp);
  EXPECT_THROW(parseJoda("292278994", "Y"), BoltUserError);
  EXPECT_THROW(parseJoda("0", "Y"), BoltUserError);
  EXPECT_THROW(parseJoda("-1", "Y"), BoltUserError);
  EXPECT_THROW(parseJoda("  ", " Y "), BoltUserError);
  EXPECT_THROW(parseJoda(" 1 2", "Y Y"), BoltUserError);

  // 2 'Y' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01", nullptr),
      parseJoda("12", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2069-01-01", nullptr),
      parseJoda("69", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01", nullptr),
      parseJoda("70", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-01", nullptr),
      parseJoda("99", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0002-01-01", nullptr),
      parseJoda("2", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01", nullptr),
      parseJoda("210", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01", nullptr),
      parseJoda("1", "YY").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01", nullptr),
      parseJoda("01", "YY").timestamp);

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0005-01-01", nullptr),
      parseJoda("1 2 3 4 5", "Y Y Y Y Y").timestamp);

  // Throws on consumption of plus sign
  EXPECT_THROW(parseJoda("+100", "Y"), BoltUserError);
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative
// years.
TEST_F(JodaDateTimeFormatterTest, parseYear) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  EXPECT_EQ(
      util::fromTimestampString("123-01-01", nullptr),
      parseJoda("123", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("321-01-01", nullptr),
      parseJoda("321", "yyyyyyyy").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("0-01-01", nullptr),
      parseJoda("0", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1-01-01", nullptr),
      parseJoda("-1", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1234-01-01", nullptr),
      parseJoda("-1234", "y").timestamp);

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0-01-01", nullptr),
      parseJoda("123 0", "Y y").timestamp);

  // 2 'y' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-01", nullptr),
      parseJoda("12", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2069-01-01", nullptr),
      parseJoda("69", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01", nullptr),
      parseJoda("70", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-01", nullptr),
      parseJoda("99", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0002-01-01", nullptr),
      parseJoda("2", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01", nullptr),
      parseJoda("210", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01", nullptr),
      parseJoda("1", "yy").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01", nullptr),
      parseJoda("01", "yy").timestamp);

  // Plus sign consumption valid when y operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-01", nullptr),
      parseJoda("+10", "y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("99-02-01", nullptr),
      parseJoda("+99 02", "y M").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("10-10-01", nullptr),
      parseJoda("10 +10", "M y").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("100-02-01", nullptr),
      parseJoda("2+100", "My").timestamp);
  EXPECT_THROW(parseJoda("+10001", "yM"), BoltUserError);
  EXPECT_THROW(parseJoda("++100", "y"), BoltUserError);

  // Probe the year range
  EXPECT_THROW(parseJoda("-292275056", "y"), BoltUserError);
  EXPECT_THROW(parseJoda("292278995", "y"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("292278994-01-01", nullptr),
      parseJoda("292278994", "y").timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseWeekYear) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00", nullptr),
      parseJoda("1970", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00", nullptr),
      parseJoda("2025", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00", nullptr),
      parseJoda("1935", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00", nullptr),
      parseJoda("1990", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00", nullptr),
      parseJoda("204", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00", nullptr),
      parseJoda("-102", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00", nullptr),
      parseJoda("-108", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00", nullptr),
      parseJoda("-1001", "x").timestamp);

  // 2 'x' token case
  EXPECT_EQ(
      util::fromTimestampString("2012-01-02", nullptr),
      parseJoda("12", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2068-12-31", nullptr),
      parseJoda("69", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29", nullptr),
      parseJoda("70", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1999-01-04", nullptr),
      parseJoda("99", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-12-31", nullptr),
      parseJoda("2", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0210-01-01", nullptr),
      parseJoda("210", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0001-01-01", nullptr),
      parseJoda("1", "xx").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2001-01-01", nullptr),
      parseJoda("01", "xx").timestamp);

  // Plus sign consumption valid when x operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-04", nullptr),
      parseJoda("+10", "x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0098-12-29", nullptr),
      parseJoda("+99 01", "x w").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0099-01-05", nullptr),
      parseJoda("+99 02", "x w").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("10-03-08", nullptr),
      parseJoda("10 +10", "w x").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("100-01-11", nullptr),
      parseJoda("2+100", "wx").timestamp);
  EXPECT_THROW(parseJoda("+10001", "xM"), BoltUserError);
  EXPECT_THROW(parseJoda("++100", "x"), BoltUserError);

  // Probe week year range
  EXPECT_THROW(parseJoda("-292275055", "x"), BoltUserError);
  EXPECT_THROW(parseJoda("292278994", "x"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseCenturyOfEra) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Probe century range
  EXPECT_EQ(
      util::fromTimestampString("292278900-01-01 00:00:00", nullptr),
      parseJoda("2922789", "CCCCCCC").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("00-01-01 00:00:00", nullptr),
      parseJoda("0", "C").timestamp);

  // Invalid century values
  EXPECT_THROW(parseJoda("-1", "CCCCCCC"), BoltUserError);
  EXPECT_THROW(parseJoda("2922790", "CCCCCCC"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseJodaMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr),
      parseJoda("1", "M").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01", nullptr),
      parseJoda(" 7", " MM").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01", nullptr),
      parseJoda("11-", "M-").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01", nullptr),
      parseJoda("-12-", "-M-").timestamp);

  EXPECT_THROW(parseJoda("0", "M"), BoltUserError);
  EXPECT_THROW(parseJoda("13", "M"), BoltUserError);
  //   EXPECT_THROW(parseJoda("12345", "M"), BoltUserError);

  // Ensure MMM and MMMM specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    std::string buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseJoda(monthsShort[i], "MMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseJoda(monthsFull[i], "MMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseJoda(monthsShort[i], "MMMM").timestamp);
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseJoda(monthsFull[i], "MMMM").timestamp);
  }

  // Month name invalid parse
  //   EXPECT_THROW(parseJoda("Decembr", "MMM"), BoltUserError);
  //   EXPECT_THROW(parseJoda("Decembr", "MMMM"), BoltUserError);
  //   EXPECT_THROW(parseJoda("Decemberary", "MMM"), BoltUserError);
  //   EXPECT_THROW(parseJoda("Decemberary", "MMMM"), BoltUserError);
  EXPECT_THROW(parseJoda("asdf", "MMM"), BoltUserError);
  EXPECT_THROW(parseJoda("asdf", "MMMM"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfWeek1Base) {
  EXPECT_EQ(
      util::fromTimestampString("2000-01-03", nullptr),
      parseJoda("1", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-04", nullptr),
      parseJoda("2", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-05", nullptr),
      parseJoda("3", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-06", nullptr),
      parseJoda("4", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07", nullptr),
      parseJoda("5", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-08", nullptr),
      parseJoda("6", "u").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-09", nullptr),
      parseJoda("7", "u").timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfMonth) {
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr),
      parseJoda("1", "d").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07", nullptr),
      parseJoda("7 ", "dd ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11", nullptr),
      parseJoda("/11", "/dd").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseJoda("/31/", "/d/").timestamp);

  EXPECT_THROW(parseJoda("0", "d"), BoltUserError);
  EXPECT_THROW(parseJoda("32", "d"), BoltUserError);
  //   EXPECT_THROW(parseJoda("12345", "d"), BoltUserError);

  EXPECT_THROW(parseJoda("02-31", "M-d"), BoltUserError);
  EXPECT_THROW(parseJoda("04-31", "M-d"), BoltUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parseJoda("1 31 20 2", "M d d M"), BoltUserError);
  EXPECT_THROW(parseJoda("2 31 20 4", "M d d M"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseJoda("2 31 1", "M d M").timestamp);

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29", nullptr),
      parseJoda("2000-02-29", "Y-M-d").timestamp);
  EXPECT_THROW(parseJoda("2001-02-29", "Y-M-d"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr),
      parseJoda("1", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07", nullptr),
      parseJoda("7 ", "DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11", nullptr),
      parseJoda("/11", "/DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseJoda("/31/", "/DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-02-01", nullptr),
      parseJoda("32", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29", nullptr),
      parseJoda("60", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-30", nullptr),
      parseJoda("365", "D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31", nullptr),
      parseJoda("366", "D").timestamp);

  // Year specified cases
  EXPECT_EQ(
      util::fromTimestampString("1950-01-01", nullptr),
      parseJoda("1950 1", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-07", nullptr),
      parseJoda("1950 7 ", "y DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11", nullptr),
      parseJoda("1950 /11", "y /DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31", nullptr),
      parseJoda("1950 /31/", "y /DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-02-01", nullptr),
      parseJoda("1950 32", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-03-01", nullptr),
      parseJoda("1950 60", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1950-12-31", nullptr),
      parseJoda("1950 365", "y D").timestamp);
  EXPECT_THROW(parseJoda("1950 366", "Y D"), BoltUserError);

  // Negative year specified cases
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-01", nullptr),
      parseJoda("-1950 1", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07", nullptr),
      parseJoda("-1950 7 ", "y DD ").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11", nullptr),
      parseJoda("-1950 /11", "y /DD").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31", nullptr),
      parseJoda("-1950 /31/", "y /DDD/").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-02-01", nullptr),
      parseJoda("-1950 32", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-03-01", nullptr),
      parseJoda("-1950 60", "y D").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31", nullptr),
      parseJoda("-1950 365", "y D").timestamp);
  EXPECT_THROW(parseJoda("-1950 366", "Y D"), BoltUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parseJoda("2000 366 2001", "y D y"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31", nullptr),
      parseJoda("2001 366 2000", "y D y").timestamp);

  EXPECT_THROW(parseJoda("0", "d"), BoltUserError);
  EXPECT_THROW(parseJoda("367", "d"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7", "H").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00", nullptr),
      parseJoda("23", "HH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0", "HHH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseJoda("10", "HHHHHHHH").timestamp);

  // Hour of day invalid
  EXPECT_THROW(parseJoda("24", "H"), BoltUserError);
  EXPECT_THROW(parseJoda("-1", "H"), BoltUserError);
  //   EXPECT_THROW(parseJoda("123456789", "H"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7", "k").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("24", "kk").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseJoda("1", "kkk").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseJoda("10", "kkkkkkkk").timestamp);

  // Clock hour of day invalid
  EXPECT_THROW(parseJoda("25", "k"), BoltUserError);
  EXPECT_THROW(parseJoda("0", "k"), BoltUserError);
  //   EXPECT_THROW(parseJoda("123456789", "k"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7", "K").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00", nullptr),
      parseJoda("11", "KK").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0", "KKK").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseJoda("10", "KKKKKKKK").timestamp);

  // Hour of half day invalid
  EXPECT_THROW(parseJoda("12", "K"), BoltUserError);
  EXPECT_THROW(parseJoda("-1", "K"), BoltUserError);
  EXPECT_THROW(parseJoda("123456789", "K"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7", "h").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("12", "hh").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseJoda("1", "hhh").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseJoda("10", "hhhhhhhh").timestamp);

  // Clock hour of half day invalid
  EXPECT_THROW(parseJoda("13", "h"), BoltUserError);
  EXPECT_THROW(parseJoda("0", "h"), BoltUserError);
  //   EXPECT_THROW(parseJoda("123456789", "h"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseHalfOfDay) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Half of day has no effect if hour or clockhour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 PM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 AM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 pm", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 am", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0 PM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0 AM", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0 pm", "H a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0 am", "H a").timestamp);

  // clock hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 PM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 AM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 pm", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseJoda("7 am", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("24 PM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("24 AM", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("24 pm", "k a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("24 am", "k a").timestamp);

  // Half of day has effect if hour or clockhour of halfday is provided
  // hour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseJoda("0 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0 AM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00", nullptr),
      parseJoda("6 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00", nullptr),
      parseJoda("6 AM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00", nullptr),
      parseJoda("11 PM", "K a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 11:00:00", nullptr),
      parseJoda("11 AM", "K a").timestamp);

  // clockhour of halfday tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseJoda("1 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseJoda("1 AM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00", nullptr),
      parseJoda("6 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00", nullptr),
      parseJoda("6 AM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseJoda("12 PM", "h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("12 AM", "h a").timestamp);

  // time gives precedent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseJoda("0 1 AM", "H h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseJoda("12 1 PM", "H h a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("1 AM 0", "h a H").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseJoda("1 AM 12", "h a H").timestamp);

  // Half of day still has effect even though hour or clockhour is not
  // provided.
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseJoda("PM", "a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:11:11", nullptr),
      parseJoda("11:11 PM", "mm:ss a").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("2018-04-28 12:59:30", nullptr),
      parseJoda("2018-04-28 59:30 PM", "yyyy-MM-dd mm:ss a").timestamp);
}

TEST_F(JodaDateTimeFormatterTest, parseMinute) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:08:00", nullptr),
      parseJoda("8", "m").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00", nullptr),
      parseJoda("59", "mm").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0/", "mmm/").timestamp);

  EXPECT_THROW(parseJoda("60", "m"), BoltUserError);
  EXPECT_THROW(parseJoda("-1", "m"), BoltUserError);
  //   EXPECT_THROW(parseJoda("123456789", "m"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseSecond) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09", nullptr),
      parseJoda("9", "s").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58", nullptr),
      parseJoda("58", "ss").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseJoda("0/", "s/").timestamp);

  EXPECT_THROW(parseJoda("60", "s"), BoltUserError);
  EXPECT_THROW(parseJoda("-1", "s"), BoltUserError);
  //   EXPECT_THROW(parseJoda("123456789", "s"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseTimezoneOffset) {
  // Broken timezone offsets; allowed formats are either "+00:00" or "+00".
  EXPECT_THROW(parseJoda("", "ZZ"), BoltUserError);
  EXPECT_THROW(parseJoda("0", "ZZ"), BoltUserError);
  EXPECT_THROW(parseJoda("00", "ZZ"), BoltUserError);
  EXPECT_THROW(parseJoda(":00", "ZZ"), BoltUserError);
  EXPECT_THROW(parseJoda("+0", "ZZ"), BoltUserError);
  //   EXPECT_THROW(parseJoda("+00:", "ZZ"), BoltUserError);
  //   EXPECT_THROW(parseJoda("+00:0", "ZZ"), BoltUserError);
  EXPECT_THROW(parseJoda("12", "YYZZ"), BoltUserError);
  //   EXPECT_THROW(parseJoda("ZZ", "Z"), BoltUserError);
  //   EXPECT_THROW(parseJoda("ZZ", "ZZ"), BoltUserError);

  // GMT
  EXPECT_EQ("+00:00", parseTZ("+00:00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("-00:00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("Z", "Z"));
  EXPECT_EQ("+00:00", parseTZ("Z", "ZZ"));

  // Valid long format:
  EXPECT_EQ("+00:01", parseTZ("+00:01", "ZZ"));
  EXPECT_EQ("-00:01", parseTZ("-00:01", "ZZ"));
  EXPECT_EQ("+01:00", parseTZ("+01:00", "ZZ"));
  EXPECT_EQ("+13:59", parseTZ("+13:59", "ZZ"));
  EXPECT_EQ("-07:32", parseTZ("-07:32", "ZZ"));
  EXPECT_EQ("+14:00", parseTZ("+14:00", "ZZ"));
  EXPECT_EQ("-14:00", parseTZ("-14:00", "ZZ"));

  // Valid long format without colon:
  EXPECT_EQ("+00:01", parseTZ("+0001", "ZZ"));
  EXPECT_EQ("+11:00", parseTZ("+1100", "ZZ"));
  EXPECT_EQ("-04:30", parseTZ("-0430", "ZZ"));

  // Invalid long format:
  EXPECT_THROW(parseTZ("+14:01", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("-14:01", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+00:60", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+00:99", "ZZ"), BoltUserError);
  //   EXPECT_THROW(parseTZ("+00:100", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+15:00", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+16:00", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("-15:00", "ZZ"), BoltUserError);

  // GMT short format:
  EXPECT_EQ("+00:00", parseTZ("+00", "ZZ"));
  EXPECT_EQ("+00:00", parseTZ("-00", "ZZ"));

  // Valid short format:
  EXPECT_EQ("+13:00", parseTZ("+13", "ZZ"));
  EXPECT_EQ("-01:00", parseTZ("-01", "ZZ"));
  EXPECT_EQ("-10:00", parseTZ("-10", "ZZ"));
  EXPECT_EQ("+03:00", parseTZ("+03", "ZZ"));
  EXPECT_EQ("-14:00", parseTZ("-14", "ZZ"));
  EXPECT_EQ("+14:00", parseTZ("+14", "ZZ"));

  // Invalid short format:
  EXPECT_THROW(parseTZ("-15", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+15", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("+16", "ZZ"), BoltUserError);
  EXPECT_THROW(parseTZ("-16", "ZZ"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseTimeZoneIds) {
  for (const auto& [name, canonical] :
       std::vector<std::pair<const char*, const char*>>{
           {"Asia/Shanghai", "Asia/Shanghai"},
           {"America/Los_Angeles", "America/Los_Angeles"},
           {"US/Pacific", "America/Los_Angeles"},
           {"America/Argentina/ComodRivadavia", "America/Argentina/Catamarca"},
           {"UTC", "UTC"},
           {"Etc/UTC", "UTC"},
           {"GMT", "UTC"},
           {"Etc/GMT+12", "-12:00"},
           {"Etc/GMT-14", "+14:00"},
           {"EST", "-05:00"},
           {"MST", "-07:00"},
           {"HST", "-10:00"}}) {
    SCOPED_TRACE(name);
    const auto input = std::string(name) + "1970";
    for (const auto* pattern : {"ZZZyyyy", "ZZZZyyyy"}) {
      auto formatter = buildJodaDateTimeFormatter(pattern);
      for (const auto parsed :
           {formatter->parse(input),
            formatter->parseWithNanosecondPrecision(input)}) {
        ASSERT_FALSE(parsed.hasError());
        EXPECT_EQ(parsed.value().timestamp, Timestamp(0, 0));
        const auto expectedId = tz::getTimeZoneID(canonical);
        EXPECT_EQ(parsed.value().timezoneId, expectedId);
      }
    }
  }
  for (const auto* invalid :
       {"", "Asia/Unknown", "asia/Shanghai", "UTCjunk", "Asia/ShanghaiX"}) {
    SCOPED_TRACE(invalid);
    EXPECT_TRUE(buildJodaDateTimeFormatter("ZZZ")->parse(invalid).hasError());
  }
  auto longest = buildJodaDateTimeFormatter("ZZZyyyy")->parse("Etc/GMT+11970");
  ASSERT_FALSE(longest.hasError());
  EXPECT_EQ(longest.value().timezoneId, tz::getTimeZoneID(-11 * 60));
  EXPECT_EQ(
      longest.value().timestamp,
      Timestamp(util::daysSinceEpochFromDate(970, 1, 1) * 86400, 0));
  EXPECT_EQ("+05:30", parseTZ("+0530", "ZZZ"));
  EXPECT_EQ("+00:00", parseTZ("Z", "ZZZ"));
  EXPECT_TRUE(buildJodaDateTimeFormatter("ZZZ'!'yyyy")
                  ->parse("Asia/Shanghai?1970")
                  .hasError());
}

TEST_F(JodaDateTimeFormatterTest, registeredTimeZoneIds) {
  const auto formatter = buildJodaDateTimeFormatter("ZZZ'@'yyyy");
  const auto printer = buildJodaDateTimeFormatter("ZZZ");
  for (const auto id : tz::getTimeZoneIDs()) {
    if (id != 0 && id <= tz::kMaxFixedOffsetTimeZoneId) {
      continue;
    }
    const auto& name = tz::locateZone(id)->name();
    SCOPED_TRACE(name);
    auto parsed = formatter->parse(name + "@1970");
    ASSERT_FALSE(parsed.hasError());
    const auto* zone = tz::locateZone(id);
    std::string canonical(printer->maxResultSize(zone), '\0');
    canonical.resize(printer->format(
        Timestamp(0, 0), zone, canonical.size(), canonical.data()));
    EXPECT_EQ(parsed.value().timezoneId, tz::getTimeZoneID(canonical));
    EXPECT_EQ(parsed.value().timestamp, Timestamp(0, 0));
  }
}

TEST_F(JodaDateTimeFormatterTest, timeZoneIdRequiresFullInput) {
  for (const auto* pattern : {"ZZZ", "ZZZZ"}) {
    const auto formatter = buildJodaDateTimeFormatter(pattern);
    for (const auto* input : {"+0530", "-05:30", "Z", "UTC"}) {
      SCOPED_TRACE(std::string(pattern) + ":" + input);
      const auto trailing = std::string(input) + "junk";
      EXPECT_FALSE(formatter->parse(input).hasError());
      EXPECT_TRUE(formatter->parse(trailing).hasError());
      EXPECT_FALSE(formatter->parse(input, TimePolicy::LEGACY).hasError());
      EXPECT_TRUE(formatter->parse(trailing, TimePolicy::LEGACY).hasError());
      EXPECT_TRUE(formatter->parseWithNanosecondPrecision(trailing).hasError());
      EXPECT_FALSE(buildJodaDateTimeFormatter(std::string(pattern) + "'junk'")
                       ->parse(trailing)
                       .hasError());
    }
  }
  EXPECT_FALSE(buildJodaDateTimeFormatter("ZZ")->parse("+0530junk").hasError());
}

TEST_F(DateTimeFormatterTest, javaTimePatternValidation) {
  for (const auto* pattern :
       {"C",
        "YYYY",
        "ww",
        "e",
        "MMMMM",
        "EEEEE",
        "zzzzz",
        "yyyy-MM-dd[ HH:mm:ss]",
        "'"}) {
    SCOPED_TRACE(pattern);
    EXPECT_THROW(buildJavaDateTimeFormatter(pattern), BoltUserError);
  }
  EXPECT_TRUE(buildJavaDateTimeFormatter("uuuu-MM-dd dd")
                  ->parse("2024-02-30 31")
                  .hasError());
  const auto literal = buildJavaDateTimeFormatter("uuuu'['MM']'dd");
  EXPECT_EQ(
      literal->parse("2024[01]02").value().timestamp,
      parseMysql("2024-01-02", "%Y-%m-%d"));
}

TEST_F(DateTimeFormatterTest, javaTimeFieldResolution) {
  struct Case {
    const char* pattern;
    const char* input;
    std::optional<Timestamp> expected;
  };
  for (const auto& test : std::vector<Case>{
           {"uuuu-MM-dd EEE", "2024-02-29 Thu", Timestamp(1709164800, 0)},
           {"uuuu-MM-dd EEE", "2024-02-29 Fri", std::nullopt},
           {"uuuu-MM-dd EEE", "2024-02-29 thu", std::nullopt},
           {"uuuu-MM-dd EEEE", "2024-02-29 Thursday", Timestamp(1709164800, 0)},
           {"uuuu-MM-dd F", "2024-02-29 1", Timestamp(1709164800, 0)},
           {"uuuu-MM-dd F", "2024-02-29 5", std::nullopt},
           {"uuuu-MM-dd MM", "2024-02-29 02", Timestamp(1709164800, 0)},
           {"uuuu-MM-dd MM", "2024-02-29 03", std::nullopt},
           {"uuuu-MM-dd MMM", "2024-02-29 Feb", Timestamp(1709164800, 0)},
           {"uuuu-MM-dd MMM", "2024-02-29 Mar", std::nullopt},
           {"uuuu-MM-dd HH:mm:ss HH",
            "2024-02-29 13:02:03 13",
            Timestamp(1709211723, 0)},
           {"uuuu-MM-dd HH:mm:ss HH", "2024-02-29 13:02:03 14", std::nullopt},
           {"uuuu-MM-dd HH:mm:ss hh a",
            "2024-02-29 13:02:03 01 PM",
            Timestamp(1709211723, 0)},
           {"uuuu-MM-dd HH:mm:ss hh a",
            "2024-02-29 13:02:03 02 PM",
            std::nullopt},
           {"uuuu-MM-dd hh a HH:mm:ss",
            "2024-02-29 01 PM 13:02:03",
            Timestamp(1709211723, 0)},
           {"uuuu-MM-dd hh a HH:mm:ss",
            "2024-02-29 02 PM 13:02:03",
            std::nullopt},
           {"uuuu-MM-dd kk:mm:ss HH",
            "2024-02-29 24:02:03 00",
            Timestamp(1709164923, 0)},
           {"uuuu-MM-dd HH:mm:ss mm", "2024-02-29 13:02:03 04", std::nullopt},
           {"uuuu-MM-dd HH:mm:ss ss", "2024-02-29 13:02:03 04", std::nullopt},
           {"uuuu-MM-dd HH:mm:ss.S S", "2024-02-29 13:02:03.4 5", std::nullopt},
           {"uuuu-MM-dd HH:mm:ss.S SS",
            "2024-02-29 13:02:03.4 40",
            Timestamp(1709211723, 400000000)},
       }) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    const auto parsed = buildJavaDateTimeFormatter(test.pattern)
                            ->parseWithNanosecondPrecision(test.input);
    ASSERT_EQ(parsed.hasError(), !test.expected.has_value());
    if (test.expected) {
      EXPECT_EQ(parsed.value().timestamp, *test.expected);
    }
  }
}

TEST_F(DateTimeFormatterTest, javaSmartDateCrossChecks) {
  for (const auto* pattern :
       {"yyyy/MM/dd HH:mm:ss D", "D yyyy/MM/dd HH:mm:ss"}) {
    const auto formatter = buildJavaDateTimeFormatter(pattern);
    const auto strict = buildJavaDateTimeFormatter(pattern, {.strict = true});
    for (int dayOfYear : {59, 60, 61}) {
      const auto input = pattern[0] == 'D'
          ? fmt::format("{} 2024/02/30 00:00:00", dayOfYear)
          : fmt::format("2024/02/30 00:00:00 {}", dayOfYear);
      SCOPED_TRACE(input);
      const auto parsed = formatter->parse(input);
      ASSERT_EQ(parsed.hasError(), dayOfYear != 60);
      if (!parsed.hasError()) {
        EXPECT_EQ(parsed.value().timestamp, Timestamp(1709164800, 0));
      }
      EXPECT_TRUE(strict->parse(input).hasError());
    }
  }
}

TEST_F(DateTimeFormatterTest, javaOffsetAndZoneResolution) {
  const auto* utc = tz::locateZone("UTC");
  for (const auto* zonePattern : {"VV", "z"}) {
    for (const auto& [zone, seconds] :
         std::vector<std::pair<std::string, int64_t>>{
             {"UTC", 0},
             {"America/New_York", 18000},
             {"UTC+17:00", -61200},
             {"+02:03:04", -7384}}) {
      for (bool zoneFirst : {true, false}) {
        const auto pattern = std::string("yyyy-MM-dd HH:mm:ss ") +
            (zoneFirst ? std::string(zonePattern) + " XXX"
                       : "XXX " + std::string(zonePattern));
        const auto input = std::string("1970-01-01 00:00:00 ") +
            (zoneFirst ? zone + " +01:00" : "+01:00 " + zone);
        SCOPED_TRACE(pattern + ":" + input);
        EXPECT_EQ(
            buildSparkDateTimeFormatter(pattern)
                ->parseToUtc(input, utc)
                .value()
                .timestamp,
            Timestamp(seconds, 0));
        EXPECT_EQ(
            buildJavaDateTimeFormatter(pattern)
                ->parseToUtc(input, utc)
                .value()
                .timestamp,
            Timestamp(-3600, 0));
      }
    }
  }
  EXPECT_EQ(
      buildSparkDateTimeFormatter("yyyy-MM-dd HH:mm:ss XXXXX")
          ->parseToUtc("1970-01-01 00:00:00 +17:00:01", utc)
          .value()
          .timestamp,
      Timestamp(-61201, 0));
}

TEST_F(DateTimeFormatterTest, sparkStandaloneMonthText) {
  for (const auto* pattern : {"LLL", "LLLL"}) {
    EXPECT_THROW(buildSparkDateTimeFormatter(pattern), BoltUserError);
    EXPECT_THROW(buildSparkDateTimeFormatter(pattern, false), BoltUserError);
    EXPECT_NO_THROW(buildSparkDateTimeFormatter(pattern, false, true));
    EXPECT_NO_THROW(buildJavaDateTimeFormatter(pattern));
  }
  for (const auto* pattern :
       {"L", "LL", "MMM", "MMMM", "EEE", "EEEE", "'LLL'"}) {
    EXPECT_NO_THROW(buildSparkDateTimeFormatter(pattern, false));
  }
}

TEST_F(DateTimeFormatterTest, javaAdjacentFieldsAndMidnight) {
  struct Case {
    const char* pattern;
    const char* input;
    std::optional<Timestamp> smart;
    std::optional<Timestamp> strict;
  };
  for (const auto& test : std::vector<Case>{
           {"yyyy-DD", "2024-366", std::nullopt, std::nullopt},
           {"yyyy-DDD",
            "2024-366",
            Timestamp(1735603200, 0),
            Timestamp(1735603200, 0)},
           {"yyyyDD", "2024366", std::nullopt, std::nullopt},
           {"DDyyyy", "3662024", std::nullopt, std::nullopt},
           {"yyyy M-dd",
            "2024 0000000000000000001-02",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyyy M-dd",
            "2024 9223372036854775807-02",
            std::nullopt,
            std::nullopt},
           {"yyyy M-dd",
            "2024 9223372036854775808-02",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd HH:mm:ss XXX XXX",
            "1970-01-01 00:00:00 +01:00 +02:00",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd HH:mm:ss X Z",
            "1970-01-01 00:00:00 Z +0000",
            Timestamp(0, 0),
            Timestamp(0, 0)},
           {"yyyy-MM-dd HH:mm:ss VV XXX",
            "1970-01-01 00:00:00 UTC +01:00",
            Timestamp(0, 0),
            Timestamp(0, 0)},
           {"yMMdd",
            "20240102",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyyMMdd",
            "20240102",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyyyMMdd",
            "20240102",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyMMdd",
            "240102",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyyyMMdd",
            "+100000102",
            Timestamp(253402387200, 0),
            Timestamp(253402387200, 0)},
           {"yyyy/MM/dd",
            "+10000/01/01",
            Timestamp(253402300800, 0),
            Timestamp(253402300800, 0)},
           {"yyyy Mdd",
            "2024 102",
            Timestamp(1704153600, 0),
            Timestamp(1704153600, 0)},
           {"yyyy Mdd",
            "2024 1231",
            Timestamp(1735603200, 0),
            Timestamp(1735603200, 0)},
           {"Mddy", "1022024", std::nullopt, std::nullopt},
           {"ddMMy",
            "01022024",
            Timestamp(1706745600, 0),
            Timestamp(1706745600, 0)},
           {"yyyyMMddHHmmssSSS",
            "20240102030405123",
            std::nullopt,
            std::nullopt},
           {"yyyy/MM/dd HH:mm:ss",
            "2024/01/31 24:00:00",
            Timestamp(1706745600, 0),
            std::nullopt},
           {"yyyy/MM/dd HH:mm:ss",
            "2024/01/31 24:00:01",
            std::nullopt,
            std::nullopt},
           {"yyyy/MM/dd HH:mm:ss.SSSSSSSSS",
            "2024/01/31 24:00:00.000000001",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd EEE HH:mm",
            "2024-01-31 Wed 24:00",
            Timestamp(1706745600, 0),
            std::nullopt},
           {"yyyy-MM-dd HH:mm a",
            "2024-01-31 24:00 AM",
            Timestamp(1706745600, 0),
            std::nullopt},
           {"yyyy-MM-dd HH:mm a",
            "2024-01-31 24:00 PM",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd a HH:mm",
            "2024-01-31 AM 24:00",
            Timestamp(1706745600, 0),
            std::nullopt},
           {"yyyy-MM-dd a HH:mm",
            "2024-01-31 PM 24:00",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd HH:mm hh",
            "2024-01-31 24:00 12",
            Timestamp(1706745600, 0),
            std::nullopt},
           {"yyyy-MM-dd HH:mm hh a",
            "2024-01-31 24:00 12 AM",
            std::nullopt,
            std::nullopt},
           {"yyyy-MM-dd HH:mm HH",
            "2024-01-31 24:00 00",
            std::nullopt,
            std::nullopt},
       }) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    for (bool strict : {false, true}) {
      if (strict &&
          std::string_view(test.pattern).find("EEE") !=
              std::string_view::npos) {
        EXPECT_THROW(buildSparkDateTimeFormatter(test.pattern), BoltUserError);
        continue;
      }
      auto formatter = strict ? buildSparkDateTimeFormatter(test.pattern)
                              : buildJavaDateTimeFormatter(test.pattern);
      const auto& expected = strict ? test.strict : test.smart;
      for (const auto& parsed :
           {formatter->parse(test.input),
            formatter->parseWithNanosecondPrecision(test.input)}) {
        ASSERT_EQ(parsed.hasError(), !expected.has_value());
        if (expected) {
          EXPECT_EQ(parsed.value().timestamp, *expected);
        }
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, javaDayFieldRange) {
  for (const auto* day : {"4294967297", "9223372032559808513"}) {
    for (const bool strict : {false, true}) {
      DateTimeParseOptions options;
      options.strict = strict;
      for (const bool dayOfYear : {false, true}) {
        const auto pattern = dayOfYear ? "uuuu-D" : "uuuu-MM-d";
        const auto input = std::string(dayOfYear ? "2024-" : "2024-01-") + day;
        SCOPED_TRACE(std::string(pattern) + ":" + input);
        const auto formatter = buildJavaDateTimeFormatter(pattern, options);
        EXPECT_TRUE(formatter->parse(input).hasError());
        EXPECT_TRUE(formatter->parseWithNanosecondPrecision(input).hasError());
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, javaClockHourZero) {
  struct Case {
    const char* pattern;
    const char* input;
    Timestamp expected;
  };
  for (const auto& test : std::vector<Case>{
           {"yyyy-MM-dd kk:mm", "2024-01-31 00:02", Timestamp(1706659320, 0)},
           {"yyyy-MM-dd hh:mm a",
            "2024-01-31 00:02 AM",
            Timestamp(1706659320, 0)},
           {"yyyy-MM-dd hh:mm a",
            "2024-01-31 00:02 PM",
            Timestamp(1706702520, 0)}}) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    for (const bool strict : {false, true}) {
      const auto formatter = strict ? buildSparkDateTimeFormatter(test.pattern)
                                    : buildJavaDateTimeFormatter(test.pattern);
      for (const auto parsed :
           {formatter->parse(test.input),
            formatter->parseWithNanosecondPrecision(test.input)}) {
        EXPECT_EQ(parsed.hasError(), strict);
        if (!parsed.hasError()) {
          EXPECT_EQ(parsed.value().timestamp, test.expected);
        }
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, javaBuilderParsing) {
  auto formatter = DateTimeFormatterBuilder(2)
                       .appendYear(4)
                       .appendLiteral("-", 1)
                       .appendMonthOfYear(2)
                       .appendLiteral("-", 1)
                       .appendDayOfMonth(2)
                       .setType(DateTimeFormatterType::JAVA_TIME)
                       .build();
  EXPECT_EQ(
      formatter->parse("2024-01-02").value().timestamp,
      Timestamp(1704153600, 0));
  for (const auto width : {1, 2, 4}) {
    formatter = DateTimeFormatterBuilder(0)
                    .appendYear(width)
                    .appendMonthOfYear(2)
                    .appendDayOfMonth(2)
                    .setType(DateTimeFormatterType::JAVA_TIME)
                    .build();
    const auto* input = width == 2 ? "240102" : "20240102";
    for (const auto parsed :
         {formatter->parse(input),
          formatter->parseWithNanosecondPrecision(input)}) {
      ASSERT_FALSE(parsed.hasError());
      EXPECT_EQ(parsed.value().timestamp, Timestamp(1704153600, 0));
    }
  }
  auto copied = std::make_shared<DateTimeFormatter>(
      std::make_unique<char[]>(0),
      0,
      std::vector<DateTimeToken>(formatter->tokens()),
      DateTimeFormatterType::JAVA_TIME);
  EXPECT_EQ(
      copied->parse("20240102").value().timestamp, Timestamp(1704153600, 0));
}

TEST_F(DateTimeFormatterTest, javaBuilderNumericWidth) {
  for (const auto append :
       {&DateTimeFormatterBuilder::appendYear,
        &DateTimeFormatterBuilder::appendYearOfEra,
        &DateTimeFormatterBuilder::appendMonthOfYear,
        &DateTimeFormatterBuilder::appendDayOfMonth,
        &DateTimeFormatterBuilder::appendDayOfYear,
        &DateTimeFormatterBuilder::appendHourOfDay,
        &DateTimeFormatterBuilder::appendClockHourOfDay,
        &DateTimeFormatterBuilder::appendHourOfHalfDay,
        &DateTimeFormatterBuilder::appendClockHourOfHalfDay,
        &DateTimeFormatterBuilder::appendMinuteOfHour,
        &DateTimeFormatterBuilder::appendSecondOfMinute,
        &DateTimeFormatterBuilder::appendDayOfWeekInMonth}) {
    DateTimeFormatterBuilder valid(0);
    EXPECT_NO_THROW(
        (valid.*append)(19).setType(DateTimeFormatterType::JAVA_TIME).build());
    DateTimeFormatterBuilder invalid(0);
    EXPECT_THROW(
        (invalid.*append)(20).setType(DateTimeFormatterType::JAVA_TIME).build(),
        BoltUserError);
  }
}

TEST_F(DateTimeFormatterTest, javaYearAndEraResolution) {
  struct Case {
    const char* pattern;
    const char* input;
    std::optional<int32_t> year;
  };
  for (const auto& test : std::vector<Case>{
           {"G yyyy uuuu-MM-dd", "BC 0001 0000-01-01", 0},
           {"G uuuu yyyy-MM-dd", "BC 0000 0001-01-01", 0},
           {"yyyy uuuu-MM-dd G", "0002 -0001-01-01 BC", -1},
           {"yyyy uuuu-MM-dd", "0001 0000-01-01", 0},
           {"uuuu yyyy-MM-dd", "0000 0001-01-01", 0},
           {"yyyy uuuu-MM-dd", "2024 2024-01-01", 2024},
           {"G yyyy uuuu-MM-dd", "BC 0001 0001-01-01", std::nullopt},
           {"G yyyy uuuu-MM-dd", "AD 0001 0000-01-01", std::nullopt},
           {"G uuuu-MM-dd", "BC 0000-01-01", 0},
           {"G uuuu-MM-dd", "AD 0000-01-01", std::nullopt},
           {"uuuu yyyy-MM-dd", "0000 0002-01-01", std::nullopt},
           {"yyyy yyyy-MM-dd", "0001 0002-01-01", std::nullopt}}) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    for (const bool strict : {false, true}) {
      DateTimeParseOptions options;
      options.strict = strict;
      auto formatter = buildJavaDateTimeFormatter(test.pattern, options);
      for (const auto parsed :
           {formatter->parse(test.input),
            formatter->parseWithNanosecondPrecision(test.input)}) {
        ASSERT_EQ(parsed.hasError(), !test.year.has_value());
        if (test.year) {
          EXPECT_EQ(
              parsed.value().timestamp,
              Timestamp(
                  util::daysSinceEpochFromDate(*test.year, 1, 1) * 86400, 0));
        }
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, javaResolvedYearRange) {
  struct Case {
    const char* pattern;
    const char* input;
    std::optional<int32_t> year;
  };
  for (const auto& test : std::vector<Case>{
           {"G y-MM-dd", "BC 292275056-01-01", util::kMinYear},
           {"G y-MM-dd", "BC 292275057-01-01", std::nullopt},
           {"y-MM-dd G", "292275057-01-01 BC", std::nullopt},
           {"G y", "BC 292275057", std::nullopt},
           {"G y-D", "BC 292275057-1", std::nullopt},
           {"G y-MM-dd", "BC 292278993-01-01", std::nullopt},
           {"G y-MM-dd", "AD 292275057-01-01", 292275057},
           {"u-MM-dd", "292278994-01-01", util::kMaxYear}}) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    for (const bool strict : {false, true}) {
      const auto formatter =
          buildJavaDateTimeFormatter(test.pattern, {.strict = strict});
      for (const auto parsed :
           {formatter->parse(test.input),
            formatter->parseWithNanosecondPrecision(test.input)}) {
        ASSERT_EQ(parsed.hasError(), !test.year.has_value());
        if (test.year) {
          EXPECT_EQ(
              parsed.value().timestamp,
              Timestamp(
                  util::daysSinceEpochFromDate(*test.year, 1, 1) * 86400, 0));
        } else {
          EXPECT_EQ(parsed.errorCodeMsg(), "year out of range.");
        }
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, javaDayOfYearFormatting) {
  auto formatter = buildJavaDateTimeFormatter("DD");
  auto* utc = tz::locateZone("UTC");
  std::string output(formatter->maxResultSize(utc), '\0');
  const Timestamp yearEnd(1735603200, 0);
  EXPECT_THROW(
      formatter->format(yearEnd, utc, output.size(), output.data()),
      BoltUserError);
  formatter = buildSparkDateTimeFormatter("DD");
  EXPECT_THROW(
      formatter->format(yearEnd, utc, output.size(), output.data()),
      BoltUserError);
  formatter = buildJavaDateTimeFormatter("DDD");
  output.resize(formatter->format(yearEnd, utc, output.size(), output.data()));
  EXPECT_EQ(output, "366");
}

TEST_F(DateTimeFormatterTest, javaEraFormatting) {
  struct Case {
    const char* pattern;
    int32_t year;
    const char* expected;
  };
  for (const auto& test : std::vector<Case>{
           {"G yy", 0, "BC 01"},     {"uuuu G", 0, "0000 BC"},
           {"yy", 0, "01"},          {"yyyy", 0, "0001"},
           {"G yy", -1, "BC 02"},    {"uuuu G", -1, "-0001 BC"},
           {"yy", -1, "02"},         {"yyyy", -1, "0002"},
           {"G yy", -99, "BC 00"},   {"uuuu G", -99, "-0099 BC"},
           {"yy", -99, "00"},        {"yyyy", -99, "0100"},
           {"G yy", -100, "BC 01"},  {"uuuu G", -100, "-0100 BC"},
           {"yy", -100, "01"},       {"yyyy", -100, "0101"},
           {"G yy", 1, "AD 01"},     {"uuuu G", 1, "0001 AD"},
           {"yy", 1, "01"},          {"yyyy", 1, "0001"},
           {"G yy", 10000, "AD 00"}, {"uuuu G", 10000, "+10000 AD"},
           {"yy", 10000, "00"},      {"yyyy", 10000, "+10000"},
       }) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + std::to_string(test.year));
    auto formatter = buildJavaDateTimeFormatter(test.pattern);
    const auto* zone = tz::locateZone("UTC");
    std::string output(formatter->maxResultSize(zone), '\0');
    output.resize(formatter->format(
        Timestamp(util::daysSinceEpochFromDate(test.year, 1, 1) * 86400, 0),
        zone,
        output.size(),
        output.data()));
    EXPECT_EQ(output, test.expected);
    if (std::string_view(test.pattern) == "G yy") {
      formatter = buildSparkDateTimeFormatter(test.pattern);
      output.resize(formatter->maxResultSize(zone));
      output.resize(formatter->format(
          Timestamp(util::daysSinceEpochFromDate(test.year, 1, 1) * 86400, 0),
          zone,
          output.size(),
          output.data()));
      EXPECT_EQ(output, test.expected);
    }
  }
}

TEST_F(DateTimeFormatterTest, javaTimeZoneOffsets) {
  struct Case {
    const char* pattern;
    const char* input;
    int32_t seconds;
  };
  for (const auto& test : std::vector<Case>{
           {"Z", "+0530", 19800},
           {"Z'1970'", "+00001970", 0},
           {"ZZZ", "-0330", -12600},
           {"ZZZZ", "GMT", 0},
           {"ZZZZ", "GMT+05:30", 19800},
           {"ZZZZZ", "Z", 0},
           {"ZZZZZ", "+05:30:15", 19815},
           {"X", "+05", 18000},
           {"X", "+0530", 19800},
           {"XX", "+0530", 19800},
           {"XX", "+053015", 19815},
           {"XXX", "+05:30", 19800},
           {"XXX", "+05:30:15", 19815},
           {"XXXX", "+053015", 19815},
           {"XXXXX", "+05:30:15", 19815},
           {"xxx", "-00:00", 0},
           {"x", "+00", 0},
           {"Z", "+1800", 64800},
           {"xxx", "-18:00", -64800}}) {
    SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
    auto formatter = buildJavaDateTimeFormatter(test.pattern);
    for (const auto parsed :
         {formatter->parse(test.input),
          formatter->parseWithNanosecondPrecision(test.input)}) {
      ASSERT_FALSE(parsed.hasError());
      EXPECT_EQ(parsed.timezoneOffsetSeconds(), test.seconds);
      EXPECT_EQ(parsed.value().timestamp, Timestamp(0, 0));
      if (test.seconds % 60 == 0 && std::abs(test.seconds) <= 14 * 3600) {
        EXPECT_EQ(
            parsed.value().timezoneId, tz::getTimeZoneID(test.seconds / 60));
      } else {
        EXPECT_EQ(parsed.value().timezoneId, -1);
      }
    }
  }
  for (const auto& [pattern, input] :
       std::vector<std::pair<std::string, std::string>>{
           {"XX", "+05:30"},
           {"Z'1970'", "+05301970"},
           {"XXX", "+0530"},
           {"Z", "Z"},
           {"ZZZ", "Asia/Shanghai"},
           {"x", "Z"},
           {"x", "+0000"},
           {"xxx", "+18:00:01"},
           {"Z", "+1860"},
           {"Z", "+1900"},
           {"XXX", "+05:60"},
           {"XXXXX", "+05:30:60"},
           {"ZZZZ", "GMT+5:30"},
           {"XXX", "+05:30junk"},
           {"XXX", std::string("+05:30\0", 7)}}) {
    SCOPED_TRACE(pattern + ":" + input);
    EXPECT_TRUE(buildJavaDateTimeFormatter(pattern)->parse(input).hasError());
  }
  for (const auto* pattern : {"XXXXXX", "xxxxxx", "ZZZZZZ", "V", "VVV"}) {
    EXPECT_THROW(buildJavaDateTimeFormatter(pattern), BoltUserError);
  }
}

TEST_F(DateTimeFormatterTest, javaTimeZoneIds) {
  for (const auto* name :
       {"Asia/Shanghai",
        "US/Pacific",
        "UTC",
        "UT",
        "GMT",
        "Etc/GMT+5",
        "Zulu"}) {
    SCOPED_TRACE(name);
    auto parsed = buildJavaDateTimeFormatter("VV'@'yyyy")
                      ->parse(std::string(name) + "@1970");
    ASSERT_FALSE(parsed.hasError());
    EXPECT_EQ(parsed.value().timezoneId, tz::getTimeZoneID(name));
    EXPECT_EQ(parsed.value().timestamp, Timestamp(0, 0));
  }
  for (const auto* name :
       {"+05:30:15", "UTC+05:30:15", "GMT+05:30:15", "UT+05:30:15"}) {
    auto parsed = buildJavaDateTimeFormatter("VV")->parse(name);
    ASSERT_FALSE(parsed.hasError());
    EXPECT_EQ(parsed.timezoneOffsetSeconds(), 19815);
  }
  auto prefix = buildJavaDateTimeFormatter("VV'+bad'")->parse("GMT+bad");
  ASSERT_FALSE(prefix.hasError());
  EXPECT_EQ(prefix.value().timezoneId, 0);
  for (const auto* name :
       {"+05",
        "+0530",
        "UTC+05",
        "Asia/ShanghaiX",
        "asia/shanghai",
        "EST",
        "MST",
        "HST",
        "SystemV/EST5EDT"}) {
    EXPECT_TRUE(buildJavaDateTimeFormatter("VV")->parse(name).hasError());
  }
}

TEST_F(DateTimeFormatterTest, javaTimeZoneFormatting) {
  const auto* zone = tz::locateZone("+05:30");
  for (const auto& [pattern, expected] :
       std::vector<std::pair<std::string, std::string>>{
           {"Z", "+0530"},
           {"ZZ", "+0530"},
           {"ZZZ", "+0530"},
           {"ZZZZ", "GMT+05:30"},
           {"ZZZZZ", "+05:30"},
           {"X", "+0530"},
           {"XX", "+0530"},
           {"XXX", "+05:30"},
           {"XXXX", "+0530"},
           {"XXXXX", "+05:30"},
           {"VV", "+05:30"}}) {
    auto formatter = buildJavaDateTimeFormatter(pattern);
    std::string output(formatter->maxResultSize(zone), '\0');
    output.resize(
        formatter->format(Timestamp(0, 0), zone, output.size(), output.data()));
    EXPECT_EQ(output, expected) << pattern;
  }
  auto idFormatter = buildJavaDateTimeFormatter("VV");
  const auto* alias = tz::locateZone("US/Pacific");
  std::string idOutput(idFormatter->maxResultSize(alias), '\0');
  idOutput.resize(idFormatter->format(
      Timestamp(0, 0), alias, idOutput.size(), idOutput.data()));
  EXPECT_EQ(idOutput, "US/Pacific");
  const auto* utc = tz::locateZone("UTC");
  for (const auto& [pattern, expected] :
       std::vector<std::pair<std::string, std::string>>{
           {"Z", "+0000"},
           {"ZZZZ", "GMT"},
           {"ZZZZZ", "Z"},
           {"X", "Z"},
           {"XXX", "Z"},
           {"x", "+00"},
           {"xx", "+0000"},
           {"xxx", "+00:00"}}) {
    auto formatter = buildJavaDateTimeFormatter(pattern);
    std::string output(formatter->maxResultSize(utc), '\0');
    output.resize(
        formatter->format(Timestamp(0, 0), utc, output.size(), output.data()));
    EXPECT_EQ(output, expected) << pattern;
  }
}

TEST_F(DateTimeFormatterTest, sparkTimeZoneBindings) {
  EXPECT_TRUE(
      buildSparkDateTimeFormatter("ZZZ")->parse("Asia/Shanghai").hasError());
  EXPECT_TRUE(buildSparkDateTimeFormatter("ZZZ")->parse("+05:30").hasError());
  EXPECT_FALSE(buildSparkDateTimeFormatter("ZZZ")->parse("+0530").hasError());
  EXPECT_TRUE(buildSparkDateTimeFormatter("XXX")->parse("+0530").hasError());
  EXPECT_FALSE(buildSparkDateTimeFormatter("XXX")->parse("+05:30").hasError());
  EXPECT_FALSE(
      buildSparkDateTimeFormatter("VV")->parse("Asia/Shanghai").hasError());
}

TEST_F(JodaDateTimeFormatterTest, parseTimezone) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  EXPECT_THROW(parseJoda("", "z"), BoltUserError);
  EXPECT_THROW(parseJoda("ANY", "z"), BoltUserError);
  EXPECT_THROW(parseJoda("GM", "z"), BoltUserError);
  EXPECT_THROW(parseJoda("Los_Angeles", "z"), BoltUserError);
  EXPECT_THROW(parseJoda("2020", "Y z"), BoltUserError);

  // Only a few three letter prefixes are supported by Joda.
  EXPECT_EQ("America/Los_Angeles", parseTZ("PST", "z"));
  EXPECT_EQ("America/Los_Angeles", parseTZ("PDT", "zz"));
  EXPECT_EQ("America/New_York", parseTZ("EST", "z"));
  EXPECT_EQ("+00:00", parseTZ("GMT", "zz"));
  EXPECT_EQ("+00:00", parseTZ("UTC", "z"));
  EXPECT_EQ("+00:00", parseTZ("UT", "z"));
}

TEST_F(JodaDateTimeFormatterTest, parseMixedYMDFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 23:00:00", nullptr),
      parseJoda("2021-01-04+23:00", "YYYY-MM-dd+HH:mm").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10", nullptr),
      parseJoda("2019-07-03 11:04:10", "YYYY-MM-dd HH:mm:ss").timestamp);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10", nullptr),
      parseJoda("10:04:11 03-07-2019", "ss:mm:HH dd-MM-YYYY").timestamp);

  // Include timezone.
  auto result = parseJoda("2021-11-05+01:00+09:00", "YYYY-MM-dd+HH:mmZZ");
  EXPECT_EQ(
      util::fromTimestampString("2021-11-05 01:00:00", nullptr),
      result.timestamp);
  EXPECT_EQ("+09:00", tz::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseJoda("-07:232021-11-05+01:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(
      util::fromTimestampString("2021-11-05 01:00:00", nullptr),
      result.timestamp);
  EXPECT_EQ("-07:23", tz::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseJoda("+01332022-03-08+13:00", "ZZYYYY-MM-dd+HH:mm");
  EXPECT_EQ(
      util::fromTimestampString("2022-03-08 13:00:00", nullptr),
      result.timestamp);
  EXPECT_EQ("+01:33", tz::getTimeZoneName(result.timezoneId));

  // Z in the input means GMT in Joda.
  EXPECT_EQ(
      util::fromTimestampString("2022-07-29 20:03:54.667", nullptr),
      parseJoda("2022-07-29T20:03:54.667Z", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
          .timestamp);

  // Timezone in string format.
  result = parseJoda("2021-11-05+01:00 PST", "YYYY-MM-dd+HH:mm zz");
  EXPECT_EQ(
      util::fromTimestampString("2021-11-05 01:00:00", nullptr),
      result.timestamp);
  EXPECT_EQ("America/Los_Angeles", tz::getTimeZoneName(result.timezoneId));
}

TEST_F(JodaDateTimeFormatterTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 1 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 1 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 4 13:29:21.213", "x w e HH:mm:ss.SSS").timestamp);

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 Mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 Mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 Thu 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 Monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 Thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 MON 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 MON 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 THU 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 MONDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 THURSDAY 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 mon 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 thu 13:29:21.213", "x w E HH:mm:ss.SSS").timestamp);

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseJoda("2021 1 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("2021 22 monday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseJoda("2021 22 thursday 13:29:21.213", "x w EEE HH:mm:ss.SSS")
          .timestamp);

  // Invalid day of week throw cases
  EXPECT_THROW(parseJoda("mOn", "E"), BoltUserError);
  EXPECT_THROW(parseJoda("tuE", "E"), BoltUserError);
  EXPECT_THROW(parseJoda("WeD", "E"), BoltUserError);
  EXPECT_THROW(parseJoda("WEd", "E"), BoltUserError);
  //   EXPECT_THROW(parseJoda("MONday", "EEE"), BoltUserError);
  //   EXPECT_THROW(parseJoda("monDAY", "EEE"), BoltUserError);
  EXPECT_THROW(parseJoda("frIday", "EEE"), BoltUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseJoda("213.21:29:13 1 22 2021", "SSS.ss:mm:HH e w x").timestamp);

  // Include timezone.
  auto result =
      parseJoda("2021 22 1 13:29:21.213+09:00", "x w e HH:mm:ss.SSSZZ");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      result.timestamp);
  EXPECT_EQ("+09:00", tz::getTimeZoneName(result.timezoneId));

  // Timezone offset in -hh:mm format.
  result = parseJoda("-07:232021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      result.timestamp);
  EXPECT_EQ("-07:23", tz::getTimeZoneName(result.timezoneId));

  // Timezone offset in +hhmm format.
  result = parseJoda("+01332021 22 1 13:29:21.213", "ZZx w e HH:mm:ss.SSS");
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      result.timestamp);
  EXPECT_EQ("+01:33", tz::getTimeZoneName(result.timezoneId));
}

TEST_F(JodaDateTimeFormatterTest, parseFractionOfSecond) {
  // Valid milliseconds and timezone with positive offset.
  auto result =
      parseJoda("2022-02-23T12:15:00.364+04:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.364", nullptr),
      result.timestamp);
  EXPECT_EQ("+04:00", tz::getTimeZoneName(result.timezoneId));

  // Valid milliseconds and timezone with negative offset.
  result =
      parseJoda("2022-02-23T12:15:00.776-14:00", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 12:15:00.776", nullptr),
      result.timestamp);
  EXPECT_EQ("-14:00", tz::getTimeZoneName(result.timezoneId));

  // Valid milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-24 02:19:33.283", nullptr),
      parseJoda("2022-02-24 02:19:33.283", "yyyy-MM-dd HH:mm:ss.SSS")
          .timestamp);

  // Test without milliseconds.
  EXPECT_EQ(
      util::fromTimestampString("2022-02-23 20:30:00", nullptr),
      parseJoda("2022-02-23T20:30:00", "yyyy-MM-dd'T'HH:mm:ss").timestamp);

  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223", nullptr),
      parseJoda("2022-02-23T12:15:00.776", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .timestamp);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000", nullptr),
      parseJoda("000", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001", nullptr),
      parseJoda("001", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999", nullptr),
      parseJoda("999", "SSS").timestamp);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045", nullptr),
      parseJoda("045", "SSS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450", nullptr),
      parseJoda("45", "SS").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450", nullptr),
      parseJoda("45", "SSSS").timestamp);

  EXPECT_THROW(parseJoda("-1", "S"), BoltUserError);
  //   EXPECT_THROW(parseJoda("999", "S"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, parseConsecutiveSpecifiers) {
  EXPECT_EQ(
      util::fromTimestampString("2012-12-01", nullptr),
      parseJoda("1212", "YYM").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0012-12-01", nullptr),
      parseJoda("1212", "MY").timestamp);
  EXPECT_THROW(parseJoda("1212", "YM"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00", nullptr),
      parseJoda("1212", "YYH").timestamp);
  EXPECT_EQ(
      util::fromTimestampString("0012-01-01 12:00:00", nullptr),
      parseJoda("1212", "HY").timestamp);
  //   EXPECT_THROW(parseJoda("1212", "YH"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00", nullptr),
      parseJoda("1212", "yyH").timestamp);
  EXPECT_THROW(parseJoda("12312", "yyH"), BoltUserError);
}

TEST_F(JodaDateTimeFormatterTest, formatResultSize) {
  auto* timezone = tz::locateZone("GMT");

  // Year can have up to 9 digits (+/-292278994) plus a leading sign.
  EXPECT_EQ(
      buildJodaDateTimeFormatter("yyyy-MM-dd")->maxResultSize(timezone), 16);
  EXPECT_EQ(buildJodaDateTimeFormatter("yyyy-MM")->maxResultSize(timezone), 13);
  EXPECT_EQ(buildJodaDateTimeFormatter("y")->maxResultSize(timezone), 10);
  EXPECT_EQ(
      buildJodaDateTimeFormatter("yyyy////MM////dd")->maxResultSize(timezone),
      22);
  EXPECT_EQ(
      buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSS")
          ->maxResultSize(timezone),
      35);
  // No padding. CENTURY_OF_ERA can be at most 7 digits.
  EXPECT_EQ(buildJodaDateTimeFormatter("C")->maxResultSize(timezone), 7);
  EXPECT_EQ(buildJodaDateTimeFormatter("CCCC")->maxResultSize(timezone), 7);
}

class MysqlDateTimeTest : public DateTimeFormatterTest {};

TEST_F(MysqlDateTimeTest, validBuild) {
  std::vector<DateTimeToken> expected;

  expected = {DateTimeToken(" ")};
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter(" ")->tokens());

  expected = {
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter(" %Y")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
      DateTimeToken(" "),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%y ")->tokens());

  expected = {DateTimeToken(" 132&2618*673 *--+= }{[]\\:")};
  EXPECT_EQ(
      expected,
      buildMysqlDateTimeFormatter(" 132&2618*673 *--+= }{[]\\:")->tokens());

  expected = {
      DateTimeToken("   "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" &^  "),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("   %Y &^  ")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
      DateTimeToken("   & "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%y  % & %Y %Y%")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken(" 'T'"),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%Y 'T'")->tokens());

  expected = {DateTimeToken("1''2")};
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("1''2")->tokens());

  expected = {
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 4}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MONTH_OF_YEAR, 2}),
      DateTimeToken("-"),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::DAY_OF_MONTH, 2}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::MINUTE_OF_HOUR, 2}),
      DateTimeToken(" "),
      DateTimeToken(FormatPattern{DateTimeFormatSpecifier::YEAR, 2}),
  };
  EXPECT_EQ(expected, buildMysqlDateTimeFormatter("%Y-%m-%d %i %y")->tokens());
}

TEST_F(MysqlDateTimeTest, invalidBuild) {
  // Unsupported specifiers
  EXPECT_THROW(buildMysqlDateTimeFormatter("%D"), BoltUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%U"), BoltUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%u"), BoltUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%V"), BoltUserError);
  EXPECT_THROW(buildMysqlDateTimeFormatter("%w"), BoltUserError);

  // Empty format string
  EXPECT_THROW(buildMysqlDateTimeFormatter(""), BoltUserError);
}

TEST_F(MysqlDateTimeTest, formatYear) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  auto* timezone = tz::locateZone("GMT");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("0-01-01", nullptr), timezone),
      "0000");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("1-01-01", nullptr), timezone),
      "0001");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("199-01-01", nullptr), timezone),
      "0199");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("9999-01-01", nullptr), timezone),
      "9999");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("-1-01-01", nullptr), timezone),
      "-0001");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("19999-01-01", nullptr), timezone),
      "19999");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("-19999-01-01", nullptr), timezone),
      "-19999");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("-1-01-01", nullptr), timezone),
      "-0001");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("-99999-01-01", nullptr), timezone),
      "-99999");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%Y", util::fromTimestampString("99999-01-01", nullptr), timezone),
      "99999");
}

TEST_F(MysqlDateTimeTest, formatMonthDay) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("0-01-01", nullptr), timezone),
      "01~01");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("0-10-24", nullptr), timezone),
      "10~24");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("199-09-30", nullptr), timezone),
      "09~30");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("9999-01-01", nullptr), timezone),
      "01~01");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("-1-01-01", nullptr), timezone),
      "01~01");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("19999-01-01", nullptr), timezone),
      "01~01");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d",
          util::fromTimestampString("-19999-01-01", nullptr),
          timezone),
      "01~01");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("-1-01-01", nullptr), timezone),
      "01~01");
  EXPECT_THROW(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("-1-13-01", nullptr), timezone),
      BoltUserError);
  EXPECT_THROW(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("1999-02-50", nullptr), timezone),
      BoltUserError);
  EXPECT_THROW(
      formatMysqlDateTime(
          "%m~%d", util::fromTimestampString("1999-02-29", nullptr), timezone),
      BoltUserError);
}

TEST_F(MysqlDateTimeTest, formatWeekday) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-04", nullptr),
          timezone),
      "Mon..->Monday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-05", nullptr),
          timezone),
      "Tue..->Tuesday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-06", nullptr),
          timezone),
      "Wed..->Wednesday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-07", nullptr),
          timezone),
      "Thu..->Thursday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-08", nullptr),
          timezone),
      "Fri..->Friday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-09", nullptr),
          timezone),
      "Sat..->Saturday");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%a..->%W",
          util::fromTimestampString("1999-01-10", nullptr),
          timezone),
      "Sun..->Sunday");
}

TEST_F(MysqlDateTimeTest, formatMonth) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-01-01", nullptr),
          timezone),
      "1-01-January");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-02-02", nullptr),
          timezone),
      "2-02-February");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-03-03", nullptr),
          timezone),
      "3-03-March");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-04-04", nullptr),
          timezone),
      "4-04-April");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-05-05", nullptr),
          timezone),
      "5-05-May");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-06-06", nullptr),
          timezone),
      "6-06-June");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-07-07", nullptr),
          timezone),
      "7-07-July");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-08-08", nullptr),
          timezone),
      "8-08-August");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-09-09", nullptr),
          timezone),
      "9-09-September");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-10-10", nullptr),
          timezone),
      "10-10-October");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-11-11", nullptr),
          timezone),
      "11-11-November");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%c-%m-%M",
          util::fromTimestampString("1999-12-12", nullptr),
          timezone),
      "12-12-December");
}

TEST_F(MysqlDateTimeTest, formatDayOfMonth) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%d-%e", util::fromTimestampString("2000-02-01", nullptr), timezone),
      "01-1");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%d-%e", util::fromTimestampString("2000-02-29", nullptr), timezone),
      "29-29");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%d-%e", util::fromTimestampString("2000-12-31", nullptr), timezone),
      "31-31");
}

TEST_F(MysqlDateTimeTest, formatFractionOfSecond) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.987", nullptr),
          timezone),
      "987000");

  // As our current precision is 3 decimal places.
  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.987654", nullptr),
          timezone),
      "987654");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.900654", nullptr),
          timezone),
      "900654");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.090654", nullptr),
          timezone),
      "090654");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.009654", nullptr),
          timezone),
      "009654");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%f",
          util::fromTimestampString("2000-02-01 00:00:00.000654", nullptr),
          timezone),
      "000654");
}

TEST_F(MysqlDateTimeTest, formatHour) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%h--%H--%I--%k--%l",
          util::fromTimestampString("2000-02-01 00:00:00", nullptr),
          timezone),
      "12--00--12--0--12");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%h--%H--%I--%k--%l",
          util::fromTimestampString("2000-02-01 12:12:01", nullptr),
          timezone),
      "12--12--12--12--12");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%h--%H--%I--%k--%l",
          util::fromTimestampString("2000-02-01 23:23:01", nullptr),
          timezone),
      "11--23--11--23--11");
}

TEST_F(MysqlDateTimeTest, formatMinute) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%i",
          util::fromTimestampString("2000-02-01 00:00:00", nullptr),
          timezone),
      "00");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%i",
          util::fromTimestampString("2000-02-01 00:09:00", nullptr),
          timezone),
      "09");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%i",
          util::fromTimestampString("2000-02-01 00:31:00", nullptr),
          timezone),
      "31");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%i",
          util::fromTimestampString("2000-02-01 00:59:00", nullptr),
          timezone),
      "59");
}

TEST_F(MysqlDateTimeTest, formatDayOfYear) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%j",
          util::fromTimestampString("2000-01-01 00:00:00", nullptr),
          timezone),
      "001");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%j",
          util::fromTimestampString("2000-12-31 00:09:00", nullptr),
          timezone),
      "366");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%j",
          util::fromTimestampString("1999-12-31 00:31:00", nullptr),
          timezone),
      "365");
}

TEST_F(MysqlDateTimeTest, formatAmPm) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%p",
          util::fromTimestampString("2000-01-01 00:00:00", nullptr),
          timezone),
      "AM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%p",
          util::fromTimestampString("2000-01-01 11:59:59", nullptr),
          timezone),
      "AM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%p",
          util::fromTimestampString("2000-01-01 12:00:00", nullptr),
          timezone),
      "PM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%p",
          util::fromTimestampString("2000-01-01 23:59:59", nullptr),
          timezone),
      "PM");
}

TEST_F(MysqlDateTimeTest, formatSecond) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      formatMysqlDateTime(
          "%s-%S",
          util::fromTimestampString("2000-01-01 00:00:00", nullptr),
          timezone),
      "00-00");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%s-%S",
          util::fromTimestampString("2000-01-01 00:00:30", nullptr),
          timezone),
      "30-30");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%s-%S",
          util::fromTimestampString("2000-01-01 00:00:59", nullptr),
          timezone),
      "59-59");
}

TEST_F(MysqlDateTimeTest, formatCompositeTime) {
  auto* timezone = tz::locateZone("GMT");

  // 12 hour %r
  EXPECT_EQ(
      formatMysqlDateTime(
          "%r",
          util::fromTimestampString("2000-01-01 00:00:00", nullptr),
          timezone),
      "12:00:00 AM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%r",
          util::fromTimestampString("2000-01-01 11:59:59", nullptr),
          timezone),
      "11:59:59 AM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%r",
          util::fromTimestampString("2000-01-01 12:00:00", nullptr),
          timezone),
      "12:00:00 PM");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%r",
          util::fromTimestampString("2000-01-01 23:59:59", nullptr),
          timezone),
      "11:59:59 PM");

  // 24 hour %T
  EXPECT_EQ(
      formatMysqlDateTime(
          "%T",
          util::fromTimestampString("2000-01-01 00:00:00", nullptr),
          timezone),
      "00:00:00");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%T",
          util::fromTimestampString("2000-01-01 11:59:59", nullptr),
          timezone),
      "11:59:59");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%T",
          util::fromTimestampString("2000-01-01 12:00:00", nullptr),
          timezone),
      "12:00:00");
  EXPECT_EQ(
      formatMysqlDateTime(
          "%T",
          util::fromTimestampString("2000-01-01 23:59:59", nullptr),
          timezone),
      "23:59:59");
}

// Same semantic as YEAR_OF_ERA, except that it accepts zero and negative
// years.
TEST_F(MysqlDateTimeTest, parseFourDigitYear) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  EXPECT_EQ(
      util::fromTimestampString("123-01-01", nullptr), parseMysql("123", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("321-01-01", nullptr), parseMysql("321", "%Y"));

  EXPECT_EQ(
      util::fromTimestampString("0-01-01", nullptr), parseMysql("0", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("-1-01-01", nullptr), parseMysql("-1", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("-1234-01-01", nullptr),
      parseMysql("-1234", "%Y"));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("0-01-01", nullptr),
      parseMysql("123 0", "%Y %Y"));

  // Plus sign consumption valid when %Y operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-01", nullptr), parseMysql("+10", "%Y"));
  EXPECT_EQ(
      util::fromTimestampString("99-02-01", nullptr),
      parseMysql("+99 02", "%Y %m"));
  EXPECT_EQ(
      util::fromTimestampString("10-10-01", nullptr),
      parseMysql("10 +10", "%m %Y"));
  EXPECT_EQ(
      util::fromTimestampString("100-02-01", nullptr),
      parseMysql("2+100", "%m%Y"));
  EXPECT_THROW(parseMysql("+10001", "%Y%m"), BoltUserError);
  EXPECT_THROW(parseMysql("++100", "%Y"), BoltUserError);

  // Probe the year range
  //   EXPECT_THROW(parseMysql("-10000", "%Y"), BoltUserError);
  //   EXPECT_THROW(parseMysql("10000", "%Y"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("9999-01-01", nullptr),
      parseMysql("9999", "%Y"));
}

TEST_F(MysqlDateTimeTest, parseTwoDigitYear) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01", nullptr), parseMysql("70", "%y"));
  EXPECT_EQ(
      util::fromTimestampString("2069-01-01", nullptr), parseMysql("69", "%y"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("00", "%y"));

  // Last token read overwrites:
  EXPECT_EQ(
      util::fromTimestampString("2030-01-01", nullptr),
      parseMysql("80 30", "%y %y"));
}

TEST_F(MysqlDateTimeTest, parseWeekYear) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Covers entire range of possible week year start dates (12-29 to 01-04)
  EXPECT_EQ(
      util::fromTimestampString("1969-12-29 00:00:00", nullptr),
      parseMysql("1970", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("2024-12-30 00:00:00", nullptr),
      parseMysql("2025", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("1934-12-31 00:00:00", nullptr),
      parseMysql("1935", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("1990-01-01 00:00:00", nullptr),
      parseMysql("1990", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("0204-01-02 00:00:00", nullptr),
      parseMysql("204", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-0102-01-03 00:00:00", nullptr),
      parseMysql("-102", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-0108-01-04 00:00:00", nullptr),
      parseMysql("-108", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("-1002-12-31 00:00:00", nullptr),
      parseMysql("-1001", "%x"));

  // Plus sign consumption valid when %x operator is not followed by another
  // specifier
  EXPECT_EQ(
      util::fromTimestampString("10-01-04", nullptr), parseMysql("+10", "%x"));
  EXPECT_EQ(
      util::fromTimestampString("0098-12-29", nullptr),
      parseMysql("+99 01", "%x %v"));
  EXPECT_EQ(
      util::fromTimestampString("0099-01-05", nullptr),
      parseMysql("+99 02", "%x %v"));
  EXPECT_EQ(
      util::fromTimestampString("10-03-08", nullptr),
      parseMysql("10 +10", "%v %x"));
  EXPECT_EQ(
      util::fromTimestampString("100-01-11", nullptr),
      parseMysql("2+100", "%v%x"));
  EXPECT_THROW(parseMysql("+10001", "%x%m"), BoltUserError);
  EXPECT_THROW(parseMysql("++100", "%x"), BoltUserError);

  // Probe week year range
  //   EXPECT_THROW(parseMysql("-292275055", "%x"), BoltUserError);
  //   EXPECT_THROW(parseMysql("292278994", "%x"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseMonth) {
  // Joda has this weird behavior where if minute or hour is specified, year
  // falls back to 2000, instead of epoch (1970)  ¯\_(ツ)_/¯
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("1", "%m"));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01", nullptr),
      parseMysql(" 7", " %m"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("01", "%m"));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01", nullptr),
      parseMysql(" 07", " %m"));
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01", nullptr),
      parseMysql("11-", "%m-"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01", nullptr),
      parseMysql("-12-", "-%m-"));

  EXPECT_THROW(parseMysql("0", "%m"), BoltUserError);
  EXPECT_THROW(parseMysql("13", "%m"), BoltUserError);
  //   EXPECT_THROW(parseMysql("12345", "%m"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("1", "%c"));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01", nullptr),
      parseMysql(" 7", " %c"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("01", "%c"));
  EXPECT_EQ(
      util::fromTimestampString("2000-07-01", nullptr),
      parseMysql(" 07", " %c"));
  EXPECT_EQ(
      util::fromTimestampString("2000-11-01", nullptr),
      parseMysql("11-", "%c-"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-01", nullptr),
      parseMysql("-12-", "-%c-"));

  EXPECT_THROW(parseMysql("0", "%c"), BoltUserError);
  EXPECT_THROW(parseMysql("13", "%c"), BoltUserError);
  //   EXPECT_THROW(parseMysql("12345", "%c"), BoltUserError);

  // Ensure %b and %M specifiers consume both short- and long-form month
  // names
  for (int i = 0; i < 12; i++) {
    std::string buildString("2000-" + std::to_string(i + 1) + "-01");
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseMysql(monthsShort[i], "%b"));
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseMysql(monthsFull[i], "%b"));
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseMysql(monthsShort[i], "%M"));
    EXPECT_EQ(
        util::fromTimestampString(StringView{buildString}, nullptr),
        parseMysql(monthsFull[i], "%M"));
  }

  // Month name invalid parse
  //   EXPECT_THROW(parseMysql("Decembr", "%b"), BoltUserError);
  //   EXPECT_THROW(parseMysql("Decembr", "%M"), BoltUserError);
  //   EXPECT_THROW(parseMysql("Decemberary", "%b"), BoltUserError);
  //   EXPECT_THROW(parseMysql("Decemberary", "%M"), BoltUserError);
  EXPECT_THROW(parseMysql("asdf", "%b"), BoltUserError);
  EXPECT_THROW(parseMysql("asdf", "%M"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfMonth) {
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("1", "%d"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07", nullptr),
      parseMysql("7 ", "%d "));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11", nullptr),
      parseMysql("/11", "/%d"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseMysql("/31/", "/%d/"));

  EXPECT_THROW(parseMysql("0", "%d"), BoltUserError);
  EXPECT_THROW(parseMysql("32", "%d"), BoltUserError);
  //   EXPECT_THROW(parseMysql("12345", "%d"), BoltUserError);

  EXPECT_THROW(parseMysql("02-31", "%m-%d"), BoltUserError);
  EXPECT_THROW(parseMysql("04-31", "%m-%d"), BoltUserError);

  // Ensure all days of month are checked against final selected month
  EXPECT_THROW(parseMysql("1 31 20 2", "%m %d %d %m"), BoltUserError);
  EXPECT_THROW(parseMysql("2 31 20 4", "%m %d %d %m"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseMysql("2 31 1", "%m %d %m"));

  // Probe around leap year.
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29", nullptr),
      parseMysql("2000-02-29", "%Y-%m-%d"));
  EXPECT_THROW(parseMysql("2001-02-29", "%Y-%m-%d"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseDayOfYear) {
  // Just day of year specifier should default to 2000. Also covers leap year
  // case
  EXPECT_EQ(
      util::fromTimestampString("2000-01-01", nullptr), parseMysql("1", "%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-07", nullptr),
      parseMysql("7 ", "%j "));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-11", nullptr),
      parseMysql("/11", "/%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-01-31", nullptr),
      parseMysql("/31/", "/%j/"));
  EXPECT_EQ(
      util::fromTimestampString("2000-02-01", nullptr), parseMysql("32", "%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-02-29", nullptr), parseMysql("60", "%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-30", nullptr),
      parseMysql("365", "%j"));
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31", nullptr),
      parseMysql("366", "%j"));

  // Year specified cases
  EXPECT_EQ(
      util::fromTimestampString("1950-01-01", nullptr),
      parseMysql("1950 1", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-07", nullptr),
      parseMysql("1950 7 ", "%Y %j "));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-11", nullptr),
      parseMysql("1950 /11", "%Y /%j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-01-31", nullptr),
      parseMysql("1950 /31/", "%Y /%j/"));
  EXPECT_EQ(
      util::fromTimestampString("1950-02-01", nullptr),
      parseMysql("1950 32", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-03-01", nullptr),
      parseMysql("1950 60", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("1950-12-31", nullptr),
      parseMysql("1950 365", "%Y %j"));
  EXPECT_THROW(parseMysql("1950 366", "%Y %j"), BoltUserError);

  // Negative year specified cases
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-01", nullptr),
      parseMysql("-1950 1", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-07", nullptr),
      parseMysql("-1950 7 ", "%Y %j "));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-11", nullptr),
      parseMysql("-1950 /11", "%Y /%j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-01-31", nullptr),
      parseMysql("-1950 /31/", "%Y /%j/"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-02-01", nullptr),
      parseMysql("-1950 32", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-03-01", nullptr),
      parseMysql("-1950 60", "%Y %j"));
  EXPECT_EQ(
      util::fromTimestampString("-1950-12-31", nullptr),
      parseMysql("-1950 365", "%Y %j"));
  EXPECT_THROW(parseMysql("-1950 366", "%Y %j"), BoltUserError);

  // Ensure all days of year are checked against final selected year
  EXPECT_THROW(parseMysql("2000 366 2001", "%Y %j %Y"), BoltUserError);
  EXPECT_EQ(
      util::fromTimestampString("2000-12-31", nullptr),
      parseMysql("2001 366 2000", "%Y %j %Y"));

  EXPECT_THROW(parseMysql("0", "%j"), BoltUserError);
  EXPECT_THROW(parseMysql("367", "%j"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseHourOfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00", nullptr),
      parseMysql("23", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0", "%H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseMysql("10", "%H"));

  // Hour of day invalid
  EXPECT_THROW(parseMysql("24", "%H"), BoltUserError);
  EXPECT_THROW(parseMysql("-1", "%H"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%H"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 23:00:00", nullptr),
      parseMysql("23", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0", "%k"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseMysql("10", "%k"));

  // Hour of day invalid
  EXPECT_THROW(parseMysql("24", "%k"), BoltUserError);
  EXPECT_THROW(parseMysql("-1", "%k"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%k"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseClockHourOfHalfDay) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1", "%h"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseMysql("10", "%h"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%h"), BoltUserError);
  EXPECT_THROW(parseMysql("0", "%h"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%h"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1", "%I"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseMysql("10", "%I"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%l"), BoltUserError);
  EXPECT_THROW(parseMysql("0", "%l"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%l"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1", "%l"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 10:00:00", nullptr),
      parseMysql("10", "%l"));

  // Clock hour of half day invalid
  EXPECT_THROW(parseMysql("13", "%l"), BoltUserError);
  EXPECT_THROW(parseMysql("0", "%l"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%l"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseHalfOfDay) {
  if (::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  // Half of day has no effect if hour of day is provided
  // hour of day tests
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 PM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 AM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 pm", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 am", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 PM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 AM", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 pm", "%H %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 am", "%H %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 PM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 AM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 pm", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 07:00:00", nullptr),
      parseMysql("7 am", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 PM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 AM", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 pm", "%k %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0 am", "%k %p"));

  // Half of day has effect if clockhour of halfday is provided
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseMysql("1 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1 AM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00", nullptr),
      parseMysql("6 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00", nullptr),
      parseMysql("6 AM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseMysql("12 PM", "%h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12 AM", "%h %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseMysql("1 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1 AM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00", nullptr),
      parseMysql("6 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00", nullptr),
      parseMysql("6 AM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseMysql("12 PM", "%I %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12 AM", "%I %p"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseMysql("1 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("1 AM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 18:00:00", nullptr),
      parseMysql("6 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 06:00:00", nullptr),
      parseMysql("6 AM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseMysql("12 PM", "%l %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("12 AM", "%l %p"));

  // time gives precedent to most recent time specifier
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 01:00:00", nullptr),
      parseMysql("0 1 AM", "%H %h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 13:00:00", nullptr),
      parseMysql("12 1 PM", "%H %h %p"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("1 AM 0", "%h %p %H"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 12:00:00", nullptr),
      parseMysql("1 AM 12", "%h %p %H"));
}

TEST_F(MysqlDateTimeTest, parseMinute) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:08:00", nullptr),
      parseMysql("8", "%i"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:59:00", nullptr),
      parseMysql("59", "%i"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0/", "%i/"));

  EXPECT_THROW(parseMysql("60", "%i"), BoltUserError);
  EXPECT_THROW(parseMysql("-1", "%i"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%i"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseSecond) {
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09", nullptr),
      parseMysql("9", "%s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58", nullptr),
      parseMysql("58", "%s"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0/", "%s/"));

  EXPECT_THROW(parseMysql("60", "%s"), BoltUserError);
  EXPECT_THROW(parseMysql("-1", "%s"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%s"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:09", nullptr),
      parseMysql("9", "%S"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:58", nullptr),
      parseMysql("58", "%S"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00", nullptr),
      parseMysql("0/", "%S/"));

  EXPECT_THROW(parseMysql("60", "%S"), BoltUserError);
  EXPECT_THROW(parseMysql("-1", "%S"), BoltUserError);
  //   EXPECT_THROW(parseMysql("123456789", "%S"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseMixedYMDFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 23:00:00", nullptr),
      parseMysql("2021-01-04+23:00:00", "%Y-%m-%d+%H:%i:%s"));

  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10", nullptr),
      parseMysql("2019-07-03 11:04:10", "%Y-%m-%d %H:%i:%s"));

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2019-07-03 11:04:10", nullptr),
      parseMysql("10:04:11 03-07-2019", "%s:%i:%H %d-%m-%Y"));
}

TEST_F(MysqlDateTimeTest, parseMixedWeekFormat) {
  // Common patterns found.
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 13:29:21.213", "%x %v %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 13:29:21.213", "%x %v %H:%i:%s.%f"));

  // Day of week short text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 Mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 Thu 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text normal capitlization
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 Monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 Thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week short text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 MON 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 THU 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text upper case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 MONDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 THURSDAY 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week short text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 mon 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 thu 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Day of week long text lower case
  EXPECT_EQ(
      util::fromTimestampString("2021-01-04 13:29:21.213", nullptr),
      parseMysql("2021 1 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("2021 22 monday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("2021-06-03 13:29:21.213", nullptr),
      parseMysql("2021 22 thursday 13:29:21.213", "%x %v %W %H:%i:%s.%f"));

  // Invalid day of week throw cases
  EXPECT_THROW(parseMysql("mOn", "E"), BoltUserError);
  EXPECT_THROW(parseMysql("tuE", "E"), BoltUserError);
  EXPECT_THROW(parseMysql("WeD", "E"), BoltUserError);
  EXPECT_THROW(parseMysql("WEd", "E"), BoltUserError);
  EXPECT_THROW(parseMysql("MONday", "EEE"), BoltUserError);
  EXPECT_THROW(parseMysql("monDAY", "EEE"), BoltUserError);
  EXPECT_THROW(parseMysql("frIday", "EEE"), BoltUserError);

  // Backwards, just for fun:
  EXPECT_EQ(
      util::fromTimestampString("2021-05-31 13:29:21.213", nullptr),
      parseMysql("213.21:29:13 22 2021", "%f.%s:%i:%H %v %x"));
}

TEST_F(MysqlDateTimeTest, parseFractionOfSecond) {
  // Assert on difference in milliseconds.
  EXPECT_NE(
      util::fromTimestampString("2022-02-23 12:15:00.223", nullptr),
      parseMysql("2022-02-23T12:15:00.776", "%Y-%m-%dT%H:%i:%s.%f"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.000", nullptr),
      parseMysql("000", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.001", nullptr),
      parseMysql("001", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.999", nullptr),
      parseMysql("999", "%f"));

  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.045", nullptr),
      parseMysql("045", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450", nullptr),
      parseMysql("45", "%f"));
  EXPECT_EQ(
      util::fromTimestampString("1970-01-01 00:00:00.450", nullptr),
      parseMysql("45", "%f"));

  EXPECT_THROW(parseMysql("-1", "%f"), BoltUserError);
  //   EXPECT_THROW(parseMysql("9999999", "%f"), BoltUserError);
}

TEST_F(MysqlDateTimeTest, parseConsecutiveSpecifiers) {
  EXPECT_EQ(
      util::fromTimestampString("2012-12-01", nullptr),
      parseMysql("1212", "%y%m"));
  EXPECT_EQ(
      util::fromTimestampString("0012-12-01", nullptr),
      parseMysql("1212", "%m%Y"));
  EXPECT_THROW(parseMysql("1212", "%Y%m"), BoltUserError);

  EXPECT_EQ(
      util::fromTimestampString("2012-01-01 12:00:00", nullptr),
      parseMysql("1212", "%y%H"));
  EXPECT_EQ(
      util::fromTimestampString("0012-01-01 12:00:00", nullptr),
      parseMysql("1212", "%H%Y"));
  EXPECT_THROW(parseMysql("1212", "%Y%H"), BoltUserError);
}

TEST_F(DateTimeFormatterTest, simpleDateFormatParsing) {
  const auto* utc = tz::locateZone("UTC");
  const Timestamp centuryStart = Timestamp::fromMillis(-946684800000LL);
  struct Case {
    const char* pattern;
    const char* input;
    int64_t millis;
  };
  const Case cases[] = {
      {"yyyy-MM-dd HH:mm:ss.SSS", "2023-02-29 25:61:61.1001", 1677722522001LL},
      {"yyyy-MM-dd HH:mm:ss.SSS", "1969-12-31 23:59:59.999", -1LL},
      {"yyyy-MM-dd", "   2024-  01-\t02", 1704153600000LL},
      {"yyyyMMdd", "２０２４０２２９", 1709164800000LL},
      {"yyyy-MM-dd", "1500-02-29", -14825894400000LL},
      {"yyyy-MM-dd", "10000-01-01", 253402300800000LL},
      {"yyyy-MM-dd", "999999-01-01", 31494753244800000LL},
      {"yy-MM-dd", "39-01-01", 2177452800000LL},
      {"yy-MM-dd", "40-01-01", 2208988800000LL},
      {"y-MM-dd", "1-01-01", -62135769600000LL},
      {"yyyy D", "1582 278", -12219292800000LL},
      {"yyyy-MM-dd D", "2024-01-02 60", 1709164800000LL},
      {"yyyy D MM-dd", "2024 60 01-02", 1704153600000LL},
      {"yyyy-MM-dd dd", "2024-01-32 02", 1704153600000LL},
      {"yyyy-MM-dd E", "2024-01-02 wEdNeSdAy", 1704153600000LL},
      {"YYYY-MM-dd", "2021-01-01", 1609027200000LL},
      {"yyyy-ww-u", "2021-1-7", 1609027200000LL},
      {"YYYY-ww-u", "2021-54-8", 1641772800000LL},
      {"yyyy-MM F E", "2024-02 -1 Wed", 1709078400000LL},
      {"yyyy-MM W u", "2024-02 6 8", 1709424000000LL},
      {"G yyyy-MM-dd", "BC 0001-01-01", -62167392000000LL},
      {"hh a", "25 PM", 133200000LL},
      {"HH a", "01 PM", 3600000LL},
      {"hh a HH", "12 PM 01", 3600000LL},
      {"MMM d y", "jAnUaRy 1 2024", 1704067200000LL},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.pattern);
    SCOPED_TRACE(test.input);
    const auto formatter =
        buildSimpleDateTimeFormatter(test.pattern, centuryStart);
    const auto parsed = formatter->parseToUtc(test.input, utc);
    ASSERT_FALSE(parsed.hasError());
    EXPECT_EQ(parsed.value().timestamp, Timestamp::fromMillis(test.millis));
    EXPECT_EQ(
        formatter->parse(test.input, TimePolicy::CORRECTED).value().timestamp,
        formatter->parseWithNanosecondPrecision(test.input).value().timestamp);
  }
  for (int width = 1; width <= 9; ++width) {
    auto formatter = buildSimpleDateTimeFormatter(std::string(width, 'S'));
    EXPECT_EQ(
        formatter->parse("1").value().timestamp, Timestamp::fromMillis(1));
    EXPECT_EQ(
        formatter->parse("1234").value().timestamp,
        Timestamp::fromMillis(1234));
  }
  for (const auto* pattern : {"C", "e", "x", "VV", "XXXX", "yyyy '", "'a''"}) {
    SCOPED_TRACE(pattern);
    EXPECT_THROW(buildSimpleDateTimeFormatter(pattern), BoltUserError);
  }
  EXPECT_TRUE(buildSimpleDateTimeFormatter("yyyy-MM-dd")
                  ->parse("+2024-01-01")
                  .hasError());
  EXPECT_TRUE(buildSimpleDateTimeFormatter("")->parse("").hasError());
}

TEST_F(DateTimeFormatterTest, simpleDateFormatFieldResolution) {
  struct Case {
    const char* pattern;
    const char* input;
    int64_t millis;
    bool strict;
  };
  const Case cases[] = {
      {"G MM-dd", "BC 01-01", -124304284800000LL, true},
      {"yy|yyyy-MM-dd", "00|1930-01-01", 1893456000000LL, true},
      {"YYYY|W", "1900|1", -2209075200000LL, false},
      {"W|yyyy|MM", "6|1582|13", -12210048000000LL, false},
      {"w|yyyy|YYYY", "54|0000|2024", 1736035200000LL, false},
      {"yyyy-MM-W-u", "1582-12-1-3", -12215232000000LL, true},
      {"yyyy-MM-W-u", "1582-10-5-3", -12218256000000LL, false},
      {"W|yyyy|MM", "54|1581|10", -12219120000000LL, false},
      {"G YYYY-w-u", "AD 1-1-1", -62136201600000LL, false},
      {"w|yyyy|YYYY H", "54|0000|2024 -48", -125976729600000LL, false},
      {"YYYY|yyyy-MM-dd", "2024|1930-01-01", -1262304000000LL, true},
  };
  for (const auto& test : cases) {
    for (bool lenient : {false, true}) {
      SCOPED_TRACE(std::string(test.pattern) + ":" + test.input);
      SCOPED_TRACE(lenient);
      const auto formatter = buildSimpleDateTimeFormatter(
          test.pattern, Timestamp::fromMillis(946684800000LL), lenient);
      const auto result =
          formatter->parseToUtc(test.input, tz::locateZone("UTC"));
      ASSERT_EQ(result.hasError(), !lenient && !test.strict);
      if (!result.hasError()) {
        EXPECT_EQ(result.value().timestamp, Timestamp::fromMillis(test.millis));
      }
    }
  }
}

TEST_F(DateTimeFormatterTest, simpleDateFormatTimeZones) {
  const auto* utc = tz::locateZone("UTC");
  for (const auto* pattern : {"Z", "ZZ", "ZZZZZ", "z", "zzzz"}) {
    auto formatter = buildSimpleDateTimeFormatter(pattern);
    for (const auto& [text, seconds] :
         std::vector<std::pair<std::string, int32_t>>{
             {"+0530", -19800},
             {"GMT+5:30", -19800},
             {"PST", 28800},
             {"Pacific Daylight Time", 25200},
             {"CEST", -7200},
             {"+2359", -86340}}) {
      SCOPED_TRACE(pattern);
      SCOPED_TRACE(text);
      EXPECT_EQ(
          formatter->parseToUtc(text, utc).value().timestamp,
          Timestamp(seconds, 0));
    }
    for (const auto* text : {"+05:30", "Z", "UT", "Asia/Shanghai", "+2400"}) {
      EXPECT_TRUE(formatter->parse(text).hasError()) << text;
    }
  }
  const auto named = buildSimpleDateTimeFormatter("z");
  EXPECT_EQ(
      named->parseToUtc("CST", tz::locateZone("Asia/Shanghai"))
          .value()
          .timestamp,
      Timestamp(-28800, 0));
  EXPECT_EQ(
      named->parseToUtc("CST", tz::locateZone("America/Chicago"))
          .value()
          .timestamp,
      Timestamp(21600, 0));
  EXPECT_EQ(
      named->parseToUtc("IST", tz::locateZone("Asia/Kolkata"))
          .value()
          .timestamp,
      Timestamp(-19800, 0));
  const char* offsets[] = {"+05", "+0530", "+05:30"};
  for (int width = 1; width <= 3; ++width) {
    auto formatter = buildSimpleDateTimeFormatter(std::string(width, 'X'));
    EXPECT_EQ(
        formatter->parseToUtc(offsets[width - 1], utc).value().timestamp,
        Timestamp(width == 1 ? -18000 : -19800, 0));
    EXPECT_EQ(
        formatter->parseToUtc("Z", utc).value().timestamp, Timestamp(0, 0));
  }
  auto formatter = buildSimpleDateTimeFormatter("yyyy-MM-dd HH:mm:ss");
  const auto* newYork = tz::locateZone("America/New_York");
  EXPECT_EQ(
      formatter->parseToUtc("2024-03-10 02:30:00", newYork).value().timestamp,
      Timestamp(1710055800, 0));
  EXPECT_EQ(
      formatter->parseToUtc("2024-11-03 01:30:00", newYork).value().timestamp,
      Timestamp(1730615400, 0));
  EXPECT_EQ(
      formatter
          ->parseToUtc("0001-01-01 00:00:00", tz::locateZone("Asia/Shanghai"))
          .value()
          .timestamp,
      Timestamp(-62135798400, 0));
}

TEST_F(DateTimeFormatterTest, simpleDateFormatCenturyWindow) {
  struct Case {
    const char* pattern;
    const char* input;
    const char* zone;
    int64_t millis;
    int64_t startMillis{-946684800000LL};
  };
  const Case cases[] = {
      {"yy-MM-dd HH:mm:ss", "39-12-31 16:00:00", "-08:00", 2208988800000LL},
      {"yy-MM-dd HH:mm:ss Z",
       "39-12-31 16:00:00 -0800",
       "UTC",
       2208988800000LL},
      {"YY-ww-u", "40-1-1", "UTC", 2209075200000LL, -946598400000LL},
      {"yy-MM-dd HH:mm:ss",
       "40-01-01 00:00:00",
       "Asia/Shanghai",
       2208960000000LL},
      {"yy-MM-dd HH:mm:ss",
       "40-01-01 07:59:59",
       "Asia/Shanghai",
       2208988799000LL},
      {"yy-MM-dd HH:mm:ss",
       "40-01-01 08:00:00",
       "Asia/Shanghai",
       2208988800000LL},
      {"yyyy-MM-dd HH:mm:ss",
       "1900-01-01 08:03:00",
       "Asia/Shanghai",
       -2208988620000LL},
      {"yy-MM-dd HH:mm:ss Z",
       "40-01-01 00:00:00 +0800",
       "UTC",
       2208960000000LL},
      {"yy-MM-dd HH:mm:ss Z",
       "40-01-01 08:00:00 +0800",
       "UTC",
       2208988800000LL},
  };
  for (const auto& test : cases) {
    const auto formatter = buildSimpleDateTimeFormatter(
        test.pattern, Timestamp::fromMillis(test.startMillis));
    const auto result =
        formatter->parseToUtc(test.input, tz::locateZone(test.zone));
    ASSERT_FALSE(result.hasError());
    EXPECT_EQ(result.value().timestamp, Timestamp::fromMillis(test.millis));
  }
}

TEST_F(DateTimeFormatterTest, simpleDateFormatOutput) {
  const auto render =
      [](std::string_view pattern, Timestamp timestamp, const char* zone) {
        const auto formatter = buildSimpleDateTimeFormatter(pattern);
        const auto* timezone = tz::locateZone(zone);
        const auto size = formatter->maxResultSize(timezone);
        std::string result(size, '\0');
        result.resize(
            formatter->format(timestamp, timezone, size, result.data(), true));
        return result;
      };
  EXPECT_EQ(
      render("S SS SSS SSSS", Timestamp::fromMillis(12), "UTC"),
      "12 12 012 0012");
  EXPECT_EQ(
      render("z zzzz Z ZZ ZZZZ X XX XXX", Timestamp(0, 0), "Asia/Kolkata"),
      "IST India Standard Time +0530 +0530 +0530 +05 +0530 +05:30");
  EXPECT_EQ(render("X XX XXX", Timestamp(0, 0), "+00:30"), "+00 +0030 +00:30");
  EXPECT_EQ(
      render("G yyyy-MM-dd D E u Y w W F", Timestamp(-12219292800, 0), "UTC"),
      "AD 1582-10-15 278 Fri 5 1582 40 1 1");
  EXPECT_EQ(
      render("yyyy-MM-dd", Timestamp(-14825894400, 0), "UTC"), "1500-02-29");
  EXPECT_EQ(render("", Timestamp(0, 0), "UTC"), "");
  EXPECT_EQ(
      render("z zzzz", Timestamp(0, 0), "Europe/Kyiv"),
      "EET Eastern European Time");
  EXPECT_EQ(
      render("z zzzz", Timestamp(1593561600, 0), "Europe/Kyiv"),
      "EEST Eastern European Summer Time");
  EXPECT_EQ(
      render("z zzzz", Timestamp(0, 0), "America/Ciudad_Juarez"),
      "MST Mountain Standard Time");
  EXPECT_EQ(
      render("z zzzz", Timestamp(1593561600, 0), "America/Ciudad_Juarez"),
      "MDT Mountain Daylight Time");
  for (auto id : tz::getTimeZoneIDs()) {
    if (id > tz::kMaxFixedOffsetTimeZoneId) {
      const auto name = tz::getTimeZoneName(id);
      SCOPED_TRACE(name);
      EXPECT_FALSE(render("z zzzz", Timestamp(0, 0), name.c_str()).empty());
    }
  }
}

TEST_F(DateTimeFormatterTest, simpleDateFormatContext) {
  auto context = std::make_shared<DateTimeFormatterContext>();
  context->defaultTimezone = tz::locateZone("Asia/Shanghai");
  auto formatter = buildSimpleDateTimeFormatter(
      "yyyy-MM-dd HH:mm:ss z", std::nullopt, false, context);
  EXPECT_EQ(
      formatter->parseToUtc("2024-01-01 00:00:00 CST", tz::locateZone("UTC"))
          .value()
          .timestamp,
      Timestamp(1704038400, 0));
  EXPECT_EQ(
      formatter
          ->parseToUtc(
              "2024-01-01 00:00:00 CST", tz::locateZone("America/Chicago"))
          .value()
          .timestamp,
      Timestamp(1704088800, 0));

  context->twoDigitYearStart = Timestamp::fromMillis(946657800000LL);
  context->twoDigitYearStartYear = 2000;
  formatter = buildSimpleDateTimeFormatter(
      "yy-MM-dd HH:mm:ss", std::nullopt, false, context);
  EXPECT_EQ(
      formatter->parseToUtc("99-12-31 23:00:00", tz::locateZone("UTC"))
          .value()
          .timestamp,
      Timestamp::fromMillis(4102441200000LL));
}

TEST_F(DateTimeFormatterTest, simpleDateFormatRawOffsetContexts) {
  const auto* utc = tz::locateZone("UTC");
  const auto* losAngeles = tz::locateZone("America/Los_Angeles");
  const auto* newYork = tz::locateZone("America/New_York");
  auto first = std::make_shared<DateTimeFormatterContext>();
  first->defaultTimezone = utc;
  first->timezoneRawOffsets = {
      {losAngeles->id(), -28800}, {newYork->id(), -18000}};
  auto second = std::make_shared<DateTimeFormatterContext>(*first);
  second->timezoneRawOffsets[losAngeles->id()] = -25200;
  second->timezoneRawOffsets[newYork->id()] = -14400;
  const auto standard =
      buildSimpleDateTimeFormatter("z", std::nullopt, false, first);
  const auto shifted =
      buildSimpleDateTimeFormatter("z", std::nullopt, false, second);
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(
        standard->parseToUtc("PST", utc).value().timestamp,
        Timestamp(28800, 0));
    EXPECT_EQ(
        shifted->parseToUtc("EST", utc).value().timestamp, Timestamp(14400, 0));
    EXPECT_EQ(
        shifted->parseToUtc("PST", utc).value().timestamp, Timestamp(25200, 0));
    EXPECT_EQ(
        standard->parseToUtc("EST", utc).value().timestamp,
        Timestamp(18000, 0));
    EXPECT_EQ(
        standard->parseToUtc("PDT", utc).value().timestamp,
        Timestamp(25200, 0));
  }
}

TEST_F(DateTimeFormatterTest, simpleDateFormatSystemTimezone) {
  std::unique_ptr<icu::TimeZone> systemZone(icu::TimeZone::createDefault());
  ASSERT_NE(systemZone, nullptr);
  icu::UnicodeString id;
  systemZone->getID(id);
  std::string name;
  id.toUTF8String(name);
  if (name.starts_with("GMT+") || name.starts_with("GMT-")) {
    name.erase(0, 3);
  }
  const auto* zone = tz::locateZone(name, false);
  const auto* utc = tz::locateZone(0);
  for (auto startMillis : {946670400000LL, 946692000000LL}) {
    auto context = std::make_shared<DateTimeFormatterContext>();
    context->twoDigitYearStart = Timestamp::fromMillis(startMillis);
    auto explicitContext = std::make_shared<DateTimeFormatterContext>(*context);
    explicitContext->defaultTimezone = zone ? zone : utc;
    for (const auto& [pattern, input] :
         {std::pair{"yyyy-MM-dd HH:mm:ss z", "2024-01-01 00:00:00 CST"},
          std::pair{"yy-MM-dd HH:mm:ss", "99-12-31 23:00:00"},
          std::pair{"yy-MM-dd HH:mm:ss", "00-01-01 01:00:00"}}) {
      SCOPED_TRACE(
          fmt::format("{} {} {} {}", name, startMillis, pattern, input));
      const auto expected = buildSimpleDateTimeFormatter(
                                pattern, std::nullopt, false, explicitContext)
                                ->parseToUtc(input, utc)
                                .value()
                                .timestamp;
      EXPECT_EQ(
          buildSimpleDateTimeFormatter(pattern, std::nullopt, false, context)
              ->parseToUtc(input, utc)
              .value()
              .timestamp,
          expected);
      EXPECT_EQ(
          buildSimpleDateTimeFormatter(
              pattern, context->twoDigitYearStart, false)
              ->parseToUtc(input, utc)
              .value()
              .timestamp,
          expected);
    }
    EXPECT_EQ(context->defaultTimezone, nullptr);
  }

  // Fixed Java 8 results also pin the UTC fallback for the invalid test zone.
  if (name == "UTC" || name == "Etc/UTC" || name == "GMT" ||
      name == "Etc/GMT" || name == "Asia/Shanghai" || name == "Invalid/Zone") {
    const bool shanghai = name == "Asia/Shanghai";
    EXPECT_EQ(
        buildSimpleDateTimeFormatter(
            "yyyy-MM-dd HH:mm:ss z", std::nullopt, false)
            ->parseToUtc("2024-01-01 00:00:00 CST", utc)
            .value()
            .timestamp,
        Timestamp(shanghai ? 1704038400 : 1704088800, 0));
    EXPECT_EQ(
        buildSimpleDateTimeFormatter(
            "yy-MM-dd HH:mm:ss", Timestamp::fromMillis(946670400000), false)
            ->parseToUtc("99-12-31 23:00:00", utc)
            .value()
            .timestamp,
        Timestamp(shanghai ? 4102441200LL : 946681200LL, 0));
  }
}

TEST_F(DateTimeFormatterTest, simpleDateFormatRebaseContext) {
  auto context = std::make_shared<DateTimeFormatterContext>();
  context->rebaseTimezone = tz::locateZone("Asia/Shanghai");
  auto formatter =
      buildLegacySparkDateTimeFormatter("yyyy-MM-dd HH:mm:ss", context);
  EXPECT_EQ(
      formatter
          ->parseToUtc(
              "1500-02-28 00:00:00", tz::locateZone("UTC"), TimePolicy::LEGACY)
          .value()
          .timestamp,
      Timestamp(-14826758743LL, 0));
  auto localContext = buildLegacySparkDateTimeFormatter("yyyy-MM-dd HH:mm:ss");
  EXPECT_EQ(
      localContext
          ->parseToUtc(
              "1500-02-28 00:00:00", tz::locateZone("UTC"), TimePolicy::LEGACY)
          .value()
          .timestamp,
      Timestamp(-14826758400LL, 0));
}

TEST_F(DateTimeFormatterTest, localTimeTransitions) {
  const auto modern = buildJavaDateTimeFormatter("uuuu-MM-dd HH:mm:ss");
  const auto simple = buildSimpleDateTimeFormatter("yyyy-MM-dd HH:mm:ss");
  const auto strict = buildLegacySparkDateTimeFormatter("yyyy-MM-dd HH:mm:ss");
  const auto* zone = tz::locateZone("America/New_York");
  struct Case {
    const char* local;
    const char* modernUtc;
    const char* simpleUtc;
    bool gap{false};
  };
  // Interleave unique times with both transition boundaries to exercise cached
  // intervals in both directions. Java time selects the earlier overlap.
  const Case cases[] = {
      {"2024-07-01 00:00:00", "2024-07-01 04:00:00", "2024-07-01 04:00:00"},
      {"2024-11-03 01:00:00", "2024-11-03 05:00:00", "2024-11-03 06:00:00"},
      {"2024-11-03 01:59:59", "2024-11-03 05:59:59", "2024-11-03 06:59:59"},
      {"2024-11-03 02:00:00", "2024-11-03 07:00:00", "2024-11-03 07:00:00"},
      {"2024-11-03 01:30:00", "2024-11-03 05:30:00", "2024-11-03 06:30:00"},
      {"2024-03-10 01:59:59", "2024-03-10 06:59:59", "2024-03-10 06:59:59"},
      {"2024-03-10 02:00:00",
       "2024-03-10 07:00:00",
       "2024-03-10 07:00:00",
       true},
      {"2024-03-10 03:00:00", "2024-03-10 07:00:00", "2024-03-10 07:00:00"},
      {"2024-03-10 02:59:59",
       "2024-03-10 07:59:59",
       "2024-03-10 07:59:59",
       true},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.local);
    const auto expected = parseMysql(test.simpleUtc, "%Y-%m-%d %H:%i:%s");
    EXPECT_EQ(
        modern->parseToUtc(test.local, zone).value().timestamp,
        parseMysql(test.modernUtc, "%Y-%m-%d %H:%i:%s"));
    EXPECT_EQ(simple->parseToUtc(test.local, zone).value().timestamp, expected);
    const auto parsed = strict->parseToUtc(test.local, zone);
    EXPECT_EQ(parsed.hasError(), test.gap);
    if (!test.gap) {
      EXPECT_EQ(parsed.value().timestamp, expected);
    }
  }
  EXPECT_EQ(
      buildLegacySparkDateTimeFormatter("yyyyMMdd")
          ->parseToUtc("19490501", tz::locateZone("Asia/Shanghai"))
          .value()
          .timestamp,
      Timestamp(-652348800, 0));
  EXPECT_TRUE(
      strict->parseToUtc("1949-05-01 00:00:00", tz::locateZone("Asia/Shanghai"))
          .hasError());
}

TEST_F(DateTimeFormatterTest, formatterCache) {
  DateTimeFormatterCache cache;
  int builds = 0;
  auto get = [&](std::string_view pattern) {
    return cache.get(pattern, [&] {
      ++builds;
      return buildJavaDateTimeFormatter(pattern);
    });
  };
  const auto year = get("yyyy");
  const auto month = get("MM");
  EXPECT_EQ(year, get("yyyy"));
  EXPECT_EQ(month, get("MM"));
  EXPECT_EQ(builds, 2);
  for (size_t i = 0; i < DateTimeFormatterCache::kMaxEntries; ++i) {
    get("yyyy'" + std::to_string(i) + "'");
  }
  EXPECT_NE(year, get("yyyy"));
  EXPECT_EQ(builds, DateTimeFormatterCache::kMaxEntries + 3);

  const auto before = builds;
  const std::string large =
      "'" + std::string(DateTimeFormatterCache::kMaxPatternSize, 'x') + "'";
  EXPECT_NE(get(large), get(large));
  EXPECT_EQ(builds, before + 2);
  EXPECT_THROW(get("invalid"), BoltUserError);
  EXPECT_THROW(get("invalid"), BoltUserError);
  EXPECT_EQ(builds, before + 4);
  EXPECT_EQ(year->parse("2024").value().timestamp, Timestamp(1704067200, 0));
}

TEST_F(DateTimeFormatterTest, formatterCacheNegativeResult) {
  DateTimeFormatterCache cache;
  int builds = 0;
  auto unsupported = [&] {
    return cache.get("VV", [&]() -> std::shared_ptr<DateTimeFormatter> {
      ++builds;
      return nullptr;
    });
  };
  EXPECT_EQ(unsupported(), nullptr);
  EXPECT_EQ(unsupported(), nullptr);
  EXPECT_EQ(builds, 1);
  for (size_t i = 0; i < DateTimeFormatterCache::kMaxEntries; ++i) {
    const auto pattern = "yyyy'" + std::to_string(i) + "'";
    EXPECT_NE(
        cache.get(pattern, [&] { return buildJavaDateTimeFormatter(pattern); }),
        nullptr);
  }
  EXPECT_EQ(unsupported(), nullptr);
  EXPECT_EQ(builds, 2);
}

TEST_F(DateTimeFormatterTest, formatterCacheCenturyWindow) {
  for (const auto* pattern : {"y-MM-dd", "yy-MM-dd", "Y-ww-u", "YY-ww-u"}) {
    DateTimeFormatterCache cache;
    auto get = [&](bool cacheTwoDigitYears) {
      return cache.get(
          pattern,
          [&] { return buildLegacySparkDateTimeFormatter(pattern); },
          cacheTwoDigitYears);
    };
    EXPECT_NE(get(false), get(false));
    EXPECT_EQ(get(true), get(true));
  }
  DateTimeFormatterCache fixedWindowCache;
  auto fixed = [&] {
    return fixedWindowCache.get(
        "yy",
        [&] { return buildSimpleDateTimeFormatter("yy", Timestamp(0, 0)); },
        false);
  };
  EXPECT_EQ(fixed(), fixed());
  DateTimeFormatterCache cache;
  auto quoted = [&] {
    return cache.get(
        "yyyy-MM-dd 'yy'",
        [&] { return buildLegacySparkDateTimeFormatter("yyyy-MM-dd 'yy'"); },
        false);
  };
  EXPECT_EQ(quoted(), quoted());
}

} // namespace bytedance::bolt::functions
