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

#include <date/tz.h>
#include <optional>
#include <string>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/tz/TimeZoneMap.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;

class PrestoSqlDateTimeFunctionsTest
    : public functions::test::FunctionBaseTest {
 protected:
  std::string daysShort[7] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

  std::string daysLong[7] = {
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
      "Sunday"};

  std::string monthsShort[12] = {
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
      "Dec"};

  std::string monthsLong[12] = {
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
      "December"};

  std::string padNumber(int number) {
    return number < 10 ? "0" + std::to_string(number) : std::to_string(number);
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  void setQueryDateTruncOptimization(const std::string& optimizationOn) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kEsDateTruncOptimization, optimizationOn}});
  }

  void disableAdjustTimestampToTimezone() {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kAdjustTimestampToTimezone, "false"},
    });
  }

  void enableAeolusFunction() {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::KEnableAeolusFunction, "true"},
    });
  }

 public:
  struct TimestampWithTimezone {
    TimestampWithTimezone(int64_t milliSeconds, int16_t timezoneId)
        : milliSeconds_(milliSeconds), timezoneId_(timezoneId) {}

    int64_t milliSeconds_{0};
    int16_t timezoneId_{0};

    // Provides a nicer printer for gtest.
    friend std::ostream& operator<<(
        std::ostream& os,
        const TimestampWithTimezone& in) {
      return os << "TimestampWithTimezone(milliSeconds: " << in.milliSeconds_
                << ", timezoneId: " << in.timezoneId_ << ")";
    }
  };

  std::optional<TimestampWithTimezone> parseDatetime(
      const std::optional<std::string>& input,
      const std::optional<std::string>& format) {
    auto resultVector = evaluate(
        "parse_datetime(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<std::string>({input}),
             makeNullableFlatVector<std::string>({format})}));
    EXPECT_EQ(1, resultVector->size());

    if (resultVector->isNullAt(0)) {
      return std::nullopt;
    }

    auto rowVector = resultVector->as<RowVector>();
    return TimestampWithTimezone{
        rowVector->children()[0]->as<SimpleVector<int64_t>>()->valueAt(0),
        rowVector->children()[1]->as<SimpleVector<int16_t>>()->valueAt(0)};
  }

  std::optional<Timestamp> dateParse(
      const std::optional<std::string>& input,
      const std::optional<std::string>& format) {
    auto resultVector = evaluate(
        "date_parse(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<std::string>({input}),
             makeNullableFlatVector<std::string>({format})}));
    EXPECT_EQ(1, resultVector->size());

    if (resultVector->isNullAt(0)) {
      return std::nullopt;
    }
    return resultVector->as<SimpleVector<Timestamp>>()->valueAt(0);
  }

  std::optional<std::string> dateFormat(
      std::optional<Timestamp> timestamp,
      const std::string& format) {
    auto resultVector = evaluate(
        "date_format(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  std::optional<std::string> formatDatetime(
      std::optional<Timestamp> timestamp,
      const std::string& format) {
    auto resultVector = evaluate(
        "format_datetime(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  std::optional<std::string> formatDatetimeWithTimezone(
      std::optional<Timestamp> timestamp,
      std::optional<std::string> timeZoneName,
      const std::string& format) {
    auto resultVector = evaluate(
        "format_datetime(c0, c1)",
        makeRowVector(
            {makeTimestampWithTimeZoneVector(
                 timestamp.value().toMillis(), timeZoneName.value().c_str()),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  template <typename T>
  std::optional<T> evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluateOnce<T>(
          expression,
          makeRowVector({BaseVector::createNullConstant(
              TIMESTAMP_WITH_TIME_ZONE(), 1, pool())}));
    }

    return evaluateOnce<T>(
        expression,
        makeRowVector({makeTimestampWithTimeZoneVector(
            timestamp.value(), timeZoneName.value().c_str())}));
  }

  VectorPtr evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluate(
          expression,
          makeRowVector({BaseVector::createNullConstant(
              TIMESTAMP_WITH_TIME_ZONE(), 1, pool())}));
    }

    return evaluate(
        expression,
        makeRowVector({makeTimestampWithTimeZoneVector(
            timestamp.value(), timeZoneName.value().c_str())}));
  }

  int32_t parseDate(const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  }

  RowVectorPtr makeTimestampWithTimeZoneVector(
      int64_t timestamp,
      const char* tz) {
    const int64_t tzid = tz::getTimeZoneID(tz, true);

    return std::make_shared<RowVector>(
        pool(),
        TIMESTAMP_WITH_TIME_ZONE(),
        nullptr,
        1,
        std::vector<VectorPtr>(
            {makeNullableFlatVector<int64_t>({timestamp}),
             makeNullableFlatVector<int16_t>({tzid})}));
  }

  RowVectorPtr makeTimestampWithTimeZoneVector(
      const VectorPtr& timestamps,
      const VectorPtr& timezones) {
    BOLT_CHECK_EQ(timestamps->size(), timezones->size());
    return std::make_shared<RowVector>(
        pool(),
        TIMESTAMP_WITH_TIME_ZONE(),
        nullptr,
        timestamps->size(),
        std::vector<VectorPtr>({timestamps, timezones}));
  }

  int32_t getCurrentDate(const std::optional<std::string>& timeZone) {
    return parseDate(::date::format(
        "%Y-%m-%d",
        timeZone.has_value()
            ? ::date::make_zoned(
                  timeZone.value(), std::chrono::system_clock::now())
            : std::chrono::system_clock::now()));
  }
};

bool operator==(
    const PrestoSqlDateTimeFunctionsTest::TimestampWithTimezone& a,
    const PrestoSqlDateTimeFunctionsTest::TimestampWithTimezone& b) {
  return a.milliSeconds_ == b.milliSeconds_ && a.timezoneId_ == b.timezoneId_;
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, dateTruncSignatures) {
  auto signatures = getSignatureStrings("date_trunc");
  ASSERT_EQ(3, signatures.size());

  ASSERT_EQ(
      1,
      signatures.count(
          "(varchar,timestamp with time zone) -> timestamp with time zone"));
  ASSERT_EQ(1, signatures.count("(varchar,date) -> date"));
  ASSERT_EQ(1, signatures.count("(varchar,timestamp) -> timestamp"));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, parseDatetimeSignatures) {
  auto signatures = getSignatureStrings("parse_datetime");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(
      1, signatures.count("(varchar,varchar) -> timestamp with time zone"));
}
TEST_F(PrestoSqlDateTimeFunctionsTest, FromUnixtimeYYYYThrowError) {
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

TEST_F(PrestoSqlDateTimeFunctionsTest, civilDateTimeMillisecondRange) {
  const auto civil = util::toCivilDateTime(
      Timestamp::maxMillis(), /*allowOverflow*/ false, /*isPrecision*/ true);
  EXPECT_EQ(292278994, civil.date.year);
  EXPECT_EQ(8, civil.date.month);
  EXPECT_EQ(17, civil.date.day);

  BOLT_ASSERT_THROW(
      util::toCivilDateTime(
          Timestamp::max(), /*allowOverflow*/ false, /*isPrecision*/ true),
      "Could not convert Timestamp(9223372036854775, 999999999) to milliseconds");
}

TEST_F(PrestoSqlDateTimeFunctionsTest, FromUnixtimeLargeValuesPrestoParity) {
#ifdef SPARK_COMPATIBLE
  const std::string prefix = "+";
#else
  const std::string prefix;
#endif
  auto fromUnixTime = [&](int64_t unixTime) {
    return evaluateOnce<std::string>(
               "format_datetime("
               "from_unixtime(CAST(c0 AS double)), 'yyyy-MM-dd HH:mm:ss')",
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

TEST_F(PrestoSqlDateTimeFunctionsTest, DateFormatYYYYThrowError) {
  using util::fromTimestampString;

  const auto dateFormat = [&](std::optional<Timestamp> timestamp,
                              const std::string& format) {
    auto resultVector = evaluate(
        "date_format(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  };
  EXPECT_THROW(
      dateFormat(fromTimestampString("1970-01-01", nullptr), "YYYY-MM-dd"),
      BoltUserError);
}
TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfXxxSignatures) {
  for (const auto& name :
       {"day_of_year", "doy", "day_of_month", "day_of_week", "dow"}) {
    SCOPED_TRACE(name);
    auto signatures = getSignatureStrings(name);
    ASSERT_EQ(3, signatures.size());

    ASSERT_EQ(1, signatures.count("(timestamp with time zone) -> bigint"));
    ASSERT_EQ(1, signatures.count("(date) -> bigint"));
    ASSERT_EQ(1, signatures.count("(timestamp) -> bigint"));
  }
}

// Test cases from PrestoDB [1] are covered here as well:
// Timestamp(998474645, 321000000) from "TIMESTAMP '2001-08-22 03:04:05.321'"
// Timestamp(998423705, 321000000) from "TIMESTAMP '2001-08-22 03:04:05.321
// +07:09'"
// [1]https://github.com/prestodb/presto/blob/master/presto-main/src/test/java/com/facebook/presto/operator/scalar/TestDateTimeFunctionsBase.java
TEST_F(PrestoSqlDateTimeFunctionsTest, toUnixtime) {
  const auto toUnixtime = [&](std::optional<Timestamp> t) {
    return evaluateOnce<double>("to_unixtime(c0)", t);
  };

  EXPECT_EQ(0, toUnixtime(Timestamp(0, 0)));
  EXPECT_EQ(-0.999991, toUnixtime(Timestamp(-1, 9000)));
  EXPECT_EQ(4000000000, toUnixtime(Timestamp(4000000000, 0)));
  EXPECT_EQ(4000000000.123, toUnixtime(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(-9999999998.9, toUnixtime(Timestamp(-9999999999, 100000000)));
  EXPECT_EQ(998474645.321, toUnixtime(Timestamp(998474645, 321000000)));
  EXPECT_EQ(998423705.321, toUnixtime(Timestamp(998423705, 321000000)));

  const auto toUnixtimeWTZ = [&](int64_t timestamp, const char* tz) {
    auto input = makeTimestampWithTimeZoneVector(timestamp, tz);

    return evaluateOnce<double>("to_unixtime(c0)", makeRowVector({input}));
  };

  // 1639426440000 is milliseconds (from PrestoDb '2021-12-13+20:14+00:00').
  EXPECT_EQ(0, toUnixtimeWTZ(0, "+00:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+00:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+03:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+04:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-07:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-00:01"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+00:01"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-14:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+14:00"));

  // test floating point and negative time
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "+00:00"));
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "+03:00"));
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "-07:00"));
  EXPECT_EQ(-16394.26, toUnixtimeWTZ(-16394260, "+12:00"));
  EXPECT_EQ(-16394.26, toUnixtimeWTZ(-16394260, "-06:00"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, fromUnixtimeRountTrip) {
  const auto testRoundTrip = [&](std::optional<Timestamp> t) {
    auto r = evaluateOnce<Timestamp>("from_unixtime(to_unixtime(c0))", t);
    EXPECT_EQ(r->getSeconds(), t->getSeconds()) << "at " << t->toString();
    EXPECT_NEAR(r->getNanos(), t->getNanos(), 1'000) << "at " << t->toString();
    return r;
  };

  testRoundTrip(Timestamp(0, 0));
  testRoundTrip(Timestamp(-1, 9000000));
  testRoundTrip(Timestamp(4000000000, 0));
  testRoundTrip(Timestamp(4000000000, 123000000));
  testRoundTrip(Timestamp(-9999999999, 100000000));
  testRoundTrip(Timestamp(998474645, 321000000));
  testRoundTrip(Timestamp(998423705, 321000000));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, fromUnixtimeWithTimeZone) {
  static const double kNan = std::numeric_limits<double>::quiet_NaN();

  vector_size_t size = 37;

  auto unixtimeAt = [](vector_size_t row) -> double {
    return 1631800000.12345 + row * 11;
  };

  auto unixtimes = makeFlatVector<double>(size, unixtimeAt);

  // Constant timezone parameter.
  {
    auto result =
        evaluate("from_unixtime(c0, '+01:00')", makeRowVector({unixtimes}));

    auto expected = makeTimestampWithTimeZoneVector(
        makeFlatVector<int64_t>(
            size, [&](auto row) { return unixtimeAt(row) * 1'000; }),
        makeConstant((int16_t)900, size));
    assertEqualVectors(expected, result);

    // NaN timestamp.
    result = evaluate(
        "from_unixtime(c0, '+01:00')",
        makeRowVector({makeFlatVector<double>({kNan, kNan})}));
    expected = makeTimestampWithTimeZoneVector(
        makeFlatVector<int64_t>({0, 0}), makeFlatVector<int16_t>({900, 900}));
    assertEqualVectors(expected, result);
  }

  // Variable timezone parameter.
  {
    std::vector<int16_t> timezoneIds = {900, 960, 1020, 1080, 1140};
    std::vector<std::string> timezoneNames = {
        "+01:00", "+02:00", "+03:00", "+04:00", "+05:00"};

    auto timezones = makeFlatVector<StringView>(
        size, [&](auto row) { return StringView(timezoneNames[row % 5]); });

    auto result = evaluate(
        "from_unixtime(c0, c1)", makeRowVector({unixtimes, timezones}));
    auto expected = makeTimestampWithTimeZoneVector(
        makeFlatVector<int64_t>(
            size, [&](auto row) { return unixtimeAt(row) * 1'000; }),
        makeFlatVector<int16_t>(
            size, [&](auto row) { return timezoneIds[row % 5]; }));
    assertEqualVectors(expected, result);

    // NaN timestamp.
    result = evaluate(
        "from_unixtime(c0, c1)",
        makeRowVector({
            makeFlatVector<double>({kNan, kNan}),
            makeNullableFlatVector<StringView>({"+01:00", "+02:00"}),
        }));
    expected = makeTimestampWithTimeZoneVector(
        makeFlatVector<int64_t>({0, 0}), makeFlatVector<int16_t>({900, 960}));
    assertEqualVectors(expected, result);
  }
}

TEST_F(PrestoSqlDateTimeFunctionsTest, fromUnixtime) {
  const auto fromUnixtime = [&](std::optional<double> t) {
    return evaluateOnce<Timestamp>("from_unixtime(c0)", t);
  };

  static const double kInf = std::numeric_limits<double>::infinity();
  static const double kNan = std::numeric_limits<double>::quiet_NaN();

  EXPECT_EQ(Timestamp(0, 0), fromUnixtime(0));
  EXPECT_EQ(Timestamp(-1, 9000000), fromUnixtime(-0.991));
  EXPECT_EQ(Timestamp(1, 0), fromUnixtime(1 - 1e-10));
  EXPECT_EQ(Timestamp(4000000000, 0), fromUnixtime(4000000000));
  EXPECT_EQ(
      Timestamp(9'223'372'036'854'775, 807'000'000), fromUnixtime(3.87111e+37));
  EXPECT_EQ(Timestamp(4000000000, 123000000), fromUnixtime(4000000000.123));
  EXPECT_EQ(Timestamp(9'223'372'036'854'775, 807'000'000), fromUnixtime(kInf));
  EXPECT_EQ(
      Timestamp(-9'223'372'036'854'776, 192'000'000), fromUnixtime(-kInf));
  EXPECT_EQ(Timestamp(0, 0), fromUnixtime(kNan));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("year(c0)", date);
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

TEST_F(PrestoSqlDateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(1969, year(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(2020, year(DATE()->toDays("2020-01-01")));
  EXPECT_EQ(1920, year(DATE()->toDays("1920-12-31")));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, yearTimestampWithTimezone) {
  EXPECT_EQ(
      1969,
      evaluateWithTimestampWithTimezone<int64_t>("year(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>("year(c0)", 0, "+00:00"));
  EXPECT_EQ(
      1973,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1966,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2001,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      1938,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", std::nullopt, std::nullopt));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, weekDate) {
  const auto weekDate = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    auto week =
        evaluateOnce<int64_t, int32_t>("week(c0)", {date}, {DATE()}).value();
    auto weekOfYear =
        evaluateOnce<int64_t, int32_t>("week_of_year(c0)", {date}, {DATE()})
            .value();
    BOLT_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekDate("1919-12-31"));
  EXPECT_EQ(1, weekDate("1920-01-01"));
  EXPECT_EQ(1, weekDate("1920-01-04"));
  EXPECT_EQ(2, weekDate("1920-01-05"));
  EXPECT_EQ(53, weekDate("1960-01-01"));
  EXPECT_EQ(53, weekDate("1960-01-03"));
  EXPECT_EQ(1, weekDate("1960-01-04"));
  EXPECT_EQ(1, weekDate("1969-12-31"));
  EXPECT_EQ(1, weekDate("1970-01-01"));
  EXPECT_EQ(1, weekDate("0001-01-01"));
  EXPECT_EQ(52, weekDate("9999-12-31"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, week) {
  const auto weekTimestamp = [&](const char* time) {
    auto timestampInSeconds = util::fromTimeString(time, nullptr) / 1'000'000;
    auto timestamp =
        std::make_optional(Timestamp(timestampInSeconds * 100'000'000, 0));
    auto week = evaluateOnce<int64_t>("week(c0)", timestamp).value();
    auto weekOfYear =
        evaluateOnce<int64_t>("week_of_year(c0)", timestamp).value();
    BOLT_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekTimestamp("00:00:00"));
  EXPECT_EQ(10, weekTimestamp("11:59:59"));
  EXPECT_EQ(51, weekTimestamp("06:01:01"));
  EXPECT_EQ(24, weekTimestamp("06:59:59"));
  EXPECT_EQ(27, weekTimestamp("12:00:01"));
  EXPECT_EQ(7, weekTimestamp("12:59:59"));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, weekTimestampWithTimezone) {
  const auto weekTimestampTimezone = [&](const char* time,
                                         const char* timezone) {
    auto timestampInSeconds = util::fromTimeString(time, nullptr) / 1'000'000;
    auto timestamp = timestampInSeconds * 100'000'000;
    auto week = evaluateWithTimestampWithTimezone<int64_t>(
                    "week(c0)", timestamp, timezone)
                    .value();
    auto weekOfYear = evaluateWithTimestampWithTimezone<int64_t>(
                          "week_of_year(c0)", timestamp, timezone)
                          .value();
    BOLT_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekTimestampTimezone("00:00:00", "-12:00"));
  EXPECT_EQ(1, weekTimestampTimezone("00:00:00", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("11:59:59", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("11:59:59", "+12:00"));
  EXPECT_EQ(33, weekTimestampTimezone("06:01:01", "-12:00"));
  EXPECT_EQ(34, weekTimestampTimezone("06:01:01", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("12:00:01", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("12:00:01", "+12:00"));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, quarter) {
  const auto quarter = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("quarter(c0)", date);
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

TEST_F(PrestoSqlDateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("quarter(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(1, quarter(0));
  EXPECT_EQ(4, quarter(-1));
  EXPECT_EQ(4, quarter(-40));
  EXPECT_EQ(2, quarter(110));
  EXPECT_EQ(3, quarter(200));
  EXPECT_EQ(1, quarter(18262));
  EXPECT_EQ(1, quarter(-18262));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, quarterTimestampWithTimezone) {
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>("quarter(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>("quarter(c0)", 0, "+00:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", std::nullopt, std::nullopt));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, month) {
  const auto month = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("month(c0)", date);
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

TEST_F(PrestoSqlDateTimeFunctionsTest, monthDate) {
  const auto month = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("month(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(1, month(0));
  EXPECT_EQ(12, month(-1));
  EXPECT_EQ(11, month(-40));
  EXPECT_EQ(2, month(40));
  EXPECT_EQ(1, month(18262));
  EXPECT_EQ(1, month(-18262));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, monthTimestampWithTimezone) {
  EXPECT_EQ(
      12, evaluateWithTimestampWithTimezone<int64_t>("month(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1, evaluateWithTimestampWithTimezone<int64_t>("month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      11,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      9,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", std::nullopt, std::nullopt));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, hour) {
  const auto hour = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("hour(c0)", date);
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(Timestamp(0, 0)));
  EXPECT_EQ(23, hour(Timestamp(-1, 9000)));
  EXPECT_EQ(23, hour(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 0)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(10, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(19, hour(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(13, hour(Timestamp(0, 0)));
  EXPECT_EQ(12, hour(Timestamp(-1, Timestamp::kMaxNanos)));
  // Disabled for now because the TZ for Pacific/Apia in 2096 varies between
  // systems.
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 0)));
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(23, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, hour(Timestamp(998423705, 321000000)));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, hourTimestampWithTimezone) {
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 998423705000, "+01:00"));
  EXPECT_EQ(
      12,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+01:00"));
  EXPECT_EQ(
      13,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+02:00"));
  EXPECT_EQ(
      14,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+03:00"));
  EXPECT_EQ(
      8,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "-03:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+14:00"));
  EXPECT_EQ(
      9,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", -100000, "-14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", -41028000, "+14:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", std::nullopt, std::nullopt));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, hourDate) {
  const auto hour = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("hour(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(0));
  EXPECT_EQ(0, hour(-1));
  EXPECT_EQ(0, hour(-40));
  EXPECT_EQ(0, hour(40));
  EXPECT_EQ(0, hour(18262));
  EXPECT_EQ(0, hour(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Timestamp(0, 0)));
  EXPECT_EQ(31, day(Timestamp(-1, 9000)));
  EXPECT_EQ(30, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(1, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(6, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(31, day(Timestamp(1635668100, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(31, day(Timestamp(0, 0)));
  EXPECT_EQ(31, day(Timestamp(-1, 9000)));
  EXPECT_EQ(30, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(1, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(6, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(31, day(Timestamp(1635668100, 0)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfMonthDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_month(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(0));
  EXPECT_EQ(31, day(-1));
  EXPECT_EQ(22, day(-40));
  EXPECT_EQ(10, day(40));
  EXPECT_EQ(1, day(18262));
  EXPECT_EQ(2, day(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, plusMinusDateIntervalDayTime) {
  const auto plus = [&](std::optional<int32_t> date,
                        std::optional<int64_t> interval) {
    return evaluateOnce<int32_t>(
        "c0 + c1",
        makeRowVector({
            makeNullableFlatVector<int32_t>({date}, DATE()),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };
  const auto minus = [&](std::optional<int32_t> date,
                         std::optional<int64_t> interval) {
    return evaluateOnce<int32_t>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<int32_t>({date}, DATE()),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };

  const int64_t oneDay(kMillisInDay * 1);
  const int64_t tenDays(kMillisInDay * 10);
  const int64_t partDay(kMillisInHour * 25);
  const int32_t baseDate(20000);
  const int32_t baseDatePlus1(20000 + 1);
  const int32_t baseDatePlus10(20000 + 10);
  const int32_t baseDateMinus1(20000 - 1);
  const int32_t baseDateMinus10(20000 - 10);

  EXPECT_EQ(std::nullopt, plus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, plus(10000, std::nullopt));
  EXPECT_EQ(baseDatePlus1, plus(baseDate, oneDay));
  EXPECT_EQ(baseDatePlus10, plus(baseDate, tenDays));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, minus(10000, std::nullopt));
  EXPECT_EQ(baseDateMinus1, minus(baseDate, oneDay));
  EXPECT_EQ(baseDateMinus10, minus(baseDate, tenDays));

  EXPECT_THROW(plus(baseDate, partDay), BoltUserError);
  EXPECT_THROW(minus(baseDate, partDay), BoltUserError);
}

TEST_F(PrestoSqlDateTimeFunctionsTest, plusMinusTimestampIntervalDayTime) {
  constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();

  const auto minus = [&](std::optional<Timestamp> timestamp,
                         std::optional<int64_t> interval) {
    return evaluateOnce<Timestamp>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<Timestamp>({timestamp}),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };

  EXPECT_EQ(std::nullopt, minus(std::nullopt, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, 1));
  EXPECT_EQ(std::nullopt, minus(Timestamp(0, 0), std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), minus(Timestamp(0, 0), 0));
  EXPECT_EQ(Timestamp(0, 0), minus(Timestamp(10, 0), 10'000));
  EXPECT_EQ(Timestamp(-10, 0), minus(Timestamp(10, 0), 20'000));
  EXPECT_EQ(
      Timestamp(-2, 50 * Timestamp::kNanosecondsInMillisecond),
      minus(Timestamp(0, 50 * Timestamp::kNanosecondsInMillisecond), 2'000));
  EXPECT_EQ(
      Timestamp(-3, 995 * Timestamp::kNanosecondsInMillisecond),
      minus(Timestamp(0, 0), 2'005));
  EXPECT_EQ(
      Timestamp(9223372036854774, 809000000),
      minus(Timestamp(-1, 0), kLongMax));
  EXPECT_EQ(
      Timestamp(-9223372036854775, 192000000),
      minus(Timestamp(1, 0), kLongMin));

  const auto plusAndVerify = [&](std::optional<Timestamp> timestamp,
                                 std::optional<int64_t> interval,
                                 std::optional<Timestamp> expected) {
    EXPECT_EQ(
        expected,
        evaluateOnce<Timestamp>(
            "c0 + c1",
            makeRowVector({
                makeNullableFlatVector<Timestamp>({timestamp}),
                makeNullableFlatVector<int64_t>(
                    {interval}, INTERVAL_DAY_TIME()),
            })));
    EXPECT_EQ(
        expected,
        evaluateOnce<Timestamp>(
            "c1 + c0",
            makeRowVector({
                makeNullableFlatVector<Timestamp>({timestamp}),
                makeNullableFlatVector<int64_t>(
                    {interval}, INTERVAL_DAY_TIME()),
            })));
  };

  plusAndVerify(std::nullopt, std::nullopt, std::nullopt);
  plusAndVerify(std::nullopt, 1, std::nullopt);
  plusAndVerify(Timestamp(0, 0), std::nullopt, std::nullopt);
  plusAndVerify(Timestamp(0, 0), 0, Timestamp(0, 0));
  plusAndVerify(Timestamp(0, 0), 10'000, Timestamp(10, 0));
  plusAndVerify(
      Timestamp(0, 0),
      20'005,
      Timestamp(20, 5 * Timestamp::kNanosecondsInMillisecond));
  plusAndVerify(
      Timestamp(0, 0),
      -30'005,
      Timestamp(-31, 995 * Timestamp::kNanosecondsInMillisecond));
  plusAndVerify(
      Timestamp(1, 0), kLongMax, Timestamp(-9223372036854775, 191000000));
  plusAndVerify(
      Timestamp(0, 0), kLongMin, Timestamp(-9223372036854776, 192000000));
  plusAndVerify(
      Timestamp(-1, 0), kLongMin, Timestamp(9223372036854774, 808000000));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, minusTimestamp) {
  const auto minus = [&](std::optional<int64_t> t1, std::optional<int64_t> t2) {
    const auto timestamp1 = (t1.has_value()) ? Timestamp(t1.value(), 0)
                                             : std::optional<Timestamp>();
    const auto timestamp2 = (t2.has_value()) ? Timestamp(t2.value(), 0)
                                             : std::optional<Timestamp>();
    return evaluateOnce<int64_t>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<Timestamp>({timestamp1}),
            makeNullableFlatVector<Timestamp>({timestamp2}),
        }));
  };

  EXPECT_EQ(std::nullopt, minus(std::nullopt, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(1, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, 1));
  EXPECT_EQ(1000, minus(1, 0));
  EXPECT_EQ(-1000, minus(1, 2));
  BOLT_ASSERT_THROW(
      minus(Timestamp::kMinSeconds, Timestamp::kMaxSeconds),
      "Could not convert Timestamp(-9223372036854776, 0) to milliseconds");
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfMonthTimestampWithTimezone) {
  EXPECT_EQ(
      31,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      30,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      18,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      14,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", std::nullopt, std::nullopt));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfWeek) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(4, day(Timestamp(0, 0)));
  EXPECT_EQ(3, day(Timestamp(-1, 9000)));
  EXPECT_EQ(1, day(Timestamp(1633940100, 0)));
  EXPECT_EQ(2, day(Timestamp(1634026500, 0)));
  EXPECT_EQ(3, day(Timestamp(1634112900, 0)));
  EXPECT_EQ(4, day(Timestamp(1634199300, 0)));
  EXPECT_EQ(5, day(Timestamp(1634285700, 0)));
  EXPECT_EQ(6, day(Timestamp(1634372100, 0)));
  EXPECT_EQ(7, day(Timestamp(1633853700, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(3, day(Timestamp(0, 0)));
  EXPECT_EQ(3, day(Timestamp(-1, 9000)));
  EXPECT_EQ(1, day(Timestamp(1633940100, 0)));
  EXPECT_EQ(2, day(Timestamp(1634026500, 0)));
  EXPECT_EQ(3, day(Timestamp(1634112900, 0)));
  EXPECT_EQ(4, day(Timestamp(1634199300, 0)));
  EXPECT_EQ(5, day(Timestamp(1634285700, 0)));
  EXPECT_EQ(6, day(Timestamp(1634372100, 0)));
  EXPECT_EQ(7, day(Timestamp(1633853700, 0)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfWeekDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_week(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(4, day(0));
  EXPECT_EQ(3, day(-1));
  EXPECT_EQ(6, day(-40));
  EXPECT_EQ(2, day(40));
  EXPECT_EQ(3, day(18262));
  EXPECT_EQ(5, day(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfWeekTimestampWithTimezone) {
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 0, "-01:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 0, "+00:00"));
  EXPECT_EQ(
      5,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", std::nullopt, std::nullopt));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfYear) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Timestamp(0, 0)));
  EXPECT_EQ(365, day(Timestamp(-1, 9000)));
  EXPECT_EQ(273, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(274, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(279, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(304, day(Timestamp(1635668100, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(365, day(Timestamp(0, 0)));
  EXPECT_EQ(365, day(Timestamp(-1, 9000)));
  EXPECT_EQ(273, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(274, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(279, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(304, day(Timestamp(1635668100, 0)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfYearDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(0));
  EXPECT_EQ(365, day(-1));
  EXPECT_EQ(326, day(-40));
  EXPECT_EQ(41, day(40));
  EXPECT_EQ(1, day(18262));
  EXPECT_EQ(2, day(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dayOfYearTimestampWithTimezone) {
  EXPECT_EQ(
      365,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 0, "+00:00"));
  EXPECT_EQ(
      334,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      33,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      108,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      257,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", std::nullopt, std::nullopt));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, yearOfWeek) {
  const auto yow = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("year_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(Timestamp(0, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-1, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-345600, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-259200, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31536000, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31708800, 0)));
  EXPECT_EQ(1971, yow(Timestamp(31795200, 0)));
  EXPECT_EQ(2021, yow(Timestamp(1632989700, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(Timestamp(0, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-1, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-345600, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-259200, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31536000, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31708800, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31795200, 0)));
  EXPECT_EQ(2021, yow(Timestamp(1632989700, 0)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, yearOfWeekDate) {
  const auto yow = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("year_of_week(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(0));
  EXPECT_EQ(1970, yow(-1));
  EXPECT_EQ(1969, yow(-4));
  EXPECT_EQ(1970, yow(-3));
  EXPECT_EQ(1970, yow(365));
  EXPECT_EQ(1970, yow(367));
  EXPECT_EQ(1971, yow(368));
  EXPECT_EQ(2021, yow(18900));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, yearOfWeekTimestampWithTimezone) {
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 0, "+00:00"));
  EXPECT_EQ(
      1973,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1966,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2001,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      1938,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", std::nullopt, std::nullopt));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, minute) {
  const auto minute = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("minute(c0)", date);
  };
  std::cout << "minute int64:"
            << minute(Timestamp(998423705, 321000000)).value() << std::endl;
  std::cout << "type: "
            << typeid(minute(Timestamp(998423705, 321000000)).value()).name()
            << std::endl;

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
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, minuteDate) {
  const auto minute = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("minute(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(0, minute(0));
  EXPECT_EQ(0, minute(-1));
  EXPECT_EQ(0, minute(40));
  EXPECT_EQ(0, minute(40));
  EXPECT_EQ(0, minute(18262));
  EXPECT_EQ(0, minute(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, minuteTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", std::nullopt, "Asia/Kolkata"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("minute(c0)", 0, "+00:00"));
  EXPECT_EQ(
      30,
      evaluateWithTimestampWithTimezone<int64_t>("minute(c0)", 0, "+05:30"));
  EXPECT_EQ(
      6,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 4000000000000, "+00:00"));
  EXPECT_EQ(
      36,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 4000000000000, "+05:30"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 998474645000, "+00:00"));
  EXPECT_EQ(
      34,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 998474645000, "+05:30"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", -1000, "+00:00"));
  EXPECT_EQ(
      29,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", -1000, "+05:30"));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, second) {
  const auto second = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("second(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(Timestamp(0, 0)));
  EXPECT_EQ(40, second(Timestamp(4000000000, 0)));
  EXPECT_EQ(59, second(Timestamp(-1, 123000000)));
  EXPECT_EQ(59, second(Timestamp(-1, Timestamp::kMaxNanos)));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, secondDate) {
  const auto second = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("second(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(0));
  EXPECT_EQ(0, second(-1));
  EXPECT_EQ(0, second(-40));
  EXPECT_EQ(0, second(40));
  EXPECT_EQ(0, second(18262));
  EXPECT_EQ(0, second(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, secondTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", std::nullopt, "+05:30"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("second(c0)", 0, "+00:00"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("second(c0)", 0, "+05:30"));
  EXPECT_EQ(
      40,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", 4000000000000, "+00:00"));
  EXPECT_EQ(
      40,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", 4000000000000, "+05:30"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", -1000, "+00:00"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", -1000, "+05:30"));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, millisecond) {
  const auto millisecond = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("millisecond(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(Timestamp(0, 0)));
  EXPECT_EQ(0, millisecond(Timestamp(4000000000, 0)));
  EXPECT_EQ(123, millisecond(Timestamp(-1, 123000000)));
  EXPECT_EQ(999, millisecond(Timestamp(-1, Timestamp::kMaxNanos)));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, millisecondDate) {
  const auto millisecond = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("millisecond(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(0));
  EXPECT_EQ(0, millisecond(-1));
  EXPECT_EQ(0, millisecond(-40));
  EXPECT_EQ(0, millisecond(40));
  EXPECT_EQ(0, millisecond(18262));
  EXPECT_EQ(0, millisecond(-18262));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, millisecondTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", std::nullopt, "+05:30"));
  EXPECT_EQ(
      0,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 0, "+00:00"));
  EXPECT_EQ(
      0,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 0, "+05:30"));
  EXPECT_EQ(
      123,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 4000000000123, "+00:00"));
  EXPECT_EQ(
      123,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 4000000000123, "+05:30"));
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", -980, "+00:00"));
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", -980, "+05:30"));
}

// date_trunc cannot be access in presto ut
#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, dateTrunc) {
  std::string optimizationFlags[] = {"true", "false"};
  for (const std::string& flag : optimizationFlags) {
    setQueryDateTruncOptimization(flag);

    const auto dateTrunc = [&](const std::string& unit,
                               std::optional<Timestamp> timestamp) {
      return evaluateOnce<Timestamp>(
          fmt::format("date_trunc('{}', c0)", unit), timestamp);
    };
    disableAdjustTimestampToTimezone();

    EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("day", Timestamp(0, 123)));
    EXPECT_EQ(Timestamp(-1, 0), dateTrunc("second", Timestamp(-1, 0)));

    EXPECT_EQ(Timestamp(-86400, 0), dateTrunc("day", Timestamp(-1, 0)));
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
        Timestamp(998265600, 0),
        dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(996624000, 0),
        dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(993945600, 0),
        dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(978307200, 0),
        dateTrunc("year", Timestamp(998'474'645, 321'001'234)));

    setQueryTimeZone("America/Los_Angeles");

    EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
    EXPECT_EQ(Timestamp(-57600, 0), dateTrunc("day", Timestamp(0, 0)));
    EXPECT_EQ(Timestamp(-57600, 0), dateTrunc("day", Timestamp(-1, 0)));
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

    setQueryTimeZone("Asia/Kolkata");

    EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
    EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
    EXPECT_EQ(Timestamp(-19800, 0), dateTrunc("day", Timestamp(0, 0)));
    EXPECT_EQ(Timestamp(-19800, 0), dateTrunc("day", Timestamp(-1, 0)));
    EXPECT_EQ(
        Timestamp(998474645, 0),
        dateTrunc("second", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(998474640, 0),
        dateTrunc("minute", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(998472600, 0),
        dateTrunc("hour", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(998418600, 0),
        dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(998245800, 0),
        dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(996604200, 0),
        dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(993925800, 0),
        dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
    EXPECT_EQ(
        Timestamp(978287400, 0),
        dateTrunc("year", Timestamp(998'474'645, 321'001'234)));
  }
}

TEST_F(PrestoSqlDateTimeFunctionsTest, trunc) {
  const static auto trunc = [&](std::optional<int32_t> date,
                                const std::string& unit) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("trunc(c0, '{}')", unit), {date}, {DATE()});
  };

  const static auto days = [](folly::StringPiece str) {
    return DATE()->toDays(str);
  };

  EXPECT_EQ(days("2023-11-16"), trunc(days("2023-11-16"), "day"));
  EXPECT_EQ(days("2023-11-01"), trunc(days("2023-11-16"), "month"));
  EXPECT_EQ(days("2023-11-13"), trunc(days("2023-11-16"), "week"));
  EXPECT_EQ(days("2023-10-01"), trunc(days("2023-11-16"), "quarter"));
  EXPECT_EQ(days("2023-01-01"), trunc(days("2023-11-16"), "year"));

  EXPECT_EQ(
      days("1970-01-01"),
      evaluateOnce<int32_t>(
          "trunc(c0, 'year')", std::make_optional(Timestamp(0, 0))));
  EXPECT_EQ(
      days("2023-11-16"),
      evaluateOnce<int32_t>(
          "trunc(c0, 'day')", std::make_optional(std::string("2023-11-16"))));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dateTruncDate) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("date_trunc('{}', c0)", unit), {date}, {DATE()});
  };

  EXPECT_EQ(std::nullopt, dateTrunc("year", std::nullopt));

  // Date(0) is 1970-01-01.
  EXPECT_EQ(0, dateTrunc("day", 0));
  EXPECT_EQ(0, dateTrunc("year", 0));
  EXPECT_EQ(0, dateTrunc("quarter", 0));
  EXPECT_EQ(0, dateTrunc("month", 0));
  EXPECT_THROW(dateTrunc("second", 0), BoltUserError);
  EXPECT_THROW(dateTrunc("minute", 0), BoltUserError);
  EXPECT_THROW(dateTrunc("hour", 0), BoltUserError);

  // Date(18297) is 2020-02-05.
  EXPECT_EQ(18297, dateTrunc("day", 18297));
  EXPECT_EQ(18293, dateTrunc("month", 18297));
  EXPECT_EQ(18262, dateTrunc("quarter", 18297));
  EXPECT_EQ(18262, dateTrunc("year", 18297));
  EXPECT_THROW(dateTrunc("second", 18297), BoltUserError);
  EXPECT_THROW(dateTrunc("minute", 18297), BoltUserError);
  EXPECT_THROW(dateTrunc("hour", 18297), BoltUserError);

  // Date(-18297) is 1919-11-28.
  EXPECT_EQ(-18297, dateTrunc("day", -18297));
  EXPECT_EQ(-18324, dateTrunc("month", -18297));
  EXPECT_EQ(-18355, dateTrunc("quarter", -18297));
  EXPECT_EQ(-18628, dateTrunc("year", -18297));
  EXPECT_THROW(dateTrunc("second", -18297), BoltUserError);
  EXPECT_THROW(dateTrunc("minute", -18297), BoltUserError);
  EXPECT_THROW(dateTrunc("hour", -18297), BoltUserError);
}

// TEST_F(DateTimeFunctionsTest, dateTruncDateForWeek) {
//   const auto dateTrunc = [&](const std::string& unit,
//                              std::optional<Date> date) {
//     return evaluateOnce<Date>(fmt::format("date_trunc('{}', c0)", unit),
//     date);
//   };

//   // Date(19576) is 2023-08-07, which is Monday, should return Monday
//   EXPECT_EQ(Date(19576), dateTrunc("week", Date(19576)));

//   // Date(19579) is 2023-08-10, Thur, should return Monday
//   EXPECT_EQ(Date(19576), dateTrunc("week", Date(19579)));

//   // Date(19570) is 2023-08-01, A non-Monday(Tue) date at the beginning of a
//   // month when the preceding Monday falls in the previous month. should
//   return
//   // 2023-07-31(19569), which is previous Monday
//   EXPECT_EQ(Date(19569), dateTrunc("week", Date(19570)));

//   // Date(19358) is 2023-01-01, A non-Monday(Sunday) date at the beginning of
//   // January where the preceding Monday falls in the previous year. should
//   // return 2022-12-26(19352), which is previous Monday
//   EXPECT_EQ(Date(19352), dateTrunc("week", Date(19358)));
//   EXPECT_EQ(Date(19779), dateTrunc("week", Date(19783)));
// }

// Reference dateTruncDateForWeek for test cases explanaitons
TEST_F(PrestoSqlDateTimeFunctionsTest, dateTruncTimeStampForWeek) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_trunc('{}', c0)", unit), timestamp);
  };

  EXPECT_EQ(
      Timestamp(19576 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19576 * 24 * 60 * 60, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19576 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19579 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19569 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19570 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19352 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19358 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19779 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19783 * 24 * 60 * 60 + 500, 321'001'234)));
}

// Logical Steps
// 1. Convert Original Millisecond Input to UTC
// 2. Apply Time Zone Offset
// 3. Truncate to the Nearest "Unit"
// 4. Convert Back to UTC (remove Time Zone offset)
// 5. Convert Back to Milliseconds Since the Unix Epoch
TEST_F(PrestoSqlDateTimeFunctionsTest, dateTruncTimeStampWithTimezoneForWeek) {
  const auto evaluateDateTrunc = [&](const std::string& truncUnit,
                                     int64_t inputTimestamp,
                                     const std::string& timeZone,
                                     int64_t expectedTimestamp) {
    assertEqualVectors(
        makeTimestampWithTimeZoneVector(expectedTimestamp, timeZone.c_str()),
        evaluateWithTimestampWithTimezone(
            fmt::format("date_trunc('{}', c0)", truncUnit),
            inputTimestamp,
            timeZone));
  };
  // input 2023-08-07 00:00:00 (19576 days) with timeZone +01:00
  // output 2023-08-06 23:00:00" in UTC.(1691362800000)
  auto inputMilli = int64_t(19576) * 24 * 60 * 60 * 1000;
  auto outputMilli = inputMilli - int64_t(1) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+01:00", outputMilli);

  // Date(19579) is 2023-08-10, Thur, should return Monday UTC (previous Sunday
  // in +03:00 timezone)
  inputMilli = int64_t(19579) * 24 * 60 * 60 * 1000;
  outputMilli = inputMilli - int64_t(3) * 24 * 60 * 60 * 1000 -
      int64_t(3) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+03:00", outputMilli);

  // Date(19570) is 2023-08-01, A non-Monday(Tue) date at the beginning of a
  // month when the preceding Monday falls in the previous month. should return
  // 2023-07-31(19569), which is previous Monday EXPECT_EQ(19569,
  // dateTrunc("week", 19570));
  inputMilli = int64_t(19570) * 24 * 60 * 60 * 1000;
  outputMilli = inputMilli - int64_t(1) * 24 * 60 * 60 * 1000 -
      int64_t(3) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+03:00", outputMilli);

  // Date(19570) is 2023-08-01, which is Tuesday; TimeZone is -05:00, so input
  // will become Monday. 2023-07-31 19:00:00, which will truncate to 2023-07-31
  // 00:00:00
  // TODO : Need to double-check with presto logic
  inputMilli = int64_t(19570) * 24 * 60 * 60 * 1000;
  outputMilli =
      int64_t(19569) * 24 * 60 * 60 * 1000 + int64_t(5) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "-05:00", outputMilli);
}

TEST_F(
    PrestoSqlDateTimeFunctionsTest,
    dateTruncTimeStampWithTimezoneStringForWeek) {
  const auto evaluateDateTruncFromStrings = [&](const std::string& truncUnit,
                                                const std::string&
                                                    inputTimestamp,
                                                const std::string&
                                                    expectedTimestamp) {
    assertEqualVectors(
        evaluate<RowVector>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<RowVector>(
            fmt::format(
                "date_trunc('{}', parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ'))",
                truncUnit),
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{inputTimestamp}})})));
  };
  // Monday
  evaluateDateTruncFromStrings(
      "week", "2023-08-07+23:01:02+14:00", "2023-08-07+00:00:00+14:00");

  // Thur
  evaluateDateTruncFromStrings(
      "week", "2023-08-10+23:01:02+14:00", "2023-08-07+00:00:00+14:00");

  // 2023-08-01, A non-Monday(Tue) date at the beginning of a
  // month when the preceding Monday falls in the previous month. should return
  // 2023-07-31, which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2023-08-01+23:01:02+14:00", "2023-07-31+00:00:00+14:00");

  // 2023-01-01, A non-Monday(Sunday) date at the beginning of
  // January where the preceding Monday falls in the previous year. should
  // return 2022-12-26, which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2023-01-01+23:01:02+14:00", "2022-12-26+00:00:00+14:00");

  // 2024-03-01, A non-Monday(Friday) date which will go over to
  // a leap day (February 29th) in a leap year. should return 2024-02-26,
  // which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2024-03-01+23:01:02+14:00", "2024-02-26+00:00:00+14:00");
}
TEST_F(PrestoSqlDateTimeFunctionsTest, dateTruncTimestampWithTimezone) {
  const auto evaluateDateTrunc = [&](const std::string& truncUnit,
                                     int64_t inputTimestamp,
                                     const std::string& timeZone,
                                     int64_t expectedTimestamp) {
    assertEqualVectors(
        makeTimestampWithTimeZoneVector(expectedTimestamp, timeZone.c_str()),
        evaluateWithTimestampWithTimezone(
            fmt::format("date_trunc('{}', c0)", truncUnit),
            inputTimestamp,
            timeZone));
  };

  evaluateDateTrunc("second", 123, "+01:00", 0);
  evaluateDateTrunc("second", 1123, "-03:00", 1000);
  evaluateDateTrunc("second", -1123, "+03:00", -2000);
  evaluateDateTrunc("second", 1234567000, "+14:00", 1234567000);
  evaluateDateTrunc("second", -1234567000, "-09:00", -1234567000);

  evaluateDateTrunc("minute", 123, "+01:00", 0);
  evaluateDateTrunc("minute", 1123, "-03:00", 0);
  evaluateDateTrunc("minute", -1123, "+03:00", -60000);
  evaluateDateTrunc("minute", 1234567000, "+14:00", 1234560000);
  evaluateDateTrunc("minute", -1234567000, "-09:00", -1234620000);

  evaluateDateTrunc("hour", 123, "+01:00", 0);
  evaluateDateTrunc("hour", 1123, "-03:00", 0);
  evaluateDateTrunc("hour", -1123, "+05:30", -1800000);
  evaluateDateTrunc("hour", 1234567000, "+14:00", 1231200000);
  evaluateDateTrunc("hour", -1234567000, "-09:30", -1236600000);

  evaluateDateTrunc("day", 123, "+01:00", -3600000);
  evaluateDateTrunc("day", 1123, "-03:00", -86400000 + 3600000 * 3);
  evaluateDateTrunc("day", -1123, "+05:30", 0 - 3600000 * 5 - 1800000);
  evaluateDateTrunc("day", 1234567000, "+14:00", 1159200000);
  evaluateDateTrunc("day", -1234567000, "-09:30", -1261800000);

  evaluateDateTrunc("month", 123, "-01:00", -2674800000);
  evaluateDateTrunc("month", 1234567000, "+14:00", -50400000);
  evaluateDateTrunc("month", -1234567000, "-09:30", -2644200000);

  evaluateDateTrunc("quarter", 123, "-01:00", -7945200000);
  evaluateDateTrunc("quarter", 123456789000, "+14:00", 118231200000);
  evaluateDateTrunc("quarter", -123456789000, "-09:30", -126196200000);

  evaluateDateTrunc("year", 123, "-01:00", -31532400000);
  evaluateDateTrunc("year", 123456789000, "+14:00", 94644000000);
  evaluateDateTrunc("year", -123456789000, "-09:30", -126196200000);

  const auto evaluateDateTruncFromStrings = [&](const std::string& truncUnit,
                                                const std::string&
                                                    inputTimestamp,
                                                const std::string&
                                                    expectedTimestamp) {
    assertEqualVectors(
        evaluate<RowVector>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<RowVector>(
            fmt::format(
                "date_trunc('{}', parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ'))",
                truncUnit),
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{inputTimestamp}})})));
  };

  evaluateDateTruncFromStrings(
      "minute", "1972-05-20+23:01:02+14:00", "1972-05-20+23:01:00+14:00");
  evaluateDateTruncFromStrings(
      "minute", "1968-05-20+23:01:02+05:30", "1968-05-20+23:01:00+05:30");
  evaluateDateTruncFromStrings(
      "hour", "1972-05-20+23:01:02+03:00", "1972-05-20+23:00:00+03:00");
  evaluateDateTruncFromStrings(
      "hour", "1968-05-20+23:01:02-09:30", "1968-05-20+23:00:00-09:30");
  evaluateDateTruncFromStrings(
      "day", "1972-05-20+23:01:02-03:00", "1972-05-20+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "day", "1968-05-20+23:01:02+05:30", "1968-05-20+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "month", "1972-05-20+23:01:02-03:00", "1972-05-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "month", "1968-05-20+23:01:02+05:30", "1968-05-01+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "quarter", "1972-05-20+23:01:02-03:00", "1972-04-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "quarter", "1968-05-20+23:01:02+05:30", "1968-04-01+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "year", "1972-05-20+23:01:02-03:00", "1972-01-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "year", "1968-05-20+23:01:02+05:30", "1968-01-01+00:00:00+05:30");
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, timestampdiff) {
  const auto timestampdiff = [&](const std::string& unit,
                                 std::optional<Timestamp> start,
                                 std::optional<Timestamp> end) {
    return evaluateOnce<int64_t>(
        fmt::format("timestampdiff('{}', c0, c1)", unit), start, end);
  };

  const auto parseTimestamp =
      [&](const std::string& str) -> std::optional<Timestamp> {
    using util::fromTimestampString;
    bool isNull{false};
    auto result = fromTimestampString(str.data(), str.size(), &isNull);
    if (isNull) {
      return std::nullopt;
    }
    return result;
  };

  const auto parseDate = [&](const std::string& str) {
    return parseTimestamp(str + " 00:00:00");
  };

  const std::map<
      std::string,
      std::vector<std::tuple<std::string, std::string, int64_t>>>
      testCases = {
          {"day",
           {{"2018-07-03 11:11:11", "2018-07-05 11:11:11", 2},
            {"2016-06-15", "2016-06-16 11:11:11", 1},
            {"2016-06-15 11:00:00", "2016-06-19", 3},
            {"2016-03-15 11:00:00", "2015-06-19", -270},
            {"2021-02-28 12:00:00", "2021-03-01 12:00:00", 1},
            {"2016-06-15", "2016-06-18", 3},
            {"2020-02-29 00:00:00", "2024-02-29 00:00:00", 1461}}},
          {"hour",
           {{"2018-07-03 11:11:11", "2018-07-04 12:12:11", 25},
            {"2016-06-15", "2016-06-16 11:11:11", 35},
            {"2016-06-15 11:00:00", "2016-06-19", 85},
            {"2016-06-15", "2016-06-12", -72},
            {"2023-12-31 23:00:00", "2024-01-01 01:00:00", 2}}},
          {"minute",
           {{"2018-07-03 11:11:11", "2018-07-03 12:10:11", 59},
            {"2016-06-15", "2016-06-16 11:11:11", 2111},
            {"2016-06-15 11:00:00", "2016-06-19", 5100},
            {"2016-06-15", "2016-06-18", 4320},
            {"2023-12-31 23:59:00", "2024-01-01 00:01:00", 2}}},
          {"second",
           {{"2018-07-03 11:11:11", "2018-07-03 11:12:12", 61},
            {"2016-06-15", "2016-06-16 11:11:11", 126671},
            {"2016-06-15 11:00:00", "2016-06-19", 306000},
            {"2016-06-15", "2016-06-18", 259200}}},
          {"week",
           {{"2018-05-03 11:11:11", "2018-07-03 11:12:12", 8},
            {"2016-04-15", "2016-07-16 11:11:11", 13},
            {"2016-04-15 11:00:00", "2016-09-19", 22},
            {"2016-08-15", "2016-06-18", -8}}},
          {"month",
           {{"2018-07-03 11:11:11", "2018-09-05 11:11:11", 2},
            {"2016-06-15", "2018-06-16 11:11:11", 24},
            {"2016-06-15 11:00:00", "2018-05-19", 23},
            {"2021-02-28 12:00:00", "2021-03-28 12:00:00", 1},
            {"2016-06-15", "2018-03-18", 21}}},
          {"quarter",
           {{"2018-01-03 11:11:11", "2018-09-05 11:11:11", 2},
            {"2016-06-15", "2018-06-16 11:11:11", 8},
            {"2016-06-15 11:00:00", "2018-05-19", 7},
            {"2016-06-15", "2018-03-18", 7}}}};

  const auto parseInput =
      [&](const std::string& s) -> std::optional<Timestamp> {
    if (s.find(' ') != std::string::npos) {
      return parseTimestamp(s);
    } else {
      return parseDate(s);
    }
  };

  const auto testTimestampdiff = [&](const std::string& unit,
                                     const std::string& startStr,
                                     const std::string& endStr,
                                     int64_t expected) {
    const auto start = parseInput(startStr);
    const auto end = parseInput(endStr);
    auto actual = timestampdiff(unit, start, end);
    std::string diffInfo = fmt::format(
        "unit: {}\nstartStr: {}\nendStr: {}\nstartTimestamp: {}\nendTimestamp: {}",
        unit,
        startStr,
        endStr,
        (start.has_value() ? start.value().toString() : "nullptr"),
        (end.has_value() ? end.value().toString() : "nullptr"));
    EXPECT_TRUE(actual.has_value()) << diffInfo;
    EXPECT_EQ(expected, actual.value()) << diffInfo;
  };

  for (const auto& [unit, cases] : testCases) {
    for (const auto& [startStr, endStr, expected] : cases) {
      testTimestampdiff(unit, startStr, endStr, expected);
    }
  }

  EXPECT_EQ(
      std::nullopt,
      timestampdiff(
          "day", std::nullopt, parseTimestamp("2016-02-24 12:42:25")));
  EXPECT_EQ(
      std::nullopt,
      timestampdiff(
          "day", parseTimestamp("2016-02-24 12:42:25"), std::nullopt));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, parseDatetime) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, parseDatetime("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, std::nullopt));

  // Ensure it throws.
  BOLT_ASSERT_THROW(parseDatetime("", ""), "Invalid pattern specification");
  BOLT_ASSERT_THROW(parseDatetime("1234", "Y Y"), "parse literal error.");

  // Simple tests. More exhaustive tests are provided as part of Joda's
  // implementation.
  EXPECT_EQ(
      TimestampWithTimezone(0, 0), parseDatetime("1970-01-01", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0),
      parseDatetime("1970-01-02", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0),
      parseDatetime("19700102", "YYYYMMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMd"));

  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(118860000, tz::getTimeZoneID("+00:00")),
      parseDatetime("1970-01-02+09:01+00:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, tz::getTimeZoneID("-09:00")),
      parseDatetime("1970-01-02+00:01-09:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, tz::getTimeZoneID("-02:00")),
      parseDatetime("1970-01-02+07:01-02:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, tz::getTimeZoneID("+14:00")),
      parseDatetime("1970-01-02+23:01+14:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(
          198060000, tz::getTimeZoneID("America/Los_Angeles")),
      parseDatetime("1970-01-02+23:01 PST", "YYYY-MM-dd+HH:mm z"));
  EXPECT_EQ(
      TimestampWithTimezone(169260000, tz::getTimeZoneID("+00:00")),
      parseDatetime("1970-01-02+23:01 GMT", "YYYY-MM-dd+HH:mm z"));

  setQueryTimeZone("Asia/Kolkata");

  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(66600000, tz::getTimeZoneID("Asia/Kolkata")),
      parseDatetime("1970-01-02+00:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(66600000, tz::getTimeZoneID("-03:00")),
      parseDatetime("1970-01-01+15:30-03:00", "YYYY-MM-dd+HH:mmZZ"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, tz::getTimeZoneID("Asia/Kolkata")),
      parseDatetime("1969-12-31+11:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, tz::getTimeZoneID("+02:00")),
      parseDatetime("1969-12-31+07:30+02:00", "YYYY-MM-dd+HH:mmZZ"));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, formatDateTime) {
  using util::fromTimestampString;

  // era test cases - 'G'
  EXPECT_EQ(
      "AD", formatDatetime(fromTimestampString("1970-01-01", nullptr), "G"));
  EXPECT_EQ(
      "BC", formatDatetime(fromTimestampString("-100-01-01", nullptr), "G"));
  EXPECT_EQ("BC", formatDatetime(fromTimestampString("0-01-01", nullptr), "G"));
  EXPECT_EQ(
      "AD", formatDatetime(fromTimestampString("01-01-01", nullptr), "G"));
  EXPECT_EQ(
      "AD",
      formatDatetime(fromTimestampString("01-01-01", nullptr), "GGGGGGG"));

  // century of era test cases - 'C'
  EXPECT_EQ(
      "19", formatDatetime(fromTimestampString("1900-01-01", nullptr), "C"));
  EXPECT_EQ(
      "19", formatDatetime(fromTimestampString("1955-01-01", nullptr), "C"));
  EXPECT_EQ(
      "20", formatDatetime(fromTimestampString("2000-01-01", nullptr), "C"));
  EXPECT_EQ(
      "20", formatDatetime(fromTimestampString("2020-01-01", nullptr), "C"));
  EXPECT_EQ("0", formatDatetime(fromTimestampString("0-01-01", nullptr), "C"));
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("-100-01-01", nullptr), "C"));
  EXPECT_EQ(
      "19", formatDatetime(fromTimestampString("-1900-01-01", nullptr), "C"));
  EXPECT_EQ(
      "000019",
      formatDatetime(fromTimestampString("1955-01-01", nullptr), "CCCCCC"));

  // year of era test cases - 'Y'
  EXPECT_EQ(
      "1970", formatDatetime(fromTimestampString("1970-01-01", nullptr), "Y"));
  EXPECT_EQ(
      "2020", formatDatetime(fromTimestampString("2020-01-01", nullptr), "Y"));
  EXPECT_EQ("1", formatDatetime(fromTimestampString("0-01-01", nullptr), "Y"));
  EXPECT_EQ(
      "101", formatDatetime(fromTimestampString("-100-01-01", nullptr), "Y"));
  EXPECT_EQ(
      "70", formatDatetime(fromTimestampString("1970-01-01", nullptr), "YY"));
  EXPECT_EQ(
      "70", formatDatetime(fromTimestampString("-1970-01-01", nullptr), "YY"));
  EXPECT_EQ(
      "1948",
      formatDatetime(fromTimestampString("1948-01-01", nullptr), "YYY"));
  EXPECT_EQ(
      "1234",
      formatDatetime(fromTimestampString("1234-01-01", nullptr), "YYYY"));
  EXPECT_EQ(
      "0000000001",
      formatDatetime(fromTimestampString("01-01-01", nullptr), "YYYYYYYYYY"));

  // day of week number - 'e'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        std::to_string(i % 7 + 1),
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "e"));
  }
  EXPECT_EQ(
      "000001",
      formatDatetime(fromTimestampString("2022-08-01", nullptr), "eeeeee"));

  // day of week text - 'E'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "E"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "EE"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "EEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "EEEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(
            fromTimestampString(StringView{date}, nullptr), "EEEEEEEE"));
  }

  // year test cases - 'y'
  EXPECT_EQ(
      "2022", formatDatetime(fromTimestampString("2022-06-20", nullptr), "y"));
  EXPECT_EQ(
      "22", formatDatetime(fromTimestampString("2022-06-20", nullptr), "yy"));
  EXPECT_EQ(
      "2022",
      formatDatetime(fromTimestampString("2022-06-20", nullptr), "yyy"));
  EXPECT_EQ(
      "2022",
      formatDatetime(fromTimestampString("2022-06-20", nullptr), "yyyy"));

  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("10-06-20", nullptr), "y"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("10-06-20", nullptr), "yy"));
  EXPECT_EQ(
      "010", formatDatetime(fromTimestampString("10-06-20", nullptr), "yyy"));
  EXPECT_EQ(
      "0010", formatDatetime(fromTimestampString("10-06-20", nullptr), "yyyy"));

  EXPECT_EQ(
      "-16", formatDatetime(fromTimestampString("-16-06-20", nullptr), "y"));
  EXPECT_EQ(
      "16", formatDatetime(fromTimestampString("-16-06-20", nullptr), "yy"));
  EXPECT_EQ(
      "-016", formatDatetime(fromTimestampString("-16-06-20", nullptr), "yyy"));
  EXPECT_EQ(
      "-0016",
      formatDatetime(fromTimestampString("-16-06-20", nullptr), "yyyy"));

  EXPECT_EQ(
      "00", formatDatetime(fromTimestampString("-1600-06-20", nullptr), "yy"));
  EXPECT_EQ(
      "01", formatDatetime(fromTimestampString("-1601-06-20", nullptr), "yy"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("-1610-06-20", nullptr), "yy"));

  // day of year test cases - 'D'
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01", nullptr), "D"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("2022-01-10", nullptr), "D"));
  EXPECT_EQ(
      "100", formatDatetime(fromTimestampString("2022-04-10", nullptr), "D"));
  EXPECT_EQ(
      "365", formatDatetime(fromTimestampString("2022-12-31", nullptr), "D"));
  EXPECT_EQ(
      "00100",
      formatDatetime(fromTimestampString("2022-04-10", nullptr), "DDDDD"));

  // leap year case
  EXPECT_EQ(
      "60", formatDatetime(fromTimestampString("2020-02-29", nullptr), "D"));
  EXPECT_EQ(
      "366", formatDatetime(fromTimestampString("2020-12-31", nullptr), "D"));

  // month of year test cases - 'M'
  for (int i = 0; i < 12; i++) {
    auto month = i + 1;
    std::string date("2022-" + std::to_string(month) + "-01");
    EXPECT_EQ(
        std::to_string(month),
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "M"));
    EXPECT_EQ(
        padNumber(month),
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "MM"));
    EXPECT_EQ(
        monthsShort[i],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "MMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(fromTimestampString(StringView{date}, nullptr), "MMMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(
            fromTimestampString(StringView{date}, nullptr), "MMMMMMMM"));
  }

  // day of month test cases - 'd'
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01", nullptr), "d"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("2022-01-10", nullptr), "d"));
  EXPECT_EQ(
      "28", formatDatetime(fromTimestampString("2022-01-28", nullptr), "d"));
  EXPECT_EQ(
      "31", formatDatetime(fromTimestampString("2022-01-31", nullptr), "d"));
  EXPECT_EQ(
      "00000031",
      formatDatetime(fromTimestampString("2022-01-31", nullptr), "dddddddd"));

  // leap year case
  EXPECT_EQ(
      "29", formatDatetime(fromTimestampString("2020-02-29", nullptr), "d"));

  // halfday of day test cases - 'a'
  EXPECT_EQ(
      "AM",
      formatDatetime(fromTimestampString("2022-01-01 00:00:00", nullptr), "a"));
  EXPECT_EQ(
      "AM",
      formatDatetime(fromTimestampString("2022-01-01 11:59:59", nullptr), "a"));
  EXPECT_EQ(
      "PM",
      formatDatetime(fromTimestampString("2022-01-01 12:00:00", nullptr), "a"));
  EXPECT_EQ(
      "PM",
      formatDatetime(fromTimestampString("2022-01-01 23:59:59", nullptr), "a"));
  EXPECT_EQ(
      "AM",
      formatDatetime(
          fromTimestampString("2022-01-01 00:00:00", nullptr), "aaaaaaaa"));
  EXPECT_EQ(
      "PM",
      formatDatetime(
          fromTimestampString("2022-01-01 12:00:00", nullptr), "aaaaaaaa"));

  // hour of halfday test cases - 'K'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string(i % 12),
        formatDatetime(fromTimestampString(date, nullptr), "K"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(
          fromTimestampString("2022-01-01 11:00:00", nullptr), "KKKKKKKK"));

  // clockhour of halfday test cases - 'h'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 11) % 12 + 1),
        formatDatetime(fromTimestampString(date, nullptr), "h"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(
          fromTimestampString("2022-01-01 11:00:00", nullptr), "hhhhhhhh"));

  // hour of day test cases - 'H'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string(i),
        formatDatetime(fromTimestampString(date, nullptr), "H"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(
          fromTimestampString("2022-01-01 11:00:00", nullptr), "HHHHHHHH"));

  // clockhour of day test cases - 'k'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 23) % 24 + 1),
        formatDatetime(fromTimestampString(date, nullptr), "k"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(
          fromTimestampString("2022-01-01 11:00:00", nullptr), "kkkkkkkk"));

  // minute of hour test cases - 'm'
  EXPECT_EQ(
      "0",
      formatDatetime(fromTimestampString("2022-01-01 00:00:00", nullptr), "m"));
  EXPECT_EQ(
      "1",
      formatDatetime(fromTimestampString("2022-01-01 01:01:00", nullptr), "m"));
  EXPECT_EQ(
      "10",
      formatDatetime(fromTimestampString("2022-01-01 02:10:00", nullptr), "m"));
  EXPECT_EQ(
      "30",
      formatDatetime(fromTimestampString("2022-01-01 03:30:00", nullptr), "m"));
  EXPECT_EQ(
      "59",
      formatDatetime(fromTimestampString("2022-01-01 04:59:00", nullptr), "m"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(
          fromTimestampString("2022-01-01 00:42:42", nullptr), "mmmmmmmm"));

  // second of minute test cases - 's'
  EXPECT_EQ(
      "0",
      formatDatetime(fromTimestampString("2022-01-01 00:00:00", nullptr), "s"));
  EXPECT_EQ(
      "1",
      formatDatetime(fromTimestampString("2022-01-01 01:01:01", nullptr), "s"));
  EXPECT_EQ(
      "10",
      formatDatetime(fromTimestampString("2022-01-01 02:10:10", nullptr), "s"));
  EXPECT_EQ(
      "30",
      formatDatetime(fromTimestampString("2022-01-01 03:30:30", nullptr), "s"));
  EXPECT_EQ(
      "59",
      formatDatetime(fromTimestampString("2022-01-01 04:59:59", nullptr), "s"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(
          fromTimestampString("2022-01-01 00:42:42", nullptr), "ssssssss"));

  // fraction of second test cases - 'S'
  EXPECT_EQ(
      "0",
      formatDatetime(
          fromTimestampString("2022-01-01 00:00:00.0", nullptr), "S"));
  EXPECT_EQ(
      "1",
      formatDatetime(
          fromTimestampString("2022-01-01 00:00:00.1", nullptr), "S"));
  EXPECT_EQ(
      "1",
      formatDatetime(
          fromTimestampString("2022-01-01 01:01:01.11", nullptr), "S"));
  EXPECT_EQ(
      "11",
      formatDatetime(
          fromTimestampString("2022-01-01 02:10:10.11", nullptr), "SS"));
  EXPECT_EQ(
      "9",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "S"));
  EXPECT_EQ(
      "99",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "SS"));
  EXPECT_EQ(
      "999",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "SSS"));
  EXPECT_EQ(
      "12300000",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.123", nullptr), "SSSSSSSS"));
  EXPECT_EQ(
      "0990",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.099", nullptr), "SSSS"));
  EXPECT_EQ(
      "0010",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.001", nullptr), "SSSS"));

  // time zone test cases - 'z'
  setQueryTimeZone("Asia/Kolkata");
  EXPECT_EQ(
      "Asia/Kolkata",
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "zzzz"));

  // literal test cases
  EXPECT_EQ(
      "hello",
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "'hello'"));
  EXPECT_EQ(
      "'", formatDatetime(fromTimestampString("1970-01-01", nullptr), "''"));
  EXPECT_EQ(
      "1970 ' 1970",
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "y '' y"));
  EXPECT_EQ(
      "he'llo",
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "'he''llo'"));
  EXPECT_EQ(
      "'he'llo'",
      formatDatetime(
          fromTimestampString("1970-01-01", nullptr), "'''he''llo'''"));
  EXPECT_EQ(
      "1234567890",
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "1234567890"));
  EXPECT_EQ(
      "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890",
      formatDatetime(
          fromTimestampString("1970-01-01", nullptr),
          "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890"));

  // Multi-specifier and literal formats
  EXPECT_EQ(
      "AD 19 1970 4 Thu 1970 1 1 1 AM 8 8 8 8 3 11 5 Asia/Kolkata",
      formatDatetime(
          fromTimestampString("1970-01-01 02:33:11.5", nullptr),
          "G C Y e E y D M d a K h H k m s S zzzz"));
  EXPECT_EQ(
      "AD 19 1970 4 asdfghjklzxcvbnmqwertyuiop Thu ' 1970 1 1 1 AM 8 8 8 8 3 11 5 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ Asia/Kolkata",
      formatDatetime(
          fromTimestampString("1970-01-01 02:33:11.5", nullptr),
          "G C Y e 'asdfghjklzxcvbnmqwertyuiop' E '' y D M d a K h H k m s S 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ zzzz"));

  // User format errors or unsupported errors
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "x"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "w"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "z"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "zz"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "zzz"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "q"),
      BoltUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01", nullptr), "'abcd"),
      BoltUserError);
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, formatDateTimeTimezone) {
  using util::fromTimestampString;
  auto zeroTs = fromTimestampString("1970-01-01", nullptr);

  // No timezone set; default to GMT.
  EXPECT_EQ(
      "1970-01-01 00:00:00", formatDatetime(zeroTs, "YYYY-MM-dd HH:mm:ss"));

  // Check that string is adjusted to the timezone set.
  EXPECT_EQ(
      "1970-01-01 05:30:00",
      formatDatetimeWithTimezone(
          zeroTs, "Asia/Kolkata", "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1969-12-31 16:00:00",
      formatDatetimeWithTimezone(
          zeroTs, "America/Los_Angeles", "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1969-12-31 16:00:00",
      formatDatetimeWithTimezone(zeroTs, "-08:00", "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1969-12-31 23:45:00",
      formatDatetimeWithTimezone(zeroTs, "-00:15", "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1970-01-01 00:07:00",
      formatDatetimeWithTimezone(zeroTs, "+00:07", "YYYY-MM-dd HH:mm:ss"));
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, dateFormat) {
  const auto dateFormatOnce = [&](std::optional<Timestamp> timestamp,
                                  const std::string& formatString) {
    return evaluateOnce<std::string>(
        fmt::format("date_format(c0, '{}')", formatString), timestamp);
  };
  using util::fromTimestampString;

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateFormatOnce(std::nullopt, "%Y"));

  // Normal cases
  EXPECT_EQ(
      "1970-01-01",
      dateFormat(fromTimestampString("1970-01-01", nullptr), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 12:00:00 AM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 00:00:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 00:00:00.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));

  // Varying digit year cases
  EXPECT_EQ("06", dateFormat(fromTimestampString("-6-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "-0006", dateFormat(fromTimestampString("-6-06-20", nullptr), "%Y"));
  EXPECT_EQ("16", dateFormat(fromTimestampString("-16-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "-0016", dateFormat(fromTimestampString("-16-06-20", nullptr), "%Y"));
  EXPECT_EQ("66", dateFormat(fromTimestampString("-166-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "-0166", dateFormat(fromTimestampString("-166-06-20", nullptr), "%Y"));
  EXPECT_EQ(
      "66", dateFormat(fromTimestampString("-1666-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "00", dateFormat(fromTimestampString("-1900-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "01", dateFormat(fromTimestampString("-1901-06-20", nullptr), "%y"));
  EXPECT_EQ(
      "10", dateFormat(fromTimestampString("-1910-06-20", nullptr), "%y"));
  EXPECT_EQ("12", dateFormat(fromTimestampString("-12-06-20", nullptr), "%y"));
  EXPECT_EQ("00", dateFormat(fromTimestampString("1900-06-20", nullptr), "%y"));
  EXPECT_EQ("01", dateFormat(fromTimestampString("1901-06-20", nullptr), "%y"));
  EXPECT_EQ("10", dateFormat(fromTimestampString("1910-06-20", nullptr), "%y"));

  // Day of week cases
  for (int i = 0; i < 8; i++) {
    std::string date("1996-01-0" + std::to_string(i + 1));
    // Full length name
    EXPECT_EQ(
        daysLong[i % 7],
        dateFormat(fromTimestampString(StringView{date}, nullptr), "%W"));
    // Abbreviated name
    EXPECT_EQ(
        daysShort[i % 7],
        dateFormat(fromTimestampString(StringView{date}, nullptr), "%a"));
  }

  // Month cases
  for (int i = 0; i < 12; i++) {
    std::string date("1996-" + std::to_string(i + 1) + "-01");
    std::string monthNum = std::to_string(i + 1);
    // Full length name
    EXPECT_EQ(
        monthsLong[i % 12],
        dateFormat(fromTimestampString(StringView{date}, nullptr), "%M"));
    // Abbreviated name
    EXPECT_EQ(
        monthsShort[i % 12],
        dateFormat(fromTimestampString(StringView{date}, nullptr), "%b"));
    // Numeric
    EXPECT_EQ(
        monthNum,
        dateFormat(fromTimestampString(StringView{date}, nullptr), "%c"));
    // Numeric 0-padded
    if (i + 1 < 10) {
      EXPECT_EQ(
          "0" + monthNum,
          dateFormat(fromTimestampString(StringView{date}, nullptr), "%m"));
    } else {
      EXPECT_EQ(
          monthNum,
          dateFormat(fromTimestampString(StringView{date}, nullptr), "%m"));
    }
  }

  // Day of month cases
  for (int i = 1; i <= 31; i++) {
    std::string dayOfMonth = std::to_string(i);
    std::string date("1970-01-" + dayOfMonth);
    EXPECT_EQ(
        dayOfMonth,
        dateFormat(util::fromTimestampString(StringView{date}, nullptr), "%e"));
    if (i < 10) {
      EXPECT_EQ(
          "0" + dayOfMonth,
          dateFormat(
              util::fromTimestampString(StringView{date}, nullptr), "%d"));
    } else {
      EXPECT_EQ(
          dayOfMonth,
          dateFormat(
              util::fromTimestampString(StringView{date}, nullptr), "%d"));
    }
  }

  // Fraction of second cases
  EXPECT_EQ(
      "000000",
      dateFormat(fromTimestampString("2022-01-01 00:00:00.0", nullptr), "%f"));
  EXPECT_EQ(
      "100000",
      dateFormat(fromTimestampString("2022-01-01 00:00:00.1", nullptr), "%f"));
  EXPECT_EQ(
      "110000",
      dateFormat(fromTimestampString("2022-01-01 01:01:01.11", nullptr), "%f"));
  EXPECT_EQ(
      "110000",
      dateFormat(fromTimestampString("2022-01-01 02:10:10.11", nullptr), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.999", nullptr), "%f"));
  EXPECT_EQ(
      "123000",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.123", nullptr), "%f"));
  EXPECT_EQ(
      "099000",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.099", nullptr), "%f"));
  EXPECT_EQ(
      "001234",
      dateFormat(
          fromTimestampString("2022-01-01 03:30:30.001234", nullptr), "%f"));

  // Hour cases
  for (int i = 0; i < 24; i++) {
    std::string hour = std::to_string(i);
    int clockHour = (i + 11) % 12 + 1;
    std::string clockHourString = std::to_string(clockHour);
    std::string toBuild = "1996-01-01 " + hour + ":00:00";
    StringView date(toBuild);
    EXPECT_EQ(hour, dateFormat(util::fromTimestampString(date, nullptr), "%k"));
    if (i < 10) {
      EXPECT_EQ(
          "0" + hour,
          dateFormat(util::fromTimestampString(date, nullptr), "%H"));
    } else {
      EXPECT_EQ(
          hour, dateFormat(util::fromTimestampString(date, nullptr), "%H"));
    }

    EXPECT_EQ(
        clockHourString,
        dateFormat(util::fromTimestampString(date, nullptr), "%l"));
    if (clockHour < 10) {
      EXPECT_EQ(
          "0" + clockHourString,
          dateFormat(util::fromTimestampString(date, nullptr), "%h"));
      EXPECT_EQ(
          "0" + clockHourString,
          dateFormat(util::fromTimestampString(date, nullptr), "%I"));
    } else {
      EXPECT_EQ(
          clockHourString,
          dateFormat(util::fromTimestampString(date, nullptr), "%h"));
      EXPECT_EQ(
          clockHourString,
          dateFormat(util::fromTimestampString(date, nullptr), "%I"));
    }
  }

  // Minute cases
  for (int i = 0; i < 60; i++) {
    std::string minute = std::to_string(i);
    std::string toBuild = "1996-01-01 00:" + minute + ":00";
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ(
          "0" + minute, dateFormat(fromTimestampString(date, nullptr), "%i"));
    } else {
      EXPECT_EQ(minute, dateFormat(fromTimestampString(date, nullptr), "%i"));
    }
  }

  // Second cases
  for (int i = 0; i < 60; i++) {
    std::string second = std::to_string(i);
    std::string toBuild = "1996-01-01 00:00:" + second;
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ(
          "0" + second, dateFormat(fromTimestampString(date, nullptr), "%S"));
      EXPECT_EQ(
          "0" + second, dateFormat(fromTimestampString(date, nullptr), "%s"));
    } else {
      EXPECT_EQ(second, dateFormat(fromTimestampString(date, nullptr), "%S"));
      EXPECT_EQ(second, dateFormat(fromTimestampString(date, nullptr), "%s"));
    }
  }

  // Day of year cases
  EXPECT_EQ(
      "001", dateFormat(fromTimestampString("2022-01-01", nullptr), "%j"));
  EXPECT_EQ(
      "010", dateFormat(fromTimestampString("2022-01-10", nullptr), "%j"));
  EXPECT_EQ(
      "100", dateFormat(fromTimestampString("2022-04-10", nullptr), "%j"));
  EXPECT_EQ(
      "365", dateFormat(fromTimestampString("2022-12-31", nullptr), "%j"));

  // Halfday of day cases
  EXPECT_EQ(
      "AM",
      dateFormat(fromTimestampString("2022-01-01 00:00:00", nullptr), "%p"));
  EXPECT_EQ(
      "AM",
      dateFormat(fromTimestampString("2022-01-01 11:59:59", nullptr), "%p"));
  EXPECT_EQ(
      "PM",
      dateFormat(fromTimestampString("2022-01-01 12:00:00", nullptr), "%p"));
  EXPECT_EQ(
      "PM",
      dateFormat(fromTimestampString("2022-01-01 23:59:59", nullptr), "%p"));

  // 12-hour time cases
  EXPECT_EQ(
      "12:00:00 AM",
      dateFormat(fromTimestampString("2022-01-01 00:00:00", nullptr), "%r"));
  EXPECT_EQ(
      "11:59:59 AM",
      dateFormat(fromTimestampString("2022-01-01 11:59:59", nullptr), "%r"));
  EXPECT_EQ(
      "12:00:00 PM",
      dateFormat(fromTimestampString("2022-01-01 12:00:00", nullptr), "%r"));
  EXPECT_EQ(
      "11:59:59 PM",
      dateFormat(fromTimestampString("2022-01-01 23:59:59", nullptr), "%r"));

  // 24-hour time cases
  EXPECT_EQ(
      "00:00:00",
      dateFormat(fromTimestampString("2022-01-01 00:00:00", nullptr), "%T"));
  EXPECT_EQ(
      "11:59:59",
      dateFormat(fromTimestampString("2022-01-01 11:59:59", nullptr), "%T"));
  EXPECT_EQ(
      "12:00:00",
      dateFormat(fromTimestampString("2022-01-01 12:00:00", nullptr), "%T"));
  EXPECT_EQ(
      "23:59:59",
      dateFormat(fromTimestampString("2022-01-01 23:59:59", nullptr), "%T"));

  // Percent followed by non-existent specifier case
  EXPECT_EQ("q", dateFormat(fromTimestampString("1970-01-01", nullptr), "%q"));
  EXPECT_EQ("z", dateFormat(fromTimestampString("1970-01-01", nullptr), "%z"));
  EXPECT_EQ("g", dateFormat(fromTimestampString("1970-01-01", nullptr), "%g"));

  // With timezone. Indian Standard Time (IST) UTC+5:30.
  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(
      "1970-01-01",
      dateFormat(fromTimestampString("1970-01-01", nullptr), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 05:30:00 AM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 05:30:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 05:53:28.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));

  // Same timestamps with a different timezone. Pacific Daylight Time (North
  // America) PDT UTC-8:00.
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(
      "1969-12-31",
      dateFormat(fromTimestampString("1970-01-01", nullptr), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-28 04:00:00 PM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-28 16:00:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-28 16:07:02.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr),
          "%Y-%m-%d %H:%i:%s.%f"));

  // User format errors or unsupported errors
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%D"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%U"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%u"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%V"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%w"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%X"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%v"),
      BoltUserError);
  EXPECT_THROW(
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987", nullptr), "%x"),
      BoltUserError);
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dateFormatTimestampWithTimezone) {
  const auto testDateFormat =
      [&](const std::string& formatString,
          std::optional<int64_t> timestamp,
          const std::optional<std::string>& timeZoneName) {
        return evaluateWithTimestampWithTimezone<std::string>(
            fmt::format("date_format(c0, '{}')", formatString),
            timestamp,
            timeZoneName);
      };

  EXPECT_EQ(
      "1969-12-31 11:00:00 PM", testDateFormat("%Y-%m-%d %r", 0, "-01:00"));
  EXPECT_EQ(
      "1973-11-30 12:33:09 AM",
      testDateFormat("%Y-%m-%d %r", 123456789000, "+03:00"));
  EXPECT_EQ(
      "1966-02-01 12:26:51 PM",
      testDateFormat("%Y-%m-%d %r", -123456789000, "-14:00"));
  EXPECT_EQ(
      "2001-04-19 18:25:21.000000",
      testDateFormat("%Y-%m-%d %H:%i:%s.%f", 987654321000, "+14:00"));
  EXPECT_EQ(
      "1938-09-14 23:34:39.000000",
      testDateFormat("%Y-%m-%d %H:%i:%s.%f", -987654321000, "+04:00"));
  EXPECT_EQ(
      "70-August-22 17:55:15 PM",
      testDateFormat("%y-%M-%e %T %p", 20220915000, "-07:00"));
  EXPECT_EQ(
      "69-May-11 20:04:45 PM",
      testDateFormat("%y-%M-%e %T %p", -20220915000, "-03:00"));
}
#endif

TEST_F(PrestoSqlDateTimeFunctionsTest, dateParse) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, dateParse("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, std::nullopt));

  // Simple tests. More exhaustive tests are provided in DateTimeFormatterTest.
  EXPECT_EQ(Timestamp(86400, 0), dateParse("1970-01-02", "%Y-%m-%d"));
  EXPECT_EQ(Timestamp(0, 0), dateParse("1970-01-01", "%Y-%m-%d"));
  EXPECT_EQ(Timestamp(86400, 0), dateParse("19700102", "%Y%m%d"));

  // Tests for differing query timezones
  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+09:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+01:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Noronha");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+07:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("+04:00");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+13:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("Asia/Kolkata");
  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      Timestamp(66600, 0), dateParse("1970-01-02+00:00", "%Y-%m-%d+%H:%i"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      Timestamp(-66600, 0), dateParse("1969-12-31+11:00", "%Y-%m-%d+%H:%i"));

  BOLT_ASSERT_THROW(dateParse("", "%y+"), "Invalid date format: ''");
  BOLT_ASSERT_THROW(dateParse("1", "%y+"), "Invalid date format: '1'");
  BOLT_ASSERT_THROW(dateParse("116", "%y+"), "Invalid date format: '116'");
}

TEST_F(PrestoSqlDateTimeFunctionsTest, date2pdate) {
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

TEST_F(PrestoSqlDateTimeFunctionsTest, dateFunctionVarchar) {
  const auto dateFunction = [&](const std::optional<std::string>& dateString) {
    return evaluateOnce<int32_t>("date(c0)", dateString);
  };

  // Date(0) is 1970-01-01.
  EXPECT_EQ(0, dateFunction("1970-01-01"));
  // Date(18297) is 2020-02-05.
  EXPECT_EQ(18297, dateFunction("2020-02-05"));
  // Date(-18297) is 1919-11-28.
  EXPECT_EQ(-18297, dateFunction("1919-11-28"));

  // Illegal date format.
  EXPECT_EQ(std::nullopt, dateFunction("asdlxcvf"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dateFunctionTimestamp) {
  static const int64_t kSecondsInDay = 86'400;
  static const uint64_t kNanosInSecond = 1'000'000'000;

  const auto dateFunction = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int32_t>("date(c0)", timestamp);
  };

  EXPECT_EQ(0, dateFunction(Timestamp()));
  EXPECT_EQ(1, dateFunction(Timestamp(kSecondsInDay, 0)));
  EXPECT_EQ(-1, dateFunction(Timestamp(-kSecondsInDay, 0)));
  EXPECT_EQ(18297, dateFunction(Timestamp(18297 * kSecondsInDay, 0)));
  EXPECT_EQ(18297, dateFunction(Timestamp(18297 * kSecondsInDay, 123)));
  EXPECT_EQ(-18297, dateFunction(Timestamp(-18297 * kSecondsInDay, 0)));
  EXPECT_EQ(-18297, dateFunction(Timestamp(-18297 * kSecondsInDay, 123)));

  // Last second of day 0
  EXPECT_EQ(0, dateFunction(Timestamp(kSecondsInDay - 1, 0)));
  // Last nanosecond of day 0
  EXPECT_EQ(0, dateFunction(Timestamp(kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -1
  EXPECT_EQ(-1, dateFunction(Timestamp(-1, 0)));
  // Last nanosecond of day -1
  EXPECT_EQ(-1, dateFunction(Timestamp(-1, kNanosInSecond - 1)));

  // Last second of day 18297
  EXPECT_EQ(
      18297,
      dateFunction(Timestamp(18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day 18297
  EXPECT_EQ(
      18297,
      dateFunction(Timestamp(
          18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -18297
  EXPECT_EQ(
      -18297,
      dateFunction(Timestamp(-18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day -18297
  EXPECT_EQ(
      -18297,
      dateFunction(Timestamp(
          -18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, dateFunctionTimestampWithTimezone) {
  static const int64_t kSecondsInDay = 86'400;

  const auto dateFunction =
      [&](std::optional<int64_t> timestamp,
          const std::optional<std::string>& timeZoneName) {
        return evaluateWithTimestampWithTimezone<int32_t>(
            "date(c0)", timestamp, timeZoneName);
      };

  // 1970-01-01 00:00:00.000 +00:00
  EXPECT_EQ(0, dateFunction(0, "+00:00"));
  EXPECT_EQ(0, dateFunction(0, "Europe/London"));
  // 1970-01-01 00:00:00.000 -08:00
  EXPECT_EQ(-1, dateFunction(0, "-08:00"));
  EXPECT_EQ(-1, dateFunction(0, "America/Los_Angeles"));
  // 1970-01-01 00:00:00.000 +08:00
  EXPECT_EQ(0, dateFunction(0, "+08:00"));
  EXPECT_EQ(0, dateFunction(0, "Asia/Chongqing"));
  // 1970-01-01 18:00:00.000 +08:00
  EXPECT_EQ(1, dateFunction(18 * 3'600 * 1'000, "+08:00"));
  EXPECT_EQ(1, dateFunction(18 * 3'600 * 1'000, "Asia/Chongqing"));
  // 1970-01-01 06:00:00.000 -08:00
  EXPECT_EQ(-1, dateFunction(6 * 3'600 * 1'000, "-08:00"));
  EXPECT_EQ(-1, dateFunction(6 * 3'600 * 1'000, "America/Los_Angeles"));

  // 2020-02-05 10:00:00.000 +08:00
  EXPECT_EQ(
      18297,
      dateFunction((18297 * kSecondsInDay + 10 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      18297,
      dateFunction(
          (18297 * kSecondsInDay + 10 * 3'600) * 1'000, "Asia/Chongqing"));
  // 2020-02-05 20:00:00.000 +08:00
  EXPECT_EQ(
      18298,
      dateFunction((18297 * kSecondsInDay + 20 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      18298,
      dateFunction(
          (18297 * kSecondsInDay + 20 * 3'600) * 1'000, "Asia/Chongqing"));
  // 2020-02-05 16:00:00.000 -08:00
  EXPECT_EQ(
      18297,
      dateFunction((18297 * kSecondsInDay + 16 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      18297,
      dateFunction(
          (18297 * kSecondsInDay + 16 * 3'600) * 1'000, "America/Los_Angeles"));
  // 2020-02-05 06:00:00.000 -08:00
  EXPECT_EQ(
      18296,
      dateFunction((18297 * kSecondsInDay + 6 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      18296,
      dateFunction(
          (18297 * kSecondsInDay + 6 * 3'600) * 1'000, "America/Los_Angeles"));

  // 1919-11-28 10:00:00.000 +08:00
  EXPECT_EQ(
      -18297,
      dateFunction((-18297 * kSecondsInDay + 10 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      -18297,
      dateFunction(
          (-18297 * kSecondsInDay + 10 * 3'600) * 1'000, "Asia/Chongqing"));
  // 1919-11-28 20:00:00.000 +08:00
  EXPECT_EQ(
      -18296,
      dateFunction((-18297 * kSecondsInDay + 20 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      -18296,
      dateFunction(
          (-18297 * kSecondsInDay + 20 * 3'600) * 1'000, "Asia/Chongqing"));
  // 1919-11-28 16:00:00.000 -08:00
  EXPECT_EQ(
      -18297,
      dateFunction((-18297 * kSecondsInDay + 16 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      -18297,
      dateFunction(
          (-18297 * kSecondsInDay + 16 * 3'600) * 1'000,
          "America/Los_Angeles"));
  // 1919-11-28 06:00:00.000 -08:00
  EXPECT_EQ(
      -18298,
      dateFunction((-18297 * kSecondsInDay + 6 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      -18298,
      dateFunction(
          (-18297 * kSecondsInDay + 6 * 3'600) * 1'000, "America/Los_Angeles"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, castDateForDateFunction) {
  setQueryTimeZone("America/Los_Angeles");

  static const int64_t kSecondsInDay = 86'400;
  static const uint64_t kNanosInSecond = 1'000'000'000;
  const auto castDateTest = [&](std::optional<Timestamp> timestamp) {
    auto r1 = evaluateOnce<int32_t>("cast(c0 as date)", timestamp);
    auto r2 = evaluateOnce<int32_t>("date(c0)", timestamp);
    EXPECT_EQ(r1, r2);
    return r1;
  };

  // Note adjustments for PST timezone.
  EXPECT_EQ(-1, castDateTest(Timestamp()));
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay, 0)));
  EXPECT_EQ(-2, castDateTest(Timestamp(-kSecondsInDay, 0)));
  EXPECT_EQ(18296, castDateTest(Timestamp(18297 * kSecondsInDay, 0)));
  EXPECT_EQ(18296, castDateTest(Timestamp(18297 * kSecondsInDay, 123)));
  EXPECT_EQ(-18298, castDateTest(Timestamp(-18297 * kSecondsInDay, 0)));
  EXPECT_EQ(-18298, castDateTest(Timestamp(-18297 * kSecondsInDay, 123)));

  // Last second of day 0.
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay - 1, 0)));
  // Last nanosecond of day 0.
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -1.
  EXPECT_EQ(-1, castDateTest(Timestamp(-1, 0)));
  // Last nanosecond of day -1.
  EXPECT_EQ(-1, castDateTest(Timestamp(-1, kNanosInSecond - 1)));

  // Last second of day 18297.
  EXPECT_EQ(
      18297,
      castDateTest(Timestamp(18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day 18297.
  EXPECT_EQ(
      18297,
      castDateTest(Timestamp(
          18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -18297.
  EXPECT_EQ(
      -18297,
      castDateTest(Timestamp(-18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day -18297.
  EXPECT_EQ(
      -18297,
      castDateTest(Timestamp(
          -18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, currentDateWithTimezone) {
  // Since the execution of the code is slightly delayed, it is difficult for us
  // to get the correct value of current_date. If you compare directly based on
  // the current time, you may get wrong result at the last second of the day,
  // and current_date may be the next day of the comparison value. In order to
  // avoid this situation, we compute a new comparison value after the execution
  // of current_date, so that the result of current_date is either consistent
  // with the first comparison value or the second comparison value, and the
  // difference between the two comparison values is at most one day.
  auto emptyRowVector = makeRowVector(ROW({}), 1);
  auto tz = "America/Los_Angeles";
  setQueryTimeZone(tz);
  auto dateBefore = getCurrentDate(tz);
  auto result = evaluateOnce<int32_t>("current_date()", emptyRowVector);
  auto dateAfter = getCurrentDate(tz);

  EXPECT_TRUE(result.has_value());
  EXPECT_LE(dateBefore, result);
  EXPECT_LE(result, dateAfter);
  EXPECT_LE(dateAfter - dateBefore, 1);
}

TEST_F(PrestoSqlDateTimeFunctionsTest, currentDateWithoutTimezone) {
  auto emptyRowVector = makeRowVector(ROW({}), 1);

  // Do not set the timezone, so the timezone obtained from QueryConfig
  // will be nullptr.
  auto dateBefore = getCurrentDate(std::nullopt);
  auto result = evaluateOnce<int32_t>("current_date()", emptyRowVector);
  auto dateAfter = getCurrentDate(std::nullopt);

  EXPECT_TRUE(result.has_value());
  EXPECT_LE(dateBefore, result);
  EXPECT_LE(result, dateAfter);
  EXPECT_LE(dateAfter - dateBefore, 1);
}

TEST_F(PrestoSqlDateTimeFunctionsTest, timeZoneHour) {
  const auto timezone_hour = [&](const char* time, const char* timezone) {
    Timestamp ts = util::fromTimestampString(time, nullptr);
    auto timestamp = ts.toMillis();
    auto hour = evaluateWithTimestampWithTimezone<int64_t>(
                    "timezone_hour(c0)", timestamp, timezone)
                    .value();
    return hour;
  };

  // Asia/Kolkata - should return 5 throughout the year
  EXPECT_EQ(5, timezone_hour("2023-01-01 03:20:00", "Asia/Kolkata"));
  EXPECT_EQ(5, timezone_hour("2023-06-01 03:20:00", "Asia/Kolkata"));

  // America/Los_Angeles - Day light savings is from March 12 to Nov 5
  EXPECT_EQ(-8, timezone_hour("2023-03-11 12:00:00", "America/Los_Angeles"));
  EXPECT_EQ(-8, timezone_hour("2023-03-12 02:30:00", "America/Los_Angeles"));
  EXPECT_EQ(-7, timezone_hour("2023-03-13 12:00:00", "America/Los_Angeles"));
  EXPECT_EQ(-7, timezone_hour("2023-11-05 01:30:00", "America/Los_Angeles"));
  EXPECT_EQ(-8, timezone_hour("2023-12-05 01:30:00", "America/Los_Angeles"));

  // Different time with same date
  EXPECT_EQ(-4, timezone_hour("2023-01-01 03:20:00", "Canada/Atlantic"));
  EXPECT_EQ(-4, timezone_hour("2023-01-01 10:00:00", "Canada/Atlantic"));

  // By definition (+/-) 00:00 offsets should always return the hour part of the
  // offset itself.
  EXPECT_EQ(0, timezone_hour("2023-12-05 01:30:00", "+00:00"));
  EXPECT_EQ(8, timezone_hour("2023-12-05 01:30:00", "+08:00"));
  EXPECT_EQ(-10, timezone_hour("2023-12-05 01:30:00", "-10:00"));

  // Invalid inputs
  BOLT_ASSERT_THROW(
      timezone_hour("invalid_date", "Canada/Atlantic"),
      "Unable to parse timestamp value: \"invalid_date\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
  BOLT_ASSERT_THROW(
      timezone_hour("123456", "Canada/Atlantic"),
#ifndef SPARK_COMPATIBLE
      "Unable to parse timestamp value: \"123456\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
#else
      "Timestamp with timezone overflow: 3833727840000000 ms");
#endif
  BOLT_ASSERT_THROW(
      timezone_hour("123456-01-01 00:00:00", "Canada/Atlantic"),
      "Timestamp with timezone overflow: 3833727840000000 ms");
}

TEST_F(PrestoSqlDateTimeFunctionsTest, timeZoneMinute) {
  const auto timezone_minute = [&](const char* time, const char* timezone) {
    Timestamp ts = util::fromTimestampString(time, nullptr);
    auto timestamp = ts.toMillis();
    auto minute = evaluateWithTimestampWithTimezone<int64_t>(
                      "timezone_minute(c0)", timestamp, timezone)
                      .value();
    return minute;
  };

  EXPECT_EQ(30, timezone_minute("1970-01-01 03:20:00", "Asia/Kolkata"));
  EXPECT_EQ(0, timezone_minute("1970-01-01 03:20:00", "America/Los_Angeles"));
  EXPECT_EQ(0, timezone_minute("1970-05-01 04:20:00", "America/Los_Angeles"));
  EXPECT_EQ(0, timezone_minute("1970-01-01 03:20:00", "Canada/Atlantic"));
  EXPECT_EQ(30, timezone_minute("1970-01-01 03:20:00", "Asia/Katmandu"));
  EXPECT_EQ(45, timezone_minute("1970-01-01 03:20:00", "Pacific/Chatham"));

  // By definition (+/-) 00:00 offsets should always return the minute part of
  // the offset itself.
  EXPECT_EQ(0, timezone_minute("2023-12-05 01:30:00", "+00:00"));
  EXPECT_EQ(17, timezone_minute("2023-12-05 01:30:00", "+08:17"));
  EXPECT_EQ(-59, timezone_minute("2023-12-05 01:30:00", "-10:59"));

  BOLT_ASSERT_THROW(
      timezone_minute("abc", "Pacific/Chatham"),
      "Unable to parse timestamp value: \"abc\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
  BOLT_ASSERT_THROW(
      timezone_minute("2023-", "Pacific/Chatham"),
      "Unable to parse timestamp value: \"2023-\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
}

TEST_F(PrestoSqlDateTimeFunctionsTest, timestampWithTimezoneComparisons) {
  auto runAndCompare = [&](std::string expr,
                           std::shared_ptr<RowVector>& inputs,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(expr, inputs);
    test::assertEqualVectors(expectedResult, actual);
  };

  RowVectorPtr timestampWithTimezoneLhs = makeTimestampWithTimeZoneVector(
      makeFlatVector<int64_t>({0, 0, 0}),
      makeFlatVector<int16_t>({900, 900, 800}));
  RowVectorPtr timestampWithTimezoneRhs = makeTimestampWithTimeZoneVector(
      makeFlatVector<int64_t>({0, 1000, 0}),
      makeFlatVector<int16_t>({900, 900, 900}));
  auto inputs =
      makeRowVector({timestampWithTimezoneLhs, timestampWithTimezoneRhs});

  auto expectedEq = makeNullableFlatVector<bool>({true, false, false});
  runAndCompare("c0 = c1", inputs, expectedEq);

  auto expectedNeq = makeNullableFlatVector<bool>({false, true, true});
  runAndCompare("c0 != c1", inputs, expectedNeq);

  auto expectedLt = makeNullableFlatVector<bool>({false, true, false});
  runAndCompare("c0 < c1", inputs, expectedLt);

  auto expectedGt = makeNullableFlatVector<bool>({false, false, true});
  runAndCompare("c0 > c1", inputs, expectedGt);

  auto expectedLte = makeNullableFlatVector<bool>({true, true, false});
  runAndCompare("c0 <= c1", inputs, expectedLte);

  auto expectedGte = makeNullableFlatVector<bool>({true, false, true});
  runAndCompare("c0 >= c1", inputs, expectedGte);
}

TEST_F(PrestoSqlDateTimeFunctionsTest, castDateToTimestamp) {
  const int64_t kSecondsInDay = kMillisInDay / 1'000;
  const auto castDateToTimestamp = [&](const std::optional<int32_t> date) {
    return evaluateOnce<Timestamp, int32_t>(
        "cast(c0 AS timestamp)", {date}, {DATE()});
  };

  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));

  const auto tz = "America/Los_Angeles";
  const auto kTimezoneOffset = 8 * kMillisInHour / 1'000;
  setQueryTimeZone(tz);
  EXPECT_EQ(
      Timestamp(kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));

  disableAdjustTimestampToTimezone();
  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, lastDayOfMonthDate) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        "last_day_of_month(c0)", {date}, {DATE()});
  };

  const auto lastDay = [&](const StringView& dateStr) {
    return lastDayFunc(DATE()->toDays(dateStr));
  };

  EXPECT_EQ(std::nullopt, lastDayFunc(std::nullopt));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01"));
  EXPECT_EQ(parseDate("2008-02-29"), lastDay("2008-02-01"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01"));
  EXPECT_EQ(parseDate("2023-03-31"), lastDay("2023-03-11"));
  EXPECT_EQ(parseDate("2023-04-30"), lastDay("2023-04-21"));
  EXPECT_EQ(parseDate("2023-05-31"), lastDay("2023-05-09"));
  EXPECT_EQ(parseDate("2023-06-30"), lastDay("2023-06-01"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-31"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-31"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-11"));
  EXPECT_EQ(parseDate("2023-08-31"), lastDay("2023-08-01"));
  EXPECT_EQ(parseDate("2023-09-30"), lastDay("2023-09-09"));
  EXPECT_EQ(parseDate("2023-10-31"), lastDay("2023-10-01"));
  EXPECT_EQ(parseDate("2023-11-30"), lastDay("2023-11-11"));
  EXPECT_EQ(parseDate("2023-12-31"), lastDay("2023-12-12"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, lastDayOfMonthTimestamp) {
  const auto lastDayFunc = [&](const std::optional<Timestamp>& date) {
    return evaluateOnce<int32_t>("last_day_of_month(c0)", date);
  };

  const auto lastDay = [&](const StringView& dateStr) {
    return lastDayFunc(util::fromTimestampString(dateStr, nullptr));
  };

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, lastDayFunc(std::nullopt));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01 20:23:00.007"));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01 12:00:00.001"));
  EXPECT_EQ(parseDate("2008-02-29"), lastDay("2008-02-01 12:00:00"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01 23:59:59.999"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01 12:00:00"));
  EXPECT_EQ(parseDate("2023-03-31"), lastDay("2023-03-11 12:00:00"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, lastDayOfMonthTimestampWithTimezone) {
  EXPECT_EQ(
      parseDate("1970-01-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      parseDate("1969-12-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 0, "-02:00"));
  EXPECT_EQ(
      parseDate("2008-02-29"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 1201881600000, "+02:00"));
  EXPECT_EQ(
      parseDate("2008-01-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 1201795200000, "-02:00"));
}

TEST_F(PrestoSqlDateTimeFunctionsTest, monthsBetween) {
  const auto monthsBetween = [&](std::optional<Timestamp> timestamp1,
                                 std::optional<Timestamp> timestamp2) {
    return evaluateOnce<double>(
        "months_between(c0, c1)", timestamp1, timestamp2);
  };

  using util::fromTimestampString;

  // Simple tests
  EXPECT_FLOAT_EQ(
      -13.0,
      monthsBetween(
          fromTimestampString("2019-02-28 10:00:00.500", nullptr),
          fromTimestampString("2020-03-28 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      14.06451613,
      monthsBetween(
          fromTimestampString("2021-05-30 10:00:00.500", nullptr),
          fromTimestampString("2020-03-28 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      0.03225806,
      monthsBetween(
          fromTimestampString("2024-05-30 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      0.0,
      monthsBetween(
          fromTimestampString("2024-05-29 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      -0.03225806,
      monthsBetween(
          fromTimestampString("2024-05-28 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      0.96774194,
      monthsBetween(
          fromTimestampString("2024-06-28 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      1.0,
      monthsBetween(
          fromTimestampString("2024-06-29 10:00:00.500", nullptr),
          fromTimestampString("2024-05-29 10:00:00.500", nullptr))
          .value());

  // Test for daylight saving. Daylight saving in US starts at 2021-03-14
  // 02:00:00 PST.
  // When adjust_timestamp_to_timezone is off, Daylight saving occurs in UTC
  EXPECT_FLOAT_EQ(
      -1.0,
      monthsBetween(
          fromTimestampString("2021-03-14 01:00:00.000", nullptr),
          fromTimestampString("2021-04-14 01:00:00.000", nullptr))
          .value());

  setQueryTimeZone("Asia/Shanghai");
  EXPECT_FLOAT_EQ(
      4.12903226,
      monthsBetween(
          Timestamp(1677600000, 0), // 2023-03-01 00:00:00.000000
          Timestamp(1666886400, 0)) // 2022-10-28
          .value());

  // When adjust_timestamp_to_timezone is on, respect Daylight saving in the
  // session time zone
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_FLOAT_EQ(
      -1.0,
      monthsBetween(
          fromTimestampString("2021-03-14 09:00:00.000", nullptr),
          fromTimestampString("2021-04-14 09:00:00.000", nullptr))
          .value());
  // Test for respecting the last day of a year-month
  EXPECT_FLOAT_EQ(
      -12.96774194,
      monthsBetween(
          fromTimestampString("2019-01-30 10:00:00.500", nullptr),
          fromTimestampString("2020-02-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      -13.0,
      monthsBetween(
          fromTimestampString("2019-01-31 10:00:00.500", nullptr),
          fromTimestampString("2020-02-29 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      -12.90322581,
      monthsBetween(
          fromTimestampString("2019-01-31 10:00:00.500", nullptr),
          fromTimestampString("2020-02-28 10:00:00.500", nullptr))
          .value());

  EXPECT_FLOAT_EQ(
      12.0,
      monthsBetween(
          fromTimestampString("2020-02-29 10:00:00.500", nullptr),
          fromTimestampString("2019-02-28 10:00:00.500", nullptr))
          .value());
}

#ifndef SPARK_COMPATIBLE
TEST_F(PrestoSqlDateTimeFunctionsTest, lastDay) {
  const auto lastDayFunc = [&](const std::optional<int32_t>& date) {
    return evaluateOnce<std::string, int32_t>("last_day(c0)", {date}, {DATE()});
  };

  const auto lastDay = [&](const std::string& dateStr) {
    return lastDayFunc(DATE()->toDays(dateStr));
  };

  EXPECT_EQ(lastDay("2015-02-28"), "2015-02-28");
  EXPECT_EQ(lastDay("2015-03-27"), "2015-03-31");
  EXPECT_EQ(lastDay("2015-04-26"), "2015-04-30");
  EXPECT_EQ(lastDay("2015-05-25"), "2015-05-31");
  EXPECT_EQ(lastDay("2015-06-24"), "2015-06-30");
  EXPECT_EQ(lastDay("2015-07-23"), "2015-07-31");
  EXPECT_EQ(lastDay("2015-08-01"), "2015-08-31");
  EXPECT_EQ(lastDay("2015-09-02"), "2015-09-30");
  EXPECT_EQ(lastDay("2015-10-03"), "2015-10-31");
  EXPECT_EQ(lastDay("2015-11-04"), "2015-11-30");
  EXPECT_EQ(lastDay("2015-12-05"), "2015-12-31");
  EXPECT_EQ(lastDay("2016-01-06"), "2016-01-31");
  EXPECT_EQ(lastDay("2016-02-07"), "2016-02-29");
  EXPECT_EQ(lastDayFunc(std::nullopt), std::nullopt);
}
#endif
