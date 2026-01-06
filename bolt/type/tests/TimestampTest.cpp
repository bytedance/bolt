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

#include <date/tz.h>
#include <gtest/gtest.h>
#include <chrono>
#include <random>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"

namespace bytedance::bolt {
namespace {

std::string timestampToString(
    Timestamp ts,
    const TimestampToStringOptions& options) {
  std::tm tm;
  Timestamp::epochToUtc(ts.getSeconds(), tm);
  return Timestamp::tmToString(tm, ts.getNanos(), options);
}

TEST(TimestampTest, fromDaysAndNanos) {
  EXPECT_EQ(
      Timestamp(Timestamp::kSecondsInDay + 2, 1),
      Timestamp::fromDaysAndNanos(
          Timestamp::kJulianToUnixEpochDays + 1,
          2 * Timestamp::kNanosInSecond + 1));
  EXPECT_EQ(
      Timestamp(Timestamp::kSecondsInDay + 2, 0),
      Timestamp::fromDaysAndNanos(
          Timestamp::kJulianToUnixEpochDays + 1,
          2 * Timestamp::kNanosInSecond));
  EXPECT_EQ(
      Timestamp(
          Timestamp::kSecondsInDay * 5 - 3, Timestamp::kNanosInSecond - 6),
      Timestamp::fromDaysAndNanos(
          Timestamp::kJulianToUnixEpochDays + 5,
          -2 * Timestamp::kNanosInSecond - 6));
  EXPECT_EQ(
      Timestamp(Timestamp::kSecondsInDay * 5 - 2, 0),
      Timestamp::fromDaysAndNanos(
          Timestamp::kJulianToUnixEpochDays + 5,
          -2 * Timestamp::kNanosInSecond));
}

TEST(TimestampTest, fromMillisAndMicros) {
  int64_t positiveSecond = 10'000;
  int64_t negativeSecond = -10'000;
  uint64_t nano = 123 * 1'000'000;

  Timestamp ts1(positiveSecond, nano);
  int64_t positiveMillis = positiveSecond * 1'000 + nano / 1'000'000;
  int64_t positiveMicros = positiveSecond * 1'000'000 + nano / 1000;
  EXPECT_EQ(ts1, Timestamp::fromMillis(positiveMillis));
  EXPECT_EQ(ts1, Timestamp::fromMicros(positiveMicros));
  EXPECT_EQ(ts1, Timestamp::fromMillis(ts1.toMillis()));
  EXPECT_EQ(ts1, Timestamp::fromMicros(ts1.toMicros()));

  Timestamp ts2(negativeSecond, nano);
  int64_t negativeMillis = negativeSecond * 1'000 + nano / 1'000'000;
  int64_t negativeMicros = negativeSecond * 1'000'000 + nano / 1000;
  EXPECT_EQ(ts2, Timestamp::fromMillis(negativeMillis));
  EXPECT_EQ(ts2, Timestamp::fromMicros(negativeMicros));
  EXPECT_EQ(ts2, Timestamp::fromMillis(ts2.toMillis()));
  EXPECT_EQ(ts2, Timestamp::fromMicros(ts2.toMicros()));

  Timestamp ts3(negativeSecond, 0);
  EXPECT_EQ(ts3, Timestamp::fromMillis(negativeSecond * 1'000));
  EXPECT_EQ(ts3, Timestamp::fromMicros(negativeSecond * 1'000'000));
  EXPECT_EQ(ts3, Timestamp::fromMillis(ts3.toMillis()));
  EXPECT_EQ(ts3, Timestamp::fromMicros(ts3.toMicros()));
}

TEST(TimestampTest, fromNanos) {
  int64_t positiveSecond = 10'000;
  int64_t negativeSecond = -10'000;
  uint64_t nano = 123'456'789;

  Timestamp ts1(positiveSecond, nano);
  int64_t positiveNanos = positiveSecond * 1'000'000'000 + nano;
  EXPECT_EQ(ts1, Timestamp::fromNanos(positiveNanos));
  EXPECT_EQ(ts1, Timestamp::fromNanos(ts1.toNanos()));

  Timestamp ts2(negativeSecond, nano);
  int64_t negativeNanos = negativeSecond * 1'000'000'000 + nano;
  EXPECT_EQ(ts2, Timestamp::fromNanos(negativeNanos));
  EXPECT_EQ(ts2, Timestamp::fromNanos(ts2.toNanos()));

  Timestamp ts3(negativeSecond, 0);
  EXPECT_EQ(ts3, Timestamp::fromNanos(negativeSecond * 1'000'000'000));
  EXPECT_EQ(ts3, Timestamp::fromNanos(ts3.toNanos()));
}

TEST(TimestampTest, arithmeticOverflow) {
  int64_t positiveSecond = Timestamp::kMaxSeconds;
  int64_t negativeSecond = Timestamp::kMinSeconds;
  uint64_t nano = Timestamp::kMaxNanos;

  Timestamp ts1(positiveSecond, nano);
  BOLT_ASSERT_THROW(
      ts1.toMillis(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          positiveSecond,
          nano));
  BOLT_ASSERT_THROW(
      ts1.toMicros(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to microseconds",
          positiveSecond,
          nano));
  BOLT_ASSERT_THROW(
      ts1.toNanos(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to nanoseconds",
          positiveSecond,
          nano));

  Timestamp ts2(negativeSecond, 0);
  BOLT_ASSERT_THROW(
      ts2.toMillis(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          negativeSecond,
          0));
  BOLT_ASSERT_THROW(
      ts2.toMicros(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to microseconds",
          negativeSecond,
          0));
  BOLT_ASSERT_THROW(
      ts2.toNanos(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to nanoseconds",
          negativeSecond,
          0));
  ASSERT_NO_THROW(Timestamp::minMillis().toMillis());
  ASSERT_NO_THROW(Timestamp::maxMillis().toMillis());
  ASSERT_NO_THROW(Timestamp(-9223372036855, 224'192'000).toMicros());
  ASSERT_NO_THROW(Timestamp(9223372036854, 775'807'000).toMicros());
}

TEST(TimestampTest, toAppend) {
  std::string tsStringZeroValue;
  toAppend(Timestamp(0, 0), &tsStringZeroValue);
  EXPECT_EQ("1970-01-01 00:00:00.000000000", tsStringZeroValue);

  std::string tsStringCommonValue;
  toAppend(Timestamp(946729316, 0), &tsStringCommonValue);
  EXPECT_EQ("2000-01-01 12:21:56.000000000", tsStringCommonValue);

  std::string tsStringFarInFuture;
  toAppend(Timestamp(94668480000, 0), &tsStringFarInFuture);
  EXPECT_EQ("4969-12-04 00:00:00.000000000", tsStringFarInFuture);

  std::string tsStringWithNanos;
  toAppend(Timestamp(946729316, 123), &tsStringWithNanos);
  EXPECT_EQ("2000-01-01 12:21:56.000000123", tsStringWithNanos);

  EXPECT_EQ(
      "2000-01-01 00:00:00.000000000",
      folly::to<std::string>(Timestamp(946684800, 0)));
  EXPECT_EQ(
      "2000-01-01 12:21:56.000000123",
      folly::to<std::string>(Timestamp(946729316, 123)));
  EXPECT_EQ(
      "1970-01-01 02:01:06.000000000",
      folly::to<std::string>(Timestamp(7266, 0)));
  EXPECT_EQ(
      "2000-01-01 12:21:56.129900000",
      folly::to<std::string>(Timestamp(946729316, 129900000)));
}

TEST(TimestampTest, now) {
  using namespace std::chrono;

  auto now = Timestamp::now();

  auto expectedEpochSecs =
      duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  auto expectedEpochMs =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();

  EXPECT_GE(expectedEpochSecs, now.getSeconds());
  EXPECT_GE(expectedEpochMs, now.toMillis());
}

DEBUG_ONLY_TEST(TimestampTest, invalidInput) {
  constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();
  constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
  constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();
  // Seconds invalid range.
  BOLT_ASSERT_THROW(Timestamp(kInt64Min, 1), "Timestamp seconds out of range");
  BOLT_ASSERT_THROW(Timestamp(kInt64Max, 1), "Timestamp seconds out of range");
  BOLT_ASSERT_THROW(
      Timestamp(Timestamp::kMinSeconds - 1, 1),
      "Timestamp seconds out of range");
  BOLT_ASSERT_THROW(
      Timestamp(Timestamp::kMaxSeconds + 1, 1),
      "Timestamp seconds out of range");

  // Nanos invalid range.
  BOLT_ASSERT_THROW(Timestamp(1, kUint64Max), "Timestamp nanos out of range");
  BOLT_ASSERT_THROW(
      Timestamp(1, Timestamp::kMaxNanos + 1), "Timestamp nanos out of range");
}

TEST(TimestampTest, toString) {
  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  EXPECT_EQ("-292275055-05-16 16:47:04.000000000", kMin.toString());
  EXPECT_EQ("292278994-08-17 07:12:55.999999999", kMax.toString());
  EXPECT_EQ(
      "1-01-01 05:17:32.000000000", Timestamp(-62135577748, 0).toString());
  EXPECT_EQ(
      "-224876953-12-19 16:58:03.000000000",
      Timestamp(-7096493348463717, 0).toString());
  EXPECT_EQ(
      "-1-11-29 19:33:20.000000000", Timestamp(-62170000000, 0).toString());
}

TEST(TimestampTest, toStringPrestoCastBehavior) {
  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMilliseconds,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };
  EXPECT_EQ("-292275055-05-16 16:47:04.000", kMin.toString(options));
  EXPECT_EQ("292278994-08-17 07:12:55.999", kMax.toString(options));
  EXPECT_EQ(
      "0001-01-01 05:17:32.000", Timestamp(-62135577748, 0).toString(options));
  EXPECT_EQ(
      "0000-03-24 13:20:00.000", Timestamp(-62160000000, 0).toString(options));
  EXPECT_EQ(
      "-224876953-12-19 16:58:03.000",
      Timestamp(-7096493348463717, 0).toString(options));
  EXPECT_EQ(
      "-0001-11-29 19:33:20.000", Timestamp(-62170000000, 0).toString(options));
}

namespace {
std::string toStringAlt(
    const Timestamp& t,
    TimestampToStringOptions::Precision precision) {
  auto seconds = t.getSeconds();
  std::tm tmValue;
  BOLT_CHECK_NOT_NULL(gmtime_r((const time_t*)&seconds, &tmValue));
  auto width = static_cast<int>(precision);
  auto value = precision == TimestampToStringOptions::Precision::kMilliseconds
      ? t.getNanos() / 1'000'000
      : t.getNanos();
  std::ostringstream oss;
  oss << std::put_time(&tmValue, "%F %T");
  oss << '.' << std::setfill('0') << std::setw(width) << value;
  return oss.str();
}
} // namespace

TEST(TimestampTest, compareWithToStringAlt) {
  uint64_t seed = 42;
  // seed = std::random_device{}();
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int64_t> distSec(
      Timestamp::kMinSeconds, Timestamp::kMaxSeconds);
  std::uniform_int_distribution<uint64_t> distNano(0, Timestamp::kMaxNanos);
  for (int i = 0; i < 10'000; ++i) {
    Timestamp t(distSec(gen), distNano(gen));
    for (auto precision :
         {TimestampToStringOptions::Precision::kMilliseconds,
          TimestampToStringOptions::Precision::kNanoseconds}) {
      TimestampToStringOptions options{};
      options.precision = precision;
      ASSERT_EQ(t.toString(options), toStringAlt(t, precision))
          << t.getSeconds() << ' ' << t.getNanos();
    }
  }
}

TEST(TimestampTest, CivilDateTimeMatchesChrono) {
  // Keep samples within a safe std::chrono range to avoid overflow in tests.
  const int64_t kSecondsPerYear = 365 * Timestamp::kSecondsInDay;
  std::vector<Timestamp> samples = {
      Timestamp(0, 0),
      Timestamp(1, 0),
      Timestamp(-1, Timestamp::kMaxNanos),
      Timestamp(kSecondsPerYear * 10, 123456789),
      Timestamp(-kSecondsPerYear * 5, 987654321),
      Timestamp(1704067200, 999999999), // 2024-01-01 00:00:00.999...
  };

  std::default_random_engine gen(42);
  std::uniform_int_distribution<int64_t> distSec(
      -50 * kSecondsPerYear, 50 * kSecondsPerYear);
  std::uniform_int_distribution<uint64_t> distNano(0, Timestamp::kMaxNanos);
  for (int i = 0; i < 200; ++i) {
    samples.emplace_back(distSec(gen), distNano(gen));
  }

  for (const auto& ts : samples) {
    const auto civil = util::toCivilDateTime(ts, true, true);

    auto chronoTimePoint = std::chrono::system_clock::time_point(
        std::chrono::seconds(ts.getSeconds()) +
        std::chrono::nanoseconds(ts.getNanos()));
    auto days = ::date::floor<::date::days>(chronoTimePoint);
    auto timeOfDay =
        ::date::make_time(std::chrono::duration_cast<std::chrono::nanoseconds>(
            chronoTimePoint - days));
    const ::date::year_month_day ymd(days);
    const ::date::weekday weekday(days);
    auto firstDayOfYear = ::date::sys_days(
        ::date::year_month_day(ymd.year(), ::date::month(1), ::date::day(1)));
    auto dayOfYear = static_cast<int32_t>((days - firstDayOfYear).count() + 1);

    EXPECT_EQ(static_cast<int>(ymd.year()), civil.date.year);
    EXPECT_EQ(static_cast<unsigned>(ymd.month()), civil.date.month);
    EXPECT_EQ(static_cast<unsigned>(ymd.day()), civil.date.day);
    EXPECT_EQ(weekday.c_encoding(), civil.weekday);
    EXPECT_EQ(dayOfYear, civil.dayOfYear);

    EXPECT_EQ(timeOfDay.hours().count(), civil.time.hour);
    EXPECT_EQ(timeOfDay.minutes().count(), civil.time.minute);
    EXPECT_EQ(timeOfDay.seconds().count(), civil.time.second);
    EXPECT_EQ(
        timeOfDay.subseconds().count(),
        static_cast<uint64_t>(civil.time.nanosecond));
  }
}

TEST(TimestampTest, increaseOperator) {
  auto ts = Timestamp(0, 999999998);
  EXPECT_EQ("1970-01-01 00:00:00.999999998", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01 00:00:00.999999999", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01 00:00:01.000000000", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01 00:00:01.000000001", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01 00:00:01.000000002", ts.toString());

  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  BOLT_ASSERT_THROW(++kMax, "Timestamp nanos out of range");
}

TEST(TimestampTest, decreaseOperator) {
  auto ts = Timestamp(0, 2);
  EXPECT_EQ("1970-01-01 00:00:00.000000002", ts.toString());
  --ts;
  EXPECT_EQ("1970-01-01 00:00:00.000000001", ts.toString());
  --ts;
  EXPECT_EQ("1970-01-01 00:00:00.000000000", ts.toString());
  --ts;
  EXPECT_EQ("1969-12-31 23:59:59.999999999", ts.toString());
  --ts;
  EXPECT_EQ("1969-12-31 23:59:59.999999998", ts.toString());

  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  BOLT_ASSERT_THROW(--kMin, "Timestamp nanos out of range");
}

TEST(TimestampTest, outOfRange) {
  auto* timezone = tz::locateZone("GMT");
  Timestamp t(-3217830796800, 0);

  auto copy = t;
  ASSERT_NO_THROW(copy.toTimezone(*timezone));
  EXPECT_EQ(copy.getSeconds(), t.getSeconds());
  copy = t;
  ASSERT_NO_THROW(copy.toGMT(*timezone));
  EXPECT_EQ(copy.getSeconds(), t.getSeconds());

  copy = t;
  auto* shanghai = tz::locateZone("Asia/Shanghai");
  const auto info = shanghai->tz()->get_info(
      ::date::sys_seconds{std::chrono::seconds{t.getSeconds()}});
  ASSERT_NO_THROW(copy.toTimezone(*shanghai));
  EXPECT_EQ(copy.getSeconds(), t.getSeconds() + info.offset.count());
}

// In debug mode, Timestamp constructor will throw exception if range check
// fails.
#ifdef NDEBUG
TEST(TimestampTest, overflow) {
  Timestamp t(std::numeric_limits<int64_t>::max(), 0);
  BOLT_ASSERT_THROW(
      t.toMillis(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          std::numeric_limits<int64_t>::max(),
          0));
  ASSERT_NO_THROW(t.toMillisAllowOverflow());
}
#endif

void checkTm(const std::tm& actual, const std::tm& expected) {
  ASSERT_EQ(expected.tm_year, actual.tm_year);
  ASSERT_EQ(expected.tm_yday, actual.tm_yday);
  ASSERT_EQ(expected.tm_mon, actual.tm_mon);
  ASSERT_EQ(expected.tm_mday, actual.tm_mday);
  ASSERT_EQ(expected.tm_wday, actual.tm_wday);
  ASSERT_EQ(expected.tm_hour, actual.tm_hour);
  ASSERT_EQ(expected.tm_min, actual.tm_min);
  ASSERT_EQ(expected.tm_sec, actual.tm_sec);
}

std::string tmToString(
    const std::tm& tmValue,
    uint64_t nanos,
    const std::string& format,
    const TimestampToStringOptions& options) {
  auto width = static_cast<int>(options.precision);
  auto value =
      options.precision == TimestampToStringOptions::Precision::kMilliseconds
      ? nanos / 1'000'000
      : nanos;

  std::ostringstream oss;
  oss << std::put_time(&tmValue, format.c_str());

  if (options.mode != TimestampToStringOptions::Mode::kDateOnly) {
    oss << '.' << std::setfill('0') << std::setw(width) << value;
  }

  return oss.str();
}

TEST(TimestampTest, epochToUtc) {
  std::tm tm{};
  ASSERT_FALSE(Timestamp::epochToUtc(-(1ll << 60), tm));
  ASSERT_FALSE(Timestamp::epochToUtc(1ll << 60, tm));
}

TEST(TimestampTest, randomEpochToUtc) {
  uint64_t seed = 42;
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<time_t> dist(
      std::numeric_limits<time_t>::min(), std::numeric_limits<time_t>::max());
  std::tm actual{};
  std::tm expected{};
  for (int i = 0; i < 10'000; ++i) {
    auto epoch = dist(gen);
    SCOPED_TRACE(fmt::format("epoch={}", epoch));
    if (gmtime_r(&epoch, &expected)) {
      ASSERT_TRUE(Timestamp::epochToUtc(epoch, actual));
      checkTm(actual, expected);
    } else {
      ASSERT_FALSE(Timestamp::epochToUtc(epoch, actual));
    }
  }
}

void testTmToString(
    const std::string& format,
    const TimestampToStringOptions::Mode mode) {
  uint64_t seed = 42;
  std::default_random_engine gen(seed);

  std::uniform_int_distribution<time_t> dist(
      std::numeric_limits<time_t>::min(), std::numeric_limits<time_t>::max());
  std::uniform_int_distribution<int> nanosDist(0, Timestamp::kMaxNanos);

  std::tm actual{};
  std::tm expected{};

  TimestampToStringOptions options;
  options.mode = mode;

  const std::vector<TimestampToStringOptions::Precision> precisions = {
      TimestampToStringOptions::Precision::kMilliseconds,
      TimestampToStringOptions::Precision::kNanoseconds};

  for (auto precision : precisions) {
    options.precision = precision;
    for (int i = 0; i < 10'000; ++i) {
      auto epoch = dist(gen);
      auto nanos = nanosDist(gen);
      SCOPED_TRACE(fmt::format(
          "epoch={}, nanos={}, mode={}, precision={}",
          epoch,
          nanos,
          mode,
          precision));
      if (gmtime_r(&epoch, &expected)) {
        ASSERT_TRUE(Timestamp::epochToUtc(epoch, actual));
        checkTm(actual, expected);

        auto actualString = Timestamp::tmToString(actual, nanos, options);
        auto expectedString = tmToString(expected, nanos, format, options);
        ASSERT_EQ(expectedString, actualString);

      } else {
        ASSERT_FALSE(Timestamp::epochToUtc(epoch, actual));
      }
    }
  }
}

TEST(TimestampTest, tmToStringDateOnly) {
  // %F - equivalent to "%Y-%m-%d" (the ISO 8601 date format)
  testTmToString("%F", TimestampToStringOptions::Mode::kDateOnly);
}

TEST(TimestampTest, tmToStringTimeOnly) {
  // %T - equivalent to "%H:%M:%S" (the ISO 8601 time format)
  testTmToString("%T", TimestampToStringOptions::Mode::kTimeOnly);
}

TEST(TimestampTest, tmToStringTimestamp) {
  // %FT%T - equivalent to "%Y-%m-%dT%H:%M:%S" (the ISO 8601 timestamp format)
  testTmToString("%F %T", TimestampToStringOptions::Mode::kFull);
}

TEST(TimestampTest, leadingPositiveSign) {
  TimestampToStringOptions options = {
      .leadingPositiveSign = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };

  ASSERT_EQ(
      timestampToString(Timestamp(253402231016, 0), options),
      "9999-12-31 04:36:56.000000000");
  ASSERT_EQ(
      timestampToString(Timestamp(253405036800, 0), options),
      "+10000-02-01 16:00:00.000000000");
}

TEST(TimestampTest, toTimezoneLargeYear) {
  // 1'764'593'923'251 seconds is about year 57887; std::chrono time_point
  // rejects it, so verify timezone adjustment works without time_point.
  Timestamp ts(1'764'593'923'251, 0);

  auto copy = ts;
  copy.toTimezone(*tz::locateZone("UTC"));
  EXPECT_EQ(copy, ts);

  copy = ts;
  copy.toTimezone(*tz::locateZone("Asia/Shanghai"));
  EXPECT_EQ(copy.getSeconds(), ts.getSeconds() + 8 * 3600);
}

TEST(TimestampTest, skipTrailingZeros) {
  TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };

  ASSERT_EQ(
      timestampToString(Timestamp(-946684800, 0), options),
      "1940-01-02 00:00:00");
  ASSERT_EQ(timestampToString(Timestamp(0, 0), options), "1970-01-01 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(0, 365), options), "1970-01-01 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(0, 65873), options),
      "1970-01-01 00:00:00.000065");
  ASSERT_EQ(
      timestampToString(Timestamp(94668480000, 0), options),
      "4969-12-04 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129999999), options),
      "2000-01-01 12:21:56.129999");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129990000), options),
      "2000-01-01 12:21:56.12999");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129900000), options),
      "2000-01-01 12:21:56.1299");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129000000), options),
      "2000-01-01 12:21:56.129");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129010000), options),
      "2000-01-01 12:21:56.12901");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129001000), options),
      "2000-01-01 12:21:56.129001");
  ASSERT_EQ(
      timestampToString(Timestamp(-50049331200, 726600000), options),
      "0384-01-01 08:00:00.7266");
}

} // namespace
} // namespace bytedance::bolt
