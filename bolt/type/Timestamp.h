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

#pragma once

#include <iomanip>
#include <sstream>
#include <string>
#include <type_traits>

#include <date/date.h>
#include <date/tz.h>
#include <folly/dynamic.h>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/type/StringView.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt {

namespace date {
class time_zone;
}

namespace {

// Assuming tzID is in [1, 1680] range.
// tzID - PrestoDB time zone ID.
inline int64_t getPrestoTZOffsetInSeconds(int16_t tzID) {
  // TODO(spershin): Maybe we need something better if we can (we could use
  //  precomputed vector for PrestoDB timezones, for instance).

  // PrestoDb time zone ids require some custom code.
  // Mapping is 1-based and covers [-14:00, +14:00] range without 00:00.
  return ((tzID <= 840) ? (tzID - 841) : (tzID - 840)) * 60;
}

constexpr int julianGregLength = 20;

constexpr int64_t julianGregDiffSwitchDay[julianGregLength] = {
    -1048195, -975145, -938620, -902095, -829045, -792520, -755995,
    -682945,  -646420, -609895, -536845, -500320, -463795, -390745,
    -354220,  -317695, -244645, -208120, -171595, -141427};

constexpr int64_t julianGregDiffs[julianGregLength] = {
    8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0};

} // namespace

enum class TimestampPrecision : int8_t {
  kMilliseconds = 3, // 10^3 milliseconds are equal to one second.
  kMicroseconds = 6, // 10^6 microseconds are equal to one second.
  kNanoseconds = 9, // 10^9 nanoseconds are equal to one second.
};

struct TimestampToStringOptions {
  using Precision = TimestampPrecision;

  Precision precision = Precision::kNanoseconds;

  // Whether to add a leading '+' when year is greater than 9999.
  bool leadingPositiveSign = false;

  /// Whether to skip trailing zeros of fractional part. E.g. when true,
  /// '2000-01-01 12:21:56.129000' becomes '2000-01-01 12:21:56.129'.
  bool skipTrailingZeros = false;

  /// Whether padding zeros are added when the digits of year is less than 4.
  /// E.g. when true, '1-01-01 05:17:32.000' becomes '0001-01-01 05:17:32.000',
  /// '-03-24 13:20:00.000' becomes '0000-03-24 13:20:00.000', and '-1-11-29
  /// 19:33:20.000' becomes '-0001-11-29 19:33:20.000'.
  bool zeroPaddingYear = false;
  char dateTimeSeparator = ' ';
  bool dateOnly = false;

  enum class Mode : int8_t {
    /// ISO 8601 timestamp format: %Y-%m-%dT%H:%M:%S.nnnnnnnnn for nanoseconds
    /// precision; %Y-%m-%dT%H:%M:%S.nnn for milliseconds precision.
    kFull,
    /// ISO 8601 date format: %Y-%m-%d.
    kDateOnly,
    /// ISO 8601 time format: %H:%M:%S.nnnnnnnnn for nanoseconds precision,
    /// or %H:%M:%S.nnn for milliseconds precision.
    kTimeOnly,
  };

  Mode mode = Mode::kFull;
};

/// Returns the max length of a converted string from timestamp.
std::string::size_type getMaxStringLength(
    const TimestampToStringOptions& options);

// Our own version of gmtime_r to avoid expensive calls to __tz_convert.  This
// might not be very significant in micro benchmark, but is causing significant
// context switching cost in real world queries with higher concurrency (71% of
// time is on __tz_convert for some queries).
//
// Return whether the epoch second can be converted to a valid std::tm.
bool epochToUtc(int64_t seconds, std::tm& out);

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
T grebaseJulianToGregorianDays(T days) {
  int begin = 0;
  int end = julianGregLength - 1;
  int out = 0;
  while (begin <= end) {
    int mid = (begin + end) >> 1;
    if (days >= julianGregDiffSwitchDay[mid]) {
      out = mid;
      begin = mid + 1;
    } else {
      end = mid - 1;
    }
  }
  return days + julianGregDiffs[out];
}

std::string
tmToString(const std::tm&, int nanos, const TimestampToStringOptions&);

struct Timestamp {
 public:
  static constexpr int64_t kMillisecondsInSecond = 1'000;
  static constexpr int64_t kMicrosecondsInMillisecond = 1'000;
  static constexpr int64_t kNanosecondsInMicrosecond = 1'000;
  static constexpr int64_t kNanosecondsInMillisecond = 1'000'000;
  static constexpr int64_t kNanosInSecond =
      kNanosecondsInMillisecond * kMillisecondsInSecond;
  // The number of days between the Julian epoch and the Unix epoch.
  static constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
  static constexpr int64_t kSecondsInDay = 86400LL;

  static constexpr int64_t kMicrosecondsInSecond =
      kMicrosecondsInMillisecond * kMillisecondsInSecond;

  // Limit the range of seconds to avoid some problems. Seconds should be
  // in the range [INT64_MIN/1000 - 1, INT64_MAX/1000].
  // Presto's Timestamp is stored in one 64-bit signed integer for
  // milliseconds, this range ensures that Timestamp's range in Bolt will not
  // be smaller than Presto, and can make Timestamp::toString work correctly.
  static constexpr int64_t kMaxSeconds =
      std::numeric_limits<int64_t>::max() / kMillisecondsInSecond;
  static constexpr int64_t kMinSeconds =
      std::numeric_limits<int64_t>::min() / kMillisecondsInSecond - 1;

  // Nanoseconds should be less than 1 second.
  static constexpr uint64_t kMaxNanos = 999'999'999;

  constexpr Timestamp() : seconds_(0), nanos_(0) {}

  Timestamp(int64_t seconds, uint64_t nanos)
      : seconds_(seconds), nanos_(nanos) {
    BOLT_USER_DCHECK_GE(seconds, kMinSeconds, "Timestamp seconds out of range");
    BOLT_USER_DCHECK_LE(seconds, kMaxSeconds, "Timestamp seconds out of range");
    BOLT_USER_DCHECK_LE(nanos, kMaxNanos, "Timestamp nanos out of range");
  }

  /// Creates a timestamp from the number of days since the Julian epoch
  /// and the number of nanoseconds.
  static Timestamp fromDaysAndNanos(int32_t days, int64_t nanos);

  static Timestamp fromMicrosNoError(int64_t micros)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    if (micros >= 0 || micros % 1'000'000 == 0) {
      return Timestamp(micros / 1'000'000, (micros % 1'000'000) * 1'000);
    }
    auto second = micros / 1'000'000 - 1;
    auto nano = ((micros - second * 1'000'000) % 1'000'000) * 1'000;
    return Timestamp(second, nano);
  }

  // Returns the current unix timestamp (ms precision).
  static Timestamp now();

  static Timestamp create(const folly::dynamic& obj) {
    auto seconds = obj["seconds"].asInt();
    auto nanos = obj["nanos"].asInt();
    return Timestamp(seconds, nanos);
  }

  int64_t getSeconds() const {
    return seconds_;
  }

  uint64_t getNanos() const {
    return nanos_;
  }

  int64_t getDays() const {
    int64_t days = seconds_ / kSecondsInDay;
    if (seconds_ % kSecondsInDay < 0) {
      days--;
    }
    return days;
  }

  uint64_t getNanosInDay() const {
    int64_t secondsInDay = seconds_ % kSecondsInDay;
    if (secondsInDay < 0) {
      secondsInDay += kSecondsInDay;
    }
    return nanos_ + kNanosInSecond * secondsInDay;
  }

  // Keep it in header for getting inlined.
  int64_t toNanos() const {
    // int64 can store around 292 years in nanos ~ till 2262-04-12.
    // When an integer overflow occurs in the calculation,
    // an exception will be thrown.
    try {
      return checkedPlus(
          checkedMultiply(seconds_, (int64_t)1'000'000'000), (int64_t)nanos_);
    } catch (const std::exception& e) {
      BOLT_USER_FAIL(
          "Could not convert Timestamp({}, {}) to nanoseconds, {}",
          seconds_,
          nanos_,
          e.what());
    }
  }

  // Keep it in header for getting inlined.
  int64_t toMillis() const {
    // We use int128_t to make sure the computation does not overflows since
    // there are cases such that seconds*1000 does not fit in int64_t,
    // but seconds*1000 + nanos does, an example is Timestamp::minMillis().

    // If the final result does not fit in int64_tw we throw.
    __int128_t result =
        (__int128_t)seconds_ * 1'000 + (int64_t)(nanos_ / 1'000'000);
    if (result < INT64_MIN || result > INT64_MAX) {
      BOLT_USER_FAIL(
          "Could not convert Timestamp({}, {}) to milliseconds",
          seconds_,
          nanos_);
    }
    return result;
  }

  // Keep it in header for getting inlined.
  int64_t toMillisAllowOverflow() const {
    // Similar to the above toMillis() except that overflowed integer is allowed
    // as result.
    auto result = seconds_ * 1'000 + (int64_t)(nanos_ / 1'000'000);
    return result;
  }

  // Keep it in header for getting inlined.
  int64_t toMicros() const {
    // We use int128_t to make sure the computation does not overflows since
    // there are cases such that a negative seconds*1000000 does not fit in
    // int64_t, but seconds*1000000 + nanos does. An example is
    // Timestamp(-9223372036855, 224'192'000).

    // If the final result does not fit in int64_t we throw.
    __int128_t result = static_cast<__int128_t>(seconds_) * 1'000'000 +
        static_cast<int64_t>(nanos_ / 1'000);
    if (result < INT64_MIN || result > INT64_MAX) {
      BOLT_USER_FAIL(
          "Could not convert Timestamp({}, {}) to microseconds",
          seconds_,
          nanos_);
    }
    return result;
  }

  Timestamp toPrecision(const TimestampPrecision& precision) const {
    uint64_t nanos = nanos_;
    switch (precision) {
      case TimestampPrecision::kMilliseconds:
        nanos = nanos / 1'000'000 * 1'000'000;
        break;
      case TimestampPrecision::kMicroseconds:
        nanos = nanos / 1'000 * 1'000;
        break;
      case TimestampPrecision::kNanoseconds:
        break;
    }
    return Timestamp(seconds_, nanos);
  }

  /// Due to the limit of std::chrono, throws if timestamp is outside of
  /// [-32767-01-01, 32767-12-31] range.
  /// If allowOverflow is true, integer overflow is allowed in converting
  /// timestmap to milliseconds.
  static Timestamp fromMillis(int64_t millis) {
    if (millis >= 0 || millis % 1'000 == 0) {
      return Timestamp(millis / 1'000, (millis % 1'000) * 1'000'000);
    }
    auto second = millis / 1'000 - 1;
    auto nano = ((millis - second * 1'000) % 1'000) * 1'000'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromMillisNoError(int64_t millis)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    if (millis >= 0 || millis % 1'000 == 0) {
      return Timestamp(millis / 1'000, (millis % 1'000) * 1'000'000);
    }
    auto second = millis / 1'000 - 1;
    auto nano = ((millis - second * 1'000) % 1'000) * 1'000'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromMicros(int64_t micros) {
    if (micros >= 0 || micros % 1'000'000 == 0) {
      return Timestamp(micros / 1'000'000, (micros % 1'000'000) * 1'000);
    }
    auto second = micros / 1'000'000 - 1;
    auto nano = ((micros - second * 1'000'000) % 1'000'000) * 1'000;
    return Timestamp(second, nano);
  }

  static Timestamp fromNanos(int64_t nanos) {
    if (nanos >= 0 || nanos % 1'000'000'000 == 0) {
      return Timestamp(nanos / 1'000'000'000, nanos % 1'000'000'000);
    }
    auto second = nanos / 1'000'000'000 - 1;
    auto nano = (nanos - second * 1'000'000'000) % 1'000'000'000;
    return Timestamp(second, nano);
  }

  static const Timestamp minMillis() {
    // The minimum Timestamp that toMillis() method will not overflow.
    // Used to calculate the minimum value of the Presto timestamp.
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    return Timestamp(
        kMinSeconds,
        (kMin % kMillisecondsInSecond + kMillisecondsInSecond) *
            kNanosecondsInMillisecond);
  }

  static const Timestamp maxMillis() {
    // The maximum Timestamp that toMillis() method will not overflow.
    // Used to calculate the maximum value of the Presto timestamp.
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    return Timestamp(
        kMaxSeconds, kMax % kMillisecondsInSecond * kNanosecondsInMillisecond);
  }

  static const Timestamp min() {
    return Timestamp(kMinSeconds, 0);
  }

  static const Timestamp max() {
    return Timestamp(kMaxSeconds, kMaxNanos);
  }

  /// Our own version of gmtime_r to avoid expensive calls to __tz_convert.
  /// This might not be very significant in micro benchmark, but is causing
  /// significant context switching cost in real world queries with higher
  /// concurrency (71% of time is on __tz_convert for some queries).
  ///
  /// Return whether the epoch second can be converted to a valid std::tm.
  static bool epochToUtc(int64_t seconds, std::tm& out);

  static int64_t calendarUtcToEpoch(const std::tm& tm);

  /// Truncates a Timestamp value to the specified precision.
  static Timestamp truncate(Timestamp ts, TimestampPrecision precision) {
    switch (precision) {
      case TimestampPrecision::kMilliseconds:
        return Timestamp::fromMillis(ts.toMillis());
      case TimestampPrecision::kMicroseconds:
        return Timestamp::fromMicros(ts.toMicros());
      case TimestampPrecision::kNanoseconds:
        return ts;
      default:
        BOLT_UNREACHABLE();
    }
  }

  /// Converts a std::tm to a time/date/timestamp string in ISO 8601 format
  /// according to TimestampToStringOptions.
  static std::string tmToString(
      const std::tm&,
      uint64_t nanos,
      const TimestampToStringOptions& options);

  // Assuming the timestamp represents a time at zone, converts it to the GMT
  // time at the same moment.
  // Example: Timestamp ts{0, 0};
  // ts.Timezone("America/Los_Angeles");
  // ts.toString() returns January 1, 1970 08:00:00
  // If error is not nullptr, it is set to true if the conversion fails.
  void toGMT(const tz::TimeZone& zone, bool* error = nullptr);

  // Same as above, but accepts PrestoDB time zone ID.
  // If error is not nullptr, it is set to true if the conversion fails.
  void toGMT(int16_t tzID, bool* error = nullptr);

  // Assuming the timestamp represents a GMT time, converts it to the time at
  // the same moment at zone.
  // Example: Timestamp ts{0, 0};
  // ts.Timezone("America/Los_Angeles");
  // ts.toString() returns December 31, 1969 16:00:00
  void toTimezone(const tz::TimeZone& zone);

  // Same as above, but accepts PrestoDB time zone ID.
  void toTimezone(int16_t tzID);

  /// A default time zone that is same across the process.
  static const tz::TimeZone& defaultTimezone();

  bool operator==(const Timestamp& b) const {
    return seconds_ == b.seconds_ && nanos_ == b.nanos_;
  }

  bool operator!=(const Timestamp& b) const {
    return seconds_ != b.seconds_ || nanos_ != b.nanos_;
  }

  bool operator<(const Timestamp& b) const {
    return seconds_ < b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ < b.nanos_);
  }

  bool operator<=(const Timestamp& b) const {
    return seconds_ < b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ <= b.nanos_);
  }

  bool operator>(const Timestamp& b) const {
    return seconds_ > b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ > b.nanos_);
  }

  bool operator>=(const Timestamp& b) const {
    return seconds_ > b.seconds_ ||
        (seconds_ == b.seconds_ && nanos_ >= b.nanos_);
  }

  void operator++() {
    if (nanos_ < kMaxNanos) {
      nanos_++;
      return;
    }
    if (seconds_ < kMaxSeconds) {
      seconds_++;
      nanos_ = 0;
      return;
    }
    BOLT_USER_FAIL("Timestamp nanos out of range");
  }

  void operator--() {
    if (nanos_ > 0) {
      nanos_--;
      return;
    }
    if (seconds_ > kMinSeconds) {
      seconds_--;
      nanos_ = kMaxNanos;
      return;
    }
    BOLT_USER_FAIL("Timestamp nanos out of range");
  }

  // Needed for serialization of FlatVector<Timestamp>
  operator StringView() const {
    return StringView("TODO: Implement");
  }

  std::string toString(const TimestampToStringOptions& options = {}) const {
    std::tm tm;
    BOLT_USER_CHECK(
        epochToUtc(seconds_, tm),
        "Can't convert seconds to time: {}",
        seconds_);
    return tmToString(tm, nanos_, options);
  }

  FOLLY_ALWAYS_INLINE
  std::string toString(
      const TimestampToStringOptions::Precision& precision,
      const tz::TimeZone* tz) const {
    using namespace std::chrono;
    const auto localSec = tz == nullptr ? seconds_ : toLocal(tz);
    const auto days =
        ::date::floor<duration<int64_t, std::ratio<24 * 60 * 60>>>(
            seconds(localSec));
    const auto timeOfDay = duration_cast<seconds>(seconds(localSec) - days);
    const auto ymd = ::date::year_month_day(::date::sys_days(days));
    const auto hhmmss = ::date::time_of_day<seconds>(timeOfDay);
#ifndef SPARK_COMPATIBLE
    const auto fracWidth = static_cast<int>(precision);
    const auto fracValue =
        precision == TimestampToStringOptions::Precision::kMilliseconds
        ? nanos_ / 1'000'000
        : nanos_;
#endif
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << static_cast<int>(ymd.year())
        << "-" << std::setw(2) << static_cast<unsigned>(ymd.month()) << "-"
        << std::setw(2) << static_cast<unsigned>(ymd.day()) << " "
        << std::setw(2) << hhmmss.hours().count() << ":" << std::setw(2)
        << hhmmss.minutes().count() << ":" << std::setw(2)
        << hhmmss.seconds().count();
#ifndef SPARK_COMPATIBLE
    oss << "." << std::setw(fracWidth) << fracValue;
#else
    if (nanos_ > 0) {
      auto nanos = nanos_;
      while (nanos % 10 == 0) {
        nanos /= 10;
      }
      oss << "." << nanos;
    }
#endif

    return oss.str();
  }

  operator std::string() const {
    return toString();
  }

  operator folly::dynamic() const {
    return folly::dynamic(seconds_);
  }

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["seconds"] = seconds_;
    obj["nanos"] = nanos_;
    return obj;
  }

  /// Converts a std::tm to a time/date/timestamp string in ISO 8601 format
  /// according to TimestampToStringOptions.
  /// @param startPosition the start position of pre-allocated memory to write
  /// string to.
  static StringView tmToStringView(
      const std::tm& tmValue,
      uint64_t nanos,
      const TimestampToStringOptions& options,
      char* const startPosition);

  /// Our own version of gmtime_r to avoid expensive calls to __tz_convert.
  /// This might not be very significant in micro benchmark, but is causing
  /// significant context switching cost in real world queries with higher
  /// concurrency (71% of time is on __tz_convert for some queries).
  ///
  /// Return whether the epoch second can be converted to a valid std::tm.
  static bool epochToCalendarUtc(int64_t seconds, std::tm& out);

  /// Converts a timestamp to a time/date/timestamp string in ISO 8601 format
  /// according to TimestampToStringOptions.
  /// @param startPosition the start position of pre-allocated memory to write
  /// string to.
  static StringView tsToStringView(
      const Timestamp& ts,
      const TimestampToStringOptions& options,
      char* const startPosition);

 private:
  FOLLY_ALWAYS_INLINE
  int64_t toLocal(const tz::TimeZone* tz) const {
    using namespace std::chrono;
    return tz->to_local(seconds(seconds_)).count();
  }

  int64_t seconds_;
  uint64_t nanos_;
};

void parseTo(folly::StringPiece in, ::bytedance::bolt::Timestamp& out);

template <typename T>
void toAppend(const ::bytedance::bolt::Timestamp& value, T* result) {
  result->append(value.toString());
}

} // namespace bytedance::bolt

namespace std {
template <>
struct hash<::bytedance::bolt::Timestamp> {
  size_t operator()(const ::bytedance::bolt::Timestamp value) const {
    return bytedance::bolt::bits::hashMix(value.getSeconds(), value.getNanos());
  }
};

std::string to_string(const ::bytedance::bolt::Timestamp& ts);

std::ostream& operator<<(
    std::ostream& os,
    const ::bytedance::bolt::Timestamp& timestamp);

template <>
class numeric_limits<bytedance::bolt::Timestamp> {
 public:
  static bytedance::bolt::Timestamp min() {
    return bytedance::bolt::Timestamp::min();
  }
  static bytedance::bolt::Timestamp max() {
    return bytedance::bolt::Timestamp::max();
  }

  static bytedance::bolt::Timestamp lowest() {
    return bytedance::bolt::Timestamp::min();
  }
};

} // namespace std

namespace folly {
template <>
struct hasher<::bytedance::bolt::Timestamp> {
  size_t operator()(const ::bytedance::bolt::Timestamp value) const {
    return bytedance::bolt::bits::hashMix(value.getSeconds(), value.getNanos());
  }
};

} // namespace folly

namespace fmt {
template <>
struct formatter<bytedance::bolt::TimestampToStringOptions::Precision>
    : formatter<int> {
  auto format(
      bytedance::bolt::TimestampToStringOptions::Precision s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
template <>
struct formatter<bytedance::bolt::TimestampToStringOptions::Mode>
    : formatter<int> {
  auto format(
      bytedance::bolt::TimestampToStringOptions::Mode s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct formatter<bytedance::bolt::Timestamp> : formatter<std::string> {
  auto format(const bytedance::bolt::Timestamp t, format_context& ctx) {
    return formatter<std::string>::format(t.toString(), ctx);
  }
};

} // namespace fmt
