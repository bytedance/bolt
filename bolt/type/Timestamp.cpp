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

#include "bolt/type/Timestamp.h"

#include <date/tz.h>
#include <charconv>
#include <chrono>
#include <iostream>
#include "bolt/common/base/CountBits.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt {

// static
Timestamp Timestamp::fromDaysAndNanos(int32_t days, int64_t nanos) {
  int64_t seconds =
      (days - kJulianToUnixEpochDays) * kSecondsInDay + nanos / kNanosInSecond;
  int64_t remainingNanos = nanos % kNanosInSecond;
  if (remainingNanos < 0) {
    remainingNanos += kNanosInSecond;
    seconds--;
  }
  return Timestamp(seconds, remainingNanos);
}

// static
Timestamp Timestamp::now() {
  auto now = std::chrono::system_clock::now();
  auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     now.time_since_epoch())
                     .count();
  return fromMillis(epochMs);
}

void Timestamp::toGMT(const ::date::time_zone& zone, bool* hasError) {
  const ::date::local_seconds local{std::chrono::seconds(seconds_)};
  const auto info = zone.get_info(local);

  auto failNonexistent = [&](const std::string& reason) {};

  switch (info.result) {
    case ::date::local_info::unique:
      seconds_ -= info.first.offset.count();
      break;
    case ::date::local_info::ambiguous:
      // Pick earliest to align with Presto behavior.
      seconds_ -= info.first.offset.count();
      break;
    case ::date::local_info::nonexistent:
      if (hasError) {
        *hasError = true;
        return;
      }
      BOLT_USER_FAIL(
          "Timestamp {} does not exist in time zone {}",
          std::to_string(seconds_),
          zone.name());
      return;
  }

  if (hasError) {
    *hasError = false;
  }
}

void Timestamp::toGMT(int16_t tzID, bool* hasError) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ -= getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toGMT(*::date::locate_zone(util::getTimeZoneName(tzID)), hasError);
  }
}

void Timestamp::toTimezone(const ::date::time_zone& zone) {
  using namespace std::chrono;
  const auto info = zone.get_info(::date::sys_seconds{seconds{seconds_}});
  seconds_ += info.offset.count();
}

void Timestamp::toTimezone(int16_t tzID) {
  if (tzID == 0) {
    // No conversion required for time zone id 0, as it is '+00:00'.
  } else if (tzID <= 1680) {
    seconds_ += getPrestoTZOffsetInSeconds(tzID);
  } else {
    // Other ids go this path.
    toTimezone(*::date::locate_zone(util::getTimeZoneName(tzID)));
  }
}

const ::date::time_zone& Timestamp::defaultTimezone() {
  static const ::date::time_zone* kDefault = ({
    // TODO: We are hard-coding PST/PDT here to be aligned with the current
    // behavior in DWRF reader/writer.  Once they are fixed, we can use
    // ::date::current_zone() here.
    auto* tz = ::date::locate_zone("America/Los_Angeles");
    BOLT_CHECK_NOT_NULL(tz);
    tz;
  });
  return *kDefault;
}

namespace {

constexpr int kTmYearBase = 1900;
constexpr int64_t kLeapYearOffset = 4000000000ll;
constexpr int64_t kSecondsPerHour = 3600;
constexpr int64_t kSecondsPerDay = 24 * kSecondsPerHour;

inline bool isLeap(int64_t y) {
  return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}

inline int64_t leapThroughEndOf(int64_t y) {
  // Add a large offset to make the calculation for negative years correct.
  y += kLeapYearOffset;
  BOLT_DCHECK_GE(y, 0);
  return y / 4 - y / 100 + y / 400;
}

const int monthLengths[][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

// clang-format off
const char intToStr[][3] = {
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61",
};
// clang-format on

inline int64_t daysBetweenYears(int64_t y1, int64_t y2) {
  return 365 * (y2 - y1) + leapThroughEndOf(y2 - 1) - leapThroughEndOf(y1 - 1);
}

const int16_t daysBeforeFirstDayOfMonth[][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

void appendSmallInt(int n, std::string& out) {
  BOLT_DCHECK_LE(n, 61);
  out.append(intToStr[n], 2);
}

std::string::size_type getCapacity(const TimestampToStringOptions& options) {
  auto precisionWidth = static_cast<int8_t>(options.precision);
  switch (options.mode) {
    case TimestampToStringOptions::Mode::kDateOnly:
      // yyyy-mm-dd
      return 10;
    case TimestampToStringOptions::Mode::kTimeOnly:
      // hh:mm:ss.precision
      return 9 + precisionWidth;
    case TimestampToStringOptions::Mode::kFull:
      return 26 + precisionWidth;
    default:
      BOLT_UNREACHABLE();
  }
}

} // namespace

std::string::size_type getMaxStringLength(
    const TimestampToStringOptions& options) {
  const auto precisionWidth = static_cast<int8_t>(options.precision);
  switch (options.mode) {
    case TimestampToStringOptions::Mode::kDateOnly:
      // Date format is %y-mm-dd, where y has 10 digits at maximum for int32.
      // Possible sign is considered.
      return 17;
    case TimestampToStringOptions::Mode::kTimeOnly:
      // hh:mm:ss.precision
      return 9 + precisionWidth;
    case TimestampToStringOptions::Mode::kFull:
      // Timestamp format is %y-%m-%dT%h:%m:%s.precision, where y has 10 digits
      // at maximum for int32. Possible sign is considered.
      return 27 + precisionWidth;
    default:
      BOLT_UNREACHABLE();
  }
}

bool Timestamp::epochToCalendarUtc(int64_t epoch, std::tm& tm) {
  constexpr int kDaysPerYear = 365;
  int64_t days = epoch / kSecondsPerDay;
  int64_t rem = epoch % kSecondsPerDay;
  while (rem < 0) {
    rem += kSecondsPerDay;
    --days;
  }
  tm.tm_hour = rem / kSecondsPerHour;
  rem = rem % kSecondsPerHour;
  tm.tm_min = rem / 60;
  tm.tm_sec = rem % 60;
  tm.tm_wday = (4 + days) % 7;
  if (tm.tm_wday < 0) {
    tm.tm_wday += 7;
  }
  int64_t y = 1970;
  if (y + days / kDaysPerYear <= -kLeapYearOffset + 10) {
    return false;
  }
  bool leapYear;
  while (days < 0 || days >= kDaysPerYear + (leapYear = isLeap(y))) {
    auto newy = y + days / kDaysPerYear - (days < 0);
    days -= daysBetweenYears(y, newy);
    y = newy;
  }
  y -= kTmYearBase;
  if (y > std::numeric_limits<decltype(tm.tm_year)>::max() ||
      y < std::numeric_limits<decltype(tm.tm_year)>::min()) {
    return false;
  }
  tm.tm_year = y;
  tm.tm_yday = days;
  auto* months = daysBeforeFirstDayOfMonth[leapYear];
  tm.tm_mon = std::upper_bound(months, months + 12, days) - months - 1;
  tm.tm_mday = days - months[tm.tm_mon] + 1;
  tm.tm_isdst = 0;
  return true;
}

StringView Timestamp::tmToStringView(
    const std::tm& tmValue,
    uint64_t nanos,
    const TimestampToStringOptions& options,
    char* const startPosition) {
  BOLT_DCHECK_LT(nanos, 1'000'000'000);

  const auto appendDigits = [](const int value,
                               const std::optional<uint32_t> minWidth,
                               char* const position) {
    const auto numDigits = countDigits(value);
    uint32_t offset = 0;
    // Append leading zeros when there is the requirement for minumum width.
    if (minWidth.has_value() && numDigits < minWidth.value()) {
      const auto leadingZeros = minWidth.value() - numDigits;
      std::memset(position, '0', leadingZeros);
      offset += leadingZeros;
    }
    const auto [endPosition, errorCode] =
        std::to_chars(position + offset, position + offset + numDigits, value);
    std::ignore = endPosition;
    BOLT_DCHECK_EQ(
        errorCode,
        std::errc(),
        "Failed to convert value to varchar: {}.",
        std::make_error_code(errorCode).message());
    offset += numDigits;
    return offset;
  };

  char* writePosition = startPosition;
  if (options.mode != TimestampToStringOptions::Mode::kTimeOnly) {
    int year = kTmYearBase + tmValue.tm_year;
    const bool leadingPositiveSign = options.leadingPositiveSign && year > 9999;
    const bool negative = year < 0;

    // Sign.
    if (negative) {
      *writePosition++ = '-';
      year = -year;
    } else if (leadingPositiveSign) {
      *writePosition++ = '+';
    }

    // Year.
    writePosition += appendDigits(
        year,
        options.zeroPaddingYear ? std::optional<uint32_t>(4) : std::nullopt,
        writePosition);

    // Month.
    *writePosition++ = '-';
    writePosition += appendDigits(1 + tmValue.tm_mon, 2, writePosition);

    // Day.
    *writePosition++ = '-';
    writePosition += appendDigits(tmValue.tm_mday, 2, writePosition);

    if (options.mode == TimestampToStringOptions::Mode::kDateOnly) {
      return StringView(startPosition, writePosition - startPosition);
    }
    *writePosition++ = options.dateTimeSeparator;
  }

  // Hour.
  writePosition += appendDigits(tmValue.tm_hour, 2, writePosition);

  // Minute.
  *writePosition++ = ':';
  writePosition += appendDigits(tmValue.tm_min, 2, writePosition);

  // Second.
  *writePosition++ = ':';
  writePosition += appendDigits(tmValue.tm_sec, 2, writePosition);

  if (options.precision == TimestampToStringOptions::Precision::kMilliseconds) {
    nanos /= 1'000'000;
  } else if (
      options.precision == TimestampToStringOptions::Precision::kMicroseconds) {
    nanos /= 1'000;
  }
  if (options.skipTrailingZeros && nanos == 0) {
    return StringView(startPosition, writePosition - startPosition);
  }

  // Fractional part.
  *writePosition++ = '.';
  // Append leading zeros.
  const auto numDigits = countDigits(nanos);
  const auto precisionWidth = static_cast<int8_t>(options.precision);
  std::memset(writePosition, '0', precisionWidth - numDigits);
  writePosition += precisionWidth - numDigits;

  // Append the remaining numeric digits.
  if (options.skipTrailingZeros) {
    std::optional<uint32_t> nonZeroOffset = std::nullopt;
    int32_t offset = numDigits - 1;
    // Write non-zero digits from end to start.
    while (nanos > 0) {
      if (nonZeroOffset.has_value() || nanos % 10 != 0) {
        *(writePosition + offset) = '0' + nanos % 10;
        if (!nonZeroOffset.has_value()) {
          nonZeroOffset = offset;
        }
      }
      --offset;
      nanos /= 10;
    }
    writePosition += nonZeroOffset.value() + 1;
  } else {
    const auto [position, errorCode] =
        std::to_chars(writePosition, writePosition + numDigits, nanos);
    BOLT_DCHECK_EQ(
        errorCode,
        std::errc(),
        "Failed to convert fractional part to chars: {}.",
        std::make_error_code(errorCode).message());
    writePosition = position;
  }
  return StringView(startPosition, writePosition - startPosition);
}

StringView Timestamp::tsToStringView(
    const Timestamp& ts,
    const TimestampToStringOptions& options,
    char* const startPosition) {
  std::tm tmValue;
  BOLT_USER_CHECK(
      epochToCalendarUtc(ts.getSeconds(), tmValue),
      "Can't convert seconds to time: {}",
      ts.getSeconds());
  const uint64_t nanos = ts.getNanos();
  return tmToStringView(tmValue, nanos, options, startPosition);
}

bool Timestamp::epochToUtc(int64_t epoch, std::tm& tm) {
  constexpr int kSecondsPerHour = 3600;
  constexpr int kSecondsPerDay = 24 * kSecondsPerHour;
  constexpr int kDaysPerYear = 365;
  int64_t days = epoch / kSecondsPerDay;
  int64_t rem = epoch % kSecondsPerDay;
  while (rem < 0) {
    rem += kSecondsPerDay;
    --days;
  }
  tm.tm_hour = rem / kSecondsPerHour;
  rem = rem % kSecondsPerHour;
  tm.tm_min = rem / 60;
  tm.tm_sec = rem % 60;
  tm.tm_wday = (4 + days) % 7;
  if (tm.tm_wday < 0) {
    tm.tm_wday += 7;
  }
  int64_t y = 1970;
  if (y + days / kDaysPerYear <= -kLeapYearOffset + 10) {
    return false;
  }
  bool leapYear;
  while (days < 0 || days >= kDaysPerYear + (leapYear = isLeap(y))) {
    auto newy = y + days / kDaysPerYear - (days < 0);
    days -= (newy - y) * kDaysPerYear + leapThroughEndOf(newy - 1) -
        leapThroughEndOf(y - 1);
    y = newy;
  }
  y -= kTmYearBase;
  if (y > std::numeric_limits<decltype(tm.tm_year)>::max() ||
      y < std::numeric_limits<decltype(tm.tm_year)>::min()) {
    return false;
  }
  tm.tm_year = y;
  tm.tm_yday = days;
  auto* ip = monthLengths[leapYear];
  for (tm.tm_mon = 0; days >= ip[tm.tm_mon]; ++tm.tm_mon) {
    days = days - ip[tm.tm_mon];
  }
  tm.tm_mday = days + 1;
  tm.tm_isdst = 0;
  return true;
}

// static
int64_t Timestamp::calendarUtcToEpoch(const std::tm& tm) {
  static_assert(sizeof(decltype(tm.tm_year)) == 4);
  // tm_year stores number of years since 1900.
  int64_t year = tm.tm_year + 1900LL;
  int64_t month = tm.tm_mon;
  if (FOLLY_UNLIKELY(month > 11)) {
    year += month / 12;
    month %= 12;
  } else if (FOLLY_UNLIKELY(month < 0)) {
    auto yearsDiff = (-month + 11) / 12;
    year -= yearsDiff;
    month += 12 * yearsDiff;
  }
  // Getting number of days since beginning of the year.
  auto dayOfYear =
      -1ll + daysBeforeFirstDayOfMonth[isLeap(year)][month] + tm.tm_mday;
  // Number of days since 1970-01-01.
  auto daysSinceEpoch = daysBetweenYears(1970, year) + dayOfYear;
  return kSecondsPerDay * daysSinceEpoch + kSecondsPerHour * tm.tm_hour +
      60ll * tm.tm_min + tm.tm_sec;
}

std::string Timestamp::tmToString(
    const std::tm& tmValue,
    uint64_t nanos,
    const TimestampToStringOptions& options) {
  BOLT_DCHECK_GE(nanos, 0);
  BOLT_DCHECK_LT(nanos, 1'000'000'000);
  auto precisionWidth = static_cast<int8_t>(options.precision);
  std::string out;
  out.reserve(getCapacity(options));

  if (options.mode != TimestampToStringOptions::Mode::kTimeOnly) {
    int n = kTmYearBase + tmValue.tm_year;
    const bool leadingPositiveSign = options.leadingPositiveSign && n > 9999;
    bool negative = n < 0;
    if (negative) {
      out += '-';
      n = -n;
    }
    while (n > 0) {
      out += '0' + n % 10;
      n /= 10;
    }
    auto zeroPaddingYearSize = negative + 4;
    if (options.zeroPaddingYear && out.size() < zeroPaddingYearSize) {
      while (out.size() < zeroPaddingYearSize) {
        out += '0';
      }
    }
    if (leadingPositiveSign) {
      out += '+';
    }
    std::reverse(out.begin() + negative, out.end());
    out += '-';
    appendSmallInt(1 + tmValue.tm_mon, out);
    out += '-';
    appendSmallInt(tmValue.tm_mday, out);
    if (options.mode == TimestampToStringOptions::Mode::kDateOnly) {
      return out;
    }

    out += options.dateTimeSeparator;
  }

  appendSmallInt(tmValue.tm_hour, out);
  out += ':';
  appendSmallInt(tmValue.tm_min, out);
  out += ':';
  appendSmallInt(tmValue.tm_sec, out);
  if (options.precision == TimestampToStringOptions::Precision::kMilliseconds) {
    nanos /= 1'000'000;
  } else if (
      options.precision == TimestampToStringOptions::Precision::kMicroseconds) {
    nanos /= 1'000;
  }
  if (options.skipTrailingZeros && nanos == 0) {
    return out;
  }
  out += '.';
  // Add nanos to the string with precisionWidth digits.
  const int offset = out.size();
  int trailingZeros = 0;

  if (options.skipTrailingZeros) {
    while (nanos > 0) {
      if (out.size() == offset && nanos % 10 == 0) {
        trailingZeros += 1;
      } else {
        out += '0' + nanos % 10;
      }
      nanos /= 10;
    }
  } else {
    while (nanos > 0) {
      out += '0' + nanos % 10;
      nanos /= 10;
    }
  }
  auto size = out.size() - offset;
  while (size < precisionWidth - trailingZeros) {
    out += '0';
    size += 1;
  }
  std::reverse(out.begin() + offset, out.end());
  return out;
}

void parseTo(folly::StringPiece in, ::bytedance::bolt::Timestamp& out) {
  // TODO Implement
}

} // namespace bytedance::bolt

namespace std {
std::string to_string(const ::bytedance::bolt::Timestamp& ts) {
  return ts.toString();
}

std::ostream& operator<<(
    std::ostream& os,
    const ::bytedance::bolt::Timestamp& ts) {
  os << ts.toString();
  return os;
}

} // namespace std
