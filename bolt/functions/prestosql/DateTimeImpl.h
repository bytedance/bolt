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

#include <date/date.h>
#include <chrono>
#include <optional>

#include "bolt/common/base/Doubles.h"
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/TimestampConversion.h"
namespace bytedance::bolt::functions {
namespace {
constexpr double kNanosecondsInSecond = 1'000'000'000;
constexpr int64_t kNanosecondsInMillisecond = 1'000'000;
constexpr int64_t kMillisecondsInSecond = 1'000;
} // namespace

FOLLY_ALWAYS_INLINE double toUnixtime(const Timestamp& timestamp) {
  double result = timestamp.getSeconds();
  result += static_cast<double>(timestamp.getNanos()) / kNanosecondsInSecond;
  return result;
}

FOLLY_ALWAYS_INLINE std::optional<Timestamp> fromUnixtime(double unixtime) {
  if (FOLLY_UNLIKELY(std::isnan(unixtime))) {
    return Timestamp(0, 0);
  }

  static const int64_t kMin = std::numeric_limits<int64_t>::min();

  if (FOLLY_UNLIKELY(unixtime >= kMinDoubleAboveInt64Max)) {
    return Timestamp::maxMillis();
  }

  if (FOLLY_UNLIKELY(unixtime <= kMin)) {
    return Timestamp::minMillis();
  }

  if (FOLLY_UNLIKELY(std::isinf(unixtime))) {
    return unixtime < 0 ? Timestamp::minMillis() : Timestamp::maxMillis();
  }

  auto seconds = std::floor(unixtime);
  auto milliseconds = std::llround((unixtime - seconds) * kMillisInSecond);
  if (FOLLY_UNLIKELY(milliseconds == kMillisInSecond)) {
    ++seconds;
    milliseconds = 0;
  }
  return Timestamp(seconds, milliseconds * kNanosecondsInMillisecond);
}

// Year, quarter or month are not uniformly incremented. Months have different
// total days, and leap years have more days than the rest. If the new year,
// quarter or month has less total days than the given one, it will be coerced
// to use the valid last day of the new month. This could result in weird
// arithmetic behavior. For example,
//
// 2022-01-30 + (1 month) = 2022-02-28
// 2022-02-28 - (1 month) = 2022-01-28
//
// 2022-08-31 + (1 quarter) = 2022-11-30
// 2022-11-30 - (1 quarter) = 2022-08-30
//
// 2020-02-29 + (1 year) = 2021-02-28
// 2021-02-28 - (1 year) = 2020-02-28
FOLLY_ALWAYS_INLINE
int32_t
addToDate(const int32_t input, const DateTimeUnit unit, const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return input;
  }

  const std::chrono::time_point<std::chrono::system_clock, ::date::days> inDate{
      ::date::days(input)};
  std::chrono::time_point<std::chrono::system_clock, ::date::days> outDate;

  if (unit == DateTimeUnit::kDay) {
    outDate = inDate + ::date::days(value);
  } else if (unit == DateTimeUnit::kWeek) {
    outDate = inDate + ::date::days(7 * value);
  } else {
    const ::date::year_month_day inCalDate(inDate);
    ::date::year_month_day outCalDate;

    if (unit == DateTimeUnit::kMonth) {
      outCalDate = inCalDate + ::date::months(value);
    } else if (unit == DateTimeUnit::kQuarter) {
      outCalDate = inCalDate + ::date::months(3 * value);
    } else if (unit == DateTimeUnit::kYear) {
      outCalDate = inCalDate + ::date::years(value);
    } else {
      BOLT_UNREACHABLE();
    }

    if (!outCalDate.ok()) {
      outCalDate = outCalDate.year() / outCalDate.month() / ::date::last;
    }
    outDate = ::date::sys_days{outCalDate};
  }

  return outDate.time_since_epoch().count();
}

FOLLY_ALWAYS_INLINE Timestamp addToTimestamp(
    const Timestamp& timestamp,
    const DateTimeUnit unit,
    const int32_t value) {
  // TODO(gaoge): Handle overflow and underflow with 64-bit representation
  if (value == 0) {
    return timestamp;
  }

  const std::chrono::
      time_point<std::chrono::system_clock, std::chrono::milliseconds>
          inTimestamp(std::chrono::milliseconds(timestamp.toMillis()));
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      outTimestamp;

  switch (unit) {
    // Year, quarter or month are not uniformly incremented in terms of number
    // of days. So we treat them differently.
    case DateTimeUnit::kYear:
    case DateTimeUnit::kQuarter:
    case DateTimeUnit::kMonth:
    case DateTimeUnit::kWeek:
    case DateTimeUnit::kDay: {
      const int32_t inDate = std::chrono::duration_cast<::date::days>(
                                 inTimestamp.time_since_epoch())
                                 .count();
      const int32_t outDate = addToDate(inDate, unit, value);

      outTimestamp = inTimestamp + ::date::days(outDate - inDate);
      break;
    }
    case DateTimeUnit::kHour: {
      outTimestamp = inTimestamp + std::chrono::hours(value);
      break;
    }
    case DateTimeUnit::kMinute: {
      outTimestamp = inTimestamp + std::chrono::minutes(value);
      break;
    }
    case DateTimeUnit::kSecond: {
      outTimestamp = inTimestamp + std::chrono::seconds(value);
      break;
    }
    case DateTimeUnit::kMillisecond: {
      outTimestamp = inTimestamp + std::chrono::milliseconds(value);
      break;
    }
    default:
      BOLT_UNREACHABLE("Unsupported datetime unit");
  }

  Timestamp milliTimestamp =
      Timestamp::fromMillis(outTimestamp.time_since_epoch().count());
  return Timestamp(
      milliTimestamp.getSeconds(),
      milliTimestamp.getNanos() +
          timestamp.getNanos() % kNanosecondsInMillisecond);
}

FOLLY_ALWAYS_INLINE int64_t diffTimestamp(
    const DateTimeUnit unit,
    const Timestamp& fromTimestamp,
    const Timestamp& toTimestamp) {
  if (fromTimestamp == toTimestamp) {
    return 0;
  }

  const int8_t sign = fromTimestamp < toTimestamp ? 1 : -1;

  const auto& low = sign == 1 ? fromTimestamp : toTimestamp;
  const auto& high = sign == 1 ? toTimestamp : fromTimestamp;
  const int64_t fromMillis = low.toMillis();
  const int64_t toMillis = high.toMillis();
  const int64_t deltaMillis = toMillis - fromMillis;

  // Millisecond, second, minute, hour and day have fixed conversion ratio
  switch (unit) {
    case DateTimeUnit::kMillisecond: {
      return sign * deltaMillis;
    }
    case DateTimeUnit::kSecond: {
      return sign * (deltaMillis / kMillisInSecond);
    }
    case DateTimeUnit::kMinute: {
      return sign * (deltaMillis / kMillisInMinute);
    }
    case DateTimeUnit::kHour: {
      return sign * (deltaMillis / kMillisInHour);
    }
    case DateTimeUnit::kDay: {
      return sign * (deltaMillis / kMillisInDay);
    }
    case DateTimeUnit::kWeek: {
      return sign * (deltaMillis / kMillisInWeek);
    }
    default:
      break;
  }

  // Month, quarter and year do not have fixed conversion ratio. Ex. a month can
  // have 28, 29, 30 or 31 days. A year can have 365 or 366 days.
  const auto fromCalDate =
      util::toCivilDateTime(low, /*allowOverflow*/ false, /*isPrecision*/ true);
  const auto toCalDate = util::toCivilDateTime(
      high, /*allowOverflow*/ false, /*isPrecision*/ true);

  auto dayMillis = [](const util::CivilDateTime& civil) -> int64_t {
    return static_cast<int64_t>(civil.time.nanosecond / 1'000'000) +
        int64_t(
            civil.time.second + civil.time.minute * 60 +
            civil.time.hour * 3'600) *
        1'000;
  };
  const int64_t fromDayMillis = dayMillis(fromCalDate);
  const int64_t toDayMillis = dayMillis(toCalDate);

  const int32_t fromDay = fromCalDate.date.day;
  const int32_t fromMonth = fromCalDate.date.month;
  const int32_t toDay = toCalDate.date.day;
  const int32_t toMonth = toCalDate.date.month;
  const int32_t toLastYearMonthDay =
      util::getMaxDayOfMonth(toCalDate.date.year, toCalDate.date.month);

  if (unit == DateTimeUnit::kMonth || unit == DateTimeUnit::kQuarter) {
    int64_t diff =
        (int64_t(toCalDate.date.year) - int64_t(fromCalDate.date.year)) * 12 +
        int64_t(toMonth) - int64_t(fromMonth);

    if ((toDay != toLastYearMonthDay && fromDay > toDay) ||
        (fromDay == toDay && fromDayMillis > toDayMillis)) {
      diff--;
    }

    diff = (unit == DateTimeUnit::kMonth) ? diff : diff / 3;
    return sign * diff;
  }

  if (unit == DateTimeUnit::kYear) {
    int64_t diff =
        int64_t(toCalDate.date.year) - int64_t(fromCalDate.date.year);

    if (fromMonth > toMonth ||
        (fromMonth == toMonth && fromDay > toDay &&
         toDay != toLastYearMonthDay) ||
        (fromMonth == toMonth && fromDay == toDay &&
         fromDayMillis > toDayMillis)) {
      diff--;
    }
    return sign * diff;
  }

  BOLT_UNREACHABLE("Unsupported datetime unit");
}

FOLLY_ALWAYS_INLINE
int64_t diffDate(
    const DateTimeUnit unit,
    const int32_t fromDate,
    const int32_t toDate) {
  if (fromDate == toDate) {
    return 0;
  }
  return diffTimestamp(
      unit,
      // prevent overflow
      Timestamp((int64_t)fromDate * util::kSecsPerDay, 0),
      Timestamp((int64_t)toDate * util::kSecsPerDay, 0));
}
} // namespace bytedance::bolt::functions
