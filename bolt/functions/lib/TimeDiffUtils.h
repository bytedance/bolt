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

#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::functions {

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

  // Millisecond, second, minute, hour, day and week have fixed ratio.
  switch (unit) {
    case DateTimeUnit::kMillisecond:
      return sign * deltaMillis;
    case DateTimeUnit::kSecond:
      return sign * (deltaMillis / kMillisInSecond);
    case DateTimeUnit::kMinute:
      return sign * (deltaMillis / kMillisInMinute);
    case DateTimeUnit::kHour:
      return sign * (deltaMillis / kMillisInHour);
    case DateTimeUnit::kDay:
      return sign * (deltaMillis / kMillisInDay);
    case DateTimeUnit::kWeek:
      return sign * (deltaMillis / kMillisInWeek);
    default:
      break;
  }

  // Month, quarter and year have variable numbers of days.
  const auto fromCalDate =
      util::toCivilDateTime(low, false /*allowOverflow*/, true /*isPrecision*/);
  const auto toCalDate = util::toCivilDateTime(
      high, false /*allowOverflow*/, true /*isPrecision*/);

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

    return sign * (unit == DateTimeUnit::kMonth ? diff : diff / 3);
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

FOLLY_ALWAYS_INLINE int64_t diffDate(
    const DateTimeUnit unit,
    const int32_t fromDate,
    const int32_t toDate) {
  if (fromDate == toDate) {
    return 0;
  }
  return diffTimestamp(
      unit,
      Timestamp(static_cast<int64_t>(fromDate) * util::kSecsPerDay, 0),
      Timestamp(static_cast<int64_t>(toDate) * util::kSecsPerDay, 0));
}

} // namespace bytedance::bolt::functions
