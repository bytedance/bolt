/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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
 * Shared calendar and timezone helpers for date-time formatter profiles.
 */

#pragma once

#include <unicode/gregocal.h>
#include <string_view>
#include <utility>

#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/tz/TimeZoneMap.h"

namespace bytedance::bolt::tz {
// Defined in TimeZoneLinks.cpp
extern const std::unordered_map<std::string, std::string>& getTimeZoneLinks();
} // namespace bytedance::bolt::tz

namespace bytedance::bolt::functions {

inline auto format_as(TimePolicy tp) {
  return fmt::underlying(tp);
}
} // namespace bytedance::bolt::functions

namespace bytedance::bolt::functions::detail {

inline constexpr std::string_view weekdaysFull[] = {
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"};
inline constexpr std::string_view weekdaysShort[] =
    {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
inline constexpr std::string_view monthsFull[] = {
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
inline constexpr std::string_view monthsShort[] = {
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

icu::GregorianCalendar& simpleDateCalendar();
inline int32_t simpleCalendarField(DateTimeFormatSpecifier specifier) {
  switch (specifier) {
    case DateTimeFormatSpecifier::ERA:
      return UCAL_ERA;
    case DateTimeFormatSpecifier::YEAR:
      return UCAL_YEAR;
    case DateTimeFormatSpecifier::WEEK_YEAR:
      return UCAL_YEAR_WOY;
    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
    case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
      return UCAL_MONTH;
    case DateTimeFormatSpecifier::DAY_OF_MONTH:
      return UCAL_DATE;
    case DateTimeFormatSpecifier::DAY_OF_YEAR:
      return UCAL_DAY_OF_YEAR;
    case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT:
      return UCAL_DAY_OF_WEEK;
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
      return UCAL_WEEK_OF_YEAR;
    case DateTimeFormatSpecifier::WEEK_OF_MONTH:
      return UCAL_WEEK_OF_MONTH;
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      return UCAL_DAY_OF_WEEK_IN_MONTH;
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
      return UCAL_HOUR_OF_DAY;
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
      return UCAL_HOUR;
    case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
      return UCAL_AM_PM;
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
      return UCAL_MINUTE;
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      return UCAL_SECOND;
    case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
      return UCAL_MILLISECOND;
    default:
      return -1;
  }
}
/// Resolves ICU offsets, using built-in SystemV metadata for unknown IDs.
std::pair<int32_t, int32_t> simpleTimeZoneOffsetsFromIcu(
    std::string_view name,
    const icu::TimeZone& javaZone);
std::pair<int32_t, int32_t> simpleTimeZoneOffsets(
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context = nullptr);
std::pair<int64_t, bool>
resolveLocalTime(int64_t seconds, const tz::TimeZone* zone, bool latest);
Timestamp simpleDateTimeToUtc(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context = nullptr,
    bool* shifted = nullptr);
Timestamp simpleDateTimeFromUtc(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    const DateTimeFormatterContext* context = nullptr);
Timestamp rebaseDateTime(
    Timestamp timestamp,
    const tz::TimeZone* zone,
    bool toGregorian,
    const DateTimeFormatterContext* context = nullptr);

} // namespace bytedance::bolt::functions::detail
