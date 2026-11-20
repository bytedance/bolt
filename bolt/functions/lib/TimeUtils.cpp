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
#include "bolt/functions/lib/TimeUtils.h"
#include <boost/algorithm/string/case_conv.hpp>
#include <charconv>

#include "bolt/functions/lib/TimeUtils.h"
namespace bytedance::bolt::functions {

std::shared_ptr<const DateTimeFormatterContext> getDateTimeFormatterContext(
    const core::QueryConfig& config) {
  const auto defaultZone = config.get<std::string>(
      core::QueryConfig::kDateTimeFormatterDefaultTimezone);
  const auto rebaseZone = config.get<std::string>(
      core::QueryConfig::kDateTimeFormatterRebaseTimezone);
  const auto pivotMillis = config.get<int64_t>(
      core::QueryConfig::kSparkLegacyTimestampParserPivotMillis);
  const auto pivotYear = config.get<int32_t>(
      core::QueryConfig::kSparkLegacyTimestampParserPivotYear);
  const auto rawOffsets = config.get<std::string>(
      core::QueryConfig::kSparkLegacyTimestampParserTimezoneRawOffsets);
  if (!defaultZone && !rebaseZone && !pivotMillis && !pivotYear &&
      !rawOffsets) {
    return nullptr;
  }
  BOLT_USER_CHECK_EQ(
      pivotMillis.has_value(),
      pivotYear.has_value(),
      "Both timestamp parser pivot milliseconds and year must be supplied");
  auto context = std::make_shared<DateTimeFormatterContext>();
  auto locate = [](std::string_view name) {
    if (name.starts_with("GMT+") || name.starts_with("GMT-")) {
      name.remove_prefix(3);
    }
    return tz::locateZone(name);
  };
  if (defaultZone) {
    context->defaultTimezone = locate(*defaultZone);
  }
  if (rebaseZone) {
    context->rebaseTimezone = locate(*rebaseZone);
  }
  if (pivotMillis) {
    BOLT_USER_CHECK_GT(
        *pivotYear, 0, "Timestamp parser pivot year must be positive");
    context->twoDigitYearStart = Timestamp::fromMillis(*pivotMillis);
    context->twoDigitYearStartYear = *pivotYear;
  }
  if (rawOffsets) {
    std::string_view remaining(*rawOffsets);
    while (!remaining.empty()) {
      const auto end = remaining.find(';');
      const auto entry = remaining.substr(0, end);
      const auto separator = entry.find('=');
      BOLT_USER_CHECK(
          separator != std::string_view::npos && separator > 0,
          "Invalid timestamp parser raw offset: {}",
          entry);
      const auto value = entry.substr(separator + 1);
      int32_t seconds;
      const auto parsed =
          std::from_chars(value.data(), value.data() + value.size(), seconds);
      BOLT_USER_CHECK(
          parsed.ec == std::errc{} &&
              parsed.ptr == value.data() + value.size() && seconds >= -86400 &&
              seconds <= 86400,
          "Invalid timestamp parser raw offset: {}",
          entry);
      if (const auto* zone =
              tz::locateZone(entry.substr(0, separator), false)) {
        context->timezoneRawOffsets.insert_or_assign(zone->id(), seconds);
      }
      if (end == std::string_view::npos) {
        break;
      }
      remaining.remove_prefix(end + 1);
    }
  }
  return context;
}

const folly::F14FastMap<std::string, int8_t> kDayOfWeekNames{
    {"th", 0},       {"fr", 1},     {"sa", 2},       {"su", 3},
    {"mo", 4},       {"tu", 5},     {"we", 6},       {"thu", 0},
    {"fri", 1},      {"sat", 2},    {"sun", 3},      {"mon", 4},
    {"tue", 5},      {"wed", 6},    {"thursday", 0}, {"friday", 1},
    {"saturday", 2}, {"sunday", 3}, {"monday", 4},   {"tuesday", 5},
    {"wednesday", 6}};

std::optional<DateTimeUnit> fromDateTimeUnitString(
    StringView unitString,
    bool throwIfInvalid,
    bool allowMicro,
    bool allowAbbreviated) {
  const auto unit = boost::algorithm::to_lower_copy(unitString.str());

  if (unit == "microsecond" && allowMicro) {
    return DateTimeUnit::kMicrosecond;
  }
  if (unit == "millisecond") {
    return DateTimeUnit::kMillisecond;
  }
  if (unit == "second") {
    return DateTimeUnit::kSecond;
  }
  if (unit == "minute") {
    return DateTimeUnit::kMinute;
  }
  if (unit == "hour") {
    return DateTimeUnit::kHour;
  }
  if (unit == "day") {
    return DateTimeUnit::kDay;
  }
  if (unit == "week") {
    return DateTimeUnit::kWeek;
  }
  if (unit == "month") {
    return DateTimeUnit::kMonth;
  }
  if (unit == "quarter") {
    return DateTimeUnit::kQuarter;
  }
  if (unit == "year") {
    return DateTimeUnit::kYear;
  }
  if (allowAbbreviated) {
    if (unit == "dd") {
      return DateTimeUnit::kDay;
    }
    if (unit == "mon" || unit == "mm") {
      return DateTimeUnit::kMonth;
    }
    if (unit == "yyyy" || unit == "yy") {
      return DateTimeUnit::kYear;
    }
  }
  if (throwIfInvalid) {
    BOLT_UNSUPPORTED("Unsupported datetime unit: {}", unitString);
  }
  return std::nullopt;
}

void adjustDateTime(std::tm& dateTime, const DateTimeUnit& unit) {
  switch (unit) {
    case DateTimeUnit::kYear:
      dateTime.tm_mon = 0;
      dateTime.tm_yday = 0;
      FMT_FALLTHROUGH;
    case DateTimeUnit::kQuarter:
      dateTime.tm_mon = dateTime.tm_mon / 3 * 3;
      FMT_FALLTHROUGH;
    case DateTimeUnit::kMonth:
      dateTime.tm_mday = 1;
      dateTime.tm_hour = 0;
      dateTime.tm_min = 0;
      dateTime.tm_sec = 0;
      break;
    case DateTimeUnit::kWeek:
      // Subtract the truncation.
      dateTime.tm_mday -= dateTime.tm_wday == 0 ? 6 : dateTime.tm_wday - 1;
      // Setting the day of the week to Monday.
      dateTime.tm_wday = 1;

      // If the adjusted day of the month falls in the previous month
      // Move to the previous month.
      if (dateTime.tm_mday < 1) {
        dateTime.tm_mon -= 1;

        // If the adjusted month falls in the previous year
        // Set to December and Move to the previous year.
        if (dateTime.tm_mon < 0) {
          dateTime.tm_mon = 11;
          dateTime.tm_year -= 1;
        }

        // Calculate the correct day of the month based on the number of days
        // in the adjusted month.
        static const int daysInMonth[] = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int daysInPrevMonth = daysInMonth[dateTime.tm_mon];

        // Adjust for leap year if February.
        if (dateTime.tm_mon == 1 && (dateTime.tm_year + 1900) % 4 == 0 &&
            ((dateTime.tm_year + 1900) % 100 != 0 ||
             (dateTime.tm_year + 1900) % 400 == 0)) {
          daysInPrevMonth = 29;
        }
        // Set to the correct day in the previous month.
        dateTime.tm_mday += daysInPrevMonth;
      }
      dateTime.tm_hour = 0;
      dateTime.tm_min = 0;
      dateTime.tm_sec = 0;
      break;
    case DateTimeUnit::kDay:
      dateTime.tm_hour = 0;
      FMT_FALLTHROUGH;
    case DateTimeUnit::kHour:
      dateTime.tm_min = 0;
      FMT_FALLTHROUGH;
    case DateTimeUnit::kMinute:
      dateTime.tm_sec = 0;
      break;
    default:
      BOLT_UNREACHABLE();
  }
}

Timestamp truncateTimestamp(
    Timestamp timestamp,
    DateTimeUnit unit,
    const tz::TimeZone* timeZone) {
  Timestamp result;
  switch (unit) {
    // For seconds ,millisecond, microsecond we just truncate the nanoseconds
    // part of the timestamp; no timezone conversion required.
    case DateTimeUnit::kMicrosecond:
      return Timestamp(
          timestamp.getSeconds(), timestamp.getNanos() / 1000 * 1000);

    case DateTimeUnit::kMillisecond:
      return Timestamp(
          timestamp.getSeconds(), timestamp.getNanos() / 1000000 * 1000000);

    case DateTimeUnit::kSecond:
      return Timestamp(timestamp.getSeconds(), 0);

    // Same for minutes; timezones and daylight savings time are at least in
    // the granularity of 30 mins, so we can just truncate the epoch directly.
    case DateTimeUnit::kMinute:
      return adjustEpoch(timestamp.getSeconds(), 60);

    // Hour truncation has to handle the corner case of daylight savings time
    // boundaries. Since conversions from local timezone to UTC may be
    // ambiguous, we need to be careful about the roundtrip of converting to
    // local time and back. So what we do is to calculate the truncation delta
    // in UTC, then applying it to the input timestamp.
    case DateTimeUnit::kHour: {
      auto epochToAdjust = getSeconds(timestamp, timeZone);
      auto secondsDelta =
          epochToAdjust - adjustEpoch(epochToAdjust, 60 * 60).getSeconds();
      return Timestamp(timestamp.getSeconds() - secondsDelta, 0);
    }

    // For the truncations below, we may first need to convert to the local
    // timestamp, truncate, then convert back to GMT.
    case DateTimeUnit::kDay:
      result = adjustEpoch(getSeconds(timestamp, timeZone), 24 * 60 * 60);
      break;

    default:
      auto dateTime = getDateTime(timestamp, timeZone);
      adjustDateTime(dateTime, unit);
      result = Timestamp(Timestamp::calendarUtcToEpoch(dateTime), 0);
      break;
  }

  if (timeZone != nullptr) {
    result.toGMT(*timeZone);
  }
  return result;
}
} // namespace bytedance::bolt::functions
