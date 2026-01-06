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

#include <date/tz.h>
#include <regex>

#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::functions {

inline constexpr int64_t kSecondsInDay = 86'400;
inline constexpr int64_t kDaysInWeek = 7;
extern const folly::F14FastMap<std::string, int8_t> kDayOfWeekNames;

FOLLY_ALWAYS_INLINE const tz::TimeZone* getTimeZoneFromConfig(
    const core::QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      return tz::locateZone(sessionTzName);
    }
  }
  return nullptr;
}

FOLLY_ALWAYS_INLINE void ensureFormatLegal(std::string_view format) {
  // avoid regex match
  bool legal = format.find("YY") == std::string_view::npos;
  BOLT_USER_CHECK(
      legal,
      "Datetime patterns week-based year {} is currently unsupported",
      format);
}

FOLLY_ALWAYS_INLINE const int64_t getTimeZoneId(const std::string& tzName) {
  if (!tzName.empty()) {
    return tz::getTimeZoneID(tzName);
  }
  return 0;
}

FOLLY_ALWAYS_INLINE const int64_t
getTimeZoneIdFromConfig(const core::QueryConfig& config) {
  if (config.adjustTimestampToTimezone()) {
    return getTimeZoneId(config.sessionTimezone());
  }
  return 0;
}

FOLLY_ALWAYS_INLINE const tz::TimeZone* getSpecTimeZoneFromConfig(
    const core::QueryConfig& config) {
  auto sessionTzName = config.specTimeZone();
  if (!sessionTzName.empty()) {
    return tz::locateZone(sessionTzName);
  }
  return nullptr;
}

FOLLY_ALWAYS_INLINE int64_t
getSeconds(Timestamp timestamp, const tz::TimeZone* timeZone) {
  if (timeZone != nullptr) {
    timestamp.toTimezone(*timeZone);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}

FOLLY_ALWAYS_INLINE int64_t
getSeconds(Timestamp timestamp, const int16_t tzID) {
  if (tzID != -1) {
    timestamp.toTimezone(tzID);
    return timestamp.getSeconds();
  } else {
    return timestamp.getSeconds();
  }
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Timestamp timestamp, const tz::TimeZone* timeZone) {
  int64_t seconds = getSeconds(timestamp, timeZone);
  std::tm dateTime;
  BOLT_USER_CHECK(
      Timestamp::epochToUtc(seconds, dateTime),
      "Timestamp is too large: {} seconds since epoch",
      seconds);
  return dateTime;
}

FOLLY_ALWAYS_INLINE
std::tm getDateTime(Timestamp timestamp, const int16_t tzID) {
  int64_t seconds = getSeconds(timestamp, tzID);
  std::tm dateTime;
  BOLT_USER_CHECK(
      Timestamp::epochToUtc(seconds, dateTime),
      "Timestamp is too large: {} seconds since epoch",
      seconds);
  return dateTime;
}

// days is the number of days since Epoch.
FOLLY_ALWAYS_INLINE
std::tm getDateTime(int32_t days) {
  int64_t seconds = days * kSecondsInDay;
  std::tm dateTime;
  BOLT_USER_CHECK(
      Timestamp::epochToUtc(seconds, dateTime),
      "Date is too large: {} days",
      days);
  return dateTime;
}

FOLLY_ALWAYS_INLINE int getYear(const std::tm& time) {
  // tm_year: years since 1900.
  return 1900 + time.tm_year;
}

FOLLY_ALWAYS_INLINE int getMonth(const std::tm& time) {
  // tm_mon: months since January – [0, 11].
  return 1 + time.tm_mon;
}

FOLLY_ALWAYS_INLINE int getDay(const std::tm& time) {
  return time.tm_mday;
}

FOLLY_ALWAYS_INLINE int32_t getQuarter(const std::tm& time) {
  return time.tm_mon / 3 + 1;
}

FOLLY_ALWAYS_INLINE int32_t getDayOfYear(const std::tm& time) {
  return time.tm_yday + 1;
}

template <typename T>
struct InitSessionTimezone {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  const tz::TimeZone* timeZone_{nullptr};

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }
};

/// Converts string as date time unit. Throws for invalid input string.
///
/// @param unitString The input string to represent date time unit.
/// @param throwIfInvalid Whether to throw an exception for invalid input
/// string.
/// @param allowMicro Whether to allow microsecond.
/// @param allowAbbreviated Whether to allow abbreviated unit string.
std::optional<DateTimeUnit> fromDateTimeUnitString(
    StringView unitString,
    bool throwIfInvalid,
    bool allowMicro = false,
    bool allowAbbreviated = false);

/// Adjusts the given date time object to the start of the specified date time
/// unit (e.g., year, quarter, month, week, day, hour, minute).
void adjustDateTime(std::tm& dateTime, const DateTimeUnit& unit);

/// Returns timestamp with seconds adjusted to the nearest lower multiple of the
/// specified interval. If the given seconds is negative and not an exact
/// multiple of the interval, it adjusts further down.
FOLLY_ALWAYS_INLINE Timestamp
adjustEpoch(int64_t seconds, int64_t intervalSeconds) {
  int64_t s = seconds / intervalSeconds;
  if (seconds < 0 && seconds % intervalSeconds) {
    s = s - 1;
  }
  int64_t truncatedSeconds = s * intervalSeconds;
  return Timestamp(truncatedSeconds, 0);
}

// Returns timestamp truncated to the specified unit.
Timestamp truncateTimestamp(
    Timestamp timestamp,
    DateTimeUnit unit,
    const tz::TimeZone* timeZone);
} // namespace bytedance::bolt::functions
