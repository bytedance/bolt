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

#include <cstdint>
#include <optional>
#include "bolt/type/Timestamp.h"
namespace bytedance::bolt::util {

constexpr const int32_t kHoursPerDay{24};
constexpr const int32_t kMinsPerHour{60};
constexpr const int32_t kSecsPerMinute{60};
constexpr const int64_t kMsecsPerSec{1000};

constexpr const int64_t kMicrosPerMsec{1000};
constexpr const int64_t kMicrosPerSec{kMicrosPerMsec * kMsecsPerSec};
constexpr const int64_t kMicrosPerMinute{kMicrosPerSec * kSecsPerMinute};
constexpr const int64_t kMicrosPerHour{kMicrosPerMinute * kMinsPerHour};

constexpr const int64_t kNanosPerMicro{1000};

constexpr const int32_t kSecsPerHour{kSecsPerMinute * kMinsPerHour};
constexpr const int32_t kSecsPerDay{kSecsPerHour * kHoursPerDay};

// Max and min year correspond to Joda datetime min and max
constexpr const int32_t kMinYear{-292275055};
constexpr const int32_t kMaxYear{292278994};

constexpr const int32_t kJulianEndYear{1582};

constexpr const int32_t kYearInterval{400};
constexpr const int32_t kDaysPerYearInterval{146097};

// Returns true if leap year, false otherwise
bool isLeapYear(int32_t year);

// Returns true if year, month, day corresponds to valid date, false otherwise
bool isValidDate(int32_t year, int32_t month, int32_t day);

// Returns true if yday of year is valid for given year
bool isValidDayOfYear(int32_t year, int32_t dayOfYear);

// Returns max day of month for inputted month of inputted year
int32_t getMaxDayOfMonth(int32_t year, int32_t month);

// Returns last day of month since unix epoch (1970-01-01).
int64_t lastDayOfMonthSinceEpochFromDate(const std::tm& dateTime);

/// Date conversions.

/// Returns the number of days before the first day of the month this year
int64_t daysBeforeMonthFirstDay(int32_t year, int32_t month);

/// Returns the (signed) number of days since unix epoch (1970-01-01).
/// When the date is invalid, throws BoltUserError if isValid is nullptr.
/// Otherwise, sets isValid to false and returns 0.
int64_t daysSinceEpochFromDate(
    int32_t year,
    int32_t month,
    int32_t day,
    bool* isValid = nullptr);

/// Returns the (signed) number of days since unix epoch (1970-01-01).
int64_t daysSinceEpochFromWeekDate(
    int32_t weekYear,
    int32_t weekOfYear,
    int32_t dayOfWeek);

/// Returns the (signed) number of days since unix epoch (1970-01-01).
int64_t daysSinceEpochFromDayOfYear(int32_t year, int32_t dayOfYear);

/// Returns the (signed) number of days since unix epoch (1970-01-01), following
/// the "YYYY-MM-DD" format (ISO 8601). ' ', '/' and '\' are also acceptable
/// separators. Negative years and a trailing "(BC)" are also supported.
///
/// Throws BoltUserError if the format or date is invalid.
int64_t fromDateString(
    const char* buf,
    size_t len,
    bool* nullOutput,
    bool sparkCompatible = false);

inline int64_t fromDateString(const StringView& str, bool* nullOutput) {
  return fromDateString(str.data(), str.size(), nullOutput);
}

/**
 * copy from spark to support the following format
 * `yyyy`
 * `yyyy-[m]m`
 * `yyyy-[m]m-[d]d`
 * `yyyy-[m]m-[d]d `
 * `yyyy-[m]m-[d]d *`
 * `yyyy-[m]m-[d]dT*`
 */
std::optional<int32_t> sparkStringToDate(const char* bytes, uint32_t size);

/// Cast string to date.
/// When isIso8601 = true, only support "[+-]YYYY-MM-DD" format (ISO 8601).
/// When isIso8601 = false, supported date formats include:
///
/// `[+-]YYYY*`
/// `[+-]YYYY*-[M]M`
/// `[+-]YYYY*-[M]M-[D]D`
/// `[+-]YYYY*-[M]M-[D]D `
/// `[+-]YYYY*-[M]M-[D]D *`
/// `[+-]YYYY*-[M]M-[D]DT*`
///
/// Throws BoltUserError if the format or date is invalid.
/// Return std::nullopt if expected result is null
std::optional<int32_t>
castFromDateString(const char* buf, size_t len, bool isIso8601);

inline std::optional<int32_t> castFromDateString(
    const StringView& str,
    bool isIso8601) {
  return castFromDateString(str.data(), str.size(), isIso8601);
}

// Extracts the day of the week from the number of days since epoch
int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch);

/// Time conversions.

/// Returns the cumulative number of microseconds.
/// Does not perform any sanity checks.
int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds);

/// Parses the input string and returns the number of cumulative microseconds,
/// following the "HH:MM:SS[.MS]" format (ISO 8601).
//
/// Throws BoltUserError if the format or time is invalid.
int64_t fromTimeString(const char* buf, size_t len, bool* nullOutput);

inline int64_t fromTimeString(const StringView& str, bool* nullOutput) {
  return fromTimeString(str.data(), str.size(), nullOutput);
}

// Timestamp conversion

/// Parses a full ISO 8601 timestamp string, following the format
/// "YYYY-MM-DD HH:MM:SS[.MS] +00:00"
Timestamp fromTimestampString(const char* buf, size_t len, bool* nullOutput);

inline Timestamp fromTimestampString(const StringView& str, bool* nullOutput) {
  return fromTimestampString(str.data(), str.size(), nullOutput);
}

/// Parses a full ISO 8601 timestamp string, following the format:
///
///  "YYYY-MM-DD HH:MM[:SS[.MS]][zoneId]"
///
/// This is a timezone-aware version of the function above
/// `fromTimestampString()` which returns both the parsed timestamp and the
/// timezone ID. It is up to the client to do the expected conversion based on
/// these two values.
///
/// The timezone information at the end of the string(zoneId):
/// zoneId:
/// 1. Z - Zulu time zone UTC+0
/// 2. +|-hh:mm
/// 3. An ID with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-, and a
/// suffix +|-hh:mm
/// 4. Region-based zone IDs in the form <area>/<city>, for example,
/// Europe/Paris.

///
/// -1 means no timezone information was found. return nullopt in case of
/// parsing errors.
std::optional<std::pair<Timestamp, int16_t>> fromTimestampWithTimezoneString(
    const char* buf,
    size_t len);

inline auto fromTimestampWithTimezoneString(const StringView& str) {
  return fromTimestampWithTimezoneString(str.data(), str.size());
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight);

/// @brief Civil Date
struct CivilDate {
  int32_t year;
  int32_t month;
  int32_t day;
};

/// @brief Civil Time
struct CivilTime {
  int32_t hour;
  int32_t minute;
  int32_t second;
  int32_t nanosecond;
};

/// @brief Civil DateTime type, combining CivilDate and CivilTime, compatible
/// with std::chrono
struct CivilDateTime {
  CivilDate date;
  CivilTime time;

  int64_t daysSinceEpoch;
  int32_t weekday; // 0 = Sunday.
  int32_t dayOfYear;

  /// Returns the ISO week number (1-53).
  int32_t isoWeek() const;
};

CivilDateTime toCivilDateTime(
    const Timestamp& timestamp,
    bool allowOverflow = false,
    bool isPrecision = true);

} // namespace bytedance::bolt::util
