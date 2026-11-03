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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/functions/lib/DateTimeFormatter.h"

#include <bolt/common/base/Exceptions.h>
#include <date/tz.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/small_vector.h>
#include <unicode/gregocal.h>
#include <array>
#include <cmath>
#include <cstring>

#include "bolt/functions/lib/DateTimeFormatterInternal.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"
#include "bolt/type/tz/TimeZoneNames.h"

namespace bytedance::bolt::functions {

static thread_local std::string timezoneBuffer = "+00:00";
static const char* defaultTrailingOffset = "00";

namespace {

struct Date {
  int32_t year = 1970;
  int32_t month = 1;
  bool hasMonth = false;
  int32_t day = 1;
  bool hasDay = false;
  bool isAd = true; // AD -> true, BC -> false.

  int32_t week = 1;
  int32_t dayOfWeek = 1;
  bool weekDateFormat = false;

  int32_t dayOfYear = 1;
  bool hasDayOfYear = false;
  bool dayOfYearFormat = false;

  std::optional<int32_t> weekInMonth;
  std::optional<int32_t> dayOfWeekInMonth;

  bool centuryFormat = false;

  bool isYearOfEra = false; // Year of era cannot be zero or negative.
  bool hasYear = false; // Whether year was explicitly specified.

  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t subsecond = 0;
  bool isAm = true; // AM -> true, PM -> false
  bool hasHalfDay = false;
  int64_t timezoneId = -1;
  std::optional<int32_t> timezoneOffsetSeconds;
  bool hasTimeZoneId = false;

  bool isClockHour = false; // Whether most recent hour specifier is clockhour
  bool isHourOfHalfDay =
      true; // Whether most recent hour specifier is of half day.
};

struct JavaDate : Date {
  bool record(DateTimeFormatSpecifier field, int64_t value) {
    const auto index = static_cast<uint8_t>(field);
    const uint32_t bit = 1u << index;
    if (fields & bit) {
      return values[index] == value;
    }
    fields |= bit;
    values[index] = value;
    return true;
  }

  bool has(DateTimeFormatSpecifier field) const {
    return fields & (1u << static_cast<uint8_t>(field));
  }

  int64_t value(DateTimeFormatSpecifier field) const {
    return values[static_cast<uint8_t>(field)];
  }

  uint32_t fields{0};
  std::array<int64_t, 30> values;
};

struct DateWithRepeatedFields : Date {
  folly::small_vector<int32_t, 1> dayOfMonthValues;
  folly::small_vector<int32_t, 1> dayOfYearValues;
};

static std::
    unordered_map<std::string_view, std::pair<std::string_view, int64_t>>
        dayOfWeekMap{
            // Capitalized.
            {"Mon", {"day", 1}},
            {"Tue", {"sday", 2}},
            {"Wed", {"nesday", 3}},
            {"Thu", {"rsday", 4}},
            {"Fri", {"day", 5}},
            {"Sat", {"urday", 6}},
            {"Sun", {"day", 7}},

            // Lower case.
            {"mon", {"day", 1}},
            {"tue", {"sday", 2}},
            {"wed", {"nesday", 3}},
            {"thu", {"rsday", 4}},
            {"fri", {"day", 5}},
            {"sat", {"urday", 6}},
            {"sun", {"day", 7}},

            // Upper case.
            {"MON", {"DAY", 1}},
            {"TUE", {"SDAY", 2}},
            {"WED", {"NESDAY", 3}},
            {"THU", {"RSDAY", 4}},
            {"FRI", {"DAY", 5}},
            {"SAT", {"URDAY", 6}},
            {"SUN", {"DAY", 7}},
        };

static std::
    unordered_map<std::string_view, std::pair<std::string_view, int64_t>>
        monthMap{
            // Capitalized.
            {"Jan", {"uary", 1}},
            {"Feb", {"ruary", 2}},
            {"Mar", {"ch", 3}},
            {"Apr", {"il", 4}},
            {"May", {"", 5}},
            {"Jun", {"e", 6}},
            {"Jul", {"y", 7}},
            {"Aug", {"ust", 8}},
            {"Sep", {"tember", 9}},
            {"Oct", {"ober", 10}},
            {"Nov", {"ember", 11}},
            {"Dec", {"ember", 12}},

            // Lower case.
            {"jan", {"uary", 1}},
            {"feb", {"ruary", 2}},
            {"mar", {"rch", 3}},
            {"apr", {"il", 4}},
            {"may", {"", 5}},
            {"jun", {"e", 6}},
            {"jul", {"y", 7}},
            {"aug", {"ust", 8}},
            {"sep", {"tember", 9}},
            {"oct", {"ober", 10}},
            {"nov", {"ember", 11}},
            {"dec", {"ember", 12}},

            // Upper case.
            {"JAN", {"UARY", 1}},
            {"FEB", {"RUARY", 2}},
            {"MAR", {"RCH", 3}},
            {"APR", {"IL", 4}},
            {"MAY", {"", 5}},
            {"JUN", {"E", 6}},
            {"JUL", {"Y", 7}},
            {"AUG", {"UST", 8}},
            {"SEP", {"TEMBER", 9}},
            {"OCT", {"OBER", 10}},
            {"NOV", {"EMBER", 11}},
            {"DEC", {"EMBER", 12}},
        };

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

bool specAllowsNegative(DateTimeFormatSpecifier s) {
  switch (s) {
    case DateTimeFormatSpecifier::YEAR:
    case DateTimeFormatSpecifier::WEEK_YEAR:
      return true;
    default:
      return false;
  }
}

bool specAllowsPlusSign(DateTimeFormatSpecifier s, bool specifierNext) {
  if (specifierNext) {
    return false;
  } else {
    switch (s) {
      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::WEEK_YEAR:
        return true;
      default:
        return false;
    }
  }
}

// Joda only supports parsing a few three-letter prefixes. The list is available
// here:
//
//  https://github.com/JodaOrg/joda-time/blob/main/src/main/java/org/joda/time/DateTimeUtils.java#L437
//
// Full timezone names (e.g. "America/Los_Angeles") are not supported by Joda
// when parsing, so we don't implement them here.
int64_t parseTimezone(
    const char* cur,
    const char* end,
    Date& date,
    bool legacySpark = false) {
  if (cur < end) {
    // If there are at least 3 letters left.
    if (end - cur >= 3) {
      static std::unordered_map<std::string_view, int64_t> defaultTzNames{
          {"UTC", 0},
          {"GMT", 0},
          {"EST", tz::getTimeZoneID("America/New_York")},
          {"EDT", tz::getTimeZoneID("America/New_York")},
          {"CST", tz::getTimeZoneID("America/Chicago")},
          {"CDT", tz::getTimeZoneID("America/Chicago")},
          {"MST", tz::getTimeZoneID("America/Denver")},
          {"MDT", tz::getTimeZoneID("America/Denver")},
          {"PST", tz::getTimeZoneID("America/Los_Angeles")},
          {"PDT", tz::getTimeZoneID("America/Los_Angeles")},
      };
      std::string_view zone(cur, 3);
      auto it = defaultTzNames.find(zone);
      if (::bytedance::bolt::kSparkCompatible && it == defaultTzNames.end()) {
        // spark accept timezone in lower case
        std::string upper;
        for (auto& c : zone) {
          upper.push_back(toupper(c));
        }
        it = defaultTzNames.find(upper);
      }
      if (it != defaultTzNames.end()) {
        date.timezoneId = it->second;
        return 3;
      }
    }
    // The format 'UT' is also accepted for UTC.
    if (!::bytedance::bolt::kSparkCompatible && end - cur == 2 && *cur == 'U' &&
        *(cur + 1) == 'T') {
      date.timezoneId = 0;
      return 2;
    }
    if (::bytedance::bolt::kSparkCompatible && (*cur == '+' || *cur == '-')) {
      int64_t timezoneId = -1;
      int64_t length = 0;
      if (legacySpark && (end - cur) >= 5) {
        // spark LEGACY accept (+/-)HHMM as time zone
        std::string tz = std::string(cur, 3) + ":" + std::string(cur + 3, 2);
        timezoneId = tz::getTimeZoneID(tz, false);
        length = 5;
      } else if (!legacySpark && (end - cur) >= 6 && *(cur + 3) == ':') {
        // spark CORRECTED accept (+/-)HH:MM as time zone
        timezoneId = tz::getTimeZoneID(std::string_view(cur, 6), false);
        length = 6;
      }
      if (timezoneId != -1) {
        date.timezoneId = timezoneId;
        return length;
      }
    }
  }
  return -1;
}

int64_t parseTimezoneOffset(const char* cur, const char* end, Date& date) {
  // For timezone offset ids, there are three formats allowed by Joda:
  //
  // 1. '+' or '-' followed by two digits: "+00"
  // 2. '+' or '-' followed by two digits, ":", then two more digits:
  //    "+00:00"
  // 3. '+' or '-' followed by four digits:
  //    "+0000"
  if (cur < end) {
    if (*cur == '-' || *cur == '+') {
      // Long format: "+00:00"
      if ((end - cur) >= 6 && *(cur + 3) == ':') {
        // Fast path for the common case ("+00:00" or "-00:00"), to prevent
        // calling getTimeZoneID(), which does a map lookup.
        if (std::strncmp(cur + 1, "00:00", 5) == 0) {
          date.timezoneId = 0;
        } else {
          date.timezoneId = tz::getTimeZoneID(std::string_view(cur, 6), false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 6;
      }
      // Long format without colon: "+0000"
      else if ((end - cur) >= 5 && *(cur + 3) != ':') {
        // Same fast path described above.
        if (std::strncmp(cur + 1, "0000", 4) == 0) {
          date.timezoneId = 0;
        } else {
          // We need to concatenate the 3 first chars with ":" followed by the
          // last 2 chars before calling getTimeZoneID, so we use a static
          // thread_local buffer to prevent extra allocations.
          std::memcpy(&timezoneBuffer[0], cur, 3);
          std::memcpy(&timezoneBuffer[4], cur + 3, 2);
          date.timezoneId = tz::getTimeZoneID(timezoneBuffer, false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 5;
      }
      // Short format: "+00"
      else if ((end - cur) >= 3) {
        // Same fast path described above.
        if (std::strncmp(cur + 1, "00", 2) == 0) {
          date.timezoneId = 0;
        } else {
          // We need to concatenate the 3 first chars with a trailing ":00"
          // before calling getTimeZoneID, so we use a static thread_local
          // buffer to prevent extra allocations.
          std::memcpy(&timezoneBuffer[0], cur, 3);
          std::memcpy(&timezoneBuffer[4], defaultTrailingOffset, 2);
          date.timezoneId = tz::getTimeZoneID(timezoneBuffer, false);
          if (date.timezoneId == -1) {
            return -1;
          }
        }
        return 3;
      }
    }
    // Single 'Z' character maps to GMT
    else if (*cur == 'Z') {
      date.timezoneId = 0;
      return 1;
    }
  }
  return -1;
}

template <bool caseSensitive = true>
int64_t parseTimeZoneId(
    const char* cur,
    const char* end,
    Date& date,
    bool canonicalize = false) {
  static const auto index = [] {
    std::pair<
        folly::F14FastMap<std::string, std::pair<int16_t, int16_t>>,
        size_t>
        result;
    auto add = [&](std::string_view name, int16_t id) {
      if (id >= 0) {
        std::string key(name);
        if constexpr (!caseSensitive) {
          folly::toLowerAscii(key);
        }
        auto it = result.first.try_emplace(key, std::pair{id, id}).first;
        it->second.second = id;
        result.second = std::max(result.second, name.size());
      }
    };
    for (auto id : tz::getTimeZoneIDs()) {
      if (id == 0 || id > tz::kMaxFixedOffsetTimeZoneId) {
        add(tz::locateZone(id)->name(), id);
      }
    }
    for (const auto& [alias, target] : tz::getTimeZoneLinks()) {
      add(alias, tz::getTimeZoneID(target, false));
    }
    for (auto name : {"GMT", "Etc/GMT", "Etc/UTC"}) {
      add(name, 0);
    }
    for (const auto& [name, minutes] :
         {std::pair{"EST", -300}, {"MST", -420}, {"HST", -600}}) {
      add(name, tz::getTimeZoneID(minutes));
      std::string key(name);
      if constexpr (!caseSensitive) {
        folly::toLowerAscii(key);
      }
      result.first.at(key).first = -1;
    }
    for (int hour = -14; hour <= 12; ++hour) {
      if (hour != 0) {
        add(fmt::format("Etc/GMT{:+}", hour), tz::getTimeZoneID(-hour * 60));
      }
    }
    return result;
  }();
  std::string normalized;
  if constexpr (!caseSensitive) {
    normalized.assign(cur, std::min<size_t>(end - cur, index.second));
    folly::toLowerAscii(normalized);
    cur = normalized.data();
    end = cur + normalized.size();
  }
  for (size_t size = std::min<size_t>(end - cur, index.second); size > 0;
       --size) {
    auto it = index.first.find(std::string_view(cur, size));
    if (it != index.first.end()) {
      const auto id = canonicalize ? it->second.second : it->second.first;
      if (id < 0) {
        continue;
      }
      date.timezoneId = id;
      date.hasTimeZoneId = true;
      return size;
    }
  }
  return -1;
}

int64_t parseJavaTimezoneOffset(
    FormatPattern pattern,
    const char* cur,
    const char* end,
    Date& date,
    bool caseSensitive = true) {
  const auto* start = cur;
  const bool rfc =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822;
  const auto digits = pattern.minRepresentDigits;
  const bool localized = rfc && digits == 4;
  const bool zeroAsZ =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601 ||
      (rfc && digits == 5);
  const bool colon = rfc ? digits >= 4 : digits == 3 || digits == 5;
  const bool optionalMinutes = !rfc && digits == 1;
  const bool optionalSeconds = rfc || digits >= 2;
  auto setOffset = [&](int32_t offset) {
    date.timezoneOffsetSeconds = offset;
    date.timezoneId = offset % 60 == 0 && std::abs(offset) <= 14 * 3600
        ? tz::getTimeZoneID(offset / 60)
        : -1;
  };
  // Java matches the zero-offset text before reading numeric fields.
  if ((rfc && digits <= 3) ||
      pattern.specifier ==
          DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601_NO_Z) {
    const std::string_view zero = rfc ? "+0000"
        : digits == 1                 ? "+00"
        : digits == 3 || digits == 5  ? "+00:00"
                                      : "+0000";
    if (end - cur >= zero.size() &&
        std::string_view(cur, zero.size()) == zero) {
      setOffset(0);
      return zero.size();
    }
  }
  if (localized) {
    if (end - cur < 3 ||
        (caseSensitive ? std::memcmp(cur, "GMT", 3) != 0
                       : !folly::StringPiece(cur, 3).equals(
                             "GMT", folly::AsciiCaseInsensitive()))) {
      return -1;
    }
    cur += 3;
    if (cur == end || (*cur != '+' && *cur != '-')) {
      setOffset(0);
      return cur - start;
    }
  } else if (
      zeroAsZ && cur < end &&
      (*cur == 'Z' || (!caseSensitive && *cur == 'z'))) {
    setOffset(0);
    return 1;
  }
  if (cur == end || (*cur != '+' && *cur != '-')) {
    return -1;
  }
  const bool negative = *cur++ == '-';
  auto readDigits = [&](int32_t& value) {
    if (end - cur < 2 || !characterIsDigit(cur[0]) ||
        !characterIsDigit(cur[1])) {
      return false;
    }
    value = (cur[0] - '0') * 10 + cur[1] - '0';
    cur += 2;
    return true;
  };
  int32_t hour = 0, minute = 0, second = 0;
  if (!readDigits(hour)) {
    return -1;
  }
  if (!optionalMinutes ||
      (end - cur >= 2 && characterIsDigit(cur[0]) &&
       characterIsDigit(cur[1]))) {
    if (colon && (cur == end || *cur++ != ':')) {
      return -1;
    }
    if (!readDigits(minute)) {
      return -1;
    }
  }
  if (optionalSeconds) {
    if (colon) {
      if (cur < end && *cur == ':') {
        ++cur;
        if (!readDigits(second)) {
          return -1;
        }
      }
    } else if (
        end - cur >= 2 && characterIsDigit(cur[0]) &&
        characterIsDigit(cur[1])) {
      if (!readDigits(second)) {
        return -1;
      }
    }
  }
  if (hour > 18 || minute > 59 || second > 59 ||
      (hour == 18 && (minute || second))) {
    return -1;
  }
  const auto offset = hour * 3600 + minute * 60 + second;
  setOffset(negative ? -offset : offset);
  return cur - start;
}

int64_t parseJavaTimeZoneId(
    const char* cur,
    const char* end,
    Date& date,
    bool caseSensitive) {
  const auto offsetPattern =
      FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601, 5};
  if (cur < end && (*cur == '+' || *cur == '-')) {
    return parseJavaTimezoneOffset(
        offsetPattern, cur, end, date, caseSensitive);
  }
  for (std::string_view prefix : {"UTC", "GMT", "UT"}) {
    if (end - cur >= prefix.size() &&
        (caseSensitive ? std::string_view(cur, prefix.size()) == prefix
                       : folly::StringPiece(cur, prefix.size())
                             .equals(prefix, folly::AsciiCaseInsensitive()))) {
      const auto* offset = cur + prefix.size();
      if (offset < end && (*offset == '+' || *offset == '-')) {
        auto size = parseJavaTimezoneOffset(
            offsetPattern, offset, end, date, caseSensitive);
        if (size >= 0) {
          return prefix.size() + size;
        }
      }
      date.timezoneId = 0;
      date.hasTimeZoneId = true;
      return prefix.size();
    }
  }
  auto size = parseTimeZoneId(cur, end, date);
  if (size < 0 && !caseSensitive) {
    size = parseTimeZoneId<false>(cur, end, date);
  }
  if (size < 0 && cur < end &&
      (*cur == 'Z' || (!caseSensitive && *cur == 'z'))) {
    date.timezoneId = 0;
    date.timezoneOffsetSeconds = 0;
    return 1;
  }
  return size;
}

template <size_t N>
int64_t parseJavaText(
    const char* cur,
    const char* end,
    const std::string_view (&names)[N],
    bool caseSensitive,
    int32_t& value) {
  for (size_t i = 0; i < N; ++i) {
    if (end - cur >= names[i].size() &&
        (caseSensitive
             ? std::string_view(cur, names[i].size()) == names[i]
             : folly::StringPiece(cur, names[i].size())
                   .equals(names[i], folly::AsciiCaseInsensitive()))) {
      value = i;
      return names[i].size();
    }
  }
  return -1;
}

int64_t parseJavaZoneText(
    FormatPattern pattern,
    const char* cur,
    const char* end,
    Date& date,
    bool caseSensitive) {
  auto size = parseJavaTimeZoneId(cur, end, date, caseSensitive);
  if (size >= 0) {
    return size;
  }
  using Names = folly::F14FastMap<std::string, int16_t>;
  static const auto names = [] {
    std::pair<std::array<Names, 4>, size_t> result;
    const auto& zoneNames = tz::getTimeZoneNames();
    auto add = [&](int16_t id) {
      auto entry = zoneNames.find(tz::locateZone(id)->name());
      if (entry == zoneNames.end()) {
        return;
      }
      const auto& n = entry->second;
      auto put = [&](std::string name, int index) {
        result.second = std::max(result.second, name.size());
        result.first[index].try_emplace(name, id);
        folly::toLowerAscii(name);
        result.first[index + 2].try_emplace(name, id);
      };
      put(n.standardTimeAbbreviation, 0);
      put(n.daylightTimeAbbreviation, 0);
      put(n.standardTimeLongName, 1);
      put(n.daylightTimeLongName, 1);
    };
    for (const auto* preferred :
         {"America/Los_Angeles",
          "America/Denver",
          "America/Chicago",
          "America/New_York"}) {
      add(tz::getTimeZoneID(preferred));
    }
    for (auto id : tz::getTimeZoneIDs()) {
      if (id > tz::kMaxFixedOffsetTimeZoneId) {
        add(id);
      }
    }
    return result;
  }();
  const auto& candidates =
      names.first
          [(pattern.minRepresentDigits < 4 ? 0 : 1) + (caseSensitive ? 0 : 2)];
  std::string normalized;
  if (!caseSensitive) {
    normalized.assign(cur, std::min<size_t>(end - cur, names.second));
    folly::toLowerAscii(normalized);
    cur = normalized.data();
    end = cur + normalized.size();
  }
  for (size = std::min<size_t>(end - cur, names.second); size > 0; --size) {
    const auto it = candidates.find(std::string_view(cur, size));
    if (it != candidates.end()) {
      date.timezoneId = it->second;
      return size;
    }
  }
  size = -1;
  return size;
}

int64_t parseEra(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 2) {
    if (std::strncmp(cur, "AD", 2) == 0 || std::strncmp(cur, "ad", 2) == 0) {
      date.isAd = true;
      return 2;
    } else if (
        std::strncmp(cur, "BC", 2) == 0 || std::strncmp(cur, "bc", 2) == 0) {
      date.isAd = false;
      return 2;
    } else {
      return -1;
    }
  } else {
    return -1;
  }
}

int64_t parseMonthText(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 3) {
    auto it = monthMap.find(std::string_view(cur, 3));
    if (it != monthMap.end()) {
      date.month = it->second.second;
      if (end - cur >= it->second.first.size() + 3) {
        if (std::strncmp(
                cur + 3, it->second.first.data(), it->second.first.size()) ==
            0) {
          return it->second.first.size() + 3;
        }
      }
      // If the suffix didn't match, still ok. Return a prefix match.
      return 3;
    }
  }
  return -1;
}

int64_t parseDayOfWeekText(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 3) {
    auto it = dayOfWeekMap.find(std::string_view(cur, 3));
    if (it != dayOfWeekMap.end()) {
      date.dayOfWeek = it->second.second;
      if (end - cur >= it->second.first.size() + 3) {
        if (std::strncmp(
                cur + 3, it->second.first.data(), it->second.first.size()) ==
            0) {
          return it->second.first.size() + 3;
        }
      }
      return 3;
    }
  }
  return -1;
}

int64_t parseHalfDayOfDay(const char* cur, const char* end, Date& date) {
  if ((end - cur) >= 2) {
    if (std::strncmp(cur, "AM", 2) == 0 || std::strncmp(cur, "am", 2) == 0) {
      date.isAm = true;
      return 2;
    } else if (
        std::strncmp(cur, "PM", 2) == 0 || std::strncmp(cur, "pm", 2) == 0) {
      date.isAm = false;
      return 2;
    } else {
      return -1;
    }
  } else {
    return -1;
  }
}

// According to DateTimeFormatSpecifier enum class
std::string getSpecifierName(DateTimeFormatSpecifier s) {
  switch (s) {
    case DateTimeFormatSpecifier::ERA:
      return "ERA";
    case DateTimeFormatSpecifier::CENTURY_OF_ERA:
      return "CENTURY_OF_ERA";
    case DateTimeFormatSpecifier::YEAR_OF_ERA:
      return "YEAR_OF_ERA";
    case DateTimeFormatSpecifier::WEEK_YEAR:
      return "WEEK_YEAR";
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
      return "WEEK_OF_WEEK_YEAR";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
      return "DAY_OF_WEEK_0_BASED";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
      return "DAY_OF_WEEK_1_BASED";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT:
      return "DAY_OF_WEEK_TEXT";
    case DateTimeFormatSpecifier::YEAR:
      return "YEAR";
    case DateTimeFormatSpecifier::DAY_OF_YEAR:
      return "DAY_OF_YEAR";
    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      return "MONTH_OF_YEAR";
    case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
      return "MONTH_OF_YEAR_TEXT";
    case DateTimeFormatSpecifier::DAY_OF_MONTH:
      return "DAY_OF_MONTH";
    case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
      return "HALFDAY_OF_DAY";
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
      return "HOUR_OF_HALFDAY";
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
      return "CLOCK_HOUR_OF_HALFDAY";
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
      return "HOUR_OF_DAY";
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
      return "CLOCK_HOUR_OF_DAY";
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
      return "MINUTE_OF_HOUR";
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      return "SECOND_OF_MINUTE";
    case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
      return "FRACTION_OF_SECOND";
    case DateTimeFormatSpecifier::TIMEZONE:
      return "TIMEZONE";
    case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
      return "TIMEZONE_OFFSET_ID";
    case DateTimeFormatSpecifier::TIMEZONE_ID:
      return "TIMEZONE_ID";
    case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601:
      return "TIMEZONE_OFFSET_ISO8601";
    case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601_NO_Z:
      return "TIMEZONE_OFFSET_ISO8601_NO_Z";
    case DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822:
      return "TIMEZONE_OFFSET_RFC822";
    case DateTimeFormatSpecifier::LITERAL_PERCENT:
      return "LITERAL_PERCENT";
    case DateTimeFormatSpecifier::WEEK_OF_MONTH:
      return "WEEK_OF_MONTH";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      return "DAY_OF_WEEK_IN_MONTH";
    default:
      return "UNKNOWN_SPECIFIER";
  }
}

FOLLY_ALWAYS_INLINE int getMaxDigitConsume(
    FormatPattern curPattern,
    bool specifierNext,
    DateTimeFormatterType type) {
  if (type == DateTimeFormatterType::JAVA_TIME &&
      curPattern.minRepresentDigits == 2 &&
      (curPattern.specifier == DateTimeFormatSpecifier::YEAR ||
       curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA)) {
    return 2;
  }
  // Does not support WEEK_YEAR, WEEK_OF_WEEK_YEAR, time zone names
  switch (curPattern.specifier) {
    case DateTimeFormatSpecifier::CENTURY_OF_ERA:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
    case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
      return curPattern.minRepresentDigits;

    case DateTimeFormatSpecifier::YEAR_OF_ERA:
    case DateTimeFormatSpecifier::YEAR:
    case DateTimeFormatSpecifier::WEEK_YEAR:
      if (specifierNext) {
        return curPattern.minRepresentDigits;
      } else {
        if (type == DateTimeFormatterType::MYSQL) {
          // MySQL format will try to read in at most 4 digits when supplied a
          // year, never more.
          return 4;
        }
        return curPattern.minRepresentDigits > 9 ? curPattern.minRepresentDigits
                                                 : 9;
      }

    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      return 2;

    case DateTimeFormatSpecifier::DAY_OF_YEAR:
      return curPattern.minRepresentDigits > 3 ? curPattern.minRepresentDigits
                                               : 3;

    case DateTimeFormatSpecifier::DAY_OF_MONTH:
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
    case DateTimeFormatSpecifier::WEEK_OF_MONTH:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      return curPattern.minRepresentDigits > 2 ? curPattern.minRepresentDigits
                                               : 2;

    default:
      return 1;
  }
}

using ErrorCode = DateTimeResult::ErrorCode;

template <
    bool nanosecondPrecision = false,
    bool javaTime = false,
    typename ParsedDate>
ErrorCode parseFromPattern(
    FormatPattern curPattern,
    const std::string_view& input,
    const char*& cur,
    const char* end,
    ParsedDate& date,
    bool specifierNext,
    DateTimeFormatterType type,
    bool legacySpark = false,
    DateTimeParseOptions options = {}) {
  if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID) {
    auto size = parseTimezoneOffset(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_TIMEZONE_OFFSET_ERROR;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE) {
    auto size = javaTime
        ? parseJavaZoneText(curPattern, cur, end, date, options.caseSensitive)
        : parseTimezone(cur, end, date, legacySpark);
    if (size == -1) {
      return ErrorCode::PARSE_TIMEZONE_ERROR;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::ERA) {
    auto size = javaTime ? -1 : parseEra(cur, end, date);
    if constexpr (javaTime) {
      constexpr std::string_view shortNames[] = {"BC", "AD"};
      constexpr std::string_view fullNames[] = {"Before Christ", "Anno Domini"};
      constexpr std::string_view narrowNames[] = {"B", "A"};
      int32_t era = 0;
      size = parseJavaText(
          cur,
          end,
          curPattern.minRepresentDigits < 4        ? shortNames
              : curPattern.minRepresentDigits == 4 ? fullNames
                                                   : narrowNames,
          options.caseSensitive,
          era);
      if (!date.record(DateTimeFormatSpecifier::ERA, era)) {
        return ErrorCode::DAY_CONFLICT;
      }
      date.isAd = era != 0;
    }
    if (size == -1) {
      return ErrorCode::PARSE_ERA_ERROR;
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT) {
    auto size = javaTime ? -1 : parseMonthText(cur, end, date);
    if constexpr (javaTime) {
      int32_t month = 0;
      size = parseJavaText(
          cur,
          end,
          curPattern.minRepresentDigits == 3 ? detail::monthsShort
                                             : detail::monthsFull,
          options.caseSensitive,
          month);
      if (!date.record(DateTimeFormatSpecifier::MONTH_OF_YEAR, month + 1)) {
        return ErrorCode::DAY_CONFLICT;
      }
      date.month = month + 1;
      date.hasMonth = true;
    }
    if (size == -1) {
      return ErrorCode::PARSE_MONTH_ERROR;
    }
    cur += size;
    if (!date.hasYear && !javaTime) {
      date.hasYear = true;
      date.year = 2000;
    }
  } else if (curPattern.specifier == DateTimeFormatSpecifier::HALFDAY_OF_DAY) {
    auto size = javaTime ? -1 : parseHalfDayOfDay(cur, end, date);
    if constexpr (javaTime) {
      constexpr std::string_view names[] = {"AM", "PM"};
      int32_t halfDay = 0;
      size = parseJavaText(cur, end, names, options.caseSensitive, halfDay);
      if (!date.record(DateTimeFormatSpecifier::HALFDAY_OF_DAY, halfDay)) {
        return ErrorCode::DAY_CONFLICT;
      }
      date.isAm = halfDay == 0;
      date.hasHalfDay = true;
    }
    if (size == -1) {
      return ErrorCode::PARSE_HALFDAY_ERROR;
    }
    if (!javaTime && ::bytedance::bolt::kSparkCompatible) {
      date.isHourOfHalfDay = true;
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT) {
    int64_t size;
    if constexpr (javaTime) {
      int32_t weekday = 0;
      size = parseJavaText(
          cur,
          end,
          curPattern.minRepresentDigits < 4 ? detail::weekdaysShort
                                            : detail::weekdaysFull,
          options.caseSensitive,
          weekday);
      if (!date.record(
              DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED,
              weekday == 0 ? 7 : weekday)) {
        return ErrorCode::DAY_CONFLICT;
      }
    } else {
      size = parseDayOfWeekText(cur, end, date);
      date.weekDateFormat = true;
      date.dayOfYearFormat = false;
      if (!date.hasYear) {
        date.hasYear = true;
        date.year = 2000;
      }
    }
    if (size == -1) {
      return ErrorCode::PARSE_DAY_ERROR;
    }
    cur += size;
  } else if (curPattern.specifier >= DateTimeFormatSpecifier::TIMEZONE_ID) {
    int64_t size;
    if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_ID) {
      if (type == DateTimeFormatterType::JAVA_TIME) {
        size = parseJavaTimeZoneId(cur, end, date, options.caseSensitive);
      } else {
        if (cur < end && (*cur == '+' || *cur == '-')) {
          size = parseTimezoneOffset(cur, end, date);
        } else {
          size = parseTimeZoneId(cur, end, date, true);
          if (size < 0) {
            size = parseTimezoneOffset(cur, end, date);
          }
        }
      }
    } else {
      size = parseJavaTimezoneOffset(
          curPattern, cur, end, date, options.caseSensitive);
    }
    if (size < 0) {
      return ErrorCode::PARSE_TIMEZONE_ERROR;
    }
    cur += size;
  } else {
    // Numeric specifier case
    bool negative = false;
    const char* signStart = cur;

    if (cur < end && specAllowsNegative(curPattern.specifier) && *cur == '-') {
      negative = true;
      ++cur;
    } else if (
        cur < end && specAllowsPlusSign(curPattern.specifier, specifierNext) &&
        *cur == '+') {
      negative = false;
      ++cur;
    }

    auto startPos = cur;
    int64_t number = 0;
    int maxDigitConsume = getMaxDigitConsume(curPattern, specifierNext, type);

    if (curPattern.specifier == DateTimeFormatSpecifier::FRACTION_OF_SECOND) {
      if (legacySpark && maxDigitConsume < 3) {
        // no matter how much 'S' in format, legacy spark always parse by 'SSS'
        // if maxDigitConsume > 3, will return null when digits count is large
        // then 3
        maxDigitConsume = 3;
      }
      // if digits count large than maxAcceptedDigit, we need to return an
      // error.
      int maxAcceptedDigit = legacySpark ? 3 : 9;
      int count = 0;
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
        ++count;
        if (count > maxAcceptedDigit) {
          // If we have more than 3/9 digits, we need to return an error.
          return ErrorCode::PARSE_FRACTION_ERROR;
        }
      }
      if (javaTime &&
          (count == 0 ||
           (!options.variableFraction &&
            count != curPattern.minRepresentDigits))) {
        return ErrorCode::PARSE_FRACTION_ERROR;
      }
      if constexpr (javaTime) {
        for (int i = count; i < 9; ++i) {
          number *= 10;
        }
        if (!date.record(DateTimeFormatSpecifier::FRACTION_OF_SECOND, number)) {
          return ErrorCode::DAY_CONFLICT;
        }
        if constexpr (!nanosecondPrecision) {
          number /= 1000;
        }
      } else if constexpr (nanosecondPrecision) {
        if (count == 0) {
          return ErrorCode::PARSE_FRACTION_ERROR;
        }
        for (int i = count; i < 9; ++i) {
          number *= 10;
        }
      } else if constexpr (::bytedance::bolt::kSparkCompatible) {
        // .8 + S => .8 CORRECTED
        // .8 + SS => .8 CORRECTED
        // .8 + SSS => .8 CORRECTED
        // .8 + SSSS => .8 CORRECTED
        // .80 + SSS => .8 CORRECTED
        // .800 + SSS => .8 CORRECTED
        // .1234 + SSSS => .1234 CORRECTED
        // .1234 + SSSSS => .1234 CORRECTED

        // .8 + S => .008 LEGACY
        // .8 + SS => .008 LEGACY
        // .8 + SSS => .008 LEGACY
        // .8 + SSSS => .008 LEGACY
        // .80 + SSS => .08 LEGACY
        // .800 + SSS => .8 LEGACY
        // .1234 + SSSS => null LEGACY
        // .1234 + SSSSS => null LEGACY
        if (!legacySpark) {
          number *= std::pow(10, 9 - count) / 1000;
        } else {
          number *= 1000;
        }
      } else {
        number *= std::pow(10, 3 - count);
      }
    } else if (
        (curPattern.specifier == DateTimeFormatSpecifier::YEAR ||
         curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA ||
         curPattern.specifier == DateTimeFormatSpecifier::WEEK_YEAR) &&
        curPattern.minRepresentDigits == 2) {
      // If abbreviated two year digit is provided in format string, try to read
      // in two digits of year and convert to appropriate full length year The
      // two-digit mapping is as follows: [00, 69] -> [2000, 2069]
      //                                  [70, 99] -> [1970, 1999]
      // If more than two digits are provided, then simply read in full year
      // normally without conversion
      int count = 0;
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
        ++count;
      }
      if (count == 2) {
        if (number >= 70 && !javaTime) {
          number += 1900;
        } else {
          number += 2000;
        }
      } else if (type == DateTimeFormatterType::MYSQL) {
        // In MySQL format, year read in must have exactly two digits, otherwise
        // return -1 to indicate parsing error.
        if (count > 2) {
          // Larger than expected, print suffix.
          cur = cur - count + 2;
          return ErrorCode::PARSE_YEAR_DIGIT_ERROR;
        } else {
          // Smaller than expected, print prefix.
          cur = cur - count;
          return ErrorCode::PARSE_YEAR_DIGIT_ERROR;
        }
      }
    } else {
      if (javaTime && maxDigitConsume == 2 &&
          curPattern.minRepresentDigits == 2) {
        if (end - cur < 2) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        const auto tens = static_cast<unsigned>(cur[0] - '0');
        const auto units = static_cast<unsigned>(cur[1] - '0');
        if (tens > 9 || units > 9) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        number = tens * 10 + units;
        cur += 2;
      } else if (
          legacySpark && !specifierNext &&
          (curPattern.specifier == DateTimeFormatSpecifier::DAY_OF_MONTH ||
           curPattern.specifier == DateTimeFormatSpecifier::HOUR_OF_DAY ||
           curPattern.specifier == DateTimeFormatSpecifier::MINUTE_OF_HOUR ||
           curPattern.specifier == DateTimeFormatSpecifier::SECOND_OF_MINUTE)) {
        // Spark LEGACY tolerance: for width-2 numeric fields followed by a
        // literal, allow exactly one extra leading '0'. Consume 3 digits when
        // present and use the last two digits as the value.
        const char* p = cur;
        int len = 0;
        while (p < end && characterIsDigit(*p)) {
          ++p;
          ++len;
        }
        if (len == 1) {
          number = cur[0] - '0';
          cur += 1;
        } else if (len == 2) {
          number = (cur[0] - '0') * 10 + (cur[1] - '0');
          cur += 2;
        } else if (len == 3 && cur[0] == '0') {
          number = (cur[1] - '0') * 10 + (cur[2] - '0');
          cur += 3;
        } else {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
      } else {
        while (cur < end && cur < startPos + maxDigitConsume &&
               characterIsDigit(*cur)) {
          number = number * 10 + (*cur - '0');
          ++cur;
        }
      }
    }

    // Need to have read at least one digit.
    if (cur <= startPos) {
      return ErrorCode::PARSE_DIGIT_ERROR;
    }

    if constexpr (javaTime) {
      const auto digits = cur - startPos;
      const auto width = curPattern.minRepresentDigits;
      const bool year = curPattern.specifier == DateTimeFormatSpecifier::YEAR ||
          curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA;
      const bool fraction =
          curPattern.specifier == DateTimeFormatSpecifier::FRACTION_OF_SECOND;
      if (!fraction && width > 1 && digits < width) {
        return ErrorCode::PARSE_DIGIT_ERROR;
      }
      if (year &&
          ((width == 2 && signStart != startPos) ||
           (width < 4 && signStart != startPos && !negative) ||
           (width >= 4 && !negative &&
            (digits > width) != (signStart != startPos)) ||
           (negative && number == 0))) {
        return ErrorCode::PARSE_YEAR_DIGIT_ERROR;
      }
    }
    if (negative) {
      number *= -1L;
    }

    if constexpr (javaTime) {
      auto field = curPattern.specifier;
      auto value = number;
      if (field == DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY) {
        field = DateTimeFormatSpecifier::HOUR_OF_DAY;
        value %= 24;
      } else if (field == DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY) {
        field = DateTimeFormatSpecifier::HOUR_OF_HALFDAY;
        value %= 12;
      }
      if (field != DateTimeFormatSpecifier::FRACTION_OF_SECOND &&
          !date.record(field, value)) {
        return ErrorCode::DAY_CONFLICT;
      }
    }

    switch (curPattern.specifier) {
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
        // Enforce Joda's year range if year was specified as "century of year".
        if (number < 0 || number > 2922789) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        date.centuryFormat = true;
        date.year = number * 100;
        date.hasYear = true;
        break;

      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
        date.centuryFormat = false;
        date.isYearOfEra =
            (curPattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA);
        // Enforce Joda's year range if year was specified as "year of era".
        if (date.isYearOfEra && (number > 292278993 || number < 1)) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        // Enforce Joda's year range if year was specified as "year".
        if (!date.isYearOfEra && (number > 292278994 || number < -292275055)) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        if (javaTime && date.hasYear && date.year != number) {
          return ErrorCode::DAY_CONFLICT;
        }
        date.hasYear = true;
        date.year = number;
        break;

      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
        if (number < 1 || number > 12) {
          return ErrorCode::MONTH_OUT_OF_RANGE;
        }
        date.month = number;
        date.hasMonth = true;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear && !javaTime) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_MONTH:
        if constexpr (javaTime) {
          if (date.hasDay && date.day != number) {
            return ErrorCode::DAY_CONFLICT;
          }
          date.hasDay = true;
        } else {
          date.dayOfMonthValues.push_back(number);
        }
        date.day = number;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear && !javaTime) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_YEAR:
        if constexpr (javaTime) {
          if (date.hasDayOfYear && date.dayOfYear != number) {
            return ErrorCode::DAY_CONFLICT;
          }
          date.hasDayOfYear = true;
        } else {
          date.dayOfYearValues.push_back(number);
        }
        date.dayOfYear = number;
        date.dayOfYearFormat = true;
        date.weekDateFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear && !javaTime) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
        if (number > 24 || number < 1) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = false;
        date.hour = number % 24;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_DAY:
        if (number > 23 || number < 0) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = false;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
        if (number > 12 || number < 1) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        date.isClockHour = true;
        date.isHourOfHalfDay = true;
        date.hour = number % 12;
        break;

      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
        if (number > 11 || number < 0) {
          return ErrorCode::HOUR_OUT_OF_RANGE;
        }
        date.isClockHour = false;
        date.isHourOfHalfDay = true;
        date.hour = number;
        break;

      case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
        if (number > 59 || number < 0) {
          return ErrorCode::MINUTE_OUT_OF_RANGE;
        }
        date.minute = number;
        break;

      case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
        if (number > 59 || number < 0) {
          return ErrorCode::MINUTE_OUT_OF_RANGE;
        }
        date.second = number;
        break;

      case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
        if constexpr (nanosecondPrecision) {
          date.subsecond = number;
        } else {
          date.subsecond = javaTime || ::bytedance::bolt::kSparkCompatible
              ? number
              : number * util::kMicrosPerMsec;
        }
        break;

      case DateTimeFormatSpecifier::WEEK_YEAR:
        // Enforce Joda's year range if year was specified as "week year".
        if (number < -292275054 || number > 292278993) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        date.year = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        date.centuryFormat = false;
        date.hasYear = true;
        break;

      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
        if (number < 1 || number > 52) {
          return ErrorCode::WEEK_OUT_OF_RANGE;
        }
        date.week = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        if (!date.hasYear && !javaTime) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::WEEK_OF_MONTH:
        if (!::bytedance::bolt::kSparkCompatible) {
          BOLT_NYI(
              "Numeric Joda specifier DateTimeFormatSpecifier::" +
              getSpecifierName(curPattern.specifier) + " not implemented yet.");
        }
        if (number < 1 || number > 5) {
          return ErrorCode::WEEK_OUT_OF_RANGE;
        }
        date.weekInMonth = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        break;

      case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
        if constexpr (javaTime) {
          if (number < 1 || number > 7) {
            return ErrorCode::DAY_OUT_OF_RANGE;
          }
          break;
        }
        if (!::bytedance::bolt::kSparkCompatible) {
          BOLT_NYI(
              "Numeric Joda specifier DateTimeFormatSpecifier::" +
              getSpecifierName(curPattern.specifier) + " not implemented yet.");
        }
        if (number < 1 || number > 5) {
          return ErrorCode::WEEK_OUT_OF_RANGE;
        }
        date.dayOfWeekInMonth = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        break;

      case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
        if (number < 1 || number > 7) {
          return ErrorCode::DAY_OUT_OF_RANGE;
        }
        date.dayOfWeek = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        if (!date.hasYear && !javaTime) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      default:
        BOLT_NYI(
            "Numeric Joda specifier DateTimeFormatSpecifier::" +
            getSpecifierName(curPattern.specifier) + " not implemented yet.");
    }
  }
  return ErrorCode::NO_ERROR;
}

} // namespace

DateTimeResult DateTimeFormatter::parseToUtc(
    const std::string_view& input,
    const tz::TimeZone* timezone,
    TimePolicy timePolicy) const {
  const auto* rebaseZone = context_ && context_->rebaseTimezone
      ? context_->rebaseTimezone
      : timezone;
  const bool simple = type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT;
  auto parsed = simple ? parseSimpleDateFormat(input, timezone) : parse(input);
  if (parsed.hasError()) {
    return parsed;
  }
  auto timestamp = parsed.value().timestamp;
  if (auto offset = parsed.timezoneOffsetSeconds()) {
    timestamp =
        Timestamp(timestamp.getSeconds() - *offset, timestamp.getNanos());
  } else {
    if (parsed.value().timezoneId >= 0) {
      timezone = tz::locateZone(parsed.value().timezoneId);
    }
    if (simple) {
      const auto local = timestamp;
      bool shifted;
      timestamp = detail::simpleDateTimeToUtc(
          timestamp, timezone, context_.get(), &shifted);
      if (parseOptions_.strict && shifted) {
        const auto normalized =
            detail::simpleDateTimeFromUtc(timestamp, timezone, context_.get());
        auto& calendar = detail::simpleDateCalendar();
        UErrorCode status = U_ZERO_ERROR;
        std::array<int32_t, UCAL_FIELD_COUNT> original{};
        calendar.setTime(local.toMillis(), status);
        for (int32_t field = 0; field < UCAL_FIELD_COUNT; ++field) {
          original[field] =
              calendar.get(static_cast<UCalendarDateFields>(field), status);
        }
        calendar.setTime(normalized.toMillis(), status);
        for (const auto& token : tokens_) {
          if (token.type != DateTimeToken::Type::kPattern) {
            continue;
          }
          const auto field =
              detail::simpleCalendarField(token.pattern.specifier);
          if (field >= 0 &&
              calendar.get(static_cast<UCalendarDateFields>(field), status) !=
                  original[field]) {
            return ErrorCode::PARSE_TIMEZONE_ERROR;
          }
        }
        if (U_FAILURE(status)) {
          return ErrorCode::PARSE_TIMEZONE_ERROR;
        }
      }
    } else if (timezone && timezone->id() != 0) {
      auto local = std::chrono::seconds(timestamp.getSeconds());
      if (local.count() < -1'096'193'692'800LL && !timezone->offset()) {
        return ErrorCode::YEAR_OUT_OF_RANGE;
      }
      timestamp = Timestamp(
          detail::resolveLocalTime(local.count(), timezone, false).first,
          timestamp.getNanos());
    }
  }
  if (simple && timePolicy == TimePolicy::LEGACY) {
    timestamp =
        detail::rebaseDateTime(timestamp, rebaseZone, true, context_.get());
  }
  return DateTimeResult{timestamp, 0};
}

DateTimeResult DateTimeFormatter::parse(const std::string_view& input) const {
  if (type_ == DateTimeFormatterType::JAVA_TIME) {
    return parseImpl<false, true>(input, TimePolicy::CORRECTED);
  }
  if (UNLIKELY(type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT)) {
    return parseSimpleDateFormat(input);
  }
  DateWithRepeatedFields date;
  const char* cur = input.data();
  const char* end = cur + input.size();

  for (int i = 0; i < tokens_.size(); i++) {
    auto& tok = tokens_[i];
    switch (tok.type) {
      case DateTimeToken::Type::kLiteral:
        if (tok.literal.size() > end - cur ||
            std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
          return ErrorCode::PARSE_LITERAL_ERROR;
        }
        cur += tok.literal.size();
        break;
      case DateTimeToken::Type::kPattern: {
        const bool specifierNext = i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern;
        const auto errorCode = parseFromPattern(
            tok.pattern, input, cur, end, date, specifierNext, type_);
        if (errorCode != ErrorCode::NO_ERROR) {
          return errorCode;
        }
      } break;
    }
  }

  if ((type_ == DateTimeFormatterType::JAVA_TIME || date.hasTimeZoneId) &&
      cur != end) {
    return ErrorCode::PARSE_LITERAL_ERROR;
  }

  // Era is BC and year of era is provided
  if (date.isYearOfEra && !date.isAd) {
    date.year = -1 * (date.year - 1);
  }

  if (date.isHourOfHalfDay) {
    if (!date.isAm) {
      date.hour += 12;
    }
  }

  // Ensure all day of month values are valid for ending month value
  for (int i = 0; i < date.dayOfMonthValues.size(); i++) {
    if (!util::isValidDate(date.year, date.month, date.dayOfMonthValues[i])) {
      return ErrorCode::DAY_OUT_OF_RANGE;
    }
  }

  // Ensure all day of year values are valid for ending year value
  for (int i = 0; i < date.dayOfYearValues.size(); i++) {
    if (!util::isValidDayOfYear(date.year, date.dayOfYearValues[i])) {
      return ErrorCode::DAY_OUT_OF_RANGE;
    }
  }

  // Convert the parsed date/time into a timestamp.
  int64_t daysSinceEpoch;
  if (date.weekDateFormat) {
    daysSinceEpoch =
        util::daysSinceEpochFromWeekDate(date.year, date.week, date.dayOfWeek);
  } else if (date.dayOfYearFormat) {
    daysSinceEpoch =
        util::daysSinceEpochFromDayOfYear(date.year, date.dayOfYear);
  } else {
    daysSinceEpoch =
        util::daysSinceEpochFromDate(date.year, date.month, date.day);
  }

  int64_t microsSinceMidnight =
      util::fromTime(date.hour, date.minute, date.second, date.subsecond);
  return DateTimeResult{
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight),
      date.timezoneId,
      date.timezoneOffsetSeconds};
}

// sql.legacy.timeParserPolicy = "legacy", "corrected", "exception"
template <bool nanosecondPrecision, bool javaTime>
FOLLY_ALWAYS_INLINE DateTimeResult DateTimeFormatter::parseImpl(
    const std::string_view& input,
    const TimePolicy timeParserPolicy) const {
  std::conditional_t<javaTime, JavaDate, DateWithRepeatedFields> date;
  const char* cur = input.data();
  const char* end = cur + input.size();

  bool isLegacy = (timeParserPolicy == TimePolicy::LEGACY) && !javaTime;
  // FIXME: for timezone with +HHmm or +HH:mm, we current get different result
  // with spark
  auto noError = [&](const ErrorCode& errorCode) {
    return (errorCode == ErrorCode::NO_ERROR);
  };

  for (int i = 0; i < tokens_.size(); i++) {
    auto& tok = tokens_[i];
    switch (tok.type) {
      case DateTimeToken::Type::kLiteral:
        if (tok.literal.size() == 1 && tok.literal[0] == ' ' && isLegacy) {
          // Spark LEGACY: tolerate multiple spaces where a single space is
          // specified. Require at least one space to be present.
          if (cur >= end || *cur != ' ') {
            return DateTimeResult::ErrorCode::PARSE_LITERAL_ERROR;
          }
          while (cur < end && *cur == ' ') {
            ++cur;
          }
        } else {
          if (tok.literal.size() > end - cur ||
              (std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0 &&
               (!javaTime || parseOptions_.caseSensitive ||
                !folly::StringPiece(cur, tok.literal.size())
                     .equals(tok.literal, folly::AsciiCaseInsensitive())))) {
            return DateTimeResult::ErrorCode::PARSE_LITERAL_ERROR;
          }
          cur += tok.literal.size();
        }
        break;
      case DateTimeToken::Type::kPattern:
        // Spark LEGACY: skip leading whitespace before numeric tokens
        if (isLegacy) {
          // Tolerate multiple spaces only; tabs are not accepted.
          while (cur < end && (*cur == ' ')) {
            ++cur;
          }
        }
        {
          const bool specifierNext = i + 1 < tokens_.size() &&
              tokens_[i + 1].type == DateTimeToken::Type::kPattern;
          const auto errorCode =
              parseFromPattern<nanosecondPrecision, javaTime>(
                  tok.pattern,
                  input,
                  cur,
                  end,
                  date,
                  specifierNext,
                  javaTime ? DateTimeFormatterType::JAVA_TIME : type_,
                  isLegacy,
                  parseOptions_);
          if (!noError(errorCode)) {
            return errorCode;
          }
        }
        break;
    }
  }

  // in legacy policy, return valid value if pattern is short than date string
  if (!isLegacy && cur != end) {
    return DateTimeResult::ErrorCode::PATTERN_TOO_SHORT;
  }

  if ((javaTime || date.hasTimeZoneId) && cur != end) {
    return ErrorCode::PARSE_LITERAL_ERROR;
  }

  // Era is BC and year of era is provided
  if (date.isYearOfEra && !date.isAd) {
    date.year = -1 * (date.year - 1);
  }

  if constexpr (javaTime) {
    if (date.has(DateTimeFormatSpecifier::HOUR_OF_DAY)) {
      date.hour = date.value(DateTimeFormatSpecifier::HOUR_OF_DAY);
      date.isHourOfHalfDay = false;
      if (date.has(DateTimeFormatSpecifier::HOUR_OF_HALFDAY) &&
          date.hour % 12 !=
              date.value(DateTimeFormatSpecifier::HOUR_OF_HALFDAY)) {
        return ErrorCode::DAY_CONFLICT;
      }
    }
  }
  if (javaTime && date.hasHalfDay && !date.isHourOfHalfDay &&
      (date.hour < 12) != date.isAm) {
    return ErrorCode::DAY_CONFLICT;
  }
  if (date.isHourOfHalfDay) {
    if (!date.isAm) {
      date.hour += 12;
    }
  }

  if (javaTime && !parseOptions_.strict && date.day > 0 && date.day <= 31 &&
      !date.dayOfYearFormat) {
    while (!util::isValidDate(date.year, date.month, date.day)) {
      --date.day;
    }
  }

  auto isJulianSpecialYear = [](int32_t year) {
    return (year >= util::kMinYear) && (year < util::kJulianEndYear) &&
        (year % 100 == 0) && (year % 400 != 0);
  };
  auto isJulianSpecialDate = [&](int32_t year, int32_t month, int32_t day) {
    return isJulianSpecialYear(year) && (month == 2) && (day == 29);
  };
  auto isJulianSpecialDay = [&](int32_t year, int32_t day) {
    return isJulianSpecialYear(year) && day <= 366;
  };

  if constexpr (javaTime) {
    if ((date.hasDay && !util::isValidDate(date.year, date.month, date.day)) ||
        (date.hasDayOfYear &&
         !util::isValidDayOfYear(date.year, date.dayOfYear))) {
      return ErrorCode::DAY_OUT_OF_RANGE;
    }
  } else {
    // Ensure all day of month values are valid for ending month value
    for (int i = 0; i < date.dayOfMonthValues.size(); i++) {
      if (!util::isValidDate(date.year, date.month, date.dayOfMonthValues[i])) {
        // LEGACY policy handle days like '1400-02-29'
        if (!(isLegacy &&
              isJulianSpecialDate(
                  date.year, date.month, date.dayOfMonthValues[i]))) {
          return DateTimeResult::ErrorCode::DAY_OUT_OF_RANGE;
        }
      }
      if (!isLegacy && date.dayOfMonthValues[i] != date.dayOfMonthValues[0]) {
        // CORRECTED policy need all days are same day
        return DateTimeResult::ErrorCode::DAY_CONFLICT;
      }
    }

    // Ensure all day of year values are valid for ending year value
    for (int i = 0; i < date.dayOfYearValues.size(); i++) {
      if (!util::isValidDayOfYear(date.year, date.dayOfYearValues[i])) {
        if (!(isLegacy &&
              isJulianSpecialDay(date.year, date.dayOfYearValues[i]))) {
          return DateTimeResult::ErrorCode::DAY_OUT_OF_RANGE;
        }
      }
      if (!isLegacy && date.dayOfYearValues[i] != date.dayOfYearValues[0]) {
        // CORRECTED policy need all days are same day
        return DateTimeResult::ErrorCode::DAY_CONFLICT;
      }
    }
  }

  if (javaTime && date.hasDayOfYear) {
    const auto day = date.dayOfYear;
    const auto resolved =
        util::toCivilDateTime(
            Timestamp(
                util::daysSinceEpochFromDayOfYear(date.year, day) *
                    util::kSecsPerDay,
                0))
            .date;
    if ((date.hasMonth && date.month != resolved.month) ||
        (date.hasDay && date.day != resolved.day)) {
      return ErrorCode::DAY_CONFLICT;
    }
    date.dayOfYear = day;
    date.dayOfYearFormat = true;
  }

  auto adjustDayOfYearForLegacy = [&](int32_t year, int32_t dayOfYear) {
    if (year == util::kJulianEndYear && dayOfYear >= 278) {
      // LEGACY yyyy-DD 1582-277 ==> 1500-10-04, 1582-278 ==> 1500-10-15
      return dayOfYear + 10;
    } else if (isJulianSpecialYear(year) && dayOfYear >= 61) {
      // LEGACY yyyy-DD 1500-60 ==> 1500-03-01, 1500-278 ==> 1500-03-01
      return dayOfYear - 1;
    }
    return dayOfYear;
  };

  // Convert the parsed date/time into a timestamp.
  int64_t daysSinceEpoch;
  if (date.weekDateFormat) {
    if (isLegacy) {
      // week year format

      // spark LEGACY week format is different with JodaTime, JodaTime use
      // ISO-8601 and monday as start, spark LEGACY use
      // java.text.SimpleDateFormat and sunday as start, For 2021 week 1 day
      // 5, JodaTime return 2021-01-08, spark LEGACY return 2021-01-01
      if (date.year > util::kJulianEndYear) {
        if (date.weekInMonth || date.dayOfWeekInMonth) {
          // week of month or day of week in month pattern
          auto daySinceEpochOfMonthFirstDay =
              util::daysSinceEpochFromDate(date.year, date.month, 1);
          int32_t weekDaysOfMonthFirstDay =
              util::extractISODayOfTheWeek(daySinceEpochOfMonthFirstDay);
          int32_t daysInMonth = 0;
          if (date.weekInMonth) {
            daysInMonth = (date.weekInMonth.value() - 1) * 7 +
                (date.dayOfWeek % 7) - weekDaysOfMonthFirstDay % 7;
          } else if (date.dayOfWeekInMonth) {
            daysInMonth = 7 * (date.dayOfWeekInMonth.value() - 1) +
                ((date.dayOfWeek - weekDaysOfMonthFirstDay % 7 + 7) % 7);
          }
          if (!util::isValidDate(date.year, date.month, daysInMonth + 1)) {
            return ErrorCode::DAY_OUT_OF_RANGE;
          }
          daysSinceEpoch = daySinceEpochOfMonthFirstDay + daysInMonth;
        } else {
          int64_t daysSinceEpochOfJanFirst =
              util::daysSinceEpochFromDate(date.year, 1, 1);
          int32_t firstDayOfWeekYear =
              util::extractISODayOfTheWeek(daysSinceEpochOfJanFirst);
          daysSinceEpoch = daysSinceEpochOfJanFirst - (firstDayOfWeekYear % 7) +
              7 * (date.week - 1) + (date.dayOfWeek % 7);
        }
      } else {
        int64_t weekdayOfJanFirst = 1;
        int64_t daysTo1582_01_01 = (util::kJulianEndYear - date.year) * 365 +
            (util::kJulianEndYear / 4 - (date.year - 1) / 4);

        int64_t dayOfYear = 0;
        if (date.weekInMonth || date.dayOfWeekInMonth) {
          dayOfYear = util::daysBeforeMonthFirstDay(date.year, date.month);
          daysTo1582_01_01 += dayOfYear;
          int64_t weekDaysOfMonthFirstDay =
              (7 + (weekdayOfJanFirst - daysTo1582_01_01) % 7) % 7;
          int daysInMonth = 0;
          if (date.weekInMonth) {
            daysInMonth = (date.weekInMonth.value() - 1) * 7 +
                (date.dayOfWeek % 7) - weekDaysOfMonthFirstDay;
          } else if (date.dayOfWeekInMonth) {
            daysInMonth = 7 * (date.dayOfWeekInMonth.value() - 1) +
                ((date.dayOfWeek - weekDaysOfMonthFirstDay) % 7);
          }
          if (!util::isValidDate(date.year, date.month, daysInMonth + 1)) {
            return ErrorCode::DAY_OUT_OF_RANGE;
          }
          dayOfYear += daysInMonth;
        } else {
          // sunday seen as 0
          int64_t weekdaysOfYearStart =
              (7 + (weekdayOfJanFirst - daysTo1582_01_01) % 7) % 7;
          dayOfYear = (date.week - 1) * 7 + (date.dayOfWeek % 7) -
              weekdaysOfYearStart + 1;
        }
        dayOfYear = adjustDayOfYearForLegacy(date.year, dayOfYear);
        if (dayOfYear <= 0) {
          return ErrorCode::DAY_OUT_OF_RANGE;
        }
        daysSinceEpoch =
            util::daysSinceEpochFromDayOfYear(date.year, dayOfYear);
      }
    } else {
      // All week-based patterns are unsupported in CORRECTED policy
      return ErrorCode::UNSUPPORT_WEEK_FORMAT;
    }
  } else if (date.dayOfYearFormat) {
    if (isLegacy) {
      date.dayOfYear = adjustDayOfYearForLegacy(date.year, date.dayOfYear);
    }
    daysSinceEpoch =
        util::daysSinceEpochFromDayOfYear(date.year, date.dayOfYear);
  } else {
    if (isLegacy) {
      if (date.year == util::kJulianEndYear && date.month == 10 &&
          (date.day >= 5 && date.day <= 14)) {
        return ErrorCode::LEGACY_INVALID_DAY;
      } else if (
          isJulianSpecialYear(date.year) && date.month == 2 && date.day == 29) {
        // LEGACY yyyy-MM-dd 1500-02-29 ==> 1500-03-01
        date.month = 3;
        date.day = 1;
      }
    }
    daysSinceEpoch =
        util::daysSinceEpochFromDate(date.year, date.month, date.day);
  }

  if constexpr (javaTime) {
    if (date.has(DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED) &&
        ::date::weekday{::date::sys_days{::date::days{daysSinceEpoch}}}
                .iso_encoding() !=
            date.value(DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED)) {
      return ErrorCode::DAY_CONFLICT;
    }
    if (date.has(DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH)) {
      const auto day = util::toCivilDateTime(
                           Timestamp(daysSinceEpoch * util::kSecsPerDay, 0))
                           .date.day;
      if ((day - 1) % 7 + 1 !=
          date.value(DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH)) {
        return ErrorCode::DAY_CONFLICT;
      }
    }
  }

  if constexpr (nanosecondPrecision) {
    return DateTimeResult{
        Timestamp(
            daysSinceEpoch * util::kSecsPerDay + date.hour * 3600 +
                date.minute * 60 + date.second,
            date.subsecond),
        date.timezoneId,
        date.timezoneOffsetSeconds};
  }
  int64_t microsSinceMidnight =
      util::fromTime(date.hour, date.minute, date.second, date.subsecond);
  return DateTimeResult{
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight),
      date.timezoneId,
      date.timezoneOffsetSeconds};
}

DateTimeResult DateTimeFormatter::parse(
    const std::string_view& input,
    TimePolicy timeParserPolicy) const {
  if (UNLIKELY(type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT)) {
    return parseSimpleDateFormat(input);
  }
  return type_ == DateTimeFormatterType::JAVA_TIME
      ? parseImpl<false, true>(input, TimePolicy::CORRECTED)
      : parseImpl<false, false>(input, timeParserPolicy);
}

DateTimeResult DateTimeFormatter::parseWithNanosecondPrecision(
    const std::string_view& input) const {
  if (UNLIKELY(type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT)) {
    return parseSimpleDateFormat(input);
  }
  return type_ == DateTimeFormatterType::JAVA_TIME
      ? parseImpl<true, true>(input, TimePolicy::CORRECTED)
      : parseImpl<true, false>(input, TimePolicy::CORRECTED);
}

} // namespace bytedance::bolt::functions
