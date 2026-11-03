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
#include <unicode/gregocal.h>
#include <unicode/timezone.h>
#include <unicode/uchar.h>
#include <unicode/utf8.h>
#include <array>
#include <charconv>
#include <cmath>
#include <cstring>
#include <stdexcept>

#include "bolt/common/base/CountBits.h"
#include "bolt/functions/lib/DateTimeFormatterBuilder.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"
#include "bolt/type/tz/TimeZoneNames.h"

namespace bytedance::bolt::tz {
// Defined in TimeZoneLinks.cpp
extern const std::unordered_map<std::string, std::string>& getTimeZoneLinks();
} // namespace bytedance::bolt::tz

namespace bytedance::bolt::functions {

static thread_local std::string timezoneBuffer = "+00:00";
static const char* defaultTrailingOffset = "00";

auto format_as(TimePolicy tp) {
  return fmt::underlying(tp);
}
namespace {

struct Date {
  int32_t year = 1970;
  int32_t month = 1;
  int32_t day = 1;
  bool isAd = true; // AD -> true, BC -> false.

  int32_t week = 1;
  int32_t dayOfWeek = 1;
  bool weekDateFormat = false;

  int32_t dayOfYear = 1;
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
  int64_t timezoneId = -1;
  std::optional<int32_t> timezoneOffsetSeconds;
  bool hasTimeZoneId = false;

  bool isClockHour = false; // Whether most recent hour specifier is clockhour
  bool isHourOfHalfDay =
      true; // Whether most recent hour specifier is of half day.

  std::vector<int32_t> dayOfMonthValues;
  std::vector<int32_t> dayOfYearValues;
};

constexpr std::string_view weekdaysFull[] = {
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"};
constexpr std::string_view weekdaysShort[] =
    {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

constexpr bool isSparkOnlyFormatSpecifier(DateTimeFormatSpecifier specifier) {
  switch (specifier) {
    case DateTimeFormatSpecifier::WEEK_YEAR:
    case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
    case DateTimeFormatSpecifier::WEEK_OF_MONTH:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      return true;
    default:
      return false;
  }
}

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

constexpr std::string_view monthsFull[] = {
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
constexpr std::string_view monthsShort[] = {
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

// Pads the content with desired padding characters. E.g. if we need to pad 999
// with three 0s in front, the result will be '000999'.
// @param content the content that is going to be padded.
// @param padding the padding that is going to be used to pad the content.
// @param totalDigits the total number of digits the padded result is desired
// to be. If totalDigits is already smaller than content length, the original
// content will be returned with no padding.
// @param maxResultEnd the end pointer to result.
// @param result the pointer to string result.
// @param padFront if the padding is in front of the content or back of the
// content.
template <typename T>
int32_t padContent(
    const T& content,
    char padding,
    const size_t totalDigits,
    char* maxResultEnd,
    char* result,
    const bool padFront = true) {
  const bool isNegative = content < 0;
  const auto digitLength =
      isNegative ? countDigits(-(__int128_t)content) : countDigits(content);
  const auto contentLength = isNegative ? digitLength + 1 : digitLength;
  if (contentLength == 0) {
    std::fill(result, result + totalDigits, padding);
    return totalDigits;
  }

  std::to_chars_result toStatus;
  if (digitLength >= totalDigits) {
    toStatus = std::to_chars(result, maxResultEnd, content);
    return toStatus.ptr - result;
  }
  const auto paddingSize = totalDigits - digitLength;
  if (padFront) {
    if (isNegative) {
      *result = '-';
      std::fill(result + 1, result + 1 + paddingSize, padding);
      toStatus =
          std::to_chars(result + 1 + paddingSize, maxResultEnd, -content);
    } else {
      std::fill(result, result + paddingSize, padding);
      toStatus = std::to_chars(result + paddingSize, maxResultEnd, content);
    }
    return toStatus.ptr - result;
  }
  toStatus = std::to_chars(result, maxResultEnd, content);
  std::fill(toStatus.ptr, toStatus.ptr + paddingSize, padding);
  return toStatus.ptr - result + paddingSize;
}

size_t countOccurence(const std::string_view& base, const std::string& target) {
  int occurrences = 0;
  std::string::size_type pos = 0;
  while ((pos = base.find(target, pos)) != std::string::npos) {
    ++occurrences;
    pos += target.length();
  }
  return occurrences;
}

int64_t numLiteralChars(
    // Counts the number of literal characters until the next closing literal
    // sequence single quote.
    const char* cur,
    const char* end) {
  int64_t count = 0;
  while (cur < end) {
    if (*cur == '\'') {
      if (cur + 1 < end && *(cur + 1) == '\'') {
        count += 2;
        cur += 2;
      } else {
        return count;
      }
    } else {
      ++count;
      ++cur;
      // No end literal single quote found
      if (cur == end) {
        return -1;
      }
    }
  }
  return count;
}

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
        auto it = result.first.try_emplace(name, std::pair{id, id}).first;
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
      result.first.at(name).first = -1;
    }
    for (int hour = -14; hour <= 12; ++hour) {
      if (hour != 0) {
        add(fmt::format("Etc/GMT{:+}", hour), tz::getTimeZoneID(-hour * 60));
      }
    }
    return result;
  }();
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
    Date& date) {
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
    if (end - cur < 3 || std::memcmp(cur, "GMT", 3) != 0) {
      return -1;
    }
    cur += 3;
    if (cur == end || (*cur != '+' && *cur != '-')) {
      setOffset(0);
      return cur - start;
    }
  } else if (zeroAsZ && cur < end && *cur == 'Z') {
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

int64_t parseJavaTimeZoneId(const char* cur, const char* end, Date& date) {
  const auto offsetPattern =
      FormatPattern{DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601, 5};
  if (cur < end && (*cur == '+' || *cur == '-')) {
    return parseJavaTimezoneOffset(offsetPattern, cur, end, date);
  }
  for (std::string_view prefix : {"UTC", "GMT", "UT"}) {
    if (end - cur >= prefix.size() &&
        std::string_view(cur, prefix.size()) == prefix) {
      const auto* offset = cur + prefix.size();
      if (offset < end && (*offset == '+' || *offset == '-')) {
        auto size = parseJavaTimezoneOffset(offsetPattern, offset, end, date);
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
  if (size < 0 && cur < end && *cur == 'Z') {
    date.timezoneId = 0;
    date.timezoneOffsetSeconds = 0;
    return 1;
  }
  return size;
}

int32_t
appendJavaTimezoneOffset(int64_t offset, FormatPattern pattern, char* result) {
  const auto* start = result;
  const bool rfc =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822;
  const auto digits = pattern.minRepresentDigits;
  const bool zeroAsZ =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601 ||
      (rfc && digits == 5);
  if (rfc && digits == 4) {
    std::memcpy(result, "GMT", 3);
    result += 3;
    if (offset == 0) {
      return 3;
    }
  } else if (zeroAsZ && offset == 0) {
    *result = 'Z';
    return 1;
  }
  *result++ = offset < 0 ? '-' : '+';
  offset = std::abs(offset);
  auto append = [&](int32_t number) {
    *result++ = '0' + number / 10;
    *result++ = '0' + number % 10;
  };
  append(offset / 3600);
  const auto minutes = offset / 60 % 60;
  const bool colon = rfc ? digits >= 4 : digits == 3 || digits == 5;
  if (rfc || digits != 1 || minutes != 0) {
    if (colon) {
      *result++ = ':';
    }
    append(minutes);
    const auto seconds = offset % 60;
    if (digits >= 4 && seconds != 0) {
      if (colon) {
        *result++ = ':';
      }
      append(seconds);
    }
  }
  return result - start;
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

std::string formatFractionOfSecond(
    int64_t subseconds,
    size_t minRepresentDigits) {
  constexpr int64_t KBase = 1'000'000'000LL;
  // subseconds range from [0, 999 '999' 999L].
  // Adding KBase can align the prefix 0
  auto subsecondsStr = std::to_string(subseconds + KBase);
  return subsecondsStr.substr(1, minRepresentDigits);
}

int32_t appendTimezoneOffset(int64_t offset, char* result, bool includeColon) {
  int pos = 0;
  if (offset >= 0) {
    result[pos++] = '+';
  } else {
    result[pos++] = '-';
    offset = -offset;
  }

  const auto hours = offset / 60 / 60;
  if (hours < 10) {
    result[pos++] = '0';
    result[pos++] = char(hours + '0');
  } else {
    result[pos++] = char(hours / 10 + '0');
    result[pos++] = char(hours % 10 + '0');
  }

  if (includeColon) {
    result[pos++] = ':';
  }

  const auto minutes = (offset / 60) % 60;
  if LIKELY (minutes == 0) {
    result[pos++] = '0';
    result[pos++] = '0';
  } else if (minutes < 10) {
    result[pos++] = '0';
    result[pos++] = char(minutes + '0');
  } else {
    result[pos++] = char(minutes / 10 + '0');
    result[pos++] = char(minutes % 10 + '0');
  }

  const auto seconds = offset % 60;
  if (seconds > 0) {
    result[pos++] = ':';

    if (seconds < 10) {
      result[pos++] = '0';
      result[pos++] = char(seconds + '0');
    } else {
      result[pos++] = char(seconds / 10 + '0');
      result[pos++] = char(seconds % 10 + '0');
    }
  }

  return pos;
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

int getMaxDigitConsume(
    FormatPattern curPattern,
    bool specifierNext,
    DateTimeFormatterType type) {
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

template <bool nanosecondPrecision = false>
ErrorCode parseFromPattern(
    FormatPattern curPattern,
    const std::string_view& input,
    const char*& cur,
    const char* end,
    Date& date,
    bool specifierNext,
    DateTimeFormatterType type,
    bool legacySpark = false) {
  if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID) {
    auto size = parseTimezoneOffset(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_TIMEZONE_OFFSET_ERROR;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE) {
    auto size = parseTimezone(cur, end, date, legacySpark);
    if (size == -1) {
      return ErrorCode::PARSE_TIMEZONE_ERROR;
    }
    cur += size;
  } else if (curPattern.specifier == DateTimeFormatSpecifier::ERA) {
    auto size = parseEra(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_ERA_ERROR;
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT) {
    auto size = parseMonthText(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_MONTH_ERROR;
    }
    cur += size;
    if (!date.hasYear) {
      date.hasYear = true;
      date.year = 2000;
    }
  } else if (curPattern.specifier == DateTimeFormatSpecifier::HALFDAY_OF_DAY) {
    auto size = parseHalfDayOfDay(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_HALFDAY_ERROR;
    }
    if (::bytedance::bolt::kSparkCompatible) {
      date.isHourOfHalfDay = true; // for get_timestamp("PM", "a") case
    }
    cur += size;
  } else if (
      curPattern.specifier == DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT) {
    auto size = parseDayOfWeekText(cur, end, date);
    if (size == -1) {
      return ErrorCode::PARSE_DAY_ERROR;
    }
    cur += size;
    date.weekDateFormat = true;
    date.dayOfYearFormat = false;
    if (!date.hasYear) {
      date.hasYear = true;
      date.year = 2000;
    }
  } else if (curPattern.specifier >= DateTimeFormatSpecifier::TIMEZONE_ID) {
    int64_t size;
    if (curPattern.specifier == DateTimeFormatSpecifier::TIMEZONE_ID) {
      if (type == DateTimeFormatterType::JAVA_TIME) {
        size = parseJavaTimeZoneId(cur, end, date);
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
      size = parseJavaTimezoneOffset(curPattern, cur, end, date);
    }
    if (size < 0) {
      return ErrorCode::PARSE_TIMEZONE_ERROR;
    }
    cur += size;
  } else {
    // Numeric specifier case
    bool negative = false;

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
      if constexpr (nanosecondPrecision) {
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
        if (number >= 70) {
          number += 1900;
        } else if (number >= 0 && number < 70) {
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
      if (legacySpark && !specifierNext &&
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

    if (negative) {
      number *= -1L;
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
        date.hasYear = true;
        date.year = number;
        break;

      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
        if (number < 1 || number > 12) {
          return ErrorCode::MONTH_OUT_OF_RANGE;
        }
        date.month = number;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_MONTH:
        date.dayOfMonthValues.push_back(number);
        date.day = number;
        date.weekDateFormat = false;
        date.dayOfYearFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
          date.hasYear = true;
          date.year = 2000;
        }
        break;

      case DateTimeFormatSpecifier::DAY_OF_YEAR:
        date.dayOfYearValues.push_back(number);
        date.dayOfYear = number;
        date.dayOfYearFormat = true;
        date.weekDateFormat = false;
        // Joda has this weird behavior where it returns 1970 as the year by
        // default (if no year is specified), but if either day or month are
        // specified, it fallsback to 2000.
        if (!date.hasYear) {
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
          date.subsecond = ::bytedance::bolt::kSparkCompatible
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
        if (!date.hasYear) {
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
        if (!date.hasYear) {
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

icu::GregorianCalendar& simpleDateCalendar() {
  // Calendar fields are scratch state; one bounded instance per thread keeps
  // shared formatters immutable and avoids allocations while resolving dates.
  static thread_local auto calendar = [] {
    UErrorCode status = U_ZERO_ERROR;
    auto result = std::make_unique<icu::GregorianCalendar>(
        *icu::TimeZone::getGMT(), icu::Locale::getUS(), status);
    BOLT_CHECK(U_SUCCESS(status), "Cannot initialize Gregorian calendar");
    result->setFirstDayOfWeek(UCAL_SUNDAY);
    result->setMinimalDaysInFirstWeek(1);
    return result;
  }();
  return *calendar;
}

bool startsWithIgnoreCase(std::string_view text, std::string_view prefix) {
  return text.size() >= prefix.size() &&
      folly::StringPiece(text.data(), prefix.size())
          .equals(prefix, folly::AsciiCaseInsensitive());
}

int32_t simpleCalendarField(DateTimeFormatSpecifier specifier) {
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

bool isSimpleNumeric(DateTimeFormatSpecifier specifier) {
  return simpleCalendarField(specifier) >= 0 &&
      specifier != DateTimeFormatSpecifier::ERA &&
      specifier != DateTimeFormatSpecifier::HALFDAY_OF_DAY &&
      specifier != DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT &&
      specifier != DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT;
}

std::pair<int32_t, int32_t> simpleTimeZoneOffsets(const tz::TimeZone* zone);

bool parseSimpleTimezone(
    std::string_view& text,
    FormatPattern pattern,
    const tz::TimeZone* timezone,
    int32_t& offset) {
  const bool iso =
      pattern.specifier == DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601;
  if (text.empty()) {
    return false;
  }
  if (iso && text.front() == 'Z') {
    offset = 0;
    text.remove_prefix(1);
    return true;
  }
  bool gmt = !iso && startsWithIgnoreCase(text, "GMT");
  if (gmt) {
    text.remove_prefix(3);
    if (text.empty() || (text.front() != '+' && text.front() != '-')) {
      offset = 0;
      return true;
    }
  }
  if (text.front() == '+' || text.front() == '-') {
    const int sign = text.front() == '-' ? -1 : 1;
    size_t pos = 1;
    auto digit = [&](int32_t& value) {
      if (pos == text.size() || text[pos] < '0' || text[pos] > '9') {
        return false;
      }
      value = value * 10 + text[pos++] - '0';
      return true;
    };
    int32_t hours = 0, minutes = 0;
    if (!digit(hours)) {
      return false;
    }
    if (!gmt || (pos < text.size() && text[pos] != ':')) {
      if (!digit(hours)) {
        return false;
      }
    }
    if (gmt || !iso || pattern.minRepresentDigits != 1) {
      if (gmt || (iso && pattern.minRepresentDigits == 3)) {
        if (pos == text.size() || text[pos++] != ':') {
          return false;
        }
      }
      if (!digit(minutes) || !digit(minutes)) {
        return false;
      }
    }
    if (hours > 23 || minutes > 59) {
      return false;
    }
    offset = sign * (hours * 3600 + minutes * 60);
    text.remove_prefix(pos);
    return true;
  }
  if (gmt || iso || static_cast<unsigned char>(text.front()) > 127) {
    return false;
  }
  auto matchZone = [&](const tz::TimeZone* zone) {
    if (!zone) {
      return false;
    }
    const auto& names = tz::getTimeZoneNames();
    const auto it = names.find(zone->name());
    if (it == names.end()) {
      return false;
    }
    const auto& namesForZone = it->second;
    const std::pair<std::string_view, bool> candidates[] = {
        {namesForZone.standardTimeLongName, false},
        {namesForZone.daylightTimeLongName, true},
        {namesForZone.standardTimeAbbreviation, false},
        {namesForZone.daylightTimeAbbreviation, true}};
    for (const auto& [name, daylight] : candidates) {
      if (startsWithIgnoreCase(text, name)) {
        const auto [raw, saving] = simpleTimeZoneOffsets(zone);
        offset = raw + (daylight ? saving : 0);
        text.remove_prefix(name.size());
        return true;
      }
    }
    return false;
  };
  static const auto* defaultZone = [] {
    try {
      return tz::locateZone(::date::current_zone()->name(), false);
    } catch (const std::runtime_error&) {
      return static_cast<const tz::TimeZone*>(nullptr);
    }
  }();
  if (matchZone(timezone) || matchZone(defaultZone)) {
    return true;
  }
  static const auto names = [] {
    folly::F14FastMap<std::string, int32_t> result;
    const auto& zones = tz::getTimeZoneNames();
    auto add = [&](const std::string& id) {
      const auto it = zones.find(id);
      if (it == zones.end()) {
        return;
      }
      std::unique_ptr<icu::TimeZone> zone(
          icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(id)));
      const auto& names = it->second;
      auto put = [&](std::string name, bool daylight) {
        folly::toLowerAscii(name);
        result.emplace(
            std::move(name),
            (zone->getRawOffset() + (daylight ? zone->getDSTSavings() : 0)) /
                1000);
      };
      put(names.standardTimeAbbreviation, false);
      put(names.standardTimeLongName, false);
      put(names.daylightTimeAbbreviation, true);
      put(names.daylightTimeLongName, true);
    };
    // Java's English symbols prefer these zones for ambiguous abbreviations.
    for (const auto* id :
         {"America/Los_Angeles",
          "America/Denver",
          "America/Chicago",
          "America/New_York",
          "Pacific/Honolulu",
          "America/Anchorage",
          "America/Halifax",
          "America/St_Johns",
          "Europe/Paris",
          "GMT",
          "Asia/Jerusalem",
          "Asia/Tokyo",
          "Europe/Bucharest",
          "Asia/Shanghai",
          "UTC"}) {
      add(id);
    }
    std::vector<std::string> ids;
    for (const auto& [id, names] : zones) {
      ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    for (const auto& id : ids) {
      add(id);
    }
    return result;
  }();
  std::array<char, 64> lower;
  const auto length = std::min(text.size(), lower.size());
  for (size_t i = 0; i < length; ++i) {
    lower[i] =
        text[i] >= 'A' && text[i] <= 'Z' ? text[i] + ('a' - 'A') : text[i];
  }
  for (size_t size = length; size > 0; --size) {
    auto it = names.find(std::string_view(lower.data(), size));
    if (it != names.end()) {
      offset = it->second;
      text.remove_prefix(size);
      return true;
    }
  }
  return false;
}

std::pair<int32_t, int32_t> simpleTimeZoneOffsets(const tz::TimeZone* zone) {
  if (!zone) {
    return {0, 0};
  }
  if (const auto fixed = zone->offset()) {
    return {
        std::chrono::duration_cast<std::chrono::seconds>(*fixed).count(), 0};
  }
  static thread_local const tz::TimeZone* cachedZone = nullptr;
  static thread_local std::pair<int32_t, int32_t> offsets;
  if (cachedZone != zone) {
    std::unique_ptr<icu::TimeZone> javaZone(icu::TimeZone::createTimeZone(
        icu::UnicodeString::fromUTF8(zone->name())));
    offsets = {
        javaZone->getRawOffset() / 1000, javaZone->getDSTSavings() / 1000};
    cachedZone = zone;
  }
  return offsets;
}

Timestamp simpleDateTimeToUtc(Timestamp timestamp, const tz::TimeZone* zone) {
  if (!zone || zone->id() == 0) {
    return timestamp;
  }
  const auto local = std::chrono::seconds(timestamp.getSeconds());
  if (zone->offset()) {
    return Timestamp(
        local.count() - zone->offset()->count() * 60, timestamp.getNanos());
  }
  constexpr int64_t start = -2'208'988'800;
  if (local.count() < start + 86'400 &&
      local < zone->to_local(std::chrono::seconds(start))) {
    return Timestamp(
        local.count() - simpleTimeZoneOffsets(zone).first,
        timestamp.getNanos());
  }
  const auto normalized = zone->correct_nonexistent_time(local);
  return Timestamp(
      zone->to_sys(normalized, tz::TimeZone::TChoose::kLatest).count(),
      timestamp.getNanos());
}

} // namespace

DateTimeResult DateTimeFormatter::parseSimpleDateFormat(
    const std::string_view& input,
    const tz::TimeZone* timezone) const {
  std::array<int32_t, UCAL_FIELD_COUNT> fields{};
  std::array<size_t, UCAL_FIELD_COUNT> stamps{};
  fields[UCAL_YEAR] = 1970;
  fields[UCAL_DATE] = 1;
  fields[UCAL_ERA] = 1;
  std::string_view text = input;
  std::optional<int32_t> offset;
  int32_t ambiguousYear = -1;
  for (size_t index = 0; index < tokens_.size(); ++index) {
    const auto& token = tokens_[index];
    if (token.type == DateTimeToken::Type::kLiteral) {
      if (!text.starts_with(token.literal)) {
        return ErrorCode::PARSE_LITERAL_ERROR;
      }
      text.remove_prefix(token.literal.size());
      continue;
    }
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
      text.remove_prefix(1);
    }
    const auto specifier = token.pattern.specifier;
    const auto field = simpleCalendarField(specifier);
    if (field < 0) {
      int32_t seconds;
      if (!parseSimpleTimezone(text, token.pattern, timezone, seconds)) {
        return ErrorCode::PARSE_TIMEZONE_ERROR;
      }
      offset = seconds;
      continue;
    }
    int64_t value = 0;
    if (isSimpleNumeric(specifier)) {
      const bool adjacent = index + 1 < tokens_.size() &&
          tokens_[index + 1].type == DateTimeToken::Type::kPattern &&
          isSimpleNumeric(tokens_[index + 1].pattern.specifier);
      size_t width = adjacent ? token.pattern.minRepresentDigits : text.size();
      const bool negative = !text.empty() && text.front() == '-';
      if (negative) {
        text.remove_prefix(1);
        if (width == 0) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        --width;
      }
      size_t digits = 0;
      while (!text.empty() && digits < width) {
        int32_t length = 1;
        int32_t digit = static_cast<unsigned char>(text.front()) - '0';
        if (static_cast<unsigned char>(text.front()) >= 128) {
          int32_t pos = 0;
          UChar32 codePoint;
          U8_NEXT(
              text.data(), pos, std::min<size_t>(text.size(), 4), codePoint);
          if (codePoint < 0 || codePoint > 0xffff) {
            break;
          }
          UVersionInfo age;
          u_charAge(codePoint, age);
          digit = age[0] < 6 || (age[0] == 6 && age[1] <= 2)
              ? u_digit(codePoint, 10)
              : -1;
          length = pos;
        }
        if (digit < 0 || digit > 9) {
          break;
        }
        value = value * 10 + digit;
        if (value > int64_t{std::numeric_limits<int32_t>::max()} + negative) {
          return ErrorCode::PARSE_DIGIT_ERROR;
        }
        text.remove_prefix(length);
        ++digits;
      }
      if (digits == 0) {
        return ErrorCode::PARSE_DIGIT_ERROR;
      }
      if (negative) {
        value = -value;
      }
      if (field == UCAL_YEAR || field == UCAL_YEAR_WOY) {
        ambiguousYear = -1;
        if (!negative && token.pattern.minRepresentDigits <= 2 && digits == 2) {
          const auto startYear =
              util::toCivilDateTime(
                  Timestamp::fromMillis(twoDigitYearStartMillis_))
                  .date.year;
          value += (startYear / 100) * 100;
          if (value < startYear) {
            value += 100;
          } else if (value == startYear) {
            ambiguousYear = field;
          }
        }
      } else if (field == UCAL_MONTH) {
        --value;
      } else if (field == UCAL_DAY_OF_WEEK && value >= 1 && value <= 7) {
        value = value % 7 + 1;
      } else if (
          specifier == DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY &&
          value == 24) {
        value = 0;
      } else if (
          specifier == DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY &&
          value == 12) {
        value = 0;
      }
    } else {
      auto match = [&](const auto& names) {
        for (int32_t i = 0; i < std::size(names); ++i) {
          if (startsWithIgnoreCase(text, names[i])) {
            text.remove_prefix(names[i].size());
            value = i;
            return true;
          }
        }
        return false;
      };
      bool matched = false;
      if (field == UCAL_MONTH) {
        matched = match(monthsFull) || match(monthsShort);
      } else if (field == UCAL_DAY_OF_WEEK) {
        matched = match(weekdaysFull) || match(weekdaysShort);
        ++value;
      } else if (field == UCAL_AM_PM) {
        constexpr std::string_view names[] = {"AM", "PM"};
        matched = match(names);
      } else if (field == UCAL_ERA) {
        constexpr std::string_view names[] = {"BC", "AD"};
        matched = match(names);
      }
      if (!matched) {
        return ErrorCode::PARSE_DIGIT_ERROR;
      }
    }
    if (value < std::numeric_limits<int32_t>::min()) {
      return ErrorCode::PARSE_DIGIT_ERROR;
    }
    fields[field] = value;
    stamps[field] = index + 1;
  }
  if (text.size() == input.size()) {
    return ErrorCode::PARSE_DIGIT_ERROR;
  }
  int64_t millis;
  if (fields[UCAL_ERA] == 1 && fields[UCAL_YEAR] >= 1583 &&
      fields[UCAL_YEAR] <= util::kMaxYear && fields[UCAL_MONTH] >= 0 &&
      fields[UCAL_MONTH] < 12 &&
      util::isValidDate(
          fields[UCAL_YEAR], fields[UCAL_MONTH] + 1, fields[UCAL_DATE]) &&
      !stamps[UCAL_YEAR_WOY] && !stamps[UCAL_WEEK_OF_YEAR] &&
      !stamps[UCAL_WEEK_OF_MONTH] && !stamps[UCAL_DAY_OF_WEEK_IN_MONTH] &&
      !stamps[UCAL_DAY_OF_WEEK] && !stamps[UCAL_DAY_OF_YEAR] &&
      !stamps[UCAL_HOUR] && !stamps[UCAL_AM_PM]) {
    const __int128_t value =
        __int128_t{util::daysSinceEpochFromDate(
            fields[UCAL_YEAR], fields[UCAL_MONTH] + 1, fields[UCAL_DATE])} *
            86'400'000 +
        int64_t{fields[UCAL_HOUR_OF_DAY]} * 3'600'000 +
        int64_t{fields[UCAL_MINUTE]} * 60'000 +
        int64_t{fields[UCAL_SECOND]} * 1000 + fields[UCAL_MILLISECOND];
    if (value < std::numeric_limits<int64_t>::min() ||
        value > std::numeric_limits<int64_t>::max()) {
      return ErrorCode::YEAR_OUT_OF_RANGE;
    }
    millis = value;
  } else {
    auto& calendar = simpleDateCalendar();
    calendar.clear();
    const bool weekYear = stamps[UCAL_YEAR_WOY] > stamps[UCAL_YEAR];
    auto rank = [&](int32_t field, bool complete) {
      const auto stamp = stamps[field];
      const bool needsWeekday = field == UCAL_WEEK_OF_YEAR ||
          field == UCAL_WEEK_OF_MONTH || field == UCAL_DAY_OF_WEEK_IN_MONTH;
      const auto score = needsWeekday
          ? (complete ? (stamp && stamps[UCAL_DAY_OF_WEEK]
                             ? std::max(stamp, stamps[UCAL_DAY_OF_WEEK])
                             : 0)
                      : (field == UCAL_DAY_OF_WEEK_IN_MONTH
                             ? std::max(stamp, stamps[UCAL_DAY_OF_WEEK])
                             : stamp))
          : stamp;
      return std::pair(score, stamp);
    };
    int32_t selected = UCAL_DATE;
    for (bool complete : {true, false}) {
      for (auto field :
           {UCAL_DATE,
            UCAL_WEEK_OF_MONTH,
            UCAL_DAY_OF_WEEK_IN_MONTH,
            UCAL_DAY_OF_YEAR,
            UCAL_WEEK_OF_YEAR}) {
        if (rank(field, complete) > rank(selected, complete)) {
          selected = field;
        }
      }
      if (rank(selected, complete).first) {
        break;
      }
    }
    const bool weekDate = weekYear || selected == UCAL_WEEK_OF_YEAR;
    if (stamps[UCAL_HOUR_OF_DAY] &&
        (!stamps[UCAL_HOUR] || !stamps[UCAL_AM_PM] ||
         std::max(stamps[UCAL_HOUR], stamps[UCAL_AM_PM]) <
             stamps[UCAL_HOUR_OF_DAY])) {
      stamps[UCAL_HOUR] = stamps[UCAL_AM_PM] = 0;
    } else if (stamps[UCAL_HOUR] || stamps[UCAL_AM_PM]) {
      stamps[UCAL_HOUR_OF_DAY] = 0;
    }
    auto setFields = [&] {
      for (int32_t field = 0; field < UCAL_FIELD_COUNT; ++field) {
        if (!stamps[field] || field == UCAL_YEAR_WOY) {
          continue;
        }
        if ((field == UCAL_DATE || field == UCAL_DAY_OF_YEAR ||
             field == UCAL_WEEK_OF_YEAR || field == UCAL_WEEK_OF_MONTH ||
             field == UCAL_DAY_OF_WEEK_IN_MONTH) &&
            field != selected) {
          continue;
        }
        if ((field == UCAL_DAY_OF_WEEK &&
             (selected == UCAL_DATE || selected == UCAL_DAY_OF_YEAR)) ||
            (field == UCAL_MONTH &&
             (selected == UCAL_DAY_OF_YEAR || selected == UCAL_WEEK_OF_YEAR))) {
          continue;
        }
        calendar.set(static_cast<UCalendarDateFields>(field), fields[field]);
      }
    };
    if (weekDate) {
      const int32_t year = fields[weekYear ? UCAL_YEAR_WOY : UCAL_YEAR];
      int64_t week = stamps[UCAL_WEEK_OF_YEAR] ? fields[UCAL_WEEK_OF_YEAR] : 1;
      int64_t day = stamps[UCAL_DAY_OF_WEEK] ? fields[UCAL_DAY_OF_WEEK] : 1;
      if (weekYear && (day < 1 || day > 7)) {
        int64_t normalized = (day - 1) % 7;
        if (normalized < 0) {
          normalized += 7;
        }
        week += (day - 1 - normalized) / 7;
        day = (normalized + 1) % 7 + 1;
      }
      calendar.set(UCAL_YEAR, year);
      int32_t era = fields[UCAL_ERA];
      if (weekYear) {
        calendar.clear();
        calendar.set(UCAL_ERA, fields[UCAL_ERA]);
        calendar.set(UCAL_YEAR, fields[UCAL_YEAR]);
        setFields();
        UErrorCode status = U_ZERO_ERROR;
        era = calendar.get(UCAL_ERA, status);
        if (U_FAILURE(status)) {
          return ErrorCode::YEAR_OUT_OF_RANGE;
        }
        calendar.clear();
        calendar.set(UCAL_YEAR, year);
      }
      calendar.set(UCAL_ERA, era);
      calendar.set(UCAL_MONTH, 0);
      calendar.set(UCAL_DATE, 1);
      UErrorCode status = U_ZERO_ERROR;
      const auto first = calendar.getTime(status);
      const int32_t weekday = calendar.get(UCAL_DAY_OF_WEEK, status);
      if (U_FAILURE(status)) {
        return ErrorCode::YEAR_OUT_OF_RANGE;
      }
      const int64_t days =
          (week - 1) * 7 + ((day - 1) % 7 + 7) % 7 - (weekday - 1);
      calendar.setTime(first + days * 86'400'000, status);
      for (auto field :
           {UCAL_YEAR,
            UCAL_YEAR_WOY,
            UCAL_MONTH,
            UCAL_DATE,
            UCAL_DAY_OF_YEAR,
            UCAL_WEEK_OF_YEAR,
            UCAL_WEEK_OF_MONTH,
            UCAL_DAY_OF_WEEK,
            UCAL_DAY_OF_WEEK_IN_MONTH,
            UCAL_ERA}) {
        stamps[field] = 0;
      }
    }
    setFields();
    UErrorCode status = U_ZERO_ERROR;
    const auto resolved = calendar.getTime(status);
    if (U_FAILURE(status)) {
      return ErrorCode::DAY_OUT_OF_RANGE;
    }
    millis = resolved;
  }
  const auto resolvedMillis = ambiguousYear < 0
      ? millis
      : (offset ? millis - int64_t{*offset} * 1000
                : simpleDateTimeToUtc(Timestamp::fromMillis(millis), timezone)
                      .toMillis());
  if (ambiguousYear >= 0 && resolvedMillis < twoDigitYearStartMillis_) {
    auto& calendar = simpleDateCalendar();
    UErrorCode status = U_ZERO_ERROR;
    calendar.setTime(millis, status);
    calendar.add(UCAL_YEAR, 100, status);
    const auto resolved = calendar.getTime(status);
    if (U_FAILURE(status)) {
      return ErrorCode::YEAR_OUT_OF_RANGE;
    }
    millis = resolved;
  }
  return DateTimeResult{
      Timestamp::fromMillis(millis),
      offset && std::abs(*offset) <= 14 * 3600 ? tz::getTimeZoneID(*offset / 60)
                                               : -1,
      offset};
}

uint32_t DateTimeFormatter::maxResultSize(const tz::TimeZone* timezone) const {
  uint32_t size = 0;
  for (const auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      size += token.literal.size();
      continue;
    }
    if (!::bytedance::bolt::kSparkCompatible &&
        type_ != DateTimeFormatterType::SIMPLE_DATE_FORMAT &&
        isSparkOnlyFormatSpecifier(token.pattern.specifier)) {
      BOLT_UNSUPPORTED(
          "format is not supported for specifier {}", token.pattern.specifier);
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::ERA:
      case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
        // Fixed size.
        size += 2;
        break;
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
        // Timestamp covers years up to +/-292278994 (9 digits), plus one
        // char for a leading sign.
        size += std::max((int)token.pattern.minRepresentDigits, 9) + 1;
        break;
      case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
      case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED:
        size += std::max((int)token.pattern.minRepresentDigits, 1);
        break;
      case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT:
      case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
        // 9 is the max size of elements in weekdaysFull or monthsFull.
        size += token.pattern.minRepresentDigits <= 3 ? 3 : 9;
        break;
      case DateTimeFormatSpecifier::WEEK_YEAR:
        [[fallthrough]];
      case DateTimeFormatSpecifier::YEAR:
        // Timestamp covers years up to +/-292278994 (9 digits), plus one
        // char for a leading sign.
        size += token.pattern.minRepresentDigits == 2
            ? 2
            : std::max((int)token.pattern.minRepresentDigits, 9) + 1;
        break;
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
        // Century of +/-292278994 has up to 7 digits.
        size += std::max((int)token.pattern.minRepresentDigits, 7);
        break;
      case DateTimeFormatSpecifier::DAY_OF_YEAR:
        size += std::max((int)token.pattern.minRepresentDigits, 3);
        break;
      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      case DateTimeFormatSpecifier::DAY_OF_MONTH:
      case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
      case DateTimeFormatSpecifier::HOUR_OF_DAY:
      case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
      case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
      case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
      case DateTimeFormatSpecifier::WEEK_OF_MONTH:
      case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
        size += std::max((int)token.pattern.minRepresentDigits, 2);
        break;
      case DateTimeFormatSpecifier::FRACTION_OF_SECOND:
        // Nanosecond is considered.
        size += std::max((int)token.pattern.minRepresentDigits, 9);
        break;
      case DateTimeFormatSpecifier::TIMEZONE:
        if (token.pattern.minRepresentDigits <= 3) {
          // The longest timezone abbreviation is 9 in the case of timezones
          // that do not have an abbreviation and use GMT offset (e.g.,
          // GMT+01:00, GMT-08:00)
          size += 9;
        } else {
          // The longest time zone long name is 40, Australian Central Western
          // Standard Time.
          // https://www.timeanddate.com/time/zones/
          size += 50;
        }
        break;
      case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601:
      case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601_NO_Z:
        size += 9;
        break;
      case DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822:
        size += 12;
        break;
      case DateTimeFormatSpecifier::TIMEZONE_ID:
      case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
        if (token.pattern.minRepresentDigits == 1) {
          // 'Z' means output the time zone offset without a colon.
          size += 8;
        } else if (token.pattern.minRepresentDigits == 2) {
          // 'ZZ' means output the time zone offset with a colon.
          size += 9;
        } else {
          // The longest time zone ID is 32, America/Argentina/ComodRivadavia.
          size += 32;
        }
        break;
      default:
        BOLT_UNSUPPORTED(
            "format is not supported for specifier {}",
            token.pattern.specifier);
    }
  }
  return size;
}

int32_t DateTimeFormatter::format(
    const Timestamp& timestamp,
    const tz::TimeZone* timezone,
    const uint32_t maxResultSize,
    char* result,
    bool allowOverflow,
    TimePolicy timePolicy,
    bool isPrecision,
    const std::optional<std::string>& zeroOffsetText) const {
  const bool simple = type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT;
  if (simple) {
    timePolicy = TimePolicy::LEGACY;
  }
  Timestamp t = timestamp;
  int64_t offset = 0;
  if (timezone != nullptr && !(simple && timezone->id() == 0)) {
    const auto utcSeconds = timestamp.getSeconds();
    if (simple && timezone->offset()) {
      t = Timestamp(
          utcSeconds + timezone->offset()->count() * 60, t.getNanos());
    } else if (simple && utcSeconds < -2'208'988'800) {
      t = Timestamp(
          utcSeconds + simpleTimeZoneOffsets(timezone).first, t.getNanos());
    } else {
      t.toTimezone(*timezone);
    }
    offset = t.getSeconds() - utcSeconds;
  }
  auto civilDateTime = util::toCivilDateTime(t, allowOverflow, isPrecision);
  bool calendarInitialized = false;
  auto calendarField = [&](UCalendarDateFields field) {
    auto& calendar = simpleDateCalendar();
    UErrorCode status = U_ZERO_ERROR;
    if (!calendarInitialized) {
      BOLT_USER_CHECK_LE(
          std::abs(static_cast<double>(t.toMillis())),
          8.64e15,
          "Date outside supported calendar range");
      calendar.setTime(t.toMillis(), status);
      calendarInitialized = true;
    }
    const auto value = calendar.get(field, status);
    BOLT_USER_CHECK(U_SUCCESS(status), "Date outside supported calendar range");
    return value;
  };
  if (simple && civilDateTime.date.year <= 1582) {
    civilDateTime.date.year = calendarField(UCAL_YEAR);
    if (calendarField(UCAL_ERA) == 0) {
      civilDateTime.date.year = 1 - civilDateTime.date.year;
    }
    civilDateTime.date.month = calendarField(UCAL_MONTH) + 1;
    civilDateTime.date.day = calendarField(UCAL_DATE);
    civilDateTime.dayOfYear = calendarField(UCAL_DAY_OF_YEAR);
  }
  const auto& calDate = civilDateTime.date;
  const auto& durationInTheDay = civilDateTime.time;
  const auto weekdayNum = civilDateTime.weekday;
  const auto dayOfYear = civilDateTime.dayOfYear;
  const auto daysSinceEpoch = civilDateTime.daysSinceEpoch;
  auto weekdayFromDays = [](int64_t daysSinceEpochInput) {
    auto weekday = static_cast<int32_t>((daysSinceEpochInput + 4) % 7);
    return weekday < 0 ? weekday + 7 : weekday;
  };

  const char* resultStart = result;
  char* maxResultEnd = result + maxResultSize;
  for (auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      std::memcpy(result, token.literal.data(), token.literal.size());
      result += token.literal.size();
    } else {
      if (!::bytedance::bolt::kSparkCompatible &&
          type_ != DateTimeFormatterType::SIMPLE_DATE_FORMAT &&
          isSparkOnlyFormatSpecifier(token.pattern.specifier)) {
        BOLT_UNSUPPORTED(
            "format is not supported for specifier {}",
            token.pattern.specifier);
      }
      if (simple &&
          (token.pattern.specifier ==
               DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR ||
           token.pattern.specifier == DateTimeFormatSpecifier::WEEK_OF_MONTH ||
           token.pattern.specifier ==
               DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH)) {
        int32_t value = calendarField(static_cast<UCalendarDateFields>(
            simpleCalendarField(token.pattern.specifier)));
        if (calDate.year == 1582 && calDate.month == 10 && calDate.day >= 15 &&
            token.pattern.specifier !=
                DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR) {
          const int32_t day = calDate.day - 10;
          value = token.pattern.specifier ==
                  DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH
              ? (day - 1) / 7 + 1
              : day / 7 + 1;
        }
        result += padContent(
            value, '0', token.pattern.minRepresentDigits, maxResultEnd, result);
        continue;
      }
      switch (token.pattern.specifier) {
        case DateTimeFormatSpecifier::ERA: {
          const std::string_view piece = calDate.year > 0 ? "AD" : "BC";
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;
        case DateTimeFormatSpecifier::CENTURY_OF_ERA: {
          auto year = calDate.year;
          year = (year < 0 ? -year : year);
          auto century = year / 100;
          result += padContent(
              century,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::YEAR_OF_ERA: {
          auto year = calDate.year;
          if (token.pattern.minRepresentDigits == 2) {
            result +=
                padContent(std::abs(year) % 100, '0', 2, maxResultEnd, result);
          } else {
            year = year <= 0 ? std::abs(year - 1) : year;
            // spark compatibility: Java DateTimeFormatter uses
            // SignStyle.EXCEEDS_PAD for 4+ pattern letters, printing '+'
            // when the year has more digits than the pattern width; legacy
            // SimpleDateFormat never prints '+'. Like Java, the decision is
            // based on the local (timezone-adjusted) year only.
            bool addPlus = ::bytedance::bolt::kSparkCompatible &&
                timePolicy != TimePolicy::LEGACY &&
                token.pattern.minRepresentDigits >= 4 && year > 0 &&
                countDigits(year) > token.pattern.minRepresentDigits;
            if (addPlus) {
              *result++ = '+';
            }
            result += padContent(
                year,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          }
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_0_BASED:
        case DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED: {
          auto weekdayValue = weekdayNum;
          if (weekdayValue == 0 &&
              token.pattern.specifier ==
                  DateTimeFormatSpecifier::DAY_OF_WEEK_1_BASED) {
            weekdayValue = 7;
          }
          result += padContent(
              weekdayValue,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_TEXT: {
          std::string_view piece;
          if (token.pattern.minRepresentDigits <= 3) {
            piece = weekdaysShort[weekdayNum];
          } else {
            piece = weekdaysFull[weekdayNum];
          }
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::WEEK_YEAR:
          [[fallthrough]];
        case DateTimeFormatSpecifier::YEAR: {
          auto year = calDate.year;
          if (simple &&
              token.pattern.specifier == DateTimeFormatSpecifier::WEEK_YEAR) {
            year = calendarField(UCAL_YEAR_WOY);
          } else if (
              ::bytedance::bolt::kSparkCompatible &&
              token.pattern.specifier == DateTimeFormatSpecifier::WEEK_YEAR) {
            BOLT_USER_CHECK_EQ(
                timePolicy,
                TimePolicy::LEGACY,
                "WEEK_YEAR is only supported in Spark LEGACY policy");
            auto weekYear = year;
            if (calDate.month == 12) {
              // If the date is in the last week of the year, and this week
              // contains next year's first day, the week year is next year.
              if (calDate.day - weekdayNum + 6 > 31) {
                weekYear += 1;
              }
            }
            year = weekYear;
          }
          if ((simple || ::bytedance::bolt::kSparkCompatible) && year <= 0 &&
              (hasEra_ || timePolicy == TimePolicy::LEGACY)) {
            // for spark compatibility, year should be the positive
            // year-of-era for LEGACY policy or when the pattern has an Era:
            // proleptic year 0 is 1 BC, -1 is 2 BC, etc.
            year = abs(year) + 1;
          }
          if (token.pattern.minRepresentDigits == 2) {
            year = std::abs(year);
            auto twoDigitYear = year % 100;
            result += padContent(
                twoDigitYear,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          } else {
            // spark compatibility: Java DateTimeFormatter uses
            // SignStyle.EXCEEDS_PAD for 4+ pattern letters, printing '+'
            // when the year has more digits than the pattern width; legacy
            // SimpleDateFormat never prints '+'. Like Java, the decision is
            // based on the local (timezone-adjusted) year only.
            bool addPlus = ::bytedance::bolt::kSparkCompatible &&
                timePolicy != TimePolicy::LEGACY &&
                token.pattern.minRepresentDigits >= 4 && year > 0 &&
                countDigits(year) > token.pattern.minRepresentDigits;
            if (addPlus) {
              *result++ = '+';
            }
            result += padContent(
                year,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          }
        } break;

        case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR: {
          // Calculate week of week year based on Java SimpleDateFormat

          int weekOfYear = 0;
          if (calDate.month == 12 && calDate.day - weekdayNum + 6 > 31) {
            // If the date is in the last week of the year, and this week
            // contains next year's first day, the week is next year's first
            // week.
            weekOfYear = 1;
          } else {
            auto year = calDate.year;
            auto firstDayOfYear = util::daysSinceEpochFromDate(year, 1, 1);

            auto daysSinceFirstDay = daysSinceEpoch - firstDayOfYear;

            // Get the weekday of the first day of the year (0=Sunday,
            // 6=Saturday)
            auto firstDayWeekday = weekdayFromDays(firstDayOfYear);

            // Calculate week number where week starts from Sunday
            // The formula is: (day_of_year + first_day_weekday) / 7 + 1
            weekOfYear = (daysSinceFirstDay + firstDayWeekday) / 7 + 1;
          }

          result += padContent(
              weekOfYear,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::WEEK_OF_MONTH: {
          // Calculate week of month based on Java SimpleDateFormat

          // like WEEK_OF_WEEK_YEAR, but calculate from the first day of
          // month
          auto firstDayOfMonth =
              util::daysSinceEpochFromDate(calDate.year, calDate.month, 1);

          auto daysSinceFirstDayOfMonth = daysSinceEpoch - firstDayOfMonth;

          // Get the weekday of the first day of the month (0=Sunday,
          // 6=Saturday)
          auto firstDayOfMonthWeekday = weekdayFromDays(firstDayOfMonth);

          // Calculate week number where week starts from Sunday
          auto weekOfMonth =
              (daysSinceFirstDayOfMonth + firstDayOfMonthWeekday) / 7 + 1;

          result += padContent(
              weekOfMonth,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH: {
          // Calculate day of week in month based on Java SimpleDateFormat

          // like WEEK_OF_MONTH, but calculates which occurrence of this
          // weekday in the month
          auto currentDayOfWeek = weekdayNum; // 0=Sunday, 6=Saturday
          auto dayOfMonth = calDate.day;

          auto firstDayOfMonth =
              util::daysSinceEpochFromDate(calDate.year, calDate.month, 1);
          auto firstDayOfMonthWeekday = weekdayFromDays(firstDayOfMonth);

          // Calculate the first occurrence of the current day of week in
          // the month
          int firstOccurrence =
              (currentDayOfWeek - firstDayOfMonthWeekday + 7) % 7 + 1;

          // Calculate which occurrence this is (1st, 2nd, 3rd, etc.)
          auto dayOfWeekInMonth = (dayOfMonth - firstOccurrence) / 7 + 1;

          result += padContent(
              dayOfWeekInMonth,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::DAY_OF_YEAR: {
          result += padContent(
              dayOfYear,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR:
          result += padContent(
              calDate.month,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT: {
          std::string_view piece;
          if (token.pattern.minRepresentDigits <= 3) {
            piece = monthsShort[calDate.month - 1];
          } else {
            piece = monthsFull[calDate.month - 1];
          }
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::DAY_OF_MONTH:
          result += padContent(
              calDate.day,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::HALFDAY_OF_DAY: {
          const std::string_view piece =
              durationInTheDay.hour < 12 ? "AM" : "PM";
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
        case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
        case DateTimeFormatSpecifier::HOUR_OF_DAY:
        case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY: {
          auto hourNum = durationInTheDay.hour;
          if (token.pattern.specifier ==
              DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY) {
            hourNum = (hourNum + 11) % 12 + 1;
          } else if (
              token.pattern.specifier ==
              DateTimeFormatSpecifier::HOUR_OF_HALFDAY) {
            hourNum = hourNum % 12;
          } else if (
              token.pattern.specifier ==
              DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY) {
            hourNum = (hourNum + 23) % 24 + 1;
          }
          result += padContent(
              hourNum,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
        } break;

        case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
          result += padContent(
              durationInTheDay.minute % 60,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
          result += padContent(
              durationInTheDay.second % 60,
              '0',
              token.pattern.minRepresentDigits,
              maxResultEnd,
              result);
          break;

        case DateTimeFormatSpecifier::FRACTION_OF_SECOND: {
          if (simple) {
            result += padContent(
                durationInTheDay.nanosecond / 1'000'000,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
            break;
          }
          const auto& piece = formatFractionOfSecond(
              durationInTheDay.nanosecond, token.pattern.minRepresentDigits);
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::TIMEZONE: {
          if (simple) {
            std::string_view piece;
            std::string fixed;
            if (timezone && timezone->id() != 0 && timezone->offset()) {
              fixed = "GMT" + timezone->name();
              piece = fixed;
            } else {
              const auto& names = tz::getTimeZoneNames().at(
                  !timezone || timezone->id() == 0 ? "UTC" : timezone->name());
              const bool daylight = timezone && timezone->tz() &&
                  timestamp.getSeconds() >= -2'208'988'800 &&
                  timezone->tz()
                          ->get_info(::date::sys_seconds(
                              std::chrono::seconds(timestamp.getSeconds())))
                          .save.count() != 0;
              piece = token.pattern.minRepresentDigits < 4
                  ? (daylight ? names.daylightTimeAbbreviation
                              : names.standardTimeAbbreviation)
                  : (daylight ? names.daylightTimeLongName
                              : names.standardTimeLongName);
            }
            BOLT_CHECK_LE(piece.size(), maxResultEnd - result);
            std::memcpy(result, piece.data(), piece.size());
            result += piece.size();
            break;
          }
          // TODO: implement short name time zone, need a map from full name to
          // short name
          if (token.pattern.minRepresentDigits <= 3) {
            BOLT_UNSUPPORTED("short name time zone is not yet supported")
          }
          if (timezone == nullptr) {
            BOLT_USER_FAIL("Timezone unknown")
          }
          const auto& piece = timezone->name();
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601:
        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ISO8601_NO_Z:
        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_RFC822:
          if (simple && token.pattern.minRepresentDigits == 1 && offset != 0) {
            *result++ = offset < 0 ? '-' : '+';
            result += padContent(
                std::abs(offset) / 3600, '0', 2, maxResultEnd, result);
          } else {
            result += appendJavaTimezoneOffset(offset, token.pattern, result);
          }
          break;
        case DateTimeFormatSpecifier::TIMEZONE_ID:
        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID: {
          if (simple) {
            result += appendTimezoneOffset(offset, result, false);
            break;
          }
          // Zone: 'Z' outputs offset without a colon, 'ZZ' outputs the offset
          // with a colon, 'ZZZ' or more outputs the zone id.
          if (offset == 0 && zeroOffsetText.has_value()) {
            std::memcpy(result, zeroOffsetText->data(), zeroOffsetText->size());
            result += zeroOffsetText->size();
            break;
          }

          if (timezone == nullptr) {
            BOLT_USER_FAIL("Timezone unknown");
          }

          if (token.pattern.minRepresentDigits >= 3) {
            // Append the time zone ID.
            const auto& piece = timezone->name();

            if (type_ == DateTimeFormatterType::JAVA_TIME) {
              std::memcpy(result, piece.data(), piece.length());
              result += piece.length();
              break;
            }
            static const auto& timeZoneLinks = tz::getTimeZoneLinks();
            auto timeZoneLinksIter = timeZoneLinks.find(piece);
            if (timeZoneLinksIter != timeZoneLinks.end()) {
              const auto& timeZoneLink = timeZoneLinksIter->second;
              std::memcpy(result, timeZoneLink.data(), timeZoneLink.length());
              result += timeZoneLink.length();
              break;
            }

            std::memcpy(result, piece.data(), piece.length());
            result += piece.length();
            break;
          }

          result += appendTimezoneOffset(
              offset, result, token.pattern.minRepresentDigits == 2);
          break;
        }
        default:
          BOLT_UNSUPPORTED(
              "format is not supported for specifier {}",
              token.pattern.specifier);
      }
    }
  }
  auto resultSize = result - resultStart;
  BOLT_CHECK_LE(resultSize, maxResultSize, "Bad allocation size for result.");
  return resultSize;
}

DateTimeResult DateTimeFormatter::parseToUtc(
    const std::string_view& input,
    const tz::TimeZone* timezone) const {
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
      timestamp = simpleDateTimeToUtc(timestamp, timezone);
    } else if (timezone) {
      timestamp = Timestamp(
          timezone->to_sys(std::chrono::seconds(timestamp.getSeconds()))
              .count(),
          timestamp.getNanos());
    }
  }
  return DateTimeResult{timestamp, 0};
}

DateTimeResult DateTimeFormatter::parse(const std::string_view& input) const {
  if (UNLIKELY(type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT)) {
    return parseSimpleDateFormat(input);
  }
  Date date;
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
      case DateTimeToken::Type::kPattern:
        if (i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern) {
          auto errorCode =
              parseFromPattern(tok.pattern, input, cur, end, date, true, type_);
          if (errorCode != ErrorCode::NO_ERROR) {
            return errorCode;
          }
        } else {
          auto errorCode = parseFromPattern(
              tok.pattern, input, cur, end, date, false, type_);
          if (errorCode != ErrorCode::NO_ERROR) {
            return errorCode;
          }
        }
        break;
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
template <bool nanosecondPrecision>
FOLLY_ALWAYS_INLINE DateTimeResult DateTimeFormatter::parseImpl(
    const std::string_view& input,
    const TimePolicy timeParserPolicy) const {
  Date date;
  const char* cur = input.data();
  const char* end = cur + input.size();

  bool isLegacy = (timeParserPolicy == TimePolicy::LEGACY);
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
              std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
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
        if (i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern) {
          auto errorCode = parseFromPattern<nanosecondPrecision>(
              tok.pattern, input, cur, end, date, true, type_, isLegacy);
          if (!noError(errorCode)) {
            return errorCode;
          }
        } else {
          auto errorCode = parseFromPattern<nanosecondPrecision>(
              tok.pattern, input, cur, end, date, false, type_, isLegacy);
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
  return parseImpl<false>(input, timeParserPolicy);
}

DateTimeResult DateTimeFormatter::parseWithNanosecondPrecision(
    const std::string_view& input) const {
  if (UNLIKELY(type_ == DateTimeFormatterType::SIMPLE_DATE_FORMAT)) {
    return parseSimpleDateFormat(input);
  }
  return parseImpl<true>(input, TimePolicy::CORRECTED);
}

std::shared_ptr<DateTimeFormatter> buildMysqlDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    BOLT_USER_FAIL("Both printing and parsing not supported");
  }

  // For %r we should reserve 1 extra space because it has 3 literals ':' ':'
  // and ' '
  DateTimeFormatterBuilder builder(
      format.size() + countOccurence(format, "%r"));

  const char* cur = format.data();
  const char* end = cur + format.size();
  while (cur < end) {
    auto tokenEnd = cur;
    if (*tokenEnd == '%') { // pattern
      ++tokenEnd;
      if (tokenEnd == end) {
        break;
      }
      switch (*tokenEnd) {
        case 'a':
          builder.appendDayOfWeekText(3);
          break;
        case 'b':
          builder.appendMonthOfYearText(3);
          break;
        case 'c':
          builder.appendMonthOfYear(1);
          break;
        case 'd':
          builder.appendDayOfMonth(2);
          break;
        case 'e':
          builder.appendDayOfMonth(1);
          break;
        case 'f':
          builder.appendFractionOfSecond(6);
          break;
        case 'H':
          builder.appendHourOfDay(2);
          break;
        case 'h':
        case 'I':
          builder.appendClockHourOfHalfDay(2);
          break;
        case 'i':
          builder.appendMinuteOfHour(2);
          break;
        case 'j':
          builder.appendDayOfYear(3);
          break;
        case 'k':
          builder.appendHourOfDay(1);
          break;
        case 'l':
          builder.appendClockHourOfHalfDay(1);
          break;
        case 'M':
          builder.appendMonthOfYearText(4);
          break;
        case 'm':
          builder.appendMonthOfYear(2);
          break;
        case 'p':
          builder.appendHalfDayOfDay();
          break;
        case 'r':
          builder.appendClockHourOfHalfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          builder.appendLiteral(":");
          builder.appendSecondOfMinute(2);
          builder.appendLiteral(" ");
          builder.appendHalfDayOfDay();
          break;
        case 'S':
        case 's':
          builder.appendSecondOfMinute(2);
          break;
        case 'T':
          builder.appendHourOfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          builder.appendLiteral(":");
          builder.appendSecondOfMinute(2);
          break;
        case 'v':
          builder.appendWeekOfWeekYear(2);
          break;
        case 'W':
          builder.appendDayOfWeekText(4);
          break;
        case 'x':
          builder.appendWeekYear(4);
          break;
        case 'Y':
          builder.appendYear(4);
          break;
        case 'y':
          builder.appendYear(2);
          break;
        case '%':
          builder.appendLiteral("%");
          break;
        case 'D':
        case 'U':
        case 'u':
        case 'V':
        case 'w':
        case 'X':
          BOLT_UNSUPPORTED("Specifier {} is not supported.", *tokenEnd)
        default:
          builder.appendLiteral(tokenEnd, 1);
          break;
      }
      ++tokenEnd;
    } else {
      while (tokenEnd < end && *tokenEnd != '%') {
        ++tokenEnd;
      }
      builder.appendLiteral(cur, tokenEnd - cur);
    }
    cur = tokenEnd;
  }
  return builder.setType(DateTimeFormatterType::MYSQL).build();
}

namespace {
std::shared_ptr<DateTimeFormatter> buildPatternDateTimeFormatter(
    const std::string_view& format,
    DateTimeFormatterType type) {
  if (format.empty()) {
    BOLT_USER_FAIL("Invalid pattern specification");
  }

  DateTimeFormatterBuilder builder(format.size());
  const char* cur = format.data();
  const char* end = cur + format.size();

  while (cur < end) {
    const char* startTokenPtr = cur;

    // Literal case
    if (*startTokenPtr == '\'') {
      // Case 1: 2 consecutive single quote
      if (cur + 1 < end && *(cur + 1) == '\'') {
        builder.appendLiteral("'");
        cur += 2;
      } else {
        // Case 2: find closing single quote
        int64_t count = numLiteralChars(startTokenPtr + 1, end);
        if (count == -1) {
          BOLT_USER_FAIL("No closing single quote for literal");
        } else {
          for (int64_t i = 1; i <= count; i++) {
            builder.appendLiteral(startTokenPtr + i, 1);
            if (*(startTokenPtr + i) == '\'') {
              i += 1;
            }
          }
          cur += count + 2;
        }
      }
    } else {
      int count = 1;
      ++cur;
      while (cur < end && *startTokenPtr == *cur) {
        ++count;
        ++cur;
      }
      switch (*startTokenPtr) {
        case 'G':
          builder.appendEra();
          break;
        case 'C':
          builder.appendCenturyOfEra(count);
          break;
        case 'Y':
          builder.appendYearOfEra(count);
          break;
        case 'x':
          if (type == DateTimeFormatterType::JAVA_TIME) {
            BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: x");
            builder.appendISOTimeZoneOffsetId(count, false);
          } else {
            builder.appendWeekYear(count);
          }
          break;
        case 'w':
          builder.appendWeekOfWeekYear(count);
          break;
        case 'e':
          builder.appendDayOfWeek1Based(count);
          break;
        case 'E':
          builder.appendDayOfWeekText(count);
          break;
        case 'y':
          builder.appendYear(count);
          break;
        case 'D':
          builder.appendDayOfYear(count);
          break;
        case 'M':
          if (count <= 2) {
            builder.appendMonthOfYear(count);
          } else {
            builder.appendMonthOfYearText(count);
          }
          break;
        case 'd':
          builder.appendDayOfMonth(count);
          break;
        case 'a':
          if (::bytedance::bolt::kSparkCompatible && count > 1) {
            BOLT_USER_FAIL("The am-pm-of-day pattern letter count must be 1.");
          }
          builder.appendHalfDayOfDay();
          break;
        case 'K':
          builder.appendHourOfHalfDay(count);
          break;
        case 'h':
          builder.appendClockHourOfHalfDay(count);
          break;
        case 'H':
          builder.appendHourOfDay(count);
          break;
        case 'k':
          builder.appendClockHourOfDay(count);
          break;
        case 'm':
          builder.appendMinuteOfHour(count);
          break;
        case 's':
          builder.appendSecondOfMinute(count);
          break;
        case 'S':
          builder.appendFractionOfSecond(count);
          break;
        case 'u':
          if (type == DateTimeFormatterType::JAVA_TIME) {
            builder.appendYear(count);
          } else {
            builder.appendDayOfWeek1Based(count);
          }
          break;
        case 'z':
          builder.appendTimeZone(count);
          break;
        case 'X':
          if (type == DateTimeFormatterType::JAVA_TIME) {
            BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: X");
            builder.appendISOTimeZoneOffsetId(count, true);
          } else {
            if (!::bytedance::bolt::kSparkCompatible) {
              BOLT_UNSUPPORTED("Specifier X is not supported.");
            }
            builder.appendTimeZoneOffsetId(count);
          }
          break;
        case 'Z':
          if (type == DateTimeFormatterType::JAVA_TIME) {
            BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: Z");
            builder.appendRFC822TimeZoneOffsetId(count);
          } else if (type == DateTimeFormatterType::JODA && count >= 3) {
            builder.appendTimeZoneId();
          } else {
            builder.appendTimeZoneOffsetId(count);
          }
          break;
        case 'V':
          if (type != DateTimeFormatterType::JAVA_TIME) {
            BOLT_UNSUPPORTED("Specifier V is not supported.");
          }
          BOLT_USER_CHECK_EQ(count, 2, "Zone ID pattern must be VV");
          builder.appendTimeZoneId();
          break;
        default:
          if (isalpha(*startTokenPtr)) {
            BOLT_UNSUPPORTED("Specifier {} is not supported.", *startTokenPtr);
          } else {
            builder.appendLiteral(startTokenPtr, cur - startTokenPtr);
          }
          break;
      }
    }
  }
  return builder.setType(type).build();
}
} // namespace

std::shared_ptr<DateTimeFormatter> buildJodaDateTimeFormatter(
    const std::string_view& format) {
  return buildPatternDateTimeFormatter(format, DateTimeFormatterType::JODA);
}

std::shared_ptr<DateTimeFormatter> buildJavaDateTimeFormatter(
    const std::string_view& format) {
  return buildPatternDateTimeFormatter(
      format, DateTimeFormatterType::JAVA_TIME);
}

std::shared_ptr<DateTimeFormatter> buildSparkDateTimeFormatter(
    const std::string_view& format) {
  return buildPatternDateTimeFormatter(
      format, DateTimeFormatterType::SPARK_CORRECTED);
}

std::shared_ptr<DateTimeFormatter> buildHiveDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    BOLT_USER_FAIL("Invalid pattern specification");
  }

  DateTimeFormatterBuilder builder(format.size());

  const char* cur = format.data();
  const char* end = cur + format.size();
  while (cur < end) {
    auto tokenEnd = cur;
    if (*tokenEnd == '%') { // pattern
      ++tokenEnd;
      if (tokenEnd == end) {
        break;
      }
      switch (*tokenEnd) {
        case 'C':
          builder.appendCenturyOfEra(2);
          break;
        case 'd':
          builder.appendDayOfMonth(2);
          break;
        case 'D':
          builder.appendMonthOfYear(2);
          builder.appendLiteral("/");
          builder.appendDayOfMonth(2);
          builder.appendLiteral("/");
          builder.appendYear(4);
          break;
        case 'e':
          builder.appendDayOfMonth(1);
          break;
        case 'F':
          builder.appendYear(4);
          builder.appendLiteral("/");
          builder.appendMonthOfYear(2);
          builder.appendLiteral("/");
          builder.appendDayOfMonth(2);
          break;
        case 'H':
          builder.appendHourOfDay(2);
          break;
        case 'I':
          builder.appendClockHourOfHalfDay(2);
          break;
        case 'j':
          builder.appendDayOfYear(3);
          break;
        case 'm':
          builder.appendMonthOfYear(2);
          break;
        case 'M':
          builder.appendMinuteOfHour(2);
          break;
        case 'n':
          builder.appendLiteral("\n");
          break;
        case 'p':
          builder.appendHalfDayOfDay();
          break;
        case 'R':
          builder.appendHourOfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          break;
        case 'S':
          builder.appendSecondOfMinute(2);
          break;
        case 't':
          builder.appendLiteral("\t");
          break;
        case 'T':
          builder.appendHourOfDay(2);
          builder.appendLiteral(":");
          builder.appendMinuteOfHour(2);
          builder.appendLiteral(":");
          builder.appendSecondOfMinute(2);
          break;
        case 'u':
          builder.appendDayOfWeek1Based(1);
          break;
        case 'V':
          builder.appendMonthOfYear(2);
          break;
        case 'w':
          builder.appendDayOfWeek0Based(1);
          break;
        case 'Y':
          builder.appendYearOfEra(4);
          break;
        case 'y':
          BOLT_UNSUPPORTED("Specifier {} is not supported.", *tokenEnd)
          break;
        default:
          builder.appendLiteral(tokenEnd, 1);
          break;
      }
      ++tokenEnd;
    } else {
      while (tokenEnd < end && *tokenEnd != '%') {
        ++tokenEnd;
      }
      builder.appendLiteral(cur, tokenEnd - cur);
    }
    cur = tokenEnd;
  }
  return builder.setType(DateTimeFormatterType::HIVE).build();
}

namespace {
std::shared_ptr<DateTimeFormatter> buildLegacyDateTimeFormatter(
    const std::string_view& format,
    DateTimeFormatterType type) {
  const bool simple = type == DateTimeFormatterType::SIMPLE_DATE_FORMAT;
  if (format.empty() && !simple) {
    BOLT_USER_FAIL("Invalid pattern specification");
  }

  bool hasWeekBasedYear = false, hasMonth = false;
  DateTimeFormatterBuilder builder(format.size());
  const char* cur = format.data();
  const char* end = cur + format.size();

  while (cur < end) {
    const char* startTokenPtr = cur;

    // Literal case
    if (*startTokenPtr == '\'') {
      // Case 1: 2 consecutive single quote
      if (cur + 1 < end && *(cur + 1) == '\'') {
        builder.appendLiteral("'");
        cur += 2;
      } else {
        // Case 2: find closing single quote
        int64_t count = numLiteralChars(startTokenPtr + 1, end);
        if (count == -1 || (simple && startTokenPtr + count + 1 >= end)) {
          BOLT_USER_FAIL("No closing single quote for literal");
        } else {
          for (int64_t i = 1; i <= count; i++) {
            builder.appendLiteral(startTokenPtr + i, 1);
            if (*(startTokenPtr + i) == '\'') {
              i += 1;
            }
          }
          cur += count + 2;
        }
      }
    } else {
      int count = 1;
      ++cur;
      while (cur < end && *startTokenPtr == *cur) {
        ++count;
        ++cur;
      }
      if (simple &&
          ((*startTokenPtr >= 'A' && *startTokenPtr <= 'Z') ||
           (*startTokenPtr >= 'a' && *startTokenPtr <= 'z'))) {
        BOLT_USER_CHECK(
            std::string_view("GyYMLwWDdFEuaHkKhmsSzZX").find(*startTokenPtr) !=
                std::string_view::npos,
            "Illegal pattern character: {}",
            *startTokenPtr);
      }
      switch (*startTokenPtr) {
        case 'G':
          builder.appendEra();
          break;
        case 'C':
          builder.appendCenturyOfEra(count);
          break;
        case 'Y':
          hasWeekBasedYear = true;
          builder.appendWeekYear(count);
          break;
        case 'w':
          builder.appendWeekOfWeekYear(count);
          break;
        case 'e':
          builder.appendDayOfWeek1Based(count);
          break;
        case 'E':
          builder.appendDayOfWeekText(count);
          break;
        case 'y':
          builder.appendYear(count);
          break;
        case 'D':
          builder.appendDayOfYear(count);
          break;
        case 'L':
          if (!simple) {
            BOLT_UNSUPPORTED("Specifier L is not supported.");
          }
          [[fallthrough]];
        case 'M':
          hasMonth = true;
          if (count <= 2) {
            builder.appendMonthOfYear(count);
          } else {
            builder.appendMonthOfYearText(count);
          }
          break;
        case 'd':
          builder.appendDayOfMonth(count);
          break;
        case 'a':
          if (!simple && ::bytedance::bolt::kSparkCompatible && count > 1) {
            BOLT_USER_FAIL("The am-pm-of-day pattern letter count must be 1.");
          }
          builder.appendHalfDayOfDay();
          break;
        case 'K':
          builder.appendHourOfHalfDay(count);
          break;
        case 'h':
          builder.appendClockHourOfHalfDay(count);
          break;
        case 'H':
          builder.appendHourOfDay(count);
          break;
        case 'k':
          builder.appendClockHourOfDay(count);
          break;
        case 'm':
          builder.appendMinuteOfHour(count);
          break;
        case 's':
          builder.appendSecondOfMinute(count);
          break;
        case 'S':
          builder.appendFractionOfSecond(count);
          break;
        case 'u':
          builder.appendDayOfWeek1Based(count);
          break;
        case 'W':
          builder.appendWeekOfMonth(count);
          break;
        case 'F':
          builder.appendDayOfWeekInMonth(count);
          break;
        case 'z':
          builder.appendTimeZone(count);
          break;
        case 'X':
          if (simple) {
            BOLT_USER_CHECK_LE(count, 3, "Invalid ISO 8601 pattern length");
            builder.appendISOTimeZoneOffsetId(count, true);
            break;
          }
          if (!::bytedance::bolt::kSparkCompatible) {
            BOLT_UNSUPPORTED("Specifier X is not supported.");
          }
          [[fallthrough]];
        case 'Z':
          builder.appendTimeZoneOffsetId(count);
          break;
        default:
          if (isalpha(*startTokenPtr)) {
            BOLT_UNSUPPORTED("Specifier {} is not supported.", *startTokenPtr);
          } else {
            builder.appendLiteral(startTokenPtr, cur - startTokenPtr);
          }
          break;
      }
    }
  }

  BOLT_USER_CHECK(
      simple || !(hasWeekBasedYear && hasMonth),
      "Cannot have both week-based year[YYYY] and month[MM] in the pattern for legacy formatter");
  return builder.setType(type).build();
}
} // namespace

std::shared_ptr<DateTimeFormatter> buildLegacySparkDateTimeFormatter(
    const std::string_view& format) {
  return buildLegacyDateTimeFormatter(
      format, DateTimeFormatterType::LEGACY_SPARK);
}

std::shared_ptr<DateTimeFormatter> buildSimpleDateTimeFormatter(
    const std::string_view& format,
    std::optional<Timestamp> twoDigitYearStart) {
  auto formatter = buildLegacyDateTimeFormatter(
      format, DateTimeFormatterType::SIMPLE_DATE_FORMAT);
  if (twoDigitYearStart) {
    formatter->twoDigitYearStartMillis_ = twoDigitYearStart->toMillis();
  } else {
    auto& calendar = simpleDateCalendar();
    UErrorCode status = U_ZERO_ERROR;
    calendar.setTime(Timestamp::now().toMillis(), status);
    calendar.add(UCAL_YEAR, -80, status);
    const auto start = calendar.getTime(status);
    BOLT_CHECK(U_SUCCESS(status), "Cannot initialize two-digit year window");
    formatter->twoDigitYearStartMillis_ = start;
  }
  return formatter;
}

} // namespace bytedance::bolt::functions
