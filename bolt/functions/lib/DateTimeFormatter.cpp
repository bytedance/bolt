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

#include "bolt/functions/lib/DateTimeFormatter.h"

#include <bolt/common/base/Exceptions.h>
#include <date/tz.h>
#include <folly/String.h>
#include <charconv>
#include <cstring>
#include <stdexcept>

#include "bolt/common/base/CountBits.h"
#include "bolt/functions/lib/DateTimeFormatterBuilder.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneMap.h"
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
  int32_t microsecond = 0;
  bool isAm = true; // AM -> true, PM -> false
  int64_t timezoneId = -1;

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
#ifdef SPARK_COMPATIBLE
      // spark accept timezone in lower case
      std::string upper;
      for (auto& c : zone) {
        upper.push_back(toupper(c));
      }
      zone = upper;
#endif
      auto it = defaultTzNames.find(zone);
      if (it != defaultTzNames.end()) {
        date.timezoneId = it->second;
        return 3;
      }
    }
#ifndef SPARK_COMPATIBLE
    // The format 'UT' is also accepted for UTC.
    else if ((end - cur == 2) && (*cur == 'U') && (*(cur + 1) == 'T')) {
      date.timezoneId = 0;
      return 2;
    }
#else
    if ((*cur == '+') || (*cur == '-')) {
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
#endif
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
    case DateTimeFormatSpecifier::LITERAL_PERCENT:
      return "LITERAL_PERCENT";
    case DateTimeFormatSpecifier::WEEK_OF_MONTH:
      return "WEEK_OF_MONTH";
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      return "DAY_OF_WEEK_IN_MONTH";
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
#ifdef SPARK_COMPATIBLE
    date.isHourOfHalfDay = true; // for get_timestamp("PM", "a") case
#endif
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
#ifdef SPARK_COMPATIBLE
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
#else
      number *= std::pow(10, 3 - count);
#endif
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
      while (cur < end && cur < startPos + maxDigitConsume &&
             characterIsDigit(*cur)) {
        number = number * 10 + (*cur - '0');
        ++cur;
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
#ifdef SPARK_COMPATIBLE
        date.microsecond = number;
#else
        date.microsecond = number * util::kMicrosPerMsec;
#endif
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

#ifdef SPARK_COMPATIBLE
      case DateTimeFormatSpecifier::WEEK_OF_MONTH:
        if (number < 1 || number > 5) {
          return ErrorCode::WEEK_OUT_OF_RANGE;
        }
        date.weekInMonth = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        break;

      case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
        if (number < 1 || number > 5) {
          return ErrorCode::WEEK_OUT_OF_RANGE;
        }
        date.dayOfWeekInMonth = number;
        date.weekDateFormat = true;
        date.dayOfYearFormat = false;
        break;
#endif

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

} // namespace

uint32_t DateTimeFormatter::maxResultSize(const tz::TimeZone* timezone) const {
  uint32_t size = 0;
  for (const auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      size += token.literal.size();
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::ERA:
      case DateTimeFormatSpecifier::HALFDAY_OF_DAY:
        // Fixed size.
        size += 2;
        break;
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
        // Timestamp is in [-32767-01-01, 32767-12-31] range.
        size += std::max((int)token.pattern.minRepresentDigits, 6);
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
      case DateTimeFormatSpecifier::YEAR:
#ifdef SPARK_COMPATIBLE
      case DateTimeFormatSpecifier::WEEK_YEAR:
#endif
        // Timestamp is in [-32767-01-01, 32767-12-31] range.
        size += token.pattern.minRepresentDigits == 2
            ? 2
            : std::max((int)token.pattern.minRepresentDigits, 6);
        break;
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
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
#ifdef SPARK_COMPATIBLE
      case DateTimeFormatSpecifier::WEEK_OF_MONTH:
      case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      case DateTimeFormatSpecifier::WEEK_OF_WEEK_YEAR:
#endif
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
    bool isPrecision) const {
  Timestamp t = timestamp;
  if (timezone != nullptr) {
    t.toTimezone(*timezone);
  }
  const auto civilDateTime =
      util::toCivilDateTime(t, allowOverflow, isPrecision);
  const auto& calDate = civilDateTime.date;
  const auto& durationInTheDay = civilDateTime.time;
  const auto weekdayNum = civilDateTime.weekday;
  const auto dayOfYear = civilDateTime.dayOfYear;
  const auto daysSinceEpoch = civilDateTime.daysSinceEpoch;
#ifdef SPARK_COMPATIBLE
  auto weekdayFromDays = [](int64_t daysSinceEpochInput) {
    auto weekday = static_cast<int32_t>((daysSinceEpochInput + 4) % 7);
    return weekday < 0 ? weekday + 7 : weekday;
  };
#endif

  const char* resultStart = result;
  char* maxResultEnd = result + maxResultSize;
  for (auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      std::memcpy(result, token.literal.data(), token.literal.size());
      result += token.literal.size();
    } else {
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
#ifdef SPARK_COMPATIBLE
            // spark compatibility: year should contain sign if > 9999
            if (year > 9999 && token.pattern.minRepresentDigits >= 4) {
              *result++ = '+';
            }
#endif
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

#ifdef SPARK_COMPATIBLE
        case DateTimeFormatSpecifier::WEEK_YEAR:
#endif
        case DateTimeFormatSpecifier::YEAR: {
          auto year = calDate.year;
#ifdef SPARK_COMPATIBLE
          if (token.pattern.specifier == DateTimeFormatSpecifier::WEEK_YEAR) {
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
          if (year < 0 && (hasEra_ || timePolicy == TimePolicy::LEGACY)) {
            // for spark compatibility, year should be positive for LEGACY
            // policy or has Era
            year = abs(year) + 1;
          }
#endif
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
#ifdef SPARK_COMPATIBLE
            // spark compatibility: year should contain sign if > 9999
            if (year > 9999 && token.pattern.minRepresentDigits >= 4) {
              *result++ = '+';
            }
#endif
            result += padContent(
                year,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
          }
        } break;

#ifdef SPARK_COMPATIBLE
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

          // like WEEK_OF_WEEK_YEAR, but calculate from the first day of month
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

          // like WEEK_OF_MONTH, but calculates which occurrence of this weekday
          // in the month
          auto currentDayOfWeek = weekdayNum; // 0=Sunday, 6=Saturday
          auto dayOfMonth = calDate.day;

          auto firstDayOfMonth =
              util::daysSinceEpochFromDate(calDate.year, calDate.month, 1);
          auto firstDayOfMonthWeekday = weekdayFromDays(firstDayOfMonth);

          // Calculate the first occurrence of the current day of week in the
          // month
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
#endif

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
          const auto& piece = formatFractionOfSecond(
              durationInTheDay.nanosecond, token.pattern.minRepresentDigits);
          std::memcpy(result, piece.data(), piece.length());
          result += piece.length();
        } break;

        case DateTimeFormatSpecifier::TIMEZONE: {
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

        case DateTimeFormatSpecifier::TIMEZONE_OFFSET_ID:
          // TODO: implement timezone offset id formatting, need a map from full
          // name to offset time
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

DateTimeResult DateTimeFormatter::parse(const std::string_view& input) const {
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
      util::fromTime(date.hour, date.minute, date.second, date.microsecond);
  return DateTimeResult{
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight), date.timezoneId};
}

// sql.legacy.timeParserPolicy = "legacy", "corrected", "exception"
DateTimeResult DateTimeFormatter::parse(
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
        if (tok.literal.size() > end - cur ||
            std::memcmp(cur, tok.literal.data(), tok.literal.size()) != 0) {
          return DateTimeResult::ErrorCode::PARSE_LITERAL_ERROR;
        }
        cur += tok.literal.size();
        break;
      case DateTimeToken::Type::kPattern:
        if (i + 1 < tokens_.size() &&
            tokens_[i + 1].type == DateTimeToken::Type::kPattern) {
          auto errorCode = parseFromPattern(
              tok.pattern, input, cur, end, date, true, type_, isLegacy);
          if (!noError(errorCode)) {
            return errorCode;
          }
        } else {
          auto errorCode = parseFromPattern(
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

  int64_t microsSinceMidnight =
      util::fromTime(date.hour, date.minute, date.second, date.microsecond);
  return DateTimeResult{
      util::fromDatetime(daysSinceEpoch, microsSinceMidnight), date.timezoneId};
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

std::shared_ptr<DateTimeFormatter> buildJodaDateTimeFormatter(
    const std::string_view& format) {
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
#ifdef SPARK_COMPATIBLE
          if (count > 1) {
            BOLT_USER_FAIL("The am-pm-of-day pattern letter count must be 1.");
          }
#endif
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
        case 'z':
          builder.appendTimeZone(count);
          break;
        case 'Z':
#ifdef SPARK_COMPATIBLE
        case 'X':
#endif
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
  return builder.setType(DateTimeFormatterType::JODA).build();
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

// https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
std::shared_ptr<DateTimeFormatter> buildLegacySparkDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
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
#ifdef SPARK_COMPATIBLE
          if (count > 1) {
            BOLT_USER_FAIL("The am-pm-of-day pattern letter count must be 1.");
          }
#endif
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
        case 'Z':
#ifdef SPARK_COMPATIBLE
        case 'X':
#endif
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
      !(hasWeekBasedYear && hasMonth),
      "Cannot have both week-based year[YYYY] and month[MM] in the pattern for legacy formatter");
  return builder.setType(DateTimeFormatterType::LEGACY_SPARK).build();
}

} // namespace bytedance::bolt::functions
