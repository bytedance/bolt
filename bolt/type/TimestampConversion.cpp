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

// Timestamp conversion code inspired by DuckDB's date/time/timestamp conversion
// libraries. License below:

/*
 * Copyright 2018 DuckDB Contributors
 * (see https://github.com/cwida/duckdb/graphs/contributors)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "bolt/type/TimestampConversion.h"
#include <iostream>
#include <limits>
#include <set>
#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::util {

constexpr int32_t kLeapDays[] =
    {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t kNormalDays[] =
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t kCumulativeDays[] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
constexpr int32_t kCumulativeLeapDays[] =
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

constexpr int32_t kCumulativeYearDays[] = {
    0,      365,    730,    1096,   1461,   1826,   2191,   2557,   2922,
    3287,   3652,   4018,   4383,   4748,   5113,   5479,   5844,   6209,
    6574,   6940,   7305,   7670,   8035,   8401,   8766,   9131,   9496,
    9862,   10227,  10592,  10957,  11323,  11688,  12053,  12418,  12784,
    13149,  13514,  13879,  14245,  14610,  14975,  15340,  15706,  16071,
    16436,  16801,  17167,  17532,  17897,  18262,  18628,  18993,  19358,
    19723,  20089,  20454,  20819,  21184,  21550,  21915,  22280,  22645,
    23011,  23376,  23741,  24106,  24472,  24837,  25202,  25567,  25933,
    26298,  26663,  27028,  27394,  27759,  28124,  28489,  28855,  29220,
    29585,  29950,  30316,  30681,  31046,  31411,  31777,  32142,  32507,
    32872,  33238,  33603,  33968,  34333,  34699,  35064,  35429,  35794,
    36160,  36525,  36890,  37255,  37621,  37986,  38351,  38716,  39082,
    39447,  39812,  40177,  40543,  40908,  41273,  41638,  42004,  42369,
    42734,  43099,  43465,  43830,  44195,  44560,  44926,  45291,  45656,
    46021,  46387,  46752,  47117,  47482,  47847,  48212,  48577,  48942,
    49308,  49673,  50038,  50403,  50769,  51134,  51499,  51864,  52230,
    52595,  52960,  53325,  53691,  54056,  54421,  54786,  55152,  55517,
    55882,  56247,  56613,  56978,  57343,  57708,  58074,  58439,  58804,
    59169,  59535,  59900,  60265,  60630,  60996,  61361,  61726,  62091,
    62457,  62822,  63187,  63552,  63918,  64283,  64648,  65013,  65379,
    65744,  66109,  66474,  66840,  67205,  67570,  67935,  68301,  68666,
    69031,  69396,  69762,  70127,  70492,  70857,  71223,  71588,  71953,
    72318,  72684,  73049,  73414,  73779,  74145,  74510,  74875,  75240,
    75606,  75971,  76336,  76701,  77067,  77432,  77797,  78162,  78528,
    78893,  79258,  79623,  79989,  80354,  80719,  81084,  81450,  81815,
    82180,  82545,  82911,  83276,  83641,  84006,  84371,  84736,  85101,
    85466,  85832,  86197,  86562,  86927,  87293,  87658,  88023,  88388,
    88754,  89119,  89484,  89849,  90215,  90580,  90945,  91310,  91676,
    92041,  92406,  92771,  93137,  93502,  93867,  94232,  94598,  94963,
    95328,  95693,  96059,  96424,  96789,  97154,  97520,  97885,  98250,
    98615,  98981,  99346,  99711,  100076, 100442, 100807, 101172, 101537,
    101903, 102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825,
    105190, 105555, 105920, 106286, 106651, 107016, 107381, 107747, 108112,
    108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034, 111399,
    111764, 112130, 112495, 112860, 113225, 113591, 113956, 114321, 114686,
    115052, 115417, 115782, 116147, 116513, 116878, 117243, 117608, 117974,
    118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260,
    121625, 121990, 122356, 122721, 123086, 123451, 123817, 124182, 124547,
    124912, 125278, 125643, 126008, 126373, 126739, 127104, 127469, 127834,
    128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122,
    131487, 131852, 132217, 132583, 132948, 133313, 133678, 134044, 134409,
    134774, 135139, 135505, 135870, 136235, 136600, 136966, 137331, 137696,
    138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983,
    141349, 141714, 142079, 142444, 142810, 143175, 143540, 143905, 144271,
    144636, 145001, 145366, 145732, 146097,
};

namespace {

// Enum to dictate parsing modes for date strings.
//
// kStrict: For date string conversion, align with DuckDB's implementation.
//
// kNonStrict: For timestamp string conversion, align with DuckDB's
// implementation.
//
// kStandardCast: Strictly processes dates in the [+-](YYYY-MM-DD) format.
// Align with Presto casting conventions.
//
// kNonStandardCast: Like standard but permits missing day/month and allows
// trailing 'T' or spaces. Align with Spark SQL casting conventions.
enum ParseMode {
  kStrict = 1,
  kNonStrict = 2,
  kStandardCast = 4,
  kNonStandardCast = 8
};

inline bool characterIsSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' ||
      c == '\r';
}

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

bool parseDoubleDigit(
    const char* buf,
    size_t len,
    size_t& pos,
    int32_t& result) {
  if (pos < len && characterIsDigit(buf[pos])) {
    result = buf[pos++] - '0';
    if (pos < len && characterIsDigit(buf[pos])) {
      result = (buf[pos++] - '0') + result * 10;
    }
    return true;
  }
  return false;
}

bool isValidWeekDate(int32_t weekYear, int32_t weekOfYear, int32_t dayOfWeek) {
  if (dayOfWeek < 1 || dayOfWeek > 7) {
    return false;
  }
  if (weekOfYear < 1 || weekOfYear > 52) {
    return false;
  }
  if (weekYear < kMinYear || weekYear > kMaxYear) {
    return false;
  }
  return true;
}

inline bool validDate(int64_t daysSinceEpoch) {
  return daysSinceEpoch >= std::numeric_limits<int32_t>::min() &&
      daysSinceEpoch <= std::numeric_limits<int32_t>::max();
}

inline void skipSpaces(const char* buf, size_t len, size_t& pos) {
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }
}

enum class DateParseResult {
  kSuccess,
  kYearTooShort,
  kEmptyInput,
  kEmptyYear,
  kInvalidYear,
  kInvalidDate,
  kInvalidSeperator,
  kInvalidMonth,
  kInvalidDay,
  kUnexpectedEnd,
};

DateParseResult tryParseDateString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& daysSinceEpoch,
    int32_t mode,
    bool sparkCompatible = false) {
  bool isValid = true;
  pos = 0;
  if (len == 0) {
    return DateParseResult::kEmptyInput;
  }

  int32_t day = 0;
  int32_t month = -1;
  int32_t year = 0;
  bool yearneg = false;
  int sep;

  // Skip leading spaces.
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }

  if (pos >= len) {
    return DateParseResult::kEmptyInput;
  }
  if (buf[pos] == '-') {
    yearneg = true;
    pos++;
    if (pos >= len) {
      return DateParseResult::kEmptyYear;
    }
  } else if (buf[pos] == '+') {
    pos++;
    if (pos >= len) {
      return DateParseResult::kEmptyYear;
    }
  }

  if (!characterIsDigit(buf[pos])) {
    return DateParseResult::kInvalidYear;
  }
  // First parse the year.
  int yearDigitNum = 0;
  for (; pos < len && characterIsDigit(buf[pos]); pos++, yearDigitNum++) {
    year = checkedPlus((buf[pos] - '0'), checkedMultiply(year, 10));
    if (year > kMaxYear) {
      break;
    }
  }
  if ((mode & ParseMode::kNonStandardCast) && yearDigitNum < 4) {
    return DateParseResult::kYearTooShort;
  }
  if (yearneg) {
    year = checkedNegate(year);
    if (year < kMinYear) {
      return DateParseResult::kInvalidYear;
    }
  }

  // No month or day.
  if ((mode & ParseMode::kNonStandardCast) && pos == len) {
    daysSinceEpoch = daysSinceEpochFromDate(year, 1, 1, &isValid);
    return (validDate(daysSinceEpoch) && isValid)
        ? DateParseResult::kSuccess
        : DateParseResult::kInvalidDate;
  }

  if (pos >= len) {
    return DateParseResult::kUnexpectedEnd;
  }

  // Fetch the separator.
  sep = buf[pos++];
  if ((mode & ParseMode::kStandardCast) ||
      (mode & ParseMode::kNonStandardCast)) {
    // Only '-' is valid for cast.
    if (sep != '-') {
      return DateParseResult::kInvalidSeperator;
    }
  } else {
    if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
      // Invalid separator.
      return DateParseResult::kInvalidSeperator;
    }
  }

  // Parse the month.
  if (!parseDoubleDigit(buf, len, pos, month)) {
    return DateParseResult::kInvalidMonth;
  }

  // No day.
  if ((mode & ParseMode::kNonStandardCast) && pos == len) {
    daysSinceEpoch = daysSinceEpochFromDate(year, month, 1, &isValid);
    return (isValid && validDate(daysSinceEpoch))
        ? DateParseResult::kSuccess
        : DateParseResult::kInvalidDate;
  }

  if (pos >= len) {
    return DateParseResult::kInvalidDate;
  }

  if (buf[pos++] != sep) {
    return DateParseResult::kInvalidSeperator;
  }

  if (pos >= len) {
    return DateParseResult::kUnexpectedEnd;
  }

  // Now parse the day.
  if (!parseDoubleDigit(buf, len, pos, day)) {
    return DateParseResult::kInvalidDay;
  }

  if (mode & ParseMode::kStandardCast) {
    daysSinceEpoch = daysSinceEpochFromDate(year, month, day, &isValid);

    if (pos == len) {
      return (isValid && validDate(daysSinceEpoch))
          ? DateParseResult::kSuccess
          : DateParseResult::kInvalidDate;
    }
    return DateParseResult::kUnexpectedEnd;
  }

  // In non-standard cast mode, an optional trailing 'T' or space followed
  // by any optional characters are valid patterns.
  if (mode & ParseMode::kNonStandardCast) {
    daysSinceEpoch = daysSinceEpochFromDate(year, month, day, &isValid);

    if (!isValid || !validDate(daysSinceEpoch)) {
      return DateParseResult::kInvalidDate;
    }

    if (pos == len) {
      return DateParseResult::kSuccess;
    }

    if (buf[pos] == 'T' || buf[pos] == ' ') {
      return DateParseResult::kSuccess;
    }
    return DateParseResult::kUnexpectedEnd;
  }

#ifndef SPARK_COMPATIBLE
  if (!sparkCompatible) {
    // Check for an optional trailing " (BC)".
    if (len - pos >= 5 && characterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
        buf[pos + 2] == 'B' && buf[pos + 3] == 'C' && buf[pos + 4] == ')') {
      if (yearneg || year == 0) {
        return DateParseResult::kInvalidYear;
      }
      year = -year + 1;
      pos += 5;

      if (year < kMinYear) {
        return DateParseResult::kInvalidYear;
      }
    }

    // In strict mode, check remaining string for non-space characters.
    if (mode & ParseMode::kStrict) {
      // Skip trailing spaces.
      while (pos < len && characterIsSpace(buf[pos])) {
        pos++;
      }
      // Check position. if end was not reached, non-space chars remaining.
      if (pos < len) {
        return DateParseResult::kUnexpectedEnd;
      }
    } else {
      // In non-strict mode, check for any direct trailing digits.
      if (pos < len && characterIsDigit(buf[pos])) {
        return DateParseResult::kUnexpectedEnd;
      }
    }
  }
#endif

  daysSinceEpoch = daysSinceEpochFromDate(year, month, day, &isValid);
  return isValid ? DateParseResult::kSuccess : DateParseResult::kInvalidDate;
}

// String format is hh:mm:ss.microseconds (microseconds are optional).
// ISO 8601
bool tryParseTimeString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& result,
    int32_t mode) {
  int32_t hour = -1, min = -1, sec = -1, micros = -1;
  pos = 0;

  if (len == 0) {
    return false;
  }

  // Skip leading spaces.
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }

  if (pos >= len) {
    return false;
  }

  if (!characterIsDigit(buf[pos])) {
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, hour)) {
    return false;
  }
  if (hour < 0 || hour >= 24) {
    return false;
  }

  // No minute and second.
  if ((mode & ParseMode::kNonStandardCast) && pos == len) {
    result = fromTime(hour, 0, 0, 0);
    return true;
  }

  if (pos >= len) {
    return false;
  }

  // Fetch the separator.
  int sep = buf[pos++];
  if (sep != ':') {
    // Invalid separator.
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, min)) {
    return false;
  }
  if (min < 0 || min >= 60) {
    return false;
  }

  // No second.
  if ((mode & ParseMode::kNonStandardCast) && pos == len) {
    result = fromTime(hour, min, 0, 0);
    return true;
  }

  if (pos >= len) {
    return false;
  }

  if (buf[pos++] != sep) {
    return false;
  }

  if (!parseDoubleDigit(buf, len, pos, sec)) {
    return false;
  }
  if (sec < 0 || sec > 60) {
    return false;
  }

  micros = 0;
  if (pos < len && buf[pos] == '.') {
    pos++;
    // We expect microseconds.
    int32_t mult = 100000;
    for (; pos < len && characterIsDigit(buf[pos]); pos++, mult /= 10) {
      if (mult > 0) {
        micros += (buf[pos] - '0') * mult;
      }
    }
  }

  // In strict mode, check remaining string for non-space characters.
  if (mode & ParseMode::kStrict) {
    // Skip trailing spaces.
    while (pos < len && characterIsSpace(buf[pos])) {
      pos++;
    }

    // Check position. If end was not reached, non-space chars remaining.
    if (pos < len) {
      return false;
    }
  }
  result = fromTime(hour, min, sec, micros);
  return true;
}

// String format is "YYYY-MM-DD hh:mm:ss.microseconds" (seconds and microseconds
// are optional). ISO 8601
bool tryParseTimestampString(
    const char* buf,
    size_t len,
    size_t& pos,
    Timestamp& result) {
  int64_t daysSinceEpoch = 0;
  int64_t microsSinceMidnight = 0;
  if (tryParseDateString(
          buf,
          len,
          pos,
          daysSinceEpoch,
          ParseMode::kNonStrict | ParseMode::kNonStandardCast) !=
      DateParseResult::kSuccess) {
    return false;
  }

  if (pos == len) {
    // No time: only a date.
    result = fromDatetime(daysSinceEpoch, 0);
    return true;
  }

  if (buf[pos] == ' ' || buf[pos] == 'T') {
    pos++;
  }

  // Try to parse a time field.
  size_t timePos = 0;
  if (!tryParseTimeString(
          buf + pos,
          len - pos,
          timePos,
          microsSinceMidnight,
          ParseMode::kNonStrict | ParseMode::kNonStandardCast)) {
    // The rest of the string is not a valid time, but it could be relevant to
    // the caller (e.g. it could be a time zone), return the date we parsed
    // and let them decide what to do with the rest.
    result = fromDatetime(daysSinceEpoch, 0);
    return true;
  }
  pos += timePos;
  result = fromDatetime(daysSinceEpoch, microsSinceMidnight);
  return true;
}

bool tryParseUTCOffsetString(
    const char* buf,
    size_t& pos,
    size_t len,
    int& hourOffset,
    int& minuteOffset) {
  minuteOffset = 0;
  size_t curpos = pos;

  // Parse the next 3 characters.
  if (curpos + 3 > len) {
    // No characters left to parse.
    return false;
  }

  char sign_char = buf[curpos];
  if (sign_char != '+' && sign_char != '-') {
    // Expected either + or -
    return false;
  }

  curpos++;
  if (!characterIsDigit(buf[curpos]) || !characterIsDigit(buf[curpos + 1])) {
    // Expected +HH or -HH
    return false;
  }

  hourOffset = (buf[curpos] - '0') * 10 + (buf[curpos + 1] - '0');

  if (sign_char == '-') {
    hourOffset = -hourOffset;
  }
  curpos += 2;

  // Optional minute specifier: expected either "MM" or ":MM".
  if (curpos >= len) {
    // Done, nothing left.
    pos = curpos;
    return true;
  }
  if (buf[curpos] == ':') {
    curpos++;
  }

  if (curpos + 2 > len || !characterIsDigit(buf[curpos]) ||
      !characterIsDigit(buf[curpos + 1])) {
    // No MM specifier.
    pos = curpos;
    return true;
  }

  // We have an MM specifier: parse it.
  minuteOffset = (buf[curpos] - '0') * 10 + (buf[curpos + 1] - '0');
  if (sign_char == '-') {
    minuteOffset = -minuteOffset;
  }
  pos = curpos + 2;
  return true;
}

} // namespace

bool isLeapYear(int32_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool isValidDate(int32_t year, int32_t month, int32_t day) {
  if (month < 1 || month > 12) {
    return false;
  }
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (day < 1) {
    return false;
  }
  return isLeapYear(year) ? day <= kLeapDays[month] : day <= kNormalDays[month];
}

bool isValidDayOfYear(int32_t year, int32_t dayOfYear) {
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (dayOfYear < 1 || dayOfYear > 365 + isLeapYear(year)) {
    return false;
  }
  return true;
}

int64_t lastDayOfMonthSinceEpochFromDate(const std::tm& dateTime) {
  auto year = dateTime.tm_year + 1900;
  auto month = dateTime.tm_mon + 1;
  auto day = util::getMaxDayOfMonth(year, month);
  return util::daysSinceEpochFromDate(year, month, day);
}

int32_t getMaxDayOfMonth(int32_t year, int32_t month) {
  return isLeapYear(year) ? kLeapDays[month] : kNormalDays[month];
}

int64_t daysBeforeMonthFirstDay(int32_t year, int32_t month) {
  return isLeapYear(year) ? kCumulativeLeapDays[month - 1]
                          : kCumulativeDays[month - 1];
}

int64_t daysSinceEpochFromDate(
    int32_t year,
    int32_t month,
    int32_t day,
    bool* isValid) {
  int64_t daysSinceEpoch = 0;

  if (!isValidDate(year, month, day)) {
    if (isValid != nullptr) {
      *isValid = false;
      return 0;
    }
    BOLT_USER_FAIL("Date out of range: {}-{}-{}", year, month, day);
  }
  while (year < 1970) {
    year += kYearInterval;
    daysSinceEpoch -= kDaysPerYearInterval;
  }
  while (year >= 2370) {
    year -= kYearInterval;
    daysSinceEpoch += kDaysPerYearInterval;
  }
  daysSinceEpoch += kCumulativeYearDays[year - 1970];
  daysSinceEpoch += isLeapYear(year) ? kCumulativeLeapDays[month - 1]
                                     : kCumulativeDays[month - 1];
  daysSinceEpoch += day - 1;
  if (isValid != nullptr) {
    *isValid = true;
  }
  return daysSinceEpoch;
}

int64_t daysSinceEpochFromWeekDate(
    int32_t weekYear,
    int32_t weekOfYear,
    int32_t dayOfWeek) {
  if (!isValidWeekDate(weekYear, weekOfYear, dayOfWeek)) {
    BOLT_USER_FAIL(
        "Date out of range: {}-{}-{}", weekYear, weekOfYear, dayOfWeek);
  }

  int64_t daysSinceEpochOfJanFourth = daysSinceEpochFromDate(weekYear, 1, 4);
  int32_t firstDayOfWeekYear =
      extractISODayOfTheWeek(daysSinceEpochOfJanFourth);

  return daysSinceEpochOfJanFourth - (firstDayOfWeekYear - 1) +
      7 * (weekOfYear - 1) + dayOfWeek - 1;
}

int64_t daysSinceEpochFromDayOfYear(int32_t year, int32_t dayOfYear) {
  if (!isValidDayOfYear(year, dayOfYear)) {
    BOLT_USER_FAIL("Day of year out of range: {}", dayOfYear);
  }
  int64_t startOfYear = daysSinceEpochFromDate(year, 1, 1);
  return startOfYear + (dayOfYear - 1);
}

int64_t fromDateString(
    const char* str,
    size_t len,
    bool* nullOutput,
    bool sparkCompatible) {
  int64_t daysSinceEpoch;
  size_t pos = 0;

  if (tryParseDateString(
          str, len, pos, daysSinceEpoch, kStrict, sparkCompatible) !=
      DateParseResult::kSuccess) {
    if (nullOutput != nullptr) {
      *nullOutput = true;
      return 0;
    } else {
      BOLT_USER_FAIL(
          "Unable to parse date value: \"{}\", expected format is (YYYY-MM-DD)",
          std::string(str, len));
    }
  }
  return daysSinceEpoch;
}

static bool isInvalidDate(int year, int month, int day) {
  static bool MONTH_OF_31_DAYS[] = {
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      true,
      false,
      true,
      false,
      true};
  if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1 ||
      day > 31) {
    return true;
  }
  if (month == 2) {
    if (isLeapYear(year) && day > 29) {
      return true;
    } else if (!isLeapYear(year) && day > 28) {
      return true;
    }
  } else if (!MONTH_OF_31_DAYS[month] && day > 30) {
    return true;
  }
  return false;
}

std::optional<int32_t> sparkStringToDate(const char* bytes, uint32_t size) {
  int segments[] = {1, 1, 1};
  int i = 0;
  int j = 0;
  int currentSegmentValue = 0;
  while (j < size && (i < 3 && !(bytes[j] == ' ' || bytes[j] == 'T'))) {
    auto b = bytes[j];
    if (i < 2 && b == '-') {
      if (i == 0 && j != 4) {
        // year should have exact four digits
        return std::nullopt;
      }
      segments[i] = currentSegmentValue;
      currentSegmentValue = 0;
      i += 1;
    } else {
      auto parsedValue = b - '0';
      if (parsedValue < 0 || parsedValue > 9) {
        return std ::nullopt;
      } else {
        currentSegmentValue = currentSegmentValue * 10 + parsedValue;
      }
    }
    j += 1;
  }
  if (i == 0 && j != 4) {
    // year should have exact four digits
    return std::nullopt;
  }
  segments[i] = currentSegmentValue;
  if (isInvalidDate(segments[0], segments[1], segments[2])) {
    return std::nullopt;
  }
  return daysSinceEpochFromDate(segments[0], segments[1], segments[2]);
}

std::optional<int32_t>
castFromDateString(const char* str, size_t len, bool isIso8601) {
  int64_t daysSinceEpoch;
  size_t pos = 0;

  auto mode =
      isIso8601 ? ParseMode::kStandardCast : ParseMode::kNonStandardCast;
  auto result = tryParseDateString(str, len, pos, daysSinceEpoch, mode);
  if (result != DateParseResult::kSuccess) {
    return std::nullopt;
  }
  return daysSinceEpoch;
}

int32_t extractISODayOfTheWeek(int32_t daysSinceEpoch) {
  // date of 0 is 1970-01-01, which was a Thursday (4)
  // -7 = 4
  // -6 = 5
  // -5 = 6
  // -4 = 7
  // -3 = 1
  // -2 = 2
  // -1 = 3
  // 0  = 4
  // 1  = 5
  // 2  = 6
  // 3  = 7
  // 4  = 1
  // 5  = 2
  // 6  = 3
  // 7  = 4
  if (daysSinceEpoch < 0) {
    // negative date: start off at 4 and cycle downwards
    return (7 - ((-int64_t(daysSinceEpoch) + 3) % 7));
  } else {
    // positive date: start off at 4 and cycle upwards
    return ((int64_t(daysSinceEpoch) + 3) % 7) + 1;
  }
}

int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
  int64_t result;
  result = hour; // hours
  result = result * kMinsPerHour + minute; // hours -> minutes
  result = result * kSecsPerMinute + second; // minutes -> seconds
  result = result * kMicrosPerSec + microseconds; // seconds -> microseconds
  return result;
}

int64_t fromTimeString(const char* str, size_t len, bool* nullOutput) {
  if (len <= 0) {
    if (nullOutput != nullptr) {
      *nullOutput = true;
      return 0;
    } else {
      BOLT_USER_FAIL("Unable to parse empty string to time");
    }
  }

  int64_t microsSinceMidnight;
  size_t pos = 0;
  int64_t sign = 1;
  if (str[0] == '+') {
    pos++;
  } else if (str[0] == '-') {
    sign = -1;
    pos++;
  }

  if (!tryParseTimeString(
          str + pos,
          len - pos,
          pos,
          microsSinceMidnight,
#ifndef SPARK_COMPATIBLE
          ParseMode::kStrict)) {
#else
          ParseMode::kNonStrict | ParseMode::kNonStandardCast)) {
#endif
    if (nullOutput != nullptr) {
      *nullOutput = true;
      return 0;
    } else {
      BOLT_USER_FAIL(
          "Unable to parse time value: \"{}\", "
          "expected format is (HH:MM:SS[.MS])",
          std::string(str, len));
    }
  }
  return sign * microsSinceMidnight;
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight) {
  int64_t secondsSinceEpoch =
      static_cast<int64_t>(daysSinceEpoch) * kSecsPerDay;
  secondsSinceEpoch += microsSinceMidnight / kMicrosPerSec;
  return Timestamp(
      secondsSinceEpoch,
      (microsSinceMidnight % kMicrosPerSec) * kNanosPerMicro);
}

namespace {

void parserError(const char* str, size_t len) {
  BOLT_USER_FAIL(
      "Unable to parse timestamp value: \"{}\", "
      "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
      std::string(str, len));
}

} // namespace

Timestamp fromTimestampString(const char* str, size_t len, bool* nullOutput) {
  size_t pos;
  int64_t daysSinceEpoch;
  int64_t microsSinceMidnight;

  auto result = tryParseDateString(
      str,
      len,
      pos,
      daysSinceEpoch,
#ifndef SPARK_COMPATIBLE
      ParseMode::kNonStrict);
#else
      ParseMode::kNonStrict | ParseMode::kNonStandardCast);
#endif
  if (result != DateParseResult::kSuccess) {
    if (nullOutput != nullptr) {
      *nullOutput = true;
      return Timestamp{};
    } else {
      parserError(str, len);
    }
  }

  if (pos == len) {
    // No time: only a date.
    return fromDatetime(daysSinceEpoch, 0);
  }

  // Try to parse a time field.
  if (str[pos] == ' ' || str[pos] == 'T') {
    pos++;
  }

  size_t timePos = 0;
  if (!tryParseTimeString(
          str + pos,
          len - pos,
          timePos,
          microsSinceMidnight,
#ifndef SPARK_COMPATIBLE
          ParseMode::kNonStrict)) {
#else
          ParseMode::kNonStrict | ParseMode::kNonStandardCast)) {
#endif
    if (nullOutput != nullptr) {
      *nullOutput = true;
      return Timestamp{};
    } else {
      parserError(str, len);
    }
  }

  pos += timePos;
  auto timestamp = fromDatetime(daysSinceEpoch, microsSinceMidnight);

  if (pos < len) {
    // Skip a "Z" at the end (as per the ISO 8601 specs).
    if (str[pos] == 'Z') {
      pos++;
    }
    int hourOffset, minuteOffset;
    if (tryParseUTCOffsetString(str, pos, len, hourOffset, minuteOffset)) {
      int32_t secondOffset =
          (hourOffset * kSecsPerHour) + (minuteOffset * kSecsPerMinute);
      timestamp = Timestamp(
          timestamp.getSeconds() - secondOffset, timestamp.getNanos());
    }

    // Skip any spaces at the end.
    while (pos < len && characterIsSpace(str[pos])) {
      pos++;
    }
    if (pos < len) {
      if (nullOutput != nullptr) {
        *nullOutput = true;
        return Timestamp{};
      } else {
        parserError(str, len);
      }
    }
  }
  return timestamp;
}

bool removePrefix(std::string_view& input, std::string_view prefix) {
  if (input.substr(0, prefix.size()) == prefix) {
    input = input.substr(prefix.size());
    return true;
  }
  return false;
}

size_t findFirstPlusOrMinus(std::string_view timezone) {
  size_t plusPos = timezone.find('+');
  size_t minusPos = timezone.find('-');
  if (plusPos == std::string_view::npos) {
    return minusPos;
  }
  if (minusPos == std::string_view::npos) {
    return plusPos;
  }
  return std::min(plusPos, minusPos);
}

bool matchSubstring(
    std::string_view str,
    size_t pos1,
    size_t pos2,
    const std::set<std::string_view>& targets) {
  if (pos1 > pos2 || pos2 >= str.size()) {
    return false;
  }
  std::string_view sub = str.substr(pos1, pos2 - pos1);
  return targets.find(sub) != targets.end();
}

std::optional<std::pair<Timestamp, int16_t>> fromTimestampWithTimezoneString(
    const char* str,
    size_t len) {
  size_t pos;
  Timestamp resultTimestamp;

  if (!tryParseTimestampString(str, len, pos, resultTimestamp)) {
    return std::nullopt;
  }

  int16_t timezoneID = -1;

  if (pos < len && characterIsSpace(str[pos])) {
    pos++;
  }

  // If there is anything left to parse, it must be a timezone definition.
  if (pos < len) {
    size_t timezonePos = pos;
    while (timezonePos < len && !characterIsSpace(str[timezonePos])) {
      timezonePos++;
    }
    std::string_view timezone(str + pos, timezonePos - pos);

    if (timezone.size() > 3) {
      auto opPos = findFirstPlusOrMinus(timezone);
      if (opPos > 0 && opPos != std::string_view::npos) {
        std::set<std::string_view> targets = {
            "UTC", "UCT", "GMT0", "GMT", "UT"};
        if (matchSubstring(timezone, 0, opPos, targets)) {
          timezone = timezone.substr(opPos);
        } else {
          return std::nullopt;
        }
      }
    }

    if ((timezoneID = tz::getTimeZoneID(timezone, false)) == -1) {
      return std::nullopt;
    }

    // Skip any spaces at the end.
    pos = timezonePos;
    skipSpaces(str, len, pos);

    if (pos < len) {
      return std::nullopt;
    }
  }
  return std::make_pair(resultTimestamp, timezoneID);
}

namespace {

CivilDate civilFromDaysSinceEpoch(int64_t daysSinceEpoch) {
  // Algorithm derived from Howard Hinnant's civil calendar conversions.
  // https://howardhinnant.github.io/date_algorithms.html
  // Copyright (c) 2015, 2016 Howard Hinnant
  //
  // This code is licensed under the MIT license.
  // https://github.com/HowardHinnant/date
  int64_t z = daysSinceEpoch + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const uint32_t doe = static_cast<uint32_t>(z - era * 146097);
  const uint32_t yoe =
      (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
  int64_t y = static_cast<int64_t>(yoe) + era * 400;
  const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
  const uint32_t mp = (5 * doy + 2) / 153; // [0, 11]
  const uint32_t day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
  const uint32_t month = mp + (mp < 10 ? 3 : -9); // [1, 12]
  y += (month <= 2);
  return {
      static_cast<int32_t>(y),
      static_cast<int32_t>(month),
      static_cast<int32_t>(day)};
}

int32_t weekdayFromDaysSinceEpoch(int64_t daysSinceEpoch) {
  // 1970-01-01 is Thursday which maps to 4 with Sunday = 0 encoding.
  auto weekday = static_cast<int32_t>((daysSinceEpoch + 4) % 7);
  return weekday < 0 ? weekday + 7 : weekday;
}

CivilTime nanosToCivilTime(uint64_t nanosInDay) {
  constexpr uint64_t kNanosPerHour = Timestamp::kNanosInSecond * kSecsPerHour;
  constexpr uint64_t kNanosPerMinute =
      Timestamp::kNanosInSecond * kSecsPerMinute;
  const auto hour = static_cast<int32_t>(nanosInDay / kNanosPerHour);
  nanosInDay %= kNanosPerHour;
  const auto minute = static_cast<int32_t>(nanosInDay / kNanosPerMinute);
  nanosInDay %= kNanosPerMinute;
  const auto second =
      static_cast<int32_t>(nanosInDay / Timestamp::kNanosInSecond);
  const auto nanosecond =
      static_cast<int32_t>(nanosInDay % Timestamp::kNanosInSecond);
  return {hour, minute, second, nanosecond};
}

void validateCivilDateTimeRange(const Timestamp& timestamp, bool isPrecision) {
  // Spark stores timestamps as int64 microseconds, while Presto stores them as
  // int64 milliseconds. Make sure the incoming timestamp can be represented in
  // the corresponding integer range before breaking it into calendar fields.
  if (isPrecision) {
    (void)timestamp.toMillis();
  } else {
    (void)timestamp.toMicros();
  }
}

} // namespace

CivilDateTime toCivilDateTime(
    const Timestamp& timestamp,
    bool allowOverflow,
    bool isPrecision) {
  if (!allowOverflow) {
    validateCivilDateTimeRange(timestamp, isPrecision);
  }

  int64_t daysSinceEpoch = 0;
  uint64_t nanosInDay = 0;
  if (isPrecision) {
    daysSinceEpoch = timestamp.getDays();
    nanosInDay = timestamp.getNanosInDay();
  } else {
    const auto millis = allowOverflow ? timestamp.toMillisAllowOverflow()
                                      : timestamp.toMillis();
    int64_t seconds = millis / 1000;
    int64_t millisRemainder = millis % 1000;
    if (millisRemainder < 0) {
      millisRemainder += 1000;
      seconds -= 1;
    }
    daysSinceEpoch = seconds / Timestamp::kSecondsInDay;
    auto secondsInDay = seconds % Timestamp::kSecondsInDay;
    if (secondsInDay < 0) {
      secondsInDay += Timestamp::kSecondsInDay;
      daysSinceEpoch -= 1;
    }
    nanosInDay =
        static_cast<uint64_t>(secondsInDay) * Timestamp::kNanosInSecond +
        static_cast<uint64_t>(millisRemainder) *
            Timestamp::kNanosecondsInMillisecond;
  }

  auto civilDate = civilFromDaysSinceEpoch(daysSinceEpoch);
  auto dayOfYear = static_cast<int32_t>(
      daysBeforeMonthFirstDay(civilDate.year, civilDate.month) + civilDate.day);
  return {
      civilDate,
      nanosToCivilTime(nanosInDay),
      daysSinceEpoch,
      weekdayFromDaysSinceEpoch(daysSinceEpoch),
      dayOfYear};
}

int32_t CivilDateTime::isoWeek() const {
  auto weeksInIsoYear = [](int32_t year) -> int32_t {
    const auto wdayJan1 =
        util::extractISODayOfTheWeek(util::daysSinceEpochFromDate(
            year,
            /*month*/ 1,
            /*day*/ 1));
    return (wdayJan1 == 4 || (wdayJan1 == 3 && util::isLeapYear(year))) ? 53
                                                                        : 52;
  };

  const auto isoWeekday = util::extractISODayOfTheWeek(daysSinceEpoch);
  int32_t isoWeek = static_cast<int32_t>((10 + dayOfYear - isoWeekday) / 7);
  int32_t isoYear = date.year;

  if (isoWeek < 1) {
    isoYear -= 1;
    isoWeek = weeksInIsoYear(isoYear);
  } else {
    const auto weeksThisYear = weeksInIsoYear(isoYear);
    if (isoWeek > weeksThisYear) {
      isoYear += 1;
      isoWeek = 1;
    }
  }
  return isoWeek;
}

} // namespace bytedance::bolt::util
