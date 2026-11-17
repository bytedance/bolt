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

#include "bolt/functions/lib/DateTimeFormatter.h"

#include <date/tz.h>
#include <charconv>
#include <cmath>
#include <cstring>

#include "bolt/common/base/CountBits.h"
#include "bolt/common/base/SparkCompatibility.h"
#include "bolt/functions/lib/DateTimeFormatterInternal.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/tz/TimeZoneNames.h"

namespace bytedance::bolt::functions {
namespace {

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

} // namespace

uint32_t DateTimeFormatter::maxResultSize(const tz::TimeZone* timezone) const {
  uint32_t size = 0;
  for (const auto& token : tokens_) {
    if (token.type == DateTimeToken::Type::kLiteral) {
      size += token.literal.size();
      continue;
    }
    if (!::bytedance::bolt::kSparkCompatible &&
        type_ != DateTimeFormatterType::SIMPLE_DATE_FORMAT &&
        type_ != DateTimeFormatterType::JAVA_TIME &&
        isSparkOnlyFormatSpecifier(token.pattern.specifier)) {
      BOLT_UNSUPPORTED(
          "format is not supported for specifier {}", token.pattern.specifier);
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::ERA:
        size += type_ == DateTimeFormatterType::JAVA_TIME &&
                token.pattern.minRepresentDigits == 4
            ? 13
            : 2;
        break;
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
        // 9 is the max size of elements in detail::weekdaysFull or
        // detail::monthsFull.
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
  Timestamp t = simple && timePolicy == TimePolicy::LEGACY
      ? detail::rebaseDateTime(
            timestamp,
            context_ && context_->rebaseTimezone ? context_->rebaseTimezone
                                                 : timezone,
            false,
            context_.get())
      : timestamp;
  const auto utcSeconds = t.getSeconds();
  if (simple) {
    timePolicy = TimePolicy::LEGACY;
    t = detail::simpleDateTimeFromUtc(t, timezone, context_.get());
  } else if (timezone) {
    t.toTimezone(*timezone);
  }
  const auto offset = t.getSeconds() - utcSeconds;
  auto civilDateTime = util::toCivilDateTime(t, allowOverflow, isPrecision);
  bool calendarInitialized = false;
  auto calendarField = [&](UCalendarDateFields field) {
    auto& calendar = detail::simpleDateCalendar();
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
          type_ != DateTimeFormatterType::JAVA_TIME &&
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
            detail::simpleCalendarField(token.pattern.specifier)));
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
          const auto width = token.pattern.minRepresentDigits;
          const std::string_view piece =
              type_ != DateTimeFormatterType::JAVA_TIME || width < 4
              ? (calDate.year > 0 ? "AD" : "BC")
              : width == 4
              ? (calDate.year > 0 ? "Anno Domini" : "Before Christ")
              : (calDate.year > 0 ? "A" : "B");
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
          if (type_ == DateTimeFormatterType::JAVA_TIME && year <= 0) {
            year = 1 - year;
          }
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
            bool addPlus = (type_ == DateTimeFormatterType::JAVA_TIME ||
                            ::bytedance::bolt::kSparkCompatible) &&
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
            piece = detail::weekdaysShort[weekdayNum];
          } else {
            piece = detail::weekdaysFull[weekdayNum];
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
          if ((simple ||
               (::bytedance::bolt::kSparkCompatible &&
                type_ != DateTimeFormatterType::JAVA_TIME)) &&
              year <= 0 && (hasEra_ || timePolicy == TimePolicy::LEGACY)) {
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
            bool addPlus = (type_ == DateTimeFormatterType::JAVA_TIME ||
                            ::bytedance::bolt::kSparkCompatible) &&
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
          if (type_ == DateTimeFormatterType::JAVA_TIME) {
            result += padContent(
                (calDate.day - 1) % 7 + 1,
                '0',
                token.pattern.minRepresentDigits,
                maxResultEnd,
                result);
            break;
          }
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
          if (type_ == DateTimeFormatterType::JAVA_TIME &&
              token.pattern.minRepresentDigits == 2) {
            BOLT_USER_CHECK_LE(dayOfYear, 99, "Day of year exceeds DD width");
          }
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
            piece = detail::monthsShort[calDate.month - 1];
          } else {
            piece = detail::monthsFull[calDate.month - 1];
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
          if (simple || type_ == DateTimeFormatterType::JAVA_TIME) {
            std::string_view piece;
            std::string fixed;
            if (timezone && timezone->id() != 0 && timezone->offset()) {
              fixed = simple ? "GMT" + timezone->name() : timezone->name();
              piece = fixed;
            } else {
              const auto& names = tz::getTimeZoneNames().at(
                  !timezone || timezone->id() == 0 ? "UTC" : timezone->name());
              const bool daylight = timezone &&
                  timestamp.getSeconds() >= -2'208'988'800 &&
                  timezone->getInfo(::date::sys_seconds(std::chrono::seconds(
                                        timestamp.getSeconds())))
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

} // namespace bytedance::bolt::functions
