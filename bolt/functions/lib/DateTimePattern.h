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

#pragma once

#include "bolt/common/base/SparkCompatibility.h"
#include "bolt/functions/lib/DateTimeFormatterBuilder.h"

namespace bytedance::bolt::functions::detail {

inline int64_t numLiteralChars(
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

FOLLY_ALWAYS_INLINE void appendCommonDateTimePattern(
    DateTimeFormatterBuilder& builder,
    char letter,
    size_t count) {
  switch (letter) {
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
    case 'u':
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
      if (count <= 2)
        builder.appendMonthOfYear(count);
      else
        builder.appendMonthOfYearText(count);
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
    case 'z':
      builder.appendTimeZone(count);
      break;
    case 'X':
      if (!::bytedance::bolt::kSparkCompatible) {
        BOLT_UNSUPPORTED("Specifier X is not supported.");
      }
      [[fallthrough]];
    case 'Z':
      builder.appendTimeZoneOffsetId(count);
      break;
    default:
      BOLT_UNSUPPORTED("Specifier {} is not supported.", letter);
  }
}

template <auto appendPattern>
std::shared_ptr<DateTimeFormatter> buildPatternDateTimeFormatter(
    const std::string_view& format,
    DateTimeFormatterType type) {
  const bool simple = type == DateTimeFormatterType::SIMPLE_DATE_FORMAT;
  if (format.empty() && !simple) {
    BOLT_USER_FAIL("Invalid pattern specification");
  }

  DateTimeFormatterBuilder builder(format.size());
  const char* cur = format.data();
  const char* end = cur + format.size();

  while (cur < end) {
    const char* startTokenPtr = cur;

    if (*startTokenPtr == '\'') {
      if (cur + 1 < end && *(cur + 1) == '\'') {
        builder.appendLiteral("'");
        cur += 2;
      } else {
        int64_t count = numLiteralChars(startTokenPtr + 1, end);
        if (count == -1 ||
            ((simple || type == DateTimeFormatterType::JAVA_TIME) &&
             startTokenPtr + count + 1 >= end)) {
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
      const auto letter = *startTokenPtr;
      if ((letter >= 'A' && letter <= 'Z') ||
          (letter >= 'a' && letter <= 'z')) {
        appendPattern(builder, letter, count);
      } else if (isalpha(static_cast<unsigned char>(letter))) {
        BOLT_UNSUPPORTED("Specifier {} is not supported.", letter);
      } else {
        BOLT_USER_CHECK(
            type != DateTimeFormatterType::JAVA_TIME ||
                std::string_view("[]#{}").find(letter) ==
                    std::string_view::npos,
            "Unsupported Java time pattern: {}",
            letter);
        builder.appendLiteral(startTokenPtr, cur - startTokenPtr);
      }
    }
  }
  return builder.setType(type).build();
}

template <auto appendPattern>
std::shared_ptr<DateTimeFormatter> buildPercentDateTimeFormatter(
    const std::string_view& format,
    DateTimeFormatterType type,
    size_t literalBufSize) {
  DateTimeFormatterBuilder builder(literalBufSize);
  const char* cur = format.data();
  const char* end = cur + format.size();
  while (cur < end) {
    auto tokenEnd = cur;
    if (*tokenEnd == '%') {
      ++tokenEnd;
      if (tokenEnd == end) {
        break;
      }
      appendPattern(builder, *tokenEnd, 1);
      ++tokenEnd;
    } else {
      while (tokenEnd < end && *tokenEnd != '%') {
        ++tokenEnd;
      }
      builder.appendLiteral(cur, tokenEnd - cur);
    }
    cur = tokenEnd;
  }
  return builder.setType(type).build();
}

} // namespace bytedance::bolt::functions::detail
