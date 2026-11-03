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

#include "bolt/functions/lib/DateTimePattern.h"

namespace bytedance::bolt::functions {

namespace {
FOLLY_ALWAYS_INLINE void
appendPattern(DateTimeFormatterBuilder& builder, char letter, size_t count) {
  switch (letter) {
    case 'F':
      BOLT_USER_CHECK_EQ(count, 1, "Too many pattern letters: F");
      builder.appendDayOfWeekInMonth(count);
      break;
    case 'G':
      BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: G");
      builder.appendEra(count);
      break;
    case 'x':
    case 'X':
      BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: {}", letter);
      builder.appendISOTimeZoneOffsetId(count, letter == 'X');
      break;
    case 'y':
      builder.appendYearOfEra(count);
      break;
    case 'u':
      builder.appendYear(count);
      break;
    case 'Z':
      BOLT_USER_CHECK_LE(count, 5, "Too many pattern letters: Z");
      builder.appendRFC822TimeZoneOffsetId(count);
      break;
    case 'V':
      BOLT_USER_CHECK_EQ(count, 2, "Zone ID pattern must be VV");
      builder.appendTimeZoneId();
      break;
    case 'L':
      BOLT_USER_CHECK_LE(
          count, 4, "Unsupported Java time pattern width: {}", letter);
      detail::appendCommonDateTimePattern(builder, 'M', count);
      break;
    default:
      switch (letter) {
        case 'd':
        case 'H':
        case 'h':
        case 'K':
        case 'k':
        case 'm':
        case 's':
          BOLT_USER_CHECK_LE(count, 2, "Too many pattern letters: {}", letter);
          break;
        case 'M':
        case 'E':
        case 'z':
          BOLT_USER_CHECK_LE(
              count, 4, "Unsupported Java time pattern width: {}", letter);
          break;
        case 'D':
          BOLT_USER_CHECK_LE(count, 3, "Too many pattern letters: D");
          break;
        case 'S':
          BOLT_USER_CHECK_LE(count, 9, "Too many pattern letters: S");
          break;
        case 'a':
          BOLT_USER_CHECK_EQ(count, 1, "Too many pattern letters: a");
          break;
        default:
          BOLT_UNSUPPORTED("Specifier {} is not supported.", letter);
      }
      detail::appendCommonDateTimePattern(builder, letter, count);
  }
}

bool isNumericField(DateTimeFormatSpecifier specifier) {
  switch (specifier) {
    case DateTimeFormatSpecifier::YEAR:
    case DateTimeFormatSpecifier::YEAR_OF_ERA:
    case DateTimeFormatSpecifier::MONTH_OF_YEAR:
    case DateTimeFormatSpecifier::DAY_OF_MONTH:
    case DateTimeFormatSpecifier::DAY_OF_YEAR:
    case DateTimeFormatSpecifier::HOUR_OF_DAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_DAY:
    case DateTimeFormatSpecifier::HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::CLOCK_HOUR_OF_HALFDAY:
    case DateTimeFormatSpecifier::MINUTE_OF_HOUR:
    case DateTimeFormatSpecifier::SECOND_OF_MINUTE:
    case DateTimeFormatSpecifier::DAY_OF_WEEK_IN_MONTH:
      return true;
    default:
      return false;
  }
}
} // namespace

std::shared_ptr<DateTimeFormatter> buildJavaDateTimeFormatter(
    const std::string_view& format,
    DateTimeParseOptions options) {
  auto formatter = detail::buildPatternDateTimeFormatter<appendPattern>(
      format, DateTimeFormatterType::JAVA_TIME);
  formatter->parseOptions_ = options;
  DateTimeToken* number = nullptr;
  for (auto& token : formatter->tokens_) {
    if (token.type != DateTimeToken::Type::kPattern) {
      number = nullptr;
      continue;
    }
    auto& pattern = token.pattern;
    const bool year = pattern.specifier == DateTimeFormatSpecifier::YEAR ||
        pattern.specifier == DateTimeFormatSpecifier::YEAR_OF_ERA;
    const auto width = pattern.minRepresentDigits;
    if (year) {
      BOLT_USER_CHECK_LE(width, 19, "Too many year pattern letters");
      if (!options.yearOfEra && !formatter->hasEra_) {
        pattern.specifier = DateTimeFormatSpecifier::YEAR;
      }
    }
    const bool fixedWidth = year ? width == 2 : width > 1;
    token.maxParseDigits = fixedWidth ||
            pattern.specifier == DateTimeFormatSpecifier::FRACTION_OF_SECOND
        ? width
        : 19;
    if (!isNumericField(pattern.specifier)) {
      number = nullptr;
      continue;
    }
    if (number && fixedWidth) {
      number->followingDigits += static_cast<uint32_t>(width);
    } else {
      if (number) {
        number->followingDigits = 0;
      }
      number = &token;
    }
  }
  return formatter;
}

} // namespace bytedance::bolt::functions
