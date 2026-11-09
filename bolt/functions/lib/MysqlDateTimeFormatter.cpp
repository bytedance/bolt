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
 *
 * MySQL percent-style pattern translation and formatter construction.
 */

#include "bolt/functions/lib/DateTimePattern.h"

namespace bytedance::bolt::functions {

namespace {
size_t countOccurence(const std::string_view& base, const std::string& target) {
  int occurrences = 0;
  std::string::size_type pos = 0;
  while ((pos = base.find(target, pos)) != std::string::npos) {
    ++occurrences;
    pos += target.length();
  }
  return occurrences;
}

void appendPattern(DateTimeFormatterBuilder& builder, char letter, size_t) {
  switch (letter) {
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
      BOLT_UNSUPPORTED("Specifier {} is not supported.", letter)
    default:
      builder.appendLiteral(&letter, 1);
      break;
  }
}
} // namespace

std::shared_ptr<DateTimeFormatter> buildMysqlDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    BOLT_USER_FAIL("Both printing and parsing not supported");
  }

  // For %r we should reserve 1 extra space because it has 3 literals ':' ':'
  // and ' '
  return detail::buildPercentDateTimeFormatter<appendPattern>(
      format,
      DateTimeFormatterType::MYSQL,
      format.size() + countOccurence(format, "%r"));
}

} // namespace bytedance::bolt::functions
