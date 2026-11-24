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
 * Hive date-time formatter profile construction.
 */

#include "bolt/functions/lib/DateTimePattern.h"

namespace bytedance::bolt::functions {

namespace {
void appendPattern(DateTimeFormatterBuilder& builder, char letter, size_t) {
  switch (letter) {
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
      BOLT_UNSUPPORTED("Specifier {} is not supported.", letter)
      break;
    default:
      builder.appendLiteral(&letter, 1);
      break;
  }
}
} // namespace

std::shared_ptr<DateTimeFormatter> buildHiveDateTimeFormatter(
    const std::string_view& format) {
  if (format.empty()) {
    BOLT_USER_FAIL("Invalid pattern specification");
  }

  return detail::buildPercentDateTimeFormatter<appendPattern>(
      format, DateTimeFormatterType::HIVE, format.size());
}

} // namespace bytedance::bolt::functions
