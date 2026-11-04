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

namespace bytedance::bolt::functions {

std::shared_ptr<DateTimeFormatter> buildSparkDateTimeFormatter(
    const std::string_view& format,
    bool isParsing) {
  return buildSparkDateTimeFormatter(format, isParsing, false);
}

std::shared_ptr<DateTimeFormatter> buildSparkDateTimeFormatter(
    const std::string_view& format,
    bool isParsing,
    bool standaloneMonthText) {
  const auto pattern = format;
  bool quoted = false;
  for (size_t i = 0; i < pattern.size();) {
    const char letter = pattern[i];
    if (letter == '\'') {
      if (i + 1 < pattern.size() && pattern[i + 1] == '\'') {
        i += 2;
      } else {
        quoted = !quoted;
        ++i;
      }
      continue;
    }
    const auto start = i++;
    while (i < pattern.size() && pattern[i] == letter) {
      ++i;
    }
    if (quoted) {
      continue;
    }
    switch (letter) {
      case 'Y':
      case 'W':
      case 'w':
      case 'u':
      case 'e':
      case 'c':
        BOLT_USER_FAIL(
            "Datetime patterns week-based year or field {} is unsupported",
            letter);
      case 'A':
      case 'B':
      case 'n':
      case 'N':
      case 'p':
        BOLT_USER_FAIL("Illegal pattern character: {}", letter);
      case 'E':
      case 'F':
      case 'q':
      case 'Q':
        BOLT_USER_CHECK(!isParsing, "Illegal pattern character: {}", letter);
        if (letter == 'F') {
          break;
        }
        [[fallthrough]];
      case 'G':
      case 'M':
      case 'L':
        BOLT_USER_CHECK(
            letter != 'L' || standaloneMonthText || i - start < 3,
            "Standalone month text is unsupported by the target JVM");
        BOLT_USER_CHECK_LT(
            i - start, 5, "Too many pattern letters: {}", letter);
        break;
      case 'y':
        BOLT_USER_CHECK_LT(i - start, 7, "Too many pattern letters: y");
        break;
      default:
        break;
    }
  }
  return buildJavaDateTimeFormatter(
      pattern,
      {.strict = true,
       .caseSensitive = false,
       .variableFraction = isParsing,
       .yearOfEra = false,
       .preferTimezoneId = true});
}

std::shared_ptr<DateTimeFormatter> buildLegacySparkDateTimeFormatter(
    const std::string_view& format) {
  return buildLegacySparkDateTimeFormatter(format, nullptr);
}

std::shared_ptr<DateTimeFormatter> buildLegacySparkDateTimeFormatter(
    const std::string_view& format,
    std::shared_ptr<const DateTimeFormatterContext> context) {
  return buildSimpleDateTimeFormatter(
      format, std::nullopt, false, std::move(context));
}

} // namespace bytedance::bolt::functions
