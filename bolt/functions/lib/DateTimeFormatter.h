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

#pragma once

#include <string>
#include <vector>
#include "bolt/common/base/Exceptions.h"
#include "bolt/type/Timestamp.h"
namespace bytedance::bolt::functions {

enum class DateTimeFormatterType { JODA, MYSQL, HIVE, LEGACY_SPARK, UNKNOWN };

enum class DateTimeFormatSpecifier : uint8_t {
  // Era, e.g: "AD"
  ERA = 0,

  // Century of era (>=0), e.g: 20
  CENTURY_OF_ERA = 1,

  // Year of era (>=0), e.g: 1996
  YEAR_OF_ERA = 2,

  // Week year based on ISO week date, e.g: 1996
  WEEK_YEAR = 3,

  // Week of week year based on ISO week date, e.g: 27
  WEEK_OF_WEEK_YEAR = 4,

  // Day of week, 0 ~ 6 with 0 representing Sunday
  DAY_OF_WEEK_0_BASED = 5,

  // Day of week, 1 ~ 7
  DAY_OF_WEEK_1_BASED = 6,

  // Day of week, e.g: "Tuesday" or "Tue", depending on number of times provided
  // formatter character repeats
  DAY_OF_WEEK_TEXT = 7,

  // Year, can be negative e.g: 1996, -2000
  YEAR = 8,

  // Day of year, 1 ~ 366 e.g: 189
  DAY_OF_YEAR = 9,

  // Month of year, e.g: 07, or 7 depending on number of times provided
  // formatter character repeats
  MONTH_OF_YEAR = 10,

  // Month of year, e.g. Dec, December depending on number of times provided
  // formatter character repeats
  MONTH_OF_YEAR_TEXT = 11,

  // Day of month, e.g: 10, 01, 001, with/without padding 0s depending on number
  // of times provided formatter character repeats
  DAY_OF_MONTH = 12,

  // Halfday of day, e.g: "PM"
  HALFDAY_OF_DAY = 13,

  // Hour of halfday (0~11)
  HOUR_OF_HALFDAY = 14,

  // Clockhour of halfday (1~12)
  CLOCK_HOUR_OF_HALFDAY = 15,

  // Hour of day (0~23)
  HOUR_OF_DAY = 16,

  // Clockhour of day (1~24)
  CLOCK_HOUR_OF_DAY = 17,

  // Minute of hour, e.g: 30
  MINUTE_OF_HOUR = 18,

  // Second of minute, e.g: 55
  SECOND_OF_MINUTE = 19,

  // Decimal fraction of a second, e.g: The fraction of 00:00:01.987 is 987
  FRACTION_OF_SECOND = 20,

  // Timezone, e.g: "Pacific Standard Time" or "PST"
  TIMEZONE = 21,

  // Timezone offset/id, e.g: "-0800", "-08:00" or "America/Los_Angeles"
  TIMEZONE_OFFSET_ID = 22,

  // A literal % character
  LITERAL_PERCENT = 23,

  WEEK_OF_MONTH = 24,

  // day of week in month (e.g., 2 means the 2nd Wednesday of the month)
  DAY_OF_WEEK_IN_MONTH = 25,
};

enum struct TimePolicy { CORRECTED, LEGACY, EXCEPTION };
auto format_as(TimePolicy tp);

inline TimePolicy parseTimePolicy(const std::string& policy) {
  auto lower = policy;
  folly::toLowerAscii(lower);
  if (lower == "corrected") {
    return TimePolicy::CORRECTED;
  } else if (lower == "legacy") {
    return TimePolicy::LEGACY;
  } else if (lower == "exception") {
    return TimePolicy::EXCEPTION;
  } else {
    BOLT_USER_FAIL("Invalid time policy: {}", policy);
  }
}

enum class DateTimeUnit {
  kMicrosecond,
  kMillisecond,
  kSecond,
  kMinute,
  kHour,
  kDay,
  kWeek,
  kMonth,
  kQuarter,
  kYear
};

struct FormatPattern {
  DateTimeFormatSpecifier specifier;

  // The minimum number of digits the formatter is going to use to represent a
  // field. The formatter is assumed to use as few digits as possible for the
  // representation. E.g: For text representation of March, with
  // minRepresentDigits being 2 or 3 it will be 'Mar'. And with
  // minRepresentDigits being 4 or 5 it will be 'March'.
  size_t minRepresentDigits;
};

struct DateTimeToken {
  enum class Type { kPattern, kLiteral } type;
  union {
    FormatPattern pattern;
    std::string_view literal;
  };

  explicit DateTimeToken(const FormatPattern& pattern)
      : type(Type::kPattern), pattern(pattern) {}

  explicit DateTimeToken(const std::string_view& literal)
      : type(Type::kLiteral), literal(literal) {}

  bool operator==(const DateTimeToken& right) const {
    if (type == right.type) {
      if (type == Type::kLiteral) {
        return literal == right.literal;
      } else {
        return pattern.specifier == right.pattern.specifier &&
            pattern.minRepresentDigits == right.pattern.minRepresentDigits;
      }
    }
    return false;
  }
};

struct DateTimeResultValue {
  Timestamp timestamp;
  int64_t timezoneId{-1};
};

class DateTimeResult {
 public:
  // if error code is not NO_ERROR, it means exceptions happened, caller should
  // handle exception depends on its policy and error code
  enum class ErrorCode : uint8_t {
    NO_ERROR = 0,
    PARSE_LITERAL_ERROR,
    PARSE_TIMEZONE_OFFSET_ERROR,
    PARSE_TIMEZONE_ERROR,
    PARSE_ERA_ERROR,
    PARSE_MONTH_ERROR,
    PARSE_HALFDAY_ERROR,
    PARSE_DAY_ERROR,
    PARSE_YEAR_DIGIT_ERROR,
    PARSE_DIGIT_ERROR,
    YEAR_OUT_OF_RANGE,
    MONTH_OUT_OF_RANGE,
    DAY_OUT_OF_RANGE,
    HOUR_OUT_OF_RANGE,
    MINUTE_OUT_OF_RANGE,
    WEEK_OUT_OF_RANGE,
    PATTERN_TOO_SHORT,
    DAY_CONFLICT,
    UNSUPPORT_WEEK_FORMAT,
    LEGACY_INVALID_DAY,
    PARSE_FRACTION_ERROR
  };

  std::string errorCodeMsg() const {
    switch (errorCode_) {
      case ErrorCode::PARSE_LITERAL_ERROR:
        return "parse literal error.";
      case ErrorCode::PARSE_TIMEZONE_OFFSET_ERROR:
        return "parse timezone offset error.";
      case ErrorCode::PARSE_TIMEZONE_ERROR:
        return "parse timezone error.";
      case ErrorCode::PARSE_ERA_ERROR:
        return "parse era error.";
      case ErrorCode::PARSE_MONTH_ERROR:
        return "parse month error.";
      case ErrorCode::PARSE_HALFDAY_ERROR:
        return "parse half day error.";
      case ErrorCode::PARSE_DAY_ERROR:
        return "parse day error.";
      case ErrorCode::PARSE_YEAR_DIGIT_ERROR:
        return "parse year digit error.";
      case ErrorCode::PARSE_DIGIT_ERROR:
        return "parse digit error.";
      case ErrorCode::YEAR_OUT_OF_RANGE:
        return "year out of range.";
      case ErrorCode::MONTH_OUT_OF_RANGE:
        return "month out of range.";
      case ErrorCode::DAY_OUT_OF_RANGE:
        return "day out of range.";
      case ErrorCode::HOUR_OUT_OF_RANGE:
        return "hour out of range.";
      case ErrorCode::MINUTE_OUT_OF_RANGE:
        return "minute out of range.";
      case ErrorCode::WEEK_OUT_OF_RANGE:
        return "week out of range.";
      case ErrorCode::PATTERN_TOO_SHORT:
        return "pattern too short.";
      case ErrorCode::PARSE_FRACTION_ERROR:
        return "parse fraction error.";
      default:
        return "";
    }
  }

  DateTimeResult(Timestamp timestamp, int64_t timezoneId)
      : value_{timestamp, timezoneId}, errorCode_(ErrorCode::NO_ERROR) {}

  DateTimeResult(ErrorCode errorCode) : errorCode_(errorCode) {
    BOLT_CHECK(errorCode != ErrorCode::NO_ERROR);
  }

  bool hasError() const {
    return errorCode_ != ErrorCode::NO_ERROR;
  }

  const DateTimeResultValue& value() const {
    throwExceptionIfErrorOccurs();
    return value_;
  }

  DateTimeResultValue& value() {
    throwExceptionIfErrorOccurs();
    return value_;
  }

  void throwExceptionIfErrorOccurs(const std::string& extraInfo = "") const {
    if (errorCode_ != ErrorCode::NO_ERROR) {
      if (extraInfo.empty()) {
        BOLT_USER_FAIL(errorCodeMsg());
      } else {
        BOLT_USER_FAIL("{}, error detail: {}", extraInfo, errorCodeMsg());
      }
    }
  }

 private:
  DateTimeResultValue value_;
  ErrorCode errorCode_;
};

/// A user defined formatter that formats/parses time to/from user provided
/// format. User can use DateTimeFormatterBuilder to build desired formatter.
/// E.g. In MySQL standard a formatter will have '%Y' '%d' and etc. as its
/// specifiers. But in Joda standard a formatter will have 'YYYY' 'dd' and etc.
/// as its specifiers. Both standards can be configured using this formatter.
class DateTimeFormatter {
 public:
  explicit DateTimeFormatter(
      std::unique_ptr<char[]>&& literalBuf,
      size_t bufSize,
      std::vector<DateTimeToken>&& tokens,
      DateTimeFormatterType type)
      : literalBuf_(std::move(literalBuf)),
        bufSize_(bufSize),
        tokens_(std::move(tokens)),
        type_(type) {
    for (const auto& token : tokens_) {
      if (token.pattern.specifier == DateTimeFormatSpecifier::ERA) {
        hasEra_ = true;
        break;
      }
    }
  }

  const std::unique_ptr<char[]>& literalBuf() const {
    return literalBuf_;
  }

  size_t bufSize() const {
    return bufSize_;
  }

  const std::vector<DateTimeToken>& tokens() const {
    return tokens_;
  }

  DateTimeResult parse(const std::string_view& input) const;

  DateTimeResult parse(
      const std::string_view& input,
      const TimePolicy timeParserPolicy) const;

  /// Returns max size of the formatted string. Can be used to preallocate
  /// memory before calling format() to avoid extra copy.
  uint32_t maxResultSize(const tz::TimeZone* timezone) const;

  int32_t format(
      const Timestamp& timestamp,
      const tz::TimeZone* timezone,
      const uint32_t maxResultSize,
      char* result,
      bool allowOverflow = false,
      TimePolicy timePolicy = TimePolicy::CORRECTED,
      bool isPrecision = true) const;

 private:
  std::unique_ptr<char[]> literalBuf_;
  size_t bufSize_;
  std::vector<DateTimeToken> tokens_;
  bool hasEra_ = false;
  DateTimeFormatterType type_;
};

std::shared_ptr<DateTimeFormatter> buildMysqlDateTimeFormatter(
    const std::string_view& format);

std::shared_ptr<DateTimeFormatter> buildJodaDateTimeFormatter(
    const std::string_view& format);

std::shared_ptr<DateTimeFormatter> buildHiveDateTimeFormatter(
    const std::string_view& format);

std::shared_ptr<DateTimeFormatter> buildLegacySparkDateTimeFormatter(
    const std::string_view& format);

} // namespace bytedance::bolt::functions

template <>
struct fmt::formatter<bytedance::bolt::functions::DateTimeFormatterType>
    : formatter<int> {
  auto format(
      bytedance::bolt::functions::DateTimeFormatterType s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct fmt::formatter<bytedance::bolt::functions::DateTimeFormatSpecifier>
    : formatter<int> {
  auto format(
      bytedance::bolt::functions::DateTimeFormatSpecifier s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
