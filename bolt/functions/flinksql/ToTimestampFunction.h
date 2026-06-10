/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <array>
#include <memory>
#include <string>
#include <string_view>

#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/DateTimeFormatter.h"

namespace bytedance::bolt::functions::flinksql {

template <typename T>
struct FlinkToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  using DateTimeFormatterPtr = std::shared_ptr<DateTimeFormatter>;

  // Set only when the format argument is a constant (known at initialize time).
  DateTimeFormatterPtr constFormatter_;
  // One-entry memo so a non-constant format column does not rebuild the
  // formatter on every row when consecutive rows share the same format.
  std::string lastFormat_;
  DateTimeFormatterPtr lastFormatter_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*dateStr*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      constFormatter_ = buildJodaDateTimeFormatter(
          std::string_view(format->data(), format->size()));
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr) {
    return parseAuto(result, dateStr);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr,
      const arg_type<Varchar>& format) {
    const DateTimeFormatter* fmt;
    if (constFormatter_ != nullptr) {
      fmt = constFormatter_.get();
    } else {
      const auto formatView = std::string_view(format.data(), format.size());
      if (lastFormatter_ == nullptr ||
          std::string_view(lastFormat_) != formatView) {
        lastFormatter_ = buildJodaDateTimeFormatter(formatView);
        lastFormat_.assign(formatView.data(), formatView.size());
      }
      fmt = lastFormatter_.get();
    }
    return tryParse(*fmt, result, dateStr);
  }

 private:
  static constexpr const char* kDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
  static constexpr const char* kDateFormat = "yyyy-MM-dd";

  static constexpr bool isAsciiWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' ||
        c == '\v';
  }

  static std::string_view trimWhitespace(std::string_view input) {
    while (!input.empty() && isAsciiWhitespace(input.front())) {
      input.remove_prefix(1);
    }
    while (!input.empty() && isAsciiWhitespace(input.back())) {
      input.remove_suffix(1);
    }
    return input;
  }

  static const DateTimeFormatter& dateTimeFormatter() {
    static const DateTimeFormatterPtr fmt =
        buildJodaDateTimeFormatter(kDateTimeFormat);
    return *fmt;
  }

  static const DateTimeFormatter& dateFormatter() {
    static const DateTimeFormatterPtr fmt =
        buildJodaDateTimeFormatter(kDateFormat);
    return *fmt;
  }

  static const DateTimeFormatter& fallbackDateFormatter() {
    static const DateTimeFormatterPtr fmt =
        buildJodaDateTimeFormatter("yyyy-M-d");
    return *fmt;
  }

  static const DateTimeFormatter& fallbackDateTimeFormatter(size_t digits) {
    static const std::array<DateTimeFormatterPtr, 10> fmts = {
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.S"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSSSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSSSSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSSSSSS"),
        buildJodaDateTimeFormatter("yyyy-M-d HH:mm:ss.SSSSSSSSS"),
    };
    return *fmts[digits];
  }

  // Cached formatters for 'yyyy-MM-dd HH:mm:ss.S' .. with 1..9 fractional
  // digits, indexed by (string length - 21).
  static const DateTimeFormatter& fracFormatter(size_t index) {
    static const std::array<DateTimeFormatterPtr, 9> fmts = {
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.S"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSSSSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSSSSSS"),
        buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
    };
    return *fmts[index];
  }

  FOLLY_ALWAYS_INLINE bool tryParseFallback(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr) const {
    const auto trimmed =
        trimWhitespace(std::string_view(dateStr.data(), dateStr.size()));
    if (trimmed.empty()) {
      return false;
    }

    const auto space = trimmed.find(' ');
    const DateTimeFormatter* fmt;
    if (space == std::string_view::npos) {
      fmt = &fallbackDateFormatter();
    } else {
      const auto dot = trimmed.find('.', space + 1);
      size_t digits = 0;
      if (dot != std::string_view::npos) {
        digits = trimmed.size() - dot - 1;
        if (digits == 0 || digits > 9) {
          return false;
        }
      }
      fmt = &fallbackDateTimeFormatter(digits);
    }

    auto r = fmt->parse(trimmed, TimePolicy::CORRECTED);
    if (r.hasError()) {
      return false;
    }
    result = r.value().timestamp;
    return true;
  }

  FOLLY_ALWAYS_INLINE bool tryParse(
      const DateTimeFormatter& fmt,
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr) const {
    auto r = fmt.parse(
        std::string_view(dateStr.data(), dateStr.size()),
        TimePolicy::CORRECTED);
    if (r.hasError()) {
      return tryParseFallback(result, dateStr);
    }
    result = r.value().timestamp;
    return true;
  }

  // Single-arg TO_TIMESTAMP selects exactly one format from the input length,
  // matching Flink's toTimestampData. A parse failure returns NULL.
  FOLLY_ALWAYS_INLINE bool parseAuto(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& dateStr) {
    const size_t len = dateStr.size();
    if (len == 10) {
      return tryParse(dateFormatter(), result, dateStr);
    }
    if (len >= 21 && len <= 29) {
      return tryParse(fracFormatter(len - 21), result, dateStr);
    }
    return tryParse(dateTimeFormatter(), result, dateStr);
  }
};

} // namespace bytedance::bolt::functions::flinksql
