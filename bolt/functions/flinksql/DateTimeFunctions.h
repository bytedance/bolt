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

#include "bolt/functions/Macros.h"

#include "bolt/functions/lib/TimeUtils.h"

#include <bit>
#include <cstdint>
#include <limits>
#include <string>
namespace bytedance::bolt::functions::flinksql {

template <typename T>
struct CurrentTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  const tz::TimeZone* timeZone_ = nullptr;
  bool isConstTimeZone_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* timeZoneString) {
    if (timeZoneString == nullptr || timeZoneString->empty()) {
      timeZone_ = getTimeZoneFromConfig(config);
    } else {
      timeZone_ = tz::locateZone(
          std::string_view(timeZoneString->data(), timeZoneString->size()));
      isConstTimeZone_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& timeZoneString) {
    if (!timeZone_ || !isConstTimeZone_) {
      timeZone_ = tz::locateZone(
          std::string_view(timeZoneString.data(), timeZoneString.size()));
    }
    result = Timestamp::now();
    if (timeZone_) {
      result.toTimezone(*timeZone_);
    }
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result) {
    result = Timestamp::now();
    if (timeZone_) {
      result.toTimezone(*timeZone_);
    }
  }
};

template <typename T>
struct FlinkUnixTimestampFunction {
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

template <typename T>
struct SimpleDateFormatFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

 protected:
  void initializeFormatter(
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* timeZone) {
    if (format) {
      updateFormat(*format);
      constFormat_ = true;
    }
    if (timeZone) {
      updateTimeZone(*timeZone);
      constTimeZone_ = true;
    }
  }

  void updateFormat(const arg_type<Varchar>& format) {
    const std::string_view value(format.data(), format.size());
    if (constFormat_ || (hasFormat_ && value == previousFormat_)) {
      return;
    }
    previousFormat_ = value;
    hasFormat_ = true;
    try {
      formatter_ = buildSimpleDateTimeFormatter(value);
      maxResultSize_ = formatter_->maxResultSize(nullptr);
    } catch (const BoltUserError&) {
      formatter_.reset();
    }
  }

  void updateTimeZone(const arg_type<Varchar>& timeZone) {
    const std::string_view value(timeZone.data(), timeZone.size());
    if (!constTimeZone_ && (!zone_ || value != previousTimeZone_)) {
      const auto zoneName =
          value.starts_with("GMT+") || value.starts_with("GMT-")
          ? value.substr(3)
          : value;
      zone_ = tz::locateZone(zoneName);
      previousTimeZone_ = value;
    }
  }

  std::shared_ptr<DateTimeFormatter> formatter_;
  const tz::TimeZone* zone_{nullptr};
  uint32_t maxResultSize_{0};
  bool constFormat_{false};
  bool constTimeZone_{false};
  bool hasFormat_{false};
  std::string previousFormat_;
  std::string previousTimeZone_;
};

template <typename T>
struct FlinkUnixTimestampFunctionWithTimeZone
    : public SimpleDateFormatFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* timeZone) {
    this->initializeFormatter(format, timeZone);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& timeZone) {
    result = std::numeric_limits<int64_t>::min();
    this->updateFormat(format);
    this->updateTimeZone(timeZone);
    if (!this->formatter_) {
      return;
    }
    const auto parsed = this->formatter_->parseToUtc(
        std::string_view(input.data(), input.size()), this->zone_);
    if (!parsed.hasError()) {
      const auto& timestamp = parsed.value().timestamp;
      result = timestamp.getSeconds();
      if (result < 0 && timestamp.getNanos() != 0) {
        ++result;
      }
    }
  }
};

template <typename T>
struct FlinkFromUnixtimeFunction : public SimpleDateFormatFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* timeZone) {
    this->initializeFormatter(format, timeZone);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& timeZone) {
    this->updateFormat(format);
    this->updateTimeZone(timeZone);
    if (!this->formatter_) {
      return false;
    }
    const auto millis =
        std::bit_cast<int64_t>(static_cast<uint64_t>(unixtime) * 1000);
    const auto timestamp = Timestamp::fromMillis(millis);
    result.reserve(this->maxResultSize_);
    try {
      result.resize(this->formatter_->format(
          timestamp, this->zone_, this->maxResultSize_, result.data(), true));
      return true;
    } catch (const BoltUserError&) {
      return false;
    }
  }
};

template <typename T>
struct FlinkTimestampToStringV2Function {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<int32_t>& precision) {
    int32_t trimmedPrecision = precision;
    if (trimmedPrecision < 0) {
      trimmedPrecision = 0;
    } else if (trimmedPrecision > 9) {
      trimmedPrecision = 9;
    }
    TimestampToStringOptions options;
    options.precision = TimestampToStringOptions::Precision::kNanoseconds;
    options.zeroPaddingYear = true;
    options.dateTimeSeparator = ' ';
    auto output = timestamp.toString(options);
    auto dot = output.find('.');
    if (dot != std::string::npos) {
      while (output.size() - dot - 1 > static_cast<size_t>(trimmedPrecision) &&
             output.back() == '0') {
        output.pop_back();
      }
      if (trimmedPrecision == 0 && output.back() == '.') {
        output.pop_back();
      }
    }
    result.copy_from(output);
  }
};

} // namespace bytedance::bolt::functions::flinksql
