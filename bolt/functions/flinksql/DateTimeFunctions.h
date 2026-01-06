/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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
#include "bolt/functions/sparksql/DateTimeFunctions.h"

#include <cstdint>
#include <limits>
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
struct FlinkUnixTimestampFunctionWithTimeZone
    : public sparksql::UnixTimestampFunctionBase<T> {
  const int64_t LongMinValue = std::numeric_limits<int64_t>::min();

  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Add timeZone string parameter to align with gluten
  // unix_timestamp(input, format, timezone)
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* timeZone) {
    this->initializeInternal(config, format, timeZone);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& /*timeZone*/) {
    if (this->invalidFormat_) {
      result = LongMinValue;
      return;
    }

    // Format error returns null.
    if (!this->isConstFormat_) {
      try {
        this->format_ = buildJodaDateTimeFormatter(
            std::string_view(format.data(), format.size()));
      } catch (const std::exception&) {
        result = LongMinValue;
        return;
      }
    }

    auto dateTimeResult =
        this->format_->parse(std::string_view(input.data(), input.size()));
    if (dateTimeResult.hasError()) {
      result = LongMinValue;
      return;
    }
    result = this->getResultInGMT(dateTimeResult.value());
  }
};

template <typename T>
struct FlinkFromUnixtimeFunction : public sparksql::FromUnixtimeFunction<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const int64_t LongMinValue = std::numeric_limits<int64_t>::min();

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& formatString,
      const arg_type<Varchar>& timeZoneString) {
    return sparksql::FromUnixtimeFunction<T>::call(
        result, unixtime == LongMinValue ? 0 : unixtime, formatString);
  }
};

} // namespace bytedance::bolt::functions::flinksql