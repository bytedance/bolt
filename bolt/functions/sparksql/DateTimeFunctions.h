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

#include <boost/algorithm/string.hpp>
#include <date/iso_week.h>

#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/functions/lib/TimeUtils.h"
#include "bolt/functions/prestosql/DateTimeImpl.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::functions::sparksql {

template <typename T>
struct YearFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }
};

template <typename T>
struct WeekFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE uint32_t getWeek(
      const Timestamp& timestamp,
      const tz::TimeZone* timezone,
      bool allowOverflow) {
    Timestamp t = timestamp;
    if (timezone) {
      t.toTimezone(*timezone);
    }
    const auto civil = util::toCivilDateTime(t, allowOverflow, false);
    return static_cast<uint32_t>(civil.isoWeek());
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeek(timestamp, this->timeZone_, false);
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    auto timestamp = Timestamp((int64_t)date * 24 * 3600, 0);
    result = getWeek(timestamp, this->timeZone_, false);
  }
};

template <typename T>
struct UnixTimestampFunctionBase {
  BOLT_DEFINE_FUNCTION_TYPES(T);

 protected:
  void setTimezone(const core::QueryConfig& config) {
    const int64_t sessionTimeZone = getTimeZoneIdFromConfig(config);
    if (sessionTimeZone) {
      if (sessionTimeZone == 1980) {
        this->isShanghai = true;
      }
      Timestamp zeroTS(0, 0);
      zeroTS.toTimezone(sessionTimeZone);
      sessionTzOffsetInSeconds_ = zeroTS.getSeconds();
      sessionTzID_ = sessionTimeZone;
    }
  }

  bool setSpecTimezone(
      const int64_t tzID,
      const tz::TimeZone* sessionTimeZone) {
    if (tzID == 0) {
      return true;
    } else if (tzID <= 1680) {
      this->sessionTzOffsetInSeconds_ = getPrestoTZOffsetInSeconds(tzID);
      return true;
    } else if (tzID == 1980) {
      this->isShanghai = true;
    }

    if (sessionTimeZone) {
      this->sessionTimeZone_ = sessionTimeZone;
      Timestamp zeroTS(0, 0);
      zeroTS.toTimezone(*sessionTimeZone);
      this->sessionTzOffsetInSeconds_ = zeroTS.getSeconds();
      return true;
    }

    return false;
  }

  int16_t getTimezoneId(const DateTimeResultValue& result) {
    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    return result.timezoneId != -1 ? result.timezoneId
                                   : sessionTzID_.value_or(0);
  }

  // calculate china/shanghai unix-timestamp
  Timestamp calculateCnUnixTimestamp(Timestamp& timestamp) {
    // between 1986 and 1991, china/shanghai use dst time
    if (timestamp.getSeconds() > 504892800 &&
        timestamp.getSeconds() < 694195200) {
      if (sessionTzID_.has_value()) {
        timestamp.toGMT(sessionTzID_.value());
        return timestamp;
      } else if (sessionTimeZone_ != nullptr) {
        timestamp.toGMT(*sessionTimeZone_);
        return timestamp;
      }
    }

    if (timestamp.getSeconds() - sessionTzOffsetInSeconds_ < kShseparator) {
      return Timestamp(
          timestamp.getSeconds() - sessionTzOffsetInSeconds_ + kShSpecOffset,
          timestamp.getNanos());
    } else {
      return Timestamp(
          timestamp.getSeconds() - sessionTzOffsetInSeconds_,
          timestamp.getNanos());
    }

    BOLT_UNREACHABLE();
  }

  int64_t getResultInGMT(DateTimeResultValue& dtr) {
    int64_t result = 0;
    if (dtr.timezoneId != -1) {
      // Use parsed timezone
      dtr.timestamp.toGMT(dtr.timezoneId);
      result = dtr.timestamp.getSeconds();
    } else if (sessionTimeZone_ != nullptr) {
      dtr.timestamp.toGMT(*sessionTimeZone_);
      result = dtr.timestamp.getSeconds();
    } else if (this->isShanghai) {
      return calculateCnUnixTimestamp(dtr.timestamp).getSeconds();

    } else {
      result = dtr.timestamp.getSeconds() - sessionTzOffsetInSeconds_;
    }

    return result;
  }
  int64_t getResultInGMT(DateTimeResultValue&& dtr) {
    return getResultInGMT(dtr);
  }

  Timestamp getTimestampResultInGMT(DateTimeResultValue& dtr) {
    if (dtr.timezoneId != -1) {
      // Use parsed timezone
      dtr.timestamp.toGMT(dtr.timezoneId);
    } else if (sessionTimeZone_ != nullptr) {
      dtr.timestamp.toGMT(*sessionTimeZone_);
    } else if (this->isShanghai) {
      return calculateCnUnixTimestamp(dtr.timestamp);
    } else {
      return Timestamp(
          dtr.timestamp.getSeconds() - sessionTzOffsetInSeconds_,
          dtr.timestamp.getNanos());
    }

    return dtr.timestamp;
  }

  Timestamp getTimestampResultInGMT(Timestamp& timestamp) {
    if (this->isShanghai) {
      return calculateCnUnixTimestamp(timestamp);
    }

    if (sessionTimeZone_ == nullptr) {
      return Timestamp(
          timestamp.getSeconds() - sessionTzOffsetInSeconds_,
          timestamp.getNanos());
    }

    timestamp.toGMT(*sessionTimeZone_);
    return timestamp;
  }

  Timestamp getTimestampResultInGMT(Timestamp&& timestamp) {
    return getTimestampResultInGMT(timestamp);
  }

  // create DateTimeFormatter according to format, return nullptr if format is
  // invalid
  std::shared_ptr<DateTimeFormatter> createFormatter(
      const arg_type<Varchar>* format) {
    try {
      if (this->timeParserPolicy == TimePolicy::LEGACY) {
        return buildLegacySparkDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      } else {
        ensureFormatLegal(std::string_view(format->data(), format->size()));
        return buildJodaDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      }
    } catch (const BoltUserError& e) {
      if (e.message().find("week-based year") != std::string::npos) {
        // rethrow exception to user
        BOLT_USER_FAIL(e.message());
      }
      return nullptr;
    }
  }

  template <typename StringType>
  void initializeInternal(
      const core::QueryConfig& config,
      const arg_type<Varchar>* format,
      const StringType* timeZone) {
    this->timeParserPolicy = parseTimePolicy(config.timeParserPolicy());

    if (format != nullptr) {
      this->format_ = createFormatter(format);
      invalidFormat_ = (this->format_ == nullptr);
      isConstFormat_ = true;
    }

    if (timeZone != nullptr) {
      // time zone like '+00:00'.
      int64_t tzID = tz::getTimeZoneID(std::string_view(*timeZone));
      // time zone like 'Asia/Shanghai'
      const tz::TimeZone* sessionTimeZone = nullptr;
      try {
        sessionTimeZone = tz::locateZone(std::string(*timeZone));
      } catch (const std::exception&) {
        sessionTimeZone = nullptr;
      }
      if (this->setSpecTimezone(tzID, sessionTimeZone)) {
        return;
      }
    } else {
      const int64_t tzID = getTimeZoneIdFromConfig(config);
      const tz::TimeZone* sessionTimeZone = nullptr;
      try {
        sessionTimeZone = getTimeZoneFromConfig(config);
      } catch (const std::exception&) {
        sessionTimeZone = nullptr;
      }
      if (this->setSpecTimezone(tzID, sessionTimeZone)) {
        return;
      }
    }

    this->setTimezone(config);
  }

  // Default if format is not specified, as per Spark documentation.
  constexpr static std::string_view kDefaultFormat{"yyyy-MM-dd HH:mm:ss"};
  constexpr static int64_t kShseparator =
      -2177481600; // 1901-01-01 00:00:00 Asia/Shanghai
                   // before 1901, Shanghai use UTC+8:05:43
                   // after, Shanghai change to CST(UTC+8)
  constexpr static int64_t kShSpecOffset = -343;
  std::shared_ptr<DateTimeFormatter> format_;
  int64_t sessionTzOffsetInSeconds_{0};

  const tz::TimeZone* sessionTimeZone_{nullptr};
  std::optional<int64_t> sessionTzID_;
  bool isShanghai{false};
  bool isConstFormat_{false};
  bool invalidFormat_{false};

  TimePolicy timeParserPolicy{TimePolicy::CORRECTED};
};

template <typename T>
struct UnixDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = date;
  }
};

template <typename T>
struct UnixTimestampFunction : public UnixTimestampFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  // unix_timestamp();
  // If no parameters, return the current unix timestamp without adjusting
  // timezones.
  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

template <typename T>
struct UnixTimestampParseFunction : public UnixTimestampFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  // unix_timestamp(input);
  // If format is not specified, assume kDefaultFormat.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    static const StringView kDefaultFormat("yyyy-MM-dd HH:mm:ss");
    this->template initializeInternal<arg_type<Varchar>>(
        config, &kDefaultFormat, nullptr);
  }

  // unix_timestamp(DATE)
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Date>* /*input*/) {
    static const StringView kDefaultFormat("yyyy-MM-dd HH:mm:ss");
    this->template initializeInternal<arg_type<Varchar>>(
        config, &kDefaultFormat, nullptr);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input) {
    auto dateTimeResult =
        this->format_->parse(std::string_view(input.data(), input.size()));
    if (dateTimeResult.hasError()) {
      return false;
    }
    result = this->getResultInGMT(dateTimeResult.value());
    return true;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& input) {
    DateTimeResultValue dateTimeResult{
        Timestamp((int64_t)input * 24 * 3600, 0)};
    result = this->getResultInGMT(dateTimeResult);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& input) {
    DateTimeResultValue dateTimeResult{input};
    result = this->getResultInGMT(dateTimeResult);
  }
};

template <typename T>
struct UnixTimestampParseWithFormatFunction
    : public UnixTimestampFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(input, format):
  // If format is constant, compile it just once per batch.
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    this->template initializeInternal<arg_type<Varchar>>(
        config, format, nullptr);
  }

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

  // get_timestamp(date, varchar, )
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Date>* /*input*/,
      const arg_type<Varchar>* format) {
    this->template initializeInternal<arg_type<Varchar>>(
        config, format, nullptr);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (this->invalidFormat_) {
      return false;
    }

    // Format error returns null.
    if (!this->isConstFormat_) {
      this->format_ = this->createFormatter(&format);
      if (this->format_ == nullptr) {
        return false;
      }
    }

    auto dateTimeResult =
        this->format_->parse(std::string_view(input.data(), input.size()));
    if (dateTimeResult.hasError()) {
      return false;
    }
    result = this->getResultInGMT(dateTimeResult.value());
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& /*timeZone*/) {
    return call(result, input, format);
  }

  // get_timestamp(varchar, format)
  FOLLY_ALWAYS_INLINE bool call(
      Timestamp& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (this->invalidFormat_) {
      if (this->timeParserPolicy != TimePolicy::LEGACY) {
        BOLT_USER_FAIL(
            "Fail to parse. You may get a different result due to the upgrading of Spark");
      }
      return false;
    }

    // Format or parsing error returns null.
    if (!this->isConstFormat_) {
      this->format_ = this->createFormatter(&format);
      if (this->format_ == nullptr) {
        if (this->timeParserPolicy == TimePolicy::EXCEPTION) {
          BOLT_USER_FAIL("Fail to parse");
        }
        return false;
      }
    }

    auto dateTimeResult = this->format_->parse(
        std::string_view(input.data(), input.size()), this->timeParserPolicy);
    if (dateTimeResult.hasError()) {
      if (this->timeParserPolicy == TimePolicy::EXCEPTION) {
        std::string msg = fmt::format(
            "parse timestamp failed, input: {}, format: {}",
            std::string_view(input.data(), input.size()),
            std::string_view(format.data(), format.size()));
        dateTimeResult.throwExceptionIfErrorOccurs(msg);
      }
      return false;
    }

    auto timestamp = dateTimeResult.value();

    result = this->getTimestampResultInGMT(dateTimeResult.value());

    return true;
  }

  // get_timestamp(date, format)
  FOLLY_ALWAYS_INLINE void call(
      Timestamp& result,
      const arg_type<Date>& input,
      const arg_type<Varchar>& format) {
    result =
        this->getTimestampResultInGMT(Timestamp((int64_t)input * 24 * 3600, 0));
  }

  // get_timestamp(date, format)
  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Date>& input,
      const arg_type<Varchar>& format) {
    result = this->getResultInGMT({Timestamp((int64_t)input * 24 * 3600, 0)});
  }
};

template <typename T>
struct UnixTimestampParseWithFormatAndTimestampFunction
    : public UnixTimestampFunctionBase<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(date, format, timezone)
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Date>* /*input*/,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* timeZone) {
    this->initializeInternal(config, format, timeZone);
  }

  // get_timestamp(timestamp, format)
  FOLLY_ALWAYS_INLINE bool callNullable(
      Timestamp& result,
      const arg_type<Timestamp>* input,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* /*timeZone*/) {
    if (input == nullptr) {
      return false;
    }
    result = Timestamp(input->getSeconds(), input->getNanos());
    return true;
  }

  // get_timestamp(timestamp, format)
  FOLLY_ALWAYS_INLINE bool callNullable(
      int64_t& result,
      const arg_type<Timestamp>* input,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* /*timeZone*/) {
    if (input == nullptr) {
      return false;
    }
    result = input->getSeconds();
    return true;
  }
  // get_timestamp(timestamp, format)
  FOLLY_ALWAYS_INLINE bool callNullable(
      Timestamp& result,
      const arg_type<Timestamp>* input,
      const arg_type<Varchar>* format) {
    if (input == nullptr) {
      return false;
    }
    result = Timestamp(input->getSeconds(), input->getNanos());
    return true;
  }

  // get_timestamp(timestamp, format)
  FOLLY_ALWAYS_INLINE bool callNullable(
      int64_t& result,
      const arg_type<Timestamp>* input,
      const arg_type<Varchar>* format) {
    if (input == nullptr) {
      return false;
    }
    result = input->getSeconds();
    return true;
  }

  // get_timestamp(date, format)
  FOLLY_ALWAYS_INLINE bool callNullable(
      int64_t& result,
      const arg_type<Date>* input,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* /*timeZone*/) {
    if (input == nullptr) {
      return false;
    }
    result =
        this->getResultInGMT({Timestamp((int64_t)(*input) * 24 * 3600, 0)});
    return true;
  }
};

template <typename T>
struct ToUtcTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      timezone_ = tz::locateZone(
          std::string_view((*timezone).data(), (*timezone).size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& timezone) {
    result = timestamp;
    auto fromTimezone = timezone_
        ? timezone_
        : tz::locateZone(std::string_view(timezone.data(), timezone.size()));
    result.toGMT(*fromTimezone);
  }

 private:
  const tz::TimeZone* timezone_{nullptr};
};

template <typename T>
struct FromUtcTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      timezone_ = tz::locateZone(
          std::string_view((*timezone).data(), (*timezone).size()));
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& timezone) {
    result = timestamp;
    auto toTimezone = timezone_
        ? timezone_
        : tz::locateZone(std::string_view(timezone.data(), timezone.size()));
    result.toTimezone(*toTimezone);
  }

 private:
  const tz::TimeZone* timezone_{nullptr};
};

/// Converts date string to Timestmap type.
template <typename T>
struct GetTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    auto sessionTimezoneName = config.sessionTimezone();
    if (!sessionTimezoneName.empty()) {
      sessionTimezoneId_ = tz::getTimeZoneID(sessionTimezoneName);
    }
    if (format != nullptr) {
      if (this->timeParserPolicy == TimePolicy::LEGACY) {
        formatter_ = buildLegacySparkDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      } else {
        // Use Joda formatter for other policies.

        ensureFormatLegal(std::string_view(format->data(), format->size()));
        formatter_ = buildJodaDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      }
      isConstantTimeFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstantTimeFormat_) {
      if (this->timeParserPolicy == TimePolicy::LEGACY) {
        formatter_ = buildLegacySparkDateTimeFormatter(
            std::string_view(format.data(), format.size()));
      } else {
        // Use Joda formatter for other policies.

        ensureFormatLegal(std::string_view(format->data(), format->size()));
        formatter_ = buildJodaDateTimeFormatter(
            std::string_view(format.data(), format.size()));
      }
    }
    auto dateTimeResult =
        formatter_->parse(std::string_view(input.data(), input.size()));
    // Null as result for parsing error.
    if (dateTimeResult.hasError()) {
      return false;
    }
    dateTimeResult.value().timestamp.toGMT(
        getTimezoneId(dateTimeResult.value()));
    result = dateTimeResult.value().timestamp;
    return true;
  }

 private:
  int16_t getTimezoneId(const DateTimeResultValue& result) const {
    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    return result.timezoneId != -1 ? result.timezoneId
                                   : sessionTimezoneId_.value_or(0);
  }

  std::shared_ptr<DateTimeFormatter> formatter_{nullptr};
  bool isConstantTimeFormat_{false};
  std::optional<int64_t> sessionTimezoneId_;
};

template <typename T>
struct MakeDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const int32_t year,
      const int32_t month,
      const int32_t day) {
    bool isValid = true;
    auto daysSinceEpoch =
        util::daysSinceEpochFromDate(year, month, day, &isValid);
    if (!isValid) {
      return false;
    }
    if (daysSinceEpoch != static_cast<int32_t>(daysSinceEpoch)) {
      return false;
    }
    result = daysSinceEpoch;
    return true;
  }
};

template <typename T>
struct LongToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;
  bool isConstTimeZone_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
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
      const arg_type<int64_t>& millis) {
    result = Timestamp::fromMillis(millis);
    if (timeZone_) {
      result.toTimezone(*timeZone_);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<int64_t>& millis,
      const arg_type<Varchar>& timeZoneString) {
    if (!timeZone_ || !isConstTimeZone_) {
      timeZone_ = tz::locateZone(
          std::string_view(timeZoneString.data(), timeZoneString.size()));
    }
    result = Timestamp::fromMillis(millis);
    if (timeZone_) {
      result.toTimezone(*timeZone_);
    }
  }
};

template <typename T>
struct LastDayFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE int64_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE int64_t getMonth(const std::tm& time) {
    return 1 + time.tm_mon;
  }

  FOLLY_ALWAYS_INLINE int64_t getDay(const std::tm& time) {
    return time.tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    auto dateTime = getDateTime(date);
    int32_t year = getYear(dateTime);
    int32_t month = getMonth(dateTime);
    int32_t day = getMonth(dateTime);
    auto lastDay = util::getMaxDayOfMonth(year, month);
    auto daysSinceEpoch = util::daysSinceEpochFromDate(year, month, lastDay);
    BOLT_USER_CHECK_EQ(
        daysSinceEpoch,
        (int32_t)daysSinceEpoch,
        "Integer overflow in last_day({}-{}-{})",
        year,
        month,
        day);
    result = daysSinceEpoch;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Date>& date) {
    auto dateTime = getDateTime(date);
    int32_t year = getYear(dateTime);
    int32_t month = getMonth(dateTime);
    int32_t day = getMonth(dateTime);
    auto lastDay = util::getMaxDayOfMonth(year, month);
    result = fmt::format("{:04d}-{:02d}-{:02d}", year, month, lastDay);
  }
};

/// Truncates a timestamp to a specified time unit. Return NULL if the format is
/// invalid. Format as abbreviated unit string and "microseconds" are allowed.
template <typename T>
struct DateTruncFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*format*/,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& format,
      const arg_type<Timestamp>& timestamp) {
    std::optional<DateTimeUnit> unitOption = fromDateTimeUnitString(
        format,
        /*throwIfInvalid=*/false,
        /*allowMicro=*/true,
        /*allowAbbreviated=*/true);
    // Return null if unit is illegal.
    if (!unitOption.has_value()) {
      return false;
    }
    result = truncateTimestamp(timestamp, unitOption.value(), timeZone_);
    return true;
  }

 private:
  const tz::TimeZone* timeZone_ = nullptr;
};

template <typename T>
struct DateFromUnixDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result, const int32_t& value) {
    result = value;
  }
};

template <typename T>
struct DateAddFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const TInput& value) {
    __builtin_add_overflow(date, value, &result);
  }
};

template <typename T>
struct DateSubFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const TInput& value) {
    __builtin_sub_overflow(date, value, &result);
  }
};

template <typename T>
struct DayOfWeekFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // 1 = Sunday, 2 = Monday, ..., 7 = Saturday
  FOLLY_ALWAYS_INLINE int32_t getDayOfWeek(const std::tm& time) {
    return time.tm_wday + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }
};

template <typename T>
struct DateDiffFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Date>& endDate,
      const arg_type<Date>& startDate)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = endDate - startDate;
  }
};

template <typename T>
struct MonthFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getMonth(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getMonth(getDateTime(timestamp, this->timeZone_));
  }
};

template <typename T>
struct QuarterFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getQuarter(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getQuarter(getDateTime(date));
  }
};

template <typename T>
struct DayFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_mday;
  }
};

template <typename T>
struct DayOfYearFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getDayOfYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfYear(getDateTime(timestamp, this->timeZone_));
  }
};

template <typename T>
struct NextDayFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Date>* /*startDate*/,
      const arg_type<Varchar>* dayOfWeek) {
    if (dayOfWeek != nullptr) {
      weekDay_ = getDayOfWeekFromString(*dayOfWeek);
      if (!weekDay_.has_value()) {
        invalidFormat_ = true;
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& startDate,
      const arg_type<Varchar>& dayOfWeek) {
    if (invalidFormat_) {
      return false;
    }
    auto weekDay = weekDay_.has_value() ? weekDay_.value()
                                        : getDayOfWeekFromString(dayOfWeek);
    if (!weekDay.has_value()) {
      return false;
    }
    auto nextDay = getNextDate(startDate, weekDay.value());
    if (nextDay != (int32_t)nextDay) {
      return false;
    }
    result = nextDay;
    return true;
  }

 private:
  static FOLLY_ALWAYS_INLINE std::optional<int8_t> getDayOfWeekFromString(
      const StringView& dayOfWeek) {
    std::string lowerDayOfWeek =
        boost::algorithm::to_lower_copy(dayOfWeek.str());
    auto it = kDayOfWeekNames.find(lowerDayOfWeek);
    if (it != kDayOfWeekNames.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  static FOLLY_ALWAYS_INLINE int64_t
  getNextDate(int64_t startDay, int8_t dayOfWeek) {
    return startDay + 1 + ((dayOfWeek - 1 - startDay) % 7 + 7) % 7;
  }

  std::optional<int8_t> weekDay_;
  bool invalidFormat_{false};
};

template <typename T>
struct HourFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_hour;
  }
};

template <typename T>
struct MinuteFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_min;
  }
};

template <typename T>
struct SecondFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, nullptr).tm_sec;
  }
};

template <typename T>
struct MakeYMIntervalFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<IntervalYearMonth>& result) {
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalYearMonth>& result,
      const int32_t year) {
    BOLT_USER_CHECK(
        !__builtin_mul_overflow(year, kMonthInYear, &result),
        "Integer overflow in make_ym_interval({})",
        year);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalYearMonth>& result,
      const int32_t year,
      const int32_t month) {
    auto totalMonths = (int64_t)year * kMonthInYear + month;
    BOLT_USER_CHECK_EQ(
        totalMonths,
        (int32_t)totalMonths,
        "Integer overflow in make_ym_interval({}, {})",
        year,
        month);
    result = totalMonths;
  }
};

template <typename T>
struct MillisecondFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }
};

// Parses unix time in seconds to a formatted string.
template <typename T>
struct FromUnixtimeFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  bool isConstFormat_ = false;
  int64_t sessionTzOffsetInSeconds_{0};
  uint32_t maxResultSize_{0};
  bool throwExceptionWhenEncounterBadTimestamp_;
  const char* badTimestampResult_ = "-12345678";
  const size_t badTimestampResultSize_ = 9;
  TimePolicy timePolicy_{TimePolicy::CORRECTED};

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* formatString,
      const arg_type<Varchar>* timeZoneString) {
    timePolicy_ = parseTimePolicy(config.timeParserPolicy());
    BOLT_CHECK(timeZoneString);

    throwExceptionWhenEncounterBadTimestamp_ =
        config.throwExceptionWhenEncounterBadTimestamp();
    int16_t tzID = tz::getTimeZoneID(std::string_view(*timeZoneString));
    // time zone like '+00:00'.
    if (tzID == 0) {
      return;
    } else if (tzID <= 1680) {
      this->sessionTzOffsetInSeconds_ = getPrestoTZOffsetInSeconds(tzID);
      return;
    } else { // time zone like 'Asia/Shanghai'
      sessionTimeZone_ = tz::locateZone(
          std::string_view(timeZoneString->data(), timeZoneString->size()));
      BOLT_CHECK(sessionTimeZone_);
    }

    if (formatString) {
      if (this->timePolicy_ == TimePolicy::LEGACY) {
        jodaDateTime_ = buildLegacySparkDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      } else {
        ensureFormatLegal(
            std::string_view(formatString->data(), formatString->size()));
        // Use Joda formatter for other policies.
        jodaDateTime_ = buildJodaDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      }
      maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* formatString) {
    timePolicy_ = parseTimePolicy(config.timeParserPolicy());

    throwExceptionWhenEncounterBadTimestamp_ =
        config.throwExceptionWhenEncounterBadTimestamp();
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      if (this->timePolicy_ == TimePolicy::LEGACY) {
        jodaDateTime_ = buildLegacySparkDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      } else {
        // Use Joda formatter for other policies.

        ensureFormatLegal(
            std::string_view(formatString->data(), formatString->size()));
        jodaDateTime_ = buildJodaDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      }
      maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/) {
    static const StringView kDefaultFormat("yyyy-MM-dd HH:mm:ss");
    initialize(inputTypes, config, nullptr, &kDefaultFormat);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      if (this->timePolicy_ == TimePolicy::LEGACY) {
        jodaDateTime_ = buildLegacySparkDateTimeFormatter(
            std::string_view(formatString.data(), formatString.size()));
      } else {
        // Use Joda formatter for other policies.
        ensureFormatLegal(
            std::string_view(formatString.data(), formatString.size()));
        jodaDateTime_ = buildJodaDateTimeFormatter(
            std::string_view(formatString.data(), formatString.size()));
      }
      maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
    }
    try {
      auto unixtimeD = unixtime + this->sessionTzOffsetInSeconds_;
      const Timestamp timestamp{unixtimeD, 0};
      result.reserve(maxResultSize_);
      const auto resultSize = jodaDateTime_->format(
          timestamp,
          sessionTimeZone_,
          maxResultSize_,
          result.data(),
          true,
          timePolicy_,
          false);
      result.resize(resultSize);
    } catch (const BoltException& e) {
      if (throwExceptionWhenEncounterBadTimestamp_) {
        BOLT_USER_FAIL(e.message());
      } else {
        result.resize(badTimestampResultSize_);
        std::memcpy(
            result.data(), badTimestampResult_, badTimestampResultSize_);
      }
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& formatString,
      const arg_type<Varchar>& timeZoneString) {
    return call(result, unixtime, formatString);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime) {
    static const StringView kDefaultFormat("yyyy-MM-dd HH:mm:ss");
    return call(result, unixtime, kDefaultFormat);
  }
};

template <typename T>
struct UnixSecondsFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.getSeconds();
  }
};

template <typename T>
struct UnixTimestampFromTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& input,
      const arg_type<Varchar>& /*format*/) {
    result = input.getSeconds();
  }
};

template <typename T>
struct FormatPdateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Convert string from yyyyMMdd to yyyy-MM-dd format
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    size_t size = input.size();
    std::string dateStr = std::string(input.data(), size);
    if (dateStr.find('-') != std::string::npos) {
      // Return input string directly to match the behavior in spark
      result.resize(size);
      std::memcpy(result.data(), input.data(), size);
      return;
    }
    if (size != 8) {
      BOLT_USER_FAIL(
          "Invalid date, expected format yyyyMMdd, but got \"{}\"", dateStr);
    }

    dateStr = dateStr.substr(0, 4) + "-" + dateStr.substr(4, 2) + "-" +
        dateStr.substr(6, 2);
    size_t resultSize = dateStr.size();
    try {
      ::bytedance::bolt::util::fromDateString(
          dateStr.data(), resultSize, nullptr);
    } catch (const bytedance::bolt::BoltUserError&) {
      BOLT_USER_FAIL(
          "Invalid date, expected format yyyyMMdd, but got \"{}\"",
          std::string(input.data(), size));
    }
    result.resize(resultSize);
    std::memcpy(result.data(), dateStr.data(), resultSize);
  }
};

template <typename T>
struct PDate2DateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  constexpr static int PDATE_LENGTH = 10;
  // PDate: yyyy-MM-dd, such as 2023-12-21, Date: yyyyMMdd, such as 20231221

  bool convert(const std::string& input, out_type<Varchar>& result) {
    // input is formated by PDate: yyyy-MM-dd
    size_t size = input.size();
    if (size != PDATE_LENGTH) {
      return false;
    }
    auto dateStr = std::string(input.data(), size);
    // yyyy-MM-dd -> yyyyMMdd
    try {
      DATE()->toDays(std::string_view{dateStr});
    } catch (const bytedance::bolt::BoltUserError& e) {
      // Return null when date format is illegal
      return false;
    }
    // out_type<Varchar> is defined as StringWriter, and StringWriter overrides
    // operator =
    result = dateStr.substr(0, 4) + dateStr.substr(5, 2) + dateStr.substr(8, 2);
    return true;
  }

  // (varchar) -> varchar
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    return convert(input, result);
  }

  // (Date) -> varchar
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Date>& input) {
    return convert(DATE()->toString(input), result);
  }
};

template <typename T>
struct Date2PDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  constexpr static int DATE_LENGTH = 8;
  // PDate: yyyy-MM-dd, such as 2023-12-21, Date: yyyyMMdd, such as 20231221

  bool convert(const std::string& dateStr, out_type<Varchar>& result) {
    size_t size = dateStr.size();
    if (size != DATE_LENGTH) {
      return false;
    }
    for (size_t i = 0; i < size; ++i) {
      if (!std::isdigit(dateStr[i])) {
        return false;
      }
    }
    // yyyyMMdd -> yyyy-MM-dd
    auto year = std::stoi(dateStr.substr(0, 4));
    auto month = std::stoi(dateStr.substr(4, 2));
    auto day = std::stoi(dateStr.substr(6, 2));

    // Compatible with Hive function `date2pdate` function using
    // DateTimeFormatter's `ResolverStyle.SMART`
    if (year == 0 || month == 0 || day == 0 || month > 12 || day > 31) {
      return false;
    }
    bool isLeapYear = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    static const std::vector<int> monthDaysInNonleapYear = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2) {
      day = std::min(
          day, monthDaysInNonleapYear[month] + static_cast<int>(isLeapYear));
    } else {
      day = std::min(day, monthDaysInNonleapYear[month]);
    }

    result = fmt::format("{:04d}-{:02d}-{:02d}", year, month, day);
    return true;
  }

  // (varchar) -> varchar
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    return convert(input, result);
  }

  // (BIGINT) -> varchar
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& input) {
    return convert(std::to_string(input), result);
  }

  // (INTEGER) -> varchar
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int32_t>& input) {
    return convert(std::to_string(input), result);
  }
};

template <typename TExec>
struct SecondsToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result, T seconds) {
    if constexpr (std::is_integral_v<T>) {
      result = Timestamp(static_cast<int64_t>(seconds), 0);
    } else {
      // Cast to double and check bounds to prevent ensuing overflow.
      const double secondsD = static_cast<double>(seconds);

      if (secondsD >= kMaxSecondsD) {
        result = Timestamp(kMaxSeconds, kMaxNanoseconds);
        return;
      }
      if (secondsD <= kMinSecondsD) {
        result = Timestamp(kMinSeconds, kMinNanoseconds);
        return;
      }

      // Scale to microseconds and truncate toward zero.
      const double microsD = secondsD * Timestamp::kMicrosecondsInSecond;
      const int64_t micros = static_cast<int64_t>(microsD);

      // Split into whole seconds and remaining microseconds.
      int64_t wholeSeconds = micros / util::kMicrosPerSec;
      int64_t remainingMicros = micros % util::kMicrosPerSec;
      if (remainingMicros < 0) {
        wholeSeconds -= 1;
        remainingMicros += util::kMicrosPerSec;
      }

      const int64_t nano =
          remainingMicros * Timestamp::kNanosecondsInMicrosecond;
      result = Timestamp(wholeSeconds, nano);
    }
  }

 private:
  static constexpr double kMaxMicrosD =
      static_cast<double>(std::numeric_limits<int64_t>::max());
  static constexpr double kMinMicrosD =
      static_cast<double>(std::numeric_limits<int64_t>::min());
  static constexpr double kMaxSecondsD =
      kMaxMicrosD / Timestamp::kMicrosecondsInSecond;
  static constexpr double kMinSecondsD =
      kMinMicrosD / Timestamp::kMicrosecondsInSecond;

  // Cutoff values are based on Java's Long.MAX_VALUE and Long.MIN_VALUE.
  static constexpr int64_t kMaxSeconds = 9223372036854LL;
  static constexpr int64_t kMaxNanoseconds = 775807000LL;
  static constexpr int64_t kMinSeconds = -9223372036855LL;
  static constexpr int64_t kMinNanoseconds = 224192000LL;
};

template <typename T>
struct AddMonthsFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(out_type<Date>& result, const arg_type<Date>& date, int32_t numMonths) {
    const auto dateTime = getDateTime(date);
    const auto year = getYear(dateTime);
    const auto month = getMonth(dateTime);
    const auto day = getDay(dateTime);

    // Similar to handling number in base 12. Here, month - 1 makes it in
    // [0, 11] range.
    int64_t monthAdded = (int64_t)month - 1 + numMonths;
    // Used to adjust month/year when monthAdded is not in [0, 11] range.
    int64_t yearOffset = (monthAdded >= 0 ? monthAdded : monthAdded - 11) / 12;
    // Adjusts monthAdded to natural month number in [1, 12] range.
    auto monthResult = static_cast<int32_t>(monthAdded - yearOffset * 12 + 1);
    // Adjusts year.
    auto yearResult = year + yearOffset;

    auto lastDayOfMonth = util::getMaxDayOfMonth(yearResult, monthResult);
    // Adjusts day to valid one.
    auto dayResult = lastDayOfMonth < day ? lastDayOfMonth : day;
    auto daysSinceEpoch =
        util::daysSinceEpochFromDate(yearResult, monthResult, dayResult);
    BOLT_USER_CHECK_EQ(
        daysSinceEpoch,
        (int32_t)daysSinceEpoch,
        "Integer overflow in add_months({}, {})",
        DATE()->toString(date),
        numMonths);
    result = daysSinceEpoch;
  }
};

template <typename T>
struct WeekdayFunction : public InitSessionTimezone<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
  FOLLY_ALWAYS_INLINE int32_t getWeekday(const std::tm& time) {
    return (time.tm_wday + 6) % 7;
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getWeekday(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeekday(getDateTime(timestamp, this->timeZone_));
  }
};

template <typename T>
struct TimestampToMicrosFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.toMicros();
  }
};

template <typename TExec>
struct MicrosToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result, const T& micros) {
    result = Timestamp::fromMicrosNoError(micros);
  }
};

template <typename T>
struct TimestampToMillisFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.toMillis();
  }
};

template <typename TExec>
struct MillisToTimestampFunction {
  BOLT_DEFINE_FUNCTION_TYPES(TExec);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result, const T& millis) {
    result = Timestamp::fromMillisNoError(millis);
  }
};

} // namespace bytedance::bolt::functions::sparksql
