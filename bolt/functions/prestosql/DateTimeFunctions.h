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

#include <boost/date_time.hpp>
#include <common/base/Exceptions.h>
#include "bolt/functions/lib/DateTimeFormatter.h"
#include "bolt/functions/lib/TimeUtils.h"
#include "bolt/functions/prestosql/DateTimeImpl.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/TimestampConversion.h"
#include "bolt/type/Type.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::functions {

template <typename T>
struct ToUnixtimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      double& result,
      const arg_type<Timestamp>& timestamp) {
    result = toUnixtime(timestamp);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      double& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto milliseconds = *timestampWithTimezone.template at<0>();
    result = (double)milliseconds / kMillisecondsInSecond;
    return true;
  }
};

template <typename T>
struct FromUnixtimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  bool isConstFormat_ = false;
  uint32_t maxResultSize_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      ensureFormatLegal(
          std::string_view(formatString->data(), formatString->size()));
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    jodaDateTime_ = buildJodaDateTimeFormatter("yyyy-MM-dd HH:mm:ss");
    maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE bool call(
      Timestamp& result,
      const arg_type<double>& unixtime) {
    auto resultOptional = fromUnixtime(unixtime);
    if (LIKELY(resultOptional.has_value())) {
      result = resultOptional.value();
      return true;
    }
    return false;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& formatString) {
    auto optionalTimestamp = fromUnixtime((double)unixtime);
    if (UNLIKELY(!optionalTimestamp.has_value())) {
      return false;
    }
    auto timestamp = optionalTimestamp.value();
    if (!isConstFormat_) {
      ensureFormatLegal(
          std::string_view(formatString.data(), formatString.size()));
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    const auto resultSize = jodaDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime) {
    auto optionalTimestamp = fromUnixtime((double)unixtime);
    if (UNLIKELY(!optionalTimestamp.has_value())) {
      return false;
    }
    auto timestamp = optionalTimestamp.value();
    result.reserve(maxResultSize_);
    const auto resultSize = jodaDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar> formatString) {
    jodaDateTime_ = buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
  }
};

template <typename T>
struct FromHiveUnixtimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> hiveDateTime_;
  bool isConstFormat_ = false;
  uint32_t maxResultSize_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& unixtime,
      const arg_type<Varchar>& formatString) {
    auto timestamp = fromUnixtime((double)unixtime);
    if (!isConstFormat_) {
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    auto resultSize = hiveDateTime_->format(
        timestamp.value(), sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    Timestamp ts = timestamp;
    if (!isConstFormat_) {
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    auto resultSize = hiveDateTime_->format(
        ts, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    hiveDateTime_ = buildHiveDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = hiveDateTime_->maxResultSize(sessionTimeZone_);
  }
};

namespace {

template <typename T>
struct TimestampWithTimezoneSupport {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Convert timestampWithTimezone to a timestamp representing the moment at the
  // zone in timestampWithTimezone. If `asGMT` is set to true, return the GMT
  // time at the same moment.
  FOLLY_ALWAYS_INLINE
  Timestamp toTimestamp(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      bool asGMT = false) {
    const auto milliseconds = *timestampWithTimezone.template at<0>();
    // presto timestamp with timezone use 52 bits to store milliseconds.
    constexpr int64_t kMaxTimestampWithZoneMillis = (1LL << 51) - 1;
    constexpr int64_t kMinTimestampWithZoneMillis = -(1LL << 51);
    if (milliseconds > kMaxTimestampWithZoneMillis ||
        milliseconds < kMinTimestampWithZoneMillis) {
      BOLT_USER_FAIL("Timestamp with timezone overflow: {} ms]", milliseconds);
    }

    auto tz = *timestampWithTimezone.template at<1>();
    Timestamp timestamp = Timestamp::fromMillis(milliseconds);
    if (!asGMT) {
      timestamp.toTimezone(*timestampWithTimezone.template at<1>());
    }

    return timestamp;
  }

  // Get offset in seconds with GMT from timestampWithTimezone.
  FOLLY_ALWAYS_INLINE
  int64_t getGMTOffsetSec(
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    Timestamp inputTimeStamp = this->toTimestamp(timestampWithTimezone);

    // Create a copy of inputTimeStamp and convert it to GMT
    auto gmtTimeStamp = inputTimeStamp;
    gmtTimeStamp.toGMT(*timestampWithTimezone.template at<1>());

    // Get offset in seconds with GMT and convert to hour
    return (inputTimeStamp.getSeconds() - gmtTimeStamp.getSeconds());
  }
};

} // namespace

template <typename T>
struct BytedanceDateFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /* date */) {
    isEnableAeolusFunction_ = config.isEnableAeolusFunction();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& date) {
    auto piece = folly::StringPiece(date.data(), date.size());
    auto stringValue = folly::trimWhitespace(piece);
    auto normalizedStringValue = std::string();
    if (isEnableAeolusFunction_ && stringValue.size() == 8 &&
        !stringValue.contains('-')) {
      normalizedStringValue = stringValue.subpiece(0, 4);
      normalizedStringValue += '-';
      normalizedStringValue += stringValue.subpiece(4, 2);
      normalizedStringValue += '-';
      normalizedStringValue += stringValue.subpiece(6, 2);
      stringValue = normalizedStringValue;
    }

    auto mayBeNull =
        util::sparkStringToDate(stringValue.data(), stringValue.size());
    if (mayBeNull.has_value()) {
      result = mayBeNull.value();
      return true;
    }
    return false;
  }

  bool isEnableAeolusFunction_ = false;
};

template <typename T>
struct DateFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  bool isConstFormat_ = false;

  int32_t timestampToDate(const Timestamp& input) {
    auto convertToDate = [](const Timestamp& t) -> int32_t {
      static const int32_t kSecsPerDay{86'400};
      auto seconds = t.getSeconds();
      if (seconds >= 0 || seconds % kSecsPerDay == 0) {
        return seconds / kSecsPerDay;
      }
      // For division with negatives, minus 1 to compensate the discarded
      // fractional part. e.g. -1/86'400 yields 0, yet it should be considered
      // as -1 day.
      return seconds / kSecsPerDay - 1;
    };

    if (timeZone_ != nullptr) {
      Timestamp t = input;
      t.toTimezone(*timeZone_);
      return convertToDate(t);
    }

    return convertToDate(input);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* date) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*dateStr*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      jodaDateTime_ = buildJodaDateTimeFormatter(
          std::string_view(formatString->data(), formatString->size()));
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* timestamp) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* timestampWithTimezone) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& date) {
    bool nullOutput = false;
    result = DATE()->toDays(date, &nullOutput);
    if (nullOutput) {
      return false;
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& dateStr,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      jodaDateTime_ = buildJodaDateTimeFormatter(
          std::string_view(formatString.data(), formatString.size()));
      isConstFormat_ = true;
    }

    auto dateTimeResult =
        jodaDateTime_->parse(std::string_view(dateStr.data(), dateStr.size()));
    if (dateTimeResult.hasError()) {
      return false;
    }
    result = timestampToDate(dateTimeResult.value().timestamp);

    return true;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestampToDate(timestamp);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    result = timestampToDate(this->toTimestamp(timestampWithTimezone));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<int64_t>& timestamp) {
    result = timestampToDate(Timestamp(timestamp, 0));
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    result = date;
  }
};

template <typename T>
struct ToTimestampFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  bool adjustTimezone_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*string*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    adjustTimezone_ = config.adjustTimestampToTimezone();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& date) {
    size_t size = date.size();
    std::string dateStr(date.data(), size);
    bool isNum = dateStr.find_first_not_of("0123456789") == std::string::npos;
    bool nullOutput = false;

    if (isNum) {
      // yyyyMMdd or unix timestamp string
      if (size == 8) {
        // try yyyyMMdd
        std::string formatted = dateStr.substr(0, 4) + "-" +
            dateStr.substr(4, 2) + "-" + dateStr.substr(6, 2);
        try {
          result = util::Converter<TypeKind::TIMESTAMP>::cast(
              formatted, &nullOutput);
          if (nullOutput) {
            return false;
          }
        } catch (const bytedance::bolt::BoltUserError& e) {
          return false;
        }
      } else {
        return false;
      }
    } else if (size >= 10 && date.data()[4] == '-' && date.data()[7] == '-') {
      // 'yyyy-MM-dd' or 'yyyy-MM-dd *' or 'yyyy-MM-ddT*'
      if (size >= 11 && date.data()[10] != ' ' && date.data()[10] != 'T') {
        return false;
      }
      try {
        result =
            util::Converter<TypeKind::TIMESTAMP>::cast(dateStr, &nullOutput);
        if (nullOutput) {
          return false;
        }
      } catch (const bytedance::bolt::BoltUserError& e) {
        return false;
      }
    } else {
      return false;
    }
    // PLEASE MAKE SURE THAT THE FOLLOWING 3 LINE IS THE SAME LOGIC IN
    if (adjustTimezone_ && sessionTimeZone_ != nullptr) {
      result.toGMT(*sessionTimeZone_);
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<int64_t>& epoch) {
    result = Timestamp::fromMillis(epoch * 1000l);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<int32_t>& epoch) {
    result = Timestamp::fromMillis(epoch * 1000l);
    return true;
  }
};

template <typename T>
struct WeekFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getWeek(const std::tm& time) {
    // The computation of ISO week from date follows the algorithm here:
    // https://en.wikipedia.org/wiki/ISO_week_date
    int64_t week = floor(
                       10 + (time.tm_yday + 1) -
                       (time.tm_wday ? time.tm_wday : kDaysInWeek)) /
        kDaysInWeek;

    if (week == 0) {
      // Distance in days between the first day of the current year and the
      // Monday of the current week.
      auto mondayOfWeek =
          time.tm_yday + 1 - (time.tm_wday + kDaysInWeek - 1) % kDaysInWeek;
      // Distance in days between the first day and the first Monday of the
      // current year.
      auto firstMondayOfYear =
          1 + (mondayOfWeek + kDaysInWeek - 1) % kDaysInWeek;

      if ((util::isLeapYear(time.tm_year + 1900 - 1) &&
           firstMondayOfYear == 2) ||
          firstMondayOfYear == 3 || firstMondayOfYear == 4) {
        week = 53;
      } else {
        week = 52;
      }
    } else if (week == 53) {
      // Distance in days between the first day of the current year and the
      // Monday of the current week.
      auto mondayOfWeek =
          time.tm_yday + 1 - (time.tm_wday + kDaysInWeek - 1) % kDaysInWeek;
      auto daysInYear = util::isLeapYear(time.tm_year + 1900) ? 366 : 365;
      if (daysInYear - mondayOfWeek < 3) {
        week = 1;
      }
    }

    return week;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = static_cast<int32_t>(getWeek(getDateTime(date)));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      TInput& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      TInput& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct QuarterFunction : public InitSessionTimezone<T>,
                         public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getQuarter(const std::tm& time) {
    return time.tm_mon / 3 + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getQuarter(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getQuarter(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getQuarter(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct MonthFunction : public InitSessionTimezone<T>,
                       public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getMonth(const std::tm& time) {
    return 1 + time.tm_mon;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getMonth(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getMonth(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getMonth(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct DayFunction : public InitSessionTimezone<T>,
                     public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_mday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_mday;
  }
};

template <typename T>
struct LastDayOfMonthFunction : public InitSessionTimezone<T>,
                                public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp) {
    auto dt = getDateTime(timestamp, this->timeZone_);
    result = util::lastDayOfMonthSinceEpochFromDate(dt);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date) {
    auto dt = getDateTime(date);
    result = util::lastDayOfMonthSinceEpochFromDate(dt);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dt = getDateTime(timestamp, nullptr);
    result = util::lastDayOfMonthSinceEpochFromDate(dt);
  }
};

namespace {

bool isIntervalWholeDays(int64_t milliseconds) {
  return (milliseconds % kMillisInDay) == 0;
}

int64_t intervalDays(int64_t milliseconds) {
  return milliseconds / kMillisInDay;
}

} // namespace

template <typename T>
struct DateMinusIntervalDayTime {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    BOLT_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot subtract hours, minutes, seconds or milliseconds from a date");
    result = addToDate(date, DateTimeUnit::kDay, -intervalDays(interval));
  }
};

template <typename T>
struct DatePlusIntervalDayTime {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    BOLT_USER_CHECK(
        isIntervalWholeDays(interval),
        "Cannot add hours, minutes, seconds or milliseconds to a date");
    result = addToDate(date, DateTimeUnit::kDay, intervalDays(interval));
  }
};

template <typename T>
struct TimestampMinusFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IntervalDayTime>& result,
      const arg_type<Timestamp>& a,
      const arg_type<Timestamp>& b) {
    result = a.toMillis() - b.toMillis();
  }
};

template <typename T>
struct TimestampPlusIntervalDayTime {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() + b);
  }
};

template <typename T>
struct IntervalDayTimePlusTimestamp {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<Timestamp>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a + b.toMillis());
  }
};

template <typename T>
struct TimestampMinusIntervalDayTime {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    result = Timestamp::fromMillisNoError(a.toMillis() - b);
  }
};

template <typename T>
struct DayOfWeekFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfWeek(const std::tm& time) {
    return time.tm_wday == 0 ? 7 : time.tm_wday;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct DayOfYearFunction : public InitSessionTimezone<T>,
                           public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t getDayOfYear(const std::tm& time) {
    return time.tm_yday + 1;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDayOfYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDayOfYear(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDayOfYear(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct YearOfWeekFunction : public InitSessionTimezone<T>,
                            public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t computeYearOfWeek(const std::tm& dateTime) {
    int isoWeekDay = dateTime.tm_wday == 0 ? 7 : dateTime.tm_wday;
    // The last few days in December may belong to the next year if they are
    // in the same week as the next January 1 and this January 1 is a Thursday
    // or before.
    if (UNLIKELY(
            dateTime.tm_mon == 11 && dateTime.tm_mday >= 29 &&
            dateTime.tm_mday - isoWeekDay >= 31 - 3)) {
      return 1900 + dateTime.tm_year + 1;
    }
    // The first few days in January may belong to the last year if they are
    // in the same week as January 1 and January 1 is a Friday or after.
    else if (UNLIKELY(
                 dateTime.tm_mon == 0 && dateTime.tm_mday <= 3 &&
                 isoWeekDay - (dateTime.tm_mday - 1) >= 5)) {
      return 1900 + dateTime.tm_year - 1;
    } else {
      return 1900 + dateTime.tm_year;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = computeYearOfWeek(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = computeYearOfWeek(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = computeYearOfWeek(getDateTime(timestamp, nullptr));
  }
};

template <typename T>
struct HourFunction : public InitSessionTimezone<T>,
                      public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_hour;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_hour;
  }
};

template <typename T>
struct MinuteFunction : public InitSessionTimezone<T>,
                        public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename U>
  FOLLY_ALWAYS_INLINE void call(
      U& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, this->timeZone_).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_min;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_min;
  }
};

template <typename T>
struct SecondFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getDateTime(timestamp, nullptr).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = getDateTime(date).tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = getDateTime(timestamp, nullptr).tm_sec;
  }
};

template <typename T>
struct MillisecondFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Date>& /*date*/) {
    // Dates do not have millisecond granularity.
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    result = timestamp.getNanos() / kNanosecondsInMillisecond;
  }
};

namespace {
inline bool isTimeUnit(const DateTimeUnit unit) {
  return unit == DateTimeUnit::kMillisecond || unit == DateTimeUnit::kSecond ||
      unit == DateTimeUnit::kMinute || unit == DateTimeUnit::kHour;
}

inline bool isDateUnit(const DateTimeUnit unit) {
  return unit == DateTimeUnit::kDay || unit == DateTimeUnit::kMonth ||
      unit == DateTimeUnit::kQuarter || unit == DateTimeUnit::kYear ||
      unit == DateTimeUnit::kWeek;
}

inline std::optional<DateTimeUnit> getDateUnit(
    const StringView& unitString,
    bool throwIfInvalid) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, throwIfInvalid);
  if (unit.has_value() && !isDateUnit(unit.value())) {
    if (throwIfInvalid) {
      BOLT_USER_FAIL("{} is not a valid DATE field", unitString);
    }
    return std::nullopt;
  }
  return unit;
}

inline std::optional<DateTimeUnit> getTimestampUnit(
    const StringView& unitString) {
  std::optional<DateTimeUnit> unit =
      fromDateTimeUnitString(unitString, false /*throwIfInvalid*/);
  BOLT_USER_CHECK(
      !(unit.has_value() && unit.value() == DateTimeUnit::kMillisecond),
      "{} is not a valid TIMESTAMP field",
      unitString);

  return unit;
}

} // namespace

namespace {
FOLLY_ALWAYS_INLINE int32_t toMonday(const std::tm& tm) {
  int64_t daysSinceEpoch = bytedance::bolt::util::daysSinceEpochFromDate(
      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
  int daysToMonday = (tm.tm_wday - 1 + 7) % 7;
  return daysSinceEpoch - daysToMonday;
}
} // namespace

template <typename T>
struct DateTruncFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_;
  bool optimizedTimeConversion_ = true;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp*/) {
    timeZone_ = getTimeZoneFromConfig(config);

    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
    optimizedTimeConversion_ = config.esDateTruncOptimization();
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestamp*/) {
    if (unitString != nullptr) {
      unit_ = getTimestampUnit(*unitString);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* unitString) {
    timeZone_ = getTimeZoneFromConfig(config);
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, true);
    }
  }
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Date>* /*date*/,
      const arg_type<Varchar>* unitString) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, true);
    }
  }
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* unitString) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, true);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*dateStr*/,
      const arg_type<Varchar>* unitString) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, true);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }
    result = truncateTimestamp(timestamp, unit, timeZone_);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (unit == DateTimeUnit::kDay) {
      result = date;
      return;
    }

    auto dateTime = getDateTime(date);
    adjustDateTime(dateTime, unit);

    result = timegm(&dateTime) / kSecondsInDay;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    DateTimeUnit unit;
    if (unit_.has_value()) {
      unit = unit_.value();
    } else {
      unit = getTimestampUnit(unitString).value();
    }

    if (unit == DateTimeUnit::kSecond) {
      auto utcTimestamp =
          Timestamp::fromMillis(*timestampWithTimezone.template at<0>());
      result.template get_writer_at<0>() = utcTimestamp.getSeconds() * 1000;
      result.template get_writer_at<1>() =
          *timestampWithTimezone.template at<1>();
      return;
    }

    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dateTime = getDateTime(timestamp, nullptr);
    adjustDateTime(dateTime, unit);

    timestamp = Timestamp::fromMillis(timegm(&dateTime) * 1000);
    timestamp.toGMT(*timestampWithTimezone.template at<1>());
    result.template get_writer_at<0>() = timestamp.toMillis();
    result.template get_writer_at<1>() =
        *timestampWithTimezone.template at<1>();
  }
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& unitString) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();
    auto dateTime = getDateTime(timestamp, timeZone_);
    adjustDateTime(dateTime, unit);
    result = timegm(&dateTime) / kSecondsInDay;
  }
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<Varchar>& unitString) {
    return call(result, unitString, date);
  }
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& unitString) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    auto dateTime = getDateTime(timestamp, nullptr);
    adjustDateTime(dateTime, unit);
    result = timegm(&dateTime) / kSecondsInDay;
  }
  FOLLY_ALWAYS_INLINE void call(
      out_type<Date>& result,
      const arg_type<Varchar>& dateStr,
      const arg_type<Varchar>& unitString) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();
    auto timestamp = util::fromTimestampString(dateStr, nullptr);
    auto dateTime = getDateTime(timestamp, nullptr);
    adjustDateTime(dateTime, unit);
    result = timegm(&dateTime) / kSecondsInDay;
  }
};

template <typename T>
struct DateAddFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const int32_t* /*value*/,
      const arg_type<Timestamp>* /*timestamp*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Timestamp>* /*timestamp*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const int64_t* /*value*/,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const int32_t value,
      const arg_type<Timestamp>& timestamp) {
    return call(result, unitString, (int64_t)value, timestamp);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Timestamp>& timestamp) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, /*throwIfInvalid=*/true).value();

    if (value != (int32_t)value) {
      BOLT_UNSUPPORTED("integer overflow");
    }

    if (LIKELY(sessionTimeZone_ != nullptr)) {
      // sessionTimeZone not null means that the config
      // adjust_timestamp_to_timezone is on.
      Timestamp zonedTimestamp = timestamp;
      zonedTimestamp.toTimezone(*sessionTimeZone_);

      Timestamp resultTimestamp =
          addToTimestamp(zonedTimestamp, unit, (int32_t)value);

      if (isTimeUnit(unit)) {
        const int64_t offset = static_cast<Timestamp>(timestamp).getSeconds() -
            zonedTimestamp.getSeconds();
        result = Timestamp(
            resultTimestamp.getSeconds() + offset, resultTimestamp.getNanos());
      } else {
        resultTimestamp.toGMT(*sessionTimeZone_);
        result = resultTimestamp;
      }
    } else {
      result = addToTimestamp(timestamp, unit, (int32_t)value);
    }

    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, true /*throwIfInvalid*/).value();

    if (value != (int32_t)value) {
      BOLT_UNSUPPORTED("integer overflow");
    }

    auto finalTimeStamp = addToTimestamp(
        this->toTimestamp(timestampWithTimezone), unit, (int32_t)value);
    finalTimeStamp.toGMT(*timestampWithTimezone.template at<1>());
    result = std::make_tuple(
        finalTimeStamp.toMillis(), *timestampWithTimezone.template at<1>());

    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Varchar>& unitString,
      const int64_t value,
      const arg_type<Date>& date) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    if (value != (int32_t)value) {
      BOLT_UNSUPPORTED("integer overflow");
    }

    result = addToDate(date, unit, (int32_t)value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int64_t value) {
    if (value != (int32_t)value) {
      BOLT_UNSUPPORTED("integer overflow");
    }

    result = addToDate(date, DateTimeUnit::kDay, (int32_t)value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int32_t value) {
    result = addToDate(date, DateTimeUnit::kDay, value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const int16_t value) {
    result = addToDate(date, DateTimeUnit::kDay, (int32_t)value);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool
  call(out_type<Date>& result, const arg_type<Date>& date, const int8_t value) {
    result = addToDate(date, DateTimeUnit::kDay, (int32_t)value);
    return true;
  }
};

template <typename T>
struct DateDiffFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::optional<DateTimeUnit> unit_ = std::nullopt;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*timestamp1*/,
      const arg_type<Timestamp>* /*timestamp2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }

    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date1*/,
      const arg_type<Date>* /*date2*/) {
    if (unitString != nullptr) {
      unit_ = getDateUnit(*unitString, false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* unitString,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone1*/,
      const arg_type<TimestampWithTimezone>* /*timestampWithTimezone2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& timestamp1,
      const arg_type<Timestamp>& timestamp2) {
    const auto unit = unit_.has_value()
        ? unit_.value()
        : fromDateTimeUnitString(unitString, true /*throwIfInvalid*/).value();

    if (LIKELY(sessionTimeZone_ != nullptr)) {
      // sessionTimeZone not null means that the config
      // adjust_timestamp_to_timezone is on.
      Timestamp fromZonedTimestamp = timestamp1;
      fromZonedTimestamp.toTimezone(*sessionTimeZone_);

      Timestamp toZonedTimestamp = timestamp2;
      if (isTimeUnit(unit)) {
        const int64_t offset = static_cast<Timestamp>(timestamp1).getSeconds() -
            fromZonedTimestamp.getSeconds();
        toZonedTimestamp = Timestamp(
            toZonedTimestamp.getSeconds() - offset,
            toZonedTimestamp.getNanos());
      } else {
        toZonedTimestamp.toTimezone(*sessionTimeZone_);
      }
      result = diffTimestamp(unit, fromZonedTimestamp, toZonedTimestamp);
    } else {
      result = diffTimestamp(unit, timestamp1, timestamp2);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    DateTimeUnit unit = unit_.has_value()
        ? unit_.value()
        : getDateUnit(unitString, true).value();

    result = diffDate(unit, date1, date2);
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<TimestampWithTimezone>& timestamp1,
      const arg_type<TimestampWithTimezone>& timestamp2) {
    call(
        result,
        unitString,
        this->toTimestamp(timestamp1, true),
        this->toTimestamp(timestamp2, true));
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    result = diffDate(DateTimeUnit::kDay, date1, date2);
    return true;
  }
};

template <typename T>
struct HiveDateDiffFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    // Hive datediff(date1, date2) returns date1 - date2
    // The order of arguments is different from aeolus_date_diff
    result = diffDate(DateTimeUnit::kDay, date2, date1);
    return true;
  }
};

template <typename T>
struct DateFormatFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      ensureFormatLegal(
          std::string_view(formatString->data(), formatString->size()));
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      ensureFormatLegal(
          std::string_view(formatString->data(), formatString->size()));
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      ensureFormatLegal(
          std::string_view(formatString.data(), formatString.size()));
      setFormatter(formatString);
    }

    result.reserve(maxResultSize_);
    const auto resultSize = mysqlDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    return call(result, timestamp, formatString);
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar> formatString) {
    mysqlDateTime_ = buildMysqlDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = mysqlDateTime_->maxResultSize(sessionTimeZone_);
  }

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> mysqlDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct JodaDateFormatFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  bool isConstFormat_ = false;
  uint32_t maxResultSize_{0};
  TimePolicy timePolicy_{TimePolicy::CORRECTED};

  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      if (timePolicy_ == TimePolicy::LEGACY) {
        jodaDateTime_ = buildLegacySparkDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      } else {
        jodaDateTime_ = buildJodaDateTimeFormatter(
            std::string_view(formatString->data(), formatString->size()));
      }
      isConstFormat_ = true;
      maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
#ifdef SPARK_COMPATIBLE
    timePolicy_ = parseTimePolicy(config.timeParserPolicy());
#else
    if (formatString != nullptr) {
      ensureFormatLegal(
          std::string_view(formatString->data(), formatString->size()));
    }
#endif
    setFormatter(formatString);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<TimestampWithTimezone>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
#ifdef SPARK_COMPATIBLE
    timePolicy_ = parseTimePolicy(config.timeParserPolicy());
#else
    if (formatString != nullptr) {
      ensureFormatLegal(
          std::string_view(formatString->data(), formatString->size()));
    }
#endif
    setFormatter(formatString);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
#ifndef SPARK_COMPATIBLE
      ensureFormatLegal(
          std::string_view(formatString.data(), formatString.size()));
#endif
      setFormatter(&formatString);
    }

    result.reserve(maxResultSize_);
    const auto resultSize = jodaDateTime_->format(
        timestamp,
        sessionTimeZone_,
        maxResultSize_,
        result.data(),
        false,
        timePolicy_);
    result.resize(resultSize);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    auto timestamp = this->toTimestamp(timestampWithTimezone);
    return call(result, timestamp, formatString);
  }
};

template <typename T>
struct DateParseFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* formatString) {
    if (formatString != nullptr) {
      format_ = buildMysqlDateTimeFormatter(
          std::string_view(formatString->data(), formatString->size()));
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = tz::getTimeZoneID(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildMysqlDateTimeFormatter(
          std::string_view(format.data(), format.size()));
    }

    auto dateTimeResult = format_->parse((std::string_view)(input));
    if (dateTimeResult.hasError()) {
      return Status::UserError("Invalid date format: '{}'", input);
    }

    // Since MySql format has no timezone specifier, simply check if session
    // timezone was provided. If not, fallback to 0 (GMT).
    int16_t timezoneId = sessionTzID_.value_or(0);
    dateTimeResult.value().timestamp.toGMT(timezoneId);
    result = dateTimeResult.value().timestamp;
    return Status::OK();
  }
};

template <typename T>
struct FormatDateTimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*timestamp*/,
      const arg_type<Varchar>* formatString) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
    if (formatString != nullptr) {
      setFormatter(*formatString);
      isConstFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void ensureFormatter(
      const arg_type<Varchar>& formatString) {
    if (!isConstFormat_) {
      setFormatter(formatString);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    // TODO: We should give dateTimeFormatter a sink/ostream to prevent the
    // copy.
    result.reserve(maxResultSize_);
    const auto resultSize = jodaDateTime_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data());
    result.resize(resultSize);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<TimestampWithTimezone>& timestampWithTimezone,
      const arg_type<Varchar>& formatString) {
    ensureFormatter(formatString);

    const auto milliseconds = *timestampWithTimezone.template at<0>();
    Timestamp timestamp = Timestamp::fromMillis(milliseconds);
    int16_t timeZoneId = *timestampWithTimezone.template at<1>();
    auto* timezonePtr = tz::locateZone(timeZoneId);
    auto maxResultSize = jodaDateTime_->maxResultSize(timezonePtr);
    result.reserve(maxResultSize);
    auto resultSize = jodaDateTime_->format(
        timestamp, timezonePtr, maxResultSize, result.data());
    result.resize(resultSize);
  }

 private:
  FOLLY_ALWAYS_INLINE void setFormatter(const arg_type<Varchar>& formatString) {
    jodaDateTime_ = buildJodaDateTimeFormatter(
        std::string_view(formatString.data(), formatString.size()));
    maxResultSize_ = jodaDateTime_->maxResultSize(sessionTimeZone_);
  }

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  std::shared_ptr<DateTimeFormatter> jodaDateTime_;
  uint32_t maxResultSize_;
  bool isConstFormat_ = false;
};

template <typename T>
struct ParseDateTimeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
  bool isConstFormat_ = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      format_ = buildJodaDateTimeFormatter(
          std::string_view(format->data(), format->size()));
      isConstFormat_ = true;
    }

    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = tz::getTimeZoneID(sessionTzName);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<TimestampWithTimezone>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstFormat_) {
      format_ = buildJodaDateTimeFormatter(
          std::string_view(format.data(), format.size()));
    }
    auto dateTimeResult =
        format_->parse(std::string_view(input.data(), input.size()));

    dateTimeResult.throwExceptionIfErrorOccurs();

    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    int16_t timezoneId = dateTimeResult.value().timezoneId != -1
        ? dateTimeResult.value().timezoneId
        : sessionTzID_.value_or(0);
    dateTimeResult.value().timestamp.toGMT(timezoneId);
    result = std::make_tuple(
        dateTimeResult.value().timestamp.toMillis(), timezoneId);
    return true;
  }
};

template <typename T>
struct CurrentDateFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* timeZone_ = nullptr;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config) {
    timeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result) {
    auto now = Timestamp::now();
    if (timeZone_ != nullptr) {
      now.toTimezone(*timeZone_);
    }
    const std::chrono::
        time_point<std::chrono::system_clock, std::chrono::milliseconds>
            localTimepoint(std::chrono::milliseconds(now.toMillis()));
    result =
        std::chrono::floor<::date::days>((localTimepoint).time_since_epoch())
            .count();
  }
};

template <typename T>
struct TimeZoneHourFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to hour
    auto offset = this->getGMTOffsetSec(input);
    result = offset / 3600;
  }
};

template <typename T>
struct TimeZoneMinuteFunction : public TimestampWithTimezoneSupport<T> {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<TimestampWithTimezone>& input) {
    // Get offset in seconds with GMT and convert to minute
    auto offset = this->getGMTOffsetSec(input);
    result = (offset / 60) % 60;
  }
};

namespace {
FOLLY_ALWAYS_INLINE int32_t
toStartOfIntervalYearMonth(const std::tm& tm, int32_t interval) {
  int tm_year = tm.tm_year;
  int tm_mon = tm.tm_mon;
  int back = (tm_year * 12 + tm_mon) % interval;
  tm_year -= back / 12;
  tm_mon -= back % 12;
  if (tm_mon < 0) {
    tm_year -= 1;
    tm_mon += 12;
  }
  return static_cast<int32_t>(bytedance::bolt::util::daysSinceEpochFromDate(
      tm_year + 1900, tm_mon + 1, 1));
}
} // namespace

template <typename T>
struct MonthsBetweenFunction {
 public:
  // hive/blob/branch-1.2/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFMonthsBetween.java
  // the function should support both short date and full timestamp format
  // time part of the timestamp should not be skipped
  BOLT_DEFINE_FUNCTION_TYPES(T);

  const tz::TimeZone* sessionTimeZone_ = nullptr;
  int16_t tzID_ = -1;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*toTimestamp*/,
      const arg_type<Timestamp>* /*fromTimestamp*/) {
    sessionTimeZone_ = getTimeZoneFromConfig(config);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Timestamp>* /*toTimestamp*/,
      const arg_type<Timestamp>* /*fromTimestamp*/,
      const arg_type<bool>* /*roundOff*/,
      const arg_type<Varchar>* timeZone) {
    if (timeZone == nullptr || timeZone->empty()) {
      sessionTimeZone_ = getTimeZoneFromConfig(config);
    } else {
      setTimeZone(*timeZone);
    }
  }

  void setTimeZone(const arg_type<Varchar>& timeZone) {
    // time zone like 'Asia/Shanghai'
    try {
      sessionTimeZone_ = tz::locateZone(std::string_view(timeZone));
    } catch (const std::exception&) {
      sessionTimeZone_ = nullptr;
    }
    // time zone like '+00:00'
    if (sessionTimeZone_ == nullptr) {
      tzID_ = tz::getTimeZoneID(std::string_view(timeZone));
    }
  }

  FOLLY_ALWAYS_INLINE static void calcMonthsBetween(
      const Timestamp& toTimestamp,
      const Timestamp& fromTimestamp,
      const bool roundOff,
      const tz::TimeZone* timeZone,
      const int16_t tzID,
      double& result) {
    if (fromTimestamp == toTimestamp) {
      result = 0.0;
      return;
    }

    auto fromDateTime = timeZone == nullptr
        ? getDateTime(fromTimestamp, tzID)
        : getDateTime(fromTimestamp, timeZone);
    auto toDateTime = timeZone == nullptr ? getDateTime(toTimestamp, tzID)
                                          : getDateTime(toTimestamp, timeZone);

    // month diff
    result = (toDateTime.tm_year - fromDateTime.tm_year) * 12 +
        (toDateTime.tm_mon - fromDateTime.tm_mon);

    // skip day/time part if both dates are end of the month
    // or the same day of the month
    if (toDateTime.tm_mday == fromDateTime.tm_mday ||
        (toDateTime.tm_mday ==
             util::getMaxDayOfMonth(
                 toDateTime.tm_year + 1900, toDateTime.tm_mon + 1) &&
         fromDateTime.tm_mday ==
             util::getMaxDayOfMonth(
                 fromDateTime.tm_year + 1900, fromDateTime.tm_mon + 1))) {
      return;
    }

    // 1 sec is 0.000000373 months (1/2678400). 1 month is 31 days.
    // there should be no adjustments for leap seconds
    result += (getDayPartInSec(toDateTime) - getDayPartInSec(fromDateTime)) /
        2678400.0;

    if (roundOff) {
      result = std::round(result * 1e8) / 1e8;
    }
  }

  FOLLY_ALWAYS_INLINE static int32_t getDayPartInSec(std::tm datetime) {
    int32_t day = datetime.tm_mday - 1;
    int32_t hour = datetime.tm_hour;
    int32_t minute = datetime.tm_min;
    int32_t second = datetime.tm_sec;
    return day * 86400 + hour * 3600 + minute * 60 + second;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<Timestamp>& toTimestamp,
      const arg_type<Timestamp>& fromTimestamp) {
    calcMonthsBetween(
        toTimestamp, fromTimestamp, false, sessionTimeZone_, tzID_, result);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<Timestamp>& toTimestamp,
      const arg_type<Timestamp>& fromTimestamp,
      const arg_type<bool>& roundOff,
      const arg_type<Varchar>& timeZone) {
    if (sessionTimeZone_ == nullptr) {
      setTimeZone(timeZone);
    }
    calcMonthsBetween(
        toTimestamp, fromTimestamp, roundOff, sessionTimeZone_, tzID_, result);
  }
};

} // namespace bytedance::bolt::functions
