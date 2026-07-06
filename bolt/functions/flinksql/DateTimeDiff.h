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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>

#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/TimeDiffUtils.h"
#include "bolt/functions/lib/TimeUtils.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/TimestampConversion.h"

namespace bytedance::bolt::functions::flinksql {

namespace detail {

// floorDiv/floorMod for month arithmetic. For supported timestamps the operands
// are positive, but keep the math correct for any input.
FOLLY_ALWAYS_INLINE int64_t floorDiv(int64_t a, int64_t b) {
  int64_t q = a / b;
  if ((a % b != 0) && ((a < 0) != (b < 0))) {
    --q;
  }
  return q;
}

FOLLY_ALWAYS_INLINE int64_t floorMod(int64_t a, int64_t b) {
  return a - floorDiv(a, b) * b;
}

FOLLY_ALWAYS_INLINE int32_t toJavaInt(int64_t value) {
  const uint32_t bits = static_cast<uint32_t>(value);
  int32_t result;
  std::memcpy(&result, &bits, sizeof(result));
  return result;
}

// Adds `months` to a date expressed as days-since-epoch, clamping the day of
// month to the last valid day (Calcite/Flink DateTimeUtils.addMonths).
FOLLY_ALWAYS_INLINE int64_t addMonthsToDay(int64_t day, int64_t months) {
  const auto civil = util::toCivilDateTime(
      Timestamp(day * util::kSecsPerDay, 0),
      /*allowOverflow=*/false,
      /*isPrecision=*/true);
  const int64_t total = static_cast<int64_t>(civil.date.year) * 12 +
      (civil.date.month - 1) + months;
  const int32_t year = static_cast<int32_t>(floorDiv(total, 12));
  const int32_t month = static_cast<int32_t>(floorMod(total, 12)) + 1;
  const int32_t dayOfMonth =
      std::min(civil.date.day, util::getMaxDayOfMonth(year, month));
  return util::daysSinceEpochFromDate(year, month, dayOfMonth);
}

// Whole months between two dates expressed as days-since-epoch
// (Calcite/Flink DateTimeUtils.subtractMonths, date variant). Plain `inline`
// rather than always-inline because it is self-recursive.
inline int64_t subtractMonthsByDay(int64_t date0, int64_t date1) {
  if (date0 < date1) {
    return -subtractMonthsByDay(date1, date0);
  }
  int64_t m = (date0 - date1) / 31;
  while (true) {
    if (addMonthsToDay(date1, m) >= date0) {
      break;
    }
    if (addMonthsToDay(date1, m + 1) > date0) {
      break;
    }
    ++m;
  }
  return m;
}

// Whole months from `from` to `to` (Calcite/Flink timestamp variant, which
// decrements when the clamped month boundary lands exactly on `to` but the
// time-of-day within that day has not been reached). This differs from Presto's
// shared diffTimestamp on month-end boundaries, hence the Flink-local copy.
FOLLY_ALWAYS_INLINE int64_t
subtractMonths(const Timestamp& from, const Timestamp& to) {
  constexpr int64_t kMillisPerDay = 86'400'000;
  const int64_t toMillis = to.toMillis();
  const int64_t fromMillis = from.toMillis();
  const int64_t toMillisOfDay = floorMod(toMillis, kMillisPerDay);
  const int64_t toDay = floorDiv(toMillis, kMillisPerDay);
  const int64_t fromMillisOfDay = floorMod(fromMillis, kMillisPerDay);
  const int64_t fromDay = floorDiv(fromMillis, kMillisPerDay);
  int64_t x = subtractMonthsByDay(toDay, fromDay);
  if (addMonthsToDay(fromDay, x) == toDay && toMillisOfDay < fromMillisOfDay) {
    --x;
  }
  return x;
}

} // namespace detail

template <typename T>
struct FlinkTimestampDiffFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  std::optional<DateTimeUnit> unit_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*ts1*/,
      const arg_type<Timestamp>* /*ts2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Timestamp>* /*ts*/,
      const arg_type<Date>* /*date*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date*/,
      const arg_type<Timestamp>* /*ts*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* unitString,
      const arg_type<Date>* /*date1*/,
      const arg_type<Date>* /*date2*/) {
    if (unitString != nullptr) {
      unit_ = fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& ts1,
      const arg_type<Timestamp>& ts2) {
    return computeDiff(result, unitString, ts1, ts2);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Timestamp>& ts,
      const arg_type<Date>& date) {
    return computeDiff(result, unitString, ts, dateToTimestamp(date));
  }

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date,
      const arg_type<Timestamp>& ts) {
    return computeDiff(result, unitString, dateToTimestamp(date), ts);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& unitString,
      const arg_type<Date>& date1,
      const arg_type<Date>& date2) {
    return computeDiff(
        result, unitString, dateToTimestamp(date1), dateToTimestamp(date2));
  }

 private:
  static FOLLY_ALWAYS_INLINE Timestamp dateToTimestamp(int32_t date) {
    return Timestamp(static_cast<int64_t>(date) * util::kSecsPerDay, 0);
  }

  FOLLY_ALWAYS_INLINE std::optional<DateTimeUnit> resolveUnit(
      const arg_type<Varchar>& unitString) {
    if (unit_.has_value()) {
      return unit_;
    }
    return fromDateTimeUnitString(unitString, /*throwIfInvalid=*/false);
  }

  // Month/quarter/year follow Calcite/Flink end-of-month semantics; the
  // fixed-ratio units are identical to Presto and reuse the shared helper.
  static FOLLY_ALWAYS_INLINE int64_t
  diffByUnit(DateTimeUnit unit, const Timestamp& from, const Timestamp& to) {
    switch (unit) {
      case DateTimeUnit::kMonth:
        return detail::subtractMonths(from, to);
      case DateTimeUnit::kQuarter:
        return detail::subtractMonths(from, to) / 3;
      case DateTimeUnit::kYear:
        return detail::subtractMonths(from, to) / 12;
      default:
        return diffTimestamp(unit, from, to);
    }
  }

  FOLLY_ALWAYS_INLINE bool computeDiff(
      int32_t& result,
      const arg_type<Varchar>& unitString,
      const Timestamp& from,
      const Timestamp& to) {
    auto unit = resolveUnit(unitString);
    if (!unit.has_value()) {
      // Flink validates the unit keyword at plan time, so an unrecognized unit
      // cannot legitimately reach here; return SQL NULL rather than a sentinel.
      return false;
    }
    // Java semantics: narrow the int64 result to int32 with two's-complement
    // wrap, matching Flink's (int) cast (it does not saturate).
    result = detail::toJavaInt(diffByUnit(unit.value(), from, to));
    return true;
  }
};

} // namespace bytedance::bolt::functions::flinksql
