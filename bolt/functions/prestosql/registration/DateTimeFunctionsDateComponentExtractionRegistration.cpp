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

#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/DateTimeFunctions.h"
#include "bolt/functions/sparksql/DateTimeFunctions.h"

namespace bytedance::bolt::functions {
namespace {
void registerDateTimeDateComponentExtractionFunctionsInternal(
    const std::string& prefix) {
#ifndef SPARK_COMPATIBLE
  registerFunction<YearFunction, int64_t, Date>({prefix + "year"});
  registerFunction<YearFunction, int64_t, Timestamp>({prefix + "year"});
  registerFunction<QuarterFunction, int64_t, Timestamp>({prefix + "quarter"});
  registerFunction<QuarterFunction, int64_t, Date>({prefix + "quarter"});
  registerFunction<MonthFunction, int64_t, Timestamp>({prefix + "month"});
  registerFunction<MonthFunction, int64_t, Date>({prefix + "month"});
  registerFunction<WeekFunction, int64_t, Timestamp>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<WeekFunction, int64_t, Date>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<HourFunction, int64_t, Timestamp>({prefix + "hour"});
  registerFunction<MinuteFunction, int64_t, Timestamp>({prefix + "minute"});
  registerFunction<SecondFunction, int64_t, Timestamp>({prefix + "second"});
  registerFunction<MillisecondFunction, int64_t, Timestamp>(
      {prefix + "millisecond"});
#endif

  registerFunction<YearFunction, int64_t, TimestampWithTimezone>(
      {prefix + "year"});
  registerFunction<WeekFunction, int32_t, Date>({prefix + "weekofyear"});
  registerFunction<WeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "week", prefix + "week_of_year"});
  registerFunction<QuarterFunction, int64_t, TimestampWithTimezone>(
      {prefix + "quarter"});
  registerFunction<MonthFunction, int64_t, TimestampWithTimezone>(
      {prefix + "month"});
  registerFunction<DayFunction, int64_t, Timestamp>(
      {prefix + "day", prefix + "day_of_month"});
  registerFunction<DayFunction, int64_t, Date>(
      {prefix + "day", prefix + "day_of_month"});

  registerFunction<DayFunction, int64_t, TimestampWithTimezone>(
      {prefix + "day", prefix + "day_of_month"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayFunction,
      int32_t,
      Date>({prefix + "dayofmonth"});
  registerFunction<DayOfWeekFunction, int64_t, Timestamp>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfWeekFunction, int64_t, Date>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfWeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "dow", prefix + "day_of_week"});
  registerFunction<DayOfYearFunction, int64_t, Timestamp>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<DayOfYearFunction, int64_t, Date>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<DayOfYearFunction, int64_t, TimestampWithTimezone>(
      {prefix + "doy", prefix + "day_of_year"});
  registerFunction<
      bytedance::bolt::functions::sparksql::DayOfYearFunction,
      int32_t,
      Date>({prefix + "dayofyear"});
  registerFunction<YearOfWeekFunction, int64_t, Timestamp>(
      {prefix + "yow", prefix + "year_of_week"});
  registerFunction<YearOfWeekFunction, int64_t, Date>(
      {prefix + "yow", prefix + "year_of_week"});
  registerFunction<YearOfWeekFunction, int64_t, TimestampWithTimezone>(
      {prefix + "yow", prefix + "year_of_week"});
}
} // namespace

void registerDateTimeDateComponentExtractionFunctions(
    const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerDateTimeDateComponentExtractionFunctionsInternal(prefix);
}
} // namespace bytedance::bolt::functions
