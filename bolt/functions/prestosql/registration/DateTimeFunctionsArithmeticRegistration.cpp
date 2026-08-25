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

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/DateTimeFunctions.h"
#include "bolt/functions/sparksql/DateTimeFunctions.h"
namespace bytedance::bolt::functions {
namespace {
void registerArithmeticFunctionsInternal(const std::string& prefix) {
  registerFunction<DateMinusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "minus"});
  registerFunction<DatePlusIntervalDayTime, Date, Date, IntervalDayTime>(
      {prefix + "plus"});
  registerFunction<
      TimestampMinusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "minus"});
  registerFunction<
      TimestampPlusIntervalDayTime,
      Timestamp,
      Timestamp,
      IntervalDayTime>({prefix + "plus"});
  registerFunction<
      IntervalDayTimePlusTimestamp,
      Timestamp,
      IntervalDayTime,
      Timestamp>({prefix + "plus"});
  registerFunction<
      TimestampMinusFunction,
      IntervalDayTime,
      Timestamp,
      Timestamp>({prefix + "minus"});

  registerFunction<HiveDateDiffFunction, int32_t, Date, Date>(
      {prefix + "datediff"});
  registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
      {prefix + "timestampdiff"});

  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    registerFunction<
        DateDiffFunction,
        int64_t,
        Varchar,
        TimestampWithTimezone,
        TimestampWithTimezone>({prefix + "date_diff"});
    registerFunction<DateFormatFunction, Varchar, Timestamp, Varchar>(
        {prefix + "date_format"});
  }

  registerFunction<DateAddFunction, Date, Varchar, int64_t, Date>(
      {prefix + "date_add"});
  registerFunction<DateAddFunction, Timestamp, Varchar, int64_t, Timestamp>(
      {prefix + "date_add", prefix + "timestampadd"});
  registerFunction<DateAddFunction, Timestamp, Varchar, int32_t, Timestamp>(
      {prefix + "date_add", prefix + "timestampadd"});
  registerFunction<
      DateAddFunction,
      TimestampWithTimezone,
      Varchar,
      int64_t,
      TimestampWithTimezone>({prefix + "date_add", prefix + "timestampadd"});

  registerFunction<DateAddFunction, Date, Date, int64_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int32_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int16_t>({prefix + "date_add"});
  registerFunction<DateAddFunction, Date, Date, int8_t>({prefix + "date_add"});

  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    registerFunction<
        DateDiffFunction,
        int64_t,
        Varchar,
        TimestampWithTimezone,
        TimestampWithTimezone>({prefix + "date_diff"});
    registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
        {prefix + "date_diff"});
    registerFunction<DateDiffFunction, int64_t, Date, Date>(
        {prefix + "date_diff"});
    registerFunction<DateFormatFunction, Varchar, Timestamp, Varchar>(
        {prefix + "date_format"});
    registerFunction<
        DateFormatFunction,
        Varchar,
        TimestampWithTimezone,
        Varchar>({prefix + "date_format"});
    registerFunction<sparksql::LastDayFunction, Varchar, Date>(
        {prefix + "last_day"});
  }
  registerFunction<HiveDateDiffFunction, int32_t, Date, Date>(
      {prefix + "datediff"});
  registerFunction<DateDiffFunction, int64_t, Varchar, Timestamp, Timestamp>(
      {prefix + "timestampdiff"});

  registerFunction<MonthsBetweenFunction, double, Timestamp, Timestamp>(
      {prefix + "months_between"});
  registerFunction<
      bytedance::bolt::functions::sparksql::NextDayFunction,
      Date,
      Date,
      Varchar>({prefix + "next_day"});
}
} // namespace

void registerDateTimeArithmeticFunctions(const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerArithmeticFunctionsInternal(prefix);
}
} // namespace bytedance::bolt::functions
