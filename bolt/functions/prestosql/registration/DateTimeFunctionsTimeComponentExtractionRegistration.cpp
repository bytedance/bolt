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

#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/DateTimeFunctions.h"
#include "bolt/functions/sparksql/DateTimeFunctions.h"

namespace bytedance::bolt::functions {
namespace {
void registerDateTimeTimeComponentExtractionFunctionsInternal(
    const std::string& prefix) {
  registerFunction<HourFunction, int64_t, Date>({prefix + "hour"});
  registerFunction<HourFunction, int64_t, TimestampWithTimezone>(
      {prefix + "hour"});
  registerFunction<LastDayOfMonthFunction, Date, Timestamp>(
      {prefix + "last_day_of_month"});
  registerFunction<LastDayOfMonthFunction, Date, Date>(
      {prefix + "last_day_of_month"});
  registerFunction<LastDayOfMonthFunction, Date, TimestampWithTimezone>(
      {prefix + "last_day_of_month"});
  registerFunction<MinuteFunction, int64_t, Date>({prefix + "minute"});
  registerFunction<MinuteFunction, int64_t, TimestampWithTimezone>(
      {prefix + "minute"});
  registerFunction<SecondFunction, int64_t, Date>({prefix + "second"});
  registerFunction<SecondFunction, int64_t, TimestampWithTimezone>(
      {prefix + "second"});
  registerFunction<MillisecondFunction, int64_t, Date>(
      {prefix + "millisecond"});
  registerFunction<MillisecondFunction, int64_t, TimestampWithTimezone>(
      {prefix + "millisecond"});

  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    registerFunction<DateTruncFunction, Timestamp, Varchar, Timestamp>(
        {prefix + "date_trunc"});
    registerFunction<DateTruncFunction, Date, Varchar, Date>(
        {prefix + "date_trunc"});
    registerFunction<
        DateTruncFunction,
        TimestampWithTimezone,
        Varchar,
        TimestampWithTimezone>({prefix + "date_trunc"});
    registerFunction<DateTruncFunction, Date, Date, Varchar>(
        {prefix + "trunc"});
    registerFunction<DateTruncFunction, Date, Timestamp, Varchar>(
        {prefix + "trunc"});
    registerFunction<DateTruncFunction, Date, TimestampWithTimezone, Varchar>(
        {prefix + "trunc"});
    registerFunction<DateTruncFunction, Date, Varchar, Varchar>(
        {prefix + "trunc"});
  }
}
} // namespace

void registerDateTimeTimeComponentExtractionFunctions(
    const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerDateTimeTimeComponentExtractionFunctionsInternal(prefix);
}
} // namespace bytedance::bolt::functions
