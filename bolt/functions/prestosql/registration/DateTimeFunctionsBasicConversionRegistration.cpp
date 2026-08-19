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
void registerBasicConversionFunctionsInternal(const std::string& prefix) {
  // Date time functions.
  registerFunction<ToUnixtimeFunction, double, Timestamp>(
      {prefix + "to_unixtime"});
  registerFunction<ToUnixtimeFunction, double, TimestampWithTimezone>(
      {prefix + "to_unixtime"});
  registerFunction<FromUnixtimeFunction, Timestamp, double>(
      {prefix + "from_unixtime"});
  registerFunction<FromUnixtimeFunction, Varchar, int64_t>(
      {prefix + "from_unixtime"});
  registerFunction<FromUnixtimeFunction, Varchar, int64_t, Varchar>(
      {prefix + "from_unixtime"});

  registerFunction<DateFunction, Date, Varchar>({
      prefix + "date",
      prefix + "to_date",
  });
  registerFunction<DateFunction, Date, Timestamp>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, TimestampWithTimezone>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, int64_t>(
      {prefix + "date", prefix + "to_date"});
  registerFunction<DateFunction, Date, Date>(
      {prefix + "date", prefix + "to_date"});

  registerFunction<TimeZoneHourFunction, int64_t, TimestampWithTimezone>(
      {prefix + "timezone_hour"});
  registerFunction<TimeZoneMinuteFunction, int64_t, TimestampWithTimezone>(
      {prefix + "timezone_minute"});
  registerFunction<ToTimestampFunction, Timestamp, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampFunction, Timestamp, int64_t>(
      {prefix + "to_timestamp"});
  registerFunction<ToTimestampFunction, Timestamp, int32_t>(
      {prefix + "to_timestamp"});

  if constexpr (!::bytedance::bolt::kSparkCompatible) {
    registerFunction<
        DateFormatFunction,
        Varchar,
        TimestampWithTimezone,
        Varchar>({prefix + "date_format"});
    registerFunction<sparksql::LastDayFunction, Varchar, Date>(
        {prefix + "last_day"});
  }

  registerFunction<FormatDateTimeFunction, Varchar, Timestamp, Varchar>(
      {prefix + "format_datetime"});
  registerFunction<
      FormatDateTimeFunction,
      Varchar,
      TimestampWithTimezone,
      Varchar>({prefix + "format_datetime"});
  registerFunction<
      ParseDateTimeFunction,
      TimestampWithTimezone,
      Varchar,
      Varchar>({prefix + "parse_datetime"});
  registerFunction<DateParseFunction, Timestamp, Varchar, Varchar>(
      {prefix + "date_parse"});
  registerFunction<CurrentDateFunction, Date>({prefix + "current_date"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, Varchar>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, int64_t>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::Date2PDateFunction, Varchar, int32_t>(
      {prefix + "date2pdate"});
  registerFunction<sparksql::PDate2DateFunction, Varchar, Varchar>(
      {prefix + "pdate2date"});
  registerFunction<sparksql::PDate2DateFunction, Varchar, Date>(
      {prefix + "pdate2date"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampFunction,
      int64_t>({prefix + "unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Date>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampParseFunction,
      int64_t,
      Timestamp>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::
          UnixTimestampParseWithFormatFunction,
      int64_t,
      Varchar,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
  registerFunction<
      bytedance::bolt::functions::sparksql::UnixTimestampFromTimestampFunction,
      int64_t,
      Timestamp,
      Varchar>({prefix + "unix_timestamp", prefix + "to_unix_timestamp"});
}
} // namespace

void registerDateTimeBasicConversionFunctions(const std::string& prefix) {
  registerTimestampWithTimeZoneType();
  registerBasicConversionFunctionsInternal(prefix);
  BOLT_REGISTER_VECTOR_FUNCTION(udf_from_unixtime, prefix + "from_unixtime");
}
} // namespace bytedance::bolt::functions
