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

#include "bolt/functions/flinksql/registration/Register.h"
#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/flinksql/DateTimeDiff.h"
#include "bolt/functions/flinksql/DateTimeFunctions.h"
#include "bolt/functions/flinksql/HashCodeFunction.h"
#include "bolt/functions/flinksql/Rand.h"
#include "bolt/functions/flinksql/String.h"
#include "bolt/functions/flinksql/ToTimestampFunction.h"
#include "bolt/functions/flinksql/specialforms/FlinkCastExpr.h"

namespace bytedance::bolt::functions {
// If the function registration order is Presto, Spark, Flink,
// then these Presto functions may be overwritten by Spark
static void registerPrestoMathFunctionAliases(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_mul, prefix + "flink_decimal_multiply");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_add, prefix + "flink_decimal_add");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_sub, prefix + "flink_decimal_subtract");
}

namespace flinksql {

static void registerStringFunctions(const std::string& prefix) {
  registerFunction<IsAlphaFunction, bool, Varchar>({prefix + "is_alpha"});
  registerFunction<IsDecimalFunction, bool, Varchar>({prefix + "is_decimal"});
  registerFunction<IsDigitFunction, bool, Varchar>({prefix + "is_digit"});
  registerFunction<SplitIndex, Varchar, Varchar, Varchar, int64_t>(
      {prefix + "split_index"});
  registerFunction<SplitIndex, Varchar, Varchar, int32_t, int64_t>(
      {prefix + "split_index"});
}

static void registerDatetimeFunctions(const std::string& prefix) {
  registerFunction<CurrentTimestampFunction, Timestamp>(
      {prefix + "current_timestamp", prefix + "now"});
  registerFunction<CurrentTimestampFunction, Timestamp, Varchar>(
      {prefix + "current_timestamp", prefix + "now"});

  registerFunction<FlinkUnixTimestampFunction, int64_t>(
      {prefix + "flink_unix_timestamp"});
  registerFunction<
      FlinkUnixTimestampFunctionWithTimeZone,
      int64_t,
      Varchar,
      Varchar,
      Constant<Varchar>>({prefix + "flink_unix_timestamp"});

  registerFunction<
      FlinkFromUnixtimeFunction,
      Varchar,
      int64_t,
      Varchar,
      Varchar>({prefix + "flink_from_unixtime"});
  registerFunction<
      FlinkTimestampToStringV2Function,
      Varchar,
      Timestamp,
      int32_t>({prefix + "flink_timestamp_to_string_v2"});

  // TIMESTAMPDIFF(unit, timestamp1, timestamp2)
  registerFunction<
      FlinkTimestampDiffFunction,
      int32_t,
      Varchar,
      Timestamp,
      Timestamp>({prefix + "timestampdiff"});
  registerFunction<
      FlinkTimestampDiffFunction,
      int32_t,
      Varchar,
      Timestamp,
      Date>({prefix + "timestampdiff"});
  registerFunction<
      FlinkTimestampDiffFunction,
      int32_t,
      Varchar,
      Date,
      Timestamp>({prefix + "timestampdiff"});
  registerFunction<FlinkTimestampDiffFunction, int32_t, Varchar, Date, Date>(
      {prefix + "timestampdiff"});

  // TO_TIMESTAMP(string) and TO_TIMESTAMP(string, format)
  registerFunction<FlinkToTimestampFunction, Timestamp, Varchar>(
      {prefix + "to_timestamp"});
  registerFunction<FlinkToTimestampFunction, Timestamp, Varchar, Varchar>(
      {prefix + "to_timestamp"});
}

static void registerMathFunctions(const std::string& prefix) {
  registerPrestoMathFunctionAliases(prefix);

  registerFunction<RandIntegerFunction, int32_t, int32_t>(
      {prefix + "rand_integer"});
  registerFunction<RandIntegerFunction, int32_t, int32_t, Constant<int32_t>>(
      {prefix + "rand_integer"});
}

static void registerJsonFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(json_str_to_map, prefix + "json_str_to_map");
  BOLT_REGISTER_VECTOR_FUNCTION(
      json_str_to_array, prefix + "json_str_to_array");
}

extern void registerFlinkElementAtFunction(const std::string& name);

static void registerArrayFunctions(const std::string& prefix) {
  registerFlinkElementAtFunction(prefix + "element_at");
}

static void registerHashFunctions(const std::string& prefix) {
  // HASH_CODE(expr) for supported types
  registerFunction<FlinkHashCodeFunction, int32_t, int8_t>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, int16_t>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, int32_t>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, int64_t>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, bool>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, float>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, double>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, Varchar>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, Varbinary>(
      {prefix + "hash_code"});
  BOLT_REGISTER_VECTOR_FUNCTION(flink_hash_code_decimal, prefix + "hash_code");
  // DATE is physically int32 days-since-epoch, but signature binding matches on
  // the logical type name, so it needs its own registration; the int32_t call
  // overload (Integer.hashCode == identity) serves it.
  registerFunction<FlinkHashCodeFunction, int32_t, Date>(
      {prefix + "hash_code"});
  registerFunction<FlinkHashCodeFunction, int32_t, Timestamp>(
      {prefix + "hash_code"});
}

namespace {

void registerSpecialFormFunctions(const std::string& prefix) {
  exec::registerFunctionCallToSpecialForm(
      "cast", std::make_unique<FlinkCastCallToSpecialForm>());
  exec::registerFunctionCallToSpecialForm(
      "try_cast", std::make_unique<FlinkTryCastCallToSpecialForm>());
}

} // namespace

void registerFunctions(const std::string& prefix) {
  registerStringFunctions(prefix);
  registerDatetimeFunctions(prefix);
  registerMathFunctions(prefix);
  registerJsonFunctions(prefix);
  registerArrayFunctions(prefix);
  registerHashFunctions(prefix);
  registerSpecialFormFunctions(prefix);
}
} // namespace flinksql
} // namespace bytedance::bolt::functions
