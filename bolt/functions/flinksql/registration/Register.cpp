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
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/flinksql/DateTimeFunctions.h"
#include "bolt/functions/flinksql/Rand.h"
#include "bolt/functions/flinksql/String.h"

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

void registerFunctions(const std::string& prefix) {
  registerStringFunctions(prefix);
  registerDatetimeFunctions(prefix);
  registerMathFunctions(prefix);
  registerJsonFunctions(prefix);
  registerArrayFunctions(prefix);
}
} // namespace flinksql
} // namespace bytedance::bolt::functions
