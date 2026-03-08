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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/In.h"
#include "bolt/functions/sparksql/MonotonicallyIncreasingId.h"
#include "bolt/functions/sparksql/RaiseError.h"
#include "bolt/functions/sparksql/SparkPartitionId.h"
#include "bolt/functions/sparksql/UnscaledValueFunction.h"
namespace bytedance::bolt::functions {
void registerSparkMiscFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_subscript, prefix + "get_array_item");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_or_array_combine, prefix + "combine");

  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_str_to_long_arr, prefix + "str_to_long_arr");
}
namespace sparksql {
void registerMiscFunctions(const std::string& prefix) {
  exec::registerVectorFunction(
      prefix + "unscaled_value",
      unscaledValueSignatures(),
      makeUnscaledValue());

  exec::registerStatefulVectorFunction(
      prefix + "concat_ws", ConcatWsSignatures(), makeConcatWs);

  registerIn(prefix);

  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_input_file_name, prefix + "input_file_name");

  registerFunction<RaiseErrorFunction, UnknownValue, Varchar>(
      {prefix + "raise_error"});

  registerFunction<SparkPartitionIdFunction, int32_t>(
      {prefix + "spark_partition_id"});

  registerFunction<MonotonicallyIncreasingIdFunction, int64_t>(
      {prefix + "monotonically_increasing_id"});
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
