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

#include "bolt/functions/lib/IsNull.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/Comparisons.h"
#include "bolt/functions/sparksql/LeastGreatest.h"
namespace bytedance::bolt::functions {
void registerSparkCompareFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_not, prefix + "not");
  registerIsNullFunction(prefix + "isnull");
  registerIsNotNullFunction(prefix + "isnotnull");
}

namespace sparksql {
void registerCompareFunctions(const std::string& prefix) {
  registerSparkCompareFunctions(prefix);
  exec::registerStatefulVectorFunction(
      prefix + "equalto", comparisonSignatures(), makeEqualTo);
  exec::registerStatefulVectorFunction(
      prefix + "lessthan", comparisonSignatures(), makeLessThan);
  exec::registerStatefulVectorFunction(
      prefix + "greaterthan", comparisonSignatures(), makeGreaterThan);
  exec::registerStatefulVectorFunction(
      prefix + "lessthanorequal", comparisonSignatures(), makeLessThanOrEqual);
  exec::registerStatefulVectorFunction(
      prefix + "greaterthanorequal",
      comparisonSignatures(),
      makeGreaterThanOrEqual);
  // Compare nullsafe functions.
  exec::registerStatefulVectorFunction(
      prefix + "equalnullsafe", equalSignatures(), makeEqualToNullSafe);

  exec::registerStatefulVectorFunction(
      prefix + "least", leastSignatures(), makeLeast);
  exec::registerStatefulVectorFunction(
      prefix + "greatest", greatestSignatures(), makeGreatest);

  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int128_t, int128_t, int128_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, float, float, float>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, double, double, double>(
      {prefix + "between"});
  // Decimal compare functions.
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_gt, prefix + "decimal_greaterthan");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_gte, prefix + "decimal_greaterthanorequal");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_lt, prefix + "decimal_lessthan");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_decimal_lte, prefix + "decimal_lessthanorequal");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_eq, prefix + "decimal_equalto");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_neq, prefix + "decimal_notequalto");
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
