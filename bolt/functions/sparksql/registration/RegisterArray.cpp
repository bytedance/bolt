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

#include "bolt/functions/lib/ArrayRemoveNullFunction.h"
#include "bolt/functions/lib/ArrayShuffle.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/lib/Repeat.h"
#include "bolt/functions/lib/Slice.h"
#include "bolt/functions/prestosql/ArrayFunctions.h"
#include "bolt/functions/sparksql/ArrayAppend.h"
#include "bolt/functions/sparksql/ArrayFlattenFunction.h"
#include "bolt/functions/sparksql/ArrayInsert.h"
#include "bolt/functions/sparksql/ArrayMinMaxFunction.h"
#include "bolt/functions/sparksql/ArrayPrepend.h"
#include "bolt/functions/sparksql/ArraySort.h"
#include "bolt/functions/sparksql/ArrayUnionFunction.h"
#include "bolt/functions/sparksql/SimpleComparisonMatcher.h"

namespace bytedance::bolt::functions {
// BOLT_REGISTER_VECTOR_FUNCTION must be invoked in the same namespace as the
// vector function definition.
// Higher order functions.
void registerSparkArrayFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "aggregate");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_constructor, prefix + "array");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_contains, prefix + "array_contains");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_array_intersect, prefix + "array_intersect");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_except, prefix + "array_except");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_array_slice_sum, prefix + "array_slice_sum");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_any_match, prefix + "exists");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_zip, prefix + "arrays_zip");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_zip_with, prefix + "zip_with");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_all_match, prefix + "forall")
}

namespace sparksql {

template <typename T>
inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerFunction<ArrayMinFunction, T, Array<T>>({prefix + "array_min"});
  registerFunction<ArrayMaxFunction, T, Array<T>>({prefix + "array_max"});
}

inline void registerArrayMinMaxFunctions(const std::string& prefix) {
  registerArrayMinMaxFunctions<int8_t>(prefix);
  registerArrayMinMaxFunctions<int16_t>(prefix);
  registerArrayMinMaxFunctions<int32_t>(prefix);
  registerArrayMinMaxFunctions<int64_t>(prefix);
  registerArrayMinMaxFunctions<int128_t>(prefix);
  registerArrayMinMaxFunctions<float>(prefix);
  registerArrayMinMaxFunctions<double>(prefix);
  registerArrayMinMaxFunctions<bool>(prefix);
  registerArrayMinMaxFunctions<Varbinary>(prefix);
  registerArrayMinMaxFunctions<Varchar>(prefix);
  registerArrayMinMaxFunctions<Timestamp>(prefix);
  registerArrayMinMaxFunctions<Date>(prefix);
  registerArrayMinMaxFunctions<Orderable<T1>>(prefix);
}

template <typename T>
inline void registerArrayPrependFunctions(const std::string& prefix) {
  registerFunction<ArrayPrependFunction, Array<T>, Array<T>, T>(
      {prefix + "array_prepend"});
}

inline void registerArrayPrependFunctions(const std::string& prefix) {
  registerArrayPrependFunctions<int8_t>(prefix);
  registerArrayPrependFunctions<int16_t>(prefix);
  registerArrayPrependFunctions<int32_t>(prefix);
  registerArrayPrependFunctions<int64_t>(prefix);
  registerArrayPrependFunctions<int128_t>(prefix);
  registerArrayPrependFunctions<float>(prefix);
  registerArrayPrependFunctions<double>(prefix);
  registerArrayPrependFunctions<bool>(prefix);
  registerArrayPrependFunctions<Timestamp>(prefix);
  registerArrayPrependFunctions<Date>(prefix);
  registerArrayPrependFunctions<Varbinary>(prefix);
  registerArrayPrependFunctions<Varchar>(prefix);
  registerArrayPrependFunctions<Generic<T1>>(prefix);
}

template <typename T>
inline void registerArrayUnionFunctions(const std::string& prefix) {
  registerFunction<sparksql::ArrayUnionFunction, Array<T>, Array<T>, Array<T>>(
      {prefix + "array_union"});
}

template <typename T>
inline void registerArrayUnionUdfFunctions(const std::string& prefix) {
  registerFunction<
      sparksql::ArrayUnionUdfFunction,
      Array<T>,
      Variadic<Array<T>>>({prefix + "array_union_udf"});
}

inline void registerArrayUnionFunctions(const std::string& prefix) {
  registerArrayUnionFunctions<bool>(prefix);
  registerArrayUnionFunctions<int8_t>(prefix);
  registerArrayUnionFunctions<int16_t>(prefix);
  registerArrayUnionFunctions<int32_t>(prefix);
  registerArrayUnionFunctions<int64_t>(prefix);
  registerArrayUnionFunctions<int128_t>(prefix);
  registerArrayUnionFunctions<float>(prefix);
  registerArrayUnionFunctions<double>(prefix);
  registerArrayUnionFunctions<Varchar>(prefix);
  registerArrayUnionFunctions<Varbinary>(prefix);
  registerArrayUnionFunctions<Date>(prefix);
  registerArrayUnionFunctions<Timestamp>(prefix);
  registerArrayUnionFunctions<Date>(prefix);
  registerArrayUnionFunctions<Generic<T1>>(prefix);
}

inline void registerArrayUnionUdfFunctions(const std::string& prefix) {
  registerArrayUnionUdfFunctions<bool>(prefix);
  registerArrayUnionUdfFunctions<int8_t>(prefix);
  registerArrayUnionUdfFunctions<int16_t>(prefix);
  registerArrayUnionUdfFunctions<int32_t>(prefix);
  registerArrayUnionUdfFunctions<int64_t>(prefix);
  registerArrayUnionUdfFunctions<int128_t>(prefix);
  registerArrayUnionUdfFunctions<float>(prefix);
  registerArrayUnionUdfFunctions<double>(prefix);
  registerArrayUnionUdfFunctions<Varchar>(prefix);
  registerArrayUnionUdfFunctions<Varbinary>(prefix);
  registerArrayUnionUdfFunctions<Date>(prefix);
  registerArrayUnionUdfFunctions<Timestamp>(prefix);
  registerArrayUnionUdfFunctions<Date>(prefix);
  registerArrayUnionUdfFunctions<Varbinary>(prefix);
}

template <typename T>
inline void registerArrayCompactFunction(const std::string& prefix) {
  registerFunction<ArrayRemoveNullFunction, Array<T>, Array<T>>(
      {prefix + "array_compact"});
}

inline void registerArrayCompactFunctions(const std::string& prefix) {
  registerArrayCompactFunction<int8_t>(prefix);
  registerArrayCompactFunction<int16_t>(prefix);
  registerArrayCompactFunction<int32_t>(prefix);
  registerArrayCompactFunction<int64_t>(prefix);
  registerArrayCompactFunction<int128_t>(prefix);
  registerArrayCompactFunction<float>(prefix);
  registerArrayCompactFunction<double>(prefix);
  registerArrayCompactFunction<bool>(prefix);
  registerArrayCompactFunction<Timestamp>(prefix);
  registerArrayCompactFunction<Date>(prefix);
  registerArrayCompactFunction<Varbinary>(prefix);
  registerArrayCompactFunction<Generic<T1>>(prefix);
  registerFunction<
      ArrayRemoveNullFunctionString,
      Array<Varchar>,
      Array<Varchar>>({prefix + "array_compact"});
}

void registerArrayFunctions(const std::string& prefix) {
  registerArrayMinMaxFunctions(prefix);
  registerArrayPrependFunctions(prefix);
  registerSparkArrayFunctions(prefix);
  registerArrayUnionFunctions(prefix);
  registerArrayUnionUdfFunctions(prefix);
  // Register array sort functions.
  exec::registerStatefulVectorFunction(
      prefix + "array_sort", arraySortSignatures(true), makeArraySortAsc);
  exec::registerStatefulVectorFunction(
      prefix + "array_sort_desc", arraySortDescSignatures(), makeArraySortDesc);
  exec::registerStatefulVectorFunction(
      prefix + "sort_array", sortArraySignatures(), makeSortArray);

  auto checker = std::make_shared<SparkSimpleComparisonChecker>();
  exec::registerExpressionRewrite([prefix, checker](const auto& expr) {
    return rewriteArraySortCall(prefix, expr, checker);
  });
  exec::registerStatefulVectorFunction(
      prefix + "array_repeat",
      repeatSignatures(),
      makeRepeatAllowNegativeCount);

  registerIntegerSliceFunction(prefix);

  // for now, register ArrayRemoveFunctionString that takes String input and
  // output is enough for our use cases. In future, if need to support more
  // input/output types, will register generic ArrayRemoveFunction
  registerFunction<
      ArrayRemoveFunctionString,
      Array<Varchar>,
      Array<Varchar>,
      Varchar>({prefix + "array_remove"});

  registerFunction<
      ArrayFlattenFunction,
      Array<Generic<T1>>,
      Array<Array<Generic<T1>>>>({prefix + "flatten"});

  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_get, prefix + "get");

  registerFunction<
      ArrayAppendFunction,
      Array<Generic<T1>>,
      Array<Generic<T1>>,
      Generic<T1>>({prefix + "array_append"});

  registerFunction<
      ArrayInsert,
      Array<Generic<T1>>,
      Array<Generic<T1>>,
      int32_t,
      Generic<T1>,
      bool>({prefix + "array_insert"});

  registerArrayCompactFunctions(prefix);

  exec::registerStatefulVectorFunction(
      prefix + "shuffle",
      arrayShuffleWithCustomSeedSignatures(),
      makeArrayShuffleWithCustomSeed);
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
