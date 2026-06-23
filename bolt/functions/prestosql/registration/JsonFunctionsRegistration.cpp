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
#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/JsonFunctions.h"
#include "bolt/functions/prestosql/SIMDJsonFunctions.h"
#include "bolt/functions/sparksql/specialforms/JsonSplit.h"
namespace bytedance::bolt::functions {
void registerJsonFunctions(const std::string& prefix) {
  registerJsonType();

  registerFunction<WrapperIsJsonScalarFunction, bool, Json>(
      {prefix + "is_json_scalar"});
  registerFunction<WrapperIsJsonScalarFunction, bool, Varchar>(
      {prefix + "is_json_scalar"});

  registerFunction<WrapperIsJsonObjectFunction, bool, Json>(
      {prefix + "is_json_object"});
  registerFunction<WrapperIsJsonObjectFunction, bool, Varchar>(
      {prefix + "is_json_object"});

  registerFunction<WrapperJsonExtractScalarFunction, Varchar, Json, Varchar>(
      {prefix + "json_extract_scalar"});
  registerFunction<WrapperJsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "json_extract_scalar"});

  registerFunction<WrapperJsonExtractFunction, Json, Json, Varchar>(
      {prefix + "json_extract"});
  registerFunction<WrapperJsonExtractFunction, Json, Varchar, Varchar>(
      {prefix + "json_extract"});

  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Json>(
      {prefix + "json_array_length"});
  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Varchar>(
      {prefix + "json_array_length"});

  registerFunction<
      JsonExtractScalarFunctionWithDetected,
      Varchar,
      Json,
      Varchar>({prefix + "get_json_object_detect_bad_json"});
  registerFunction<
      JsonExtractScalarFunctionWithDetected,
      Varchar,
      Varchar,
      Varchar>({prefix + "get_json_object_detect_bad_json"});

  registerFunction<JsonExtractScalarFunction, Varchar, Json, Varchar>(
      {prefix + "get_json_object"});
  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  registerFunction<WrapperJsonArrayContainsFunction, bool, Json, bool>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Varchar, bool>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Json, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Varchar, int64_t>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Json, double>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Varchar, double>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Json, Varchar>(
      {prefix + "json_array_contains"});
  registerFunction<WrapperJsonArrayContainsFunction, bool, Varchar, Varchar>(
      {prefix + "json_array_contains"});

  registerFunction<WrapperJsonSizeFunction, int64_t, Json, Varchar>(
      {prefix + "json_size"});
  registerFunction<WrapperJsonSizeFunction, int64_t, Varchar, Varchar>(
      {prefix + "json_size"});

  BOLT_REGISTER_VECTOR_FUNCTION(udf_json_format, prefix + "json_format");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_json_parse, prefix + "json_parse");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_json_to_map, prefix + "json_to_map");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_to_json, prefix + "to_json");

  // prefix is "presto.default"
  exec::registerFunctionCallToSpecialForm(
      prefix + "json_split",
      std::make_unique<
          bytedance::bolt::functions::sparksql::JsonSplitToSpecialForm>());
}

} // namespace bytedance::bolt::functions
