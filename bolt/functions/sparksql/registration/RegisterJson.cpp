/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/JsonFunctions.h"
#include "bolt/functions/prestosql/SIMDJsonFunctions.h"
#include "bolt/functions/sparksql/JsonObjectKeys.h"
#include "bolt/functions/sparksql/JsonTuple.h"
#include "bolt/functions/sparksql/SIMDJsonFunctions.h"
#include "bolt/functions/sparksql/ToJson.h"
#include "bolt/functions/sparksql/specialforms/FromJson.h"
#include "bolt/functions/sparksql/specialforms/JsonSplit.h"
namespace bytedance::bolt::functions {
namespace sparksql {
void registerJsonFunctions(const std::string& prefix) {
  registerFunctionCallToSpecialForm(
      "json_split", std::make_unique<JsonSplitToSpecialForm>());
  //   registerFunctionCallToSpecialForm(
  //       "from_json", std::make_unique<FromJsonToSpecialForm>());

  BOLT_REGISTER_VECTOR_FUNCTION(udf_json_to_map, prefix + "json_to_map");
  exec::registerStatefulVectorFunction(
      prefix + "json_tuple_with_codegen", jsonTupleSignatures(), makeJsonTuple);
  registerJsonType(); // to register Json type
  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Json>(
      {prefix + "json_array_length"});
  registerFunction<WrapperJsonArrayLengthFunction, int64_t, Varchar>(
      {prefix + "json_array_length"});

  //   registerFunction<SIMDGetJsonObjectFunction, Varchar, Varchar, Varchar>(
  //       {prefix + "get_json_object"});

  registerFunction<JsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {prefix + "get_json_object"});

  registerFunction<JsonObjectKeysFunction, Array<Varchar>, Varchar>(
      {prefix + "json_object_keys"});
  registerFunction<ToJsonFunction, Varchar, Generic<T1>>({prefix + "to_json"});
  registerFunction<ToJsonFunction, Varchar, Generic<T1>, Varchar>(
      {prefix + "to_json"});
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
