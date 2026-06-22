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

#include "bolt/functions/lib/MapConcat.h"
#include "bolt/functions/lib/MapFromEntries.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/lib/Size.h"
#include "bolt/functions/sparksql/MapFromArrays.h"
namespace bytedance::bolt::functions {
extern void registerElementAtFunction(
    const std::string& name,
    bool enableCaching);

void registerSparkMapFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_filter, prefix + "map_filter");
  // Complex types.

  sparksql::registerMapFromArrays(prefix + "map_from_arrays");
  registerMapFromEntriesFunction(prefix + "map_from_entries", false);
  registerMapConcatFunction(prefix + "map_concat");
  registerMapConcatEmptyNullsFunction(prefix + "map_concat");
  registerMapConcatAllowSingleArg(prefix + "map_concat");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_update, prefix + "map_update");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_subscript, prefix + "get_map_value");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_map_filter_keys, prefix + "map_filter_keys");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_or_array_combine, prefix + "combine");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_str_to_long_arr, prefix + "str_to_long_arr");
}

namespace sparksql {
void registerMapFunctions(const std::string& prefix) {
  registerSparkMapFunctions(prefix);
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");
  // This is the semantics of spark.sql.ansi.enabled = false.
  registerElementAtFunction(prefix + "element_at", true);
  registerSize(prefix + "size");
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
