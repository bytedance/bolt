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

#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/sparksql/CharVarchar.h"

namespace bytedance::bolt::functions::sparksql {

void registerCharVarcharFunctions(const std::string& prefix) {
  registerFunction<CharTypeWriteSideCheckFunction, Varchar, Varchar, int32_t>(
      {prefix + "char_type_write_side_check"});
  registerFunction<
      VarcharTypeWriteSideCheckFunction,
      Varchar,
      Varchar,
      int32_t>({prefix + "varchar_type_write_side_check"});
  registerFunction<ReadSidePaddingFunction, Varchar, Varchar, int32_t>(
      {prefix + "read_side_padding"});
}

} // namespace bytedance::bolt::functions::sparksql
