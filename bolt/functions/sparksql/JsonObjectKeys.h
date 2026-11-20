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
#pragma once

#include "bolt/functions/prestosql/json/SIMDJsonUtil.h"

namespace bytedance::bolt::functions::sparksql {

/// json_object_keys(jsonString) -> array(string)
///
/// Returns all the keys of the outermost JSON object as an array if a valid
/// JSON object is given.
template <typename T>
struct JsonObjectKeysFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Array<Varchar>>& out,
      const arg_type<Varchar>& json) {
    simdjson::ondemand::document jsonDoc;

    paddedInput_.assign(json.data(), json.size());
    paddedInput_.append(simdjson::SIMDJSON_PADDING, '\0');
    simdjson::padded_string_view paddedJson(
        paddedInput_.data(), json.size(), paddedInput_.size());
    // The result is NULL if the given string is not a valid JSON string.
    if (simdjsonParse(paddedJson).get(jsonDoc)) {
      return false;
    }

    // The result is NULL if the given string is not a JSON object string.
    if (jsonDoc.type() != simdjson::ondemand::json_type::object) {
      return false;
    }

    simdjson::ondemand::object jsonObject;
    // The result is NULL if the given string is not a valid JSON object string.
    if (jsonDoc.get_object().get(jsonObject)) {
      return false;
    }

    for (auto field : jsonObject) {
      out.add_item().copy_from(std::string_view(field.unescaped_key(false)));
    }
    return true;
  }

 private:
  std::string paddedInput_;
};

} // namespace bytedance::bolt::functions::sparksql
