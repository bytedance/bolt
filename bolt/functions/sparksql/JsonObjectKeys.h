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

    simdjson::padded_string paddedJson(json.data(), json.size());
    // The result is NULL if the given string is not a valid JSON string.
    if (simdjsonParse(paddedJson).get(jsonDoc)) {
      return false;
    }

    // On-Demand parsing is lazy. Check the result before reading the type to
    // avoid surfacing parser errors for invalid JSON.
    auto jsonType = jsonDoc.type();
    if (jsonType.error() != simdjson::SUCCESS ||
        jsonType.value_unsafe() != simdjson::ondemand::json_type::object) {
      return false;
    }

    simdjson::ondemand::object jsonObject;
    // The result is NULL if the given string is not a valid JSON object string.
    if (jsonDoc.get_object().get(jsonObject)) {
      return false;
    }

    for (auto field : jsonObject) {
      if (field.error() != simdjson::SUCCESS) {
        return false;
      }

      auto key = field.unescaped_key(false);
      if (key.error() != simdjson::SUCCESS) {
        return false;
      }
      out.add_item().copy_from(std::string_view(key.value_unsafe()));
    }

    // Iteration validates the object structure, while at_end() rejects valid
    // objects followed by trailing non-whitespace content.
    return jsonDoc.at_end();
  }
};

} // namespace bytedance::bolt::functions::sparksql
