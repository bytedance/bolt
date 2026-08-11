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

#include "bolt/functions/Macros.h"
#include "bolt/functions/UDFOutputString.h"
#include "bolt/functions/prestosql/SIMDJsonFunctions.h"
#include "bolt/functions/prestosql/json/JsonExtractor.h"
#include "bolt/functions/prestosql/json/JsonPathTokenizer.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"
#include "bolt/functions/prestosql/types/JsonType.h"
namespace bytedance::bolt::functions {

template <typename T>
struct IsJsonScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Json>& json) {
    auto parsedJson = folly::parseJson(json);
    result = parsedJson.isNumber() || parsedJson.isString() ||
        parsedJson.isBool() || parsedJson.isNull();
  }
};

// jsonExtractScalar(json, json_path) -> varchar
// Current implementation support UTF-8 in json, but not in json_path.
// Returns the result value as a string rather than JSON-encoded text. The value
// referenced by json_path must be a scalar (boolean, number or string).
template <typename T>
struct JsonExtractScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  bool throwExceptionWhenEncounterBadJson_;
  bool useDOMParserInGetJsonObject_;
  bool getJsonObjectEscapeEmoji_;
  bool useSonicJson_;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*json*/,
      const arg_type<Varchar>* jsonPath) {
    throwExceptionWhenEncounterBadJson_ =
        config.throwExceptionWhenEncounterBadJson();
    useDOMParserInGetJsonObject_ = config.useDOMParserInGetJsonObject();
    getJsonObjectEscapeEmoji_ = config.getJsonObjectEscapeEmoji();

    useSonicJson_ = config.useSonicJson();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath) {
    const folly::StringPiece& jsonStringPiece = json;
    const folly::StringPiece& jsonPathStringPiece = jsonPath;
    auto extractResult = jsonExtractScalar(
        jsonStringPiece,
        jsonPathStringPiece,
        throwExceptionWhenEncounterBadJson_,
        useDOMParserInGetJsonObject_,
        getJsonObjectEscapeEmoji_,
        true,
        useSonicJson_);
    if (extractResult.hasValue()) {
      UDFOutputString::assign(result, extractResult.value());
      return true;
    } else {
      return false;
    }
  }
};

template <typename T>
struct JsonExtractScalarFunctionWithDetected {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  bool simd_check_scalar(const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());
    ctx.parseDocument();
    bool result =
        (ctx.jsonDoc.type() == simdjson::ondemand::json_type::number ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::string ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::boolean ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::null);
    return result;
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    simdjson::error_code error = parser.parse(json).get(doc);

    if (error && !simd_check_scalar(json)) {
      return false;
    } else {
      const folly::StringPiece& jsonStringPiece = json;
      const folly::StringPiece& jsonPathStringPiece = jsonPath;
      auto extractResult =
          jsonExtractScalar(jsonStringPiece, jsonPathStringPiece);
      if (extractResult.hasValue()) {
        UDFOutputString::assign(result, *extractResult);
        return true;
      } else {
        return false;
      }
    }
  }
};

template <typename T>
struct JsonArrayLengthFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Json>& json) {
    // auto parsedJson = folly::parseJson(json);
    // if (!parsedJson.isArray()) {
    //   return false;
    // }

    // result = parsedJson.size();
    // return true;

    auto res = jsonArrayLength(json);
    if (res.has_value()) {
      result = res.value();
      return true;
    }
    return false;
  }
};

template <typename T>
struct JsonArrayContainsFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    auto parsedJson = folly::parseJson(json);
    if (!parsedJson.isArray()) {
      return false;
    }

    result = false;
    for (const auto& v : parsedJson) {
      if constexpr (std::is_same_v<TInput, bool>) {
        if (v.isBool() && v == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, int64_t>) {
        if (v.isInt() && v == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, double>) {
        if (v.isDouble() && v == value) {
          result = true;
          break;
        }
      } else {
        if (v.isString() && v == value) {
          result = true;
          break;
        }
      }
    }
    return true;
  }
};

template <typename T>
struct JsonSizeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    const folly::StringPiece& jsonStringPiece = json;
    const folly::StringPiece& jsonPathStringPiece = jsonPath;

    auto extractResult = jsonExtractSize(jsonStringPiece, jsonPathStringPiece);
    if (UNLIKELY(!extractResult.has_value())) {
      return false;
    }
    result = extractResult.value();
    return true;
  }
};

} // namespace bytedance::bolt::functions
