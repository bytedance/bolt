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

#include "bolt/functions/Macros.h"
#include "bolt/functions/UDFOutputString.h"
#include "bolt/functions/prestosql/json/JsonPathTokenizer.h"
#include "bolt/functions/prestosql/json/SIMDJsonExtractor.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"
#include "bolt/functions/prestosql/types/JsonType.h"

#include "sonic/dom/parser.h"
#include "sonic/sonic.h"

#include <re2/re2.h>
#include <sstream>
namespace bytedance::bolt::functions {

struct ParserContext {
 public:
  explicit ParserContext() noexcept;
  explicit ParserContext(const char* data, size_t length) noexcept
      : padded_json(data, length) {}
  void parseElement() {
    jsonEle = domParser.parse(padded_json);
  }
  void parseDocument() {
    jsonDoc = ondemandParser.iterate(padded_json);
  }
  simdjson::dom::element jsonEle;
  simdjson::ondemand::document jsonDoc;

 private:
  simdjson::padded_string padded_json;
  simdjson::dom::parser domParser;
  simdjson::ondemand::parser ondemandParser;
};

template <typename T>
struct SIMDIsJsonScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static void call(
      bool& result,
      const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());

    ctx.parseDocument();

    result =
        (ctx.jsonDoc.type() == simdjson::ondemand::json_type::number ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::string ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::boolean ||
         ctx.jsonDoc.type() == simdjson::ondemand::json_type::null);
  }
};

template <typename T>
struct SIMDIsJsonObjectFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static void call(
      bool& result,
      const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());
    try {
      ctx.parseDocument();
      result = (ctx.jsonDoc.type() == simdjson::ondemand::json_type::object);
    } catch (const simdjson::simdjson_error&) {
      result = false;
    }
  }
};

template <typename T>
struct SonicIsJsonScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      bool& result,
      const arg_type<Json>& json) {
    sonic_json::Document jsonDoc;

    jsonDoc.Parse(json.data(), json.size());

    if (jsonDoc.HasParseError()) {
      return false;
    }

    result =
        (jsonDoc.IsNumber() || jsonDoc.IsString() || jsonDoc.IsBool() ||
         jsonDoc.IsNull());
    return true;
  }
};

template <typename T>
struct SonicIsJsonObjectFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static void call(
      bool& result,
      const arg_type<Json>& json) {
    sonic_json::Document jsonDoc;
    jsonDoc.Parse(json.data(), json.size());
    if (jsonDoc.HasParseError()) {
      result = false;
      return;
    }
    result = jsonDoc.IsObject();
  }
};

template <typename T>
class WrapperIsJsonScalarFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json) {
    useSonic = config.enableSonicIsJsonScalar();
  }

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Json>& json) {
    if (useSonic) {
      SonicIsJsonScalarFunction<T>::call(result, json);
    } else {
      SIMDIsJsonScalarFunction<T>::call(result, json);
    }
  }

  bool useSonic = true;
};

template <typename T>
class WrapperIsJsonObjectFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* /*json*/) {
    useSonic = config.enableSonicIsJsonScalar();
  }

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Json>& json) {
    if (useSonic) {
      SonicIsJsonObjectFunction<T>::call(result, json);
    } else {
      SIMDIsJsonObjectFunction<T>::call(result, json);
    }
  }

  bool useSonic = true;
};

template <typename T>
struct SIMDJsonArrayContainsFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput>
  FOLLY_ALWAYS_INLINE static bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    ParserContext ctx(json.data(), json.size());
    result = false;

    try {
      ctx.parseDocument();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    if (ctx.jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    for (auto&& v : ctx.jsonDoc) {
      try {
        if constexpr (std::is_same_v<TInput, bool>) {
          if (v.type() == simdjson::ondemand::json_type::boolean &&
              v.get_bool() == value) {
            result = true;
            break;
          }
        } else if constexpr (std::is_same_v<TInput, int64_t>) {
          if (v.type() == simdjson::ondemand::json_type::number &&
              v.get_number_type() ==
                  simdjson::ondemand::number_type::signed_integer &&
              v.get_int64() == value) {
            result = true;
            break;
          }
        } else if constexpr (std::is_same_v<TInput, double>) {
          if (v.type() == simdjson::ondemand::json_type::number &&
              v.get_number_type() ==
                  simdjson::ondemand::number_type::floating_point_number &&
              v.get_double() == value) {
            result = true;
            break;
          }
        } else {
          if (v.type() == simdjson::ondemand::json_type::string) {
            std::string_view rlt = v.get_string(true);
            std::string str_value{value.getString()};
            if (rlt.compare(str_value) == 0) {
              result = true;
              break;
            }
          }
        }
      } catch (const simdjson::simdjson_error& e) {
        // For bool/int64_t/double type, "get_bool()/get_int64()/get_double()"
        // may throw an exception if the conversion of json value to the
        // specified type failed, and the corresponding error code is
        // "INCORRECT_TYPE" or "NUMBER_ERROR".
        // If there are multiple json values in the json array, and some of them
        // cannot be converted to the type of input `value`, it should not
        // return null directly. It should continue to judge whether there is a
        // json value that matches the input value, e.g.
        // jsonArrayContains("[truet, false]", false) => true.
        if (e.error() != simdjson::INCORRECT_TYPE &&
            e.error() != simdjson::NUMBER_ERROR) {
          return false;
        }
      }
    }

    return true;
  }
};

template <typename T>
struct SonicJsonArrayContainsFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput>
  FOLLY_ALWAYS_INLINE static bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    sonic_json::Document jsonDoc;

    jsonDoc.Parse(json.data(), json.size());

    if (jsonDoc.HasParseError()) {
      return false;
    }

    if (!jsonDoc.IsArray()) {
      return false;
    }

    for (auto v = jsonDoc.Begin(); v != jsonDoc.End(); v++) {
      if constexpr (std::is_same_v<TInput, bool>) {
        if (v->IsBool() && v->GetBool() == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, int64_t>) {
        if (v->IsInt64() && v->GetInt64() == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, double>) {
        if (v->IsDouble() && v->GetDouble() == value) {
          result = true;
          break;
        }
      } else {
        if (v->IsString()) {
          std::string_view str_view = v->GetStringView();
          if ((std::string_view)value == str_view) {
            result = true;
            break;
          }
        }
      }
    }

    return true;
  }
};

template <typename T>
class WrapperJsonArrayContainsFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json,
      const TInput* value) {
    useSonic = config.enableSonicJsonArrayContains();
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    if (useSonic) {
      return SonicJsonArrayContainsFunction<T>::call(result, json, value);
    } else {
      return SIMDJsonArrayContainsFunction<T>::call(result, json, value);
    }
  }

  bool useSonic = true;
};

template <typename T>
struct SIMDJsonArrayLengthFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      int64_t& len,
      const arg_type<Json>& json) {
    ParserContext ctx(json.data(), json.size());

    try {
      ctx.parseDocument();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    if (ctx.jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    try {
      len = ctx.jsonDoc.count_elements();
    } catch (const simdjson::simdjson_error&) {
      return false;
    }

    return true;
  }
};

template <typename T>
struct SonicJsonArrayLengthFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      int64_t& len,
      const arg_type<Json>& json) {
    sonic_json::Document jsonDoc;

    jsonDoc.Parse(json.data(), json.size());

    if (jsonDoc.HasParseError()) {
      return false;
    }

    if (!jsonDoc.IsArray()) {
      return false;
    }

    len = jsonDoc.Size();

    return true;
  }
};

template <typename T>
class WrapperJsonArrayLengthFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json) {
    useSonic = config.enableSonicJsonArrayLength();
  }

  template <typename TResult>
  FOLLY_ALWAYS_INLINE bool call(TResult& len, const arg_type<Json>& json) {
    int64_t jsonArrayLength;
    if (useSonic) {
      if (!SonicJsonArrayLengthFunction<T>::call(jsonArrayLength, json)) {
        return false;
      }
    } else {
      if (!SIMDJsonArrayLengthFunction<T>::call(jsonArrayLength, json)) {
        return false;
      }
    }

    len = static_cast<TResult>(jsonArrayLength);
    return true;
  }

  bool useSonic = true;
};

static constexpr std::string_view kNullString{"null"};

template <typename StringType = SONIC_JSON_POINTER_NODE_STRING_DEFAULT_TYPE>
bool jsonpath_to_pointer(
    const std::string& jsonpath,
    sonic_json::GenericJsonPointer<StringType>& result) {
  static const RE2 jsonPathRegex(
      R"(^\$(?:\[(\d+|\'[^']+\')\]|\.[a-zA-Z_][a-zA-Z0-9_]*|\['[^']+'\])*$)");

  if (!RE2::FullMatch(jsonpath, jsonPathRegex)) {
    return false;
  }

  const std::string path = jsonpath.substr(1); // keep this alive
  RE2 token_regex(R"(\['([^']+)'\]|\[(\d+)\]|\.([a-zA-Z0-9_]+))");

  re2::StringPiece input(path);
  std::string token1, token2, token3;

  while (RE2::Consume(&input, token_regex, &token1, &token2, &token3)) {
    if (!token1.empty()) {
      result = result / std::string_view(token1.data(), token1.size());
    } else if (!token2.empty()) {
      result = result / std::stoi(token2);
    } else if (!token3.empty()) {
      result = result / std::string_view(token3.data(), token3.size());
    }

    // Reset capture groups for the next iteration
    token1.clear();
    token2.clear();
    token3.clear();
  }

  return true;
}

// jsonExtractScalar(json, json_path) -> varchar
// Like jsonExtract(), but returns the result value as a string (as opposed
// to being encoded as JSON). The value referenced by json_path must be a scalar
// (boolean, number or string)
template <typename T>
struct SIMDJsonExtractScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  FOLLY_ALWAYS_INLINE static bool callImpl(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    bool resultPopulated = false;
    std::optional<std::string> resultStr;
    auto consumer = [&resultStr, &resultPopulated](auto& v) {
      if (resultPopulated) {
        // We should just get a single value, if we see multiple, it's an error
        // and we should return null.
        resultStr = std::nullopt;
        return simdjson::SUCCESS;
      }

      resultPopulated = true;

      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(bool vbool, v.get_bool());
          resultStr = vbool ? "true" : "false";
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, v.get_string(true));
          break;
        }
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::null:
          // Do nothing.
          break;
        default: {
          SIMDJSON_ASSIGN_OR_RAISE(resultStr, simdjson::to_json_string(v));
        }
      }
      return simdjson::SUCCESS;
    };

    SIMDJSON_TRY(simdJsonExtract(json, jsonPath, consumer));

    if (resultStr.has_value()) {
      result.copy_from(*resultStr);
      return simdjson::SUCCESS;
    } else {
      return simdjson::NO_SUCH_FIELD;
    }
  }
};

// jsonExtractScalar(json, json_path) -> varchar
// Like jsonExtract(), but returns the result value as a string (as opposed
// to being encoded as JSON). The value referenced by json_path must be a scalar
// (boolean, number or string)
template <typename T>
struct SonicJsonExtractScalarFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    sonic_json::Document jsonDoc;

    sonic_json::GenericJsonPointer<SONIC_JSON_POINTER_NODE_STRING_DEFAULT_TYPE>
        jsonPtr;
    if (!jsonpath_to_pointer(jsonPath, jsonPtr)) {
      BOLT_USER_FAIL("Invalid JSON path");
    }

    jsonDoc.ParseOnDemand(json.data(), json.size(), jsonPtr);

    if (jsonDoc.HasParseError()) {
      return false;
    }

    if (jsonDoc.IsString()) {
      result.copy_from(jsonDoc.GetStringView());
      return true;
    } else if (jsonDoc.IsNumber()) {
      sonic_json::WriteBuffer wb;
      jsonDoc.Serialize(wb);
      auto str = wb.ToString();
      result.copy_from(str);
      return true;
    } else if (jsonDoc.IsBool()) {
      result.copy_from(jsonDoc.GetBool() ? "true" : "false");
      return true;
    } else if (jsonDoc.IsNull()) {
      result.copy_from(kNullString);
      return true;
    } else {
      return false;
    }

    return true;
  }
};

template <typename T>
class WrapperJsonExtractScalarFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json,
      const arg_type<Varchar>* jsonPath) {
    useSonic = config.enableSonicJsonExtractScalar();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    if (useSonic) {
      return SonicJsonExtractScalarFunction<T>::call(result, json, jsonPath);
    } else {
      return SIMDJsonExtractScalarFunction<T>::call(result, json, jsonPath);
    }
  }

  bool useSonic = true;
};

template <typename T>
struct SIMDJsonExtractFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static bool call(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  static simdjson::error_code callImpl(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    static constexpr std::string_view kNullString{"null"};
    std::string results;
    size_t resultSize = 0;
    auto consumer = [&results, &resultSize](auto& v) {
      // Add the separator for the JSON array.
      if (resultSize++ > 0) {
        results += ",";
      }
      // We could just convert v to a string using to_json_string directly, but
      // in that case the JSON wouldn't be parsed (it would just return the
      // contents directly) and we might miss invalid JSON.
      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::object: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_object()));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::array: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_array()));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::string:
        case simdjson::ondemand::json_type::number:
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(auto jsonStr, simdjson::to_json_string(v));
          results += jsonStr;
          break;
        }
        case simdjson::ondemand::json_type::null:
          results += kNullString;
          break;
      }
      return simdjson::SUCCESS;
    };

    SIMDJSON_TRY(simdJsonExtract(json, jsonPath, consumer));

    if (resultSize == 0) {
      // If the path didn't map to anything in the JSON object, return null.
      return simdjson::NO_SUCH_FIELD;
    }

    if (resultSize == 1) {
      // If there was only one value mapped to by the path, don't wrap it in an
      // array.
      result.copy_from(results);
    } else {
      // Add the square brackets to make it a valid JSON array.
      result.copy_from("[" + results + "]");
    }
    return simdjson::SUCCESS;
  }
};

template <typename T>
struct SonicJsonExtractFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static bool call(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    sonic_json::Document jsonDoc;

    sonic_json::GenericJsonPointer<SONIC_JSON_POINTER_NODE_STRING_DEFAULT_TYPE>
        jsonPtr;
    if (!jsonpath_to_pointer(jsonPath, jsonPtr)) {
      BOLT_USER_FAIL("Invalid JSON path: {}", jsonPath);
      return false;
    }

    jsonDoc.ParseOnDemand(json.data(), json.size(), jsonPtr);

    if (jsonDoc.HasParseError()) {
      return false;
    }

    sonic_json::WriteBuffer wb;
    jsonDoc.Serialize(wb);
    auto str = wb.ToString();
    result.copy_from(str);
    return true;
  }
};

template <typename T>
class WrapperJsonExtractFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json,
      const arg_type<Varchar>* jsonPath) {
    useSonic = config.enableSonicJsonExtract();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    if (useSonic) {
      return SonicJsonExtractFunction<T>::call(result, json, jsonPath);
    } else {
      return SIMDJsonExtractFunction<T>::call(result, json, jsonPath);
    }
  }

  bool useSonic = true;
};

template <typename T>
struct SIMDJsonSizeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    return callImpl(result, json, jsonPath) == simdjson::SUCCESS;
  }

 private:
  FOLLY_ALWAYS_INLINE static simdjson::error_code callImpl(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    size_t resultCount = 0;
    size_t singleResultSize = 0;
    auto consumer = [&resultCount, &singleResultSize](auto& v) {
      resultCount++;

      if (resultCount == 1) {
        // We only need the size of the actual object if there's only one
        // returned, if multiple are returned we use the number of objects
        // returned instead.
        SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
        switch (vtype) {
          case simdjson::ondemand::json_type::object: {
            SIMDJSON_ASSIGN_OR_RAISE(singleResultSize, v.count_fields());
            break;
          }
          case simdjson::ondemand::json_type::array: {
            SIMDJSON_ASSIGN_OR_RAISE(singleResultSize, v.count_elements());
            break;
          }
          case simdjson::ondemand::json_type::string:
          case simdjson::ondemand::json_type::number:
          case simdjson::ondemand::json_type::boolean:
          case simdjson::ondemand::json_type::null:
            singleResultSize = 0;
            break;
        }
      }
      return simdjson::SUCCESS;
    };

    SIMDJSON_TRY(simdJsonExtract(json, jsonPath, consumer));

    if (resultCount == 0) {
      // If the path didn't map to anything in the JSON object, return null.
      return simdjson::NO_SUCH_FIELD;
    }

    result = resultCount == 1 ? singleResultSize : resultCount;

    return simdjson::SUCCESS;
  }
};

template <typename T>
struct SonicJsonSizeFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE static bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    sonic_json::Document jsonDoc;

    sonic_json::GenericJsonPointer<SONIC_JSON_POINTER_NODE_STRING_DEFAULT_TYPE>
        jsonPtr;
    if (!jsonpath_to_pointer(jsonPath, jsonPtr)) {
      BOLT_USER_FAIL("Invalid JSON path: {}", jsonPath);
      return false;
    }

    jsonDoc.ParseOnDemand(json.data(), json.size(), jsonPtr);

    if (jsonDoc.HasParseError()) {
      return false;
    }

    if (jsonDoc.IsArray()) {
      result = jsonDoc.Size();
      return true;
    } else if (jsonDoc.IsObject()) {
      result = jsonDoc.Size();
      return true;
    } else if (
        jsonDoc.IsNumber() || jsonDoc.IsString() || jsonDoc.IsBool() ||
        jsonDoc.IsNull()) {
      result = 0;
      return true;
    } else {
      return false;
    }
  }
};

template <typename T>
class WrapperJsonSizeFunction {
 public:
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Json>* json,
      const arg_type<Varchar>* jsonPath) {
    useSonic = config.enableSonicJsonSize();
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    if (useSonic) {
      return SonicJsonSizeFunction<T>::call(result, json, jsonPath);
    } else {
      return SIMDJsonSizeFunction<T>::call(result, json, jsonPath);
    }
  }

  bool useSonic = true;
};

} // namespace bytedance::bolt::functions
