/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <ryu/ryu.h>
#include <charconv>
#include "bolt/functions/prestosql/json/JsonEscape.h"
#include "bolt/functions/prestosql/json/SIMDJsonUtil.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"

namespace bytedance::bolt::functions {

namespace JsonUtil {

using JsonEscape::escape;
using JsonEscape::escapeOutputString;

using domElement = simdjson::dom::element;
using domElementResult = simdjson::simdjson_result<domElement>;
using ondemandElement = simdjson::ondemand::value;
using ondemandElementResult = simdjson::simdjson_result<ondemandElement>;
using domArray = simdjson::dom::array;
using ondemandArray = simdjson::ondemand::array;

template <typename T>
concept JsonElementConcept = std::is_same_v<T, ondemandElement> ||
    std::is_same_v<T, ondemandElementResult> || std::is_same_v<T, domElement> ||
    std::is_same_v<T, domElementResult>;

template <typename T>
concept JsonObjectKeyValuePairConcept =
    std::is_same_v<T, simdjson::ondemand::field> ||
    std::is_same_v<T, simdjson::simdjson_result<simdjson::ondemand::field>> ||
    std::is_same_v<T, simdjson::dom::key_value_pair> ||
    std::is_same_v<T, simdjson::simdjson_result<simdjson::dom::key_value_pair>>;

enum BoltJsonElementType {
  ARRAY = 1,
  OBJECT,
  INT64,
  UINT64,
  INT128,
  DOUBLE,
  STRING,
  BOOL,
  NULL_VALUE,
  UNKNOWN
};

inline BoltJsonElementType toBoltJsonType(ondemandElement& element) {
  using namespace simdjson;
  ondemand::json_type type;
  auto error = element.type().get(type);
  if (error) {
    return BoltJsonElementType::UNKNOWN;
  }
  switch (type) {
    case ondemand::json_type::array:
      return BoltJsonElementType::ARRAY;
    case ondemand::json_type::object:
      return BoltJsonElementType::OBJECT;
    case ondemand::json_type::number: {
      switch (element.get_number_type()) {
        case ondemand::number_type::signed_integer:
          return BoltJsonElementType::INT64;
        case ondemand::number_type::unsigned_integer:
          return BoltJsonElementType::UINT64;
        case ondemand::number_type::floating_point_number:
          return BoltJsonElementType::DOUBLE;
        case ondemand::number_type::big_integer:
          return BoltJsonElementType::INT128;
        default:
          BOLT_UNREACHABLE("Unsupported type {}", (int)type);
      }
    }
    case ondemand::json_type::string:
      return BoltJsonElementType::STRING;
    case ondemand::json_type::boolean:
      return BoltJsonElementType::BOOL;
    case ondemand::json_type::null:
      return BoltJsonElementType::NULL_VALUE;
  }
  return BoltJsonElementType::UNKNOWN;
}

inline BoltJsonElementType toBoltJsonType(ondemandElementResult& element) {
  // element.value() might throw an exception with SIMDJSON TAPE_ERROR and cause
  // the Spark job to fail.
  if (element.error()) {
    return BoltJsonElementType::UNKNOWN;
  }
  return toBoltJsonType(element.value());
}

inline BoltJsonElementType toBoltJsonType(domElement& element) {
  using namespace simdjson;
  switch (element.type()) {
    case dom::element_type::ARRAY:
      return BoltJsonElementType::ARRAY;
    case dom::element_type::OBJECT:
      return BoltJsonElementType::OBJECT;
    case dom::element_type::INT64:
      return BoltJsonElementType::INT64;
    case dom::element_type::UINT64:
      return BoltJsonElementType::UINT64;
    case dom::element_type::DOUBLE:
      return BoltJsonElementType::DOUBLE;
    case dom::element_type::STRING:
      return BoltJsonElementType::STRING;
    case dom::element_type::BOOL:
      return BoltJsonElementType::BOOL;
    case dom::element_type::NULL_VALUE:
      return BoltJsonElementType::NULL_VALUE;
  }
  return BoltJsonElementType::UNKNOWN;
}

inline BoltJsonElementType toBoltJsonType(domElementResult& element) {
  return toBoltJsonType(element.value());
}

template <JsonElementConcept T>
inline auto&& extractValueFromResult(T& element) {
  if constexpr (
      std::is_same_v<T, domElementResult> ||
      std::is_same_v<T, ondemandElementResult>) {
    return element.value();
  }
  if constexpr (
      std::is_same_v<T, domElement> || std::is_same_v<T, ondemandElement>) {
    return element;
  }
}

template <bool escapeEmoji, JsonObjectKeyValuePairConcept T>
inline void getKeyFromKeyValuePair(T& value, std::string& ans) {
  using simdjson::dom::key_value_pair;
  using simdjson::ondemand::field;
  if constexpr (
      std::is_same_v<T, field> ||
      std::is_same_v<T, simdjson::simdjson_result<field>>) {
    // don't need escape ascii, e.g. \" -> \"
    std::string_view sv = value.unescaped_key(false);
    if constexpr (escapeEmoji) {
      escapeOutputString<false>(sv, ans);
    } else {
      ans.append(sv);
    }
  } else if constexpr (
      std::is_same_v<T, key_value_pair> ||
      std::is_same_v<T, simdjson::simdjson_result<key_value_pair>>) {
    escapeOutputString(std::string_view(value.key), ans);
  }
}

template <JsonObjectKeyValuePairConcept T>
inline auto getValueFromKeyValuePair(T& value) {
  using simdjson::dom::key_value_pair;
  using simdjson::ondemand::field;
  if constexpr (
      std::is_same_v<T, field> ||
      std::is_same_v<T, simdjson::simdjson_result<field>>) {
    return value.value();
  } else if constexpr (
      std::is_same_v<T, key_value_pair> ||
      std::is_same_v<T, simdjson::simdjson_result<key_value_pair>>) {
    return value.value;
  }
}

template <JsonElementConcept T>
inline bool
extractElementByKeyFromObject(T& obj, const std::string& token, T& ans) {
  if constexpr (
      std::is_same_v<T, domElement> || std::is_same_v<T, domElementResult>) {
    auto error = obj.get_object()[token].get(ans);
    if (error) {
      return true; // mean error
    }
    return false;
  }
  if constexpr (
      std::is_same_v<T, ondemandElement> ||
      std::is_same_v<T, ondemandElementResult>) {
    bool childHasBeenSet = false;
    for (auto pair : obj.get_object()) {
      std::string_view key = pair.unescaped_key(true);
      if (key == token) {
        auto error = pair.value().get(ans);
        if (error) {
          return true;
        }
        childHasBeenSet = true;
        break;
      }
    }
    return !childHasBeenSet;
  }
  return true;
}

template <bool escapeEmoji, JsonElementConcept T>
inline bool boltToJsonString(
    T element,
    std::string& ans,
    bool isTopLevel,
    uint32_t wildcardCount,
    bool& resultIsNull) {
  using namespace simdjson;
  bool error{false};
  switch (toBoltJsonType(element)) {
    case BoltJsonElementType::ARRAY: {
      ans += "[";
      bool add_comma = false;
      for (auto child : element.get_array()) {
        if (add_comma) {
          ans += ",";
        }
        if (boltToJsonString<escapeEmoji>(
                child,
                ans,
                /*isTopLevel=*/false,
                wildcardCount,
                resultIsNull)) {
          error = true;
        }
        add_comma = true;
      }
      ans += "]";
      break;
    }
    case BoltJsonElementType::OBJECT: {
      ans += "{";
      bool add_comma = false;
      for (auto field : element.get_object()) {
        if (add_comma) {
          ans += ",";
        }
        ans.push_back('\"');
        getKeyFromKeyValuePair<escapeEmoji>(field, ans);
        ans += "\":";
        if (boltToJsonString<escapeEmoji>(
                getValueFromKeyValuePair(field),
                ans,
                /*isTopLevel=*/false,
                wildcardCount,
                resultIsNull)) {
          error = true;
        }
        add_comma = true;
      }
      ans += "}";
      break;
    }
    case BoltJsonElementType::INT64: {
      int64_t res = 0;
      auto errorCode = element.get(res);
      if (UNLIKELY(errorCode)) {
        resultIsNull = true;
        error = true;
        VLOG(100) << "simdjson error: " << errorCode;
      } else {
        char strRes[50];
        // char* bytesEnd = itoa(res, strRes);
        char* bytesEnd = std::to_chars(strRes, strRes + 50, res).ptr;
        ans.append(strRes, static_cast<int32_t>(bytesEnd - strRes));
      }
      break;
    }
    case BoltJsonElementType::UINT64: {
      uint64_t res = 0;
      auto errorCode = element.get(res);
      if (UNLIKELY(errorCode)) {
        resultIsNull = true;
        error = true;
        VLOG(100) << "simdjson error: " << errorCode;
      } else {
        char strRes[50];
        // char* bytesEnd = itoa(res, strRes);
        char* bytesEnd = std::to_chars(strRes, strRes + 50, res).ptr;
        ans.append(strRes, static_cast<int32_t>(bytesEnd - strRes));
      }
      break;
    }
    case BoltJsonElementType::INT128: {
      if constexpr (
          std::is_same_v<T, ondemandElement> ||
          std::is_same_v<T, ondemandElementResult>) {
        std::string_view sv = element.raw_json_token();
        ans.append(sv);
      } else {
        BOLT_UNREACHABLE("Unsupported big integer type in dom mode");
      }
      break;
    }
    case BoltJsonElementType::DOUBLE: {
      double res = 0.0;
      auto errorCode = element.get(res);
      if (UNLIKELY(errorCode)) {
        resultIsNull = true;
        error = true;
        VLOG(100) << "simdjson error: " << errorCode;
      } else {
        auto doublePtr = d2s(res);
        std::string doubleVal(doublePtr);
        free(doublePtr);
        ans.append(doubleVal);
      }
      break;
    }
    case BoltJsonElementType::STRING: {
      if (wildcardCount == 0 && isTopLevel) {
        // need escape ascii, e.g. \" -> "
        std::string_view sv;
        if constexpr (
            std::is_same_v<T, ondemandElement> ||
            std::is_same_v<T, ondemandElementResult>) {
          element.get_string(true).get(sv);
        } else {
          sv = std::string_view(element);
        }
        ans.append(sv);
      } else {
        ans.push_back('\"');
        // don't need escape ascii, e.g. \" -> \"
        std::string_view sv;
        if constexpr (
            std::is_same_v<T, ondemandElement> ||
            std::is_same_v<T, ondemandElementResult>) {
          element.get_string(false).get(sv);
        } else {
          sv = std::string_view(element);
        }
        if constexpr (escapeEmoji) {
          if constexpr (
              std::is_same_v<T, domElement> ||
              std::is_same_v<T, domElementResult>) {
            escapeOutputString<true>(sv, ans);
          } else {
            escapeOutputString<false>(sv, ans);
          }
        } else {
          ans.append(sv);
        }
        ans.push_back('\"');
      }
      break;
    }
    case BoltJsonElementType::BOOL: {
      ans += (bool(element) ? "true" : "false");
      break;
    }
    case BoltJsonElementType::NULL_VALUE: {
      if (wildcardCount == 0 && isTopLevel) {
        resultIsNull = true;
      } else {
        ans += "null";
      }
      break;
    }
    case BoltJsonElementType::UNKNOWN: {
      resultIsNull = true;
      break;
    }
  }
  return error;
}

// just for from_json UDF, this function takes error_code as the return value,
// and when simdjson encounters parsing failure, it returns error_code instead
// of throwing an exception
template <JsonElementConcept T>
inline simdjson::error_code boltToJsonString(
    T element,
    bool isTopLevel,
    uint32_t wildcardCount,
    std::string& ans) {
  switch (toBoltJsonType(element)) {
    case BoltJsonElementType::ARRAY: {
      ans += "[";
      bool add_comma = false;
      SIMDJSON_ASSIGN_OR_RAISE(auto array, element.get_array());
      for (auto elementResult : array) {
        SIMDJSON_ASSIGN_OR_RAISE(auto child, elementResult);
        if (add_comma) {
          ans += ",";
        }
        SIMDJSON_TRY(
            boltToJsonString(child, /*isTopLevel=*/false, wildcardCount, ans));
        add_comma = true;
      }
      ans += "]";
      break;
    }
    case BoltJsonElementType::OBJECT: {
      ans += "{";
      bool add_comma = false;
      SIMDJSON_ASSIGN_OR_RAISE(auto object, element.get_object());
      for (auto fieldResult : object) {
        SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldResult);
        SIMDJSON_ASSIGN_OR_RAISE(
            std::string_view fieldStr, field.unescaped_key(true));
        if (add_comma) {
          ans += ",";
        }
        ans.push_back('\"');
        ans.append(fieldStr);
        ans += "\":";
        SIMDJSON_TRY(boltToJsonString(
            field.value(),
            /*isTopLevel=*/false,
            wildcardCount,
            ans));
        add_comma = true;
      }
      ans += "}";
      break;
    }
    case BoltJsonElementType::INT64: {
      SIMDJSON_ASSIGN_OR_RAISE(int64_t num, element.get_int64());
      char strRes[50];
      // char* bytesEnd = std::itoa(num, strRes);
      char* bytesEnd = std::to_chars(strRes, strRes + 50, num).ptr;
      ans.append(strRes, static_cast<int32_t>(bytesEnd - strRes));
      break;
    }
    case BoltJsonElementType::UINT64: {
      SIMDJSON_ASSIGN_OR_RAISE(uint64_t num, element.get_uint64());
      char strRes[50];
      // char* bytesEnd = std::itoa(num, strRes);
      char* bytesEnd = std::to_chars(strRes, strRes + 50, num).ptr;
      ans.append(strRes, static_cast<int32_t>(bytesEnd - strRes));
      break;
    }
    case BoltJsonElementType::INT128: {
      std::string_view sv = element.raw_json_token();
      ans.append(sv);
      break;
    }
    case BoltJsonElementType::DOUBLE: {
      SIMDJSON_ASSIGN_OR_RAISE(double num, element.get_double());
      auto doublePtr = d2s(num);
      std::string doubleStr(doublePtr);
      free(doublePtr);
      ans.append(doubleStr);
      break;
    }
    case BoltJsonElementType::STRING: {
      SIMDJSON_ASSIGN_OR_RAISE(std::string_view str, element.get_string(true));
      if (wildcardCount == 0 && isTopLevel) {
        ans.append(str.data(), str.size());
      } else {
        ans.push_back('\"');
        escapeOutputString(str, ans);
        ans.push_back('\"');
      }
      break;
    }
    case BoltJsonElementType::BOOL: {
      SIMDJSON_ASSIGN_OR_RAISE(bool value, element.get_bool());
      ans += (value ? "true" : "false");
      break;
    }
    case BoltJsonElementType::NULL_VALUE: {
      if (wildcardCount == 0 && isTopLevel) {
        return simdjson::INCORRECT_TYPE;
      } else {
        ans += "null";
      }
      break;
    }
    case BoltJsonElementType::UNKNOWN: {
      return simdjson::INCORRECT_TYPE;
      break;
    }
  }
  return simdjson::SUCCESS;
}

template <typename Visitor, JsonElementConcept T>
inline bool traversingByTokens(
    T& element,
    const std::vector<std::string>& tokens,
    int tokenIdx,
    std::string& ans,
    Visitor visitor,
    uint32_t wildcardCount);

template <typename Visitor, JsonElementConcept T>
inline bool traversingArrayByTokens(
    T& array,
    const std::vector<std::string>& tokens,
    int tokenIdx,
    std::string& ans,
    Visitor visitor,
    uint32_t wildcardCount,
    int32_t& returnNums) {
  bool result = false;
  std::vector<std::string> resArr;
  for (auto child : array.get_array()) {
    std::string res;
    result |= traversingByTokens(
        extractValueFromResult(child),
        tokens,
        tokenIdx + 1,
        res,
        visitor,
        /*wildcardCount=*/wildcardCount + 1);
    if (!res.empty()) {
      resArr.emplace_back(res);
    }
  }
  if (!result) {
    return false;
  }
  returnNums = resArr.size();
  if (returnNums == 1) {
    ans = std::move(resArr[0]);
  } else {
    bool addComma = false;
    for (const auto& s : resArr) {
      if (addComma) {
        ans += ",";
      }
      ans += s;
      addComma = true;
    }
  }
  return result;
}

template <typename Visitor, JsonElementConcept T>
inline bool traversingByTokens(
    T& element,
    const std::vector<std::string>& tokens,
    int tokenIdx,
    std::string& ans,
    Visitor visitor,
    uint32_t wildcardCount) {
  using namespace simdjson;
  if (tokens.size() == tokenIdx) {
    bool resultIsNull = false;
    visitor(element, ans, /*isTopLevel=*/true, wildcardCount, resultIsNull);
    return !resultIsNull;
  }
  switch (toBoltJsonType(element)) {
    case BoltJsonElementType::ARRAY: {
      if (tokens[tokenIdx] == "*") {
        int32_t returnNums = 0;
        std::string tempAns;
        bool result = traversingArrayByTokens(
            element,
            tokens,
            tokenIdx,
            tempAns,
            visitor,
            wildcardCount,
            returnNums);
        if (!result) {
          return result;
        }
        if (returnNums == 0) {
          return false;
        }
        if (returnNums > 1 || wildcardCount >= 1) {
          ans += '[';
          ans += tempAns;
          ans += ']';
        } else {
          ans += tempAns;
        }
        return result;
      } else {
        int32_t arrayIdx;
        try {
          arrayIdx = std::stoi(tokens[tokenIdx]);
        } catch (...) {
          return false;
        }
        T childElement;
        auto error = element.get_array().at(arrayIdx).get(childElement);
        if (error) {
          return wildcardCount >= 1;
        }
        return traversingByTokens(
            childElement, tokens, tokenIdx + 1, ans, visitor, wildcardCount);
      }
    }
    case BoltJsonElementType::OBJECT: {
      T childElement;
      auto error = extractElementByKeyFromObject(
          element, tokens[tokenIdx], childElement);
      if (error) {
        return wildcardCount >= 1;
      }
      if (toBoltJsonType(childElement) == NULL_VALUE) {
        return false;
      }
      return traversingByTokens(
          childElement, tokens, tokenIdx + 1, ans, visitor, wildcardCount);
    }
    case BoltJsonElementType::UNKNOWN: {
      ans.clear();
      return false;
    }
    default: {
      // Can't apply JSONPath selector on simple type
      ans.clear();
      return false;
    }
  }
}

} // namespace JsonUtil
} // namespace bytedance::bolt::functions
