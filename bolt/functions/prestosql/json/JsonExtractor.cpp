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

#include "bolt/functions/prestosql/json/JsonExtractor.h"

#include <string_view>
#include "folly/Optional.h"
#include "folly/String.h"
#include "folly/json.h"

#define SONIC_EXPONENT_ALWAYS_DOT 1
#define SONIC_EXPONENT_UPPERCASE 1
#define SONIC_EXPONENT_ALWAYS_SIGN 0
#define SONIC_UES_EXPONENT 1
#define SONIC_SPARK_FORMAT 1

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/prestosql/json/JsonEscape.h"
#include "bolt/functions/prestosql/json/JsonPathTokenizer.h"
#include "bolt/functions/prestosql/json/JsonUtil.h"
#include "sonic/error.h"
#include "sonic/sonic.h"
namespace bytedance::bolt::functions {

using JsonEscape::escape;
using JsonUtil::boltToJsonString;
using JsonUtil::domElement;
using JsonUtil::ondemandElement;
using JsonUtil::traversingByTokens;

static constexpr double EXPAND_FACTOR{1.5};

namespace {

using JsonVector = std::vector<const folly::dynamic*>;
using SimdJsonVector = std::vector<simdjson::ondemand::value>;

bool isScalarType(const folly::Optional<folly::dynamic>& json) {
  return json.has_value() && !json->isObject() && !json->isArray() &&
      !json->isNull();
}
// can't escape whole json, such as {\n "name": "xxx"} can't be parsed
static void escapeControlCharInJsonStr(
    folly::StringPiece::const_iterator it,
    folly::StringPiece::const_iterator end,
    std::string& ans) {
  while (it != end) {
    while (it != end && *it != '"') {
      ans.push_back(*it++);
    }

    if (it == end) {
      return;
    }
    // append left "
    ans.push_back(*it++);

    if (it == end) {
      return;
    }

    char beforeChar = ' ';
    // stop: beforeChar != '\\' && *it == '"'
    while (it != end && (beforeChar == '\\' || *it != '"')) {
      // deal with corner case like "abc\\"
      beforeChar = (beforeChar == '\\' && *it == '\\') ? ' ' : *it;
      if (*it >= 0 && *it <= 31) {
        ans += escape(it, it + 1, true);
      } else {
        ans.push_back(*it);
      }
      it++;
    }

    if (it == end) {
      return;
    }
    // append right "
    ans.push_back(*it++);

    if (it == end) {
      return;
    }
  }
}
inline void simdJsonPaddingJson(std::string& buffer, folly::StringPiece json) {
  // We can not assume that json is padded with SIMDJSON_PADDING, so
  if (UNLIKELY(json.size() + simdjson::SIMDJSON_PADDING > buffer.capacity())) {
    buffer.reserve(EXPAND_FACTOR * json.size() + simdjson::SIMDJSON_PADDING);
  }

  buffer.clear();
  escapeControlCharInJsonStr(json.begin(), json.end(), buffer);
}
} // namespace

class JsonExtractor {
 public:
  // Use this method to get an instance of JsonExtractor given a json path.
  static JsonExtractor& getInstance(
      folly::StringPiece path,
      bool parsePath = true) {
    // Pre-process
    // Spark does not trim the path passed in, so remove trim to be compatible
    // with Spark
    // auto trimedPath = folly::trimWhitespace(path).str();
    std::string cacheKey = fmt::format("{}:{}", path, parsePath);
    std::shared_ptr<JsonExtractor> op;
    if (kExtractorCache.count(cacheKey)) {
      op = kExtractorCache.at(cacheKey);
    } else {
      if (kExtractorCache.size() == kMaxCacheNum) {
        // TODO: Blindly evict the first one, use better policy
        kExtractorCache.erase(kExtractorCache.begin());
      }
      op = std::make_shared<JsonExtractor>(path, parsePath);
      kExtractorCache[cacheKey] = op;
    }
    return *op;
  }

  [[deprecated("Use simdExtract(const folly::StringPiece json)")]] folly::
      Optional<folly::dynamic>
      extract(const folly::dynamic& json);

  [[nodiscard]] size_t tokensSize() {
    return this->tokens_.size();
  }

  [[nodiscard]] folly::Optional<std::string_view> simdExtract(
      const folly::StringPiece json,
      const bool throwExceptionWhenEncounterBadJson,
      const bool useDOMParserInGetJsonObject,
      const bool getJsonObjectEscapeEmoji,
      const bool useSonicJson) {
    if (!isJsonPathValid_ || json.size() == 0) {
      return folly::none;
    }

    // Because Sonic always escape; sonic is not DOM based; sonic does not throw
    // Actually use sonic iff below is true
    const bool actuallyUseSonic = !throwExceptionWhenEncounterBadJson &&
        !useDOMParserInGetJsonObject && getJsonObjectEscapeEmoji &&
        useSonicJson;
    if (actuallyUseSonic) {
      std::tuple<std::string, sonic_json::SonicError> result{};
      result = sonic_json::GetByJsonPathOnDemand(json.toString(), this->path_);
      const auto sonic_status = std::get<1>(result);
      // If no error and no match, return none.
      if (sonic_json::kErrorNoneNoMatch == sonic_status) {
        return folly::none;
      }
      // No error and has result
      if (sonic_json::kErrorNone == sonic_status) {
        auto answer = std::get<0>(result);
        ans_.clear();
        ans_.append(answer);
        return std::string_view{ans_};
      }
      // There was an error (likely parse invalid json), there may or may
      // not be partial result in buffer.
      auto answer = std::get<0>(result);
      // If no partial result in buffer, return none.
      // Otherwise, return whatever is in buffer to match spark
      // behavior
      if (answer.empty()) {
        return folly::none;
      }
      ans_.clear();
      ans_.append(answer);
      return std::string_view{ans_};
    }

    simdJsonPaddingJson(buffer_, json);

    if (useDOMParserInGetJsonObject) {
      domElement root;
      simdjson::error_code simdErr = simdDOMParser_
                                         .parse(
                                             buffer_.c_str(),
                                             buffer_.size(),
                                             /*realloc_if_needed=*/false)
                                         .get(root);
      ans_.clear();

      if (simdErr) {
        if (throwExceptionWhenEncounterBadJson) {
          std::stringstream ss;
          ss << "Parse json error, detail=" << simdErr;
          BOLT_USER_FAIL(ss.str());
        }
        return folly::none;
      }

      if (root.is_null() && path_ == "$") {
        return std::string_view{"null"};
      }

      // The kGetJsonObjectEscapeEmoji option has no effect on
      // the domElement scene, and regardless of the option, the domElement
      // scene will still escape emoji and unescape ascii
      if (!traversingByTokens(
              root,
              tokens_,
              /*tokenIdx=*/0,
              ans_,
              boltToJsonString<true, domElement>,
              /*wildcardCount=*/0)) {
        return folly::none;
      }

      return std::string_view{ans_};

    } else {
      simdjson::ondemand::document doc;
      auto simdErr = simdParser_
                         .iterate(
                             buffer_.c_str(),
                             buffer_.size(),
                             buffer_.size() + simdjson::SIMDJSON_PADDING)
                         .get(doc);

      if (UNLIKELY(simdErr)) {
        if (throwExceptionWhenEncounterBadJson) {
          std::stringstream ss;
          ss << "Parse json error, detail=" << simdErr;
          BOLT_USER_FAIL(ss.str());
        }
        return folly::none;
      }

      ans_.clear();
      ondemandElement value;
      auto error = doc.get(value);

      if (error) {
        if (error == simdjson::SCALAR_DOCUMENT_AS_VALUE && path_ == "$") {
          return simdExtractScale(doc);
        }
        return folly::none;
      }

      if (!traversingByTokens(
              value,
              tokens_,
              /*tokenIdx=*/0,
              ans_,
              getJsonObjectEscapeEmoji
                  ? boltToJsonString<true, ondemandElement>
                  : boltToJsonString<false, ondemandElement>,
              /*wildcardCount=*/0)) {
        return folly::none;
      }
      return std::string_view{ans_};
    }
  }

  template <bool isOnlyForArrayLen>
  [[nodiscard]] folly::Optional<size_t> simdJsonSize(
      const folly::StringPiece json) {
    simdJsonPaddingJson(buffer_, json);

    simdjson::ondemand::document doc;
    auto simdErr = simdParser_.iterate(buffer_).get(doc);
    if (UNLIKELY(simdErr)) {
      return folly::none;
    }

    simdjson::simdjson_result<simdjson::ondemand::value> queryRes =
        doc.at_pointer(queryPointer_);

    simdjson::ondemand::value val;
    simdErr = queryRes.get(val);

    if (LIKELY(simdErr == simdjson::SUCCESS)) {
      if constexpr (isOnlyForArrayLen) {
        if (val.type() == simdjson::ondemand::json_type::array) {
          return val.count_elements().value();
        }
      } else {
        switch (val.type()) {
          case simdjson::ondemand::json_type::object:
            return val.count_fields().value();
          case simdjson::ondemand::json_type::array:
            return val.count_elements().value();
            break;
          default:
            return 0;
        }
      }
    }
    return folly::none;
  }

  template <typename T>
  [[nodiscard]] folly::Optional<bool> simdJsonArrayContains(
      const folly::StringPiece json,
      const T& t) {
    return false; // TODO
  }

  // Shouldn't instantiate directly - use getInstance().
  explicit JsonExtractor(folly::StringPiece path, bool parsePath = true) {
    path_ = path;
    buffer_.reserve(INIT_CAPACITY);
    if (parsePath) {
      isJsonPathValid_ = tokenize(path);
    } else {
      isJsonPathValid_ = true;
      queryPointer_ = path;
      tokens_.emplace_back(path);
    }
  }

 private:
  bool tokenize(folly::StringPiece path) {
    if (path.empty()) {
      return false;
    }
    if (path == "$") {
      return true;
    }
    if (!kTokenizer.reset(path)) {
      return false;
    }
    queryPointer_.clear();
    tokens_.clear();
    while (kTokenizer.hasNext()) {
      if (auto token = kTokenizer.getNext()) {
        tokens_.emplace_back(token.value());
      } else {
        tokens_.clear();
        return false;
      }
    }
    queryPointer_ = kTokenizer.getSimdjsonPathPointer().value();
    return true;
  }

  [[nodiscard]] static folly::Optional<std::string_view> simdExtractScale(
      simdjson::ondemand::document& value) {
    switch (value.type()) {
      case simdjson::ondemand::json_type::number:
      case simdjson::ondemand::json_type::boolean: {
        std::string_view res;
        auto r = simdjson::to_json_string(value);
        if (simdjson::to_json_string(value).get(res) == simdjson::SUCCESS) {
          return res;
        }
      } break;
      case simdjson::ondemand::json_type::string: {
        std::string_view res;
        if (value.get_string(true).get(res) == simdjson::SUCCESS) {
          return res;
        }
      } break;
      case simdjson::ondemand::json_type::null: {
        return std::string_view{"null"};
      } break;
      default:
        break;
    } // ~switch

    return folly::none;
  }

  // Cache tokenize operations in JsonExtractor across invocations in the same
  // thread for the same JsonPath.
  thread_local static std::
      unordered_map<std::string, std::shared_ptr<JsonExtractor>>
          kExtractorCache;
  thread_local static JsonPathTokenizer kTokenizer;

  // Max extractor number in extractor cache
  static constexpr uint32_t kMaxCacheNum{32};

  std::vector<std::string> tokens_;

  // query path for simdjson
  std::string queryPointer_;

  // pre allocated to avoid frequent allocation.
  // simd parser can expand its capacity if needed.
  static constexpr size_t INIT_CAPACITY{1024};

  // Reuse parser to avoid internal buffer allocation.
  simdjson::ondemand::parser simdParser_;

  simdjson::dom::parser simdDOMParser_;

  bool isJsonPathValid_;

  // Reuse paded buffer
  std::string buffer_;

  std::string path_;

  // store to_json_string result
  std::string ans_;
};

thread_local std::unordered_map<std::string, std::shared_ptr<JsonExtractor>>
    JsonExtractor::kExtractorCache;
thread_local JsonPathTokenizer JsonExtractor::kTokenizer;

[[deprecated("Use simdjson")]] void extractObject(
    const folly::dynamic* jsonObj,
    const std::string& key,
    JsonVector& ret) {
  auto val = jsonObj->get_ptr(key);
  if (val) {
    ret.push_back(val);
  }
}

[[deprecated("Use simdjson")]] void extractArray(
    const folly::dynamic* jsonArray,
    const std::string& key,
    JsonVector& ret) {
  auto arrayLen = jsonArray->size();
  if (key == "*") {
    for (size_t i = 0; i < arrayLen; ++i) {
      ret.push_back(jsonArray->get_ptr(i));
    }
  } else {
    auto rv = folly::tryTo<int32_t>(key);
    if (rv.hasValue()) {
      auto idx = rv.value();
      if (idx >= 0 && idx < arrayLen) {
        ret.push_back(jsonArray->get_ptr(idx));
      }
    }
  }
}

[[deprecated("Use simdjson")]] folly::Optional<folly::dynamic>
JsonExtractor::extract(const folly::dynamic& json) {
  JsonVector input;
  // Temporary extraction result holder, swap with input after
  // each iteration.
  JsonVector result;
  input.push_back(&json);

  for (auto& token : tokens_) {
    for (auto& jsonObj : input) {
      if (jsonObj->isObject()) {
        extractObject(jsonObj, token, result);
      } else if (jsonObj->isArray()) {
        extractArray(jsonObj, token, result);
      }
    }
    if (result.empty()) {
      return folly::none;
    }
    input.clear();
    result.swap(input);
  }

  auto len = input.size();
  if (0 == len) {
    return folly::none;
  } else if (1 == len) {
    return *input.front();
  } else {
    folly::dynamic array = folly::dynamic::array;
    for (auto obj : input) {
      array.push_back(*obj);
    }
    return array;
  }
}

folly::Optional<folly::dynamic> jsonExtract(
    folly::StringPiece json,
    folly::StringPiece path) {
  try {
    // If extractor fails to parse the path, this will throw a BoltUserError,
    // and we want to let this exception bubble up to the client. We only catch
    // json parsing failures (in which cases we return folly::none instead of
    // throw).
    auto& extractor = JsonExtractor::getInstance(path);
    return extractor.extract(folly::parseJson(json));
  } catch (const folly::json::parse_error&) {
  } catch (const folly::ConversionError&) {
    // Folly might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }
  return folly::none;
}

folly::Optional<folly::dynamic> jsonExtract(
    const std::string& json,
    const std::string& path) {
  return jsonExtract(folly::StringPiece(json), folly::StringPiece(path));
}

folly::Optional<folly::dynamic> jsonExtract(
    const folly::dynamic& json,
    folly::StringPiece path) {
  try {
    return JsonExtractor::getInstance(path).extract(json);
  } catch (const folly::json::parse_error&) {
  }
  return folly::none;
}

folly::Optional<std::string_view> jsonExtractScalar(
    folly::StringPiece json,
    folly::StringPiece path,
    const bool throwExceptionWhenEncounterBadJson,
    const bool useDOMParserInGetJsonObject,
    const bool getJsonObjectEscapeEmoji,
    const bool parsePath,
    const bool useSonicPath) {
  // If extractor fails to parse the path, this will throw a BoltUserError,
  // and we want to let this exception bubble up to the client. We only catch
  // json parsing failures (in which cases we return folly::none instead of
  // throw).
  auto& extractor = JsonExtractor::getInstance(path, parsePath);
  try {
    return extractor.simdExtract(
        json,
        throwExceptionWhenEncounterBadJson,
        useDOMParserInGetJsonObject,
        getJsonObjectEscapeEmoji,
        useSonicPath);
  } catch (simdjson::simdjson_error& e) {
    VLOG(100) << "simdjson error: " << e.what();
  }

  return folly::none;
}

// [[deprecated]], for benchmark
folly::Optional<std::string> follyJsonExtractScalar(
    const std::string& json,
    const std::string& path) {
  folly::StringPiece jsonPiece{json};
  folly::StringPiece pathPiece{path};

  auto& extractor = JsonExtractor::getInstance(pathPiece);
  try {
    auto res = extractor.extract(folly::parseJson(jsonPiece));

    if (isScalarType(res)) {
      if (res->isBool()) {
        return res->asBool() ? std::string{"true"} : std::string{"false"};
      } else {
        return res->asString();
      }
    }
  } catch (const folly::json::parse_error&) {
  } catch (const folly::ConversionError&) {
    // Folly might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }
  return folly::none;
}

folly::Optional<std::string_view> jsonExtractScalar(
    const std::string& json,
    const std::string& path,
    const bool throwExceptionWhenEncounterBadJson,
    const bool useDOMParserInGetJsonObject,
    const bool getJsonObjectEscapeEmoji,
    const bool useSonicJson) {
  folly::StringPiece jsonPiece{json};
  folly::StringPiece pathPiece{path};

  return jsonExtractScalar(
      jsonPiece,
      pathPiece,
      throwExceptionWhenEncounterBadJson,
      useDOMParserInGetJsonObject,
      getJsonObjectEscapeEmoji,
      useSonicJson);
}

folly::Optional<size_t> jsonExtractSize(
    folly::StringPiece json,
    folly::StringPiece path) {
  // If extractor fails to parse the path, this will throw a BoltUserError,
  // and we want to let this exception bubble up to the client. We only catch
  // json parsing failures (in which cases we return folly::none instead of
  // throw).
  auto& extractor = JsonExtractor::getInstance(path);

  try {
    return extractor.simdJsonSize<false>(json);
  } catch (simdjson::simdjson_error& e) {
    // e.error()
    // Just follow previous behivor in jsonExtract()
    // simdjson might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }

  return folly::none;
}

folly::Optional<size_t> jsonExtractSize(
    const std::string& json,
    const std::string& path) {
  folly::StringPiece jsonPiece{json};
  folly::StringPiece pathPiece{path};

  return jsonExtractSize(jsonPiece, pathPiece);
}

folly::Optional<size_t> jsonArrayLength(folly::StringPiece json) {
  // If extractor fails to parse the path, this will throw a BoltUserError,
  // and we want to let this exception bubble up to the client. We only catch
  // json parsing failures (in which cases we return folly::none instead of
  // throw).
  auto& extractor = JsonExtractor::getInstance("$");
  try {
    return extractor.simdJsonSize<true>(json);
  } catch (simdjson::simdjson_error& e) {
    // e.error()
    // Just follow previous behivor in jsonExtract()
    // simdjson might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }

  return folly::none;
}

// For JsonArrayContainsFunction
template <typename T>
folly::Optional<bool> jsonArrayContains(folly::StringPiece json, const T& t) {
  // If extractor fails to parse the path, this will throw a BoltUserError,
  // and we want to let this exception bubble up to the client. We only catch
  // json parsing failures (in which cases we return folly::none instead of
  // throw).
  auto& extractor = JsonExtractor::getInstance("$");
  try {
    auto& extractor = JsonExtractor::getInstance("$");
    return extractor.simdJsonArrayContains<T>(json);
  } catch (simdjson::simdjson_error& e) {
    // e.error()
    // Just follow previous behivor in jsonExtract()
    // simdjson might throw a conversion error while parsing the input json. In
    // this case, let it return null.
  }

  return folly::none;
}

std::vector<folly::Optional<std::string>> jsonExtractTupleSonic(
    folly::StringPiece json,
    std::vector<folly::Optional<folly::StringPiece>> paths,
    bool legacy) {
  std::vector<std::string_view> paths_string_view{};
  std::vector<size_t> non_null_index{};
  for (int i = 0; i < paths.size(); i++) {
    if (paths[i].has_value()) {
      non_null_index.push_back(i);
      paths_string_view.push_back(paths[i].value());
    }
  }
  auto result =
      sonic_json::JsonTupleWithCodeGen(json, paths_string_view, legacy);
  std::vector<folly::Optional<std::string>> ret(paths.size(), folly::none);
  for (int i = 0; i < paths_string_view.size(); i++) {
    if (result[i].has_value()) {
      ret[non_null_index[i]] = std::move(result[i].value());
    }
  }
  return ret;
}

std::vector<folly::Optional<std::string>> jsonExtractTuple(
    folly::StringPiece json,
    std::vector<folly::Optional<folly::StringPiece>> paths,
    bool legacy,
    const bool useSonicLibrary) {
  if (LIKELY(useSonicLibrary)) {
    return jsonExtractTupleSonic(std::move(json), std::move(paths), legacy);
  }
  std::vector<folly::Optional<std::string>> ret{paths.size(), folly::none};
  std::string json_str;
  simdJsonPaddingJson(json_str, json);
  simdjson::ondemand::document doc;
  simdjson::ondemand::parser parser;
  auto error = parser.iterate(json_str).get(doc);

  if (UNLIKELY(error)) {
    VLOG(100) << "simdjson error: " << error << " json is: " << json;
    return ret;
  }
  simdjson::ondemand::value value;
  error = doc.get(value);
  if (UNLIKELY(error)) {
    VLOG(100) << "simdjson error: " << error << " json is: " << json;
    return ret;
  }
  std::map<std::string_view, std::vector<size_t>> pathIndex;
  for (size_t i = 0; i < paths.size(); i++) {
    if (paths[i].has_value()) {
      pathIndex[paths[i].value()].push_back(i);
    }
  }
  for (auto&& pair : value.get_object()) {
    if (UNLIKELY(pair.error())) {
      VLOG(100) << "simdjson error: " << pair.error() << " json is: " << json;
      if (legacy) {
        continue;
      } else {
        for (size_t i = 0; i < paths.size(); i++) {
          ret[i] = folly::none;
        }
        return ret;
      }
    }
    auto it = pathIndex.find(pair.unescaped_key(true));
    if (it != pathIndex.end()) {
      simdjson::ondemand::value child;
      error = pair.value().get(child);
      if (error) {
        if (legacy) {
          continue;
        } else {
          for (size_t i = 0; i < paths.size(); i++) {
            ret[i] = folly::none;
          }
          return ret;
        }
      }
      std::string resultValue;
      bool resultIsNull = false;
      bool hasError = boltToJsonString<true, simdjson::ondemand::value>(
          child, resultValue, true, 0, resultIsNull);
      if (hasError && !legacy) {
        for (size_t i = 0; i < paths.size(); i++) {
          ret[i] = folly::none;
        }
        return ret;
      }
      if (!resultIsNull) {
        for (auto i : it->second) {
          ret[i] = folly::make_optional<std::string>(std::move(resultValue));
        }
      } else {
        for (auto i : it->second) {
          ret[i] = folly::none;
        }
      }
    }
  }
  return ret;
}

} // namespace bytedance::bolt::functions
