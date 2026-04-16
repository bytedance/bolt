/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/expression/SimpleFunctionAdapter.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/prestosql/json/JsonUtil.h"
#include "bolt/functions/prestosql/json/SIMDJsonExtractor.h"
#include "bolt/functions/prestosql/json/SIMDJsonUtil.h"
#include "bolt/functions/sparksql/VariantEncoding.h"
#include "bolt/type/Variant.h"

namespace bytedance::bolt::functions::sparksql {

template <typename T>
struct ParseJsonFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Variant>& result,
      const arg_type<Varchar>& json) {
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    auto error = parser.parse(json.data(), json.size()).get(doc);
    if (error) {
      return false;
    }

    variant::StringDictionary dict;
    // FIX #20: collectKeys is a lightweight key-only traversal that is much
    // cheaper than full DOM traversal with encoding.  A true single-pass
    // approach would require streaming the dictionary while encoding, which
    // conflicts with Spark's sorted-dictionary requirement.  This two-pass
    // approach is the practical minimum.
    variant::SparkVariantEncoder::collectKeys(doc, dict);
    dict.finalize();

    std::string value;
    try {
      variant::SparkVariantEncoder::encode(doc, dict, value);
    } catch (...) {
      return false;
    }

    result.value.append(value);
    result.metadata.append(dict.serialize());
    return true;
  }
};

namespace detail {

/// Extract a JSON value at `path` from a JSON string using simdjson.
/// Returns the extracted string (unquoted for strings), or std::nullopt on
/// failure.
inline std::optional<std::string> extractJsonAtPath(
    const StringView& jsonValue,
    const StringView& path) {
  if (jsonValue.empty()) {
    return std::nullopt;
  }

  // Root access: return the value directly.
  if (path == "$") {
    if (jsonValue.size() >= 2 && jsonValue.data()[0] == '"' &&
        jsonValue.data()[jsonValue.size() - 1] == '"') {
      return std::string(jsonValue.data() + 1, jsonValue.size() - 2);
    }
    return std::string(jsonValue.data(), jsonValue.size());
  }

  std::optional<std::string> extractedString;

  auto consumer = [&](auto&& extracted) {
    simdjson::ondemand::value val;
    if constexpr (std::is_same_v<
                      std::decay_t<decltype(extracted)>,
                      simdjson::ondemand::document>) {
      auto res = extracted.get_value();
      if (res.error())
        return res.error();
      val = res.value_unsafe();
    } else {
      val = extracted;
    }

    simdjson::ondemand::json_type type;
    if (val.type().get(type))
      return simdjson::INCORRECT_TYPE;

    if (type == simdjson::ondemand::json_type::string) {
      std::string_view sv;
      if (val.get_string(true).get(sv))
        return simdjson::INCORRECT_TYPE;
      extractedString.emplace(sv);
    } else {
      auto jsonStr = simdjson::to_json_string(val);
      if (jsonStr.error()) {
        return jsonStr.error();
      }
      extractedString.emplace(jsonStr.value());
    }
    return simdjson::SUCCESS;
  };

  simdjson::error_code error;
  try {
    error = simdJsonExtract(jsonValue, path, consumer);
  } catch (const std::exception&) {
    return std::nullopt;
  }

  if (error == simdjson::SCALAR_DOCUMENT_AS_VALUE && path == "$") {
    simdjson::padded_string padded(jsonValue.data(), jsonValue.size());
    simdjson::ondemand::document doc;
    if (simdjsonParse(padded).get(doc) == simdjson::SUCCESS) {
      if (consumer(doc) != simdjson::SUCCESS || !extractedString.has_value()) {
        return std::nullopt;
      }
      return extractedString;
    }
  }

  if (error != simdjson::SUCCESS || !extractedString.has_value()) {
    return std::nullopt;
  }
  return extractedString;
}

/// Unquote a JSON string value: "\"foo\"" → "foo".
inline std::string unquoteJson(const std::string& s) {
  if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
    return s.substr(1, s.size() - 2);
  }
  return s;
}

} // namespace detail

/// FIX #21/N9: Cleaned-up variant_get with a clear strategy hierarchy:
///
/// Strategy 1: Binary navigation (getSubVariant) — fastest, uses Spark format
///             directly without full decode.
/// Strategy 2: Compact binary navigation (getSubVariantCompact) — for compact
///             encoded variants.
/// Strategy 3: Full decode to JSON + simdjson extraction — fallback for
///             any format.
///
/// Each strategy is tried once.  No redundant decode calls.
template <typename T>
struct VariantGetFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Variant>& variant,
      const arg_type<Varchar>& path) {
    if (variant.metadata.empty()) {
      // No metadata: treat value as raw JSON.
      auto extracted = detail::extractJsonAtPath(variant.value, path);
      if (extracted.has_value()) {
        result.append(*extracted);
        return true;
      }
      return false;
    }

    // Has metadata: try Spark binary format strategies.
    auto dictionary =
        variant::SparkVariantReader::parseDictionary(variant.metadata);

    // Strategy 1: Direct binary navigation (Spark bit-coded format).
    auto subValue = variant::SparkVariantReader::getSubVariant(
        variant.value, dictionary, std::string_view(path));

    if (subValue.has_value()) {
      StringView sv = subValue.value();
      // Fast path: if the sub-value is a string primitive, extract directly.
      if (!sv.empty() &&
          (sv.data()[0] & 0x03) == variant::BASIC_TYPE_PRIMITIVE &&
          (sv.data()[0] >> 2) == variant::PRIMITIVE_STRING) {
        if (sv.size() >= 5) {
          uint32_t size;
          memcpy(&size, sv.data() + 1, 4);
          if (sv.size() >= 5 + size) {
            result.append(std::string_view(sv.data() + 5, size));
            return true;
          }
        }
      }
      // Decode the sub-value to JSON.
      auto decoded = variant::SparkVariantReader::decodeWithDict(
          sv, variant.metadata, dictionary);
      if (decoded.has_value() && !decoded->empty()) {
        result.append(detail::unquoteJson(*decoded));
        return true;
      }
    }

    // Strategy 2: Compact binary navigation.
    auto compactResult = variant::SparkVariantReader::getSubVariantCompact(
        variant.value, dictionary, std::string_view(path));
    if (compactResult.has_value() && !compactResult->empty()) {
      result.append(detail::unquoteJson(*compactResult));
      return true;
    }

    // Strategy 3: Full decode to JSON, then simdjson extraction.
    auto decodedJson =
        variant::SparkVariantReader::decode(variant.value, variant.metadata);
    if (decodedJson.has_value()) {
      auto extracted =
          detail::extractJsonAtPath(StringView(*decodedJson), path);
      if (extracted.has_value()) {
        result.append(*extracted);
        return true;
      }
    }

    return false;
  }
};

} // namespace bytedance::bolt::functions::sparksql
