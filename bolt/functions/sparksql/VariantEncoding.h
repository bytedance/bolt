/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>
#include "bolt/type/StringView.h"

// Forward-declare simdjson types to avoid pulling the full header into every
// TU.
namespace simdjson::dom {
class element;
} // namespace simdjson::dom

namespace bytedance::bolt::functions::sparksql::variant {

// Spark Variant Constants (Spark 4.0)
static constexpr uint8_t VERSION = 1;

// Basic types (bits 0-1 of the header)
static constexpr uint8_t BASIC_TYPE_PRIMITIVE = 0;
static constexpr uint8_t BASIC_TYPE_OBJECT = 1;
static constexpr uint8_t BASIC_TYPE_ARRAY = 2;

// Primitive subtypes (bits 2-5 when basic type is 0)
static constexpr uint8_t PRIMITIVE_NULL = 0;
static constexpr uint8_t PRIMITIVE_TRUE = 1;
static constexpr uint8_t PRIMITIVE_FALSE = 2;
static constexpr uint8_t PRIMITIVE_INT1 = 3;
static constexpr uint8_t PRIMITIVE_INT2 = 4;
static constexpr uint8_t PRIMITIVE_INT4 = 5;
static constexpr uint8_t PRIMITIVE_INT8 = 6;
static constexpr uint8_t PRIMITIVE_DOUBLE = 7;
static constexpr uint8_t PRIMITIVE_DECIMAL4 = 10;
static constexpr uint8_t PRIMITIVE_DECIMAL8 = 11;
static constexpr uint8_t PRIMITIVE_DECIMAL16 = 12;
static constexpr uint8_t PRIMITIVE_FLOAT = 14;
static constexpr uint8_t PRIMITIVE_BINARY = 15;
static constexpr uint8_t PRIMITIVE_STRING = 16;

static constexpr uint32_t kInvalidDictionaryId =
    std::numeric_limits<uint32_t>::max();

/// Collects string keys from a JSON document for the Spark VARIANT dictionary.
class StringDictionary {
 public:
  /// Add a key to the dictionary (deduplicated).
  void add(std::string_view str);

  /// Sort the dictionary and assign IDs.  Must be called before serialize()
  /// or getSortedId().
  void finalize();

  /// Serialize to the Spark 4.0 binary dictionary format.
  std::string serialize() const;

  /// Look up the sorted ID of a key.  Returns kInvalidDictionaryId if absent.
  uint32_t getSortedId(std::string_view str) const;

 private:
  std::vector<std::string> list_;
  std::map<std::string, uint32_t> map_;
  std::vector<std::string> sortedList_;
  std::map<std::string, uint32_t> sortedMap_;
};

/// Encodes simdjson DOM elements into Spark 4.0 VARIANT binary format.
class SparkVariantEncoder {
 public:
  /// Encode a simdjson DOM element into Spark VARIANT binary.
  /// The dictionary must already be finalized.
  static void encode(
      simdjson::dom::element val,
      const StringDictionary& dict,
      std::string& out);

  /// Collect all object keys from a simdjson DOM tree into a dictionary.
  /// Call this before finalize() + encode().
  static void collectKeys(simdjson::dom::element ele, StringDictionary& dict);
};

/// Reads and decodes Spark 4.0 VARIANT binary values.
class SparkVariantReader {
 public:
  /// Encoding formats that a variant value blob might use.
  enum class Format {
    SPARK_BITCODED, // Standard Spark 4.0 bit-coded binary format
    COMPACT, // Compact container format
    RAW_JSON, // Raw JSON string (legacy or non-Spark)
    UNKNOWN, // Cannot determine format
  };

  /// Detect the format of a variant value blob.
  static Format detectFormat(StringView value, StringView metadata);

  /// Decode a variant value+metadata pair to a JSON string.
  /// Returns std::nullopt on decode failure; returns "null" for a valid JSON
  /// null.  This eliminates the sentinel collision where the string "null" was
  /// previously used for both real nulls and error conditions.
  static std::optional<std::string> decode(
      StringView value,
      StringView metadata);

  /// Parse the metadata dictionary blob into a list of strings.
  static std::vector<std::string> parseDictionary(StringView metadata);

  /// Check if a metadata blob looks like a valid Spark VARIANT dictionary.
  static bool isValidDictionaryBlob(StringView metadata);

  /// Navigate to a sub-variant by JSONPath in Spark bit-coded binary format.
  static std::optional<StringView> getSubVariant(
      StringView value,
      const std::vector<std::string>& dictionary,
      std::string_view path);

  /// Navigate to a sub-variant by JSONPath in compact binary format.
  /// Returns the decoded JSON string of the sub-value if found.
  static std::optional<std::string> getSubVariantCompact(
      StringView value,
      const std::vector<std::string>& dictionary,
      std::string_view path);

  /// Decode a value blob using a pre-parsed dictionary.
  static std::optional<std::string> decodeWithDict(
      StringView value,
      StringView metadata,
      const std::vector<std::string>& dictionary);

  // --- Implementation helpers (public for testing) ---

  /// Decode the bit-coded Spark format.  Returns std::nullopt on failure.
  static std::optional<std::string> decodeImpl(
      StringView value,
      StringView metadata,
      const std::vector<std::string>& dictionary);

  /// Decode a compact-format value.  Returns std::nullopt on failure.
  static std::optional<std::string> decodeCompactValue(
      StringView value,
      const std::vector<std::string>& dictionary);

  /// Build sorted end-offsets for compact container elements (O(n log n)).
  static std::vector<uint32_t>
  computeCompactEndOffsets(const char* offsetsPtr, uint8_t num, uint32_t total);

 private:
  template <typename T>
  static std::string formatDecimal(T raw, uint8_t scale);

  static std::optional<size_t> widthFromCode(uint8_t code);
  static uint32_t readUIntLE(const char* p, size_t width);

  static std::optional<StringView> navigateObject(
      StringView value,
      const std::vector<std::string>& dictionary,
      std::string_view key);

  static std::optional<StringView> navigateArray(
      StringView value,
      uint32_t index);
};

} // namespace bytedance::bolt::functions::sparksql::variant
