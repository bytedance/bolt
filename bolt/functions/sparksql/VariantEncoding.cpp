/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/functions/sparksql/VariantEncoding.h"

#include <fmt/core.h>
#include <algorithm>
#include <cstring>
#include "bolt/common/encode/Base64.h"
#include "bolt/common/memory/ByteStream.h"
#include "bolt/functions/prestosql/json/JsonUtil.h"
#include "folly/Conv.h"
#include "simdjson.h"

namespace bytedance::bolt::functions::sparksql::variant {

// ============================================================================
// StringDictionary
// ============================================================================

void StringDictionary::add(std::string_view str) {
  if (map_.find(std::string(str)) != map_.end()) {
    return;
  }
  map_[std::string(str)] = 0;
  list_.emplace_back(str);
}

void StringDictionary::finalize() {
  sortedList_ = list_;
  std::sort(sortedList_.begin(), sortedList_.end());
  sortedMap_.clear();
  for (uint32_t i = 0; i < sortedList_.size(); ++i) {
    sortedMap_[sortedList_[i]] = i;
  }
}

std::string StringDictionary::serialize() const {
  std::string result;
  result.push_back(VERSION);

  uint32_t n = sortedList_.size();
  result.append(reinterpret_cast<const char*>(&n), 4);

  uint32_t offset = 0;
  for (const auto& s : sortedList_) {
    result.append(reinterpret_cast<const char*>(&offset), 4);
    offset += s.size();
  }
  result.append(reinterpret_cast<const char*>(&offset), 4);

  for (const auto& s : sortedList_) {
    result.append(s);
  }
  return result;
}

uint32_t StringDictionary::getSortedId(std::string_view str) const {
  auto it = sortedMap_.find(std::string(str));
  if (it != sortedMap_.end()) {
    return it->second;
  }
  return kInvalidDictionaryId;
}

// ============================================================================
// SparkVariantEncoder
// ============================================================================

void SparkVariantEncoder::collectKeys(
    simdjson::dom::element ele,
    StringDictionary& dict) {
  switch (ele.type()) {
    case simdjson::dom::element_type::OBJECT:
      for (auto field : ele.get_object()) {
        dict.add(field.key);
        collectKeys(field.value, dict);
      }
      break;
    case simdjson::dom::element_type::ARRAY:
      for (auto child : ele.get_array()) {
        collectKeys(child, dict);
      }
      break;
    default:
      break;
  }
}

void SparkVariantEncoder::encode(
    simdjson::dom::element val,
    const StringDictionary& dict,
    std::string& out) {
  switch (val.type()) {
    case simdjson::dom::element_type::NULL_VALUE:
      out.push_back(PRIMITIVE_NULL << 2 | BASIC_TYPE_PRIMITIVE);
      break;
    case simdjson::dom::element_type::BOOL:
      if (val.get<bool>().value_unsafe()) {
        out.push_back(PRIMITIVE_TRUE << 2 | BASIC_TYPE_PRIMITIVE);
      } else {
        out.push_back(PRIMITIVE_FALSE << 2 | BASIC_TYPE_PRIMITIVE);
      }
      break;
    case simdjson::dom::element_type::INT64: {
      int64_t i = val.get<int64_t>().value_unsafe();
      if (i >= std::numeric_limits<int8_t>::min() &&
          i <= std::numeric_limits<int8_t>::max()) {
        out.push_back(PRIMITIVE_INT1 << 2 | BASIC_TYPE_PRIMITIVE);
        int8_t v = static_cast<int8_t>(i);
        out.append(reinterpret_cast<const char*>(&v), 1);
      } else if (
          i >= std::numeric_limits<int16_t>::min() &&
          i <= std::numeric_limits<int16_t>::max()) {
        out.push_back(PRIMITIVE_INT2 << 2 | BASIC_TYPE_PRIMITIVE);
        int16_t v = static_cast<int16_t>(i);
        out.append(reinterpret_cast<const char*>(&v), 2);
      } else if (
          i >= std::numeric_limits<int32_t>::min() &&
          i <= std::numeric_limits<int32_t>::max()) {
        out.push_back(PRIMITIVE_INT4 << 2 | BASIC_TYPE_PRIMITIVE);
        int32_t v = static_cast<int32_t>(i);
        out.append(reinterpret_cast<const char*>(&v), 4);
      } else {
        out.push_back(PRIMITIVE_INT8 << 2 | BASIC_TYPE_PRIMITIVE);
        out.append(reinterpret_cast<const char*>(&i), 8);
      }
      break;
    }
    case simdjson::dom::element_type::UINT64: {
      uint64_t u = val.get<uint64_t>().value_unsafe();
      if (u <= (uint64_t)std::numeric_limits<int64_t>::max()) {
        int64_t i = static_cast<int64_t>(u);
        if (i <= std::numeric_limits<int8_t>::max()) {
          out.push_back(PRIMITIVE_INT1 << 2 | BASIC_TYPE_PRIMITIVE);
          int8_t v = static_cast<int8_t>(i);
          out.append(reinterpret_cast<const char*>(&v), 1);
        } else if (i <= std::numeric_limits<int16_t>::max()) {
          out.push_back(PRIMITIVE_INT2 << 2 | BASIC_TYPE_PRIMITIVE);
          int16_t v = static_cast<int16_t>(i);
          out.append(reinterpret_cast<const char*>(&v), 2);
        } else if (i <= std::numeric_limits<int32_t>::max()) {
          out.push_back(PRIMITIVE_INT4 << 2 | BASIC_TYPE_PRIMITIVE);
          int32_t v = static_cast<int32_t>(i);
          out.append(reinterpret_cast<const char*>(&v), 4);
        } else {
          out.push_back(PRIMITIVE_INT8 << 2 | BASIC_TYPE_PRIMITIVE);
          out.append(reinterpret_cast<const char*>(&i), 8);
        }
      } else {
        double d = static_cast<double>(u);
        out.push_back(PRIMITIVE_DOUBLE << 2 | BASIC_TYPE_PRIMITIVE);
        out.append(reinterpret_cast<const char*>(&d), 8);
      }
      break;
    }
    case simdjson::dom::element_type::DOUBLE: {
      double d = val.get<double>().value_unsafe();
      out.push_back(PRIMITIVE_DOUBLE << 2 | BASIC_TYPE_PRIMITIVE);
      out.append(reinterpret_cast<const char*>(&d), 8);
      break;
    }
    case simdjson::dom::element_type::STRING: {
      std::string_view s = val.get<std::string_view>().value_unsafe();
      out.push_back(PRIMITIVE_STRING << 2 | BASIC_TYPE_PRIMITIVE);
      uint32_t size = s.size();
      out.append(reinterpret_cast<const char*>(&size), 4);
      out.append(s);
      break;
    }
    case simdjson::dom::element_type::ARRAY: {
      auto arr = val.get_array();
      std::vector<std::string> elements;
      for (auto element : arr) {
        std::string encoded;
        encode(element, dict, encoded);
        elements.push_back(std::move(encoded));
      }
      out.push_back(2 << 2 | BASIC_TYPE_ARRAY);
      uint32_t numElements = elements.size();
      out.append(reinterpret_cast<const char*>(&numElements), 4);

      uint32_t currentOffset = 0;
      for (const auto& e : elements) {
        out.append(reinterpret_cast<const char*>(&currentOffset), 4);
        currentOffset += e.size();
      }
      out.append(reinterpret_cast<const char*>(&currentOffset), 4);
      for (const auto& e : elements) {
        out.append(e);
      }
      break;
    }
    case simdjson::dom::element_type::OBJECT: {
      auto obj = val.get_object();
      struct Field {
        uint32_t id;
        std::string encodedValue;
      };
      std::vector<Field> fields;
      for (auto field : obj) {
        std::string encoded;
        encode(field.value, dict, encoded);
        uint32_t id = dict.getSortedId(field.key);
        if (id == kInvalidDictionaryId) {
          throw std::runtime_error(
              "Key not found in dictionary: " + std::string(field.key));
        }
        fields.push_back({id, std::move(encoded)});
      }

      std::sort(
          fields.begin(), fields.end(), [](const Field& a, const Field& b) {
            return a.id < b.id;
          });

      out.push_back(2 << 4 | 2 << 2 | BASIC_TYPE_OBJECT);
      uint32_t numFields = fields.size();
      out.append(reinterpret_cast<const char*>(&numFields), 4);

      for (const auto& f : fields) {
        out.append(reinterpret_cast<const char*>(&f.id), 4);
      }

      uint32_t currentOffset = 0;
      for (const auto& f : fields) {
        out.append(reinterpret_cast<const char*>(&currentOffset), 4);
        currentOffset += f.encodedValue.size();
      }
      out.append(reinterpret_cast<const char*>(&currentOffset), 4);

      for (const auto& f : fields) {
        out.append(f.encodedValue);
      }
      break;
    }
  }
}

// ============================================================================
// SparkVariantReader — helpers
// ============================================================================

template <typename T>
std::string SparkVariantReader::formatDecimal(T raw, uint8_t scale) {
  bool neg = raw < 0;
  using UnsignedT = std::make_unsigned_t<T>;
  UnsignedT absVal = neg
      ? static_cast<UnsignedT>(0) - static_cast<UnsignedT>(raw)
      : static_cast<UnsignedT>(raw);
  std::string digits = std::to_string(absVal);
  if (scale == 0) {
    return neg ? "-" + digits : digits;
  }
  while (digits.size() <= scale) {
    digits.insert(digits.begin(), '0');
  }
  digits.insert(digits.size() - scale, 1, '.');
  return neg ? "-" + digits : digits;
}

// Explicit instantiations for the types we use.
template std::string SparkVariantReader::formatDecimal<int32_t>(
    int32_t,
    uint8_t);
template std::string SparkVariantReader::formatDecimal<int64_t>(
    int64_t,
    uint8_t);

std::optional<size_t> SparkVariantReader::widthFromCode(uint8_t code) {
  if (code <= 2) {
    return static_cast<size_t>(1u) << code;
  }
  return std::nullopt;
}

uint32_t SparkVariantReader::readUIntLE(const char* p, size_t width) {
  if (width == 1) {
    return static_cast<uint8_t>(*p);
  }
  if (width == 2) {
    uint16_t v;
    memcpy(&v, p, 2);
    return static_cast<uint32_t>(v);
  }
  uint32_t v;
  memcpy(&v, p, 4);
  return v;
}

// ============================================================================
// SparkVariantReader — format detection
// ============================================================================

SparkVariantReader::Format SparkVariantReader::detectFormat(
    StringView value,
    StringView metadata) {
  if (value.empty()) {
    return Format::UNKNOWN;
  }
  uint8_t header = static_cast<uint8_t>(value.data()[0]);
  uint8_t basicType = header & 0x03;

  // Standard Spark bit-coded format: basic type 0-2 with valid metadata.
  if (!metadata.empty() && basicType <= BASIC_TYPE_ARRAY) {
    if (basicType == BASIC_TYPE_PRIMITIVE) {
      uint8_t subtype = header >> 2;
      // Valid Spark primitive subtypes are 0-16 (with gaps).
      if (subtype <= PRIMITIVE_STRING) {
        return Format::SPARK_BITCODED;
      }
    } else {
      // OBJECT or ARRAY basic type with metadata → bit-coded.
      return Format::SPARK_BITCODED;
    }
  }

  // Bit-coded primitives without metadata (standalone primitive values).
  // Check this BEFORE compact detection to avoid ambiguity: a standalone
  // primitive has basicType == 0, while compact tags use different bit
  // patterns.
  if (basicType == BASIC_TYPE_PRIMITIVE && metadata.empty()) {
    uint8_t subtype = header >> 2;
    if (subtype <= PRIMITIVE_STRING) {
      return Format::SPARK_BITCODED;
    }
  }

  // Compact format markers:
  //   0x02 = compact object, 0x03 = compact array,
  //   (low 2 bits == 1) = compact inline string,
  //   0x20 = compact decimal.
  // Only check these if we haven't already matched SPARK_BITCODED above.
  if (header == 0x02 || header == 0x03 || (header & 0x03) == 0x01 ||
      header == 0x20) {
    return Format::COMPACT;
  }

  // Try raw JSON detection.
  char first = value.data()[0];
  if (first == '{' || first == '[' || first == '"' || first == 't' ||
      first == 'f' || first == 'n' || first == '-' ||
      (first >= '0' && first <= '9')) {
    return Format::RAW_JSON;
  }

  return Format::UNKNOWN;
}

// ============================================================================
// SparkVariantReader — bit-coded decode
// ============================================================================

std::optional<std::string> SparkVariantReader::decodeImpl(
    StringView value,
    StringView metadata,
    const std::vector<std::string>& dictionary) {
  if (value.empty())
    return std::nullopt;

  uint8_t header = value.data()[0];
  uint8_t basicType = header & 0x03;

  if (basicType == BASIC_TYPE_PRIMITIVE) {
    uint8_t primitiveType = header >> 2;
    switch (primitiveType) {
      case PRIMITIVE_NULL:
        return "null";
      case PRIMITIVE_TRUE:
        return "true";
      case PRIMITIVE_FALSE:
        return "false";
      case PRIMITIVE_INT1: {
        if (value.size() < 2)
          return std::nullopt;
        int8_t v;
        memcpy(&v, value.data() + 1, 1);
        return std::to_string(v);
      }
      case PRIMITIVE_INT2: {
        if (value.size() < 3)
          return std::nullopt;
        int16_t v;
        memcpy(&v, value.data() + 1, 2);
        return std::to_string(v);
      }
      case PRIMITIVE_INT4: {
        if (value.size() < 5)
          return std::nullopt;
        int32_t v;
        memcpy(&v, value.data() + 1, 4);
        return std::to_string(v);
      }
      case PRIMITIVE_INT8: {
        if (value.size() < 9)
          return std::nullopt;
        int64_t v;
        memcpy(&v, value.data() + 1, 8);
        return std::to_string(v);
      }
      case PRIMITIVE_DOUBLE: {
        if (value.size() < 9)
          return std::nullopt;
        double v;
        memcpy(&v, value.data() + 1, 8);
        return fmt::format("{}", v);
      }
      case PRIMITIVE_FLOAT: {
        if (value.size() < 5)
          return std::nullopt;
        float v;
        memcpy(&v, value.data() + 1, 4);
        return fmt::format("{}", v);
      }
      case PRIMITIVE_DECIMAL4: {
        if (value.size() < 6)
          return std::nullopt;
        uint8_t scale = static_cast<uint8_t>(value.data()[1]);
        int32_t raw;
        memcpy(&raw, value.data() + 2, 4);
        return formatDecimal(raw, scale);
      }
      case PRIMITIVE_DECIMAL8: {
        if (value.size() < 10)
          return std::nullopt;
        uint8_t scale = static_cast<uint8_t>(value.data()[1]);
        int64_t raw;
        memcpy(&raw, value.data() + 2, 8);
        return formatDecimal(raw, scale);
      }
      case PRIMITIVE_DECIMAL16: {
        if (value.size() < 18)
          return std::nullopt;
        uint8_t scale = static_cast<uint8_t>(value.data()[1]);
        int64_t lo;
        int64_t hi;
        memcpy(&lo, value.data() + 2, 8);
        memcpy(&hi, value.data() + 10, 8);
        if (hi == 0) {
          return formatDecimal(lo, scale);
        } else if (hi == -1 && lo < 0) {
          return formatDecimal(lo, scale);
        }
        __int128 val =
            static_cast<__int128>(hi) << 64 | static_cast<uint64_t>(lo);
        bool neg = val < 0;
        if (neg)
          val = -val;
        std::string digits;
        if (val == 0) {
          digits = "0";
        } else {
          while (val > 0) {
            digits.push_back('0' + static_cast<int>(val % 10));
            val /= 10;
          }
          std::reverse(digits.begin(), digits.end());
        }
        if (scale == 0) {
          return neg ? "-" + digits : digits;
        }
        while (digits.size() <= scale) {
          digits.insert(digits.begin(), '0');
        }
        digits.insert(digits.size() - scale, 1, '.');
        return neg ? "-" + digits : digits;
      }
      case PRIMITIVE_STRING: {
        if (value.size() < 5)
          return std::nullopt;
        uint32_t size;
        memcpy(&size, value.data() + 1, 4);
        if (value.size() < 5 + size)
          return std::nullopt;
        std::string raw(value.data() + 5, size);
        std::string escaped = "\"";
        JsonEscape::escapeOutputString<true>(raw, escaped);
        escaped += "\"";
        return escaped;
      }
      case PRIMITIVE_BINARY: {
        if (value.size() < 5)
          return std::nullopt;
        uint32_t size;
        memcpy(&size, value.data() + 1, 4);
        if (value.size() < 5 + size)
          return std::nullopt;
        return "\"" +
            bytedance::bolt::encoding::Base64::encode(value.data() + 5, size) +
            "\"";
      }
      default:
        return std::nullopt;
    }
  } else if (basicType == BASIC_TYPE_ARRAY) {
    const auto offsetWidth = widthFromCode((header >> 2) & 0x03);
    if (!offsetWidth.has_value()) {
      return std::nullopt;
    }
    if (value.size() < 5)
      return std::nullopt;
    uint32_t numElements;
    memcpy(&numElements, value.data() + 1, 4);

    const size_t offsetsBytes =
        (static_cast<size_t>(numElements) + 1) * *offsetWidth;
    if (value.size() < 5 + offsetsBytes)
      return std::nullopt;

    std::string res = "[";
    const char* offsetsPtr = value.data() + 5;
    const char* dataPtr = offsetsPtr + offsetsBytes;
    for (uint32_t i = 0; i < numElements; ++i) {
      uint32_t start =
          readUIntLE(offsetsPtr + i * (*offsetWidth), *offsetWidth);
      uint32_t end =
          readUIntLE(offsetsPtr + (i + 1) * (*offsetWidth), *offsetWidth);
      if (end < start || dataPtr + end > value.data() + value.size())
        return std::nullopt;
      auto elemResult = decodeImpl(
          StringView(dataPtr + start, end - start), metadata, dictionary);
      if (!elemResult.has_value()) {
        return std::nullopt;
      }
      res += *elemResult;
      if (i < numElements - 1)
        res += ",";
    }
    res += "]";
    return res;
  } else if (basicType == BASIC_TYPE_OBJECT) {
    const auto offsetWidth = widthFromCode((header >> 2) & 0x03);
    const auto idWidth = widthFromCode((header >> 4) & 0x03);
    if (!offsetWidth.has_value() || !idWidth.has_value()) {
      return std::nullopt;
    }
    if (value.size() < 5)
      return std::nullopt;
    uint32_t numFields;
    memcpy(&numFields, value.data() + 1, 4);

    const size_t idsBytes = static_cast<size_t>(numFields) * *idWidth;
    const size_t offsetsBytes =
        (static_cast<size_t>(numFields) + 1) * *offsetWidth;
    if (value.size() < 5 + idsBytes + offsetsBytes)
      return std::nullopt;

    std::string res = "{";
    const char* idsPtr = value.data() + 5;
    const char* offsetsPtr = idsPtr + idsBytes;
    const char* dataPtr = offsetsPtr + offsetsBytes;

    for (uint32_t i = 0; i < numFields; ++i) {
      uint32_t id = readUIntLE(idsPtr + i * (*idWidth), *idWidth);
      std::string key = (id < dictionary.size())
          ? dictionary[id]
          : ("key_" + std::to_string(id));
      std::string escapedKey = "\"";
      JsonEscape::escapeOutputString<true>(key, escapedKey);
      escapedKey += "\"";
      res += escapedKey + ":";

      uint32_t start =
          readUIntLE(offsetsPtr + i * (*offsetWidth), *offsetWidth);
      uint32_t end =
          readUIntLE(offsetsPtr + (i + 1) * (*offsetWidth), *offsetWidth);
      if (end < start || dataPtr + end > value.data() + value.size())
        return std::nullopt;
      auto elemResult = decodeImpl(
          StringView(dataPtr + start, end - start), metadata, dictionary);
      if (!elemResult.has_value()) {
        return std::nullopt;
      }
      res += *elemResult;
      if (i < numFields - 1)
        res += ",";
    }
    res += "}";
    return res;
  }
  return std::nullopt;
}

// ============================================================================
// SparkVariantReader — compact decode
// ============================================================================

std::vector<uint32_t> SparkVariantReader::computeCompactEndOffsets(
    const char* offsetsPtr,
    uint8_t num,
    uint32_t total) {
  std::vector<uint32_t> sorted;
  sorted.reserve(num + 1);
  for (uint32_t i = 0; i < num; ++i) {
    sorted.push_back(static_cast<uint8_t>(offsetsPtr[i]));
  }
  sorted.push_back(total);
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

  std::vector<uint32_t> ends(num);
  for (uint32_t i = 0; i < num; ++i) {
    uint32_t start = static_cast<uint8_t>(offsetsPtr[i]);
    auto it = std::upper_bound(sorted.begin(), sorted.end(), start);
    ends[i] = (it != sorted.end()) ? *it : total;
  }
  return ends;
}

std::optional<std::string> SparkVariantReader::decodeCompactValue(
    StringView value,
    const std::vector<std::string>& dictionary) {
  if (value.size() < 1) {
    return std::nullopt;
  }
  auto tag = static_cast<uint8_t>(value.data()[0]);
  if (tag == 0x00) {
    return "null";
  }
  // Compact string: low 2 bits == 1, length stored in upper 6 bits.
  if ((tag & 0x03) == 0x01) {
    const uint32_t len = static_cast<uint32_t>(tag >> 2);
    if (value.size() < 1 + len) {
      return std::nullopt;
    }
    std::string raw(value.data() + 1, len);
    std::string escaped = "\"";
    JsonEscape::escapeOutputString<true>(raw, escaped);
    escaped += "\"";
    return escaped;
  }
  if (tag == 0x20) {
    if (value.size() < 6) {
      return std::nullopt;
    }
    uint8_t scale = static_cast<uint8_t>(value.data()[1]);
    int32_t raw;
    memcpy(&raw, value.data() + 2, 4);
    return formatDecimal(raw, scale);
  }

  // Compact containers may embed bit-coded primitive values.
  if ((tag & 0x03) == BASIC_TYPE_PRIMITIVE) {
    return decodeImpl(value, StringView(), dictionary);
  }
  if (tag == 0x03) {
    // Compact array
    if (value.size() < 2) {
      return std::nullopt;
    }
    uint8_t num = static_cast<uint8_t>(value.data()[1]);
    size_t offsetsSize = static_cast<size_t>(num) + 1;
    if (value.size() < 2 + offsetsSize) {
      return std::nullopt;
    }
    const char* offsetsPtr = value.data() + 2;
    const char* dataPtr = offsetsPtr + offsetsSize;
    const uint32_t total = static_cast<uint8_t>(offsetsPtr[num]);
    const size_t dataBytes =
        static_cast<size_t>(value.data() + value.size() - dataPtr);
    if (static_cast<size_t>(total) > dataBytes) {
      return std::nullopt;
    }
    auto ends = computeCompactEndOffsets(offsetsPtr, num, total);
    std::string res = "[";
    for (uint32_t i = 0; i < num; ++i) {
      const uint32_t start = static_cast<uint8_t>(offsetsPtr[i]);
      if (start > total || ends[i] < start) {
        return std::nullopt;
      }
      StringView slice(dataPtr + start, ends[i] - start);
      if (i > 0) {
        res += ",";
      }
      auto elem = decodeCompactValue(slice, dictionary);
      if (!elem.has_value()) {
        return std::nullopt;
      }
      res += *elem;
    }
    res += "]";
    return res;
  }
  if (tag == 0x02) {
    // Compact object
    if (value.size() < 3) {
      return std::nullopt;
    }
    uint8_t num = static_cast<uint8_t>(value.data()[1]);
    if (value.size() < 2 + num + (num + 1)) {
      return std::nullopt;
    }
    const char* idsPtr = value.data() + 2;
    const char* offsetsPtr = idsPtr + num;
    const char* dataPtr = offsetsPtr + (num + 1);
    const uint32_t total = static_cast<uint8_t>(offsetsPtr[num]);
    const size_t dataBytes =
        static_cast<size_t>(value.data() + value.size() - dataPtr);
    if (static_cast<size_t>(total) > dataBytes) {
      return std::nullopt;
    }
    auto ends = computeCompactEndOffsets(offsetsPtr, num, total);
    std::string res = "{";
    for (uint32_t i = 0; i < num; ++i) {
      uint32_t id = static_cast<uint8_t>(idsPtr[i]);
      const uint32_t start = static_cast<uint8_t>(offsetsPtr[i]);
      if (start > total || ends[i] < start) {
        return std::nullopt;
      }
      std::string key =
          id < dictionary.size() ? dictionary[id] : "key_" + std::to_string(id);
      std::string escapedKey = "\"";
      JsonEscape::escapeOutputString<true>(key, escapedKey);
      escapedKey += "\"";
      StringView slice(dataPtr + start, ends[i] - start);
      if (i > 0) {
        res += ",";
      }
      auto elem = decodeCompactValue(slice, dictionary);
      if (!elem.has_value()) {
        return std::nullopt;
      }
      res += escapedKey + ":" + *elem;
    }
    res += "}";
    return res;
  }
  return std::nullopt;
}

// ============================================================================
// SparkVariantReader — dictionary parsing
// ============================================================================

std::vector<std::string> SparkVariantReader::parseDictionary(
    StringView metadata) {
  std::vector<std::string> dictionary;
  if (metadata.size() < 2) {
    return dictionary;
  }
  const char* base = metadata.data();
  uint8_t version = static_cast<uint8_t>(base[0]);
  if (version != VERSION) {
    return dictionary;
  }

  auto parseWithOffsets =
      [&](uint32_t n,
          size_t offsetWidth,
          const char* offsetsPtr) -> std::optional<std::vector<std::string>> {
    const size_t offsetsBytes = (static_cast<size_t>(n) + 1) * offsetWidth;
    if (metadata.size() < 5 + offsetsBytes) {
      return std::nullopt;
    }
    const char* dataPtr = offsetsPtr + offsetsBytes;
    const size_t dataBytes =
        static_cast<size_t>(metadata.data() + metadata.size() - dataPtr);

    std::vector<std::string> out;
    out.reserve(n);

    auto readOffset = [&](uint32_t i) -> uint32_t {
      if (offsetWidth == 4) {
        uint32_t off;
        memcpy(&off, offsetsPtr + i * 4, 4);
        return off;
      }
      if (offsetWidth == 2) {
        uint16_t off;
        memcpy(&off, offsetsPtr + i * 2, 2);
        return static_cast<uint32_t>(off);
      }
      return static_cast<uint8_t>(offsetsPtr[i]);
    };

    for (uint32_t i = 0; i < n; ++i) {
      const uint32_t start = readOffset(i);
      const uint32_t end = readOffset(i + 1);
      if (end < start || static_cast<size_t>(end) > dataBytes) {
        return std::nullopt;
      }
      out.emplace_back(dataPtr + start, end - start);
    }
    return out;
  };

  if (metadata.size() >= 5) {
    uint32_t n;
    memcpy(&n, base + 1, 4);
    const char* offsetsPtr = base + 5;

    for (auto width : {size_t{4}, size_t{2}, size_t{1}}) {
      auto parsed = parseWithOffsets(n, width, offsetsPtr);
      if (parsed.has_value()) {
        return std::move(*parsed);
      }
    }
  }

  uint8_t n8 = static_cast<uint8_t>(base[1]);
  size_t offsetsSize = static_cast<size_t>(n8) + 1;
  if (metadata.size() < 2 + offsetsSize) {
    return dictionary;
  }
  const char* offsetsPtr = base + 2;
  const char* dataPtr = offsetsPtr + offsetsSize;
  for (uint32_t i = 0; i < n8; ++i) {
    uint32_t start = static_cast<uint8_t>(offsetsPtr[i]);
    uint32_t end = static_cast<uint8_t>(offsetsPtr[i + 1]);
    if (end < start || dataPtr + end > metadata.data() + metadata.size()) {
      return std::vector<std::string>();
    }
    dictionary.push_back(std::string(dataPtr + start, end - start));
  }
  return dictionary;
}

bool SparkVariantReader::isValidDictionaryBlob(StringView metadata) {
  if (metadata.size() < 1) {
    return false;
  }
  const char* base = metadata.data();
  if (static_cast<uint8_t>(base[0]) != VERSION) {
    return false;
  }

  auto validateWithOffsets = [&](uint32_t n, size_t offsetWidth) -> bool {
    const size_t offsetsBytes = (static_cast<size_t>(n) + 1) * offsetWidth;
    if (metadata.size() < 5 + offsetsBytes) {
      return false;
    }
    const char* offsetsPtr = base + 5;
    const char* dataPtr = offsetsPtr + offsetsBytes;
    const size_t dataBytes =
        static_cast<size_t>(metadata.data() + metadata.size() - dataPtr);

    auto readOffset = [&](uint32_t i) -> uint32_t {
      if (offsetWidth == 4) {
        uint32_t off;
        memcpy(&off, offsetsPtr + i * 4, 4);
        return off;
      }
      if (offsetWidth == 2) {
        uint16_t off;
        memcpy(&off, offsetsPtr + i * 2, 2);
        return static_cast<uint32_t>(off);
      }
      return static_cast<uint8_t>(offsetsPtr[i]);
    };

    uint32_t prev = 0;
    for (uint32_t i = 0; i <= n; ++i) {
      const uint32_t off = readOffset(i);
      if (off < prev) {
        return false;
      }
      if (static_cast<size_t>(off) > dataBytes) {
        return false;
      }
      prev = off;
    }
    return true;
  };

  if (metadata.size() >= 5) {
    uint32_t n;
    memcpy(&n, base + 1, 4);
    if (validateWithOffsets(n, 4) || validateWithOffsets(n, 2) ||
        validateWithOffsets(n, 1)) {
      return true;
    }
  }

  if (metadata.size() < 2) {
    return false;
  }
  uint8_t n8 = static_cast<uint8_t>(base[1]);
  const size_t offsetsSize = static_cast<size_t>(n8) + 1;
  if (metadata.size() < 2 + offsetsSize) {
    return false;
  }
  const char* offsetsPtr = base + 2;
  const char* dataPtr = offsetsPtr + offsetsSize;
  const size_t dataBytes =
      static_cast<size_t>(metadata.data() + metadata.size() - dataPtr);

  uint32_t prev = 0;
  for (uint32_t i = 0; i <= n8; ++i) {
    uint32_t off = static_cast<uint8_t>(offsetsPtr[i]);
    if (off < prev) {
      return false;
    }
    if (static_cast<size_t>(off) > dataBytes) {
      return false;
    }
    prev = off;
  }
  return true;
}

// ============================================================================
// SparkVariantReader — top-level decode (FIX #5/N6: std::optional)
// ============================================================================

std::optional<std::string> SparkVariantReader::decode(
    StringView value,
    StringView metadata) {
  if (value.empty()) {
    return std::nullopt;
  }

  auto dictionary = parseDictionary(metadata);
  auto format = detectFormat(value, metadata);

  switch (format) {
    case Format::SPARK_BITCODED: {
      auto result = decodeImpl(value, metadata, dictionary);
      if (result.has_value()) {
        return result;
      }
      // Header might match both formats; try compact as fallback.
      auto compactResult = decodeCompactValue(value, dictionary);
      if (compactResult.has_value()) {
        return compactResult;
      }
      return std::nullopt;
    }
    case Format::COMPACT: {
      auto result = decodeCompactValue(value, dictionary);
      if (result.has_value()) {
        return result;
      }
      // Compact tag matched but decode failed — try bit-coded.
      return decodeImpl(value, metadata, dictionary);
    }
    case Format::RAW_JSON: {
      simdjson::dom::parser parser;
      simdjson::dom::element doc;
      if (parser.parse(value.data(), value.size()).get(doc) ==
          simdjson::SUCCESS) {
        return std::string(value.data(), value.size());
      }
      // Invalid JSON — try binary decoders.
      auto result = decodeImpl(value, metadata, dictionary);
      if (result.has_value()) {
        return result;
      }
      return decodeCompactValue(value, dictionary);
    }
    case Format::UNKNOWN: {
      // Best-effort: try all decoders.
      auto result = decodeImpl(value, metadata, dictionary);
      if (result.has_value()) {
        return result;
      }
      return decodeCompactValue(value, dictionary);
    }
  }
  return std::nullopt;
}

std::optional<std::string> SparkVariantReader::decodeWithDict(
    StringView value,
    StringView metadata,
    const std::vector<std::string>& dictionary) {
  return decodeImpl(value, metadata, dictionary);
}

// ============================================================================
// SparkVariantReader — navigation
// ============================================================================

std::optional<StringView> SparkVariantReader::navigateObject(
    StringView value,
    const std::vector<std::string>& dictionary,
    std::string_view key) {
  if (value.empty())
    return std::nullopt;
  uint8_t header = value.data()[0];
  if ((header & 0x03) != BASIC_TYPE_OBJECT)
    return std::nullopt;

  const auto offsetWidth = widthFromCode((header >> 2) & 0x03);
  const auto idWidth = widthFromCode((header >> 4) & 0x03);
  if (!offsetWidth.has_value() || !idWidth.has_value()) {
    return std::nullopt;
  }

  if (value.size() < 5)
    return std::nullopt;
  uint32_t numFields;
  memcpy(&numFields, value.data() + 1, 4);

  const size_t idsBytes = static_cast<size_t>(numFields) * *idWidth;
  const size_t offsetsBytes =
      (static_cast<size_t>(numFields) + 1) * *offsetWidth;
  if (value.size() < 5 + idsBytes + offsetsBytes)
    return std::nullopt;

  const char* idsPtr = value.data() + 5;
  const char* offsetsPtr = idsPtr + idsBytes;
  const char* dataPtr = offsetsPtr + offsetsBytes;

  int32_t targetId = -1;
  for (uint32_t i = 0; i < dictionary.size(); ++i) {
    if (dictionary[i] == key) {
      targetId = static_cast<int32_t>(i);
      break;
    }
  }
  if (targetId == -1)
    return std::nullopt;

  // Binary search for targetId in field IDs.
  uint32_t low = 0, high = numFields;
  while (low < high) {
    uint32_t mid = low + (high - low) / 2;
    uint32_t id = readUIntLE(idsPtr + mid * (*idWidth), *idWidth);
    if (id == (uint32_t)targetId) {
      uint32_t start =
          readUIntLE(offsetsPtr + mid * (*offsetWidth), *offsetWidth);
      uint32_t end =
          readUIntLE(offsetsPtr + (mid + 1) * (*offsetWidth), *offsetWidth);
      if (end < start || dataPtr + end > value.data() + value.size())
        return std::nullopt;
      return StringView(dataPtr + start, end - start);
    }
    if (id < (uint32_t)targetId) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return std::nullopt;
}

std::optional<StringView> SparkVariantReader::navigateArray(
    StringView value,
    uint32_t index) {
  if (value.empty())
    return std::nullopt;
  uint8_t header = value.data()[0];
  if ((header & 0x03) != BASIC_TYPE_ARRAY)
    return std::nullopt;

  const auto offsetWidth = widthFromCode((header >> 2) & 0x03);
  if (!offsetWidth.has_value()) {
    return std::nullopt;
  }

  if (value.size() < 5)
    return std::nullopt;
  uint32_t numElements;
  memcpy(&numElements, value.data() + 1, 4);
  if (index >= numElements)
    return std::nullopt;

  const size_t offsetsBytes =
      (static_cast<size_t>(numElements) + 1) * *offsetWidth;
  if (value.size() < 5 + offsetsBytes)
    return std::nullopt;

  const char* offsetsPtr = value.data() + 5;
  const char* dataPtr = offsetsPtr + offsetsBytes;

  uint32_t start =
      readUIntLE(offsetsPtr + index * (*offsetWidth), *offsetWidth);
  uint32_t end =
      readUIntLE(offsetsPtr + (index + 1) * (*offsetWidth), *offsetWidth);
  if (end < start || dataPtr + end > value.data() + value.size())
    return std::nullopt;
  return StringView(dataPtr + start, end - start);
}

std::optional<StringView> SparkVariantReader::getSubVariant(
    StringView value,
    const std::vector<std::string>& dictionary,
    std::string_view path) {
  if (path == "$") {
    return value;
  }

  if (path.empty() || path[0] != '$') {
    return std::nullopt;
  }

  StringView current = value;
  size_t pos = 1;

  auto parseQuotedKey = [&](char quote) -> std::optional<std::string> {
    std::string key;
    while (pos < path.size()) {
      if (path[pos] == '\\') {
        pos++;
        if (pos >= path.size())
          return std::nullopt;
        key.push_back(path[pos++]);
      } else if (path[pos] == quote) {
        pos++;
        return key;
      } else {
        key.push_back(path[pos++]);
      }
    }
    return std::nullopt;
  };

  while (pos < path.size()) {
    if (path[pos] == '.') {
      pos++;
      if (pos >= path.size())
        return std::nullopt;

      std::string key;
      if (path[pos] == '\'' || path[pos] == '\"') {
        auto res = parseQuotedKey(path[pos++]);
        if (!res.has_value())
          return std::nullopt;
        key = std::move(res.value());
      } else {
        size_t end = path.find_first_of(".[", pos);
        key = std::string(path.substr(pos, end - pos));
        pos = (end == std::string_view::npos) ? path.size() : end;
      }

      if (key.empty())
        return std::nullopt;
      auto res = navigateObject(current, dictionary, key);
      if (!res.has_value())
        return std::nullopt;
      current = res.value();
    } else if (path[pos] == '[') {
      pos++;
      if (pos >= path.size())
        return std::nullopt;

      while (pos < path.size() && std::isspace(path[pos]))
        pos++;

      if (pos < path.size() && (path[pos] == '\'' || path[pos] == '\"')) {
        auto res = parseQuotedKey(path[pos++]);
        if (!res.has_value())
          return std::nullopt;
        std::string key = std::move(res.value());

        while (pos < path.size() && std::isspace(path[pos]))
          pos++;
        if (pos >= path.size() || path[pos] != ']')
          return std::nullopt;
        pos++;

        auto navigateRes = navigateObject(current, dictionary, key);
        if (!navigateRes.has_value())
          return std::nullopt;
        current = navigateRes.value();
      } else {
        size_t end = path.find(']', pos);
        if (end == std::string_view::npos)
          return std::nullopt;
        std::string_view indexStr = path.substr(pos, end - pos);
        while (!indexStr.empty() && std::isspace(indexStr.back()))
          indexStr.remove_suffix(1);
        while (!indexStr.empty() && std::isspace(indexStr.front()))
          indexStr.remove_prefix(1);

        if (indexStr.empty() ||
            !std::all_of(indexStr.begin(), indexStr.end(), ::isdigit)) {
          return std::nullopt;
        }

        uint32_t index = 0;
        try {
          index = std::stoul(std::string(indexStr));
        } catch (...) {
          return std::nullopt;
        }

        pos = end + 1;
        auto res = navigateArray(current, index);
        if (!res.has_value())
          return std::nullopt;
        current = res.value();
      }
    } else {
      return std::nullopt;
    }
  }
  return current;
}

std::optional<std::string> SparkVariantReader::getSubVariantCompact(
    StringView value,
    const std::vector<std::string>& dictionary,
    std::string_view path) {
  if (path.empty() || path[0] != '$') {
    return std::nullopt;
  }
  struct Token {
    bool isIndex;
    std::string_view key;
    int64_t index;
  };
  std::vector<Token> tokens;
  size_t i = 1;
  while (i < path.size()) {
    if (path[i] == '.') {
      ++i;
      size_t start = i;
      while (i < path.size() && path[i] != '.' && path[i] != '[') {
        ++i;
      }
      if (start == i) {
        return std::nullopt;
      }
      tokens.push_back({false, path.substr(start, i - start), 0});
    } else if (path[i] == '[') {
      ++i;
      size_t start = i;
      while (i < path.size() && path[i] != ']') {
        ++i;
      }
      if (i >= path.size()) {
        return std::nullopt;
      }
      auto idxView = path.substr(start, i - start);
      ++i;
      int64_t idx = 0;
      try {
        idx = folly::to<int64_t>(idxView);
      } catch (const std::exception&) {
        return std::nullopt;
      }
      tokens.push_back({true, {}, idx});
    } else {
      return std::nullopt;
    }
  }

  StringView current = value;
  for (const auto& token : tokens) {
    if (current.size() < 2) {
      return std::nullopt;
    }
    auto tag = static_cast<uint8_t>(current.data()[0]);
    if (!token.isIndex) {
      if (tag != 0x02) {
        return std::nullopt;
      }
      uint8_t num = static_cast<uint8_t>(current.data()[1]);
      if (current.size() < 2 + num + (num + 1)) {
        return std::nullopt;
      }
      const char* idsPtr = current.data() + 2;
      const char* offsetsPtr = idsPtr + num;
      const char* dataPtr = offsetsPtr + (num + 1);
      const uint32_t total = static_cast<uint8_t>(offsetsPtr[num]);
      const size_t dataBytes =
          static_cast<size_t>(current.data() + current.size() - dataPtr);
      if (static_cast<size_t>(total) > dataBytes) {
        return std::nullopt;
      }

      // FIX N1: Use computeCompactEndOffsets instead of O(n^2) inner loop.
      auto ends = computeCompactEndOffsets(offsetsPtr, num, total);

      bool found = false;
      for (uint32_t idx = 0; idx < num; ++idx) {
        uint32_t id = static_cast<uint8_t>(idsPtr[idx]);
        std::string_view key = id < dictionary.size()
            ? std::string_view(dictionary[id])
            : std::string_view();
        if (key != token.key) {
          continue;
        }
        const uint32_t start = static_cast<uint8_t>(offsetsPtr[idx]);
        if (start > total || ends[idx] < start) {
          return std::nullopt;
        }
        current = StringView(dataPtr + start, ends[idx] - start);
        found = true;
        break;
      }
      if (!found) {
        return std::nullopt;
      }
    } else {
      if (tag != 0x03) {
        return std::nullopt;
      }
      uint8_t num = static_cast<uint8_t>(current.data()[1]);
      if (token.index < 0 || token.index >= num) {
        return std::nullopt;
      }
      size_t offsetsSize = static_cast<size_t>(num) + 1;
      if (current.size() < 2 + offsetsSize) {
        return std::nullopt;
      }
      const char* offsetsPtr = current.data() + 2;
      const char* dataPtr = offsetsPtr + offsetsSize;
      const uint32_t total = static_cast<uint8_t>(offsetsPtr[num]);
      const size_t dataBytes =
          static_cast<size_t>(current.data() + current.size() - dataPtr);
      if (static_cast<size_t>(total) > dataBytes) {
        return std::nullopt;
      }

      // FIX N1: Use computeCompactEndOffsets instead of O(n^2) inner loop.
      auto ends = computeCompactEndOffsets(offsetsPtr, num, total);

      const uint32_t start = static_cast<uint8_t>(offsetsPtr[token.index]);
      if (start > total || ends[token.index] < start) {
        return std::nullopt;
      }
      current = StringView(dataPtr + start, ends[token.index] - start);
    }
  }
  return decodeCompactValue(current, dictionary);
}

} // namespace bytedance::bolt::functions::sparksql::variant
