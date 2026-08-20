/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/shuffle/sparksql/tests/columnar_payload/Format.h"

#include <cstring>
#include <iterator>
#include <limits>

namespace bytedance::bolt::shuffle::sparksql::test {

namespace {

/// One row per physical type, so that adding a type is a single edit instead
/// of six parallel switches. The old switches used `default:`, which silently
/// gave a new type the wrong width, signedness and range; the static_assert
/// and the missing default here turn that into a compile error.
struct TypeInfo {
  PhysicalType type;
  size_t width; // 0 for the variable length kString
  bool encodingLoop;
  bool signedIntegral;
  int64_t min;
  int64_t max;
  const char* name;
};

constexpr int64_t kInt8Min = -128;
constexpr int64_t kInt8Max = 127;
constexpr int64_t kInt16Min = -32768;
constexpr int64_t kInt16Max = 32767;
constexpr int64_t kInt32Min = -2147483648LL;
constexpr int64_t kInt32Max = 2147483647LL;
constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

constexpr TypeInfo kTypes[] = {
    // type                    width loop  signed min        max        name
    {PhysicalType::kTinyInt, 1, false, true, kInt8Min, kInt8Max, "TinyInt"},
    {PhysicalType::kSmallInt, 2, true, true, kInt16Min, kInt16Max, "SmallInt"},
    {PhysicalType::kInteger, 4, true, true, kInt32Min, kInt32Max, "Integer"},
    {PhysicalType::kBigint, 8, true, true, kInt64Min, kInt64Max, "Bigint"},
    {PhysicalType::kDate, 4, true, true, kInt32Min, kInt32Max, "Date"},
    {PhysicalType::kFloat, 4, false, false, 0, 0, "Float"},
    {PhysicalType::kDouble, 8, false, false, 0, 0, "Double"},
    {PhysicalType::kString, 0, false, false, 0, 0, "String"},
};

static_assert(
    std::size(kTypes) == static_cast<size_t>(PhysicalType::kString) + 1,
    "kTypes must carry one row per PhysicalType");

/// info() indexes kTypes by the enumerator's value, which is only sound while
/// every row sits at the index its own type names.
constexpr bool typesAreDense() {
  for (size_t index = 0; index < std::size(kTypes); ++index) {
    if (static_cast<size_t>(kTypes[index].type) != index) {
      return false;
    }
  }
  return true;
}

static_assert(typesAreDense(), "kTypes must be indexed by PhysicalType");

const TypeInfo& info(PhysicalType type) {
  return kTypes[static_cast<size_t>(type)];
}

} // namespace

size_t typeWidth(PhysicalType type) {
  return info(type).width;
}

bool usesEncodingLoop(PhysicalType type) {
  return info(type).encodingLoop;
}

bool isSignedIntegral(PhysicalType type) {
  return info(type).signedIntegral;
}

size_t streamCount(PhysicalType type) {
  return type == PhysicalType::kString ? 2 : 1;
}

int64_t typeMin(PhysicalType type) {
  return info(type).min;
}

int64_t typeMax(PhysicalType type) {
  return info(type).max;
}

const char* toString(PhysicalType type) {
  return info(type).name;
}

const char* toString(EncodingKind kind) {
  switch (kind) {
    case EncodingKind::kConstNarrow:
      return "CONST_NARROW";
    case EncodingKind::kBitPack:
      return "BIT_PACK";
    case EncodingKind::kForBitPack:
      return "FOR_BIT_PACK";
    case EncodingKind::kPlain:
      return "PLAIN";
  }
  return "?";
}

const char* toString(CompressionLayout layout) {
  switch (layout) {
    case CompressionLayout::kCombined:
      return "COMBINED";
    case CompressionLayout::kSeparate:
      return "SEPARATE";
    case CompressionLayout::kCombinedStored:
      return "COMBINED_STORED";
  }
  return "?";
}

size_t FlatColumn::nonNullCount() const {
  size_t count = 0;
  for (size_t row = 0; row < isNull.size(); ++row) {
    if (!isNull[row]) {
      ++count;
    }
  }
  return count;
}

std::vector<PhysicalType> FlatTable::schema() const {
  std::vector<PhysicalType> types;
  types.reserve(columns.size());
  for (const auto& column : columns) {
    types.push_back(column.type);
  }
  return types;
}

FlatTable normalized(const FlatTable& table) {
  FlatTable result = table;
  for (auto& column : result.columns) {
    if (column.type != PhysicalType::kFloat) {
      continue;
    }
    for (auto& value : column.doubleValues) {
      value = static_cast<double>(static_cast<float>(value));
    }
  }
  return result;
}

bool operator==(const FlatColumn& lhs, const FlatColumn& rhs) {
  if (lhs.type != rhs.type || lhs.isNull != rhs.isNull) {
    return false;
  }
  switch (lhs.type) {
    case PhysicalType::kString:
      return lhs.stringValues == rhs.stringValues;
    case PhysicalType::kFloat:
    case PhysicalType::kDouble:
      // Bitwise comparison so that NaN payloads survive the round trip check.
      if (lhs.doubleValues.size() != rhs.doubleValues.size()) {
        return false;
      }
      for (size_t i = 0; i < lhs.doubleValues.size(); ++i) {
        uint64_t left = 0;
        uint64_t right = 0;
        std::memcpy(&left, &lhs.doubleValues[i], sizeof(left));
        std::memcpy(&right, &rhs.doubleValues[i], sizeof(right));
        if (left != right) {
          return false;
        }
      }
      return true;
    default:
      return lhs.intValues == rhs.intValues;
  }
}

bool operator==(const FlatTable& lhs, const FlatTable& rhs) {
  return lhs.rowCount == rhs.rowCount && lhs.columns == rhs.columns;
}

std::vector<uint8_t> IdentityCodec::compress(const uint8_t* data, size_t size) {
  return std::vector<uint8_t>(data, data + size);
}

bool IdentityCodec::decompress(
    const uint8_t* data,
    size_t size,
    size_t decodedSize,
    std::vector<uint8_t>& out) {
  // decodedSize is a buffer sizing hint, not something a codec should judge:
  // comparing it against what came out is the validator's job, and doing it
  // here would steal the rule number from the diagnosis.
  (void)decodedSize;
  out.assign(data, data + size);
  return true;
}

std::vector<uint8_t> MaskCodec::compress(const uint8_t* data, size_t size) {
  std::vector<uint8_t> out(sizeof(uint32_t) + size);
  const auto decodedSize = static_cast<uint32_t>(size);
  std::memcpy(out.data(), &decodedSize, sizeof(decodedSize));
  for (size_t i = 0; i < size; ++i) {
    out[sizeof(uint32_t) + i] = static_cast<uint8_t>(data[i] ^ kMask);
  }
  return out;
}

bool MaskCodec::decompress(
    const uint8_t* data,
    size_t size,
    size_t decodedSize,
    std::vector<uint8_t>& out) {
  if (size < sizeof(uint32_t)) {
    return false;
  }
  (void)decodedSize;
  uint32_t framed = 0;
  std::memcpy(&framed, data, sizeof(framed));
  if (framed != size - sizeof(uint32_t)) {
    return false;
  }
  out.resize(framed);
  for (size_t i = 0; i < framed; ++i) {
    out[i] = static_cast<uint8_t>(data[sizeof(uint32_t) + i] ^ kMask);
  }
  return true;
}

} // namespace bytedance::bolt::shuffle::sparksql::test
