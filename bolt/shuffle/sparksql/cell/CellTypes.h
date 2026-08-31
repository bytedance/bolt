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

/// Shared vocabulary of the Cell shuffle writer and reader.
///
/// Wire semantics come from bolt/shuffle/sparksql/ColumnarPayloadFormat.md;
/// every constant marked "spec" below mirrors a value that document fixes.
/// Changing one is a wire format change and follows its section 11 rules.

#pragma once

#include <cstdint>
#include <vector>

#include "bolt/type/Type.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Source bytes covered by one full Encoding Block (spec section 7.2).
inline constexpr uint32_t kBlockSourceBytes = 64;

/// Fixed payload header length (spec section 3).
inline constexpr uint32_t kPayloadFixedHeaderBytes = 24;

/// Worst-case serialized size of one Encoding Block: 1 byte EncodingByte,
/// 8 bytes FOR base, 64 bytes body (spec section 7.3).
inline constexpr uint32_t kMaxBlockBytes = 1 + 8 + kBlockSourceBytes;

/// String dictionary limits (spec section 8): an entry's length byte is at
/// most 63, and the serialized entries of one dictionary must stay under 64
/// bytes in total, terminator excluded.
inline constexpr uint32_t kDictEntryMaxLen = 63;
inline constexpr uint32_t kDictSerializedBudget = 63;

/// Dictionary sequence markers ending an entry list (spec section 8):
/// another dictionary follows, or the fallback tail begins.
inline constexpr uint8_t kDictMoreMarker = 0xFE;
inline constexpr uint8_t kDictLastMarker = 0xFF;

/// A closing dictionary segment must have indexed at least this many rows
/// per entry for a successor segment to open; otherwise the partition
/// demotes to the fallback tail (hit rate below 1 - 1/factor is not worth
/// per-row lookups).
inline constexpr uint32_t kDictSegmentContinueFactor = 4;

/// Spec section 7.3, low 2 bits of the EncodingByte.
enum class EncodingKind : uint8_t {
  kConstNarrow = 0,
  kBitPack = 1,
  kForBitPack = 2,
  kPlain = 3,
};

/// Spec section 4.2, 2-bit per-column null tag.
enum class NullTag : uint8_t {
  kAllNull = 0b00,
  kNoNull = 0b01,
  kRawNull = 0b10,
  kReserved = 0b11,
};

/// Spec section 5, Run compression layout byte.
enum class RunLayout : uint8_t {
  kCombined = 0x00,
  kSeparate = 0x01,
  kCombinedStored = 0x02,
};

/// How one stream's bytes are produced and parsed (spec sections 1.4, 7.1).
enum class StreamKind : uint8_t {
  /// Encoding Loop stream: SmallInt/Integer/Bigint/Date value stream, or a
  /// String Length/Index stream (lengths encoded as Bigint).
  kEncoded,
  /// Raw fixed-width value stream: TinyInt/Float/Double.
  kRawFixed,
  /// String Data stream: raw variable-length bytes.
  kStringData,
};

/// One physical stream of the layout (spec section 1.4).
struct CellStream {
  uint16_t column; // logical column index
  StreamKind kind;
  /// Bytes per source value: 1/2/4/8 for value streams, 8 for a String
  /// length stream, 0 for kStringData.
  uint8_t sourceWidth;
  /// Sign-extension rule for encoded streams (spec section 7.3). String
  /// lengths are non-negative so either interpretation matches; they are
  /// flagged signed to match their Bigint definition.
  bool isSigned;
};

/// Maps a shuffle row type (pid column already stripped) onto the spec's
/// stream model. The factory must have rejected unsupported types before a
/// layout is built; create() throws on any it cannot map.
class CellLayout {
 public:
  static bool isSupportedType(const TypePtr& type) {
    switch (type->kind()) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER: // includes DATE (spec type Date)
      case TypeKind::BIGINT: // includes short DECIMAL, mapped to Bigint
      case TypeKind::REAL:
      case TypeKind::DOUBLE:
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return true;
      default:
        return false;
    }
  }

  static bool isSupportedRowType(const RowTypePtr& rowType) {
    for (const auto& child : rowType->children()) {
      if (!isSupportedType(child)) {
        return false;
      }
    }
    return rowType->size() >= 1; // spec section 9: C == 0 is illegal
  }

  static CellLayout create(const RowTypePtr& rowType);

  uint32_t numColumns() const {
    return numColumns_;
  }

  uint32_t numStreams() const {
    return static_cast<uint32_t>(streams_.size());
  }

  const CellStream& stream(uint32_t index) const {
    return streams_[index];
  }

  const std::vector<CellStream>& streams() const {
    return streams_;
  }

  /// First stream of a logical column; a String column owns this stream
  /// (Length/Index) and the next one (Data).
  uint32_t columnStream(uint32_t column) const {
    return columnStream_[column];
  }

  bool isStringColumn(uint32_t column) const {
    return isString_[column];
  }

  const RowTypePtr& rowType() const {
    return rowType_;
  }

 private:
  RowTypePtr rowType_;
  uint32_t numColumns_{0};
  std::vector<CellStream> streams_;
  std::vector<uint32_t> columnStream_;
  std::vector<bool> isString_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
