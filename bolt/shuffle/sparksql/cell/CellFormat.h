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

/// Shared wire format and stream layout of the Cell shuffle writer and reader.
/// This header is the format's definition: every constant and enum below is
/// on the wire, and changing one changes the format for both sides.
///
/// A partition's shuffle bytes are blocks back to back, each opening with a
/// CellBlockKind byte. A ColumnarPayload holds some rows of one partition:
///
///   header      24 bytes: rowCount u32, runCount u32, variableBytes u64 (a
///               preallocation hint, never validated), nullStoredSize u32,
///               nullDecodedSize u32 (0: the null body is stored as is;
///               else it is compressed and decodes to this many bytes)
///   null body   nullStoredSize bytes: one 2-bit NullTag per wire column,
///               then a ceil(rowCount / 8)-byte bitmap (bit 1 = non-null,
///               unused high bits zero) per kRawNull column, in column order
///   enc tags    ceil(W / 8) bytes, W wire columns: bit set = the string
///               column is in dictionary form
///   runs        runCount runs, each: RunLayout u8, stored size(s) u64 (one,
///               or one per stream for kSeparate), decoded size u64 per
///               stream, then the stored bytes
///
/// UNKNOWN columns have no wire column. A wire column's values live in its
/// streams (CellLayout): only non-null rows have values, and an all-null
/// column's streams are empty. A stream is the
/// concatenation of its bytes in every run, in run order; a run may end a
/// raw stream's bytes anywhere, but never inside an Encoding Block or a
/// dictionary. Encoded streams
/// are Encoding Blocks back to back: each covers kBlockSourceBytes of source
/// values (the stream's last block fewer) and opens with an EncodingByte,
/// EncodingKind in the low 2 bits and a parameter in the high 6:
///
///   kConstNarrow  every value equal; body: parameter (1..width) bytes,
///                 sign-extended
///   kBitPack      body: values in parameter (1..min(63, 8 * width)) bits,
///                 LSB-first, sign-extended
///   kForBitPack   body: base (width bytes), then value - base in parameter
///                 (0..63) bits, LSB-first
///   kPlain        parameter 0; body: the values as they are, little endian
///
/// Raw streams hold one fixed-width value per non-null row (a Boolean as
/// one 0/1 byte). A string column has a length stream (encoded, Bigint) and
/// a data stream (the bytes). In dictionary form, the data stream starts
/// with a dictionary sequence - per dictionary, entries of a length byte
/// (<= kDictEntryMaxLen) and the bytes, kDictMoreMarker or kDictLastMarker,
/// then the u32 count of rows it matches - and ends with the raw bytes of
/// the unmatched rows; the length stream holds one index byte per matched
/// row, then the lengths of the unmatched ones. Matched rows come first, in
/// dictionary order.

#pragma once

#include <cstdint>
#include <vector>

#include "bolt/type/Type.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Source bytes covered by one full Encoding Block.
inline constexpr uint32_t kBlockSourceBytes = 64;

/// Fixed payload header length.
inline constexpr uint32_t kPayloadFixedHeaderBytes = 24;

/// Most rows one partition gathers in a window, and so one payload holds.
/// Not a memory bound: the reader decodes a payload in batches, whatever
/// its row count. It keeps the u32 row fields exact and a payload
/// decodable as one vector.
inline constexpr uint32_t kMaxPayloadRows = (1u << 31) - 1;

/// Worst-case serialized size of one Encoding Block: 1 byte EncodingByte,
/// 8 bytes FOR base, 64 bytes body.
inline constexpr uint32_t kMaxBlockBytes = 1 + 8 + kBlockSourceBytes;

/// String dictionary limits: an entry's length byte is at most 63, and the
/// serialized entries of one dictionary must stay under 64 bytes in total,
/// terminator excluded.
inline constexpr uint32_t kDictEntryMaxLen = 63;
inline constexpr uint32_t kDictSerializedBudget = 63;

/// Dictionary sequence markers ending an entry list: another dictionary
/// follows, or the fallback tail begins.
inline constexpr uint8_t kDictMoreMarker = 0xFE;
inline constexpr uint8_t kDictLastMarker = 0xFF;

/// A closing dictionary segment must have indexed at least this many rows
/// per entry for a successor segment to open; otherwise the partition
/// demotes to the fallback tail (hit rate below 1 - 1/factor is not worth
/// per-row lookups).
inline constexpr uint32_t kDictSegmentContinueFactor = 4;

/// The low 2 bits of an EncodingByte.
enum class EncodingKind : uint8_t {
  kConstNarrow = 0,
  kBitPack = 1,
  kForBitPack = 2,
  kPlain = 3,
};

/// A wire column's 2-bit null tag in the null body.
enum class NullTag : uint8_t {
  kAllNull = 0b00,
  kNoNull = 0b01,
  kRawNull = 0b10,
  kReserved = 0b11,
};

/// What a block of a Cell shuffle stream holds: every block on the wire - a
/// ColumnarPayload or a CompositeRows block - starts with this byte, as
/// every V1 block starts with its layout byte.
enum class CellBlockKind : uint8_t {
  kPayload = 0,
  kCompositeRows = 1,
};

/// A CompositeRows block carries CompositeRowVector rows - serialized
/// aggregation state - as opaque bytes, row-framed as the V1 shuffle ships
/// them:
///
///   kind      u8   kCompositeRows
///   rowCount  u32
///   rawBytes  u64  body bytes before compression
///   stored    u64  body bytes that follow
///   layout    u8   RunLayout::kCombined (compressed) or kCombinedUncompressed
///   body           rows back to back, each [size i32][size bytes]
constexpr uint64_t kCompositeBlockHeaderBytes = 1 + 4 + 8 + 8 + 1;

/// A run's compression layout: all streams compressed as one buffer, each
/// stream compressed on its own, or all streams stored uncompressed. A
/// writer falls back to the stored form when compression does not pay.
enum class RunLayout : uint8_t {
  kCombined = 0x00,
  kSeparate = 0x01,
  kCombinedUncompressed = 0x02,
};

/// How one stream's bytes are produced and parsed.
enum class StreamKind : uint8_t {
  /// Encoded stream: SmallInt/Integer/Bigint/Date value stream, a
  /// Timestamp Seconds or Nanos stream, or a String Length/Index stream
  /// (Timestamp halves and lengths encoded as Bigint).
  kEncoded,
  /// Raw fixed-width value stream: Boolean/TinyInt/Hugeint/Float/Double.
  kRawFixed,
  /// String Data stream: raw variable-length bytes.
  kStringData,
};

/// One physical stream of the layout.
struct CellStream {
  uint16_t column; // logical column index
  StreamKind kind;
  /// Bytes per source value: 1/2/4/8/16 for value streams, 8 for a String
  /// length stream, 0 for kStringData.
  uint8_t sourceWidth;
  /// Sign extension of kConstNarrow and kBitPack values. String lengths are
  /// non-negative so either interpretation matches; they are flagged signed
  /// to match their Bigint encoding.
  bool isSigned;
};

/// Maps a shuffle row type (pid column already stripped) onto wire columns
/// and streams. The input adapter converts supported complex types to binary;
/// create() rejects physical types it cannot map.
class CellLayout {
 public:
  static bool isSupportedType(const TypePtr& type) {
    switch (type->kind()) {
      case TypeKind::UNKNOWN:
      case TypeKind::BOOLEAN:
      case TypeKind::TIMESTAMP:
      case TypeKind::HUGEINT:
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER: // includes DATE
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
    // C == 0 is a pid-only shuffle: a layout without wire columns, whose
    // payloads are headers only.
    return true;
  }

  static CellLayout create(const RowTypePtr& rowType);

  uint32_t numColumns() const {
    return numColumns_;
  }

  // UNKNOWN has no wire slot, null state or value stream.
  bool isUnknownColumn(uint32_t column) const {
    return rowType_->childAt(column)->kind() == TypeKind::UNKNOWN;
  }

  uint32_t numWireColumns() const {
    return wireColumns_.size();
  }

  uint32_t wireColumn(uint32_t column) const {
    return wireIndex_[column];
  }

  uint32_t logicalColumn(uint32_t wireColumn) const {
    return wireColumns_[wireColumn];
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
  /// (Length/Index) and the next one (Data), a Timestamp column this one
  /// (Seconds) and the next one (Nanos).
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
  std::vector<uint32_t> wireColumns_;
  std::vector<uint32_t> wireIndex_;
  std::vector<CellStream> streams_;
  std::vector<uint32_t> columnStream_;
  std::vector<bool> isString_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
