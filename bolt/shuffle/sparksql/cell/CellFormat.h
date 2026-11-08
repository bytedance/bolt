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
/// This header is the format's definition: the wire constants and enums
/// below are the format, and changing one changes it for both sides.
/// kDictSegmentContinueFactor and kMaxPayloadRows are writer policy.
///
/// A partition's shuffle bytes are blocks back to back, each opening with a
/// CellBlockKind byte. A ColumnarPayload holds some rows of one partition:
///
///   header      24 bytes: rowCount u32, runCount u32, variableBytes u64
///               (a writer statistic, unused by the reader), nullStoredSize
///               u32, nullDecodedSize u32 (0: the null body is stored as
///               is; else it is compressed and decodes to this many bytes).
///               With W wire columns, W = 0 requires nullStoredSize,
///               nullDecodedSize, runCount and variableBytes all 0 (a
///               payload of its header alone); W > 0 requires
///               nullStoredSize >= 1. The reader limits rowCount to
///               kMaxPayloadRows and runCount to 2^24 (CellDecodeLimits).
///   null body   nullStoredSize bytes: one 2-bit NullTag per wire column,
///               four to a byte from the low bits, ceil(W / 4) bytes, the
///               unused high bits zero and kReserved rejected; then per
///               kRawNull column, in column order, its null records: a u32
///               count, then the records. The body ends exactly after the
///               last column's records.
///   enc tags    ceil(W / 8) bytes, W wire columns, LSB first: bit set =
///               the string column is in dictionary form. A bit on a
///               non-string column is an error; unused high bits are zero.
///   runs        runCount runs, each: RunLayout u8, stored size(s) u64 (one,
///               or one per stream for kSeparate), decoded size u64 per
///               stream, then the stored bytes. kSeparate: a stream's
///               decoded size 0 means it is stored as is, and a stored size
///               0 requires decoded 0; kCombinedUncompressed: the stored
///               size equals the decoded sizes' sum; kCombined: the stored
///               bytes decompress to that sum.
///
/// A null record holds consecutive positions of a column, from the position
/// after the previous record's: a run of up to kNullRecordMaxRun positions
/// of one null-ness, then a bitmap of up to kNullRecordBits positions. Its
/// kNullRecordBytes (28) bytes: run u16 (the run's positions, plus
/// kNullRecordRunNull when they are null), fill u8 (the bitmap's
/// positions), then the kNullRecordBitmapBytes (25)-byte bitmap, bit 1 =
/// non-null, its bits past fill zero.
/// A record covers at least one position; a column's records cover at most
/// its positions, the positions past them non-null. A column's positions
/// are the payload's rows, except in a nested layout.
///
/// Nested layouts. A row type with ARRAY, MAP or ROW columns takes one when
/// every leaf type is supported and it shreds into at most 65535 columns;
/// one that shreds into more, or any such row type when writer and reader
/// are configured to pack nested types (CellShuffleOptions::
/// nativeNestedTypes off), packs its complex columns into one trailing
/// binary column. It is shredded in pre-order into plain
/// columns: a leaf is a column of its type; an ARRAY or MAP an Integer
/// column holding each non-null entry's length, followed by the columns of
/// its element, or of its key and then its value; a ROW a SmallInt column
/// holding 1 at each non-null struct, followed by the columns of its
/// fields. A top-level column's positions are the payload's rows. An
/// ARRAY's element column and a MAP's key and value columns have one
/// position per element of the container's non-null entries, entry by
/// entry, as many as its lengths add up to and at most 2^31 - 8 in a
/// payload; a ROW's field columns have the ROW's positions. A column is
/// null wherever a ROW above it at the same positions is null.
///
/// UNKNOWN columns have no wire column. A wire column's values live in its
/// streams (CellLayout): only non-null positions have values, and an
/// all-null column's streams are empty. A stream is the
/// concatenation of its bytes in every run, in run order; a run may end an
/// unencoded stream's bytes anywhere, but never inside an Encoding Block or
/// a dictionary (a writer guarantee: the reader concatenates the runs
/// first). Encoded streams are Encoding Blocks back to back: each
/// covers the values of one kCacheLineBytes cache line (the stream's last
/// block fewer) and opens with an EncodingByte, EncodingKind in the low 2
/// bits and a parameter in the high 6:
///
///   kConstNarrow  every value equal; body: parameter (1..width) bytes,
///                 sign-extended
///   kBitPack      body: values in parameter (1..min(63, 8 * width)) bits,
///                 LSB-first, sign-extended
///   kForBitPack   body: base (width bytes), then value - base in parameter
///                 (0..63) bits, LSB-first
///   kPlain        parameter 0; body: the values as they are, little endian
///
/// Raw streams hold one fixed-width value per non-null position (a Boolean
/// as one 0/1 byte). A string column has a length stream (encoded, Bigint) and
/// a data stream (the bytes). In dictionary form, the data stream starts
/// with a dictionary sequence - per dictionary, entries of a length byte
/// (<= kDictEntryMaxLen) and the bytes, at most kDictSerializedBudget bytes
/// together, an empty entry list allowed, then kDictMoreMarker or
/// kDictLastMarker, then the u32 count of rows it matches - and ends with
/// the bytes of the fallback rows; the length stream holds one index byte
/// per matched row, then the lengths of the fallback rows. Matched rows
/// come first, in dictionary order; an index byte is below the entry count
/// of the dictionary whose matched rows it falls in, and the matched counts
/// add up to at most the non-null positions.
///
/// The reader also requires: Boolean bytes 0 or 1, string lengths within
/// [0, INT32_MAX], Timestamp seconds and nanos within Timestamp's range,
/// and every stream consumed exactly by its values.

#pragma once

#include <folly/lang/Bits.h>
#include <cstdint>
#include <cstring>
#include <vector>

#include "bolt/type/Type.h"

/// Starts a hot Cell kernel on a cache line. Its loops then keep their
/// offsets within a line whatever code links before it, so that a size
/// change in an unrelated function cannot move a tight loop across a line
/// and shift throughput by several percent.
#define BOLT_CELL_KERNEL __attribute__((aligned(64)))

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The writer stages each (partition, stream) in one cache line of this
/// many bytes and flushes a full line as one Encoding Block, so an Encoding
/// Block covers the values of one cache line: 8 Bigints, 16 Integers or 32
/// SmallInts. A fixed format constant, not the host's cache line size.
/// kMaxBlockBytes bounds the encoded size.
inline constexpr uint32_t kCacheLineBytes = 64;

/// Fixed payload header length, after the CellBlockKind byte. Unlike
/// kCompositeBlockHeaderBytes, it does not count that byte.
inline constexpr uint32_t kPayloadFixedHeaderBytes = 24;

/// Most rows one partition gathers in a window, and so one payload holds.
/// Not a memory bound: the reader decodes a payload in batches, whatever
/// its row count. It keeps the u32 row fields exact and a payload
/// decodable as one vector.
inline constexpr uint32_t kMaxPayloadRows = (1u << 31) - 1;

/// Worst-case serialized size of one Encoding Block: 1 byte EncodingByte,
/// 8 bytes FOR base, 64 bytes body.
inline constexpr uint32_t kMaxBlockBytes = 1 + 8 + kCacheLineBytes;

/// String dictionary limits: an entry's length byte is at most 63, and the
/// serialized entries of one dictionary must stay under 64 bytes in total,
/// terminator excluded.
inline constexpr uint32_t kDictEntryMaxLen = 63;
inline constexpr uint32_t kDictSerializedBudget = 63;

/// Dictionary sequence markers ending an entry list: another dictionary
/// follows, or the fallback rows begin.
inline constexpr uint8_t kDictMoreMarker = 0xFE;
inline constexpr uint8_t kDictLastMarker = 0xFF;

/// A closing dictionary must have indexed at least this many rows
/// per entry for a successor segment to open; otherwise the partition
/// demotes to fallback rows (match rate below 1 - 1/factor is not worth
/// per-row lookups).
inline constexpr uint32_t kDictSegmentContinueFactor = 4;

/// The low 2 bits of an EncodingByte.
enum class EncodingKind : uint8_t {
  kConstNarrow = 0,
  kBitPack = 1,
  kForBitPack = 2,
  kPlain = 3,
};

/// A null record: its bytes, the rows of its bitmap, the flag marking its
/// run null and the rows a run holds at most.
inline constexpr uint32_t kNullRecordBytes = 28;
inline constexpr uint32_t kNullRecordBitmapBytes = 25;
inline constexpr uint32_t kNullRecordBits = 8 * kNullRecordBitmapBytes;
inline constexpr uint16_t kNullRecordRunNull = 0x8000;
inline constexpr uint32_t kNullRecordMaxRun = 0x7FFF;

/// A wire column's 2-bit null tag in the null body.
enum class NullTag : uint8_t {
  kAllNull = 0b00,
  kNoNull = 0b01,
  kRawNull = 0b10,
  kReserved = 0b11,
};

/// What a block of a Cell shuffle stream holds: every block on the wire - a
/// ColumnarPayload or a CompositeRows block - starts with this byte.
enum class CellBlockKind : uint8_t {
  kPayload = 0,
  kCompositeRows = 1,
};

/// A CompositeRows block carries CompositeRowVector rows - serialized
/// aggregation state - as opaque bytes, row-framed as the V1 shuffle ships
/// them:
///
///   kind         u8   kCompositeRows
///   rowCount     u32
///   decodedSize  u64  body bytes before compression
///   stored       u64  body bytes that follow
///   layout       u8   RunLayout::kCombined (compressed) or
///                     kCombinedUncompressed
///   body              rows back to back, each [size i32][size bytes]
///
/// The reader requires rowCount in [1, kMaxPayloadRows], decodedSize at least
/// 4 * rowCount, a non-negative size per row, rows consuming the body
/// exactly, stored = decodedSize for kCombinedUncompressed, and a codec for
/// kCombined. The header size counts the kind byte, unlike
/// kPayloadFixedHeaderBytes: the reader, past the kind byte, reads the
/// rest of it.
inline constexpr uint64_t kCompositeBlockHeaderBytes = 1 + 4 + 8 + 8 + 1;

/// The size prefix of a CompositeRows row. Rows lie back to back, so the
/// prefix is in general unaligned.
inline int32_t compositeRowSize(const void* row) {
  return folly::loadUnaligned<int32_t>(row);
}

/// Bytes of a CompositeRows row with its size prefix.
inline uint64_t compositeRowBytes(const void* row) {
  return sizeof(int32_t) + static_cast<uint64_t>(compositeRowSize(row));
}

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
  uint16_t column; // logical column (rowType child), not wire column
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
/// and streams. Complex types reach it shredded (nested layouts) or packed
/// into binary; create() rejects physical types it cannot map.
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

  static CellLayout create(const RowTypePtr& rowType);

  uint32_t numColumns() const {
    return numColumns_;
  }

  // UNKNOWN has no wire slot, null state or value stream.
  bool isUnknownColumn(uint32_t column) const {
    return rowType_->childAt(column)->kind() == TypeKind::UNKNOWN;
  }

  uint32_t numWireColumns() const {
    return logicalOfWire_.size();
  }

  uint32_t wireColumn(uint32_t column) const {
    return wireOfColumn_[column];
  }

  uint32_t logicalColumn(uint32_t wireColumn) const {
    return logicalOfWire_[wireColumn];
  }

  uint32_t numStreams() const {
    return static_cast<uint32_t>(streams_.size());
  }

  const CellStream& stream(uint32_t index) const {
    return streams_[index];
  }

  /// First stream of a logical column; a String column owns this stream
  /// (Length/Index) and the next one (Data), a Timestamp column this one
  /// (Seconds) and the next one (Nanos).
  uint32_t firstStream(uint32_t column) const {
    return firstStream_[column];
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
  std::vector<uint32_t> logicalOfWire_;
  std::vector<uint32_t> wireOfColumn_;
  std::vector<CellStream> streams_;
  std::vector<uint32_t> firstStream_;
  std::vector<bool> isString_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
