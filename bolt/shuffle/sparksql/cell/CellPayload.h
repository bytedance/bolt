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

/// ColumnarPayload decode (format in CellFormat.h), from bytes to
/// RowVectors, plus the payload pieces the writer's outputs assemble.
///
/// Every field is validated before it is used: a malformed payload is
/// reported, never crashed on and never guessed at.

#pragma once

#include <span>
#include <string_view>
#include <variant>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/base/ShuffleByteSource.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/type/StringView.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Throws a malformed-payload runtime exception with a uniform prefix.
[[noreturn]] FOLLY_NOINLINE void failMalformedPayload(const char* what);

/// Decodes ColumnarPayloads of one schema into RowVectors, streaming: a
/// payload's encoded bytes are read whole, its rows come out in pieces of
/// the caller's size into a batch that may span payloads. Memory is the
/// payload's decoded streams, a bitmap per kRawNull column, and one batch,
/// whatever the payload's row count. Reusable across payloads; scratch buffers
/// are retained. For a nested payload, one decoder reads the physical streams.
/// Each group decoder opens the columns that share its position count.
class CellPayloadDecoder {
 public:
  /// `decompressor` may be null when payloads are known to be uncompressed;
  /// a compressed buffer is then a format error.
  CellPayloadDecoder(
      CellLayout layout,
      ShuffleDecompressor* decompressor,
      memory::MemoryPool* pool);

  /// Projects these physical columns in order from a stream-reading decoder.
  /// The source must outlive this projection. The column mapping is copied.
  CellPayloadDecoder(
      CellPayloadDecoder& source,
      std::span<const uint32_t> sourceColumns);

  CellPayloadDecoder(const CellPayloadDecoder&) = delete;
  CellPayloadDecoder& operator=(const CellPayloadDecoder&) = delete;

  /// Reads a payload and opens all its columns for appendRows(). Fully append
  /// the previous payload first. On malformed input, throws after consuming
  /// an unspecified prefix of `in`.
  void openPayload(ShuffleByteSource& in);

  /// Reads the header, null metadata and streams for use by projected
  /// decoders. Returns the top-level row count. The source owns the streams
  /// until it reads another payload. Throws on malformed input.
  uint32_t readPayloadStreams(ShuffleByteSource& in);

  /// Opens this projection at its group's row count after the source reads
  /// the payload. Consume these rows before the source reads another payload.
  void openProjectedColumns(uint32_t rowCount);

  /// Rows of the current payload not appended yet.
  uint32_t payloadRowsLeft() const {
    return rowCount_ - rowsRead_;
  }

  /// Reserves room for at least `minCapacity` total rows in the output batch.
  void reserveRows(uint64_t minCapacity);

  /// Appends the next `numRows` payload rows, growing the batch as needed.
  /// A zero-row append opens an empty batch if necessary. The last rows
  /// trigger whole-stream checks. If decoding throws, discard the reader.
  void appendRows(uint32_t numRows);

  vector_size_t batchRows() const {
    return batchRows_;
  }

  vector_size_t batchCapacity() const {
    return batchCapacity_;
  }

  /// Bytes the batch's values take: fixed widths plus out-of-line strings.
  uint64_t batchBytes() const {
    return static_cast<uint64_t>(batchRows_) * rowBytes_ + batchStringBytes_;
  }

  /// Closes the output batch. With appendRemainingRows, first includes every
  /// remaining row of the current payload. When reading a whole payload this
  /// way, all-null columns use constant vectors. The default returns nullptr
  /// if no batch is open.
  RowVectorPtr finishBatch(bool appendRemainingRows = false);

  /// A stream's bytes in the payload read or opened last.
  std::string_view streamBytes(uint32_t stream) const {
    return streamViews_[stream];
  }

  /// Fixed bytes of one decoded row.
  uint32_t rowBytes() const {
    return rowBytes_;
  }

  /// Estimated decoded bytes per row of the open payload (fixed widths plus
  /// average variable bytes), at least 1. Updated on every openPayload().
  uint64_t payloadRowBytes() const {
    return payloadRowBytes_;
  }

 private:
  struct Dictionary {
    size_t firstEntry;
    size_t numEntries;
    uint32_t matchedRows;
    bool allInline;
  };

  template <typename T>
  struct RawCursor {
    /// Non-null values consumed: the raw-stream offset in values.
    uint64_t valuesRead{0};
  };

  struct TimestampCursor {
    StreamCursor<int64_t> seconds;
    StreamCursor<int64_t> nanos;
  };

  struct StringCursor {
    /// String lengths, or dictionary fallback lengths.
    StreamCursor<int64_t> lengths;
    /// The data stream, or the fallback bytes after the dictionaries;
    /// dataOffset is the next value's start.
    std::string_view data;
    uint64_t dataOffset{0};
    bool isDictionary{false};
    // Inline entries own their bytes; long entries still view the payload.
    std::vector<StringView> entries;
    std::vector<Dictionary> dictionaries;
    const uint8_t* indexes{nullptr};
    uint64_t matchedRows{0};
    uint64_t matchedRead{0};
    size_t currentDictionary{0};
    uint32_t currentMatchedLeft{0};
  };

  /// Read position of one column in the current payload.
  using ColumnCursor = std::variant<
      RawCursor<bool>,
      RawCursor<int8_t>,
      RawCursor<float>,
      RawCursor<double>,
      RawCursor<int128_t>,
      StreamCursor<int16_t>,
      StreamCursor<int32_t>,
      StreamCursor<int64_t>,
      TimestampCursor,
      StringCursor>;

  /// One column of the batch under construction.
  struct ColumnBatch {
    BufferPtr values;
    BufferPtr nulls; // allocated at the batch's first null
    std::vector<BufferPtr> strings;
    char* charsFree{nullptr};
    size_t charsLeft{0};
  };

  /// Header, null body and every run; returns the header's rowCount.
  uint32_t parsePayload(ShuffleByteSource& in);

  /// Reads the null body's tags and locates each kRawNull column's records.
  void parseNullBody(
      ShuffleByteSource& in,
      uint32_t nullStoredSize,
      uint32_t nullDecodedSize);

  /// Expands the located records into bitmaps of `rowCount` rows.
  void expandNulls(uint32_t rowCount);

  /// `onlyRun`: the payload's only run, whose combined buffer the streams may
  /// view in place instead of copying it apart. Sets `streamsInCombinedDecoded`
  /// when stream views point directly into `combinedDecoded_`.
  void
  parseRun(ShuffleByteSource& in, bool onlyRun, bool& streamsInCombinedDecoded);

  /// Expands the nulls and opens every column at `rowCount` rows; with none,
  /// runs the whole-stream checks at once.
  void openColumns(uint32_t rowCount);

  /// Positions the column's cursor at the payload's first row, checking
  /// what can be checked before any value is read.
  void openColumn(uint32_t col);

  void openStringColumn(uint32_t col, StringCursor& cursor);

  /// The whole-stream checks, once the last row was appended: the blocks
  /// and string bytes consumed their streams exactly.
  void finishPayload();

  void openBatch(vector_size_t capacity);

  /// Grows the open batch to hold `rows` rows, doubling it, within one
  /// vector.
  void growBatch(uint64_t rows);

  void dropBatch();

  /// Decodes a chunk from rowsRead_ into values starting at batchRows_.
  void appendColumnChunk(
      uint32_t col,
      uint32_t numRows,
      uint32_t nonNull,
      Buffer& values);

  /// Marks the nulls of the next numRows payload rows in the batch; returns
  /// how many of those rows are non-null.
  BOLT_CELL_KERNEL uint32_t appendNulls(uint32_t col, uint32_t numRows);

  /// Reads numNonNullValues into their positions in the numRows output slots,
  /// filling null positions with T{}.
  template <typename T, typename Read>
  BOLT_CELL_KERNEL bool placeValues(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      T* out,
      const Read& read);

  template <typename T>
  void appendEncoded(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      Buffer& values,
      StreamCursor<T>& cursor);

  template <typename T>
  void appendRaw(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      Buffer& values,
      RawCursor<T>& cursor);

  void appendBoolean(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      Buffer& values,
      RawCursor<bool>& cursor);

  void appendTimestamp(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      Buffer& values,
      TimestampCursor& cursor);

  BOLT_CELL_KERNEL void appendString(
      uint32_t col,
      uint32_t numRows,
      uint32_t numNonNullValues,
      Buffer& values,
      StringCursor& cursor);

  class StringRows;

  /// Ordinary strings and dictionary fallback share the length/data streams.
  /// Keep their block loop separate from dictionary index resolution.
  BOLT_CELL_KERNEL FOLLY_NOINLINE void appendStringFlat(
      uint32_t col,
      uint32_t numNonNullValues,
      StringRows& rows,
      StringCursor& cursor);

  BOLT_CELL_KERNEL FOLLY_NOINLINE void appendStringDictionary(
      uint32_t col,
      uint32_t numNonNullValues,
      StringRows& rows,
      StringCursor& cursor);

  /// `bytes` of string space in the column's batch buffers.
  FOLLY_NOINLINE char* stringSpace(uint32_t col, uint64_t bytes);

  template <typename T>
  VectorPtr makeFlat(uint32_t col, const TypePtr& type);

  uint64_t payloadBitmapBytes() const {
    return (static_cast<uint64_t>(rowCount_) + 7) / 8;
  }

  const CellLayout layout_;
  ShuffleDecompressor* const decompressor_;
  memory::MemoryPool* const pool_;
  const CellPayloadDecoder* source_{nullptr};
  std::vector<uint32_t> sourceColumns_;
  std::vector<uint32_t> sourceStreams_;
  /// Fixed bytes of one decoded row, the batch byte estimate's base.
  uint32_t rowBytes_{0};
  std::vector<uint32_t> stringDataStreams_;

  // Per-payload state, reused across calls. The byte buffers sized from
  // untrusted payload fields live in the reader pool so decoded runs stay
  // inside task memory accounting.
  std::vector<BufferPtr> streamBytes_;
  /// The payload's decoded streams: into streamBytes_, or into combinedDecoded_
  /// when a onlyRun combined run is decompressed there.
  std::vector<std::string_view> streamViews_;
  BufferPtr nullBody_;
  std::vector<NullTag> nullTags_;
  /// Per kRawNull column, its records in the null body of the reading
  /// decoder.
  std::vector<const uint8_t*> records_;
  std::vector<uint32_t> nonNullCount_;
  /// The kRawNull columns' records expanded, a bitmap per column.
  BufferPtr expandedNulls_;
  std::vector<const uint8_t*> bitmaps_; // into expandedNulls_, per column
  std::vector<uint8_t> encodingTags_;
  std::vector<uint64_t> storedSizes_;
  std::vector<uint64_t> decodedSizes_;
  std::vector<ColumnCursor> cursors_;
  uint32_t rowCount_{0};
  uint32_t rowsRead_{0};
  uint64_t payloadVariableBytes_{0};
  uint64_t payloadRowBytes_{1};
  BufferPtr compressedScratch_;
  BufferPtr combinedDecoded_;

  // Per-chunk scratch, at most one chunk of dense values (or dense seconds
  // followed by dense nanos for TIMESTAMP).
  BufferPtr denseScratch_;

  // Reuse the projected row and its children while they are exclusively owned.
  RowVectorPtr projectionBatch_;
  std::vector<ColumnBatch> batch_;
  bool batchOpen_{false};
  vector_size_t batchCapacity_{0};
  vector_size_t batchRows_{0};
  uint64_t batchStringBytes_{0};
};

/// Expands the null records of one kRawNull column at `cursor` - their u32
/// count, then the records (CellFormat.h) - into `bitmap`, the
/// ceil(rowCount / 8)-byte bitmap of the column (bit 1 = non-null, unused
/// high bits zero), counts its non-null rows and moves `cursor` past the
/// records; never reads past `end`. Writes up to 7 bytes past the bitmap,
/// which must be writable. Throws when malformed.
BOLT_CELL_KERNEL void expandNullRecords(
    const uint8_t*& cursor,
    const uint8_t* end,
    uint32_t rowCount,
    uint8_t* bitmap,
    uint32_t& nonNullCount);

// Buffer helpers shared by both sides, and the writer's payload pieces.

struct CellWindowInput;
class RunCompressor;

/// A run at least this large is written kSeparate, each stream its own
/// frame.
constexpr uint64_t kMinSeparateRunBytes = 64 << 10;

/// Offset of the per-stream decoded size array in a kCombined or
/// kCombinedUncompressed run header: layout byte + stored size u64.
constexpr uint64_t kCombinedRunDecodedSizesOffset = 1 + sizeof(uint64_t);

/// Bytes of a kCombined or kCombinedUncompressed run header: layout byte,
/// stored size u64, decoded size u64 per stream.
inline uint64_t combinedRunHeaderBytes(uint32_t numStreams) {
  return kCombinedRunDecodedSizesOffset + sizeof(uint64_t) * numStreams;
}

/// Bytes of a kSeparate run header: layout byte, stored size u64 and decoded
/// size u64 per stream.
inline uint64_t separateRunHeaderBytes(uint32_t numStreams) {
  return 1 + 2 * sizeof(uint64_t) * numStreams;
}

/// Appends the null body of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Appends everything of one partition's payload that precedes its runs,
/// for the window in memory: fixed header, null body - compressed by
/// `compressor` when that pays - and encoding tags. Returns the bytes the
/// null body's compression saved, to count the payload's uncompressed size.
uint64_t appendPayloadPrefix(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t runCount,
    RunCompressor& compressor,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Bytes of one partition's resident cells, all streams together.
uint64_t residentBytes(const CellWindowInput& in, uint32_t pid);

} // namespace bytedance::bolt::shuffle::sparksql::cell
