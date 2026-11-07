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

#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Pull-based byte input. Reads are exact: short input is a format error the
/// decoder reports, so read() returns false rather than a partial count.
class CellByteSource {
 public:
  virtual ~CellByteSource() = default;

  /// Reads exactly `n` bytes into out. False when the source ends first.
  virtual bool read(void* out, size_t n) = 0;

  /// True when no byte is left. Lets a stream reader distinguish "clean end
  /// between payloads" from truncation.
  virtual bool atEnd() const = 0;

  /// Upper bound on bytes still available, SIZE_MAX when unknown. The
  /// decoder compares declared lengths against it before allocating or
  /// reading.
  virtual size_t remainingBound() const {
    return std::numeric_limits<size_t>::max();
  }
};

class MemoryByteSource final : public CellByteSource {
 public:
  explicit MemoryByteSource(std::span<const uint8_t> bytes)
      : data_(bytes.data()), size_(bytes.size()) {}

  bool read(void* out, size_t n) override {
    if (size_ - pos_ < n) {
      return false;
    }
    if (n != 0) {
      ::memcpy(out, data_ + pos_, n);
    }
    pos_ += n;
    return true;
  }

  bool atEnd() const override {
    return pos_ == size_;
  }

  size_t remainingBound() const override {
    return size_ - pos_;
  }

 private:
  const uint8_t* const data_;
  const size_t size_;
  size_t pos_{0};
};

/// Bounds on untrusted input; zero disables a limit. The defaults are what
/// the writer can emit. Run and null-body buffers come from the reader
/// pool, so a corrupt size fails on the pool's capacity.
struct CellDecodeLimits {
  uint32_t maxRowCount{kMaxPayloadRows};
  uint32_t maxRunCount{1u << 16};
};

/// The codec, decompression side. The format never identifies a codec;
/// writer and reader are configured with the same one by the outer
/// protocol.
class CellDecompressor {
 public:
  virtual ~CellDecompressor() = default;

  /// Decompresses `in` into exactly out.size() bytes at `out`; false when
  /// the input cannot be decoded or produces a different length.
  virtual bool decompress(
      std::span<const uint8_t> in,
      std::span<uint8_t> out) = 0;
};

/// Decodes ColumnarPayloads of one schema into RowVectors, streaming: a
/// payload's encoded bytes are read whole, its rows come out in pieces of
/// the caller's size into a batch that may span payloads. Memory is the
/// payload's decoded streams, a bitmap per kRawNull column, and one batch,
/// whatever the payload's row count. Reusable across payloads; scratch buffers
/// are retained. A payload whose columns have different position counts is read
/// by one decoder (readPayload()), and the columns at the same positions are
/// decoded by a decoder of their own (openPayload()).
class CellPayloadDecoder {
 public:
  /// `decompressor` may be null when payloads are known to be uncompressed;
  /// a compressed buffer is then a format error.
  CellPayloadDecoder(
      CellLayout layout,
      CellDecompressor* decompressor,
      memory::MemoryPool* pool,
      CellDecodeLimits limits = {});

  CellPayloadDecoder(const CellPayloadDecoder&) = delete;
  CellPayloadDecoder& operator=(const CellPayloadDecoder&) = delete;
  CellPayloadDecoder(CellPayloadDecoder&&) = default;

  /// Reads one payload from `in`: header, null body and every run. Its
  /// rows then come out through appendRows(). The previous payload must be
  /// fully appended. On malformed input returns false with `error` set,
  /// having consumed an unspecified prefix of `in`.
  bool beginPayload(CellByteSource& in, std::string& error);

  /// Reads one payload as beginPayload() does, but opens no column: the
  /// decoders of its columns open it with openPayload(). `rowCount` is the
  /// header's. What it read stays valid until the next readPayload(),
  /// beginPayload() or decode() of this decoder.
  bool readPayload(CellByteSource& in, uint32_t& rowCount, std::string& error);

  /// Opens the payload another decoder, `source`, read for this decoder's
  /// columns, column c being source's sourceColumns[c] - one entry per
  /// column of this decoder - of the same type, at
  /// `positions` positions each: they come out through appendRows() as this
  /// decoder's rows, and `source` reads no other payload until they are all
  /// appended. The previous payload must be fully appended. On malformed
  /// input returns false with `error` set.
  bool openPayload(
      const CellPayloadDecoder& source,
      std::span<const uint32_t> sourceColumns,
      uint32_t positions,
      std::string& error);

  /// Rows of the current payload not appended yet.
  uint32_t payloadRowsLeft() const {
    return rowCount_ - rowsRead_;
  }

  /// Appends the next `numRows` rows of the current payload to the batch
  /// under construction, opening one of `capacity` rows when none is open
  /// and growing it when they do not fit: in a layout whose columns have
  /// different position counts, a batch's position count varies with the
  /// The checks that need a whole stream run with the payload's last rows.
  /// On malformed input returns false with `error` set; the payload and the
  /// batch are then dropped.
  bool appendRows(uint32_t numRows, vector_size_t capacity, std::string& error);

  vector_size_t batchRows() const {
    return batchRows_;
  }

  /// Bytes the batch's values take: fixed widths plus out-of-line strings.
  uint64_t batchBytes() const {
    return static_cast<uint64_t>(batchRows_) * rowBytes_ + batchStringBytes_;
  }

  /// The batch built so far, closing it; nullptr when none is open.
  RowVectorPtr finishBatch();

  /// Non-null rows of a column in the payload opened last.
  uint32_t nonNullCount(uint32_t col) const {
    return nonNullCount_[col];
  }

  /// A stream's bytes in the payload read or opened last.
  std::string_view streamBytes(uint32_t stream) const {
    return streamViews_[stream];
  }

  /// Fixed bytes of one decoded row.
  uint32_t rowBytes() const {
    return rowBytes_;
  }

  /// A column's values in the open batch, T{} at its null rows;
  /// appendRows() may move them.
  const BufferPtr& batchValues(uint32_t col) const {
    return batch_[col].values;
  }

  /// Decodes exactly one payload from `in` as one vector of all its rows.
  /// On success fills `out` and returns true; on malformed input returns
  /// false with `error` set and consumes an unspecified prefix of `in`.
  bool decode(CellByteSource& in, RowVectorPtr& out, std::string& error);

  const CellLayout& layout() const {
    return layout_;
  }

  /// The fixed header of the payload read last, its bytes as read: when
  /// reading it failed, only the first bytes, as many as the source gave,
  /// are its own. What a malformed stream's error shows of it.
  std::span<const uint8_t, kPayloadFixedHeaderBytes> payloadHeader() const {
    return std::span<const uint8_t, kPayloadFixedHeaderBytes>(header_);
  }

 private:
  struct Dictionary {
    size_t firstEntry;
    size_t numEntries;
    uint32_t matchedRows;
  };

  /// Read position of one column in the current payload.
  struct ColumnCursor {
    /// Non-null values consumed: the raw-stream offset in values.
    uint64_t valuesRead{0};
    StreamCursor<int16_t> values16;
    StreamCursor<int32_t> values32;
    /// Bigint values, or timestamp seconds.
    StreamCursor<int64_t> values64;
    /// Timestamp nanos, string lengths, or dictionary fallback lengths.
    StreamCursor<int64_t> lengthsOrNanos;
    /// Strings: the data stream, or the fallback bytes after the
    /// dictionaries; dataOffset is the next value's start.
    std::string_view data;
    uint64_t dataOffset{0};
    bool isDictionary{false};
    std::vector<std::string_view> entries;
    std::vector<Dictionary> dictionaries;
    const uint8_t* indexes{nullptr};
    uint64_t matchedRows{0};
    uint64_t matchedRead{0};
    size_t nextDictionary{0};
    size_t currentDictionary{0};
    uint32_t currentMatchedLeft{0};
  };

  /// One column of the batch under construction.
  struct ColumnBatch {
    BufferPtr values;
    BufferPtr nulls; // allocated at the batch's first null
    std::vector<BufferPtr> strings;
    char* charsFree{nullptr};
    size_t charsLeft{0};
  };

  bool fail(std::string& error, const char* what) const {
    error = what;
    return false;
  }

  /// Header, null body and every run.
  bool parsePayload(CellByteSource& in, uint32_t& rowCount, std::string& error);

  /// Reads the null body's tags and locates each kRawNull column's records.
  bool parseNullBody(
      CellByteSource& in,
      uint32_t nullStoredSize,
      uint32_t nullDecodedSize,
      std::string& error);

  /// Expands the located records into bitmaps of `rowCount` rows.
  bool expandNulls(uint32_t rowCount, std::string& error);

  /// `onlyRun`: the payload's only run, whose combined buffer the streams may
  /// view in place instead of copying it apart.
  bool parseRun(CellByteSource& in, bool onlyRun, std::string& error);

  /// Expands the nulls and opens every column at `rowCount` rows; with none,
  /// runs the whole-stream checks at once.
  bool openColumns(uint32_t rowCount, std::string& error);

  /// Positions the column's cursor at the payload's first row, checking
  /// what can be checked before any value is read.
  bool openColumn(uint32_t col, std::string& error);

  bool openStringColumn(uint32_t col, std::string& error);

  /// The whole-stream checks, once the last row was appended: the blocks
  /// and string bytes consumed their streams exactly.
  bool finishPayload(std::string& error);

  void openBatch(vector_size_t capacity);

  /// Grows the open batch to hold `rows` rows, doubling it, within one
  /// vector.
  void growBatch(uint64_t rows);

  void dropBatch();

  /// Marks the nulls of the next numRows payload rows in the batch; returns
  /// how many of those rows are non-null.
  BOLT_CELL_KERNEL uint32_t appendNulls(uint32_t col, uint32_t numRows);

  /// Writes numRows values at out: `dense` non-null ones from read(count,
  /// dst), scattered by the payload bitmap, T{} at the null rows.
  template <typename T, typename Read>
  BOLT_CELL_KERNEL bool placeValues(
      uint32_t col,
      uint32_t numRows,
      uint32_t dense,
      T* out,
      const Read& read);

  template <typename T>
  bool appendEncoded(
      uint32_t col,
      uint32_t numRows,
      uint32_t dense,
      StreamCursor<T>& cursor,
      std::string& error);

  template <typename T>
  bool appendRaw(uint32_t col, uint32_t numRows, uint32_t dense);

  bool appendBoolean(
      uint32_t col,
      uint32_t numRows,
      uint32_t dense,
      std::string& error);

  bool appendTimestamp(
      uint32_t col,
      uint32_t numRows,
      uint32_t dense,
      std::string& error);

  BOLT_CELL_KERNEL bool appendString(
      uint32_t col,
      uint32_t numRows,
      uint32_t dense,
      std::string& error);

  /// `bytes` of string space in the column's batch buffers.
  char* stringSpace(uint32_t col, uint64_t bytes);

  template <typename T>
  VectorPtr makeFlat(uint32_t col, const TypePtr& type);

  const CellLayout layout_;
  CellDecompressor* const decompressor_;
  memory::MemoryPool* const pool_;
  const CellDecodeLimits limits_;
  /// Fixed bytes of one decoded row, the batch byte estimate's base.
  uint32_t rowBytes_{0};

  // Per-payload state, reused across calls. The byte buffers sized from
  // untrusted payload fields live in the reader pool so decoded runs stay
  // inside task memory accounting.
  std::vector<BufferPtr> streamBytes_;
  /// The payload's decoded streams: into streamBytes_, or into combinedDecoded_
  /// when a onlyRun combined run is decompressed there.
  std::vector<std::string_view> streamViews_;
  bool streamsInCombinedDecoded_{false};
  BufferPtr nullBody_;
  std::vector<NullTag> nullTags_;
  /// Per kRawNull column, its records in the null body of the reading
  /// decoder.
  std::vector<const uint8_t*> records_;
  std::vector<uint32_t> nonNullCount_;
  /// The kRawNull columns' records expanded, a bitmap per column.
  BufferPtr expandedNulls_;
  std::vector<const uint8_t*> bitmaps_; // into expandedNulls_, per column
  uint64_t bitmapBytes_{0}; // of each bitmap in expandedNulls_
  std::vector<uint8_t> encodingTags_;
  std::vector<uint64_t> storedSizes_;
  std::vector<uint64_t> decodedSizes_;
  std::vector<ColumnCursor> cursors_;
  uint32_t rowCount_{0};
  uint32_t rowsRead_{0};
  BufferPtr compressedScratch_;
  BufferPtr combinedDecoded_;

  // Per-append scratch, at most one batch of dense values.
  BufferPtr denseScratch_;
  BufferPtr timestampScratch_; // dense seconds then nanos
  std::vector<int64_t> lengthScratch_;
  std::vector<std::string_view> stringViews_;

  std::vector<ColumnBatch> batch_;
  bool batchOpen_{false};
  vector_size_t batchCapacity_{0};
  vector_size_t batchRows_{0};
  uint64_t batchStringBytes_{0};

  /// The header parsePayload() read last.
  uint8_t header_[kPayloadFixedHeaderBytes]{};
};

/// Expands the null records of one kRawNull column at `cursor` - their u32
/// count, then the records (CellFormat.h) - into `bitmap`, the
/// ceil(rowCount / 8)-byte bitmap of the column (bit 1 = non-null, unused
/// high bits zero), counts its non-null rows and moves `cursor` past the
/// records; never reads past `end`. Writes up to 7 bytes past the bitmap,
/// which must be writable. False, with `error`, when they are malformed.
BOLT_CELL_KERNEL bool expandNullRecords(
    const uint8_t*& cursor,
    const uint8_t* end,
    uint32_t rowCount,
    uint8_t* bitmap,
    uint32_t& nonNullCount,
    std::string& error);

// Buffer helpers shared by both sides, and the writer's payload pieces.

struct CellWindowInput;
class RunCompressor;

/// Bytes below which a frame is stored rather than compressed - a run, a
/// stream of a kSeparate run, a null body, a CompositeRows body: the
/// codec's frame would not pay for itself. The one threshold for every frame
/// the Cell writer compresses.
constexpr uint64_t kMinCompressBytes = 256;
/// A run at least this large is written kSeparate, each stream its own
/// frame.
constexpr uint64_t kMinSeparateRunBytes = 64 << 10;

/// Bytes of a kCombined or kCombinedUncompressed run header: layout byte,
/// stored size u64, decoded size u64 per stream.
inline uint64_t combinedRunHeaderBytes(uint32_t numStreams) {
  return 1 + 8 + 8ull * numStreams;
}

uint64_t currentTimeNs();

/// Sets the size of `buffer`, allocating or growing it from `pool`, and
/// returns its data. Keeps the bytes - growth copies them and doubles the
/// capacity - for buffers appended to; and the capacity on shrink, so one
/// buffer serves every partition or payload.
char* resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

/// resizeBuffer() for a workspace about to be overwritten: the bytes are
/// not kept. Within the capacity it only sets the size; past it, the old
/// buffer goes back to `pool` before one of exactly `size` is allocated, so
/// growing neither copies nor holds both, nor over-allocates.
char* resetBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool);

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool);

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool);

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

/// Writes a run header at `out`, which holds combinedRunHeaderBytes() of
/// the decodedSizes.size() streams.
void writeCombinedRunHeader(
    char* out,
    RunLayout layout,
    uint64_t stored,
    std::span<const uint64_t> decodedSizes);

} // namespace bytedance::bolt::shuffle::sparksql::cell
