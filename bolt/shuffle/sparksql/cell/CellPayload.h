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

/// L6 of the Cell shuffle: ColumnarPayload decode, from bytes to RowVector.
///
/// Implements ColumnarPayloadFormat.md sections 3-9 in the reading order its
/// section 12.5 recommends, with every L1 validation of section 10.1: a
/// malformed payload is reported, never crashed on and never guessed at.
/// Deliberately independent of the existing shuffle reader stack: no Payload
/// class, no arrow buffers; vectors are built directly.

#pragma once

#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "bolt/buffer/Buffer.h"
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
  /// decoder compares declared lengths against it before allocating (the
  /// bounds-before-reads principle of spec section 10).
  virtual size_t remainingBound() const {
    return std::numeric_limits<size_t>::max();
  }
};

class MemoryByteSource final : public CellByteSource {
 public:
  MemoryByteSource(const uint8_t* data, size_t size)
      : data_(data), size_(size) {}

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

/// Implementation-level bounds on untrusted input (spec section 10.3); zero
/// disables a limit. The row and run counts match what the writer can emit
/// (it slices batches to at most 1 << 24 rows per window). Decoded run size
/// is unbounded by default: a run holds a whole window of one partition and
/// a single value may reach 2 GiB, so no fixed cap is safe for payloads of
/// the same-binary writer (spec section 11.1). Every buffer sized from the
/// run and null-region size fields comes from the reader pool, so a corrupt
/// size fails on the pool's capacity rather than exhausting the process.
/// Decode scratch outside the pool (per-row value views, dictionary tables)
/// is linear in maxRowCount and in those pool-backed stream bytes.
/// Untrusted deployments set it.
struct CellDecodeLimits {
  uint32_t maxRowCount{1u << 24};
  uint32_t maxRunCount{1u << 16};
  uint64_t maxDecodedBytes{0};
};

/// The external codec context of spec section 2, decompression side. The
/// format never identifies a codec; writer and reader are configured with
/// the same one by the outer protocol.
class CellDecompressor {
 public:
  virtual ~CellDecompressor() = default;

  /// Decompresses data into exactly decodedSize bytes at out; false when the
  /// input cannot be decoded or produces a different length (L1 rule 11).
  virtual bool decompress(
      const uint8_t* data,
      size_t size,
      uint8_t* out,
      size_t decodedSize) = 0;
};

/// Decodes ColumnarPayloads of one schema into RowVectors. Reusable across
/// payloads; scratch buffers are retained between calls.
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

  /// Decodes exactly one payload from `in`. On success fills `out` and
  /// returns true; on malformed input returns false with `error` set and
  /// consumes an unspecified prefix of `in`.
  bool decode(CellByteSource& in, RowVectorPtr& out, std::string& error);

  const CellLayout& layout() const {
    return layout_;
  }

 private:
  bool fail(std::string& error, const char* what) const {
    error = what;
    return false;
  }

  bool parseNullRegion(
      CellByteSource& in,
      uint32_t rowCount,
      uint32_t nullStoredSize,
      uint32_t nullDecodedSize,
      std::string& error);

  bool parseRun(CellByteSource& in, std::string& error);

  template <typename T>
  bool buildEncodedColumn(
      uint32_t col,
      uint32_t rowCount,
      const TypePtr& type,
      VectorPtr& out,
      std::string& error);

  template <typename T>
  bool buildRawColumn(
      uint32_t col,
      uint32_t rowCount,
      const TypePtr& type,
      VectorPtr& out,
      std::string& error);

  bool buildStringColumn(
      uint32_t col,
      uint32_t rowCount,
      const TypePtr& type,
      bool dictionaryEncoded,
      VectorPtr& out,
      std::string& error);

  bool buildBooleanColumn(
      uint32_t col,
      uint32_t rowCount,
      const TypePtr& type,
      VectorPtr& out,
      std::string& error);

  bool buildTimestampColumn(
      uint32_t col,
      uint32_t rowCount,
      const TypePtr& type,
      VectorPtr& out,
      std::string& error);

  /// Nulls buffer for a column: nullptr for NO_NULL, otherwise bits in the
  /// engine convention (bit set = non-null), which is byte-identical to the
  /// spec's bitmap.
  BufferPtr makeNulls(uint32_t col, uint32_t rowCount);

  const CellLayout layout_;
  CellDecompressor* const decompressor_;
  memory::MemoryPool* const pool_;
  const CellDecodeLimits limits_;

  // Per-payload state, reused across calls. The byte buffers sized from
  // untrusted payload fields live in the reader pool so decoded runs stay
  // inside task memory accounting.
  std::vector<BufferPtr> streamBytes_;
  BufferPtr nullBody_;
  std::vector<NullTag> tags_;
  std::vector<uint32_t> nonNullCount_;
  std::vector<const uint8_t*> bitmaps_; // into nullBody_, per column
  std::vector<uint8_t> encodingTags_;
  std::vector<uint64_t> storedSizes_;
  std::vector<uint64_t> decodedSizes_;
  BufferPtr scratch_;
  BufferPtr scratch2_;
  BufferPtr timestampScratch_; // dense seconds then nanos, 2 x non-null
  std::vector<int64_t> lengthScratch_;
};

// Writer side: the pieces both output backends assemble payloads from.

struct CellWindowInput;

/// Runs below this size are stored: compression would not pay for itself.
constexpr uint64_t kMinCompressRunBytes = 1 << 10;

/// Bytes of a run header: layout byte, stored size, per-stream sizes.
inline uint64_t runHeaderBytes(uint32_t numStreams) {
  return 1 + 8 + 8ull * numStreams;
}

uint64_t currentTimeNs();

/// Sets the size of `buffer`, allocating or growing it from `pool`, and
/// returns its data. Keeps the capacity on clear or shrink, so one workspace
/// serves every partition or payload.
char* resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool);

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool);

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool);

/// Appends the null body (spec section 4.2) of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Appends everything of one partition's payload that precedes its runs,
/// for the window in memory: fixed header, uncompressed null region and
/// encoding tags.
void appendPayloadPrefix(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t runCount,
    BufferPtr& out,
    memory::MemoryPool* pool);

/// Bytes of one partition's resident cells, all streams together.
uint64_t residentBytes(const CellWindowInput& in, uint32_t pid);

/// Copies one partition's resident cells stream-major into `out`, which
/// holds residentBytes(); fills the per-stream sizes.
void gatherResident(
    const CellWindowInput& in,
    uint32_t pid,
    char* out,
    uint64_t* streamSizes);

/// Writes a run header at `out`, which holds runHeaderBytes().
void writeRunHeader(
    char* out,
    RunLayout layout,
    uint64_t stored,
    const uint64_t* decodedSizes,
    uint32_t numStreams);

/// The run compression policy: compresses into `scratch` and points
/// `body`/`stored` at the COMBINED form when the codec shrinks the bytes,
/// else at the input (spec section 5: fall back to the stored form when
/// compression does not pay). Adds the codec time to `compressTimeNs`.
/// Returns the layout to declare.
RunLayout maybeCompressRun(
    Codec* codec,
    const char* data,
    uint64_t dataBytes,
    BufferPtr& scratch,
    memory::MemoryPool* pool,
    const char*& body,
    uint64_t& stored,
    uint64_t& compressTimeNs);

} // namespace bytedance::bolt::shuffle::sparksql::cell
