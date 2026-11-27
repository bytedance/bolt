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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockFormat.h"
#include "bolt/shuffle/sparksql/base/ShuffleByteSource.h"
#include "bolt/vector/ComplexVector.h"

namespace arrow::io {
class InputStream;
} // namespace arrow::io

namespace bytedance::bolt::shuffle::sparksql {

class Codec;

/// Base interface for block-based shuffle readers consumed by
/// SparkShuffleReader and external wrappers. Only invoked at batch boundaries
/// and close().
class BlockShuffleReader {
 public:
  struct Metrics {
    uint64_t decodeTimeNs{0};
    uint64_t decompressTimeNs{0};
    uint64_t deserializeTimeNs{0};
    uint64_t readTimeNs{0};
    uint64_t rowsRead{0};
    uint64_t payloadsRead{0};
    uint64_t streamsRead{0};
  };

  virtual ~BlockShuffleReader() = default;

  /// Next output batch; nullptr when every stream is exhausted.
  virtual RowVectorPtr next() = 0;

  /// Total decode time: reading the input, block/payload parsing,
  /// decompression, vector building and type restoration together.
  virtual uint64_t decodeTimeNs() const = 0;

  /// The decompression share of decodeTimeNs(), metered at the codec seam so
  /// the reader node can report decompress and deserialize as disjoint metrics.
  virtual uint64_t decompressTimeNs() const = 0;

  /// The input's share of decodeTimeNs(), disjoint from decompressTimeNs():
  /// opening each stream and reading each block up to building vectors from it.
  virtual uint64_t readTimeNs() const = 0;

  /// The deserialization share of decodeTimeNs(), disjoint from
  /// decompressTimeNs() and readTimeNs().
  uint64_t deserializeTimeNs() const {
    return disjointDeserializeNs(
        decodeTimeNs(), decompressTimeNs(), readTimeNs());
  }

  /// Snapshot of all reader metrics, with decompressTimeNs, readTimeNs and
  /// deserializeTimeNs disjoint.
  Metrics metrics() const {
    const uint64_t decode = decodeTimeNs();
    const uint64_t decompress = decompressTimeNs();
    const uint64_t read = readTimeNs();
    return Metrics{
        .decodeTimeNs = decode,
        .decompressTimeNs = decompress,
        .deserializeTimeNs = disjointDeserializeNs(decode, decompress, read),
        .readTimeNs = read,
        .rowsRead = rowsRead(),
        .payloadsRead = payloadsRead(),
        .streamsRead = streamsRead(),
    };
  }

  /// Rows next() returned so far.
  virtual uint64_t rowsRead() const = 0;

  /// Payloads or data blocks read so far.
  virtual uint64_t payloadsRead() const = 0;

  /// Streams opened so far.
  virtual uint64_t streamsRead() const = 0;

  /// Reader name for summary logging.
  virtual std::string_view name() const = 0;

 private:
  static uint64_t
  disjointDeserializeNs(uint64_t decode, uint64_t decompress, uint64_t read) {
    return decode > decompress && decode - decompress > read
        ? decode - decompress - read
        : 0;
  }
};

/// ShuffleByteSource over an arrow input stream, with one byte of lookahead
/// so a clean end between blocks is distinguishable from truncation.
class StreamByteSource final : public ShuffleByteSource {
 public:
  explicit StreamByteSource(std::shared_ptr<arrow::io::InputStream> in)
      : in_(std::move(in)) {
    BOLT_CHECK_NOT_NULL(in_, "shuffle input stream must not be null");
  }

  bool read(void* out, size_t n) override;
  bool atEnd() override;

 private:
  enum class State : uint8_t {
    kOpen,
    kPeeked,
    kEnded,
  };

  std::shared_ptr<arrow::io::InputStream> in_;
  State state_{State::kOpen};
  uint8_t peekByte_{0};
};

/// ShuffleDecompressor over a Codec, metering total decompression time.
class CodecDecompressor final : public ShuffleDecompressor {
 public:
  explicit CodecDecompressor(Codec* codec) : codec_(codec) {
    BOLT_CHECK_NOT_NULL(codec_, "shuffle decompressor codec must not be null");
  }

  bool decompress(std::span<const uint8_t> in, std::span<uint8_t> out) override;

  uint64_t decompressTimeNs() const {
    return decompressTimeNs_;
  }

 private:
  Codec* const codec_;
  uint64_t decompressTimeNs_{0};
};

/// Decoded framed rows from one or more row blocks. `buffers` owns the
/// backing memory referenced by every string_view in `rows`.
struct RowBlock {
  std::vector<BufferPtr> buffers;
  std::vector<std::string_view> rows;
  uint64_t rowBytes{0};

  bool empty() const {
    return rows.empty();
  }

  size_t size() const {
    return rows.size();
  }

  void clear() {
    rows.clear();
    buffers.clear();
    rowBytes = 0;
  }
};

/// Reads framed row blocks (ShuffleBlockFormat.h, past the kind byte) from a
/// ShuffleByteSource, validating headers and row boundaries and decompressing
/// when needed.
///
/// Output is written into a caller-owned `RowBlock`:
/// - `read()` replaces `out` with one block and reuses a single unique backing
///   buffer across calls.
/// - `append()` appends a block's backing buffer and row views onto `out` so
///   callers can combine small row blocks across multiple blocks/streams into
///   one larger batch without copying rows or invalidating earlier views.
class RowBlockReader {
 public:
  RowBlockReader(ShuffleDecompressor* decompressor, memory::MemoryPool* pool)
      : decompressor_(decompressor), pool_(pool) {}

  /// Reads one row block whose kind byte was already consumed, replacing `out`.
  /// Throws on malformed input.
  void read(
      ShuffleByteSource& in,
      RowBlock& out,
      ShuffleBlockKind kind = ShuffleBlockKind::kCompositeRows);

  /// Reads one row block whose kind byte was already consumed and appends its
  /// backing buffer and row views to `out`. On malformed input, throws without
  /// modifying previously accumulated rows in `out`.
  void append(
      ShuffleByteSource& in,
      RowBlock& out,
      ShuffleBlockKind kind = ShuffleBlockKind::kRows);

 private:
  void decodeBlock(
      ShuffleByteSource& in,
      BufferPtr& blockBuffer,
      std::vector<std::string_view>& rows,
      uint64_t& rowBytes,
      ShuffleBlockKind kind);

  ShuffleDecompressor* const decompressor_;
  memory::MemoryPool* const pool_;
  /// Scratch for compressed input bytes; retained across blocks.
  BufferPtr compressed_;
};

} // namespace bytedance::bolt::shuffle::sparksql
