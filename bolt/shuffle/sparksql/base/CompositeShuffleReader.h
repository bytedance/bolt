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
#include <optional>

#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockReader.h"

namespace bytedance::bolt::shuffle::sparksql {

class Codec;
struct ShuffleReaderOptions;

/// Base reader for block-based shuffle formats produced by
/// CompositeShuffleWriter subclasses. Owns stream traversal, decompression,
/// row-block reader, composite-row deserialization, and common timing/count
/// metrics. Subclasses decode their format-specific non-composite blocks via
/// nextNonComposite() and readNextBlock().
class CompositeShuffleReader : public BlockShuffleReader {
 public:
  /// Creates the reader from ShuffleReaderOptions, owning its decompression
  /// codec (null when compressionType is UNCOMPRESSED).
  /// `options.shuffleBatchByteSize` must be positive; `options.batchSize`
  /// is clamped to at least 1.
  CompositeShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType,
      const ShuffleReaderOptions& options,
      arrow::MemoryPool* arrowPool,
      memory::MemoryPool* pool);

  /// `maxBatchBytes` must be positive; `maxBatchRows` is clamped to at least 1.
  CompositeShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType,
      Codec* codec,
      arrow::MemoryPool* arrowPool,
      memory::MemoryPool* pool,
      int32_t maxBatchRows,
      int64_t maxBatchBytes);

  ~CompositeShuffleReader() override;

  /// Next output batch; nullptr when every stream is exhausted.
  /// CompositeRows blocks come out as CompositeRowVectors of at most
  /// maxBatchRows rows, never concatenated with non-composite rows.
  RowVectorPtr next() final;

  uint64_t decodeTimeNs() const override {
    return decodeTimeNs_;
  }

  uint64_t decompressTimeNs() const override {
    return decompressor_ == nullptr ? 0 : decompressor_->decompressTimeNs();
  }

  uint64_t readTimeNs() const override {
    const uint64_t decompress = decompressTimeNs();
    return readAndDecompressTimeNs_ > decompress
        ? readAndDecompressTimeNs_ - decompress
        : 0;
  }

  uint64_t rowsRead() const override {
    return rowsRead_;
  }

  uint64_t payloadsRead() const override {
    return payloadsRead_;
  }

  uint64_t streamsRead() const override {
    return streamsOpened_;
  }

 protected:
  /// Produces the next non-composite batch, or nullptr if EOF or a
  /// kCompositeRows block was encountered before any non-composite rows were
  /// collected.
  virtual RowVectorPtr nextNonComposite() = 0;

  /// Advances across streams for the next block and meters block read +
  /// decompression wall time; readTimeNs() subtracts decompressTimeNs() at
  /// query time.
  /// - If a kCompositeRows block is encountered, decodes it into
  ///   compositeBlock_ and returns false so the caller flushes any pending
  ///   non-composite batch and lets next() emit the composite rows next.
  /// - If a non-composite block is encountered, invokes onBlock(kind, source)
  ///   under the same read timer and returns true.
  /// - Returns false at end of input.
  template <typename OnBlock>
  bool readNextBlock(OnBlock&& onBlock) {
    // Opening streams and reading blocks is the input's share of decode time,
    // decompression aside. Timed per block, not per read.
    NanosecondTimer timer(&readAndDecompressTimeNs_);
    const auto kind = nextBlockKind();
    if (!kind.has_value()) {
      return false;
    }
    if (*kind == ShuffleBlockKind::kCompositeRows) {
      readCompositeBlock();
      return false;
    }
    onBlock(*kind, *source_);
    return true;
  }

  CodecDecompressor* decompressor() const {
    return decompressor_.get();
  }

  RowBlockReader& rowBlockReader() {
    return rowBlockReader_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  int32_t maxBatchRows() const {
    return maxBatchRows_;
  }

  int64_t maxBatchBytes() const {
    return maxBatchBytes_;
  }

  void incrementPayloadsRead() {
    ++payloadsRead_;
  }

 private:
  CompositeShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType,
      std::unique_ptr<Codec> ownedCodec,
      Codec* codec,
      arrow::MemoryPool* arrowPool,
      memory::MemoryPool* pool,
      int32_t maxBatchRows,
      int64_t maxBatchBytes);

  /// Opens streams until a non-empty stream yields a block kind byte.
  /// Returns std::nullopt when every stream is exhausted.
  std::optional<ShuffleBlockKind> nextBlockKind();

  bool hasCompositeRows() const {
    return nextCompositeRow_ < compositeBlock_.rows.size();
  }

  void readCompositeBlock();

  /// The next up to maxBatchRows rows of the last CompositeRows block, rebuilt
  /// as a CompositeRowVector the way the V1 reader does.
  RowVectorPtr nextComposite();

  // Declared before decompressor_, which holds a non-owning pointer to it.
  std::unique_ptr<Codec> ownedCodec_;
  std::unique_ptr<CodecDecompressor> decompressor_;
  std::shared_ptr<ReaderStreamIterator> streams_;
  arrow::MemoryPool* const arrowPool_;
  std::unique_ptr<StreamByteSource> source_;
  bool exhausted_{false};
  uint64_t streamsOpened_{0};
  RowBlockReader rowBlockReader_;
  memory::MemoryPool* const pool_;
  const int32_t maxBatchRows_;
  const int64_t maxBatchBytes_;
  ShuffleRowToColumnarConverter compositeConverter_;
  /// Last CompositeRows block's backing buffer and row views (without size
  /// prefix).
  RowBlock compositeBlock_;
  size_t nextCompositeRow_{0};
  uint64_t decodeTimeNs_{0};
  uint64_t readAndDecompressTimeNs_{0};
  uint64_t rowsRead_{0};
  uint64_t payloadsRead_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql
