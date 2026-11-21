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

#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/base/ShuffleBlockReader.h"

namespace bytedance::bolt::shuffle::sparksql {

class Codec;

/// Base reader for block-based shuffle formats produced by
/// CompositeShuffleWriter subclasses. Owns the stream cursor, decompressor,
/// row-block reader, composite-row deserialization, and common timing/count
/// metrics. Subclasses decode their format-specific non-composite blocks via
/// nextNonComposite() and readNextBlock().
class CompositeShuffleReader : public BlockShuffleReader {
 public:
  CompositeShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType,
      Codec* codec,
      arrow::MemoryPool* arrowPool,
      memory::MemoryPool* pool,
      int32_t batchSize,
      int64_t batchByteSize);

  ~CompositeShuffleReader() override = default;

  /// Next output batch; nullptr when every stream is exhausted.
  /// CompositeRows blocks come out as CompositeRowVectors of at most
  /// batchSize rows, never concatenated with non-composite rows.
  RowVectorPtr next() final;

  uint64_t decodeTimeNs() const override {
    return decodeTimeNs_;
  }

  uint64_t decompressTimeNs() const override {
    return decompressor_ == nullptr ? 0 : decompressor_->decompressTimeNs();
  }

  uint64_t readTimeNs() const override {
    return cursor_.readTimeNs();
  }

  uint64_t rowsRead() const override {
    return rowsRead_;
  }

  uint64_t payloadsRead() const override {
    return payloadsRead_;
  }

  uint64_t streamsRead() const override {
    return cursor_.streamsOpened();
  }

 protected:
  /// Produces the next non-composite batch, or nullptr if EOF or a
  /// kCompositeRows block was encountered before any non-composite rows were
  /// collected.
  virtual RowVectorPtr nextNonComposite() = 0;

  /// Advances across streams for the next block under cursor_.timeRead().
  /// - If a kCompositeRows block is encountered, decodes it into
  ///   compositeBlock_ and returns false so the caller flushes any pending
  ///   non-composite batch and lets next() emit the composite rows next.
  /// - If a non-composite block is encountered, invokes onBlock(kind, source)
  ///   under the same read timer and returns true.
  /// - Returns false at end of input.
  template <typename OnBlock>
  bool readNextBlock(OnBlock&& onBlock) {
    // Opening streams and reading blocks is the input's share of the decode
    // time, decompression aside. Timed per block, not per read.
    auto timer = cursor_.timeRead();
    uint8_t kind = 0;
    if (!cursor_.nextBlockKind(kind)) {
      return false;
    }
    if (kind == static_cast<uint8_t>(ShuffleBlockKind::kCompositeRows)) {
      readCompositeBlock();
      return false;
    }
    onBlock(kind, cursor_.source());
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

  int32_t batchSize() const {
    return batchSize_;
  }

  int64_t batchByteSize() const {
    return batchByteSize_;
  }

  void addDecodeTimeNs(uint64_t ns) {
    decodeTimeNs_ += ns;
  }

  void incrementPayloadsRead() {
    ++payloadsRead_;
  }

 private:
  bool hasCompositeRows() const {
    return nextCompositeRow_ < compositeBlock_.rows.size();
  }

  void readCompositeBlock();

  /// The next up to batchSize rows of the last CompositeRows block, rebuilt
  /// as a CompositeRowVector the way the V1 reader does.
  RowVectorPtr nextComposite();

  std::unique_ptr<CodecDecompressor> decompressor_;
  ShuffleStreamCursor cursor_;
  RowBlockReader rowBlockReader_;
  memory::MemoryPool* const pool_;
  const int32_t batchSize_;
  const int64_t batchByteSize_;
  ShuffleRowToColumnarConverter compositeConverter_;
  /// Last CompositeRows block's backing buffer and row views (without size
  /// prefix).
  RowBlock compositeBlock_;
  size_t nextCompositeRow_{0};
  uint64_t decodeTimeNs_{0};
  uint64_t rowsRead_{0};
  uint64_t payloadsRead_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql
