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

#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/common/ShuffleBlockReader.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

class CellNestedAssembler;

/// The Cell shuffle read chain, complete in itself: pulls reader streams,
/// parses each stream's blocks (CellFormat.h) - a CellBlockKind byte, then
/// a ColumnarPayload or a CompositeRows block; the protocol is not
/// compatible with the other writers and is selected by shuffleWriterType
/// on both sides - and decodes the payloads into batches of at most
/// batchSize rows: a large payload over several batches, small ones into
/// one.
///
/// Its inputs are the stream iterator, the logical schema and the codec.
/// Batches come out in the logical schema: a nested layout's by a
/// CellNestedAssembler, complex columns packed by the writer restored here by
/// the type adapter.
/// Every block opens with a kind byte; CompositeRows blocks come out as
/// CompositeRowVectors of at most batchSize rows, never concatenated with
/// payload rows.
class CellShuffleReader final : public BlockShuffleReader {
 public:
  CellShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      const RowTypePtr& outputType, // logical row type, no pid column
      Codec* codec, // decompression context; may be null (uncompressed)
      arrow::MemoryPool* arrowPool, // only handed to nextStream()
      memory::MemoryPool* pool,
      int32_t batchSize,
      int64_t batchByteSize,
      // The writer's CellShuffleOptions::nativeNestedTypes.
      bool nativeNestedTypes = true);

  ~CellShuffleReader() override;

  /// Next output batch; nullptr when every stream is exhausted.
  RowVectorPtr next() override;

  /// Total decode time: reading the input, payload parsing,
  /// decompression, vector building and complex-column restoration
  /// together.
  uint64_t decodeTimeNs() const override {
    return decodeTimeNs_;
  }

  /// The decompression share of decodeTimeNs(), metered at the codec
  /// seam so the reader node can report decompress and deserialize as
  /// disjoint metrics, matching the legacy reader's accounting.
  uint64_t decompressTimeNs() const override {
    return decompressor_ == nullptr ? 0 : decompressor_->decompressTimeNs();
  }

  /// The input's share of decodeTimeNs(), disjoint from decompressTimeNs():
  /// opening each stream and reading each block - its bytes read, its
  /// headers parsed, its buffers allocated and its columns opened - up to
  /// building vectors from it. In a Spark task the reads are calls into
  /// the shuffle fetch's Java streams; the legacy reader leaves all of
  /// this out of its deserialize time too.
  uint64_t readTimeNs() const override {
    return cursor_.readTimeNs();
  }

  /// Rows next() returned so far.
  uint64_t rowsRead() const override {
    return rowsRead_;
  }

  /// Payloads read so far, CompositeRows blocks aside.
  uint64_t payloadsRead() const override {
    return payloadsRead_;
  }

  /// Streams opened so far.
  uint64_t streamsRead() const override {
    return cursor_.streamsOpened();
  }

  std::string_view readerName() const override {
    return "CellShuffleReader";
  }

 private:
  enum class NextBlock { kPayload, kCompositeRows, kEnd };

  /// Opens the next block across streams: a payload in the decoder, or a
  /// CompositeRows block into compositeBlock_.
  NextBlock nextBlock();

  bool hasCompositeRows() const {
    return nextCompositeRow_ < compositeBlock_.rows.size();
  }

  /// The next up to batchSize rows of the last CompositeRows block, rebuilt
  /// as a CompositeRowVector the way the V1 reader does.
  RowVectorPtr nextComposite();

  /// Next batch in the physical (Cell) schema, at most batchSize rows;
  /// nullptr at the end.
  RowVectorPtr nextPhysical();

  /// Built first: its schema check covers nestedAssembler_ too.
  const CellShuffleTypeAdapter adapter_;
  std::unique_ptr<CodecDecompressor> decompressor_;
  ShuffleStreamCursor cursor_;
  RowBlockReader rowBlockReader_;
  memory::MemoryPool* const pool_;
  const int32_t batchSize_;
  const int64_t batchByteSize_;

  CellPayloadDecoder decoder_;
  /// Set for a nested layout, which it reads in place of decoder_.
  std::unique_ptr<CellNestedAssembler> nestedAssembler_;
  ShuffleRowToColumnarConverter compositeConverter_;
  /// Last CompositeRows block's backing buffer and row views (without size
  /// prefix).
  RowBlock compositeBlock_;
  size_t nextCompositeRow_{0};
  uint64_t decodeTimeNs_{0};
  uint64_t rowsRead_{0};
  uint64_t payloadsRead_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
