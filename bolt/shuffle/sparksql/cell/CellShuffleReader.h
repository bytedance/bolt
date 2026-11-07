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

#include <functional>

#include "bolt/shuffle/sparksql/ReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/ShuffleRowToColumnarConverter.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Builds batches from nested-layout payloads, whose columns have different
/// position counts and so need one CellPayloadDecoder per position group
/// (makeCellNestedAssembler()).
class CellBatchSource {
 public:
  virtual ~CellBatchSource() = default;

  /// Reads one payload, its kind byte consumed. On malformed input returns
  /// false with `error` set.
  virtual bool beginPayload(CellByteSource& in, std::string& error) = 0;

  /// The fixed header beginPayload() read last, as
  /// CellPayloadDecoder::payloadHeader() gives it.
  virtual std::span<const uint8_t> payloadHeader() const = 0;

  /// The next batch: at most `capacity` rows of about `byteBudget` bytes,
  /// one append when that is <= 0. `nextPayload` begins the next payload,
  /// false at the end or before a CompositeRows block. nullptr: no row.
  virtual RowVectorPtr nextBatch(
      vector_size_t capacity,
      int64_t byteBudget,
      const std::function<bool()>& nextPayload) = 0;
};

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
/// CellBatchSource, complex columns packed by the writer restored here by
/// the type adapter.
/// Every block opens with a kind byte; CompositeRows blocks come out as
/// CompositeRowVectors of at most batchSize rows, never concatenated with
/// payload rows.
class CellShuffleReader {
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

  /// Next output batch; nullptr when every stream is exhausted.
  RowVectorPtr next();

  /// Total decode time: payload parsing, decompression, vector building
  /// and complex-column restoration together.
  uint64_t decodeTimeNs() const {
    return decodeTimeNs_;
  }

  /// The decompression share of decodeTimeNs(), metered at the codec
  /// seam so the reader node can report decompress and deserialize as
  /// disjoint metrics, matching the legacy reader's accounting.
  uint64_t decompressTimeNs() const {
    return decompressor_ == nullptr ? 0 : decompressor_->decompressTimeNs();
  }

 private:
  /// CellByteSource over an arrow input stream, with one byte of lookahead
  /// so a clean end between payloads is distinguishable from truncation.
  class StreamSource final : public CellByteSource {
   public:
    explicit StreamSource(std::shared_ptr<arrow::io::InputStream> in)
        : in_(std::move(in)) {}

    bool read(void* out, size_t n) override;
    bool atEnd() const override;

    /// Bytes read() gave so far, the lookahead byte only once read() gave
    /// it: the stream offset of the next byte.
    uint64_t consumed() const {
      return consumed_;
    }

    /// Up to `n` of the next bytes into `out`, for a malformed stream's
    /// error: as many as the stream gives, never an exception. Returns how
    /// many; consumed() does not count them.
    size_t readAhead(uint8_t* out, size_t n) noexcept;

   private:
    std::shared_ptr<arrow::io::InputStream> in_;
    uint64_t consumed_{0};
    mutable bool peeked_{false};
    mutable bool ended_{false};
    mutable uint8_t peekByte_{0};
  };

  class CodecDecompressor final : public CellDecompressor {
   public:
    explicit CodecDecompressor(Codec* codec) : codec_(codec) {}

    bool decompress(std::span<const uint8_t> in, std::span<uint8_t> out)
        override;

    uint64_t decompressTimeNs() const {
      return decompressTimeNs_;
    }

   private:
    Codec* const codec_;
    uint64_t decompressTimeNs_{0};
  };

  enum class NextBlock { kPayload, kCompositeRows, kEnd };

  /// Opens the next block across streams: a payload in the decoder, or a
  /// CompositeRows block into compositeRows_.
  NextBlock nextBlock();

  /// Reads one CompositeRows block (CellFormat.h) whose kind byte was
  /// consumed; false, with `error`, on a malformed one.
  bool readCompositeBlock(std::string& error);

  /// What the error of a malformed block, of kind byte `kind`, starting at
  /// `blockStart` in the current stream adds to its message: the stream and
  /// the block's index and offset in it, the stream's previous payload, the
  /// block's header bytes read and up to 32 bytes after them. Reads those
  /// from the stream: for the failure path only.
  std::string blockDiagnostics(uint64_t blockStart, uint8_t kind);

  bool hasCompositeRows() const {
    return nextCompositeRow_ < compositeRows_.size();
  }

  /// The next up to batchSize rows of the last CompositeRows block, rebuilt
  /// as a CompositeRowVector the way the V1 reader does.
  RowVectorPtr nextComposite();

  /// Next batch in the physical (Cell) schema, at most batchSize rows;
  /// nullptr at the end.
  RowVectorPtr nextPhysical();

  std::shared_ptr<ReaderStreamIterator> streams_;
  /// Built first: its schema check covers nested_ too.
  const CellShuffleTypeAdapter adapter_;
  std::unique_ptr<CodecDecompressor> decompressor_;
  arrow::MemoryPool* const arrowPool_;
  memory::MemoryPool* const pool_;
  const int32_t batchSize_;
  const int64_t batchByteSize_;

  CellPayloadDecoder decoder_;
  /// Set for a nested layout, which it reads in place of decoder_.
  std::unique_ptr<CellBatchSource> nested_;
  ShuffleRowToColumnarConverter compositeConverter_;
  BufferPtr compositeStored_;
  BufferPtr compositeDecoded_;
  /// Row bodies (without the size prefix) of the last CompositeRows block.
  std::vector<std::string_view> compositeRows_;
  size_t nextCompositeRow_{0};
  std::unique_ptr<StreamSource> source_;
  bool exhausted_{false};
  uint64_t decodeTimeNs_{0};

  // Where the reader is, for a malformed block's error.
  /// Streams opened, the current one last.
  uint64_t streamsOpened_{0};
  /// Blocks of the current stream read whole.
  uint64_t streamBlocks_{0};
  /// The current stream's last payload read whole: where its kind byte
  /// lies and where its bytes end in the stream, and its fixed header.
  bool hasLastPayload_{false};
  uint64_t lastPayloadStart_{0};
  uint64_t lastPayloadEnd_{0};
  uint8_t lastPayloadHeader_[kPayloadFixedHeaderBytes]{};
  /// The header, past the kind byte, of the last CompositeRows block.
  uint8_t compositeHeader_[kCompositeBlockHeaderBytes - 1]{};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
