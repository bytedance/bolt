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
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The Cell shuffle read chain, complete in itself: pulls reader streams,
/// parses back-to-back ColumnarPayloads (no dispatch byte, no outer
/// framing — the wire protocol is not compatible with the other writers and
/// is selected by shuffleWriterType on both sides), and batches small
/// payloads into output vectors.
///
/// Deliberately shares nothing with the existing deserializer stack; its
/// only inputs are the stream iterator, the schema and the codec.
class CellShuffleReader {
 public:
  CellShuffleReader(
      std::shared_ptr<ReaderStreamIterator> streams,
      CellLayout layout,
      Codec* codec, // decompression context; may be null (uncompressed)
      arrow::MemoryPool* arrowPool, // only handed to nextStream()
      memory::MemoryPool* pool,
      int32_t batchSize,
      int64_t batchByteSize);

  /// Next output batch; nullptr when every stream is exhausted.
  RowVectorPtr next();

  /// Total decode time: payload parsing, decompression and vector
  /// building together.
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

   private:
    std::shared_ptr<arrow::io::InputStream> in_;
    mutable bool peeked_{false};
    mutable bool ended_{false};
    mutable uint8_t peekByte_{0};
  };

  class CodecDecompressor final : public CellDecompressor {
   public:
    explicit CodecDecompressor(Codec* codec) : codec_(codec) {}

    bool decompress(
        const uint8_t* data,
        size_t size,
        uint8_t* out,
        size_t decodedSize) override;

    uint64_t decompressTimeNs() const {
      return decompressTimeNs_;
    }

   private:
    Codec* const codec_;
    uint64_t decompressTimeNs_{0};
  };

  /// Decodes the next payload across streams; false at the end of input.
  bool nextDecoded(RowVectorPtr& out);

  RowVectorPtr concat(const std::vector<RowVectorPtr>& parts);

  std::shared_ptr<ReaderStreamIterator> streams_;
  const CellLayout layout_;
  std::unique_ptr<CodecDecompressor> decompressor_;
  arrow::MemoryPool* const arrowPool_;
  memory::MemoryPool* const pool_;
  const int32_t batchSize_;
  const int64_t batchByteSize_;

  CellPayloadDecoder decoder_;
  std::unique_ptr<StreamSource> source_;
  bool exhausted_{false};
  uint64_t decodeTimeNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
