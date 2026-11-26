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

#include "bolt/shuffle/sparksql/base/CompositeShuffleReader.h"

#include <algorithm>
#include <limits>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql {
namespace {

std::unique_ptr<Codec> createReaderCodec(const ShuffleReaderOptions& options) {
  if (options.compressionType == arrow::Compression::UNCOMPRESSED) {
    return nullptr;
  }
  return createCodec(
      options.compressionType,
      CodecOptions{
          getCodecBackend(options.codecBackend),
          kDefaultCompressionLevel,
          options.checksumEnabled});
}

} // namespace

CompositeShuffleReader::CompositeShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    std::unique_ptr<Codec> ownedCodec,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t maxBatchRows,
    int64_t maxBatchBytes)
    : ownedCodec_(std::move(ownedCodec)),
      decompressor_(
          (ownedCodec_ ? ownedCodec_.get() : codec) == nullptr
              ? nullptr
              : std::make_unique<CodecDecompressor>(
                    ownedCodec_ ? ownedCodec_.get() : codec)),
      streams_(std::move(streams)),
      arrowPool_(arrowPool),
      rowBlockReader_(decompressor_.get(), pool),
      pool_(pool),
      maxBatchRows_(std::max(maxBatchRows, 1)),
      maxBatchBytes_(maxBatchBytes),
      compositeConverter_(outputType, pool_) {
  BOLT_CHECK_NOT_NULL(streams_, "shuffle stream iterator must not be null");
  BOLT_CHECK_GT(maxBatchBytes_, 0, "maxBatchBytes must be positive");
}

CompositeShuffleReader::CompositeShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    const ShuffleReaderOptions& options,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool)
    : CompositeShuffleReader(
          std::move(streams),
          outputType,
          createReaderCodec(options),
          /*codec=*/nullptr,
          arrowPool,
          pool,
          options.batchSize,
          options.shuffleBatchByteSize) {}

CompositeShuffleReader::CompositeShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t maxBatchRows,
    int64_t maxBatchBytes)
    : CompositeShuffleReader(
          std::move(streams),
          outputType,
          /*ownedCodec=*/nullptr,
          codec,
          arrowPool,
          pool,
          maxBatchRows,
          maxBatchBytes) {}

CompositeShuffleReader::~CompositeShuffleReader() = default;

std::optional<ShuffleBlockKind> CompositeShuffleReader::nextBlockKind() {
  while (!exhausted_) {
    if (source_ == nullptr) {
      auto in = streams_->nextStream(arrowPool_);
      if (in == nullptr) {
        exhausted_ = true;
        return std::nullopt;
      }
      source_ = std::make_unique<StreamByteSource>(std::move(in));
      ++streamsOpened_;
    }
    if (source_->atEnd()) {
      source_.reset();
      continue;
    }
    uint8_t rawKind = 0;
    BOLT_CHECK(source_->read(&rawKind, 1), "truncated shuffle block");
    return static_cast<ShuffleBlockKind>(rawKind);
  }
  return std::nullopt;
}

void CompositeShuffleReader::readCompositeBlock() {
  nextCompositeRow_ = 0;
  rowBlockReader_.read(
      *source_, compositeBlock_, ShuffleBlockKind::kCompositeRows);
}

RowVectorPtr CompositeShuffleReader::nextComposite() {
  const size_t limit = std::min(
      compositeBlock_.rows.size(),
      nextCompositeRow_ + static_cast<size_t>(maxBatchRows_));
  const uint64_t byteCap = std::min<uint64_t>(
      static_cast<uint64_t>(maxBatchBytes_),
      std::numeric_limits<int32_t>::max());
  size_t end = nextCompositeRow_;
  uint64_t total = 0;
  while (end < limit) {
    const uint64_t rowBytes = compositeBlock_.rows[end].size();
    if (end > nextCompositeRow_ && total + rowBytes > byteCap) {
      break;
    }
    total += rowBytes;
    ++end;
  }
  BOLT_CHECK_LE(
      total,
      std::numeric_limits<int32_t>::max(),
      "composite rows too large for one vector");
  std::vector<std::string_view> slice(
      compositeBlock_.rows.begin() + nextCompositeRow_,
      compositeBlock_.rows.begin() + end);
  nextCompositeRow_ = end;
  auto output = compositeConverter_.convertToComposite(
      slice, static_cast<int32_t>(total));
  rowsRead_ += output->size();
  return output;
}

RowVectorPtr CompositeShuffleReader::next() {
  NanosecondTimer timer(&decodeTimeNs_);
  // A composite block met while batching non-composite blocks goes out next.
  if (hasCompositeRows()) {
    return nextComposite();
  }
  RowVectorPtr output = nextNonComposite();
  if (output == nullptr && hasCompositeRows()) {
    return nextComposite();
  }
  if (output != nullptr) {
    rowsRead_ += output->size();
  }
  return output;
}

} // namespace bytedance::bolt::shuffle::sparksql
