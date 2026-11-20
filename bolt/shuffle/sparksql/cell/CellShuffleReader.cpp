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

#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"

#include <algorithm>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellNestedAssembler.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

CellShuffleReader::CellShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool nativeNestedTypes)
    : adapter_(outputType),
      decompressor_(
          codec == nullptr ? nullptr
                           : std::make_unique<CodecDecompressor>(codec)),
      cursor_(std::move(streams), arrowPool, decompressor_.get()),
      rowBlockReader_(decompressor_.get(), pool),
      pool_(pool),
      batchSize_(batchSize),
      batchByteSize_(batchByteSize),
      decoder_(
          CellLayout::create(adapter_.physicalType()),
          decompressor_.get(),
          pool_),
      nestedAssembler_(
          nativeNestedTypes && CellNestedLayout::supports(outputType)
              ? std::make_unique<CellNestedAssembler>(
                    outputType,
                    decompressor_.get(),
                    pool_,
                    CellDecodeLimits{})
              : nullptr),
      compositeConverter_(outputType, pool_) {}

CellShuffleReader::~CellShuffleReader() = default;

CellShuffleReader::NextBlock CellShuffleReader::nextBlock() {
  // Opening streams and reading blocks is the input's share of the decode
  // time, decompression aside. Timed per block, not per read: a payload
  // takes a dozen small reads, and from memory two clock reads around
  // each would cost more than the reads.
  auto timer = cursor_.timeRead();
  uint8_t kind = 0;
  if (!cursor_.nextBlockKind(kind)) {
    return NextBlock::kEnd;
  }
  if (kind == static_cast<uint8_t>(CellBlockKind::kCompositeRows)) {
    nextCompositeRow_ = 0;
    rowBlockReader_.read(cursor_.source(), compositeBlock_, "composite");
    return NextBlock::kCompositeRows;
  }
  BOLT_CHECK_EQ(
      kind,
      static_cast<uint8_t>(CellBlockKind::kPayload),
      "unknown cell shuffle block kind");
  std::string error;
  const bool ok = nestedAssembler_ != nullptr
      ? nestedAssembler_->beginPayload(cursor_.source(), error)
      : decoder_.beginPayload(cursor_.source(), error);
  BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
  ++payloadsRead_;
  return NextBlock::kPayload;
}

RowVectorPtr CellShuffleReader::nextComposite() {
  const uint64_t start = currentTimeNs();
  const size_t limit = std::min(
      compositeBlock_.rows.size(),
      nextCompositeRow_ + static_cast<size_t>(std::max(batchSize_, 1)));
  const uint64_t byteCap = batchByteSize_ > 0
      ? std::min<uint64_t>(
            static_cast<uint64_t>(batchByteSize_),
            std::numeric_limits<int32_t>::max())
      : static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
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
  decodeTimeNs_ += currentTimeNs() - start;
  rowsRead_ += output->size();
  return output;
}

RowVectorPtr CellShuffleReader::next() {
  // A composite block met while batching payloads goes out next.
  if (hasCompositeRows()) {
    return nextComposite();
  }
  RowVectorPtr output;
  if (nestedAssembler_ != nullptr) {
    const uint64_t start = currentTimeNs();
    output = nestedAssembler_->nextBatch(
        std::max(batchSize_, 1), batchByteSize_, [&] {
          return nextBlock() == NextBlock::kPayload;
        });
    decodeTimeNs_ += currentTimeNs() - start;
  } else {
    output = nextPhysical();
    if (output != nullptr && adapter_.hasComplexColumns()) {
      const uint64_t start = currentTimeNs();
      output = adapter_.unpack(output, pool_);
      decodeTimeNs_ += currentTimeNs() - start;
    }
  }
  if (output == nullptr && hasCompositeRows()) {
    return nextComposite();
  }
  if (output != nullptr) {
    rowsRead_ += output->size();
  }
  return output;
}

RowVectorPtr CellShuffleReader::nextPhysical() {
  const uint64_t start = currentTimeNs();
  const vector_size_t capacity = std::max(batchSize_, 1);
  // One batch at a time: a large payload comes out over several calls,
  // small ones fill the same batch until it is full. An empty batch always
  // takes one append, so a byte target at or below zero reads one append
  // per batch.
  while (decoder_.batchRows() < capacity &&
         (decoder_.batchRows() == 0 ||
          static_cast<int64_t>(decoder_.batchBytes()) < batchByteSize_)) {
    const uint32_t left = decoder_.payloadRowsLeft();
    if (left == 0) {
      if (nextBlock() != NextBlock::kPayload) {
        // End of input, or a composite block that next() emits on its own.
        break;
      }
      continue;
    }
    const auto rows = static_cast<uint32_t>(
        std::min<int64_t>(left, capacity - decoder_.batchRows()));
    std::string error;
    const bool ok = decoder_.appendRows(rows, capacity, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
  }
  auto output = decoder_.finishBatch();
  decodeTimeNs_ += currentTimeNs() - start;
  return output;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
