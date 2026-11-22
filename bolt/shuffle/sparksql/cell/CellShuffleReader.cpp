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
    : CompositeShuffleReader(
          std::move(streams),
          outputType,
          codec,
          arrowPool,
          pool,
          batchSize,
          batchByteSize) {
  BOLT_CHECK(
      CellShuffleTypeAdapter::isSupported(outputType),
      "Unsupported Cell logical schema: {}",
      outputType->toString());
  if (nativeNestedTypes && CellNestedLayout::supports(outputType)) {
    nestedAssembler_ = std::make_unique<CellNestedAssembler>(
        outputType, decompressor(), pool, CellDecodeLimits{});
  } else {
    adapter_ = std::make_unique<CellShuffleTypeAdapter>(outputType);
    decoder_ = std::make_unique<CellPayloadDecoder>(
        CellLayout::create(adapter_->physicalType()), decompressor(), pool);
  }
}

CellShuffleReader::~CellShuffleReader() = default;

bool CellShuffleReader::nextPayload() {
  return readNextBlock([&](ShuffleBlockKind kind, ShuffleByteSource& source) {
    BOLT_CHECK_EQ(
        kind,
        ShuffleBlockKind::kCellPayload,
        "unknown cell shuffle block kind");
    std::string error;
    const bool ok = nestedAssembler_ != nullptr
        ? nestedAssembler_->beginPayload(source, error)
        : decoder_->beginPayload(source, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
    incrementPayloadsRead();
  });
}

RowVectorPtr CellShuffleReader::nextNonComposite() {
  if (nestedAssembler_ != nullptr) {
    return nestedAssembler_->nextBatch(
        std::max(batchSize(), 1), batchByteSize(), [&] {
          return nextPayload();
        });
  }
  auto output = nextPhysical();
  if (output != nullptr && adapter_->hasComplexColumns()) {
    output = adapter_->unpack(output, pool());
  }
  return output;
}

RowVectorPtr CellShuffleReader::nextPhysical() {
  const vector_size_t capacity = std::max(batchSize(), 1);
  // One batch at a time: a large payload comes out over several calls,
  // small ones fill the same batch until it is full. An empty batch always
  // takes one append, so a byte target at or below zero reads one append
  // per batch.
  while (decoder_->batchRows() < capacity &&
         (decoder_->batchRows() == 0 ||
          static_cast<int64_t>(decoder_->batchBytes()) < batchByteSize())) {
    const uint32_t left = decoder_->payloadRowsLeft();
    if (left == 0) {
      if (!nextPayload()) {
        // End of input, or a composite block that next() emits on its own.
        break;
      }
      continue;
    }
    const auto rows = static_cast<uint32_t>(
        std::min<int64_t>(left, capacity - decoder_->batchRows()));
    std::string error;
    const bool ok = decoder_->appendRows(rows, capacity, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
  }
  return decoder_->finishBatch();
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
