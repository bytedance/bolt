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
    const ShuffleReaderOptions& options,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool)
    : CompositeShuffleReader(
          std::move(streams),
          outputType,
          options,
          arrowPool,
          pool) {
  init(outputType, options.cellNativeNestedTypes);
}

CellShuffleReader::CellShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t maxBatchRows,
    int64_t maxBatchBytes,
    bool nativeNestedTypes)
    : CompositeShuffleReader(
          std::move(streams),
          outputType,
          codec,
          arrowPool,
          pool,
          maxBatchRows,
          maxBatchBytes) {
  init(outputType, nativeNestedTypes);
}

void CellShuffleReader::init(
    const RowTypePtr& outputType,
    bool nativeNestedTypes) {
  BOLT_CHECK(
      CellShuffleTypeAdapter::isSupported(outputType),
      "Unsupported Cell logical schema: {}",
      outputType->toString());
  if (nativeNestedTypes && CellNestedLayout::supports(outputType)) {
    nestedAssembler_ = std::make_unique<CellNestedAssembler>(
        outputType, decompressor(), pool());
  } else {
    adapter_ = std::make_unique<CellShuffleTypeAdapter>(outputType);
    decoder_ = std::make_unique<CellPayloadDecoder>(
        CellLayout::create(adapter_->physicalType()), decompressor(), pool());
  }
}

CellShuffleReader::~CellShuffleReader() = default;

bool CellShuffleReader::nextPayload() {
  return readNextBlock([&](ShuffleBlockKind kind, ShuffleByteSource& source) {
    BOLT_CHECK_EQ(
        kind,
        ShuffleBlockKind::kCellPayload,
        "unknown cell shuffle block kind");
    if (nestedAssembler_ != nullptr) {
      nestedAssembler_->openPayload(source);
    } else {
      decoder_->openPayload(source);
    }
    incrementPayloadsRead();
  });
}

RowVectorPtr CellShuffleReader::nextNonComposite() {
  if (nestedAssembler_ != nullptr) {
    return nestedAssembler_->nextBatch(
        maxBatchRows(), maxBatchBytes(), [&] { return nextPayload(); });
  }
  auto output = nextPhysical();
  if (output != nullptr && adapter_->needsAdaptation()) {
    output = adapter_->unpack(output, pool());
  }
  return output;
}

RowVectorPtr CellShuffleReader::nextPhysical() {
  while (decoder_->payloadRowsLeft() == 0) {
    if (!nextPayload()) {
      // End of input, or a composite block that next() emits on its own.
      return nullptr;
    }
  }
  const uint64_t budget = static_cast<uint64_t>(maxBatchBytes());
  decoder_->reserveRows(std::min<uint64_t>(
      maxBatchRows(), (budget - 1) / decoder_->payloadRowBytes() + 1));
  while (decoder_->batchRows() < decoder_->batchCapacity() &&
         decoder_->batchBytes() < budget) {
    const uint32_t left = decoder_->payloadRowsLeft();
    if (left == 0) {
      if (!nextPayload()) {
        // End of input, or a composite block that next() emits on its own.
        break;
      }
      continue;
    }
    const uint64_t remainingBytes = budget - decoder_->batchBytes();
    const uint64_t maxRowsByBytes =
        (remainingBytes - 1) / decoder_->payloadRowBytes() + 1;
    const uint32_t rows = static_cast<uint32_t>(std::min<uint64_t>(
        left,
        std::min<uint64_t>(
            decoder_->batchCapacity() - decoder_->batchRows(),
            maxRowsByBytes)));
    decoder_->appendRows(rows);
  }
  return decoder_->finishBatch();
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
