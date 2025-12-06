/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "BoltShuffleReader.h"

#include <arrow/array/array_binary.h>
#include <arrow/buffer.h>
#include <arrow/io/buffered.h>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/arrow/Bridge.h"

#include <cstdint>
#include <iostream>

// using namespace facebook;
using namespace bytedance::bolt;
namespace bytedance::bolt::shuffle::sparksql {

namespace {

struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr, nullptr) {}
  BufferViewReleaser(
      std::shared_ptr<arrow::Buffer> arrowBuffer,
      arrow::MemoryPool* memoryPool = nullptr)
      : bufferReleaser_(std::move(arrowBuffer)) {
    if (auto boltArrowPool = dynamic_cast<BoltArrowMemoryPool*>(memoryPool)) {
      // Keep a reference to the memory pool to ensure it outlives the arrow
      // buffer. This prevents use-after-free when the buffer is destroyed after
      // the reader operator.
      boltArrowPool_ = boltArrowPool->shared_from_this();
    }
  }

  void addRef() const {}
  void release() const {}

 private:
  std::shared_ptr<BoltArrowMemoryPool> boltArrowPool_;
  const std::shared_ptr<arrow::Buffer> bufferReleaser_;
};

BufferPtr wrapInBufferViewAsOwner(
    const void* buffer,
    size_t length,
    std::shared_ptr<arrow::Buffer> bufferReleaser,
    arrow::MemoryPool* memoryPool = nullptr) {
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer),
      length,
      {std::move(bufferReleaser), memoryPool});
}

BufferPtr convertToBoltBuffer(
    std::shared_ptr<arrow::Buffer> buffer,
    arrow::MemoryPool* memoryPool = nullptr) {
  if (buffer == nullptr) {
    return nullptr;
  }
  return wrapInBufferViewAsOwner(
      buffer->data(), buffer->size(), buffer, memoryPool);
}

template <TypeKind kind>
VectorPtr readFlatVector(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto values = buffers[bufferIdx++];
  std::vector<BufferPtr> stringBuffers;
  using T = typename TypeTraits<kind>::NativeType;
  if (nulls == nullptr || nulls->size() == 0) {
    return std::make_shared<FlatVector<T>>(
        pool,
        type,
        BufferPtr(nullptr),
        length,
        std::move(values),
        std::move(stringBuffers));
  }
  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      std::move(nulls),
      length,
      std::move(values),
      std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::UNKNOWN>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(type, length, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::HUGEINT>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valueBuffer = buffers[bufferIdx++];
  // Because if buffer does not compress, it will get from netty, the address
  // maynot aligned 16B, which will cause int128_t = xxx coredump by instruction
  // movdqa
  auto data = valueBuffer->as<int128_t>();
  BufferPtr values;
  if ((reinterpret_cast<uintptr_t>(data) & 0xf) == 0) {
    values = valueBuffer;
  } else {
    values = AlignedBuffer::allocate<char>(valueBuffer->size(), pool);
    bytedance::bolt::simd::memcpy(
        values->asMutable<char>(),
        valueBuffer->as<char>(),
        valueBuffer->size());
  }
  std::vector<BufferPtr> stringBuffers;
  if (nulls == nullptr || nulls->size() == 0) {
    auto vp = std::make_shared<FlatVector<int128_t>>(
        pool,
        type,
        BufferPtr(nullptr),
        length,
        std::move(values),
        std::move(stringBuffers));
    return vp;
  }
  return std::make_shared<FlatVector<int128_t>>(
      pool,
      type,
      std::move(nulls),
      length,
      std::move(values),
      std::move(stringBuffers));
}

VectorPtr readFlatVectorStringView(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto lengthBuffer = buffers[bufferIdx++];
  auto valueBuffer = buffers[bufferIdx++];
  const auto* rawLength = lengthBuffer->as<BinaryArrayLengthBufferType>();

  std::vector<BufferPtr> stringBuffers;
  auto values =
      AlignedBuffer::allocate<char>(sizeof(StringView) * length, pool);
  auto rawValues = values->asMutable<StringView>();
  auto rawChars = valueBuffer->as<char>();
  uint64_t offset = 0;
  for (int32_t i = 0; i < length; ++i) {
    rawValues[i] = StringView(rawChars + offset, rawLength[i]);
    offset += rawLength[i];
  }
  stringBuffers.emplace_back(valueBuffer);
  if (nulls == nullptr || nulls->size() == 0) {
    return std::make_shared<FlatVector<StringView>>(
        pool,
        type,
        BufferPtr(nullptr),
        length,
        std::move(values),
        std::move(stringBuffers));
  }
  return std::make_shared<FlatVector<StringView>>(
      pool,
      type,
      std::move(nulls),
      length,
      std::move(values),
      std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::VARCHAR>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::VARBINARY>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, pool);
}

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<ByteInputStream>(byteRanges);
  return byteStream;
}

RowVectorPtr readComplexType(
    BufferPtr buffer,
    RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  RowVectorPtr result;
  auto byteStream =
      toByteStream(const_cast<uint8_t*>(buffer->as<uint8_t>()), buffer->size());
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  serde->deserialize(
      byteStream.get(), pool, rowType, &result, /* serdeOptions */ nullptr);
  return result;
}

RowTypePtr getComplexWriteType(const std::vector<TypePtr>& types) {
  std::vector<std::string> complexTypeColNames;
  std::vector<TypePtr> complexTypeChildrens;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        complexTypeColNames.emplace_back(types[i]->name());
        complexTypeChildrens.emplace_back(types[i]);
      } break;
      default:
        break;
    }
  }
  return std::make_shared<const RowType>(
      std::move(complexTypeColNames), std::move(complexTypeChildrens));
}

void readColumns(
    std::vector<BufferPtr>& buffers,
    memory::MemoryPool* pool,
    uint32_t numRows,
    const std::vector<TypePtr>& types,
    std::vector<VectorPtr>& result) {
  int32_t bufferIdx = 0;
  std::vector<VectorPtr> complexChildren;
  auto complexRowType = getComplexWriteType(types);
  if (complexRowType->children().size() > 0) {
    complexChildren =
        readComplexType(buffers[buffers.size() - 1], complexRowType, pool)
            ->children();
  }

  int32_t complexIdx = 0;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        result.emplace_back(std::move(complexChildren[complexIdx]));
        complexIdx++;
      } break;
      default: {
        auto res = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            readFlatVector,
            types[i]->kind(),
            buffers,
            bufferIdx,
            numRows,
            types[i],
            pool);
        result.emplace_back(std::move(res));
      } break;
    }
  }
}

RowVectorPtr deserialize(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<BufferPtr>& buffers,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto childTypes = type->as<TypeKind::ROW>().children();
  readColumns(buffers, pool, numRows, childTypes, children);
  return std::make_shared<RowVector>(
      pool, type, BufferPtr(nullptr), numRows, children);
}

RowVectorPtr makeColumnarBatch(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers,
    memory::MemoryPool* pool,
    uint64_t& deserializeTime,
    arrow::MemoryPool* arrowPool = nullptr) {
  bytedance::bolt::NanosecondTimer timer(&deserializeTime);
  std::vector<BufferPtr> boltBuffers;
  boltBuffers.reserve(arrowBuffers.size());
  for (auto& buffer : arrowBuffers) {
    boltBuffers.push_back(convertToBoltBuffer(std::move(buffer), arrowPool));
  }
  return deserialize(type, numRows, boltBuffers, pool);
}

RowVectorPtr makeColumnarBatch(
    RowTypePtr type,
    std::unique_ptr<InMemoryPayload> payload,
    memory::MemoryPool* pool,
    uint64_t& deserializeTime,
    arrow::MemoryPool* arrowPool = nullptr) {
  bytedance::bolt::NanosecondTimer timer(&deserializeTime);
  std::vector<BufferPtr> boltBuffers;
  auto numBuffers = payload->numBuffers();
  boltBuffers.reserve(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    auto buffer = payload->readBufferAt(i);
    BOLT_CHECK(
        buffer.ok(), "Failed to read buffer at index " + std::to_string(i));
    boltBuffers.push_back(
        convertToBoltBuffer(std::move(buffer.ValueUnsafe()), arrowPool));
  }
  return deserialize(type, payload->numRows(), boltBuffers, pool);
}

} // namespace

BoltColumnarBatchDeserializer::BoltColumnarBatchDeserializer(
    std::shared_ptr<arrow::io::InputStream> in,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const bytedance::bolt::RowTypePtr& rowType,
    int32_t batchSize,
    int32_t shuffleBatchByteSize,
    arrow::MemoryPool* memoryPool,
    bytedance::bolt::memory::MemoryPool* boltPool,
    std::vector<bool>* isValidityBuffer,
    bool hasComplexType,
    uint64_t& deserializeTime,
    uint64_t& decompressTime,
    bool isRowFormat,
    ZstdStreamCodec* zstdCodec,
    RowBufferPool* rowBufferPool,
    ShuffleRowToColumnarConverter* row2ColConverter)
    : schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      shuffleBatchByteSize_(shuffleBatchByteSize),
      memoryPool_(memoryPool),
      boltPool_(boltPool),
      isValidityBuffer_(isValidityBuffer),
      hasComplexType_(hasComplexType),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime),
      isRowFormat_(isRowFormat),
      zstdCodec_(zstdCodec),
      rowBufferPool_(rowBufferPool),
      row2ColConverter_(row2ColConverter) {
  auto result = arrow::io::BufferedInputStream::Create(
      shuffleBatchByteSize, memoryPool, std::move(in));
  BOLT_CHECK(
      result.ok(),
      "Failed to create BufferedInputStream: " + result.status().message());
  in_ = result.ValueUnsafe();
}

RowVectorPtr BoltColumnarBatchDeserializer::next() {
  if (isRowFormat_) {
    return nextFromRows();
  }
  uint8_t type;
  // check output vector's layout
  if (vectorLayout_ == RowVectorLayout::kInvalid) {
    int64_t bytes = 0;
    auto result = BlockPayload::getVectorLayout(in_.get(), type, bytes);
    BOLT_CHECK(result.ok(), "Failed to get vector layout: " + result.message());
    BOLT_CHECK(bytes != 0, "bytes should not be zero");
    if (type == static_cast<uint8_t>(RowVectorLayout::kComposite)) {
      vectorLayout_ = RowVectorLayout::kComposite;
      // first byte has been read by checkVectorLayout
      zstdCodec_->markHeaderSkipped(0);
      return nextFromRows();
    } else {
      vectorLayout_ = RowVectorLayout::kColumnar;
      payloadType_ = type;
    }
  }

  if (vectorLayout_ == RowVectorLayout::kComposite) {
    return nextFromRows();
  }

  if (hasComplexType_) {
    uint32_t numRows;
    if (!payloadType_.has_value()) {
      int64_t bytes = 0;
      bool isComposite = isCompositeRowVectorLayout(bytes);
      if (bytes == 0) {
        // Reach EOS.
        reachEos_ = true;
        return nullptr;
      }
      if (isComposite) {
        vectorLayout_ = RowVectorLayout::kComposite;
        zstdCodec_->markHeaderSkipped(readAheadBuffer_.size);
        readAheadBuffer_.reset();
        return nextFromRows();
      }
    }
    auto arrowBuffers = BlockPayload::deserialize(
        in_.get(),
        schema_,
        codec_,
        memoryPool_,
        numRows,
        decompressTime_,
        payloadType_,
        readAheadBuffer_.size > 0 ? &readAheadBuffer_ : nullptr);
    BOLT_CHECK(arrowBuffers.ok());

    return makeColumnarBatch(
        rowType_,
        numRows,
        std::move(arrowBuffers.ValueUnsafe()),
        boltPool_,
        deserializeTime_,
        memoryPool_);
  }

  if (reachEos_) {
    if (merged_) {
      return makeColumnarBatch(
          rowType_,
          std::move(merged_),
          boltPool_,
          deserializeTime_,
          memoryPool_);
    }
    return nullptr;
  }

  std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers{};
  uint32_t numRows = 0;
  while (!merged_ ||
         (merged_->numRows() < batchSize_ &&
          merged_->getBufferSize() < shuffleBatchByteSize_)) {
    if (!payloadType_.has_value()) {
      int64_t bytes = 0;
      bool isComposite = isCompositeRowVectorLayout(bytes);
      if (bytes == 0) {
        reachEos_ = true;
        break;
      }
      if (isComposite) {
        vectorLayout_ = RowVectorLayout::kComposite;
        zstdCodec_->markHeaderSkipped(readAheadBuffer_.size);
        readAheadBuffer_.reset();
        if (!merged_) {
          return nextFromRows();
        } else {
          break;
        }
      }
    }
    auto result = BlockPayload::deserialize(
        in_.get(),
        schema_,
        codec_,
        memoryPool_,
        numRows,
        decompressTime_,
        payloadType_,
        readAheadBuffer_.size > 0 ? &readAheadBuffer_ : nullptr);
    BOLT_CHECK(
        result.ok(),
        "Failed to deserialize BlockPayload: " + result.status().message());
    arrowBuffers = std::move(result.ValueUnsafe());
    if (!merged_) {
      merged_ = std::make_unique<InMemoryPayload>(
          numRows, isValidityBuffer_, std::move(arrowBuffers));
      arrowBuffers.clear();
      continue;
    }
    auto mergedRows = merged_->numRows() + numRows;
    auto mergedByteSize =
        merged_->getBufferSize() + getBufferSize(arrowBuffers);
    if (mergedRows > batchSize_ || mergedByteSize > shuffleBatchByteSize_) {
      break;
    }

    auto append = std::make_unique<InMemoryPayload>(
        numRows, isValidityBuffer_, std::move(arrowBuffers));
    auto mergeResult = InMemoryPayload::merge(
        std::move(merged_),
        std::move(append),
        memoryPool_,
        INT64_MAX,
        INT64_MIN);
    BOLT_CHECK(
        mergeResult.ok(),
        "Failed to merge payloads: " + mergeResult.status().message());
    merged_ = std::move(mergeResult.ValueUnsafe());

    arrowBuffers.clear();
  }

  // Reach EOS.
  if (reachEos_ && !merged_) {
    return nullptr;
  }

  auto columnarBatch = makeColumnarBatch(
      rowType_, std::move(merged_), boltPool_, deserializeTime_, memoryPool_);

  // Save remaining rows.
  if (!arrowBuffers.empty()) {
    merged_ = std::make_unique<InMemoryPayload>(
        numRows, isValidityBuffer_, std::move(arrowBuffers));
  }
  return columnarBatch;
}

RowVectorPtr BoltColumnarBatchDeserializer::nextFromRows() {
  if (reachEos_) {
    BOLT_CHECK(
        partialRowSize_ == 0 && partialRow_ == nullptr,
        "nextFromRows() reachEos_ but partialRowSize_ = " +
            std::to_string(partialRowSize_));
    return nullptr;
  }
  std::vector<std::string_view> outputRows;
  uint8_t* dst;
  int32_t dstSize;
  int32_t offset = 0, outputLen = 0;
  bool layoutEnd = false;
  RowVectorLayout layout;
  int32_t totalRowSize = 0;
  bool releaseAllMemory = false;

  while (outputRows.size() <= batchSize_ &&
         rowBufferPool_->bytesUsed() < shuffleBatchByteSize_ && !reachEos_ &&
         !layoutEnd) {
    offset = 0;
    outputLen = 0;
    constexpr int32_t kRowSizeBytes = sizeof(int32_t);
    int32_t atLeastOneRowSize = 0;
    if (partialRow_ && partialRowSize_ >= kRowSizeBytes) {
      atLeastOneRowSize = *(int32_t*)(partialRow_) + kRowSizeBytes;
    }
    rowBufferPool_->getRowBuffer(&dst, dstSize, atLeastOneRowSize);
    // copy previous row fragment
    if (partialRowSize_) {
      offset = partialRowSize_;
      memcpy(dst, partialRow_, offset);
      partialRowSize_ = 0;
      partialRow_ = nullptr;
    }

    auto status = RowBlockPayload::deserialize(
        in_.get(),
        dst,
        dstSize,
        offset,
        zstdCodec_,
        outputRows,
        outputLen,
        reachEos_,
        layoutEnd,
        layout,
        decompressTime_);
    BOLT_CHECK(
        status.ok(),
        "RowBlockPayload::deserialize failed : " + status.ToString());
    // save row fragment
    if (outputLen) {
      partialRowSize_ = outputLen;
      partialRow_ = dst + offset;
    }
    totalRowSize += offset;
  }

  if (layoutEnd) {
    BOLT_CHECK(
        partialRowSize_ == 0,
        "partialRowSize_ = " + std::to_string(partialRowSize_) +
            ", but expected 0 when layoutEnd");
    if (zstdCodec_->nextLayout() <
        static_cast<uint8_t>(RowVectorLayout::kRowStart)) {
      BOLT_CHECK(
          !isRowFormat_,
          "RowBasedShuffleReader do not support layout type less than kRowStart");
      vectorLayout_ = RowVectorLayout::kColumnar;
      payloadType_ = zstdCodec_->nextLayout();
      zstdCodec_->getReadAheadData(
          &readAheadBuffer_.data, readAheadBuffer_.size);
      releaseAllMemory = true;
    }
  }

  // copy left data to tail buffer
  if (partialRowSize_) {
    if (tailBuffer_ == nullptr) {
      tailBuffer_ = arrow::AllocateResizableBuffer(partialRowSize_, memoryPool_)
                        .ValueOrDie();
    } else if (tailBuffer_->size() < partialRowSize_) {
      auto status = tailBuffer_->Resize(partialRowSize_);
      BOLT_CHECK(
          status.ok(), "resize tail buffer failed : " + status.ToString());
    }
    memcpy(tailBuffer_->mutable_data(), partialRow_, partialRowSize_);
    partialRow_ = tailBuffer_->mutable_data();
  }

  if (outputRows.size()) [[likely]] {
    bytedance::bolt::NanosecondTimer timer(&deserializeTime_);
    bytedance::bolt::RowVectorPtr batch;
    if (layout == RowVectorLayout::kColumnar) {
      batch = row2ColConverter_->convert(outputRows);
    } else {
      batch = row2ColConverter_->convertToComposite(
          outputRows, totalRowSize - outputRows.size() * sizeof(int32_t));
    }
    // free all memory used by rows if switch to ColumnarShuffleReader
    releaseAllMemory ? rowBufferPool_->release() : rowBufferPool_->reset();
    return batch;
  }

  rowBufferPool_->reset();
  BOLT_CHECK(reachEos_ == true, "empty batch but have not reaches eof");
  return nullptr;
}

bool BoltColumnarBatchDeserializer::isCompositeRowVectorLayout(int64_t& bytes) {
  uint8_t type;
  if (readAheadBuffer_.size >= sizeof(Payload::Type)) {
    bytes = sizeof(Payload::Type);
    type = readAheadBuffer_.data[0];
    readAheadBuffer_.advance(bytes);
  } else {
    auto status = BlockPayload::getVectorLayout(in_.get(), type, bytes);
    BOLT_CHECK(status.ok(), "Failed to get vector layout: " + status.message());
    if (bytes == 0) {
      // Reach EOS.
      return false;
    }
  }
  if (type > static_cast<uint8_t>(RowVectorLayout::kRowStart)) {
    return true;
  } else {
    vectorLayout_ = RowVectorLayout::kColumnar;
    payloadType_ = type;
    return false;
  }
} // namespace bytedance::bolt::shuffle::sparksql

BoltColumnarBatchDeserializerFactory::BoltColumnarBatchDeserializerFactory(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const RowTypePtr& rowType,
    int32_t batchSize,
    int32_t shuffleBatchByteSize,
    arrow::MemoryPool* memoryPool,
    bytedance::bolt::memory::MemoryPool* boltPool)
    : schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      shuffleBatchByteSize_(shuffleBatchByteSize),
      memoryPool_(memoryPool),
      boltPool_(boltPool) {
  initFromSchema();
}

std::unique_ptr<BoltColumnarBatchDeserializer>
BoltColumnarBatchDeserializerFactory::createDeserializer(
    std::shared_ptr<arrow::io::InputStream> in) {
  // must be same as BoltRuntime::decideBoltShuffleWriterType
  bool isRowBased = (!partitioningShortName_.compare("hash") ||
                     !partitioningShortName_.compare("rr") ||
                     !partitioningShortName_.compare("range")) &&
      ((shuffleWriterType_ == ShuffleWriterType::Adaptive &&
        numPartitions_ >= rowBasePartitionThreshold &&
        schema_->num_fields() >= rowBaseColumnNumThreshold) ||
       (shuffleWriterType_ == ShuffleWriterType::RowBased));
  if (!zstdCodec_) {
    zstdCodec_ =
        std::make_shared<ZstdStreamCodec>(1 /*not used*/, false, memoryPool_);
    rowBufferPool_ = std::make_shared<RowBufferPool>(memoryPool_);
    row2ColConverter_ =
        std::make_shared<ShuffleRowToColumnarConverter>(rowType_, boltPool_);
  }
  return std::make_unique<BoltColumnarBatchDeserializer>(
      std::move(in),
      schema_,
      codec_,
      rowType_,
      batchSize_,
      shuffleBatchByteSize_,
      memoryPool_,
      boltPool_,
      &isValidityBuffer_,
      hasComplexType_,
      deserializeTime_,
      decompressTime_,
      isRowBased,
      zstdCodec_.get(),
      rowBufferPool_.get(),
      row2ColConverter_.get());
}

arrow::MemoryPool* BoltColumnarBatchDeserializerFactory::getPool() {
  return memoryPool_;
}

int64_t BoltColumnarBatchDeserializerFactory::getDecompressTime() {
  return decompressTime_;
}

int64_t BoltColumnarBatchDeserializerFactory::getDeserializeTime() {
  return deserializeTime_;
}

void BoltColumnarBatchDeserializerFactory::initFromSchema() {
  auto result = toShuffleTypeId(schema_->fields());
  BOLT_CHECK(
      result.ok(),
      "Failed to convert Arrow schema to Bolt types: " +
          result.status().message());
  auto arrowColumnTypes = result.ValueUnsafe();
  isValidityBuffer_.reserve(arrowColumnTypes.size());
  for (size_t i = 0; i < arrowColumnTypes.size(); ++i) {
    switch (arrowColumnTypes[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        hasComplexType_ = true;
      } break;
      case arrow::BooleanType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(true);
      } break;
      case arrow::NullType::type_id:
        break;
      default: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }
}

bytedance::bolt::TypePtr fromBoltTypeToArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema) {
  ArrowSchema cSchema;
  auto status = arrow::ExportSchema(*schema, &cSchema);
  BOLT_CHECK(status.ok(), "Failed to export Arrow schema: " + status.message());
  bytedance::bolt::TypePtr typePtr = bytedance::bolt::importFromArrow(cSchema);
  // It should be bolt::importFromArrow's duty to release the imported arrow c
  // schema. Since exported Bolt type prt doesn't hold memory from the c
  // schema.
  ArrowSchemaRelease(&cSchema); // otherwise the c schema leaks memory
  return typePtr;
}

BoltShuffleReader::BoltShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ShuffleReaderOptions options,
    arrow::MemoryPool* pool,
    bytedance::bolt::memory::MemoryPool* boltPool)
    : factory_(std::make_unique<BoltColumnarBatchDeserializerFactory>(
          schema,
          std::move(createArrowIpcCodec(
              options.compressionType,
              getCodecBackend(options.codecBackend))),
          bytedance::bolt::asRowType(fromBoltTypeToArrowSchema(schema)),
          options.batchSize,
          options.shuffleBatchByteSize,
          pool,
          boltPool)) {
  factory_->setNumPartitions(options.numPartitions);
  factory_->setShuffleWriterType(options.forceShuffleWriterType);
  factory_->setpartitioningShortName(options.partitionShortName);
}

} // namespace bytedance::bolt::shuffle::sparksql
