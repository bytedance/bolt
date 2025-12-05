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

#include "BoltShuffleWriter.h"
#include <arrow/io/memory.h>
#include <cstdint>
#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Nulls.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/simd.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Timestamp.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"

#include "ArrowFixedSizeBufferOutputStream.h"
#include "arrow/c/bridge.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltRowBasedSortShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriterV2.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif
namespace bytedance::bolt::shuffle::sparksql {

#define BOLT_SHUFFLE_WRITER_LOG_FLAG 0

// macro to rotate left an 8-bit value 'x' given the shift 's' is a 32-bit
// integer (x is left shifted by 's' modulo 8) OR (x right shifted by (8 - 's'
// modulo 8))
#if !defined(__x86_64__)
#define rotateLeft(x, s) \
  (x << (s - ((s >> 3) << 3)) | x >> (8 - (s - ((s >> 3) << 3))))
#endif

// on x86 machines, _MM_HINT_T0,T1,T2 are defined as 1, 2, 3
// equivalent mapping to __builtin_prefetch hints is 3, 2, 1
#if defined(__x86_64__)
#define PREFETCHT0(ptr) _mm_prefetch(ptr, _MM_HINT_T0)
#define PREFETCHT1(ptr) _mm_prefetch(ptr, _MM_HINT_T1)
#define PREFETCHT2(ptr) _mm_prefetch(ptr, _MM_HINT_T2)
#else
#define PREFETCHT0(ptr) __builtin_prefetch(ptr, 0, 3)
#define PREFETCHT1(ptr) __builtin_prefetch(ptr, 0, 2)
#define PREFETCHT2(ptr) __builtin_prefetch(ptr, 0, 1)
#endif

namespace {

arrow::Result<std::shared_ptr<arrow::Buffer>> toArrowBuffer(
    bytedance::bolt::BufferPtr buffer,
    arrow::MemoryPool* pool) {
  if (buffer == nullptr) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(
      auto arrowBuffer, arrow::AllocateResizableBuffer(buffer->size(), pool));
  fastCopy(arrowBuffer->mutable_data(), buffer->as<void>(), buffer->size());
  return arrowBuffer;
}

bool vectorHasNull(const bytedance::bolt::VectorPtr& vp) {
  if (!vp->mayHaveNulls()) {
    return false;
  }
  return vp->countNulls(vp->nulls(), vp->size()) != 0;
}

bytedance::bolt::RowVectorPtr getStrippedRowVector(
    const bytedance::bolt::RowVector& rv) {
  // get new row type
  auto rowType = rv.type()->asRow();
  auto typeChildren = rowType.children();
  typeChildren.erase(typeChildren.begin());
  auto newRowType = bytedance::bolt::ROW(std::move(typeChildren));

  // get length
  auto length = rv.size();

  // get children
  auto children = rv.children();
  children.erase(children.begin());

  return std::make_shared<bytedance::bolt::RowVector>(
      rv.pool(),
      newRowType,
      bytedance::bolt::BufferPtr(nullptr),
      length,
      std::move(children));
}

const int32_t* getFirstColumn(const bytedance::bolt::RowVector& rv) {
  BOLT_CHECK(rv.childrenSize() > 0, "RowVector missing partition id column.");

  auto& firstChild = rv.childAt(0);
  BOLT_CHECK(
      firstChild->isFlatEncoding(),
      "Partition id (field 0) is not flat encoding.");
  BOLT_CHECK(
      firstChild->type()->isInteger(),
      "Partition id (field 0) should be integer, but got {}",
      firstChild->type()->toString());

  // first column is partition key hash value or pid
  return firstChild->asFlatVector<int32_t>()->rawValues();
}

class BinaryArrayResizeGuard {
 public:
  explicit BinaryArrayResizeGuard(BinaryArrayResizeState& state)
      : state_(state) {
    state_.inResize = true;
  }

  ~BinaryArrayResizeGuard() {
    state_.inResize = false;
  }

 private:
  BinaryArrayResizeState& state_;
};

template <bytedance::bolt::TypeKind kind>
arrow::Status collectFlatVectorBuffer(
    bytedance::bolt::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  using T = typename bytedance::bolt::TypeTraits<kind>::NativeType;
  auto flatVector = dynamic_cast<const bytedance::bolt::FlatVector<T>*>(vector);
  buffers.emplace_back();
  ARROW_ASSIGN_OR_RAISE(
      buffers.back(), toArrowBuffer(flatVector->nulls(), pool));
  buffers.emplace_back();
  ARROW_ASSIGN_OR_RAISE(
      buffers.back(), toArrowBuffer(flatVector->values(), pool));
  return arrow::Status::OK();
}

template <>
arrow::Status collectFlatVectorBuffer<bytedance::bolt::TypeKind::UNKNOWN>(
    bytedance::bolt::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return arrow::Status::OK();
}

arrow::Status collectFlatVectorBufferStringView(
    bytedance::bolt::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  auto flatVector = dynamic_cast<
      const bytedance::bolt::FlatVector<bytedance::bolt::StringView>*>(vector);
  buffers.emplace_back();
  auto result = toArrowBuffer(flatVector->nulls(), pool);
  BOLT_CHECK(
      result.ok(),
      "Failed to convert nulls to Arrow buffer: {}",
      result.status().message());
  buffers.back() = result.ValueUnsafe();

  const bool mayHaveNulls = (flatVector->nulls() != nullptr);
  auto rawValues = flatVector->rawValues();
  // last offset is the totalStringSize
  auto lengthBufferSize =
      sizeof(BinaryArrayLengthBufferType) * flatVector->size();
  ARROW_ASSIGN_OR_RAISE(
      auto lengthBuffer,
      arrow::AllocateResizableBuffer(lengthBufferSize, pool));
  auto* rawLength = reinterpret_cast<BinaryArrayLengthBufferType*>(
      lengthBuffer->mutable_data());
  uint64_t offset = 0;
  if (mayHaveNulls) {
    for (int32_t i = 0; i < flatVector->size(); i++) {
      auto length = flatVector->isNullAt(i) ? 0 : rawValues[i].size();
      *rawLength++ = length;
      offset += length;
    }
  } else {
    for (int32_t i = 0; i < flatVector->size(); i++) {
      auto length = rawValues[i].size();
      *rawLength++ = length;
      offset += length;
    }
  }
  buffers.push_back(std::move(lengthBuffer));

  ARROW_ASSIGN_OR_RAISE(
      auto valueBuffer, arrow::AllocateResizableBuffer(offset, pool));
  auto raw = reinterpret_cast<char*>(valueBuffer->mutable_data());
  if (mayHaveNulls) {
    for (int32_t i = 0; i < flatVector->size(); i++) {
      if (!flatVector->isNullAt(i)) {
        fastCopy(raw, rawValues[i].data(), rawValues[i].size());
        raw += rawValues[i].size();
      }
    }
  } else {
    for (int32_t i = 0; i < flatVector->size(); i++) {
      fastCopy(raw, rawValues[i].data(), rawValues[i].size());
      raw += rawValues[i].size();
    }
  }
  buffers.push_back(std::move(valueBuffer));
  return arrow::Status::OK();
}

template <>
arrow::Status collectFlatVectorBuffer<bytedance::bolt::TypeKind::VARCHAR>(
    bytedance::bolt::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return collectFlatVectorBufferStringView(vector, buffers, pool);
}

template <>
arrow::Status collectFlatVectorBuffer<bytedance::bolt::TypeKind::VARBINARY>(
    bytedance::bolt::BaseVector* vector,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool) {
  return collectFlatVectorBufferStringView(vector, buffers, pool);
}

} // namespace

ShuffleWriterType decideBoltShuffleWriterType(
    const ShuffleWriterOptions& options,
    int32_t numColumnsExludePid,
    int64_t firstBatchRowNumber,
    int64_t firstBatchFlatSize,
    int64_t memLimit,
    bytedance::bolt::memory::MemoryPool* boltPool) {
  int numPartitions = options.partitionWriterOptions.numPartitions;
  bool hashOrRoundRobinWithPid =
      (options.partitioning == Partitioning::kRange ||
       options.partitioning == Partitioning::kHash ||
       (options.partitioning == Partitioning::kRoundRobin &&
        options.sort_before_repartition));
  // 0 is adaptive strategy, > 0 means shuffle type forced
  if (options.forceShuffleWriterType && hashOrRoundRobinWithPid) {
    return (ShuffleWriterType)options.forceShuffleWriterType;
  }
  constexpr int32_t v1PartitionThreholdL1 = 10000;
  constexpr int32_t v1PartitionThreholdL2 = 50000;
  constexpr int32_t v1PreAllocSizeL1 = 20;
  constexpr int32_t v1PreAllocSizeL2 = 10;

  // calculate prealloc row size
  int32_t preAllocSize = kDefaultPreAllocSize;
  if (options.forceShuffleWriterType == 0 && hashOrRoundRobinWithPid) {
    preAllocSize = BoltShuffleWriter::calculatePreallocBufferSize(
        firstBatchRowNumber,
        firstBatchFlatSize,
        memLimit,
        numPartitions,
        options.bufferSize);
  }
  // V1 by default for single/range partitioning
  ShuffleWriterType type = ShuffleWriterType::V1;
  if (hashOrRoundRobinWithPid) {
    // for large partition number with multiple columns
    if (numPartitions >= rowBasePartitionThreshold &&
        numColumnsExludePid >= rowBaseColumnNumThreshold) {
      type = ShuffleWriterType::RowBased;
      LOG(INFO) << __FUNCTION__ << ": forceShuffleWriterType = "
                << options.forceShuffleWriterType
                << ", numColumns = " << numColumnsExludePid
                << ", partitions = " << numPartitions
                << ", partitioning =" << options.partitioning
                << ", recommendedColumn2RowSize = "
                << options.recommendedColumn2RowSize
                << ", use BoltRowBasedSortShuffleWriter";
    } else if (
        (preAllocSize > options.useV2PreallocSizeThreshold) ||
        (numPartitions > v1PartitionThreholdL1 &&
         preAllocSize > v1PreAllocSizeL1) ||
        (numPartitions > v1PartitionThreholdL2 &&
         preAllocSize > v1PreAllocSizeL2)) {
      // take batch size 32k as an example
      // 32k/10k = 3-4 rows for one partition per batch
      // if estimated prealloc size is larger than 20
      // V1 is supposed to behave much better due to prealloc buffer for each
      // partition(experimental)
      type = ShuffleWriterType::V1;

      LOG(INFO) << __FUNCTION__ << ": forceShuffleWriterType = "
                << options.forceShuffleWriterType << ", preAllocSize "
                << preAllocSize << " >= " << options.useV2PreallocSizeThreshold
                << ", partitions = " << numPartitions
                << ", partitioning = " << options.partitioning
                << ", or not hashPartitioning, use BoltShuffleWriter";
    } else {
      type = ShuffleWriterType::V2;
      LOG(INFO) << __FUNCTION__ << ": forceShuffleWriterType = "
                << options.forceShuffleWriterType << ", preAllocSize "
                << preAllocSize << " <= " << options.useV2PreallocSizeThreshold
                << ", partitions = " << numPartitions
                << ", partitioning = " << options.partitioning
                << ", use BoltShuffleWriterV2";
    }
  }
  return type;
}

std::shared_ptr<BoltShuffleWriter> BoltShuffleWriter::create(
    ShuffleWriterOptions options,
    int32_t numColumnsExludePid,
    int64_t firstBatchRowNumber,
    int64_t firstBatchFlatSize,
    int64_t memLimit,
    bytedance::bolt::memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool) {
  ShuffleWriterType type = decideBoltShuffleWriterType(
      options,
      numColumnsExludePid,
      firstBatchRowNumber,
      firstBatchFlatSize,
      memLimit,
      boltPool);

  std::shared_ptr<BoltShuffleWriter> shuffle_writer;
  switch (type) {
    case ShuffleWriterType::V1: {
      shuffle_writer = std::make_shared<BoltShuffleWriter>(
          std::move(options), boltPool, arrowPool);
      break;
    }
    case ShuffleWriterType::V2: {
      if (options.partitioning == Partitioning::kSingle) {
        BOLT_FAIL("BoltShuffleWriterV2 does not support single partitioning");
      }
      shuffle_writer = std::make_shared<BoltShuffleWriterV2>(
          std::move(options), boltPool, arrowPool);
      break;
    }
    case ShuffleWriterType::RowBased: {
      if (options.partitioning == Partitioning::kSingle) {
        BOLT_FAIL(
            "BoltRowBasedSortShuffleWriter does not support single partitioning");
      }
      shuffle_writer = std::make_shared<BoltRowBasedSortShuffleWriter>(
          std::move(options), boltPool, arrowPool);
      break;
    }
    default:
      BOLT_CHECK(
          false,
          "Illegal bolt shuffle writer type " + std::to_string((int32_t)type));
  }
  auto status = shuffle_writer->init();
  BOLT_CHECK(status.ok(), "Failed to init BoltShuffleWriter");
  return shuffle_writer;
}

std::shared_ptr<BoltShuffleWriter> BoltShuffleWriter::createDefault(
    ShuffleWriterOptions options,
    bytedance::bolt::memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool) {
  auto writer = std::make_shared<BoltShuffleWriter>(
      std::move(options), boltPool, arrowPool);
  auto status = writer->init();
  BOLT_CHECK(status.ok(), "Failed to init BoltShuffleWriter");
  return writer;
}

void BoltShuffleWriter::ensureVectorLoaded(bytedance::bolt::RowVectorPtr rv) {
  if (isLazyNotLoaded(*rv)) {
    rv->loadedVector();
  }
}

arrow::Status BoltShuffleWriter::init() {
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#else
  supportAvx512_ = false;
#endif

  // Split record batch size should be less than 32k.
  // BOLT_CHECK_LE(options_.bufferSize, 32 * 1024);

  ARROW_ASSIGN_OR_RAISE(
      partitioner_,
      Partitioner::make(
          options_.partitioning,
          numPartitions_,
          options_.startPartitionId,
          options_.sort_before_repartition));

  // pre-allocated buffer size for each partition, unit is row count
  // when partitioner is SinglePart, partial variables don`t need init
  if (options_.partitioning != Partitioning::kSingle) {
    partition2RowCount_.resize(numPartitions_);
    partitionBufferSize_.resize(numPartitions_);
    partition2RowOffsetBase_.resize(numPartitions_ + 1);
  }

  partitionBufferBase_.resize(numPartitions_);
  partitionBufferBaseInBatches_.resize(numPartitions_);

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::initPartitions() {
  auto simpleColumnCount = simpleColumnIndices_.size();

  partitionValidityAddrs_.resize(simpleColumnCount);
  std::for_each(
      partitionValidityAddrs_.begin(),
      partitionValidityAddrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(numPartitions_, nullptr); });

  partitionFixedWidthValueAddrs_.resize(fixedWidthColumnCount_);
  std::for_each(
      partitionFixedWidthValueAddrs_.begin(),
      partitionFixedWidthValueAddrs_.end(),
      [this](std::vector<uint8_t*>& v) { v.resize(numPartitions_, nullptr); });

  partitionBuffers_.resize(simpleColumnCount);
  std::for_each(
      partitionBuffers_.begin(), partitionBuffers_.end(), [this](auto& v) {
        v.resize(numPartitions_);
      });

  partitionBinaryAddrs_.resize(binaryColumnIndices_.size());
  std::for_each(
      partitionBinaryAddrs_.begin(),
      partitionBinaryAddrs_.end(),
      [this](std::vector<BinaryBuf>& v) { v.resize(numPartitions_); });

  return arrow::Status::OK();
}

int64_t BoltShuffleWriter::rawPartitionBytes() const {
  return std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      0LL);
}

void BoltShuffleWriter::setPartitionBufferSize(uint32_t newSize) {
  options_.bufferSize = newSize;
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
BoltShuffleWriter::generateComplexTypeBuffers(
    bytedance::bolt::RowVectorPtr vector) {
  auto arena = std::make_unique<bytedance::bolt::StreamArena>(boltPool_);
  auto serializer = serde_.createSerializer(
      asRowType(vector->type()),
      vector->size(),
      arena.get(),
      /* serdeOptions */ nullptr);
  const bytedance::bolt::IndexRange allRows{0, vector->size()};
  serializer->append(vector, folly::Range(&allRows, 1));
  auto serializedSize = serializer->maxSerializedSize();
  auto flushBuffer = complexTypeFlushBuffer_[0];
  if (flushBuffer == nullptr) {
    ARROW_ASSIGN_OR_RAISE(
        flushBuffer,
        arrow::AllocateResizableBuffer(
            serializedSize, partitionBufferPool_.get()));
  } else if (serializedSize > flushBuffer->capacity()) {
    RETURN_NOT_OK(flushBuffer->Reserve(serializedSize));
  }

  auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 0, serializedSize);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  bytedance::bolt::serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  return valueBuffer;
}

arrow::Status BoltShuffleWriter::split(
    bytedance::bolt::RowVectorPtr rv,
    int64_t memLimit) {
  bytedance::bolt::NanosecondTimer splitTimer(&totalSplitTime_);
  updateInputMetrics(rv);
  if (options_.partitioning == Partitioning::kSingle) {
    if (bytedance::bolt::RowVector::isComposite(rv)) {
      if (vectorLayout_ == RowVectorLayout::kColumnar) {
        RETURN_NOT_OK(tryEvict());
      }
      return splitCompositeVector(rv, memLimit);
    }

    // input vector is not composite , but layout is kComposite
    // which means from composite to columnar switch, flush all previous batches
    if (vectorLayout_ == RowVectorLayout::kComposite) {
      RETURN_NOT_OK(tryEvict());
    }
    vectorLayout_ = RowVectorLayout::kColumnar;

    {
      bytedance::bolt::NanosecondTimer timer(&flattenTime_);
      ensureFlatten(rv);
    }
    RETURN_NOT_OK(initFromRowVector(*rv));
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    std::vector<bytedance::bolt::VectorPtr> complexChildren;
    for (auto& child : rv->children()) {
      if (child->encoding() == bytedance::bolt::VectorEncoding::Simple::FLAT) {
        auto status = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            collectFlatVectorBuffer,
            child->typeKind(),
            child.get(),
            buffers,
            partitionBufferPool_.get());
        RETURN_NOT_OK(status);
      } else {
        complexChildren.emplace_back(child);
      }
    }
    if (complexChildren.size() > 0) {
      auto rowVector = std::make_shared<bytedance::bolt::RowVector>(
          boltPool_,
          complexWriteType_,
          bytedance::bolt::BufferPtr(nullptr),
          rv->size(),
          std::move(complexChildren));
      buffers.emplace_back();
      ARROW_ASSIGN_OR_RAISE(
          buffers.back(), generateComplexTypeBuffers(rowVector));
    }
    RETURN_NOT_OK(evictBuffers(0, rv->size(), std::move(buffers), false));
  } else if (options_.partitioning == Partitioning::kRange) {
    if (bytedance::bolt::RowVector::isComposite(rv)) {
      if (vectorLayout_ == RowVectorLayout::kColumnar) {
        RETURN_NOT_OK(tryEvict());
      }
      return splitCompositeVector(rv, memLimit);
    }
    if (vectorLayout_ == RowVectorLayout::kComposite) {
      RETURN_NOT_OK(tryEvict());
    }
    vectorLayout_ = RowVectorLayout::kColumnar;

    {
      bytedance::bolt::NanosecondTimer timer(&flattenTime_);
      ensureFlatten(rv);
    }
    auto pidArr = getFirstColumn(*rv);
    START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
    RETURN_NOT_OK(partitioner_->compute(
        pidArr, rv->size(), row2Partition_, partition2RowCount_));
    END_TIMING();
    // for range partition CompositeColumnarBatch, it has already been combined
    // into a single batch. So we should strip the pid column here.
    auto strippedRv = getStrippedRowVector(*rv);
    RETURN_NOT_OK(initFromRowVector(*strippedRv));
    RETURN_NOT_OK(doSplit(*strippedRv, memLimit));
  } else {
    {
      if (bytedance::bolt::RowVector::isComposite(rv)) {
        if (vectorLayout_ == RowVectorLayout::kColumnar) {
          RETURN_NOT_OK(tryEvict());
        }
        return splitCompositeVector(rv, memLimit);
      }
      if (vectorLayout_ == RowVectorLayout::kComposite) {
        RETURN_NOT_OK(tryEvict());
      }
      vectorLayout_ = RowVectorLayout::kColumnar;

      bytedance::bolt::NanosecondTimer timer(&flattenTime_);
      ensureFlatten(rv);
    }
    bool enableAccumulateBatch = options_.enableVectorCombination &&
        !hasComplexType_ &&
        rv->childrenSize() < options_.accumulateBatchMaxColumns;
    if (enableAccumulateBatch) {
      int64_t occupiedMemory = (accumulateDataset_ == nullptr
                                    ? 0
                                    : accumulateDataset_->estimateFlatSize()) +
          rv->estimateFlatSize();
      if (accumulateRows_ + rv->size() < options_.accumulateBatchMaxBatches &&
          occupiedMemory < 0.25 * memLimit) {
        bytedance::bolt::NanosecondTimer timer(&combineVectorCost_);
        accumulateRows_ += rv->size();
        initAccumulateDataset(rv);
        accumulateDataset_->append(rv.get());
        combinedVectorNumber_ += rv->size();
        ++combinedVectorNumber_;
        return arrow::Status::OK();
      } else {
        initAccumulateDataset(rv);
        if (accumulateRows_ > 0) {
          accumulateDataset_->append(rv.get());
          rv = std::move(accumulateDataset_);
        }
        accumulateDataset_ = nullptr;
        accumulateRows_ = 0;
      }
    }

    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(
          pidArr, rv->size(), row2Partition_, partition2RowCount_));
      END_TIMING();
      auto strippedRv = getStrippedRowVector(*rv);
      RETURN_NOT_OK(initFromRowVector(*strippedRv));
      RETURN_NOT_OK(doSplit(*strippedRv, memLimit));
    } else {
      RETURN_NOT_OK(initFromRowVector(*rv));
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      RETURN_NOT_OK(partitioner_->compute(
          nullptr, rv->size(), row2Partition_, partition2RowCount_));
      END_TIMING();
      RETURN_NOT_OK(doSplit(*rv, memLimit));
    }
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::stop() {
  bytedance::bolt::NanosecondTimer stopTimer(&stopTime_);
  if (vectorLayout_ != RowVectorLayout::kComposite) {
    partitionWriter_->setRowFormat(false);
    if (accumulateDataset_ != nullptr) {
      if (partitioner_->hasPid()) {
        auto pidArr = getFirstColumn(*accumulateDataset_);
        START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
        RETURN_NOT_OK(partitioner_->compute(
            pidArr,
            accumulateDataset_->size(),
            row2Partition_,
            partition2RowCount_));
        END_TIMING();
        auto strippedRv = getStrippedRowVector(*accumulateDataset_);
        RETURN_NOT_OK(initFromRowVector(*strippedRv));
        RETURN_NOT_OK(doSplit(*strippedRv, kMinMemLimit));
      } else {
        RETURN_NOT_OK(initFromRowVector(*accumulateDataset_));
        START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
        RETURN_NOT_OK(partitioner_->compute(
            nullptr,
            accumulateDataset_->size(),
            row2Partition_,
            partition2RowCount_));
        END_TIMING();
        RETURN_NOT_OK(doSplit(*accumulateDataset_, kMinMemLimit));
      }
    }
    if (options_.partitioning != Partitioning::kSingle) {
      for (auto pid = 0; pid < numPartitions_; ++pid) {
        RETURN_NOT_OK(evictPartitionBuffers(pid, false));
      }
    }
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
      setSplitState(SplitState::kStop);
      RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
      metrics_.avgPreallocSize =
          (preallocCount_ == 0) ? 0 : totalPreallocSize_ / preallocCount_;
      partitionBuffers_.clear();
    }

    stat();
  } else {
    setSplitState(SplitState::kStop);
    RETURN_NOT_OK(tryEvict());
    {
      SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
      RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
    }
  }
  metrics_.useV2 = 0;
  finalizeMetrics();

  boltPool_->release();
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::buildPartition2Row(uint32_t rowNum) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingBuildPartition]);

  // calc partition2RowOffsetBase_
  partition2RowOffsetBase_[0] = 0;
  for (auto pid = 1; pid <= numPartitions_; ++pid) {
    partition2RowOffsetBase_[pid] =
        partition2RowOffsetBase_[pid - 1] + partition2RowCount_[pid - 1];
  }

  // calc rowOffset2RowId_
  rowOffset2RowId_.resize(rowNum);
  for (auto row = 0; row < rowNum; ++row) {
    auto pid = row2Partition_[row];
    rowOffset2RowId_[partition2RowOffsetBase_[pid]++] = row;
  }

  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partition2RowOffsetBase_[pid] -= partition2RowCount_[pid];
  }

  // calc valid partition list
  partitionUsed_.clear();
  for (auto pid = 0; pid != numPartitions_; ++pid) {
    if (partition2RowCount_[pid] > 0) {
      partitionUsed_.push_back(pid);
    }
  }

  printPartition2Row();

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::updateInputHasNull(
    const bytedance::bolt::RowVector& rv,
    uint32_t offset) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingHasNull]);

  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    if (!inputHasNull_[col]) {
      auto colIdx = simpleColumnIndices_[col];
      if (vectorHasNull(rv.childAt(colIdx + offset))) {
        inputHasNull_[col] = true;
      }
    }
  }

  printInputHasNull();

  return arrow::Status::OK();
}

void BoltShuffleWriter::setSplitState(SplitState state) {
  splitState_ = state;
}

arrow::Status BoltShuffleWriter::doSplit(
    const bytedance::bolt::RowVector& rv,
    int64_t memLimit) {
  auto rowNum = rv.size();
  RETURN_NOT_OK(buildPartition2Row(rowNum));
  RETURN_NOT_OK(updateInputHasNull(rv));

  START_TIMING(cpuWallTimingList_[CpuWallTimingIteratePartitions]);

  setSplitState(SplitState::kPreAlloc);
  // Calculate buffer size based on available offheap memory, history average
  // bytes per row and options_.bufferSize.
  auto preAllocBufferSize = calculatePartitionBufferSize(rv, memLimit);
  ++preallocCount_;
  totalPreallocSize_ += preAllocBufferSize;
  RETURN_NOT_OK(preAllocPartitionBuffers(preAllocBufferSize));
  END_TIMING();

  printPartitionBuffer();

  setSplitState(SplitState::kSplit);
  RETURN_NOT_OK(splitRowVector(rv));

  printPartitionBuffer();

  setSplitState(SplitState::kEvictBigBuffer);
  // evict some big buffer to avoid OOM
  for (auto pid : needEvicted_) {
    RETURN_NOT_OK(evictPartitionBuffers(pid, /*reuseBuffer=*/false));
    variableMemoryUsage_[pid] = 0;
  }
  if (pool_ != spillArrowPool_) {
    // try evict all cached payload if using different spill pool
    evictCachedPayload(std::numeric_limits<int64_t>::max());
  }
  maxVariableMemoryUsage_ = *std::max_element(
      variableMemoryUsage_.begin(), variableMemoryUsage_.end());
  needEvicted_.clear();

  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitRowVector(
    const bytedance::bolt::RowVector& rv) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingSplitRV]);

  // now start to split the RowVector
  RETURN_NOT_OK(splitFixedWidthValueBuffer(rv, partitionFixedWidthValueAddrs_));
  RETURN_NOT_OK(splitValidityBuffer<true>(rv));
  RETURN_NOT_OK(splitBinaryArray(rv));
  RETURN_NOT_OK(splitComplexType(rv));

  // update partition buffer base after split
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    partitionBufferBase_[pid] += partition2RowCount_[pid];
  }

  return arrow::Status::OK();
}

template <typename T>
arrow::Status BoltShuffleWriter::splitFixedWidthValueBuffer(
    const bytedance::bolt::RowVector& rv,
    T& fixedWidthValueAddrs) {
  for (auto col = 0; col < fixedWidthColumnCount_; ++col) {
    auto colIdx = simpleColumnIndices_[col];
    auto& column = rv.childAt(colIdx);
    const uint8_t* srcAddr = (const uint8_t*)column->valuesAsVoid();
    const auto& dstAddrs = fixedWidthValueAddrs[col];

    switch (arrow::bit_width(arrowColumnTypes_[colIdx]->id())) {
      case 0: // arrow::NullType::type_id:
        // No value buffer created for NullType.
        break;
      case 1: // arrow::BooleanType::type_id:
        RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
        break;
      case 8:
        RETURN_NOT_OK(splitFixedType<uint8_t>(srcAddr, dstAddrs));
        break;
      case 16:
        RETURN_NOT_OK(splitFixedType<uint16_t>(srcAddr, dstAddrs));
        break;
      case 32:
        RETURN_NOT_OK(splitFixedType<uint32_t>(srcAddr, dstAddrs));
        break;
      case 64: {
        if (column->type()->kind() == bytedance::bolt::TypeKind::TIMESTAMP) {
          RETURN_NOT_OK(
              splitFixedType<bytedance::bolt::Timestamp>(srcAddr, dstAddrs));
        } else {
          RETURN_NOT_OK(splitFixedType<uint64_t>(srcAddr, dstAddrs));
        }
      } break;
      case 128: // arrow::Decimal128Type::type_id
        // too bad gcc generates movdqa even we use __m128i_u data type.
        // splitFixedType<__m128i_u>(srcAddr, dstAddrs);
        {
          if (column->type()->isShortDecimal()) {
            RETURN_NOT_OK(splitFixedType<int64_t>(srcAddr, dstAddrs));
          } else if (column->type()->isLongDecimal()) {
            // assume batch size = 32k; reducer# = 4K; row/reducer = 8
            RETURN_NOT_OK(
                splitFixedType<bytedance::bolt::int128_t>(srcAddr, dstAddrs));
          } else {
            return arrow::Status::Invalid(
                "Column type " + schema_->field(colIdx)->type()->ToString() +
                " is not supported.");
          }
        }
        break;
      default:
        return arrow::Status::Invalid(
            "Column type " + schema_->field(colIdx)->type()->ToString() +
            " is not fixed width");
    }
  }

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitBoolType(
    const uint8_t* srcAddr,
    const std::vector<uint8_t*>& dstAddrs) {
  for (auto& pid : partitionUsed_) {
    uint8_t* dstaddr = dstAddrs[pid];
    if (dstaddr != nullptr) {
      splitBoolTypeInternal(srcAddr, dstaddr, pid);
    }
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitBoolType(
    const uint8_t* srcAddr,
    const std::vector<std::vector<uint8_t*>>& dstAddrs) {
  for (auto& pid : partitionUsed_) {
    uint8_t* dstaddr = dstAddrs[pid][0];
    BOLT_DCHECK(dstaddr != nullptr);
    splitBoolTypeInternal(srcAddr, dstaddr, pid);
  }
  return arrow::Status::OK();
}

void BoltShuffleWriter::splitBoolTypeInternal(
    const uint8_t* srcAddr,
    uint8_t* dstaddr,
    uint32_t pid) {
  auto r = partition2RowOffsetBase_[pid]; /*8k*/
  auto size = partition2RowOffsetBase_[pid + 1];
  auto dstOffset = partitionBufferBase_[pid];
  auto dstOffsetInByte = (8 - (dstOffset & 0x7)) & 0x7;
  auto dstIdxByte = dstOffsetInByte;
  auto dst = dstaddr[dstOffset >> 3];

  for (; r < size && dstIdxByte > 0; r++, dstIdxByte--) {
    auto srcOffset = rowOffset2RowId_[r]; /*16k*/
    auto src = srcAddr[srcOffset >> 3];
    src = src >> (srcOffset & 7) |
        0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
    src = __rolb(src, 8 - dstIdxByte);
#else
    src = rotateLeft(src, (8 - dstIdxByte));
#endif
    dst = dst & src; // only take the useful bit.
  }
  dstaddr[dstOffset >> 3] = dst;
  if (r == size) {
    return;
  }
  dstOffset += dstOffsetInByte;
  // now dst_offset is 8 aligned
  for (; r + 8 < size; r += 8) {
    dst = extractBitsToByteSimd(srcAddr, &rowOffset2RowId_[r]);
    dstaddr[dstOffset >> 3] = dst;
    dstOffset += 8;
    //_mm_prefetch(dstaddr + (dst_offset >> 3) + 64, _MM_HINT_T0);
  }
  // last byte, set it to 0xff is ok
  dst = 0xff;
  dstIdxByte = 0;
  for (; r < size; r++, dstIdxByte++) {
    auto srcOffset = rowOffset2RowId_[r]; /*16k*/
    auto src = srcAddr[srcOffset >> 3];
    src = src >> (srcOffset & 7) |
        0xfe; // get the bit in bit 0, other bits set to 1
#if defined(__x86_64__)
    src = __rolb(src, dstIdxByte);
#else
    src = rotateLeft(src, dstIdxByte);
#endif
    dst = dst & src; // only take the useful bit.
  }
  dstaddr[dstOffset >> 3] = dst;
}

template <bool needAlloc>
arrow::Status BoltShuffleWriter::splitValidityBuffer(
    const bytedance::bolt::RowVector& rv) {
  for (size_t col = 0; col < simpleColumnIndices_.size(); ++col) {
    auto colIdx = simpleColumnIndices_[col];
    auto& column = rv.childAt(colIdx);
    if (vectorHasNull(column)) {
      auto& dstAddrs = partitionValidityAddrs_[col];
      if constexpr (needAlloc) {
        for (auto& pid : partitionUsed_) {
          if (dstAddrs[pid] == nullptr) {
            // Init bitmap if it's null.
            ARROW_ASSIGN_OR_RAISE(
                auto validityBuffer,
                arrow::AllocateResizableBuffer(
                    arrow::bit_util::BytesForBits(partitionBufferSize_[pid]),
                    partitionBufferPool_.get()));
            dstAddrs[pid] = const_cast<uint8_t*>(validityBuffer->data());
            memset(
                validityBuffer->mutable_data(),
                0xff,
                validityBuffer->capacity());
            partitionBuffers_[col][pid][kValidityBufferIndex] =
                std::move(validityBuffer);
          }
        }
      }

      auto srcAddr = (const uint8_t*)(column->mutableRawNulls());
      RETURN_NOT_OK(splitBoolType(srcAddr, dstAddrs));
    } else {
      VsPrintLF(colIdx, " column hasn't null");
    }
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitBinaryType(
    uint32_t binaryIdx,
    const bytedance::bolt::FlatVector<bytedance::bolt::StringView>& src,
    std::vector<BinaryBuf>& dst) {
  const auto* srcRawValues = src.rawValues();
  const auto* srcRawNulls = src.rawNulls();

  for (auto& pid : partitionUsed_) {
    auto& binaryBuf = dst[pid];

    // use 32bit offset
    auto dstLengthBase = (BinaryArrayLengthBufferType*)(binaryBuf.lengthPtr) +
        partitionBufferBase_[pid];

    auto valueOffset = binaryBuf.valueOffset;
    auto dstValuePtr = binaryBuf.valuePtr + valueOffset;
    auto capacity = binaryBuf.valueCapacity;

    auto rowOffsetBase = partition2RowOffsetBase_[pid];
    auto numRows = partition2RowCount_[pid];
    auto multiply = 1;

    for (auto i = 0; i < numRows; i++) {
      auto rowId = rowOffset2RowId_[rowOffsetBase + i];
      auto& stringView = srcRawValues[rowId];
      size_t isNull =
          srcRawNulls && bytedance::bolt::bits::isBitNull(srcRawNulls, rowId);
      auto stringLen = (isNull - 1) & stringView.size();

      // 1. copy length, update offset.
      dstLengthBase[i] = stringLen;
      valueOffset += stringLen;

      // Resize if necessary.
      if (valueOffset >= capacity) {
        auto oldCapacity = capacity;
        (void)oldCapacity; // suppress warning
        capacity =
            capacity + std::max((capacity >> multiply), (uint64_t)stringLen);
        multiply = std::min(3, multiply + 1);

        const auto& valueBuffer =
            partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][pid]
                             [kBinaryValueBufferIndex];
        {
          binaryArrayResizeState_ = BinaryArrayResizeState{pid, binaryIdx};
          BinaryArrayResizeGuard guard(binaryArrayResizeState_);
          RETURN_NOT_OK(valueBuffer->Reserve(capacity));
        }

        binaryBuf.valuePtr = valueBuffer->mutable_data();
        binaryBuf.valueCapacity = capacity;
        dstValuePtr = binaryBuf.valuePtr + valueOffset - stringLen;
        // Need to update dstLengthBase because lengthPtr can be updated if
        // Reserve triggers spill.
        dstLengthBase = (BinaryArrayLengthBufferType*)(binaryBuf.lengthPtr) +
            partitionBufferBase_[pid];
      }

      // 2. copy value
      if (stringLen != 0) {
        fastCopy(dstValuePtr, stringView.data(), stringLen);

        dstValuePtr += stringLen;
      }
    }

    updateVariableMemoryUsage(
        pid,
        valueOffset - binaryBuf.valueOffset +
            numRows * sizeof(BinaryArrayLengthBufferType));

    binaryBuf.valueOffset = valueOffset;
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitBinaryArray(
    const bytedance::bolt::RowVector& rv) {
  for (auto col = fixedWidthColumnCount_; col < simpleColumnIndices_.size();
       ++col) {
    auto binaryIdx = col - fixedWidthColumnCount_;
    auto& dstAddrs = partitionBinaryAddrs_[binaryIdx];
    auto colIdx = simpleColumnIndices_[col];
    auto column =
        rv.childAt(colIdx)->asFlatVector<bytedance::bolt::StringView>();
    RETURN_NOT_OK(splitBinaryType(binaryIdx, *column, dstAddrs));
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitComplexType(
    const bytedance::bolt::RowVector& rv) {
  if (complexColumnIndices_.size() == 0) {
    return arrow::Status::OK();
  }
  auto numRows = rv.size();
  std::vector<std::vector<bytedance::bolt::IndexRange>> rowIndexs;
  rowIndexs.resize(numPartitions_);
  // TODO: maybe an estimated row is more reasonable
  for (auto row = 0; row < numRows; ++row) {
    auto partition = row2Partition_[row];
    if (complexTypeData_[partition] == nullptr) {
      // TODO: maybe memory issue, copy many times
      if (arenas_[partition] == nullptr) {
        arenas_[partition] =
            std::make_unique<bytedance::bolt::StreamArena>(boltPool_);
      }
      complexTypeData_[partition] = serde_.createSerializer(
          complexWriteType_,
          partition2RowCount_[partition],
          arenas_[partition].get(),
          /* serdeOptions */ nullptr);
    }
    rowIndexs[partition].emplace_back(bytedance::bolt::IndexRange{row, 1});
  }

  std::vector<bytedance::bolt::VectorPtr> children;
  children.reserve(complexColumnIndices_.size());
  for (size_t i = 0; i < complexColumnIndices_.size(); ++i) {
    auto colIdx = complexColumnIndices_[i];
    children.emplace_back(rv.childAt(colIdx));
  }
  auto rowVector = std::make_shared<bytedance::bolt::RowVector>(
      boltPool_,
      complexWriteType_,
      bytedance::bolt::BufferPtr(nullptr),
      rv.size(),
      std::move(children));

  for (auto& pid : partitionUsed_) {
    if (rowIndexs[pid].size() != 0) {
      auto beforeSize = arenas_[pid]->size();
      complexTypeData_[pid]->append(
          rowVector,
          folly::Range(rowIndexs[pid].data(), rowIndexs[pid].size()));
      auto usedSize = arenas_[pid]->size() - beforeSize;
      updateVariableMemoryUsage(pid, usedSize);
    }
  }

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::initColumnTypes(
    const bytedance::bolt::RowVector& rv) {
  schema_ = boltTypeToArrowSchema(rv.type(), boltPool_);
  for (size_t i = 0; i < rv.childrenSize(); ++i) {
    boltColumnTypes_.push_back(rv.childAt(i)->type());
  }

  VsPrintSplitLF("schema_", schema_->ToString());

  // get arrow_column_types_ from schema
  ARROW_ASSIGN_OR_RAISE(arrowColumnTypes_, toShuffleTypeId(schema_->fields()));

  std::vector<std::string> complexNames;
  std::vector<bytedance::bolt::TypePtr> complexChildrens;

  for (size_t i = 0; i < arrowColumnTypes_.size(); ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        binaryColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        complexColumnIndices_.push_back(i);
        complexNames.emplace_back(boltColumnTypes_[i]->name());
        complexChildrens.emplace_back(boltColumnTypes_[i]);
        hasComplexType_ = true;
      } break;
      case arrow::BooleanType::type_id: {
        simpleColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(true);
      } break;
      case arrow::NullType::type_id:
        break;
      default: {
        simpleColumnIndices_.push_back(i);
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }

  fixedWidthColumnCount_ = simpleColumnIndices_.size();

  simpleColumnIndices_.insert(
      simpleColumnIndices_.end(),
      binaryColumnIndices_.begin(),
      binaryColumnIndices_.end());

  printColumnsInfo();

  binaryArrayTotalSizeBytes_.resize(binaryColumnIndices_.size(), 0);

  inputHasNull_.resize(simpleColumnIndices_.size(), false);

  complexTypeData_.resize(numPartitions_);
  complexTypeFlushBuffer_.resize(numPartitions_);

  complexWriteType_ = std::make_shared<bytedance::bolt::RowType>(
      std::move(complexNames), std::move(complexChildrens));

  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::initFromRowVector(
    const bytedance::bolt::RowVector& rv) {
  if (boltColumnTypes_.empty()) {
    RETURN_NOT_OK(initColumnTypes(rv));
    RETURN_NOT_OK(initPartitions());
    calculateSimpleColumnBytes();
  }
  return arrow::Status::OK();
}

inline bool BoltShuffleWriter::beyondThreshold(
    uint32_t partitionId,
    uint32_t newSize) {
  auto currentBufferSize = partitionBufferSize_[partitionId];
  return newSize > (1 + options_.bufferReallocThreshold) * currentBufferSize ||
      newSize < (1 - options_.bufferReallocThreshold) * currentBufferSize;
}

void BoltShuffleWriter::calculateSimpleColumnBytes() {
  fixedWidthBufferBytes_ = 0;
  for (size_t col = 0; col < fixedWidthColumnCount_; ++col) {
    auto colIdx = simpleColumnIndices_[col];
    // `bool(1) >> 3` gets 0, so +7
    fixedWidthBufferBytes_ +=
        ((arrow::bit_width(arrowColumnTypes_[colIdx]->id()) + 7) >> 3);
  }
  fixedWidthBufferBytes_ +=
      kSizeOfBinaryArrayLengthBuffer * binaryColumnIndices_.size();
}

uint32_t BoltShuffleWriter::calculatePartitionBufferSize(
    const bytedance::bolt::RowVector& rv,
    int64_t memLimit) {
  auto bytesPerRow = fixedWidthBufferBytes_;

  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCalculateBufferSize]);
  auto numRows = rv.size();
  // Calculate average size bytes (bytes per row) for each binary array.
  std::vector<uint64_t> binaryArrayAvgBytesPerRow(binaryColumnIndices_.size());
  for (size_t i = 0; i < binaryColumnIndices_.size(); ++i) {
    uint64_t binarySizeBytes = 0;
    auto column = rv.childAt(binaryColumnIndices_[i])
                      ->asFlatVector<bytedance::bolt::StringView>();

    const auto* srcRawValues = column->rawValues();
    const auto* srcRawNulls = column->rawNulls();

    for (auto idx = 0; idx < numRows; idx++) {
      auto& stringView = srcRawValues[idx];
      size_t isNull =
          srcRawNulls && bytedance::bolt::bits::isBitNull(srcRawNulls, idx);
      auto stringLen = (isNull - 1) & stringView.size();
      binarySizeBytes += stringLen;
    }

    binaryArrayTotalSizeBytes_[i] += binarySizeBytes;
    binaryArrayAvgBytesPerRow[i] =
        binaryArrayTotalSizeBytes_[i] / (totalInputNumRows_ + numRows);
    bytesPerRow += binaryArrayAvgBytesPerRow[i];
  }

  // complex type
  if (hasComplexType_ &&
      (boltPool_->reservedBytes() > pool_->bytes_allocated()) &&
      totalExistingInputNumRows_ > 0) {
    bytesPerRow += (boltPool_->reservedBytes() - pool_->bytes_allocated()) /
        totalExistingInputNumRows_;
  }

  VS_PRINT_VECTOR_MAPPING(binaryArrayAvgBytesPerRow);

  VS_PRINTLF(bytesPerRow);

  memLimit += cachedPayloadSize();
  // make sure split buffer uses 128M memory at least, let's hardcode it here
  // for now
  if (memLimit < kMinMemLimit) {
    memLimit = kMinMemLimit;
  }

  uint64_t preAllocRowCnt = memLimit > 0 && bytesPerRow > 0
      ? memLimit / bytesPerRow / numPartitions_ >> 2
      : options_.bufferSize;
  preAllocRowCnt = std::min(preAllocRowCnt, (uint64_t)options_.bufferSize);
  DLOG(INFO) << "Calculated preAllocRowCnt: " << preAllocRowCnt
             << ", memLimit: " << memLimit << ", bytesPerRow: " << bytesPerRow
             << ", numPartitions: " << numPartitions_ << std::endl;

  VLOG(9) << "Calculated partition buffer size -  memLimit: " << memLimit
          << ", bytesPerRow: " << bytesPerRow
          << ", preAllocRowCnt: " << preAllocRowCnt << std::endl;

  VS_PRINTLF(preAllocRowCnt);

  totalInputNumRows_ += numRows;
  totalExistingInputNumRows_ += numRows;

  return (uint32_t)preAllocRowCnt;
}

arrow::Result<std::shared_ptr<arrow::ResizableBuffer>>
BoltShuffleWriter::allocateValidityBuffer(
    uint32_t col,
    uint32_t partitionId,
    uint32_t newSize) {
  if (inputHasNull_[col]) {
    ARROW_ASSIGN_OR_RAISE(
        auto validityBuffer,
        arrow::AllocateResizableBuffer(
            arrow::bit_util::BytesForBits(newSize),
            partitionBufferPool_.get()));
    // initialize all true once allocated
    memset(validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
    partitionValidityAddrs_[col][partitionId] = validityBuffer->mutable_data();
    return validityBuffer;
  }
  partitionValidityAddrs_[col][partitionId] = nullptr;
  return nullptr;
}

arrow::Status BoltShuffleWriter::updateValidityBuffers(
    uint32_t partitionId,
    uint32_t newSize) {
  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    // If the validity buffer is not yet allocated, allocate and fill 0xff based
    // on inputHasNull_.
    if (partitionValidityAddrs_[i][partitionId] == nullptr) {
      ARROW_ASSIGN_OR_RAISE(
          partitionBuffers_[i][partitionId][kValidityBufferIndex],
          allocateValidityBuffer(i, partitionId, newSize));
    }
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::allocatePartitionBuffer(
    uint32_t partitionId,
    uint32_t newSize) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingAllocateBuffer]);

  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
    auto& buffers = partitionBuffers_[i][partitionId];

    std::shared_ptr<arrow::ResizableBuffer> validityBuffer{};
    ARROW_ASSIGN_OR_RAISE(
        validityBuffer, allocateValidityBuffer(i, partitionId, newSize));
    switch (columnType) {
      // binary types
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        auto binaryIdx = i - fixedWidthColumnCount_;

        std::shared_ptr<arrow::ResizableBuffer> lengthBuffer{};
        auto lengthBufferSize = newSize * kSizeOfBinaryArrayLengthBuffer;
        ARROW_ASSIGN_OR_RAISE(
            lengthBuffer,
            arrow::AllocateResizableBuffer(
                lengthBufferSize, partitionBufferPool_.get()));

        std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};
        auto valueBufferSize =
            valueBufferSizeForBinaryArray(binaryIdx, newSize);
        ARROW_ASSIGN_OR_RAISE(
            valueBuffer,
            arrow::AllocateResizableBuffer(
                valueBufferSize, partitionBufferPool_.get()));

        partitionBinaryAddrs_[binaryIdx][partitionId] = BinaryBuf(
            valueBuffer->mutable_data(),
            lengthBuffer->mutable_data(),
            valueBufferSize);
        buffers = {
            std::move(validityBuffer),
            std::move(lengthBuffer),
            std::move(valueBuffer)};
        break;
      }
      case arrow::NullType::type_id: {
        break;
      }
      default: { // fixed-width types
        std::shared_ptr<arrow::ResizableBuffer> valueBuffer{};
        ARROW_ASSIGN_OR_RAISE(
            valueBuffer,
            arrow::AllocateResizableBuffer(
                valueBufferSizeForFixedWidthArray(i, newSize),
                partitionBufferPool_.get()));
        partitionFixedWidthValueAddrs_[i][partitionId] =
            valueBuffer->mutable_data();
        buffers = {std::move(validityBuffer), std::move(valueBuffer)};
        break;
      }
    }
  }
  partitionBufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::evictBuffers(
    uint32_t partitionId,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    bool reuseBuffers) {
  if (!buffers.empty()) {
    bool isRowVectorCompress =
        buffers.size() >= options_.rowvectorModeCompressionMinColumns &&
        getBufferSize(buffers) < options_.rowvectorModeCompressionMaxBufferSize;
    auto payload = std::make_unique<InMemoryPayload>(
        numRows, &isValidityBuffer_, std::move(buffers), isRowVectorCompress);
    RETURN_NOT_OK(partitionWriter_->evict(
        partitionId,
        std::move(payload),
        Evict::kCache,
        reuseBuffers,
        hasComplexType_));
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::evictPartitionBuffers(
    uint32_t partitionId,
    bool reuseBuffers) {
  auto numRows = partitionBufferBase_[partitionId];
  if (numRows > 0) {
    ARROW_ASSIGN_OR_RAISE(
        auto buffers, assembleBuffers(partitionId, reuseBuffers));
    RETURN_NOT_OK(
        evictBuffers(partitionId, numRows, std::move(buffers), reuseBuffers));
  }
  return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
BoltShuffleWriter::assembleBuffers(uint32_t partitionId, bool reuseBuffers) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingCreateRbFromBuffer]);

  if (partitionBufferBase_[partitionId] == 0) {
    return std::vector<std::shared_ptr<arrow::Buffer>>{};
  }

  auto numRows = partitionBufferBase_[partitionId];
  auto fixedWidthIdx = 0;
  auto binaryIdx = 0;
  auto numFields = schema_->num_fields();

  std::vector<std::shared_ptr<arrow::Array>> arrays(numFields);
  std::vector<std::shared_ptr<arrow::Buffer>> allBuffers;
  // One column should have 2 buffers at least, string column has 3 column
  // buffers.
  allBuffers.reserve(
      fixedWidthColumnCount_ * 2 + binaryColumnIndices_.size() * 3 +
      hasComplexType_);
  for (int i = 0; i < numFields; ++i) {
    switch (arrowColumnTypes_[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        const auto& buffers =
            partitionBuffers_[fixedWidthColumnCount_ + binaryIdx][partitionId];
        auto& binaryBuf = partitionBinaryAddrs_[binaryIdx][partitionId];
        // validity buffer
        if (buffers[kValidityBufferIndex] != nullptr) {
          auto validityBufferSize = arrow::bit_util::BytesForBits(numRows);
          if (reuseBuffers) {
            allBuffers.push_back(arrow::SliceBuffer(
                buffers[kValidityBufferIndex],
                0,
                arrow::bit_util::BytesForBits(numRows)));
          } else {
            RETURN_NOT_OK(buffers[kValidityBufferIndex]->Resize(
                validityBufferSize, true));
            allBuffers.push_back(std::move(buffers[kValidityBufferIndex]));
          }
        } else {
          allBuffers.push_back(nullptr);
        }
        // Length buffer.
        auto lengthBufferSize = numRows * kSizeOfBinaryArrayLengthBuffer;
        ARROW_RETURN_IF(
            !buffers[kBinaryLengthBufferIndex],
            arrow::Status::Invalid("Offset buffer of binary array is null."));
        if (reuseBuffers) {
          allBuffers.push_back(arrow::SliceBuffer(
              buffers[kBinaryLengthBufferIndex], 0, lengthBufferSize));
        } else {
          RETURN_NOT_OK(buffers[kBinaryLengthBufferIndex]->Resize(
              lengthBufferSize, true));
          allBuffers.push_back(std::move(buffers[kBinaryLengthBufferIndex]));
        }

        // Value buffer.
        auto valueBufferSize = binaryBuf.valueOffset;
        ARROW_RETURN_IF(
            !buffers[kBinaryValueBufferIndex],
            arrow::Status::Invalid("Value buffer of binary array is null."));
        if (reuseBuffers) {
          allBuffers.push_back(arrow::SliceBuffer(
              buffers[kBinaryValueBufferIndex], 0, valueBufferSize));
        } else if (valueBufferSize > 0) {
          RETURN_NOT_OK(
              buffers[kBinaryValueBufferIndex]->Resize(valueBufferSize, true));
          allBuffers.push_back(std::move(buffers[kBinaryValueBufferIndex]));
        } else {
          // Binary value buffer size can be 0, in which case cannot be resized.
          allBuffers.push_back(zeroLengthNullBuffer());
        }

        if (reuseBuffers) {
          // Set the first value offset to 0.
          binaryBuf.valueOffset = 0;
        }
        binaryIdx++;
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
        break;
      case arrow::NullType::type_id: {
        break;
      }
      default: {
        auto& buffers = partitionBuffers_[fixedWidthIdx][partitionId];
        // validity buffer
        if (buffers[kValidityBufferIndex] != nullptr) {
          auto validityBufferSize = arrow::bit_util::BytesForBits(numRows);
          if (reuseBuffers) {
            allBuffers.push_back(arrow::SliceBuffer(
                buffers[kValidityBufferIndex],
                0,
                arrow::bit_util::BytesForBits(numRows)));
          } else {
            RETURN_NOT_OK(buffers[kValidityBufferIndex]->Resize(
                validityBufferSize, true));
            allBuffers.push_back(std::move(buffers[kValidityBufferIndex]));
          }
        } else {
          allBuffers.push_back(nullptr);
        }
        // Value buffer.
        uint64_t valueBufferSize = 0;
        auto& valueBuffer = buffers[kFixedWidthValueBufferIndex];
        ARROW_RETURN_IF(
            !valueBuffer,
            arrow::Status::Invalid(
                "Value buffer of fixed-width array is null."));
        if (arrowColumnTypes_[i]->id() == arrow::BooleanType::type_id) {
          valueBufferSize = arrow::bit_util::BytesForBits(numRows);
        } else if (boltColumnTypes_[i]->isShortDecimal()) {
          valueBufferSize =
              numRows * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
        } else if (
            boltColumnTypes_[i]->kind() ==
            bytedance::bolt::TypeKind::TIMESTAMP) {
          valueBufferSize =
              bytedance::bolt::BaseVector::byteSize<bytedance::bolt::Timestamp>(
                  numRows);
        } else {
          valueBufferSize =
              numRows * (arrow::bit_width(arrowColumnTypes_[i]->id()) >> 3);
        }
        if (reuseBuffers) {
          auto slicedValueBuffer =
              arrow::SliceBuffer(valueBuffer, 0, valueBufferSize);
          allBuffers.push_back(std::move(slicedValueBuffer));
        } else {
          RETURN_NOT_OK(buffers[kFixedWidthValueBufferIndex]->Resize(
              valueBufferSize, true));
          allBuffers.push_back(std::move(buffers[kFixedWidthValueBufferIndex]));
        }
        fixedWidthIdx++;
        break;
      }
    }
  }
  if (hasComplexType_ && complexTypeData_[partitionId] != nullptr) {
    auto serializedSize = complexTypeData_[partitionId]->maxSerializedSize();
    ARROW_ASSIGN_OR_RAISE(
        auto flushBuffer,
        arrow::AllocateBuffer(serializedSize, spillArrowPool_));
    std::shared_ptr<arrow::Buffer> valueBuffer = std::move(flushBuffer);
    auto output =
        std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
    bytedance::bolt::serializer::presto::PrestoOutputStreamListener listener;
    ArrowFixedSizeBufferOutputStream out(output, &listener);
    complexTypeData_[partitionId]->flush(&out);
    allBuffers.emplace_back(valueBuffer);
    complexTypeData_[partitionId] = nullptr;
    arenas_[partitionId] = nullptr;
  }

  partitionBufferBase_[partitionId] = 0;
  if (!reuseBuffers) {
    RETURN_NOT_OK(resetPartitionBuffer(partitionId));
  }
  totalExistingInputNumRows_ -= numRows;
  return allBuffers;
}

arrow::Status BoltShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable) {
    *actual = 0;
    return arrow::Status::OK();
  }

  if (vectorLayout_ == RowVectorLayout::kComposite) {
    if (splitState_ == SplitState::kStop || !isCompositeInitialized_) {
      *actual = 0;
      return arrow::Status::OK();
    }
  }

  EvictGuard evictGuard{evictState_};

  if (vectorLayout_ == RowVectorLayout::kComposite) {
    RETURN_NOT_OK(reclaimFixedSizeInCompositeLayout(actual));
  } else {
    partitionWriter_->setRowFormat(false);
    int64_t reclaimed = 0;
    if (reclaimed < size) {
      ARROW_ASSIGN_OR_RAISE(auto cached, evictCachedPayload(size - reclaimed));
      reclaimed += cached;
    }
    if (reclaimed < size && shrinkPartitionBuffersAfterSpill()) {
      ARROW_ASSIGN_OR_RAISE(
          auto shrunken, shrinkPartitionBuffersMinSize(size - reclaimed));
      reclaimed += shrunken;
    }
    if (reclaimed < size && evictPartitionBuffersAfterSpill()) {
      ARROW_ASSIGN_OR_RAISE(
          auto evicted, evictPartitionBuffersMinSize(size - reclaimed));
      reclaimed += evicted;
    }
    *actual = reclaimed;
  }
  return arrow::Status::OK();
}

arrow::Result<int64_t> BoltShuffleWriter::evictCachedPayload(int64_t size) {
  SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingEvictPartition]);
  int64_t actual;
  auto before = partitionBufferPool_->bytes_allocated();
  RETURN_NOT_OK(partitionWriter_->reclaimFixedSize(size, &actual));
  // Need to count the changes from partitionBufferPool as well.
  // When the evicted partition buffers are not copied, the merged ones
  // are resized from the original buffers thus allocated from
  // partitionBufferPool.
  actual += before - partitionBufferPool_->bytes_allocated();

  DLOG(INFO) << "Evicted all cached payloads. " << std::to_string(actual)
             << " bytes released" << std::endl;
  return actual;
}

arrow::Status BoltShuffleWriter::resetValidityBuffer(uint32_t partitionId) {
  std::for_each(
      partitionBuffers_.begin(),
      partitionBuffers_.end(),
      [partitionId](auto& bufs) {
        if (bufs[partitionId].size() != 0 &&
            bufs[partitionId][kValidityBufferIndex] != nullptr) {
          // initialize all true once allocated
          auto validityBuffer = bufs[partitionId][kValidityBufferIndex];
          memset(
              validityBuffer->mutable_data(), 0xff, validityBuffer->capacity());
        }
      });
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::resizePartitionBuffer(
    uint32_t partitionId,
    uint32_t newSize,
    bool preserveData) {
  for (auto i = 0; i < simpleColumnIndices_.size(); ++i) {
    auto columnType = schema_->field(simpleColumnIndices_[i])->type()->id();
    auto& buffers = partitionBuffers_[i][partitionId];

    // Handle validity buffer first.
    auto& validityBuffer = buffers[kValidityBufferIndex];
    if (!preserveData) {
      ARROW_ASSIGN_OR_RAISE(
          validityBuffer, allocateValidityBuffer(i, partitionId, newSize));
    } else if (buffers[kValidityBufferIndex]) {
      // Resize validity.
      auto filled = validityBuffer->capacity();
      RETURN_NOT_OK(
          validityBuffer->Resize(arrow::bit_util::BytesForBits(newSize)));
      partitionValidityAddrs_[i][partitionId] = validityBuffer->mutable_data();

      // If newSize is larger, fill 1 to the newly allocated bytes.
      if (validityBuffer->capacity() > filled) {
        memset(
            validityBuffer->mutable_data() + filled,
            0xff,
            validityBuffer->capacity() - filled);
      }
    }

    // Resize value buffer if fixed-width, offset & value buffers if binary.
    switch (columnType) {
      // binary types
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // Resize length buffer.
        auto binaryIdx = i - fixedWidthColumnCount_;
        auto& binaryBuf = partitionBinaryAddrs_[binaryIdx][partitionId];
        auto& lengthBuffer = buffers[kBinaryLengthBufferIndex];
        ARROW_RETURN_IF(
            !lengthBuffer,
            arrow::Status::Invalid("Offset buffer of binary array is null."));
        RETURN_NOT_OK(
            lengthBuffer->Resize(newSize * kSizeOfBinaryArrayLengthBuffer));

        // Skip Resize value buffer if the spill is triggered by resizing this
        // split binary buffer. Only update length buffer ptr.
        if (binaryArrayResizeState_.inResize &&
            partitionId == binaryArrayResizeState_.partitionId &&
            binaryIdx == binaryArrayResizeState_.binaryIdx) {
          binaryBuf.lengthPtr = lengthBuffer->mutable_data();
          break;
        }

        // Resize value buffer.
        auto& valueBuffer = buffers[kBinaryValueBufferIndex];
        ARROW_RETURN_IF(
            !valueBuffer,
            arrow::Status::Invalid("Value buffer of binary array is null."));
        // Determine the new Size for value buffer.
        auto valueBufferSize =
            valueBufferSizeForBinaryArray(binaryIdx, newSize);
        // If shrink is triggered by spill, and binary new size is larger, do
        // not resize the buffer to avoid issuing another spill. Only update
        // length buffer ptr.
        if (evictState_ == EvictState::kUnevictable &&
            newSize <= partitionBufferSize_[partitionId] &&
            valueBufferSize >= valueBuffer->size()) {
          binaryBuf.lengthPtr = lengthBuffer->mutable_data();
          break;
        }
        auto valueOffset = 0;
        // If preserve data, the new valueBufferSize should not be smaller than
        // the current offset.
        if (preserveData) {
          valueBufferSize = std::max(binaryBuf.valueOffset, valueBufferSize);
          valueOffset = binaryBuf.valueOffset;
        }
        RETURN_NOT_OK(valueBuffer->Resize(valueBufferSize));

        binaryBuf = BinaryBuf(
            valueBuffer->mutable_data(),
            lengthBuffer->mutable_data(),
            valueBufferSize,
            valueOffset);
        break;
      }
      case arrow::NullType::type_id:
        break;
      default: { // fixed-width types
        auto& valueBuffer = buffers[kFixedWidthValueBufferIndex];
        ARROW_RETURN_IF(
            !valueBuffer,
            arrow::Status::Invalid(
                "Value buffer of fixed-width array is null."));
        RETURN_NOT_OK(
            valueBuffer->Resize(valueBufferSizeForFixedWidthArray(i, newSize)));
        partitionFixedWidthValueAddrs_[i][partitionId] =
            valueBuffer->mutable_data();
        break;
      }
    }
  }
  partitionBufferSize_[partitionId] = newSize;
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::shrinkPartitionBuffer(uint32_t partitionId) {
  auto bufferSize = partitionBufferSize_[partitionId];
  if (bufferSize == 0) {
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(
      auto newSize, partitionBufferSizeAfterShrink(partitionId));
  if (newSize > bufferSize) {
    std::stringstream invalid;
    invalid << "Cannot shrink to larger size. Partition: " << partitionId
            << ", before shrink: " << bufferSize << ", after shrink" << newSize;
    return arrow::Status::Invalid(invalid.str());
  }
  if (newSize == bufferSize) {
    // No space to shrink.
    return arrow::Status::OK();
  }
  if (newSize == 0) {
    return resetPartitionBuffer(partitionId);
  }
  return resizePartitionBuffer(partitionId, newSize, /*preserveData=*/true);
}

uint64_t BoltShuffleWriter::valueBufferSizeForBinaryArray(
    uint32_t binaryIdx,
    uint32_t newSize) {
  return (binaryArrayTotalSizeBytes_[binaryIdx] + totalInputNumRows_ - 1) /
      totalInputNumRows_ * newSize +
      1024;
}

uint64_t BoltShuffleWriter::valueBufferSizeForFixedWidthArray(
    uint32_t fixedWidthIndex,
    uint32_t newSize) {
  uint64_t valueBufferSize = 0;
  auto columnIdx = simpleColumnIndices_[fixedWidthIndex];
  if (arrowColumnTypes_[columnIdx]->id() == arrow::BooleanType::type_id) {
    valueBufferSize = arrow::bit_util::BytesForBits(newSize);
  } else if (boltColumnTypes_[columnIdx]->isShortDecimal()) {
    valueBufferSize =
        newSize * (arrow::bit_width(arrow::Int64Type::type_id) >> 3);
  } else if (
      boltColumnTypes_[columnIdx]->kind() ==
      bytedance::bolt::TypeKind::TIMESTAMP) {
    valueBufferSize =
        bytedance::bolt::BaseVector::byteSize<bytedance::bolt::Timestamp>(
            newSize);
  } else {
    valueBufferSize =
        newSize * (arrow::bit_width(arrowColumnTypes_[columnIdx]->id()) >> 3);
  }
  return valueBufferSize;
}

void BoltShuffleWriter::stat() const {
#if BOLT_SHUFFLE_WRITER_LOG_FLAG
  for (int i = CpuWallTimingBegin; i != CpuWallTimingEnd; ++i) {
    std::ostringstream oss;
    auto& timing = cpuWallTimingList_[i];
    oss << "Bolt shuffle writer stat:"
        << CpuWallTimingName((CpuWallTimingType)i);
    oss << " " << timing.toString();
    if (timing.count > 0) {
      oss << " wallNanos-avg:" << timing.wallNanos / timing.count;
      oss << " cpuNanos-avg:" << timing.cpuNanos / timing.count;
    }
    LOG(INFO) << oss.str();
  }
#endif
}

arrow::Status BoltShuffleWriter::resetPartitionBuffer(uint32_t partitionId) {
  // Reset fixed-width partition buffers
  for (auto i = 0; i < fixedWidthColumnCount_; ++i) {
    partitionValidityAddrs_[i][partitionId] = nullptr;
    partitionFixedWidthValueAddrs_[i][partitionId] = nullptr;
    partitionBuffers_[i][partitionId].clear();
  }

  // Reset binary partition buffers
  for (auto i = 0; i < binaryColumnIndices_.size(); ++i) {
    auto binaryIdx = i + fixedWidthColumnCount_;
    partitionValidityAddrs_[binaryIdx][partitionId] = nullptr;
    partitionBinaryAddrs_[i][partitionId] = BinaryBuf();
    partitionBuffers_[binaryIdx][partitionId].clear();
  }

  partitionBufferSize_[partitionId] = 0;
  return arrow::Status::OK();
}

const uint64_t BoltShuffleWriter::cachedPayloadSize() const {
  return partitionWriter_->cachedPayloadSize();
}

arrow::Result<int64_t> BoltShuffleWriter::shrinkPartitionBuffersMinSize(
    int64_t size) {
  // Sort partition buffers by (partitionBufferSize_ - partitionBufferBase_)
  std::vector<std::pair<uint32_t, uint32_t>> pidToSize;
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionBufferSize_[pid] > 0 &&
        partitionBufferSize_[pid] > partitionBufferBase_[pid]) {
      pidToSize.emplace_back(
          pid, partitionBufferSize_[pid] - partitionBufferBase_[pid]);
    }
  }
  // No shrinkable partition buffer.
  if (pidToSize.empty()) {
    return 0;
  }

  std::sort(
      pidToSize.begin(), pidToSize.end(), [&](const auto& a, const auto& b) {
        return a.second > b.second;
      });

  auto beforeShrink = partitionBufferPool_->bytes_allocated();
  auto shrunken = 0;
  auto iter = pidToSize.begin();

  // Shrink in order to reclaim the largest amount of space with fewer resizes.
  do {
    RETURN_NOT_OK(shrinkPartitionBuffer(iter->first));
    shrunken = beforeShrink - partitionBufferPool_->bytes_allocated();
    iter++;
  } while (shrunken < size && iter != pidToSize.end());
  return shrunken;
}

arrow::Result<int64_t> BoltShuffleWriter::evictPartitionBuffersMinSize(
    int64_t size) {
  // Evict partition buffers, only when splitState_ == SplitState::kInit, and
  // space freed from shrinking is not enough. In this case partitionBufferSize_
  // == partitionBufferBase_
  int64_t beforeEvict = partitionBufferPool_->bytes_allocated();
  int64_t evicted = 0;
  std::vector<std::pair<uint32_t, uint32_t>> pidToSize;
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    if (partitionBufferSize_[pid] == 0) {
      continue;
    }
    pidToSize.emplace_back(pid, partitionBufferSize_[pid]);
  }
  if (!pidToSize.empty()) {
    for (auto& item : pidToSize) {
      auto pid = item.first;
      ARROW_ASSIGN_OR_RAISE(auto buffers, assembleBuffers(pid, false));
      bool isRowVectorCompress =
          buffers.size() >= options_.rowvectorModeCompressionMinColumns &&
          getBufferSize(buffers) <
              options_.rowvectorModeCompressionMaxBufferSize;
      auto payload = std::make_unique<InMemoryPayload>(
          item.second,
          &isValidityBuffer_,
          std::move(buffers),
          isRowVectorCompress);
      RETURN_NOT_OK(partitionWriter_->evict(
          pid, std::move(payload), Evict::kSpill, false, hasComplexType_));
      evicted = beforeEvict - partitionBufferPool_->bytes_allocated();
      variableMemoryUsage_[pid] = 0;
      if (evicted >= size) {
        break;
      }
    }
    maxVariableMemoryUsage_ = *std::max_element(
        variableMemoryUsage_.begin(), variableMemoryUsage_.end());
  }
  return evicted;
}

bool BoltShuffleWriter::shrinkPartitionBuffersAfterSpill() const {
  // If OOM happens during SplitState::kSplit, it is triggered by binary buffers
  // resize. Or during SplitState::kInit, it is triggered by other operators.
  // The reclaim order is spill->shrink, because the partition buffers can be
  // reused. SinglePartitioning doesn't maintain partition buffers.
  return options_.partitioning != Partitioning::kSingle &&
      (splitState_ == SplitState::kSplit || splitState_ == SplitState::kInit);
}

bool BoltShuffleWriter::evictPartitionBuffersAfterSpill() const {
  // If OOM triggered by other operators, the splitState_ is SplitState::kInit.
  // The last resort is to evict the partition buffers to reclaim more space.
  return options_.partitioning != Partitioning::kSingle &&
      splitState_ == SplitState::kInit;
}

arrow::Result<uint32_t> BoltShuffleWriter::partitionBufferSizeAfterShrink(
    uint32_t partitionId) const {
  if (splitState_ == SplitState::kSplit) {
    return partitionBufferBase_[partitionId] + partition2RowCount_[partitionId];
  }
  if (splitState_ == kInit || splitState_ == SplitState::kStop ||
      splitState_ == SplitState::kEvictBigBuffer) {
    return partitionBufferBase_[partitionId];
  }
  return arrow::Status::Invalid(
      "Cannot shrink partition buffers in SplitState: " +
      std::to_string(splitState_));
}

arrow::Status BoltShuffleWriter::preAllocPartitionBuffers(
    uint32_t preAllocBufferSize) {
  for (auto& pid : partitionUsed_) {
    auto newSize = std::max(preAllocBufferSize, partition2RowCount_[pid]);
    VLOG_IF(9, partitionBufferSize_[pid] != newSize)
        << "Actual partition buffer size - current: "
        << partitionBufferSize_[pid] << ", newSize: " << newSize << std::endl;
    // Make sure the size to be allocated is larger than the size to be filled.
    if (partitionBufferSize_[pid] == 0) {
      // Allocate buffer if it's not yet allocated.
      RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
    } else if (beyondThreshold(pid, newSize)) {
      if (newSize <= partitionBufferBase_[pid]) {
        // If the newSize is smaller, cache the buffered data and reuse and
        // shrink the buffer.
        RETURN_NOT_OK(evictPartitionBuffers(pid, true));
        RETURN_NOT_OK(
            resizePartitionBuffer(pid, newSize, /*preserveData=*/false));
      } else {
        // If the newSize is larger, check if alreadyFilled + toBeFilled <=
        // newSize
        if (partitionBufferBase_[pid] + partition2RowCount_[pid] <= newSize) {
          // If so, keep the data in buffers and resize buffers.
          RETURN_NOT_OK(
              resizePartitionBuffer(pid, newSize, /*preserveData=*/true));
          // Because inputHasNull_ is updated every time split is called, and
          // resizePartitionBuffer won't allocate validity buffer.
          RETURN_NOT_OK(updateValidityBuffers(pid, newSize));
        } else {
          // Otherwise cache the buffered data.
          // If newSize <= allocated buffer size, reuse and shrink the buffer.
          // Else free and allocate new buffers.
          bool reuseBuffers = newSize <= partitionBufferSize_[pid];
          RETURN_NOT_OK(evictPartitionBuffers(pid, reuseBuffers));
          if (reuseBuffers) {
            RETURN_NOT_OK(
                resizePartitionBuffer(pid, newSize, /*preserveData=*/false));
          } else {
            RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
          }
        }
      }
    } else if (
        partitionBufferBase_[pid] + partition2RowCount_[pid] >
        partitionBufferSize_[pid]) {
      // If the size to be filled + already filled > the buffer size, need to
      // free current buffers and allocate new buffer.
      if (newSize > partitionBufferSize_[pid]) {
        // If the partition size after split is already larger than allocated
        // buffer size, need reallocate.
        RETURN_NOT_OK(evictPartitionBuffers(pid, false));
        RETURN_NOT_OK(allocatePartitionBuffer(pid, newSize));
      } else {
        // Partition size after split is smaller than buffer size. Reuse the
        // buffers.
        RETURN_NOT_OK(evictPartitionBuffers(pid, true));
        // Reset validity buffer for reallocate.
        RETURN_NOT_OK(resetValidityBuffer(pid));
      }
    }

    // some big buffer should be evicted directly before next epoch
    if (partition2RowCount_[pid] > preAllocBufferSize) {
      needEvicted_.insert(pid);
    }
  }
  return arrow::Status::OK();
}

template arrow::Status BoltShuffleWriter::splitFixedWidthValueBuffer<
    std::vector<std::vector<uint8_t*>>>(
    const bytedance::bolt::RowVector& rv,
    std::vector<std::vector<uint8_t*>>& valueAddrs);
template arrow::Status BoltShuffleWriter::splitFixedWidthValueBuffer<
    std::vector<std::vector<std::vector<uint8_t*>>>>(
    const bytedance::bolt::RowVector& rv,
    std::vector<std::vector<std::vector<uint8_t*>>>& valueAddrs);
template arrow::Status BoltShuffleWriter::splitValidityBuffer<true>(
    const bytedance::bolt::RowVector& rv);
template arrow::Status BoltShuffleWriter::splitValidityBuffer<false>(
    const bytedance::bolt::RowVector& rv);

// for V2
bytedance::bolt::RowVectorPtr BoltShuffleWriter::getStrippedRowVectorWrapper(
    const bytedance::bolt::RowVector& rv) {
  return getStrippedRowVector(rv);
}

const int32_t* BoltShuffleWriter::getFirstColumnWrapper(
    const bytedance::bolt::RowVector& rv) {
  return getFirstColumn(rv);
}

int32_t BoltShuffleWriter::calculatePreallocBufferSize(
    int64_t firstBatchRowNumber,
    int64_t firstBatchFlatSize,
    int64_t memLimit,
    int32_t numPartitions,
    int32_t bufferSize) {
  if (!firstBatchRowNumber) {
    return std::numeric_limits<int32_t>::max();
  }
  if (memLimit < kMinMemLimit) {
    memLimit = kMinMemLimit;
  }

  int32_t bytesPerRow = firstBatchFlatSize / firstBatchRowNumber;
  int32_t preAllocRowCnt = memLimit > 0 && bytesPerRow > 0
      ? memLimit / bytesPerRow / numPartitions >> 2
      : bufferSize;
  DLOG(INFO) << "BoltShuffleWriter::calculatePreallocBufferSize: "
             << "firstBatchRowNumber: " << firstBatchRowNumber
             << ", firstBatchFlatSize: " << firstBatchFlatSize
             << ", bytesPerRow: " << bytesPerRow
             << ", preAllocRowCnt: " << preAllocRowCnt
             << ", memLimit: " << memLimit << ", bufferSize: " << bufferSize;
  preAllocRowCnt = std::min(preAllocRowCnt, bufferSize);

  return preAllocRowCnt;
}

// for CompositeRowVector
arrow::Status BoltShuffleWriter::tryEvict(int64_t) {
  if (vectorLayout_ == RowVectorLayout::kColumnar) {
    partitionWriter_->setRowFormat(false);
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      RETURN_NOT_OK(evictPartitionBuffers(pid, false));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto released, evictCachedPayload(std::numeric_limits<int64_t>::max()));
    std::fill(partitionBufferSize_.begin(), partitionBufferSize_.end(), 0);
    maxVariableMemoryUsage_ = 0;
    boltPool_->release();
  } else {
    RETURN_NOT_OK(tryEvictComposite());
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::initFromRowVectorForComposite(
    const bytedance::bolt::RowVector& rv) {
  compositeRowVectorConverter_ =
      std::make_unique<ShuffleRowToRowConverter>(boltPool_);
  sortedRows_.resize(numPartitions_);
  partitionBytes_.resize(numPartitions_, 0);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::splitCompositeVector(
    bytedance::bolt::RowVectorPtr rowVector,
    int64_t memLimit) {
  bytedance::bolt::CompositeRowVectorPtr rv;
  vectorLayout_ = RowVectorLayout::kComposite;
  {
    bytedance::bolt::NanosecondTimer timer(&flattenTime_);
    ensurePartialFlatten(rowVector, {0});
    rv = std::dynamic_pointer_cast<bytedance::bolt::CompositeRowVector>(
        rowVector);
    BOLT_CHECK(rv != nullptr && partitioner_->hasPid());
    // if memLimit is too small, tryEvict to free memory
    if (rv->totalRowSize() > memLimit) {
      RETURN_NOT_OK(tryEvict());
    }
    if (!isCompositeInitialized_) {
      // Caution: rv is not stripped and the first column is pid
      RETURN_NOT_OK(initFromRowVectorForComposite(*rv));
      isCompositeInitialized_ = true;
    }
  }

  {
    bytedance::bolt::NanosecondTimer timer(&computePidTime_);
    auto pidArr = getFirstColumnWrapper(*rv);
    RETURN_NOT_OK(partitioner_->compute(
        pidArr, rv->size(), row2Partition_, partition2RowCount_));
    setSplitState(SplitState::kSplit);
  }

  {
    bytedance::bolt::NanosecondTimer timer(&convertTime_);
    compositeRowVectorConverter_->convert(rv, row2Partition_, sortedRows_);
  }

  if (requestSpill_) {
    RETURN_NOT_OK(tryEvict());
  }
  setSplitState(SplitState::kInit);
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::reclaimFixedSizeInCompositeLayout(
    int64_t* actual) {
  if (splitState_ == SplitState::kInit) {
    auto memoryBefore = partitionBufferSize() + boltPool_->currentBytes();
    RETURN_NOT_OK(tryEvict());
    *actual =
        memoryBefore - (partitionBufferSize() + boltPool_->currentBytes());
    if (*actual < 0) [[unlikely]] {
      LOG(INFO) << __FUNCTION__ << ": reclaim negative memory " << *actual;
      *actual = 0;
    }
  } else {
    requestSpill_ = true;
    *actual = 0;
  }
  return arrow::Status::OK();
}

arrow::Status BoltShuffleWriter::tryEvictComposite() {
  partitionWriter_->setRowFormat(true);
  std::vector<int64_t> dummy;
  RETURN_NOT_OK(partitionWriter_->evict(sortedRows_, dummy, true));
  compositeRowVectorConverter_->reset();
  boltPool_->release();
  requestSpill_ = false;
  return arrow::Status::OK();
}

arrow::MemoryPool* BoltShuffleWriter::getSpillArrowPool(
    arrow::MemoryPool* pool) {
  if (dynamic_cast<BoltArrowMemoryPool*>(pool) != nullptr) {
    // If the pool is BoltArrowMemoryPool, shuffle is offload as bolt operator
    static std::shared_ptr<arrow::MemoryPool> spillPool =
        std::make_shared<BoltArrowMemoryPool>(
            bytedance::bolt::memory::spillMemoryPool());
    return spillPool.get();
  } else {
    return pool;
  }
}

} // namespace bytedance::bolt::shuffle::sparksql
