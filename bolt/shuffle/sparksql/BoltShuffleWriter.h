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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "bolt/common/time/CpuWallTimer.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorStream.h"

#include <arrow/array/util.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/ShuffleColumnarToRowConverter.h"
#include "bolt/shuffle/sparksql/ShuffleWriter.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/partition_writer/PartitionWriter.h"
#include "bolt/shuffle/sparksql/partitioner/Partitioner.h"
namespace bytedance::bolt::shuffle::sparksql {

static inline void fastCopy(void* dst, const void* src, size_t n) {
  bytedance::bolt::simd::memcpy(dst, src, n);
}

#define START_TIMING(timing)                                     \
  {                                                              \
    auto ptiming = &timing;                                      \
    bytedance::bolt::DeltaCpuWallTimer timer{                    \
        [ptiming](const bytedance::bolt::CpuWallTiming& delta) { \
          ptiming->add(delta);                                   \
        }};

#define END_TIMING() }

#define SCOPED_TIMER(timing)                                   \
  auto ptiming = &timing;                                      \
  bytedance::bolt::DeltaCpuWallTimer timer{                    \
      [ptiming](const bytedance::bolt::CpuWallTiming& delta) { \
        ptiming->add(delta);                                   \
      }};

// Print related code has already removed in cpp/core/utils/Print.h, so we just
// keep empty definitions here.
#define VsPrint(...) // NOLINT
#define VsPrintLF(...) // NOLINT
#define VsPrintSplit(...) // NOLINT
#define VsPrintSplitLF(...) // NOLINT
#define VsPrintVectorRange(...) // NOLINT
#define VS_PRINT(a)
#define VS_PRINTLF(a)
#define VS_PRINT_FUNCTION_NAME()
#define VS_PRINT_FUNCTION_SPLIT_LINE()
#define VS_PRINT_CONTAINER(c)
#define VS_PRINT_CONTAINER_TO_STRING(c)
#define VS_PRINT_CONTAINER_2_STRING(c)
#define VS_PRINT_VECTOR_TO_STRING(v)
#define VS_PRINT_VECTOR_2_STRING(v)
#define VS_PRINT_VECTOR_MAPPING(v)

enum SplitState { kInit, kPreAlloc, kSplit, kEvictBigBuffer, kStop };
enum EvictState { kEvictable, kUnevictable };

struct BinaryArrayResizeState {
  bool inResize;
  uint32_t partitionId;
  uint32_t binaryIdx;

  BinaryArrayResizeState() : inResize(false) {}
  BinaryArrayResizeState(uint32_t partitionId, uint32_t binaryIdx)
      : inResize(false), partitionId(partitionId), binaryIdx(binaryIdx) {}
};

class EvictGuard {
 public:
  explicit EvictGuard(EvictState& evictState) : evictState_(evictState) {
    evictState_ = EvictState::kUnevictable;
  }

  ~EvictGuard() {
    evictState_ = EvictState::kEvictable;
  }

  // For safety and clarity.
  EvictGuard(const EvictGuard&) = delete;
  EvictGuard& operator=(const EvictGuard&) = delete;
  EvictGuard(EvictGuard&&) = delete;
  EvictGuard& operator=(EvictGuard&&) = delete;

 private:
  EvictState& evictState_;
};

class BoltShuffleWriter : public ShuffleWriter {
  enum {
    kValidityBufferIndex = 0,
    kFixedWidthValueBufferIndex = 1,
    kBinaryValueBufferIndex = 2,
    kBinaryLengthBufferIndex = kFixedWidthValueBufferIndex
  };

 public:
  struct BinaryBuf {
    BinaryBuf(
        uint8_t* value,
        uint8_t* length,
        uint64_t valueCapacityIn,
        uint64_t valueOffsetIn)
        : valuePtr(value),
          lengthPtr(length),
          valueCapacity(valueCapacityIn),
          valueOffset(valueOffsetIn) {}

    BinaryBuf(uint8_t* value, uint8_t* length, uint64_t valueCapacity)
        : BinaryBuf(value, length, valueCapacity, 0) {}

    BinaryBuf() : BinaryBuf(nullptr, nullptr, 0) {}

    uint8_t* valuePtr;
    uint8_t* lengthPtr;
    uint64_t valueCapacity;
    uint64_t valueOffset;
  };

  // Factory method to create a BoltShuffleWriter instance depending on the
  // options and input RowVector. May return
  // BoltShuffleWriter/BoltShuffleWriterV2 or RowBasedSortShuffleWriter.
  static std::shared_ptr<BoltShuffleWriter> create(
      ShuffleWriterOptions options,
      int32_t numColumnsExludePid,
      int64_t firstBatchRowNumber,
      int64_t firstBatchFlatSize,
      int64_t memLimit,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  // create a BoltShuffleWriter instance
  static std::shared_ptr<BoltShuffleWriter> createDefault(
      ShuffleWriterOptions options,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* arrowPool);

  static void ensureVectorLoaded(bytedance::bolt::RowVectorPtr rv);

  virtual arrow::Status split(
      bytedance::bolt::RowVectorPtr rv,
      int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  const uint64_t cachedPayloadSize() const override;

  arrow::Status evictPartitionBuffers(uint32_t partitionId, bool reuseBuffers);

  int64_t rawPartitionBytes() const;

  // For test only.
  void setPartitionBufferSize(uint32_t newSize);

  // for debugging
  void printColumnsInfo() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINTLF(fixed_width_column_count_);

    VS_PRINT_CONTAINER(simple_column_indices_);
    VS_PRINT_CONTAINER(binary_column_indices_);
    VS_PRINT_CONTAINER(complex_column_indices_);

    VS_PRINT_VECTOR_2_STRING(bolt_column_types_);
    VS_PRINT_VECTOR_TO_STRING(arrow_column_types_);
  }

  void printPartition() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    // row ID -> partition ID
    VS_PRINT_VECTOR_MAPPING(row_2_partition_);

    // partition -> row count
    VS_PRINT_VECTOR_MAPPING(partition_2_row_count_);
  }

  void printPartitionBuffer() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition_2_buffer_size_);
    VS_PRINT_VECTOR_MAPPING(partitionBufferBase_);
  }

  void printPartition2Row() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition2RowOffsetBase_);

#if BOLT_SHUFFLE_WRITER_PRINT
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      auto begin = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      VsPrint("partition", pid);
      VsPrintVectorRange(rowOffset2RowId_, begin, end);
    }
#endif
  }

  void printInputHasNull() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_CONTAINER(input_has_null_);
  }

  virtual ~BoltShuffleWriter() {}

  static int32_t calculatePreallocBufferSize(
      int64_t firstBatchRowNumber,
      int64_t firstBatchFlatSize,
      int64_t memLimit,
      int32_t numPartitions,
      int32_t bufferSize);

  // Returns a shared_ptr to the Arrow memory pool used for spilling.
  // If shuffle offload is enabled, it returns a wrapper of spillMemoryPool(),
  // otherwise it returns the original Arrow memory pool.
  static arrow::MemoryPool* getSpillArrowPool(arrow::MemoryPool* pool);

  BoltShuffleWriter(
      ShuffleWriterOptions options,
      bytedance::bolt::memory::MemoryPool* boltPool,
      arrow::MemoryPool* pool)
      : ShuffleWriter(
            options.partitionWriterOptions.numPartitions,
            PartitionWriter::create(
                options.partitionWriterOptions,
                getSpillArrowPool(pool)),
            options,
            pool),
        boltPool_(std::move(boltPool)),
        spillArrowPool_(getSpillArrowPool(pool)) {
    arenas_.resize(options.partitionWriterOptions.numPartitions);
    variableMemoryUsage_.resize(
        options.partitionWriterOptions.numPartitions, 0);
  }

  bytedance::bolt::memory::MemoryPool* boltPool() const {
    return boltPool_;
  }

  static void ensureFlatten(bytedance::bolt::RowVectorPtr rv) {
    for (auto& child : rv->children()) {
      bytedance::bolt::BaseVector::flattenVector(child);
      if (child->isLazy()) {
        child = child->as<bytedance::bolt::LazyVector>()->loadedVectorShared();
        BOLT_DCHECK_NOT_NULL(child);
      }
      // In case of output from Limit, RowVector size can be smaller than its
      // children size.
      if (child->size() > rv->size()) {
        child = child->slice(0, rv->size());
      }
    }
  }

  static void ensureLoaded(bytedance::bolt::RowVectorPtr rv) {
    if (isLazyNotLoaded(*rv)) {
      rv->loadedVector();
    }
  }

  static void ensurePartialFlatten(
      bytedance::bolt::RowVectorPtr rv,
      const std::vector<int32_t>& columnIndices) {
    for (auto indice : columnIndices) {
      auto& child = rv->children()[indice];
      bytedance::bolt::BaseVector::flattenVector(child);
      if (child->isLazy()) {
        child = child->as<bytedance::bolt::LazyVector>()->loadedVectorShared();
        BOLT_DCHECK_NOT_NULL(child);
      }
      // In case of output from Limit, RowVector size can be smaller than its
      // children size.
      if (child->size() > rv->size()) {
        child = child->slice(0, rv->size());
      }
    }
  }

 protected:
  virtual arrow::Status init();

  virtual arrow::Status initPartitions();

  arrow::Status initColumnTypes(const bytedance::bolt::RowVector& rv);

  virtual arrow::Status splitRowVector(const bytedance::bolt::RowVector& rv);

  virtual arrow::Status initFromRowVector(const bytedance::bolt::RowVector& rv);

  arrow::Status buildPartition2Row(uint32_t rowNum);

  arrow::Status updateInputHasNull(
      const bytedance::bolt::RowVector& rv,
      uint32_t offset = 0);

  void setSplitState(SplitState state);

  arrow::Status doSplit(const bytedance::bolt::RowVector& rv, int64_t memLimit);

  bool beyondThreshold(uint32_t partitionId, uint32_t newSize);

  uint32_t calculatePartitionBufferSize(
      const bytedance::bolt::RowVector& rv,
      int64_t memLimit);

  arrow::Status preAllocPartitionBuffers(uint32_t preAllocBufferSize);

  arrow::Status updateValidityBuffers(uint32_t partitionId, uint32_t newSize);

  arrow::Result<std::shared_ptr<arrow::ResizableBuffer>>
  allocateValidityBuffer(uint32_t col, uint32_t partitionId, uint32_t newSize);

  arrow::Status allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize);

  template <typename T>
  arrow::Status splitFixedWidthValueBuffer(
      const bytedance::bolt::RowVector& rv,
      T& valueAddrs);

  arrow::Status splitBoolType(
      const uint8_t* srcAddr,
      const std::vector<uint8_t*>& dstAddrs);

  arrow::Status splitBoolType(
      const uint8_t* srcAddr,
      const std::vector<std::vector<uint8_t*>>& dstAddrs);

  void
  splitBoolTypeInternal(const uint8_t* srcAddr, uint8_t* dstaddr, uint32_t pid);

  template <bool needAlloc>
  arrow::Status splitValidityBuffer(const bytedance::bolt::RowVector& rv);

  virtual arrow::Status splitBinaryArray(const bytedance::bolt::RowVector& rv);

  arrow::Status splitComplexType(const bytedance::bolt::RowVector& rv);

  arrow::Status evictBuffers(
      uint32_t partitionId,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      bool reuseBuffers);

  virtual arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  assembleBuffers(uint32_t partitionId, bool reuseBuffers);

  template <typename T>
  arrow::Status splitFixedType(
      const uint8_t* srcAddr,
      const std::vector<uint8_t*>& dstAddrs) {
    for (auto& pid : partitionUsed_) {
      auto dstPidBase =
          (T*)(dstAddrs[pid] + partitionBufferBase_[pid] * sizeof(T));
      auto pos = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      for (; pos < end; ++pos) {
        auto rowId = rowOffset2RowId_[pos];
        *dstPidBase++ = reinterpret_cast<const T*>(srcAddr)[rowId]; // copy
      }
    }
    return arrow::Status::OK();
  }

  // for V2
  template <typename T>
  arrow::Status splitFixedType(
      const uint8_t* srcAddr,
      const std::vector<std::vector<uint8_t*>>& dstAddrs) {
    for (auto& pid : partitionUsed_) {
      auto dstPidBase =
          (T*)(dstAddrs[pid].back() + partitionBufferBaseInBatches_[pid] * sizeof(T));
      auto pos = partition2RowOffsetBase_[pid];
      auto end = partition2RowOffsetBase_[pid + 1];
      for (; pos < end; ++pos) {
        auto rowId = rowOffset2RowId_[pos];
        *dstPidBase++ = reinterpret_cast<const T*>(srcAddr)[rowId]; // copy
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status splitBinaryType(
      uint32_t binaryIdx,
      const bytedance::bolt::FlatVector<bytedance::bolt::StringView>& src,
      std::vector<BinaryBuf>& dst);

  arrow::Result<int64_t> evictCachedPayload(int64_t size);

  arrow::Result<std::shared_ptr<arrow::Buffer>> generateComplexTypeBuffers(
      bytedance::bolt::RowVectorPtr vector);

  virtual arrow::Status resetValidityBuffer(uint32_t partitionId);

  arrow::Result<int64_t> shrinkPartitionBuffersMinSize(int64_t size);

  virtual arrow::Result<int64_t> evictPartitionBuffersMinSize(int64_t size);

  arrow::Status shrinkPartitionBuffer(uint32_t partitionId);

  arrow::Status resetPartitionBuffer(uint32_t partitionId);

  // Resize the partition buffer to newSize. If preserveData is true, it will
  // keep the data in buffer. Note when preserveData is false, and newSize is
  // larger, this function can introduce unnecessary memory copy. In this case,
  // use allocatePartitionBuffer to free current buffers and allocate new
  // buffers instead.
  arrow::Status resizePartitionBuffer(
      uint32_t partitionId,
      uint32_t newSize,
      bool preserveData);

  uint64_t valueBufferSizeForBinaryArray(uint32_t binaryIdx, uint32_t newSize);

  uint64_t valueBufferSizeForFixedWidthArray(
      uint32_t fixedWidthIndex,
      uint32_t newSize);

  void calculateSimpleColumnBytes();

  void stat() const;

  bool shrinkPartitionBuffersAfterSpill() const;

  bool evictPartitionBuffersAfterSpill() const;

  arrow::Result<uint32_t> partitionBufferSizeAfterShrink(
      uint32_t partitionId) const;

  void initAccumulateDataset(bytedance::bolt::RowVectorPtr& rv) {
    if (accumulateDataset_) {
      return;
    }
    std::vector<bytedance::bolt::VectorPtr> children(
        rv->children().size(), nullptr);
    accumulateDataset_ = std::make_shared<bytedance::bolt::RowVector>(
        boltPool_, rv->type(), nullptr, 0, std::move(children));
  }

  void updateInputMetrics(const bytedance::bolt::RowVectorPtr& rv) {
    if (rv) {
      metrics_.totalInputRowNumber += rv->size();
      metrics_.totalInputBatches += 1;
    }
  }

  void finalizeMetrics() {
    metrics_.flattenTime = flattenTime_;
    metrics_.computePidTime = computePidTime_;
    metrics_.convertTime = convertTime_;
    metrics_.combinedVectorNumber = combinedVectorNumber_;
    metrics_.combineVectorTimes = combineVectorTimes_;
    metrics_.combineVectorCost = combineVectorCost_;
    metrics_.shuffleWriteTime = stopTime_ + totalSplitTime_;

    metrics_.splitTime = totalSplitTime_ -
        (metrics_.totalEvictTime + metrics_.totalWriteTime +
         metrics_.totalCompressTime + metrics_.flattenTime +
         metrics_.computePidTime + metrics_.convertTime);

    metrics_.dataSize = std::accumulate(
        metrics_.rawPartitionLengths.begin(),
        metrics_.rawPartitionLengths.end(),
        0LL);
  }

  // for CompositeRowVector
  virtual arrow::Status tryEvict(
      int64_t memLimit = std::numeric_limits<int64_t>::max());
  arrow::Status initFromRowVectorForComposite(
      const bytedance::bolt::RowVector& rv);
  virtual arrow::Status splitCompositeVector(
      bytedance::bolt::RowVectorPtr rv,
      int64_t memLimit);
  virtual arrow::Status reclaimFixedSizeInCompositeLayout(int64_t* actual);
  virtual arrow::Status tryEvictComposite();

  SplitState splitState_{kInit};

  EvictState evictState_{kEvictable};

  BinaryArrayResizeState binaryArrayResizeState_{};

  bool supportAvx512_ = false;

  bool hasComplexType_ = false;
  std::vector<bool> isValidityBuffer_;

  // Store arrow column types. Calculated once.
  std::vector<std::shared_ptr<arrow::DataType>> arrowColumnTypes_;

  // Store bolt column types. Calculated once.
  std::vector<std::shared_ptr<const bytedance::bolt::Type>> boltColumnTypes_;

  // How many fixed-width columns in the schema. Calculated once.
  uint32_t fixedWidthColumnCount_ = 0;

  // The column indices of all binary types in the schema.
  std::vector<uint32_t> binaryColumnIndices_;

  // The column indices of all fixed-width and binary columns in the schema.
  std::vector<uint32_t> simpleColumnIndices_;

  // The column indices of all complex types in the schema, including Struct,
  // Map, List columns.
  std::vector<uint32_t> complexColumnIndices_;

  // Total bytes of fixed-width buffers of all simple columns. Including
  // validity buffers, value buffers of fixed-width types and length buffers of
  // binary types. Used for estimating pre-allocated partition buffer size.
  // Calculated once.
  uint32_t fixedWidthBufferBytes_ = 0;

  // Used for calculating the average binary length.
  // Updated for each input RowVector.
  uint64_t totalInputNumRows_ = 0;
  uint64_t totalExistingInputNumRows_ = 0;
  std::vector<uint64_t> binaryArrayTotalSizeBytes_;

  // True if input column has null in any processed input RowVector.
  // In the order of fixed-width columns + binary columns.
  std::vector<bool> inputHasNull_;

  // Records which partitions are actually occurred in the current input
  // RowVector. Most of the loops can loop on this array to avoid visiting
  // unused partition id.
  std::vector<uint32_t> partitionUsed_;

  // Row ID -> Partition ID
  // subscript: The index of row in the current input RowVector
  // value: Partition ID
  // Updated for each input RowVector.
  std::vector<uint32_t> row2Partition_;

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: How many rows does this partition have in the current input
  // RowVector Updated for each input RowVector.
  std::vector<uint32_t> partition2RowCount_;

  // Note: partition2RowOffsetBase_ and rowOffset2RowId_ are the optimization of
  // flattening the 2-dimensional vector into single dimension. The first
  // dimension is the partition id. The second dimension is the ith occurrence
  // of this partition in the input RowVector. The value is the index of the row
  // in the input RowVector. partition2RowOffsetBase_ records the offset of the
  // first dimension.
  //
  // The index of the ith occurrence of a give partition `pid` in the input
  // RowVector can be calculated via
  // rowOffset2RowId_[partition2RowOffsetBase_[pid] + i]
  // i is in the range of [0, partition2RowCount_[pid])

  // Partition ID -> Row offset, elements num: Partition num + 1
  // subscript: Partition ID
  // value: The base row offset of this Partition
  // Updated for each input RowVector.
  std::vector<uint32_t> partition2RowOffsetBase_;

  // Row offset -> Source row ID, elements num: input RowVector row num
  // subscript: Row offset
  // value: The index of row in the current input RowVector
  // Updated for each input RowVector.
  std::vector<uint32_t> rowOffset2RowId_;

  // Partition buffers are used for holding the intermediate data during split.
  // Partition ID -> Partition buffer size(unit is row)
  std::vector<uint32_t> partitionBufferSize_;

  // The write position of partition buffer. Updated after split. Reset when
  // partition buffers are reallocated.
  std::vector<uint32_t> partitionBufferBase_;

  // for assemble input batches
  std::vector<uint32_t> partitionBufferBaseInBatches_;

  // Used by all simple types. Stores raw pointers of partition buffers.
  std::vector<std::vector<uint8_t*>> partitionValidityAddrs_;
  // Used by fixed-width types. Stores raw pointers of partition buffers.
  std::vector<std::vector<uint8_t*>> partitionFixedWidthValueAddrs_;
  // Used by binary types. Stores raw pointers and metadata of partition
  // buffers.
  std::vector<std::vector<BinaryBuf>> partitionBinaryAddrs_;

  // Used by complex types.
  // Partition id -> Serialized complex data.
  std::vector<std::unique_ptr<bytedance::bolt::VectorSerializer>>
      complexTypeData_;
  std::vector<std::shared_ptr<arrow::ResizableBuffer>> complexTypeFlushBuffer_;
  std::shared_ptr<const bytedance::bolt::RowType> complexWriteType_;

  bytedance::bolt::memory::MemoryPool* boltPool_;
  std::vector<std::unique_ptr<bytedance::bolt::StreamArena>> arenas_;
  bytedance::bolt::serializer::presto::PrestoVectorSerde serde_;
  arrow::MemoryPool* spillArrowPool_;

  std::set<uint32_t> needEvicted_;

  // variableMemoryUsage for each partition
  std::vector<int64_t> variableMemoryUsage_;
  int64_t maxVariableMemoryUsage_{0};

  void updateVariableMemoryUsage(uint32_t partitionId, int64_t size) {
    variableMemoryUsage_[partitionId] += size;
    if (variableMemoryUsage_[partitionId] > maxVariableMemoryUsage_) {
      maxVariableMemoryUsage_ = variableMemoryUsage_[partitionId];
    }
  }

  // stat
  enum CpuWallTimingType {
    CpuWallTimingBegin = 0,
    CpuWallTimingCompute = CpuWallTimingBegin,
    CpuWallTimingBuildPartition,
    CpuWallTimingEvictPartition,
    CpuWallTimingHasNull,
    CpuWallTimingCalculateBufferSize,
    CpuWallTimingAllocateBuffer,
    CpuWallTimingCreateRbFromBuffer,
    CpuWallTimingMakeRB,
    CpuWallTimingCacheRB,
    CpuWallTimingFlattenRV,
    CpuWallTimingSplitRV,
    CpuWallTimingIteratePartitions,
    CpuWallTimingStop,
    CpuWallTimingEnd,
    CpuWallTimingNum = CpuWallTimingEnd - CpuWallTimingBegin
  };

  static std::string CpuWallTimingName(CpuWallTimingType type) {
    switch (type) {
      case CpuWallTimingCompute:
        return "CpuWallTimingCompute";
      case CpuWallTimingBuildPartition:
        return "CpuWallTimingBuildPartition";
      case CpuWallTimingEvictPartition:
        return "CpuWallTimingEvictPartition";
      case CpuWallTimingHasNull:
        return "CpuWallTimingHasNull";
      case CpuWallTimingCalculateBufferSize:
        return "CpuWallTimingCalculateBufferSize";
      case CpuWallTimingAllocateBuffer:
        return "CpuWallTimingAllocateBuffer";
      case CpuWallTimingCreateRbFromBuffer:
        return "CpuWallTimingCreateRbFromBuffer";
      case CpuWallTimingMakeRB:
        return "CpuWallTimingMakeRB";
      case CpuWallTimingCacheRB:
        return "CpuWallTimingCacheRB";
      case CpuWallTimingFlattenRV:
        return "CpuWallTimingFlattenRV";
      case CpuWallTimingSplitRV:
        return "CpuWallTimingSplitRV";
      case CpuWallTimingIteratePartitions:
        return "CpuWallTimingIteratePartitions";
      case CpuWallTimingStop:
        return "CpuWallTimingStop";
      default:
        return "CpuWallTimingUnknown";
    }
  }

  bytedance::bolt::CpuWallTiming cpuWallTimingList_[CpuWallTimingNum];
  // for V2
  bytedance::bolt::RowVectorPtr getStrippedRowVectorWrapper(
      const bytedance::bolt::RowVector& rv);
  const int32_t* getFirstColumnWrapper(const bytedance::bolt::RowVector& rv);

  int64_t preallocCount_{0};

  int64_t totalPreallocSize_{0};

  uint32_t accumulateRows_{0};

  bytedance::bolt::RowVectorPtr accumulateDataset_{nullptr};

  uint64_t combinedVectorNumber_{0}, combineVectorTimes_{0},
      combineVectorCost_{0};
  // detailed time
  uint64_t flattenTime_{0};
  uint64_t computePidTime_{0};
  uint64_t totalSplitTime_{0};
  uint64_t stopTime_{0};

  // for CompositeRowVector
  RowVectorLayout vectorLayout_{RowVectorLayout::kInvalid};
  bool isCompositeInitialized_{false};
  bool requestSpill_{false};
  std::vector<std::vector<uint8_t*>> sortedRows_;
  std::unique_ptr<ShuffleRowToRowConverter> compositeRowVectorConverter_{
      nullptr};
  std::vector<int64_t> partitionBytes_;
  uint64_t convertTime_{0};
  uint64_t maxBatchBytes_{kMaxShuffleWriterBatchBytes};

}; // class BoltShuffleWriter

} // namespace bytedance::bolt::shuffle::sparksql
