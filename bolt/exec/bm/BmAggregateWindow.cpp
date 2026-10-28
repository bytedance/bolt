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

#include "bolt/exec/bm/BmAggregateWindow.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include <folly/ScopeGuard.h>

#ifndef NDEBUG
#include <folly/Synchronized.h>
#endif

#include <algorithm>
#include <array>
#include <limits>

namespace bytedance::bolt::exec::window {

namespace {

constexpr vector_size_t kAggregateInputBatchRows = 1024;
// -1 follows the production policy; 0/1 override it for comparisons.
tsan_atomic<int32_t> inputAggregationOverrideForTest{-1};

#ifndef NDEBUG
folly::Synchronized<BmAggregateWindowTestStats>& testStats() {
  static folly::Synchronized<BmAggregateWindowTestStats> stats;
  return stats;
}

void recordMaterializedRows(vector_size_t numRows) {
  testStats().withWLock([&](auto& stats) {
    ++stats.materializeCalls;
    stats.materializedRows += numRows;
    stats.maxMaterializedRows =
        std::max<uint64_t>(stats.maxMaterializedRows, numRows);
  });
}

void recordReverseIncrementalRows(vector_size_t numRows) {
  testStats().withWLock([&](auto& stats) {
    ++stats.reverseIncrementalCalls;
    stats.reverseIncrementalRows += numRows;
  });
}

void recordSharedScan(size_t numFunctions) {
  testStats().withWLock([&](auto& stats) {
    ++stats.sharedScanCalls;
    stats.sharedScanFunctions += numFunctions;
  });
}

#endif

bool supportsReverseIncrementalAggregation(const std::string& name) {
  return name == "sum" || name == "count" || name == "min" || name == "max" ||
      name == "avg" || name == "collect_list" || name == "collect_set" ||
      name == "max_by" || name == "min_by";
}

class BmWindowFrameReader {
 public:
  BmWindowFrameReader(
      const std::vector<column_index_t>& argIndices,
      const std::vector<TypePtr>& argTypes,
      std::vector<VectorPtr>& argVectors)
      : argIndices_(argIndices), argTypes_(argTypes), argVectors_(argVectors) {
    for (auto& batch : batches_) {
      batch.argVectors.reserve(argIndices_.size());
      for (auto i = 0; i < argIndices_.size(); ++i) {
        batch.argVectors.push_back(
            argIndices_[i] == kConstantChannel
                ? argVectors_[i]
                : BaseVector::create(argTypes_[i], 0, argVectors_[i]->pool()));
      }
      batch.columns.reserve(argIndices_.size());
      batch.results.reserve(argIndices_.size());
    }
  }

  void resetPartition(const BmWindowPartition* partition) {
    BOLT_CHECK_NOT_NULL(partition);
    partition_ = partition;
    for (auto& batch : batches_) {
      batch.valid = false;
      batch.numRows = 0;
    }
  }

  template <typename Callback>
  void forEachArgBatch(
      vector_size_t firstRow,
      vector_size_t lastRow,
      Callback callback) {
    if (lastRow < firstRow) {
      return;
    }
    BOLT_CHECK_NOT_NULL(partition_);

    auto nextRow = firstRow;
    while (nextRow <= lastRow) {
      const auto batchStart =
          (nextRow / kAggregateInputBatchRows) * kAggregateInputBatchRows;
      const auto batchRows = std::min<vector_size_t>(
          kAggregateInputBatchRows, partition_->numRows() - batchStart);
      auto& batch = materializeBatch(batchStart, batchRows);

      const auto localStart = nextRow - batchStart;
      const auto localEnd =
          std::min<vector_size_t>(batchRows, lastRow + 1 - batchStart);
      callback(batch.argVectors, batchRows, localStart, localEnd);
      nextRow = batchStart + localEnd;
    }
  }

 private:
  struct CachedBatch {
    bool valid{false};
    vector_size_t firstRow{0};
    vector_size_t numRows{0};
    uint64_t lastUsed{0};
    std::vector<VectorPtr> argVectors;
    std::vector<column_index_t> columns;
    std::vector<VectorPtr> results;
  };

  CachedBatch& materializeBatch(vector_size_t firstRow, vector_size_t numRows) {
    for (auto& batch : batches_) {
      if (batch.valid && batch.firstRow == firstRow &&
          batch.numRows == numRows) {
        batch.lastUsed = ++useClock_;
        return batch;
      }
    }

    auto* batchToUse = &batches_[0];
    for (auto& batch : batches_) {
      if (!batch.valid) {
        batchToUse = &batch;
        break;
      }
      if (batch.lastUsed < batchToUse->lastUsed) {
        batchToUse = &batch;
      }
    }

    auto& batch = *batchToUse;
    batch.valid = true;
    batch.firstRow = firstRow;
    batch.numRows = numRows;
    batch.lastUsed = ++useClock_;
    batch.columns.clear();
    batch.results.clear();

    for (auto i = 0; i < argIndices_.size(); ++i) {
      if (argIndices_[i] == kConstantChannel) {
        continue;
      }
      BaseVector::prepareForReuse(batch.argVectors[i], numRows);
      batch.columns.push_back(argIndices_[i]);
      batch.results.push_back(batch.argVectors[i]);
    }

    if (!batch.columns.empty()) {
#ifndef NDEBUG
      recordMaterializedRows(numRows);
#endif

      partition_->extractColumns(
          {batch.columns.data(), batch.columns.size()},
          firstRow,
          numRows,
          {batch.results.data(), batch.results.size()});
    }
    return batch;
  }

  const BmWindowPartition* partition_{nullptr};
  const std::vector<column_index_t>& argIndices_;
  const std::vector<TypePtr>& argTypes_;
  std::vector<VectorPtr>& argVectors_;
  std::array<CachedBatch, 2> batches_;
  uint64_t useClock_{0};
};

bool isEntirePartitionFrame(const core::WindowNode::Frame* frame) {
  return frame != nullptr &&
      frame->startType == core::WindowNode::BoundType::kUnboundedPreceding &&
      frame->endType == core::WindowNode::BoundType::kUnboundedFollowing;
}

} // namespace

class BmAggregateWindowFunction : public exec::WindowFunction {
 public:
  BmAggregateWindowFunction(
      const std::string& name,
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator,
      const core::QueryConfig& config)
      : WindowFunction(resultType, pool, stringAllocator),
        supportsReverseIncremental_(
            supportsReverseIncrementalAggregation(name)) {
    BOLT_USER_CHECK(
        !ignoreNulls, "Aggregate window functions do not support IGNORE NULLS");
    argTypes_.reserve(args.size());
    argIndices_.reserve(args.size());
    argVectors_.reserve(args.size());
    for (const auto& arg : args) {
      argTypes_.push_back(arg.type);
      if (arg.constantValue) {
        argIndices_.push_back(kConstantChannel);
        argVectors_.push_back(arg.constantValue);
      } else {
        BOLT_CHECK(arg.index.has_value());
        argIndices_.push_back(arg.index.value());
        argVectors_.push_back(BaseVector::create(arg.type, 0, pool_));
      }
    }
    frameReader_ = std::make_unique<BmWindowFrameReader>(
        argIndices_, argTypes_, argVectors_);

    aggregate_ = exec::Aggregate::create(
        name,
        core::AggregationNode::Step::kSingle,
        argTypes_,
        resultType,
        config);
    aggregate_->setAllocator(stringAllocator_);

    static const int32_t kNullOffset = 0;
    static const int32_t kRowSizeOffset = bits::nbytes(1);
    singleGroupRowSize_ = kRowSizeOffset + sizeof(int32_t);
    singleGroupRowSize_ = bits::roundUp(
        singleGroupRowSize_, aggregate_->accumulatorAlignmentSize());
    aggregate_->setOffsets(
        singleGroupRowSize_,
        exec::RowContainer::nullByte(kNullOffset),
        exec::RowContainer::nullMask(kNullOffset),
        kRowSizeOffset);
    singleGroupRowSize_ += aggregate_->accumulatorFixedWidthSize();

    singleGroupRowBufferPtr_ =
        AlignedBuffer::allocate<char>(singleGroupRowSize_, pool_);
    rawSingleGroupRow_ = singleGroupRowBufferPtr_->asMutable<char>();
    aggregateResultVector_ = BaseVector::create(resultType, 1, pool_);

    computeDefaultAggregateValue(resultType);
  }

  void setEntirePartitionAggregation(
      std::shared_ptr<BmEntirePartitionAggregation> aggregation) {
    BOLT_CHECK_NOT_NULL(aggregation);
    BOLT_CHECK_NULL(entirePartitionAggregation_);
    entirePartitionAggregation_ = std::move(aggregation);
    entirePartitionAggregation_->registerFunction(this);
  }

  ~BmAggregateWindowFunction() override {
    if (aggregateInitialized_) {
      std::vector<char*> singleGroupRowVector = {rawSingleGroupRow_};
      aggregate_->destroy(folly::Range(singleGroupRowVector.data(), 1));
    }
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = dynamic_cast<const BmWindowPartition*>(partition);
    BOLT_CHECK_NOT_NULL(
        partition_, "BmAggregateWindowFunction requires BmWindowPartition");
    previousFrameMetadata_.reset();
    frameReader_->resetPartition(partition_);
    invalidateAggregateResultCache();
    if (entirePartitionAggregation_ != nullptr) {
      entirePartitionAggregation_->resetPartition(partition_);
    }
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    if (handleAllEmptyFrames(validRows, resultOffset, result)) {
      return;
    }

    if (entirePartitionAggregation_ != nullptr) {
      entirePartitionAggregation_->prepare();
      copyAggregateResultToRows(validRows, resultOffset, result);
      return;
    }

    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();
    auto frameMetadata =
        analyzeFrameValues(validRows, rawFrameStarts, rawFrameEnds);

    if (frameMetadata.incrementalAggregation) {
      vector_size_t nextRowToAdd;
      if (frameMetadata.usePreviousAggregate) {
        nextRowToAdd = previousFrameMetadata_->lastRow + 1;
      } else {
        nextRowToAdd = frameMetadata.firstRow;
        resetAggregateGroup();
      }
      if (frameMetadata.allFramesEqual) {
        if (frameMetadata.lastRow >= nextRowToAdd) {
          addFrameRows(nextRowToAdd, frameMetadata.lastRow);
        }
        copyAggregateResultToRows(validRows, resultOffset, result);
      } else {
        incrementalAggregation(
            validRows, nextRowToAdd, rawFrameEnds, resultOffset, result);
      }
    } else if (frameMetadata.reverseIncrementalAggregation) {
      reverseIncrementalAggregation(
          validRows, rawFrameStarts, rawFrameEnds, resultOffset, result);
    } else {
      simpleAggregation(
          validRows, rawFrameStarts, rawFrameEnds, resultOffset, result);
    }
    previousFrameMetadata_ = frameMetadata;
  }

 private:
  friend class BmEntirePartitionAggregation;

  struct FrameMetadata {
    vector_size_t firstRow;
    vector_size_t lastRow;
    bool incrementalAggregation;
    bool reverseIncrementalAggregation;
    bool allFramesEqual;
    bool usePreviousAggregate;
  };

  bool handleAllEmptyFrames(
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (!validRows.hasSelections()) {
      setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
      return true;
    }
    return false;
  }

  FrameMetadata analyzeFrameValues(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds) {
    BOLT_DCHECK(validRows.hasSelections());

    auto firstValidRow = validRows.begin();
    vector_size_t firstRow = rawFrameStarts[firstValidRow];
    vector_size_t fixedFrameStartRow = firstRow;
    vector_size_t lastRow = rawFrameEnds[firstValidRow];
    vector_size_t prevFrameEnd = lastRow;
    vector_size_t fixedFrameEndRow = lastRow;
    vector_size_t prevFrameStart = firstRow;

    bool incrementalAggregation = true;
    bool reverseIncrementalAggregation = supportsReverseIncremental_;
    bool allFramesEqual = true;
    validRows.applyToSelected([&](auto i) {
      firstRow = std::min(firstRow, rawFrameStarts[i]);
      lastRow = std::max(lastRow, rawFrameEnds[i]);
      allFramesEqual &= rawFrameStarts[i] == fixedFrameStartRow;
      allFramesEqual &= rawFrameEnds[i] == fixedFrameEndRow;
      incrementalAggregation &= rawFrameStarts[i] == fixedFrameStartRow;
      incrementalAggregation &= rawFrameEnds[i] >= prevFrameEnd;
      prevFrameEnd = rawFrameEnds[i];
      reverseIncrementalAggregation &= rawFrameEnds[i] == fixedFrameEndRow;
      reverseIncrementalAggregation &= rawFrameStarts[i] >= prevFrameStart;
      prevFrameStart = rawFrameStarts[i];
    });

    bool usePreviousAggregate = false;
    if (previousFrameMetadata_.has_value()) {
      const auto& previousFrame = previousFrameMetadata_.value();
      if (incrementalAggregation && previousFrame.incrementalAggregation &&
          previousFrame.firstRow == firstRow &&
          previousFrame.lastRow <= rawFrameEnds[firstValidRow]) {
        usePreviousAggregate = true;
      }
    }

    return {
        firstRow,
        lastRow,
        incrementalAggregation,
        reverseIncrementalAggregation,
        allFramesEqual,
        usePreviousAggregate};
  }

  void resetAggregateGroup() {
    aggregate_->clear();
    if (aggregateInitialized_) {
      aggregate_->destroy(folly::Range(&rawSingleGroupRow_, 1));
    }
    aggregate_->initializeNewGroups(
        &rawSingleGroupRow_,
        folly::Range<const vector_size_t*>(
            singleGroupIndex_.data(), singleGroupIndex_.size()));
    aggregateInitialized_ = true;
    markAggregateStateChanged();
  }

  void addFrameRows(vector_size_t firstRow, vector_size_t lastRow) {
    if (lastRow < firstRow) {
      return;
    }

    bool addedRows = false;
    frameReader_->forEachArgBatch(
        firstRow,
        lastRow,
        [&](const std::vector<VectorPtr>& argVectors,
            vector_size_t numRows,
            vector_size_t localStart,
            vector_size_t localEnd) {
          selectAggregateRows(numRows, localStart, localEnd);
          aggregate_->addSingleGroupRawInput(
              rawSingleGroupRow_, aggregateRows_, argVectors, false);
          addedRows = true;
        });
    if (addedRows) {
      markAggregateStateChanged();
    }
  }

  void extractAggregateResult(const VectorPtr& result, vector_size_t row) {
    if (extractedStateVersion_ != aggregateStateVersion_) {
      BaseVector::prepareForReuse(aggregateResultVector_, 1);
      aggregate_->extractValues(
          &rawSingleGroupRow_, 1, &aggregateResultVector_);
      extractedStateVersion_ = aggregateStateVersion_;
    }
    result->copy(aggregateResultVector_.get(), row, 0, 1);
  }

  void copyAggregateResultToRows(
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (extractedStateVersion_ != aggregateStateVersion_) {
      BaseVector::prepareForReuse(aggregateResultVector_, 1);
      aggregate_->extractValues(
          &rawSingleGroupRow_, 1, &aggregateResultVector_);
      extractedStateVersion_ = aggregateStateVersion_;
    }

    if (validRows.isAllSelected()) {
      if (broadcastSingleResult(resultOffset, validRows.size(), result)) {
        return;
      }
      // Complex results and anything the typed path declines fall back to the
      // original constant wrap.
      auto repeated = BaseVector::wrapInConstant(
          validRows.size(), 0, aggregateResultVector_);
      result->copy(repeated.get(), resultOffset, 0, validRows.size());
      return;
    }

    validRows.applyToSelected([&](auto row) {
      result->copy(aggregateResultVector_.get(), resultOffset + row, 0, 1);
    });
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  // Repeats the single aggregate result across the output range without
  // constructing a temporary ConstantVector. 'aggregateResultVector_' holds the
  // one value at index 0; this writes it into [resultOffset, resultOffset +
  // count). Returns false for types the direct path does not handle so the
  // caller can fall back.
  template <TypeKind Kind>
  bool broadcastResultTyped(
      vector_size_t resultOffset,
      vector_size_t count,
      const VectorPtr& result) {
    if constexpr (
        Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType) {
      return false;
    } else {
      using T = typename TypeTraits<Kind>::NativeType;
      auto* source = aggregateResultVector_->asFlatVector<T>();
      auto* flatResult = result->asFlatVector<T>();
      if (source == nullptr || flatResult == nullptr) {
        return false;
      }

      const vector_size_t end = resultOffset + count;
      if (source->isNullAt(0)) {
        bits::fillBits(
            flatResult->mutableRawNulls(), resultOffset, end, bits::kNull);
        return true;
      }

      // Non-null value: clear any existing null bits in this output range.
      if (flatResult->rawNulls() != nullptr) {
        flatResult->clearNulls(resultOffset, end);
      }

      const T value = source->valueAt(0);
      if constexpr (std::is_same_v<T, bool>) {
        bits::fillBits(
            flatResult->template mutableRawValues<uint64_t>(),
            resultOffset,
            end,
            value);
      } else if constexpr (std::is_same_v<T, StringView>) {
        StringView writeValue = value;
        if (!value.isInline()) {
          // Copy the payload once into the output's growing string buffer and
          // point every row at that copy. Avoid retaining references to the
          // scratch string buffers so they remain eligible for reuse by
          // prepareForReuse(). Results from many small partitions can share a
          // consolidated output buffer.
          char* target = flatResult->getRawStringBufferWithSpace(value.size());
          memcpy(target, value.data(), value.size());
          writeValue = StringView(target, value.size());
        }
        auto* rawValues = flatResult->mutableRawValues();
        std::fill(rawValues + resultOffset, rawValues + end, writeValue);
      } else {
        auto* rawValues = flatResult->mutableRawValues();
        std::fill(rawValues + resultOffset, rawValues + end, value);
      }
      return true;
    }
  }

  bool broadcastSingleResult(
      vector_size_t resultOffset,
      vector_size_t count,
      const VectorPtr& result) {
    if (!result->type()->isPrimitiveType()) {
      return false;
    }
    return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
        broadcastResultTyped,
        aggregateResultVector_->typeKind(),
        resultOffset,
        count,
        result);
  }

  void selectAggregateRows(
      vector_size_t numRows,
      vector_size_t localStart,
      vector_size_t localEnd) {
    if (aggregateRows_.size() != numRows) {
      aggregateRows_.resizeFill(numRows, false);
      selectedAggregateRowsValid_ = false;
    } else if (selectedAggregateRowsValid_) {
      aggregateRows_.setValidRange(
          selectedAggregateRowsStart_, selectedAggregateRowsEnd_, false);
    } else {
      aggregateRows_.clearAll();
    }

    aggregateRows_.setValidRange(localStart, localEnd, true);
    aggregateRows_.updateBounds();
    selectedAggregateRowsStart_ = localStart;
    selectedAggregateRowsEnd_ = localEnd;
    selectedAggregateRowsValid_ = true;
  }

  void markAggregateStateChanged() {
    ++aggregateStateVersion_;
  }

  void invalidateAggregateResultCache() {
    extractedStateVersion_ = std::numeric_limits<uint64_t>::max();
  }

  void incrementalAggregation(
      const SelectivityVector& validRows,
      vector_size_t nextRowToAdd,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    validRows.applyToSelected([&](auto i) {
      if (rawFrameEnds[i] >= nextRowToAdd) {
        addFrameRows(nextRowToAdd, rawFrameEnds[i]);
        nextRowToAdd = rawFrameEnds[i] + 1;
      }
      extractAggregateResult(result, resultOffset + i);
    });

    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void reverseIncrementalAggregation(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
#ifndef NDEBUG
    recordReverseIncrementalRows(validRows.countSelected());
#endif
    resetAggregateGroup();

    vector_size_t firstAddedRow = rawFrameEnds[validRows.end() - 1] + 1;
    for (auto row = validRows.end(); row > validRows.begin();) {
      --row;
      if (!validRows.isValid(row)) {
        continue;
      }
      if (rawFrameStarts[row] < firstAddedRow) {
        addFrameRows(rawFrameStarts[row], firstAddedRow - 1);
        firstAddedRow = rawFrameStarts[row];
      }
      extractAggregateResult(result, resultOffset + row);
    }

    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void simpleAggregation(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    // Valid frame bounds are nonnegative. Recompute the first selected frame
    // and reuse the existing aggregate state/result only within this call.
    vector_size_t previousStart = -1;
    vector_size_t previousEnd = -1;
    validRows.applyToSelected([&](auto i) {
      const auto frameStart = rawFrameStarts[i];
      const auto frameEnd = rawFrameEnds[i];
      if (frameStart != previousStart || frameEnd != previousEnd) {
        resetAggregateGroup();
        addFrameRows(frameStart, frameEnd);
        previousStart = frameStart;
        previousEnd = frameEnd;
      }
      extractAggregateResult(result, resultOffset + i);
    });

    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void computeDefaultAggregateValue(const TypePtr& resultType) {
    resetAggregateGroup();
    emptyResult_ = BaseVector::create(resultType, 1, pool_);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &emptyResult_);
    aggregate_->clear();
  }

  std::unique_ptr<exec::Aggregate> aggregate_;
  bool aggregateInitialized_{false};
  const BmWindowPartition* partition_{nullptr};

  std::vector<TypePtr> argTypes_;
  std::vector<column_index_t> argIndices_;
  std::vector<VectorPtr> argVectors_;
  std::vector<VectorPtr> arguments_;
  std::unique_ptr<BmWindowFrameReader> frameReader_;
  bool supportsReverseIncremental_{false};
  std::shared_ptr<BmEntirePartitionAggregation> entirePartitionAggregation_;
  SelectivityVector aggregateRows_;
  vector_size_t selectedAggregateRowsStart_{0};
  vector_size_t selectedAggregateRowsEnd_{0};
  bool selectedAggregateRowsValid_{false};

  BufferPtr singleGroupRowBufferPtr_;
  char* rawSingleGroupRow_;
  vector_size_t singleGroupRowSize_;
  std::array<vector_size_t, 1> singleGroupIndex_{{0}};
  VectorPtr aggregateResultVector_;
  uint64_t aggregateStateVersion_{0};
  uint64_t extractedStateVersion_{std::numeric_limits<uint64_t>::max()};
  std::optional<FrameMetadata> previousFrameMetadata_;
  VectorPtr emptyResult_;
};

BmEntirePartitionAggregation::BmEntirePartitionAggregation(
    memory::MemoryPool* pool)
    : pool_(pool) {
  BOLT_CHECK_NOT_NULL(pool_);
}

void BmEntirePartitionAggregation::registerFunction(
    BmAggregateWindowFunction* function) {
  BOLT_CHECK_NOT_NULL(function);
  functions_.push_back(function);
  std::vector<std::optional<size_t>> inputIndices;
  inputIndices.reserve(function->argIndices_.size());
  for (auto i = 0; i < function->argIndices_.size(); ++i) {
    const auto channel = function->argIndices_[i];
    if (channel == kConstantChannel) {
      inputIndices.push_back(std::nullopt);
      continue;
    }
    const auto existing =
        std::find(inputChannels_.begin(), inputChannels_.end(), channel);
    if (existing != inputChannels_.end()) {
      const auto existingIndex = existing - inputChannels_.begin();
      BOLT_CHECK(
          inputTypes_[existingIndex]->equivalent(*function->argTypes_[i]));
      inputIndices.push_back(existingIndex);
      continue;
    }
    inputIndices.push_back(inputChannels_.size());
    inputChannels_.push_back(channel);
    inputTypes_.push_back(function->argTypes_[i]);
    inputVectors_.push_back(
        BaseVector::create(function->argTypes_[i], 0, pool_));
  }
  functionInputIndices_.push_back(std::move(inputIndices));
  function->arguments_.reserve(function->argIndices_.size());
}

void BmEntirePartitionAggregation::resetPartition(
    const BmWindowPartition* partition) {
  BOLT_CHECK_NOT_NULL(partition);
  partition_ = partition;
  prepared_ = false;
}

void BmEntirePartitionAggregation::addInput(
    const RowVectorPtr& input,
    vector_size_t offset,
    vector_size_t size) {
  BOLT_CHECK_GT(size, 0);
  BOLT_CHECK_LE(offset + size, input->size());
  if (inputRows_ == 0) {
    for (auto* function : functions_) {
      function->resetAggregateGroup();
    }
  }
  std::vector<VectorPtr> inputs;
  inputs.reserve(inputChannels_.size());
  for (const auto channel : inputChannels_) {
    inputs.push_back(input->childAt(channel));
  }
  // Use the original vectors and select only this partition's input range.
  addBatch(inputs, input->size(), offset, size);
  inputRows_ += size;
}

void BmEntirePartitionAggregation::addBatch(
    const std::vector<VectorPtr>& inputs,
    vector_size_t batchRows,
    vector_size_t offset,
    vector_size_t size) {
  for (auto functionIndex = 0; functionIndex < functions_.size();
       ++functionIndex) {
    auto* function = functions_[functionIndex];
    const auto& inputIndices = functionInputIndices_[functionIndex];
    auto& arguments = function->arguments_;
    BOLT_DCHECK(arguments.empty());
    // Retain capacity, but release references before input scratch is reused.
    auto clearArguments = folly::makeGuard([&arguments] { arguments.clear(); });
    for (auto i = 0; i < inputIndices.size(); ++i) {
      arguments.push_back(
          inputIndices[i].has_value() ? inputs[inputIndices[i].value()]
                                      : function->argVectors_[i]);
    }
    function->selectAggregateRows(batchRows, offset, offset + size);
    function->aggregate_->addSingleGroupRawInput(
        function->rawSingleGroupRow_,
        function->aggregateRows_,
        arguments,
        false);
    function->markAggregateStateChanged();
  }
}

std::vector<VectorPtr> BmEntirePartitionAggregation::finishPartition() {
  BOLT_CHECK_GT(inputRows_, 0);
  std::vector<VectorPtr> results;
  results.reserve(functions_.size());
  for (auto* function : functions_) {
    // Each closed partition owns its result, including variable-width payloads.
    // Do not reuse the output scratch vector while another partition holds it.
    auto result =
        BaseVector::create(function->aggregateResultVector_->type(), 1, pool_);
    function->aggregate_->extractValues(
        &function->rawSingleGroupRow_, 1, &result);
    BOLT_CHECK_NOT_NULL(result);
    BOLT_CHECK_EQ(result->size(), 1);
    results.push_back(std::move(result));
  }
#ifndef NDEBUG
  testStats().withWLock([&](auto& stats) {
    stats.inputAggregationRows += inputRows_;
    ++stats.inputAggregationPartitions;
    stats.inputAggregationFunctions += functions_.size();
  });
#endif
  inputRows_ = 0;
  return results;
}

void BmEntirePartitionAggregation::prepare() {
  if (prepared_) {
    return;
  }
  BOLT_CHECK_NOT_NULL(partition_);
  BOLT_CHECK_GT(functions_.size(), 0);
  const auto& results = partition_->aggregateResults();
  if (!results.empty()) {
    BOLT_CHECK_EQ(results.size(), functions_.size());
    for (auto i = 0; i < functions_.size(); ++i) {
      auto* function = functions_[i];
      function->aggregateResultVector_ = results[i];
      function->extractedStateVersion_ = function->aggregateStateVersion_;
    }
    prepared_ = true;
    return;
  }

  for (auto* function : functions_) {
    BOLT_CHECK_NOT_NULL(function);
    function->resetAggregateGroup();
  }

  for (vector_size_t batchStart = 0; batchStart < partition_->numRows();
       batchStart += kAggregateInputBatchRows) {
    const auto batchRows = std::min<vector_size_t>(
        kAggregateInputBatchRows, partition_->numRows() - batchStart);
    if (!inputChannels_.empty()) {
      for (auto& input : inputVectors_) {
        BaseVector::prepareForReuse(input, batchRows);
      }
#ifndef NDEBUG
      recordMaterializedRows(batchRows);
#endif
      partition_->extractColumns(
          {inputChannels_.data(), inputChannels_.size()},
          batchStart,
          batchRows,
          {inputVectors_.data(), inputVectors_.size()});
    }

    addBatch(inputVectors_, batchRows, 0, batchRows);
  }

  prepared_ = true;
#ifndef NDEBUG
  recordSharedScan(functions_.size());
#endif
}

#ifndef NDEBUG
void resetBmAggregateWindowTestStats() {
  testStats().withWLock(
      [](auto& stats) { stats = BmAggregateWindowTestStats{}; });
}

BmAggregateWindowTestStats bmAggregateWindowTestStats() {
  return testStats().copy();
}
#else
void resetBmAggregateWindowTestStats() {}

BmAggregateWindowTestStats bmAggregateWindowTestStats() {
  return {};
}
#endif

std::unique_ptr<exec::WindowFunction> createBmAggregateWindowFunction(
    const std::string& name,
    const std::vector<exec::WindowFunctionArg>& args,
    const TypePtr& resultType,
    bool ignoreNulls,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  if (!exec::getAggregateFunctionSignatures(name).has_value()) {
    return nullptr;
  }
  if (name == "percentile") {
    return nullptr;
  }

  return std::make_unique<BmAggregateWindowFunction>(
      name, args, resultType, ignoreNulls, pool, stringAllocator, config);
}

std::shared_ptr<BmEntirePartitionAggregation> prepareBmAggregateWindowFunctions(
    const core::WindowNode& windowNode,
    const std::vector<std::unique_ptr<exec::WindowFunction>>& functions,
    memory::MemoryPool* pool) {
  const auto& functionSpecs = windowNode.windowFunctions();
  BOLT_CHECK_EQ(functionSpecs.size(), functions.size());

  std::vector<BmAggregateWindowFunction*> candidates;
  // Keyed windows scan on output to avoid retaining a result per partition.
  // A global window has one result and avoids rescanning BM rows on output.
  const int32_t inputAggregationOverride = inputAggregationOverrideForTest;
  bool canAccumulateInput = inputAggregationOverride < 0
      ? windowNode.partitionKeys().empty()
      : inputAggregationOverride != 0;
  for (auto i = 0; i < functions.size(); ++i) {
    if (!isEntirePartitionFrame(&functionSpecs[i].frame)) {
      continue;
    }
    if (auto* function =
            dynamic_cast<BmAggregateWindowFunction*>(functions[i].get())) {
      candidates.push_back(function);
      const auto& name = functionSpecs[i].functionCall->name();
      // Match Spillable's bounded-state full-partition aggregate scope.
      canAccumulateInput &= name == "sum" || name == "count" || name == "min" ||
          name == "max" || name == "avg";
    }
  }

  if (candidates.empty()) {
    return nullptr;
  }

  auto aggregation = std::make_shared<BmEntirePartitionAggregation>(pool);
  for (auto* function : candidates) {
    function->setEntirePartitionAggregation(aggregation);
  }
  // The return value only enables input accumulation. Even when returning
  // nullptr, candidates retain this coordinator for shared scans during output.
  return canAccumulateInput ? aggregation : nullptr;
}

void setBmInputAggregationEnabledForTest(std::optional<bool> enabled) {
  inputAggregationOverrideForTest =
      enabled.has_value() ? int32_t(*enabled) : -1;
}

} // namespace bytedance::bolt::exec::window
