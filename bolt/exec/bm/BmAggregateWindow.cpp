#include "bolt/exec/bm/BmAggregateWindow.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include <folly/Synchronized.h>

#include <algorithm>
#include <array>
#include <limits>

namespace bytedance::bolt::exec::window {

namespace {

constexpr vector_size_t kAggregateInputBatchRows = 1024;

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

bool supportsReverseIncrementalAggregation(const std::string& name) {
  return name == "sum" || name == "count" || name == "min" ||
      name == "max" || name == "avg";
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
      recordMaterializedRows(numRows);

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
      auto repeated =
          BaseVector::wrapInConstant(validRows.size(), 0, aggregateResultVector_);
      result->copy(repeated.get(), resultOffset, 0, validRows.size());
      return;
    }

    validRows.applyToSelected([&](auto row) {
      result->copy(aggregateResultVector_.get(), resultOffset + row, 0, 1);
    });
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
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
    recordReverseIncrementalRows(validRows.countSelected());
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
    validRows.applyToSelected([&](auto i) {
      resetAggregateGroup();
      addFrameRows(rawFrameStarts[i], rawFrameEnds[i]);
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
  std::unique_ptr<BmWindowFrameReader> frameReader_;
  bool supportsReverseIncremental_{false};
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

} // namespace

void resetBmAggregateWindowTestStats() {
  testStats().withWLock(
      [](auto& stats) { stats = BmAggregateWindowTestStats{}; });
}

BmAggregateWindowTestStats bmAggregateWindowTestStats() {
  return testStats().copy();
}

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
  if (name == "collect_list" || name == "collect_set" || name == "max_by" ||
      name == "min_by" || name == "percentile") {
    return nullptr;
  }

  return std::make_unique<BmAggregateWindowFunction>(
      name, args, resultType, ignoreNulls, pool, stringAllocator, config);
}

} // namespace bytedance::bolt::exec::window
