#include "bolt/exec/bm/BmAggregateWindow.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/bm/BmWindowPartition.h"

#include <folly/Synchronized.h>

namespace bytedance::bolt::exec::window {

namespace {

constexpr vector_size_t kAggregateInputBatchRows = 4096;

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

class BmWindowFrameReader {
 public:
  BmWindowFrameReader(
      const BmWindowPartition* partition,
      const std::vector<column_index_t>& argIndices,
      std::vector<VectorPtr>& argVectors)
      : partition_(partition), argIndices_(argIndices), argVectors_(argVectors) {
    BOLT_CHECK_NOT_NULL(partition_);
  }

  template <typename Callback>
  void forEachArgBatch(
      vector_size_t firstRow,
      vector_size_t lastRow,
      Callback callback) {
    if (lastRow < firstRow) {
      return;
    }

    auto nextRow = firstRow;
    while (nextRow <= lastRow) {
      const auto numRows = std::min<vector_size_t>(
          kAggregateInputBatchRows, lastRow - nextRow + 1);
      materializeArgVectors(nextRow, numRows);
      callback(numRows);
      nextRow += numRows;
    }
  }

 private:
  void materializeArgVectors(vector_size_t firstRow, vector_size_t numRows) {
    std::vector<column_index_t> columns;
    std::vector<VectorPtr> results;
    columns.reserve(argIndices_.size());
    results.reserve(argIndices_.size());

    for (auto i = 0; i < argIndices_.size(); ++i) {
      if (argIndices_[i] == kConstantChannel) {
        continue;
      }
      BaseVector::prepareForReuse(argVectors_[i], numRows);
      columns.push_back(argIndices_[i]);
      results.push_back(argVectors_[i]);
    }

    if (columns.empty()) {
      return;
    }

    recordMaterializedRows(numRows);

    partition_->extractColumns(
        {columns.data(), columns.size()},
        firstRow,
        numRows,
        {results.data(), results.size()});
  }

  const BmWindowPartition* partition_;
  const std::vector<column_index_t>& argIndices_;
  std::vector<VectorPtr>& argVectors_;
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
      : WindowFunction(resultType, pool, stringAllocator) {
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
      incrementalAggregation(
          validRows, nextRowToAdd, rawFrameEnds, resultOffset, result);
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

    bool incrementalAggregation = true;
    validRows.applyToSelected([&](auto i) {
      firstRow = std::min(firstRow, rawFrameStarts[i]);
      lastRow = std::max(lastRow, rawFrameEnds[i]);
      incrementalAggregation &= rawFrameStarts[i] == fixedFrameStartRow;
      incrementalAggregation &= rawFrameEnds[i] >= prevFrameEnd;
      prevFrameEnd = rawFrameEnds[i];
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

    return {firstRow, lastRow, incrementalAggregation, usePreviousAggregate};
  }

  void resetAggregateGroup() {
    aggregate_->clear();
    if (aggregateInitialized_) {
      aggregate_->destroy(folly::Range(&rawSingleGroupRow_, 1));
    }
    aggregate_->initializeNewGroups(
        &rawSingleGroupRow_, std::vector<vector_size_t>{0});
    aggregateInitialized_ = true;
  }

  void addFrameRows(vector_size_t firstRow, vector_size_t lastRow) {
    if (lastRow < firstRow) {
      return;
    }

    BmWindowFrameReader frameReader(partition_, argIndices_, argVectors_);
    frameReader.forEachArgBatch(firstRow, lastRow, [&](vector_size_t numRows) {
      SelectivityVector rows(numRows);
      aggregate_->addSingleGroupRawInput(
          rawSingleGroupRow_, rows, argVectors_, false);
    });
  }

  void extractAggregateResult(const VectorPtr& result, vector_size_t row) {
    BaseVector::prepareForReuse(aggregateResultVector_, 1);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &aggregateResultVector_);
    result->copy(aggregateResultVector_.get(), row, 0, 1);
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

  BufferPtr singleGroupRowBufferPtr_;
  char* rawSingleGroupRow_;
  vector_size_t singleGroupRowSize_;
  VectorPtr aggregateResultVector_;
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
  if (name == "percentile") {
    return nullptr;
  }

  return std::make_unique<BmAggregateWindowFunction>(
      name, args, resultType, ignoreNulls, pool, stringAllocator, config);
}

} // namespace bytedance::bolt::exec::window
