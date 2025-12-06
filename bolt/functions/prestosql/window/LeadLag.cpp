/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/WindowFunction.h"
namespace bytedance::bolt::window::prestosql {

namespace {

template <bool isLag>
class LeadLagFunction : public exec::WindowFunction {
 public:
  explicit LeadLagFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      bolt::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr), ignoreNulls_(ignoreNulls) {
    valueIndex_ = args[0].index.value();
    resultType_ = resultType;
    numArgs_ = args.size();
    initializeOffset(args);
    initializeDefaultValue(args);

    nulls_ = allocateNulls(0, pool);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
    ignoreNullsForPartition_ = false;

    if (ignoreNulls_) {
      auto partitionSize = partition_->numRows();
      AlignedBuffer::reallocate<bool>(&nulls_, partitionSize);

      partition_->extractNulls(valueIndex_, 0, partitionSize, nulls_);
      // There are null bits so the special ignoreNulls processing is required
      // for this partition.
      ignoreNullsForPartition_ =
          bits::countBits(nulls_->as<uint64_t>(), 0, partitionSize) > 0;
    }
  }

  void computeSpillableAggregate(bool doInitialize) override {
    constexpr int32_t kBatchSize = 4096;
    int32_t remainingRowCount = 0;
    if constexpr (isLag) {
      // since we include one more row from previous sub-partition (except the
      // first sub-partition) so remainingRowCount = partition_->numRows() - 1
      remainingRowCount =
          doInitialize ? partition_->numRows() : partition_->numRows() - 1;
    } else {
      if (partition_->numRows() == 1) {
        remainingRowCount = partition_->numRows();
      } else {
        remainingRowCount = partition_->numRows() - 1;
      }
    }

    do {
      auto numRows = std::min(remainingRowCount, kBatchSize);
      rowNumbers_.resize(numRows);
      auto result = BaseVector::create(resultType_, numRows, pool());

      if (constantOffset_.has_value() || isConstantOffsetNull_) {
        setRowNumbersForConstantOffset();
      } else {
        if (ignoreNullsForPartition_) {
          setRowNumbers<true>(numRows);
        } else {
          setRowNumbers<false>(numRows);
        }
      }
      if constexpr (isLag) {
        if (doInitialize) {
          // this is the first sub-partition. For the remaining sub-partitions,
          // when calling LeadLagFunction<true>::setRowNumbersForConstantOffset
          // do not compare offset and partitionOffset_
          // because For the remaining sub-partitions, the first row in the
          // partition_ is the 'previousRow_' and 'previousRow_' is JUST for
          // reference to calculate lag() for current row, so need to skip the
          // first row in the sub-partition for result correctness
          compareOffsetAndPartitionOffset = false;
        } else {
          // if not the first sub-partition, bump all rowNumbers_ by 1
          // because the first row in the partition_ is the 'previousRow_'
          // and 'previousRow_' is JUST for reference to calculate lag() for
          // current row
          for (int i = 0; i < rowNumbers_.size(); i++) {
            rowNumbers_[i] = rowNumbers_[i] + 1;
          }
        }
      }
      auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
      partition_->extractColumn(valueIndex_, rowNumbersRange, 0, result);
      setDefaultValue(result, 0);
      results_.emplace_back(std::move(result));
      partitionOffset_ += numRows;
      remainingRowCount -= numRows;
    } while (remainingRowCount);
  } // namespace

  std::deque<VectorPtr> getAggregateResultVector() override {
    if (isLag) {
      compareOffsetAndPartitionOffset = true;
    } else {
      auto lastBatchResult = results_.back();
      // increase the final batch result size by 1
      auto correctLastBatchResult =
          BaseVector::create(resultType_, lastBatchResult->size() + 1, pool());
      correctLastBatchResult->copy(
          lastBatchResult.get(), 0, 0, lastBatchResult->size());
      if (numArgs_ > 2) {
        if (constantDefaultValue_) {
          // lead(c1, 1, 100)
          // Copy default value into 'correctLastBatchResult'
          correctLastBatchResult->copy(
              constantDefaultValue_.get(),
              correctLastBatchResult->size() - 1,
              0,
              1);
        } else {
          // lead(c1, 1, c1) or lead(c1)
          // Copy last value of 'lastBatchResult' into 'correctLastBatchResult'
          correctLastBatchResult->copy(
              lastBatchResult.get(),
              correctLastBatchResult->size() - 1,
              lastBatchResult->size() - 1,
              lastBatchResult->size());
        }
      } else {
        // lead(c1, 1)
        // lead(c1)
        correctLastBatchResult->setNull(
            correctLastBatchResult->size() - 1, true);
      }
      // update the last batch result in results_
      results_.pop_back();
      results_.emplace_back(std::move(correctLastBatchResult));
    }
    return results_;
  }

  void cleanUp() override {
    results_.clear();
    results_.shrink_to_fit();
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& /*frameEnds*/,
      const SelectivityVector& /*validRows*/,
      int32_t resultOffset,
      const VectorPtr& result) override {
    const auto numRows = frameStarts->size() / sizeof(vector_size_t);

    rowNumbers_.resize(numRows);

    if (constantOffset_.has_value() || isConstantOffsetNull_) {
      setRowNumbersForConstantOffset();
    } else {
      if (ignoreNullsForPartition_) {
        setRowNumbers<true>(numRows);
      } else {
        setRowNumbers<false>(numRows);
      }
    }
    bool exactSize = (partition_->numRows() == 1) ? true : false;
    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result, exactSize);

    setDefaultValue(result, resultOffset);

    partitionOffset_ += numRows;
  }

 private:
  // Lead/Lag return default value (using kDefaultValueRow) if offsets for
  // target rowNumbers are outside the partition. If offset is null, then the
  // functions return null (using kNullRow) .
  //
  // kDefaultValueRow needs to be a negative number so that
  // WindowPartition::extractColumn calls skip this row. It is set to -2 to
  // distinguish it from kNullRow which is -1.
  static constexpr vector_size_t kDefaultValueRow = -2;

  void initializeOffset(const std::vector<exec::WindowFunctionArg>& args) {
    if (args.size() == 1) {
      constantOffset_ = 1;
      return;
    }

    const auto& offsetArg = args[1];
    if (auto constantOffset = offsetArg.constantValue) {
      if (constantOffset->isNullAt(0)) {
        isConstantOffsetNull_ = true;
      } else {
        if (offsetArg.type->kind() == TypeKind::INTEGER) {
          constantOffset_ =
              constantOffset->as<ConstantVector<int32_t>>()->valueAt(0);
        } else {
          constantOffset_ =
              constantOffset->as<ConstantVector<int64_t>>()->valueAt(0);
        }
        BOLT_USER_CHECK_GE(
            constantOffset_.value(), 0, "Offset must be at least 0");
        if (constantOffset_.value() == 0) {
          isConstantOffsetZero_ = true;
        }
      }
    } else {
      offsetIndex_ = offsetArg.index.value();
      offsets_ = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool());
    }
  }

  void initializeDefaultValue(
      const std::vector<exec::WindowFunctionArg>& args) {
    if (args.size() <= 2) {
      return;
    }

    const auto& defaultValueArg = args[2];
    if (defaultValueArg.constantValue) {
      // Null default value is equivalent to no default value.
      if (!defaultValueArg.constantValue->isNullAt(0)) {
        constantDefaultValue_ = defaultValueArg.constantValue;
      }
    } else {
      defaultValueIndex_ = defaultValueArg.index.value();
      defaultValues_ = BaseVector::create(defaultValueArg.type, 0, pool());
    }
  }

  void setRowNumbersForConstantOffset(vector_size_t offset);

  void setRowNumbersForConstantOffset() {
    // Set row number to kNullRow for NULL offset.
    if (isConstantOffsetNull_) {
      std::fill(rowNumbers_.begin(), rowNumbers_.end(), kNullRow);
      return;
    }
    // If the offset is 0 then it means always return the current row.
    if (isConstantOffsetZero_) {
      std::iota(rowNumbers_.begin(), rowNumbers_.end(), partitionOffset_);
      return;
    }

    auto constantOffsetValue = constantOffset_.value();
    // Set row number to kDefaultValueRow for out of range offset.
    if (constantOffsetValue > partition_->numRows()) {
      std::fill(rowNumbers_.begin(), rowNumbers_.end(), kDefaultValueRow);
      return;
    }

    setRowNumbersForConstantOffset(constantOffsetValue);
  }

  template <bool ignoreNulls>
  void setRowNumbers(vector_size_t numRows) {
    offsets_->resize(numRows);
    bool exactSize = (partition_->numRows() == 1) ? true : false;
    partition_->extractColumn(
        offsetIndex_, partitionOffset_, numRows, 0, offsets_, exactSize);

    const auto maxRowNumber = partition_->numRows() - 1;
    auto* rawNulls = nulls_->as<uint64_t>();
    for (auto i = 0; i < numRows; ++i) {
      // Set row number to kNullRow for NULL offset.
      if (offsets_->isNullAt(i)) {
        rowNumbers_[i] = kNullRow;
      } else {
        auto offset = offsets_->valueAt(i);
        BOLT_USER_CHECK_GE(offset, 0, "Offset must be at least 0");
        // Set rowNumber to kDefaultValueRow for out of range offset.
        if (offset > partition_->numRows()) {
          rowNumbers_[i] = kDefaultValueRow;
          continue;
        }
        // If the offset is 0 then it means always return the current row.
        if (offset == 0) {
          rowNumbers_[i] = partitionOffset_ + i;
          continue;
        }

        if constexpr (isLag) {
          if constexpr (ignoreNulls) {
            rowNumbers_[i] = rowNumberIgnoreNull(
                rawNulls, offset, partitionOffset_ + i - 1, -1, -1);
          } else {
            // Set rowNumber to kDefaultValueRow for out of range offset.
            auto rowNumber = partitionOffset_ + i - offset;
            rowNumbers_[i] = rowNumber >= 0 ? rowNumber : kDefaultValueRow;
          }
        } else {
          if constexpr (ignoreNulls) {
            rowNumbers_[i] = rowNumberIgnoreNull(
                rawNulls,
                offset,
                partitionOffset_ + i + 1,
                partition_->numRows(),
                1);
          } else {
            // Set rowNumber to kDefaultValueRow for out of range offset.
            auto rowNumber = partitionOffset_ + i + offset;
            rowNumbers_[i] =
                rowNumber <= maxRowNumber ? rowNumber : kDefaultValueRow;
          }
        }
      }
    }
  }

  // This method assumes the input offset > 0
  vector_size_t rowNumberIgnoreNull(
      const uint64_t* rawNulls,
      vector_size_t offset,
      vector_size_t start,
      vector_size_t end,
      vector_size_t step) {
    auto nonNullCount = 0;
    for (auto j = start; j != end; j += step) {
      if (!bits::isBitSet(rawNulls, j)) {
        nonNullCount++;
        if (nonNullCount == offset) {
          return j;
        }
      }
    }

    return kDefaultValueRow;
  }

  void setDefaultValue(const VectorPtr& result, int32_t resultOffset) {
    // Default value is not specified, just return.
    if (!constantDefaultValue_ && !defaultValueIndex_) {
      return;
    }

    // Copy default values into 'result' for rows with invalid offsets or empty
    // frames.
    if (constantDefaultValue_) {
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kDefaultValueRow) {
          result->copy(constantDefaultValue_.get(), resultOffset + i, 0, 1);
        }
      }
    } else {
      std::vector<vector_size_t> defaultValueRowNumbers;
      defaultValueRowNumbers.reserve(rowNumbers_.size());
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kDefaultValueRow) {
          defaultValueRowNumbers.push_back(partitionOffset_ + i);
        }
      }

      if (defaultValueRowNumbers.empty()) {
        return;
      }
      bool exactSize = (partition_->numRows() == 1) ? true : false;
      partition_->extractColumn(
          defaultValueIndex_.value(),
          folly::Range(
              defaultValueRowNumbers.data(), defaultValueRowNumbers.size()),
          0,
          defaultValues_,
          exactSize);

      for (auto i = 0; i < defaultValueRowNumbers.size(); ++i) {
        result->copy(
            defaultValues_.get(),
            resultOffset + defaultValueRowNumbers[i] - partitionOffset_,
            i,
            1);
      }
    }
  }

  const bool ignoreNulls_;

  // Certain partitions may not have null values. So ignore nulls processing can
  // be skipped for them. Used for tracking this at the partition level.
  bool ignoreNullsForPartition_;

  // Index of the 'value' argument.
  column_index_t valueIndex_;

  // Index of the 'offset' argument if offset is not constant.
  column_index_t offsetIndex_;

  TypePtr resultType_;

  // store the result from each call of computeSpillableAggregate()
  std::deque<VectorPtr> results_;

  // 1. when using spillable window, a large partition will be further
  // cut into several smaller sub-partitions. This variable indicates if this is
  // the first iteration, aka. first sub-partiton, that needs to compare the
  // 'offset' defined in LAG function and partitionOffsets_ when executing
  // setRowNumbersForConstantOffset()
  // 2. when NOT using spillable window, this variable is always true
  bool compareOffsetAndPartitionOffset = true;

  // Value of the 'offset' if constant.
  std::optional<int64_t> constantOffset_;
  bool isConstantOffsetNull_ = false;
  bool isConstantOffsetZero_ = false;

  // Index of the 'default_value' argument if default value is specified and not
  // constant.
  std::optional<column_index_t> defaultValueIndex_;

  // Constant 'default_value' or null if default value is not constant.
  VectorPtr constantDefaultValue_;

  const exec::WindowPartition* partition_;

  // Reusable vector of offsets if these are not constant.
  FlatVectorPtr<int64_t> offsets_;

  // Reusable vector of default values if these are not constant.
  VectorPtr defaultValues_;

  // Null positions buffer to use for ignoreNulls.
  BufferPtr nulls_;

  // This offset tracks how far along the partition rows have been output.
  // This can be used to optimize reading offset column values corresponding
  // to the present row set in getOutput.
  vector_size_t partitionOffset_ = 0;

  // The Lag function directly writes from the input column to the
  // resultVector using the extractColumn API specifying the rowNumber mapping
  // to copy between the 2 vectors. This variable is used for the rowNumber
  // vector across getOutput calls.
  std::vector<vector_size_t> rowNumbers_;

  int numArgs_;
}; // namespace

template <>
void LeadLagFunction<true>::setRowNumbersForConstantOffset(
    vector_size_t offset) {
  // Figure out how many rows at the start is out of range.
  vector_size_t nullCnt = 0;
  if (offset > partitionOffset_ && compareOffsetAndPartitionOffset) {
    nullCnt =
        std::min<vector_size_t>(offset - partitionOffset_, rowNumbers_.size());
    if (nullCnt) {
      std::fill(
          rowNumbers_.begin(), rowNumbers_.begin() + nullCnt, kDefaultValueRow);
    }
  }

  if (ignoreNullsForPartition_) {
    auto rawNulls = nulls_->as<uint64_t>();
    for (auto i = nullCnt; i < rowNumbers_.size(); i++) {
      rowNumbers_[i] = rowNumberIgnoreNull(
          rawNulls, offset, partitionOffset_ + i - 1, -1, -1);
    }
  } else {
    // Populate sequential values for non-NULL rows.
    std::iota(
        rowNumbers_.begin() + nullCnt,
        rowNumbers_.end(),
        partitionOffset_ + nullCnt - offset);
  }
}

template <>
void LeadLagFunction<false>::setRowNumbersForConstantOffset(
    vector_size_t offset) {
  // Figure out how many rows at the end is out of range.
  vector_size_t nonNullCnt = std::max<vector_size_t>(
      0,
      std::min<vector_size_t>(
          rowNumbers_.size(),
          partition_->numRows() - partitionOffset_ - offset));
  if (nonNullCnt < rowNumbers_.size()) {
    std::fill(
        rowNumbers_.begin() + nonNullCnt, rowNumbers_.end(), kDefaultValueRow);
  }

  // Populate sequential values for non-NULL rows.
  if (nonNullCnt > 0) {
    if (ignoreNullsForPartition_) {
      auto rawNulls = nulls_->as<uint64_t>();
      for (auto i = 0; i < nonNullCnt; i++) {
        rowNumbers_[i] = rowNumberIgnoreNull(
            rawNulls,
            offset,
            partitionOffset_ + i + 1,
            partition_->numRows(),
            1);
      }
    } else {
      std::iota(
          rowNumbers_.begin(),
          rowNumbers_.begin() + nonNullCnt,
          partitionOffset_ + offset);
    }
  }
}

std::vector<exec::FunctionSignaturePtr> signatures() {
  return {
      // T -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .build(),
      // (T, bigint) -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("bigint")
          .build(),
      // (T, bigint, T) -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("bigint")
          .argumentType("T")
          .build(),
  };
}

} // namespace

void registerLag(const std::string& name) {
  exec::registerWindowFunction(
      name,
      signatures(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool ignoreNulls,
          bolt::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const bolt::core::QueryConfig&
          /*queryConfig*/) -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<true>>(
            args, resultType, ignoreNulls, pool);
      });
}

void registerLead(const std::string& name) {
  exec::registerWindowFunction(
      name,
      signatures(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool ignoreNulls,
          bolt::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const bolt::core::QueryConfig&
          /*queryConfig*/) -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<false>>(
            args, resultType, ignoreNulls, pool);
      });
}
} // namespace bytedance::bolt::window::prestosql