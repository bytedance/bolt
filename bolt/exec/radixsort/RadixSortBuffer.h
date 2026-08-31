/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <optional>
#include <vector>

#include <folly/Synchronized.h>

#include "bolt/common/base/Portability.h"
#include "bolt/common/base/SortStat.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/exec/SortBufferBase.h"
#include "bolt/exec/radixsort/RadixSortRun.h"
#include "bolt/exec/radixsort/RadixSortSpill.h"

namespace bytedance::bolt::exec::radixsort {

namespace test {
class RadixSortBufferTestHelper;
}

class RadixSortBuffer : public SortBufferBase {
 public:
  RadixSortBuffer(
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& sortColumnIndices,
      const std::vector<CompareFlags>& sortCompareFlags,
      memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig = nullptr,
      uint64_t spillMemoryThreshold = 0,
      OperatorCtx* operatorCtx = nullptr,
      tsan_atomic<bool>* nonReclaimableSection = nullptr);

  ~RadixSortBuffer() override;

  void addInput(const VectorPtr& input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput(vector_size_t maxOutputRows) override;

  std::optional<common::SortStats> sortStats() const override;

  bool canSpill() const override {
    return spillConfig_ != nullptr;
  }

  bool canReclaim() const override {
    if (!canSpill()) {
      return false;
    }
    if (!noMoreInput_) {
      return run_->size() > 0;
    }
    return run_->retainedBytes() > 0;
  }

  void spill() override;

  std::optional<common::SpillStats> spilledStats() const override {
    auto stats = stats_.copy();
    return stats.spillRuns == 0 ? std::nullopt
                                : std::make_optional(std::move(stats));
  }

  size_t testingSpilledRunCount() const {
    return spilledRuns_.size();
  }

  size_t testingMergeStreamCount() const {
    return merger_ == nullptr ? 0 : merger_->testingNumStreams();
  }

  std::optional<common::SpillReadStats> spillReadStats() const override;

  size_t numInputRows() const override;

  size_t numOutputRows() const override;

  std::optional<uint64_t> estimateOutputRowSize() const override;

 private:
  friend class test::RadixSortBufferTestHelper;

  struct OutputAdmissionEstimate {
    uint64_t outputGrowth{0};
    uint64_t scratchGrowth{0};

    uint64_t total() const;
  };

  std::unique_ptr<RadixSortRun> makeRun() const;

  void ensureInputFits(const VectorPtr& input);

  void ensureMergeFits();

  void ensureOutputFits(vector_size_t batchSize);

  void reserveOutputForCurrentState(vector_size_t batchSize);

  OutputAdmissionEstimate outputAdmissionEstimate(vector_size_t batchSize);

  bool canReuseOutput(vector_size_t batchSize) const;

  void spillBuildingRun();

  void spillMemoryRun();

  void prepareMerge();

  void prepareOutputShell(vector_size_t outputBatchSize);

  void prepareMergeOutputVector(vector_size_t outputBatchSize);

  RowVectorPtr getMergedOutput(vector_size_t count);

  void ensureMergeRowPointerBuffers(vector_size_t count);

  const RowTypePtr inputType_;
  memory::MemoryPool* const pool_;
  tsan_atomic<bool>* const nonReclaimableSection_;
  RowTypePtr keyType_;
  std::vector<CompareFlags> sortCompareFlags_;
  std::vector<column_index_t> directKeyChannels_;
  std::vector<uint8_t> keyMayHaveNulls_;
  std::vector<uint8_t> payloadMayHaveNulls_;
  RadixSortRunOptions runOptions_;
  const common::SpillConfig* spillConfig_{nullptr};
  uint64_t spillMemoryThreshold_{0};
  OperatorCtx* operatorCtx_{nullptr};
  std::unique_ptr<RadixSortRun> run_;
  std::vector<RadixSortSpillRun> spilledRuns_;
  folly::Synchronized<common::SpillStats> stats_;
  std::unique_ptr<RadixSortMerger> merger_;
  BufferPtr mergeKeyRows_;
  BufferPtr mergePayloadRows_;
  RowVectorPtr output_;
  std::optional<uint64_t> estimatedOutputRowSize_;
  bool noMoreInput_{false};
  uint64_t inputRows_{0};
  uint64_t outputRows_{0};
  uint64_t storedRows_{0};
  uint64_t storedBytes_{0};
  bool variableKeysFitRadixPrefix_{true};
  uint64_t encodeTimeUs_{0};
  uint64_t appendTimeUs_{0};
  uint64_t sortTimeUs_{0};
  uint64_t outputTimeUs_{0};
};

} // namespace bytedance::bolt::exec::radixsort
