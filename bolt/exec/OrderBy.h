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

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Spiller.h"
namespace bytedance::bolt::exec {

/// OrderBy operator implementation: OrderBy stores all its inputs in a
/// RowContainer as the inputs are added. Until all inputs are available,
/// it blocks the pipeline. Once all inputs are available, it sorts pointers
/// to the rows using the RowContainer's compare() function. And finally it
/// constructs and returns the sorted output RowVector using the data in the
/// RowContainer.
/// Limitations:
/// * It memcopies twice: 1) input to RowContainer and 2) RowContainer to
/// output.
class OrderBy : public Operator {
 public:
  OrderBy(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL driverCtx,
      const std::shared_ptr<const core::OrderByNode>& orderByNode);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* FOLLY_NULLABLE /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  bool canReclaim() const override {
    // sortBuffer_ may be released
    return Operator::canReclaim() && sortBuffer_;
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  // Invoked to record the spilling stats in operator stats after processing all
  // the inputs.
  void recordSpillStats();
  void recordSpillReadStats();
  void recordSortStats();

  std::unique_ptr<SortBuffer> sortBuffer_;
  bool finished_ = false;
  vector_size_t maxOutputRows_;
};
} // namespace bytedance::bolt::exec
