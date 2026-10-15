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

#pragma once

#include "bolt/exec/WindowFunction.h"

#include <optional>

namespace bytedance::bolt::exec::window {

// Test observation counters are collected only in Debug builds.
struct BmAggregateWindowTestStats {
  uint64_t materializeCalls{0};
  uint64_t materializedRows{0};
  uint64_t maxMaterializedRows{0};
  uint64_t reverseIncrementalCalls{0};
  uint64_t reverseIncrementalRows{0};
  uint64_t sharedScanCalls{0};
  uint64_t sharedScanFunctions{0};
  uint64_t inputAggregationRows{0};
  uint64_t inputAggregationPartitions{0};
  uint64_t inputAggregationFunctions{0};
};

void resetBmAggregateWindowTestStats();

BmAggregateWindowTestStats bmAggregateWindowTestStats();

std::unique_ptr<exec::WindowFunction> createBmAggregateWindowFunction(
    const std::string& name,
    const std::vector<exec::WindowFunctionArg>& args,
    const TypePtr& resultType,
    bool ignoreNulls,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config);

class BmAggregateWindowFunction;
class BmWindowPartition;

// Shares argument columns and aggregate updates for full-partition frames.
// Batches come from the original input or a shared scan of stored BM rows.
class BmEntirePartitionAggregation {
 public:
  explicit BmEntirePartitionAggregation(memory::MemoryPool* pool);

  void registerFunction(BmAggregateWindowFunction* function);

  void resetPartition(const BmWindowPartition* partition);

  void prepare();

  void
  addInput(const RowVectorPtr& input, vector_size_t offset, vector_size_t size);

  std::vector<VectorPtr> finishPartition();

  size_t numFunctions() const {
    return functions_.size();
  }

 private:
  void addBatch(
      const std::vector<VectorPtr>& inputs,
      vector_size_t batchRows,
      vector_size_t offset,
      vector_size_t size);

  memory::MemoryPool* const pool_;
  const BmWindowPartition* partition_{nullptr};
  bool prepared_{false};
  uint64_t inputRows_{0};
  std::vector<BmAggregateWindowFunction*> functions_;
  std::vector<std::vector<std::optional<size_t>>> functionInputIndices_;
  std::vector<column_index_t> inputChannels_;
  std::vector<TypePtr> inputTypes_;
  std::vector<VectorPtr> inputVectors_;
};

std::shared_ptr<BmEntirePartitionAggregation> prepareBmAggregateWindowFunctions(
    const std::vector<core::WindowNode::Function>& functionSpecs,
    const std::vector<std::unique_ptr<exec::WindowFunction>>& functions,
    memory::MemoryPool* pool,
    bool enableInputAggregation = true);

} // namespace bytedance::bolt::exec::window
