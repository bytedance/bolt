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

namespace bytedance::bolt::exec::window {

// Test observation counters are collected only in Debug builds.
struct BmAggregateWindowTestStats {
  uint64_t materializeCalls{0};
  uint64_t materializedRows{0};
  uint64_t maxMaterializedRows{0};
  uint64_t reverseIncrementalCalls{0};
  uint64_t reverseIncrementalRows{0};
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

} // namespace bytedance::bolt::exec::window
