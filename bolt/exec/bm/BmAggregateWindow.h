#pragma once

#include "bolt/exec/WindowFunction.h"

namespace bytedance::bolt::exec::window {

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
