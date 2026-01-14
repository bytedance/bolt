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

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>

#include "bolt/common/encode/StringDictionary.h"
#include "bolt/common/memory/Allocation.h"
namespace bytedance::bolt::memory {

// Why not use the debug mode in MemoryPool directly?
//
// First, because we have some logic for real-time monitoring of memory usage
// and printing stacks, the code will be more complicated and difficult to debug
// after being combined with the debug mode. This is the main reason.
//
// Second, the original debug mode in MemoryPool is designed to detect memory
// leaks, so there is only a mapping relationship of address -> (size,
// callstack), and there is no mapping relationship from callstack to address,
// which is not perfect in terms of function.
//
// In addition, the previous design directly stored the stack string, which
// consumed more memory. We used dictionaries to make some optimizations, which
// will improve the speed of container operations.
class MemoryAllocationListener {
 public:
  // There are 4 memory allocation monitoring modes in total:
  // 1. kDisable, ListenMode_ is in this mode when both
  // singleAllocationThreshold and cumulativeAllocationThreshold are 0. Online
  // tasks should also be in this mode. This mode has almost no impact on
  // performance.
  // 2. kOnlySingle, ListenMode_ is in this mode when singleAllocationThreshold
  // is not 0 and cumulativeAllocationThreshold is 0. In this mode, if a large
  // amount of memory allocation is monitored at one time, the stack will be
  // printed.
  // 3. kOnlyAccumulative, ListenMode_ is in this mode when
  // singleAllocationThreshold is 0 and cumulativeAllocationThreshold is not 0.
  // In this mode, only the cumulative memory allocation is monitored, and the
  // single memory allocation is not paid attention to.
  // 4. kBoth, ListenMode_ is in this mode when singleAllocationThreshold is not
  // 0 and cumulativeAllocationThreshold is not 0. This mode is equivalent to
  // the combination of kOnlySingle mode and kOnlyAccumulative mode.
  enum ListenMode { kDisable = 0, kOnlySingle, kOnlyAccumulative, kBoth };

  MemoryAllocationListener(
      const std::string& name,
      const MemoryPool* pool,
      const std::string& poolRegex,
      int64_t singleAllocationThreshold,
      int64_t accumulativeAllocationThreshold);

  ~MemoryAllocationListener();

  int64_t getSingleAllocationThreshold();

  int64_t getAccumulativeAllocationThreshold();

  std::string getPoolRegex();

  void recordAlloc(const Allocation& allocation);

  void recordAlloc(const ContiguousAllocation& allocation);

  void recordAlloc(const void* addr, int64_t size);

  void recordFree(const Allocation& allocation);

  void recordFree(const ContiguousAllocation& allocation);

  void recordFree(const void* addr, int64_t size);

  void recordGrow(
      const void* oldAddr,
      const void* addr,
      int64_t oldSize,
      int64_t Size);

  void detectHugeReserve(int64_t size);

  void memoryLeakCheck();

  std::string getAllStack(bool ignoreEmptyAllocation = true);

  void printAllStack(bool ignoreEmptyAllocation = true);

  void printOnceAllocateStack(const void* addr, int64_t size);

 private:
  void recordAllocInternal(const void* addr, int64_t size);

  void recordFreeInternal(const void* addr, int64_t size);

  void recordGrowInternal(
      const void* oldAddr,
      const void* addr,
      int64_t oldSize,
      int64_t size);

  int64_t storeStack(const std::string& stack);

  std::string retrieveStack(const int64_t stackId) const;

  struct MemRecord {
    int64_t stackId;
    int64_t size;
  };

  ListenMode listenMode_;
  std::string poolName_;
  std::string poolRegex_;
  const int64_t kAccumulativeReportStep;
  int64_t allocateBytes_{0};
  int64_t singleAllocationThreshold_{0};
  int64_t accumulativeAllocationThreshold_{0};

  std::mutex mutex_;
  std::unordered_map<uint64_t, MemRecord> addrToStack_;
  std::unordered_map<int64_t, int64_t> stackToSize_;
  // store stack via dict
  StringDictionary<int64_t> stackDict_;

  bool usageTrackingEnabled_{false};
  std::map<std::string, std::tuple<int, int, std::string, std::string>>
      usageMap_;
  std::map<const void*, std::string> usageAddrToStackMap_;
};

} // namespace bytedance::bolt::memory

template <>
struct fmt::formatter<
    bytedance::bolt::memory::MemoryAllocationListener::ListenMode>
    : fmt::formatter<std::string_view> {
  auto format(
      bytedance::bolt::memory::MemoryAllocationListener::ListenMode c,
      format_context& ctx) const {
    switch (c) {
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kDisable:
        return fmt::formatter<std::string_view>::format("kDisable", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kOnlySingle:
        return fmt::formatter<std::string_view>::format("kOnlySingle", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kOnlyAccumulative:
        return fmt::formatter<std::string_view>::format(
            "kOnlyAccumulative", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::kBoth:
        return fmt::formatter<std::string_view>::format("kBoth", ctx);
      default:
        return fmt::format_to(ctx.out(), "unknown[{}]", static_cast<int>(c));
    }
  }
};
