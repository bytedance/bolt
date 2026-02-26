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

#include "bolt/common/memory/MemoryAllocationListener.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/memory/MemoryPool.h"

#include <folly/Likely.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>
namespace bytedance::bolt::memory {

MemoryAllocationListener::MemoryAllocationListener(
    const std::string& name,
    const MemoryPool* pool,
    const std::string& poolRegex,
    int64_t singleAllocationThreshold,
    int64_t accumulativeAllocationThreshold)
    : poolName_(name),
      poolRegex_(poolRegex),
      kAccumulativeReportStep(accumulativeAllocationThreshold),
      singleAllocationThreshold_(singleAllocationThreshold),
      accumulativeAllocationThreshold_(accumulativeAllocationThreshold) {
  listenMode_ = ListenMode::kDisable;
  if (accumulativeAllocationThreshold == 0 && singleAllocationThreshold == 0) {
    listenMode_ = ListenMode::kDisable;
  } else if (
      accumulativeAllocationThreshold > 0 && singleAllocationThreshold == 0) {
    listenMode_ = ListenMode::kOnlyAccumulative;
  } else if (
      accumulativeAllocationThreshold == 0 && singleAllocationThreshold > 0) {
    listenMode_ = ListenMode::kOnlySingle;
  } else if (
      accumulativeAllocationThreshold > 0 && singleAllocationThreshold > 0) {
    listenMode_ = ListenMode::kBoth;
  }

  if (listenMode_ != ListenMode::kDisable) {
    std::string bloodline;
    const MemoryPool* p = pool;
    while (p != nullptr) {
      bloodline += p->name();
      p = p->parent();
    }
    const std::regex regex(poolRegex);
    // When debugging, regex portability is more important than performance
    if (!poolRegex.empty() && !std::regex_match(bloodline, regex)) {
      listenMode_ = ListenMode::kDisable;
    }
    // In order to keep the position consistent with the stack printing, the log
    // here also needs to be printed to std::cout.
    std::cout << "ListenMode is " << listenMode_
              << ", singleAllocationThreshold=" << singleAllocationThreshold_
              << ", accumulativeAllocationThreshold="
              << accumulativeAllocationThreshold_ << ", poolName=" << name
              << ", bloodline=" << bloodline << ", poolRegx=" << poolRegex_
              << std::endl;
  }
  usageTrackingEnabled_ = false;
}

MemoryAllocationListener::~MemoryAllocationListener() {
  if (usageTrackingEnabled_) {
    std::string ans = "";
    std::vector<std::string> keys;
    for (const auto& pair : usageMap_) {
      keys.emplace_back(pair.first);
    }
    std::sort(
        keys.begin(),
        keys.end(),
        [](const std::string& left, const std::string& right) {
          return stoi(left.substr(0, left.find('_'))) >
              stoi(right.substr(0, right.find('_')));
        });

    for (const auto& key : keys) {
      auto pair = usageMap_.find(key);
      if (pair == usageMap_.end()) {
        continue;
      }
      auto notBeUsedSize = std::get<0>(pair->second);
      auto totalSize = std::get<1>(pair->second);
      auto freeStack = std::get<2>(pair->second);
      auto allocateStack = std::get<3>(pair->second);
      if (notBeUsedSize == 0) {
        continue;
      }
      ans += "name is:" + pair->first + ", waste ratio is:" +
          std::to_string(notBeUsedSize * 1.0 / totalSize) + "." +
          " The memory not be use size is " + std::to_string(notBeUsedSize) +
          ", the total size is " + std::to_string(totalSize) +
          ", the allocate stack is below:" + allocateStack +
          "\n, the freeStack is below:\n" + freeStack;
    }
    if (!ans.empty()) {
      LOG(INFO) << ans;
    }
  }
}

int64_t MemoryAllocationListener::getSingleAllocationThreshold() {
  return singleAllocationThreshold_;
}

int64_t MemoryAllocationListener::getAccumulativeAllocationThreshold() {
  return kAccumulativeReportStep;
}

std::string MemoryAllocationListener::getPoolRegex() {
  return poolRegex_;
}

void MemoryAllocationListener::recordAlloc(const void* addr, int64_t size) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  recordAllocInternal(addr, size);
}

void MemoryAllocationListener::recordAlloc(const Allocation& allocation) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (allocation.empty()) {
    return;
  }
  for (int i = 0; i < allocation.numRuns(); i++) {
    auto run = allocation.runAt(i);
    void* addr = run.data();
    int64_t size = run.numBytes();
    recordAllocInternal(addr, size);
  }
}

void MemoryAllocationListener::recordAlloc(
    const ContiguousAllocation& allocation) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (allocation.empty()) {
    return;
  }
  void* addr = allocation.data();
  int64_t size = allocation.size();
  recordAllocInternal(addr, size);
}

void MemoryAllocationListener::recordFree(const void* addr, int64_t freeSize) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  recordFreeInternal(addr, freeSize);
}

void MemoryAllocationListener::recordFree(const Allocation& allocation) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (allocation.empty()) {
    return;
  }
  for (int i = 0; i < allocation.numRuns(); i++) {
    auto run = allocation.runAt(i);
    void* addr = run.data();
    int64_t size = run.numBytes();
    recordFreeInternal(addr, size);
  }
}

void MemoryAllocationListener::recordFree(
    const ContiguousAllocation& allocation) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (allocation.empty()) {
    return;
  }
  recordFreeInternal(allocation.data(), allocation.size());
}

void MemoryAllocationListener::recordGrow(
    const void* oldAddr,
    const void* addr,
    int64_t oldSize,
    int64_t size) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  recordGrowInternal(oldAddr, addr, oldSize, size);
}

void MemoryAllocationListener::memoryLeakCheck() {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (allocateBytes_ != 0) {
    std::cout << "Expect free all memory, but still exists "
              << succinctBytes(allocateBytes_) << std::endl;
    printAllStack();
  } else {
    std::cout << "Free all memory correctly!" << std::endl;
  }
}

void MemoryAllocationListener::detectHugeReserve(int64_t size) {
  if (FOLLY_LIKELY(listenMode_ == ListenMode::kDisable)) {
    return;
  }
  if (size > singleAllocationThreshold_ && singleAllocationThreshold_ != 0) {
    const auto stackTrace = process::StackTrace().toString();
    // To avoid mixing with other logs, we use std::cout instead of LOG(...)
    std::cout << "Detect huge reserve, size=" << succinctBytes(size)
              << ", poolName=" << poolName_ << ", stack is below:" << std::endl
              << stackTrace << std::endl;
  }
}

void MemoryAllocationListener::recordAllocInternal(
    const void* addr,
    int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (size <= 0) {
    return;
  }

  if (usageTrackingEnabled_) {
    std::memset(const_cast<void*>(addr), 0x00, size);
    auto stack = process::StackTrace().toString();
    usageAddrToStackMap_.emplace(addr, stack);
  }

  auto updateAccumulative = [&]() {
    uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
    const auto stackTrace = process::StackTrace().toString();
    const int64_t stackId = storeStack(stackTrace);

    auto s2s = stackToSize_.find(stackId);
    if (s2s != stackToSize_.end()) {
      s2s->second += size;
    } else {
      stackToSize_.emplace(stackId, size);
    }
    addrToStack_[addrUint64] = MemRecord{stackId, size};

    // update all allocate
    allocateBytes_ += size;

    if (allocateBytes_ > accumulativeAllocationThreshold_) {
      printAllStack();
      accumulativeAllocationThreshold_ += kAccumulativeReportStep;
    }
  };

  auto updateSingle = [&]() {
    if (size > singleAllocationThreshold_) {
      printOnceAllocateStack(addr, size);
    }
  };

  switch (listenMode_) {
    case ListenMode::kBoth: {
      updateAccumulative();
      updateSingle();
      break;
    }
    case ListenMode::kOnlyAccumulative: {
      updateAccumulative();
      break;
    }
    case ListenMode::kOnlySingle: {
      updateSingle();
      break;
    }
    case ListenMode::kDisable: {
      break;
    }
    default: {
      BOLT_FAIL("Unknown listen type {}", listenMode_);
    }
  }
}

void MemoryAllocationListener::recordFreeInternal(
    const void* addr,
    int64_t freeSize) {
  if (addr == nullptr || freeSize <= 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);

  if (usageTrackingEnabled_) {
    static int64_t order = 0;
    int count = 0;
    for (int i = 0; i < freeSize; i++) {
      if (reinterpret_cast<const char*>(addr)[i] == 0x00) {
        count++;
      }
    }
    if (count != 0) {
      auto it = usageAddrToStackMap_.find(addr);
      std::string allocateStack;
      if (it == usageAddrToStackMap_.end()) {
        allocateStack = "empty";
      } else {
        allocateStack = it->second;
      }
      auto freeStack = process::StackTrace().toString();
      usageMap_.emplace(
          std::to_string(count) + "_" + std::to_string(order++),
          std::make_tuple(count, freeSize, freeStack, allocateStack));
    }
  }

  auto updateAccumulative = [&]() {
    uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
    auto a2s = addrToStack_.find(addrUint64);
    BOLT_CHECK(
        a2s != addrToStack_.end(),
        "Freeing of un-allocated memory. Free address {}.",
        addrUint64);
    MemRecord record = a2s->second;
    if (record.size != freeSize) {
      const auto allocStackTrace = retrieveStack(record.stackId);
      const auto freeStackTrace = process::StackTrace().toString();
      BOLT_FAIL(fmt::format(
          "[MemoryPool] Trying to free {} bytes on an allocation of {} bytes on address {}.\n"
          "======== Allocation Stack ========\n"
          "{}\n"
          "============ Free Stack ==========\n"
          "{}\n",
          freeSize,
          record.size,
          addr,
          allocStackTrace,
          freeStackTrace));
    }
    auto s2s = stackToSize_.find(record.stackId);
    if (s2s == stackToSize_.end()) {
      const auto stack = retrieveStack(record.stackId);
      BOLT_FAIL("Can't find size usage by stack: {}", stack);
    }
    s2s->second -= freeSize;
    // doesn't need `stackToSize_.erase(as->second)`, because 1 stack may
    // allocate n times, must erase in `addrToStack_`, because memory will be
    // reused
    addrToStack_.erase(addrUint64);

    // update total allocate
    allocateBytes_ -= freeSize;
  };

  switch (listenMode_) {
    case ListenMode::kBoth: {
      updateAccumulative();
      break;
    }
    case ListenMode::kOnlyAccumulative: {
      updateAccumulative();
      break;
    }
    case ListenMode::kOnlySingle: {
      break;
    }
    case ListenMode::kDisable: {
      break;
    }
    default: {
      BOLT_FAIL("Unknown listen type {}", listenMode_);
    }
  }
}

void MemoryAllocationListener::recordGrowInternal(
    const void* oldAddr,
    const void* addr,
    int64_t oldSize,
    int64_t size) {
  if (addr == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);

  auto updateAccumulative = [&]() {
    if (addr == oldAddr) {
      uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
      auto a2s = addrToStack_.find(addrUint64);
      BOLT_CHECK(
          a2s != addrToStack_.end(),
          "Growing of un-allocated memory. Address is {}.",
          addr);
      MemRecord record = a2s->second;
      auto s2s = stackToSize_.find(record.stackId);
      if (s2s == stackToSize_.end()) {
        const auto stack = retrieveStack(record.stackId);
        BOLT_FAIL("Can't find size usage by stack: {}", stack);
      }
      BOLT_CHECK(
          record.size == oldSize,
          "Unexpected extra memory use, record={}, size={}",
          record.size,
          oldSize);
      auto increment = size - record.size;
      // update stack mem usage
      s2s->second = s2s->second + increment;
      // update addr mem usage
      addrToStack_[addrUint64] = MemRecord{record.stackId, size};
      // update total allocate
      allocateBytes_ = allocateBytes_ + increment;

      if (allocateBytes_ > accumulativeAllocationThreshold_) {
        printAllStack();
        accumulativeAllocationThreshold_ += kAccumulativeReportStep;
      }
    } else {
      // re-allocate memory with new address
      uint64_t oldAddrUint64 = reinterpret_cast<uint64_t>(oldAddr);
      auto a2s = addrToStack_.find(oldAddrUint64);
      BOLT_CHECK(
          a2s != addrToStack_.end(),
          "Growing of un-allocated memory. Address is {}.",
          addr);
      MemRecord record = a2s->second;
      auto s2s = stackToSize_.find(record.stackId);
      if (s2s == stackToSize_.end()) {
        const auto stack = retrieveStack(record.stackId);
        BOLT_FAIL("Can't find size usage by stack: {}", stack);
      }
      BOLT_CHECK(
          record.size == oldSize,
          "Unexpected extra memory use, record={}, size={}",
          record.size,
          oldSize);
      // release old
      s2s->second -= record.size;
      allocateBytes_ -= record.size;
      auto stackId = record.stackId;
      addrToStack_.erase(oldAddrUint64);

      // record new
      uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
      s2s = stackToSize_.find(stackId);
      if (s2s != stackToSize_.end()) {
        s2s->second += size;
      } else {
        stackToSize_.emplace(stackId, size);
      }
      addrToStack_[addrUint64] = MemRecord{stackId, size};
      // update all allocate
      allocateBytes_ += size;

      if (allocateBytes_ > accumulativeAllocationThreshold_) {
        printAllStack();
        accumulativeAllocationThreshold_ += kAccumulativeReportStep;
      }
    }
  };

  auto updateSingle = [&]() {
    auto increment = size - oldSize;

    if (increment > singleAllocationThreshold_) {
      printOnceAllocateStack(addr, increment);
    }
  };

  switch (listenMode_) {
    case ListenMode::kBoth: {
      updateAccumulative();
      updateSingle();
      break;
    }
    case ListenMode::kOnlyAccumulative: {
      updateAccumulative();
      break;
    }
    case ListenMode::kOnlySingle: {
      updateSingle();
      break;
    }
    case ListenMode::kDisable: {
      break;
    }
    default: {
      BOLT_FAIL("Unknown listen type {}", listenMode_);
    }
  }
}

int64_t MemoryAllocationListener::storeStack(const std::string& stack) {
  return stackDict_.storeString(stack);
}

std::string MemoryAllocationListener::retrieveStack(
    const int64_t stackId) const {
  const auto stack = stackDict_.getString(stackId);
  BOLT_CHECK(stack.has_value(), "Can't translate stackId={} to stack", stackId);
  return stack.value();
}

void MemoryAllocationListener::printOnceAllocateStack(
    const void* addr,
    int64_t size) {
  const auto stackTrace = process::StackTrace().toString();
  // To avoid mixing with other logs, we use std::cout instead of LOG(...)
  std::cout << "Detect once huge allocation on address=" << addr
            << ", size=" << succinctBytes(size) << ", poolName=" << poolName_
            << ", stack is below:" << std::endl
            << stackTrace << std::endl;
}

std::string MemoryAllocationListener::getAllStack(bool ignoreEmptyAllocation) {
  std::vector<MemRecord> records;
  std::string ans;
  records.reserve(stackToSize_.size());
  for (const auto& it : stackToSize_) {
    if (it.second == 0 && ignoreEmptyAllocation) {
      continue;
    }
    // stack -> size
    records.emplace_back(MemRecord{.stackId = it.first, .size = it.second});
  }
  std::sort(
      records.begin(),
      records.end(),
      [](const MemRecord& left, const MemRecord& right) {
        return left.size > right.size;
      });
  // To avoid mixing with other logs, we use std::cout instead of LOG(...)
  const std::string partDelimeter = "******************************";
  const std::string stackDelimeter = "+++++++++++++++";
  ans += partDelimeter + "\n";
  int rank = 1;
  for (const auto& [stackId, memUsage] : records) {
    const auto stack = retrieveStack(stackId);
    ans += stackDelimeter + "\n";
    ans += "The memory usage ranking is " + std::to_string(rank++) +
        ", this stack uses a total of " + succinctBytes(memUsage) +
        " of memory." + "\n";
    ans += stack + "\n";
    ans += stackDelimeter + "\n";
  }
  ans += partDelimeter + "\n";
  return ans;
}

void MemoryAllocationListener::printAllStack(bool ignoreEmptyAllocation) {
  std::cout << "Total allocate " << succinctBytes(allocateBytes_) << " exceed "
            << succinctBytes(accumulativeAllocationThreshold_) << " on pool "
            << poolName_ << std::endl;
  std::string allStacks = getAllStack(ignoreEmptyAllocation);
  std::cout << allStacks << std::endl;
}

} // namespace bytedance::bolt::memory
