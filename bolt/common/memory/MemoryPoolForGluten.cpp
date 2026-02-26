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

#include "bolt/common/memory/MemoryPoolForGluten.h"

#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory {

MemoryPoolForGluten::MemoryPoolForGluten(
    MemoryManager* manager,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    const Options& options)
    : MemoryPoolImpl(
          manager,
          name,
          kind,
          std::move(parent),
          std::move(reclaimer),
          options) {
  enableDynamicMemoryQuotaManager_ = options.enableDynamicMemoryQuotaManager;
}

MemoryPoolForGluten::~MemoryPoolForGluten() = default;

std::shared_ptr<MemoryPool> MemoryPoolForGluten::genChild(
    std::shared_ptr<MemoryPool> parent,
    const std::string& name,
    Kind kind,
    bool threadSafe,
    const std::function<size_t(size_t)>& getPreferredSize,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  Options poolOptions{
      .alignment = alignment_,
      .trackUsage = trackUsage_,
      .threadSafe = threadSafe,
      .coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled_,
      .getPreferredSize = getPreferredSize,
      .debugOptions = debugOptions_,
      .poolRegex = listener_->getPoolRegex(),
      .singleAllocationThreshold = listener_->getSingleAllocationThreshold(),
      .accumulativeAllocationThreshold =
          listener_->getAccumulativeAllocationThreshold(),
      .enableDynamicMemoryQuotaManager = enableDynamicMemoryQuotaManager_};
  return std::make_shared<MemoryPoolForGluten>(
      manager_, name, kind, parent, std::move(reclaimer), poolOptions);
}

bool MemoryPoolForGluten::maybeReserve(uint64_t size) {
  if (size == 0) {
    return true;
  }
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(size, kGrowthQuantum);
  bool ans = maybeReserveThreadSafe(reservationToAdd);
  // release unused but reserved memory, avoid DynamicMemoryQuotaManager feature
  // got wrong memory usage.
  if (enableDynamicMemoryQuotaManager_) {
    releaseThreadSafe(0, false);
  }

  return ans;
}

bool MemoryPoolForGluten::maybeReserveThreadSafe(uint64_t size) {
  BOLT_CHECK(isLeaf());

  bool success = false;
  int32_t numAttempts = 0;
  int64_t increment = 0;

  for (;; ++numAttempts) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      increment = reservationSizeLocked(size);
      if (increment == 0) {
        minReservationBytes_ = tsanAtomicValue(reservationBytes_);
        sanityCheckLocked();
        // reserve success, skip for-loop
        success = true;
        break;
      }
    }

    bool ans;
    try {
      // In normal case, exception will not be thrown here.
      // But in some unit tests, exception will not be thrown
      ans = maybeReserveIncrementReservationThreadSafe(this, increment);
    } catch (const BoltException& e) {
      LOG(ERROR) << "Unexpected bolt exception occurred! message is: "
                 << e.message() << ", e.what()=" << e.what()
                 << ", stack trace is: " << e.stackTrace();
      releaseThreadSafe(0, false);
      std::rethrow_exception(std::current_exception());
    } catch (const std::exception& e) {
      LOG(ERROR) << "Unexpected std::exception occurred! reason is: "
                 << e.what();
      releaseThreadSafe(0, false);
      std::rethrow_exception(std::current_exception());
    }
    if (!ans) {
      // release unused reservation bytes
      releaseThreadSafe(0, false);
      return false;
    }
    // got enough mem, try again
  }

  return success;
}

bool MemoryPoolForGluten::maybeReserveIncrementReservationThreadSafe(
    MemoryPool* requestor,
    uint64_t size) {
  if (parent_ != nullptr) {
    auto* parentPtr = static_cast<MemoryPoolForGluten*>(parent_.get());
    if (!parentPtr->maybeReserveIncrementReservationThreadSafe(
            requestor, size)) {
      return false;
    }
  }

  if (maybeIncrementReservation(size)) {
    return true;
  }

  BOLT_CHECK_NULL(parent_);

  ++numCapacityGrowths_;
  return growCapacity(requestor, size);
}

} // namespace bytedance::bolt::memory
