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

#include "bolt/common/memory/MemoryPool.h"
#include <folly/system/ThreadName.h>
#include <re2/re2.h>
#include <signal.h>
#include <stdexcept>
#include <string>

#include "bolt/common/Casts.h"
#include "bolt/common/base/Counters.h"
#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/base/StatsReporter.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryUtils.h"
#include "bolt/common/memory/sparksql/OomPrinter.h"
#include "bolt/common/process/StackTrace.h"
#include "bolt/common/testutil/TestValue.h"

#include <re2/re2.h>

DEFINE_bool(
    bolt_memory_pool_capacity_transfer_across_tasks,
    false,
    "Whether allow to memory capacity transfer between memory pools from different tasks, which might happen in use case like Spark-Gluten");

DECLARE_bool(bolt_suppress_memory_capacity_exceeding_error_message);

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::memory {
namespace {
// Check if memory operation is allowed and increment the named stats.
#define CHECK_AND_INC_MEM_OP_STATS(pool, stats)                       \
  do {                                                                \
    if (FOLLY_UNLIKELY(pool->kind_ != Kind::kLeaf)) {                 \
      BOLT_FAIL(                                                      \
          "Memory operation is only allowed on leaf memory pool: {}", \
          pool->toString());                                          \
    }                                                                 \
    ++pool->num##stats##_;                                            \
  } while (0)

// Check if memory operation is allowed and increment the named stats.
#define INC_MEM_OP_STATS(stats) ++num##stats##_;

// Check if a memory pool management operation is allowed.
#define CHECK_POOL_MANAGEMENT_OP(opName)                                             \
  do {                                                                               \
    if (FOLLY_UNLIKELY(kind_ != Kind::kAggregate)) {                                 \
      BOLT_FAIL(                                                                     \
          "Memory pool {} operation is only allowed on aggregation memory pool: {}", \
          #opName,                                                                   \
          toString());                                                               \
    }                                                                                \
  } while (0)

// Collect the memory usage from memory pool for memory capacity exceeded error
// message generation.
struct MemoryUsage {
  std::string name;
  uint64_t currentUsage;
  uint64_t reservedUsage;
  uint64_t peakUsage;

  bool operator>(const MemoryUsage& other) const {
    return std::tie(reservedUsage, currentUsage, peakUsage, name) >
        std::tie(
               other.reservedUsage,
               other.currentUsage,
               other.peakUsage,
               other.name);
  }

  std::string toString() const {
    return fmt::format(
        "{} usage {} reserved {} peak {}",
        name,
        succinctBytes(currentUsage),
        succinctBytes(reservedUsage),
        succinctBytes(peakUsage));
  }
};

struct MemoryUsageComp {
  bool operator()(const MemoryUsage& lhs, const MemoryUsage& rhs) const {
    return lhs > rhs;
  }
};
using MemoryUsageHeap =
    std::priority_queue<MemoryUsage, std::vector<MemoryUsage>, MemoryUsageComp>;

static constexpr size_t kCapMessageIndentSize = 4;

std::vector<MemoryUsage> sortMemoryUsages(MemoryUsageHeap& heap) {
  std::vector<MemoryUsage> usages;
  usages.reserve(heap.size());
  while (!heap.empty()) {
    usages.push_back(heap.top());
    heap.pop();
  }
  std::reverse(usages.begin(), usages.end());
  return usages;
}

// Invoked by visitChildren() to traverse the memory pool structure to build the
// memory capacity exceeded exception error message.
void treeMemoryUsageVisitor(
    MemoryPool* pool,
    size_t indent,
    MemoryUsageHeap& topLeafMemUsages,
    bool skipEmptyPool,
    std::stringstream& out) {
  const MemoryPool::Stats stats = pool->stats();
  // Avoid logging empty pools if 'skipEmptyPool' is true.
  if (stats.empty() && skipEmptyPool) {
    return;
  }
  const MemoryUsage usage{
      .name = pool->name(),
      .currentUsage = stats.usedBytes,
      .reservedUsage = stats.reservedBytes,
      .peakUsage = stats.peakBytes,
  };
  out << std::string(indent, ' ') << usage.toString() << "\n";

  if (pool->kind() == MemoryPool::Kind::kLeaf) {
    if (stats.empty()) {
      return;
    }
    static const size_t kTopNLeafMessages = 10;
    topLeafMemUsages.push(usage);
    if (topLeafMemUsages.size() > kTopNLeafMessages) {
      topLeafMemUsages.pop();
    }
    return;
  }
  pool->visitChildren([&, indent = indent + kCapMessageIndentSize](
                          MemoryPool* pool) {
    treeMemoryUsageVisitor(pool, indent, topLeafMemUsages, skipEmptyPool, out);
    return true;
  });
}

std::string capacityToString(int64_t capacity) {
  return capacity == kMaxMemory ? "UNLIMITED" : succinctBytes(capacity);
}

#define DEBUG_RECORD_ALLOC(pool, ...)         \
  if (FOLLY_UNLIKELY(pool->debugEnabled())) { \
    pool->recordAllocDbg(__VA_ARGS__);        \
  }
#define DEBUG_RECORD_FREE(...)          \
  if (FOLLY_UNLIKELY(debugEnabled())) { \
    recordFreeDbg(__VA_ARGS__);         \
  }
#define DEBUG_LEAK_CHECK()              \
  if (FOLLY_UNLIKELY(debugEnabled())) { \
    leakCheckDbg();                     \
  }
#define RECORD_ALLOC(...)                 \
  {                                       \
    if (FOLLY_UNLIKELY(debugEnabled())) { \
      recordAllocDbg(__VA_ARGS__);        \
    }                                     \
    listener_->recordAlloc(__VA_ARGS__);  \
  }
#define RECORD_ALLOC_SPEC(pool, ...)               \
  {                                                \
    if (FOLLY_UNLIKELY(pool->debugEnabled())) {    \
      pool->recordAllocDbg(__VA_ARGS__);           \
    }                                              \
    pool->getListener()->recordAlloc(__VA_ARGS__); \
  }
#define RECORD_FREE(...)                  \
  {                                       \
    if (FOLLY_UNLIKELY(debugEnabled())) { \
      recordFreeDbg(__VA_ARGS__);         \
    }                                     \
    listener_->recordFree(__VA_ARGS__);   \
  }
#define RECORD_GROW(...)                  \
  {                                       \
    if (FOLLY_UNLIKELY(debugEnabled())) { \
      recordGrowDbg(__VA_ARGS__);         \
    }                                     \
    listener_->recordGrow(__VA_ARGS__);   \
  }
#define LEAK_CHECK()                      \
  {                                       \
    if (FOLLY_UNLIKELY(debugEnabled())) { \
      leakCheckDbg();                     \
    }                                     \
    listener_->memoryLeakCheck();         \
  }
#define DETECT_HUGE_RESERVE(...) \
  { listener_->detectHugeReserve(__VA_ARGS__); }
} // namespace

std::string MemoryPool::Stats::toString() const {
  return fmt::format(
      "usedBytes:{} reservedBytes:{} peakBytes:{} cumulativeBytes:{} numAllocs:{} numFrees:{} numReserves:{} numReleases:{} numShrinks:{} numReclaims:{} numCollisions:{} numCapacityGrowths:{}",
      succinctBytes(usedBytes),
      succinctBytes(reservedBytes),
      succinctBytes(peakBytes),
      succinctBytes(cumulativeBytes),
      numAllocs,
      numFrees,
      numReserves,
      numReleases,
      numShrinks,
      numReclaims,
      numCollisions,
      numCapacityGrowths);
}

bool MemoryPool::Stats::operator==(const MemoryPool::Stats& other) const {
  return std::tie(
             usedBytes,
             reservedBytes,
             peakBytes,
             cumulativeBytes,
             numAllocs,
             numFrees,
             numReserves,
             numReleases,
             numCollisions,
             numCapacityGrowths) ==
      std::tie(
             other.usedBytes,
             other.reservedBytes,
             other.peakBytes,
             other.cumulativeBytes,
             other.numAllocs,
             other.numFrees,
             other.numReserves,
             other.numReleases,
             other.numCollisions,
             other.numCapacityGrowths);
}

std::ostream& operator<<(std::ostream& os, const MemoryPool::Stats& stats) {
  return os << stats.toString();
}

MemoryPool::MemoryPool(
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    const Options& options)
    : name_(name),
      kind_(kind),
      alignment_(options.alignment),
      parent_(std::move(parent)),
      maxCapacity_(parent_ == nullptr ? options.maxCapacity : kMaxMemory),
      trackUsage_(options.trackUsage),
      threadSafe_(options.threadSafe),
      debugOptions_(options.debugOptions),
      coreOnAllocationFailureEnabled_(options.coreOnAllocationFailureEnabled),
      getPreferredSize_(
          options.getPreferredSize == nullptr
              ? [](size_t size) { return MemoryPool::getPreferredSize(size); }
              : options.getPreferredSize) {
  BOLT_CHECK(!isRoot() || !isLeaf());
  BOLT_CHECK_GT(
      maxCapacity_, 0, "Memory pool {} max capacity can't be zero", name_);
  BOLT_CHECK_NOT_NULL(getPreferredSize_);
  MemoryAllocator::alignmentCheck(0, alignment_);
}

MemoryPool::~MemoryPool() {
  BOLT_CHECK(children_.empty());
}

// static
std::string MemoryPool::kindString(Kind kind) {
  switch (kind) {
    case Kind::kLeaf:
      return "LEAF";
    case Kind::kAggregate:
      return "AGGREGATE";
    default:
      return fmt::format("UNKNOWN_{}", static_cast<int>(kind));
  }
}

std::ostream& operator<<(std::ostream& out, MemoryPool::Kind kind) {
  return out << MemoryPool::kindString(kind);
}

const std::string& MemoryPool::name() const {
  return name_;
}

MemoryPool::Kind MemoryPool::kind() const {
  return kind_;
}

MemoryPool* MemoryPool::parent() const {
  return parent_.get();
}

MemoryPool* MemoryPool::root() const {
  const MemoryPool* pool = this;
  while (pool->parent_ != nullptr) {
    pool = pool->parent_.get();
  }
  return const_cast<MemoryPool*>(pool);
}

uint64_t MemoryPool::getChildCount() const {
  std::shared_lock guard{poolMutex_};
  return children_.size();
}

void MemoryPool::visitChildren(
    const std::function<bool(MemoryPool*)>& visitor) const {
  std::vector<std::shared_ptr<MemoryPool>> children;
  {
    std::shared_lock guard{poolMutex_};
    children.reserve(children_.size());
    for (auto& entry : children_) {
      auto child = entry.second.lock();
      if (child != nullptr) {
        children.push_back(std::move(child));
      }
    }
  }

  // NOTE: we should call 'visitor' on child pool object out of 'poolMutex_' to
  // avoid potential recursive locking issues. Firstly, the user provided
  // 'visitor' might try to acquire this memory pool lock again. Secondly, the
  // shared child pool reference created from the weak pointer might be the last
  // reference if some other threads drop all the external references during
  // this time window. Then drop of this last shared reference after 'visitor'
  // call will trigger child memory pool destruction in that case. The child
  // memory pool destructor will remove its weak pointer reference from the
  // parent pool which needs to acquire this memory pool lock again.
  for (auto& child : children) {
    if (!visitor(child.get())) {
      return;
    }
  }
}

std::shared_ptr<MemoryPool> MemoryPool::addLeafChild(
    const std::string& name,
    bool threadSafe,
    std::unique_ptr<MemoryReclaimer> _reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addLeafChild);
  // NOTE: we shall only set reclaimer in a child pool if its parent has also
  // set. Otherwise it should be miss-configured.
  BOLT_CHECK(
      reclaimer() != nullptr || _reclaimer == nullptr,
      "Child memory pool {} shall only set memory reclaimer if its parent {} has also set",
      name,
      name_);

  std::unique_lock guard{poolMutex_};
  BOLT_CHECK_EQ(
      children_.count(name),
      0,
      "Leaf child memory pool {} already exists in {}",
      name,
      name_);
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kLeaf,
      threadSafe,
      getPreferredSize_,
      std::move(_reclaimer));
  children_.emplace(name, child);
  return child;
}

std::shared_ptr<MemoryPool> MemoryPool::addAggregateChild(
    const std::string& name,
    std::unique_ptr<MemoryReclaimer> _reclaimer) {
  CHECK_POOL_MANAGEMENT_OP(addAggregateChild);
  // NOTE: we shall only set reclaimer in a child pool if its parent has also
  // set. Otherwise it should be miss-configured.
  BOLT_CHECK(
      reclaimer() != nullptr || _reclaimer == nullptr,
      "Child memory pool {} shall only set memory reclaimer if its parent {} has also set",
      name,
      name_);

  std::unique_lock guard{poolMutex_};
  BOLT_CHECK_EQ(
      children_.count(name),
      0,
      "Child memory pool {} already exists in {}",
      name,
      name_);
  auto child = genChild(
      shared_from_this(),
      name,
      MemoryPool::Kind::kAggregate,
      true,
      getPreferredSize_,
      std::move(_reclaimer));
  children_.emplace(name, child);
  return child;
}

void MemoryPool::dropChild(const MemoryPool* child) {
  CHECK_POOL_MANAGEMENT_OP(dropChild);
  std::unique_lock guard{poolMutex_};
  const auto ret = children_.erase(child->name());
  BOLT_CHECK_EQ(
      ret,
      1,
      "Child memory pool {} doesn't exist in {}",
      child->name(),
      name());
}

bool MemoryPool::aborted() const {
  if (parent_ != nullptr) {
    return parent_->aborted();
  }
  return aborted_;
}

std::exception_ptr MemoryPool::abortError() const {
  if (parent_ != nullptr) {
    return parent_->abortError();
  }
  return abortError_;
}

size_t MemoryPool::preferredSize(size_t size) {
  const auto preferredSize = getPreferredSize_(size);
  BOLT_CHECK_GE(preferredSize, size);
  return preferredSize;
}

// static.
size_t MemoryPool::getPreferredSize(size_t size) {
  if (size < 8) {
    return 8;
  }
  int32_t bits = 63 - bits::countLeadingZeros<uint64_t>(size);
  size_t lower = 1ULL << bits;
  // Size is a power of 2.
  if (lower == size) {
    return size;
  }
  // If size is below 1.5 * previous power of two, return 1.5 *
  // the previous power of two, else the next power of 2.
  if (lower + (lower / 2) >= size) {
    return lower + (lower / 2);
  }
  return lower * 2;
}

void MemoryPool::setPreferredSize(
    std::function<size_t(size_t)> getPreferredSizeFunc) {
  BOLT_CHECK_NOT_NULL(getPreferredSizeFunc);
  getPreferredSize_ = getPreferredSizeFunc;
}

MemoryPoolImpl::MemoryPoolImpl(
    MemoryManager* memoryManager,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    std::unique_ptr<MemoryReclaimer> reclaimer,
    const Options& options)
    : MemoryPool{name, kind, parent, options},
      manager_{memoryManager},
      allocator_{manager_->allocator()},
      arbitrator_{manager_->arbitrator()},
      reclaimer_(std::move(reclaimer)),
      // The memory manager sets the capacity through grow() according to the
      // actually used memory arbitration policy.
      capacity_(parent_ != nullptr ? kMaxMemory : 0) {
  BOLT_CHECK(options.threadSafe || isLeaf());
  listener_ = std::make_unique<MemoryAllocationListener>(
      name,
      this,
      options.poolRegex,
      options.singleAllocationThreshold,
      options.accumulativeAllocationThreshold);
}

MemoryPoolImpl::~MemoryPoolImpl() {
  LEAK_CHECK();
  if (parent_ != nullptr) {
    toImpl(parent_)->dropChild(this);
  }

  if (isLeaf()) {
    if (usedReservationBytes_ > 0) {
      BOLT_MEM_LOG(ERROR) << "Memory leak (Used memory): " << toString();
      RECORD_METRIC_VALUE(
          kMetricMemoryPoolUsageLeakBytes, usedReservationBytes_);
    }

    if (minReservationBytes_ > 0) {
      BOLT_MEM_LOG(ERROR) << "Memory leak (Reserved Memory): " << toString();
      RECORD_METRIC_VALUE(
          kMetricMemoryPoolReservationLeakBytes, minReservationBytes_);
    }
  }
  BOLT_DCHECK_EQ(
      usedReservationBytes_, 0, "Memory leak (Used memory): {}", toString());
  BOLT_DCHECK_EQ(
      reservationBytes_, 0, "Memory leak (Reserved Memory): {}", toString());
  BOLT_DCHECK_EQ(
      minReservationBytes_,
      0,
      "Memory leak (Min Reserved Memory): {}",
      toString());

  if (isRoot()) {
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricMemoryPoolCapacityGrowCount, numCapacityGrowths_);
  }

  if (destructionCb_ != nullptr) {
    destructionCb_(this);
  }
}

MemoryPool::Stats MemoryPoolImpl::stats() const {
  std::lock_guard<std::mutex> l(mutex_);
  return statsLocked();
}

MemoryPool::Stats MemoryPoolImpl::statsLocked() const {
  Stats stats;
  stats.usedBytes = usedBytes();
  stats.currentBytes = stats.usedBytes;
  stats.reservedBytes = reservationBytes_;
  stats.peakBytes = peakBytes_;
  stats.cumulativeBytes = cumulativeBytes_;
  stats.numAllocs = numAllocs_;
  stats.numFrees = numFrees_;
  stats.numReserves = numReserves_;
  stats.numReleases = numReleases_;
  stats.numReclaims = numReclaims_;
  stats.numShrinks = numShrinks_;
  stats.numCollisions = numCollisions_;
  stats.numCapacityGrowths = numCapacityGrowths_;
  return stats;
}

void* MemoryPoolImpl::allocate(
    int64_t size,
    std::optional<uint32_t> alignment) {
  uint8_t alignmentValue = alignment_;
  if (alignment.has_value()) {
    alignmentValue = alignment.value();
    if (FOLLY_UNLIKELY(
            !(bits::isPowerOfTwo(alignmentValue) &&
              alignmentValue <= alignment_))) {
      BOLT_UNSUPPORTED(
          "Memory pool only supports fixed alignment allocations. Requested "
          "alignment {} must already be aligned with this memory pool's fixed "
          "alignment {}.",
          alignmentValue,
          alignment_);
    }
  }

  CHECK_AND_INC_MEM_OP_STATS(this, Allocs);
  const auto alignedSize = sizeAlign(size, alignmentValue);
  reserve(alignedSize);
  void* buffer = allocator_->allocateBytes(alignedSize, alignmentValue);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    handleAllocationFailure(fmt::format(
        "{} failed with {} from {} {}",
        __FUNCTION__,
        succinctBytes(size),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  RECORD_ALLOC(buffer, size);
  return buffer;
}

void* MemoryPoolImpl::allocateZeroFilled(
    int64_t numEntries,
    int64_t sizeEach,
    std::optional<uint8_t> alignment) {
  CHECK_AND_INC_MEM_OP_STATS(this, Allocs);
  const auto size = sizeEach * numEntries;
  auto alignmentValue = alignment.has_value() ? alignment.value() : alignment_;
  const auto alignedSize = sizeAlign(size, alignmentValue);
  reserve(alignedSize);
  void* buffer = allocator_->allocateZeroFilled(alignedSize);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    handleAllocationFailure(fmt::format(
        "{} failed with {} entries and {} each from {} {}",
        __FUNCTION__,
        numEntries,
        succinctBytes(sizeEach),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  RECORD_ALLOC(buffer, size);
  return buffer;
}

void* MemoryPoolImpl::reallocate(
    void* p,
    int64_t size,
    int64_t newSize,
    std::optional<uint8_t> alignment) {
  CHECK_AND_INC_MEM_OP_STATS(this, Allocs);
  auto alignmentValue = alignment.has_value() ? alignment.value() : alignment_;
  const auto alignedNewSize = sizeAlign(newSize, alignmentValue);
  reserve(alignedNewSize);

  void* newP{nullptr};
  if ((p == nullptr) || !jemallocEnabled()) {
    newP = allocator_->allocateBytes(alignedNewSize, alignmentValue);
  } else {
    newP = allocator_->reallocateBytes(p, alignedNewSize, alignmentValue);
  }
  if (FOLLY_UNLIKELY(newP == nullptr)) {
    release(alignedNewSize);
    handleAllocationFailure(fmt::format(
        "{} failed with new {} and old {} from {} {}",
        __FUNCTION__,
        succinctBytes(newSize),
        succinctBytes(size),
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  if (newP != p) {
    if (p) {
      RECORD_FREE(p, size);
    }
    RECORD_ALLOC(newP, newSize);
  } else {
    RECORD_GROW(p, newP, size, newSize);
  }
  if (p == nullptr) {
    return newP;
  }
  if (jemallocEnabled()) {
    CHECK_AND_INC_MEM_OP_STATS(this, Frees);
    const auto alignedSize = sizeAlign(size, alignmentValue);
    allocator_->decrementUsageBytes(alignedSize);
    release(alignedSize);
  } else {
    ::memcpy(newP, p, std::min(size, newSize));
    free(p, size, alignment_, false);
  }
  return newP;
}

void MemoryPoolImpl::free(
    void* p,
    int64_t size,
    std::optional<uint8_t> alignment,
    bool needRecordFree) {
  CHECK_AND_INC_MEM_OP_STATS(this, Frees);
  auto alignmentValue = alignment.has_value() ? alignment.value() : alignment_;
  const auto alignedSize = sizeAlign(size, alignmentValue);
  if (needRecordFree) {
    RECORD_FREE(p, size);
  }
  allocator_->freeBytes(p, alignedSize);
  release(alignedSize);
}

bool MemoryPoolImpl::transferTo(MemoryPool* dest, void* buffer, uint64_t size) {
  if (!isLeaf() || !dest->isLeaf()) {
    return false;
  }
  BOLT_CHECK_NOT_NULL(dest);
  auto* destImpl = checked_pointer_cast<MemoryPoolImpl, MemoryPool>(dest);
  if (allocator_ != destImpl->allocator_) {
    return false;
  }

  CHECK_AND_INC_MEM_OP_STATS(destImpl, Allocs);
  const auto alignedSize = sizeAlign(size);
  destImpl->reserve(alignedSize);
  RECORD_ALLOC_SPEC(destImpl, buffer, size);

  CHECK_AND_INC_MEM_OP_STATS(this, Frees);
  RECORD_FREE(buffer, size);
  release(alignedSize);

  return true;
}

void MemoryPoolImpl::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    MachinePageCount minSizeClass) {
  CHECK_AND_INC_MEM_OP_STATS(this, Allocs);
  if (!out.empty()) {
    INC_MEM_OP_STATS(Frees);
  }
  BOLT_CHECK_GT(numPages, 0);
  BOLT_TEST_ADJUST(
      "bytedance::bolt::common::memory::MemoryPoolImpl::allocateNonContiguous",
      this);
  RECORD_FREE(out);
  if (!allocator_->allocateNonContiguous(
          numPages,
          out,
          [this](uint64_t allocBytes, bool preAllocate) {
            if (preAllocate) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          minSizeClass)) {
    BOLT_CHECK(out.empty());
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        numPages,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  RECORD_ALLOC(out);
  BOLT_CHECK(!out.empty());
  BOLT_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeNonContiguous(Allocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(this, Frees);
  RECORD_FREE(allocation);
  const int64_t freedBytes = allocator_->freeNonContiguous(allocation);
  BOLT_CHECK(allocation.empty());
  release(freedBytes);
}

MachinePageCount MemoryPoolImpl::largestSizeClass() const {
  return allocator_->largestSizeClass();
}

const std::vector<MachinePageCount>& MemoryPoolImpl::sizeClasses() const {
  return allocator_->sizeClasses();
}

void MemoryPoolImpl::allocateContiguous(
    MachinePageCount numPages,
    ContiguousAllocation& out,
    MachinePageCount maxPages) {
  CHECK_AND_INC_MEM_OP_STATS(this, Allocs);
  if (!out.empty()) {
    INC_MEM_OP_STATS(Frees);
  }
  BOLT_CHECK_GT(numPages, 0);
  RECORD_FREE(out);
  if (!allocator_->allocateContiguous(
          numPages,
          nullptr,
          out,
          [this](uint64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          maxPages)) {
    BOLT_CHECK(out.empty());
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        numPages,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  RECORD_ALLOC(out);
  BOLT_CHECK(!out.empty());
  BOLT_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeContiguous(ContiguousAllocation& allocation) {
  CHECK_AND_INC_MEM_OP_STATS(this, Frees);
  const int64_t bytesToFree = allocation.size();
  RECORD_FREE(allocation);
  allocator_->freeContiguous(allocation);
  BOLT_CHECK(allocation.empty());
  release(bytesToFree);
}

void MemoryPoolImpl::growContiguous(
    MachinePageCount increment,
    ContiguousAllocation& allocation) {
  const void* oldAddr = allocation.data();
  const uint64_t oldSize = allocation.size();
  if (!allocator_->growContiguous(
          increment, allocation, [this](uint64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          })) {
    handleAllocationFailure(fmt::format(
        "{} failed with {} pages from {} {}",
        __FUNCTION__,
        increment,
        toString(),
        allocator_->getAndClearFailureMessage()));
  }
  RECORD_GROW(oldAddr, allocation.data(), oldSize, allocation.size());
}

int64_t MemoryPoolImpl::capacity() const {
  if (parent_ != nullptr) {
    return parent_->capacity();
  }
  std::lock_guard<std::mutex> l(mutex_);
  return capacity_;
}

int64_t MemoryPoolImpl::usedBytes() const {
  if (isLeaf()) {
    return usedReservationBytes_;
  }
  if (reservedBytes() == 0) {
    return 0;
  }
  int64_t usedBytes{0};
  visitChildren([&](MemoryPool* pool) {
    usedBytes += pool->usedBytes();
    return true;
  });
  return usedBytes;
}

int64_t MemoryPoolImpl::currentBytes() const {
  return usedBytes();
}

int64_t MemoryPoolImpl::releasableReservation() const {
  if (isLeaf()) {
    std::lock_guard<std::mutex> l(mutex_);
    return std::max<int64_t>(
        0, reservationBytes_ - quantizedSize(usedReservationBytes_));
  }
  if (reservedBytes() == 0) {
    return 0;
  }
  int64_t releasableBytes{0};
  visitChildren([&](MemoryPool* pool) {
    releasableBytes += pool->releasableReservation();
    return true;
  });
  return releasableBytes;
}

std::shared_ptr<MemoryPool> MemoryPoolImpl::genChild(
    std::shared_ptr<MemoryPool> parent,
    const std::string& name,
    Kind kind,
    bool threadSafe,
    const std::function<size_t(size_t)>& getPreferredSize,
    std::unique_ptr<MemoryReclaimer> reclaimer) {
  return std::make_shared<MemoryPoolImpl>(
      manager_,
      name,
      kind,
      parent,
      std::move(reclaimer),
      Options{
          .alignment = alignment_,
          .trackUsage = trackUsage_,
          .threadSafe = threadSafe,
          .coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled_,
          .getPreferredSize = getPreferredSize,
          .debugOptions = debugOptions_,
          .poolRegex = listener_->getPoolRegex(),
          .singleAllocationThreshold =
              listener_->getSingleAllocationThreshold(),
          .accumulativeAllocationThreshold =
              listener_->getAccumulativeAllocationThreshold()});
}

bool MemoryPoolImpl::maybeReserve(uint64_t increment) {
  CHECK_AND_INC_MEM_OP_STATS(this, Reserves);
  BOLT_TEST_ADJUST(
      "bytedance::bolt::common::memory::MemoryPoolImpl::maybeReserve", this);
  // TODO: make this a configurable memory pool option.
  constexpr int32_t kGrowthQuantum = 8 << 20;
  const auto reservationToAdd = bits::roundUp(increment, kGrowthQuantum);
  try {
    reserve(reservationToAdd, true);
  } catch (const std::exception& e) {
    LOG(INFO) << "maybeReserve failed, reason is:" << e.what();
    if (aborted()) {
      // NOTE: we shall throw to stop the query execution if the root memory
      // pool has been aborted. It is also unsafe to proceed as the memory
      // abort code path might have already freed up the memory resource of
      // this operator while it is under memory arbitration.
      std::rethrow_exception(std::current_exception());
    }
    return false;
  }
  return true;
}

void MemoryPoolImpl::reserve(uint64_t size, bool reserveOnly) {
  DETECT_HUGE_RESERVE(size);
  if (FOLLY_LIKELY(trackUsage_)) {
    if (FOLLY_LIKELY(threadSafe_)) {
      reserveThreadSafe(size, reserveOnly);
    } else {
      reserveNonThreadSafe(size, reserveOnly);
    }
  }
}

void MemoryPoolImpl::reserveNonThreadSafe(uint64_t size, bool reserveOnly) {
  BOLT_CHECK(isLeaf());

  int32_t numAttempts{0};
  for (;; ++numAttempts) {
    int64_t increment = reservationSizeLocked(size);
    if (FOLLY_LIKELY(increment == 0)) {
      if (FOLLY_UNLIKELY(reserveOnly)) {
        minReservationBytes_ = tsanAtomicValue(reservationBytes_);
      } else {
        usedReservationBytes_ += size;
        cumulativeBytes_ += size;
        maybeUpdatePeakBytesLocked(usedReservationBytes_);
      }
      sanityCheckLocked();
      break;
    }
    incrementReservationNonThreadSafe(this, increment);
  }

  // NOTE: in case of concurrent reserve requests to the same root memory pool
  // from the other leaf memory pools, we might have to retry
  // incrementReservation(). This should happen rarely in production
  // as the leaf tracker does quantized memory reservation so that we don't
  // expect high concurrency at the root memory pool.
  if (FOLLY_UNLIKELY(numAttempts > 1)) {
    numCollisions_ += numAttempts - 1;
  }
}

void MemoryPoolImpl::reserveThreadSafe(uint64_t size, bool reserveOnly) {
  BOLT_CHECK(isLeaf());

  int32_t numAttempts = 0;
  int64_t increment = 0;
  for (;; ++numAttempts) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      increment = reservationSizeLocked(size);
      if (increment == 0) {
        if (reserveOnly) {
          minReservationBytes_ = tsanAtomicValue(reservationBytes_);
        } else {
          usedReservationBytes_ += size;
          cumulativeBytes_ += size;
          maybeUpdatePeakBytesLocked(usedReservationBytes_);
        }
        sanityCheckLocked();
        break;
      }
    }
    BOLT_TEST_ADJUST(
        "bytedance::bolt::memory::MemoryPoolImpl::reserveThreadSafe", this);
    try {
      incrementReservationThreadSafe(this, increment);
    } catch (const std::exception&) {
      // When race with concurrent memory reservation free, we might end up
      // with unused reservation but no used reservation if a retry memory
      // reservation attempt run into memory capacity exceeded error.
      releaseThreadSafe(0, false);
      std::rethrow_exception(std::current_exception());
    }
  }

  // NOTE: in case of concurrent reserve and release requests, we might see
  // potential conflicts as the quantized memory release might free up extra
  // reservation bytes so reserve might go extra round to reserve more bytes.
  // This should happen rarely in production as the leaf tracker updates are
  // mostly single thread executed.
  if (numAttempts > 1) {
    numCollisions_ += numAttempts - 1;
  }
}

void MemoryPoolImpl::incrementReservationThreadSafe(
    MemoryPool* requestor,
    uint64_t size) {
  BOLT_CHECK(threadSafe_);
  BOLT_CHECK_GT(size, 0);

  // Propagate the increment to the root memory pool to check the capacity
  // limit first. If it exceeds the capacity and can't grow, the root memory
  // pool will throw an exception to fail the request.
  if (parent_ != nullptr) {
    toImpl(parent_)->incrementReservationThreadSafe(requestor, size);
  }

  if (maybeIncrementReservation(size)) {
    return;
  }

  BOLT_CHECK_NULL(parent_);

  if (growCapacity(requestor, size)) {
    BOLT_TEST_ADJUST(
        "bytedance::bolt::memory::MemoryPoolImpl::incrementReservationThreadSafe::AfterGrowCallback",
        this);
    return;
  }

  // NOTE: if memory arbitration succeeds, it should have already committed
  // the reservation 'size' in the root memory pool.
  std::string threadName = "";
  if (folly::getCurrentThreadName().has_value()) {
    threadName = folly::getCurrentThreadName().value();
    if (threadName.find(kAsyncPreloadThreadName) == 0) {
      auto msg = fmt::format(
          "{} request memory failed, memory pool name is {}",
          threadName,
          name());
      LOG(WARNING) << msg;
      throw std::runtime_error(msg);
    }
  }

  BOLT_MEM_POOL_CAP_EXCEEDED(fmt::format(
      "Exceeded memory pool cap of {} with max {} when requesting {}, memory "
      "manager cap is {}, requestor '{}' with current usage {}, process RSS is {}\n{}",
      capacityToString(capacity()),
      capacityToString(maxCapacity_),
      succinctBytes(size),
      capacityToString(manager_->capacity()),
      requestor->name(),
      succinctBytes(requestor->currentBytes()),
      succinctBytes(MemoryUtils::getProcessRss()),
      sparksql::OomPrinter::linked() ? sparksql::OomPrinter::OomMessage(this)
                                     : treeMemoryUsage()));
}

bool MemoryPoolImpl::growCapacity(MemoryPool* requestor, uint64_t size) {
  BOLT_CHECK(requestor->isLeaf());
  ++numCapacityGrowths_;

  {
    MemoryPoolArbitrationSection arbitrationSection(requestor);
    if (arbitrator_->growCapacity(this, size)) {
      return true;
    }
  }
  // The memory pool might have been aborted during the time it leaves the
  // arbitration no matter the arbitration succeed or not.
  if (FOLLY_UNLIKELY(aborted())) {
    // Release the reservation committed by the memory arbitration on success.
    decrementReservation(size);
    BOLT_CHECK_NOT_NULL(abortError());
    std::rethrow_exception(abortError());
  }
  return false;
}

// Returns the needed reservation size. If there is sufficient unused memory
// reservation, this function returns zero.
int64_t MemoryPoolImpl::reservationSizeLocked(int64_t size) {
  const int64_t neededSize = size - (reservationBytes_ - usedReservationBytes_);
  if (neededSize <= 0) {
    return 0;
  }
  return roundedDelta(reservationBytes_, neededSize);
}

void MemoryPoolImpl::maybeUpdatePeakBytesLocked(int64_t newPeak) {
  static constexpr int64_t STEP = 128 * 1024 * 1024;
  peakBytes_ = std::max<int64_t>(peakBytes_, newPeak);
  if (FOLLY_UNLIKELY(
          peakBytes_ == newPeak && peakBytes_ / STEP < newPeak / STEP &&
          peakBytes_ != 0)) {
    LOG(INFO) << "pool: " << name() << " peak memory bytes reach " << newPeak;
  }
}

void MemoryPoolImpl::incrementReservationNonThreadSafe(
    MemoryPool* requestor,
    uint64_t size) {
  BOLT_CHECK_NOT_NULL(parent_);
  BOLT_CHECK(isLeaf());
  toImpl(parent_)->incrementReservationThreadSafe(requestor, size);
  reservationBytes_ += size;
}

bool MemoryPoolImpl::maybeIncrementReservation(uint64_t size) {
  std::lock_guard<std::mutex> l(mutex_);
  if (isRoot()) {
    checkIfAborted();

    // NOTE: we allow memory pool to overuse its memory during the memory
    // arbitration process. The memory arbitration process itself needs to
    // ensure the memory pool usage of the memory pool is within the capacity
    // limit after the arbitration operation completes.
    if (FOLLY_UNLIKELY(
            (reservationBytes_ + size > capacity_) &&
            !underMemoryArbitration())) {
      return false;
    }
  }
  incrementReservationLocked(size);
  return true;
}

void MemoryPoolImpl::incrementReservationLocked(uint64_t bytes) {
  reservationBytes_ += bytes;
  if (!isLeaf()) {
    cumulativeBytes_ += bytes;
    maybeUpdatePeakBytesLocked(reservationBytes_);
  }
}

void MemoryPoolImpl::release() {
  CHECK_AND_INC_MEM_OP_STATS(this, Releases);
  release(0, true);
}

void MemoryPoolImpl::release(uint64_t size, bool releaseOnly) {
  if (FOLLY_LIKELY(trackUsage_)) {
    if (FOLLY_LIKELY(threadSafe_)) {
      releaseThreadSafe(size, releaseOnly);
    } else {
      releaseNonThreadSafe(size, releaseOnly);
    }
  }
}

void MemoryPoolImpl::releaseThreadSafe(uint64_t size, bool releaseOnly) {
  BOLT_CHECK(isLeaf());
  BOLT_DCHECK_NOT_NULL(parent_);

  int64_t freeable = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    int64_t newQuantized;
    if (FOLLY_UNLIKELY(releaseOnly)) {
      BOLT_DCHECK_EQ(size, 0);
      if (minReservationBytes_ == 0) {
        return;
      }
      newQuantized = quantizedSize(usedReservationBytes_);
      minReservationBytes_ = 0;
    } else {
      usedReservationBytes_ -= size;
      const int64_t newCap =
          std::max(minReservationBytes_, usedReservationBytes_);
      newQuantized = quantizedSize(newCap);
    }
    freeable = reservationBytes_ - newQuantized;
    if (freeable > 0) {
      reservationBytes_ = newQuantized;
    }
    sanityCheckLocked();
  }
  if (freeable > 0) {
    toImpl(parent_)->decrementReservation(freeable);
  }
}

void MemoryPoolImpl::releaseNonThreadSafe(uint64_t size, bool releaseOnly) {
  BOLT_CHECK(isLeaf());
  BOLT_DCHECK_NOT_NULL(parent_);

  int64_t newQuantized;
  if (FOLLY_UNLIKELY(releaseOnly)) {
    BOLT_DCHECK_EQ(size, 0);
    if (minReservationBytes_ == 0) {
      return;
    }
    newQuantized = quantizedSize(usedReservationBytes_);
    minReservationBytes_ = 0;
  } else {
    usedReservationBytes_ -= size;
    const int64_t newCap =
        std::max(minReservationBytes_, usedReservationBytes_);
    newQuantized = quantizedSize(newCap);
  }

  const int64_t freeable = reservationBytes_ - newQuantized;
  if (FOLLY_UNLIKELY(freeable > 0)) {
    reservationBytes_ = newQuantized;
    sanityCheckLocked();
    toImpl(parent_)->decrementReservation(freeable);
  }
}

void MemoryPoolImpl::decrementReservation(uint64_t size) noexcept {
  BOLT_CHECK_GT(size, 0);

  if (parent_ != nullptr) {
    toImpl(parent_)->decrementReservation(size);
  }
  std::lock_guard<std::mutex> l(mutex_);
  reservationBytes_ -= size;
  sanityCheckLocked();
}

std::string MemoryPoolImpl::toString(bool detail) const {
  std::string result;
  {
    std::lock_guard<std::mutex> l(mutex_);
    result = toStringLocked();
  }
  if (detail) {
    result += "\n";
    result += treeMemoryUsage();
  }
  if (FOLLY_UNLIKELY(debugEnabled())) {
    result += "\n";
    result += dumpRecordsDbg();
  }
  return result;
}

std::string MemoryPoolImpl::treeMemoryUsage(
    bool skipEmptyPool,
    bool printTopUsage) const {
  if (parent_ != nullptr) {
    return parent_->treeMemoryUsage(skipEmptyPool);
  }
  if (FLAGS_bolt_suppress_memory_capacity_exceeding_error_message) {
    return "";
  }
  std::stringstream out;
  {
    std::lock_guard<std::mutex> l(mutex_);
    const Stats stats = statsLocked();
    const MemoryUsage usage{
        .name = name(),
        .currentUsage = stats.currentBytes,
        .reservedUsage = stats.reservedBytes,
        .peakUsage = stats.peakBytes};
    out << usage.toString() << "\n";
  }

  MemoryUsageHeap topLeafMemUsages;
  visitChildren([&, indent = kCapMessageIndentSize](MemoryPool* pool) {
    treeMemoryUsageVisitor(pool, indent, topLeafMemUsages, skipEmptyPool, out);
    return true;
  });

  if (printTopUsage && !topLeafMemUsages.empty()) {
    out << "\nTop " << topLeafMemUsages.size() << " leaf memory pool usages:\n";
    std::vector<MemoryUsage> usages = sortMemoryUsages(topLeafMemUsages);
    for (const auto& usage : usages) {
      out << std::string(kCapMessageIndentSize, ' ') << usage.toString()
          << "\n";
    }
  }
  return out.str();
}

uint64_t MemoryPoolImpl::freeBytes() const {
  if (parent_ != nullptr) {
    return parent_->freeBytes();
  }
  std::lock_guard<std::mutex> l(mutex_);
  if (capacity_ == kMaxMemory) {
    return 0;
  }
  if (capacity_ < reservationBytes_) {
    // NOTE: the memory reservation could be temporarily larger than its
    // capacity if this memory pool is under memory arbitration processing.
    return 0;
  }
  return capacity_ - reservationBytes_;
}

void MemoryPoolImpl::setReclaimer(std::unique_ptr<MemoryReclaimer> reclaimer) {
  BOLT_CHECK_NOT_NULL(reclaimer);
  if (parent_ != nullptr) {
    BOLT_CHECK_NOT_NULL(
        parent_->reclaimer(),
        "Child memory pool {} shall only set reclaimer if its parent {} has also set",
        name_,
        parent_->name());
  }
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_CHECK_NULL(reclaimer_);
  reclaimer_ = std::move(reclaimer);
}

MemoryReclaimer* MemoryPoolImpl::reclaimer() const {
  tsan_lock_guard<std::mutex> l(mutex_);
  return reclaimer_.get();
}

std::optional<uint64_t> MemoryPoolImpl::reclaimableBytes() const {
  if (reclaimer() == nullptr) {
    return std::nullopt;
  }

  uint64_t reclaimableBytes = 0;
  if (!reclaimer()->reclaimableBytes(*this, reclaimableBytes)) {
    return std::nullopt;
  }

  return reclaimableBytes;
}

uint64_t MemoryPoolImpl::reclaim(
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  if (reclaimer() == nullptr) {
    return 0;
  }
  ++numReclaims_;
  return reclaimer()->reclaim(this, targetBytes, maxWaitMs, stats);
}

void MemoryPoolImpl::enterArbitration() {
  if (reclaimer() != nullptr) {
    reclaimer()->enterArbitration();
  }
}

void MemoryPoolImpl::leaveArbitration() noexcept {
  if (reclaimer() != nullptr) {
    reclaimer()->leaveArbitration();
  }
}

uint64_t MemoryPoolImpl::shrink(uint64_t targetBytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->shrink(targetBytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  // We don't expect to shrink a memory pool without capacity limit.
  BOLT_CHECK_NE(capacity_, kMaxMemory);
  uint64_t freeBytes = std::max<uint64_t>(0, capacity_ - reservationBytes_);
  if (targetBytes != 0) {
    freeBytes = std::min(targetBytes, freeBytes);
  }
  capacity_ -= freeBytes;
  ++numShrinks_;
  return freeBytes;
}

bool MemoryPoolImpl::grow(uint64_t growBytes, uint64_t reservationBytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->grow(growBytes, reservationBytes);
  }
  // TODO: add to prevent from growing beyond the max capacity and the
  // corresponding support in memory arbitrator.
  std::lock_guard<std::mutex> l(mutex_);
  // We don't expect to grow a memory pool without capacity limit.
  BOLT_CHECK_NE(capacity_, kMaxMemory, "Can't grow with unlimited capacity");
  if (capacity_ + growBytes > maxCapacity_) {
    return false;
  }
  if (reservationBytes_ + reservationBytes > capacity_ + growBytes) {
    return false;
  }

  capacity_ += growBytes;
  BOLT_CHECK_GE(capacity_, growBytes);
  if (reservationBytes > 0) {
    incrementReservationLocked(reservationBytes);
    BOLT_CHECK_LE(reservationBytes, reservationBytes_);
  }
  return true;
}

void MemoryPoolImpl::abort(const std::exception_ptr& error) {
  BOLT_CHECK_NOT_NULL(error);
  if (parent_ != nullptr) {
    parent_->abort(error);
    return;
  }
  setAbortError(error);
  if (reclaimer() == nullptr) {
    return;
  }
  reclaimer()->abort(this, error);
}

void MemoryPoolImpl::setAbortError(const std::exception_ptr& error) {
  BOLT_CHECK(
      !aborted_,
      "Trying to set another abort error on an already aborted pool.");
  abortError_ = error;
  aborted_ = true;
}

void MemoryPoolImpl::checkIfAborted() const {
  if (FOLLY_UNLIKELY(aborted())) {
    BOLT_CHECK_NOT_NULL(abortError());
    std::rethrow_exception(abortError());
  }
}

void MemoryPoolImpl::setDestructionCallback(
    const DestructionCallback& callback) {
  BOLT_CHECK_NOT_NULL(callback);
  BOLT_CHECK(
      isRoot(),
      "Only root memory pool allows to set destruction callbacks: {}",
      name_);
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_CHECK_NULL(destructionCb_);
  destructionCb_ = callback;
}

void MemoryPoolImpl::testingSetCapacity(int64_t bytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->testingSetCapacity(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  capacity_ = bytes;
}

void MemoryPoolImpl::testingSetReservation(int64_t bytes) {
  if (parent_ != nullptr) {
    return toImpl(parent_)->testingSetReservation(bytes);
  }
  std::lock_guard<std::mutex> l(mutex_);
  reservationBytes_ = bytes;
}

bool MemoryPoolImpl::needRecordDbg(bool /* isAlloc */) {
  BOLT_CHECK(debugEnabled());
  if (debugOptions_->debugPoolNameRegex.empty()) {
    return false;
  }
  return RE2::FullMatch(name_, debugOptions_->debugPoolNameRegex);
  // TODO(jtan6): Add sample based condition support.
}

void MemoryPoolImpl::recordAllocDbg(const void* addr, uint64_t size) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(true)) {
    return;
  }
  AllocationRecord allocationRecord{size, process::StackTrace()};
  std::lock_guard<std::mutex> debugAllocLock(debugAllocMutex_);
  auto [it, inserted] = debugAllocRecords_.try_emplace(
      reinterpret_cast<uint64_t>(addr), std::move(allocationRecord));
  BOLT_CHECK(inserted);
  if (debugOptions_->debugPoolWarnThresholdBytes == 0 ||
      debugWarnThresholdExceeded_) {
    return;
  }
  const auto usedBytes = [this]() -> int64_t {
    std::lock_guard<std::mutex> l(mutex_);
    return reservedBytes();
  }();
  if (usedBytes >= debugOptions_->debugPoolWarnThresholdBytes) {
    debugWarnThresholdExceeded_ = true;
    BOLT_MEM_LOG(WARNING) << fmt::format(
        "[MemoryPool] Memory pool '{}' exceeded warning threshold of {} with allocation of {}, resulting in total size of {}.\n"
        "======== Allocation Stack ========\n"
        "{}\n"
        "======= Current Allocations ======\n"
        "{}",
        name_,
        succinctBytes(debugOptions_->debugPoolWarnThresholdBytes),
        succinctBytes(size),
        succinctBytes(usedBytes),
        it->second.callStack.toString(),
        dumpRecordsDbgLocked());
  }
}

void MemoryPoolImpl::recordAllocDbg(const Allocation& allocation) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(true) || allocation.empty()) {
    return;
  }
  recordAllocDbg(allocation.runAt(0).data(), allocation.byteSize());
}

void MemoryPoolImpl::recordAllocDbg(const ContiguousAllocation& allocation) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(true) || allocation.empty()) {
    return;
  }
  recordAllocDbg(allocation.data(), allocation.size());
}

void MemoryPoolImpl::recordFreeDbg(const void* addr, uint64_t size) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(false) || addr == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> l(debugAllocMutex_);
  uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
  auto allocResult = debugAllocRecords_.find(addrUint64);
  if (allocResult == debugAllocRecords_.end()) {
    BOLT_FAIL("Freeing of un-allocated memory. Free address {}.", addrUint64);
  }
  const auto allocRecord = allocResult->second;
  if (allocRecord.size != size) {
    const auto freeStackTrace = process::StackTrace().toString();
    BOLT_FAIL(fmt::format(
        "[MemoryPool] Trying to free {} bytes on an allocation of {} bytes.\n"
        "======== Allocation Stack ========\n"
        "{}\n"
        "============ Free Stack ==========\n"
        "{}\n",
        size,
        allocRecord.size,
        allocRecord.callStack.toString(),
        freeStackTrace));
  }
  debugAllocRecords_.erase(addrUint64);
}

void MemoryPoolImpl::recordFreeDbg(const Allocation& allocation) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(false) || allocation.empty()) {
    return;
  }
  recordFreeDbg(allocation.runAt(0).data(), allocation.byteSize());
}

void MemoryPoolImpl::recordFreeDbg(const ContiguousAllocation& allocation) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(false) || allocation.empty()) {
    return;
  }
  recordFreeDbg(allocation.data(), allocation.size());
}

void MemoryPoolImpl::recordGrowDbg(
    const void* oldAddr,
    const void* addr,
    uint64_t oldSize,
    uint64_t size) {
  BOLT_CHECK(debugEnabled());
  if (!needRecordDbg(false) || addr == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> l(debugAllocMutex_);
  uint64_t addrUint64 = reinterpret_cast<uint64_t>(addr);
  auto allocResult = debugAllocRecords_.find(addrUint64);
  if (allocResult == debugAllocRecords_.end()) {
    BOLT_FAIL("Growing of un-allocated memory. Free address {}.", addrUint64);
  }
  allocResult->second.size = size;
}

void MemoryPoolImpl::leakCheckDbg() {
  BOLT_CHECK(debugEnabled());
  if (debugAllocRecords_.empty()) {
    return;
  }
  BOLT_FAIL(fmt::format(
      "[MemoryPool] Leak check failed for '{}' pool - {}",
      name_,
      dumpRecordsDbg()));
}

std::string MemoryPoolImpl::dumpRecordsDbgLocked() const {
  BOLT_CHECK(debugEnabled());
  std::stringstream oss;
  oss << fmt::format("Found {} allocations:\n", debugAllocRecords_.size());
  struct AllocationStats {
    uint64_t size{0};
    uint64_t numAllocations{0};
  };
  std::unordered_map<std::string, AllocationStats> sizeAggregatedRecords;
  for (const auto& itr : debugAllocRecords_) {
    const auto& allocationRecord = itr.second;
    const auto stackStr = allocationRecord.callStack.toString();
    if (sizeAggregatedRecords.count(stackStr) == 0) {
      sizeAggregatedRecords[stackStr] = AllocationStats();
    }
    sizeAggregatedRecords[stackStr].size += allocationRecord.size;
    ++sizeAggregatedRecords[stackStr].numAllocations;
  }
  std::vector<std::pair<std::string, AllocationStats>> sortedRecords(
      sizeAggregatedRecords.begin(), sizeAggregatedRecords.end());
  std::sort(
      sortedRecords.begin(),
      sortedRecords.end(),
      [](const std::pair<std::string, AllocationStats>& a,
         std::pair<std::string, AllocationStats>& b) {
        return a.second.size > b.second.size;
      });
  for (const auto& pair : sortedRecords) {
    oss << fmt::format(
        "======== {} allocations of {} total size ========\n{}\n",
        pair.second.numAllocations,
        succinctBytes(pair.second.size),
        pair.first);
  }
  return oss.str();
}

void MemoryPoolImpl::handleAllocationFailure(
    const std::string& failureMessage) {
  if (coreOnAllocationFailureEnabled_) {
    BOLT_MEM_LOG(ERROR) << failureMessage;
    // SIGBUS is one of the standard signals in Linux that triggers a core
    // dump Normally it is raised by the operating system when a misaligned
    // memory access occurs. On x86 and aarch64 misaligned access is allowed
    // by default hence this signal should never occur naturally. Raising a
    // signal other than SIGABRT makes it easier to distinguish an allocation
    // failure from any other crash
    raise(SIGBUS);
  }

  BOLT_MEM_ALLOC_ERROR(failureMessage);
}
} // namespace bytedance::bolt::memory
