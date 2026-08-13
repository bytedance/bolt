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

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>

#include <common/memory/Allocation.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <sys/stat.h>
#include "bolt/common/memory/Memory.h"

#define PRESSURE
// #define ALIGNMENT

#ifdef PRESSURE
DEFINE_int64(
    bolt_benchmark_memory_allocation_count,
    1000,
    "The number of allocations");
DEFINE_int64(
    bolt_benchmark_memory_allocation_bytes,
    51'539'607'552,
    "The cap of memory allocation bytes");
DEFINE_int64(
    bolt_benchmark_allocation_size_seed,
    99887766,
    "Seed for random memory size generator");
DEFINE_int64(
    bolt_benchmark_memory_free_every_n_operations,
    1000,
    "Specifies memory free for every N operations. If it is 5, then we free one of existing memory allocation for every 5 memory operations");

#else
DEFINE_int64(
    bolt_benchmark_memory_allocation_count,
    1000,
    "The number of allocations");
DEFINE_int64(
    bolt_benchmark_memory_allocation_bytes,
    1'000'000'000,
    "The cap of memory allocation bytes");
DEFINE_int64(
    bolt_benchmark_allocation_size_seed,
    99887766,
    "Seed for random memory size generator");
DEFINE_int64(
    bolt_benchmark_memory_free_every_n_operations,
    5,
    "Specifies memory free for every N operations. If it is 5, then we free one of existing memory allocation for every 5 memory operations");
#endif
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;

namespace {

enum class Type {
  kStd = 0,
  kMmap = 1,
};

class MemoryPoolReallocateBenchMark {
 public:
  MemoryPoolReallocateBenchMark(
      Type type,
      uint16_t alignment,
      size_t minSize,
      size_t maxSize)
      : type_(type), minSize_(minSize), maxSize_(maxSize) {
    MemoryManager::Options options;
    options.alignment = alignment;
    switch (type_) {
      case Type::kMmap:
        manager_ = std::make_shared<MemoryManager>(options);
        break;
      case Type::kStd:
        manager_ = std::make_shared<MemoryManager>(options);
        break;
      default:
        BOLT_USER_FAIL("Unknown allocator type: {}", static_cast<int>(type_));
        break;
    }
    rng_.seed(FLAGS_bolt_benchmark_allocation_size_seed);
    pool_ = manager_->addLeafPool("MemoryPoolReallocateBenchMark");
  }

  ~MemoryPoolReallocateBenchMark() {
    while (!empty()) {
      free();
    }
  }

  size_t runAllocate();

  size_t runAllocateContiguous();
  size_t runAllocateContiguousOnce();

  size_t runAllocateZeroFilled();

  size_t runReallocate();

  size_t runReallocateLarger();

 private:
  struct Allocation {
    void* ptr;
    size_t size;

    Allocation(void* _ptr, size_t _size) : ptr(_ptr), size(_size) {}
  };

  void allocate() {
    const size_t size = allocSize();
    allocations_.emplace_back(pool_->allocate(size), size);
    ++numAllocs_;
    sumAllocBytes_ += size;
  }

  void allocateContiguous(MachinePageCount numPage) {
    memory::ContiguousAllocation contiguousAllocation;
    // MachinePageCount numPage = allocSize();
    size_t size = numPage * AllocationTraits::kPageSize;
    pool_->allocateContiguous(numPage, contiguousAllocation);
    char** table_ = contiguousAllocation.data<char*>();
    memset(table_, 0, size);
    contiguousAllocations_.emplace_back(std::move(contiguousAllocation));
    ++numAllocs_;
    sumAllocBytes_ += size;
  }

  void allocateZeroFilled() {
    const size_t size = allocSize();
    const size_t numEntries = 1 + folly::Random::rand32(size, rng_);
    const size_t sizeEach = size / numEntries;
    allocations_.emplace_back(
        pool_->allocateZeroFilled(numEntries, sizeEach), numEntries * sizeEach);
    ++numAllocs_;
    sumAllocBytes_ += numEntries * sizeEach;
  }

  void reallocate(const size_t oldSize, const size_t newSize) {
    void* oldPtr = pool_->allocate(oldSize);
    char* charPtr = static_cast<char*>(oldPtr);
    void* newPtr = pool_->reallocate(oldPtr, oldSize, newSize);
    allocations_.emplace_back(newPtr, newSize);
    ++numAllocs_;
    sumAllocBytes_ += newSize;
  }

  void reallocateLarger() {
    const size_t oldSize = allocSize();
    const size_t newSize = 1.5 * oldSize;
    void* oldPtr = pool_->allocate(oldSize);
    char* charPtr = static_cast<char*>(oldPtr);
    void* newPtr = pool_->reallocate(oldPtr, oldSize, newSize);
    allocations_.emplace_back(newPtr, newSize);
    ++numAllocs_;
    sumAllocBytes_ += newSize;
  }

  void free() {
    Allocation allocation = allocations_.front();
    allocations_.pop_front();
    pool_->free(allocation.ptr, allocation.size);
    sumAllocBytes_ -= allocation.size;
  }

  void freeContiguous() {
    ContiguousAllocation contiguousAllocation =
        std::move(contiguousAllocations_.front());
    contiguousAllocations_.pop_front();
    size_t size = contiguousAllocation.maxSize();
    pool_->freeContiguous(contiguousAllocation);
    sumAllocBytes_ -= size;
  }

  bool full() const {
    return sumAllocBytes_ >= FLAGS_bolt_benchmark_memory_allocation_bytes;
  }

  bool empty() const {
    return allocations_.empty();
  }

  bool contiguousAllocationEmpty() const {
    auto res = contiguousAllocations_.empty();
    return res;
  }

  size_t allocSize() {
    return minSize_ + folly::Random::rand32(maxSize_ - minSize_ + 1, rng_);
  }

  const Type type_;
  const size_t minSize_;
  const size_t maxSize_;
  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<MemoryManager> manager_;
  std::shared_ptr<MemoryPool> pool_;
  uint64_t sumAllocBytes_{0};
  uint64_t numAllocs_{0};
  std::deque<Allocation> allocations_;
  std::deque<ContiguousAllocation> contiguousAllocations_;
  std::vector<MachinePageCount> allocPages_;
  std::vector<MachinePageCount> reallocPages_;
};

size_t MemoryPoolReallocateBenchMark::runAllocate() {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_bolt_benchmark_memory_allocation_count;
       ++iter) {
    if (iter % FLAGS_bolt_benchmark_memory_free_every_n_operations == 0 &&
        !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    allocate();
  }
  return FLAGS_bolt_benchmark_memory_allocation_count;
}

size_t MemoryPoolReallocateBenchMark::runAllocateContiguous() {
  for (int i = 0; i < 100; i++) {
    allocPages_.emplace_back(allocSize());
  }

  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_bolt_benchmark_memory_allocation_count;
       ++iter) {
    if (iter % FLAGS_bolt_benchmark_memory_free_every_n_operations == 0 &&
        !contiguousAllocationEmpty()) {
      freeContiguous();
    }
    while (full()) {
      freeContiguous();
    }
    allocateContiguous(allocPages_[iter % 100]);
  }
  allocPages_.clear();
  return FLAGS_bolt_benchmark_memory_allocation_count;
}

size_t MemoryPoolReallocateBenchMark::runAllocateContiguousOnce() {
  allocateContiguous(10485760);
  return 1;
}

size_t MemoryPoolReallocateBenchMark::runAllocateZeroFilled() {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_bolt_benchmark_memory_allocation_count;
       ++iter) {
    if (iter % FLAGS_bolt_benchmark_memory_free_every_n_operations == 0 &&
        !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    allocateZeroFilled();
  }
  return FLAGS_bolt_benchmark_memory_allocation_count;
}

size_t MemoryPoolReallocateBenchMark::runReallocate() {
  for (int i = 0; i < 100; i++) {
    allocPages_.emplace_back(allocSize());
    reallocPages_.emplace_back(allocSize());
  }

  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_bolt_benchmark_memory_allocation_count;
       ++iter) {
    if (iter % FLAGS_bolt_benchmark_memory_free_every_n_operations == 0 &&
        !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    reallocate(allocPages_[iter % 100], reallocPages_[iter % 100]);
  }
  allocPages_.clear();
  reallocPages_.clear();
  return FLAGS_bolt_benchmark_memory_allocation_count;
}

size_t MemoryPoolReallocateBenchMark::runReallocateLarger() {
  for (int i = 0; i < 100; i++) {
    allocPages_.emplace_back(allocSize());
  }
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_bolt_benchmark_memory_allocation_count;
       ++iter) {
    if (iter % FLAGS_bolt_benchmark_memory_free_every_n_operations == 0 &&
        !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    reallocate(allocPages_[iter % 100], allocPages_[iter % 100] * 1.5);
  }
  allocPages_.clear();
  return FLAGS_bolt_benchmark_memory_allocation_count;
}

#ifdef PRESSURE
BENCHMARK_MULTI(StdAllocateInitialAlloc40G) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 1, 512);
  return benchmark.runAllocateContiguousOnce();
}
#endif

// benchmark for reallocate
// 128 ~ 4K
#ifndef ALIGNMENT
BENCHMARK_MULTI(StdReallocateNoAlignment4K) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 128, 4 << 10);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment4K) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 128, 4 << 10);
  return benchmark.runReallocateLarger();
}

// 4k ~ 2M
BENCHMARK_MULTI(StdReallocateNoAlignment4KTo2M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 4 << 10, 2 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateLargerNoAlignment4KTo2M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 4 << 10, 2 << 20);
  return benchmark.runReallocateLarger();
}

// 2M ~ 4M
BENCHMARK_MULTI(StdReallocateNoAlignment2MTo4M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 2 << 20, 4 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment2MTo4M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 2 << 20, 4 << 20);
  return benchmark.runReallocateLarger();
}
// 4M ~ 8M
BENCHMARK_MULTI(StdReallocateNoAlignment4MTo8M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 4 << 20, 8 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment4MTo8M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 4 << 20, 8 << 20);
  return benchmark.runReallocateLarger();
}
// 8M ~ 16M
BENCHMARK_MULTI(StdReallocateNoAlignment8MTo16M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 8 << 20, 16 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment8MTo16M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 8 << 20, 16 << 20);
  return benchmark.runReallocateLarger();
}
// 16M ~ 32M
BENCHMARK_MULTI(StdReallocateNoAlignment16MTo32M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 16 << 20, 32 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment16MTo32M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 16 << 20, 32 << 20);
  return benchmark.runReallocateLarger();
}
// 32M ~ 64M
BENCHMARK_MULTI(StdReallocateNoAlignment32MTo64M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 32 << 20, 64 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment32MTo64M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 32 << 20, 64 << 20);
  return benchmark.runReallocateLarger();
}

// 64M ~ 1G
BENCHMARK_MULTI(StdReallocateNoAlignment64MTo1G) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 64 << 20, 1 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerNoAlignment64MTo1G) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 64 << 20, 1 << 20);
  return benchmark.runReallocateLarger();
}
// Mix
BENCHMARK_MULTI(StdReallocateMixNoAlignment) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 128, 64 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerMixNoAlignment) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 16, 128, 64 << 20);
  return benchmark.runReallocateLarger();
}
#endif

#ifdef ALIGNMENT
// alignment
// 128 ~ 4K
BENCHMARK_MULTI(StdReallocate4K) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 128, 4 << 10);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger4K) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 128, 4 << 10);
  return benchmark.runReallocateLarger();
}

// 4k ~ 2M
BENCHMARK_MULTI(StdReallocate4KTo2M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 4 << 10, 2 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateLarger4KTo2M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 4 << 10, 2 << 20);
  return benchmark.runReallocateLarger();
}

// 2M ~ 4M
BENCHMARK_MULTI(StdReallocatet2MTo4M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 2 << 20, 4 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger2MTo4M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 2 << 20, 4 << 20);
  return benchmark.runReallocateLarger();
}
// 4M ~ 8M
BENCHMARK_MULTI(StdReallocate4MTo8M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 4 << 20, 8 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger4MTo8M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 4 << 20, 8 << 20);
  return benchmark.runReallocateLarger();
}
// 8M ~ 16M
BENCHMARK_MULTI(StdReallocate8MTo16M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 8 << 20, 16 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger8MTo16M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 8 << 20, 16 << 20);
  return benchmark.runReallocateLarger();
}
// 16M ~ 32M
BENCHMARK_MULTI(StdReallocate16MTo32M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 16 << 20, 32 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger16MTo32M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 16 << 20, 32 << 20);
  return benchmark.runReallocateLarger();
}
// 32M ~ 64M
BENCHMARK_MULTI(StdReallocate32MTo64M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 32 << 20, 64 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger32MTo64M) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 32 << 20, 64 << 20);
  return benchmark.runReallocateLarger();
}

// 64M ~ 1G
BENCHMARK_MULTI(StdReallocate64MTo1G) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 64 << 20, 1 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLarger64MTo1G) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 64 << 20, 1 << 20);
  return benchmark.runReallocateLarger();
}
// Mix
BENCHMARK_MULTI(StdReallocateMix) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 128, 64 << 20);
  return benchmark.runReallocate();
}
BENCHMARK_MULTI(StdReallocateLargerMix) {
  MemoryPoolReallocateBenchMark benchmark(Type::kStd, 64, 128, 64 << 20);
  return benchmark.runReallocateLarger();
}
#endif

} // namespace

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  // TODO: add to run benchmark as a standalone program with multithreading as
  // well as actual memory access to trigger minor page faults in OS which traps
  // into kernel context to setup physical pages for the lazy-mapped virtual
  // process memory space.
  folly::runBenchmarks();
  return 0;
}
