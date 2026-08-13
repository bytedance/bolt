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
#define PRESSURE2

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

class MemoryPoolAllocateContiguousBenchMark {
 public:
  MemoryPoolAllocateContiguousBenchMark(
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
    pool_ = manager_->addLeafPool("MemoryPoolAllocateContiguousBenchMark");
  }

  ~MemoryPoolAllocateContiguousBenchMark() {
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

size_t MemoryPoolAllocateContiguousBenchMark::runAllocate() {
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

size_t MemoryPoolAllocateContiguousBenchMark::runAllocateContiguous() {
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

size_t MemoryPoolAllocateContiguousBenchMark::runAllocateContiguousOnce() {
  allocateContiguous(10485760);
  return 1;
}

size_t MemoryPoolAllocateContiguousBenchMark::runAllocateZeroFilled() {
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

size_t MemoryPoolAllocateContiguousBenchMark::runReallocate() {
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

size_t MemoryPoolAllocateContiguousBenchMark::runReallocateLarger() {
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

// benchmark for allocateContiguous
#ifdef PRESSURE2
BENCHMARK_MULTI(StdAllocateInitialAlloc40G) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 1, 512);
  return benchmark.runAllocateContiguousOnce();
}
#endif
BENCHMARK_MULTI(StdAllocateContiguousSmallNoAlignment2M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 1, 512);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousSmall2M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 1, 512);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousSmallNoAlignment2MTo4M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 512, 1024);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousSmall2MTo4M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 512, 1024);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousSmallNoAlignment4MTo8M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 1024, 2048);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousSmall4MTo8M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 1024, 2048);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousNoAlignment8MTo16M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 2048, 4096);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguous8MTo16M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 2048, 4096);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousNoAlignment16MTo32M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 4096, 8192);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguous16MTo32M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 4096, 8192);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousNoAlignment32MTo64M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 8192, 16384);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguous32MTo64M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 8192, 16384);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousNoAlignment64MTo128M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 16, 16384, 32768);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguous64MTo128M) {
  MemoryPoolAllocateContiguousBenchMark benchmark(Type::kStd, 64, 16384, 32768);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguousNoAlignment128MTo1G) {
  MemoryPoolAllocateContiguousBenchMark benchmark(
      Type::kStd, 16, 32768, 262144);
  return benchmark.runAllocateContiguous();
}
BENCHMARK_MULTI(StdAllocateContiguous128MTo1G) {
  MemoryPoolAllocateContiguousBenchMark benchmark(
      Type::kStd, 64, 32768, 262144);
  return benchmark.runAllocateContiguous();
}
} // namespace

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
