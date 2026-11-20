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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "bolt/common/memory/MemoryResource.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"

DEFINE_int64(
    bolt_benchmark_slab_allocator_benchmark_count,
    2'000'000,
    "The total number of small allocations in allocator benchmark");

using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;

// Multi-threaded results report total operations divided by wall-clock time.
// Re-run benchmarks after changes to allocation or accounting semantics.

namespace {

constexpr int64_t kBenchmarkCapacity = 1L << 30;
constexpr size_t kCachedAllocations256K = 256 * 1024;

constexpr std::array<int32_t, 128> kAllocationSizes = {
    9,  15, 18, 20, 15, 13, 21, 20, 19, 23, 16, 12, 15, 29, 5,  21, 16, 10, 15,
    15, 11, 17, 30, 13, 23, 26, 16, 19, 15, 24, 3,  7,  10, 13, 20, 13, 11, 12,
    13, 22, 28, 7,  14, 16, 30, 23, 21, 16, 19, 20, 6,  11, 27, 17, 9,  21, 14,
    16, 18, 2,  4,  11, 22, 10, 13, 16, 22, 29, 25, 18, 26, 15, 12, 25, 12, 14,
    14, 22, 24, 16, 14, 17, 17, 8,  17, 11, 18, 15, 18, 8,  12, 14, 1,  20, 7,
    13, 17, 12, 24, 18, 18, 8,  19, 17, 11, 19, 21, 9,  18, 12, 16, 13, 28, 9,
    19, 5,  14, 20, 6,  16, 10, 14, 15, 17, 10, 15, 27, 19};

static_assert(kAllocationSizes.size() == 128);

class SlabAllocatorBenchmark {
 public:
  SlabAllocatorBenchmark() {
    MemoryManager::Options options;
    options.allocatorCapacity = kBenchmarkCapacity;
    options.arbitratorCapacity = kBenchmarkCapacity;
    options.useMmapAllocator = false;
    manager_ = std::make_unique<MemoryManager>(options);
    pool_ = manager_->addLeafPool("slab_allocator_benchmark");
  }

  template <size_t kCachedAllocations, size_t kConcurrency>
  int64_t runSlabAllocFree() {
    return runConcurrentFifoAllocFree<kConcurrency>(
        [this](size_t threadId, int64_t numAllocations) {
          SlabMemoryResource resource(pool_.get());
          auto allocator = SlabAllocator<char>(&resource);
          runFifoAllocFree<
              cachedAllocationsPerThread<kCachedAllocations, kConcurrency>()>(
              allocator, numAllocations, threadId * kAllocationSizes.size());
        });
  }

  template <size_t kCachedAllocations, size_t kConcurrency>
  int64_t runHashStringAllocFree() {
    return runConcurrentFifoAllocFree<kConcurrency>(
        [this](size_t threadId, int64_t numAllocations) {
          HashStringAllocator hashAllocator(pool_.get());
          bytedance::bolt::StlAllocator<char> allocator(&hashAllocator);
          runFifoAllocFree<
              cachedAllocationsPerThread<kCachedAllocations, kConcurrency>()>(
              allocator, numAllocations, threadId * kAllocationSizes.size());
        });
  }

  template <size_t kCachedAllocations, size_t kConcurrency>
  int64_t runAlignedAllocFree() {
    return runConcurrentFifoAllocFree<kConcurrency>(
        [this](size_t threadId, int64_t numAllocations) {
          bytedance::bolt::memory::AlignedStlAllocator<char> allocator(
              pool_.get());
          runFifoAllocFree<
              cachedAllocationsPerThread<kCachedAllocations, kConcurrency>()>(
              allocator, numAllocations, threadId * kAllocationSizes.size());
        });
  }

 private:
  template <size_t kCachedAllocations, size_t kConcurrency>
  static constexpr size_t cachedAllocationsPerThread() {
    return (kCachedAllocations + kConcurrency - 1) / kConcurrency;
  }

  template <size_t kConcurrency, typename Worker>
  int64_t runConcurrentFifoAllocFree(Worker&& worker) {
    static_assert(kConcurrency > 0);

    std::atomic<size_t> readyThreads{0};
    std::atomic<bool> start{false};
    std::exception_ptr exception;
    std::mutex exceptionMutex;
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    folly::BenchmarkSuspender suspender;
    for (size_t threadId = 0; threadId < kConcurrency; ++threadId) {
      threads.emplace_back([&, threadId]() {
        readyThreads.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }

        try {
          worker(threadId, allocationsPerThread(threadId, kConcurrency));
        } catch (...) {
          std::lock_guard<std::mutex> l(exceptionMutex);
          if (!exception) {
            exception = std::current_exception();
          }
        }
      });
    }

    while (readyThreads.load(std::memory_order_acquire) < kConcurrency) {
      std::this_thread::yield();
    }
    suspender.dismiss();
    start.store(true, std::memory_order_release);

    for (auto& thread : threads) {
      thread.join();
    }
    if (exception) {
      std::rethrow_exception(exception);
    }

    return FLAGS_bolt_benchmark_slab_allocator_benchmark_count;
  }

  static int64_t allocationsPerThread(size_t threadId, size_t concurrency) {
    const auto base =
        FLAGS_bolt_benchmark_slab_allocator_benchmark_count / concurrency;
    const auto remainder =
        FLAGS_bolt_benchmark_slab_allocator_benchmark_count % concurrency;
    return base + (threadId < remainder ? 1 : 0);
  }

  template <size_t kCachedAllocations, typename Allocator>
  void runFifoAllocFree(
      Allocator& allocator,
      int64_t numAllocations,
      size_t sizeOffset) {
    auto allocations = std::make_unique<char*[]>(kCachedAllocations);
    auto sizes = std::make_unique<int32_t[]>(kCachedAllocations);
    size_t cachedAllocations{0};
    size_t nextToFree{0};

    for (int64_t i = 0; i < numAllocations; ++i) {
      const auto size =
          kAllocationSizes[(sizeOffset + i) % kAllocationSizes.size()];
      auto* p = allocator.allocate(size);
      folly::doNotOptimizeAway(p);
      if ((i & 3) == 0) {
        allocator.deallocate(p, size);
        continue;
      }

      if (cachedAllocations < kCachedAllocations) {
        allocations[cachedAllocations] = p;
        sizes[cachedAllocations] = size;
        ++cachedAllocations;
      } else {
        allocator.deallocate(allocations[nextToFree], sizes[nextToFree]);
        allocations[nextToFree] = p;
        sizes[nextToFree] = size;
        nextToFree = (nextToFree + 1) % kCachedAllocations;
      }
    }

    for (size_t i = 0; i < cachedAllocations; ++i) {
      const auto index = (nextToFree + i) % cachedAllocations;
      allocator.deallocate(allocations[index], sizes[index]);
    }
  }

  std::unique_ptr<MemoryManager> manager_;
  std::shared_ptr<MemoryPool> pool_;
};

BENCHMARK_MULTI(HashStringAllocatorFifoAllocFree256K_1Thread) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runHashStringAllocFree<kCachedAllocations256K, 1>();
}

BENCHMARK_RELATIVE_MULTI(AlignedStlAllocatorFifoAllocFree256K_1Thread) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runAlignedAllocFree<kCachedAllocations256K, 1>();
}

BENCHMARK_RELATIVE_MULTI(SlabAllocatorFifoAllocFree256K_1Thread) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runSlabAllocFree<kCachedAllocations256K, 1>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(HashStringAllocatorFifoAllocFree256K_8Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runHashStringAllocFree<kCachedAllocations256K, 8>();
}

BENCHMARK_RELATIVE_MULTI(AlignedStlAllocatorFifoAllocFree256K_8Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runAlignedAllocFree<kCachedAllocations256K, 8>();
}

BENCHMARK_RELATIVE_MULTI(SlabAllocatorFifoAllocFree256K_8Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runSlabAllocFree<kCachedAllocations256K, 8>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(HashStringAllocatorFifoAllocFree256K_16Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runHashStringAllocFree<kCachedAllocations256K, 16>();
}

BENCHMARK_RELATIVE_MULTI(AlignedStlAllocatorFifoAllocFree256K_16Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runAlignedAllocFree<kCachedAllocations256K, 16>();
}

BENCHMARK_RELATIVE_MULTI(SlabAllocatorFifoAllocFree256K_16Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runSlabAllocFree<kCachedAllocations256K, 16>();
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(HashStringAllocatorFifoAllocFree256K_32Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runHashStringAllocFree<kCachedAllocations256K, 32>();
}

BENCHMARK_RELATIVE_MULTI(AlignedStlAllocatorFifoAllocFree256K_32Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runAlignedAllocFree<kCachedAllocations256K, 32>();
}

BENCHMARK_RELATIVE_MULTI(SlabAllocatorFifoAllocFree256K_32Threads) {
  SlabAllocatorBenchmark benchmark;
  return benchmark.runSlabAllocFree<kCachedAllocations256K, 32>();
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
