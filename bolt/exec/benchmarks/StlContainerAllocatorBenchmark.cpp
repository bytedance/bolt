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

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <atomic>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/MemoryResource.h"

DEFINE_int64(
    stl_container_allocator_max_total_insertions,
    600'000'000,
    "Skip cases whose concurrent vector+map insertion count is larger than "
    "this cap");

DEFINE_int32(
    stl_container_allocator_repeats,
    1,
    "Repeat count for each benchmark case");

DEFINE_int32(
    stl_container_allocator_concurrency,
    4,
    "Number of concurrent allocator workers to run per benchmark case");

using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;

namespace {

// clang-format off
// Run cases one by one and observe peak RSS from outside:
//
// bench='_build/Release/bolt/exec/benchmarks/bolt_stl_container_allocator_benchmark'
// for case in \
//   HashStringStlAllocator_N1K_M10 SlabAllocator_N1K_M10 MemoryStlAllocator_N1K_M10 \
//   HashStringStlAllocator_N1K_M100 SlabAllocator_N1K_M100 MemoryStlAllocator_N1K_M100 \
//   HashStringStlAllocator_N1K_M1000 SlabAllocator_N1K_M1000 MemoryStlAllocator_N1K_M1000 \
//   HashStringStlAllocator_N1K_M10000 SlabAllocator_N1K_M10000 MemoryStlAllocator_N1K_M10000 \
//   HashStringStlAllocator_N1K_M100000 SlabAllocator_N1K_M100000 MemoryStlAllocator_N1K_M100000 \
//   HashStringStlAllocator_N16K_M10 SlabAllocator_N16K_M10 MemoryStlAllocator_N16K_M10 \
//   HashStringStlAllocator_N16K_M100 SlabAllocator_N16K_M100 MemoryStlAllocator_N16K_M100 \
//   HashStringStlAllocator_N16K_M1000 SlabAllocator_N16K_M1000 MemoryStlAllocator_N16K_M1000 \
//   HashStringStlAllocator_N16K_M10000 SlabAllocator_N16K_M10000 MemoryStlAllocator_N16K_M10000 \
//   HashStringStlAllocator_N16K_M100000 SlabAllocator_N16K_M100000 MemoryStlAllocator_N16K_M100000 \
//   HashStringStlAllocator_N256K_M10 SlabAllocator_N256K_M10 MemoryStlAllocator_N256K_M10 \
//   HashStringStlAllocator_N256K_M100 SlabAllocator_N256K_M100 MemoryStlAllocator_N256K_M100 \
//   HashStringStlAllocator_N256K_M1000 SlabAllocator_N256K_M1000 MemoryStlAllocator_N256K_M1000 \
//   HashStringStlAllocator_N256K_M10000 SlabAllocator_N256K_M10000 MemoryStlAllocator_N256K_M10000 \
//   HashStringStlAllocator_N256K_M100000 SlabAllocator_N256K_M100000 MemoryStlAllocator_N256K_M100000; do
//   echo "=== $case ==="
//   "$bench" --benchmark --bm_regex="^${case}$" --bm_min_iters=1 --bm_max_secs=1 \
//     > /tmp/${case}.observe.out 2>&1 &
//   pid=$!
//   peak=0
//   last=0
//   while kill -0 "$pid" 2>/dev/null; do
//     hwm=$(awk '/VmHWM:/ {print $2}' /proc/$pid/status 2>/dev/null)
//     rss=$(awk '/VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null)
//     val=${hwm:-$rss}
//     if [ -n "$val" ] && [ "$val" -gt "$peak" ]; then
//       peak=$val
//     fi
//     if [ -n "$rss" ]; then
//       last=$rss
//     fi
//     sleep 0.05
//   done
//   wait "$pid"
//   code=$?
//   cat /tmp/${case}.observe.out
//   echo "external_peak_rss_kb=$peak"
//   echo "last_observed_rss_kb=$last"
//   echo "exit_code=$code"
// done
//
// The default max-total-insertions cap skips cases that are too large for
// routine local runs. Increase --stl_container_allocator_max_total_insertions
// to run larger cases explicitly.
//
// Result on 2026-07-01, Release build, concurrency=4:
//
// | allocator              | N    | M      | time/iter | retained/reserved bytes | external peak RSS | skipped |
// |------------------------|------|--------|-----------|-------------------------|-------------------|---------|
// | HashStringStlAllocator | 1K   | 10     | 2.11ms    | 9,437,184               | 63,332 KiB        | false   |
// | SlabAllocator          | 1K   | 10     | 1.15ms    | 4,194,304               | 57,392 KiB        | false   |
// | MemoryStlAllocator     | 1K   | 10     | 3.36ms    | 0                       | 62,460 KiB        | false   |
// | HashStringStlAllocator | 1K   | 100    | 16.35ms   | 26,214,400              | 79,656 KiB        | false   |
// | SlabAllocator          | 1K   | 100    | 12.79ms   | 25,165,824              | 82,720 KiB        | false   |
// | MemoryStlAllocator     | 1K   | 100    | 30.66ms   | 0                       | 132,992 KiB       | false   |
// | HashStringStlAllocator | 1K   | 1000   | 190.12ms  | 251,920,384             | 300,428 KiB       | false   |
// | SlabAllocator          | 1K   | 1000   | 144.13ms  | 230,686,720             | 342,592 KiB       | false   |
// | MemoryStlAllocator     | 1K   | 1000   | 338.29ms  | 0                       | 850,088 KiB       | false   |
// | HashStringStlAllocator | 1K   | 10000  | 2.22s     | 2,459,435,008           | 2,456,160 KiB     | false   |
// | SlabAllocator          | 1K   | 10000  | 2.35s     | 1,967,128,576           | 2,933,756 KiB     | false   |
// | MemoryStlAllocator     | 1K   | 10000  | 5.01s     | 0                       | 7,821,940 KiB     | false   |
// | HashStringStlAllocator | 1K   | 100000 | 18.98ns   | 0                       | 51,472 KiB        | true    |
// | SlabAllocator          | 1K   | 100000 | 3.01ns    | 0                       | 51,632 KiB        | true    |
// | MemoryStlAllocator     | 1K   | 100000 | 18.86ns   | 0                       | 51,620 KiB        | true    |
// | HashStringStlAllocator | 16K  | 10     | 23.74ms   | 42,991,616              | 101,288 KiB       | false   |
// | SlabAllocator          | 16K  | 10     | 17.39ms   | 37,748,736              | 106,864 KiB       | false   |
// | MemoryStlAllocator     | 16K  | 10     | 52.05ms   | 0                       | 198,564 KiB       | false   |
// | HashStringStlAllocator | 16K  | 100    | 272.13ms  | 395,313,152             | 445,908 KiB       | false   |
// | SlabAllocator          | 16K  | 100    | 259.06ms  | 369,098,752             | 521,020 KiB       | false   |
// | MemoryStlAllocator     | 16K  | 100    | 631.95ms  | 0                       | 1,348,996 KiB     | false   |
// | HashStringStlAllocator | 16K  | 1000   | 4.43s     | 3,939,500,032           | 3,912,092 KiB     | false   |
// | SlabAllocator          | 16K  | 1000   | 2.79s     | 3,670,016,000           | 4,667,840 KiB     | false   |
// | MemoryStlAllocator     | 16K  | 1000   | 6.40s     | 0                       | 12,503,524 KiB    | false   |
// | HashStringStlAllocator | 16K  | 10000  | 18.88ns   | 0                       | 51,564 KiB        | true    |
// | SlabAllocator          | 16K  | 10000  | 7.17ns    | 0                       | 51,584 KiB        | true    |
// | MemoryStlAllocator     | 16K  | 10000  | 18.88ns   | 0                       | 51,192 KiB        | true    |
// | HashStringStlAllocator | 16K  | 100000 | 18.92ns   | 0                       | 51,528 KiB        | true    |
// | SlabAllocator          | 16K  | 100000 | 3.34ns    | 0                       | 51,572 KiB        | true    |
// | MemoryStlAllocator     | 16K  | 100000 | 18.68ns   | 0                       | 51,532 KiB        | true    |
// | HashStringStlAllocator | 256K | 10     | 383.91ms  | 638,582,784             | 767,828 KiB       | false   |
// | SlabAllocator          | 256K | 10     | 381.02ms  | 587,202,560             | 897,828 KiB       | false   |
// | MemoryStlAllocator     | 256K | 10     | 1.28s     | 0                       | 2,339,372 KiB     | false   |
// | HashStringStlAllocator | 256K | 100    | 4.50s     | 6,300,893,184           | 6,297,236 KiB     | false   |
// | SlabAllocator          | 256K | 100    | 4.45s     | 5,872,025,600           | 7,532,656 KiB     | false   |
// | MemoryStlAllocator     | 256K | 100    | 10.72s    | 0                       | 20,190,108 KiB    | false   |
// | HashStringStlAllocator | 256K | 1000   | 20.72ns   | 0                       | 51,528 KiB        | true    |
// | SlabAllocator          | 256K | 1000   | 3.01ns    | 0                       | 51,508 KiB        | true    |
// | MemoryStlAllocator     | 256K | 1000   | 20.49ns   | 0                       | 51,556 KiB        | true    |
// | HashStringStlAllocator | 256K | 10000  | 18.54ns   | 0                       | 51,508 KiB        | true    |
// | SlabAllocator          | 256K | 10000  | 3.34ns    | 0                       | 51,584 KiB        | true    |
// | MemoryStlAllocator     | 256K | 10000  | 18.65ns   | 0                       | 51,532 KiB        | true    |
// | HashStringStlAllocator | 256K | 100000 | 25.67ns   | 0                       | 51,476 KiB        | true    |
// | SlabAllocator          | 256K | 100000 | 7.02ns    | 0                       | 51,460 KiB        | true    |
// | MemoryStlAllocator     | 256K | 100000 | 18.92ns   | 0                       | 51,532 KiB        | true    |
// clang-format on

constexpr int64_t kAllocatorCapacity = 64L << 30;
constexpr int64_t kOneK = 1 << 10;
constexpr int64_t k16K = 16 << 10;
constexpr int64_t k256K = 256 << 10;
constexpr int64_t k1000 = 1000;
constexpr int64_t k10000 = 10000;
constexpr int64_t k100000 = 100000;

struct BenchmarkStats {
  std::string allocator;
  int64_t numContainers{0};
  int64_t elementsPerContainer{0};
  int64_t repeats{0};
  int32_t concurrency{0};
  int64_t vectorInsertions{0};
  int64_t mapInsertions{0};
  int64_t checksum{0};
  int64_t retainedBytes{0};
  int64_t slabUsedBytes{0};
  int64_t slabReservedBytes{0};
  bool skipped{false};
};

class BenchmarkMemory {
 public:
  explicit BenchmarkMemory(const char* name) {
    MemoryManager::Options options;
    options.allocatorCapacity = kAllocatorCapacity;
    options.arbitratorCapacity = kAllocatorCapacity;
    options.useMmapAllocator = false;
    manager_ = std::make_unique<MemoryManager>(options);
    pool_ = manager_->addLeafPool(name);
  }

  MemoryPool* pool() const {
    return pool_.get();
  }

 private:
  std::unique_ptr<MemoryManager> manager_;
  std::shared_ptr<MemoryPool> pool_;
};

bool shouldSkip(int64_t n, int64_t m) {
  return n > 0 &&
      m > FLAGS_stl_container_allocator_max_total_insertions / n / 2 /
          FLAGS_stl_container_allocator_concurrency;
}

void printStats(const BenchmarkStats& stats) {
  fmt::print(
      "{} N={} M={} repeats={} concurrency={} vectorInsertions={} mapInsertions={} checksum={} retainedBytes={} slabUsedBytes={} slabReservedBytes={} skipped={}\n",
      stats.allocator,
      stats.numContainers,
      stats.elementsPerContainer,
      stats.repeats,
      stats.concurrency,
      stats.vectorInsertions,
      stats.mapInsertions,
      stats.checksum,
      stats.retainedBytes,
      stats.slabUsedBytes,
      stats.slabReservedBytes,
      stats.skipped);
}

void mergeStats(BenchmarkStats& stats, const BenchmarkStats& local) {
  stats.vectorInsertions += local.vectorInsertions;
  stats.mapInsertions += local.mapInsertions;
  stats.checksum += local.checksum;
  stats.retainedBytes += local.retainedBytes;
  stats.slabUsedBytes += local.slabUsedBytes;
  stats.slabReservedBytes += local.slabReservedBytes;
}

template <typename Worker>
void runConcurrentWorkers(Worker&& worker) {
  const auto concurrency = FLAGS_stl_container_allocator_concurrency;
  BOLT_CHECK_GT(concurrency, 0);

  std::atomic<int32_t> ready{0};
  std::atomic<bool> start{false};
  std::exception_ptr exception;
  std::mutex exceptionMutex;
  std::vector<std::thread> threads;
  threads.reserve(concurrency);

  folly::BenchmarkSuspender suspender;
  for (int32_t threadId = 0; threadId < concurrency; ++threadId) {
    threads.emplace_back([&, threadId]() {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      try {
        worker(threadId);
      } catch (...) {
        std::lock_guard<std::mutex> l(exceptionMutex);
        if (!exception) {
          exception = std::current_exception();
        }
      }
    });
  }

  while (ready.load(std::memory_order_acquire) < concurrency) {
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
}

template <int64_t N, int64_t M>
BenchmarkStats runHashStringStlAllocator() {
  BenchmarkStats stats{
      .allocator = "HashStringStlAllocator",
      .numContainers = N,
      .elementsPerContainer = M,
      .repeats = FLAGS_stl_container_allocator_repeats,
      .concurrency = FLAGS_stl_container_allocator_concurrency,
      .skipped = shouldSkip(N, M),
  };
  if (stats.skipped) {
    return stats;
  }

  std::mutex statsMutex;
  runConcurrentWorkers([&](int32_t threadId) {
    BenchmarkStats local;
    BenchmarkMemory memory{"hash_string_stl_container_allocator_benchmark"};
    HashStringAllocator hashAllocator(memory.pool());
    using VectorAllocator = bytedance::bolt::StlAllocator<int64_t>;
    using MapValue = std::pair<const int64_t, int64_t>;
    using MapAllocator = bytedance::bolt::StlAllocator<MapValue>;
    using Vector = std::vector<int64_t, VectorAllocator>;
    using Map = std::map<int64_t, int64_t, std::less<int64_t>, MapAllocator>;

    VectorAllocator vectorAllocator{&hashAllocator};
    MapAllocator mapAllocator{&hashAllocator};
    std::vector<Vector> vectors;
    std::vector<Map> maps;
    vectors.reserve(N);
    maps.reserve(N);
    for (int64_t i = 0; i < N; ++i) {
      vectors.emplace_back(vectorAllocator);
      maps.emplace_back(std::less<int64_t>{}, mapAllocator);
    }

    for (int32_t repeat = 0; repeat < FLAGS_stl_container_allocator_repeats;
         ++repeat) {
      int64_t checksum = 0;
      for (int64_t i = 0; i < N; ++i) {
        auto& vector = vectors[i];
        vector.reserve(M);
        auto& map = maps[i];
        for (int64_t j = 0; j < M; ++j) {
          const auto value =
              (static_cast<int64_t>(threadId) << 40) ^ (i << 20) ^ j;
          vector.push_back(value);
          map.emplace(j + static_cast<int64_t>(repeat) * M, value);
        }
        checksum += vector.size();
        checksum += map.size();
        if (M > 0) {
          checksum += vector.back();
          checksum += map.rbegin()->second;
        }
      }
      local.vectorInsertions += N * M;
      local.mapInsertions += N * M;
      local.checksum += checksum;
    }

    folly::doNotOptimizeAway(vectors.data());
    folly::doNotOptimizeAway(maps.data());
    local.retainedBytes = hashAllocator.retainedSize();
    std::lock_guard<std::mutex> l(statsMutex);
    mergeStats(stats, local);
  });

  return stats;
}

template <int64_t N, int64_t M>
BenchmarkStats runSlabAllocator() {
  BenchmarkStats stats{
      .allocator = "SlabAllocator",
      .numContainers = N,
      .elementsPerContainer = M,
      .repeats = FLAGS_stl_container_allocator_repeats,
      .concurrency = FLAGS_stl_container_allocator_concurrency,
      .skipped = shouldSkip(N, M),
  };
  if (stats.skipped) {
    return stats;
  }

  std::mutex statsMutex;
  runConcurrentWorkers([&](int32_t threadId) {
    BenchmarkStats local;
    BenchmarkMemory memory{"slab_stl_container_allocator_benchmark"};
    SlabMemoryResource resource{memory.pool()};
    using VectorAllocator = SlabAllocator<int64_t, alignof(int64_t)>;
    using MapValue = std::pair<const int64_t, int64_t>;
    using MapAllocator = SlabAllocator<MapValue, alignof(MapValue)>;
    using Vector = std::vector<int64_t, VectorAllocator>;
    using Map = std::map<int64_t, int64_t, std::less<int64_t>, MapAllocator>;

    VectorAllocator vectorAllocator{resource};
    MapAllocator mapAllocator{resource};
    std::vector<Vector> vectors;
    std::vector<Map> maps;
    vectors.reserve(N);
    maps.reserve(N);
    for (int64_t i = 0; i < N; ++i) {
      vectors.emplace_back(vectorAllocator);
      maps.emplace_back(std::less<int64_t>{}, mapAllocator);
    }

    for (int32_t repeat = 0; repeat < FLAGS_stl_container_allocator_repeats;
         ++repeat) {
      int64_t checksum = 0;
      for (int64_t i = 0; i < N; ++i) {
        auto& vector = vectors[i];
        vector.reserve(M);
        auto& map = maps[i];
        for (int64_t j = 0; j < M; ++j) {
          const auto value =
              (static_cast<int64_t>(threadId) << 40) ^ (i << 20) ^ j;
          vector.push_back(value);
          map.emplace(j + static_cast<int64_t>(repeat) * M, value);
        }
        checksum += vector.size();
        checksum += map.size();
        if (M > 0) {
          checksum += vector.back();
          checksum += map.rbegin()->second;
        }
      }
      local.vectorInsertions += N * M;
      local.mapInsertions += N * M;
      local.checksum += checksum;
    }

    folly::doNotOptimizeAway(vectors.data());
    folly::doNotOptimizeAway(maps.data());
    local.slabUsedBytes = resource.usedBytes();
    local.slabReservedBytes = resource.reservedBytes();
    std::lock_guard<std::mutex> l(statsMutex);
    mergeStats(stats, local);
  });

  return stats;
}

template <int64_t N, int64_t M>
BenchmarkStats runMemoryStlAllocator() {
  BenchmarkStats stats{
      .allocator = "MemoryStlAllocator",
      .numContainers = N,
      .elementsPerContainer = M,
      .repeats = FLAGS_stl_container_allocator_repeats,
      .concurrency = FLAGS_stl_container_allocator_concurrency,
      .skipped = shouldSkip(N, M),
  };
  if (stats.skipped) {
    return stats;
  }

  std::mutex statsMutex;
  runConcurrentWorkers([&](int32_t threadId) {
    BenchmarkStats local;
    BenchmarkMemory memory{"memory_stl_container_allocator_benchmark"};
    using VectorAllocator = bytedance::bolt::memory::StlAllocator<int64_t>;
    using MapValue = std::pair<const int64_t, int64_t>;
    using MapAllocator = bytedance::bolt::memory::StlAllocator<MapValue>;
    using Vector = std::vector<int64_t, VectorAllocator>;
    using Map = std::map<int64_t, int64_t, std::less<int64_t>, MapAllocator>;

    VectorAllocator vectorAllocator{memory.pool()};
    MapAllocator mapAllocator{memory.pool()};
    std::vector<Vector> vectors;
    std::vector<Map> maps;
    vectors.reserve(N);
    maps.reserve(N);
    for (int64_t i = 0; i < N; ++i) {
      vectors.emplace_back(vectorAllocator);
      maps.emplace_back(std::less<int64_t>{}, mapAllocator);
    }

    for (int32_t repeat = 0; repeat < FLAGS_stl_container_allocator_repeats;
         ++repeat) {
      int64_t checksum = 0;
      for (int64_t i = 0; i < N; ++i) {
        auto& vector = vectors[i];
        vector.reserve(M);
        auto& map = maps[i];
        for (int64_t j = 0; j < M; ++j) {
          const auto value =
              (static_cast<int64_t>(threadId) << 40) ^ (i << 20) ^ j;
          vector.push_back(value);
          map.emplace(j + static_cast<int64_t>(repeat) * M, value);
        }
        checksum += vector.size();
        checksum += map.size();
        if (M > 0) {
          checksum += vector.back();
          checksum += map.rbegin()->second;
        }
      }
      local.vectorInsertions += N * M;
      local.mapInsertions += N * M;
      local.checksum += checksum;
    }

    folly::doNotOptimizeAway(vectors.data());
    folly::doNotOptimizeAway(maps.data());
    local.retainedBytes = memory.pool()->peakBytes();
    std::lock_guard<std::mutex> l(statsMutex);
    mergeStats(stats, local);
  });

  return stats;
}

#define BENCHMARK_CONTAINER_CASE(N_LABEL, N_VALUE, M_LABEL, M_VALUE) \
  BENCHMARK(HashStringStlAllocator_##N_LABEL##_##M_LABEL) {          \
    static bool printed = false;                                     \
    auto stats = runHashStringStlAllocator<N_VALUE, M_VALUE>();      \
    if (!printed) {                                                  \
      printStats(stats);                                             \
      printed = true;                                                \
    }                                                                \
  }                                                                  \
  BENCHMARK(SlabAllocator_##N_LABEL##_##M_LABEL) {                   \
    static bool printed = false;                                     \
    auto stats = runSlabAllocator<N_VALUE, M_VALUE>();               \
    if (!printed) {                                                  \
      printStats(stats);                                             \
      printed = true;                                                \
    }                                                                \
  }                                                                  \
  BENCHMARK(MemoryStlAllocator_##N_LABEL##_##M_LABEL) {              \
    static bool printed = false;                                     \
    auto stats = runMemoryStlAllocator<N_VALUE, M_VALUE>();          \
    if (!printed) {                                                  \
      printStats(stats);                                             \
      printed = true;                                                \
    }                                                                \
  }                                                                  \
  BENCHMARK_DRAW_LINE();

BENCHMARK_CONTAINER_CASE(N1K, kOneK, M10, 10)
BENCHMARK_CONTAINER_CASE(N1K, kOneK, M100, 100)
BENCHMARK_CONTAINER_CASE(N1K, kOneK, M1000, k1000)
BENCHMARK_CONTAINER_CASE(N1K, kOneK, M10000, k10000)
BENCHMARK_CONTAINER_CASE(N1K, kOneK, M100000, k100000)

BENCHMARK_CONTAINER_CASE(N16K, k16K, M10, 10)
BENCHMARK_CONTAINER_CASE(N16K, k16K, M100, 100)
BENCHMARK_CONTAINER_CASE(N16K, k16K, M1000, k1000)
BENCHMARK_CONTAINER_CASE(N16K, k16K, M10000, k10000)
BENCHMARK_CONTAINER_CASE(N16K, k16K, M100000, k100000)

BENCHMARK_CONTAINER_CASE(N256K, k256K, M10, 10)
BENCHMARK_CONTAINER_CASE(N256K, k256K, M100, 100)
BENCHMARK_CONTAINER_CASE(N256K, k256K, M1000, k1000)
BENCHMARK_CONTAINER_CASE(N256K, k256K, M10000, k10000)
BENCHMARK_CONTAINER_CASE(N256K, k256K, M100000, k100000)

#undef BENCHMARK_CONTAINER_CASE

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
