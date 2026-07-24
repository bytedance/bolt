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

/*
============================================================================
[...]StringMonotonicAllocatorBenchmark.cpp     relative  time/iter   iters/s
============================================================================
HashStringAllocatorAllocateNoFree                            6.22s   160.90m
MonotonicMemoryResourceAllocateNoFree           1287.9%   482.60ms      2.07
*/

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/MemoryResource.h"

DEFINE_int64(
    allocator_compare_payload_bytes,
    1L << 30,
    "Total requested payload bytes to allocate");

using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;

namespace {

// clang-format off
// Run the two cases as separate processes and observe peak RSS from outside:
//
// bench='_build/Release/bolt/exec/benchmarks/bolt_hash_string_monotonic_allocator_benchmark'
// for case in HashStringAllocatorAllocateNoFree MonotonicMemoryResourceAllocateNoFree; do
//   echo "=== $case ==="
//   "$bench" --benchmark --bm_regex="$case" --bm_min_iters=1 --bm_max_secs=1 \
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
// Result on 2026-07-01, Release build:
//
// | case                                 | time/iter | allocations | requested bytes | reserved bytes | external peak RSS |
// |--------------------------------------|-----------|-------------|-----------------|----------------|-------------------|
// | HashStringAllocatorAllocateNoFree    | 4.36s     | 54,216,550  | 1,073,741,833   | 0              | 1,354,144 KiB     |
// | MonotonicMemoryResourceAllocateNoFree| 809.35ms  | 54,216,550  | 1,073,741,833   | 2,145,386,496  | 1,099,896 KiB     |
// clang-format on

constexpr int64_t kAllocatorCapacity = 4L << 30;

constexpr std::array<int32_t, 128> kAllocationSizes = {
    10, 13, 22, 17, 28, 19, 11, 30, 25, 14, 16, 21, 12, 29, 18, 23, 15, 20, 27,
    10, 24, 26, 13, 18, 22, 11, 30, 17, 19, 28, 14, 16, 21, 25, 12, 29, 15, 23,
    20, 27, 10, 18, 24, 13, 26, 16, 22, 11, 30, 19, 28, 14, 17, 21, 25, 12, 29,
    15, 23, 20, 27, 10, 18, 24, 13, 26, 16, 22, 11, 30, 19, 28, 14, 17, 21, 25,
    12, 29, 15, 23, 20, 27, 10, 18, 24, 13, 26, 16, 22, 11, 30, 19, 28, 14, 17,
    21, 25, 12, 29, 15, 23, 20, 27, 10, 18, 24, 13, 26, 16, 22, 11, 30, 19, 28,
    14, 17, 21, 25, 12, 29, 15, 23, 20, 27, 10, 18, 24, 13};

static_assert(kAllocationSizes.size() == 128);

struct BenchmarkStats {
  int64_t allocations{0};
  int64_t requestedBytes{0};
  int64_t poolPeakBytes{0};
  int64_t resourceReservedBytes{0};
};

class AllocatorCompareBenchmark {
 public:
  AllocatorCompareBenchmark() {
    MemoryManager::Options options;
    options.allocatorCapacity = kAllocatorCapacity;
    options.arbitratorCapacity = kAllocatorCapacity;
    options.useMmapAllocator = false;
    manager_ = std::make_unique<MemoryManager>(options);
    pool_ = manager_->addLeafPool("hash_string_monotonic_allocator_benchmark");
  }

  BenchmarkStats runHashStringAllocator() {
    HashStringAllocator allocator(pool_.get());

    int64_t requestedBytes = 0;
    int64_t allocations = 0;
    for (int64_t i = 0; requestedBytes < FLAGS_allocator_compare_payload_bytes;
         ++i) {
      const auto size = nextSize(i);
      auto* header = allocator.allocate(size);
      auto* p = header->begin();
      p[size - 1] = '\0';
      folly::doNotOptimizeAway(p);
      requestedBytes += size;
      ++allocations;
    }

    return BenchmarkStats{
        .allocations = allocations,
        .requestedBytes = requestedBytes,
        .poolPeakBytes = static_cast<int64_t>(pool_->stats().peakBytes),
    };
  }

  BenchmarkStats runMonotonicMemoryResource() {
    MonotonicMemoryResource resource(pool_.get());
    MonotonicAllocator<char> allocator(resource);

    int64_t requestedBytes = 0;
    int64_t allocations = 0;
    for (int64_t i = 0; requestedBytes < FLAGS_allocator_compare_payload_bytes;
         ++i) {
      const auto size = nextSize(i);
      auto* p = allocator.allocate(size);
      p[size - 1] = '\0';
      folly::doNotOptimizeAway(p);
      requestedBytes += size;
      ++allocations;
    }

    return BenchmarkStats{
        .allocations = allocations,
        .requestedBytes = requestedBytes,
        .poolPeakBytes = static_cast<int64_t>(pool_->stats().peakBytes),
        .resourceReservedBytes = static_cast<int64_t>(resource.reservedBytes()),
    };
  }

 private:
  static int32_t nextSize(int64_t index) {
    return kAllocationSizes[index % kAllocationSizes.size()];
  }

  std::unique_ptr<MemoryManager> manager_;
  std::shared_ptr<MemoryPool> pool_;
};

void printStats(const char* name, const BenchmarkStats& stats) {
  fmt::print(
      "{} allocations={} requestedBytes={} poolPeakBytes={} resourceReservedBytes={}\n",
      name,
      stats.allocations,
      stats.requestedBytes,
      stats.poolPeakBytes,
      stats.resourceReservedBytes);
}

BENCHMARK(HashStringAllocatorAllocateNoFree) {
  AllocatorCompareBenchmark benchmark;
  printStats("HashStringAllocator", benchmark.runHashStringAllocator());
}

BENCHMARK_RELATIVE(MonotonicMemoryResourceAllocateNoFree) {
  AllocatorCompareBenchmark benchmark;
  printStats("MonotonicMemoryResource", benchmark.runMonotonicMemoryResource());
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
