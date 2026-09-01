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

#include <folly/futures/Future.h>
#include <limits>

#include "bolt/common/memory/Memory.h"
#include "folly/Benchmark.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/init/Init.h"
using namespace bytedance::bolt::memory;

constexpr int64_t kMemoryFootprintIncrement = MemoryAllocator::kMaxAlignment;
namespace bytedance::bolt::memory {

namespace {
MemoryPool& systemRootPool(MemoryManager& manager) {
  return *manager.spillPool()->parent();
}
} // namespace

class BenchmarkHelper {
 public:
  explicit BenchmarkHelper(MemoryManager& manager)
      : manager_{manager}, leaves_{findLeaves()} {}

  std::vector<MemoryPool*> findLeaves() {
    std::vector<MemoryPool*> leaves;
    findLeavesOf(systemRootPool(manager_), leaves);
    return leaves;
  }

  void findLeavesOf(MemoryPool& subtree, std::vector<MemoryPool*>& leaves) {
    if (subtree.getChildCount() == 0) {
      leaves.push_back(&subtree);
      return;
    }

    subtree.visitChildren([this, &leaves](MemoryPool* child) {
      findLeavesOf(*child, leaves);
      return true;
    });
  }

  // Always in parallel, single pool performance can be tested with
  // depth-2 stick.
  void runForEachPool(std::function<void(MemoryPool&)> allocSequence) {
    folly::CPUThreadPoolExecutor threadPool(leaves_.size());
    for (auto& pool : leaves_) {
      threadPool.add([&]() { allocSequence(*pool); });
    }

    threadPool.join();
  }

 private:
  // TODO: move semantic is better
  MemoryManager& manager_;
  std::vector<MemoryPool*> leaves_;
};
} // namespace bytedance::bolt::memory

BENCHMARK(SingleNodeAlloc, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto pool = manager.addRootPool("pride_of_higara");

  for (size_t i = 0; i < iters; ++i) {
    auto dataSize = (i + 1) * kMemoryFootprintIncrement;
    suspender.dismiss();
    auto p = pool->allocate(dataSize);
    suspender.rehire();
    pool->free(p, dataSize);
  }
}

BENCHMARK(SingleNodeFree, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto pool = manager.addRootPool("pride_of_higara");

  for (size_t i = 0; i < iters; ++i) {
    auto dataSize = (i + 1) * kMemoryFootprintIncrement;
    auto p = pool->allocate(dataSize);
    suspender.dismiss();
    pool->free(p, dataSize);
    suspender.rehire();
  }
}

BENCHMARK(SingleNodeRealloc, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto pool = manager.addRootPool("pride_of_higara");

  void* p = pool->allocate(kMemoryFootprintIncrement);
  for (size_t i = 0; i < iters; ++i) {
    suspender.dismiss();
    p = pool->reallocate(
        p,
        (i + 1) * kMemoryFootprintIncrement,
        (i + 2) * kMemoryFootprintIncrement);
    suspender.rehire();
  }
  pool->free(p, (iters + 1) * kMemoryFootprintIncrement);
}

BENCHMARK(SingleNodeAlignedAlloc, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto pool = manager.addRootPool("pride_of_higara");

  for (size_t i = 0; i < iters; ++i) {
    auto dataSize = (i + 1) * kMemoryFootprintIncrement;
    suspender.dismiss();
    auto p = pool->allocate(dataSize);
    suspender.rehire();
    pool->free(p, dataSize);
  }
}

BENCHMARK(SingleNodeAlignedRealloc, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto pool = manager.addRootPool("pride_of_higara");

  void* p = pool->allocate(kMemoryFootprintIncrement);
  for (size_t i = 0; i < iters; ++i) {
    suspender.dismiss();
    p = pool->reallocate(
        p,
        (i + 1) * kMemoryFootprintIncrement,
        (i + 2) * kMemoryFootprintIncrement);
    suspender.rehire();
  }
  pool->free(p, (iters + 1) * kMemoryFootprintIncrement);
}

namespace {
void addNLeaves(MemoryPool& pool, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    pool.addLeafChild(pool.name() + ".leaf_" + folly::to<std::string>(i));
  }
}
} // namespace

// No periodic aggregation, should have similar perf
// with single leaf runs.
BENCHMARK(FlatTree, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  addNLeaves(systemRootPool(manager), 10 * 20);
  BenchmarkHelper helper{manager};
  suspender.dismiss();
  helper.runForEachPool([iters](MemoryPool& pool) {
    void* p = pool.allocate(kMemoryFootprintIncrement);
    for (size_t i = 1; i < iters; ++i) {
      pool.reallocate(
          p,
          i * kMemoryFootprintIncrement,
          (i + 1) * kMemoryFootprintIncrement);
    }
    pool.free(p, iters * kMemoryFootprintIncrement);
  });
}

// No periodic aggregation, should have similar perf
// with single leaf runs.
BENCHMARK(DeeperTree, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  for (size_t i = 0; i < 10; ++i) {
    auto child =
        manager.addRootPool("query_fragment_" + folly::to<std::string>(i));
    addNLeaves(*child, 20);
  }
  BenchmarkHelper helper{manager};
  suspender.dismiss();
  helper.runForEachPool([iters](MemoryPool& pool) {
    void* p = pool.allocate(kMemoryFootprintIncrement);
    for (size_t i = 1; i < iters; ++i) {
      pool.reallocate(
          p,
          i * kMemoryFootprintIncrement,
          (i + 1) * kMemoryFootprintIncrement);
    }
    pool.free(p, iters * kMemoryFootprintIncrement);
  });
}

BENCHMARK(FlatSubtree, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  auto child = manager.addRootPool("query_fragment_1");
  addNLeaves(*child, 10 * 20);
  BenchmarkHelper helper{manager};
  suspender.dismiss();
  helper.runForEachPool([iters](MemoryPool& pool) {
    void* p = pool.allocate(kMemoryFootprintIncrement);
    for (size_t i = 1; i < iters; ++i) {
      pool.reallocate(
          p,
          i * kMemoryFootprintIncrement,
          (i + 1) * kMemoryFootprintIncrement);
    }
    pool.free(p, iters * kMemoryFootprintIncrement);
  });
}

BENCHMARK(FlatSticks, iters) {
  folly::BenchmarkSuspender suspender;
  MemoryManager manager{};
  for (size_t i = 0; i < 10 * 20; ++i) {
    auto child =
        manager.addRootPool("query_fragment_" + folly::to<std::string>(i));
    addNLeaves(*child, 1);
  }
  BenchmarkHelper helper{manager};
  suspender.dismiss();
  helper.runForEachPool([iters](MemoryPool& pool) {
    void* p = pool.allocate(kMemoryFootprintIncrement);
    for (size_t i = 1; i < iters; ++i) {
      pool.reallocate(
          p,
          i * kMemoryFootprintIncrement,
          (i + 1) * kMemoryFootprintIncrement);
    }
    pool.free(p, iters * kMemoryFootprintIncrement);
  });
}

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
