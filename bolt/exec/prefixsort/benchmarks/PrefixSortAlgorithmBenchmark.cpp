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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "bolt/buffer/Buffer.h"
#include "bolt/exec/prefixsort/PrefixSortAlgorithm.h"
#include "bolt/exec/prefixsort/tests/utils/EncoderTestUtils.h"

DEFINE_int32(
    bolt_benchmark_sort_data_seed,
    1,
    "random test data generate seed.");
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

class PrefixSortAlgorithmBenchmark {
 public:
  void seed(int32_t seed) {
    rng_.seed(seed);
  }

  void runQuickSort(std::vector<int64_t> vec) {
    char* start = (char*)vec.data();
    uint32_t entrySize = sizeof(int64_t);
    auto swapBuffer = AlignedBuffer::allocate<char>(entrySize, pool_.get());
    auto sortRunner =
        prefixsort::PrefixSortRunner(entrySize, swapBuffer->asMutable<char>());
    sortRunner.quickSort(
        start, start + entrySize * vec.size(), [&](char* a, char* b) {
          return memcmp(a, b, 8);
        });
  }

  std::vector<int64_t> generateTestVector(int32_t size) {
    std::vector<int64_t> randomTestVec(size);
    std::generate(randomTestVec.begin(), randomTestVec.end(), [&]() {
      return folly::Random::rand64(rng_);
    });
    prefixsort::test::encodeInPlace(randomTestVec);
    return randomTestVec;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  folly::Random::DefaultGenerator rng_;
};

std::unique_ptr<PrefixSortAlgorithmBenchmark> bm;

std::vector<int64_t> data10k;
std::vector<int64_t> data100k;
std::vector<int64_t> data1000k;
std::vector<int64_t> data10000k;

BENCHMARK(PrefixSort_algorithm_10k) {
  bm->runQuickSort(data10k);
}

BENCHMARK(PrefixSort_algorithm_100k) {
  bm->runQuickSort(data100k);
}

BENCHMARK(PrefixSort_algorithm_1000k) {
  bm->runQuickSort(data1000k);
}

BENCHMARK(PrefixSort_algorithm_10000k) {
  bm->runQuickSort(data10000k);
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  bm = std::make_unique<PrefixSortAlgorithmBenchmark>();
  bm->seed(FLAGS_bolt_benchmark_sort_data_seed);
  data10k = bm->generateTestVector(10'000);
  data100k = bm->generateTestVector(100'000);
  data1000k = bm->generateTestVector(1'000'000);
  data10000k = bm->generateTestVector(10'000'000);
  folly::runBenchmarks();
  return 0;
}
