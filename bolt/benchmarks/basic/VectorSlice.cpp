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

#include <gflags/gflags.h>

#include "bolt/common/flags/BoltFlags.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    1,
    "Seed for random input dataset generator");
DEFINE_int32(bolt_benchmark_slice_size, 512, "Slice size");
DEFINE_bool(
    bolt_benchmark_use_thread_safe_memory_usage_track,
    false,
    "If true, use thread safe memory usage tracking");
namespace bytedance::bolt {
namespace {

constexpr int kVectorSize = 16 << 10;

struct BenchmarkData {
  BenchmarkData()
      : pool_(memory::memoryManager()->addLeafPool(
            "BenchmarkData",
            FLAGS_bolt_benchmark_use_thread_safe_memory_usage_track)) {
    VectorFuzzer::Options opts;
    opts.nullRatio = 0.01;
    opts.vectorSize = kVectorSize;
    VectorFuzzer fuzzer(opts, pool_.get(), FLAGS_bolt_benchmark_fuzzer_seed);
    flatVector = fuzzer.fuzzFlat(BIGINT());
    arrayVector = fuzzer.fuzzFlat(ARRAY(BIGINT()));
    mapVector = fuzzer.fuzzFlat(MAP(BIGINT(), BIGINT()));
    rowVector = fuzzer.fuzzFlat(ROW({BIGINT(), BIGINT(), BIGINT()}));
  }

  ~BenchmarkData() {
    flatVector.reset();
    arrayVector.reset();
    mapVector.reset();
    rowVector.reset();
  }

  VectorPtr flatVector;
  VectorPtr arrayVector;
  VectorPtr mapVector;
  VectorPtr rowVector;

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
};

std::unique_ptr<BenchmarkData> data;

int runSlice(const BaseVector& vec, vector_size_t offset) {
  int count = 0;
  for (int i = offset; i + FLAGS_bolt_benchmark_slice_size < vec.size();
       i += FLAGS_bolt_benchmark_slice_size) {
    auto slice = vec.slice(i, FLAGS_bolt_benchmark_slice_size);
    folly::doNotOptimizeAway(slice);
    ++count;
  }
  return count;
}

int runCopy(const BaseVector& vec) {
  int count = 0;
  for (int i = 0; i + FLAGS_bolt_benchmark_slice_size < vec.size();
       i += FLAGS_bolt_benchmark_slice_size) {
    auto copy = BaseVector::create(
        vec.type(), FLAGS_bolt_benchmark_slice_size, vec.pool());
    copy->copy(&vec, 0, i, FLAGS_bolt_benchmark_slice_size);
    folly::doNotOptimizeAway(copy);
    ++count;
  }
  return count;
}

#define DEFINE_BENCHMARKS(name)                    \
  BENCHMARK_MULTI(name##Slice) {                   \
    return runSlice(*data->name##Vector, 0);       \
  }                                                \
  BENCHMARK_RELATIVE_MULTI(name##SliceUnaligned) { \
    return runSlice(*data->name##Vector, 1);       \
  }                                                \
  BENCHMARK_RELATIVE_MULTI(name##Copy) {           \
    return runCopy(*data->name##Vector);           \
  }

DEFINE_BENCHMARKS(flat)
DEFINE_BENCHMARKS(array)
DEFINE_BENCHMARKS(map)
DEFINE_BENCHMARKS(row)

} // namespace
} // namespace bytedance::bolt

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  using namespace bytedance::bolt;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  BOLT_CHECK_LE(FLAGS_bolt_benchmark_slice_size, kVectorSize);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  data = std::make_unique<BenchmarkData>();
  folly::runBenchmarks();
  data.reset();
  return 0;
}
