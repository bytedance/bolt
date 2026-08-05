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

#include "bolt/common/base/CompareFlags.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::test;

namespace {

class VectorCompareBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit VectorCompareBenchmark(size_t vectorSize)
      : FunctionBenchmarkBase(), vectorSize_(vectorSize), rows_(vectorSize) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.nullRatio = 0;
    opts.containerVariableLength = 1000;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_bolt_benchmark_fuzzer_seed);

    flatVector_ = fuzzer.fuzzFlat(BIGINT());

    arrayVector_ = fuzzer.fuzzFlat(ARRAY(BIGINT()));

    mapVector_ = fuzzer.fuzzFlat(MAP(BIGINT(), BIGINT()));

    rowVector_ = fuzzer.fuzzFlat(ROW({BIGINT(), BIGINT(), BIGINT()}));
  }

  size_t run(const VectorPtr& vector) {
    size_t sum = 0;

    for (auto i = 0; i < vectorSize_; i++) {
      sum += *vector->compare(vector.get(), i, i, kFlags);
    }
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  // Avoid dynamic dispatch by casting the vector before calling compare to its
  // derived that have final compare function.
  size_t runFastFlat() {
    size_t sum = 0;
    auto flatVector = flatVector_->as<FlatVector<int64_t>>();
    for (auto i = 0; i < vectorSize_; i++) {
      sum += *flatVector->compare(flatVector, i, vectorSize_ - i - 1, kFlags);
    }
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  VectorPtr flatVector_;
  VectorPtr arrayVector_;
  VectorPtr mapVector_;
  VectorPtr rowVector_;

 private:
  static constexpr CompareFlags kFlags{
      true,
      true,
      false,
      CompareFlags::NullHandlingMode::kNullAsValue};

  const size_t vectorSize_;
  SelectivityVector rows_;
};

std::unique_ptr<VectorCompareBenchmark> benchmark;

BENCHMARK(compareSimilarSimpleFlat) {
  benchmark->run(benchmark->flatVector_);
}

BENCHMARK(compareSimilarSimpleFlatNoDispatch) {
  benchmark->runFastFlat();
}

BENCHMARK(compareSimilarArray) {
  benchmark->run(benchmark->arrayVector_);
}

BENCHMARK(compareSimilarMap) {
  benchmark->run(benchmark->mapVector_);
}

BENCHMARK(compareSimilarRow) {
  benchmark->run(benchmark->rowVector_);
}

BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  benchmark = std::make_unique<VectorCompareBenchmark>(1000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
