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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <random>
#include <string>

#include "bolt/exec/OperatorUtils.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory;

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
DEFINE_int64(
    bolt_benchmark_multiple_vector_count,
    10,
    "different vector count to gather");
static constexpr int32_t kRowsPerVector = 10'000;

struct GatherCopyParam {
  GatherCopyParam(TypePtr type, int32_t batchSize = kRowsPerVector)
      : pool(memory::memoryManager()->addLeafPool()),
        name(type->toString()),
        type(type),
        batchSize(batchSize),
        result(
            BaseVector::create<RowVector>(ROW({type}), batchSize, pool.get())) {
    indices.resize(batchSize);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937 g(42);
    std::shuffle(indices.begin(), indices.end(), g);
    VectorFuzzer::Options opts;
    opts.vectorSize = batchSize;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool.get(), FLAGS_bolt_benchmark_fuzzer_seed);
    for (int32_t i = 0; i < FLAGS_bolt_benchmark_multiple_vector_count; i++) {
      inputsHolder.push_back(fuzzer.fuzzFlat(ROW({type})));
    }
    for (int32_t i = 0; i < batchSize; i++) {
      inputs.push_back(
          inputsHolder[rand() % inputsHolder.size()]->as<RowVector>());
    }
  }

  std::shared_ptr<MemoryPool> pool;
  std::string name;
  TypePtr type;
  int32_t batchSize;
  std::vector<const RowVector*> inputs;
  std::vector<VectorPtr> inputsHolder;
  std::vector<vector_size_t> indices;
  RowVectorPtr result;
};

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  std::vector<TypePtr> types = {
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      ROW({BIGINT(), BIGINT()}),
      ARRAY(BIGINT()),
      MAP(VARCHAR(), VARCHAR())};

  std::vector<GatherCopyParam> params;
  for (auto type : types) {
    params.emplace_back(type);
  }

  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.name, [&param]() {
      for (int i = 0; i < 10000; i++) {
        gatherCopy(
            param.result.get(),
            0,
            param.batchSize,
            param.inputs,
            param.indices);
      }
      return 1;
    });
  }
  folly::runBenchmarks();
  return 0;
}
