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

#include "bolt/common/memory/Memory.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");

namespace {
using namespace bytedance::bolt;

memory::MemoryPool* pool() {
  static auto leaf = memory::MemoryManager::getInstance()->addLeafPool();
  return leaf.get();
}

VectorFuzzer::Options getOpts(size_t n, double nullRatio = 0) {
  VectorFuzzer::Options opts;
  opts.vectorSize = n;
  opts.nullRatio = nullRatio;
  return opts;
}

BENCHMARK_MULTI(flatInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatIntegerHalfNull, n) {
  VectorFuzzer fuzzer(
      getOpts(n, 0.5), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatDouble, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(DOUBLE()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatBool, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BOOLEAN()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatVarcharAscii, n) {
  auto opts = getOpts(n);
  opts.charEncodings = {UTF8CharList::ASCII};

  VectorFuzzer fuzzer(opts, pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(VARCHAR()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatVarcharUtf8, n) {
  auto opts = getOpts(n);
  opts.charEncodings = {UTF8CharList::EXTENDED_UNICODE};

  VectorFuzzer fuzzer(opts, pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(VARCHAR()));
  return n;
}

BENCHMARK_DRAW_LINE();

BENCHMARK_RELATIVE_MULTI(constantInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzConstant(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(dictionaryInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzDictionary(fuzzer.fuzzFlat(BIGINT())));
  return n;
}

BENCHMARK_DRAW_LINE();

BENCHMARK_RELATIVE_MULTI(flatArray, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;
  folly::doNotOptimizeAway(
      fuzzer.fuzzArray(fuzzer.fuzzFlat(BIGINT(), elementsSize), n));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatMap, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;
  folly::doNotOptimizeAway(fuzzer.fuzzMap(
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      n));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatMapArrayNested, n) {
  VectorFuzzer fuzzer(getOpts(n), pool(), FLAGS_bolt_benchmark_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;

  folly::doNotOptimizeAway(fuzzer.fuzzMap(
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      fuzzer.fuzzArray(
          fuzzer.fuzzFlat(BIGINT(), elementsSize * 10), elementsSize),
      n));
  return n;
}

} // namespace

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
