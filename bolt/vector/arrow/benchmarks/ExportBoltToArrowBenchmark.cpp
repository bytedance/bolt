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
#include <string>

#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kRowsPerVector = 100'000;

namespace {

// Boiler plate structures required by vectorMaker.
memory::MemoryManager memoryManager;
std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
std::shared_ptr<memory::MemoryPool> pool_{memoryManager.addLeafPool()};
core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
bytedance::bolt::test::VectorMaker vectorMaker_{execCtx_.pool()};

void runExportToArrow(
    uint32_t,
    VectorPtr vec,
    ArrowOptions options = ArrowOptions{}) {
  ArrowArray arrowArray;
  exportToArrow(vec, arrowArray, pool_.get(), options);
}

RowTypePtr complexType = ROW({
    {"array", ARRAY(REAL())},
    {"map", MAP(INTEGER(), DOUBLE())},
    {"row",
     ROW({
         {"a", REAL()},
         {"b", INTEGER()},
     })},
    {"nested",
     ARRAY(ROW({
         {"a", INTEGER()},
         {"b", MAP(REAL(), REAL())},
     }))},
});

VectorPtr integerVec;
VectorPtr bigintVec;
VectorPtr realVec;
VectorPtr doubleVec;
VectorPtr varcharVec;
VectorPtr varchar2Vec;
VectorPtr complexVec;
ArrowOptions options{.exportToView = true};

VectorPtr integerVecHalfNull;
VectorPtr bigintVecHalfNull;
VectorPtr realVecHalfNull;
VectorPtr doubleVecHalfNull;
VectorPtr varcharVecHalfNull;
VectorPtr varchar2VecHalfNull;
VectorPtr complexVecHalfNull;

void createVectors() {
  VectorFuzzer::Options opts;
  opts.vectorSize = kRowsPerVector;
  opts.nullRatio = 0;
  opts.stringLength = 50;
  opts.stringVariableLength = true;
  VectorFuzzer fuzzer(opts, pool_.get(), FLAGS_bolt_benchmark_fuzzer_seed);

  integerVec = fuzzer.fuzzFlat(INTEGER());
  bigintVec = fuzzer.fuzzFlat(BIGINT());
  realVec = fuzzer.fuzzFlat(REAL());
  doubleVec = fuzzer.fuzzFlat(DOUBLE());
  varcharVec = fuzzer.fuzzFlat(VARCHAR());
  varchar2Vec = fuzzer.fuzzFlat(VARCHAR());
  complexVec = fuzzer.fuzzFlat(complexType);

  // Generate random values with nulls.
  opts.nullRatio = 0.5; // 50%
  fuzzer.setOptions(opts);

  integerVecHalfNull = fuzzer.fuzzFlat(INTEGER());
  bigintVecHalfNull = fuzzer.fuzzFlat(BIGINT());
  realVecHalfNull = fuzzer.fuzzFlat(REAL());
  doubleVecHalfNull = fuzzer.fuzzFlat(DOUBLE());
  varcharVecHalfNull = fuzzer.fuzzFlat(VARCHAR());
  varchar2VecHalfNull = fuzzer.fuzzFlat(VARCHAR());
  complexVecHalfNull = fuzzer.fuzzFlat(complexType);
}

BENCHMARK_NAMED_PARAM(runExportToArrow, integer, integerVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, bigint, bigintVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, real, realVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, double_, doubleVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, utf8, varcharVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, complex, complexVec);
BENCHMARK_NAMED_PARAM(runExportToArrow, utf8view, varchar2Vec, options);

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(runExportToArrow, integerHalfNull, integerVecHalfNull);
BENCHMARK_NAMED_PARAM(runExportToArrow, bigintHalfNull, bigintVecHalfNull);
BENCHMARK_NAMED_PARAM(runExportToArrow, realHalfNull, realVecHalfNull);
BENCHMARK_NAMED_PARAM(runExportToArrow, doubleHalfNull, doubleVecHalfNull);
BENCHMARK_NAMED_PARAM(runExportToArrow, utf8HalfNull, varcharVecHalfNull);
BENCHMARK_NAMED_PARAM(runExportToArrow, complexHalfNull, complexVecHalfNull);
BENCHMARK_NAMED_PARAM(
    runExportToArrow,
    utf8viewHalfNull,
    varchar2VecHalfNull,
    options);

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  createVectors();
  folly::runBenchmarks();
  return 0;
}
