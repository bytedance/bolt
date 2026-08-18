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

#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_int64(
    bolt_benchmark_fuzzer_seed,
    99887766,
    "Seed for random input dataset generator");
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;

static constexpr int32_t kRowsPerVector = 100'000;
static constexpr vector_size_t kBatchRows = 8'192;
static constexpr size_t kWideBatchColumns = 1'700;

namespace {

// Boiler plate structures required by vectorMaker.
memory::MemoryManager memoryManager;
std::shared_ptr<memory::MemoryPool> pool_{memoryManager.addLeafPool()};
bytedance::bolt::test::VectorMaker vectorMaker_{pool_.get()};

void runExportToArrow(
    uint32_t iterations,
    VectorPtr vec,
    ArrowOptions options = ArrowOptions{}) {
  for (uint32_t i = 0; i < iterations; ++i) {
    ArrowArray arrowArray{};
    exportToArrow(vec, arrowArray, pool_.get(), options);
    folly::doNotOptimizeAway(arrowArray.buffers);
    arrowArray.release(&arrowArray);
  }
}

void runExportArrowBatch(uint32_t iterations, const VectorPtr& vector) {
  for (uint32_t i = 0; i < iterations; ++i) {
    ArrowSchema schema{};
    ArrowArray array{};
    exportToArrow(vector, schema, ArrowOptions{}, {}, pool_.get());
    exportToArrow(vector, array, pool_.get(), ArrowOptions{});
    folly::doNotOptimizeAway(schema.format);
    folly::doNotOptimizeAway(array.buffers);
    array.release(&array);
    schema.release(&schema);
  }
}

void runExportReusableArrowBatch(
    uint32_t iterations,
    const VectorPtr& vector,
    ReusableArrowBatchPool* batchPool) {
  for (uint32_t i = 0; i < iterations; ++i) {
    ArrowSchema schema{};
    ArrowArray array{};
    const auto schemaExported = batchPool->exportToArrow(
        vector, pool_.get(), ArrowOptions{}, &schema, &array);
    folly::doNotOptimizeAway(schemaExported);
    folly::doNotOptimizeAway(schema.format);
    folly::doNotOptimizeAway(array.buffers);
    array.release(&array);
    schema.release(&schema);
  }
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

VectorPtr narrowBatch;
VectorPtr nestedBatch;
VectorPtr wideBatch;
std::unique_ptr<ReusableArrowBatchPool> narrowBatchPool;
std::unique_ptr<ReusableArrowBatchPool> nestedBatchPool;
std::unique_ptr<ReusableArrowBatchPool> wideBatchPool;

void warmUpBatchPool(
    const VectorPtr& vector,
    ReusableArrowBatchPool& batchPool) {
  ArrowSchema schema{};
  ArrowArray array{};
  batchPool.exportToArrow(vector, pool_.get(), ArrowOptions{}, &schema, &array);
  array.release(&array);
  schema.release(&schema);
}

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

  narrowBatch = vectorMaker_.rowVector({
      integerVec->slice(0, kBatchRows),
      bigintVec->slice(0, kBatchRows),
      realVec->slice(0, kBatchRows),
      doubleVec->slice(0, kBatchRows),
      varcharVec->slice(0, kBatchRows),
      integerVecHalfNull->slice(0, kBatchRows),
      bigintVecHalfNull->slice(0, kBatchRows),
      varcharVecHalfNull->slice(0, kBatchRows),
  });
  nestedBatch = complexVec->slice(0, kBatchRows);
  // Share data vectors so this case isolates per-column Arrow metadata costs.
  wideBatch = vectorMaker_.rowVector(std::vector<VectorPtr>(
      kWideBatchColumns, integerVec->slice(0, kBatchRows)));

  narrowBatchPool = std::make_unique<ReusableArrowBatchPool>(1);
  nestedBatchPool = std::make_unique<ReusableArrowBatchPool>(1);
  wideBatchPool = std::make_unique<ReusableArrowBatchPool>(1);
  warmUpBatchPool(narrowBatch, *narrowBatchPool);
  warmUpBatchPool(nestedBatch, *nestedBatchPool);
  warmUpBatchPool(wideBatch, *wideBatchPool);
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

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(runExportArrowBatch, narrow8x8192, narrowBatch);
BENCHMARK_NAMED_PARAM(
    runExportReusableArrowBatch,
    narrow8x8192,
    narrowBatch,
    narrowBatchPool.get());

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(runExportArrowBatch, nested4x8192, nestedBatch);
BENCHMARK_NAMED_PARAM(
    runExportReusableArrowBatch,
    nested4x8192,
    nestedBatch,
    nestedBatchPool.get());

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(runExportArrowBatch, wide1700x8192, wideBatch);
BENCHMARK_NAMED_PARAM(
    runExportReusableArrowBatch,
    wide1700x8192,
    wideBatch,
    wideBatchPool.get());

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  createVectors();
  folly::runBenchmarks();
  return 0;
}
