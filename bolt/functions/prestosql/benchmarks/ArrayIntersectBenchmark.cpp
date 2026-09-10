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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

using namespace bytedance::bolt;

namespace {

RowVectorPtr makeBigintArrayInput(
    test::VectorMaker& vectorMaker,
    vector_size_t rowCount,
    vector_size_t leftSize,
    vector_size_t rightSize,
    int64_t distinctValues) {
  auto left = vectorMaker.arrayVector<int64_t>(
      rowCount,
      [leftSize](vector_size_t /*row*/) { return leftSize; },
      [distinctValues](vector_size_t row, vector_size_t index) {
        return (row * 17 + index) % distinctValues;
      });

  auto right = vectorMaker.arrayVector<int64_t>(
      rowCount,
      [rightSize](vector_size_t /*row*/) { return rightSize; },
      [distinctValues](vector_size_t row, vector_size_t index) {
        return (row * 17 + index * 3 + 7) % distinctValues;
      });

  return vectorMaker.rowVector({left, right});
}

RowVectorPtr makeConstantRightInput(
    test::VectorMaker& vectorMaker,
    vector_size_t rowCount,
    vector_size_t leftSize,
    vector_size_t rightSize,
    int64_t distinctValues) {
  auto left = vectorMaker.arrayVector<int64_t>(
      rowCount,
      [leftSize](vector_size_t /*row*/) { return leftSize; },
      [distinctValues](vector_size_t row, vector_size_t index) {
        return (row * 11 + index) % distinctValues;
      });

  std::vector<int64_t> values;
  values.reserve(rightSize);
  for (vector_size_t i = 0; i < rightSize; ++i) {
    values.push_back((i * 5 + 3) % distinctValues);
  }
  auto constantRight = BaseVector::wrapInConstant(
      rowCount, 0, vectorMaker.arrayVector<int64_t>({values}));

  return vectorMaker.rowVector({left, constantRight});
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerArrayFunctions();

  ExpressionBenchmarkBuilder benchmarkBuilder;

  benchmarkBuilder
      .addBenchmarkSet(
          "array_intersect_bigint_128x64",
          makeBigintArrayInput(
              benchmarkBuilder.vectorMaker(),
              1'000,
              /*leftSize=*/128,
              /*rightSize=*/64,
              /*distinctValues=*/256))
      .withIterations(100)
      .addExpression("vector", "array_intersect(c0, c1)");

  benchmarkBuilder
      .addBenchmarkSet(
          "array_intersect_bigint_32x16",
          makeBigintArrayInput(
              benchmarkBuilder.vectorMaker(),
              2'000,
              /*leftSize=*/32,
              /*rightSize=*/16,
              /*distinctValues=*/128))
      .withIterations(300)
      .addExpression("vector", "array_intersect(c0, c1)");

  benchmarkBuilder
      .addBenchmarkSet(
          "array_intersect_bigint_encoded_const_rhs",
          makeConstantRightInput(
              benchmarkBuilder.vectorMaker(),
              1'000,
              /*leftSize=*/128,
              /*rightSize=*/64,
              /*distinctValues=*/256))
      .withIterations(100)
      .addExpression("vector", "array_intersect(c0, c1)");

  benchmarkBuilder
      .addBenchmarkSet(
          "array_intersect_bigint_literal_const_rhs",
          ROW({"c0"}, {ARRAY(BIGINT())}))
      .withFuzzerOptions({.vectorSize = 1'000, .nullRatio = 0})
      .withIterations(100)
      .addExpression(
          "vector",
          "array_intersect(c0, ARRAY[3,8,13,18,23,28,33,38,43,48,53,58,63,68,73,78])");

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();
  return 0;
}
