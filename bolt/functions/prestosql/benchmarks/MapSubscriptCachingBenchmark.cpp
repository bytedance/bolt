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

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions;
namespace bytedance::bolt::functions {
extern void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching);
}

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  bytedance::bolt::functions::prestosql::registerAllScalarFunctions();
  bytedance::bolt::functions::registerSubscriptFunction(
      "subscriptNocaching", false);

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  auto createSet = [&](const TypePtr& mapType,
                       size_t mapLength,
                       size_t baseVectorSize,
                       size_t numberOfBatches = 1000) {
    VectorFuzzer::Options options;
    options.vectorSize = 1000;
    options.containerLength = mapLength;
    options.complexElementsMaxSize = 10000000000;
    options.containerVariableLength = false;

    VectorFuzzer fuzzer(options, pool);
    std::vector<VectorPtr> columns;

    // Fuzz input map vector.
    auto flatBase = fuzzer.fuzzFlat(mapType, baseVectorSize);
    auto dictionary = fuzzer.fuzzDictionary(flatBase, options.vectorSize);

    columns.push_back(dictionary);

    // Fuzz indices vector.
    DecodedVector decoded(*dictionary);
    auto* map = flatBase->as<MapVector>();
    auto indices = allocateIndices(options.vectorSize, pool);
    auto* mutableIndices = indices->asMutable<vector_size_t>();

    for (int i = 0; i < options.vectorSize; i++) {
      auto mapIndex = decoded.index(i);
      // Select a random existing key except when map is empty.
      if (map->sizeAt(mapIndex) == 0) {
        mutableIndices[i] = 0;
      }
      mutableIndices[i] = folly::Random::rand32() % map->sizeAt(mapIndex) +
          map->offsetAt(mapIndex);
    }

    auto indicesVector = BaseVector::wrapInDictionary(
        nullptr, indices, options.vectorSize, map->mapKeys());

    columns.push_back(indicesVector);

    auto name = fmt::format(
        "{}_{}_{}", mapType->childAt(0)->toString(), mapLength, baseVectorSize);

    benchmarkBuilder.addBenchmarkSet(name, vm.rowVector(columns))
        .addExpression("subscript", "element_at(c0, c1)")
        .addExpression("subscriptNocaching", "subscriptNocaching(c0, c1)")
        .withIterations(numberOfBatches);
  };

  auto createSetsForType = [&](const auto& keyType) {
    createSet(MAP(keyType, INTEGER()), 10, 1);
    createSet(MAP(keyType, INTEGER()), 10, 1000);

    createSet(MAP(keyType, INTEGER()), 100, 1);
    createSet(MAP(keyType, INTEGER()), 100, 1000);

    createSet(MAP(keyType, INTEGER()), 1000, 1);
    createSet(MAP(keyType, INTEGER()), 1000, 1000);

    createSet(MAP(keyType, INTEGER()), 10000, 1);
    createSet(MAP(keyType, INTEGER()), 10000, 1000);
  };

  createSetsForType(INTEGER());
  createSetsForType(VARCHAR());

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();
  return 0;
}
