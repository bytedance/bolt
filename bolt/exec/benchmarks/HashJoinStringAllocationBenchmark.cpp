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

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/memory/Memory.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::test;

namespace {

constexpr vector_size_t kNumRows = 10'000'000;
constexpr vector_size_t kBatchSize = 10'000;
constexpr int32_t kNumColumns = 30;

class InnerJoinBenchmark : public VectorTestBase {
 public:
  InnerJoinBenchmark() {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(kNumColumns);
    types.reserve(kNumColumns);
    for (auto column = 0; column < kNumColumns; ++column) {
      names.push_back(fmt::format("c{}", column));
      types.push_back(VARCHAR());
    }
    rowType_ = ROW(std::move(names), std::move(types));
    makeData();
  }

  int64_t run() {
    auto joinPool = rootPool_->addLeafChild("hash_join_build");
    auto table = makeTable(joinPool.get());
    build(*table);
    table->prepareJoinTable({}, nullptr);
    const auto hits = probe(*table);
    BOLT_CHECK_EQ(hits, kNumRows);
    folly::doNotOptimizeAway(hits);
    return joinPool->peakBytes();
  }

 private:
  std::unique_ptr<HashTable<true>> makeTable(memory::MemoryPool* joinPool) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(VARCHAR(), 0));
    std::vector<TypePtr> dependentTypes(
        rowType_->children().begin() + 1, rowType_->children().end());
    return HashTable<true>::createForJoin(
        std::move(hashers),
        dependentTypes,
        false /*allowDuplicates*/,
        false /*hasProbedFlag*/,
        BaseHashTable::HashMode::kHash,
        0 /*minTableSizeForParallelJoinBuild*/,
        joinPool,
        false /*enableJitRowEqVectors*/,
        false /*hybridMode*/);
  }

  void makeData() {
    batches_.reserve(kNumRows / kBatchSize);
    for (vector_size_t batchStart = 0; batchStart < kNumRows;
         batchStart += kBatchSize) {
      std::vector<VectorPtr> children;
      children.reserve(kNumColumns);
      for (auto column = 0; column < kNumColumns; ++column) {
        const auto length = 20 + column % 11;
        children.push_back(makeFlatVector<std::string>(
            kBatchSize, [batchStart, column, length](auto row) {
              auto value =
                  fmt::format("{:010d}{:02d}", batchStart + row, column);
              value.append(length - value.size(), 'a' + column % 26);
              return value;
            }));
      }
      batches_.push_back(makeRowVector(rowType_->names(), children));
    }
  }

  void build(HashTable<true>& table) {
    auto* container = table.rows();
    std::vector<DecodedVector> decoded(kNumColumns);
    for (const auto& batch : batches_) {
      for (auto column = 0; column < kNumColumns; ++column) {
        decoded[column].decode(*batch->childAt(column));
      }
      for (auto row = 0; row < batch->size(); ++row) {
        auto* newRow = container->newRow();
        for (auto column = 0; column < kNumColumns; ++column) {
          container->store(decoded[column], row, newRow, column);
        }
      }
    }
  }

  uint64_t probe(HashTable<true>& table) {
    HashLookup lookup(table.hashers());
    SelectivityVector rows(kBatchSize);
    uint64_t hits = 0;
    auto& hasher = *table.hashers().front();
    for (const auto& batch : batches_) {
      lookup.reset(batch->size());
      hasher.decode(*batch->childAt(0), rows);
      hasher.hash(rows, false, lookup.hashes);
      lookup.rows.resize(batch->size());
      std::iota(lookup.rows.begin(), lookup.rows.end(), 0);
      table.joinProbe(lookup);
      for (auto row = 0; row < batch->size(); ++row) {
        hits += lookup.hits[row] != nullptr;
      }
    }
    return hits;
  }

  RowTypePtr rowType_;
  std::vector<RowVectorPtr> batches_;
};

InnerJoinBenchmark& benchmark() {
  static auto instance = std::make_unique<InnerJoinBenchmark>();
  return *instance;
}

BENCHMARK(InnerJoin_10MRows_30VarcharColumns) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark();
  suspender.dismiss();
  const auto peakBytes = instance.run();
  suspender.rehire();
  fmt::print("poolPeakBytes={}\n", peakBytes);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::Options options;
  options.useMmapAllocator = false;
  options.allocatorCapacity = 64UL << 30;
  memory::MemoryManager::initialize(options);
  folly::runBenchmarks();
  return 0;
}
