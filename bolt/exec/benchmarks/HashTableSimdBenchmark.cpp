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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <cstring>
#if defined(__linux__)
#include <sched.h>
#include <cerrno>
#endif
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <random>
#include <vector>
#include "bolt/exec/HashTable.h"
#include "bolt/vector/FlatVector.h"

DEFINE_int32(
    bolt_benchmark_simd_hash_table_iters,
    10,
    "Number of measured iterations per case (averaged)");
DEFINE_int32(
    bolt_benchmark_simd_hash_table_warmup,
    2,
    "Number of warmup iterations per case (discarded)");
DEFINE_int32(
    bolt_benchmark_simd_hash_table_trim,
    1,
    "Number of max-value outliers to drop from each sample set (robust mean)");
DEFINE_int32(
    bolt_benchmark_simd_hash_table_pin_cpu,
    -1,
    "If >=0, pin the benchmark thread to this CPU core (taskset-style) "
    "for stable cache state. Set governor=performance externally.");

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using Clock = std::chrono::high_resolution_clock;

namespace {
std::shared_ptr<memory::MemoryPool> benchmarkPool;

struct SampleStatistics {
  double mean = 0;
  double standardDeviation = 0;
  double minimum = 0;
  double maximum = 0;
};

SampleStatistics computeSampleStatistics(const std::vector<double>& samples) {
  SampleStatistics statistics;
  if (samples.empty()) {
    return statistics;
  }
  // Robust mean: drop the requested largest samples (single-threaded noise
  // spikes tend to inflate max). Always keep min. If trim >= size, fall back to
  // no trimming.
  std::vector<double> sortedSamples = samples;
  std::sort(sortedSamples.begin(), sortedSamples.end());
  int trim = std::max(0, FLAGS_bolt_benchmark_simd_hash_table_trim);
  if (trim >= static_cast<int>(sortedSamples.size())) {
    trim = 0;
  }
  int end = static_cast<int>(sortedSamples.size()) - trim;
  double sum = 0;
  statistics.minimum = sortedSamples.front();
  statistics.maximum = sortedSamples.back();
  for (int i = 0; i < end; ++i) {
    sum += sortedSamples[i];
  }
  statistics.mean = sum / end;
  double squaredDeviationSum = 0;
  for (int i = 0; i < end; ++i) {
    double d = sortedSamples[i] - statistics.mean;
    squaredDeviationSum += d * d;
  }
  statistics.standardDeviation =
      end > 1 ? std::sqrt(squaredDeviationSum / (end - 1)) : 0.0;
  return statistics;
}
// Pin the current thread to a single core. Reduces measurement jitter caused
// by CPU migrations (L1/L2/iTLB invalidation). Call once at process start.
void pinToCpuIfRequested() {
#if defined(__linux__)
  if (FLAGS_bolt_benchmark_simd_hash_table_pin_cpu < 0) {
    return;
  }
  if (FLAGS_bolt_benchmark_simd_hash_table_pin_cpu >= CPU_SETSIZE) {
    printf(
        "[warn] invalid CPU %d: must be less than CPU_SETSIZE=%d\n",
        FLAGS_bolt_benchmark_simd_hash_table_pin_cpu,
        CPU_SETSIZE);
    return;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(FLAGS_bolt_benchmark_simd_hash_table_pin_cpu, &set);
  if (sched_setaffinity(0, sizeof(set), &set) != 0) {
    printf(
        "[warn] failed to pin to CPU %d (errno=%d)\n",
        FLAGS_bolt_benchmark_simd_hash_table_pin_cpu,
        errno);
  } else {
    printf(
        "[info] pinned thread to CPU %d\n",
        FLAGS_bolt_benchmark_simd_hash_table_pin_cpu);
  }
#endif
}

std::vector<RowVectorPtr>
makeBigintBatches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto kv = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto* raw = kv->as<FlatVector<int64_t>>()->mutableRawValues();
    for (int i = 0; i < batchSize; i++)
      raw[i] = rng() % numDistinct;
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k"}, {BIGINT()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{kv}));
  }
  return batches;
}

// INT32 variant of makeBigintBatches. Primary generator for single-key cases
// (most benchmarks use this; BIGINT is kept for a single coverage case).
std::vector<RowVectorPtr>
makeInt32Batches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto kv = BaseVector::create(INTEGER(), batchSize, benchmarkPool.get());
    auto* raw = kv->as<FlatVector<int32_t>>()->mutableRawValues();
    for (int i = 0; i < batchSize; i++)
      raw[i] = static_cast<int32_t>(rng() % numDistinct);
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k"}, {INTEGER()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{kv}));
  }
  return batches;
}

// Generate batches with unique sequential keys [0, totalRows).
// Each key appears exactly once. Used for join build to avoid dedup issues.
std::vector<RowVectorPtr> makeBigintUniqueBatches(
    int totalRows,
    int batchSize) {
  std::vector<RowVectorPtr> batches;
  for (int offset = 0; offset < totalRows; offset += batchSize) {
    int n = std::min(batchSize, totalRows - offset);
    auto kv = BaseVector::create(BIGINT(), n, benchmarkPool.get());
    auto* raw = kv->as<FlatVector<int64_t>>()->mutableRawValues();
    for (int i = 0; i < n; i++)
      raw[i] = offset + i;
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k"}, {BIGINT()}),
        nullptr,
        n,
        std::vector<VectorPtr>{kv}));
  }
  return batches;
}

// INT32 variant of makeBigintUniqueBatches.
std::vector<RowVectorPtr> makeInt32UniqueBatches(int totalRows, int batchSize) {
  std::vector<RowVectorPtr> batches;
  for (int offset = 0; offset < totalRows; offset += batchSize) {
    int n = std::min(batchSize, totalRows - offset);
    auto kv = BaseVector::create(INTEGER(), n, benchmarkPool.get());
    auto* raw = kv->as<FlatVector<int32_t>>()->mutableRawValues();
    for (int i = 0; i < n; i++)
      raw[i] = static_cast<int32_t>(offset + i);
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k"}, {INTEGER()}),
        nullptr,
        n,
        std::vector<VectorPtr>{kv}));
  }
  return batches;
}

std::vector<RowVectorPtr>
makeBigintInt32Batches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto k1 = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto k2 = BaseVector::create(INTEGER(), batchSize, benchmarkPool.get());
    auto* raw1 = k1->as<FlatVector<int64_t>>()->mutableRawValues();
    auto* raw2 = k2->as<FlatVector<int32_t>>()->mutableRawValues();
    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      raw1[i] = v;
      raw2[i] = static_cast<int32_t>(v * 7 + 13); // correlated second key
    }
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2"}, {BIGINT(), INTEGER()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{k1, k2}));
  }
  return batches;
}

std::vector<RowVectorPtr> makeBigintVarcharBatches(
    int numBatches,
    int batchSize,
    int numDistinct,
    double nullFraction = 0.0) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto k1 = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto k2 = BaseVector::create(VARCHAR(), batchSize, benchmarkPool.get());
    auto* raw1 = k1->as<FlatVector<int64_t>>()->mutableRawValues();
    auto* strVec = k2->as<FlatVector<StringView>>();

    BufferPtr nulls;
    uint64_t* nullBits = nullptr;
    if (nullFraction > 0.0) {
      nulls =
          AlignedBuffer::allocate<bool>(batchSize, benchmarkPool.get(), true);
      nullBits = nulls->asMutable<uint64_t>();
      // Start with all non-null.
      memset(nullBits, 0xFF, bits::nbytes(batchSize));
    }

    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      raw1[i] = v;
      // Random-length string in [8..20] with content fully determined by v
      // (so identical v maps to identical string, preserving numDistinct).
      // A per-v mt19937 seeded from v gives reproducible output across runs.
      std::mt19937 strRng(static_cast<uint32_t>(v * 2654435761u + 0x9E3779B9u));
      const int len = 8 + static_cast<int>(strRng() % 13); // [8..20]
      std::string s(len, '\0');
      for (int j = 0; j < len; ++j) {
        s[j] = static_cast<char>('a' + (strRng() % 26));
      }
      strVec->set(i, StringView(s));

      if (nullBits && (rng() % 100) < (int)(nullFraction * 100)) {
        bits::clearBit(nullBits, i);
        k1->setNull(i, true);
        k2->setNull(i, true);
      }
    }

    if (nulls) {
      k1->setNulls(nulls);
      k2->setNulls(nulls);
    }

    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2"}, {BIGINT(), VARCHAR()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{k1, k2}));
  }
  return batches;
}

// Mixed-type keys: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR.
// Exercises per-column SIMD/scalar fallback — some columns are SIMD-capable
// (INT32, INT64, VARCHAR) and some are not (BOOL, INT8, INT16).
std::vector<RowVectorPtr>
makeMixedTypeBatches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto kBool = BaseVector::create(BOOLEAN(), batchSize, benchmarkPool.get());
    auto kTiny = BaseVector::create(TINYINT(), batchSize, benchmarkPool.get());
    auto kSmall =
        BaseVector::create(SMALLINT(), batchSize, benchmarkPool.get());
    auto kInt = BaseVector::create(INTEGER(), batchSize, benchmarkPool.get());
    auto kBig = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto kStr = BaseVector::create(VARCHAR(), batchSize, benchmarkPool.get());

    auto* rawBool = kBool->as<FlatVector<bool>>();
    auto* rawTiny = kTiny->as<FlatVector<int8_t>>()->mutableRawValues();
    auto* rawSmall = kSmall->as<FlatVector<int16_t>>()->mutableRawValues();
    auto* rawInt = kInt->as<FlatVector<int32_t>>()->mutableRawValues();
    auto* rawBig = kBig->as<FlatVector<int64_t>>()->mutableRawValues();
    auto* strVec = kStr->as<FlatVector<StringView>>();

    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      rawBool->set(i, (v & 1) != 0);
      rawTiny[i] = static_cast<int8_t>(v % 127);
      rawSmall[i] = static_cast<int16_t>(v % 32000);
      rawInt[i] = static_cast<int32_t>(v);
      rawBig[i] = static_cast<int64_t>(v);
      std::mt19937 strRng(static_cast<uint32_t>(v * 2654435761u + 0x9E3779B9u));
      const int len = 8 + static_cast<int>(strRng() % 13); // [8..20]
      std::string s(len, '\0');
      for (int j = 0; j < len; ++j) {
        s[j] = static_cast<char>('a' + (strRng() % 26));
      }
      strVec->set(i, StringView(s));
    }

    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2", "k3", "k4", "k5", "k6"},
            {BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), VARCHAR()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{kBool, kTiny, kSmall, kInt, kBig, kStr}));
  }
  return batches;
}

std::vector<RowVectorPtr>
makeInt16Int64NullBatches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto k1 = BaseVector::create(SMALLINT(), batchSize, benchmarkPool.get());
    auto k2 = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto* raw1 = k1->as<FlatVector<int16_t>>()->mutableRawValues();
    auto* raw2 = k2->as<FlatVector<int64_t>>()->mutableRawValues();

    auto nulls1 =
        AlignedBuffer::allocate<bool>(batchSize, benchmarkPool.get(), true);
    auto nulls2 =
        AlignedBuffer::allocate<bool>(batchSize, benchmarkPool.get(), true);
    auto* nb1 = nulls1->asMutable<uint64_t>();
    auto* nb2 = nulls2->asMutable<uint64_t>();
    memset(nb1, 0xFF, bits::nbytes(batchSize));
    memset(nb2, 0xFF, bits::nbytes(batchSize));

    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      raw1[i] = static_cast<int16_t>(v % 30000);
      raw2[i] = static_cast<int64_t>(v);
      if ((rng() % 10) == 0) {
        bits::clearBit(nb1, i);
      }
      if ((rng() % 10) == 0) {
        bits::clearBit(nb2, i);
      }
    }
    k1->setNulls(nulls1);
    k2->setNulls(nulls2);

    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2"}, {SMALLINT(), BIGINT()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{k1, k2}));
  }
  return batches;
}

std::vector<RowVectorPtr>
makeBoolInt64Batches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto k1 = BaseVector::create(BOOLEAN(), batchSize, benchmarkPool.get());
    auto k2 = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto* boolVec = k1->as<FlatVector<bool>>();
    auto* raw2 = k2->as<FlatVector<int64_t>>()->mutableRawValues();
    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      boolVec->set(i, (v & 1) != 0);
      raw2[i] = static_cast<int64_t>(v);
    }
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2"}, {BOOLEAN(), BIGINT()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{k1, k2}));
  }
  return batches;
}

std::vector<RowVectorPtr>
makeFloatInt64Batches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto k1 = BaseVector::create(REAL(), batchSize, benchmarkPool.get());
    auto k2 = BaseVector::create(BIGINT(), batchSize, benchmarkPool.get());
    auto* raw1 = k1->as<FlatVector<float>>()->mutableRawValues();
    auto* raw2 = k2->as<FlatVector<int64_t>>()->mutableRawValues();
    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      raw1[i] = static_cast<float>(v) + 0.5f;
      raw2[i] = static_cast<int64_t>(v);
    }
    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2"}, {REAL(), BIGINT()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{k1, k2}));
  }
  return batches;
}

std::vector<RowVectorPtr>
makeAllScalarBatches(int numBatches, int batchSize, int numDistinct) {
  std::vector<RowVectorPtr> batches;
  std::mt19937 rng(42);
  for (int b = 0; b < numBatches; b++) {
    auto kBool = BaseVector::create(BOOLEAN(), batchSize, benchmarkPool.get());
    auto kTiny = BaseVector::create(TINYINT(), batchSize, benchmarkPool.get());
    auto kSmall =
        BaseVector::create(SMALLINT(), batchSize, benchmarkPool.get());
    auto kFloat = BaseVector::create(REAL(), batchSize, benchmarkPool.get());
    auto kDouble = BaseVector::create(DOUBLE(), batchSize, benchmarkPool.get());

    auto* boolVec = kBool->as<FlatVector<bool>>();
    auto* rawTiny = kTiny->as<FlatVector<int8_t>>()->mutableRawValues();
    auto* rawSmall = kSmall->as<FlatVector<int16_t>>()->mutableRawValues();
    auto* rawFloat = kFloat->as<FlatVector<float>>()->mutableRawValues();
    auto* rawDouble = kDouble->as<FlatVector<double>>()->mutableRawValues();

    for (int i = 0; i < batchSize; i++) {
      auto v = rng() % numDistinct;
      boolVec->set(i, (v & 1) != 0);
      rawTiny[i] = static_cast<int8_t>(v % 127);
      rawSmall[i] = static_cast<int16_t>(v % 30000);
      rawFloat[i] = static_cast<float>(v) + 0.5f;
      rawDouble[i] = static_cast<double>(v) + 0.25;
    }

    batches.push_back(std::make_shared<RowVector>(
        benchmarkPool.get(),
        ROW({"k1", "k2", "k3", "k4", "k5"},
            {BOOLEAN(), TINYINT(), SMALLINT(), REAL(), DOUBLE()}),
        nullptr,
        batchSize,
        std::vector<VectorPtr>{kBool, kTiny, kSmall, kFloat, kDouble}));
  }
  return batches;
}

// Single iteration of aggregation groupProbe benchmark. All scaffolding
// (hashers, table, lookup) and prepareForGroupProbe() are outside the timed
// region; only the hot groupProbe() call is timed.
struct AggregationIterationResult {
  double nsPerRow = 0.0;
  int64_t totalRows = 0;
  int64_t numDistinct = 0;
  int64_t capacity = 0;
  int64_t newGroups = 0;
  std::string hashMode;
};

AggregationIterationResult runAggregationIteration(
    bool enableSimdHashTable,
    bool enableJit,
    const std::vector<RowVectorPtr>& batches,
    std::vector<SelectivityVector>& selectedRowsByBatch,
    // Optional sparse selected-count per batch (empty → use batch->size()).
    const std::vector<int64_t>& selectedRowCountsByBatch) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  for (int i = 0; i < batches[0]->childrenSize(); i++) {
    hashers.push_back(
        std::make_unique<VectorHasher>(batches[0]->childAt(i)->type(), i));
  }
  std::unique_ptr<BaseHashTable> table = HashTable<true>::createForAggregation(
      std::move(hashers),
      std::vector<Accumulator>{},
      benchmarkPool.get(),
      nullptr,
      enableJit);
  table->setSimdEnabled(enableSimdHashTable);

  auto lookup = std::make_unique<HashLookup>(table->hashers(), enableJit);
  int64_t totalRows = 0, totalNewGroups = 0;

  // Time only groupProbe(); exclude lookup->reset and prepareForGroupProbe().
  int64_t groupProbeNanos = 0;
  for (size_t b = 0; b < batches.size(); ++b) {
    const auto& batch = batches[b];
    auto& rows = selectedRowsByBatch[b];
    if (!rows.hasSelections()) {
      continue;
    }
    lookup->reset(batch->size());
    table->prepareForGroupProbe(
        *lookup,
        batch,
        rows,
        /*ignoreNullKeys=*/true,
        BaseHashTable::kNoSpillInputStartPartitionBit);
    auto t0 = Clock::now();
    table->groupProbe(*lookup);
    auto t1 = Clock::now();
    groupProbeNanos +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    totalRows += selectedRowCountsByBatch.empty() ? batch->size()
                                                  : selectedRowCountsByBatch[b];
    totalNewGroups += lookup->newGroups.size();
  }

  double ns = static_cast<double>(groupProbeNanos);
  AggregationIterationResult r;
  r.nsPerRow = totalRows > 0 ? ns / totalRows : 0.0;
  r.totalRows = totalRows;
  r.numDistinct = table->numDistinct();
  r.capacity = table->capacity();
  r.newGroups = totalNewGroups;
  r.hashMode = std::string(BaseHashTable::modeString(table->hashMode()));
  return r;
}

// Print one line of stats.
void printAggregationStatistics(
    const char* label,
    const SampleStatistics& s,
    const AggregationIterationResult& report,
    int n) {
  printf(
      "  %-7s [%-14s] %6ldK rows  %5ldK grp  cap=%6luK  "
      "groupProbe=%5.1f±%4.1f ns/row  (min=%4.1f max=%4.1f)  "
      "newGrp=%ldK  (n=%d)\n",
      label,
      report.hashMode.c_str(),
      (long)report.totalRows / 1000,
      (long)report.numDistinct / 1000,
      (unsigned long)report.capacity / 1000,
      s.mean,
      s.standardDeviation,
      s.minimum,
      s.maximum,
      (long)report.newGroups / 1000,
      n);
}

// Per-side sequential harness (block A/B): run all F14 iters, then all SIMD
// iters. Avoids cross-contamination between two algorithms' cache footprints.
void runAggregationVariant(
    const char* label,
    bool enableSimdHashTable,
    bool enableJit,
    const std::vector<RowVectorPtr>& batches,
    std::vector<SelectivityVector>& selectedRowsByBatch,
    const std::vector<int64_t>& selectedRowCountsByBatch = {}) {
  int warmup = std::max(0, FLAGS_bolt_benchmark_simd_hash_table_warmup);
  int measured = std::max(1, FLAGS_bolt_benchmark_simd_hash_table_iters);
  std::vector<double> samples;
  samples.reserve(measured);
  AggregationIterationResult report;
  for (int it = 0; it < warmup + measured; ++it) {
    auto r = runAggregationIteration(
        enableSimdHashTable,
        enableJit,
        batches,
        selectedRowsByBatch,
        selectedRowCountsByBatch);
    if (it >= warmup) {
      samples.push_back(r.nsPerRow);
      report = r;
    }
  }
  printAggregationStatistics(
      label, computeSampleStatistics(samples), report, measured);
}

// Emits an F14 (scalar) row plus, when SIMD is disabled and the build has JIT
// row-equality, a JIT row. When enableSimdHashTable is true, emits only the
// SIMD row. This keeps all call sites unchanged (they pass label="F14"/"SIMD").
void runAggregationVariants(
    const char* label,
    bool enableSimdHashTable,
    const std::vector<RowVectorPtr>& batches,
    std::vector<SelectivityVector>& selectedRowsByBatch,
    const std::vector<int64_t>& selectedRowCountsByBatch = {}) {
  if (enableSimdHashTable) {
    runAggregationVariant(
        label,
        true,
        false,
        batches,
        selectedRowsByBatch,
        selectedRowCountsByBatch);
    return;
  }
  runAggregationVariant(
      label,
      false,
      false,
      batches,
      selectedRowsByBatch,
      selectedRowCountsByBatch);
#ifdef ENABLE_BOLT_JIT
  printf("  %-7s", "");
  runAggregationVariant(
      "JIT",
      false,
      true,
      batches,
      selectedRowsByBatch,
      selectedRowCountsByBatch);
#endif
}

void runAggregationCase(
    const char* label,
    bool enableSimdHashTable,
    const std::vector<RowVectorPtr>& batches) {
  std::vector<SelectivityVector> selectedRowsByBatch;
  selectedRowsByBatch.reserve(batches.size());
  for (auto& batch : batches) {
    SelectivityVector sv(batch->size());
    sv.setAll();
    selectedRowsByBatch.push_back(std::move(sv));
  }
  runAggregationVariants(
      label, enableSimdHashTable, batches, selectedRowsByBatch);
}

// Sparse-batch variant: only 'selectivityPercent'% of rows are selected per
// batch.
void runSparseAggregationCase(
    const char* label,
    bool enableSimdHashTable,
    const std::vector<RowVectorPtr>& batches,
    int selectivityPercent) {
  std::vector<SelectivityVector> selectedRowsByBatch;
  selectedRowsByBatch.reserve(batches.size());
  std::vector<int64_t> selectedRowCounts(batches.size(), 0);
  std::mt19937 filterRng(123);
  for (size_t b = 0; b < batches.size(); ++b) {
    SelectivityVector sv(batches[b]->size());
    sv.clearAll();
    int64_t cnt = 0;
    for (int i = 0; i < batches[b]->size(); ++i) {
      if (static_cast<int>(filterRng() % 100) < selectivityPercent) {
        sv.setValid(i, true);
        ++cnt;
      }
    }
    sv.updateBounds();
    selectedRowCounts[b] = cnt;
    selectedRowsByBatch.push_back(std::move(sv));
  }
  runAggregationVariants(
      label,
      enableSimdHashTable,
      batches,
      selectedRowsByBatch,
      selectedRowCounts);
}

struct JoinIterationResult {
  double buildMs = 0.0;
  double probeNsPerRow = 0.0;
  int64_t numDistinct = 0;
  int64_t capacity = 0;
  int64_t totalHits = 0;
  std::string hashMode;
};

JoinIterationResult runSingleKeyJoinIteration(
    bool enableSimdHashTable,
    bool enableJit,
    const std::vector<RowVectorPtr>& buildBatches,
    const std::vector<RowVectorPtr>& probeBatches,
    const TypePtr& keyType,
    int batchSize) {
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(std::make_unique<VectorHasher>(keyType, 0));
  std::unique_ptr<BaseHashTable> table = HashTable<true>::createForJoin(
      std::move(hashers),
      {},
      false,
      false,
      BaseHashTable::HashMode::kArray,
      0,
      benchmarkPool.get(),
      enableJit);
  table->setSimdEnabled(enableSimdHashTable);

  raw_vector<uint64_t> analyzeHashes;
  analyzeHashes.resize(batchSize);
  for (auto& batch : buildBatches) {
    SelectivityVector rows(batch->size());
    auto key = batch->childAt(0)->loadedVector();
    table->hashers()[0]->decode(*key, rows);
    table->hashers()[0]->computeValueIds(rows, analyzeHashes);
  }
  auto* rowContainer = table->rows();
  // Pre-populate all rows OUTSIDE the timed region — we only time
  // prepareJoinTable() (which calls insertForJoinThin).
  for (auto& batch : buildBatches) {
    SelectivityVector rows(batch->size());
    auto key = batch->childAt(0)->loadedVector();
    table->hashers()[0]->decode(*key, rows);
    for (int i = 0; i < batch->size(); ++i) {
      char* newRow = rowContainer->newRow();
      rowContainer->store(table->hashers()[0]->decodedVector(), i, newRow, 0);
    }
  }
  auto tBuild0 = Clock::now();
  table->prepareJoinTable({});
  auto tBuild1 = Clock::now();

  auto lookup = std::make_unique<HashLookup>(table->hashers(), enableJit);
  int64_t totalProbeRows = 0, totalHits = 0;
  // Time only joinProbe() — exclude reset, prepare, hit-scan.
  int64_t joinProbeNanos = 0;
  for (auto& batch : probeBatches) {
    SelectivityVector rows(batch->size());
    lookup->reset(batch->size());
    table->prepareForJoinProbe(*lookup, batch, rows, true);
    auto t0 = Clock::now();
    table->joinProbe(*lookup);
    auto t1 = Clock::now();
    joinProbeNanos +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    totalProbeRows += batch->size();
    for (auto row : lookup->rows) {
      if (lookup->hits[row])
        ++totalHits;
    }
  }

  JoinIterationResult r;
  r.buildMs =
      std::chrono::duration<double, std::milli>(tBuild1 - tBuild0).count();
  r.probeNsPerRow = totalProbeRows > 0
      ? static_cast<double>(joinProbeNanos) / totalProbeRows
      : 0.0;
  r.numDistinct = table->numDistinct();
  r.capacity = table->capacity();
  r.totalHits = totalHits;
  r.hashMode = std::string(BaseHashTable::modeString(table->hashMode()));
  return r;
}

void printJoinStatistics(
    const char* label,
    const SampleStatistics& sBuild,
    const SampleStatistics& sProbe,
    const JoinIterationResult& report,
    int n) {
  printf(
      "  %-7s [%-14s] build=%4.0fms±%3.0f  %5ldK grp  cap=%6luK  "
      "probe=%5.1f±%4.1f ns/row  (min=%4.1f max=%4.1f)  hits=%ldK  (n=%d)\n",
      label,
      report.hashMode.c_str(),
      sBuild.mean,
      sBuild.standardDeviation,
      (long)report.numDistinct / 1000,
      (unsigned long)report.capacity / 1000,
      sProbe.mean,
      sProbe.standardDeviation,
      sProbe.minimum,
      sProbe.maximum,
      (long)report.totalHits / 1000,
      n);
}

void runSingleKeyJoinVariant(
    const char* label,
    bool enableSimdHashTable,
    bool enableJit,
    const std::vector<RowVectorPtr>& buildBatches,
    const std::vector<RowVectorPtr>& probeBatches,
    const TypePtr& keyType,
    int batchSize) {
  int warmup = std::max(0, FLAGS_bolt_benchmark_simd_hash_table_warmup);
  int measured = std::max(1, FLAGS_bolt_benchmark_simd_hash_table_iters);
  std::vector<double> buildSamples, probeSamples;
  buildSamples.reserve(measured);
  probeSamples.reserve(measured);
  JoinIterationResult report;
  for (int it = 0; it < warmup + measured; ++it) {
    auto r = runSingleKeyJoinIteration(
        enableSimdHashTable,
        enableJit,
        buildBatches,
        probeBatches,
        keyType,
        batchSize);
    if (it >= warmup) {
      buildSamples.push_back(r.buildMs);
      probeSamples.push_back(r.probeNsPerRow);
      report = r;
    }
  }
  printJoinStatistics(
      label,
      computeSampleStatistics(buildSamples),
      computeSampleStatistics(probeSamples),
      report,
      measured);
}

void runSingleKeyJoinCase(
    const char* label,
    bool enableSimdHashTable,
    int numBuildDistinct,
    int numProbeBatches,
    int batchSize = 4096,
    bool useInt32 = true) {
  auto buildBatches = useInt32
      ? makeInt32UniqueBatches(numBuildDistinct, batchSize)
      : makeBigintUniqueBatches(numBuildDistinct, batchSize);
  auto probeBatches = useInt32
      ? makeInt32Batches(numProbeBatches, batchSize, numBuildDistinct)
      : makeBigintBatches(numProbeBatches, batchSize, numBuildDistinct);
  const TypePtr keyType = useInt32 ? TypePtr(INTEGER()) : TypePtr(BIGINT());

  if (enableSimdHashTable) {
    runSingleKeyJoinVariant(
        label, true, false, buildBatches, probeBatches, keyType, batchSize);
    return;
  }
  runSingleKeyJoinVariant(
      label, false, false, buildBatches, probeBatches, keyType, batchSize);
#ifdef ENABLE_BOLT_JIT
  printf("  %-7s", "");
  runSingleKeyJoinVariant(
      "JIT", false, true, buildBatches, probeBatches, keyType, batchSize);
#endif
}

// Generic multi-column join benchmark.
void runMultiColumnJoinVariant(
    const char* label,
    bool enableSimdHashTable,
    bool enableJit,
    const std::vector<RowVectorPtr>& buildBatches,
    const std::vector<RowVectorPtr>& probeBatches) {
  int warmup = std::max(0, FLAGS_bolt_benchmark_simd_hash_table_warmup);
  int measured = std::max(1, FLAGS_bolt_benchmark_simd_hash_table_iters);
  int totalIters = warmup + measured;

  std::vector<double> buildSamples, probeSamples;
  buildSamples.reserve(measured);
  probeSamples.reserve(measured);
  uint64_t reportDistinct = 0, reportCapacity = 0, reportHits = 0;
  std::string reportHashMode;

  auto numKeyCols = buildBatches[0]->childrenSize();

  for (int it = 0; it < totalIters; ++it) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    for (int i = 0; i < numKeyCols; i++) {
      hashers.push_back(std::make_unique<VectorHasher>(
          buildBatches[0]->childAt(i)->type(), i));
    }
    std::unique_ptr<BaseHashTable> table = HashTable<true>::createForJoin(
        std::move(hashers),
        {},
        false,
        false,
        BaseHashTable::HashMode::kArray,
        0,
        benchmarkPool.get(),
        enableJit);
    if (enableSimdHashTable) {
      table->setSimdEnabled(true);
    } else {
      table->setSimdEnabled(false);
    }

    raw_vector<uint64_t> analyzeHashes;
    analyzeHashes.resize(buildBatches[0]->size());
    for (auto& batch : buildBatches) {
      SelectivityVector rows(batch->size());
      for (int c = 0; c < numKeyCols; c++) {
        auto key = batch->childAt(c)->loadedVector();
        table->hashers()[c]->decode(*key, rows);
        table->hashers()[c]->hash(rows, c > 0, analyzeHashes);
      }
    }

    auto* rowContainer = table->rows();
    // Populate rows OUTSIDE timed region.
    for (auto& batch : buildBatches) {
      SelectivityVector rows(batch->size());
      for (int c = 0; c < numKeyCols; c++) {
        table->hashers()[c]->decode(*batch->childAt(c)->loadedVector(), rows);
      }
      for (int i = 0; i < batch->size(); ++i) {
        char* newRow = rowContainer->newRow();
        for (int c = 0; c < numKeyCols; c++) {
          rowContainer->store(
              table->hashers()[c]->decodedVector(), i, newRow, c);
        }
      }
    }
    auto tBuild0 = Clock::now();
    table->prepareJoinTable({});
    auto tBuild1 = Clock::now();
    double buildMs =
        std::chrono::duration<double, std::milli>(tBuild1 - tBuild0).count();

    auto lookup = std::make_unique<HashLookup>(table->hashers(), enableJit);
    int64_t totalProbeRows = 0, totalHits = 0;
    int64_t joinProbeNanos = 0;
    for (auto& batch : probeBatches) {
      SelectivityVector rows(batch->size());
      lookup->reset(batch->size());
      table->prepareForJoinProbe(*lookup, batch, rows, true);
      auto t0 = Clock::now();
      table->joinProbe(*lookup);
      auto t1 = Clock::now();
      joinProbeNanos +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
      totalProbeRows += batch->size();
      for (auto row : lookup->rows) {
        if (lookup->hits[row])
          ++totalHits;
      }
    }
    double probeMillis = static_cast<double>(joinProbeNanos) / 1e6;

    if (it >= warmup) {
      buildSamples.push_back(buildMs);
      probeSamples.push_back(probeMillis * 1e6 / totalProbeRows);
      reportDistinct = (long)table->numDistinct();
      reportCapacity = (unsigned long)table->capacity();
      reportHits = totalHits;
      reportHashMode =
          std::string(BaseHashTable::modeString(table->hashMode()));
    }
  }
  auto sBuild = computeSampleStatistics(buildSamples);
  auto sProbe = computeSampleStatistics(probeSamples);
  printf(
      "  %-7s [%-14s] build=%4.0fms±%3.0f  %5ldK grp  cap=%6luK  "
      "probe=%5.1f±%4.1f ns/row  (min=%4.1f)  hits=%ldK  (n=%d)\n",
      label,
      reportHashMode.c_str(),
      sBuild.mean,
      sBuild.standardDeviation,
      (long)reportDistinct / 1000,
      (unsigned long)reportCapacity / 1000,
      sProbe.mean,
      sProbe.standardDeviation,
      sProbe.minimum,
      (long)reportHits / 1000,
      (int)measured);
}

void runMultiColumnJoinCase(
    const char* label,
    bool enableSimdHashTable,
    const std::vector<RowVectorPtr>& buildBatches,
    const std::vector<RowVectorPtr>& probeBatches) {
  if (enableSimdHashTable) {
    runMultiColumnJoinVariant(label, true, false, buildBatches, probeBatches);
    return;
  }
  runMultiColumnJoinVariant(label, false, false, buildBatches, probeBatches);
#ifdef ENABLE_BOLT_JIT
  printf("  %-7s", "");
  runMultiColumnJoinVariant("JIT", false, true, buildBatches, probeBatches);
#endif
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  benchmarkPool = memory::memoryManager()->addLeafPool("bench");
  pinToCpuIfRequested();
  printf(
      "[cfg] iters=%d warmup=%d trim=%d  "
      "(for max stability: taskset -c <core> --; "
      "governor=performance; disable turbo; pin_cpu=N)\n\n",
      FLAGS_bolt_benchmark_simd_hash_table_iters,
      FLAGS_bolt_benchmark_simd_hash_table_warmup,
      FLAGS_bolt_benchmark_simd_hash_table_trim);

  // ========== 1. Single Key (INT32) ==========
  printf("=== 1. Single Key (INT32) ===\n\n");
  {
    constexpr int numBatches = 500;
    constexpr int batchSize = 4096;
    constexpr int numDistinct = 2'000'000;
    const int64_t totalRows = static_cast<int64_t>(numBatches) * batchSize;
    printf(
        "--- %ldK rows, %dK distinct (%s) ---\n",
        totalRows / 1000,
        numDistinct / 1000,
        "INT32");
    auto batches = makeInt32Batches(numBatches, batchSize, numDistinct);
    runAggregationCase("F14", false, batches);
    runAggregationCase("SIMD", true, batches);
    printf("\n");
  }

  // ========== 2. Two integer keys: BIGINT + INT32 ==========
  printf("=== 2. BIGINT + INT32 Keys ===\n\n");
  {
    struct AggregationCase {
      int numBatches;
      int batchSize;
      int numDistinct;
    };
    AggregationCase cases[] = {
        {100, 4096, 100000},
        {500, 4096, 2000000},
    };
    for (const auto& testCase : cases) {
      int64_t total = (int64_t)testCase.numBatches * testCase.batchSize;
      printf(
          "--- %ldK rows, %dK distinct ---\n",
          total / 1000,
          testCase.numDistinct / 1000);
      auto batches = makeBigintInt32Batches(
          testCase.numBatches, testCase.batchSize, testCase.numDistinct);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  // ========== 3. Mixed: BIGINT + VARCHAR (no nulls) ==========
  printf("=== 3. BIGINT + VARCHAR Keys (no nulls) ===\n\n");
  {
    struct AggregationCase {
      int numBatches;
      int batchSize;
      int numDistinct;
    };
    AggregationCase cases[] = {
        {100, 4096, 100000},
        {500, 4096, 2000000},
    };
    for (const auto& testCase : cases) {
      int64_t total = (int64_t)testCase.numBatches * testCase.batchSize;
      printf(
          "--- %ldK rows, %dK distinct ---\n",
          total / 1000,
          testCase.numDistinct / 1000);
      auto batches = makeBigintVarcharBatches(
          testCase.numBatches, testCase.batchSize, testCase.numDistinct, 0.0);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  // ========== 4. Mixed: BIGINT + VARCHAR with 10% nulls ==========
  printf("=== 4. BIGINT + VARCHAR Keys (10%% nulls) ===\n\n");
  {
    struct AggregationCase {
      int numBatches;
      int batchSize;
      int numDistinct;
    };
    AggregationCase cases[] = {
        {100, 4096, 100000},
        {500, 4096, 2000000},
    };
    for (const auto& testCase : cases) {
      int64_t total = (int64_t)testCase.numBatches * testCase.batchSize;
      printf(
          "--- %ldK rows, %dK distinct ---\n",
          total / 1000,
          testCase.numDistinct / 1000);
      auto batches = makeBigintVarcharBatches(
          testCase.numBatches, testCase.batchSize, testCase.numDistinct, 0.10);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  // ========== 5. Sparse Batch: INT32 key with post-filter selectivity ======
  printf("=== 5. Sparse Batch: INT32 Key (post-filter selectivity) ===\n\n");
  {
    auto batches = makeInt32Batches(500, 4096, 2000000);
    int pcts[] = {100, 50, 25, 10};
    for (auto pct : pcts) {
      printf("--- selectivity=%d%% ---\n", pct);
      if (pct == 100) {
        runAggregationCase("F14", false, batches);
        runAggregationCase("SIMD", true, batches);
      } else {
        runSparseAggregationCase("F14", false, batches, pct);
        runSparseAggregationCase("SIMD", true, batches, pct);
      }
      printf("\n");
    }
  }

  printf("=== 6. Join Build + Probe: Single-key (INT32 + one BIGINT) ===\n\n");
  {
    const int batchSize = 4096;
    struct SingleKeyJoinCase {
      int numBuildDistinct;
      int probeBatches;
      bool useInt64;
    };
    SingleKeyJoinCase cases[] = {
        {2000000, 500, false},
        // One BIGINT coverage case.
        {2000000, 500, true},
    };
    for (const auto& testCase : cases) {
      int64_t probeRows = (int64_t)testCase.probeBatches * batchSize;
      printf(
          "--- %dK build, %ldK probe (%s) ---\n",
          testCase.numBuildDistinct / 1000,
          probeRows / 1000,
          testCase.useInt64 ? "BIGINT" : "INT32");
      runSingleKeyJoinCase(
          "F14",
          false,
          testCase.numBuildDistinct,
          testCase.probeBatches,
          batchSize,
          !testCase.useInt64);
      runSingleKeyJoinCase(
          "SIMD",
          true,
          testCase.numBuildDistinct,
          testCase.probeBatches,
          batchSize,
          !testCase.useInt64);
      printf("\n");
    }
  }

  // ========== 7. Large Agg (> LLC, DRAM) ==========
  printf("=== 7. Large Agg (DRAM) ===\n\n");
  {
    // Single INT32.
    for (int numDistinct : {10000000, 20000000}) {
      auto batches = makeInt32Batches(600, 4096, numDistinct);
      printf("--- INT32 %dM distinct, 2.5M rows ---\n", numDistinct / 1000000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
    // Multi-column: BIGINT+INT32 (keeps BIGINT coverage).
    {
      auto batches = makeBigintInt32Batches(600, 4096, 10000000);
      printf("--- BIGINT+INT32 10M distinct, 2.5M rows ---\n");
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  // ========== 8. Large Join (DRAM) ==========
  printf("=== 8. Large Join (DRAM) ===\n\n");
  {
    printf("--- 10M build, 2.5M probe (INT32) ---\n");
    runSingleKeyJoinCase("F14", false, 10000000, 600, 4096, /*useInt32=*/true);
    runSingleKeyJoinCase("SIMD", true, 10000000, 600, 4096, /*useInt32=*/true);
    printf("\n");
  }

  // ========== 9. High Collision Agg (SKIPPED — pre-existing abort) ==========
  printf("=== 9. High Collision Agg (SKIPPED) ===\n\n");

  // ========== 11. Mixed Type Keys (BOOL+INT8+INT16+INT32+INT64+VARCHAR) =====
  printf("=== 11. Mixed Type Agg ===\n\n");
  {
    for (auto [numDistinct, numBatches] : std::vector<std::pair<int, int>>{
             {1000, 100}, {100000, 100}, {2000000, 500}}) {
      auto batches = makeMixedTypeBatches(numBatches, 4096, numDistinct);
      auto nRows = numBatches * 4096;
      printf(
          "--- Mixed 6-col, %dK rows, %dK distinct ---\n",
          nRows / 1000,
          numDistinct / 1000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  // ========== 12. Mixed Type Join (BOOL+INT8+INT16+INT32+INT64+VARCHAR) =====
  printf("=== 12. Mixed Type Join ===\n\n");
  {
    for (int numBuildDistinct : {100000, 2000000}) {
      auto buildBatches = makeMixedTypeBatches(
          numBuildDistinct / 4096 + 1, 4096, numBuildDistinct);
      auto probeBatches = makeMixedTypeBatches(50, 4096, numBuildDistinct);
      printf(
          "--- Mixed 6-col, %dK build, 200K probe ---\n",
          numBuildDistinct / 1000);
      runMultiColumnJoinCase("F14", false, buildBatches, probeBatches);
      runMultiColumnJoinCase("SIMD", true, buildBatches, probeBatches);
      printf("\n");
    }
  }

  // ========== 13. INT16+INT64 with Nulls (hash mode) ==========
  printf("=== 13. INT16+INT64 Nullable Agg ===\n\n");
  {
    // INT16+INT64 forces kHash mode (not kNK/kArray for multi-type).
    // 10% null rate per column.

    for (auto [numDistinct, numBatches] : std::vector<std::pair<int, int>>{
             {1000, 100}, {100000, 100}, {2000000, 500}}) {
      auto batches = makeInt16Int64NullBatches(numBatches, 4096, numDistinct);
      auto nRows = numBatches * 4096;
      printf(
          "--- INT16+INT64 nullable, %dK rows, %dK distinct ---\n",
          nRows / 1000,
          numDistinct / 1000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  printf("=== 14. INT16+INT64 Nullable Join ===\n\n");
  {
    for (int numBuildDistinct : {100000, 2000000}) {
      auto buildBatches = makeInt16Int64NullBatches(
          numBuildDistinct / 4096 + 1, 4096, numBuildDistinct);
      auto probeBatches = makeInt16Int64NullBatches(50, 4096, numBuildDistinct);
      printf(
          "--- INT16+INT64 nullable, %dK build, 200K probe ---\n",
          numBuildDistinct / 1000);
      runMultiColumnJoinCase("F14", false, buildBatches, probeBatches);
      runMultiColumnJoinCase("SIMD", true, buildBatches, probeBatches);
      printf("\n");
    }
  }

  // ========== 15. BOOL+INT64 Agg+Join ==========
  printf("=== 15. BOOL+INT64 Agg ===\n\n");
  {
    for (auto [numDistinct, numBatches] :
         std::vector<std::pair<int, int>>{{2000000, 500}}) {
      auto batches = makeBoolInt64Batches(numBatches, 4096, numDistinct);
      printf(
          "--- BOOL+INT64, %dK rows, %dK distinct ---\n",
          numBatches * 4096 / 1000,
          numDistinct / 1000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  printf("=== 15b. BOOL+INT64 Join ===\n\n");
  {
    for (int numBuildDistinct : {100000, 2000000}) {
      auto build = makeBoolInt64Batches(
          numBuildDistinct / 4096 + 1, 4096, numBuildDistinct);
      auto probe = makeBoolInt64Batches(50, 4096, numBuildDistinct);
      printf("--- BOOL+INT64, %dK build ---\n", numBuildDistinct / 1000);
      runMultiColumnJoinCase("F14", false, build, probe);
      runMultiColumnJoinCase("SIMD", true, build, probe);
      printf("\n");
    }
  }

  // ========== 16. FLOAT+INT64 Agg+Join ==========
  printf("=== 16. FLOAT+INT64 Agg ===\n\n");
  {
    for (auto [numDistinct, numBatches] :
         std::vector<std::pair<int, int>>{{1000, 100}, {2000000, 500}}) {
      auto batches = makeFloatInt64Batches(numBatches, 4096, numDistinct);
      printf(
          "--- FLOAT+INT64, %dK rows, %dK distinct ---\n",
          numBatches * 4096 / 1000,
          numDistinct / 1000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  printf("=== 16b. FLOAT+INT64 Join ===\n\n");
  {
    for (int numBuildDistinct : {100000, 2000000}) {
      auto build = makeFloatInt64Batches(
          numBuildDistinct / 4096 + 1, 4096, numBuildDistinct);
      auto probe = makeFloatInt64Batches(50, 4096, numBuildDistinct);
      printf("--- FLOAT+INT64, %dK build ---\n", numBuildDistinct / 1000);
      runMultiColumnJoinCase("F14", false, build, probe);
      runMultiColumnJoinCase("SIMD", true, build, probe);
      printf("\n");
    }
  }

  // ========== 17. BOOL+INT8+INT16+FLOAT+DOUBLE Agg+Join ==========
  printf("=== 17. BOOL+INT8+INT16+FLOAT+DOUBLE Agg ===\n\n");
  {
    for (auto [numDistinct, numBatches] :
         std::vector<std::pair<int, int>>{{1000, 100}, {2000000, 500}}) {
      auto batches = makeAllScalarBatches(numBatches, 4096, numDistinct);
      printf(
          "--- 5-col scalar, %dK rows, %dK distinct ---\n",
          numBatches * 4096 / 1000,
          numDistinct / 1000);
      runAggregationCase("F14", false, batches);
      runAggregationCase("SIMD", true, batches);
      printf("\n");
    }
  }

  printf("=== 17b. BOOL+INT8+INT16+FLOAT+DOUBLE Join ===\n\n");
  {
    for (int numBuildDistinct : {100000, 2000000}) {
      auto build = makeAllScalarBatches(
          numBuildDistinct / 4096 + 1, 4096, numBuildDistinct);
      auto probe = makeAllScalarBatches(50, 4096, numBuildDistinct);
      printf("--- 5-col scalar, %dK build ---\n", numBuildDistinct / 1000);
      runMultiColumnJoinCase("F14", false, build, probe);
      runMultiColumnJoinCase("SIMD", true, build, probe);
      printf("\n");
    }
  }

  return 0;
}
