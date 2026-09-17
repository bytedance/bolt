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

#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/exec/BmContainerRowSerde.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cstdint>
#include <vector>

DECLARE_uint64(bm_row_container_data_bytes);
DECLARE_bool(bm_row_container_spill_metrics);

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

uint64_t dataBytes(uint64_t bytes) {
  return bytes == 0 ? FLAGS_bm_row_container_data_bytes : bytes;
}

BenchmarkOptions compareOptions(DatasetKind dataset, uint64_t bytes) {
  auto opts = options(dataset, dataBytes(bytes));
  if (rowCount(opts) < 2) {
    opts.dataBytes = logicalBytesProcessed(opts) + 1;
  }
  return opts;
}

void printFootprint(
    const char* operation,
    const char* container,
    DatasetKind dataset,
    uint64_t rows,
    uint64_t logicalBytes,
    uint64_t residentBytes) {
  if (!FLAGS_bm_row_container_spill_metrics ||
      !shouldPrintSpillMetrics(
          operation, dataset, SpillCompressionKind::kRaw)) {
    return;
  }
  folly::BenchmarkSuspender suspender;
  fmt::print(
      stderr,
      "[bm-row-container-operation-metrics] operation={} container={} "
      "dataset={} rows={} logical_bytes={} resident_bytes={} "
      "resident_bytes_per_row={:.3f}\n",
      operation,
      container,
      datasetName(dataset),
      rows,
      logicalBytes,
      residentBytes,
      rows == 0 ? 0.0 : static_cast<double>(residentBytes) / rows);
}

void storeRowOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  uint64_t storedRows = 0;
  uint64_t residentBytes = 0;
  uint64_t logicalBytes = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    const auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("complex-store-row-old", opts.dataBytes);
    auto container = makeOldKeyRowContainer(dataset, context.pool.get());
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    storeReusableInputBatchesOld(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
    storedRows = container->numRows();
    residentBytes = container->allocatedBytes();
    logicalBytes = logicalBytesProcessed(opts);
  }
  printFootprint(
      "storeRowOld", "old", dataset, storedRows, logicalBytes, residentBytes);
}

void storeRowBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  uint64_t storedRows = 0;
  uint64_t residentBytes = 0;
  uint64_t logicalBytes = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    const auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("complex-store-row-bm", opts.dataBytes);
    auto container = makeBmKeyRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    storeReusableInputBatchesBm(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
    const auto stats = context.bufferManager->stats();
    storedRows = container->numRows();
    residentBytes = stats.pinnedResidentBytes + stats.unpinnedResidentBytes;
    logicalBytes = logicalBytesProcessed(opts);
  }
  printFootprint(
      "storeRowBm", "bm", dataset, storedRows, logicalBytes, residentBytes);
}

void storeBatchOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    const auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("complex-store-batch-old", opts.dataBytes);
    auto container = makeOldKeyRowContainer(dataset, context.pool.get());
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    storeReusableInputBatchesOldBatch(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

void storeBatchBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    const auto opts = options(dataset, dataBytes(bytes));
    BenchmarkContext context("complex-store-batch-bm", opts.dataBytes);
    auto container = makeBmKeyRowContainer(dataset, context.bufferManager);
    auto input = makeReusableInputBatches(context.pool.get(), opts);
    suspender.dismiss();
    storeReusableInputBatchesBmBatch(*container, input, opts);
    folly::doNotOptimizeAway(container->numRows());
    suspender.rehire();
  }
}

void serializedSizeScalar(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-size-scalar", opts.dataBytes);
  auto input = makeReusableInputBatches(context.pool.get(), opts);
  std::vector<DecodedVector> decoded;
  decoded.reserve(input.batches.size());
  for (const auto& batch : input.batches) {
    decoded.emplace_back(*batch->childAt(0));
  }
  uint64_t result = 0;
  const ContainerRowSerdeOptions serdeOptions{.isKey = true};
  suspender.dismiss();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    uint64_t remaining = rowCount(opts);
    size_t nextBatch = 0;
    while (remaining > 0) {
      const auto& values = decoded[nextBatch];
      const auto batchSize = static_cast<vector_size_t>(
          std::min<uint64_t>(values.size(), remaining));
      for (vector_size_t row = 0; row < batchSize; ++row) {
        result += BmContainerRowSerde::serializedSize(
            *values.base(), values.index(row), serdeOptions);
      }
      remaining -= batchSize;
      nextBatch = (nextBatch + 1) % decoded.size();
    }
  }
  folly::doNotOptimizeAway(result);
  suspender.rehire();
}

void serializedSizesBatch(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-size-batch", opts.dataBytes);
  auto input = makeReusableInputBatches(context.pool.get(), opts);
  std::vector<DecodedVector> decoded;
  decoded.reserve(input.batches.size());
  vector_size_t maxBatchSize = 0;
  for (const auto& batch : input.batches) {
    decoded.emplace_back(*batch->childAt(0));
    maxBatchSize = std::max(maxBatchSize, batch->size());
  }
  std::vector<uint64_t> sizes(maxBatchSize);
  uint64_t result = 0;
  const ContainerRowSerdeOptions serdeOptions{.isKey = true};
  suspender.dismiss();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    uint64_t remaining = rowCount(opts);
    size_t nextBatch = 0;
    while (remaining > 0) {
      const auto& values = decoded[nextBatch];
      const auto batchSize = static_cast<vector_size_t>(
          std::min<uint64_t>(values.size(), remaining));
      const auto output = std::span<uint64_t>(sizes.data(), batchSize);
      BmContainerRowSerde::serializedSizes(
          values, 0, batchSize, serdeOptions, output);
      for (const auto size : output) {
        result += size;
      }
      remaining -= batchSize;
      nextBatch = (nextBatch + 1) % decoded.size();
    }
  }
  folly::doNotOptimizeAway(result);
  suspender.rehire();
}

void extractOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-extract-old", opts.dataBytes);
  auto stored = storeOldKeyRows(context, opts, true);
  suspender.dismiss();
  for (uint32_t i = 0; i < iterations; ++i) {
    extractOldRows(*stored.container, stored.rows, opts, context.pool.get());
  }
  suspender.rehire();
}

void extractBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-extract-bm", opts.dataBytes);
  auto stored = storeBmKeyRows(context, opts, true);
  suspender.dismiss();
  for (uint32_t i = 0; i < iterations; ++i) {
    extractBmRowsResident(
        *stored.container, stored.rows, opts, context.pool.get());
  }
  suspender.rehire();
}

template <bool kEqual>
void compareOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = compareOptions(dataset, bytes);
  BenchmarkContext context("complex-compare-old", opts.dataBytes);
  auto input = makeReusableInputBatches(context.pool.get(), opts, 2);
  OldStoredRows stored{makeOldKeyRowContainer(dataset, context.pool.get()), {}};
  storeReusableInputBatchesOld(*stored.container, input, opts, &stored.rows);
  OldStoredRows equalRows;
  if constexpr (kEqual) {
    equalRows.container = makeOldKeyRowContainer(dataset, context.pool.get());
    storeReusableInputBatchesOld(
        *equalRows.container, input, opts, &equalRows.rows);
  }
  int64_t result = 0;
  suspender.dismiss();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    for (size_t row = 0; row < stored.rows.size(); ++row) {
      const auto* other = kEqual ? equalRows.rows[row]
                                 : stored.rows[(row + 1) % stored.rows.size()];
      result += stored.container->compare(stored.rows[row], other, 0);
    }
  }
  folly::doNotOptimizeAway(result);
  suspender.rehire();
}

template <bool kEqual>
void compareBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = compareOptions(dataset, bytes);
  BenchmarkContext context("complex-compare-bm", opts.dataBytes);
  auto input = makeReusableInputBatches(context.pool.get(), opts, 2);
  BmStoredRows stored{
      makeBmKeyRowContainer(dataset, context.bufferManager), {}};
  storeReusableInputBatchesBm(*stored.container, input, opts, &stored.rows);
  BmStoredRows equalRows;
  if constexpr (kEqual) {
    equalRows.container = makeBmKeyRowContainer(dataset, context.bufferManager);
    storeReusableInputBatchesBm(
        *equalRows.container, input, opts, &equalRows.rows);
  }
  int64_t result = 0;
  suspender.dismiss();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    for (size_t row = 0; row < stored.rows.size(); ++row) {
      const auto* other = kEqual ? equalRows.rows[row]
                                 : stored.rows[(row + 1) % stored.rows.size()];
      result += stored.container->compare(stored.rows[row], other, 0);
    }
  }
  folly::doNotOptimizeAway(result);
  suspender.rehire();
}

void compareEqualOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  compareOld<true>(iterations, dataset, bytes);
}

void compareEqualBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  compareBm<true>(iterations, dataset, bytes);
}

void compareFirstDifferenceOld(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  compareOld<false>(iterations, dataset, bytes);
}

void compareFirstDifferenceBm(
    uint32_t iterations,
    DatasetKind dataset,
    uint64_t bytes) {
  compareBm<false>(iterations, dataset, bytes);
}

void hashOld(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-hash-old", opts.dataBytes);
  auto stored = storeOldKeyRows(context, opts, true);
  std::vector<uint64_t> hashes(stored.rows.size());
  suspender.dismiss();
  for (uint32_t i = 0; i < iterations; ++i) {
    stored.container->hash(
        0,
        folly::Range<char**>(stored.rows.data(), stored.rows.size()),
        false,
        hashes.data());
  }
  folly::doNotOptimizeAway(hashes.data());
  suspender.rehire();
}

void hashBm(uint32_t iterations, DatasetKind dataset, uint64_t bytes) {
  folly::BenchmarkSuspender suspender;
  const auto opts = options(dataset, dataBytes(bytes));
  BenchmarkContext context("complex-hash-bm", opts.dataBytes);
  auto stored = storeBmKeyRows(context, opts, true);
  uint64_t result = 0;
  suspender.dismiss();
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    for (const auto* row : stored.rows) {
      result += stored.container->hash(row, 0);
    }
  }
  folly::doNotOptimizeAway(result);
  suspender.rehire();
}

#define COMPLEX_CASE(operation, datasetNameToken, dataset)              \
  BENCHMARK_NAMED_PARAM(                                                \
      operation##Old, old_##datasetNameToken, DatasetKind::dataset, 0); \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                       \
      operation##Bm, bm_##datasetNameToken, DatasetKind::dataset, 0)

#define COMPLEX_DATASETS(operation)       \
  COMPLEX_CASE(operation, array, kArray); \
  COMPLEX_CASE(operation, map, kMap);     \
  COMPLEX_CASE(operation, row, kRow);     \
  BENCHMARK_DRAW_LINE()

#define SIZE_DATASETS(operation)                                   \
  BENCHMARK_NAMED_PARAM(operation, array, DatasetKind::kArray, 0); \
  BENCHMARK_NAMED_PARAM(operation, map, DatasetKind::kMap, 0);     \
  BENCHMARK_NAMED_PARAM(operation, row, DatasetKind::kRow, 0);     \
  BENCHMARK_DRAW_LINE()

COMPLEX_DATASETS(storeRow);
COMPLEX_DATASETS(storeBatch);
SIZE_DATASETS(serializedSizeScalar);
SIZE_DATASETS(serializedSizesBatch);
COMPLEX_DATASETS(extract);
COMPLEX_DATASETS(compareEqual);
COMPLEX_DATASETS(compareFirstDifference);
COMPLEX_DATASETS(hash);

#undef COMPLEX_DATASETS
#undef COMPLEX_CASE
#undef SIZE_DATASETS

} // namespace
} // namespace bytedance::bolt::exec::bm::benchmarks
