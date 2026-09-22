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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <optional>

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/lance/NativeLanceReader.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"

namespace bytedance::bolt::lance::reader::benchmark {
namespace {

std::shared_ptr<memory::MemoryPool> rootPool;
std::shared_ptr<memory::MemoryPool> readerPool;
std::shared_ptr<folly::CPUThreadPoolExecutor> decodingExecutor;
std::string lanceFilePath = [] {
  const auto* path = std::getenv("BOLT_LANCE_BENCHMARK_FILE");
  return path == nullptr ? std::string{} : std::string(path);
}();
std::string parquetFilePath = [] {
  const auto* path = std::getenv("BOLT_LANCE_BENCHMARK_PARQUET_FILE");
  return path == nullptr ? std::string{} : std::string(path);
}();
std::string readerMode = [] {
  const auto* mode = std::getenv("BOLT_LANCE_READER_MODE");
  return mode == nullptr ? std::string{} : std::string(mode);
}();

std::optional<uint64_t> expectedRows = []() -> std::optional<uint64_t> {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_EXPECTED_ROWS");
  if (value == nullptr) {
    return std::nullopt;
  }
  return std::stoull(value);
}();
size_t projectedColumnCount = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_COLUMNS");
  return value == nullptr ? 0 : std::stoull(value);
}();
uint64_t batchSize = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_BATCH_SIZE");
  return value == nullptr ? 4'096 : std::stoull(value);
}();
size_t decodingThreads = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_DECODE_THREADS");
  return value == nullptr ? 16 : std::max<size_t>(1, std::stoull(value));
}();
bool printStats = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_PRINT_STATS");
  return value != nullptr && std::string_view(value) != "0";
}();
bool skipMaterialize = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_SKIP_MATERIALIZE");
  return value != nullptr && std::string_view(value) != "0";
}();

std::unique_ptr<dwio::common::BufferedInput> open(const std::string& path) {
  return std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(path), *readerPool);
}

std::vector<std::string> projectedColumns(const RowTypePtr& rowType) {
  const auto count = projectedColumnCount == 0
      ? static_cast<size_t>(rowType->size())
      : std::min(projectedColumnCount, static_cast<size_t>(rowType->size()));
  return {rowType->names().begin(), rowType->names().begin() + count};
}

void materialize(const VectorPtr& result) {
  const auto* row = result->asUnchecked<RowVector>();
  for (size_t i = 0; i < row->childrenSize(); ++i) {
    const auto loaded = row->childAt(i)->loadedVector();
    folly::doNotOptimizeAway(loaded->retainedSize());
  }
}

uint64_t scan(dwio::common::Reader& reader) {
  const auto names = projectedColumns(reader.rowType());
  auto selector =
      std::make_shared<dwio::common::ColumnSelector>(reader.rowType(), names);
  dwio::common::RowReaderOptions options;
  if (decodingExecutor != nullptr) {
    options.setDecodingExecutor(decodingExecutor);
    options.setDecodingParallelismFactor(decodingThreads);
  }
  options.select(selector);
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  for (size_t i = 0; i < names.size(); ++i) {
    const auto fileColumn = reader.rowType()->getChildIdx(names[i]);
    scanSpec->addFieldRecursively(
        names[i], *reader.rowType()->childAt(fileColumn), i);
  }
  options.setScanSpec(std::move(scanSpec));
  auto rows = reader.createRowReader(options);
  dwio::common::RuntimeStatistics beforeStats;
  if (printStats) {
    rows->updateRuntimeStats(beforeStats);
  }
  VectorPtr result = BaseVector::create(
      selector->buildSelectedReordered(), 0, readerPool.get());
  uint64_t count = 0;
  uint64_t batches = 0;
  while (const auto scanned = rows->next(batchSize, result)) {
    count += scanned;
    ++batches;
    if (!skipMaterialize) {
      materialize(result);
    }
  }
  if (printStats) {
    dwio::common::RuntimeStatistics stats;
    rows->updateRuntimeStats(stats);
    std::cerr << "BOLT_LANCE_BENCHMARK_STATS mode=" << readerMode
              << " batches=" << batches
              << " decode_ns=" << stats.decodeTimeNs - beforeStats.decodeTimeNs
              << "\n";
  }
  if (expectedRows.has_value()) {
    BOLT_CHECK_EQ(count, expectedRows.value());
  }
  return count;
}

uint64_t nativeReader(uint32_t iterations) {
  uint64_t rows = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    dwio::common::ReaderOptions options(readerPool.get());
    NativeLanceReader reader(open(lanceFilePath), options);
    rows += scan(reader);
    if (printStats) {
      const auto stats = reader.debugStats();
      std::cerr << "BOLT_LANCE_CACHE_STATS mode=native"
                << " cache_hits=" << stats.decompressedCacheHits
                << " cache_misses=" << stats.decompressedCacheMisses
                << " compressed_bytes=" << stats.compressedBytesRead
                << " decompressed_bytes=" << stats.decompressedBytesProduced
                << "\n";
    }
  }
  return rows;
}

uint64_t parquetReader(uint32_t iterations) {
  uint64_t rows = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    dwio::common::ReaderOptions options(readerPool.get());
    parquet::ParquetReader reader(open(parquetFilePath), options);
    rows += scan(reader);
  }
  return rows;
}

BENCHMARK_MULTI(scan, n) {
  if (readerMode == "native") {
    return nativeReader(n);
  }
  if (readerMode == "parquet") {
    return parquetReader(n);
  }
  BOLT_FAIL("BOLT_LANCE_READER_MODE must be 'native' or 'parquet'");
}

} // namespace
} // namespace bytedance::bolt::lance::reader::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  using namespace bytedance::bolt::lance::reader::benchmark;
  if ((readerMode == "parquet" && parquetFilePath.empty()) ||
      (readerMode != "parquet" && lanceFilePath.empty()) ||
      (readerMode != "native" && readerMode != "parquet")) {
    std::cerr << "set BOLT_LANCE_BENCHMARK_FILE, "
                 "BOLT_LANCE_BENCHMARK_PARQUET_FILE for parquet mode, and set "
                 "BOLT_LANCE_READER_MODE to native or parquet\n";
    return 1;
  }
  bytedance::bolt::memory::MemoryManager::initialize({});
  rootPool = bytedance::bolt::memory::memoryManager()->addRootPool(
      "native_lance_benchmark");
  readerPool = rootPool->addLeafChild("reader");
  if (decodingThreads > 1) {
    decodingExecutor =
        std::make_shared<folly::CPUThreadPoolExecutor>(decodingThreads - 1);
  }
  folly::runBenchmarks();
  decodingExecutor.reset();
  readerPool.reset();
  rootPool.reset();
  return 0;
}
