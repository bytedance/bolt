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

#include <sys/resource.h>
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string_view>

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/lance/NativeLanceReader.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/type/Filter.h"

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
std::string scenario = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_SCENARIO");
  return value == nullptr ? std::string("full_scan") : std::string(value);
}();
std::string filterColumn = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_FILTER_COLUMN");
  return value == nullptr ? std::string("filter_key") : std::string(value);
}();
std::string checksumColumn = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_CHECKSUM_COLUMN");
  return value == nullptr ? std::string("row_id") : std::string(value);
}();

std::optional<uint64_t> optionalEnvU64(const char* name) {
  const auto* value = std::getenv(name);
  return value == nullptr ? std::nullopt
                          : std::optional<uint64_t>(std::stoull(value));
}

std::optional<uint64_t> expectedRows =
    optionalEnvU64("BOLT_LANCE_BENCHMARK_EXPECTED_ROWS");
std::optional<uint64_t> expectedOutputRows =
    optionalEnvU64("BOLT_LANCE_BENCHMARK_EXPECTED_OUTPUT_ROWS");
std::optional<uint64_t> expectedChecksum =
    optionalEnvU64("BOLT_LANCE_BENCHMARK_EXPECTED_CHECKSUM");
int64_t filterMin = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_FILTER_MIN");
  return value == nullptr ? 0 : std::stoll(value);
}();
int64_t filterMax = [] {
  const auto* value = std::getenv("BOLT_LANCE_BENCHMARK_FILTER_MAX");
  return value == nullptr ? 0 : std::stoll(value);
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
  auto* row = result->asUnchecked<RowVector>();
  row->loadedVector();
  for (size_t i = 0; i < row->childrenSize(); ++i) {
    folly::doNotOptimizeAway(row->childAt(i)->retainedSize());
  }
}

bool usesFilter() {
  return scenario == "filter_1pct_all_types" || scenario == "filter";
}

bool usesChecksum() {
  return scenario == "full_scan_all_types" || usesFilter() ||
      expectedChecksum.has_value();
}

struct ScanResult {
  uint64_t sourceRows;
  uint64_t inputRows;
  uint64_t outputRows;
  uint64_t checksum;
};

ScanResult scan(dwio::common::Reader& reader) {
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
    auto* field = scanSpec->addFieldRecursively(
        names[i], *reader.rowType()->childAt(fileColumn), i);
    // The benchmark consumes every projected column in the current batch.
    // Request eager extraction so lazy loaders cannot outlive their row group
    // when a selective reader advances across row groups.
    field->setExtractValues(true);
  }
  if (usesFilter()) {
    BOLT_USER_CHECK_LE(
        filterMin, filterMax, "benchmark filter minimum exceeds maximum");
    auto* filter = scanSpec->childByName(filterColumn);
    BOLT_USER_CHECK_NOT_NULL(
        filter, "benchmark filter column '{}' is not projected", filterColumn);
    filter->setFilter(
        std::make_unique<common::BigintRange>(filterMin, filterMax, false));
  }
  options.setScanSpec(std::move(scanSpec));
  auto rows = reader.createRowReader(options);
  dwio::common::RuntimeStatistics beforeStats;
  if (printStats) {
    rows->updateRuntimeStats(beforeStats);
  }
  VectorPtr result = BaseVector::create(
      selector->buildSelectedReordered(), 0, readerPool.get());
  BOLT_USER_CHECK(
      !usesChecksum() || reader.rowType()->containsChild(checksumColumn),
      "benchmark checksum column '{}' is missing",
      checksumColumn);
  uint64_t inputRows = 0;
  uint64_t outputRows = 0;
  uint64_t checksum = 0;
  uint64_t batches = 0;
  uint64_t maxOutputRetainedBytes = 0;
  while (const auto scanned = rows->next(batchSize, result)) {
    inputRows += scanned;
    outputRows += result->size();
    ++batches;
    maxOutputRetainedBytes =
        std::max<uint64_t>(maxOutputRetainedBytes, result->retainedSize());
    if (!skipMaterialize) {
      materialize(result);
    }
    if (usesChecksum()) {
      const auto outputChecksumChannel =
          result->type()->asRow().getChildIdx(checksumColumn);
      BOLT_USER_CHECK_GE(
          outputChecksumChannel,
          0,
          "benchmark checksum column '{}' is not projected",
          checksumColumn);
      const auto* values = result->asUnchecked<RowVector>()
                               ->childAt(outputChecksumChannel)
                               ->asUnchecked<SimpleVector<int64_t>>();
      for (vector_size_t row = 0; row < result->size(); ++row) {
        BOLT_USER_CHECK(
            !values->isNullAt(row),
            "benchmark checksum column '{}' contains nulls",
            checksumColumn);
        checksum += static_cast<uint64_t>(values->valueAt(row));
      }
    }
  }
  if (printStats) {
    dwio::common::RuntimeStatistics stats;
    rows->updateRuntimeStats(stats);
    struct rusage usage {};
    BOLT_CHECK_EQ(getrusage(RUSAGE_SELF, &usage), 0);
    std::ostringstream line;
    line << "BOLT_LANCE_BENCHMARK_STATS mode=" << readerMode
         << " scenario=" << scenario << " batches=" << batches
         << " input_rows=" << inputRows << " output_rows=" << outputRows
         << " checksum=" << checksum
         << " decode_ns=" << stats.decodeTimeNs - beforeStats.decodeTimeNs
         << " peak_rss_kb=" << usage.ru_maxrss
         << " pool_current_bytes=" << readerPool->currentBytes()
         << " pool_peak_bytes=" << readerPool->peakBytes()
         << " max_output_retained_bytes=" << maxOutputRetainedBytes << '\n';
    std::cerr << line.str();
  }
  const auto sourceRows = reader.numberOfRows().value_or(inputRows);
  if (expectedRows.has_value()) {
    BOLT_CHECK_EQ(sourceRows, expectedRows.value());
  }
  if (!usesFilter()) {
    BOLT_CHECK_EQ(inputRows, sourceRows);
  }
  if (expectedOutputRows.has_value()) {
    BOLT_CHECK_EQ(outputRows, expectedOutputRows.value());
  }
  if (expectedChecksum.has_value()) {
    BOLT_CHECK_EQ(checksum, expectedChecksum.value());
  }
  return {sourceRows, inputRows, outputRows, checksum};
}

uint64_t nativeReader(uint32_t iterations) {
  uint64_t rows = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    dwio::common::ReaderOptions options(readerPool.get());
    NativeLanceReader reader(open(lanceFilePath), options);
    rows += scan(reader).sourceRows;
  }
  return rows;
}

uint64_t parquetReader(uint32_t iterations) {
  uint64_t rows = 0;
  for (uint32_t i = 0; i < iterations; ++i) {
    dwio::common::ReaderOptions options(readerPool.get());
    parquet::ParquetReader reader(open(parquetFilePath), options);
    rows += scan(reader).sourceRows;
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
  if (scenario != "full_scan" && scenario != "full_scan_all_types" &&
      scenario != "filter" && scenario != "filter_1pct_all_types") {
    std::cerr << "BOLT_LANCE_BENCHMARK_SCENARIO must be 'full_scan', "
                 "'full_scan_all_types', 'filter', or "
                 "'filter_1pct_all_types'\n";
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
