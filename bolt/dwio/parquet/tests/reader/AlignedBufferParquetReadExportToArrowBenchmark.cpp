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

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include <gtest/gtest.h>

#include "bolt/common/base/Fs.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::parquet {
namespace {

constexpr uint64_t kMaxBatchBytes = 2UL << 20;
std::string parquetFile;

class ArrowArrayGuard {
 public:
  ~ArrowArrayGuard() {
    if (array.release != nullptr) {
      array.release(&array);
    }
  }

  ArrowArray array{};
};

class ArrowSchemaGuard {
 public:
  ~ArrowSchemaGuard() {
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }

  ArrowSchema schema{};
};

memory::MemoryManager::Options memoryManagerOptions(
    bool enableAlignedBufAllocStrategy) {
  memory::MemoryManager::Options options;
  options.enableAlignedBufAllocStrategy = enableAlignedBufAllocStrategy;
  return options;
}

class ExportContext {
 public:
  explicit ExportContext(bool enableAlignedBufAllocStrategy)
      : memoryManager(memoryManagerOptions(enableAlignedBufAllocStrategy)),
        rootPool(memoryManager.addRootPool(fmt::format(
            "AlignedBufferParquetReadExportToArrowBenchmark.{}",
            enableAlignedBufAllocStrategy ? "enabled" : "disabled"))),
        readerPool(rootPool->addLeafChild("reader")),
        exportPool(rootPool->addLeafChild("export")) {}

  memory::MemoryManager memoryManager;
  std::shared_ptr<memory::MemoryPool> rootPool;
  std::shared_ptr<memory::MemoryPool> readerPool;
  std::shared_ptr<memory::MemoryPool> exportPool;
  std::queue<std::unique_ptr<ArrowArrayGuard>> arrowBatches;
};

struct BenchmarkResult {
  uint64_t exportedRows{0};
  int64_t peakBytes{0};
  int64_t elapsedMillis{0};
};

void runBenchmark(bool enableAlignedBufAllocStrategy, BenchmarkResult& result) {
  const auto start = std::chrono::steady_clock::now();
  ExportContext context(enableAlignedBufAllocStrategy);

  dwio::common::ReaderOptions readerOptions{context.readerPool.get()};
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(parquetFile), *context.readerPool);
  auto reader =
      std::make_unique<ParquetReader>(std::move(input), readerOptions);

  const auto rowType = reader->rowType();
  ASSERT_NE(rowType, nullptr);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      rowType, rowType->names()));
  auto scanSpec = std::make_shared<common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  rowReaderOptions.setScanSpec(scanSpec);
  // Keep the estimated batch size strictly below 2 MiB unless a single row is
  // already wider than the limit. ParquetRowReader refreshes its row-width
  // estimate for each row group and applies this cap internally as well.
  rowReaderOptions.setMaxBatchBytes(kMaxBatchBytes - 1);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  ArrowOptions arrowOptions{
      .flattenDictionary = true,
      .flattenConstant = true,
  };
  ArrowSchemaGuard arrowSchema;
  exportToArrow(
      BaseVector::create(rowType, 0, context.exportPool.get()),
      arrowSchema.schema,
      arrowOptions);
  ASSERT_NE(arrowSchema.schema.release, nullptr);
  ASSERT_EQ(arrowSchema.schema.n_children, rowType->size());

  VectorPtr batch = BaseVector::create(rowType, 0, context.readerPool.get());
  while (true) {
    const auto estimatedRowWidth =
        std::max<size_t>(1, rowReader->estimatedRowSize().value_or(1));
    const auto batchRows =
        std::max<uint64_t>(1, (kMaxBatchBytes - 1) / estimatedRowWidth);
    if (!rowReader->next(batchRows, batch)) {
      break;
    }
    ASSERT_NE(batch, nullptr);

    auto arrowBatch = std::make_unique<ArrowArrayGuard>();
    exportToArrow(
        batch, arrowBatch->array, context.exportPool.get(), arrowOptions);
    ASSERT_NE(arrowBatch->array.release, nullptr);
    EXPECT_EQ(arrowBatch->array.length, batch->size());
    EXPECT_EQ(arrowBatch->array.n_children, rowType->size());
    result.exportedRows += batch->size();
    // Keep every exported batch alive until peak memory is recorded.
    context.arrowBatches.push(std::move(arrowBatch));
  }

  ASSERT_TRUE(reader->numberOfRows().has_value());
  EXPECT_EQ(result.exportedRows, reader->numberOfRows().value());
  EXPECT_FALSE(context.arrowBatches.empty());
  result.peakBytes = context.rootPool->peakBytes();
  result.elapsedMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
}

TEST(AlignedBufferParquetReadExportToArrowBenchmark, comparePoolPeakBytes) {
  if (parquetFile.empty()) {
    GTEST_SKIP() << "Specify a Parquet file with --parquet_file=<path> or "
                    "BOLT_PARQUET_FILE=<path>";
  }

  if (!std::filesystem::is_regular_file(parquetFile)) {
    GTEST_SKIP() << "Parquet file does not exist: " << parquetFile;
  }

  BenchmarkResult strategyDisabled;
  runBenchmark(false, strategyDisabled);
  BenchmarkResult strategyEnabled;
  runBenchmark(true, strategyEnabled);

  ASSERT_EQ(strategyDisabled.exportedRows, strategyEnabled.exportedRows);
  std::cout << "\nAlignedBuffer allocation strategy MemoryPool peak:\n"
            << "  disabled: " << strategyDisabled.peakBytes << " bytes\n"
            << "  enabled : " << strategyEnabled.peakBytes << " bytes\n"
            << "  saved   : "
            << strategyDisabled.peakBytes - strategyEnabled.peakBytes
            << " bytes\n"
            << "Elapsed time:\n"
            << "  disabled: " << strategyDisabled.elapsedMillis << " ms\n"
            << "  enabled : " << strategyEnabled.elapsedMillis << " ms\n"
            << "  saved   : "
            << strategyDisabled.elapsedMillis - strategyEnabled.elapsedMillis
            << " ms\n";
}

} // namespace
} // namespace bytedance::bolt::parquet

int main(int argc, char** argv) {
  const std::string prefix = "--parquet_file=";
  int outputArgc = 1;
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    if (argument.starts_with(prefix)) {
      bytedance::bolt::parquet::parquetFile = argument.substr(prefix.size());
    } else {
      argv[outputArgc++] = argv[i];
    }
  }
  argc = outputArgc;

  if (bytedance::bolt::parquet::parquetFile.empty()) {
    if (const auto* file = std::getenv("BOLT_PARQUET_FILE")) {
      bytedance::bolt::parquet::parquetFile = file;
    }
  }

  bytedance::bolt::memory::MemoryManager::testingSetInstance(
      bytedance::bolt::memory::MemoryManager::Options{});
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
