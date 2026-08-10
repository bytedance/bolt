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

#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "bolt/common/base/Fs.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/core/Config.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"

#ifdef BOLT_ENABLE_HDFS
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#endif

using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

DEFINE_string(
    parquet_memory_input_file,
    "",
    "Absolute path of the parquet file to read for memory baseline.");
DEFINE_uint64(
    parquet_memory_next_batch_size,
    1000,
    "Batch size passed to RowReader::next().");
DEFINE_int32(
    parquet_memory_repdef_preload_window_count,
    0,
    "Parquet rep/def preload window count. 0 disables window preload.");
DEFINE_bool(
    parquet_memory_verbose_batches,
    false,
    "Print per-batch RSS and memory-pool stats.");

namespace {

struct ProcessRssStats {
  int64_t vmRssKb{-1};
  int64_t vmHwmKb{-1};
  int64_t ruMaxRssKb{-1};
};

std::optional<int64_t> readProcStatusValueKb(const std::string& key) {
  std::ifstream input("/proc/self/status");
  if (!input.is_open()) {
    return std::nullopt;
  }

  const auto prefix = key + ":";
  std::string line;
  while (std::getline(input, line)) {
    if (line.rfind(prefix, 0) != 0) {
      continue;
    }

    std::istringstream lineStream(line.substr(prefix.size()));
    int64_t value = 0;
    std::string unit;
    if (lineStream >> value >> unit) {
      return value;
    }
    return std::nullopt;
  }
  return std::nullopt;
}

ProcessRssStats captureProcessRssStats() {
  struct rusage usage {};
  getrusage(RUSAGE_SELF, &usage);
  return ProcessRssStats{
      .vmRssKb = readProcStatusValueKb("VmRSS").value_or(-1),
      .vmHwmKb = readProcStatusValueKb("VmHWM").value_or(-1),
      .ruMaxRssKb = usage.ru_maxrss};
}

bool startsWith(std::string_view value, std::string_view prefix) {
  return value.size() >= prefix.size() &&
      value.compare(0, prefix.size(), prefix) == 0;
}

bool isHdfsPath(std::string_view path) {
  return startsWith(path, "hdfs://") || startsWith(path, "viewfs://");
}

struct ReadIoStats {
  std::atomic<uint64_t> preadCalls{0};
  std::atomic<uint64_t> preadBytes{0};
  std::atomic<uint64_t> preadvCalls{0};
  std::atomic<uint64_t> preadvRegions{0};
  std::atomic<uint64_t> preadvBytes{0};
};

class InstrumentedReadFile final : public ReadFile {
 public:
  InstrumentedReadFile(std::shared_ptr<ReadFile> inner, ReadIoStats& stats)
      : inner_(std::move(inner)), stats_(stats) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const override {
    stats_.preadCalls.fetch_add(1, std::memory_order_relaxed);
    stats_.preadBytes.fetch_add(length, std::memory_order_relaxed);
    return inner_->pread(offset, length, buf);
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    stats_.preadCalls.fetch_add(1, std::memory_order_relaxed);
    stats_.preadBytes.fetch_add(length, std::memory_order_relaxed);
    return inner_->pread(offset, length);
  }

  void preadv(
      folly::Range<const bytedance::bolt::common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const override {
    stats_.preadvCalls.fetch_add(1, std::memory_order_relaxed);
    stats_.preadvRegions.fetch_add(regions.size(), std::memory_order_relaxed);
    uint64_t bytes = 0;
    for (const auto& region : regions) {
      bytes += region.length;
    }
    stats_.preadvBytes.fetch_add(bytes, std::memory_order_relaxed);
    inner_->preadv(regions, iobufs);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    stats_.preadvCalls.fetch_add(1, std::memory_order_relaxed);
    stats_.preadvRegions.fetch_add(buffers.size(), std::memory_order_relaxed);
    uint64_t bytes = 0;
    for (const auto& buffer : buffers) {
      bytes += buffer.size();
    }
    stats_.preadvBytes.fetch_add(bytes, std::memory_order_relaxed);
    return inner_->preadv(offset, buffers);
  }

  bool shouldCoalesce() const override {
    return inner_->shouldCoalesce();
  }

  uint64_t size() const override {
    return inner_->size();
  }

  uint64_t memoryUsage() const override {
    return inner_->memoryUsage();
  }

  uint64_t bytesRead() const override {
    return inner_->bytesRead();
  }

  void resetBytesRead() override {
    inner_->resetBytesRead();
  }

  std::string getName() const override {
    return inner_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return inner_->getNaturalReadSize();
  }

 private:
  std::shared_ptr<ReadFile> inner_;
  ReadIoStats& stats_;
};

class ParquetReaderMemoryTest : public ParquetTestBase {};

TEST_F(ParquetReaderMemoryTest, readRealFileAndReportMemory) {
  ASSERT_FALSE(FLAGS_parquet_memory_input_file.empty())
      << "--parquet_memory_input_file must be provided";
  ASSERT_GT(FLAGS_parquet_memory_next_batch_size, 0)
      << "--parquet_memory_next_batch_size must be > 0";
  ASSERT_GE(FLAGS_parquet_memory_repdef_preload_window_count, 0)
      << "--parquet_memory_repdef_preload_window_count must be >= 0";

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  std::shared_ptr<ReadFile> readFile;
  if (isHdfsPath(FLAGS_parquet_memory_input_file)) {
#ifdef BOLT_ENABLE_HDFS
    filesystems::registerHdfsFileSystem();
    auto fsConfig = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    auto fileSystem =
        filesystems::getFileSystem(FLAGS_parquet_memory_input_file, fsConfig);
    ASSERT_NE(fileSystem, nullptr) << "failed to resolve HDFS filesystem for: "
                                   << FLAGS_parquet_memory_input_file;
    ASSERT_TRUE(fileSystem->exists(FLAGS_parquet_memory_input_file))
        << "input parquet file does not exist: "
        << FLAGS_parquet_memory_input_file;
    readFile = fileSystem->openFileForRead(FLAGS_parquet_memory_input_file);
#else
    FAIL() << "HDFS input requires BOLT_ENABLE_HDFS=ON: "
           << FLAGS_parquet_memory_input_file;
#endif
  } else {
    ASSERT_TRUE(fs::exists(FLAGS_parquet_memory_input_file))
        << "input parquet file does not exist: "
        << FLAGS_parquet_memory_input_file;
    readFile = std::make_shared<LocalReadFile>(FLAGS_parquet_memory_input_file);
  }

  ReadIoStats readIoStats;
  readFile =
      std::make_shared<InstrumentedReadFile>(std::move(readFile), readIoStats);

  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, readerOptions.getMemoryPool());
  auto reader = std::make_unique<bytedance::bolt::parquet::ParquetReader>(
      std::move(input), readerOptions);
  auto rowType = reader->rowType();
  ASSERT_NE(rowType, nullptr);

  RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(
      std::make_shared<ColumnSelector>(rowType, rowType->names()));
  rowReaderOptions.setScanSpec(makeScanSpec(rowType));
  rowReaderOptions.setParquetRepDefPreloadWindowCount(
      FLAGS_parquet_memory_repdef_preload_window_count);
  auto rowReader = reader->createRowReader(rowReaderOptions);

  const auto fileRows = reader->numberOfRows().value_or(0);
  const auto fileMeta = reader->fileMetaData();
  const auto rowGroupCount = fileMeta.numRowGroups();

  auto rssBefore = captureProcessRssStats();
  int64_t sampledPeakRssKb = std::max<int64_t>(0, rssBefore.vmRssKb);
  uint64_t rowsRead = 0;
  uint64_t batches = 0;
  uint64_t maxBatchRows = 0;
  uint64_t rowReaderNextNs = 0;

  VectorPtr result = BaseVector::create(rowType, 0, leafPool_.get());
  while (true) {
    const auto nextStart = std::chrono::steady_clock::now();
    const auto numRows =
        rowReader->next(FLAGS_parquet_memory_next_batch_size, result);
    const auto nextEnd = std::chrono::steady_clock::now();
    rowReaderNextNs += std::chrono::duration_cast<std::chrono::nanoseconds>(
                           nextEnd - nextStart)
                           .count();
    if (numRows == 0) {
      break;
    }

    auto rowVector = result->asUnchecked<RowVector>();
    for (auto i = 0; i < rowVector->childrenSize(); ++i) {
      rowVector->childAt(i)->loadedVector();
    }

    rowsRead += rowVector->size();
    maxBatchRows = std::max<uint64_t>(maxBatchRows, rowVector->size());
    ++batches;

    const auto rssNow = captureProcessRssStats();
    sampledPeakRssKb = std::max(sampledPeakRssKb, rssNow.vmRssKb);

    if (FLAGS_parquet_memory_verbose_batches) {
      fmt::print(
          "PARQUET_MEMORY_BATCH batch={} rows={} vm_rss_kb={} vm_hwm_kb={} "
          "root_current_bytes={} root_peak_bytes={} leaf_current_bytes={} "
          "leaf_peak_bytes={}\n",
          batches,
          rowVector->size(),
          rssNow.vmRssKb,
          rssNow.vmHwmKb,
          rootPool_->currentBytes(),
          rootPool_->peakBytes(),
          leafPool_->currentBytes(),
          leafPool_->peakBytes());
    }
  }

  const auto rssAfter = captureProcessRssStats();

  fmt::print(
      "PARQUET_MEMORY_SUMMARY\n"
      "input_file={}\n"
      "next_batch_size={}\n"
      "repdef_preload_window_count={}\n"
      "column_count={}\n"
      "file_rows={}\n"
      "rows_read={}\n"
      "row_groups={}\n"
      "batches={}\n"
      "max_batch_rows={}\n"
      "row_reader_next_ns={}\n"
      "rss_before_kb={}\n"
      "rss_after_kb={}\n"
      "rss_sampled_peak_kb={}\n"
      "vm_hwm_kb={}\n"
      "ru_maxrss_kb={}\n"
      "root_current_bytes={}\n"
      "root_peak_bytes={}\n"
      "leaf_current_bytes={}\n"
      "leaf_peak_bytes={}\n"
      "pread_calls={}\n"
      "pread_bytes={}\n"
      "preadv_calls={}\n"
      "preadv_regions={}\n"
      "preadv_bytes={}\n",
      FLAGS_parquet_memory_input_file,
      FLAGS_parquet_memory_next_batch_size,
      FLAGS_parquet_memory_repdef_preload_window_count,
      rowType->size(),
      fileRows,
      rowsRead,
      rowGroupCount,
      batches,
      maxBatchRows,
      rowReaderNextNs,
      rssBefore.vmRssKb,
      rssAfter.vmRssKb,
      sampledPeakRssKb,
      rssAfter.vmHwmKb,
      rssAfter.ruMaxRssKb,
      rootPool_->currentBytes(),
      rootPool_->peakBytes(),
      leafPool_->currentBytes(),
      leafPool_->peakBytes(),
      readIoStats.preadCalls.load(std::memory_order_relaxed),
      readIoStats.preadBytes.load(std::memory_order_relaxed),
      readIoStats.preadvCalls.load(std::memory_order_relaxed),
      readIoStats.preadvRegions.load(std::memory_order_relaxed),
      readIoStats.preadvBytes.load(std::memory_order_relaxed));

  ASSERT_GT(rowsRead, 0);
  if (fileRows > 0) {
    ASSERT_EQ(rowsRead, fileRows);
  }
  ASSERT_GT(rootPool_->peakBytes(), 0);
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
