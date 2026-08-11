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

#include "bolt/common/file/File.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/arrow/Properties.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <iostream>
#include <mutex>
#include <unordered_set>
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::test;

constexpr uint32_t kNumBatches = 50;
constexpr uint32_t kNumRowsPerRowGroup = 10000;
constexpr int64_t kNoSplitDataPageSize = 64 * 1024 * 1024;
constexpr int64_t kNoSplitSinkCapacity = 8 * 1024 * 1024;

struct WriteMetrics {
  uint64_t outputSize;
  uint64_t dataPageCount;
};

class ParquetWriterBenchmark {
 public:
  explicit ParquetWriterBenchmark(
      bool disableDictionary,
      const RowTypePtr& rowType,
      int64_t dataPageSize,
      bool useMemorySink)
      : disableDictionary_(disableDictionary) {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetWriterBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetWriterBenchmark");
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);
    std::unique_ptr<FileSink> sink;
    if (useMemorySink) {
      auto memorySink = std::make_unique<MemorySink>(
          kNoSplitSinkCapacity, FileSink::Options{.pool = leafPool_.get()});
      sink_ = memorySink.get();
      sink = std::move(memorySink);
    } else {
      fileFolder_ = bytedance::bolt::exec::test::TempDirectoryPath::create();
      auto path = fileFolder_->path + "/test.parquet";
      auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
      sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
    }
    bytedance::bolt::parquet::WriterOptions options;
    options.enableFlushBasedOnBlockSize = true;
    options.parquetWriteTimestampUnit = TimestampUnit::kNano;
    options.writeInt96AsTimestamp = true;
    options.dataPageSize = dataPageSize;
    options.dataPageVersion =
        bytedance::bolt::parquet::arrow::ParquetDataPageVersion::V2;
    if (disableDictionary_) {
      // The parquet file is in plain encoding format.
      options.enableDictionary = false;
    }
    options.memoryPool = rootPool_.get();
    writer_ = std::make_unique<bytedance::bolt::parquet::Writer>(
        std::move(sink), options, rowType);
  }

  ~ParquetWriterBenchmark() = default;

  void writeToSink(
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) {
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
    writer_->close();
  }

  std::unique_ptr<std::vector<RowVectorPtr>> makeSingleColumnData(
      const std::string& columnName,
      const TypePtr& type,
      uint8_t nullsRateX100,
      uint32_t numBatches,
      uint32_t batchSize) {
    auto rowType = ROW({columnName}, {type});
    // Generating the data (consider the null rate).
    return dataSetBuilder_->makeDataset(rowType, numBatches, batchSize)
        .withRowGroupSpecificData(kNumRowsPerRowGroup)
        .withNullsForField(Subfield(columnName), nullsRateX100)
        .build();
  }

  WriteMetrics collectMetrics() const {
    BOLT_CHECK_NOT_NULL(sink_);
    std::string_view data(sink_->data(), sink_->size());
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    ParquetReader reader(
        std::make_unique<BufferedInput>(
            std::make_shared<InMemoryReadFile>(data),
            readerOptions.getMemoryPool()),
        readerOptions);

    uint64_t dataPageCount = 0;
    const auto metadata = reader.fileMetaData();
    for (int rowGroupIndex = 0; rowGroupIndex < metadata.numRowGroups();
         ++rowGroupIndex) {
      const auto rowGroup = metadata.rowGroup(rowGroupIndex);
      for (int columnIndex = 0; columnIndex < rowGroup.numColumns();
           ++columnIndex) {
        for (const auto& stats :
             rowGroup.columnChunk(columnIndex).pageEncodingStats()) {
          if (stats.page_type == thrift::PageType::DATA_PAGE ||
              stats.page_type == thrift::PageType::DATA_PAGE_V2) {
            dataPageCount += stats.count;
          }
        }
      }
    }
    return {
        .outputSize = static_cast<uint64_t>(sink_->size()),
        .dataPageCount = dataPageCount};
  }

 private:
  const bool disableDictionary_;

  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<bytedance::bolt::exec::test::TempDirectoryPath> fileFolder_;
  MemorySink* sink_{nullptr};
  std::unique_ptr<bytedance::bolt::parquet::Writer> writer_;
};

void reportLayoutOnce(
    const std::string& benchmarkName,
    const WriteMetrics& metrics) {
  static std::mutex mutex;
  static std::unordered_set<std::string> reportedBenchmarks;
  std::lock_guard<std::mutex> lock(mutex);
  if (reportedBenchmarks.insert(benchmarkName).second) {
    std::cerr << fmt::format(
        "LAYOUT {} output_bytes={} data_pages={}\n",
        benchmarkName,
        metrics.outputSize,
        metrics.dataPageCount);
  }
}

void runImpl(
    uint32_t iterations,
    const std::string& columnName,
    const TypePtr& type,
    uint8_t nullsRateX100,
    uint32_t batchSize,
    uint32_t numBatches,
    bool disableDictionary,
    int64_t dataPageSize,
    bool reportLayout) {
  RowTypePtr rowType = ROW({columnName}, {type});
  for (uint32_t i = 0; i < iterations; ++i) {
    // Measure only write/flush/close. Writer construction, input generation,
    // and output metadata inspection are deliberately suspended so the
    // benchmark remains sensitive to the ColumnWriter hot path.
    folly::BenchmarkSuspender suspender;
    ParquetWriterBenchmark benchmark(
        disableDictionary, rowType, dataPageSize, reportLayout);
    const auto batches = benchmark.makeSingleColumnData(
        columnName, type, nullsRateX100, numBatches, batchSize);
    suspender.dismiss();
    benchmark.writeToSink(*batches, true);
    suspender.rehire();

    if (reportLayout) {
      const auto metrics = benchmark.collectMetrics();
      folly::doNotOptimizeAway(metrics.outputSize);
      folly::doNotOptimizeAway(metrics.dataPageCount);
      BOLT_CHECK_EQ(metrics.dataPageCount, 1);
      reportLayoutOnce(
          fmt::format(
              "{}_batch_{}_null{}", columnName, batchSize, nullsRateX100),
          metrics);
    }
  }
}

void run(
    uint32_t iterations,
    const std::string& columnName,
    const TypePtr& type,
    uint8_t nullsRateX100,
    uint32_t batchSize,
    bool disableDictionary) {
  runImpl(
      iterations,
      columnName,
      type,
      nullsRateX100,
      batchSize,
      kNumBatches,
      disableDictionary,
      bytedance::bolt::parquet::WriterOptions{}.dataPageSize,
      false);
}

void runNoSplit(
    uint32_t iterations,
    const std::string& columnName,
    const TypePtr& type,
    uint8_t nullsRateX100,
    uint32_t batchSize) {
  runImpl(
      iterations,
      columnName,
      type,
      nullsRateX100,
      batchSize,
      1,
      true,
      kNoSplitDataPageSize,
      true);
}

void runDictionaryNoSplit(
    uint32_t iterations,
    const std::string& columnName,
    const TypePtr& type,
    uint8_t nullsRateX100,
    uint32_t batchSize) {
  runImpl(
      iterations,
      columnName,
      type,
      nullsRateX100,
      batchSize,
      1,
      false,
      kNoSplitDataPageSize,
      true);
}

#define PARQUET_BENCHMARKS_NULLS(_type_, _name_, _null_)                      \
  BENCHMARK_NAMED_PARAM(                                                      \
      run, _name_##_batch_4k_dict, #_name_, _type_, _null_, 4096, false);     \
  BENCHMARK_NAMED_PARAM(                                                      \
      run, _name_##_batch_32k_dict, #_name_, _type_, _null_, 32768, false);   \
  BENCHMARK_NAMED_PARAM(                                                      \
      run, _name_##_batch_256k_dict, #_name_, _type_, _null_, 262144, false); \
  BENCHMARK_NAMED_PARAM(                                                      \
      run, _name_##_batch_1M_dict, #_name_, _type_, _null_, 1048576, false);  \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS(_type_, _name_) \
  PARQUET_BENCHMARKS_NULLS(_type_, _name_, 20)

#define PARQUET_NO_SPLIT_BENCHMARKS_NULLS(_type_, _name_, _null_) \
  BENCHMARK_NAMED_PARAM(                                          \
      runNoSplit,                                                 \
      _name_##_batch_4k_null##_null_,                             \
      #_name_,                                                    \
      _type_,                                                     \
      _null_,                                                     \
      4096);                                                      \
  BENCHMARK_NAMED_PARAM(                                          \
      runNoSplit,                                                 \
      _name_##_batch_32k_null##_null_,                            \
      #_name_,                                                    \
      _type_,                                                     \
      _null_,                                                     \
      32768);                                                     \
  BENCHMARK_NAMED_PARAM(                                          \
      runNoSplit,                                                 \
      _name_##_batch_256k_null##_null_,                           \
      #_name_,                                                    \
      _type_,                                                     \
      _null_,                                                     \
      262144);                                                    \
  BENCHMARK_DRAW_LINE();

#define PARQUET_NESTED_NO_SPLIT_BENCHMARKS_NULLS(_type_, _name_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                 \
      runNoSplit,                                                        \
      _name_##_batch_4k_null##_null_,                                    \
      #_name_,                                                           \
      _type_,                                                            \
      _null_,                                                            \
      4096);                                                             \
  BENCHMARK_NAMED_PARAM(                                                 \
      runNoSplit,                                                        \
      _name_##_batch_32k_null##_null_,                                   \
      #_name_,                                                           \
      _type_,                                                            \
      _null_,                                                            \
      32768);                                                            \
  BENCHMARK_DRAW_LINE();

#define PARQUET_DICTIONARY_NO_SPLIT_BENCHMARKS_NULLS(_type_, _name_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                     \
      runDictionaryNoSplit,                                                  \
      _name_##_batch_4k_null##_null_,                                        \
      #_name_,                                                               \
      _type_,                                                                \
      _null_,                                                                \
      4096);                                                                 \
  BENCHMARK_NAMED_PARAM(                                                     \
      runDictionaryNoSplit,                                                  \
      _name_##_batch_32k_null##_null_,                                       \
      #_name_,                                                               \
      _type_,                                                                \
      _null_,                                                                \
      32768);                                                                \
  BENCHMARK_DRAW_LINE();

PARQUET_BENCHMARKS(VARCHAR(), Varchar);
PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);
PARQUET_BENCHMARKS(DECIMAL(18, 3), ShortDecimalType);
PARQUET_BENCHMARKS(DECIMAL(38, 3), LongDecimalType);
PARQUET_BENCHMARKS(MAP(BIGINT(), BIGINT()), Map);
PARQUET_BENCHMARKS(ARRAY(BIGINT()), List);

// These cases use one batch of short values and a 64 MiB page target. Both the
// baseline and the fixed writer must therefore produce exactly one data page.
// This isolates the no-split BYTE_ARRAY hot path and prints output size/page
// count so page-layout changes cannot be mistaken for CPU improvements.
PARQUET_NO_SPLIT_BENCHMARKS_NULLS(VARCHAR(), VarcharPlainNoSplit, 0);
PARQUET_NO_SPLIT_BENCHMARKS_NULLS(VARCHAR(), VarcharPlainNoSplit, 20);
PARQUET_NO_SPLIT_BENCHMARKS_NULLS(VARBINARY(), VarbinaryPlainNoSplit, 20);
PARQUET_NESTED_NO_SPLIT_BENCHMARKS_NULLS(
    ARRAY(VARCHAR()),
    VarcharListPlainNoSplit,
    20);
PARQUET_DICTIONARY_NO_SPLIT_BENCHMARKS_NULLS(
    VARCHAR(),
    VarcharDictionaryNoSplit,
    20);

// TODO: Add all data types

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
