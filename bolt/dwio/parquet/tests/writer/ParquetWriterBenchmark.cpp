/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/arrow/Properties.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::test;

const uint32_t kNumBatches = 50;
const uint32_t kNumRowsPerRowGroup = 10000;

class ParquetWriterBenchmark {
 public:
  explicit ParquetWriterBenchmark(
      bool disableDictionary,
      const RowTypePtr& rowType)
      : disableDictionary_(disableDictionary) {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetWriterBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetWriterBenchmark");
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);
    auto path = fileFolder_->path + "/" + fileName_;
    auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
    bytedance::bolt::parquet::WriterOptions options;
    options.enableFlushBasedOnBlockSize = true;
    options.parquetWriteTimestampUnit = TimestampUnit::kNano;
    options.writeInt96AsTimestamp = true;
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

  ~ParquetWriterBenchmark() {}

  void writeToFile(
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) {
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
    writer_->close();
  }

  void writeSingleColumn(
      const std::string& columnName,
      const TypePtr& type,
      uint8_t nullsRateX100,
      uint32_t batchSize) {
    folly::BenchmarkSuspender suspender;

    auto rowType = ROW({columnName}, {type});
    // Generating the data (consider the null rate).
    auto batches = dataSetBuilder_->makeDataset(rowType, kNumBatches, batchSize)
                       .withRowGroupSpecificData(kNumRowsPerRowGroup)
                       .withNullsForField(Subfield(columnName), nullsRateX100)
                       .build();
    suspender.dismiss();
    writeToFile(*batches, true);
  }

 private:
  const std::string fileName_ = "test.parquet";
  const std::shared_ptr<bytedance::bolt::exec::test::TempDirectoryPath>
      fileFolder_ = bytedance::bolt::exec::test::TempDirectoryPath::create();
  const bool disableDictionary_;

  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::unique_ptr<bytedance::bolt::parquet::Writer> writer_;
};

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    uint8_t nullsRateX100,
    uint32_t batchSize,
    bool disableDictionary) {
  RowTypePtr rowType = ROW({columnName}, {type});
  ParquetWriterBenchmark benchmark(disableDictionary, rowType);
  BIGINT()->toString();
  benchmark.writeSingleColumn(columnName, type, nullsRateX100, batchSize);
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

PARQUET_BENCHMARKS(VARCHAR(), Varchar);
PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);
PARQUET_BENCHMARKS(DECIMAL(18, 3), ShortDecimalType);
PARQUET_BENCHMARKS(DECIMAL(38, 3), LongDecimalType);
PARQUET_BENCHMARKS(MAP(BIGINT(), BIGINT()), Map);
PARQUET_BENCHMARKS(ARRAY(BIGINT()), List);

// TODO: Add all data types

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
