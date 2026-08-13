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
#include "bolt/vector/tests/utils/VectorMaker.h"

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
constexpr vector_size_t kEncodedInputRows = 100'000;
constexpr vector_size_t kSchemaBatchRows = 10'000;
constexpr int64_t kNoSplitDataPageSize = 64 * 1024 * 1024;
constexpr int64_t kNoSplitSinkCapacity = 8 * 1024 * 1024;
constexpr int64_t kEncodedInputSinkCapacity = 16 * 1024 * 1024;

enum class WritePath { kDirect, kStaging };

struct WriteMetrics {
  uint64_t outputSize;
  uint64_t rowCount;
  uint64_t rowGroupCount;
  uint64_t dataPageCount;
  uint64_t dictionaryPageCount;
};

class ParquetWriterBenchmark {
 public:
  explicit ParquetWriterBenchmark(
      bool disableDictionary,
      const RowTypePtr& rowType,
      int64_t dataPageSize,
      bool useMemorySink,
      int64_t memorySinkCapacity = kNoSplitSinkCapacity,
      WritePath writePath = WritePath::kDirect)
      : disableDictionary_(disableDictionary) {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetWriterBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetWriterBenchmark");
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);
    std::unique_ptr<FileSink> sink;
    if (useMemorySink) {
      auto memorySink = std::make_unique<MemorySink>(
          memorySinkCapacity, FileSink::Options{.pool = leafPool_.get()});
      sink_ = memorySink.get();
      sink = std::move(memorySink);
    } else {
      fileFolder_ = bytedance::bolt::exec::test::TempDirectoryPath::create();
      auto path = fileFolder_->path + "/test.parquet";
      auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
      sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
    }
    bytedance::bolt::parquet::WriterOptions options;
    options.enableFlushBasedOnBlockSize = writePath == WritePath::kDirect;
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

  memory::MemoryPool* memoryPool() const {
    return leafPool_.get();
  }

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
    uint64_t dictionaryPageCount = 0;
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
          } else if (stats.page_type == thrift::PageType::DICTIONARY_PAGE) {
            dictionaryPageCount += stats.count;
          }
        }
      }
    }
    return {
        .outputSize = static_cast<uint64_t>(sink_->size()),
        .rowCount = static_cast<uint64_t>(metadata.numRows()),
        .rowGroupCount = static_cast<uint64_t>(metadata.numRowGroups()),
        .dataPageCount = dataPageCount,
        .dictionaryPageCount = dictionaryPageCount};
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
        "LAYOUT {} output_bytes={} rows={} row_groups={} data_pages={} "
        "dictionary_pages={}\n",
        benchmarkName,
        metrics.outputSize,
        metrics.rowCount,
        metrics.rowGroupCount,
        metrics.dataPageCount,
        metrics.dictionaryPageCount);
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
    uint64_t expectedRows = 0;
    for (const auto& batch : *batches) {
      expectedRows += batch->size();
    }
    suspender.dismiss();
    benchmark.writeToSink(*batches, true);
    suspender.rehire();

    if (reportLayout) {
      const auto metrics = benchmark.collectMetrics();
      folly::doNotOptimizeAway(metrics.outputSize);
      folly::doNotOptimizeAway(metrics.rowCount);
      folly::doNotOptimizeAway(metrics.dataPageCount);
      BOLT_CHECK_EQ(metrics.rowCount, expectedRows);
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

template <typename T, typename MakeValue>
VectorPtr makeDictionaryVector(
    vector_size_t numRows,
    int32_t dictionarySize,
    memory::MemoryPool* pool,
    MakeValue makeValue) {
  test::VectorMaker maker(pool);
  auto dictionary = maker.flatVector<T>(dictionarySize, std::move(makeValue));
  auto indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    rawIndices[row] = row % dictionarySize;
  }
  return BaseVector::wrapInDictionary(
      nullptr, std::move(indices), numRows, std::move(dictionary));
}

VectorPtr makeDictionaryVarchar(
    vector_size_t numRows,
    int32_t dictionarySize,
    memory::MemoryPool* pool) {
  return makeDictionaryVector<std::string>(
      numRows, dictionarySize, pool, [](vector_size_t row) {
        return fmt::format("value_{:06d}", row);
      });
}

VectorPtr makeDictionaryInteger(
    vector_size_t numRows,
    int32_t dictionarySize,
    memory::MemoryPool* pool) {
  return makeDictionaryVector<int32_t>(
      numRows, dictionarySize, pool, [](vector_size_t row) {
        return static_cast<int32_t>(row * 7);
      });
}

VectorPtr makeNestedDictionaryInteger(
    vector_size_t numRows,
    int32_t dictionarySize,
    memory::MemoryPool* pool) {
  auto innerDictionary = makeDictionaryInteger(numRows, dictionarySize, pool);
  auto outerIndices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* rawOuterIndices = outerIndices->asMutable<vector_size_t>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    rawOuterIndices[row] = row;
  }
  return BaseVector::wrapInDictionary(
      nullptr, std::move(outerIndices), numRows, std::move(innerDictionary));
}

RowTypePtr makeRepeatedRowType(
    int32_t numColumns,
    const TypePtr& type,
    bool appendInteger = false) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(numColumns + static_cast<int32_t>(appendInteger));
  types.reserve(numColumns + static_cast<int32_t>(appendInteger));
  for (int32_t i = 0; i < numColumns; ++i) {
    names.push_back(fmt::format("c{}", i));
    types.push_back(type);
  }
  if (appendInteger) {
    names.push_back("nested");
    types.push_back(INTEGER());
  }
  return ROW(std::move(names), std::move(types));
}

template <typename MakeBatches>
void runEncodedInputBenchmark(
    uint32_t iterations,
    const std::string& benchmarkName,
    const RowTypePtr& rowType,
    MakeBatches makeBatches,
    WritePath writePath = WritePath::kDirect) {
  for (uint32_t i = 0; i < iterations; ++i) {
    folly::BenchmarkSuspender suspender;
    ParquetWriterBenchmark benchmark(
        false,
        rowType,
        bytedance::bolt::parquet::WriterOptions{}.dataPageSize,
        true,
        kEncodedInputSinkCapacity,
        writePath);
    auto batches = makeBatches(benchmark.memoryPool());
    uint64_t expectedRows = 0;
    for (const auto& batch : batches) {
      expectedRows += batch->size();
    }
    suspender.dismiss();
    benchmark.writeToSink(batches, false);
    suspender.rehire();

    const auto metrics = benchmark.collectMetrics();
    folly::doNotOptimizeAway(metrics.outputSize);
    folly::doNotOptimizeAway(metrics.rowCount);
    folly::doNotOptimizeAway(metrics.rowGroupCount);
    folly::doNotOptimizeAway(metrics.dataPageCount);
    folly::doNotOptimizeAway(metrics.dictionaryPageCount);
    BOLT_CHECK_EQ(metrics.rowCount, expectedRows);
    reportLayoutOnce(benchmarkName, metrics);
  }
}

void runInputDictionaryVarchar(uint32_t iterations, int32_t dictionarySize) {
  runEncodedInputBenchmark(
      iterations,
      fmt::format("InputDictionaryVarchar_Card{}", dictionarySize),
      ROW({"c0"}, {VARCHAR()}),
      [dictionarySize](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        return std::vector<RowVectorPtr>{maker.rowVector(
            {"c0"},
            {makeDictionaryVarchar(kEncodedInputRows, dictionarySize, pool)})};
      });
}

void runInputDictionaryInteger(uint32_t iterations, int32_t dictionarySize) {
  runEncodedInputBenchmark(
      iterations,
      fmt::format("InputDictionaryInteger_Card{}", dictionarySize),
      ROW({"c0"}, {INTEGER()}),
      [dictionarySize](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        return std::vector<RowVectorPtr>{maker.rowVector(
            {"c0"},
            {makeDictionaryInteger(kEncodedInputRows, dictionarySize, pool)})};
      });
}

void runInputDictionaryControl(
    uint32_t iterations,
    const std::string& typeName) {
  const bool isVarchar = typeName == "varchar";
  TypePtr type;
  if (isVarchar) {
    type = VARCHAR();
  } else {
    type = INTEGER();
  }
  runEncodedInputBenchmark(
      iterations,
      fmt::format("InputDictionaryControl_Flat{}", typeName),
      ROW({"c0"}, {type}),
      [isVarchar](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        VectorPtr column;
        if (isVarchar) {
          column = maker.flatVector<std::string>(
              kEncodedInputRows, [](vector_size_t row) {
                return fmt::format("value_{:06d}", row % 10);
              });
        } else {
          column = maker.flatVector<int32_t>(
              kEncodedInputRows,
              [](vector_size_t row) { return static_cast<int32_t>(row); });
        }
        return std::vector<RowVectorPtr>{
            maker.rowVector({"c0"}, {std::move(column)})};
      });
}

void runMixedInputEncodings(
    uint32_t iterations,
    int32_t numDictionaryVarcharColumns) {
  runEncodedInputBenchmark(
      iterations,
      fmt::format(
          "MixedInput_{}DictionaryVarchar_1NestedDictionaryInteger",
          numDictionaryVarcharColumns),
      makeRepeatedRowType(numDictionaryVarcharColumns, VARCHAR(), true),
      [numDictionaryVarcharColumns](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        std::vector<std::string> names;
        std::vector<VectorPtr> columns;
        names.reserve(numDictionaryVarcharColumns + 1);
        columns.reserve(numDictionaryVarcharColumns + 1);
        for (int32_t i = 0; i < numDictionaryVarcharColumns; ++i) {
          names.push_back(fmt::format("c{}", i));
          columns.push_back(makeDictionaryVarchar(kEncodedInputRows, 10, pool));
        }
        names.push_back("nested");
        columns.push_back(
            makeNestedDictionaryInteger(kEncodedInputRows, 50, pool));
        return std::vector<RowVectorPtr>{
            maker.rowVector(std::move(names), columns)};
      });
}

void runMultiColumnInputDictionary(uint32_t iterations, int32_t numColumns) {
  runEncodedInputBenchmark(
      iterations,
      fmt::format("InputDictionaryVarchar_{}Columns", numColumns),
      makeRepeatedRowType(numColumns, VARCHAR()),
      [numColumns](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        std::vector<std::string> names;
        std::vector<VectorPtr> columns;
        names.reserve(numColumns);
        columns.reserve(numColumns);
        for (int32_t i = 0; i < numColumns; ++i) {
          names.push_back(fmt::format("c{}", i));
          columns.push_back(makeDictionaryVarchar(kEncodedInputRows, 10, pool));
        }
        return std::vector<RowVectorPtr>{
            maker.rowVector(std::move(names), columns)};
      });
}

void runSchemaMultiBatch(
    uint32_t iterations,
    WritePath writePath,
    int32_t numColumns,
    int32_t numBatches) {
  const auto pathName = writePath == WritePath::kDirect ? "Direct" : "Staging";
  runEncodedInputBenchmark(
      iterations,
      fmt::format(
          "SchemaMultiBatch_{}_{}Columns_{}Batches",
          pathName,
          numColumns,
          numBatches),
      makeRepeatedRowType(numColumns, VARCHAR()),
      [numColumns, numBatches](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        std::vector<std::string> names;
        std::vector<VectorPtr> columns;
        names.reserve(numColumns);
        columns.reserve(numColumns);
        for (int32_t i = 0; i < numColumns; ++i) {
          names.push_back(fmt::format("c{}", i));
          columns.push_back(makeDictionaryVarchar(kSchemaBatchRows, 10, pool));
        }
        auto batch = maker.rowVector(std::move(names), columns);
        return std::vector<RowVectorPtr>(numBatches, std::move(batch));
      },
      writePath);
}

void runMapVarcharInteger(uint32_t iterations, int32_t entriesPerRow) {
  runEncodedInputBenchmark(
      iterations,
      fmt::format("MapVarcharInteger_{}Entries", entriesPerRow),
      ROW({"c0"}, {MAP(VARCHAR(), INTEGER())}),
      [entriesPerRow](memory::MemoryPool* pool) {
        test::VectorMaker maker(pool);
        auto map = maker.mapVector<std::string, int32_t>(
            kEncodedInputRows,
            [entriesPerRow](vector_size_t /*row*/) { return entriesPerRow; },
            [entriesPerRow](vector_size_t index) {
              return fmt::format("key_{:02d}", index % entriesPerRow);
            },
            [](vector_size_t index) { return static_cast<int32_t>(index); });
        return std::vector<RowVectorPtr>{
            maker.rowVector({"c0"}, {std::move(map)})};
      });
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

// Input vector encoding and schema-shape benchmarks.
BENCHMARK_NAMED_PARAM(runInputDictionaryVarchar, Card10, 10);
BENCHMARK_NAMED_PARAM(runInputDictionaryVarchar, Card100, 100);
BENCHMARK_NAMED_PARAM(runInputDictionaryVarchar, Card1000, 1'000);
BENCHMARK_NAMED_PARAM(runInputDictionaryVarchar, Card10000, 10'000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(runInputDictionaryInteger, Card10, 10);
BENCHMARK_NAMED_PARAM(runInputDictionaryInteger, Card100, 100);
BENCHMARK_NAMED_PARAM(runInputDictionaryInteger, Card1000, 1'000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(runInputDictionaryControl, FlatVarchar, "varchar");
BENCHMARK_NAMED_PARAM(runInputDictionaryControl, FlatInteger, "integer");
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(runMixedInputEncodings, OneDictionaryColumn, 1);
BENCHMARK_NAMED_PARAM(runMixedInputEncodings, FiveDictionaryColumns, 5);
BENCHMARK_NAMED_PARAM(runMixedInputEncodings, TenDictionaryColumns, 10);
BENCHMARK_NAMED_PARAM(runMixedInputEncodings, TwentyDictionaryColumns, 20);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(runMultiColumnInputDictionary, FiveColumns, 5);
BENCHMARK_NAMED_PARAM(runMultiColumnInputDictionary, TenColumns, 10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    DirectFiveColumns50Batches,
    WritePath::kDirect,
    5,
    50);
BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    DirectTenColumns50Batches,
    WritePath::kDirect,
    10,
    50);
BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    DirectTwentyColumns50Batches,
    WritePath::kDirect,
    20,
    50);
BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    DirectTenColumns200Batches,
    WritePath::kDirect,
    10,
    200);
BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    StagingTenColumns50Batches,
    WritePath::kStaging,
    10,
    50);
BENCHMARK_NAMED_PARAM(
    runSchemaMultiBatch,
    StagingTenColumns200Batches,
    WritePath::kStaging,
    10,
    200);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(runMapVarcharInteger, ThreeEntries, 3);
BENCHMARK_NAMED_PARAM(runMapVarcharInteger, FiveEntries, 5);
BENCHMARK_NAMED_PARAM(runMapVarcharInteger, TenEntries, 10);

// TODO: Add all data types

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
