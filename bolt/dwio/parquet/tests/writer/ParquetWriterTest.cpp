/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/SparkCompatibility.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/dwio/parquet/arrow/ColumnPage.h"
#include "bolt/dwio/parquet/arrow/Encoding.h"
#include "bolt/dwio/parquet/arrow/tests/ColumnReader.h"
#include "bolt/dwio/parquet/arrow/tests/FileReader.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/arrow/Bridge.h"
#include "bolt/version/version.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
namespace vp = bytedance::bolt::parquet;
using namespace vp;

class ParquetWriterTest : public ParquetTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    BOLT_TEST_VALUE_ENABLE();
    bytedance::bolt::connector::hive::CheckHiveConnectorFactoryInit<
        bytedance::bolt::connector::hive::HiveConnectorFactory>();
    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
    bytedance::bolt::parquet::registerParquetWriterFactory();
  }

  std::unique_ptr<RowReader> createRowReaderWithSchema(
      const std::unique_ptr<Reader> reader,
      const RowTypePtr& rowType) {
    auto rowReaderOpts = getReaderOpts(rowType);
    auto scanSpec = makeScanSpec(rowType);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  };

  std::unique_ptr<vp::ParquetReader> createReaderInMemory(
      const dwio::common::MemorySink& sink,
      const dwio::common::ReaderOptions& opts) {
    std::string_view data(sink.data(), sink.size());
    return std::make_unique<vp::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<InMemoryReadFile>(data), opts.getMemoryPool()),
        opts);
  };

  std::unique_ptr<vp::Writer> createLocalWriter(
      const std::string& parquetPath,
      RowTypePtr schema,
      vp::WriterOptions& writerOptions,
      std::shared_ptr<::arrow::Schema> arrowSchema = nullptr,
      ::arrow::MemoryPool* arrowPool = ::arrow::default_memory_pool()) {
    writerOptions.enableFlushBasedOnBlockSize = true;
    writerOptions.parquetWriteTimestampUnit = TimestampUnit::kNano;
    writerOptions.writeInt96AsTimestamp = true;
    auto sink =
        dwio::common::FileSink::create(parquetPath, {.pool = pool_.get()});
    auto sinkPtr = sink.get();
    writerOptions.memoryPool = leafPool_.get();

    return std::make_unique<vp::Writer>(
        std::move(sink),
        writerOptions,
        rootPool_,
        arrowPool,
        schema,
        arrowSchema);
  }

  std::unique_ptr<vp::ParquetReader> createLocalParquetReader(
      const std::string& parquetPath) {
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    timestampPrecision_ = TimestampPrecision::kNanoseconds;
    return std::make_unique<vp::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(parquetPath),
            readerOptions.getMemoryPool()),
        readerOptions);
  }

  bool hasPageEncoding(
      const std::string& parquetPath,
      int32_t column,
      thrift::PageType::type pageType,
      thrift::Encoding::type encoding) {
    const auto stats = createLocalParquetReader(parquetPath)
                           ->fileMetaData()
                           .rowGroup(0)
                           .columnChunk(column)
                           .pageEncodingStats();
    return std::find_if(stats.begin(), stats.end(), [&](const auto& stat) {
             return stat.page_type == pageType && stat.encoding == encoding;
           }) != stats.end();
  }

  template <typename Callback>
  int64_t forEachDataPage(
      const std::string& parquetPath,
      int columnIndex,
      Callback&& callback) {
    auto fileReader = vp::arrow::ParquetFileReader::OpenFile(parquetPath);
    auto pageReader = fileReader->RowGroup(0)->GetColumnPageReader(columnIndex);
    int64_t dataPageCount = 0;
    while (auto page = pageReader->NextPage()) {
      if (page->type() == vp::arrow::PageType::DATA_PAGE ||
          page->type() == vp::arrow::PageType::DATA_PAGE_V2) {
        callback(page);
        ++dataPageCount;
      }
    }
    return dataPageCount;
  }

  ::arrow::MemoryPool* getArrowMemoryPool() {
    if (arrowPool_) {
      return arrowPool_.get();
    }
    static auto memAlloc = memory::sparksql::DefaultMemoryAllocatorGetter::
        defaultMemoryAllocator();
    arrowPool_ = std::make_shared<memory::sparksql::ArrowMemoryPool>(memAlloc);
    return arrowPool_.get();
  }

  void assertRead(
      const std::string& parquetPath,
      size_t rows,
      const RowTypePtr& schema,
      const VectorPtr& expected) {
    auto reader = createLocalParquetReader(parquetPath);
    ASSERT_EQ(reader->numberOfRows(), rows);
    ASSERT_EQ(*reader->rowType(), *schema);

    auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
    assertReadWithReaderAndExpected(
        schema,
        *rowReader,
        std::static_pointer_cast<RowVector>(expected),
        *leafPool_);
  }

  void assertWrite(
      const std::string& parquetPath,
      size_t rows,
      const RowTypePtr& schema,
      const VectorPtr& data,
      vp::WriterOptions writerOptions = {}) {
    auto writer = createLocalWriter(parquetPath, schema, writerOptions);
    writer->write(data);
    writer->close();
    assertRead(parquetPath, rows, schema, data);
  }

  VectorPtr repeatDictionary(
      VectorPtr values,
      vector_size_t size,
      BufferPtr nulls = nullptr) {
    BOLT_CHECK_GT(values->size(), 0);
    auto indices = makeIndices(
        size, [&](vector_size_t row) { return row % values->size(); });
    return BaseVector::wrapInDictionary(
        std::move(nulls), std::move(indices), size, std::move(values));
  }

  std::shared_ptr<const Type> getType() {
    bytedance::bolt::type::fbhive::HiveTypeParser parser;
    return parser.parse(
        R"(
              struct<
                bool_val:boolean,
                byte_val:tinyint,
                short_val:smallint,
                int_val:int,
                long_val:bigint,
                float_val:float,
                double_val:double,
                string_val:string,
                binary_val:binary,
                timestamp_val:timestamp,
                date_val:date,
                decimal38_val:decimal(38,4),
                decimal18_val:decimal(18,2),
                decimal9_val:decimal(9,2),

                array_bool:array<boolean>,
                array_tinyint:array<tinyint>,
                array_smallint:array<smallint>,
                array_int:array<int>,
                array_bigint:array<bigint>,
                array_float:array<float>,
                array_double:array<double>,
                array_string:array<string>,
                array_binary:array<binary>,
                array_timestamp:array<timestamp>,
                array_date:array<date>,
                array_decimal38:array<decimal(38,4)>,
                array_array_int:array<array<int>>,
                array_array_string:array<array<string>>,
                array_array_decimal:array<array<decimal(18,2)>>,
                array_map_int_string:array<map<int,string>>,
                array_map_string_double:array<map<string,double>>,

                map_int_double:map<int,double>,
                map_string_bool:map<string,boolean>,
                map_bigint_decimal:map<bigint,decimal(9,2)>,
                map_smallint_timestamp:map<smallint, timestamp>,
                map_tinyint_float:map<tinyint, float>,
                map_key_array:map<string,array<int>>,
                map_val_array:map<bigint,array<map<string,double>>>,
                map_struct_val:map<int,struct<a:float,b:double>>,
                array_map_array_struct:array<map<string,array<struct<id:bigint,value:double>>>>,
                struct_val:struct<a:float,b:double>
              >)");
  }

  inline static const std::string kHiveConnectorId = "test-hive";
  std::shared_ptr<::arrow::MemoryPool> arrowPool_ = nullptr;
};

std::vector<CompressionKind> params = {
    CompressionKind::CompressionKind_NONE,
    CompressionKind::CompressionKind_SNAPPY,
    CompressionKind::CompressionKind_ZSTD,
    CompressionKind::CompressionKind_LZ4,
    CompressionKind::CompressionKind_GZIP,
};

TEST_F(ParquetWriterTest, compression) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {INTEGER(),
           DOUBLE(),
           BIGINT(),
           INTEGER(),
           BIGINT(),
           INTEGER(),
           DOUBLE()});
  const int64_t kRows = 10'000;
  const auto data = makeRowVector({
      makeFlatVector<int32_t>(kRows, [](auto row) { return row + 5; }),
      makeFlatVector<double>(kRows, [](auto row) { return row - 10; }),
      makeFlatVector<int64_t>(kRows, [](auto row) { return row - 15; }),
      makeFlatVector<uint32_t>(kRows, [](auto row) { return row + 20; }),
      makeFlatVector<uint64_t>(kRows, [](auto row) { return row + 25; }),
      makeFlatVector<int32_t>(kRows, [](auto row) { return row + 30; }),
      makeFlatVector<double>(kRows, [](auto row) { return row - 25; }),
  });

  // Create an in-memory writer
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto sinkPtr = sink.get();
  vp::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.compression = CompressionKind::CompressionKind_SNAPPY;

  const auto& fieldNames = schema->names();

  for (int i = 0; i < params.size(); i++) {
    writerOptions.columnCompressionsMap[fieldNames[i]] = params[i];
  }

  auto writer = std::make_unique<vp::Writer>(
      std::move(sink),
      writerOptions,
      rootPool_,
      ::arrow::default_memory_pool(),
      schema);
  writer->write(data);
  writer->close();

  const auto metrics = writer->metrics();
  EXPECT_GT(metrics.writeRecodeWallNanos, 0);
  EXPECT_GT(metrics.writeEncodeWallNanos, 0);
  EXPECT_GT(metrics.writeCompressionWallNanos, 0);
  EXPECT_GT(metrics.writeIOWallNanos, 0);
  EXPECT_GT(metrics.writeFinalizeWallNanos, 0);

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReaderInMemory(*sinkPtr, readerOptions);

  ASSERT_EQ(reader->numberOfRows(), kRows);
  ASSERT_EQ(*reader->rowType(), *schema);

  for (int i = 0; i < params.size(); i++) {
    EXPECT_EQ(
        reader->fileMetaData().rowGroup(0).columnChunk(i).compression(),
        (i < params.size()) ? params[i]
                            : CompressionKind::CompressionKind_SNAPPY);
  }

  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
  assertReadWithReaderAndExpected(schema, *rowReader, data, *leafPool_);
};

TEST_F(ParquetWriterTest, defaultCreatedBy) {
  auto schema = ROW({"c0"}, {INTEGER()});
  const int64_t kRows = 10;
  const auto data = makeRowVector(
      {makeFlatVector<int32_t>(kRows, [](auto row) { return row; })});

  std::string parquetPath = tempPath_->path + "/defaultCreatedBy.parquet";
  vp::WriterOptions writerOptions{};
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  auto fileReader =
      bytedance::bolt::parquet::arrow::ParquetFileReader::OpenFile(
          parquetPath, false);
  const auto& createdBy = fileReader->metadata()->created_by();

  ASSERT_EQ(
      std::string("parquet-cpp-bolt version ") +
          BOLT_PARQUET_CREATED_BY_VERSION + " (build " +
          ::bytedance::bolt::BuildInfo::shortHash + ")",
      createdBy);

  bytedance::bolt::parquet::arrow::ApplicationVersion version(createdBy);
  EXPECT_EQ("parquet-cpp-bolt", version.application_);
  EXPECT_EQ(::bytedance::bolt::BuildInfo::shortHash, version.build_);
}

TEST_F(ParquetWriterTest, lz4Hadoop) {
  const int64_t kRows = 10'000'000;
  bytedance::bolt::type::fbhive::HiveTypeParser parser;
  auto type = parser.parse(
      R"(
        struct<
          bool_val:boolean,
          int_val:int,
          long_val:bigint,
          double_val:double,
          string_val:string,
          decimal38_val:decimal(38,4),
          array_bigint:array<bigint>,
          map_int_double:map<int,double>
        >)");
  auto schema = std::static_pointer_cast<const RowType>(type);
  auto data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 1000 == 0; });

  std::string parquetPath = tempPath_->path + "/lz4Hadoop.parquet";
  vp::WriterOptions writerOptions{};
  writerOptions.compression = CompressionKind::CompressionKind_LZ4;
  assertWrite(parquetPath, kRows, schema, data, writerOptions);

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createLocalParquetReader(parquetPath);
  EXPECT_EQ(
      CompressionKind::CompressionKind_LZ4,
      reader->fileMetaData().rowGroup(0).columnChunk(0).compression());
}

TEST_F(ParquetWriterTest, comparison) {
  const size_t kRows = 1100;

  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  std::string parquetPath = tempPath_->path + "/comparison.parquet";
  vp::WriterOptions writerOptions{};
  assertWrite(parquetPath, kRows, schema, data, writerOptions);
};

TEST_F(ParquetWriterTest, dictToArrow) {
  const size_t kRows = 1100;
  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });
  auto& children = std::dynamic_pointer_cast<RowVector>(data)->children();
  for (int i = 0; i < children.size(); ++i) {
    auto reversedIndices = makeIndicesInReverse(kRows);
    auto nulls = makeNulls(kRows, [](auto row) { return row % 19 == 0; });
    children[i] = BaseVector::wrapInDictionary(
        nulls, reversedIndices, kRows, children[i]);
  }
  std::string parquetPath = tempPath_->path + "/dictToArrow.parquet";
  assertWrite(parquetPath, kRows, schema, data);
};

TEST_F(ParquetWriterTest, dictionaryPassthroughDuplicateValues) {
  const vector_size_t kRows = 10'000;
  auto dictionary = makeFlatVector<std::string>(
      64, [](auto /*row*/) { return std::string(256, 'a'); });
  auto data = makeRowVector({repeatDictionary(dictionary, kRows)});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughDuplicateValues.parquet";

  vp::WriterOptions writerOptions{};
  writerOptions.dictionaryPageSizeLimit = 64 * 1024;
  writerOptions.dataPageSize = 1024;
  writerOptions.compression = CompressionKind_NONE;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  const auto stats = createLocalParquetReader(parquetPath)
                         ->fileMetaData()
                         .rowGroup(0)
                         .columnChunk(0)
                         .pageEncodingStats();
  const auto hasPage = [&](thrift::PageType::type pageType,
                           thrift::Encoding::type encoding) {
    return std::find_if(stats.begin(), stats.end(), [&](const auto& stat) {
             return stat.page_type == pageType && stat.encoding == encoding;
           }) != stats.end();
  };
  EXPECT_TRUE(
      hasPage(thrift::PageType::DICTIONARY_PAGE, thrift::Encoding::PLAIN));
  EXPECT_TRUE(hasPage(thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
}

TEST_F(ParquetWriterTest, dictionaryPassthroughAfterEmptyBatch) {
  const vector_size_t kRows = 10'000;
  auto data = makeRowVector({repeatDictionary(
      makeFlatVector<std::string>(
          16, [](auto /*row*/) { return std::string("value"); }),
      kRows)});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughAfterEmptyBatch.parquet";

  vp::WriterOptions writerOptions{};
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(BaseVector::create(schema, 0, leafPool_.get()));
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  const auto stats = createLocalParquetReader(parquetPath)
                         ->fileMetaData()
                         .rowGroup(0)
                         .columnChunk(0)
                         .pageEncodingStats();
  EXPECT_NE(
      std::find_if(
          stats.begin(),
          stats.end(),
          [](const auto& stat) {
            return stat.page_type == thrift::PageType::DATA_PAGE &&
                stat.encoding == thrift::Encoding::PLAIN;
          }),
      stats.end());
}

TEST_F(ParquetWriterTest, dictionaryPassthroughStringTypesAndMetrics) {
  const vector_size_t kRows = 4 * 1024;
  auto varchar = repeatDictionary(
      makeFlatVector<std::string>(
          16, [](auto row) { return fmt::format("varchar_{:02}", row); }),
      kRows,
      makeNulls(kRows, [](auto row) { return row % 17 == 0; }));
  auto varbinary = repeatDictionary(
      makeFlatVector<std::string>(
          32,
          [](auto row) { return fmt::format("binary_{:02}", row); },
          nullptr,
          VARBINARY()),
      kRows,
      makeNulls(kRows, [](auto row) { return row % 23 == 0; }));
  auto allNull = repeatDictionary(
      makeFlatVector<std::string>({"unused_0", "unused_1"}),
      kRows,
      makeNulls(kRows, [](auto /*row*/) { return true; }));
  auto data = makeRowVector({varchar, varbinary, allNull});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughStringTypes.parquet";

  ArrowSchema cArrowSchema;
  exportToArrow(
      BaseVector::create(schema, 0, leafPool_.get()),
      cArrowSchema,
      ArrowOptions{
          .flattenDictionary = true,
          .flattenConstant = true,
          .useLargeString = true});
  auto flatArrowSchema = ::arrow::ImportSchema(&cArrowSchema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> nullableFields;
  nullableFields.reserve(flatArrowSchema->num_fields());
  for (const auto& field : flatArrowSchema->fields()) {
    nullableFields.push_back(field->WithNullable(true));
  }

  vp::WriterOptions writerOptions{};
  writerOptions.writeBatchBytes = 1024;
  writerOptions.minBatchSize = 128;
  writerOptions.compression = CompressionKind_SNAPPY;
  writerOptions.dataPageVersion = vp::arrow::ParquetDataPageVersion::V2;
  auto writer = createLocalWriter(
      parquetPath,
      schema,
      writerOptions,
      ::arrow::schema(std::move(nullableFields)),
      nullptr);
  writer->write(data);
  const auto firstWriteMetrics = writer->metrics();
  EXPECT_GT(firstWriteMetrics.writeRecodeWallNanos, 0);
  EXPECT_GT(firstWriteMetrics.writeEncodeWallNanos, 0);
  EXPECT_GT(firstWriteMetrics.writeIOWallNanos, 0);
  EXPECT_EQ(firstWriteMetrics.writeFinalizeWallNanos, 0);

  writer->write(data);
  const auto secondWriteMetrics = writer->metrics();
  EXPECT_GT(
      secondWriteMetrics.writeRecodeWallNanos,
      firstWriteMetrics.writeRecodeWallNanos);
  EXPECT_GT(
      secondWriteMetrics.writeEncodeWallNanos,
      firstWriteMetrics.writeEncodeWallNanos);
  writer->close();
  const auto closedMetrics = writer->metrics();
  EXPECT_GT(closedMetrics.writeCompressionWallNanos, 0);
  EXPECT_GT(closedMetrics.writeFinalizeWallNanos, 0);

  auto expected = BaseVector::create(schema, 2 * kRows, pool_.get());
  expected->copy(data.get(), 0, 0, kRows);
  expected->copy(data.get(), kRows, 0, kRows);
  assertRead(parquetPath, 2 * kRows, schema, expected);

  auto reader = createLocalParquetReader(parquetPath);
  for (int32_t column = 0; column < 2; ++column) {
    const auto stats = reader->fileMetaData()
                           .rowGroup(0)
                           .columnChunk(column)
                           .pageEncodingStats();
    EXPECT_NE(
        std::find_if(
            stats.begin(),
            stats.end(),
            [](const auto& stat) {
              return stat.page_type == thrift::PageType::DICTIONARY_PAGE;
            }),
        stats.end());
  }
}

TEST_F(ParquetWriterTest, dictionaryPassthroughSelectiveFlatten) {
  const vector_size_t kRows = 1024;
  auto integer = repeatDictionary(
      makeFlatVector<int32_t>(8, [](auto row) { return row * 7; }), kRows);
  auto innerDictionary = repeatDictionary(
      makeFlatVector<std::string>(
          16, [](auto row) { return fmt::format("nested_{:02}", row); }),
      kRows);
  auto nestedDictionary = repeatDictionary(innerDictionary, kRows);
  auto nullableDictionary = repeatDictionary(
      makeFlatVector<std::string>(
          8,
          [](auto row) { return fmt::format("nullable_{:02}", row); },
          [](auto row) { return row == 3; }),
      kRows);

  auto array = makeArrayVector<std::string>(
      kRows,
      [](auto /*row*/) { return 2; },
      [](auto index) { return fmt::format("element_{:02}", index % 11); });
  auto* arrayVector = array->asUnchecked<ArrayVector>();
  arrayVector->elements() = repeatDictionary(
      arrayVector->elements(), arrayVector->elements()->size());

  auto passthrough = repeatDictionary(
      makeFlatVector<std::string>(
          8, [](auto /*row*/) { return std::string("passthrough"); }),
      kRows);
  auto data = makeRowVector(
      {integer, nestedDictionary, nullableDictionary, array, passthrough});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughSelectiveFlatten.parquet";

  vp::WriterOptions writerOptions{};
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  EXPECT_EQ(VectorEncoding::Simple::DICTIONARY, integer->encoding());
  EXPECT_EQ(VectorEncoding::Simple::DICTIONARY, nestedDictionary->encoding());
  EXPECT_EQ(VectorEncoding::Simple::DICTIONARY, nullableDictionary->encoding());
  EXPECT_EQ(
      VectorEncoding::Simple::DICTIONARY, arrayVector->elements()->encoding());
  EXPECT_EQ(VectorEncoding::Simple::DICTIONARY, passthrough->encoding());
  assertRead(parquetPath, kRows, schema, data);

  const auto stats = createLocalParquetReader(parquetPath)
                         ->fileMetaData()
                         .rowGroup(0)
                         .columnChunk(4)
                         .pageEncodingStats();
  EXPECT_NE(
      std::find_if(
          stats.begin(),
          stats.end(),
          [](const auto& stat) {
            return stat.page_type == thrift::PageType::DATA_PAGE &&
                stat.encoding == thrift::Encoding::PLAIN;
          }),
      stats.end());
}

TEST_F(ParquetWriterTest, dictionaryPassthroughSchemaTransitions) {
  const vector_size_t kRows = 2048;
  auto firstDictionary = repeatDictionary(
      makeFlatVector<std::string>(
          16, [](auto row) { return fmt::format("first_{:02}", row); }),
      kRows);
  auto secondDictionary = repeatDictionary(
      makeFlatVector<std::string>(
          8, [](auto row) { return fmt::format("second_{:02}", row); }),
      kRows);
  auto nestedDictionary = repeatDictionary(firstDictionary, kRows);
  auto nullableDictionary = repeatDictionary(
      makeFlatVector<std::string>(
          8,
          [](auto row) { return fmt::format("nullable_{:02}", row); },
          [](auto row) { return row == 3; }),
      kRows);
  auto flat = makeFlatVector<std::string>(
      kRows,
      [](auto row) { return fmt::format("flat_{:04}", row); },
      [](auto row) { return row % 29 == 0; });
  std::vector<RowVectorPtr> batches = {
      makeRowVector({firstDictionary}),
      makeRowVector({secondDictionary}),
      makeRowVector({nestedDictionary}),
      makeRowVector({nullableDictionary}),
      makeRowVector({flat}),
      makeRowVector({firstDictionary})};
  auto schema = std::static_pointer_cast<const RowType>(batches[0]->type());
  auto expected = BaseVector::create(
      schema, batches.size() * static_cast<size_t>(kRows), pool_.get());
  for (size_t i = 0; i < batches.size(); ++i) {
    expected->copy(batches[i].get(), i * kRows, 0, kRows);
  }
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughSchemaTransitions.parquet";

  ArrowSchema cArrowSchema;
  exportToArrow(
      BaseVector::create(schema, 0, leafPool_.get()),
      cArrowSchema,
      ArrowOptions{
          .flattenDictionary = true,
          .flattenConstant = true,
          .useLargeString = true});
  auto nullableSchema = ::arrow::ImportSchema(&cArrowSchema).ValueOrDie();

  vp::WriterOptions writerOptions{};
  auto writer =
      createLocalWriter(parquetPath, schema, writerOptions, nullableSchema);
  for (size_t i = 0; i < batches.size(); ++i) {
    writer->write(batches[i]);
    if (i == 1) {
      writer->flush();
    }
  }
  writer->close();

  assertRead(parquetPath, batches.size() * kRows, schema, expected);
  const auto metadata =
      vp::arrow::ParquetFileReader::OpenFile(parquetPath)->metadata();
  EXPECT_TRUE(metadata->schema()->group_node()->field(0)->is_optional());
}

TEST_F(ParquetWriterTest, dictionaryInputRespectsOutputDictionaryOptions) {
  const vector_size_t kRows = 1024;
  const auto makeColumn = [&]() {
    return repeatDictionary(
        makeFlatVector<std::string>(
            8, [](auto row) { return fmt::format("value_{:02}", row); }),
        kRows);
  };
  auto data =
      makeRowVector({"enabled", "disabled"}, {makeColumn(), makeColumn()});
  auto schema = std::static_pointer_cast<const RowType>(data->type());

  const auto writeAndCheck = [&](const std::string& name,
                                 vp::WriterOptions writerOptions,
                                 const std::vector<bool>& expectedPages) {
    const auto parquetPath = tempPath_->path + "/" + name + ".parquet";
    auto writer = createLocalWriter(parquetPath, schema, writerOptions);
    writer->write(data);
    writer->close();
    assertRead(parquetPath, kRows, schema, data);

    auto reader = createLocalParquetReader(parquetPath);
    for (int32_t column = 0; column < expectedPages.size(); ++column) {
      const auto stats = reader->fileMetaData()
                             .rowGroup(0)
                             .columnChunk(column)
                             .pageEncodingStats();
      const bool hasDictionaryPage =
          std::find_if(stats.begin(), stats.end(), [](const auto& stat) {
            return stat.page_type == thrift::PageType::DICTIONARY_PAGE;
          }) != stats.end();
      EXPECT_EQ(expectedPages[column], hasDictionaryPage);
    }
  };

  vp::WriterOptions globallyDisabled{};
  globallyDisabled.enableDictionary = false;
  writeAndCheck(
      "inputDictionaryGloballyDisabled", globallyDisabled, {false, false});

  vp::WriterOptions columnDisabled{};
  columnDisabled.columnEnableDictionaryMap["disabled"] = false;
  writeAndCheck("inputDictionaryColumnDisabled", columnDisabled, {true, false});
}

TEST_F(ParquetWriterTest, dictionaryPassthroughUsesWholeInputForReuse) {
  constexpr vector_size_t kRows = 16 * 100;
  const auto makeColumn = [&](vector_size_t dictionarySize) {
    return repeatDictionary(
        makeFlatVector<std::string>(
            dictionarySize, [](auto /*row*/) { return std::string(64, 'a'); }),
        kRows);
  };
  auto data = makeRowVector(
      {"at_threshold", "over_threshold"}, {makeColumn(16), makeColumn(17)});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughReuseThreshold.parquet";

  vp::WriterOptions writerOptions{};
  writerOptions.writeBatchBytes = 1;
  writerOptions.minBatchSize = 128;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  EXPECT_TRUE(hasPageEncoding(
      parquetPath, 0, thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
  EXPECT_FALSE(hasPageEncoding(
      parquetPath, 1, thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
  EXPECT_TRUE(hasPageEncoding(
      parquetPath,
      1,
      thrift::PageType::DATA_PAGE,
      thrift::Encoding::RLE_DICTIONARY));
}

TEST_F(ParquetWriterTest, dictionaryPassthroughHonorsColumnOptions) {
  constexpr vector_size_t kRows = 16 * 100;
  const auto makeColumn = [&]() {
    return repeatDictionary(
        makeFlatVector<std::string>(
            16, [](auto /*row*/) { return std::string(64, 'a'); }),
        kRows);
  };
  auto globalLimit = makeColumn();
  const auto dictionaryBytes = globalLimit->valueVector()->estimateFlatSize();
  auto data = makeRowVector(
      {"global_limit", "column_limit", "disabled"},
      {globalLimit, makeColumn(), makeColumn()});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto parquetPath =
      tempPath_->path + "/dictionaryPassthroughColumnOptions.parquet";

  vp::WriterOptions writerOptions{};
  writerOptions.dictionaryPageSizeLimit = dictionaryBytes - 1;
  writerOptions.columnDictionaryPageSizeLimitMap["column_limit"] =
      dictionaryBytes;
  writerOptions.columnEnableDictionaryMap["disabled"] = false;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  EXPECT_FALSE(hasPageEncoding(
      parquetPath, 0, thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
  EXPECT_TRUE(hasPageEncoding(
      parquetPath,
      0,
      thrift::PageType::DATA_PAGE,
      thrift::Encoding::RLE_DICTIONARY));
  EXPECT_TRUE(hasPageEncoding(
      parquetPath, 1, thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
  EXPECT_FALSE(hasPageEncoding(
      parquetPath,
      2,
      thrift::PageType::DICTIONARY_PAGE,
      thrift::Encoding::PLAIN));
  EXPECT_TRUE(hasPageEncoding(
      parquetPath, 2, thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN));
}

TEST_F(ParquetWriterTest, dictionaryPassthroughOnlyUsesDirectWritePath) {
  constexpr vector_size_t kRows = 16 * 100;
  auto data = makeRowVector({repeatDictionary(
      makeFlatVector<std::string>(
          16, [](auto /*row*/) { return std::string(64, 'a'); }),
      kRows)});
  auto schema = std::static_pointer_cast<const RowType>(data->type());
  const auto writeAndCheck = [&](const std::string& name,
                                 std::unique_ptr<vp::Writer> writer,
                                 bool expectPlainDataPage) {
    const auto parquetPath = tempPath_->path + "/" + name + ".parquet";
    writer->write(data);
    writer->close();
    assertRead(parquetPath, kRows, schema, data);
    EXPECT_EQ(
        expectPlainDataPage,
        hasPageEncoding(
            parquetPath,
            0,
            thrift::PageType::DATA_PAGE,
            thrift::Encoding::PLAIN));
  };

  vp::WriterOptions directOptions{};
  const auto directPath =
      tempPath_->path + "/dictionaryPassthroughDirect.parquet";
  writeAndCheck(
      "dictionaryPassthroughDirect",
      createLocalWriter(directPath, schema, directOptions),
      true);

  const auto stagingPath =
      tempPath_->path + "/dictionaryPassthroughStaging.parquet";
  auto stagingWriter = createWriter(
      createSink(stagingPath),
      [&]() {
        return std::make_unique<LambdaFlushPolicy>(
            kRowsInRowGroup, kBytesInRowGroup, []() { return false; });
      },
      schema);
  writeAndCheck(
      "dictionaryPassthroughStaging", std::move(stagingWriter), false);

  vp::WriterOptions alignedOptions{};
  alignedOptions.enableRowGroupAlignedWrite = true;
  alignedOptions.expectedRowsInEachBlock = {kRows + 1};
  const auto alignedPath =
      tempPath_->path + "/dictionaryPassthroughAligned.parquet";
  writeAndCheck(
      "dictionaryPassthroughAligned",
      createLocalWriter(alignedPath, schema, alignedOptions),
      false);
}

TEST_F(ParquetWriterTest, reuseArrowSchemaAcrossEncodings) {
  const vector_size_t kRows = 64;
  auto makeBatch = [&](int32_t offset) {
    return makeRowVector(
        {"id", "items", "attributes"},
        {makeFlatVector<int32_t>(kRows, [&](auto row) { return offset + row; }),
         makeArrayVector<int64_t>(
             kRows,
             [](auto row) { return row % 3; },
             [&](auto row, auto index) { return offset + row * 10 + index; }),
         makeMapVector<int64_t, int32_t>(
             kRows,
             [](auto row) { return row % 2 + 1; },
             [&](auto index) { return offset + index; },
             [&](auto index) { return offset - index; })});
  };
  auto first = makeBatch(0);
  auto second = makeBatch(1'000);
  auto schema = std::static_pointer_cast<const RowType>(first->type());

  auto& children = second->children();
  for (auto& child : children) {
    child = BaseVector::wrapInDictionary(
        makeNulls(kRows, [](auto row) { return row % 19 == 0; }),
        makeIndicesInReverse(kRows),
        kRows,
        child);
  }

  auto expected = BaseVector::create(schema, 2 * kRows, pool_.get());
  expected->copy(first.get(), 0, 0, kRows);
  expected->copy(second.get(), kRows, 0, kRows);

  std::string parquetPath = tempPath_->path + "/cachedArrowSchema.parquet";
  auto writer = createWriter(
      createSink(parquetPath),
      [&]() {
        return std::make_unique<LambdaFlushPolicy>(
            kRowsInRowGroup, kBytesInRowGroup, []() { return false; });
      },
      schema);
  writer->write(BaseVector::create(schema, 0, leafPool_.get()));
  writer->write(first);
  writer->flush();
  writer->write(second);
  writer->close();

  assertRead(parquetPath, 2 * kRows, schema, expected);
  auto reader = createLocalParquetReader(parquetPath);
  const auto& metadata = reader->fileMetaData();
  ASSERT_EQ(2, metadata.numRowGroups());
  EXPECT_EQ(kRows, metadata.rowGroup(0).numRows());
  EXPECT_EQ(kRows, metadata.rowGroup(1).numRows());
}

TEST_F(ParquetWriterTest, constantComplexToArrow) {
  const size_t kRows = 1100;
  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });
  auto& children = std::dynamic_pointer_cast<RowVector>(data)->children();
  for (int i = 0; i < children.size(); ++i) {
    children[i] = BaseVector::wrapInConstant(kRows, 99, children[i]);
  }
  std::string parquetPath = tempPath_->path + "/constantComplexToArrow.parquet";
  assertWrite(parquetPath, kRows, schema, data);
};

TEST_F(ParquetWriterTest, constantToArrow) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3"}, {INTEGER(), VARCHAR(), BIGINT(), DOUBLE()});
  const int64_t kRows = 10'000;
  const auto data = makeRowVector(
      {BaseVector::createConstant(INTEGER(), 100, kRows, leafPool_.get()),
       BaseVector::createConstant(
           VARCHAR(),
           "ParquetWriterTestconstantToArrowTest",
           kRows,
           leafPool_.get()),
       BaseVector::createConstant(
           BIGINT(),
           static_cast<int64_t>(10000000000L),
           kRows,
           leafPool_.get()),
       BaseVector::createConstant(DOUBLE(), 1000.0, kRows, leafPool_.get())});
  std::string parquetPath = tempPath_->path + "/constantToArrow.parquet";
  assertWrite(parquetPath, kRows, schema, data);
};

TEST_F(ParquetWriterTest, splitWrite) {
  const vector_size_t kRows = 4 * 1024;

  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  auto first = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });
  auto second = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 7 == 0; });
  auto& children = std::dynamic_pointer_cast<RowVector>(second)->children();
  for (auto& child : children) {
    child = BaseVector::wrapInDictionary(
        makeNulls(kRows, [](auto row) { return row % 19 == 0; }),
        makeIndicesInReverse(kRows),
        kRows,
        child);
  }

  auto expected = BaseVector::create(schema, 2 * kRows, pool_.get());
  expected->copy(first.get(), 0, 0, kRows);
  expected->copy(second.get(), kRows, 0, kRows);

  std::string parquetPath = tempPath_->path + "/splitWrite.parquet";
  vp::WriterOptions writerOptions{};
  // Set a smaller value to exercise multiple record batches per write.
  writerOptions.writeBatchBytes = 1024;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(first);
  writer->flush();
  writer->write(second);
  writer->close();

  assertRead(parquetPath, 2 * kRows, schema, expected);
};

TEST_F(ParquetWriterTest, flush) {
  const size_t kRows = 4 * 1024;

  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  auto data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  {
    std::string parquetPath = tempPath_->path + "/flush1.parquet";
    vp::WriterOptions writerOptions{};

    auto writer = createLocalWriter(parquetPath, schema, writerOptions);
    writer->write(data);
    writer->flush();
    writer->close();

    assertRead(parquetPath, data->size(), schema, data);
    auto reader = createLocalParquetReader(parquetPath);
    EXPECT_EQ(1, reader->fileMetaData().numRowGroups());
    EXPECT_EQ(data->size(), reader->fileMetaData().rowGroup(0).numRows());
  }
  {
    std::string parquetPath = tempPath_->path + "/flush2.parquet";
    vp::WriterOptions writerOptions{};
    auto writer = createLocalWriter(parquetPath, schema, writerOptions);
    auto size = data->size() / 2;
    writer->write(data->slice(0, size));
    writer->flush();
    writer->write(data->slice(size, data->size() - size));
    writer->close();

    assertRead(parquetPath, data->size(), schema, data);
    auto reader = createLocalParquetReader(parquetPath);
    const auto& fileMetaData = reader->fileMetaData();
    ASSERT_EQ(2, fileMetaData.numRowGroups());
    EXPECT_EQ(size, fileMetaData.rowGroup(0).numRows());
    EXPECT_EQ(data->size() - size, fileMetaData.rowGroup(1).numRows());
  }
};

TEST_F(ParquetWriterTest, columnNullable) {
  const size_t kRows = 1100;
  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return false; });

  vp::WriterOptions writerOptions{};
  ArrowSchema cArrowSchema;
  exportToArrow(
      bytedance::bolt::BaseVector::create(schema, 0, leafPool_.get()),
      cArrowSchema,
      ArrowOptions{
          .flattenDictionary = true,
          .flattenConstant = true,
          .useLargeString = true});
  auto arrowSchema = ::arrow::ImportSchema(&cArrowSchema).ValueOrDie();
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = arrowSchema->num_fields();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(arrowSchema->field(i)->WithNullable(i % 2 == 0));
  }
  auto nullableSchema = ::arrow::schema(newFields);

  auto assertNullability = [&](const std::string& parquetPath,
                               int64_t expectedRows,
                               int expectedRowGroups) {
    SCOPED_TRACE(parquetPath);
    auto fileReader = vp::arrow::ParquetFileReader::OpenFile(parquetPath);
    const auto metadata = fileReader->metadata();
    EXPECT_EQ(metadata->num_rows(), expectedRows);
    EXPECT_EQ(metadata->num_row_groups(), expectedRowGroups);

    const auto* parquetSchema = metadata->schema()->group_node();
    ASSERT_EQ(parquetSchema->field_count(), childSize);
    for (auto i = 0; i < childSize; ++i) {
      EXPECT_EQ(parquetSchema->field(i)->is_optional(), i % 2 == 0);
    }
  };

  const auto parquetPath = tempPath_->path + "/columnNullable.parquet";
  {
    auto writer =
        createLocalWriter(parquetPath, schema, writerOptions, nullableSchema);
    writer->write(data);
    writer->write(data);
    writer->close();
  }

  auto expected = BaseVector::create(schema, 2 * kRows, pool_.get());
  expected->copy(data.get(), 0, 0, kRows);
  expected->copy(data.get(), kRows, 0, kRows);
  assertRead(parquetPath, 2 * kRows, schema, expected);
  assertNullability(parquetPath, 2 * kRows, 1);

  auto emptyData = BaseVector::create(schema, 0, leafPool_.get());
  const auto emptyWritePath =
      tempPath_->path + "/columnNullableEmptyWrite.parquet";
  {
    auto writer = createLocalWriter(
        emptyWritePath, schema, writerOptions, nullableSchema);
    writer->write(emptyData);
    writer->close();
  }
  assertRead(emptyWritePath, 0, schema, emptyData);
  assertNullability(emptyWritePath, 0, 0);

  const auto noWritePath = tempPath_->path + "/columnNullableNoWrite.parquet";
  {
    auto writer =
        createLocalWriter(noWritePath, schema, writerOptions, nullableSchema);
    writer->close();
  }
  assertRead(noWritePath, 0, schema, emptyData);
  assertNullability(noWritePath, 0, 0);
};

TEST_F(ParquetWriterTest, emptyParquet) {
  auto schema = ROW({"c0", "c1"}, {INTEGER(), DOUBLE()});

  std::string parquetPath = tempPath_->path + "/emptyParquet.parquet";
  vp::WriterOptions writerOptions{};
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->close();

  auto reader = createLocalParquetReader(parquetPath);
  ASSERT_EQ(reader->numberOfRows(), 0);
  ASSERT_EQ(*reader->rowType(), *schema);

  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
  assertReadWithReaderAndExpected(
      schema,
      *rowReader,
      std::static_pointer_cast<RowVector>(BaseVector::create(
          std::static_pointer_cast<const Type>(schema), 0, leafPool_.get())),
      *leafPool_);
};

TEST_F(ParquetWriterTest, emptyBatch) {
  auto schema = ROW({"c0", "c1"}, {INTEGER(), DOUBLE()});
  auto data = BaseVector::create(schema, 0, leafPool_.get());
  std::string parquetPath = tempPath_->path + "/emptyBatch.parquet";
  vp::WriterOptions writerOptions{};
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, 0, schema, data);
}

TEST_F(ParquetWriterTest, allNulls) {
  auto schema = ROW({"c0"}, {INTEGER()});
  const int64_t kRows = 4096;
  // Create a column with all elements being null.
  auto nulls = makeNulls(kRows, [](auto /*row*/) { return true; });
  auto flatVector = std::make_shared<FlatVector<int32_t>>(
      pool_.get(),
      schema->childAt(0),
      nulls,
      kRows,
      /*values=*/nullptr,
      std::vector<BufferPtr>());
  auto data = std::make_shared<RowVector>(
      pool_.get(), schema, nullptr, kRows, std::vector<VectorPtr>{flatVector});

  // Create an in-memory writer.
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto sinkPtr = sink.get();
  vp::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.enableDictionary = false;
  writerOptions.maxRowsPerDataPage = 1'000;

  auto writer = std::make_unique<vp::Writer>(
      std::move(sink),
      writerOptions,
      rootPool_,
      ::arrow::default_memory_pool(),
      schema);
  writer->write(data);
  writer->close();

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReaderInMemory(*sinkPtr, readerOptions);
  ASSERT_EQ(reader->numberOfRows(), kRows);
  ASSERT_EQ(*reader->rowType(), *schema);
  int32_t dataPageCount = 0;
  const auto columnChunk = reader->fileMetaData().rowGroup(0).columnChunk(0);
  for (const auto& stats : columnChunk.pageEncodingStats()) {
    if (stats.page_type == thrift::PageType::DATA_PAGE ||
        stats.page_type == thrift::PageType::DATA_PAGE_V2) {
      dataPageCount += stats.count;
    }
  }
  ASSERT_EQ(dataPageCount, 5);

  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
  assertReadWithReaderAndExpected(schema, *rowReader, data, *leafPool_);
};

TEST_F(ParquetWriterTest, allNullsFlatVector) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
          {INTEGER(),
           BOOLEAN(),
           TINYINT(),
           INTEGER(),
           BIGINT(),
           DOUBLE(),
           VARCHAR(),
           TIMESTAMP()});

  std::string parquetPath = tempPath_->path + "/allNullsFlatVector.parquet";
  const int64_t kRows = 10'000;
  const auto data = makeRowVector(
      {makeFlatVector<int32_t>(kRows, [](auto row) { return row; }),
       makeAllNullFlatVectorWithNullValues<bool>(kRows),
       makeAllNullFlatVector<int8_t>(kRows),
       makeAllNullFlatVectorWithNullValues<int32_t>(kRows),
       makeAllNullFlatVectorWithNullValues<int64_t>(kRows),
       makeAllNullFlatVectorWithNullValues<double>(kRows),
       makeAllNullFlatVectorWithNullValues<StringView>(kRows),
       makeAllNullFlatVectorWithNullValues<Timestamp>(kRows)});

  vp::WriterOptions writerOptions{};
  assertWrite(parquetPath, kRows, schema, data, writerOptions);
};

TEST_F(ParquetWriterTest, columnCompressionLevel) {
  const size_t kRows = 1024 * 1024;
  CompressionKind compressionKind = CompressionKind::CompressionKind_ZSTD;
  bytedance::bolt::type::fbhive::HiveTypeParser parser;
  auto type = parser.parse(
      R"(struct<
                int_val:int,
                string_val:string,
                map_struct_val:map<int,struct<a:bigint,b:double>>
            >)");
  auto schema = std::static_pointer_cast<const RowType>(type);
  auto data =
      bytedance::bolt::test::BatchMaker::createBatch(type, kRows, *leafPool_);

  std::string parquetPath1 = tempPath_->path + "/columnCompression1.parquet";
  std::string parquetPath2 = tempPath_->path + "/columnCompression2.parquet";
  std::string stringValPath = "string_val";
  std::string intValPath = "int_val";
  std::string nestedCoumnPath = "map_struct_val.key_value.value.a";

  auto writeParquetAndCheckData = [&](std::string parquetPath,
                                      int32_t compressionLevel) {
    vp::WriterOptions writerOptions{};
    writerOptions.enableDictionary = false;
    writerOptions.columnCompressionsMap[stringValPath] = compressionKind;
    writerOptions.columnCompressionsMap[nestedCoumnPath] = compressionKind;
    writerOptions.columnCodecOptionsMap[stringValPath] =
        std::make_shared<CodecOptions>(compressionLevel);
    writerOptions.columnCodecOptionsMap[nestedCoumnPath] =
        std::make_shared<CodecOptions>(compressionLevel);
    auto writer = createLocalWriter(parquetPath, schema, writerOptions);
    writer->write(data);
    writer->close();

    assertRead(parquetPath, kRows, schema, data);
  };

  writeParquetAndCheckData(parquetPath1, 1);
  writeParquetAndCheckData(parquetPath2, 9);

  auto reader1 = createLocalParquetReader(parquetPath1);
  auto reader2 = createLocalParquetReader(parquetPath2);
  auto rg1 = reader1->fileMetaData().rowGroup(0);
  auto rg2 = reader2->fileMetaData().rowGroup(0);

  // int_val
  ASSERT_EQ(CompressionKind_NONE, rg1.columnChunk(0).compression());
  ASSERT_EQ(CompressionKind_NONE, rg2.columnChunk(0).compression());
  ASSERT_EQ(
      rg1.columnChunk(0).totalCompressedSize(),
      rg2.columnChunk(0).totalCompressedSize());
  // string_val
  ASSERT_EQ(compressionKind, rg1.columnChunk(1).compression());
  ASSERT_EQ(compressionKind, rg2.columnChunk(1).compression());
  ASSERT_GT(
      rg1.columnChunk(1).totalCompressedSize(),
      rg2.columnChunk(1).totalCompressedSize());
  // map_struct_val.key_value.value.a
  ASSERT_EQ(compressionKind, rg1.columnChunk(3).compression());
  ASSERT_EQ(compressionKind, rg2.columnChunk(3).compression());
  ASSERT_GT(
      rg1.columnChunk(3).totalCompressedSize(),
      rg2.columnChunk(3).totalCompressedSize());
}

TEST_F(ParquetWriterTest, columnPageSize) {
  std::string c0{"c0"}, c1{"c1"}, c2{"c2"};
  auto schema = ROW({c0, c1, c2}, {INTEGER(), INTEGER(), INTEGER()});
  const int64_t kRows = 1;
  const int64_t DataCount = 4;
  std::string parquetPath = tempPath_->path + "/pageSize.parquet";

  vp::WriterOptions writerOptions{};
  writerOptions.columnEnableDictionaryMap[c1] = true;
  writerOptions.columnDictionaryPageSizeLimitMap[c1] = 4; // 4 bytes
  writerOptions.columnEnableDictionaryMap[c2] = false;
  writerOptions.columnDataPageSizeMap[c2] = 4; // 4 bytes
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  auto mergedData = BaseVector::create(schema, DataCount * kRows, pool_.get());
  for (int i = 0; i < DataCount; ++i) {
    auto data = makeRowVector(
        {makeFlatVector<int32_t>(kRows, [&i](auto row) { return i; }),
         makeFlatVector<int32_t>(kRows, [&i](auto row) { return i; }),
         makeFlatVector<int32_t>(kRows, [&i](auto row) { return i; })});
    mergedData->copy(data.get(), i * kRows, 0, data->size());
    writer->write(data);
  }
  writer->close();
  assertRead(parquetPath, DataCount * kRows, schema, mergedData);

  auto reader = createLocalParquetReader(parquetPath);
  auto rg = reader->fileMetaData().rowGroup(0);

  // c0 default, enable dictionary, dictionaryPageSizeLimit=1M, dataPageSize=1M
  auto chunk0PageEncodingStats = rg.columnChunk(0).pageEncodingStats();
  ASSERT_EQ(2, chunk0PageEncodingStats.size());
  ASSERT_EQ(1, chunk0PageEncodingStats[0].count); // dictionary page num
  ASSERT_EQ(1, chunk0PageEncodingStats[1].count); // data page of dictionary num

  // c1 enable dictionary, dictionaryPageSizeLimit=4bytes, dataPageSize=1M
  // encoding_stats=[
  // PageEncodingStats(page_type=DICTIONARY_PAGE, encoding=PLAIN, count=1),
  // PageEncodingStats(page_type=DATA_PAGE, encoding=PLAIN, count=1),
  // PageEncodingStats(page_type=DATA_PAGE, encoding=RLE_DICTIONARY, count=1)]
  auto chunk1PageEncodingStats = rg.columnChunk(1).pageEncodingStats();
  ASSERT_EQ(3, chunk1PageEncodingStats.size());
  ASSERT_EQ(1, chunk1PageEncodingStats[0].count); // dictionary page num
  ASSERT_EQ(1, chunk1PageEncodingStats[1].count); // data page num
  ASSERT_EQ(1, chunk1PageEncodingStats[2].count); // data page of dictionary num

  // c2 disable dictionary, dataPageSize=4bytes
  auto chunk2PageEncodingStats = rg.columnChunk(2).pageEncodingStats();
  ASSERT_EQ(1, chunk2PageEncodingStats.size());
  ASSERT_EQ(4, chunk2PageEncodingStats[0].count); // data page num
}

TEST_F(ParquetWriterTest, byteArrayPlainEncodingRespectsDataPageSize) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARCHAR()});
  const int64_t kRows = 1024;
  const int64_t kValueSize = 256;
  const int64_t kDataPageSize = 1024;
  const int64_t kMaxValuesPerPage =
      kDataPageSize / (kValueSize + sizeof(uint32_t));
  std::string parquetPath = tempPath_->path + "/byteArrayPageSize.parquet";

  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.columnDataPageSizeMap[c0] = kDataPageSize;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  auto data = makeRowVector({makeFlatVector<std::string>(kRows, [&](auto row) {
    return std::string(kValueSize, 'a' + row % 26);
  })});

  writer->write(data);
  writer->close();
  assertRead(parquetPath, kRows, schema, data);

  int64_t totalValues = 0;
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        ASSERT_EQ(vp::arrow::PageType::DATA_PAGE, page->type());
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPage>(page);
        ASSERT_EQ(vp::arrow::Encoding::PLAIN, dataPage->encoding());
        // All values are non-null and have the same length, so this directly
        // bounds the PLAIN value bytes in each page without counting level
        // encoding overhead.
        EXPECT_LE(dataPage->num_values(), kMaxValuesPerPage);
        totalValues += dataPage->num_values();
      });
  EXPECT_EQ(kRows, totalValues);
  EXPECT_GT(dataPageCount, 1);
}

TEST_F(ParquetWriterTest, nullableByteArrayPlainEncodingSplitsPages) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARBINARY()});
  const int64_t kRows = 1024;
  const int64_t kValueSize = 128;
  const int64_t kDataPageSize = 1024;
  std::string parquetPath = tempPath_->path + "/nullableVarbinaryPages.parquet";

  auto data = makeRowVector({makeFlatVector<std::string>(
      kRows,
      [&](auto row) { return std::string(kValueSize, 'a' + row % 26); },
      [](auto row) { return row % 5 == 0; },
      VARBINARY())});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.dataPageVersion = vp::arrow::ParquetDataPageVersion::V2;
  writerOptions.columnDataPageSizeMap[c0] = kDataPageSize;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  int64_t totalValues = 0;
  int64_t totalNulls = 0;
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        ASSERT_EQ(vp::arrow::PageType::DATA_PAGE_V2, page->type());
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPageV2>(page);
        ASSERT_EQ(vp::arrow::Encoding::PLAIN, dataPage->encoding());
        const int64_t nonNullValues =
            dataPage->num_values() - dataPage->num_nulls();
        EXPECT_LE(
            nonNullValues * (kValueSize + sizeof(uint32_t)), kDataPageSize);
        totalValues += dataPage->num_values();
        totalNulls += dataPage->num_nulls();
      });
  EXPECT_EQ(kRows, totalValues);
  EXPECT_EQ((kRows + 4) / 5, totalNulls);
  EXPECT_GT(dataPageCount, 1);
}

TEST_F(ParquetWriterTest, nestedByteArrayPlainEncodingSplitsAtRecords) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {ARRAY(VARCHAR())});
  const vector_size_t kRows = 32;
  const vector_size_t kElementsPerRow = 32;
  const int64_t kValueSize = 64;
  const int64_t kDataPageSize = 1024;
  const int64_t kNullArrayCount = (kRows + 10) / 11;
  const int64_t kExpectedLevels =
      (kRows - kNullArrayCount) * kElementsPerRow + kNullArrayCount;
  std::string parquetPath = tempPath_->path + "/nestedVarcharPages.parquet";

  auto data = makeRowVector({makeArrayVector<std::string>(
      kRows,
      [&](auto /*row*/) { return kElementsPerRow; },
      [&](auto index) { return std::string(kValueSize, 'a' + index % 26); },
      [](auto row) { return row % 11 == 0; },
      [](auto index) { return index % 13 == 0; })});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.dataPageVersion = vp::arrow::ParquetDataPageVersion::V2;
  writerOptions.columnDataPageSizeMap["c0.list.element"] = kDataPageSize;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  int64_t totalRows = 0;
  int64_t totalLevels = 0;
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        ASSERT_EQ(vp::arrow::PageType::DATA_PAGE_V2, page->type());
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPageV2>(page);
        ASSERT_EQ(vp::arrow::Encoding::PLAIN, dataPage->encoding());
        // Every non-null array is larger than the page target and therefore
        // must occupy its own page. A neighboring null array may share that
        // page, but no page may contain two non-null array records.
        EXPECT_GT(dataPage->num_rows(), 0);
        EXPECT_LE(dataPage->num_rows(), 2);
        EXPECT_LE(
            dataPage->num_values() - dataPage->num_nulls(), kElementsPerRow);
        totalRows += dataPage->num_rows();
        totalLevels += dataPage->num_values();
      });
  EXPECT_EQ(kRows, totalRows);
  EXPECT_EQ(kExpectedLevels, totalLevels);
  EXPECT_GT(dataPageCount, 2);
}

TEST_F(ParquetWriterTest, nestedFinalOversizedRecordStartsNewPage) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {ARRAY(VARCHAR())});
  const vector_size_t kRows = 2;
  const vector_size_t kElementsPerRow = 32;
  const int64_t kValueSize = 64;
  std::string parquetPath =
      tempPath_->path + "/nestedFinalOversizedRecord.parquet";

  auto data = makeRowVector({makeArrayVector<std::string>(
      kRows,
      [&](auto row) { return row == 0 ? 1 : kElementsPerRow; },
      [&](auto index) { return std::string(kValueSize, 'a' + index % 26); })});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.dataPageVersion = vp::arrow::ParquetDataPageVersion::V2;
  writerOptions.columnDataPageSizeMap["c0.list.element"] = 1024;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  int64_t totalRows = 0;
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        ASSERT_EQ(vp::arrow::PageType::DATA_PAGE_V2, page->type());
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPageV2>(page);
        EXPECT_EQ(1, dataPage->num_rows());
        totalRows += dataPage->num_rows();
      });
  EXPECT_EQ(2, dataPageCount);
  EXPECT_EQ(kRows, totalRows);
}

TEST_F(ParquetWriterTest, byteArrayDictionaryFallbackSplitsPlainPages) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARCHAR()});
  const int64_t kRows = 2048;
  const int64_t kValuePayloadSize = 128;
  const int64_t kDataPageSize = 1024;
  const int64_t kMinEncodedValueSize = kValuePayloadSize + 1 + sizeof(uint32_t);
  const int64_t kMaxPlainValuesPerPage = kDataPageSize / kMinEncodedValueSize;
  std::string parquetPath =
      tempPath_->path + "/dictionaryFallbackPages.parquet";

  auto data = makeRowVector({makeFlatVector<std::string>(kRows, [&](auto row) {
    return std::to_string(row) + std::string(kValuePayloadSize, 'a' + row % 26);
  })});
  vp::WriterOptions writerOptions{};
  writerOptions.dictionaryPageSizeLimit = 512;
  writerOptions.columnDataPageSizeMap[c0] = kDataPageSize;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
  auto reader = createLocalParquetReader(parquetPath);
  auto pageEncodingStats =
      reader->fileMetaData().rowGroup(0).columnChunk(0).pageEncodingStats();
  ASSERT_EQ(3, pageEncodingStats.size());

  auto findPageStats = [&](thrift::PageType::type pageType,
                           thrift::Encoding::type encoding) {
    return std::find_if(
        pageEncodingStats.begin(),
        pageEncodingStats.end(),
        [&](const auto& stats) {
          return stats.page_type == pageType && stats.encoding == encoding;
        });
  };
  const auto dictionaryPage =
      findPageStats(thrift::PageType::DICTIONARY_PAGE, thrift::Encoding::PLAIN);
  const auto dictionaryDataPage = findPageStats(
      thrift::PageType::DATA_PAGE, thrift::Encoding::RLE_DICTIONARY);
  const auto plainDataPage =
      findPageStats(thrift::PageType::DATA_PAGE, thrift::Encoding::PLAIN);

  ASSERT_NE(dictionaryPage, pageEncodingStats.end());
  ASSERT_NE(dictionaryDataPage, pageEncodingStats.end());
  ASSERT_NE(plainDataPage, pageEncodingStats.end());

  int64_t totalValues = 0;
  int64_t observedPlainDataPages = 0;
  forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPage>(page);
        totalValues += dataPage->num_values();
        if (dataPage->encoding() == vp::arrow::Encoding::PLAIN) {
          ++observedPlainDataPages;
          EXPECT_LE(dataPage->num_values(), kMaxPlainValuesPerPage);
        }
      });
  EXPECT_EQ(kRows, totalValues);
  EXPECT_EQ(plainDataPage->count, observedPlainDataPages);
  EXPECT_GT(observedPlainDataPages, 1);
}

TEST_F(ParquetWriterTest, byteArrayDictionaryPageUsesByteBudgetBeforeFallback) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARBINARY()});
  const int64_t kRows = 1024;
  const int64_t kValueSize = 256;
  const int64_t kDictionaryPageSize = 1024;
  const int64_t kEncodedValueSize = kValueSize + sizeof(uint32_t);
  std::string parquetPath =
      tempPath_->path + "/dictionaryPageByteBudget.parquet";

  auto data = makeRowVector({makeFlatVector<std::string>(
      kRows,
      [&](auto row) {
        std::string value = std::to_string(row);
        value.resize(kValueSize, 'a' + row % 26);
        return value;
      },
      [](auto row) { return row % 7 == 0; },
      VARBINARY())});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = true;
  writerOptions.dictionaryPageSizeLimit = kDictionaryPageSize;
  writerOptions.columnDataPageSizeMap[c0] = kDictionaryPageSize;
  writerOptions.compression = CompressionKind::CompressionKind_NONE;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);

  auto fileReader = vp::arrow::ParquetFileReader::OpenFile(parquetPath);
  auto pageReader = fileReader->RowGroup(0)->GetColumnPageReader(0);
  bool sawDictionaryPage = false;
  bool sawPlainDataPage = false;
  while (auto page = pageReader->NextPage()) {
    if (page->type() == vp::arrow::PageType::DICTIONARY_PAGE) {
      sawDictionaryPage = true;
      const auto dictionaryPage =
          std::static_pointer_cast<vp::arrow::DictionaryPage>(page);
      // The soft limit may be crossed by one boundary value, but not by the
      // entire writer batch. This bounds dictionary peak memory before
      // fallback in the same way as the plain data-page path.
      EXPECT_LE(
          dictionaryPage->size(), kDictionaryPageSize + kEncodedValueSize);
    } else if (
        page->type() == vp::arrow::PageType::DATA_PAGE ||
        page->type() == vp::arrow::PageType::DATA_PAGE_V2) {
      const auto dataPage = std::static_pointer_cast<vp::arrow::DataPage>(page);
      sawPlainDataPage |= dataPage->encoding() == vp::arrow::Encoding::PLAIN;
    }
  }
  EXPECT_TRUE(sawDictionaryPage);
  EXPECT_TRUE(sawPlainDataPage);
}

TEST_F(ParquetWriterTest, repeatedByteArrayValuesDoNotForceDictionaryFallback) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARCHAR()});
  const int64_t kRows = 64;
  const int64_t kValueSize = 256;
  const int64_t kDictionaryPageSize = 1024;
  std::string parquetPath =
      tempPath_->path + "/repeatedDictionaryValues.parquet";

  auto data = makeRowVector({makeFlatVector<std::string>(
      kRows, [&](auto /*row*/) { return std::string(kValueSize, 'a'); })});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = true;
  writerOptions.dictionaryPageSizeLimit = kDictionaryPageSize;
  writerOptions.columnDataPageSizeMap[c0] = kDictionaryPageSize;
  writerOptions.compression = CompressionKind::CompressionKind_NONE;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);

  auto reader = createLocalParquetReader(parquetPath);
  const auto pageEncodingStats =
      reader->fileMetaData().rowGroup(0).columnChunk(0).pageEncodingStats();
  const auto plainDataPage = std::find_if(
      pageEncodingStats.begin(),
      pageEncodingStats.end(),
      [](const auto& stats) {
        return stats.page_type == thrift::PageType::DATA_PAGE &&
            stats.encoding == thrift::Encoding::PLAIN;
      });
  EXPECT_EQ(plainDataPage, pageEncodingStats.end());
}

TEST_F(ParquetWriterTest, nullableByteArrayWholeChunkStartsNewPage) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARBINARY()});
  const vector_size_t kRowsPerChunk = 5;
  const int64_t kValueSize = 128;
  const int64_t kDataPageSize = 1024;
  std::string parquetPath =
      tempPath_->path + "/nullableWholeChunkPages.parquet";

  auto makeChunk = [&](char value) {
    return makeRowVector({makeFlatVector<std::string>(
        kRowsPerChunk,
        [&](auto /*row*/) { return std::string(kValueSize, value); },
        [](auto row) { return row == 0; },
        VARBINARY())});
  };
  auto first = makeChunk('a');
  auto second = makeChunk('b');
  auto expected = BaseVector::create(schema, 2 * kRowsPerChunk, pool_.get());
  expected->copy(first.get(), 0, 0, first->size());
  expected->copy(second.get(), kRowsPerChunk, 0, second->size());

  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.dataPageVersion = vp::arrow::ParquetDataPageVersion::V2;
  writerOptions.columnDataPageSizeMap[c0] = kDataPageSize;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(first);
  writer->write(second);
  writer->close();

  assertRead(parquetPath, 2 * kRowsPerChunk, schema, expected);
  int64_t totalValues = 0;
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        ASSERT_EQ(vp::arrow::PageType::DATA_PAGE_V2, page->type());
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPageV2>(page);
        EXPECT_EQ(kRowsPerChunk, dataPage->num_values());
        EXPECT_EQ(1, dataPage->num_nulls());
        totalValues += dataPage->num_values();
      });
  EXPECT_EQ(2, dataPageCount);
  EXPECT_EQ(2 * kRowsPerChunk, totalValues);
}

TEST_F(ParquetWriterTest, singleOversizedByteArrayValueMakesProgress) {
  std::string c0{"c0"};
  auto schema = ROW({c0}, {VARCHAR()});
  const int64_t kValueSize = 2048;
  std::string parquetPath =
      tempPath_->path + "/singleOversizedByteArray.parquet";

  auto data = makeRowVector(
      {makeFlatVector<std::string>({std::string(kValueSize, 'a')})});
  vp::WriterOptions writerOptions{};
  writerOptions.enableDictionary = false;
  writerOptions.columnDataPageSizeMap[c0] = 1024;
  auto writer = createLocalWriter(parquetPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  assertRead(parquetPath, 1, schema, data);
  const auto dataPageCount = forEachDataPage(
      parquetPath, 0, [&](const std::shared_ptr<vp::arrow::Page>& page) {
        const auto dataPage =
            std::static_pointer_cast<vp::arrow::DataPage>(page);
        EXPECT_EQ(vp::arrow::Encoding::PLAIN, dataPage->encoding());
        EXPECT_EQ(1, dataPage->num_values());
      });
  EXPECT_EQ(1, dataPageCount);
}

TEST_F(ParquetWriterTest, arrowPool) {
  const size_t kRows = 4 * 1024;
  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });
  std::string parquetPath = tempPath_->path + "/arrowPool.parquet";

  auto* arrowPool = getArrowMemoryPool();
  vp::WriterOptions writerOptions{};
  auto writer =
      createLocalWriter(parquetPath, schema, writerOptions, nullptr, arrowPool);
  writer->write(data);
  auto* defaultMemoryPool = ::arrow::default_memory_pool();
  ASSERT_EQ(0, defaultMemoryPool->bytes_allocated());
  ASSERT_LT(0, arrowPool->bytes_allocated());
  writer->close();
  ASSERT_EQ(0, arrowPool->bytes_allocated());
  ASSERT_EQ(0, defaultMemoryPool->bytes_allocated());
  auto reader = createLocalParquetReader(parquetPath);
  ASSERT_EQ(reader->numberOfRows(), kRows);
  ASSERT_EQ(*reader->rowType(), *schema);
  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
  assertReadWithReaderAndExpected(
      schema,
      *rowReader,
      std::static_pointer_cast<RowVector>(data),
      *leafPool_);
};

TEST_F(ParquetWriterTest, encoderTestSinkResize0) {
  if (!::bytedance::bolt::kSparkCompatible) {
    GTEST_SKIP();
  }
  int levels_per_page = 100;
  int num_pages = 50;
  auto max_def_level_ = 4;
  auto max_rep_level_ = 0;
  bytedance::bolt::parquet::arrow::schema::NodePtr type =
      bytedance::bolt::parquet::arrow::schema::Int32(
          "b", bytedance::bolt::parquet::arrow::Repetition::OPTIONAL);
  const bytedance::bolt::parquet::arrow::ColumnDescriptor descr(
      type, max_def_level_, max_rep_level_);
  auto encoder = bytedance::bolt::parquet::arrow::MakeEncoder(
      bytedance::bolt::parquet::arrow::Type::INT32,
      bytedance::bolt::parquet::arrow::Encoding::PLAIN,
      false,
      &descr,
      getArrowMemoryPool());
  encoder->testSinkResize0();
};
