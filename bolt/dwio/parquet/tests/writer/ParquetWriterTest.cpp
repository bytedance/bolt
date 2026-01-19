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

#include <cstdio>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h" // @manual
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/arrow/Bridge.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
namespace vp = bytedance::bolt::parquet;
using namespace vp;

class ParquetWriterTest : public ParquetTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    testutil::TestValue::enable();
    bytedance::bolt::connector::hive::CheckHiveConnectorFactoryInit<
        bytedance::bolt::connector::hive::HiveConnectorFactory>();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
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
      std::shared_ptr<::arrow::Schema> arrowSchema = nullptr) {
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
        ::arrow::default_memory_pool(),
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
           BIGINT(), 10000000000L, kRows, leafPool_.get()),
       BaseVector::createConstant(DOUBLE(), 1000.0, kRows, leafPool_.get())});
  std::string parquetPath = tempPath_->path + "/constantToArrow.parquet";
  assertWrite(parquetPath, kRows, schema, data);
};

TEST_F(ParquetWriterTest, splitWrite) {
  const size_t kRows = 4 * 1024;

  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  auto data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  std::string parquetPath = tempPath_->path + "/splitWrite.parquet";
  vp::WriterOptions writerOptions{};
  // Set a smaller value to trigger split write and verify it.
  writerOptions.writeBatchBytes = 1024;
  assertWrite(parquetPath, kRows, schema, data, writerOptions);
};

TEST_F(ParquetWriterTest, columnNullable) {
  const size_t kRows = 1100;
  auto type = getType();
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return false; });

  std::string parquetPath = tempPath_->path + "/columnNullable.parquet";
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
  auto writer = createLocalWriter(
      parquetPath, schema, writerOptions, ::arrow::schema(newFields));
  writer->write(data);
  writer->close();

  assertRead(parquetPath, kRows, schema, data);
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
