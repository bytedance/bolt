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

#include <bolt/vector/tests/utils/VectorTestBase.h>
#include <cstdio>

#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/dwrf/reader/DwrfReader.h"
#include "bolt/dwio/orc/writer/OrcWriter.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::orc::writer;
using namespace bytedance::bolt::dwrf;

class OrcWriterTest : public testing::Test, public test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("ParquetTests");
    leafPool_ = rootPool_->addLeafChild("ParquetTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

 protected:
  void assertEqualVectorPart(
      const VectorPtr& expected,
      const VectorPtr& actual,
      vector_size_t offset) {
    ASSERT_GE(expected->size(), actual->size() + offset);
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    for (vector_size_t i = 0; i < actual->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
          << "at " << (i + offset) << ": expected "
          << expected->toString(i + offset) << ", but got "
          << actual->toString(i);
    }
  }

  void assertReadWithReaderAndExpected(
      std::shared_ptr<const RowType> outputType,
      dwio::common::RowReader& reader,
      RowVectorPtr expected,
      memory::MemoryPool& memoryPool) {
    uint64_t total = 0;
    while (total < expected->size()) {
      VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);
      auto part = reader.next(10000, result);
      if (part != 0) {
        assertEqualVectorPart(expected, result, total);
        total += result->size();
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
  }

  std::unique_ptr<ArrowOrcWriter> createLocalWriter(
      const std::string& parquetPath,
      RowTypePtr schema,
      BoltOrcWriteOption& writerOptions) {
    writerOptions.enableFlushBasedOnBlockSize = true;
    auto sink =
        dwio::common::FileSink::create(parquetPath, {.pool = pool_.get()});
    auto sinkPtr = sink.get();

    return std::make_unique<ArrowOrcWriter>(
        std::move(sink),
        writerOptions,
        rootPool_,
        ::arrow::default_memory_pool(),
        schema);
  }

  std::unique_ptr<DwrfReader> createLocalReader(
      const std::string& orcFilePath) {
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    readerOptions.setFileFormat(dwio::common::FileFormat::ORC);
    readerOptions.setMemoryPool(*leafPool_);
    auto reader = DwrfReader::create(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(orcFilePath),
            readerOptions.getMemoryPool()),
        readerOptions);
    return reader;
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};

std::map<CompressionKind, CompressionKind> compressionKindMap = {
    {CompressionKind::CompressionKind_NONE,
     CompressionKind::CompressionKind_NONE},
    {CompressionKind::CompressionKind_SNAPPY,
     CompressionKind::CompressionKind_SNAPPY},
    {CompressionKind::CompressionKind_ZSTD,
     CompressionKind::CompressionKind_ZSTD},
    {CompressionKind::CompressionKind_LZ4,
     CompressionKind::CompressionKind_LZ4},
    // gzip and zlib compression are similar, and zlib is used in orc only.
    // ref: https://orc.apache.org/specification/ORCv1/
    {CompressionKind::CompressionKind_GZIP,
     CompressionKind::CompressionKind_ZLIB},
    {CompressionKind::CompressionKind_ZLIB,
     CompressionKind::CompressionKind_ZLIB}};

TEST_F(OrcWriterTest, compression) {
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
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      schema, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  for (const auto& [inputCompression, actualCompression] : compressionKindMap) {
    std::string orcTestPath = tempPath_->path + "/" +
        common::compressionKindToString(actualCompression) + "_comparison.orc";
    BoltOrcWriteOption writerOptions;
    writerOptions.compression = inputCompression;
    auto writer = createLocalWriter(orcTestPath, schema, writerOptions);
    writer->write(data);
    writer->close();

    auto reader = createLocalReader(orcTestPath);
    ASSERT_EQ(reader->numberOfRows(), kRows);
    ASSERT_EQ(*reader->rowType(), *schema);
    EXPECT_EQ(reader->getCompression(), actualCompression);

    RowReaderOptions rowReaderOptions;
    auto rowReader = reader->createRowReader(rowReaderOptions);
    assertReadWithReaderAndExpected(
        schema,
        *rowReader,
        std::static_pointer_cast<RowVector>(data),
        *leafPool_);
  }
};

TEST_F(OrcWriterTest, base) {
  const size_t kRows = 1100;

  bytedance::bolt::type::fbhive::HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "float_val:float,"
      "double_val:double,"
      "string_val:string,"
      "binary_val:binary,"
      "timestamp_val:timestamp,"
      "date_val:date,"
      "array_val1:array<float>,"
      "array_val2:array<array<int>>,"
      "array_val3:array<map<int,string>>,"
      "map_val1:map<int,double>,"
      "map_val2:map<bigint,array<string>>,"
      "map_val3:map<bigint,map<string, int>>,"
      "struct_val:struct<a:float,b:double>"
      ">");
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  std::string orcTestPath = tempPath_->path + "/OrcWriterTestBase.orc";
  BoltOrcWriteOption writerOptions;
  auto writer = createLocalWriter(orcTestPath, schema, writerOptions);
  writer->write(data);
  writer->close();

  auto reader = createLocalReader(orcTestPath);
  ASSERT_EQ(reader->numberOfRows(), kRows);
  ASSERT_EQ(*reader->rowType(), *schema);

  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  assertReadWithReaderAndExpected(
      schema,
      *rowReader,
      std::static_pointer_cast<RowVector>(data),
      *leafPool_);
};
