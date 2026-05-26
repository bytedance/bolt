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

#include "bolt/dwio/txt/writer/TxtWriter.h"
#include <cstdio>
#include "bolt/common/testutil/TempDirectoryPath.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/dwio/txt/reader/TxtReader.h"
#include "bolt/dwio/txt/tests/TxtTestBase.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::txt;
using namespace bytedance::bolt::txt::writer;
using namespace bytedance::bolt::txt::reader;

class TxtWriterTest : public bytedance::bolt::txt::TxtTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("txtWriterTests");
    leafPool_ = rootPool_->addLeafChild("txtWriterTests");
    tempPath_ = bytedance::bolt::test::TempDirectoryPath::create();
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

  std::unique_ptr<TxtWriter> createLocalWriter(
      const std::string& txtPath,
      RowTypePtr schema) {
    BoltTxtWriteOption writerOptions;
    writerOptions.enableFlushBasedOnBlockSize = true;
    auto sink = dwio::common::FileSink::create(txtPath, {.pool = pool_.get()});
    auto sinkPtr = sink.get();

    return std::make_unique<TxtWriter>(
        std::move(sink),
        writerOptions,
        rootPool_,
        ::arrow::default_memory_pool(),
        schema);
  }

  std::unique_ptr<TxtReader> createLocalReader(
      const std::string& txtFilePath,
      RowTypePtr schema) {
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    readerOptions.setFileFormat(dwio::common::FileFormat::TEXT);
    readerOptions.setMemoryPool(*leafPool_);
    SerDeOptions serDe(uint8_t(','), uint8_t(';'), uint8_t(':'));
    serDe.nullString = "";
    readerOptions.setSerDeOptions(serDe);
    readerOptions.setFileSchema(schema);
    auto reader = std::make_unique<TxtReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(txtFilePath),
            readerOptions.getMemoryPool()),
        readerOptions);
    return reader;
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<bytedance::bolt::test::TempDirectoryPath> tempPath_;
};

TEST_F(TxtWriterTest, comparison) {
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
      //    below types are not supported for now:
      //      "date_val:date,"
      //      "array_val1:array<float>,"
      //      "array_val2:array<array<int>>,"
      //      "array_val3:array<map<int,string>>,"
      //      "map_val1:map<int,double>,"
      //      "map_val2:map<bigint,array<string>>,"
      //      "map_val3:map<bigint,map<string, int>>,"
      //      "struct_val:struct<a:float,b:double>"
      ">");
  auto schema = std::static_pointer_cast<const RowType>(type);
  VectorPtr data = bytedance::bolt::test::BatchMaker::createBatch(
      type, kRows, *leafPool_, [](auto row) { return row % 10 == 0; });

  std::string temp_dir = ::testing::TempDir();

  // Create a file path within the temporary directory
  std::string txtTestPath = temp_dir + "/TxtWriterTest.txt";
  auto writer = createLocalWriter(txtTestPath, schema);
  writer->write(data);
  writer->close();

  auto reader = createLocalReader(txtTestPath, schema);
  RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(makeScanSpec(schema));
  auto rowReader = reader->createRowReader(rowReaderOptions);
  assertReadWithReaderAndExpected(
      schema,
      *rowReader,
      std::static_pointer_cast<RowVector>(data),
      *leafPool_);
  std::remove(txtTestPath.c_str());
};
