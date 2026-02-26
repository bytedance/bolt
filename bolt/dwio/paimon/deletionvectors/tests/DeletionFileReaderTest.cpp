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

#include <gtest/gtest.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/paimon/deletionvectors/DeletionFileReader.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::paimon {
class DeleteionFileReaderTest : public parquet::ParquetTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
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
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return bytedance::bolt::test::getDataFilePath("./examples/" + fileName);
  }

  std::unique_ptr<parquet::ParquetReader> createLocalParquetReader(
      const std::string& parquetPath) {
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    timestampPrecision_ = TimestampPrecision::kNanoseconds;
    return std::make_unique<parquet::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<LocalReadFile>(parquetPath),
            readerOptions.getMemoryPool()),
        readerOptions);
  }

  std::unique_ptr<dwio::common::RowReader> createRowReaderWithSchema(
      const std::unique_ptr<dwio::common::Reader> reader,
      const RowTypePtr& rowType) {
    auto rowReaderOpts = getReaderOpts(rowType);
    auto scanSpec = makeScanSpec(rowType);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  };

  VectorPtr readRows(
      std::string parquetPath,
      const SelectivityVector& rows,
      memory::MemoryPool* memoryPool) {
    auto reader = createLocalParquetReader(parquetPath);
    auto schema = reader->fileSchema();
    auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
    std::vector<VectorPtr> data;
    VectorPtr output = BaseVector::create(schema, 0, memoryPool);
    while (true) {
      auto part = rowReader->next(1024, output);
      if (part > 0) {
        data.emplace_back(output);
      } else {
        break;
      }
    }
    BOLT_CHECK(!data.empty())

    VectorPtr fullRows = std::move(data[0]);
    for (int i = 1; i < data.size(); ++i) {
      fullRows->append(data[i].get());
    }
    VectorPtr result = BaseVector::create(schema, rows.end(), memoryPool);
    int resultIndex{0};
    rows.applyToSelected([&](auto row) {
      result->copy(fullRows.get(), resultIndex, row, 1);
      resultIndex++;
    });
    result->resize(resultIndex);
    return result;
  }

  void assertReadWithDeletionVector(
      std::shared_ptr<const RowType> outputType,
      dwio::common::RowReader* reader,
      DeletionFileReader* deletionFileReader,
      RowVectorPtr expected,
      memory::MemoryPool* memoryPool) {
    BufferPtr deletionVector;
    uint64_t total = 0;
    uint64_t batchSize = 1024;
    VectorPtr result = BaseVector::create(outputType, 0, memoryPool);
    while (true) {
      auto nextRowNumber = reader->nextRowNumber();
      if (nextRowNumber == dwio::common::RowReader::kAtEnd) {
        break;
      }
      auto readSize = reader->nextReadSize(batchSize);
      deletionFileReader->getDeletionVector(
          nextRowNumber, readSize, &deletionVector);
      dwio::common::Mutation mutation;
      mutation.deletedRows = deletionVector->as<uint64_t>();
      auto part = reader->next(batchSize, result, &mutation);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += result->size();
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader->next(batchSize, result), 0);
  }

  inline static const std::string kHiveConnectorId = "test-hive-paimon";
};

TEST_F(DeleteionFileReaderTest, normal) {
  // {bin1.parquet=(offset: 1, size: 12, deleteRows: {})
  // bin2.parquet=(offset: 21, size: 32, deleteRows: {1, 5, 6, 9, 13, 14}),
  // bin3.parquet=(offset: 61, size: 26, deleteRows: {3, 5, 9})}
  std::string deletionFile = getExampleFilePath("deletionFile");
  std::string parquet1Path = getExampleFilePath("bin1.parquet");
  const int32_t KRows = 15;
  SelectivityVector rows(KRows, true);
  rows.setValid(1, false);
  rows.setValid(5, false);
  rows.setValid(6, false);
  rows.setValid(9, false);
  rows.setValid(13, false);
  rows.setValid(14, false);
  auto expectedData = readRows(parquet1Path, rows, leafPool_.get());

  auto reader = createLocalParquetReader(parquet1Path);
  ASSERT_EQ(KRows, reader->numberOfRows());
  auto schema = reader->fileSchema();
  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);

  auto deletionFileInput = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(deletionFile), *leafPool_);
  auto options = DeletionFileReader::Options{
      .offset = 21, .size = 32, .memoryPool = leafPool_.get()};
  auto deletionFileReader = std::make_unique<DeletionFileReader>(
      std::move(deletionFileInput), options);
  assertReadWithDeletionVector(
      schema,
      rowReader.get(),
      deletionFileReader.get(),
      std::static_pointer_cast<RowVector>(expectedData),
      leafPool_.get());
};

TEST_F(DeleteionFileReaderTest, deletionFile) {
  auto deletionFilePath = getExampleFilePath("deletionFile");
  auto deletionFileInput = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(deletionFilePath), *leafPool_);
  auto options = DeletionFileReader::Options{
      .offset = 61, .size = 26, .memoryPool = leafPool_.get()};
  auto deletionFileReader = std::make_unique<DeletionFileReader>(
      std::move(deletionFileInput), options);
  BufferPtr deletionVector;
  deletionFileReader->getDeletionVector(0, 15, &deletionVector);
  ASSERT_GE(deletionVector->size(), bits::nbytes(15));
  ASSERT_TRUE(bits::isBitSet(deletionVector->as<uint8_t>(), 3));
  ASSERT_TRUE(bits::isBitSet(deletionVector->as<uint8_t>(), 5));
  ASSERT_TRUE(bits::isBitSet(deletionVector->as<uint8_t>(), 9));
  ASSERT_EQ(3, bits::countBits(deletionVector->as<uint64_t>(), 0, 15));
};

} // namespace bytedance::bolt::paimon
