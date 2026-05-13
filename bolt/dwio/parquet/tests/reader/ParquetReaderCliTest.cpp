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

#include "bolt/dwio/parquet/reader/ParquetReaderCli.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::test;

const uint32_t kBatchCount = 3;
const uint32_t kNumRows = 1000000;
const uint32_t kNumRowsPerGroup = 10000;
const uint32_t kDefaultReadingSize = 1000;
const uint32_t kDefaultSkipSize = 100000;

class ParquetReaderCliTest : public ParquetTestBase {
 protected:
  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetReaderCliTest");
    leafPool_ = rootPool_->addLeafChild("ParquetReaderCliTest");
  }

  void createFiles(
      uint32_t numFiles,
      std::shared_ptr<const RowType> rowType,
      uint32_t batchCount = kBatchCount,
      uint32_t numRows = kNumRows,
      uint32_t numRowsPerGroup = kNumRowsPerGroup) {
    fileDirectory_ = bytedance::bolt::test::TempDirectoryPath::create();
    auto dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);

    for (uint32_t i = 0; i < numFiles; i++) {
      auto fileName = std::to_string(i) + ".parquet";
      auto path = fileDirectory_->path + "/" + fileName;
      auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
      auto sink =
          std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
      bytedance::bolt::parquet::WriterOptions options;
      options.writeInt96AsTimestamp = true;
      options.parquetWriteTimestampUnit = TimestampUnit::kMicro;
      options.parquetWriteTimestampTimeZone = "UTC";
      options.memoryPool = rootPool_.get();

      auto writer_ = std::make_unique<bytedance::bolt::parquet::Writer>(
          std::move(sink), options, rowType);

      auto batches = dataSetBuilder_->makeDataset(rowType, batchCount, numRows)
                         .withRowGroupSpecificData(numRowsPerGroup)
                         .build();

      for (auto& batch : *batches) {
        writer_->write(batch);
      }
      writer_->flush();
      writer_->close();
      files_.push_back(std::make_shared<LocalReadFile>(path));
    }
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<bytedance::bolt::test::TempDirectoryPath> fileDirectory_;
  std::vector<std::shared_ptr<ReadFile>> files_;
};

TEST_F(ParquetReaderCliTest, readLocalFiles) {
  auto rowType =
      ROW({"bigint", "double", "string"}, {BIGINT(), DOUBLE(), VARCHAR()});
  createFiles(3, rowType);

  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  auto filterBuilder = [&]() {
    SubfieldFilters filters;
    return filters;
  };
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filterBuilder()));

  for (auto file : files_) {
    auto reader = ParquetReaderCli(
        kDefaultReadingSize,
        rootPool_.get(),
        leafPool_.get(),
        FileLocation::LOCAL,
        file,
        rowType,
        scanSpec);
    auto hasData = true;
    uint32_t totalSize = 0;

    while (hasData) {
      auto result = BaseVector::create(rowType, 0, leafPool_.get());
      auto numRowsRead = reader.read(result);

      hasData = numRowsRead != 0;
      if (hasData) {
        auto rowVector = result->asUnchecked<RowVector>();

        totalSize += rowVector->size();
        EXPECT_EQ(numRowsRead, rowVector->size());
      }
    }

    EXPECT_EQ(totalSize, kBatchCount * kNumRows);
  }
}

TEST_F(ParquetReaderCliTest, readLocalFilesWithFilter) {
  auto rowType =
      ROW({"bigint", "double", "string"}, {BIGINT(), DOUBLE(), VARCHAR()});
  createFiles(3, rowType);

  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  auto filterBuilder = [&]() {
    SubfieldFilters filters;
    filters[Subfield("bigint")] =
        std::make_unique<bytedance::bolt::common::BigintRange>(
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max(),
            false);
    return filters;
  };
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filterBuilder()));

  for (auto file : files_) {
    auto reader = ParquetReaderCli(
        kDefaultReadingSize,
        rootPool_.get(),
        leafPool_.get(),
        FileLocation::LOCAL,
        file,
        rowType,
        scanSpec);

    auto hasData = true;
    uint32_t totalSize = 0;

    while (hasData) {
      auto result = BaseVector::create(rowType, 0, leafPool_.get());
      auto numRowsRead = reader.read(result);

      hasData = numRowsRead != 0;

      if (hasData) {
        auto rowVector = result->asUnchecked<RowVector>();
        totalSize += rowVector->size();
      }
    }

    EXPECT_NE(totalSize, kBatchCount * kNumRows);
  }
}

TEST_F(ParquetReaderCliTest, readLocalFilesWithSkip) {
  auto rowType =
      ROW({"bigint", "double", "string"}, {BIGINT(), DOUBLE(), VARCHAR()});
  createFiles(3, rowType);

  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  auto filterBuilder = [&]() {
    SubfieldFilters filters;
    return filters;
  };
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filterBuilder()));

  for (auto file : files_) {
    for (auto skipSize = kDefaultSkipSize; skipSize < kNumRows * kBatchCount;
         skipSize += kDefaultSkipSize) {
      auto reader = ParquetReaderCli(
          kDefaultReadingSize,
          rootPool_.get(),
          leafPool_.get(),
          FileLocation::LOCAL,
          file,
          rowType,
          scanSpec);
      auto hasData = true;
      auto toRead = kDefaultReadingSize;
      uint32_t totalSize = 0;

      reader.skip(skipSize);
      while (hasData) {
        auto result = BaseVector::create(rowType, 0, leafPool_.get());

        auto numRowsRead = reader.read(result, toRead);

        hasData = numRowsRead != 0;
        toRead -= numRowsRead;

        if (hasData) {
          auto rowVector = result->asUnchecked<RowVector>();
          totalSize += rowVector->size();
        }

        if (toRead == 0) {
          break;
        }
      }

      EXPECT_EQ(
          totalSize,
          std::min(kDefaultReadingSize, kNumRows * kBatchCount - skipSize));
    }
  }
}

TEST_F(ParquetReaderCliTest, readLocalFilesWithSkipComplexType) {
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
      "date_val: date,"
      "array_val1:array<float>,"
      "array_val2:array<array<int>>,"
      "array_val3:array<map<int,string>>,"
      "map_val1:map<int,double>,"
      "map_val2:map<bigint,array<string>>,"
      "map_val3:map<bigint,map<string, int>>,"
      "struct_val:struct<a:float,b:double>"
      ">");
  auto rowType = asRowType(type);
  auto numRows = kNumRows / 100;
  createFiles(1, rowType, kBatchCount, numRows, kNumRowsPerGroup / 100);

  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  auto filterBuilder = [&]() {
    SubfieldFilters filters;
    return filters;
  };
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filterBuilder()));

  auto defaultSkipSize = kDefaultSkipSize / 100;
  auto defaultReadingSize = kDefaultReadingSize / 100;
  for (auto file : files_) {
    for (auto skipSize = defaultSkipSize; skipSize < numRows * kBatchCount;
         skipSize += defaultSkipSize) {
      auto reader = ParquetReaderCli(
          defaultReadingSize,
          rootPool_.get(),
          leafPool_.get(),
          FileLocation::LOCAL,
          file,
          rowType,
          scanSpec);
      auto hasData = true;
      auto toRead = defaultReadingSize;
      uint32_t totalSize = 0;

      reader.skip(skipSize);
      while (hasData) {
        auto result = BaseVector::create(rowType, 0, leafPool_.get());

        auto numRowsRead = reader.read(result, toRead);

        hasData = numRowsRead != 0;
        toRead -= numRowsRead;

        if (hasData) {
          auto rowVector = result->asUnchecked<RowVector>();
          totalSize += rowVector->size();
        }

        if (toRead == 0) {
          break;
        }
      }

      EXPECT_EQ(
          totalSize,
          std::min(
              defaultReadingSize, kNumRows / 100 * kBatchCount - skipSize));
    }
  }
}

TEST_F(ParquetReaderCliTest, readLocalFilesWithMultipleSkips) {
  auto rowType =
      ROW({"bigint", "double", "string"}, {BIGINT(), DOUBLE(), VARCHAR()});
  createFiles(3, rowType);

  std::unique_ptr<FilterGenerator> filterGenerator =
      std::make_unique<FilterGenerator>(rowType, 0);
  auto filterBuilder = [&]() {
    SubfieldFilters filters;
    return filters;
  };
  auto scanSpec = filterGenerator->makeScanSpec(std::move(filterBuilder()));

  for (auto file : files_) {
    for (auto skipSize = kDefaultSkipSize; skipSize < kNumRows * kBatchCount;
         skipSize += kDefaultSkipSize) {
      auto reader = ParquetReaderCli(
          kDefaultReadingSize,
          rootPool_.get(),
          leafPool_.get(),
          FileLocation::LOCAL,
          file,
          rowType,
          scanSpec);
      auto hasData = true;
      auto toRead = kDefaultReadingSize;
      uint32_t totalSize = 0;
      uint64_t skipped = 0;

      // skip twice
      skipped += reader.skip(skipSize);
      skipped += reader.skip(skipSize);
      while (hasData) {
        auto result = BaseVector::create(rowType, 0, leafPool_.get());

        auto numRowsRead = reader.read(result, toRead);

        hasData = numRowsRead != 0;
        toRead -= numRowsRead;

        if (hasData) {
          auto rowVector = result->asUnchecked<RowVector>();
          totalSize += rowVector->size();
        }

        if (toRead == 0) {
          break;
        }
      }

      EXPECT_EQ(
          totalSize,
          std::min(
              (uint64_t)kDefaultReadingSize, kNumRows * kBatchCount - skipped));
    }
  }
}
