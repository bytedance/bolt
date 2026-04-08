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
 * 2026-03-30.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/File.h"
#include "bolt/connectors/hive/iceberg/IcebergConnector.h"
#include "bolt/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "bolt/connectors/hive/iceberg/IcebergSplit.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "gtest/gtest.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

static constexpr const char* kIcebergConnectorId = "test-iceberg";

class IcebergReadTest : public exec::test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    CheckIcebergConnectorFactoryInit<>();
    auto icebergConnector =
        connector::getConnectorFactory(
            IcebergConnectorFactory::kIcebergConnectorName)
            ->newConnector(
                kIcebergConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>{}),
                ioExecutor_.get());
    connector::registerConnector(icebergConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kIcebergConnectorId);
    HiveConnectorTestBase::TearDown();
  }

  void writeParquet(
      const std::string& path,
      const std::vector<RowVectorPtr>& batches,
      bool newRowGroupPerBatch = false) {
    parquet::WriterOptions options;
    if (newRowGroupPerBatch) {
      const auto rowsPerBatch = batches.front()->size();
      options.flushPolicyFactory = [rowsPerBatch]() {
        return std::make_unique<parquet::LambdaFlushPolicy>(
            rowsPerBatch, std::numeric_limits<int64_t>::max(), []() {
              return true;
            });
      };
    }
    auto writerPool = rootPool_->addAggregateChild("IcebergReadTest.Writer");
    options.memoryPool = writerPool.get();
    std::unique_ptr<dwio::common::FileSink> sink =
        std::make_unique<dwio::common::WriteFileSink>(
            std::make_unique<LocalWriteFile>(path, true, false), path);
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink),
        options,
        std::dynamic_pointer_cast<const RowType>(batches[0]->type()));
    for (int i = 0; i < batches.size(); ++i) {
      const auto& batch = batches[i];
      writer->write(batch);
    }
    writer->close();
  }

  std::shared_ptr<connector::ConnectorSplit> makeIcebergSplit(
      const std::string& connectorId,
      const std::string& dataFilePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      std::vector<IcebergDeleteFile> deleteFiles = {},
      std::unordered_map<std::string, std::optional<std::string>>
          partitionKeys = {},
      std::unordered_map<std::string, std::string> infoColumns = {}) {
    return std::make_shared<HiveIcebergSplit>(
        connectorId,
        dataFilePath,
        dwio::common::FileFormat::PARQUET,
        start,
        length,
        std::move(partitionKeys),
        std::nullopt,
        std::unordered_map<std::string, std::string>{},
        nullptr,
        true,
        std::move(deleteFiles),
        std::move(infoColumns));
  }

  std::shared_ptr<connector::ConnectorSplit> makeIcebergSplit(
      const std::string& dataFilePath,
      uint64_t start = 0,
      uint64_t length = std::numeric_limits<uint64_t>::max(),
      std::vector<IcebergDeleteFile> deleteFiles = {},
      std::unordered_map<std::string, std::optional<std::string>>
          partitionKeys = {},
      std::unordered_map<std::string, std::string> infoColumns = {}) {
    return makeIcebergSplit(
        exec::test::kHiveConnectorId,
        dataFilePath,
        start,
        length,
        std::move(deleteFiles),
        std::move(partitionKeys),
        std::move(infoColumns));
  }

  std::shared_ptr<connector::ConnectorSplit> makeIcebergSplit(
      const std::string& dataFilePath,
      std::vector<IcebergDeleteFile> deleteFiles,
      std::unordered_map<std::string, std::optional<std::string>>
          partitionKeys = {},
      std::unordered_map<std::string, std::string> infoColumns = {}) {
    return makeIcebergSplit(
        exec::test::kHiveConnectorId,
        dataFilePath,
        0,
        std::numeric_limits<uint64_t>::max(),
        std::move(deleteFiles),
        std::move(partitionKeys),
        std::move(infoColumns));
  }

  std::shared_ptr<connector::ConnectorSplit> makeIcebergSplitForConnector(
      const std::string& connectorId,
      const std::string& dataFilePath,
      std::vector<IcebergDeleteFile> deleteFiles,
      std::unordered_map<std::string, std::optional<std::string>>
          partitionKeys = {},
      std::unordered_map<std::string, std::string> infoColumns = {}) {
    return makeIcebergSplit(
        connectorId,
        dataFilePath,
        0,
        std::numeric_limits<uint64_t>::max(),
        std::move(deleteFiles),
        std::move(partitionKeys),
        std::move(infoColumns));
  }

  std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandleForConnector(
      const std::string& connectorId,
      const RowTypePtr& dataColumns) {
    return std::make_shared<connector::hive::HiveTableHandle>(
        connectorId,
        "iceberg_table",
        true,
        common::test::SubfieldFilters{},
        nullptr,
        dataColumns);
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeIcebergSplits(
      const std::string& dataFilePath,
      uint32_t splitCount,
      std::vector<IcebergDeleteFile> deleteFiles = {}) {
    const auto fileSize = filesystems::getFileSystem(dataFilePath, nullptr)
                              ->openFileForRead(dataFilePath)
                              ->size();
    const auto splitSize =
        static_cast<uint64_t>((fileSize + splitCount - 1) / splitCount);
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    splits.reserve(splitCount);
    for (uint32_t i = 0; i < splitCount; ++i) {
      splits.push_back(makeIcebergSplit(
          dataFilePath, i * splitSize, splitSize, deleteFiles, {}, {}));
    }
    return splits;
  }
};

TEST_F(IcebergReadTest, appliesPositionalDeletes) {
  auto dataFile = exec::test::TempFilePath::create();
  auto deleteFile = exec::test::TempFilePath::create();

  auto data = makeRowVector(
      {"c0"}, {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})});
  writeParquet(dataFile->path, {data});

  auto deletes = makeRowVector(
      {"file_path", "pos"},
      {makeFlatVector<StringView>(
           std::vector<StringView>(3, StringView(dataFile->path))),
       makeFlatVector<int64_t>({1, 4, 7})});
  writeParquet(deleteFile->path, {deletes});

  auto split = makeIcebergSplit(
      dataFile->path,
      {IcebergDeleteFile(
          FileContent::kPositionalDeletes,
          deleteFile->path,
          dwio::common::FileFormat::PARQUET,
          deletes->size(),
          deleteFile->fileSize())});

  auto outputType = ROW({"c0"}, {BIGINT()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      outputType,
                      makeTableHandle({}, nullptr, "iceberg_table", outputType),
                      {{"c0", regularColumn("c0", BIGINT())}})
                  .planNode();

  auto expected =
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({0, 2, 3, 5, 6, 8, 9})});
  createDuckDbTable({expected});

  assertQuery(
      plan,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>{split},
      "SELECT * FROM tmp");
}

TEST_F(IcebergReadTest, appliesMultipleDeleteFiles) {
  auto dataFile = exec::test::TempFilePath::create();
  auto deleteFile1 = exec::test::TempFilePath::create();
  auto deleteFile2 = exec::test::TempFilePath::create();

  auto data = makeRowVector(
      {"c0"}, {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})});
  writeParquet(dataFile->path, {data});

  auto deletes1 = makeRowVector(
      {"file_path", "pos"},
      {makeFlatVector<StringView>(
           std::vector<StringView>(2, StringView(dataFile->path))),
       makeFlatVector<int64_t>({1, 4})});
  auto deletes2 = makeRowVector(
      {"file_path", "pos"},
      {makeFlatVector<StringView>(
           std::vector<StringView>(2, StringView(dataFile->path))),
       makeFlatVector<int64_t>({2, 7})});
  writeParquet(deleteFile1->path, {deletes1});
  writeParquet(deleteFile2->path, {deletes2});

  auto split = makeIcebergSplit(
      dataFile->path,
      {IcebergDeleteFile(
           FileContent::kPositionalDeletes,
           deleteFile1->path,
           dwio::common::FileFormat::PARQUET,
           deletes1->size(),
           deleteFile1->fileSize()),
       IcebergDeleteFile(
           FileContent::kPositionalDeletes,
           deleteFile2->path,
           dwio::common::FileFormat::PARQUET,
           deletes2->size(),
           deleteFile2->fileSize())});

  auto outputType = ROW({"c0"}, {BIGINT()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      outputType,
                      makeTableHandle({}, nullptr, "iceberg_table", outputType),
                      {{"c0", regularColumn("c0", BIGINT())}})
                  .planNode();

  auto expected =
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({0, 3, 5, 6, 8, 9})});
  createDuckDbTable({expected});

  assertQuery(
      plan,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>{split},
      "SELECT * FROM tmp");
}

TEST_F(IcebergReadTest, readsUsingIcebergConnector) {
  auto dataFile = exec::test::TempFilePath::create();
  auto deleteFile = exec::test::TempFilePath::create();

  auto data = makeRowVector(
      {"c0"}, {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})});
  writeParquet(dataFile->path, {data});

  auto deletes = makeRowVector(
      {"file_path", "pos"},
      {makeFlatVector<StringView>(
           std::vector<StringView>(3, StringView(dataFile->path))),
       makeFlatVector<int64_t>({0, 4, 9})});
  writeParquet(deleteFile->path, {deletes});

  auto split = makeIcebergSplitForConnector(
      kIcebergConnectorId,
      dataFile->path,
      {IcebergDeleteFile(
          FileContent::kPositionalDeletes,
          deleteFile->path,
          dwio::common::FileFormat::PARQUET,
          deletes->size(),
          deleteFile->fileSize())});

  auto outputType = ROW({"c0"}, {BIGINT()});
  auto plan =
      exec::test::PlanBuilder()
          .tableScan(
              outputType,
              makeTableHandleForConnector(kIcebergConnectorId, outputType),
              {{"c0", regularColumn("c0", BIGINT())}})
          .planNode();

  auto expected =
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3, 5, 6, 7, 8})});
  createDuckDbTable({expected});

  assertQuery(
      plan,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>{split},
      "SELECT * FROM tmp");
}

TEST_F(IcebergReadTest, backfillsPartitionAndInfoColumns) {
  auto dataFile = exec::test::TempFilePath::create();

  auto data = makeRowVector({"c0"}, {makeFlatVector<int64_t>({10, 20, 30})});
  writeParquet(dataFile->path, {data});

  auto split = makeIcebergSplit(
      dataFile->path,
      {},
      {{"ds", std::make_optional<std::string>("2024-01-01")}},
      {{"meta_file_size", std::to_string(dataFile->fileSize())}});

  auto outputType =
      ROW({"c0", "ds", "meta_file_size"}, {BIGINT(), VARCHAR(), BIGINT()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      outputType,
                      makeTableHandle({}, nullptr, "iceberg_table", outputType),
                      {{"c0", regularColumn("c0", BIGINT())},
                       {"ds", partitionKey("ds", VARCHAR())},
                       {"meta_file_size",
                        synthesizedColumn("meta_file_size", BIGINT())}})
                  .planNode();

  auto expected = makeRowVector(
      {"c0", "ds", "meta_file_size"},
      {makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<StringView>(
           std::vector<StringView>(3, StringView("2024-01-01"))),
       makeFlatVector<int64_t>(std::vector<int64_t>(3, dataFile->fileSize()))});
  createDuckDbTable({expected});

  assertQuery(
      plan,
      std::vector<std::shared_ptr<connector::ConnectorSplit>>{split},
      "SELECT * FROM tmp");
}

TEST_F(IcebergReadTest, appliesDeletesAcrossMultipleSplits) {
  auto dataFile = exec::test::TempFilePath::create();
  auto deleteFile = exec::test::TempFilePath::create();

  auto firstBatch =
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({0, 1, 2, 3, 4})});
  auto secondBatch =
      makeRowVector({"c0"}, {makeFlatVector<int64_t>({5, 6, 7, 8, 9})});
  writeParquet(dataFile->path, {firstBatch, secondBatch}, true);

  auto deletes = makeRowVector(
      {"file_path", "pos"},
      {makeFlatVector<StringView>(
           std::vector<StringView>(2, StringView(dataFile->path))),
       makeFlatVector<int64_t>({6, 8})});
  writeParquet(deleteFile->path, {deletes});

  auto splits = makeIcebergSplits(
      dataFile->path,
      2,
      {IcebergDeleteFile(
          FileContent::kPositionalDeletes,
          deleteFile->path,
          dwio::common::FileFormat::PARQUET,
          deletes->size(),
          deleteFile->fileSize())});
  ASSERT_EQ(splits.size(), 2);

  auto outputType = ROW({"c0"}, {BIGINT()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      outputType,
                      makeTableHandle({}, nullptr, "iceberg_table", outputType),
                      {{"c0", regularColumn("c0", BIGINT())}})
                  .planNode();

  auto expected = makeRowVector(
      {"c0"}, {makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 7, 9})});
  createDuckDbTable({expected});

  assertQuery(plan, splits, "SELECT * FROM tmp");
}

} // namespace
} // namespace bytedance::bolt::connector::hive::iceberg
