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

#include "bolt/connectors/hive/iceberg/IcebergConnector.h"

#include <filesystem>
#include <limits>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/encode/Base64.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "bolt/connectors/hive/iceberg/IcebergParquetStatsCollector.h"
#include "bolt/connectors/hive/iceberg/IcebergSplit.h"
#include "bolt/connectors/hive/iceberg/PartitionSpec.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/expression/Expr.h"

using namespace bytedance::bolt;

namespace bytedance::bolt::connector::hive::iceberg {
namespace {

template <typename T>
T decodeBound(const std::string& encoded) {
  const auto decoded = encoding::Base64::decode(encoded);
  EXPECT_EQ(decoded.size(), sizeof(T));
  T value;
  std::memcpy(&value, decoded.data(), sizeof(T));
  return value;
}

static constexpr const char* kIcebergConnectorId = "test-iceberg";

class IcebergWriteTest : public exec::test::HiveConnectorTestBase {
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

    root_ = memory::memoryManager()->addRootPool(
        "IcebergWriteTest", 1L << 30, exec::MemoryReclaimer::create());
    opPool_ = root_->addLeafChild("operator");
    connectorPool_ =
        root_->addAggregateChild("connector", exec::MemoryReclaimer::create());
    expressionQueryCtx_ = core::QueryCtx::create();
    connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
        opPool_.get(),
        connectorPool_.get(),
        connectorSessionProperties_.get(),
        nullptr,
        nullptr,
        std::make_unique<exec::SimpleExpressionEvaluator>(
            expressionQueryCtx_.get(), opPool_.get()),
        nullptr,
        "query.IcebergWriteTest",
        "task.IcebergWriteTest",
        "planNodeId.IcebergWriteTest",
        0);
  }

  void TearDown() override {
    connectorQueryCtx_.reset();
    expressionQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();
    connector::unregisterConnector(kIcebergConnectorId);
    HiveConnectorTestBase::TearDown();
  }

  std::shared_ptr<IcebergInsertTableHandle> makeInsertHandle(
      const RowTypePtr& rowType,
      const std::string& outputDirectory,
      const IcebergPartitionSpecPtr& partitionSpec = nullptr,
      FieldIdToColumnPathMap fieldIdToColumnPath = {}) {
    std::vector<IcebergColumnHandlePtr> columns;
    columns.reserve(rowType->size());
    for (auto i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      const auto& type = rowType->childAt(i);
      auto fieldId = i + 1;
      if (!fieldIdToColumnPath.empty()) {
        for (const auto& [candidateId, path] : fieldIdToColumnPath) {
          if (path == name) {
            fieldId = candidateId;
            break;
          }
        }
      }
      columns.push_back(std::make_shared<const IcebergColumnHandle>(
          name,
          HiveColumnHandle::ColumnType::kRegular,
          type,
          parquet::ParquetFieldId{fieldId, {}}));
    }

    return std::make_shared<IcebergInsertTableHandle>(
        columns,
        std::make_shared<LocationHandle>(
            outputDirectory, outputDirectory, LocationHandle::TableType::kNew),
        dwio::common::FileFormat::PARQUET,
        partitionSpec,
        common::CompressionKind::CompressionKind_ZSTD,
        std::unordered_map<std::string, std::string>{},
        std::move(fieldIdToColumnPath));
  }

  std::vector<std::string> listFiles(const std::string& dirPath) {
    std::vector<std::string> files;
    for (auto& dirEntry :
         std::filesystem::recursive_directory_iterator(dirPath)) {
      if (dirEntry.is_regular_file()) {
        files.push_back(dirEntry.path().string());
      }
    }
    return files;
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>> makeIcebergSplits(
      const std::vector<std::string>& files) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    splits.reserve(files.size());
    for (const auto& file : files) {
      auto readFile =
          filesystems::getFileSystem(file, nullptr)->openFileForRead(file);
      splits.push_back(std::make_shared<HiveIcebergSplit>(
          kIcebergConnectorId,
          file,
          dwio::common::FileFormat::PARQUET,
          0,
          readFile->size(),
          std::unordered_map<std::string, std::optional<std::string>>{},
          std::nullopt,
          std::unordered_map<std::string, std::string>{},
          nullptr,
          true,
          std::vector<IcebergDeleteFile>{},
          std::unordered_map<std::string, std::string>{}));
    }
    return splits;
  }

  std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandleForConnector(
      const RowTypePtr& rowType) {
    return std::make_shared<connector::hive::HiveTableHandle>(
        kIcebergConnectorId,
        "iceberg_table",
        true,
        common::test::SubfieldFilters{},
        nullptr,
        rowType);
  }

  std::shared_ptr<parquet::ParquetReader> createParquetReader(
      const std::string& path) {
    dwio::common::ReaderOptions readerOptions{pool()};
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(path), readerOptions.getMemoryPool());
    return std::make_shared<parquet::ParquetReader>(
        std::move(input), readerOptions);
  }

  std::string writeParquetWithRowGroupSize(
      const RowTypePtr& rowType,
      const RowVectorPtr& input,
      uint64_t rowsInRowGroup) {
    auto outputDir = exec::test::TempDirectoryPath::create();
    tempDirs_.push_back(outputDir);
    auto filePath = fmt::format("{}/stats.parquet", outputDir->path);
    dwio::common::FileSink::Options sinkOptions;
    sinkOptions.pool = root_.get();
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", filePath), sinkOptions);

    parquet::WriterOptions options;
    options.memoryPool = root_.get();
    options.flushPolicyFactory = [rowsInRowGroup]() {
      return std::make_unique<parquet::DefaultFlushPolicy>(
          rowsInRowGroup, 1 << 20);
    };

    auto writerPool = connectorPool_->addAggregateChild("parquet_stats_writer");
    parquet::Writer writer(
        std::move(sink),
        options,
        writerPool,
        ::arrow::default_memory_pool(),
        rowType);
    writer.write(input);
    writer.close();
    return filePath;
  }

  std::shared_ptr<parquet::arrow::FileMetaData> writeParquetAndGetMetadata(
      const RowTypePtr& rowType,
      const RowVectorPtr& input,
      uint64_t rowsInRowGroup) {
    auto outputDir = exec::test::TempDirectoryPath::create();
    tempDirs_.push_back(outputDir);
    auto filePath = fmt::format("{}/stats.parquet", outputDir->path);
    dwio::common::FileSink::Options sinkOptions;
    sinkOptions.pool = root_.get();
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", filePath), sinkOptions);

    parquet::WriterOptions options;
    options.memoryPool = root_.get();
    options.flushPolicyFactory = [rowsInRowGroup]() {
      return std::make_unique<parquet::DefaultFlushPolicy>(
          rowsInRowGroup, 1 << 20);
    };

    auto writerPool =
        connectorPool_->addAggregateChild("parquet_stats_metadata_writer");
    parquet::Writer writer(
        std::move(sink),
        options,
        writerPool,
        ::arrow::default_memory_pool(),
        rowType);
    writer.write(input);
    writer.close();
    return writer.metadata();
  }

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> opPool_;
  std::shared_ptr<memory::MemoryPool> connectorPool_;
  std::shared_ptr<core::QueryCtx> expressionQueryCtx_;
  std::shared_ptr<config::ConfigBase> connectorSessionProperties_ =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>());
  std::unique_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  core::QueryConfig queryConfig_{{}};
  std::vector<std::shared_ptr<exec::test::TempDirectoryPath>> tempDirs_;
};

TEST_F(IcebergWriteTest, basicCommitMessageAndReadBack) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto input = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<StringView>({"a", "b", "c"})});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 1);
  auto task = folly::parseJson(commitTasks[0]);
  EXPECT_EQ(task["partitionSpecJson"].asInt(), 0);
  EXPECT_EQ(task["sortOrderId"].asInt(), 0);
  EXPECT_EQ(task["fileFormat"].asString(), "PARQUET");
  EXPECT_EQ(task["content"].asString(), "DATA");
  EXPECT_EQ(task["metrics"]["recordCount"].asInt(), 3);
  EXPECT_TRUE(task["metrics"]["columnSizes"].count("1"));
  EXPECT_TRUE(task["metrics"]["columnSizes"].count("2"));
  EXPECT_EQ(task["metrics"]["valueCounts"]["1"].asInt(), 3);
  EXPECT_EQ(task["metrics"]["valueCounts"]["2"].asInt(), 3);
  EXPECT_EQ(task["metrics"]["nullValueCounts"]["1"].asInt(), 0);
  EXPECT_EQ(task["metrics"]["nullValueCounts"]["2"].asInt(), 0);
  EXPECT_TRUE(task["metrics"]["lowerBounds"].count("1"));
  EXPECT_TRUE(task["metrics"]["lowerBounds"].count("2"));
  EXPECT_TRUE(task["metrics"]["upperBounds"].count("1"));
  EXPECT_TRUE(task["metrics"]["upperBounds"].count("2"));
  EXPECT_GT(task["fileSizeInBytes"].asInt(), 0);

  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 1);
  EXPECT_EQ(task["path"].asString(), files[0]);

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"c0", regularColumn("c0", BIGINT())},
                       {"c1", regularColumn("c1", VARCHAR())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, commitMessageContainsNullCountsAndBounds) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto input = makeRowVector(
      {"c0", "c1"},
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 3}),
       makeNullableFlatVector<StringView>({"a", "b", std::nullopt})});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 1);
  auto task = folly::parseJson(commitTasks[0]);
  EXPECT_EQ(task["metrics"]["nullValueCounts"]["1"].asInt(), 1);
  EXPECT_EQ(task["metrics"]["nullValueCounts"]["2"].asInt(), 1);
  EXPECT_TRUE(task["metrics"]["lowerBounds"].count("1"));
  EXPECT_TRUE(task["metrics"]["lowerBounds"].count("2"));
  EXPECT_TRUE(task["metrics"]["upperBounds"].count("1"));
  EXPECT_TRUE(task["metrics"]["upperBounds"].count("2"));
}

TEST_F(IcebergWriteTest, insertHandleRequiresParquet) {
  auto rowType = ROW({"c0"}, {BIGINT()});
  std::vector<IcebergColumnHandlePtr> columns{
      std::make_shared<const IcebergColumnHandle>(
          "c0",
          HiveColumnHandle::ColumnType::kRegular,
          BIGINT(),
          parquet::ParquetFieldId{1, {}})};

  BOLT_ASSERT_THROW(
      std::make_shared<IcebergInsertTableHandle>(
          columns,
          std::make_shared<LocationHandle>(
              "/tmp/target", "/tmp/target", LocationHandle::TableType::kNew),
          dwio::common::FileFormat::DWRF),
      "Only Parquet file format is supported when writing Iceberg tables.");
}

TEST_F(IcebergWriteTest, aggregatesBoundsAcrossMultipleRowGroups) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
  auto input = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int64_t>({5, 10, -7, 99}),
       makeFlatVector<StringView>({"m", "z", "aa", "zz"})});
  auto filePath = writeParquetWithRowGroupSize(rowType, input, 2);

  IcebergParquetStatsCollector collector(pool());
  auto stats = collector.aggregate(filePath);

  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->columnStats.contains(1));
  ASSERT_TRUE(stats->columnStats.contains(2));

  const auto& c0Stats = stats->columnStats.at(1);
  ASSERT_TRUE(c0Stats.lowerBound.has_value());
  ASSERT_TRUE(c0Stats.upperBound.has_value());
  EXPECT_EQ(decodeBound<int64_t>(*c0Stats.lowerBound), -7);
  EXPECT_EQ(decodeBound<int64_t>(*c0Stats.upperBound), 99);

  const auto& c1Stats = stats->columnStats.at(2);
  ASSERT_TRUE(c1Stats.lowerBound.has_value());
  ASSERT_TRUE(c1Stats.upperBound.has_value());
  EXPECT_EQ(encoding::Base64::decode(*c1Stats.lowerBound), "aa");
  EXPECT_EQ(encoding::Base64::decode(*c1Stats.upperBound), "zz");
}

TEST_F(IcebergWriteTest, aggregatesIcebergStringBounds) {
  auto rowType = ROW({"c0"}, {VARCHAR()});
  auto input = makeRowVector(
      {"c0"},
      {makeFlatVector<StringView>(
          {"Customer#0000010_alpha_suffix", "Customer#000009z_omega_suffix"})});
  auto filePath = writeParquetWithRowGroupSize(rowType, input, 1);

  IcebergParquetStatsCollector collector(pool());
  auto stats = collector.aggregate(filePath);

  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->columnStats.contains(1));
  const auto& c0Stats = stats->columnStats.at(1);
  ASSERT_TRUE(c0Stats.lowerBound.has_value());
  ASSERT_TRUE(c0Stats.upperBound.has_value());
  EXPECT_EQ(encoding::Base64::decode(*c0Stats.lowerBound), "Customer#0000010");
  EXPECT_EQ(encoding::Base64::decode(*c0Stats.upperBound), "Customer#000009{");
}

TEST_F(IcebergWriteTest, aggregatesDecimalBounds) {
  auto rowType = ROW({"c0"}, {DECIMAL(12, 2)});
  auto input = makeRowVector(
      {"c0"}, {makeFlatVector<int64_t>({12345, -501, 5}, DECIMAL(12, 2))});
  auto filePath = writeParquetWithRowGroupSize(rowType, input, 2);

  IcebergParquetStatsCollector collector(pool());
  auto stats = collector.aggregate(filePath);

  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->columnStats.contains(1));
  const auto& c0Stats = stats->columnStats.at(1);
  ASSERT_TRUE(c0Stats.lowerBound.has_value());
  ASSERT_TRUE(c0Stats.upperBound.has_value());
  EXPECT_FALSE(encoding::Base64::decode(*c0Stats.lowerBound).empty());
  EXPECT_FALSE(encoding::Base64::decode(*c0Stats.upperBound).empty());
}

TEST_F(IcebergWriteTest, aggregatesNanValueCounts) {
  auto nestedType = ROW({"d"}, {DOUBLE()});
  auto rowType = ROW({"f", "s"}, {REAL(), nestedType});
  auto input = makeRowVector(
      {"f", "s"},
      {makeNullableFlatVector<float>(
           {1.0f,
            std::numeric_limits<float>::quiet_NaN(),
            std::nullopt,
            std::numeric_limits<float>::quiet_NaN()}),
       makeRowVector(
           {"d"},
           {makeNullableFlatVector<double>(
               {std::numeric_limits<double>::quiet_NaN(),
                2.0,
                std::numeric_limits<double>::quiet_NaN(),
                std::nullopt})})});
  auto filePath = writeParquetWithRowGroupSize(rowType, input, 2);

  IcebergParquetStatsCollector collector(pool());
  auto stats = collector.aggregate(filePath);

  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->columnStats.contains(1));
  ASSERT_TRUE(stats->columnStats.contains(3));
  ASSERT_TRUE(stats->columnStats.at(1).nanValueCount.has_value());
  ASSERT_TRUE(stats->columnStats.at(3).nanValueCount.has_value());
  EXPECT_EQ(stats->columnStats.at(1).nanValueCount.value(), 2);
  EXPECT_EQ(stats->columnStats.at(3).nanValueCount.value(), 2);
}

TEST_F(IcebergWriteTest, aggregatesNanValueCountsFromWriterMetadata) {
  auto nestedType = ROW({"d"}, {DOUBLE()});
  auto rowType = ROW({"f", "s"}, {REAL(), nestedType});
  auto input = makeRowVector(
      {"f", "s"},
      {makeNullableFlatVector<float>(
           {1.0f,
            std::numeric_limits<float>::quiet_NaN(),
            std::nullopt,
            std::numeric_limits<float>::quiet_NaN()}),
       makeRowVector(
           {"d"},
           {makeNullableFlatVector<double>(
               {std::numeric_limits<double>::quiet_NaN(),
                2.0,
                std::numeric_limits<double>::quiet_NaN(),
                std::nullopt})})});

  auto metadata = writeParquetAndGetMetadata(rowType, input, 2);

  std::vector<IcebergColumnHandlePtr> columns{
      std::make_shared<const IcebergColumnHandle>(
          "f",
          HiveColumnHandle::ColumnType::kRegular,
          REAL(),
          parquet::ParquetFieldId{1, {}}),
      std::make_shared<const IcebergColumnHandle>(
          "s",
          HiveColumnHandle::ColumnType::kRegular,
          nestedType,
          parquet::ParquetFieldId{2, {parquet::ParquetFieldId{3, {}}}})};
  IcebergParquetStatsCollector collector(columns, pool());
  auto stats = collector.aggregate(metadata);

  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->columnStats.contains(1));
  ASSERT_TRUE(stats->columnStats.contains(3));
  ASSERT_TRUE(stats->columnStats.at(1).nanValueCount.has_value());
  ASSERT_TRUE(stats->columnStats.at(3).nanValueCount.has_value());
  EXPECT_EQ(stats->columnStats.at(1).nanValueCount.value(), 2);
  EXPECT_EQ(stats->columnStats.at(3).nanValueCount.value(), 2);
}

TEST_F(IcebergWriteTest, collectorBuildsParquetFieldIdsAndSkipBoundsMetadata) {
  auto nestedType = ROW(
      {"s", "arr"}, {ROW({"x", "y"}, {BIGINT(), VARCHAR()}), ARRAY(INTEGER())});
  std::vector<IcebergColumnHandlePtr> columns{
      std::make_shared<const IcebergColumnHandle>(
          "nested",
          HiveColumnHandle::ColumnType::kRegular,
          nestedType,
          parquet::ParquetFieldId{
              1,
              {
                  parquet::ParquetFieldId{
                      2,
                      {parquet::ParquetFieldId{3, {}},
                       parquet::ParquetFieldId{4, {}}}},
                  parquet::ParquetFieldId{5, {parquet::ParquetFieldId{6, {}}}},
              }})};

  IcebergParquetStatsCollector collector(columns, pool());

  const auto& fields = collector.parquetFieldIds().children;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].fieldId, 1);
  ASSERT_EQ(fields[0].children.size(), 2);
  EXPECT_EQ(fields[0].children[0].fieldId, 2);
  EXPECT_EQ(fields[0].children[0].children[0].fieldId, 3);
  EXPECT_EQ(fields[0].children[0].children[1].fieldId, 4);
  EXPECT_EQ(fields[0].children[1].fieldId, 5);
  EXPECT_EQ(fields[0].children[1].children[0].fieldId, 6);
}

TEST_F(IcebergWriteTest, writesParquetFieldIds) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto structType = ROW({"x", "y"}, {BIGINT(), VARCHAR()});
  auto rowType = ROW({"id", "payload", "s"}, {BIGINT(), VARCHAR(), structType});
  auto input = makeRowVector(
      {"id", "payload", "s"},
      {makeFlatVector<int64_t>({1, 2}),
       makeFlatVector<StringView>({"a", "b"}),
       makeRowVector(
           {"x", "y"},
           {makeFlatVector<int64_t>({10, 20}),
            makeFlatVector<StringView>({"u", "v"})})});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(
          rowType,
          outputDir->path,
          nullptr,
          {{1, "id"}, {2, "payload"}, {3, "s"}, {4, "s.x"}, {5, "s.y"}}),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();
  ASSERT_EQ(commitTasks.size(), 1);

  auto filePath = folly::parseJson(commitTasks[0])["path"].asString();
  auto reader = createParquetReader(filePath);
  auto typeWithId = reader->typeWithId();
  ASSERT_NE(typeWithId, nullptr);
  EXPECT_EQ(typeWithId->childByName("id")->id(), 1);
  EXPECT_EQ(typeWithId->childByName("payload")->id(), 2);
  auto structNode = typeWithId->childByName("s");
  ASSERT_NE(structNode, nullptr);
  EXPECT_EQ(structNode->id(), 3);
  EXPECT_EQ(structNode->childAt(0)->id(), 4);
  EXPECT_EQ(structNode->childAt(1)->id(), 5);
}

TEST_F(IcebergWriteTest, identityPartitionCommitMessage) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"ds", "c0"}, {VARCHAR(), BIGINT()});
  auto input = makeRowVector(
      {"ds", "c0"},
      {makeFlatVector<StringView>({"2024-01-01", "2024-01-02"}),
       makeFlatVector<int64_t>({10, 20})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"ds", VARCHAR(), TransformType::kIdentity, std::nullopt}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 2);
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 2);
  EXPECT_NE((files[0] + files[1]).find("/ds=2024-01-0"), std::string::npos);

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 1);
    ASSERT_TRUE(partitionData["partitionValues"][0].isString());
    EXPECT_TRUE(
        partitionData["partitionValues"][0].asString() == "2024-01-01" ||
        partitionData["partitionValues"][0].asString() == "2024-01-02");
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"ds", regularColumn("ds", VARCHAR())},
                       {"c0", regularColumn("c0", BIGINT())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, yearPartitionCommitMessageAndReadBack) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"ts", "c0"}, {TIMESTAMP(), BIGINT()});
  auto input = makeRowVector(
      {"ts", "c0"},
      {makeFlatVector<Timestamp>(
           {Timestamp::fromMillis(1704067200000),
            Timestamp::fromMillis(1735689600000)}),
       makeFlatVector<int64_t>({10, 20})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"ts", TIMESTAMP(), TransformType::kYear, std::nullopt}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 2);
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 2);
  EXPECT_NE((files[0] + files[1]).find("/ts_year=202"), std::string::npos);

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 1);
    ASSERT_TRUE(partitionData["partitionValues"][0].isInt());
    EXPECT_TRUE(
        partitionData["partitionValues"][0].asInt() == 54 ||
        partitionData["partitionValues"][0].asInt() == 55);
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"ts", regularColumn("ts", TIMESTAMP())},
                       {"c0", regularColumn("c0", BIGINT())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, bucketPartitionCommitMessage) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"id", "c0"}, {BIGINT(), VARCHAR()});
  auto input = makeRowVector(
      {"id", "c0"},
      {makeFlatVector<int64_t>({1, 2, 3, 4}),
       makeFlatVector<StringView>({"a", "b", "c", "d"})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"id", BIGINT(), TransformType::kBucket, 2}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 2);
  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 1);
    ASSERT_TRUE(partitionData["partitionValues"][0].isInt());
  }
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 2);
  EXPECT_NE((files[0] + files[1]).find("/id_bucket="), std::string::npos);
}

TEST_F(IcebergWriteTest, mixedTemporalAndTruncatePartitions) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType =
      ROW({"month_ts", "day_date", "hour_ts", "name", "c0"},
          {TIMESTAMP(), DATE(), TIMESTAMP(), VARCHAR(), BIGINT()});
  auto input = makeRowVector(
      {"month_ts", "day_date", "hour_ts", "name", "c0"},
      {makeFlatVector<Timestamp>(
           {Timestamp::fromMillis(1704067200000),
            Timestamp::fromMillis(1706745600000)}),
       makeFlatVector<int32_t>(
           {DATE()->toDays("2024-01-15"), DATE()->toDays("2024-02-01")},
           DATE()),
       makeFlatVector<Timestamp>(
           {Timestamp::fromMillis(1705320000000),
            Timestamp::fromMillis(1706785200000)}),
       makeFlatVector<StringView>({"alphabet", "bravo"}),
       makeFlatVector<int64_t>({10, 20})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"month_ts", TIMESTAMP(), TransformType::kMonth, std::nullopt},
          {"day_date", DATE(), TransformType::kDay, std::nullopt},
          {"hour_ts", TIMESTAMP(), TransformType::kHour, std::nullopt},
          {"name", VARCHAR(), TransformType::kTruncate, 3}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 2);
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 2);
  auto allPaths = files[0] + files[1];
  EXPECT_NE(allPaths.find("/month_ts_month=2024-01"), std::string::npos);
  EXPECT_NE(allPaths.find("/month_ts_month=2024-02"), std::string::npos);
  EXPECT_NE(allPaths.find("/day_date_day=2024-01-15"), std::string::npos);
  EXPECT_NE(allPaths.find("/day_date_day=2024-02-01"), std::string::npos);
  EXPECT_NE(allPaths.find("/hour_ts_hour=2024-01-15-12"), std::string::npos);
  EXPECT_NE(allPaths.find("/hour_ts_hour=2024-02-01-11"), std::string::npos);
  EXPECT_NE(allPaths.find("/name_trunc=alp"), std::string::npos);
  EXPECT_NE(allPaths.find("/name_trunc=bra"), std::string::npos);

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 4);
    ASSERT_TRUE(partitionData["partitionValues"][0].isInt());
    ASSERT_TRUE(partitionData["partitionValues"][1].isInt());
    ASSERT_TRUE(partitionData["partitionValues"][2].isInt());
    ASSERT_TRUE(partitionData["partitionValues"][3].isString());
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"month_ts", regularColumn("month_ts", TIMESTAMP())},
                       {"day_date", regularColumn("day_date", DATE())},
                       {"hour_ts", regularColumn("hour_ts", TIMESTAMP())},
                       {"name", regularColumn("name", VARCHAR())},
                       {"c0", regularColumn("c0", BIGINT())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, multipleTransformCategoriesOnSameColumn) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"id", "payload"}, {BIGINT(), VARCHAR()});
  auto input = makeRowVector(
      {"id", "payload"},
      {makeFlatVector<int64_t>({11, 25}),
       makeFlatVector<StringView>({"x", "y"})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"id", BIGINT(), TransformType::kIdentity, std::nullopt},
          {"id", BIGINT(), TransformType::kBucket, 4},
          {"id", BIGINT(), TransformType::kTruncate, 10}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 2);
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 2);
  auto allPaths = files[0] + files[1];
  EXPECT_NE(allPaths.find("/id=11"), std::string::npos);
  EXPECT_NE(allPaths.find("/id=25"), std::string::npos);
  EXPECT_NE(allPaths.find("/id_trunc=10"), std::string::npos);
  EXPECT_NE(allPaths.find("/id_trunc=20"), std::string::npos);
  EXPECT_NE(allPaths.find("/id_bucket="), std::string::npos);

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 3);
    ASSERT_TRUE(partitionData["partitionValues"][0].isInt());
    ASSERT_TRUE(partitionData["partitionValues"][1].isInt());
    ASSERT_TRUE(partitionData["partitionValues"][2].isInt());
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"id", regularColumn("id", BIGINT())},
                       {"payload", regularColumn("payload", VARCHAR())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, decimalTruncatePartition) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"amount", "payload"}, {DECIMAL(6, 2), VARCHAR()});
  auto input = makeRowVector(
      {"amount", "payload"},
      {makeFlatVector<int64_t>({12345, -501, 5}, DECIMAL(6, 2)),
       makeFlatVector<StringView>({"a", "b", "c"})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"amount", DECIMAL(6, 2), TransformType::kTruncate, 10}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_EQ(commitTasks.size(), 3);
  auto files = listFiles(outputDir->path);
  ASSERT_EQ(files.size(), 3);
  auto allPaths = files[0] + files[1] + files[2];
  EXPECT_NE(allPaths.find("/amount_trunc=123.40"), std::string::npos);
  EXPECT_NE(allPaths.find("/amount_trunc=-5.10"), std::string::npos);
  EXPECT_NE(allPaths.find("/amount_trunc=0.00"), std::string::npos);

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 1);
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"amount", regularColumn("amount", DECIMAL(6, 2))},
                       {"payload", regularColumn("payload", VARCHAR())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

TEST_F(IcebergWriteTest, decimalBucketPartition) {
  auto outputDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({"amount", "payload"}, {DECIMAL(20, 3), VARCHAR()});
  auto input = makeRowVector(
      {"amount", "payload"},
      {makeFlatVector<int128_t>({1234567890123, -5010, 42}, DECIMAL(20, 3)),
       makeFlatVector<StringView>({"a", "b", "c"})});

  auto partitionSpec = std::make_shared<const IcebergPartitionSpec>(
      1,
      std::vector<IcebergPartitionSpec::Field>{
          {"amount", DECIMAL(20, 3), TransformType::kBucket, 8}});

  auto connector = connector::getConnector(kIcebergConnectorId);
  auto dataSink = connector->createDataSink(
      rowType,
      makeInsertHandle(rowType, outputDir->path, partitionSpec),
      connectorQueryCtx_.get(),
      CommitStrategy::kNoCommit,
      queryConfig_);

  dataSink->appendData(input);
  auto commitTasks = dataSink->close();

  ASSERT_FALSE(commitTasks.empty());
  auto files = listFiles(outputDir->path);
  ASSERT_FALSE(files.empty());
  for (const auto& file : files) {
    EXPECT_NE(file.find("/amount_bucket="), std::string::npos);
  }

  for (const auto& taskJson : commitTasks) {
    auto task = folly::parseJson(taskJson);
    ASSERT_TRUE(task.count("partitionDataJson") > 0);
    auto partitionData = folly::parseJson(task["partitionDataJson"].asString());
    ASSERT_EQ(partitionData["partitionValues"].size(), 1);
  }

  createDuckDbTable({input});
  auto plan = exec::test::PlanBuilder()
                  .tableScan(
                      rowType,
                      makeTableHandleForConnector(rowType),
                      {{"amount", regularColumn("amount", DECIMAL(20, 3))},
                       {"payload", regularColumn("payload", VARCHAR())}})
                  .planNode();
  assertQuery(plan, makeIcebergSplits(files), "SELECT * FROM tmp");
}

} // namespace
} // namespace bytedance::bolt::connector::hive::iceberg
