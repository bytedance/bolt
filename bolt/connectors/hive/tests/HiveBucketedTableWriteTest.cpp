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
 * 2026-04-09.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <atomic>
#include <set>
#include <string>
#include <unordered_map>

#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/dynamic.h>
#include <re2/re2.h>

#include "bolt/common/base/Fs.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveDataSink.h"
#include "bolt/connectors/hive/HivePartitionFunction.h"
#include "bolt/connectors/hive/tests/HiveConnectorTestBase.h"
#include "bolt/dwio/dwrf/writer/Writer.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Spill.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/exec/tests/utils/ArbitratorTestUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

namespace bytedance::bolt::connector::hive {
namespace {

using namespace bytedance::bolt::common;
using namespace bytedance::bolt::common::testutil;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

enum class BucketedTestMode {
  kOnlyBucketed,
  kPartitionedBucketed,
};

std::function<PlanNodePtr(std::string, PlanNodePtr)> addTableWriter(
    const RowTypePtr& inputColumns,
    const std::vector<std::string>& tableColumnNames,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    bool hasPartitioningScheme,
    connector::CommitStrategy commitStrategy =
        connector::CommitStrategy::kNoCommit) {
  return [=](core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<core::TableWriteNode>(
        nodeId,
        inputColumns,
        tableColumnNames,
        nullptr,
        insertHandle,
        hasPartitioningScheme,
        TableWriteTraits::outputType(nullptr),
        commitStrategy,
        std::move(source));
  };
}

std::function<PlanNodePtr(std::string, PlanNodePtr)> addLocalPartitionByBucket(
    const std::shared_ptr<HiveBucketProperty>& bucketProperty,
    const RowTypePtr& inputType) {
  std::vector<column_index_t> bucketChannels;
  bucketChannels.reserve(bucketProperty->bucketedBy().size());
  for (const auto& bucketColumn : bucketProperty->bucketedBy()) {
    bucketChannels.push_back(inputType->getChildIdx(bucketColumn));
  }
  auto spec = std::make_shared<HivePartitionFunctionSpec>(
      bucketProperty->bucketCount(), bucketChannels, std::vector<VectorPtr>{});
  return [spec = std::move(spec)](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<core::LocalPartitionNode>(
        nodeId,
        core::LocalPartitionNode::Type::kRepartition,
        spec,
        std::vector<core::PlanNodePtr>{std::move(source)});
  };
}

RowTypePtr getNonPartitionsColumns(
    const std::vector<std::string>& partitionedKeys,
    const RowTypePtr& rowType) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);
    if (std::find(partitionedKeys.begin(), partitionedKeys.end(), name) ==
        partitionedKeys.end()) {
      names.push_back(name);
      types.push_back(rowType->childAt(i));
    }
  }
  return ROW(std::move(names), std::move(types));
}

class HiveBucketedTableWriteTestBase : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    Type::registerSerDe();
    HiveSortingColumn::registerSerDe();
    HiveBucketProperty::registerSerDe();

    rowType_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
            {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()});
    tableSchema_ = rowType_;
  }

  void setPartitionBy(const std::vector<std::string>& partitionBy) {
    partitionedBy_ = partitionBy;
    partitionTypes_.clear();
    for (const auto& partitionColumn : partitionedBy_) {
      for (int i = 0; i < rowType_->size(); ++i) {
        if (rowType_->nameOf(i) == partitionColumn) {
          partitionTypes_.emplace_back(rowType_->childAt(i));
        }
      }
    }
  }

  void setBucketProperty(
      uint32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const std::vector<TypePtr>& bucketedTypes,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy =
          {}) {
    bucketProperty_ = std::make_shared<HiveBucketProperty>(
        HiveBucketProperty::Kind::kHiveCompatible,
        bucketCount,
        bucketedBy,
        bucketedTypes,
        sortedBy);
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t numVectors,
      int32_t rowsPerVector) {
    auto vectors =
        HiveConnectorTestBase::makeVectors(rowType_, numVectors, rowsPerVector);
    if (mode_ == BucketedTestMode::kOnlyBucketed) {
      return vectors;
    }
    for (auto& rowVector : vectors) {
      rowVector->childAt(0) =
          makeFlatVector<int64_t>(rowsPerVector, [&](auto /*unused*/) {
            return folly::Random().rand32() % numPartitionKeyValues_[0];
          });
      rowVector->childAt(1) =
          makeFlatVector<int32_t>(rowsPerVector, [&](auto /*unused*/) {
            return folly::Random().rand32() % numPartitionKeyValues_[1];
          });
    }
    return vectors;
  }

  std::vector<RowVectorPtr> makeBatches(
      vector_size_t numBatches,
      std::function<RowVectorPtr(int32_t)> makeVector) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(makeVector(i));
    }
    return batches;
  }

  std::shared_ptr<core::InsertTableHandle> createInsertTableHandle(
      const RowTypePtr& outputRowType,
      const LocationHandle::TableType& outputTableType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy,
      const std::shared_ptr<HiveBucketProperty>& bucketProperty,
      const std::optional<CompressionKind>& compressionKind = {}) {
    return std::make_shared<core::InsertTableHandle>(
        connectorId(),
        makeHiveInsertTableHandle(
            outputRowType->names(),
            outputRowType->children(),
            partitionedBy,
            bucketProperty,
            makeLocationHandle(
                outputDirectoryPath, std::nullopt, outputTableType),
            dwio::common::FileFormat::DWRF,
            compressionKind));
  }

  PlanNodePtr createInsertPlan(
      PlanBuilder& inputPlan,
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionedBy = {},
      std::shared_ptr<HiveBucketProperty> bucketProperty = {},
      const std::optional<CompressionKind>& compressionKind = {},
      int numTableWriters = 1,
      const LocationHandle::TableType& outputTableType =
          LocationHandle::TableType::kNew,
      const CommitStrategy& outputCommitStrategy = CommitStrategy::kNoCommit) {
    if (numTableWriters == 1) {
      return inputPlan
          .addNode(addTableWriter(
              inputPlan.planNode()->outputType(),
              outputRowType->names(),
              createInsertTableHandle(
                  outputRowType,
                  outputTableType,
                  outputDirectoryPath,
                  partitionedBy,
                  bucketProperty,
                  compressionKind),
              bucketProperty != nullptr,
              outputCommitStrategy))
          .capturePlanNodeId(tableWriteNodeId_)
          .project({TableWriteTraits::rowCountColumnName()})
          .singleAggregation(
              {},
              {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())})
          .planNode();
    }

    std::vector<std::string> bucketColumns;
    bucketColumns.reserve(bucketProperty->bucketedBy().size());
    auto inputType = inputPlan.planNode()->outputType();
    for (int i = 0; i < bucketProperty->bucketedBy().size(); ++i) {
      bucketColumns.push_back(inputType->names()[outputRowType->getChildIdx(
          bucketProperty->bucketedBy()[i])]);
    }
    auto localBucketProperty = std::make_shared<HiveBucketProperty>(
        bucketProperty->kind(),
        bucketProperty->bucketCount(),
        bucketColumns,
        bucketProperty->bucketedTypes(),
        bucketProperty->sortedBy());

    return inputPlan
        .addNode(addLocalPartitionByBucket(localBucketProperty, inputType))
        .addNode(addTableWriter(
            inputType,
            outputRowType->names(),
            createInsertTableHandle(
                outputRowType,
                outputTableType,
                outputDirectoryPath,
                partitionedBy,
                bucketProperty,
                compressionKind),
            true,
            outputCommitStrategy))
        .capturePlanNodeId(tableWriteNodeId_)
        .localPartition({})
        .tableWriteMerge()
        .project({TableWriteTraits::rowCountColumnName()})
        .singleAggregation(
            {},
            {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())})
        .planNode();
  }

  std::shared_ptr<Task> assertQueryWithWriterConfigs(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql,
      bool spillEnabled = false) {
    AssertQueryBuilder builder(plan, duckDbQueryRunner_);
    builder
        .maxDrivers(
            2 * std::max(numTableWriterCount_, numPartitionedTableWriterCount_))
        .config(
            QueryConfig::kTaskWriterCount, std::to_string(numTableWriterCount_))
        .config(
            QueryConfig::kTaskPartitionedWriterCount,
            std::to_string(numPartitionedTableWriterCount_));
    if (!spillEnabled) {
      return builder.assertResults(duckDbSql);
    }

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    TestScopedSpillInjection scopedSpillInjection(100);
    return builder.spillDirectory(spillDirectory->path)
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(QueryConfig::kWriterSpillEnabled, "true")
        .assertResults(duckDbSql);
  }

  void verifyBucketedFileName(const std::filesystem::path& filePath) const {
    ASSERT_TRUE(RE2::FullMatch(
        filePath.filename().string(), "0[0-9]+_0_TaskCursorQuery_[0-9]+"))
        << filePath.filename().string();
  }

  void verifyPartitionedDirPath(
      const std::filesystem::path& dirPath,
      const std::string& targetDir) const {
    std::string regex(targetDir);
    bool matched = false;
    for (int i = 0; i < partitionedBy_.size(); ++i) {
      regex = fmt::format("{}/{}=.+", regex, partitionedBy_[i]);
      if (RE2::FullMatch(dirPath.string(), regex)) {
        matched = true;
        break;
      }
    }
    ASSERT_TRUE(matched) << dirPath;
  }

  uint32_t parseBucketId(const std::string& bucketFileName) const {
    uint32_t bucketId;
    BOLT_CHECK(RE2::FullMatch(bucketFileName, "(\\d+)_.+", &bucketId));
    return bucketId;
  }

  std::unique_ptr<core::PartitionFunction> getBucketFunction(
      const RowTypePtr& outputType) const {
    std::vector<column_index_t> channels;
    channels.reserve(bucketProperty_->bucketedBy().size());
    for (const auto& bucketColumn : bucketProperty_->bucketedBy()) {
      for (column_index_t channel = 0; channel < outputType->size();
           ++channel) {
        if (outputType->nameOf(channel) == bucketColumn) {
          channels.push_back(channel);
          break;
        }
      }
    }
    return std::make_unique<HivePartitionFunction>(
        bucketProperty_->bucketCount(), channels);
  }

  std::vector<std::string> getPartitionDirNames(
      const std::filesystem::path& dirPath) const {
    std::vector<std::string> dirNames;
    auto nextPath = dirPath;
    for (int i = 0; i < partitionedBy_.size(); ++i) {
      dirNames.push_back(nextPath.filename().string());
      nextPath = nextPath.parent_path();
    }
    return dirNames;
  }

  std::string partitionNameToPredicate(
      const std::vector<std::string>& partitionDirNames) const {
    std::vector<std::string> conjuncts;
    for (int i = 0; i < partitionDirNames.size(); ++i) {
      auto keyValue = partitionDirNames[i];
      if (partitionTypes_[i]->isVarchar() ||
          partitionTypes_[i]->isVarbinary() || partitionTypes_[i]->isDate()) {
        conjuncts.push_back(
            keyValue.replace(keyValue.find("="), 1, "='").append("'"));
      } else {
        conjuncts.push_back(keyValue);
      }
    }
    return folly::join(" AND ", conjuncts);
  }

  void verifyPartitionedFilesData(
      const std::vector<std::filesystem::path>& filePaths,
      const std::filesystem::path& dirPath,
      const RowTypePtr& outputType) {
    ConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(outputType).planNode(),
        {makeConnectorSplits(filePaths)},
        fmt::format(
            "SELECT c2, c3, c4, c5 FROM tmp WHERE {}",
            partitionNameToPredicate(getPartitionDirNames(dirPath))));
  }

  void verifyBucketedFileData(
      const std::filesystem::path& filePath,
      const RowTypePtr& outputType) {
    core::PlanNodeId scanNodeId;
    auto plan = PlanBuilder()
                    .tableScan(outputType, {}, "", outputType)
                    .capturePlanNodeId(scanNodeId)
                    .planNode();
    auto result = AssertQueryBuilder(plan)
                      .splits(
                          scanNodeId,
                          makeConnectorSplits(
                              std::vector<std::filesystem::path>{filePath}))
                      .copyResults(pool());
    auto bucketFunction = getBucketFunction(outputType);
    std::vector<uint32_t> bucketIds(result->size());
    bucketFunction->partition(*result, bucketIds);
    const auto expectedBucketId = parseBucketId(filePath.filename().string());
    for (auto bucketId : bucketIds) {
      ASSERT_EQ(expectedBucketId, bucketId);
    }

    if (!bucketSort_) {
      return;
    }
    for (int i = 0; i < result->size() - 1; ++i) {
      auto compareResult =
          result->childAt(sortColumnIndex_)
              ->compare(
                  result->childAt(sortColumnIndex_)->wrappedVector(),
                  i,
                  i + 1,
                  CompareFlags{true, true});
      if (compareResult.has_value() && compareResult.value() >= 0) {
        ASSERT_EQ(compareResult.value(), 0);
      }
    }
  }

  void verifyTableWriterOutput(
      const std::string& targetDir,
      const RowTypePtr& outputType) {
    std::vector<std::filesystem::path> filePaths;
    std::vector<std::filesystem::path> dirPaths;
    for (auto& path : fs::recursive_directory_iterator(targetDir)) {
      if (path.is_regular_file()) {
        filePaths.push_back(path.path());
      } else {
        dirPaths.push_back(path.path());
      }
    }

    if (mode_ == BucketedTestMode::kOnlyBucketed) {
      ASSERT_TRUE(dirPaths.empty());
      for (const auto& filePath : filePaths) {
        ASSERT_EQ(filePath.parent_path().string(), targetDir);
        verifyBucketedFileName(filePath);
        verifyBucketedFileData(filePath, outputType);
      }
      return;
    }

    std::unordered_map<std::string, std::vector<std::filesystem::path>>
        filesByPartition;
    for (const auto& filePath : filePaths) {
      verifyPartitionedDirPath(filePath.parent_path(), targetDir);
      verifyBucketedFileName(filePath);
      verifyBucketedFileData(filePath, outputType);
      filesByPartition[filePath.parent_path().string()].push_back(filePath);
    }

    for (const auto& [partitionDir, partitionFiles] : filesByPartition) {
      verifyPartitionedFilesData(
          partitionFiles, std::filesystem::path(partitionDir), outputType);
    }
  }

  int getNumWriters() const {
    return bucketProperty_ != nullptr ? numPartitionedTableWriterCount_
                                      : numTableWriterCount_;
  }

  BucketedTestMode mode_{BucketedTestMode::kOnlyBucketed};
  int numTableWriterCount_{1};
  int numPartitionedTableWriterCount_{1};
  bool bucketSort_{false};
  column_index_t sortColumnIndex_{0};
  RowTypePtr rowType_;
  RowTypePtr tableSchema_;
  std::vector<std::string> partitionedBy_;
  std::vector<TypePtr> partitionTypes_;
  std::vector<uint32_t> numPartitionKeyValues_;
  std::shared_ptr<HiveBucketProperty> bucketProperty_;
  std::optional<CompressionKind> compressionKind_{CompressionKind_ZSTD};
  CommitStrategy commitStrategy_{CommitStrategy::kNoCommit};
  core::PlanNodeId tableWriteNodeId_;
};

class BucketedUnpartitionedTableWriterTest
    : public HiveBucketedTableWriteTestBase {
 protected:
  void SetUp() override {
    HiveBucketedTableWriteTestBase::SetUp();
    mode_ = BucketedTestMode::kOnlyBucketed;
    bucketSort_ = true;
    sortColumnIndex_ = 4;
    setBucketProperty(
        4,
        {"c3", "c5"},
        {REAL(), VARCHAR()},
        {std::make_shared<const HiveSortingColumn>(
            "c4", core::SortOrder{true, true})});
  }
};

class BucketedPartitionedTableWriterTest
    : public HiveBucketedTableWriteTestBase {
 protected:
  void SetUp() override {
    HiveBucketedTableWriteTestBase::SetUp();
    mode_ = BucketedTestMode::kPartitionedBucketed;
    setPartitionBy({"c0", "c1"});
    numPartitionKeyValues_ = {4, 4};
    numPartitionedTableWriterCount_ = 2;
    setBucketProperty(4, {"c3", "c5"}, {REAL(), VARCHAR()});
  }
};

class BucketSortOnlyTableWriterTest : public HiveBucketedTableWriteTestBase {
 protected:
  void SetUp() override {
    HiveBucketedTableWriteTestBase::SetUp();
    mode_ = BucketedTestMode::kPartitionedBucketed;
    bucketSort_ = true;
    sortColumnIndex_ = 2;
    setPartitionBy({"c0", "c1"});
    numPartitionKeyValues_ = {4, 4};
    numPartitionedTableWriterCount_ = 2;
    setBucketProperty(
        4,
        {"c3", "c5"},
        {REAL(), VARCHAR()},
        {std::make_shared<const HiveSortingColumn>(
            "c4", core::SortOrder{true, true})});
  }
};

class HiveSpecificTableWriterTest : public HiveBucketedTableWriteTestBase {
 protected:
  void SetUp() override {
    HiveBucketedTableWriteTestBase::SetUp();
  }
};

TEST_F(BucketedUnpartitionedTableWriterTest, bucketNonPartitioned) {
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);

  auto outputDirectory = TempDirectoryPath::create();
  auto planBuilder = PlanBuilder().values(input);
  auto plan = createInsertPlan(
      planBuilder,
      rowType_,
      outputDirectory->getPath(),
      {},
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      LocationHandle::TableType::kExisting,
      commitStrategy_);
  assertQueryWithWriterConfigs(plan, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      makeConnectorSplits(outputDirectory),
      "SELECT * FROM tmp");
  verifyTableWriterOutput(outputDirectory->getPath(), rowType_);
}

TEST_F(BucketedPartitionedTableWriterTest, bucketCountLimit) {
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);

  struct {
    uint32_t bucketCount;
    bool expectedError;
  } testSettings[] = {
      {1, false},
      {3, false},
      {HiveDataSink::maxBucketCount() - 1, false},
      {HiveDataSink::maxBucketCount(), true},
      {HiveDataSink::maxBucketCount() + 1, true},
      {HiveDataSink::maxBucketCount() * 2, true}};

  for (const auto& testData : testSettings) {
    auto outputDirectory = TempDirectoryPath::create();
    setBucketProperty(
        testData.bucketCount,
        bucketProperty_->bucketedBy(),
        bucketProperty_->bucketedTypes(),
        bucketProperty_->sortedBy());
    auto planBuilder = PlanBuilder().values(input);
    auto plan = createInsertPlan(
        planBuilder,
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        compressionKind_,
        getNumWriters(),
        LocationHandle::TableType::kNew,
        commitStrategy_);

    if (testData.expectedError) {
      BOLT_ASSERT_THROW(
          AssertQueryBuilder(plan)
              .connectorSessionProperty(
                  connectorId(),
                  HiveConfig::kMaxPartitionsPerWritersSession,
                  folly::to<std::string>(testData.bucketCount * 2))
              .copyResults(pool()),
          "bucketCount exceeds the limit");
      continue;
    }

    assertQueryWithWriterConfigs(plan, "SELECT count(*) FROM tmp");
    auto outputType = getNonPartitionsColumns(partitionedBy_, tableSchema_);
    assertQuery(
        PlanBuilder().tableScan(outputType).planNode(),
        makeConnectorSplits(outputDirectory),
        "SELECT c2, c3, c4, c5 FROM tmp");
    verifyTableWriterOutput(outputDirectory->path, outputType);
  }
}

TEST_F(BucketedPartitionedTableWriterTest, mismatchedBucketTypes) {
  auto input = makeVectors(1, 100);
  createDuckDbTable(input);

  auto outputDirectory = TempDirectoryPath::create();
  auto badBucketedTypes = bucketProperty_->bucketedTypes();
  const auto oldType = badBucketedTypes[0];
  badBucketedTypes[0] = VARCHAR();
  setBucketProperty(
      bucketProperty_->bucketCount(),
      bucketProperty_->bucketedBy(),
      badBucketedTypes,
      bucketProperty_->sortedBy());
  auto planBuilder = PlanBuilder().values(input);
  auto plan = createInsertPlan(
      planBuilder,
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      LocationHandle::TableType::kNew,
      commitStrategy_);

  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      fmt::format(
          "Input column {} type {} doesn't match bucket type {}",
          bucketProperty_->bucketedBy()[0],
          oldType->toString(),
          bucketProperty_->bucketedTypes()[0]));
}

TEST_F(BucketSortOnlyTableWriterTest, sortWriterSpill) {
  const auto vectors = makeVectors(5, 500);
  createDuckDbTable(vectors);

  auto outputDirectory = TempDirectoryPath::create();
  auto planBuilder = PlanBuilder().values(vectors);
  auto plan = createInsertPlan(
      planBuilder,
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters(),
      LocationHandle::TableType::kNew,
      commitStrategy_);

  const auto spillStats = common::globalSpillStats();
  auto task = assertQueryWithWriterConfigs(
      plan, fmt::format("SELECT {}", 5 * 500), true);
  verifyTableWriterOutput(
      outputDirectory->path, getNonPartitionsColumns(partitionedBy_, rowType_));

  const auto updatedSpillStats = common::globalSpillStats();
  ASSERT_GT(updatedSpillStats.spilledBytes, spillStats.spilledBytes);
  ASSERT_GT(updatedSpillStats.spilledPartitions, spillStats.spilledPartitions);
  auto taskStats = exec::toPlanStats(task->taskStats());
  auto& stats = taskStats.at(tableWriteNodeId_);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledBytes, 0);
  const int numWrittenFiles = stats.customStats["numWrittenFiles"].sum;
  ASSERT_GE(stats.spilledPartitions, numWrittenFiles);
  ASSERT_GT(stats.customStats["spillRuns"].sum, 0);
  ASSERT_GT(stats.customStats["spillFillTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillSortTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillSerializationTime"].sum, 0);
  ASSERT_GT(stats.customStats["spillFlushTime"].sum, 0);
  ASSERT_GT(stats.customStats[Operator::kSpillWrites].sum, 0);
  ASSERT_GT(stats.customStats["spillWriteTime"].sum, 0);
}

DEBUG_ONLY_TEST_F(BucketSortOnlyTableWriterTest, outputBatchRows) {
  struct {
    uint32_t maxOutputRows;
    std::string maxOutputBytes;
    int expectedOutputCount;
  } testSettings[] = {
      {10000, "1000kB", 4}, {1, "1kB", 1000}, {10000, "6200B", 12}};

  for (const auto& testData : testSettings) {
    std::atomic_int outputCount{0};
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::dwrf::Writer::write",
        std::function<void(dwrf::Writer*)>(
            [&](dwrf::Writer* /*unused*/) { ++outputCount; }));

    auto rowType =
        ROW({"c0", "p0", "c1", "c3", "c4", "c5"},
            {VARCHAR(), BIGINT(), INTEGER(), REAL(), DOUBLE(), VARCHAR()});
    std::vector<std::string> partitionKeys = {"p0"};
    auto vectors = makeBatches(1, [&](auto) {
      return makeRowVector(
          rowType->names(),
          {makeFlatVector<StringView>(
               1'000,
               [&](auto row) {
                 return StringView::makeInline(fmt::format("str_{}", row));
               }),
           makeConstant((int64_t)365, 1'000),
           makeConstant((int32_t)365, 1'000),
           makeFlatVector<float>(1'000, [&](auto row) { return row + 33.23; }),
           makeFlatVector<double>(1'000, [&](auto row) { return row + 33.23; }),
           makeFlatVector<StringView>(1'000, [&](auto row) {
             return StringView::makeInline(fmt::format("bucket_{}", row * 3));
           })});
    });
    createDuckDbTable(vectors);

    auto outputDirectory = TempDirectoryPath::create();
    auto planBuilder = PlanBuilder().values(vectors);
    auto plan = createInsertPlan(
        planBuilder,
        rowType,
        outputDirectory->path,
        partitionKeys,
        bucketProperty_,
        compressionKind_,
        1,
        LocationHandle::TableType::kNew,
        commitStrategy_);
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(QueryConfig::kTaskWriterCount, "1")
        .connectorSessionProperty(
            connectorId(),
            HiveConfig::kSortWriterMaxOutputRowsSession,
            folly::to<std::string>(testData.maxOutputRows))
        .connectorSessionProperty(
            connectorId(),
            HiveConfig::kSortWriterMaxOutputBytesSession,
            folly::to<std::string>(testData.maxOutputBytes))
        .assertResults("SELECT count(*) FROM tmp");
    ASSERT_EQ(outputCount, testData.expectedOutputCount);
  }
}

DEBUG_ONLY_TEST_F(HiveSpecificTableWriterTest, dataSinkAbortError) {
  VectorFuzzer::Options options;
  const int batchSize = 100;
  options.vectorSize = batchSize;
  VectorFuzzer fuzzer(options, pool());
  auto vector = fuzzer.fuzzInputRow(rowType_);

  std::atomic<bool> triggerWriterErrorOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::dwrf::Writer::write",
      std::function<void(dwrf::Writer*)>([&](dwrf::Writer* /*unused*/) {
        if (!triggerWriterErrorOnce.exchange(false)) {
          return;
        }
        BOLT_FAIL("inject writer error");
      }));

  std::atomic<bool> triggerAbortErrorOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::connector::hive::HiveDataSink::closeInternal",
      std::function<void(const HiveDataSink*)>(
          [&](const HiveDataSink* /*unused*/) {
            if (!triggerAbortErrorOnce.exchange(false)) {
              return;
            }
            BOLT_FAIL("inject abort error");
          }));

  auto outputDirectory = TempDirectoryPath::create();
  auto plan =
      PlanBuilder()
          .values({vector})
          .tableWrite(outputDirectory->path, dwio::common::FileFormat::DWRF)
          .planNode();
  BOLT_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()), "inject writer error");
  ASSERT_FALSE(triggerWriterErrorOnce);
  ASSERT_FALSE(triggerAbortErrorOnce);
}

DEBUG_ONLY_TEST_F(BucketSortOnlyTableWriterTest, reclaimFromSortTableWriter) {
  VectorFuzzer::Options options;
  const int batchSize = 1'000;
  options.vectorSize = batchSize;
  options.stringVariableLength = false;
  options.stringLength = 1'000;
  VectorFuzzer fuzzer(options, pool());
  const int numBatches = 20;
  std::vector<RowVectorPtr> vectors;
  int numRows{0};
  const auto partitionKeyVector = makeFlatVector<int32_t>(
      batchSize, [&](vector_size_t /*unused*/) { return 0; });
  for (int i = 0; i < numBatches; ++i) {
    numRows += batchSize;
    vectors.push_back(fuzzer.fuzzInputRow(rowType_));
    vectors.back()->childAt(0) = partitionKeyVector;
  }
  createDuckDbTable(vectors);

  for (bool writerSpillEnabled : {false, true}) {
    SCOPED_TRACE(fmt::format("writerSpillEnabled: {}", writerSpillEnabled));
    auto memoryManager = createMemoryManager();
    auto arbitrator = memoryManager->arbitrator();
    auto queryCtx =
        newQueryCtx(memoryManager.get(), executor_.get(), kMemoryCapacity);
    ASSERT_EQ(queryCtx->pool()->capacity(), kMemoryPoolInitCapacity);

    const auto spillStats = common::globalSpillStats();
    std::atomic<int> numInputs{0};
    SCOPED_TESTVALUE_SET(
        "bytedance::bolt::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "TableWrite") {
            return;
          }
          ASSERT_FALSE(op->canReclaim());
          if (++numInputs != numBatches) {
            return;
          }

          const auto fakeAllocationSize = arbitrator->stats().maxCapacityBytes -
              op->pool()->parent()->reservedBytes();
          if (writerSpillEnabled) {
            auto* buffer = op->pool()->allocate(fakeAllocationSize);
            op->pool()->free(buffer, fakeAllocationSize);
          } else {
            BOLT_ASSERT_THROW(
                op->pool()->allocate(fakeAllocationSize),
                "Exceeded memory pool");
          }
        })));

    auto outputDirectory = TempDirectoryPath::create();
    auto planBuilder = PlanBuilder().values(vectors);
    auto writerPlan = createInsertPlan(
        planBuilder,
        rowType_,
        outputDirectory->path,
        partitionedBy_,
        bucketProperty_,
        compressionKind_,
        getNumWriters());
    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder(duckDbQueryRunner_)
        .queryCtx(queryCtx)
        .maxDrivers(1)
        .spillDirectory(spillDirectory->path)
        .config(core::QueryConfig::kSpillEnabled, writerSpillEnabled)
        .config(core::QueryConfig::kWriterSpillEnabled, writerSpillEnabled)
        .config(core::QueryConfig::kWriterFlushThresholdBytes, 0)
        .plan(std::move(writerPlan))
        .assertResults(fmt::format("SELECT {}", numRows));

    ASSERT_EQ(arbitrator->stats().numFailures, writerSpillEnabled ? 0 : 1);
    ASSERT_EQ(arbitrator->stats().numNonReclaimableAttempts, 0);
    waitForAllTasksToBeDeleted(3'000'000);
    const auto updatedSpillStats = common::globalSpillStats();
    if (writerSpillEnabled) {
      ASSERT_GT(updatedSpillStats.spilledBytes, spillStats.spilledBytes);
      ASSERT_GT(
          updatedSpillStats.spilledPartitions, spillStats.spilledPartitions);
    } else {
      ASSERT_EQ(updatedSpillStats, spillStats);
    }
  }
}

DEBUG_ONLY_TEST_F(
    BucketSortOnlyTableWriterTest,
    reclaimFromNonReclaimableSortTableWriter) {
  VectorFuzzer::Options options;
  const int batchSize = 1'000;
  options.vectorSize = batchSize;
  options.stringVariableLength = false;
  options.stringLength = 1'000;
  VectorFuzzer fuzzer(options, pool());
  const int numBatches = 20;
  std::vector<RowVectorPtr> vectors;
  int numRows{0};
  const auto partitionKeyVector = makeFlatVector<int32_t>(
      batchSize, [&](vector_size_t /*unused*/) { return 0; });
  for (int i = 0; i < numBatches; ++i) {
    numRows += batchSize;
    vectors.push_back(fuzzer.fuzzInputRow(rowType_));
    vectors.back()->childAt(0) = partitionKeyVector;
  }

  createDuckDbTable(vectors);

  auto memoryManager = createMemoryManager();
  auto arbitrator = memoryManager->arbitrator();
  auto queryCtx =
      newQueryCtx(memoryManager.get(), executor_.get(), kMemoryCapacity);
  ASSERT_EQ(queryCtx->pool()->capacity(), kMemoryPoolInitCapacity);

  std::atomic<bool> injectFakeAllocationOnce{true};
  SCOPED_TESTVALUE_SET(
      "bytedance::bolt::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>(([&](memory::MemoryPool* pool) {
        const std::string re(".*sort");
        if (!RE2::FullMatch(pool->name(), re)) {
          return;
        }
        const int writerMemoryUsage = 4L << 20;
        if (pool->parent()->reservedBytes() < writerMemoryUsage) {
          return;
        }
        if (!injectFakeAllocationOnce.exchange(false)) {
          return;
        }
        const auto fakeAllocationSize = arbitrator->stats().maxCapacityBytes -
            pool->parent()->reservedBytes();
        BOLT_ASSERT_THROW(
            pool->allocate(fakeAllocationSize), "Exceeded memory pool");
      })));

  auto outputDirectory = TempDirectoryPath::create();
  auto planBuilder = PlanBuilder().values(vectors);
  auto writerPlan = createInsertPlan(
      planBuilder,
      rowType_,
      outputDirectory->path,
      partitionedBy_,
      bucketProperty_,
      compressionKind_,
      getNumWriters());

  const auto spillStats = common::globalSpillStats();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  AssertQueryBuilder(duckDbQueryRunner_)
      .queryCtx(queryCtx)
      .maxDrivers(1)
      .spillDirectory(spillDirectory->path)
      .config(core::QueryConfig::kSpillEnabled, "true")
      .config(core::QueryConfig::kWriterSpillEnabled, "true")
      .config(core::QueryConfig::kWriterFlushThresholdBytes, "0")
      .connectorSessionProperty(
          connectorId(), HiveConfig::kOrcWriterMaxStripeSizeSession, "1GB")
      .connectorSessionProperty(
          connectorId(),
          HiveConfig::kOrcWriterMaxDictionaryMemorySession,
          "1GB")
      .plan(std::move(writerPlan))
      .assertResults(fmt::format("SELECT {}", numRows));

  ASSERT_EQ(arbitrator->stats().numFailures, 1);
  ASSERT_EQ(arbitrator->stats().numNonReclaimableAttempts, 1);
  const auto updatedSpillStats = common::globalSpillStats();
  ASSERT_EQ(updatedSpillStats, spillStats);
  waitForAllTasksToBeDeleted();
}

} // namespace
} // namespace bytedance::bolt::connector::hive
