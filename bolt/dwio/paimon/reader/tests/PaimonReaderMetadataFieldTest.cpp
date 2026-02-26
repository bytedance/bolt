/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include <connectors/hive/paimon_merge_engines/PaimonRowKind.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "bolt/common/file/FileSystems.h"
#include "bolt/common/file/Utils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/paimon/reader/tests/PaimonTestUtils.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::connector::paimon;

namespace bytedance::bolt::dwio::paimon::test {
namespace {

class PaimonReaderMetadataFieldTest
    : public testing::Test,
      public bytedance::bolt::test::VectorTestBase {
 protected:
  static const std::string kHiveConnectorId;
  std::shared_ptr<folly::Executor> executor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency());

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    rootPool_ = memory::memoryManager()->addRootPool("PaimonTests");
    leafPool_ = rootPool_->addLeafChild("PaimonTests");

    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kHiveConnectorId);
    leafPool_.reset();
    rootPool_.reset();
  }

  std::vector<std::string> getFilePaths(std::string directoryPath) {
    std::vector<std::string> filePaths;
    auto fs = filesystems::getFileSystem(directoryPath, nullptr);

    for (const auto& entry : fs->list(directoryPath)) {
      if (!fs->isDirectory(entry)) {
        filePaths.push_back(entry);
      }
    }

    return filePaths;
  }
  void assertOutput(
      const RowTypePtr& readType,
      RowTypePtr fileRowType,
      const std::shared_ptr<exec::test::TempDirectoryPath>& tempDir,
      std::unordered_map<std::string, std::string> tableParameters,
      std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>
          assignments,
      const RowVectorPtr& expected,
      std::optional<int32_t> bucketNumber = std::nullopt,
      std::optional<std::unordered_map<std::string, std::optional<std::string>>>
          partitionValues = std::nullopt,
      std::vector<std::string> filters = {},
      std::unordered_map<std::string, std::string> customProperties = {},
      bool readAsLowerCase = false) {
    core::PlanNodeId scanNodeId;
    auto readPlanBuilder = exec::test::PlanBuilder(leafPool_.get());
    auto readPlanFragment =
        exec::test::PlanBuilder::TableScanBuilder(readPlanBuilder)
            .tableName("hive_table")
            .assignments(std::move(assignments))
            .outputType(readType)
            .dataColumns(std::move(fileRowType))
            .subfieldFilters(std::move(filters))
            .isFilterPushdownEnabled(true)
            .parameters(std::move(tableParameters))
            .endTableScan()
            .capturePlanNodeId(scanNodeId)
            .planFragment();

    // Create the reader task.
    std::unordered_map<std::string, std::string> connectorSessionProperties = {
        {HiveConfig::kFileColumnNamesReadAsLowerCaseSession,
         fmt::format("{}", readAsLowerCase)}};

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(
            executor_.get(),
            core::QueryConfig{{}},
            {{"test-hive",
              std::make_shared<config::ConfigBase>(
                  std::move(connectorSessionProperties))}}),
        exec::Task::ExecutionMode::kSerial);

    if (readType->containsChild(kSEQUENCE_NUMBER) &&
        customProperties.find(kFileMetaMaxSequenceNumber) ==
            customProperties.end()) {
      customProperties[kFileMetaMaxSequenceNumber] = "1000";
    }
    auto filePaths = getFilePaths(tempDir->getPath());

    std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
    for (const auto& filePath : filePaths) {
      if (bucketNumber.has_value() || partitionValues.has_value() ||
          !customProperties.empty()) {
        const auto partValues = partitionValues.value_or(
            std::unordered_map<std::string, std::optional<std::string>>{});
        hiveSplits.push_back(std::make_shared<hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath,
            dwio::common::FileFormat::PARQUET,
            0,
            std::numeric_limits<uint64_t>::max(),
            partValues,
            bucketNumber,
            nullptr,
            customProperties));

      } else {
        hiveSplits.push_back(std::make_shared<hive::HiveConnectorSplit>(
            kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET));
      }
    }

    if (!hiveSplits.empty()) {
      auto connectorSplit =
          std::make_shared<connector::hive::PaimonConnectorSplit>(
              kHiveConnectorId, hiveSplits);

      readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
    }
    readTask->noMoreSplits(scanNodeId);
    // accumulate results into a single vector
    std::vector<RowVectorPtr> results;
    while (auto result = readTask->next()) {
      results.push_back(result);
    }

    // concatenate the results
    auto concatenatedResult = BaseVector::create(readType, 0, pool_.get());
    for (const auto& result : results) {
      concatenatedResult->append(result.get());
    }

    bytedance::bolt::test::assertEqualVectors(expected, concatenatedResult);
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
};

const std::string PaimonReaderMetadataFieldTest::kHiveConnectorId = "test-hive";

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowIndex) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});

  // Create vectors with 20000 elements, a starts at 101, b starts at 103
  const int32_t kNumElements = 20000;
  auto aValues = std::vector<int32_t>(kNumElements);
  auto bValues = std::vector<int32_t>(kNumElements);
  auto sequence = std::vector<int64_t>(kNumElements);
  std::iota(aValues.begin(), aValues.end(), 101);
  std::iota(bValues.begin(), bValues.end(), 103);
  std::iota(sequence.begin(), sequence.end(), 0);

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>(aValues),
       vectorMaker_.flatVector<int32_t>(bValues)},
      sequence,
      std::vector(kNumElements, int8_t(PaimonRowKind::INSERT)));

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {kColumnNameRowIndex, BIGINT()}});
  auto assignments = getIdentityAssignment(readType);

  // Create expected row indices (0 to 19999)
  auto rowIndices = std::vector<int64_t>(kNumElements);
  std::iota(rowIndices.begin(), rowIndices.end(), 0);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      kNumElements,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>(aValues),
          vectorMaker_.flatVector<int64_t>(rowIndices)});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // flip the order
  readType = ROW({{kColumnNameRowIndex, BIGINT()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      kNumElements,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>(rowIndices),
          vectorMaker_.flatVector<int32_t>(aValues)});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // row index only
  readType = ROW({{kColumnNameRowIndex, BIGINT()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      kNumElements,
      std::vector<VectorPtr>{vectorMaker_.flatVector<int64_t>(rowIndices)});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowIndexWithFilters) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto numRows = 3 * 1024 * 1024; // roughly 3 row groups with default settings
  auto aVec = std::vector<int32_t>(numRows);
  auto bVec = std::vector<int32_t>(numRows);
  auto sequence = std::vector<int64_t>(numRows);
  auto valueKind = std::vector<int8_t>(numRows);
  std::iota(aVec.begin(), aVec.end(), 101);
  std::fill(bVec.begin(), bVec.end(), 200);
  std::iota(sequence.begin(), sequence.end(), 0);
  std::fill(valueKind.begin(), valueKind.end(), int8_t(PaimonRowKind::INSERT));

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>(aVec),
       vectorMaker_.flatVector<int32_t>(bVec)},
      sequence,
      valueKind);

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {kColumnNameRowIndex, BIGINT()}});
  auto assignments = getIdentityAssignment(readType);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<int64_t>({0, 1})});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a <= 102::INTEGER"});
  // flip the order
  readType = ROW({{kColumnNameRowIndex, BIGINT()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({0, 1}),
          vectorMaker_.flatVector<int32_t>({101, 102})});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a <= 102::INTEGER"});

  // row index only
  readType = ROW({{kColumnNameRowIndex, BIGINT()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{vectorMaker_.flatVector<int64_t>({0, 1})});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a <= 102::INTEGER"});

  // row index only, multiple row groups
  const int64_t rowCount = 14899;
  auto aValues = std::vector<int32_t>(rowCount);
  std::iota(aValues.begin(), aValues.end(), 101);
  auto expectedIndices = std::vector<int64_t>(rowCount);
  std::iota(expectedIndices.begin(), expectedIndices.end(), 0);
  readType = ROW({{kColumnNameRowIndex, BIGINT()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      rowCount,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>(expectedIndices)});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a < 15000::INTEGER"});
}

TEST_F(
    PaimonReaderMetadataFieldTest,
    DISABLED_testPaimonRowIndexWithFiltersPushdown) {
  // DISABLED until bug in parquet filter pushdown is resolved

  // reads high end of file that has multiple row groups with filters
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto numRows = 3 * 1024 * 1024; // roughly 3 row groups with default settings
  auto aVec = std::vector<int32_t>(numRows);
  auto bVec = std::vector<int32_t>(numRows);
  auto sequence = std::vector<int64_t>(numRows);
  auto valueKind = std::vector<int8_t>(numRows);
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>(aVec),
       vectorMaker_.flatVector<int32_t>(bVec)},
      sequence,
      valueKind);
  auto readType = ROW({{kColumnNameRowIndex, BIGINT()}});
  auto assignments = getIdentityAssignment(readType);
  // Calculate the correct range of row indices where a >= 30000
  // Since aVec starts at 101 and increments by 1, the first index where a >=
  // 30000 is 30000 - 101 = 29899
  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};
  const int64_t startIdx = 30000 - 101;
  const int64_t endIdx = 32767;
  const int64_t count = endIdx - startIdx + 1;
  auto expectedIndices = std::vector<int64_t>(count);
  std::iota(expectedIndices.begin(), expectedIndices.end(), startIdx);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      count,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>(expectedIndices)});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a > 30000"});

  readType = ROW({{"a", INTEGER()}, {"__paimon_row_index_2", BIGINT()}});
  assignments = getIdentityAssignment(readType);
  auto aValues = std::vector<int32_t>(count);
  std::iota(aValues.begin(), aValues.end(), 30000);

  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      count,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>(expectedIndices),
          vectorMaker_.flatVector<int32_t>(aValues),
          vectorMaker_.flatVector<int64_t>(expectedIndices)});

  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"a >= 30000"},
      std::unordered_map<std::string, std::string>(
          {{"__paimon_row_index_2", kColumnNameRowIndex}}));
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowIndexWithSelectiveFilter) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto numRows = 3 * 1024 * 1024;
  auto aVec = std::vector<int32_t>(numRows);
  auto bVec = std::vector<int32_t>(numRows);
  auto sequence = std::vector<int64_t>(numRows);
  auto valueKind = std::vector<int8_t>(numRows);
  std::iota(aVec.begin(), aVec.end(), 1);
  std::fill(bVec.begin(), bVec.end(), 888);
  std::iota(sequence.begin(), sequence.end(), 0);
  std::fill(valueKind.begin(), valueKind.end(), int8_t(PaimonRowKind::INSERT));
  // set 9 indices to 0 across different row groups
  auto rowIndexes = std::vector<int64_t>(9);
  for (int i = 0; i < 9; i++) {
    rowIndexes[i] = (numRows / 9) * i;
    bVec[rowIndexes[i]] = 0;
  }

  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>(aVec),
       vectorMaker_.flatVector<int32_t>(bVec)},
      sequence,
      valueKind);

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"b", INTEGER()}, {kColumnNameRowIndex, BIGINT()}});
  auto assignments = getIdentityAssignment(readType);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      9,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>(std::vector(9, 0)),
          vectorMaker_.flatVector<int64_t>(rowIndexes)});
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {"b = 0::INTEGER"});
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowId) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)},
      true);

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {"_ROW_ID", BIGINT()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<int64_t>({9001, 9002})});

  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // swap the order
  readType = ROW({{"_ROW_ID", BIGINT()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({9001, 9002}),
          vectorMaker_.flatVector<int32_t>({101, 102})});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // _ROW_ID only
  readType = ROW({{"_ROW_ID", BIGINT()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{vectorMaker_.flatVector<int64_t>({9001, 9002})});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);
}

// Disabled until paimon support querying extra columns
TEST_F(PaimonReaderMetadataFieldTest, DISABLED_testPaimonExtraColumns) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)},
      false,
      {{"_extra_meta", vectorMaker_.flatVector<int64_t>({7, 6})}});

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType =
      ROW({{"a", INTEGER()}, {"_ROW_ID", BIGINT()}, {"_extra_meta", BIGINT()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<int64_t>({0, 1}),
          vectorMaker_.flatVector<int64_t>({7, 6}),
      });
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // Test with metadata column at index 0
  readType =
      ROW({{"_extra_meta", BIGINT()}, {"_ROW_ID", BIGINT()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({7, 6}),
          vectorMaker_.flatVector<int64_t>({0, 1}),
          vectorMaker_.flatVector<int32_t>({101, 102}),
      });
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // Test with metadata column at middle index
  readType =
      ROW({{"a", INTEGER()}, {"_extra_meta", BIGINT()}, {"_ROW_ID", BIGINT()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<int64_t>({7, 6}),
          vectorMaker_.flatVector<int64_t>({0, 1}),
      });
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonFilePathColumn) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)},
      false,
      {{"_extra_meta", vectorMaker_.flatVector<int64_t>({7, 6})}});
  std::string parquetFilePath;
  for (const auto& entry :
       std::filesystem::directory_iterator(tempDir->getPath())) {
    if (std::filesystem::is_regular_file(entry)) {
      parquetFilePath = entry.path().string();
      break; // should only be one file
    }
  }

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {kColumnNameFilePath, VARCHAR()}});
  auto assignments = getIdentityAssignment(readType);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<std::string>(
              {parquetFilePath, parquetFilePath}),
      });

  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // Test with metadata column at index 0
  readType = ROW({{kColumnNameFilePath, VARCHAR()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<std::string>(
              {parquetFilePath, parquetFilePath}),
          vectorMaker_.flatVector<int32_t>({101, 102}),
      });
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  // test empty table.
  readType = ROW({{kColumnNameFilePath, VARCHAR()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      0,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<std::string>({}),
          vectorMaker_.flatVector<int32_t>({}),
      });
  auto emptyTableDir = exec::test::TempDirectoryPath::create();
  assertOutput(
      readType,
      fileRowType,
      emptyTableDir,
      tableParameters,
      assignments,
      expected);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonFilePathColumnMultiFile) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)},
      false);
  std::string firstFile;
  for (const auto& entry :
       std::filesystem::directory_iterator(tempDir->getPath())) {
    if (std::filesystem::is_regular_file(entry)) {
      firstFile = entry.path().string();
      break;
    }
  }
  createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({111, 112}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)},
      false);
  std::string secondFile;
  for (const auto& entry :
       std::filesystem::directory_iterator(tempDir->getPath())) {
    if (std::filesystem::is_regular_file(entry) &&
        entry.path().string() != firstFile) {
      secondFile = entry.path().string();
    }
  }

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {kColumnNameFilePath, VARCHAR()}});
  auto assignments = getIdentityAssignment(readType);

  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102, 111, 112}),
          vectorMaker_.flatVector<std::string>(
              {firstFile, firstFile, secondFile, secondFile}),
      });

  core::PlanNodeId scanNodeId;
  auto readPlanBuilder = exec::test::PlanBuilder(leafPool_.get());
  auto readPlanFragment =
      exec::test::PlanBuilder::TableScanBuilder(readPlanBuilder)
          .tableName("hive_table")
          .assignments(std::move(assignments))
          .outputType(readType)
          .dataColumns(std::move(fileRowType))
          .isFilterPushdownEnabled(true)
          .parameters(std::move(tableParameters))
          .endTableScan()
          .capturePlanNodeId(scanNodeId)
          .planFragment();

  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(
          executor_.get(),
          core::QueryConfig{{}},
          {{"test-hive",
            std::make_shared<config::ConfigBase>(
                std::unordered_map<std::string, std::string>())}}),
      exec::Task::ExecutionMode::kSerial);

  auto filePaths = getFilePaths(tempDir->getPath());

  std::vector<std::shared_ptr<hive::HiveConnectorSplit>> hiveSplits;
  for (const auto& filePath : filePaths) {
    hiveSplits.push_back(std::make_shared<hive::HiveConnectorSplit>(
        kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET));
  }
  auto connectorSplit =
      std::make_shared<PaimonConnectorSplit>(kHiveConnectorId, hiveSplits);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);
  std::vector<RowVectorPtr> results;
  while (auto result = readTask->next()) {
    results.push_back(result);
  }

  auto concatenatedResult = BaseVector::create(readType, 0, pool_.get());
  for (const auto& result : results) {
    concatenatedResult->append(result.get());
  }

  bolt::test::assertEqualVectors(expected, concatenatedResult);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonBucketColumn) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto readType = ROW({{"a", INTEGER()}, {kColumnNameBucket, INTEGER()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101, 102}),
          vectorMaker_.flatVector<int32_t>({54321, 54321}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      54321);

  // Test with metadata column at index 0
  readType = ROW({{kColumnNameBucket, INTEGER()}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({54321, 54321}),
          vectorMaker_.flatVector<int32_t>({101, 102}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      54321);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonPartitionColumn) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102}),
       vectorMaker_.flatVector<int32_t>({103, 104})},
      {4, 3},
      {int8_t(PaimonRowKind::INSERT), int8_t(PaimonRowKind::INSERT)});

  std::unordered_map<std::string, std::string> tableParameters{
      {connector::paimon::kMergeEngine,
       connector::paimon::kDeduplicateMergeEngine},
      {connector::paimon::kPrimaryKey, "a"},
      {connector::paimon::kIgnoreDelete, "true"}};

  auto partitionFieldType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  auto readType =
      ROW({{"a", INTEGER()}, {kColumnNamePartition, partitionFieldType}});
  auto assignments =
      getIdentityAssignment(readType, {{"a", INTEGER()}, {"b", INTEGER()}});
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      1,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int32_t>({101}),
          vectorMaker_.constantRow(
              partitionFieldType, variant::row({103, 101}), 1),
      });

  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::unordered_map<std::string, std::optional<std::string>>{
          {"a", "101"}, {"b", "103"}});

  // Test with metadata column at index 0
  readType =
      ROW({{kColumnNamePartition, partitionFieldType}, {"a", INTEGER()}});
  assignments =
      getIdentityAssignment(readType, {{"a", INTEGER()}, {"b", INTEGER()}});
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      1,
      std::vector<VectorPtr>{
          vectorMaker_.constantRow(
              partitionFieldType, variant::row({103, 101}), 1),
          vectorMaker_.flatVector<int32_t>({101}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::unordered_map<std::string, std::optional<std::string>>{
          {"a", "101"}, {"b", "103"}});

  // Test with no partition
  RowTypePtr emptyRowPartitionType = ROW({}, {});
  readType =
      ROW({{kColumnNamePartition, emptyRowPartitionType}, {"a", INTEGER()}});
  assignments = getIdentityAssignment(readType, {});
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      2,
      std::vector<VectorPtr>{
          vectorMaker_.constantRow(emptyRowPartitionType, variant::row({}), 2),
          vectorMaker_.flatVector<int32_t>({101, 102})});
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowIdColumnWithoutRowIdInFile) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  // write a paimon file, without the rowId column
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
       vectorMaker_.flatVector<int32_t>({105, 106, 107, 108})},
      {4, 4, 4, 4},
      {static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT)},
      false);
  // read the file with the _ROW_ID column, expect the rowId to be a starting
  // number in the split (700) plus the row index.
  auto readType =
      ROW({{kColumnNameRowID, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({700, 701, 702, 703}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  std::unordered_map<std::string, std::string> tableParameters{
      {kRowTrackingEnabled, "true"}};
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaFirstRowID, "700"}});

  std::string lowerCaseRowId = kColumnNameRowID;
  folly::toLowerAscii(lowerCaseRowId);
  readType =
      ROW({{lowerCaseRowId, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({700, 701, 702, 703}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaFirstRowID, "700"}},
      true);
}

TEST_F(PaimonReaderMetadataFieldTest, testPaimonRowIdColumnWithRowIdInFile) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  // write a paimon file, with the rowId column
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
       vectorMaker_.flatVector<int32_t>({105, 106, 107, 108})},
      {4, 4, 4, 4},
      {static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT)},
      true);

  auto readType =
      ROW({{kColumnNameRowID, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({9001, 9002, 9003, 9004}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  std::unordered_map<std::string, std::string> tableParameters{
      {kRowTrackingEnabled, "true"}};
  assertOutput(
      readType, fileRowType, tempDir, tableParameters, assignments, expected);

  std::string lowerCaseRowId = kColumnNameRowID;
  folly::toLowerAscii(lowerCaseRowId);
  readType =
      ROW({{lowerCaseRowId, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({9001, 9002, 9003, 9004}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaFirstRowID, "700"}},
      true);
}

TEST_F(
    PaimonReaderMetadataFieldTest,
    testPaimonSequenceNumberColumnWithSequenceNumberInFile) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  // write a paimon file, with the sequence number column
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
       vectorMaker_.flatVector<int32_t>({105, 106, 107, 108})},
      {7, 6, 5, 4},
      {static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT)});

  auto readType = ROW(
      {{connector::paimon::kSEQUENCE_NUMBER, BIGINT()},
       {"a", INTEGER()},
       {"b", INTEGER()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({7, 6, 5, 4}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  std::unordered_map<std::string, std::string> tableParameters{
      {kRowTrackingEnabled, "true"}};
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaMaxSequenceNumber, "101"}});

  std::string sequenceNumberLower = kSEQUENCE_NUMBER;
  folly::toLowerAscii(sequenceNumberLower);
  readType = ROW(
      {{sequenceNumberLower, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({7, 6, 5, 4}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaMaxSequenceNumber, "101"}},
      true);
}

TEST_F(
    PaimonReaderMetadataFieldTest,
    testPaimonSequenceNumberColumnWithoutSequenceNumberInFile) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto rowType = ROW({{"a", INTEGER()}, {"b", INTEGER()}});
  // write a paimon file, with the sequence number column
  auto fileRowType = createPaimonFile(
      vectorMaker_,
      leafPool_.get(),
      tempDir->getPath(),
      executor_,
      {0},
      rowType,
      {vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
       vectorMaker_.flatVector<int32_t>({105, 106, 107, 108})},
      {},
      {static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT),
       static_cast<int8_t>(PaimonRowKind::INSERT)});

  auto readType = ROW(
      {{connector::paimon::kSEQUENCE_NUMBER, BIGINT()},
       {"a", INTEGER()},
       {"b", INTEGER()}});
  auto assignments = getIdentityAssignment(readType);
  auto expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({501, 501, 501, 501}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  std::unordered_map<std::string, std::string> tableParameters{
      {kRowTrackingEnabled, "true"}};
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaMaxSequenceNumber, "501"}});

  std::string sequenceNumberLower = kSEQUENCE_NUMBER;
  folly::toLowerAscii(sequenceNumberLower);
  readType = ROW(
      {{sequenceNumberLower, BIGINT()}, {"a", INTEGER()}, {"b", INTEGER()}});
  assignments = getIdentityAssignment(readType);
  expected = std::make_shared<RowVector>(
      leafPool_.get(),
      readType,
      BufferPtr(nullptr),
      4,
      std::vector<VectorPtr>{
          vectorMaker_.flatVector<int64_t>({501, 501, 501, 501}),
          vectorMaker_.flatVector<int32_t>({101, 102, 103, 104}),
          vectorMaker_.flatVector<int32_t>({105, 106, 107, 108}),
      });
  assertOutput(
      readType,
      fileRowType,
      tempDir,
      tableParameters,
      assignments,
      expected,
      std::nullopt,
      std::nullopt,
      {},
      {{kFileMetaMaxSequenceNumber, "501"}},
      true);
}

} // namespace
} // namespace bytedance::bolt::dwio::paimon::test
