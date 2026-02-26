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

#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/common/testutil/GPerf.h"

#include <limits>
#include <map>
#include <optional>
#include <string>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <bolt/dwio/common/BufferedInput.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
#include "folly/Range.h"

#include "bolt/common/base/Fs.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/HiveConnector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/PaimonConnectorSplit.h"
#include "bolt/connectors/hive/PaimonConstants.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/hive/paimon_merge_engines/PaimonRowKind.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::connector::hive;

DEFINE_string(
    data_path,
    "",
    "Root path of paimon data. All the files under the path will be merged as reading. Example layout for '-data_path=~/bolt/benchmark_data'\n");

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}
} // namespace

DEFINE_validator(data_path, &notEmpty);

class PaimonBenchmark : public QueryBenchmarkBase {
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  static const std::string kHiveConnectorId;
  std::shared_ptr<bytedance::bolt::test::VectorMaker> vectorMaker_;

 public:
  PaimonBenchmark() {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetTests");
    leafPool_ = rootPool_->addLeafChild("ParquetTests");
    vectorMaker_ =
        std::make_shared<bytedance::bolt::test::VectorMaker>(leafPool_.get());
  }

  ~PaimonBenchmark() {
    leafPool_.reset();
    rootPool_.reset();
  }

  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
  getIdentityAssignment(RowTypePtr rowType) {
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;

    for (int i = 0; i < rowType->size(); i++) {
      auto& colType = rowType->childAt(i);
      auto& colName = rowType->nameOf(i);
      assignments[colName] =
          std::make_shared<connector::hive::HiveColumnHandle>(
              colName,
              HiveColumnHandle::ColumnType::kRegular,
              colType,
              colType);
    }

    return assignments;
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

 public:
  void runMain(std::ostream& out, RunStats& runStats) {
    BoltProfilerStart("paimon.prof");

    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);

    filesystems::registerLocalFileSystem();

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto fileRowType = ROW(
        {{"pk_a", INTEGER()},
         {"a", INTEGER()},
         {"_SEQUENCE_NUMBER", BIGINT()},
         {"_VALUE_KIND", TINYINT()}});
    auto rowType = ROW({{"a", INTEGER()}});

    core::PlanNodeId scanNodeId;

    std::unordered_map<std::string, std::string> tableParameters{
        {connector::paimon::kMergeEngine,
         connector::paimon::kDeduplicateMergeEngine},
        {connector::paimon::kPrimaryKey, "a"},
        {connector::paimon::kIgnoreDelete, "true"}};

    auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        true,
        SubfieldFilters{},
        nullptr,
        fileRowType,
        tableParameters);

    auto assignments = getIdentityAssignment(fileRowType);

    auto readPlanFragment = exec::test::PlanBuilder()
                                .tableScan(rowType, tableHandle, assignments)
                                .capturePlanNodeId(scanNodeId)
                                .planFragment();

    // Create the reader task.
    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kSerial);

    auto filePaths = getFilePaths(FLAGS_data_path);

    std::vector<std::shared_ptr<HiveConnectorSplit>> hiveSplits;
    for (auto filePath : filePaths) {
      auto hiveSplit = std::make_shared<HiveConnectorSplit>(
          kHiveConnectorId, filePath, dwio::common::FileFormat::PARQUET);
      hiveSplits.push_back(hiveSplit);
    }

    auto connectorSplit =
        std::make_shared<connector::hive::PaimonConnectorSplit>(
            kHiveConnectorId, hiveSplits);

    readTask->addSplit(scanNodeId, exec::Split{connectorSplit});

    readTask->noMoreSplits(scanNodeId);

    const auto stats = readTask->taskStats();
    while (auto result = readTask->next()) {
    }

    connector::unregisterConnector(kHiveConnectorId);
    BoltProfilerStop();
  }
};

const std::string PaimonBenchmark::kHiveConnectorId = "test-hive";

BENCHMARK(reader) {
  PaimonBenchmark benchmark;
  RunStats runStats;
  benchmark.runMain(std::cout, runStats);
}

int paimonBenchmarkMain() {
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
