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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <paimon/scan_context.h>
#include <paimon/table/source/plan.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_scan.h>
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <regex>
#include <sstream>
#include "Type.h"
#include "bolt/benchmarks/QueryBenchmarkBase.h"
#include "bolt/common/config/Config.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/connectors/paimon/BoltMemoryPool.h"
#include "bolt/connectors/paimon/PaimonConnector.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "bolt/connectors/paimon/PaimonTableHandle.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

using bytedance::bolt::BIGINT;
using bytedance::bolt::INTEGER;
using bytedance::bolt::QueryBenchmarkBase;
using bytedance::bolt::ROW;
using bytedance::bolt::RowTypePtr;
using bytedance::bolt::RunStats;
using bytedance::bolt::VARCHAR;
using bytedance::bolt::config::ConfigBase;
using bytedance::bolt::connector::ColumnHandle;
using bytedance::bolt::connector::ConnectorSplit;
using bytedance::bolt::connector::ConnectorTableHandle;
using bytedance::bolt::connector::getConnectorFactory;
using bytedance::bolt::connector::isConnectorRegistered;
using bytedance::bolt::connector::kHiveConnectorName;
using bytedance::bolt::connector::registerConnector;
using bytedance::bolt::connector::registerConnectorFactory;
using bytedance::bolt::connector::hive::HiveColumnHandle;
using bytedance::bolt::connector::hive::HiveConnectorSplit;
using bytedance::bolt::connector::hive::HiveTableHandle;
using bytedance::bolt::connector::hive::SubfieldFilters;
using bytedance::bolt::connector::paimon::PaimonColumnHandle;
using bytedance::bolt::connector::paimon::PaimonConnectorFactory;
using bytedance::bolt::connector::paimon::PaimonConnectorSplit;
using bytedance::bolt::connector::paimon::PaimonTableHandle;

DEFINE_string(data_path, "", "Path to the benchmark warehouse directory");
DEFINE_string(
    benchmark_tables,
    "append_only,aggregate,partial_update,deduplicate",
    "Comma separated list of tables to benchmark");
DEFINE_double(
    scale_factor,
    1.0,
    "Scale factor used to pick benchmark database name (sfX)");
DEFINE_string(
    db_name,
    "",
    "Override benchmark database name (defaults to benchmark_db_sf{scale_factor})");
DEFINE_int32(num_runs, 10, "Number of runs per benchmark iteration");

namespace {

class PaimonBenchmark : public QueryBenchmarkBase {
 public:
  void initialize() override {
    QueryBenchmarkBase::initialize();

    // Register Paimon connector
    registerConnectorFactory(std::make_shared<PaimonConnectorFactory>());
    auto paimonFactory =
        getConnectorFactory(PaimonConnectorFactory::kPaimonConnectorName);
    auto paimonConnector = paimonFactory->newConnector(
        "paimon", std::shared_ptr<const ConfigBase>{}, ioExecutor_.get());
    registerConnector(paimonConnector);

    // Register Hive connector with ID "hive" (QueryBenchmarkBase registers
    // "test-hive")
    if (!isConnectorRegistered("hive")) {
      auto hiveFactory = getConnectorFactory(kHiveConnectorName);
      auto config = std::make_shared<const ConfigBase>(
          std::unordered_map<std::string, std::string>());
      auto hiveConnector =
          hiveFactory->newConnector("hive", config, ioExecutor_.get());
      registerConnector(hiveConnector);
    }
  }

  static size_t runBenchmark(
      const std::string& tableName,
      const std::string& connectorName) {
    folly::BenchmarkSuspender suspender;
    auto databaseBaseName = [&]() {
      if (!FLAGS_db_name.empty()) {
        return FLAGS_db_name;
      }
      auto sf = FLAGS_scale_factor;
      // Build "benchmark_db_sf{sf}"
      std::string suffix;
      if (static_cast<double>(static_cast<int64_t>(sf)) == sf) {
        suffix = std::to_string(static_cast<int64_t>(sf));
      } else {
        // Format with minimal precision, replace '.' with '_'
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%g", sf);
        suffix = buf;
        std::replace(suffix.begin(), suffix.end(), '.', '_');
      }
      return std::string("benchmark_db_sf") + suffix;
    }();

    auto dbPath = std::filesystem::path(FLAGS_data_path) /
        (databaseBaseName + ".db") / tableName;
    std::string tablePath = "file:" + dbPath.string();

    // specific schema for benchmark tables
    // pt(string), id(long), key_col(string), val_col(string), num_col(int)
    auto rowType =
        ROW({"pt", "id", "key_col", "val_col", "num_col"},
            {VARCHAR(), BIGINT(), VARCHAR(), VARCHAR(), INTEGER()});

    std::shared_ptr<ConnectorTableHandle> tableHandle;
    std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>
        columnHandles;
    std::vector<std::shared_ptr<ConnectorSplit>> splits;

    if (connectorName == "paimon") {
      setupPaimon(tableName, tablePath, tableHandle, columnHandles, splits);
    } else if (connectorName == "hive") {
      setupHive(
          tableName, tablePath, rowType, tableHandle, columnHandles, splits);
    } else {
      LOG(FATAL) << "Unknown connector: " << connectorName;
    }

    auto plan = bytedance::bolt::exec::test::PlanBuilder()
                    .tableScan(rowType, tableHandle, columnHandles)
                    .planNode();

    bytedance::bolt::exec::test::CursorParameters params;
    params.planNode = plan;
    auto cursor = bytedance::bolt::exec::test::TaskCursor::create(params);
    auto tableScanNodeId = plan->id();

    suspender.dismiss();

    for (const auto& split : splits) {
      cursor->task()->addSplit(
          tableScanNodeId,
          bytedance::bolt::exec::Split(std::shared_ptr<ConnectorSplit>(split)));
    }
    cursor->task()->noMoreSplits(tableScanNodeId);

    int64_t totalRows = 0;
    while (cursor->moveNext()) {
      totalRows += cursor->current()->size();
    }

    folly::doNotOptimizeAway(totalRows);
    return totalRows;
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    // Not used for this benchmark structure
  }

 private:
  static void setupPaimon(
      const std::string& tableName,
      const std::string& tablePath,
      std::shared_ptr<ConnectorTableHandle>& tableHandle,
      std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      std::vector<std::shared_ptr<ConnectorSplit>>& splits) {
    columnHandles["pt"] = std::make_shared<PaimonColumnHandle>("pt", VARCHAR());
    columnHandles["id"] = std::make_shared<PaimonColumnHandle>("id", BIGINT());
    columnHandles["key_col"] =
        std::make_shared<PaimonColumnHandle>("key_col", VARCHAR());
    columnHandles["val_col"] =
        std::make_shared<PaimonColumnHandle>("val_col", VARCHAR());
    columnHandles["num_col"] =
        std::make_shared<PaimonColumnHandle>("num_col", INTEGER());

    tableHandle = std::make_shared<PaimonTableHandle>(
        "paimon",
        tableName,
        tablePath,
        std::unordered_map<std::string, std::string>());

    // Generate splits using Paimon SDK via TableScan.
    ::paimon::ScanContextBuilder contextBuilder(tablePath);
    auto scanContext =
        contextBuilder.AddOption(::paimon::Options::FILE_SYSTEM, "local")
            .Finish()
            .value();
    auto tableScan =
        ::paimon::TableScan::Create(std::move(scanContext)).value();
    auto scanResult = tableScan->CreatePlan();
    const auto& plan = scanResult.value();
    if (!plan) {
      LOG(FATAL) << "Paimon plan is null";
    }

    auto paimonSplits = plan->Splits();
    auto paimonPool = std::make_shared<
        bytedance::bolt::connector::paimon::BoltPaimonMemoryPool>(
        bytedance::bolt::memory::memoryManager()
            ->addRootPool("PaimonBenchmark")
            .get());
    for (auto& split : paimonSplits) {
      const auto serialized =
          ::paimon::Split::Serialize(split, paimonPool).value();
      splits.push_back(std::make_shared<PaimonConnectorSplit>(
          "paimon", serialized.data(), serialized.length()));
    }
  }

  static void setupHive(
      const std::string& tableName,
      const std::string& tablePath,
      const RowTypePtr& rowType,
      std::shared_ptr<ConnectorTableHandle>& tableHandle,
      std::unordered_map<std::string, std::shared_ptr<ColumnHandle>>&
          columnHandles,
      std::vector<std::shared_ptr<ConnectorSplit>>& splits) {
    columnHandles["pt"] = std::make_shared<HiveColumnHandle>(
        "pt",
        HiveColumnHandle::ColumnType::kPartitionKey,
        VARCHAR(),
        VARCHAR());
    columnHandles["id"] = std::make_shared<HiveColumnHandle>(
        "id", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT());
    columnHandles["key_col"] = std::make_shared<HiveColumnHandle>(
        "key_col",
        HiveColumnHandle::ColumnType::kRegular,
        VARCHAR(),
        VARCHAR());
    columnHandles["val_col"] = std::make_shared<HiveColumnHandle>(
        "val_col",
        HiveColumnHandle::ColumnType::kRegular,
        VARCHAR(),
        VARCHAR());
    columnHandles["num_col"] = std::make_shared<HiveColumnHandle>(
        "num_col",
        HiveColumnHandle::ColumnType::kRegular,
        INTEGER(),
        INTEGER());

    tableHandle = std::make_shared<HiveTableHandle>(
        "hive", tableName, true, SubfieldFilters{}, nullptr, rowType);

    // Scan directory for parquet files
    std::filesystem::path path(tablePath.substr(5)); // remove file:
    if (!std::filesystem::exists(path)) {
      LOG(FATAL) << "Path does not exist: " << path;
    }

    std::regex partitionRegex("pt=([0-9]+)");

    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(path)) {
      if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
        std::string filePath = entry.path().string();
        std::string parentDir = entry.path().parent_path().string();

        // Extract partition key
        std::smatch match;
        std::optional<std::string> ptValue;
        if (std::regex_search(parentDir, match, partitionRegex)) {
          ptValue = match[1].str();
        }

        std::unordered_map<std::string, std::optional<std::string>>
            partitionKeys;
        if (ptValue) {
          partitionKeys["pt"] = ptValue;
        }

        splits.push_back(std::make_shared<HiveConnectorSplit>(
            "hive",
            "file:" + filePath,
            bytedance::bolt::dwio::common::FileFormat::PARQUET,
            0,
            std::filesystem::file_size(entry.path()),
            partitionKeys));
      }
    }
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  if (FLAGS_data_path.empty()) {
    LOG(FATAL) << "--data_path is required";
  }

  PaimonBenchmark benchmark;
  benchmark.initialize();

  std::vector<std::string> tables;
  std::stringstream ss(FLAGS_benchmark_tables);
  std::string table;
  while (std::getline(ss, table, ',')) {
    tables.push_back(table);
  }

  for (const auto& t : tables) {
    folly::addBenchmark(__FILE__, "hive_" + t, [&benchmark, t]() {
      size_t total = 0;
      for (int i = 0; i < FLAGS_num_runs; ++i) {
        total += PaimonBenchmark::runBenchmark(t, "hive");
      }
      // Return total rows processed so folly accounts for work
      return total;
    });
    folly::addBenchmark(__FILE__, "paimon_" + t, [&benchmark, t]() {
      size_t total = 0;
      for (int i = 0; i < FLAGS_num_runs; ++i) {
        total += PaimonBenchmark::runBenchmark(t, "paimon");
      }
      return total;
    });
  }

  folly::runBenchmarks();

  benchmark.shutdown();
  return 0;
}
