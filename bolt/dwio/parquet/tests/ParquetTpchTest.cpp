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
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/init/Init.h>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/exec/tests/utils/TpchQueryBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/Register.h"
#include "bolt/parse/TypeResolver.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using bytedance::bolt::connector::tpch::kBoltTpchConnectorId;

class ParquetTpchTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    duckDb_ = std::make_shared<DuckDbQueryRunner>();
    tempDirectory_ = TempDirectoryPath::create();
    tpchBuilder_ =
        std::make_shared<TpchQueryBuilder>(dwio::common::FileFormat::PARQUET);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
#ifdef SPARK_COMPATIBLE
    functions::sparksql::registerFunctions("");
#endif

    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();

    bytedance::bolt::parquet::registerParquetReaderFactory();
    bytedance::bolt::parquet::registerParquetWriterFactory();

    auto hiveConnector =
        connector::getConnectorFactory(connector::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);

    auto tpchConnector =
        connector::getConnectorFactory(connector::kTpchConnectorName)
            ->newConnector(
                kBoltTpchConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpchConnector);

    saveTpchTablesAsParquet();
    tpchBuilder_->initialize(tempDirectory_->path);
  }

  static void TearDownTestSuite() {
    connector::unregisterConnector(kHiveConnectorId);
    connector::unregisterConnector(kBoltTpchConnectorId);
    bytedance::bolt::parquet::unregisterParquetReaderFactory();
    bytedance::bolt::parquet::unregisterParquetWriterFactory();
    tpchBuilder_.reset();
    tempDirectory_.reset();
    duckDb_.reset();
  }

  static void saveTpchTablesAsParquet() {
    std::shared_ptr<memory::MemoryPool> rootPool{
        memory::memoryManager()->addRootPool()};
    std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

    for (const auto& table : tpch::tables) {
      auto tableName = toTableName(table);
      auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->path, tableName);
      auto tableSchema = tpch::getTableSchema(table, true);
      auto columnNames = tableSchema->names();
      auto plan =
          PlanBuilder()
              .tpchTableScan(
                  table, std::move(columnNames), 0.01, kBoltTpchConnectorId)
              .planNode();
      auto split =
          exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
              kBoltTpchConnectorId, 1, 0));

      auto rows =
          AssertQueryBuilder(plan).splits({split}).copyResults(pool.get());
      duckDb_->createTable(tableName.data(), {rows});

      plan = PlanBuilder()
                 .values({rows})
                 .tableWrite(tableDirectory, dwio::common::FileFormat::PARQUET)
                 .planNode();

      AssertQueryBuilder(plan).copyResults(pool.get());
    }
  }

  void assertQuery(
      int queryId,
      const std::optional<std::vector<uint32_t>>& sortingKeys = {}) {
    auto tpchPlan = tpchBuilder_->getQueryPlan(queryId);
    auto duckDbSql = tpch::getQuery(queryId);
    assertQuery(tpchPlan, duckDbSql, sortingKeys);
  }

  std::shared_ptr<Task> assertQuery(
      const TpchPlan& tpchPlan,
      const std::string& duckQuery,
      const std::optional<std::vector<uint32_t>>& sortingKeys) const {
    bool noMoreSplits = false;
    constexpr int kNumSplits = 10;
    constexpr int kNumDrivers = 4;
    auto addSplits = [&](Task* task) {
      if (!noMoreSplits) {
        for (const auto& entry : tpchPlan.dataFiles) {
          for (const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, kNumSplits, tpchPlan.dataFileFormat);
            for (const auto& split : splits) {
              task->addSplit(entry.first, Split(split));
            }
          }
          task->noMoreSplits(entry.first);
        }
      }
      noMoreSplits = true;
    };
    CursorParameters params;
    params.maxDrivers = kNumDrivers;
    params.planNode = tpchPlan.plan;
    return exec::test::assertQuery(
        params, addSplits, duckQuery, *duckDb_, sortingKeys);
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<TempDirectoryPath> tempDirectory_;
  static std::shared_ptr<TpchQueryBuilder> tpchBuilder_;

  static constexpr char const* kTpchConnectorId{"test-tpch"};
};

std::shared_ptr<DuckDbQueryRunner> ParquetTpchTest::duckDb_ = nullptr;
std::shared_ptr<TempDirectoryPath> ParquetTpchTest::tempDirectory_ = nullptr;
std::shared_ptr<TpchQueryBuilder> ParquetTpchTest::tpchBuilder_ = nullptr;

TEST_F(ParquetTpchTest, Q1) {
  assertQuery(1);
}

TEST_F(ParquetTpchTest, Q3) {
  std::vector<uint32_t> sortingKeys{1, 2};
  assertQuery(3, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q5) {
  std::vector<uint32_t> sortingKeys{1};
  assertQuery(5, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q6) {
  assertQuery(6);
}

TEST_F(ParquetTpchTest, Q7) {
  std::vector<uint32_t> sortingKeys{0, 1, 2};
  assertQuery(7, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q8) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(8, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q9) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(9, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q10) {
  std::vector<uint32_t> sortingKeys{2};
  assertQuery(10, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q12) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(12, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q13) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(13, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q14) {
  assertQuery(14);
}

TEST_F(ParquetTpchTest, Q15) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(15, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q16) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQuery(16, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q17) {
  assertQuery(17);
}

TEST_F(ParquetTpchTest, Q18) {
  assertQuery(18);
}

TEST_F(ParquetTpchTest, Q19) {
  assertQuery(19);
}

TEST_F(ParquetTpchTest, Q20) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(20, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q21) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQuery(21, std::move(sortingKeys));
}

TEST_F(ParquetTpchTest, Q22) {
  std::vector<uint32_t> sortingKeys{0};
  assertQuery(22, std::move(sortingKeys));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
