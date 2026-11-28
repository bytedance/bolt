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

#include <algorithm>
#include <filesystem>
#include <unordered_set>

#include <gtest/gtest.h>

#include "bolt/benchmarks/clickbench/ClickBenchQueryBuilder.h"
#include "bolt/connectors/hive/TableHandle.h"
#include "bolt/dwio/parquet/RegisterParquetReader.h"
#include "bolt/dwio/parquet/RegisterParquetWriter.h"
#include "bolt/exec/PartitionFunction.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::exec::test {
namespace {

void collectScans(
    const core::PlanNodePtr& node,
    std::vector<core::TableScanNodePtr>& scans) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    scans.push_back(std::move(scan));
  }
  for (const auto& source : node->sources()) {
    collectScans(source, scans);
  }
}

class ClickBenchQueryBuilderTest : public testing::Test,
                                   public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
    filesystems::registerLocalFileSystem();
    bytedance::bolt::parquet::registerParquetReaderFactory();
    bytedance::bolt::parquet::registerParquetWriterFactory();
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

  static void TearDownTestSuite() {
    connector::unregisterConnector(kHiveConnectorId);
    bytedance::bolt::parquet::unregisterParquetReaderFactory();
    bytedance::bolt::parquet::unregisterParquetWriterFactory();
  }

  static RowTypePtr hitsType() {
    const std::unordered_set<std::string> varcharColumns = {
        "title",
        "url",
        "referer",
        "flashminor2",
        "useragentminor",
        "mobilephonemodel",
        "params",
        "searchphrase",
        "pagecharset",
        "originalurl",
        "hitcolor",
        "browserlanguage",
        "browsercountry",
        "socialnetwork",
        "socialaction",
        "socialsourcepage",
        "paramorderid",
        "paramcurrency",
        "openstatservicename",
        "openstatcampaignid",
        "openstatadid",
        "openstatsourceid",
        "utmsource",
        "utmmedium",
        "utmcampaign",
        "utmcontent",
        "utmterm",
        "fromtag"};
    std::vector<TypePtr> types;
    types.reserve(ClickBenchQueryBuilder::columnNames().size());
    for (const auto& name : ClickBenchQueryBuilder::columnNames()) {
      if (varcharColumns.count(name)) {
        types.push_back(VARCHAR());
      } else {
        types.push_back(BIGINT());
      }
    }
    auto names = ClickBenchQueryBuilder::columnNames();
    return ROW(std::move(names), std::move(types));
  }

  static std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      const TpchPlan& query) {
    std::vector<std::shared_ptr<connector::ConnectorSplit>> result;
    for (const auto& entry : query.dataFiles) {
      for (const auto& path : entry.second) {
        auto fileSplits = HiveConnectorTestBase::makeHiveConnectorSplits(
            path, 1, query.dataFileFormat);
        result.insert(result.end(), fileSplits.begin(), fileSplits.end());
      }
    }
    return result;
  }

  void writeSmallParquetData(const std::string& tableDirectory) {
    auto type = hitsType();
    const vector_size_t rows = 8;
    std::vector<VectorPtr> children;
    children.reserve(type->size());
    for (const auto& name : type->names()) {
      if (type->findChild(name)->isVarchar()) {
        children.push_back(makeFlatVector<std::string>(rows, [&](auto row) {
          if (name == "url") {
            return std::string{
                row % 2 == 0 ? "https://google.example/search" : "other"};
          }
          if (name == "title") {
            return std::string{row % 2 == 0 ? "Google Result" : "Other"};
          }
          if (name == "searchphrase") {
            return std::string{row % 3 == 0 ? "bolt" : "clickbench"};
          }
          if (name == "mobilephonemodel") {
            return std::string{row % 2 == 0 ? "phone" : ""};
          }
          if (name == "referer") {
            return std::string{"https://www.example.com/path"};
          }
          return name + std::to_string(row);
        }));
      } else {
        children.push_back(makeFlatVector<int64_t>(rows, [&](auto row) {
          if (name == "counterid") {
            return int64_t{62};
          }
          if (name == "eventdate") {
            return int64_t{15887 + row};
          }
          if (name == "eventtime") {
            return int64_t{1'373'000'000 + row * 60};
          }
          if (name == "userid") {
            return int64_t{100 + (row % 4)};
          }
          if (name == "advengineid") {
            return int64_t{row % 3};
          }
          if (name == "resolutionwidth") {
            return int64_t{1024 + row};
          }
          if (name == "isrefresh" || name == "dontcounthits" ||
              name == "isdownload") {
            return int64_t{0};
          }
          if (name == "islink") {
            return int64_t{1};
          }
          if (name == "traficsourceid") {
            return int64_t{row % 2 == 0 ? 6 : -1};
          }
          if (name == "refererhash") {
            return int64_t{3594120000172545465};
          }
          if (name == "urlhash") {
            return int64_t{2868770270353813622};
          }
          return int64_t{row + 1};
        }));
      }
    }

    auto input = makeRowVector(type->names(), children);
    auto writePlan =
        PlanBuilder()
            .values({input})
            .tableWrite(tableDirectory, dwio::common::FileFormat::PARQUET)
            .planNode();
    AssertQueryBuilder(writePlan).copyResults(pool());
  }

  RowVectorPtr runQuery(
      const ClickBenchQueryBuilder& builder,
      int32_t queryId) {
    auto query = builder.getQueryPlan(queryId);
    return AssertQueryBuilder(query.plan)
        .splits(splits(query))
        .copyResults(pool());
  }
};

TEST_F(ClickBenchQueryBuilderTest, queryManifest) {
  const auto& names = ClickBenchQueryBuilder::queryNames();
  ASSERT_EQ(names.size(), 43);
  for (int32_t i = 1; i <= 43; ++i) {
    EXPECT_EQ(names[i - 1], "q" + std::to_string(i));
  }
  EXPECT_EQ(ClickBenchQueryBuilder::columnNames().size(), 105);
}

TEST_F(ClickBenchQueryBuilderTest, buildsAllQueries) {
  ClickBenchQueryBuilder builder;
  builder.initialize(hitsType(), {"/tmp/hits.parquet"});

  for (int32_t queryId = 1; queryId <= 43; ++queryId) {
    SCOPED_TRACE(queryId);
    const auto query = builder.getQueryPlan(queryId);
    EXPECT_EQ(query.planName, "q" + std::to_string(queryId));
    EXPECT_EQ(query.dataFileFormat, dwio::common::FileFormat::PARQUET);
    ASSERT_NE(query.plan, nullptr);

    std::vector<core::TableScanNodePtr> scans;
    collectScans(query.plan, scans);
    ASSERT_EQ(scans.size(), 1);
    ASSERT_EQ(query.dataFiles.size(), 1);
    EXPECT_EQ(
        query.dataFiles.at(scans.front()->id()),
        std::vector<std::string>{"/tmp/hits.parquet"});
  }
}

TEST_F(ClickBenchQueryBuilderTest, rejectsInvalidInputs) {
  ClickBenchQueryBuilder builder;
  EXPECT_THROW(builder.initialize(hitsType(), {}), BoltUserError);

  builder.initialize(hitsType(), {"/tmp/hits.parquet"});
  EXPECT_THROW(builder.getQueryPlan(0), BoltUserError);
  EXPECT_THROW(builder.getQueryPlan(44), BoltUserError);

  auto names = ClickBenchQueryBuilder::columnNames();
  names.pop_back();
  std::vector<TypePtr> types(names.size(), BIGINT());
  EXPECT_THROW(
      builder.initialize(ROW(std::move(names), std::move(types)), {"file"}),
      BoltUserError);
}

TEST_F(ClickBenchQueryBuilderTest, executesRepresentativeQueriesOnParquet) {
  const auto directory = TempDirectoryPath::create();
  const auto tableDirectory =
      (std::filesystem::path(directory->path) / "hits").string();
  writeSmallParquetData(tableDirectory);

  ClickBenchQueryBuilder builder;
  builder.initialize(directory->path);

  EXPECT_EQ(runQuery(builder, 1)->size(), 1);
  EXPECT_EQ(runQuery(builder, 5)->size(), 1);
  EXPECT_EQ(runQuery(builder, 8)->size(), 2);
  EXPECT_EQ(runQuery(builder, 19)->size(), 8);
  EXPECT_EQ(runQuery(builder, 21)->size(), 1);
  EXPECT_EQ(runQuery(builder, 24)->size(), 4);
  EXPECT_EQ(runQuery(builder, 37)->size(), 2);
  EXPECT_EQ(runQuery(builder, 43)->size(), 0);
}

} // namespace
} // namespace bytedance::bolt::exec::test
