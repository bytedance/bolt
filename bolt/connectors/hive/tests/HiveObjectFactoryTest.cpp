/*
 * Copyright (c) International Business Machines Corporation
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
#include <folly/dynamic.h>
#include <gtest/gtest.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryAllocator.h"
#include "bolt/connectors/ConnectorOptions.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/connectors/hive/HiveObjectFactory.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

namespace bytedance::bolt::connector::hive::test {

using namespace bytedance::bolt;
using namespace bytedance::bolt::connector::hive;
using bytedance::bolt::connector::ConnectorLocationHandle;

static constexpr char kConnectorId[] = "hive-test";

class HiveObjectFactoryTest : public exec::test::OperatorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();

    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveConnectorSplit::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    Type::registerSerDe();
  }

  void SetUp() override {
    factory_ = std::make_unique<HiveObjectFactory>(kConnectorId);
  }

  void TearDown() override {
    factory_.reset();
  }

  std::shared_ptr<connector::ColumnHandle> makeHiveColumnHandle(
      const std::string& name,
      HiveColumnHandle::ColumnType columnType,
      const TypePtr& dataType,
      const TypePtr& hiveType,
      std::vector<std::string> requiredSubfields) {
    connector::DynamicConnectorOptions opts;
    opts.options = folly::dynamic::object;

    // columnType would be serialized as int32_t
    opts.options["columnType"] = (int32_t)columnType;
    opts.options["hiveType"] = hiveType->serialize();

    opts.options["requiredSubfields"] = folly::dynamic::array();
    for (const auto& subfield : requiredSubfields) {
      opts.options["requiredSubfields"].push_back(subfield);
    }

    auto colHandle = factory_->makeColumnHandle(name, dataType, opts);

    return colHandle;
  }

 protected:
  std::unique_ptr<HiveObjectFactory> factory_;
};

TEST_F(HiveObjectFactoryTest, MakeConnectorSplitDefaults) {
  // No options: only filePath/start/length/connectorId are set
  auto splitPtr = factory_->makeConnectorSplit(
      "s3://bucket/path/file.orc", 123, 456, connector::makeOptions({}));
  auto* split = dynamic_cast<HiveConnectorSplit*>(splitPtr.get());
  ASSERT_NE(split, nullptr);
  EXPECT_EQ(split->filePath, "s3://bucket/path/file.orc");
  EXPECT_EQ(split->start, 123);
  EXPECT_EQ(split->length, 456);
  EXPECT_EQ(split->connectorId, kConnectorId);

  // Defaults: PARQUET format
  // TODO: use constant literal
  EXPECT_EQ(split->fileFormat, dwio::common::FileFormat::PARQUET);
}

TEST_F(HiveObjectFactoryTest, MakeConnectorSplitWithOptions) {
  connector::DynamicConnectorOptions opts;
  opts.options = folly::dynamic::object;

  // Basic configuration
  opts.options["fileFormat"] =
      static_cast<int>(dwio::common::FileFormat::PARQUET);

  // Info columns map: column name -> info string
  opts.options["infoColumns"] = folly::dynamic::object;
  opts.options["infoColumns"]["colA"] = "infoA";
  opts.options["infoColumns"]["colB"] = "infoB";

  // Partition keys map: key -> value
  opts.options["partitionKeys"] = folly::dynamic::object;
  opts.options["partitionKeys"]["p1"] = "v1";

  auto splitPtr = factory_->makeConnectorSplit("/tmp/f.p", 0, 10, opts);
  auto* split = dynamic_cast<HiveConnectorSplit*>(splitPtr.get());
  ASSERT_NE(split, nullptr);

  EXPECT_EQ(split->fileFormat, dwio::common::FileFormat::PARQUET);

  // infoColumns
  auto info = split->infoColumns;
  EXPECT_EQ(info.at("colA"), "infoA");
  EXPECT_EQ(info.at("colB"), "infoB");

  // partitionKeys
  auto parts = split->partitionKeys;
  ASSERT_EQ(parts.size(), 1);
  EXPECT_EQ(parts.count("p1"), 1);
  EXPECT_EQ(parts["p1"], "v1");
}

TEST_F(HiveObjectFactoryTest, MakeTableHandleWithOptions) {
  // Build a RowType for data columns: two ints
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  // ColumnHandles
  auto c0 = makeHiveColumnHandle(
      "c0",
      HiveColumnHandle::ColumnType::kRegular,
      INTEGER(),
      INTEGER(),
      {"c0"});
  auto c1 = makeHiveColumnHandle(
      "c1", HiveColumnHandle::ColumnType::kRegular, BIGINT(), BIGINT(), {"c1"});

  // Options: disable filter pushdown, add subfield filter & remaining filter &
  // tableParameters
  connector::DynamicConnectorOptions opts;
  opts.options = folly::dynamic::object;

  // Basic configuration
  opts.options["filterPushdownEnabled"] = false;

  // common::SubfieldFilters : std::unordered_map<Subfield, FilterPtr>;
  auto filter = std::make_unique<common::BigintRange>(-100, 100, false);
  opts.options["subfieldFilters"] = folly::dynamic::object;
  opts.options["subfieldFilters"]["c0"] = filter->serialize();

  // core::TypedExprPtr remainingFilter : std::shared_ptr<const ITypedExpr>;
  auto remainingFilter = parseExpr("c1 + 1 > 1", rowType);
  opts.options["remainingFilter"] = remainingFilter->serialize();

  // Arbitrary table parameters
  opts.options["tableParameters"] = folly::dynamic::object;
  opts.options["tableParameters"]["pA"] = "vA";

  auto handlePtr = factory_->makeTableHandle("tbl", {c0, c1}, opts);
  auto* hiveHandle = dynamic_cast<HiveTableHandle*>(handlePtr.get());
  ASSERT_NE(hiveHandle, nullptr);

  EXPECT_FALSE(hiveHandle->isFilterPushdownEnabled());

  const auto& subfieldFilters = hiveHandle->subfieldFilters();
  ASSERT_EQ(subfieldFilters.count(common::Subfield("c0")), 1);
  subfieldFilters.at(common::Subfield("c0"))->testingEquals(*filter);

  auto remainingFilterCreated = hiveHandle->remainingFilter();
  ASSERT_NE(remainingFilterCreated, nullptr);
  ASSERT_EQ(*remainingFilterCreated, *remainingFilter);

  auto params = hiveHandle->tableParameters();
  ASSERT_EQ(params.size(), 1);
  EXPECT_EQ(params.at("pA"), "vA");
}

TEST_F(HiveObjectFactoryTest, MakeColumnHandle) {
  auto colHandle = makeHiveColumnHandle(
      "colX",
      HiveColumnHandle::ColumnType::kRegular,
      BIGINT(),
      BIGINT(),
      std::vector<std::string>({"f1", "f2"}));

  auto* hiveColumnHandle = dynamic_cast<HiveColumnHandle*>(colHandle.get());
  ASSERT_NE(hiveColumnHandle, nullptr);

  EXPECT_EQ(hiveColumnHandle->name(), "colX");
  EXPECT_EQ(
      hiveColumnHandle->columnType(), HiveColumnHandle::ColumnType::kRegular);
  EXPECT_EQ(hiveColumnHandle->dataType(), BIGINT());
  EXPECT_EQ(hiveColumnHandle->hiveType(), BIGINT());

  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("f1");
  requiredSubfields.emplace_back("f2");
  EXPECT_EQ(hiveColumnHandle->requiredSubfields(), requiredSubfields);
}

TEST_F(HiveObjectFactoryTest, MakeLocationHandle) {
  connector::DynamicConnectorOptions opts;
  opts.options = folly::dynamic::object;

  // Basic configuration
  opts.options["targetPath"] = "/tmp/out1";
  opts.options["writePath"] = "/tmp/out2";
  opts.options["targetFileName"] = "test.parquet";

  // Default: writeDirectory == targetDirectory
  auto locationHandle1 = factory_->makeLocationHandle(
      ConnectorLocationHandle::TableType::kNew, opts);

  auto* hiveLocationHandle =
      dynamic_cast<hive::LocationHandle*>(locationHandle1.get());
  ASSERT_NE(hiveLocationHandle, nullptr);

  EXPECT_EQ(hiveLocationHandle->targetPath(), "/tmp/out1");
  EXPECT_EQ(hiveLocationHandle->writePath(), "/tmp/out2");
  EXPECT_EQ(hiveLocationHandle->targetFileName(), "test.parquet");
  EXPECT_EQ(
      locationHandle1->tableType(), ConnectorLocationHandle::TableType::kNew);
}

} // namespace bytedance::bolt::connector::hive::test
