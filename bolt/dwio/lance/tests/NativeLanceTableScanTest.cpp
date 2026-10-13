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

#include <gtest/gtest.h>

#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

namespace bytedance::bolt::lance::reader::test {
namespace {

using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::connector::hive;

class NativeLanceTableScanTest : public HiveConnectorTestBase {
 protected:
  std::string example(const std::string& name) const {
    return bytedance::bolt::test::getDataFilePath("examples/" + name);
  }

  core::PlanNodePtr scan(
      const RowTypePtr& outputType,
      std::vector<std::string> filters = {}) {
    return PlanBuilder(pool())
        .tableScan(outputType, std::move(filters))
        .planNode();
  }
};

TEST_F(NativeLanceTableScanTest, projectionFilterAndAggregation) {
  const auto path = example("sample.lance");
  const auto expected = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
       makeFlatVector<double>(20, [](auto row) { return row + 1; })});
  createDuckDbTable({expected});

  const auto hiveSplits =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE);
  const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      hiveSplits.begin(), hiveSplits.end());
  assertQuery(scan(ROW({"b"}, {DOUBLE()})), splits, "SELECT b FROM tmp");
  assertQuery(
      PlanBuilder(pool())
          .tableScan(
              ROW({"b"}, {DOUBLE()}),
              {"a BETWEEN 8 AND 10"},
              "",
              ROW({"a", "b"}, {BIGINT(), DOUBLE()}))
          .planNode(),
      splits,
      "SELECT b FROM tmp WHERE a BETWEEN 8 AND 10");
  assertQuery(
      PlanBuilder(pool())
          .tableScan(ROW({"a"}, {BIGINT()}))
          .singleAggregation({}, {"sum(a)"})
          .planNode(),
      splits,
      "SELECT sum(a) FROM tmp");
}

TEST_F(NativeLanceTableScanTest, filterOnlyNestedStructField) {
  const auto path = example("packed_fixed_v2_2.lance");
  const auto hiveSplits =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE);
  const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      hiveSplits.begin(), hiveSplits.end());
  const auto fileType =
      ROW({"packed"}, {ROW({"x", "y"}, {INTEGER(), BIGINT()})});

  assertQuery(
      PlanBuilder(pool())
          .tableScan(
              ROW({}, {}),
              {"packed.x BETWEEN 100::INTEGER AND 102::INTEGER"},
              "",
              fileType)
          .singleAggregation({}, {"count(0)"})
          .planNode(),
      splits,
      "SELECT 3");
}

TEST_F(NativeLanceTableScanTest, requiredStructSubfield) {
  const auto path = example("packed_fixed_v2_2.lance");
  const auto packedType = ROW({"x", "y"}, {INTEGER(), BIGINT()});
  const auto outputType = ROW({"packed"}, {packedType});
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("packed.x");
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  assignments["packed"] = std::make_shared<HiveColumnHandle>(
      "packed",
      HiveColumnHandle::ColumnType::kRegular,
      packedType,
      packedType,
      std::move(requiredSubfields));
  const auto plan = PlanBuilder(pool())
                        .startTableScan()
                        .outputType(outputType)
                        .assignments(std::move(assignments))
                        .endTableScan()
                        .planNode();
  const auto split =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE)[0];
  const auto result = AssertQueryBuilder(plan).split(split).copyResults(pool());

  ASSERT_EQ(result->size(), 2'051);
  const auto* packed = result->childAt(0)->as<RowVector>();
  ASSERT_NE(packed, nullptr);
  const auto* x = packed->childAt(0)->as<SimpleVector<int32_t>>();
  ASSERT_NE(x, nullptr);
  for (vector_size_t row = 0; row < result->size(); ++row) {
    EXPECT_EQ(packed->isNullAt(row), row % 19 == 0);
    if (!packed->isNullAt(row)) {
      EXPECT_EQ(x->valueAt(row), row);
    }
    EXPECT_TRUE(packed->isNullAt(row) || packed->childAt(1)->isNullAt(row));
  }
}

TEST_F(NativeLanceTableScanTest, requiredArrayAndMapSubfields) {
  const auto path = example("complex_v2_2.lance");
  const auto mapType = MAP(INTEGER(), BIGINT());
  const auto arrayType = ARRAY(INTEGER());
  const auto outputType = ROW({"map_val", "fsl"}, {mapType, arrayType});
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  std::vector<common::Subfield> mapSubfields;
  mapSubfields.emplace_back("map_val[31]");
  assignments["map_val"] = std::make_shared<HiveColumnHandle>(
      "map_val",
      HiveColumnHandle::ColumnType::kRegular,
      mapType,
      mapType,
      std::move(mapSubfields));
  std::vector<common::Subfield> arraySubfields;
  arraySubfields.emplace_back("fsl[2]");
  assignments["fsl"] = std::make_shared<HiveColumnHandle>(
      "fsl",
      HiveColumnHandle::ColumnType::kRegular,
      arrayType,
      arrayType,
      std::move(arraySubfields));
  const auto plan = PlanBuilder(pool())
                        .startTableScan()
                        .outputType(outputType)
                        .assignments(std::move(assignments))
                        .endTableScan()
                        .planNode();
  const auto split =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE)[0];
  const auto result = AssertQueryBuilder(plan).split(split).copyResults(pool());

  ASSERT_EQ(result->size(), 2'051);
  const auto* maps = result->childAt(0)->as<MapVector>();
  const auto* arrays = result->childAt(1)->as<ArrayVector>();
  ASSERT_NE(maps, nullptr);
  ASSERT_NE(arrays, nullptr);
  for (vector_size_t row = 0; row < result->size(); ++row) {
    EXPECT_EQ(maps->isNullAt(row), row % 17 == 0);
    EXPECT_EQ(arrays->isNullAt(row), row % 31 == 0);
    if (!maps->isNullAt(row)) {
      EXPECT_EQ(maps->sizeAt(row), row == 3 ? 1 : 0);
    }
    if (!arrays->isNullAt(row)) {
      EXPECT_EQ(arrays->sizeAt(row), 2);
    }
  }
  ASSERT_EQ(maps->mapKeys()->size(), 1);
  EXPECT_EQ(maps->mapKeys()->as<SimpleVector<int32_t>>()->valueAt(0), 31);
  EXPECT_EQ(maps->mapValues()->as<SimpleVector<int64_t>>()->valueAt(0), 301);
}

TEST_F(NativeLanceTableScanTest, multipleSplitsDoNotDuplicatePages) {
  const auto path = example("v2_0_self_described.lance");
  const auto expected = makeRowVector(
      {"id"}, {makeFlatVector<int32_t>(257, [](auto row) { return row; })});
  createDuckDbTable({expected});

  const auto hiveSplits =
      makeHiveConnectorSplits(path, 4, dwio::common::FileFormat::LANCE);
  const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      hiveSplits.begin(), hiveSplits.end());
  assertQuery(
      PlanBuilder(pool())
          .tableScan(ROW({"id"}, {INTEGER()}))
          .singleAggregation({}, {"count(0)", "sum(id)"})
          .planNode(),
      splits,
      "SELECT count(*), sum(id) FROM tmp");
}

TEST_F(NativeLanceTableScanTest, countStarWithoutDataColumns) {
  const auto path = example("sample.lance");
  const auto hiveSplits =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE);
  const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      hiveSplits.begin(), hiveSplits.end());

  assertQuery(
      PlanBuilder(pool())
          .tableScan(ROW({}, {}))
          .singleAggregation({}, {"count(0)"})
          .planNode(),
      splits,
      "SELECT 20");
}

TEST_F(NativeLanceTableScanTest, missingColumnsAreNullConstants) {
  const auto path = example("sample.lance");
  const auto expected = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
       makeFlatVector<double>(20, [](auto row) { return row + 1; })});
  createDuckDbTable({expected});
  const auto hiveSplits =
      makeHiveConnectorSplits(path, 1, dwio::common::FileFormat::LANCE);
  const std::vector<std::shared_ptr<connector::ConnectorSplit>> splits(
      hiveSplits.begin(), hiveSplits.end());

  assertQuery(
      PlanBuilder(pool())
          .tableScan(
              ROW({"a", "missing"}, {BIGINT(), ARRAY(VARBINARY())}),
              {},
              "",
              ROW({"a", "b", "missing"},
                  {BIGINT(), DOUBLE(), ARRAY(VARBINARY())}))
          .planNode(),
      splits,
      "SELECT a, NULL FROM tmp");
}

} // namespace
} // namespace bytedance::bolt::lance::reader::test
