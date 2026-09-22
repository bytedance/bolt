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
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

namespace bytedance::bolt::lance::reader::test {
namespace {

using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

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
