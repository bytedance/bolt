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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/fuzzer/tests/FuzzerConnectorTestBase.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "gtest/gtest.h"
namespace bytedance::bolt::connector::fuzzer::test {

class FuzzerConnectorTest : public FuzzerConnectorTestBase {};

using bytedance::bolt::exec::test::PlanBuilder;

TEST_F(FuzzerConnectorTest, singleSplit) {
  const size_t numRows = 100;
  auto type = ROW({BIGINT(), DOUBLE(), VARCHAR()});

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeFuzzerTableHandle())
                  .endTableScan()
                  .planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, floatingPoints) {
  const size_t numRows = 1000;
  auto type = ROW({REAL(), DOUBLE()});

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeFuzzerTableHandle())
                  .endTableScan()
                  .planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, complexTypes) {
  const size_t numRows = 100;
  auto type = ROW({
      ARRAY(BIGINT()),
      ROW({VARCHAR(), MAP(INTEGER(), ARRAY(DOUBLE())), VARBINARY()}),
      REAL(),
  });

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeFuzzerTableHandle())
                  .endTableScan()
                  .planNode();

  exec::test::AssertQueryBuilder(plan)
      .split(makeFuzzerSplit(numRows))
      .assertTypeAndNumRows(type, numRows);
}

TEST_F(FuzzerConnectorTest, multipleSplits) {
  const size_t rowsPerSplit = 100;
  const size_t numSplits = 10;
  auto type = ROW({BIGINT(), DOUBLE(), VARCHAR()});

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(type)
                  .tableHandle(makeFuzzerTableHandle())
                  .endTableScan()
                  .planNode();

  exec::test::AssertQueryBuilder(plan)
      .splits(makeFuzzerSplits(rowsPerSplit, numSplits))
      .assertTypeAndNumRows(type, rowsPerSplit * numSplits);
}

TEST_F(FuzzerConnectorTest, randomTypes) {
  const size_t rowsPerSplit = 100;
  const size_t numSplits = 10;

  const size_t iterations = 20;

  for (size_t i = 0; i < iterations; ++i) {
    auto type = VectorFuzzer({}, pool()).randRowType();

    auto plan = PlanBuilder()
                    .startTableScan()
                    .outputType(type)
                    .tableHandle(makeFuzzerTableHandle())
                    .endTableScan()
                    .planNode();
    exec::test::AssertQueryBuilder(plan)
        .splits(makeFuzzerSplits(rowsPerSplit, numSplits))
        .assertTypeAndNumRows(type, rowsPerSplit * numSplits);
  }
}

TEST_F(FuzzerConnectorTest, reproducible) {
  const size_t numRows = 100;
  auto type = ROW({BIGINT(), ARRAY(INTEGER()), VARCHAR()});

  auto plan1 = PlanBuilder()
                   .startTableScan()
                   .outputType(type)
                   .tableHandle(makeFuzzerTableHandle(/*fuzerSeed=*/1234))
                   .endTableScan()
                   .planNode();
  auto plan2 = PlanBuilder()
                   .startTableScan()
                   .outputType(type)
                   .tableHandle(makeFuzzerTableHandle(/*fuzerSeed=*/1234))
                   .endTableScan()
                   .planNode();

  auto results1 = exec::test::AssertQueryBuilder(plan1)
                      .split(makeFuzzerSplit(numRows))
                      .copyResults(pool());
  auto results2 = exec::test::AssertQueryBuilder(plan2)
                      .split(makeFuzzerSplit(numRows))
                      .copyResults(pool());

  exec::test::assertEqualResults({results1}, {results2});
}

} // namespace bytedance::bolt::connector::fuzzer::test
