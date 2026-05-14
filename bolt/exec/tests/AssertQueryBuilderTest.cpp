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

#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/connectors/ConnectorNames.h"
#include "bolt/connectors/tests/utils/ConnectorTestBase.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::exec::test {

class AssertQueryBuilderTest : public OperatorTestBase,
                               public ::testing::WithParamInterface<
                                   connector::test::ConnectorTestParam> {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    auto emptyConfig = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>());
    connector::test::registerTestConnector(
        GetParam().connectorName,
        GetParam().connectorId,
        ioExecutor_.get(),
        emptyConfig,
        GetParam().factoryRegistrar);
  }

  void TearDown() override {
    connector::test::unregisterTestConnector(
        GetParam().connectorName, GetParam().connectorId);
    OperatorTestBase::TearDown();
  }
};

TEST_P(AssertQueryBuilderTest, basic) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  AssertQueryBuilder(
      PlanBuilder().values({data}).planNode(), duckDbQueryRunner_)
      .assertResults("VALUES (1), (2), (3)");

  AssertQueryBuilder(PlanBuilder().values({data}).planNode())
      .assertResults(data);
}

TEST_P(AssertQueryBuilderTest, serialExecution) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  PlanBuilder builder;
  const auto& plan = builder.values({data}).planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .serialExecution(true)
      .assertResults("VALUES (1), (2), (3)");

  AssertQueryBuilder(plan).serialExecution(true).assertResults(data);
}

TEST_P(AssertQueryBuilderTest, orderedResults) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  AssertQueryBuilder(
      PlanBuilder().values({data}).orderBy({"c0 DESC"}, true).planNode(),
      duckDbQueryRunner_)
      .assertResults("VALUES (3), (2), (1)", {{0}});
}

TEST_P(AssertQueryBuilderTest, concurrency) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  AssertQueryBuilder(
      PlanBuilder().values({data}, true).planNode(), duckDbQueryRunner_)
      .maxDrivers(3)
      .assertResults("VALUES (1), (2), (3), (1), (2), (3), (1), (2), (3)");

  AssertQueryBuilder(PlanBuilder().values({data}, true).planNode())
      .maxDrivers(3)
      .assertResults({data, data, data});
}

TEST_P(AssertQueryBuilderTest, config) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  AssertQueryBuilder(
      PlanBuilder().values({data}).project({"c0 * 2"}).planNode(),
      duckDbQueryRunner_)
      .config(core::QueryConfig::kExprEvalSimplified, "true")
      .assertResults("VALUES (2), (4), (6)");
}

// Connector-specific split coverage (partition-keyed splits, etc.) lives in
// each connector's test directory (e.g.
// bolt/connectors/hive/tests/HiveAssertQueryBuilderTest.cpp for the Hive case).

TEST_P(AssertQueryBuilderTest, encodedResults) {
  VectorFuzzer::Options opts;
  opts.vectorSize = 1000;
  opts.nullRatio = 0.1;

  VectorFuzzer fuzzer(opts, pool_.get());

  // Dict(Array).
  auto input =
      makeRowVector({fuzzer.fuzzDictionary(fuzzer.fuzzFlat(ARRAY(INTEGER())))});
  auto flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});

  // Const(Array).
  input = makeRowVector({fuzzer.fuzzConstant(ARRAY(INTEGER()))});
  flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});

  // Dict(Map).
  input = makeRowVector(
      {fuzzer.fuzzDictionary(fuzzer.fuzzFlat(MAP(INTEGER(), VARCHAR())))});
  flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});

  // Const(Map).
  input = makeRowVector({fuzzer.fuzzConstant(MAP(INTEGER(), VARCHAR()))});
  flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});

  // Dict(Row).
  input = makeRowVector({fuzzer.fuzzDictionary(fuzzer.fuzzFlat(
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), VARCHAR(), BOOLEAN(), ARRAY(INTEGER())})))});
  flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});

  // Const(Row).
  input = makeRowVector({fuzzer.fuzzConstant(
      ROW({"c0", "c1", "c2", "c3"},
          {INTEGER(), VARCHAR(), BOOLEAN(), ARRAY(INTEGER())}))});
  flatInput = flatten<RowVector>(input);
  assertEqualResults({flatInput}, {input});
}

TEST_P(AssertQueryBuilderTest, nestedArrayMapResults) {
  VectorFuzzer::Options opts;
  opts.vectorSize = 1000;
  opts.nullRatio = 0.1;

  VectorFuzzer fuzzer(opts, pool_.get());

  // Array(Array).
  auto input = makeRowVector({fuzzer.fuzzFlat(ARRAY(ARRAY(BIGINT())))});
  assertEqualResults({input}, {input});

  // Map(Map).
  input = makeRowVector({fuzzer.fuzzDictionary(
      fuzzer.fuzzFlat(MAP(INTEGER(), MAP(INTEGER(), VARCHAR()))))});
  assertEqualResults({input}, {input});

  // Map(Array).
  input = makeRowVector({fuzzer.fuzzDictionary(
      fuzzer.fuzzFlat(MAP(INTEGER(), ARRAY(VARCHAR()))))});
  assertEqualResults({input}, {input});

  // Array(Map).
  input = makeRowVector({fuzzer.fuzzDictionary(
      fuzzer.fuzzFlat(ARRAY(MAP(INTEGER(), VARCHAR()))))});
  assertEqualResults({input}, {input});
}

INSTANTIATE_TEST_SUITE_P(
    Connectors,
    AssertQueryBuilderTest,
    ::testing::ValuesIn(connector::test::paramsFor(
        {std::string(connector::kHiveConnectorName)})));

} // namespace bytedance::bolt::exec::test
