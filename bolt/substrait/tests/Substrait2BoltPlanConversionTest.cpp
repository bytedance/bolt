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

#include "bolt/substrait/tests/JsonToProtoConverter.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/HiveConnectorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/substrait/SubstraitToBoltPlan.h"
#include "bolt/type/Type.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::connector::hive;
using namespace bytedance::bolt::exec;

class Substrait2BoltPlanConversionTest
    : public exec::test::HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    exec::test::HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>>
  makeSplits(
      const bytedance::bolt::substrait::SubstraitBoltPlanConverter& converter,
      std::shared_ptr<const core::PlanNode> planNode) {
    const auto& splitsMap = converter.splitInfos();
    auto leafPlanNodeIds = planNode->leafPlanNodeIds();
    // Only one leaf node is expected here.
    EXPECT_EQ(1, leafPlanNodeIds.size());
    const auto& splits = splitsMap.at(*leafPlanNodeIds.begin());

    std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>>
        result;
    result.reserve(splits.size());
    for (const auto& s : splits) {
      result.emplace_back(s.connectorSplit);
    }
    return result;
  }

  static ::substrait::Rel valuesRel(
      const std::string& name,
      const std::vector<int64_t>& values) {
    ::substrait::Rel rel;
    auto* read = rel.mutable_read();
    read->mutable_base_schema()->add_names(name);
    read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64();
    read->mutable_virtual_table();
    for (auto value : values) {
      read->mutable_virtual_table()->add_values()->add_fields()->set_i64(value);
    }
    return rel;
  }

  static ::substrait::Rel joinRel(::substrait::HashJoinRel::JoinType type) {
    ::substrait::Rel rel;
    auto* join = rel.mutable_hash_join();
    *join->mutable_left() = valuesRel("left_key", {1, 2});
    *join->mutable_right() = valuesRel("right_key", {2, 3});
    join->set_type(type);
    auto* key = join->add_keys();
    key->mutable_left()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(0);
    key->mutable_right()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(0);
    key->mutable_comparison()->set_simple(
        ::substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
    return rel;
  }

  core::PlanNodePtr convert(
      const ::substrait::Rel& rel,
      bool appendProject = true) {
    ::substrait::Plan plan;
    *plan.add_relations()->mutable_rel() = rel;
    bytedance::bolt::substrait::SubstraitBoltPlanConverter converter(
        pool_.get(), appendProject);
    return converter.toBoltPlan(plan);
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{
      exec::test::TempDirectoryPath::create()};
};

TEST_F(Substrait2BoltPlanConversionTest, partitionKeysUnescapeAfterSplit) {
  ::substrait::ReadRel readRel;
  auto* file = readRel.mutable_local_files()->add_items();
  file->set_uri_file(
      "/tmp/table/part=value%3Dwith%3Dequals%2Fslash/"
      "escaped%3Dkey=key%3Dvalue/bad=value=with=equals/file.orc");
  file->mutable_orc();

  auto* baseSchema = readRel.mutable_base_schema();
  baseSchema->add_names("part");
  baseSchema->mutable_struct_()->add_types()->mutable_string();

  std::vector<exec::Split> splits;
  bytedance::bolt::substrait::SubstraitBoltPlanConverter planConverter(
      pool_.get());
  planConverter.toBoltPlan(readRel, splits);

  ASSERT_EQ(1, splits.size());
  auto hiveSplit =
      std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(
          splits[0].connectorSplit);
  ASSERT_NE(nullptr, hiveSplit);

  const auto& partitionKeys = hiveSplit->partitionKeys;
  auto part = partitionKeys.find("part");
  ASSERT_NE(partitionKeys.end(), part);
  ASSERT_TRUE(part->second.has_value());
  EXPECT_EQ("value=with=equals/slash", part->second.value());

  auto escapedKey = partitionKeys.find("escaped=key");
  ASSERT_NE(partitionKeys.end(), escapedKey);
  ASSERT_TRUE(escapedKey->second.has_value());
  EXPECT_EQ("key=value", escapedKey->second.value());

  EXPECT_EQ(0, partitionKeys.count("bad"));
}

TEST_F(Substrait2BoltPlanConversionTest, semiMarkAndRightAntiJoins) {
  using Join = ::substrait::HashJoinRel;
  for (auto type : {Join::JOIN_TYPE_LEFT_SEMI, Join::JOIN_TYPE_RIGHT_SEMI}) {
    auto node = convert(joinRel(type));
    exec::test::AssertQueryBuilder(node).assertResults(
        makeRowVector({makeFlatVector<int64_t>({2})}));
  }
  exec::test::AssertQueryBuilder(convert(joinRel(Join::JOIN_TYPE_LEFT_ANTI)))
      .assertResults(makeRowVector({makeFlatVector<int64_t>({1})}));
  exec::test::AssertQueryBuilder(convert(joinRel(Join::JOIN_TYPE_RIGHT_ANTI)))
      .assertResults(makeRowVector({makeFlatVector<int64_t>({3})}));
  exec::test::AssertQueryBuilder(convert(joinRel(Join::JOIN_TYPE_LEFT_MARK)))
      .assertResults(makeRowVector(
          {makeFlatVector<int64_t>({1, 2}),
           makeFlatVector<bool>({false, true})}));
  exec::test::AssertQueryBuilder(convert(joinRel(Join::JOIN_TYPE_RIGHT_MARK)))
      .assertResults(makeRowVector(
          {makeFlatVector<int64_t>({2, 3}),
           makeFlatVector<bool>({true, false})}));
}

TEST_F(Substrait2BoltPlanConversionTest, postJoinFilterRunsAfterOuterJoin) {
  auto rel = joinRel(::substrait::HashJoinRel::JOIN_TYPE_LEFT);
  rel.mutable_hash_join()
      ->mutable_post_join_filter()
      ->mutable_literal()
      ->set_boolean(false);
  exec::test::AssertQueryBuilder(convert(rel)).assertEmptyResults();
}

TEST_F(Substrait2BoltPlanConversionTest, rejectNullSafeJoinKeys) {
  auto rel = joinRel(::substrait::HashJoinRel::JOIN_TYPE_INNER);
  rel.mutable_hash_join()->mutable_keys(0)->mutable_comparison()->set_simple(
      ::substrait::ComparisonJoinKey::
          SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM);
  BOLT_ASSERT_THROW(
      convert(rel), "IS_NOT_DISTINCT_FROM keys are not supported");
}

TEST_F(Substrait2BoltPlanConversionTest, outputNamesAreAliasesAfterEmit) {
  ::substrait::Rel rel;
  auto* project = rel.mutable_project();
  *project->mutable_input() = valuesRel("original", {1, 2});
  project->add_expressions()->mutable_literal()->set_i64(7);
  project->mutable_common()->mutable_hint()->mutable_stats()->set_row_count(2);
  auto node = convert(rel);
  EXPECT_EQ(node->outputType()->size(), 2);
  exec::test::AssertQueryBuilder(node).assertResults(makeRowVector(
      {makeFlatVector<int64_t>({1, 2}), makeFlatVector<int64_t>({7, 7})}));

  project->mutable_common()->mutable_emit()->add_output_mapping(1);
  project->mutable_common()->mutable_hint()->add_output_names("renamed");
  node = convert(rel);
  EXPECT_EQ(node->outputType()->names(), std::vector<std::string>({"renamed"}));
  exec::test::AssertQueryBuilder(node).assertResults(
      makeRowVector({makeFlatVector<int64_t>({7, 7})}));

  rel = joinRel(::substrait::HashJoinRel::JOIN_TYPE_INNER);
  auto* names = rel.mutable_hash_join()->mutable_common()->mutable_hint();
  names->add_output_names("x");
  names->add_output_names("y");
  node = convert(rel);
  EXPECT_EQ(node->outputType()->names(), std::vector<std::string>({"x", "y"}));
  exec::test::AssertQueryBuilder(node).assertResults(makeRowVector(
      {makeFlatVector<int64_t>({2}), makeFlatVector<int64_t>({2})}));
}

TEST_F(
    Substrait2BoltPlanConversionTest,
    boltMlReplacementProjectAndJoinLayout) {
  ::substrait::Rel rel;
  auto* project = rel.mutable_project();
  *project->mutable_input() = valuesRel("original", {1, 2});
  project->add_expressions()
      ->mutable_selection()
      ->mutable_direct_reference()
      ->mutable_struct_field()
      ->set_field(0);
  project->mutable_common()->mutable_hint()->add_output_names("projected");
  auto node = convert(rel, false);
  EXPECT_EQ(
      node->outputType()->names(), std::vector<std::string>({"projected"}));
  exec::test::AssertQueryBuilder(node).assertResults(
      makeRowVector({makeFlatVector<int64_t>({1, 2})}));

  rel = joinRel(::substrait::HashJoinRel::JOIN_TYPE_INNER);
  rel.mutable_hash_join()->mutable_common()->mutable_hint()->add_output_names(
      "right_key");
  node = convert(rel, false);
  EXPECT_EQ(
      node->outputType()->names(), std::vector<std::string>({"right_key"}));
  exec::test::AssertQueryBuilder(node).assertResults(
      makeRowVector({makeFlatVector<int64_t>({2})}));
}

TEST_F(Substrait2BoltPlanConversionTest, legacyColumnMajorVirtualTable) {
  auto rel = valuesRel("a", {});
  auto* read = rel.mutable_read();
  read->mutable_base_schema()->add_names("b");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64();
  auto* batch = read->mutable_virtual_table()->add_values();
  for (auto value : {1, 2, 10, 20}) {
    batch->add_fields()->set_i64(value);
  }
  exec::test::AssertQueryBuilder(convert(rel))
      .assertResults(makeRowVector(
          {makeFlatVector<int64_t>({1, 2}),
           makeFlatVector<int64_t>({10, 20})}));
}

// This test will firstly generate mock TPC-H lineitem ORC file. Then, Bolt's
// computing will be tested based on the generated ORC file.
// Input: Json file of the Substrait plan for the below modified TPC-H Q6 query:
//
//  SELECT sum(l_extendedprice * l_discount) AS revenue
//  FROM lineitem
//  WHERE
//    l_shipdate_new >= 8766 AND l_shipdate_new < 9131 AND
//    l_discount BETWEEN .06 - 0.01 AND .06 + 0.01 AND
//    l_quantity < 24
//
//  Tested Bolt operators: TableScan (Filter Pushdown), Project, Aggregate.
TEST_F(Substrait2BoltPlanConversionTest, DISABLED_q6) {
  // Generate the used ORC file.
  auto type =
      ROW({"l_orderkey",
           "l_partkey",
           "l_suppkey",
           "l_linenumber",
           "l_quantity",
           "l_extendedprice",
           "l_discount",
           "l_tax",
           "l_returnflag",
           "l_linestatus",
           "l_shipdate",
           "l_commitdate",
           "l_receiptdate",
           "l_shipinstruct",
           "l_shipmode",
           "l_comment"},
          {BIGINT(),
           BIGINT(),
           BIGINT(),
           INTEGER(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           VARCHAR()});
  std::shared_ptr<memory::MemoryPool> pool{
      memory::memoryManager()->addLeafPool()};
  std::vector<VectorPtr> vectors;
  // TPC-H lineitem table has 16 columns.
  int colNum = 16;
  vectors.reserve(colNum);
  std::vector<int64_t> lOrderkeyData = {
      4636438147,
      2012485446,
      1635327427,
      8374290148,
      2972204230,
      8001568994,
      989963396,
      2142695974,
      6354246853,
      4141748419};
  vectors.emplace_back(makeFlatVector<int64_t>(lOrderkeyData));
  std::vector<int64_t> lPartkeyData = {
      263222018,
      255918298,
      143549509,
      96877642,
      201976875,
      196938305,
      100260625,
      273511608,
      112999357,
      299103530};
  vectors.emplace_back(makeFlatVector<int64_t>(lPartkeyData));
  std::vector<int64_t> lSuppkeyData = {
      2102019,
      13998315,
      12989528,
      4717643,
      9976902,
      12618306,
      11940632,
      871626,
      1639379,
      3423588};
  vectors.emplace_back(makeFlatVector<int64_t>(lSuppkeyData));
  std::vector<int32_t> lLinenumberData = {4, 6, 1, 5, 1, 2, 1, 5, 2, 6};
  vectors.emplace_back(makeFlatVector<int32_t>(lLinenumberData));
  std::vector<double> lQuantityData = {
      6.0, 1.0, 19.0, 4.0, 6.0, 12.0, 23.0, 11.0, 16.0, 19.0};
  vectors.emplace_back(makeFlatVector<double>(lQuantityData));
  std::vector<double> lExtendedpriceData = {
      30586.05,
      7821.0,
      1551.33,
      30681.2,
      1941.78,
      66673.0,
      6322.44,
      41754.18,
      8704.26,
      63780.36};
  vectors.emplace_back(makeFlatVector<double>(lExtendedpriceData));
  std::vector<double> lDiscountData = {
      0.05, 0.06, 0.01, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06, 0.07};
  vectors.emplace_back(makeFlatVector<double>(lDiscountData));
  std::vector<double> lTaxData = {
      0.02, 0.03, 0.01, 0.0, 0.01, 0.01, 0.03, 0.07, 0.01, 0.04};
  vectors.emplace_back(makeFlatVector<double>(lTaxData));
  std::vector<std::string> lReturnflagData = {
      "N", "A", "A", "R", "A", "N", "A", "A", "N", "R"};
  vectors.emplace_back(makeFlatVector<std::string>(lReturnflagData));
  std::vector<std::string> lLinestatusData = {
      "O", "F", "F", "F", "F", "O", "F", "F", "O", "F"};
  vectors.emplace_back(makeFlatVector<std::string>(lLinestatusData));
  std::vector<double> lShipdateNewData = {
      8953.666666666666,
      8773.666666666666,
      9034.666666666666,
      8558.666666666666,
      9072.666666666666,
      8864.666666666666,
      9004.666666666666,
      8778.666666666666,
      9013.666666666666,
      8832.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lShipdateNewData));
  std::vector<double> lCommitdateNewData = {
      10447.666666666666,
      8953.666666666666,
      8325.666666666666,
      8527.666666666666,
      8438.666666666666,
      10049.666666666666,
      9036.666666666666,
      8666.666666666666,
      9519.666666666666,
      9138.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lCommitdateNewData));
  std::vector<double> lReceiptdateNewData = {
      10456.666666666666,
      8979.666666666666,
      8299.666666666666,
      8474.666666666666,
      8525.666666666666,
      9996.666666666666,
      9103.666666666666,
      8726.666666666666,
      9593.666666666666,
      9178.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lReceiptdateNewData));
  std::vector<std::string> lShipinstructData = {
      "COLLECT COD",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "DELIVER IN PERSON",
      "DELIVER IN PERSON",
      "TAKE BACK RETURN",
      "NONE"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipinstructData));
  std::vector<std::string> lShipmodeData = {
      "FOB",
      "REG AIR",
      "MAIL",
      "FOB",
      "RAIL",
      "SHIP",
      "REG AIR",
      "REG AIR",
      "TRUCK",
      "AIR"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipmodeData));
  std::vector<std::string> lCommentData = {
      " the furiously final foxes. quickly final p",
      "thely ironic",
      "ate furiously. even, pending pinto bean",
      "ackages af",
      "odolites. slyl",
      "ng the regular requests sleep above",
      "lets above the slyly ironic theodolites sl",
      "lyly regular excuses affi",
      "lly unusual theodolites grow slyly above",
      " the quickly ironic pains lose car"};
  vectors.emplace_back(makeFlatVector<std::string>(lCommentData));

  // Write data into an ORC file.
  writeToFile(
      tmpDir_->path + "/mock_lineitem.orc",
      {makeRowVector(type->names(), vectors)});

  // Find and deserialize Substrait plan json file.
  std::string planPath = getDataFilePath("data/q6_first_stage.json");

  // Read q6_first_stage.json and resume the Substrait plan.
  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);

  // Convert to Bolt PlanNode.
  bytedance::bolt::substrait::SubstraitBoltPlanConverter planConverter(
      pool_.get());
  auto planNode = planConverter.toBoltPlan(substraitPlan);

  auto expectedResult = makeRowVector({
      makeFlatVector<double>(1, [](auto /*row*/) { return 13613.1921; }),
  });

  exec::test::AssertQueryBuilder(planNode)
      .splits(makeSplits(planConverter, planNode))
      .assertResults(expectedResult);
}
