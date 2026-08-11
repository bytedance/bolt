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

#include <gtest/gtest.h>

#include "bolt/exec/Aggregate.h"
#include "bolt/exec/tests/DummyAggregateFunction.h"
#include "bolt/expression/FunctionSignature.h"
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::exec::test {

namespace {

struct TestCompanionSignatureEntry {
  std::string functionName;
  std::vector<std::string> signatures;
};

struct TestCompanionSignatureMap {
  std::vector<TestCompanionSignatureEntry> partial;
  std::vector<TestCompanionSignatureEntry> merge;
  std::vector<TestCompanionSignatureEntry> extract;
  std::vector<TestCompanionSignatureEntry> mergeExtract;
};

class AggregateCompanionSignaturesTest : public testing::Test {
 public:
  AggregateCompanionSignaturesTest() {
    exec::aggregateFunctions().withWLock([&](auto& functionsMap) {
      functionsMap.clear();
      EXPECT_EQ(0, functionsMap.size());
    });
  }

 protected:
  void assertEqual(
      const std::vector<CompanionSignatureEntry>& actual,
      const std::vector<TestCompanionSignatureEntry>& expected) {
    EXPECT_EQ(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); ++i) {
      EXPECT_EQ(actual[i].functionName, expected[i].functionName);
      for (int j = 0; j < actual[i].signatures.size(); ++j) {
        EXPECT_EQ(
            actual[i].signatures[j]->toString(), expected[i].signatures[j]);
      }
    }
  }

  void assertEqual(
      const CompanionFunctionSignatureMap& actual,
      const TestCompanionSignatureMap& expected) {
    assertEqual(actual.partial, expected.partial);
    assertEqual(actual.merge, expected.merge);
    assertEqual(actual.extract, expected.extract);
    assertEqual(actual.mergeExtract, expected.mergeExtract);
  }
};

TEST_F(AggregateCompanionSignaturesTest, basic) {
  std::vector<AggregateFunctionSignaturePtr> signatures{
      AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("array(double)")
          .argumentType("double")
          .build(),
      AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("array(bigint)")
          .argumentType("bigint")
          .build()};
  registerDummyAggregateFunction("aggregateFunc1", signatures);
  auto companionSignatures = getCompanionFunctionSignatures("aggregateFunc1");
  EXPECT_TRUE(companionSignatures.has_value());

  TestCompanionSignatureMap expected;
  expected.partial = {
      {"aggregateFunc1_partial",
       {"(double) -> array(double) -> array(double)",
        "(bigint) -> array(bigint) -> array(bigint)"}}};
  expected.merge = {
      {"aggregateFunc1_merge",
       {"(array(double)) -> array(double) -> array(double)",
        "(array(bigint)) -> array(bigint) -> array(bigint)"}}};
  expected.mergeExtract = {
      {"aggregateFunc1_merge_extract",
       {"(array(double)) -> array(double) -> double",
        "(array(bigint)) -> array(bigint) -> bigint"}}};
  expected.extract = {
      {"aggregateFunc1_extract",
       {"(array(double)) -> double", "(array(bigint)) -> bigint"}}};

  assertEqual(*companionSignatures, expected);
}

TEST_F(AggregateCompanionSignaturesTest, extractFunctionNameWithSuffix) {
  std::vector<AggregateFunctionSignaturePtr> signatures{
      AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("array(double)")
          .argumentType("double")
          .build(),
      AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("array(double)")
          .argumentType("double")
          .argumentType("double")
          .build(),
      AggregateFunctionSignatureBuilder()
          .returnType("array(row(bigint))")
          .intermediateType("array(double)")
          .argumentType("bigint")
          .build()};
  registerDummyAggregateFunction("aggregateFunc2", signatures);
  auto companionSignatures = getCompanionFunctionSignatures("aggregateFunc2");
  EXPECT_TRUE(companionSignatures.has_value());

  TestCompanionSignatureMap expected;
  expected.partial = {
      {"aggregateFunc2_partial",
       {"(double) -> array(double) -> array(double)",
        "(double,double) -> array(double) -> array(double)",
        "(bigint) -> array(double) -> array(double)"}}};
  expected.merge = {
      {"aggregateFunc2_merge",
       {"(array(double)) -> array(double) -> array(double)"}}};
  expected.mergeExtract = {
      {"aggregateFunc2_merge_extract_array_row_bigint_endrow",
       {"(array(double)) -> array(double) -> array(row(bigint))"}},
      {"aggregateFunc2_merge_extract_double",
       {"(array(double)) -> array(double) -> double"}}};
  expected.extract = {
      {"aggregateFunc2_extract_array_row_bigint_endrow",
       {"(array(double)) -> array(row(bigint))"}},
      {"aggregateFunc2_extract_double", {"(array(double)) -> double"}}};

  assertEqual(*companionSignatures, expected);
}

TEST_F(AggregateCompanionSignaturesTest, templateSignature) {
  std::vector<AggregateFunctionSignaturePtr> signatures{
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("double")
          .intermediateType("T")
          .argumentType("double")
          .argumentType("T")
          .build(),
      AggregateFunctionSignatureBuilder()
          .typeVariable("K")
          .returnType("bigint")
          .intermediateType("K")
          .argumentType("bigint")
          .argumentType("K")
          .build()};
  registerDummyAggregateFunction("aggregateFunc3", signatures);
  auto companionSignatures = getCompanionFunctionSignatures("aggregateFunc3");
  EXPECT_TRUE(companionSignatures.has_value());

  TestCompanionSignatureMap expected;
  expected.partial = {
      {"aggregateFunc3_partial",
       {"(double,T) -> T -> T", "(bigint,K) -> K -> K"}}};
  expected.merge = {{"aggregateFunc3_merge", {"(T) -> T -> T"}}};
  expected.mergeExtract = {
      {"aggregateFunc3_merge_extract_bigint", {"(K) -> K -> bigint"}},
      {"aggregateFunc3_merge_extract_double", {"(T) -> T -> double"}}};
  expected.extract = {
      {"aggregateFunc3_extract_bigint", {"(K) -> bigint"}},
      {"aggregateFunc3_extract_double", {"(T) -> double"}}};

  assertEqual(*companionSignatures, expected);
}

TEST_F(
    AggregateCompanionSignaturesTest,
    resultTypeNotResolvableFromIntermediateType) {
  std::vector<AggregateFunctionSignaturePtr> signatures{
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("varbinary")
          .argumentType("T")
          .build()};
  registerDummyAggregateFunction("aggregateFunc4", signatures);
  auto companionSignatures = getCompanionFunctionSignatures("aggregateFunc4");
  EXPECT_TRUE(companionSignatures.has_value());

  assertEqual(
      companionSignatures->partial,
      {{"aggregateFunc4_partial", {"(T) -> varbinary -> varbinary"}}});
  assertEqual(
      companionSignatures->merge,
      {{"aggregateFunc4_merge", {"(varbinary) -> varbinary -> varbinary"}}});
  EXPECT_TRUE(companionSignatures->extract.empty());
  EXPECT_TRUE(companionSignatures->mergeExtract.empty());
}

} // namespace

} // namespace bytedance::bolt::exec::test
