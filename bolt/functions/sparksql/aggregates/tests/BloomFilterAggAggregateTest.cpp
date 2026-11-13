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

#include "bolt/common/base/BloomFilter.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
namespace bytedance::bolt::functions::aggregate::sparksql::test {
namespace {
class BloomFilterAggAggregateTest
    : public aggregate::test::AggregationTestBase {
 public:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    allowInputShuffle();
  }

  VectorPtr getSerializedBloomFilter(int32_t capacity) {
    BloomFilter bloomFilter;
    bloomFilter.reset(capacity);
    for (auto i = 0; i < 9; ++i) {
      bloomFilter.insert(folly::hasher<int64_t>()(i));
    }
    std::string data;
    data.resize(bloomFilter.serializedSize());
    bloomFilter.serialize(data.data());
    return makeConstant(StringView(data), 1, VARBINARY());
  }
};
} // namespace

TEST_F(BloomFilterAggAggregateTest, basic) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>(
      100, [](vector_size_t row) { return row % 9; })})};
  auto expected = {makeRowVector({getSerializedBloomFilter(4)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expected);
}

TEST_F(BloomFilterAggAggregateTest, bloomFilterAggArgument) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>(
      100, [](vector_size_t row) { return row % 9; })})};

  auto expected1 = {makeRowVector({getSerializedBloomFilter(13)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 6)"}, expected1);

  auto expected2 = {makeRowVector({getSerializedBloomFilter(524288)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0)"}, expected2);

  // Max bits case: bloom filter is at its largest possible size.
  auto expected3 = {makeRowVector({getSerializedBloomFilter(4194304)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 10000000)"}, expected3);
}

TEST_F(BloomFilterAggAggregateTest, emptyInput) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>({})})};
  auto expected = {makeRowVector({makeNullConstant(TypeKind::VARBINARY, 1)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expected);
}

TEST_F(BloomFilterAggAggregateTest, nullInput) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(100, [](vector_size_t row) { return row % 9; }),
       makeAllNullFlatVector<int64_t>(100)})};
  auto expected = {makeRowVector({getSerializedBloomFilter(4)})};
  testAggregations(vectors, {}, {"bloom_filter_agg(c0, 5, 64)"}, expected);
}

TEST_F(BloomFilterAggAggregateTest, config) {
  auto vector = {makeRowVector({makeFlatVector<int64_t>(
      100, [](vector_size_t row) { return row % 9; })})};
  std::vector<RowVectorPtr> expected = {
      makeRowVector({getSerializedBloomFilter(100)})};

  // This config will decide the bloom filter capacity, the expected value is
  // the serialized bloom filter, it should be consistent.
  testAggregations(
      vector,
      {},
      {"bloom_filter_agg(c0)"},
      expected,
      {{core::QueryConfig::kSparkBloomFilterMaxNumBits, "1600"}});

  // Test fails without setting the config.
  auto planNode = exec::test::PlanBuilder(pool())
                      .values(vector)
                      .partialAggregation({}, {"bloom_filter_agg(c0)"})
                      .finalAggregation()
                      .planNode();
  auto actual = exec::test::AssertQueryBuilder(planNode).copyResults(pool());
  EXPECT_FALSE(
      expected[0]->childAt(0)->equalValueAt(actual->childAt(0).get(), 0, 0));
}
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
