/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include <algorithm>
#include <tuple>

#include "bolt/common/memory/Memory.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::aggregate::prestosql {
void registerMapSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
} // namespace bytedance::bolt::aggregate::prestosql

namespace bytedance::bolt::aggregate::test {
namespace {

// Nested keys, nested values, single group, intermediate input.
class MapSumAggregateInputTest
    : public testing::TestWithParam<std::tuple<bool, bool, bool, bool>> {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    prestosql::registerMapSumAggregate("input_test_", false, true);
  }

  BufferPtr indices(std::initializer_list<vector_size_t> values) {
    auto buffer = allocateIndices(values.size(), pool_.get());
    std::copy(values.begin(), values.end(), buffer->asMutable<vector_size_t>());
    return buffer;
  }

  VectorPtr dictionary(
      const VectorPtr& base,
      std::initializer_list<vector_size_t> mapping) {
    return BaseVector::wrapInDictionary(
        nullptr, indices(mapping), mapping.size(), base);
  }

  MapVectorPtr makeInput() {
    const auto [nestedKeys, nestedValues, singleGroup, intermediate] =
        GetParam();
    auto keyBase = BaseVector::create(VARCHAR(), 2, pool_.get());
    keyBase->asFlatVector<StringView>()->set(
        0, StringView("metric_zero_total"));
    keyBase->asFlatVector<StringView>()->set(1, StringView("metric_one"));
    auto keys = dictionary(keyBase, {0, 0, 1, 0});
    if (nestedKeys) {
      keys = dictionary(keys, {0, 1, 2, 3});
    }

    auto valueBase = BaseVector::create(BIGINT(), 2, pool_.get());
    valueBase->asFlatVector<int64_t>()->set(0, 0);
    valueBase->asFlatVector<int64_t>()->set(1, 1);
    auto values = dictionary(valueBase, {0, 0, 1, 0});
    if (nestedValues) {
      values = dictionary(values, {0, 1, 2, 3});
    }

    // Row 3 contains {metric_one: 1, metric_zero_total: 0} at element
    // indices 2 and 3. Decoding the children with map row {3} leaves element
    // index 2 undecoded. Four child elements keep this repro in bounds.
    return std::make_shared<MapVector>(
        pool_.get(),
        MAP(VARCHAR(), BIGINT()),
        nullptr,
        4,
        indices({0, 1, 2, 2}),
        indices({1, 1, 0, 2}),
        keys,
        values);
  }

  void checkInput(const VectorPtr& input, const SelectivityVector& rows) {
    const auto [nestedKeys, nestedValues, singleGroup, intermediate] =
        GetParam();
    HashStringAllocator allocator(pool_.get());
    auto aggregate = exec::Aggregate::create(
        "input_test_aggregate_map_sum",
        intermediate ? core::AggregationNode::Step::kFinal
                     : core::AggregationNode::Step::kSingle,
        {input->type()},
        MAP(VARCHAR(), BIGINT()),
        core::QueryConfig({}));
    aggregate->setAllocator(&allocator);
    aggregate->setOffsets(16, 0, 1, 8);
    auto storage = AlignedBuffer::allocate<char>(
        16 + aggregate->accumulatorFixedWidthSize(), pool_.get(), 0);
    char* group = storage->asMutable<char>();
    std::vector<char*> groups(input->size(), group);
    const std::vector<vector_size_t> newGroups{0};
    aggregate->initializeNewGroups(groups.data(), newGroups);

    if (singleGroup) {
      if (intermediate) {
        aggregate->addSingleGroupIntermediateResults(
            group, rows, {input}, false);
      } else {
        aggregate->addSingleGroupRawInput(group, rows, {input}, false);
      }
    } else if (intermediate) {
      aggregate->addIntermediateResults(groups.data(), rows, {input}, false);
    } else {
      aggregate->addRawInput(groups.data(), rows, {input}, false);
    }

    auto result = BaseVector::create(MAP(VARCHAR(), BIGINT()), 1, pool_.get());
    aggregate->extractValues(&group, 1, &result);
    aggregate->destroy(folly::Range<char**>(&group, 1));

    // Compare the whole map, including presence of both keys. The reference
    // map is flat and does not depend on the encoded input's ordering.
    auto expectedKeys = BaseVector::create(VARCHAR(), 2, pool_.get());
    expectedKeys->asFlatVector<StringView>()->set(0, StringView("metric_one"));
    expectedKeys->asFlatVector<StringView>()->set(
        1, StringView("metric_zero_total"));
    auto expectedValues = BaseVector::create(BIGINT(), 2, pool_.get());
    expectedValues->asFlatVector<int64_t>()->set(0, 1);
    expectedValues->asFlatVector<int64_t>()->set(1, 0);
    auto expected = std::make_shared<MapVector>(
        pool_.get(),
        MAP(VARCHAR(), BIGINT()),
        nullptr,
        1,
        indices({0}),
        indices({2}),
        expectedKeys,
        expectedValues);
    EXPECT_TRUE(expected->equalValueAt(result.get(), 0, 0))
        << "Expected " << expected->toString(0) << ", got "
        << result->toString(0);
  }

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::MemoryManager::getInstance()->addLeafPool();
};

TEST_P(MapSumAggregateInputTest, sparseRows) {
  SelectivityVector rows(4, false);
  rows.setValid(3, true);
  rows.updateBounds();
  checkInput(makeInput(), rows);
}

TEST_P(MapSumAggregateInputTest, wrappedMaps) {
  auto input = makeInput();
  checkInput(dictionary(input, {3}), SelectivityVector(1));
  checkInput(BaseVector::wrapInConstant(1, 3, input), SelectivityVector(1));
}

INSTANTIATE_TEST_SUITE_P(
    Encodings,
    MapSumAggregateInputTest,
    testing::Combine(
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool()));

} // namespace
} // namespace bytedance::bolt::aggregate::test
