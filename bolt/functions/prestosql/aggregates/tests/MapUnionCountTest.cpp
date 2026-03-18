/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
using bytedance::bolt::test::emptyArray;
namespace bytedance::bolt::aggregate::test {

namespace {

class MapUnionCountTest : public AggregationTestBase {};

TEST_F(MapUnionCountTest, global) {
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          emptyArray, // empty map
          std::nullopt, // null map
          {{{1, 10}, {2, 20}}},
          {{{1, 11}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {{1, 3}, {2, 1}, {3, 2}, {4, 1}, {5, 1}},
      }),
  });

  testAggregations({data}, {}, {"map_union_count(c0)"}, {expected});
}

TEST_F(MapUnionCountTest, nullAndEmptyMaps) {
  auto allEmptyMaps = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {},
          {},
          {},
      }),
  });

  auto expectedEmpty = makeRowVector({
      makeMapVector<int64_t, int64_t>({
          {},
      }),
  });

  testAggregations(
      {allEmptyMaps}, {}, {"map_union_count(c0)"}, {expectedEmpty});

  auto allNullMaps = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
          std::nullopt,
          std::nullopt,
      }),
  });

  auto expectedNull = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
      }),
  });

  testAggregations({allNullMaps}, {}, {"map_union_count(c0)"}, {expectedNull});

  auto emptyAndNullMaps = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          std::nullopt,
          emptyArray,
          std::nullopt,
          emptyArray,
      }),
  });

  testAggregations(
      {emptyAndNullMaps}, {}, {"map_union_count(c0)"}, {expectedEmpty});
}

TEST_F(MapUnionCountTest, groupBy) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1}),
      makeNullableMapVector<int64_t, int64_t>({
          {}, // empty map
          std::nullopt, // null map
          {{{1, 10}, {2, 20}}},
          {{{1, 11}, {3, 30}, {4, 40}}},
          {{{3, 30}, {5, 50}, {1, 12}}},
      }),
  });

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeMapVector<int64_t, int64_t>({
          {{1, 2}, {2, 1}, {3, 1}, {5, 1}},
          {{1, 1}, {3, 1}, {4, 1}},
      }),
  });

  testAggregations({data}, {"c0"}, {"map_union_count(c1)"}, {expected});
}

} // namespace
} // namespace bytedance::bolt::aggregate::test
