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

#include <fmt/format.h>
#include <limits>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

class MinMaxByAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    AggregationTestBase::disallowInputShuffle();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(MinMaxByAggregateTest, maxBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({11, 12, 12}),
  })};

  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(std::vector<int32_t>({3})),
  })};

  testAggregations(vectors, {}, {"spark_max_by(c0, c1)"}, expected);
}

TEST_F(MinMaxByAggregateTest, minBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({12, 11, 11}),
  })};

  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(std::vector<int32_t>({3})),
  })};

  testAggregations(vectors, {}, {"spark_min_by(c0, c1)"}, expected);
}

TEST_F(MinMaxByAggregateTest, encodedIntermediateRows) {
  auto wrapField = [&](const VectorPtr& field) {
    auto reversed = BaseVector::wrapInDictionary(
        nullptr, makeIndices({3, 2, 1, 0}), 4, field);
    return BaseVector::wrapInDictionary(
        nullptr, makeIndices({0, 1, 2, 3}), 4, reversed);
  };
  auto intermediate = makeRowVector({
      wrapField(makeFlatVector<int64_t>({10, 20, 30, 40})),
      wrapField(makeFlatVector<int64_t>({1, 2, 3, 4})),
  });

  auto testInput = [&](const VectorPtr& encoded,
                       int64_t fullMin,
                       int64_t fullMax,
                       int64_t selectedMin,
                       int64_t selectedMax) {
    for (bool sparse : {false, true}) {
      auto input = makeRowVector({
          makeFlatVector<int32_t>({0, 0, 0, 0}),
          encoded,
          makeFlatVector<bool>({true, !sparse, !sparse, true}),
      });
      for (bool grouped : {false, true}) {
        SCOPED_TRACE(fmt::format("sparse={}, grouped={}", sparse, grouped));
        const std::vector<std::string> keys = grouped
            ? std::vector<std::string>{"c0"}
            : std::vector<std::string>{};
        // Feed encoded intermediate states directly to final aggregation so
        // partial aggregation or serialization cannot flatten the fields.
        auto plan = exec::test::PlanBuilder()
                        .values({input})
                        .finalAggregation(
                            keys,
                            {"spark_min_by(c1) FILTER (WHERE c2)",
                             "spark_max_by(c1) FILTER (WHERE c2)"},
                            {{BIGINT(), BIGINT()}, {BIGINT(), BIGINT()}})
                        .planNode();
        std::vector<VectorPtr> expected;
        if (grouped) {
          expected.push_back(makeFlatVector<int32_t>({0}));
        }
        expected.push_back(
            makeFlatVector<int64_t>({sparse ? selectedMin : fullMin}));
        expected.push_back(
            makeFlatVector<int64_t>({sparse ? selectedMax : fullMax}));
        exec::test::AssertQueryBuilder(plan).maxDrivers(1).assertResults(
            makeRowVector(expected));
      }
    }
  };

  testInput(intermediate, 10, 40, 10, 40);
  testInput(
      BaseVector::wrapInDictionary(
          nullptr, makeIndices({2, 0, 3, 1}), 4, intermediate),
      10,
      40,
      20,
      30);
  testInput(BaseVector::wrapInConstant(4, 1, intermediate), 30, 30, 30, 30);
  testInput(
      BaseVector::wrapInDictionary(
          makeNulls({true, false, false, false}),
          makeIndices({2, 0, 3, 1}),
          4,
          intermediate),
      10,
      40,
      30,
      30);
}

TEST_F(MinMaxByAggregateTest, decimalCompare) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int128_t>({12345678, 345678, 23456789}, DECIMAL(38, 6)),
  })};

  {
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(std::vector<int32_t>{3}),
    })};
    testAggregations(
        vectors, {}, {"spark_max_by(c0, c1)"}, expected, {}, false);
  }

  {
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(std::vector<int32_t>{2}),
    })};

    testAggregations(
        vectors, {}, {"spark_min_by(c0, c1)"}, expected, {}, false);
  }
}

TEST_F(MinMaxByAggregateTest, arrayCompare) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeNullableArrayVector<int64_t>({
          {4, 5},
          {std::nullopt, 2},
          {6, 7, 8},
      }),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {4, 5},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"spark_min_by(c0, c1)", "spark_max_by(c0, c1)"}, {expected});

  data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {3, std::nullopt, 4},
          {6, 7, 8},
      }),
  });

  expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"spark_min_by(c0, c1)", "spark_max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByAggregateTest, mapCompare) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({}),
      makeNullableMapVector<int64_t, int64_t>({}),
  });
  std::vector<RowVectorPtr> expected = {};

  BOLT_ASSERT_USER_THROW(
      testAggregations({data}, {}, {"spark_min_by(c0, c1)"}, expected),
      "Aggregate function signature is not supported");

  BOLT_ASSERT_USER_THROW(
      testAggregations({data}, {}, {"spark_max_by(c0, c1)"}, expected),
      "Aggregate function signature is not supported");
}

TEST_F(MinMaxByAggregateTest, rowCompare) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeRowVector({makeNullableFlatVector<int32_t>({
          1,
          std::nullopt,
          3,
      })}),
  });

  auto expected = makeRowVector({
      makeArrayVector<int64_t>({
          {4, 5},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"spark_min_by(c0, c1)", "spark_max_by(c0, c1)"}, {expected});

  data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8},
      }),
      makeRowVector({makeNullableFlatVector<int32_t>({
          1,
          2,
          3,
      })}),
  });

  expected = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
      }),
      makeArrayVector<int64_t>({
          {6, 7, 8},
      }),
  });

  testAggregations(
      {data}, {}, {"spark_min_by(c0, c1)", "spark_max_by(c0, c1)"}, {expected});
}

TEST_F(MinMaxByAggregateTest, nan) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<double>({1, -1, std::numeric_limits<double>::quiet_NaN()}),
  })};

  {
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(std::vector<int32_t>({3})),
    })};

    testAggregations(vectors, {}, {"spark_max_by(c0, c1)"}, expected);
  }

  {
    auto expected = {makeRowVector({
        makeFlatVector<int32_t>(std::vector<int32_t>({2})),
    })};

    testAggregations(vectors, {}, {"spark_min_by(c0, c1)"}, expected);
  }
}

} // namespace
} // namespace bytedance::bolt::functions::aggregate::sparksql::test
