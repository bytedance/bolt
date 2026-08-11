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

#include <cstring>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/BitmapConstructAggAggregate.h"
#include "bolt/functions/sparksql/aggregates/Register.h"

namespace bytedance::bolt::functions::aggregate::sparksql::test {

namespace {

std::string makeBitmap(const std::vector<int64_t>& positions) {
  std::string bitmap(kBitmapNumBytes, '\0');
  for (auto pos : positions) {
    int32_t byteIdx = static_cast<int32_t>(pos / 8);
    int32_t bitIdx = static_cast<int32_t>(pos % 8);
    reinterpret_cast<uint8_t*>(bitmap.data())[byteIdx] |=
        static_cast<uint8_t>(1 << bitIdx);
  }
  return bitmap;
}

class BitmapConstructAggAggregateTest
    : public aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
    allowInputShuffle();
  }

  VectorPtr makeBitmapVector(const std::string& bitmap) {
    return makeConstant(StringView(bitmap), 1, VARBINARY());
  }
};

} // namespace

// ---- Basic correctness ----

TEST_F(BitmapConstructAggAggregateTest, singlePosition) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(1, [](vector_size_t) { return 5; })})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({5}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, multiplePositions) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(3, [](vector_size_t row) { return row; })})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({0, 1, 2}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, duplicatePositions) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(3, [](vector_size_t) { return 7; })})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({7}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, emptyInput) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>({})})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, nullInputs) {
  auto vectors = {makeRowVector({makeNullableFlatVector<int64_t>(
      {1, std::nullopt, 3, std::nullopt, 5})})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({1, 3, 5}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, allNullInputs) {
  auto vectors = {makeRowVector({makeAllNullFlatVector<int64_t>(5)})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, groupBy) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({0, 0, 1, 1, 2, 2}),
       makeFlatVector<int64_t>({1, 3, 1, 5, 7, 7})})};
  std::vector<std::string> bitmaps = {
      makeBitmap({1, 3}), makeBitmap({1, 5}), makeBitmap({7})};
  auto expected = {makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 2}),
       makeFlatVector<StringView>(
           3,
           [&](vector_size_t row) { return StringView(bitmaps[row]); },
           nullptr,
           VARBINARY())})};
  testAggregations(vectors, {"c0"}, {"bitmap_construct_agg(c1)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, highPosition) {
  int64_t pos = kBitmapNumBits - 1; // 32767
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(1, [&](vector_size_t) { return pos; })})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({pos}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, positionZero) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>(1, [](vector_size_t) { return 0; })})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({0}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

// ---- Error cases ----

TEST_F(BitmapConstructAggAggregateTest, invalidNegativePosition) {
  auto vectors = {makeRowVector({makeFlatVector<int64_t>({0, -1, 2})})};
  testFailingAggregations(
      vectors, {}, {"bitmap_construct_agg(c0)"}, "Invalid bitmap position");
}

TEST_F(BitmapConstructAggAggregateTest, invalidTooLargePosition) {
  auto vectors = {
      makeRowVector({makeFlatVector<int64_t>({0, kBitmapNumBits, 2})})};
  testFailingAggregations(
      vectors, {}, {"bitmap_construct_agg(c0)"}, "Invalid bitmap position");
}

// ---- Merge / round-trip ----

TEST_F(BitmapConstructAggAggregateTest, mergePartialBitmaps) {
  auto batch1 = makeRowVector(
      {makeFlatVector<int64_t>(2, [](vector_size_t r) { return r; })});
  auto batch2 = makeRowVector(
      {makeFlatVector<int64_t>(2, [](vector_size_t r) { return r + 2; })});
  auto vectors = {batch1, batch2};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({0, 1, 2, 3}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, mergeAcrossSimdBoundaries) {
  // Positions spanning SIMD batch boundaries (NEON batch = 16 bytes).
  auto batch1 = makeRowVector({makeFlatVector<int64_t>({7, 127, 2811})});
  auto batch2 = makeRowVector({makeFlatVector<int64_t>({128, 32767})});
  auto vectors = {batch1, batch2};
  auto expected = {makeRowVector(
      {makeBitmapVector(makeBitmap({7, 127, 128, 2811, 32767}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

TEST_F(BitmapConstructAggAggregateTest, roundTripSerializeMerge) {
  auto rawInput1 = std::vector<VectorPtr>{
      makeFlatVector<int64_t>(3, [](vector_size_t r) { return r; })};
  auto rawInput2 = std::vector<VectorPtr>{
      makeFlatVector<int64_t>(2, [](vector_size_t r) { return r + 7; })};
  auto result =
      testStreaming("bitmap_construct_agg", true, rawInput1, rawInput2);
  ::bytedance::bolt::test::assertEqualVectors(
      makeBitmapVector(makeBitmap({0, 1, 2, 7, 8})), result);
}

TEST_F(BitmapConstructAggAggregateTest, emptyInputThenMerge) {
  auto batch1 = makeRowVector({makeFlatVector<int64_t>({})});
  auto batch2 = makeRowVector(
      {makeFlatVector<int64_t>(2, [](vector_size_t r) { return r * 100; })});
  auto vectors = {batch1, batch2};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({0, 100}))})};
  testAggregations(vectors, {}, {"bitmap_construct_agg(c0)"}, expected);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql::test
