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
#include "bolt/functions/sparksql/aggregates/BitmapUtil.h"
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

class BitmapOrAggAggregateTest : public aggregate::test::AggregationTestBase {
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

TEST_F(BitmapOrAggAggregateTest, singleBitmap) {
  auto bitmap = makeBitmap({3, 7, 15});
  auto vectors = {makeRowVector({makeBitmapVector(bitmap)})};
  auto expected = {makeRowVector({makeBitmapVector(bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, multipleDisjoint) {
  auto b1 = makeBitmap({0, 1});
  auto b2 = makeBitmap({100, 200});
  auto expected_bitmap = makeBitmap({0, 1, 100, 200});
  std::vector<std::string> bitmaps = {b1, b2};
  auto vectors = {makeRowVector({makeFlatVector<StringView>(
      2,
      [&](vector_size_t row) { return StringView(bitmaps[row]); },
      nullptr,
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(expected_bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, overlappingBits) {
  auto b1 = makeBitmap({1, 2, 3});
  auto b2 = makeBitmap({3, 4, 5});
  auto expected_bitmap = makeBitmap({1, 2, 3, 4, 5});
  std::vector<std::string> bitmaps = {b1, b2};
  auto vectors = {makeRowVector({makeFlatVector<StringView>(
      2,
      [&](vector_size_t row) { return StringView(bitmaps[row]); },
      nullptr,
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(expected_bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, duplicateBitmaps) {
  auto bitmap = makeBitmap({42, 99});
  std::vector<std::string> bitmaps = {bitmap, bitmap, bitmap};
  auto vectors = {makeRowVector({makeFlatVector<StringView>(
      3,
      [&](vector_size_t row) { return StringView(bitmaps[row]); },
      nullptr,
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, emptyInput) {
  auto vectors = {
      makeRowVector({BaseVector::create(VARBINARY(), 0, pool_.get())})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({}))})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, nullInputs) {
  auto b1 = makeBitmap({10});
  auto b3 = makeBitmap({30});
  auto expected_bitmap = makeBitmap({10, 30});
  std::vector<std::string> bitmaps = {b1, "", b3};
  auto vectors = {makeRowVector({makeNullableFlatVector<StringView>(
      {StringView(bitmaps[0]), std::nullopt, StringView(bitmaps[2])},
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(expected_bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, allNullInputs) {
  auto nullVector = BaseVector::createNullConstant(VARBINARY(), 5, pool_.get());
  auto vectors = {makeRowVector({nullVector})};
  auto expected = {makeRowVector({makeBitmapVector(makeBitmap({}))})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, groupBy) {
  auto g1_b1 = makeBitmap({1, 3});
  auto g1_b2 = makeBitmap({5});
  auto g2_b1 = makeBitmap({7});
  auto g2_b2 = makeBitmap({9, 11});
  auto expected_g1 = makeBitmap({1, 3, 5});
  auto expected_g2 = makeBitmap({7, 9, 11});
  std::vector<std::string> bitmaps = {g1_b1, g2_b1, g1_b2, g2_b2};
  std::vector<std::string> expected_bitmaps = {expected_g1, expected_g2};
  auto vectors = {makeRowVector(
      {makeFlatVector<int64_t>({0, 1, 0, 1}),
       makeFlatVector<StringView>(
           4,
           [&](vector_size_t row) { return StringView(bitmaps[row]); },
           nullptr,
           VARBINARY())})};
  auto expected = {makeRowVector(
      {makeFlatVector<int64_t>({0, 1}),
       makeFlatVector<StringView>(
           2,
           [&](vector_size_t row) { return StringView(expected_bitmaps[row]); },
           nullptr,
           VARBINARY())})};
  testAggregations(vectors, {"c0"}, {"bitmap_or_agg(c1)"}, expected);
}

// ---- Interoperation with bitmap_construct_agg ----

// bitmap_or_agg consumes bitmaps in the same 4096-byte format that
// bitmap_construct_agg produces. makeBitmap uses the identical layout.
TEST_F(BitmapOrAggAggregateTest, interopWithConstruct) {
  // Simulate two bitmaps produced by bitmap_construct_agg.
  auto b1 = makeBitmap({1, 2, 3});
  auto b2 = makeBitmap({4, 5, 6});
  auto expected_or = makeBitmap({1, 2, 3, 4, 5, 6});
  std::vector<std::string> bitmaps = {b1, b2};
  auto vectors = {makeRowVector({makeFlatVector<StringView>(
      2,
      [&](vector_size_t row) { return StringView(bitmaps[row]); },
      nullptr,
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(expected_or)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

// ---- Error cases ----

TEST_F(BitmapOrAggAggregateTest, invalidSize) {
  // the streaming path requires at least 2 rows to execute.
  std::string shortBitmap(100, '\0');
  auto vectors = {makeRowVector({makeNullableFlatVector<StringView>(
      {StringView(shortBitmap), StringView(shortBitmap)}, VARBINARY())})};
  testFailingAggregations(
      vectors, {}, {"bitmap_or_agg(c0)"}, "Input bitmap must be 4096 bytes");
}

// ---- Merge / round-trip ----

TEST_F(BitmapOrAggAggregateTest, mergePartialBitmaps) {
  auto b1 = makeBitmap({0, 1, 10});
  auto b2 = makeBitmap({10, 20, 30});
  auto expected_bitmap = makeBitmap({0, 1, 10, 20, 30});
  std::vector<std::string> bitmaps1 = {b1};
  std::vector<std::string> bitmaps2 = {b2};
  auto batch1 = makeRowVector({makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps1[0]); },
      nullptr,
      VARBINARY())});
  auto batch2 = makeRowVector({makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps2[0]); },
      nullptr,
      VARBINARY())});
  auto vectors = {batch1, batch2};
  auto expected = {makeRowVector({makeBitmapVector(expected_bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, mergeAcrossSimdBoundaries) {
  // Construct bitmaps with bits set near SIMD batch boundaries.
  auto b1 = makeBitmap({0, 127, 2811});
  auto b2 = makeBitmap({128, 32767});
  auto expected_bitmap = makeBitmap({0, 127, 128, 2811, 32767});
  std::vector<std::string> bitmaps = {b1, b2};
  auto vectors = {makeRowVector({makeFlatVector<StringView>(
      2,
      [&](vector_size_t row) { return StringView(bitmaps[row]); },
      nullptr,
      VARBINARY())})};
  auto expected = {makeRowVector({makeBitmapVector(expected_bitmap)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

TEST_F(BitmapOrAggAggregateTest, roundTripSerializeMerge) {
  auto b1 = makeBitmap({0, 1, 2});
  auto b2 = makeBitmap({7, 8});
  std::vector<std::string> bitmaps1 = {b1};
  std::vector<std::string> bitmaps2 = {b2};
  auto rawInput1 = std::vector<VectorPtr>{makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps1[0]); },
      nullptr,
      VARBINARY())};
  auto rawInput2 = std::vector<VectorPtr>{makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps2[0]); },
      nullptr,
      VARBINARY())};
  auto result = testStreaming("bitmap_or_agg", true, rawInput1, rawInput2);
  ::bytedance::bolt::test::assertEqualVectors(
      makeBitmapVector(makeBitmap({0, 1, 2, 7, 8})), result);
}

TEST_F(BitmapOrAggAggregateTest, emptyInputThenMerge) {
  auto b1 = makeBitmap({});
  auto b2 = makeBitmap({100, 200});
  std::vector<std::string> bitmaps1 = {b1};
  std::vector<std::string> bitmaps2 = {b2};
  auto batch1 = makeRowVector({makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps1[0]); },
      nullptr,
      VARBINARY())});
  auto batch2 = makeRowVector({makeFlatVector<StringView>(
      1,
      [&](vector_size_t) { return StringView(bitmaps2[0]); },
      nullptr,
      VARBINARY())});
  auto vectors = {batch1, batch2};
  auto expected = {makeRowVector({makeBitmapVector(b2)})};
  testAggregations(vectors, {}, {"bitmap_or_agg(c0)"}, expected);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql::test
