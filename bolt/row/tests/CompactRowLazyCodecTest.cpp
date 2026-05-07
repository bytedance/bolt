/*
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
#include "bolt/row/CompactRowLazyCodec.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using bytedance::bolt::test::assertEqualVectors;

namespace bytedance::bolt::row::test {
namespace {

class CompactRowLazyCodecTest : public testing::Test,
                                public bolt::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  const CompactRowLazyCodec codec_;

  void assertRoundTrip(const VectorPtr& input) {
    auto lazy = codec_.encode(input, pool());
    ASSERT_EQ(lazy->size(), input->size());
    ASSERT_EQ(lazy->encoding(), VectorEncoding::Simple::LAZY_COMPLEX);
    SelectivityVector all(input->size());
    auto decoded = codec_.decode(*lazy, all, pool());
    assertEqualVectors(input, decoded);
  }
};

TEST_F(CompactRowLazyCodecTest, arrayBigint) {
  auto v = makeArrayVector<int64_t>({{1, 2, 3}, {}, {4, 5}, {}, {6, 7, 8, 9}});
  assertRoundTrip(v);
}

TEST_F(CompactRowLazyCodecTest, mapVarcharArrayReal) {
  auto v = makeMapVector<StringView, float>(
      {{{StringView("a"), 1.0f}, {StringView("b"), 2.0f}},
       {{StringView("c"), 3.0f}}});
  assertRoundTrip(v);
}

TEST_F(CompactRowLazyCodecTest, rowNested) {
  auto inner = makeArrayVector<int64_t>({{1, 2}, {3}, {}});
  auto v = makeRowVector({makeFlatVector<int64_t>({10, 20, 30}), inner});
  assertRoundTrip(v);
}

TEST_F(CompactRowLazyCodecTest, nullsSparseAndAll) {
  auto v = makeNullableArrayVector<int64_t>(
      {std::nullopt, {{1, 2}}, std::nullopt, {{}}});
  assertRoundTrip(v);
}

TEST_F(CompactRowLazyCodecTest, emptyBatch) {
  auto v = makeArrayVector<int64_t>(std::vector<std::vector<int64_t>>{});
  ASSERT_EQ(v->size(), 0);
  assertRoundTrip(v);
}

TEST_F(CompactRowLazyCodecTest, encodeToLazyIdempotentOnLazyInput) {
  auto v = makeArrayVector<int64_t>({{1, 2}, {3}});
  auto lazy = codec_.encode(v, pool());
  auto again = encodeToLazy(lazy, pool(), codec_);
  EXPECT_EQ(lazy.get(), again.get()); // zero-encode fast path
}

TEST_F(CompactRowLazyCodecTest, encodeToLazyRejectsPrimitive) {
  auto v = makeFlatVector<int64_t>({1, 2, 3});
  EXPECT_THROW(encodeToLazy(v, pool(), codec_), BoltException);
}

} // namespace
} // namespace bytedance::bolt::row::test
