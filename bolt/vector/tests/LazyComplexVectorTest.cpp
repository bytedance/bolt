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
#include "bolt/vector/LazyComplexVector.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/row/CompactRowLazyCodec.h"
#include "bolt/vector/LazyComplexCodec.h"
#include "bolt/vector/tests/utils/ScopedActiveLazyFormat.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::test {
namespace {

class LazyComplexVectorTest : public testing::Test, public VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(LazyComplexVectorTest, encodingAndType) {
  // FlatVector<StringView> requires values_ || nulls_; allocate a values
  // buffer.
  auto valuesBuf = AlignedBuffer::allocate<StringView>(1, pool());
  auto bytesBuf = AlignedBuffer::allocate<char>(4, pool());
  auto flat = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      /*nulls*/ nullptr,
      /*length*/ 0,
      /*values*/ valuesBuf,
      std::vector<BufferPtr>{bytesBuf});
  auto lazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), flat);
  EXPECT_EQ(lazy->encoding(), VectorEncoding::Simple::LAZY_COMPLEX);
  EXPECT_TRUE(lazy->type()->equivalent(*ARRAY(BIGINT())));
}

TEST_F(LazyComplexVectorTest, asComplexReturnsNull) {
  auto flat = makeFlatVector<StringView>({});
  auto lazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), flat);
  EXPECT_EQ(lazy->as<RowVector>(), nullptr);
  EXPECT_EQ(lazy->as<ArrayVector>(), nullptr);
  EXPECT_EQ(lazy->as<MapVector>(), nullptr);
  EXPECT_EQ(lazy->as<FlatVector<StringView>>(), nullptr);
}

TEST_F(LazyComplexVectorTest, hashCompareThrow) {
  auto flat = makeFlatVector<StringView>({});
  auto lazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), flat);
  EXPECT_THROW((void)lazy->hashValueAt(0), BoltException);
  EXPECT_THROW(
      (void)lazy->compare(lazy.get(), 0, 0, CompareFlags{}), BoltException);
}

TEST_F(LazyComplexVectorTest, toStringPlaceholder) {
  auto flat = makeFlatVector<StringView>({StringView("hello")});
  auto lazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), flat);
  EXPECT_NE(lazy->toString(0).find("<lazy"), std::string::npos);
}

TEST_F(LazyComplexVectorTest, encodeDecodeRoundTrip) {
  ScopedActiveLazyFormat codec("compact_row");
  auto original = makeArrayVector<int64_t>({{1, 2, 3}, {}, {4, 5}});
  auto* activeCodec = LazyComplexCodec::activeCodec();
  ASSERT_NE(activeCodec, nullptr);
  auto lazy = activeCodec->encode(original, pool());
  ASSERT_EQ(lazy->encoding(), VectorEncoding::Simple::LAZY_COMPLEX);
  ASSERT_EQ(lazy->size(), original->size());
  SelectivityVector all(lazy->size());
  auto decoded = lazy->decode(all, pool());
  assertEqualVectors(original, decoded);
}

TEST_F(LazyComplexVectorTest, encodeDecodeWithNulls) {
  ScopedActiveLazyFormat codec("compact_row");
  auto original = makeNullableArrayVector<int64_t>(
      {std::nullopt, {{1, 2}}, std::nullopt, {{}}});
  auto* activeCodec = LazyComplexCodec::activeCodec();
  ASSERT_NE(activeCodec, nullptr);
  auto lazy = activeCodec->encode(original, pool());
  ASSERT_EQ(lazy->encoding(), VectorEncoding::Simple::LAZY_COMPLEX);
  SelectivityVector all(lazy->size());
  auto decoded = lazy->decode(all, pool());
  assertEqualVectors(original, decoded);
}

TEST_F(LazyComplexVectorTest, copyRangesLazyToLazy) {
  // NestedLoopJoin-style copy: bytewise copy between two LazyComplexVectors
  // of the same original type. Both source + target must be lazy; the inner
  // FlatVector<StringView>'s copyRanges handles the actual byte copy.
  ScopedActiveLazyFormat scopedCodec("compact_row");

  // Build source lazy vector from real data.
  row::CompactRowLazyCodec codec;
  auto srcOriginal = makeArrayVector<int64_t>({{1, 2, 3}, {}, {4, 5}, {6}});
  auto srcLazy = codec.encode(srcOriginal, pool());
  ASSERT_EQ(srcLazy->size(), 4);

  // Build empty target lazy vector of the same type, size 6. Values must be
  // default-initialised — pool memory can come back recycled with garbage
  // that downstream copyRanges / decode would interpret as out-of-line
  // StringView pointers.
  const vector_size_t targetSize = 6;
  auto targetValues = AlignedBuffer::allocate<StringView>(
      targetSize, pool(), std::optional<StringView>{StringView{}});
  auto targetFlat = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      /*nulls=*/nullptr,
      targetSize,
      targetValues,
      std::vector<BufferPtr>{});
  auto targetLazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), targetFlat);

  // Copy source rows [0, 3) into target rows [2, 5).
  BaseVector::CopyRange range{
      /*sourceIndex=*/0, /*targetIndex=*/2, /*count=*/3};
  targetLazy->copyRanges(
      srcLazy.get(), folly::Range<const BaseVector::CopyRange*>(&range, 1));

  // Verify byte-level match at copied positions.
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(targetLazy->valueAt(i + 2), srcLazy->valueAt(i))
        << "byte mismatch at target row " << (i + 2);
  }

  // Decode-then-compare: decoded target [2, 5) should match decoded source
  // [0, 3). Confirms the bytes actually round-trip. Rows outside [2, 5) are
  // uninitialized StringViews — feeding them to the decoder reads garbage,
  // so restrict the SelectivityVector to the copied range.
  SelectivityVector copiedRows(targetSize, false);
  copiedRows.setValidRange(2, 5, true);
  copiedRows.updateBounds();
  auto decodedTarget = targetLazy->decode(copiedRows, pool());

  SelectivityVector allSrc(srcLazy->size());
  auto decodedSrc = srcLazy->decode(allSrc, pool());

  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(decodedTarget->equalValueAt(decodedSrc.get(), i + 2, i))
        << "decoded mismatch at target row " << (i + 2);
  }
}

TEST_F(LazyComplexVectorTest, decodedVectorThroughDictionaryOverLazy) {
  // Spark shuffle reproduces this shape: a DictionaryVector wraps a
  // LazyComplexVector. DecodedVector::combineWrappers must walk through
  // the lazy wrapper to its inner FlatVector<StringView>; otherwise it
  // hits "Unsupported vector encoding".
  ScopedActiveLazyFormat scopedCodec("compact_row");

  row::CompactRowLazyCodec codec;
  auto original = makeArrayVector<int64_t>({{1, 2, 3}, {}, {4, 5}, {6}});
  auto lazy = codec.encode(original, pool());

  // Build dictionary indices that pick rows [3, 0, 2] from the lazy bytes.
  const std::vector<vector_size_t> picks{3, 0, 2};
  auto indices = AlignedBuffer::allocate<vector_size_t>(picks.size(), pool());
  std::memcpy(
      indices->asMutable<vector_size_t>(),
      picks.data(),
      sizeof(vector_size_t) * picks.size());
  auto dict = BaseVector::wrapInDictionary(
      /*nulls=*/nullptr, indices, picks.size(), VectorPtr(lazy));

  // Decode through the dictionary; the inner FlatVector<StringView> bytes
  // are exposed via the dictionary's index mapping.
  SelectivityVector rows(picks.size());
  DecodedVector decoded;
  decoded.decode(*dict, rows, /*loadLazy=*/true);

  ASSERT_EQ(decoded.base()->encoding(), VectorEncoding::Simple::FLAT);
  ASSERT_EQ(decoded.base()->typeKind(), TypeKind::VARBINARY);
  const auto* baseFlat = decoded.base()->as<FlatVector<StringView>>();
  ASSERT_NE(baseFlat, nullptr);
  for (vector_size_t i = 0; i < static_cast<vector_size_t>(picks.size()); ++i) {
    EXPECT_EQ(baseFlat->valueAt(decoded.index(i)), lazy->valueAt(picks[i]))
        << "byte mismatch at picked row " << i;
  }
}

TEST_F(LazyComplexVectorTest, copyRangesFromNonLazyThrows) {
  ScopedActiveLazyFormat scopedCodec("compact_row");

  auto targetValues = AlignedBuffer::allocate<StringView>(2, pool());
  auto targetFlat = std::make_shared<FlatVector<StringView>>(
      pool(),
      VARBINARY(),
      /*nulls=*/nullptr,
      2,
      targetValues,
      std::vector<BufferPtr>{});
  auto targetLazy =
      std::make_shared<LazyComplexVector>(pool(), ARRAY(BIGINT()), targetFlat);

  // Regular ArrayVector source — should be rejected loudly.
  auto regular = makeArrayVector<int64_t>({{1, 2}, {3}});
  BaseVector::CopyRange range{0, 0, 2};
  EXPECT_THROW(
      targetLazy->copyRanges(
          regular.get(), folly::Range<const BaseVector::CopyRange*>(&range, 1)),
      std::exception);
}

} // namespace
} // namespace bytedance::bolt::test
