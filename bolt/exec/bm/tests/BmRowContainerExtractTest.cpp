#include "bolt/exec/bm/tests/BmRowContainerTestUtil.h"

namespace bytedance::bolt::exec {
namespace {

TEST_F(BmRowContainerTest, PreservesNullsInFixedWidthColumn) {
  auto bufferManager = makeBufferManager("nulls");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, std::nullopt, 33});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  rows.reserve(input->size());
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(BIGINT(), input->size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, result);

  auto* actual = result->asFlatVector<int64_t>();
  EXPECT_FALSE(result->isNullAt(0));
  EXPECT_EQ(11, actual->valueAt(0));
  EXPECT_TRUE(result->isNullAt(1));
  EXPECT_FALSE(result->isNullAt(2));
  EXPECT_EQ(33, actual->valueAt(2));
}


TEST_F(BmRowContainerTest, ExtractsNulls) {
  auto bufferManager = makeBufferManager("extract-nulls");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, std::nullopt, 33});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto nulls = allocateNulls(rows.size(), leaf_.get());
  container.extractNulls(rows.data(), rows.size(), 0, nulls);
  const auto* rawNulls = nulls->as<uint64_t>();
  EXPECT_FALSE(bits::isBitSet(rawNulls, 0));
  EXPECT_TRUE(bits::isBitSet(rawNulls, 1));
  EXPECT_FALSE(bits::isBitSet(rawNulls, 2));
}


TEST_F(BmRowContainerTest, ExtractsColumnAtResultOffset) {
  auto bufferManager = makeBufferManager("extract-offset");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {11, 22});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(BIGINT(), 4, leaf_.get());
  container.extractColumn(
      folly::Range<const RowId*>(rows.data(), rows.size()),
      0,
      1,
      result);

  auto* actual = result->asFlatVector<int64_t>();
  EXPECT_EQ(11, actual->valueAt(1));
  EXPECT_EQ(22, actual->valueAt(2));
}


TEST_F(BmRowContainerTest, ExtractsRowsAcrossMultipleBlocks) {
  auto bufferManager = makeBufferManager("multi-block");
  BmRowContainer container({BIGINT()}, {}, bufferManager);

  auto input = makeBigintVector(leaf_.get(), {7});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.allocatedBytes() < 2 * kLargeBlockBytes) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() || row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }

  ASSERT_GE(sampledRows.size(), 2);
  auto result = BaseVector::create(BIGINT(), sampledRows.size(), leaf_.get());
  container.extractColumn(sampledRows.data(), sampledRows.size(), 0, result);

  auto* actual = result->asFlatVector<int64_t>();
  for (auto i = 0; i < sampledRows.size(); ++i) {
    EXPECT_FALSE(result->isNullAt(i));
    EXPECT_EQ(7, actual->valueAt(i));
  }
}


TEST_F(BmRowContainerTest, StoresExtractsAndComparesVariableWidthColumns) {
  auto bufferManager = makeBufferManager("varchar");
  BmRowContainer container({VARCHAR()}, {}, bufferManager);

  const std::string largeValue(128, 'x');
  auto input = makeVarcharVector(
      leaf_.get(),
      {StringView("abc"),
       StringView("abcd"),
       std::nullopt,
       StringView(largeValue)});
  DecodedVector decoded(*input);

  std::vector<RowId> rows;
  for (auto i = 0; i < input->size(); ++i) {
    auto row = container.newRow();
    container.store(decoded, i, row, 0);
    rows.push_back(row);
  }

  auto result = BaseVector::create(VARCHAR(), rows.size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, result);

  auto* actual = result->asFlatVector<StringView>();
  EXPECT_EQ("abc", actual->valueAt(0).getString());
  EXPECT_EQ("abcd", actual->valueAt(1).getString());
  EXPECT_TRUE(result->isNullAt(2));
  EXPECT_EQ(largeValue, actual->valueAt(3).getString());

  EXPECT_LT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_GT(container.compare(rows[1], rows[0], 0), 0);
  EXPECT_LT(container.compare(rows[2], rows[0], 0), 0);
}


TEST_F(BmRowContainerTest, StoresVariableWidthDataAcrossMultipleHeapBlocks) {
  auto bufferManager = makeBufferManager("multi-heap-block");
  BmRowContainer container({VARCHAR()}, {}, bufferManager);

  const std::string payload(1024, 'z');
  auto input = makeVarcharVector(leaf_.get(), {StringView(payload)});
  DecodedVector decoded(*input);

  std::vector<RowId> sampledRows;
  while (container.heapAllocatedBytes() < 2 * kLargeBlockBytes) {
    auto row = container.newRow();
    container.store(decoded, 0, row, 0);
    if (sampledRows.empty() ||
        row.blockId != sampledRows.back().blockId) {
      sampledRows.push_back(row);
    }
  }

  ASSERT_GE(container.heapAllocatedBytes(), 2 * kLargeBlockBytes);
  ASSERT_FALSE(sampledRows.empty());
  auto result = BaseVector::create(VARCHAR(), sampledRows.size(), leaf_.get());
  container.extractColumn(sampledRows.data(), sampledRows.size(), 0, result);

  auto* actual = result->asFlatVector<StringView>();
  for (auto i = 0; i < sampledRows.size(); ++i) {
    EXPECT_EQ(payload, actual->valueAt(i).getString());
  }
}


TEST_F(BmRowContainerTest, ExtractsVariableWidthWithExactSizeAtResultOffset) {
  auto bufferManager = makeBufferManager("varchar-exact-size-offset");
  BmRowContainer container({BIGINT()}, {VARCHAR()}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 30, 40});
  const std::string largeValue(256, 'e');
  auto strings = makeVarcharVector(
      leaf_.get(),
      {StringView("aa"),
       StringView(""),
       std::nullopt,
       StringView(largeValue)});
  DecodedVector decodedBigints(*bigints);
  DecodedVector decodedStrings(*strings);

  std::vector<RowId> rows;
  for (auto i = 0; i < bigints->size(); ++i) {
    auto row = container.newRow();
    container.store(decodedBigints, i, row, 0);
    container.store(decodedStrings, i, row, 1);
    rows.push_back(row);
  }

  auto result = BaseVector::create(VARCHAR(), rows.size() + 2, leaf_.get());
  container.extractColumn(
      folly::Range<const RowId*>(rows.data(), rows.size()),
      1,
      1,
      result,
      true);

  auto* actual = result->asFlatVector<StringView>();
  EXPECT_EQ("aa", actual->valueAt(1).getString());
  EXPECT_EQ("", actual->valueAt(2).getString());
  EXPECT_TRUE(result->isNullAt(3));
  EXPECT_EQ(largeValue, actual->valueAt(4).getString());
}


} // namespace
} // namespace bytedance::bolt::exec
