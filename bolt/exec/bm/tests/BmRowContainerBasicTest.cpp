#include "bolt/exec/bm/tests/BmRowContainerTestUtil.h"

namespace bytedance::bolt::exec {
namespace {

TEST_F(BmRowContainerTest, RequiresBufferManager) {
  EXPECT_THROW(
      BmRowContainer({BIGINT()}, {}, nullptr), std::invalid_argument);
}


TEST_F(BmRowContainerTest, ExposesRequiredAccessorsAndClear) {
  auto bufferManager = makeBufferManager("accessors");
  BmRowContainer container({BIGINT()}, {INTEGER()}, bufferManager);

  EXPECT_EQ(0, container.numRows());
  EXPECT_GT(container.fixedRowSize(), 0);
  EXPECT_EQ(0, container.allocatedBytes());
  EXPECT_EQ(0, container.usedBytes());
  EXPECT_EQ(std::nullopt, container.estimateRowSize());
  EXPECT_EQ(2, container.columns().size());
  EXPECT_EQ(2, container.columnTypes().size());
  EXPECT_EQ(1, container.keyTypes().size());

  auto input = makeBigintVector(leaf_.get(), {11});
  DecodedVector decoded(*input);
  auto row = container.newRow();
  container.store(decoded, 0, row, 0);

  EXPECT_EQ(1, container.numRows());
  ASSERT_TRUE(container.estimateRowSize().has_value());
  EXPECT_GT(container.estimateRowSize().value(), 0);

  container.clear();
  EXPECT_EQ(0, container.numRows());
  EXPECT_EQ(0, container.allocatedBytes());
  EXPECT_EQ(0, container.usedBytes());
  EXPECT_EQ(std::nullopt, container.estimateRowSize());
}


TEST_F(BmRowContainerTest, StoresRowVectorBatch) {
  auto bufferManager = makeBufferManager("row-vector-store");
  BmRowContainer container({BIGINT()}, {VARCHAR()}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 30});
  auto strings = makeVarcharVector(
      leaf_.get(), {StringView("aa"), std::nullopt, StringView("cccc")});
  auto input =
      makeRowVector(leaf_.get(), {"c0", "c1"}, {bigints, strings});

  auto rows = container.store(input);
  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), leaf_.get());
  auto stringResult = BaseVector::create(VARCHAR(), rows.size(), leaf_.get());
  container.extractColumn(rows.data(), rows.size(), 0, bigintResult);
  container.extractColumn(rows.data(), rows.size(), 1, stringResult);

  auto* actualBigints = bigintResult->asFlatVector<int64_t>();
  auto* actualStrings = stringResult->asFlatVector<StringView>();
  EXPECT_EQ(10, actualBigints->valueAt(0));
  EXPECT_EQ("aa", actualStrings->valueAt(0).getString());
  EXPECT_TRUE(stringResult->isNullAt(1));
  EXPECT_EQ("cccc", actualStrings->valueAt(2).getString());
}


} // namespace
} // namespace bytedance::bolt::exec
