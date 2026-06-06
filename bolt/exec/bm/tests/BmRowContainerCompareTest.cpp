#include "bolt/exec/bm/tests/BmRowContainerTestUtil.h"

namespace bytedance::bolt::exec {
namespace {

TEST_F(BmRowContainerTest, ComparesFixedWidthColumnsAndRows) {
  auto bufferManager = makeBufferManager("fixed-compare");
  BmRowContainer container({BIGINT(), INTEGER()}, {}, bufferManager);

  auto bigints = makeBigintVector(leaf_.get(), {10, 20, 20, std::nullopt});
  auto integers = makeIntegerVector(leaf_.get(), {1, 1, 2, 1});
  DecodedVector decodedBigints(*bigints);
  DecodedVector decodedIntegers(*integers);

  std::vector<RowId> rows;
  for (auto i = 0; i < bigints->size(); ++i) {
    auto row = container.newRow();
    container.store(decodedBigints, i, row, 0);
    container.store(decodedIntegers, i, row, 1);
    rows.push_back(row);
  }

  EXPECT_LT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_GT(container.compare(rows[1], rows[0], 0), 0);
  EXPECT_GT(
      container.compare(
          rows[0], rows[1], 0, CompareFlags{.ascending = false}),
      0);
  EXPECT_LT(container.compareRows(rows[1], rows[2]), 0);
  EXPECT_EQ(container.compareRows(rows[1], rows[1]), 0);
  EXPECT_LT(container.compare(rows[3], rows[0], 0), 0);
  EXPECT_GT(
      container.compare(
          rows[3], rows[0], 0, CompareFlags{.nullsFirst = false}),
      0);
}


} // namespace
} // namespace bytedance::bolt::exec
