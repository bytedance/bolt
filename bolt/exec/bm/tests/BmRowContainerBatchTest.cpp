#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, AppendBatchStoresFixedAndVariableRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({11, 22, 33}),
      makeFlatVector<std::string>({"alpha", "bravo", "charlie"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  EXPECT_EQ(3, container.numRows());

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_EQ(11, bigintFlat->valueAt(0));
  EXPECT_EQ(22, bigintFlat->valueAt(1));
  EXPECT_EQ(33, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("alpha", varcharFlat->valueAt(0).str());
  EXPECT_EQ("bravo", varcharFlat->valueAt(1).str());
  EXPECT_EQ("charlie", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchPreservesNulls) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30}),
      makeNullableFlatVector<std::string>({"alpha", std::nullopt, "charlie"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());
  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_TRUE(bigintFlat->isNullAt(1));
  EXPECT_FALSE(bigintFlat->isNullAt(2));
  EXPECT_EQ(10, bigintFlat->valueAt(0));
  EXPECT_EQ(30, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("alpha", varcharFlat->valueAt(0).str());
  EXPECT_TRUE(varcharFlat->isNullAt(1));
  EXPECT_EQ("charlie", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, AppendBatchCanCrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee", "ffffff"}),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(6, rows.size());
  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  for (auto i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(i + 1, bigintFlat->valueAt(i));
  }

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("a", varcharFlat->valueAt(0).str());
  EXPECT_EQ("bb", varcharFlat->valueAt(1).str());
  EXPECT_EQ("ccc", varcharFlat->valueAt(2).str());
  EXPECT_EQ("dddd", varcharFlat->valueAt(3).str());
  EXPECT_EQ("eeeee", varcharFlat->valueAt(4).str());
  EXPECT_EQ("ffffff", varcharFlat->valueAt(5).str());
}

TEST_F(BmRowContainerTest, AppendBatchStoresNonInlineStringsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({
          std::string(40, 'a'),
          std::string(41, 'b'),
          std::string(42, 'c'),
          std::string(43, 'd'),
          std::string(44, 'e'),
          std::string(45, 'f'),
      }),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(6, rows.size());
  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ(std::string(40, 'a'), varcharFlat->valueAt(0).str());
  EXPECT_EQ(std::string(41, 'b'), varcharFlat->valueAt(1).str());
  EXPECT_EQ(std::string(42, 'c'), varcharFlat->valueAt(2).str());
  EXPECT_EQ(std::string(43, 'd'), varcharFlat->valueAt(3).str());
  EXPECT_EQ(std::string(44, 'e'), varcharFlat->valueAt(4).str());
  EXPECT_EQ(std::string(45, 'f'), varcharFlat->valueAt(5).str());
}

TEST_F(BmRowContainerTest, AppendBatchStoresMultipleNonInlineStringColumns) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      bufferManager_,
      MemoryTag::kTesting,
      256,
      64);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4}),
      makeFlatVector<std::string>({
          std::string(40, 'a'),
          std::string(41, 'b'),
          std::string(42, 'c'),
          std::string(43, 'd'),
      }),
      makeFlatVector<std::string>({
          std::string(44, 'e'),
          std::string(45, 'f'),
          std::string(46, 'g'),
          std::string(47, 'h'),
      }),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(4, rows.size());
  auto left = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, left);
  auto leftFlat = left->asFlatVector<StringView>();
  ASSERT_NE(nullptr, leftFlat);
  EXPECT_EQ(std::string(40, 'a'), leftFlat->valueAt(0).str());
  EXPECT_EQ(std::string(41, 'b'), leftFlat->valueAt(1).str());
  EXPECT_EQ(std::string(42, 'c'), leftFlat->valueAt(2).str());
  EXPECT_EQ(std::string(43, 'd'), leftFlat->valueAt(3).str());

  auto right = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 2, right);
  auto rightFlat = right->asFlatVector<StringView>();
  ASSERT_NE(nullptr, rightFlat);
  EXPECT_EQ(std::string(44, 'e'), rightFlat->valueAt(0).str());
  EXPECT_EQ(std::string(45, 'f'), rightFlat->valueAt(1).str());
  EXPECT_EQ(std::string(46, 'g'), rightFlat->valueAt(2).str());
  EXPECT_EQ(std::string(47, 'h'), rightFlat->valueAt(3).str());
}

TEST_F(BmRowContainerTest, AppendBatchReloadsMultipleStringColumnsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      bufferManager_,
      MemoryTag::kTesting,
      128,
      64);
  constexpr vector_size_t kRows = 12;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row + 100; }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return std::string(40 + row, static_cast<char>('a' + row));
      }),
      makeFlatVector<std::string>(kRows, [](auto row) {
        return std::string(48 + row, static_cast<char>('m' + row));
      }),
  });

  container.appendBatch(input);
  auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());
  auto rows = session.loadRows({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, rows.size());

  auto bigint = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigint);
  auto bigintFlat = bigint->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  auto left = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, left);
  auto leftFlat = left->asFlatVector<StringView>();
  ASSERT_NE(nullptr, leftFlat);
  auto right = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 2, right);
  auto rightFlat = right->asFlatVector<StringView>();
  ASSERT_NE(nullptr, rightFlat);

  for (auto row = 0; row < kRows; ++row) {
    EXPECT_EQ(row + 100, bigintFlat->valueAt(row));
    EXPECT_EQ(
        std::string(40 + row, static_cast<char>('a' + row)),
        leftFlat->valueAt(row).str());
    EXPECT_EQ(
        std::string(48 + row, static_cast<char>('m' + row)),
        rightFlat->valueAt(row).str());
  }
}

} // namespace
} // namespace bytedance::bolt::exec::bm
