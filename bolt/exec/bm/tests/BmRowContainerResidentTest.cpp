#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/exec/bm/BmRowLayout.h"

#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, ResidentStoreCompareAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeInput();
  auto rows = storeAll(container, input);

  EXPECT_GT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_LT(container.compare(rows[1], rows[2], 0), 0);
  EXPECT_GT(container.compare(rows[0], rows[1], 1), 0);

  auto result = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);

  auto flat = result->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ(10, flat->valueAt(0));
  EXPECT_EQ(3, flat->valueAt(1));
  EXPECT_EQ(7, flat->valueAt(2));
  EXPECT_EQ(3, flat->valueAt(3));
}

TEST_F(BmRowContainerTest, AppendRowsAndStringCompare) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>(
          {"prefix_same_a", "prefix_same_b", "prefix_same_a"}),
  });
  auto rows = storeAll(container, input);

  EXPECT_LT(container.compare(rows[0], rows[1], 1), 0);
  EXPECT_EQ(0, container.compare(rows[0], rows[2], 1));

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("prefix_same_a", flat->valueAt(0).str());
  EXPECT_EQ("prefix_same_b", flat->valueAt(1).str());
  EXPECT_EQ("prefix_same_a", flat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, RowWriteContextKeepsCurrentChunkPointers) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      bufferManager_,
      MemoryTag::kTesting);
  auto context = container.appendRow();

  ASSERT_NE(nullptr, context.segment());
  ASSERT_NE(nullptr, context.chunk());
  EXPECT_EQ(context.segment()->meta.id, context.chunk()->meta.segmentId);
  EXPECT_EQ(context.row(), context.chunk()->rowBlock.ptr);
}

TEST_F(BmRowContainerTest, RowLayoutInitializesOnlyNulls) {
  {
    BmRowLayout layout({BIGINT(), INTEGER()}, {false, false}, 4 << 20);
    std::vector<char> row(layout.rowSize(), static_cast<char>(0x7f));

    layout.initializeNulls(row.data());

    for (auto byte : row) {
      EXPECT_EQ(static_cast<char>(0x7f), byte);
    }
  }

  {
    BmRowLayout layout({BIGINT(), VARCHAR()}, {true, false}, 4 << 20);
    std::vector<char> row(layout.rowSize(), static_cast<char>(0x7f));

    layout.initializeNulls(row.data());

    EXPECT_EQ(0, row[0]);
    for (uint32_t i = layout.column(0).offset;
         i < layout.column(0).offset + layout.column(0).width;
         ++i) {
      EXPECT_EQ(static_cast<char>(0x7f), row[i]);
    }
    for (uint32_t i = layout.column(1).offset;
         i < layout.column(1).offset + layout.column(1).width;
         ++i) {
      EXPECT_EQ(static_cast<char>(0x7f), row[i]);
    }
  }
}

TEST_F(BmRowContainerTest, NullableExtractPreservesNulls) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt, 7}),
      makeNullableFlatVector<std::string>({"delta", std::nullopt, "alpha"}),
  });
  auto rows = storeAll(container, input);

  auto bigintResult = BaseVector::create(BIGINT(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_TRUE(bigintFlat->isNullAt(1));
  EXPECT_FALSE(bigintFlat->isNullAt(2));
  EXPECT_EQ(10, bigintFlat->valueAt(0));
  EXPECT_EQ(7, bigintFlat->valueAt(2));

  auto varcharResult = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_EQ("delta", varcharFlat->valueAt(0).str());
  EXPECT_TRUE(varcharFlat->isNullAt(1));
  EXPECT_EQ("alpha", varcharFlat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, NullableStoreClearsNullBitForNonNullValue) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {true, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt, 42}),
      makeNullableFlatVector<std::string>({std::nullopt, "value"}),
  });
  SelectivityVector rows(input->size());
  DecodedVector bigint;
  DecodedVector varchar;
  bigint.decode(*input->childAt(0), rows);
  varchar.decode(*input->childAt(1), rows);

  auto context = container.appendRow();
  container.store(context, bigint, 0, 0);
  container.store(context, varchar, 0, 1);
  EXPECT_TRUE(context.row()[0] & 0x1);
  EXPECT_TRUE(context.row()[0] & 0x2);

  container.store(context, bigint, 1, 0);
  container.store(context, varchar, 1, 1);

  auto* storedRow = context.row();
  auto bigintResult = BaseVector::create(BIGINT(), 1, pool());
  container.extractColumnResident(&storedRow, 1, 0, bigintResult);
  auto bigintFlat = bigintResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, bigintFlat);
  EXPECT_FALSE(bigintFlat->isNullAt(0));
  EXPECT_EQ(42, bigintFlat->valueAt(0));

  auto varcharResult = BaseVector::create(VARCHAR(), 1, pool());
  container.extractColumnResident(&storedRow, 1, 1, varcharResult);
  auto varcharFlat = varcharResult->asFlatVector<StringView>();
  ASSERT_NE(nullptr, varcharFlat);
  EXPECT_FALSE(varcharFlat->isNullAt(0));
  EXPECT_EQ("value", varcharFlat->valueAt(0).str());
}

TEST_F(BmRowContainerTest, NullableStringNullSurvivesSpillRead) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, true},
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeNullableFlatVector<std::string>({"delta", std::nullopt, "alpha"}),
  });
  storeAll(container, input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();

  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result);
  auto flat = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flat);
  EXPECT_EQ("delta", flat->valueAt(0).str());
  EXPECT_TRUE(flat->isNullAt(1));
  EXPECT_EQ("alpha", flat->valueAt(2).str());
}

TEST_F(BmRowContainerTest, RejectsUnsupportedComplexTypes) {
  EXPECT_THROW(
      BmRowContainer(
          {ARRAY(BIGINT())}, {false}, bufferManager_, MemoryTag::kTesting),
      BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
