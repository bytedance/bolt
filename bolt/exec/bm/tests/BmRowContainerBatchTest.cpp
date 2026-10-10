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

#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

#include "bolt/exec/BmContainerRowSerde.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/bm/BmRowLayout.h"

#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, AppendBatchStoresComplexEncodings) {
  auto arrays = makeNullableArrayVector<std::string>({
      {{{"one", std::nullopt, std::string(40, 'x')}}},
      {{std::vector<std::optional<std::string>>{}}},
      {{{"three"}}},
  });
  auto indices = makeIndices({2, 0, 1});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr, indices, indices->size() / sizeof(vector_size_t), arrays);
  auto constant = BaseVector::wrapInConstant(3, 0, arrays);
  auto input = makeRowVector({dictionary, constant});
  BmRowContainer container(
      {arrays->type(), arrays->type()},
      {false, false},
      1,
      bufferManager_,
      MemoryTag::kTesting,
      128,
      64);

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(3, rows.size());

  auto dictionaryResult = BaseVector::create(arrays->type(), 3, pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 0, dictionaryResult, true);
  auto constantResult = BaseVector::create(arrays->type(), 3, pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 1, constantResult, true);

  auto expectedDictionary = makeNullableArrayVector<std::string>({
      {{{"three"}}},
      {{{"one", std::nullopt, std::string(40, 'x')}}},
      {{std::vector<std::optional<std::string>>{}}},
  });
  auto expectedConstant = makeNullableArrayVector<std::string>({
      {{{"one", std::nullopt, std::string(40, 'x')}}},
      {{{"one", std::nullopt, std::string(40, 'x')}}},
      {{{"one", std::nullopt, std::string(40, 'x')}}},
  });
  test::assertEqualVectors(expectedDictionary, dictionaryResult);
  test::assertEqualVectors(expectedConstant, constantResult);
}

TEST_F(BmRowContainerTest, AppendBatchStoresFixedAndVariableRows) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
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
      0,
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

TEST_F(BmRowContainerTest, AppendBatchStoresDecimalRows) {
  auto shortDecimalType = DECIMAL(10, 2);
  auto longDecimalType = DECIMAL(30, 4);
  BmRowContainer container(
      {shortDecimalType, longDecimalType},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({100, -250, 375}, shortDecimalType),
      makeFlatVector<int128_t>({1000, -2500, 3750}, longDecimalType),
  });

  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);

  ASSERT_EQ(3, rows.size());

  auto shortResult = BaseVector::create(shortDecimalType, rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, shortResult);
  auto shortFlat = shortResult->asFlatVector<int64_t>();
  ASSERT_NE(nullptr, shortFlat);
  EXPECT_EQ(100, shortFlat->valueAt(0));
  EXPECT_EQ(-250, shortFlat->valueAt(1));
  EXPECT_EQ(375, shortFlat->valueAt(2));

  auto longResult = BaseVector::create(longDecimalType, rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, longResult);
  auto longFlat = longResult->asFlatVector<int128_t>();
  ASSERT_NE(nullptr, longFlat);
  EXPECT_EQ(1000, longFlat->valueAt(0));
  EXPECT_EQ(-2500, longFlat->valueAt(1));
  EXPECT_EQ(3750, longFlat->valueAt(2));
}

TEST_F(BmRowContainerTest, AppendBatchCanCrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      32);
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>(
          {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"}),
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
      0,
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
      0,
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

TEST_F(
    BmRowContainerDiskIoTest,
    AppendBatchReloadsMultipleStringColumnsAcrossChunks) {
  BmRowContainer container(
      {BIGINT(), VARCHAR(), VARCHAR()},
      {false, false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      128,
      64);
  constexpr vector_size_t kRows = 12;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row + 100; }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
            return std::string(40 + row, static_cast<char>('a' + row));
          }),
      makeFlatVector<std::string>(
          kRows,
          [](auto row) {
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

// ---------------------------------------------------------------------------
// Speculative complex-value write round-trip tests. Complex-type columns only
// (ARRAY / MAP / ROW). These exercise both the single-row store() path (via
// storeAll) and appendBatch() where noted.
// ---------------------------------------------------------------------------

// Heap tail large enough for every value: correct round-trip through both the
// single-row store() and batch appendBatch() paths.
TEST_F(BmRowContainerTest, SpeculativeHeapTailLargeEnough) {
  auto maps = makeMapVector<int64_t, std::string>({
      {{2, "two"}, {1, std::string(40, 'a')}},
      {{4, "four"}, {3, std::string(50, 'b')}},
      {{5, "five"}},
  });
  auto input = makeRowVector({maps});

  BmRowContainer single(
      {maps->type()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  auto singleRows = storeAll(single, input);
  ASSERT_EQ(3, singleRows.size());
  auto singleResult = BaseVector::create(maps->type(), 3, pool());
  single.extractColumnResident(
      singleRows.data(), singleRows.size(), 0, singleResult, true);
  test::assertEqualVectors(maps, singleResult);

  BmRowContainer batch(
      {maps->type()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  std::vector<char*> batchRows;
  batch.appendBatch(input, kDefaultPartition, &batchRows);
  ASSERT_EQ(3, batchRows.size());
  auto batchResult = BaseVector::create(maps->type(), 3, pool());
  batch.extractColumnResident(
      batchRows.data(), batchRows.size(), 0, batchResult, true);
  test::assertEqualVectors(maps, batchResult);
}

// A tiny heap block leaves each value's tail too small, forcing the overflow
// fallback that allocates a fresh block; values must still round-trip.
TEST_F(BmRowContainerTest, SpeculativeHeapTailTooSmall) {
  auto arrays = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 10},
      {11, 12, 13, 14, 15},
  });
  auto input = makeRowVector({arrays});

  // heapBlockSize of 64 bytes forces overflow between rows (~48 bytes each).
  BmRowContainer container(
      {arrays->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4096,
      64);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(3, rows.size());
  auto result = BaseVector::create(arrays->type(), 3, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(arrays, result);
}

// A value larger than the heap block size triggers an oversized block
// allocation; it must still round-trip.
TEST_F(BmRowContainerTest, SpeculativeValueLargerThanBlock) {
  auto arrays = makeArrayVector<std::string>({
      {std::string(200, 'x')},
      {std::string(300, 'y')},
  });
  auto input = makeRowVector({arrays});

  // heapBlockSize smaller than a single value forces oversized allocation.
  BmRowContainer container(
      {arrays->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4096,
      64);
  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());
  auto result = BaseVector::create(arrays->type(), 2, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(arrays, result);
}

// Inline-sized values must not grow heap usage (resident behavior unchanged)
// and must round-trip: every stored StringView stays inline.
TEST_F(BmRowContainerTest, SpeculativeInlineValuesDoNotGrowHeap) {
  auto emptyArrays = makeArrayVector<int64_t>({{}, {}, {}});
  auto input = makeRowVector({emptyArrays});
  const ContainerRowSerdeOptions options{.isKey = false};
  for (vector_size_t row = 0; row < emptyArrays->size(); ++row) {
    ASSERT_LE(
        BmContainerRowSerde::serializedSize(*emptyArrays, row, options),
        StringView::kInlineSize);
  }

  BmRowContainer container(
      {emptyArrays->type()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  auto rows = storeAll(container, input);
  ASSERT_EQ(3, rows.size());

  // Every stored value must be an inline StringView (no heap payload). Rebuild
  // the layout with the same parameters to locate the column's StringView.
  BmRowLayout layout(
      {emptyArrays->type()},
      {false},
      0,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)));
  for (const auto* row : rows) {
    EXPECT_TRUE(reinterpret_cast<const StringView*>(layout.valueAddress(row, 0))
                    ->isInline());
  }

  auto result = BaseVector::create(emptyArrays->type(), 3, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(emptyArrays, result);
}

// A heap tail smaller than an inline-sized value forces the stack-buffer
// inline fallback: it must round-trip without growing the heap.
TEST_F(BmRowContainerTest, SpeculativeHeapTailSmallerThanInline) {
  // First row is a large array (forces a heap block, leaves a small tail);
  // second row is an empty array (inline, must fall back to a stack buffer if
  // the tail is too small).
  auto arrays = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5, 6, 7},
      {},
  });
  auto input = makeRowVector({arrays});

  BmRowContainer container(
      {arrays->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4096,
      64);
  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());

  // The inline fallback keeps the second value in an inline StringView. Rebuild
  // the layout with the same parameters to locate it.
  BmRowLayout layout({arrays->type()}, {false}, 0, 4096);
  EXPECT_TRUE(
      reinterpret_cast<const StringView*>(layout.valueAddress(rows[1], 0))
          ->isInline());

  auto result = BaseVector::create(arrays->type(), 2, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(arrays, result);
}

// Null complex rows skip the writer entirely and round-trip as null, through
// both the single-row and batch paths.
TEST_F(BmRowContainerTest, SpeculativeNullableComplexValues) {
  auto arrays = makeNullableArrayVector<int64_t>(
      {{{1, 2, 3}}, std::nullopt, {{4, std::nullopt, 6}}, std::nullopt});
  auto input = makeRowVector({arrays});

  BmRowContainer single(
      {arrays->type()}, {true}, 0, bufferManager_, MemoryTag::kTesting);
  auto singleRows = storeAll(single, input);
  auto singleResult = BaseVector::create(arrays->type(), 4, pool());
  single.extractColumnResident(
      singleRows.data(), singleRows.size(), 0, singleResult, true);
  test::assertEqualVectors(arrays, singleResult);

  BmRowContainer batch(
      {arrays->type()}, {true}, 0, bufferManager_, MemoryTag::kTesting);
  std::vector<char*> batchRows;
  batch.appendBatch(input, kDefaultPartition, &batchRows);
  auto batchResult = BaseVector::create(arrays->type(), 4, pool());
  batch.extractColumnResident(
      batchRows.data(), batchRows.size(), 0, batchResult, true);
  test::assertEqualVectors(arrays, batchResult);
}

// Nested ARRAY<MAP> round-trips through both the single-row and batch paths.
TEST_F(BmRowContainerTest, SpeculativeNestedComplexRoundTrip) {
  using MapEntry = std::pair<int64_t, std::optional<std::string>>;
  const std::vector<MapEntry> firstMap{{2, "two"}, {1, "one"}};
  const std::vector<MapEntry> secondMap{{3, "three"}};
  const std::vector<std::vector<std::vector<MapEntry>>> arrayOfMapsData{
      {firstMap, secondMap}, {secondMap}};
  VectorPtr arrayOfMaps =
      makeArrayOfMapVector<int64_t, std::string>(arrayOfMapsData);
  auto input = makeRowVector({arrayOfMaps});

  BmRowContainer single(
      {arrayOfMaps->type()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  auto singleRows = storeAll(single, input);
  auto singleResult = BaseVector::create(arrayOfMaps->type(), 2, pool());
  single.extractColumnResident(
      singleRows.data(), singleRows.size(), 0, singleResult, true);
  test::assertEqualVectors(arrayOfMaps, singleResult);

  BmRowContainer batch(
      {arrayOfMaps->type()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  std::vector<char*> batchRows;
  batch.appendBatch(input, kDefaultPartition, &batchRows);
  auto batchResult = BaseVector::create(arrayOfMaps->type(), 2, pool());
  batch.extractColumnResident(
      batchRows.data(), batchRows.size(), 0, batchResult, true);
  test::assertEqualVectors(arrayOfMaps, batchResult);
}

// Dictionary and constant encoded inputs round-trip correctly.
TEST_F(BmRowContainerTest, SpeculativeWrappedEncodingsRoundTrip) {
  auto arrays = makeNullableArrayVector<std::string>({
      {{{std::nullopt, "short", std::string(40, 'x')}}},
      {{{"last"}}},
  });
  auto dictionary = wrapInDictionary(makeIndices({1, 0}), arrays);
  auto constant = BaseVector::wrapInConstant(2, 0, arrays);
  auto input = makeRowVector({dictionary, constant});

  BmRowContainer container(
      {arrays->type(), arrays->type()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(2, rows.size());

  auto dictionaryResult = BaseVector::create(arrays->type(), 2, pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 0, dictionaryResult, true);
  auto expectedDictionary = makeNullableArrayVector<std::string>({
      {{{"last"}}},
      {{{std::nullopt, "short", std::string(40, 'x')}}},
  });
  test::assertEqualVectors(expectedDictionary, dictionaryResult);

  auto constantResult = BaseVector::create(arrays->type(), 2, pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 1, constantResult, true);
  auto expectedConstant = makeNullableArrayVector<std::string>({
      {{{std::nullopt, "short", std::string(40, 'x')}}},
      {{{std::nullopt, "short", std::string(40, 'x')}}},
  });
  test::assertEqualVectors(expectedConstant, constantResult);
}

// Key MAPs with different physical entry order but equal contents compare
// equal and hash identically after store, i.e. canonical ordering is kept.
TEST_F(BmRowContainerTest, SpeculativeMapCanonicalHashCompare) {
  auto keyMaps = makeMapVector<int64_t, int64_t>({
      {{3, 30}, {1, 10}, {2, 20}},
      {{1, 10}, {2, 20}, {3, 30}},
  });
  auto input = makeRowVector({keyMaps});
  BmRowContainer container(
      {keyMaps->type()}, {false}, 1, bufferManager_, MemoryTag::kTesting);
  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());

  // Logically-equal maps with different physical order compare equal and hash
  // identically.
  EXPECT_EQ(0, container.compare(rows[0], rows[1], 0));
  EXPECT_EQ(container.hash(rows[0], 0), container.hash(rows[1], 0));
}

// A batch whose values span several heap blocks round-trips for every row,
// including the ones straddling a block boundary.
TEST_F(BmRowContainerTest, SpeculativeBatchCrossesHeapBlocks) {
  std::vector<std::vector<int64_t>> data;
  for (int i = 0; i < 20; ++i) {
    data.push_back({i, i + 1, i + 2, i + 3});
  }
  auto arrays = makeArrayVector<int64_t>(data);
  auto input = makeRowVector({arrays});

  // Small heap block forces multiple block crossings within one batch.
  BmRowContainer container(
      {arrays->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      8192,
      64);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(20, rows.size());
  auto result = BaseVector::create(arrays->type(), 20, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(arrays, result);
}

// Uncommitted overflow tail bytes live beyond heap.used, so a spill+bulk-read
// round-trip reproduces the values with no leaked garbage. A tiny heap block
// forces the overflow that leaves such tail bytes behind.
TEST_F(BmRowContainerDiskIoTest, SpeculativeSpillExcludesUncommittedTail) {
  auto arrays = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 10},
      {11, 12, 13, 14, 15},
  });
  auto input = makeRowVector({arrays});

  BmRowContainer container(
      {arrays->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4096,
      64);
  container.appendBatch(input);

  auto segment = container.spillActiveSegment();
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();
  ASSERT_EQ(3, rows.size());
  auto result = BaseVector::create(arrays->type(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
  test::assertEqualVectors(arrays, result);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
