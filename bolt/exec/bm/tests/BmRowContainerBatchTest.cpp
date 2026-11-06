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

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/bm/BmContainerRowSerde.h"
#include "bolt/exec/bm/BmRowLayout.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

class BmBatchAppendTypeTest : public BmRowContainerTest,
                              public testing::WithParamInterface<TypePtr> {
 public:
  static void SetUpTestSuite() {
    BmRowContainerTest::SetUpTestSuite();
  }

 protected:
  // Cross two 64-bit words and exercise a partial trailing word in Boolean
  // values and null bitmaps.
  static constexpr vector_size_t kRows = 130;

  VectorPtr makeValues(const TypePtr& type) {
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        return makeFlatVector<bool>(kRows, [](auto row) {
          return row % 3 == 0 || row == 63 || row == 64;
        });
      case TypeKind::TINYINT:
        return makeFlatVector<int8_t>(
            kRows, [](auto row) { return row - 65; }, nullptr, type);
      case TypeKind::SMALLINT:
        return makeFlatVector<int16_t>(
            kRows, [](auto row) { return (row - 65) * 503; }, nullptr, type);
      case TypeKind::INTEGER:
        return makeFlatVector<int32_t>(
            kRows, [](auto row) { return (row - 65) * 7919; }, nullptr, type);
      case TypeKind::BIGINT:
        return makeFlatVector<int64_t>(
            kRows,
            [](auto row) { return static_cast<int64_t>(row - 65) * 104729; },
            nullptr,
            type);
      case TypeKind::HUGEINT:
        return makeFlatVector<int128_t>(
            kRows,
            [](auto row) {
              return static_cast<int128_t>(row - 65) * (int128_t{1} << 80);
            },
            nullptr,
            type);
      case TypeKind::REAL:
        return makeFlatVector<float>(kRows, [](auto row) {
          return row == 63 ? std::numeric_limits<float>::quiet_NaN()
              : row == 64  ? -0.0f
              : row == 127 ? std::numeric_limits<float>::infinity()
              : row == 128 ? -std::numeric_limits<float>::infinity()
                           : (row - 65) * 1.25f;
        });
      case TypeKind::DOUBLE:
        return makeFlatVector<double>(kRows, [](auto row) {
          return row == 63 ? std::numeric_limits<double>::quiet_NaN()
              : row == 64  ? -0.0
              : row == 127 ? std::numeric_limits<double>::infinity()
              : row == 128 ? -std::numeric_limits<double>::infinity()
                           : (row - 65) * 1.25;
        });
      case TypeKind::TIMESTAMP:
        return makeFlatVector<Timestamp>(
            kRows, [](auto row) { return Timestamp(row - 65, row * 7919); });
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return makeFlatVector<std::string>(
            kRows,
            [](auto row) {
              return row % 7 == 0
                  ? std::string{}
                  : std::string("a\0z", 3) + std::string(row % 40, 'x');
            },
            nullptr,
            type);
      case TypeKind::ARRAY:
        return makeArrayVector<bool>(
            kRows,
            [](auto row) { return row % 4; },
            [](auto index) { return index % 3 == 0; },
            nullptr,
            [](auto index) { return index % 5 == 0; });
      case TypeKind::MAP:
        return makeMapVector<int64_t, bool>(
            kRows,
            [](auto row) { return row % 4; },
            [](auto index) { return index; },
            [](auto index) { return index % 3 == 0; },
            nullptr,
            [](auto index) { return index % 5 == 0; });
      case TypeKind::ROW:
        return makeRowVector(
            {"flag", "items", "text"},
            {makeValues(BOOLEAN()),
             makeValues(ARRAY(BOOLEAN())),
             makeValues(VARCHAR())});
      default:
        BOLT_FAIL("Unsupported batch test type {}", type->toString());
    }
  }

  VectorPtr makeMixedNullValues() {
    auto input = makeValues(GetParam());
    for (vector_size_t row = 0; row < kRows; row += 3) {
      input->setNull(row, true);
    }
    return input;
  }

  VectorPtr makeAllNullValues() {
    auto input = makeValues(GetParam());
    for (vector_size_t row = 0; row < kRows; ++row) {
      input->setNull(row, true);
    }
    return input;
  }

  void assertBatchRoundTrip(const VectorPtr& input, bool nullable) {
    SCOPED_TRACE(nullable ? "nullable column" : "non-nullable column");
    ASSERT_TRUE(input->type()->equivalent(*GetParam()));
    // Small row and heap blocks force the batch to span BM chunks.
    BmRowContainer container(
        {input->type()},
        {nullable},
        0,
        bufferManager_,
        MemoryTag::kTesting,
        64,
        64);
    std::vector<char*> rows;
    container.appendBatch(makeRowVector({input}), kDefaultPartition, &rows);
    ASSERT_EQ(input->size(), rows.size());
    auto result = BaseVector::create(input->type(), rows.size(), pool());
    container.extractColumnResident(rows.data(), rows.size(), 0, result, true);
    test::assertEqualVectors(input, result);
  }
};

TEST_P(BmBatchAppendTypeTest, FlatInputWithoutNulls) {
  auto input = makeValues(GetParam());
  assertBatchRoundTrip(input, /*nullable=*/false);
  assertBatchRoundTrip(input, /*nullable=*/true);
}

TEST_P(BmBatchAppendTypeTest, FlatInputWithMixedNulls) {
  assertBatchRoundTrip(makeMixedNullValues(), /*nullable=*/true);
}

TEST_P(BmBatchAppendTypeTest, FlatInputWithAllNulls) {
  assertBatchRoundTrip(makeAllNullValues(), /*nullable=*/true);
}

TEST_P(BmBatchAppendTypeTest, NullBitmapBoundaries) {
  auto input = makeValues(GetParam());
  // Null both sides of byte and 64-bit word boundaries. Neighboring rows
  // remain non-null, so a shifted or missing null bit changes the result.
  for (vector_size_t row : {7, 8, 63, 64, 127, 128}) {
    input->setNull(row, true);
  }
  assertBatchRoundTrip(input, /*nullable=*/true);
}

TEST_P(BmBatchAppendTypeTest, DictionaryInput) {
  const auto reverse = [&](const VectorPtr& input) {
    return wrapInDictionary(makeIndicesInReverse(kRows), input);
  };
  auto nonNullInput = reverse(makeValues(GetParam()));
  assertBatchRoundTrip(nonNullInput, /*nullable=*/false);
  assertBatchRoundTrip(nonNullInput, /*nullable=*/true);
  assertBatchRoundTrip(reverse(makeMixedNullValues()), /*nullable=*/true);
  assertBatchRoundTrip(reverse(makeAllNullValues()), /*nullable=*/true);
}

TEST_P(BmBatchAppendTypeTest, ConstantAndDictionaryOverConstantInput) {
  // The source contains no top-level nulls. For BOOLEAN, indices 0 and 1
  // select true and false, respectively.
  auto values = makeValues(GetParam());
  for (vector_size_t sourceRow : {0, 1}) {
    SCOPED_TRACE(sourceRow);
    auto constant = BaseVector::wrapInConstant(kRows, sourceRow, values);
    auto dictionary = wrapInDictionary(makeIndicesInReverse(kRows), constant);
    assertBatchRoundTrip(constant, /*nullable=*/false);
    assertBatchRoundTrip(constant, /*nullable=*/true);
    assertBatchRoundTrip(dictionary, /*nullable=*/false);
    assertBatchRoundTrip(dictionary, /*nullable=*/true);
  }

  // Construct a null constant explicitly, independently of source row values.
  auto nullConstant = BaseVector::createNullConstant(GetParam(), kRows, pool());
  assertBatchRoundTrip(nullConstant, /*nullable=*/true);
  assertBatchRoundTrip(
      wrapInDictionary(makeIndicesInReverse(kRows), nullConstant),
      /*nullable=*/true);
}

INSTANTIATE_TEST_SUITE_P(
    AllSupportedTypes,
    BmBatchAppendTypeTest,
    testing::Values(
        BOOLEAN(),
        TINYINT(),
        SMALLINT(),
        INTEGER(),
        BIGINT(),
        HUGEINT(),
        REAL(),
        DOUBLE(),
        TIMESTAMP(),
        VARCHAR(),
        VARBINARY(),
        DATE(),
        INTERVAL_DAY_TIME(),
        INTERVAL_YEAR_MONTH(),
        DECIMAL(10, 2),
        DECIMAL(30, 4),
        ARRAY(BOOLEAN()),
        MAP(BIGINT(), BOOLEAN()),
        ROW({"flag", "items", "text"},
            {BOOLEAN(), ARRAY(BOOLEAN()), VARCHAR()})),
    [](const testing::TestParamInfo<TypePtr>& info) {
      auto name = info.param->toString();
      for (auto& character : name) {
        if (!std::isalnum(static_cast<unsigned char>(character))) {
          character = '_';
        }
      }
      return name;
    });

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

TEST_F(BmRowContainerTest, AppendBatchPreservesBooleanBitPatterns) {
  // Cross byte and word boundaries in the bit-packed source. The nullable
  // column exercises decoded.valueAt<bool>() as a reference for the fast path.
  auto values = makeFlatVector<bool>(
      130, [](auto row) { return row % 3 == 0 || row == 63 || row == 64; });
  auto input = makeRowVector({values, values});
  BmRowContainer container(
      {BOOLEAN(), BOOLEAN()},
      {false, true},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      64);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(input->size(), rows.size());

  for (int32_t column = 0; column < 2; ++column) {
    SCOPED_TRACE(column);
    auto result = BaseVector::create(BOOLEAN(), rows.size(), pool());
    container.extractColumnResident(rows.data(), rows.size(), column, result);
    test::assertEqualVectors(values, result);
  }
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

// rowBlockId identifies physical row-block ownership. Group only adjacent equal
// ids; the id itself is not a chunk index.
std::vector<size_t> chunkRowCounts(const std::vector<RowId>& rowIds) {
  std::vector<size_t> chunkRows;
  BlockId previousRowBlock{kNoBlock};
  for (const auto& rowId : rowIds) {
    if (rowId.rowBlockId != previousRowBlock) {
      chunkRows.push_back(1);
      previousRowBlock = rowId.rowBlockId;
    } else {
      ++chunkRows.back();
    }
  }
  return chunkRows;
}

TEST_F(
    BmRowContainerDiskIoTest,
    BatchHeapTargetSealsShortChunksAndReloadStrings) {
  constexpr vector_size_t kRows = 65;
  constexpr uint32_t kHeapBlockSize = 512;
  auto ids =
      makeFlatVector<int64_t>(kRows, [](auto row) { return 1000 + row; });
  ids->setNull(32, true);
  auto stringsA = makeFlatVector<std::string>(kRows, [](auto row) {
    return std::string(40, static_cast<char>('a' + row));
  });
  auto stringsB = makeFlatVector<std::string>(kRows, [](auto row) {
    return std::string(20, static_cast<char>('k' + row));
  });
  auto input = makeRowVector({ids, stringsA, stringsB});

  const auto exercise = [&](uint64_t chunkHeapTargetBytes) {
    BmRowContainer container(
        {BIGINT(), VARCHAR(), VARCHAR()},
        {true, false, false},
        0,
        bufferManager_,
        MemoryTag::kTesting,
        16384,
        kHeapBlockSize,
        nullptr,
        chunkHeapTargetBytes);
    const auto beforeAppend = bufferManager_->stats().allocatedBlocks;
    container.appendBatch(input);
    const auto allocatedBlocks =
        bufferManager_->stats().allocatedBlocks - beforeAppend;
    const auto segment = container.spillActiveSegment();
    auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
    const auto rowIds = session.listRowIds();
    ASSERT_EQ(kRows, rowIds.size());
    const auto chunkRows = chunkRowCounts(rowIds);
    if (chunkHeapTargetBytes != 0) {
      EXPECT_GT(chunkRows.size(), 1);
      // The target is soft: the final complete piece can cross it.
      EXPECT_LE(
          (allocatedBlocks - chunkRows.size()) * kHeapBlockSize,
          chunkRows.size() * (chunkHeapTargetBytes + 2 * kHeapBlockSize));
    } else {
      EXPECT_EQ(std::vector<size_t>({kRows}), chunkRows);
    }

    // Load once, release all pins, reclaim the resident blocks, then require
    // the reordered read below to reload from disk.
    auto firstLoad = session.loadRowsScoped({rowIds.data(), rowIds.size()});
    ASSERT_EQ(kRows, firstLoad.size());
    firstLoad.reset();
    ASSERT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
    ASSERT_GT(bufferManager_->reclaimableBytes(), 0);
    const auto beforeReclaim = bufferManager_->stats();
    memory::MemoryReclaimer::Stats reclaimStats;
    memory::ScopedMemoryArbitrationContext arbitrationContext(root_.get());
    ASSERT_GT(
        root_->reclaim(bufferManager_->reclaimableBytes(), 0, reclaimStats), 0);
    const auto afterReclaim = bufferManager_->stats();
    EXPECT_GT(afterReclaim.reclaimedBytes, beforeReclaim.reclaimedBytes);

    constexpr vector_size_t kReadOrder[]{64, 32, 31, 63, 0, 32, 1, 62, 64, 5};
    std::vector<RowId> reordered;
    reordered.reserve(std::size(kReadOrder));
    for (auto source : kReadOrder) {
      reordered.push_back(rowIds[source]);
    }
    auto loaded = session.loadRowsScoped({reordered.data(), reordered.size()});
    EXPECT_GT(
        bufferManager_->stats().spillReadCount, afterReclaim.spillReadCount);

    auto idResult = BaseVector::create(BIGINT(), loaded.size(), pool());
    container.extractColumnResident(
        loaded.data(), loaded.size(), 0, idResult, true);
    auto aResult = BaseVector::create(VARCHAR(), loaded.size(), pool());
    container.extractColumnResident(
        loaded.data(), loaded.size(), 1, aResult, true);
    auto bResult = BaseVector::create(VARCHAR(), loaded.size(), pool());
    container.extractColumnResident(
        loaded.data(), loaded.size(), 2, bResult, true);

    auto idFlat = idResult->asFlatVector<int64_t>();
    auto aFlat = aResult->asFlatVector<StringView>();
    auto bFlat = bResult->asFlatVector<StringView>();
    ASSERT_NE(nullptr, idFlat);
    ASSERT_NE(nullptr, aFlat);
    ASSERT_NE(nullptr, bFlat);
    for (size_t i = 0; i < std::size(kReadOrder); ++i) {
      const auto source = kReadOrder[i];
      EXPECT_EQ(source == 32, idFlat->isNullAt(i));
      if (source != 32) {
        EXPECT_EQ(1000 + source, idFlat->valueAt(i));
      }
      EXPECT_EQ(
          std::string(40, static_cast<char>('a' + source)),
          aFlat->valueAt(i).str());
      EXPECT_EQ(
          std::string(20, static_cast<char>('k' + source)),
          bFlat->valueAt(i).str());
    }
  };

  exercise(1024);
  exercise(0);
}

TEST_F(BmRowContainerDiskIoTest, BatchHeapTargetSealsComplexValuesAndReload) {
  constexpr vector_size_t kRows = 65;
  constexpr uint32_t kHeapBlockSize = 512;
  std::vector<std::vector<int64_t>> arrayData;
  std::vector<std::vector<std::pair<int64_t, std::optional<std::string>>>>
      mapData;
  arrayData.reserve(kRows);
  mapData.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    std::vector<int64_t> elements;
    for (vector_size_t i = 0; i < 2 + row % 3; ++i) {
      elements.push_back(row * 10 + i);
    }
    arrayData.push_back(std::move(elements));
    mapData.push_back({{row, std::string(18 + row % 5, 'm')}});
  }
  auto arrays = makeArrayVector<int64_t>(arrayData);
  auto maps = makeMapVector<int64_t, std::string>(mapData);
  auto nestedRows = makeRowVector(
      {"number", "text"},
      {makeFlatVector<int64_t>(kRows, [](auto row) { return 10000 + row; }),
       makeFlatVector<std::string>(kRows, [](auto row) {
         const auto length = row == 31 ? 96 : 24 + row % 7;
         return std::string(length, static_cast<char>('a' + row % 26));
       })});
  auto input = makeRowVector({arrays, maps, nestedRows});

  // Keep every complex value below the fixed heap block size, so every heap
  // block is exactly kHeapBlockSize and the allocated block count bounds the
  // per-chunk heap.
  const ContainerRowSerdeOptions options{.isKey = false};
  std::array<VectorPtr, 3> complexColumns{arrays, maps, nestedRows};
  for (const auto& column : complexColumns) {
    for (auto row = 0; row < kRows; ++row) {
      ASSERT_LT(
          BmContainerRowSerde::serializedSize(*column, row, options),
          kHeapBlockSize);
    }
  }

  // About two heap blocks per column. The batch needs more, so it spans
  // several chunks; without a target all rows would share one row block.
  constexpr uint64_t kChunkHeapTarget = 6 * kHeapBlockSize;
  BmRowContainer container(
      {arrays->type(), maps->type(), nestedRows->type()},
      {false, false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      16384,
      kHeapBlockSize,
      nullptr,
      kChunkHeapTarget);
  const auto beforeAppend = bufferManager_->stats().allocatedBlocks;
  container.appendBatch(input);
  const auto allocatedBlocks =
      bufferManager_->stats().allocatedBlocks - beforeAppend;

  const auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());
  const auto chunkRows = chunkRowCounts(rowIds);
  ASSERT_GT(chunkRows.size(), 1);
  EXPECT_LE(
      (allocatedBlocks - chunkRows.size()) * kHeapBlockSize,
      chunkRows.size() * (kChunkHeapTarget + 3 * kHeapBlockSize));

  auto loaded = session.loadRowsScoped({rowIds.data(), rowIds.size()});
  ASSERT_EQ(kRows, loaded.size());
  loaded.reset();
  ASSERT_EQ(0, bufferManager_->stats().pinnedResidentBytes);
  ASSERT_GT(bufferManager_->reclaimableBytes(), 0);
  memory::MemoryReclaimer::Stats reclaimStats;
  memory::ScopedMemoryArbitrationContext arbitrationContext(root_.get());
  ASSERT_GT(
      root_->reclaim(bufferManager_->reclaimableBytes(), 0, reclaimStats), 0);
  const auto afterReclaim = bufferManager_->stats();

  loaded = session.loadRowsScoped({rowIds.data(), rowIds.size()});
  EXPECT_GT(
      bufferManager_->stats().spillReadCount, afterReclaim.spillReadCount);
  for (column_index_t column = 0; column < complexColumns.size(); ++column) {
    auto result =
        BaseVector::create(complexColumns[column]->type(), kRows, pool());
    container.extractColumnResident(
        loaded.data(), loaded.size(), column, result, true);
    test::assertEqualVectors(complexColumns[column], result);
  }
}

TEST_F(BmRowContainerTest, BatchEstimateSelectsWholeBatchOnlyBelowTarget) {
  constexpr vector_size_t kRows = 64;
  constexpr uint32_t kBlockBytes = 16384;
  const std::vector<TypePtr> types{VARCHAR(), VARCHAR(), BIGINT()};
  const std::vector<bool> nullable{false, false, false};
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          kRows, [](auto) { return std::string(32, 'a'); }),
      makeFlatVector<std::string>(
          kRows, [](auto) { return std::string(32, 'b'); }),
      wrapInDictionary(
          makeIndicesInReverse(kRows),
          makeFlatVector<int64_t>(kRows, [](auto row) { return row; })),
  });
  const auto estimate = input->usedSize();
  ASSERT_GT(estimate, 0);
  ASSERT_LT(estimate, kBlockBytes);
  const BmRowLayout layout(types, nullable, 0, kBlockBytes);
  const auto check = [&](uint64_t target, bool expectWholeBatch) {
    BmRowContainer container(
        types,
        nullable,
        0,
        bufferManager_,
        MemoryTag::kTesting,
        kBlockBytes,
        kBlockBytes,
        nullptr,
        target);
    std::vector<char*> rows;
    container.appendBatch(input, kDefaultPartition, &rows);
    ASSERT_EQ(kRows, rows.size());
    const auto offset = layout.column(0).offset;
    const auto* previous =
        reinterpret_cast<const StringView*>(rows[31] + offset);
    const auto* next = reinterpret_cast<const StringView*>(rows[32] + offset);
    // Whole-batch writes all of column 0 before column 1; pieces interleave
    // the columns. Both layouts must read back identical values.
    EXPECT_EQ(
        expectWholeBatch, previous->data() + previous->size() == next->data());
    for (column_index_t column = 0; column < types.size(); ++column) {
      auto output = BaseVector::create(types[column], kRows, pool());
      container.extractColumnResident(rows.data(), rows.size(), column, output);
      test::assertEqualVectors(input->childAt(column), output);
    }
  };
  check(estimate + 1, true);
  check(estimate, false);
}

TEST_F(BmRowContainerTest, BatchEstimateUsesPiecesForStringDictionary) {
  constexpr vector_size_t kRows = 64;
  constexpr uint32_t kBlockBytes = 16384;
  const std::vector<TypePtr> types{VARCHAR(), VARCHAR()};
  const std::vector<bool> nullable{false, false};
  auto values =
      makeFlatVector<std::string>({std::string(32, 'a'), std::string(32, 'b')});
  std::vector<vector_size_t> indices(kRows);
  for (auto i = 0; i < kRows; ++i) {
    indices[i] = i % 2;
  }
  auto input = makeRowVector({
      BaseVector::wrapInConstant(kRows, 0, values),
      wrapInDictionary(makeIndices(indices), values),
  });
  BmRowContainer container(
      types,
      nullable,
      0,
      bufferManager_,
      MemoryTag::kTesting,
      kBlockBytes,
      kBlockBytes,
      nullptr,
      1 << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(kRows, rows.size());
  const BmRowLayout layout(types, nullable, 0, kBlockBytes);
  const auto offset = layout.column(0).offset;
  const auto* previous = reinterpret_cast<const StringView*>(rows[31] + offset);
  const auto* next = reinterpret_cast<const StringView*>(rows[32] + offset);
  EXPECT_NE(previous->data() + previous->size(), next->data());
  for (column_index_t column = 0; column < 2; ++column) {
    auto output = BaseVector::create(VARCHAR(), kRows, pool());
    container.extractColumnResident(rows.data(), rows.size(), column, output);
    test::assertEqualVectors(input->childAt(column), output);
  }
}

TEST_F(BmRowContainerTest, BatchEstimateHandlesUnloadedLazyInput) {
  constexpr vector_size_t kRows = 64;
  constexpr uint32_t kBlockBytes = 16384;
  auto lazy = makeLazyFlatVector<std::string>(
      kRows, [](auto) { return std::string(32, 'l'); });
  auto other = makeFlatVector<std::string>(
      kRows, [](auto) { return std::string(32, 'r'); });
  auto input = makeRowVector({lazy, other});
  ASSERT_FALSE(lazy->isLoaded());
  BmRowContainer container(
      {VARCHAR(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      kBlockBytes,
      kBlockBytes,
      nullptr,
      1 << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_TRUE(lazy->isLoaded());
  ASSERT_EQ(kRows, rows.size());
  const BmRowLayout layout(
      {VARCHAR(), VARCHAR()}, {false, false}, 0, kBlockBytes);
  const auto offset = layout.column(0).offset;
  const auto* previous = reinterpret_cast<const StringView*>(rows[31] + offset);
  const auto* next = reinterpret_cast<const StringView*>(rows[32] + offset);
  // An incomplete estimate chooses pieces even though the target is large.
  EXPECT_NE(previous->data() + previous->size(), next->data());
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(lazy->loadedVectorShared(), result);
}

TEST_F(BmRowContainerTest, BatchEstimateHandlesDictionaryOverLazyInput) {
  constexpr vector_size_t kRows = 160;
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "dictionary-lazy-wide-strings",
      1ULL << 30,
      memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  auto lazy = makeLazyFlatVector<std::string>(
      kRows, [](auto) { return std::string(1 << 20, 'l'); });
  auto dictionary = wrapInDictionary(makeIndicesInReverse(kRows), lazy);
  auto input = makeRowVector({dictionary});
  ASSERT_FALSE(lazy->isLoaded());
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_TRUE(lazy->isLoaded());
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  EXPECT_EQ(
      (std::vector<size_t>{128, 32}), chunkRowCounts(session.listRowIds()));
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(dictionary, result);
}

TEST_F(BmRowContainerTest, BatchStoreSkewedDictionaryStrings) {
  constexpr vector_size_t kRows = 256;
  auto base = makeFlatVector<std::string>(1024, [](auto row) {
    return row == 0 ? std::string(64 << 10, 'd') : std::string("x");
  });
  auto dictionary =
      wrapInDictionary(makeIndices(std::vector<vector_size_t>(kRows, 0)), base);
  auto input = makeRowVector({dictionary});
  ASSERT_LT(input->usedSize(), 2ULL << 20);
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      16 << 10,
      1 << 20,
      nullptr,
      2ULL << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(kRows, rows.size());
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(dictionary, result);
}

TEST_F(BmRowContainerTest, SkewedDictionarySealsAtHeapTarget) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "dictionary-skewed-wide-strings",
      1ULL << 30,
      memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  constexpr vector_size_t kRows = 256;
  constexpr uint32_t kStringBytes = 1 << 20;
  auto base = makeFlatVector<std::string>(1024, [](auto row) {
    return row == 0 ? std::string(kStringBytes, 'd') : std::string("x");
  });
  auto dictionary =
      wrapInDictionary(makeIndices(std::vector<vector_size_t>(kRows, 0)), base);
  auto input = makeRowVector({dictionary});
  ASSERT_LT(input->usedSize(), 128ULL << 20);

  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  container.appendBatch(input);
  const auto segment = container.spillActiveSegment();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto rowIds = session.listRowIds();
  ASSERT_EQ(kRows, rowIds.size());
  EXPECT_GT(chunkRowCounts(rowIds).size(), 1);

  auto firstLoad = session.loadRowsScoped({rowIds.data(), rowIds.size()});
  firstLoad.reset();
  memory::MemoryReclaimer::Stats reclaimStats;
  memory::ScopedMemoryArbitrationContext arbitrationContext(root_.get());
  ASSERT_GT(
      root_->reclaim(bufferManager_->reclaimableBytes(), 0, reclaimStats), 0);
  const auto beforeRead = bufferManager_->stats().spillReadCount;
  auto loaded = session.loadRowsScoped({rowIds.data(), 1});
  EXPECT_GT(bufferManager_->stats().spillReadCount, beforeRead);
  auto result = BaseVector::create(VARCHAR(), 1, pool());
  container.extractColumnResident(loaded.data(), 1, 0, result);
  EXPECT_EQ(
      result->asFlatVector<StringView>()->valueAt(0).str(),
      std::string(kStringBytes, 'd'));
}

TEST_F(BmRowContainerTest, BatchEstimateUsesFlattenedStringStats) {
  constexpr vector_size_t kRows = 256;
  auto base = makeFlatVector<std::string>({std::string(64 << 10, 'd')});
  VectorPtr values =
      wrapInDictionary(makeIndices(std::vector<vector_size_t>(kRows, 0)), base);
  BaseVector::flattenVector(values);
  ASSERT_TRUE(values->asFlatVector<StringView>()->stringStats().has_value());
  auto input = makeRowVector({values});
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      16 << 10,
      1 << 20,
      nullptr,
      2ULL << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  EXPECT_GT(chunkRowCounts(session.listRowIds()).size(), 1);
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(values, result);
}

TEST_F(BmRowContainerTest, BatchStoreSharedStringBuffers) {
  constexpr vector_size_t kRows = 256;
  auto base = makeFlatVector<std::string>({std::string(64 << 10, 's')});
  auto shared = BaseVector::create(VARCHAR(), kRows, pool());
  auto* flat = shared->asFlatVector<StringView>();
  flat->acquireSharedStringBuffers(base.get());
  const auto value = base->asFlatVector<StringView>()->valueAt(0);
  for (vector_size_t row = 0; row < kRows; ++row) {
    flat->setNoCopy(row, value);
  }
  ASSERT_FALSE(flat->stringStats().has_value());
  auto input = makeRowVector({shared});
  ASSERT_LT(input->usedSize(), 2ULL << 20);
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      16 << 10,
      1 << 20,
      nullptr,
      2ULL << 20);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  ASSERT_EQ(kRows, rows.size());
  auto result = BaseVector::create(VARCHAR(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(shared, result);
}

TEST_F(BmRowContainerTest, BatchHeapTargetCutsOnlyPastTheTarget) {
  constexpr uint32_t kHeapBlockSize = 1024;
  auto input = makeRowVector({
      makeFlatVector<std::string>(
          65, [](auto) { return std::string(64, 'a'); }),
      makeFlatVector<std::string>(
          65, [](auto) { return std::string(64, 'b'); }),
  });
  // Each column fits 16 values per block and needs five blocks for 65 rows.
  // The fifth pair opens at row 64 and brings the chunk to exactly 10 blocks.
  constexpr uint64_t kBatchHeapBytes = 10 * kHeapBlockSize;
  const auto exercise = [&](uint64_t target,
                            BmBatchStringStoreMode mode,
                            const std::vector<size_t>& expectedChunkRows) {
    SCOPED_TRACE(
        fmt::format("target={} mode={}", target, static_cast<int>(mode)));
    BmRowContainer container(
        {VARCHAR(), VARCHAR()},
        {false, false},
        0,
        bufferManager_,
        MemoryTag::kTesting,
        16384,
        kHeapBlockSize,
        nullptr,
        target);
    std::vector<char*> rows;
    container.appendBatch(input, kDefaultPartition, &rows, mode);
    const auto segment = container.activeSegmentId();
    auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
    EXPECT_EQ(expectedChunkRows, chunkRowCounts(session.listRowIds()));
    for (column_index_t column = 0; column < 2; ++column) {
      auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
      container.extractColumnResident(rows.data(), rows.size(), column, result);
      test::assertEqualVectors(input->childAt(column), result);
    }
  };
  exercise(kBatchHeapBytes, BmBatchStringStoreMode::kCopy, {65});
  // A complete final piece may cross the soft target.
  exercise(kBatchHeapBytes - 1, BmBatchStringStoreMode::kCopy, {65});
  // Referenced strings allocate no heap, so even a tiny target never cuts.
  exercise(1, BmBatchStringStoreMode::kReferenceInputStringForBenchmark, {65});
}

TEST_F(BmRowContainerTest, BatchHeapEstimateStartsFreshAfterSealing) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "fresh-chunk-estimate", 1ULL << 30, memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  BmRowContainer container(
      {VARCHAR(), VARCHAR()},
      {false, false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  auto large = makeFlatVector<std::string>({std::string(1 << 20, 'x')});
  auto small = makeFlatVector<std::string>({std::string(64, 'y')});
  container.appendBatch(makeRowVector({
      BaseVector::wrapInConstant(128, 0, large),
      BaseVector::wrapInConstant(128, 0, small),
  }));

  auto input = makeRowVector({
      makeFlatVector<std::string>(
          65, [](auto) { return std::string(64, 'a'); }),
      makeFlatVector<std::string>(
          65, [](auto) { return std::string(64, 'b'); }),
  });
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(193, ids.size());
  EXPECT_NE(ids[127].rowBlockId, ids[128].rowBlockId);
  // The small follow-up batch should fit in the new chunk.
  EXPECT_EQ(ids[128].rowBlockId, ids[192].rowBlockId);
  for (column_index_t column = 0; column < 2; ++column) {
    auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
    container.extractColumnResident(rows.data(), rows.size(), column, result);
    test::assertEqualVectors(input->childAt(column), result);
  }
}

TEST_F(BmRowContainerTest, BatchHeapEstimateHandlesSuddenWideFlatInput) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "large-batch-estimate", 1ULL << 30, memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  auto narrow = makeRowVector({makeFlatVector<std::string>(
      128, [](auto) { return std::string(64, 'n'); })});
  auto wide = makeRowVector({makeFlatVector<std::string>(
      65, [](auto row) { return std::string(2 << 20, 'a' + row % 26); })});
  std::vector<char*> rows;
  container.appendBatch(narrow, kDefaultPartition, &rows);
  container.appendBatch(wide, kDefaultPartition, &rows);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(193, ids.size());
  EXPECT_NE(ids.front().rowBlockId, ids.back().rowBlockId);
  auto result = BaseVector::create(VARCHAR(), 65, pool());
  container.extractColumnResident(rows.data() + 128, 65, 0, result);
  test::assertEqualVectors(wide->childAt(0), result);
}

TEST_F(BmRowContainerTest, BatchHeapEstimateHandlesWidthChangeWithinBatch) {
  constexpr vector_size_t kNarrowRows = 32;
  constexpr vector_size_t kWideRows = 64;
  constexpr uint64_t kTargetBytes = 8ULL << 20;
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "batch-width-change", 1ULL << 30, memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  auto values =
      makeFlatVector<std::string>(kNarrowRows + kWideRows, [](auto row) {
        return row < kNarrowRows ? std::string(64, 'n')
                                 : std::string(1 << 20, 'w');
      });
  auto input = makeRowVector({values});
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      kTargetBytes);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(kNarrowRows + kWideRows, ids.size());
  const auto chunks = chunkRowCounts(ids);
  EXPECT_GT(chunks.size(), 1);
  // The soft target permits the last piece to overshoot. The wide run must
  // still not accumulate into one chunk after the width change.
  size_t wideInChunk = 0;
  for (vector_size_t i = kNarrowRows; i < ids.size(); ++i) {
    if (i == kNarrowRows || ids[i].rowBlockId != ids[i - 1].rowBlockId) {
      wideInChunk = 0;
    }
    EXPECT_LE(++wideInChunk, 40);
  }
  auto result = BaseVector::create(VARCHAR(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(values, result);
}

TEST_F(BmRowContainerTest, BatchHeapTargetSealsAfterFixed32RowPiece) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "fixed-piece-wide-strings",
      1ULL << 30,
      memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);

  // The maximum object_data width observed in the 20261024 case: three
  // values fit in a 4 MiB heap block. Prime with 31 rows so the 128 MiB
  // threshold is crossed within, rather than at the start of, a piece.
  auto value = makeFlatVector<std::string>({std::string(1'121'206, 'x')});
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  container.appendBatch(
      makeRowVector({BaseVector::wrapInConstant(31, 0, value)}));
  container.appendBatch(
      makeRowVector({BaseVector::wrapInConstant(128, 0, value)}));

  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(159, ids.size());
  EXPECT_EQ((std::vector<size_t>{95, 64}), chunkRowCounts(ids));
  std::vector<RowId> selected{ids[0], ids[94], ids[95], ids[158]};
  auto loaded = session.loadRowsScoped({selected.data(), selected.size()});
  auto result = BaseVector::create(VARCHAR(), selected.size(), pool());
  container.extractColumnResident(
      loaded.data(), selected.size(), 0, result, true);
  test::assertEqualVectors(
      BaseVector::wrapInConstant(selected.size(), 0, value), result);
}

TEST_F(BmRowContainerTest, BatchHeapEstimateAccountsForRepeatedString) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "large-batch-estimate", 1ULL << 30, memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  auto value = makeFlatVector<std::string>({std::string(1 << 20, 'x')});
  auto input = makeRowVector({BaseVector::wrapInConstant(512, 0, value)});
  ASSERT_LT(input->usedSize(), 128ULL << 20);
  RecordProperty("used_bytes", std::to_string(input->usedSize()));
  RecordProperty(
      "estimated_flat_bytes", std::to_string(input->estimateFlatSize()));
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  const auto before = bufferManager_->stats();
  container.appendBatch(input);
  const auto stored = bufferManager_->stats();
  const auto residentGrowth = stored.pinnedResidentBytes +
      stored.unpinnedResidentBytes - before.pinnedResidentBytes -
      before.unpinnedResidentBytes;
  // Multiple chunks are expected; a complete piece can cross the target.
  EXPECT_GE(residentGrowth, 528ULL << 20);
  EXPECT_LE(residentGrowth, 552ULL << 20);
  RecordProperty("resident_bytes", std::to_string(residentGrowth));
  auto tail =
      makeRowVector({makeFlatVector<std::string>({std::string(64, 't')})});
  container.appendBatch(tail);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(513, ids.size());
  const auto chunkRows = chunkRowCounts(ids);
  EXPECT_GE(chunkRows.size(), 4);
  for (const auto count : chunkRows) {
    EXPECT_LE(count, 132);
  }
}

TEST_F(BmRowContainerTest, BatchHeapEstimateAccountsForRepeatedArray) {
  bufferManager_.reset();
  root_ = memory::memoryManager()->addRootPool(
      "large-array-estimate", 1ULL << 30, memory::MemoryReclaimer::create());
  resetBufferManagerCompression(memory::bm::compress::CompressionKind::kNone);
  auto value = makeArrayVector<int64_t>({std::vector<int64_t>(65536, 42)});
  auto repeated = BaseVector::wrapInConstant(512, 0, value);
  auto input = makeRowVector({repeated});
  ASSERT_LT(input->usedSize(), 128ULL << 20);
  BmRowContainer container(
      {value->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4 << 20,
      4 << 20,
      nullptr,
      128ULL << 20);
  container.appendBatch(input);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(512, ids.size());
  // Each serialized array has 512 KiB of values plus its null bitmap; seven
  // fit in a 4 MiB heap block. The 32-row boundaries seal at rows 224 and 448.
  EXPECT_EQ(ids[0].rowBlockId, ids[223].rowBlockId);
  EXPECT_NE(ids[223].rowBlockId, ids[224].rowBlockId);
  EXPECT_EQ(ids[224].rowBlockId, ids[447].rowBlockId);
  EXPECT_NE(ids[447].rowBlockId, ids[448].rowBlockId);
  EXPECT_EQ(ids[448].rowBlockId, ids[511].rowBlockId);
  std::vector<RowId> selected{
      ids[0], ids[223], ids[224], ids[447], ids[448], ids[511]};
  auto loaded = session.loadRowsScoped({selected.data(), selected.size()});
  auto result = BaseVector::create(value->type(), selected.size(), pool());
  container.extractColumnResident(loaded.data(), selected.size(), 0, result);
  test::assertEqualVectors(
      BaseVector::wrapInConstant(selected.size(), 0, value), result);
}

TEST_F(BmRowContainerTest, BatchHeapEstimateExpandsNestedRepeatedConstants) {
  constexpr vector_size_t kRows = 128;
  auto value = makeFlatVector<std::string>({std::string(4096, 'x')});
  auto elements = BaseVector::wrapInConstant(8, 0, value);
  auto array = makeArrayVector(std::vector<vector_size_t>{0}, elements);
  auto repeated = BaseVector::wrapInConstant(kRows, 0, array);
  auto input = makeRowVector({repeated});
  BmRowContainer container(
      {array->type()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      16 << 10,
      64 << 10,
      nullptr,
      256 << 10);
  std::vector<char*> rows;
  container.appendBatch(input, kDefaultPartition, &rows);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto ids = session.listRowIds();
  ASSERT_EQ(kRows, ids.size());
  EXPECT_GT(chunkRowCounts(ids).size(), 1);
  auto result = BaseVector::create(array->type(), kRows, pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, result);
  test::assertEqualVectors(repeated, result);
}

TEST_F(BmRowContainerTest, BatchHeapTargetAllowsOversizedSingleValue) {
  const std::string oversized(400, 'z');
  auto input = makeRowVector({makeFlatVector<std::string>({oversized})});
  BmRowContainer container(
      {VARCHAR()},
      {false},
      0,
      bufferManager_,
      MemoryTag::kTesting,
      4096,
      64,
      nullptr,
      128);

  container.appendBatch(input);
  const auto segment = container.activeSegmentId();
  auto session = container.beginReadOnlyWindowReadSegments({&segment, 1});
  const auto rowIds = session.listRowIds();
  ASSERT_EQ(1, rowIds.size());
  auto loaded = session.loadRowsScoped({rowIds.data(), rowIds.size()});
  auto result = BaseVector::create(VARCHAR(), 1, pool());
  container.extractColumnResident(loaded.data(), 1, 0, result, true);
  auto* flatResult = result->asFlatVector<StringView>();
  ASSERT_NE(nullptr, flatResult);
  EXPECT_EQ(oversized, flatResult->valueAt(0).str());
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
      64,
      nullptr,
      256);
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
  auto rows = session.loadRowsScoped({rowIds.data(), rowIds.size()});
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
