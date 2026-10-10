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

#include <cmath>
#include <optional>
#include <vector>

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, StoresComplexKeyAndPayloadColumns) {
  auto keyMaps = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {1, 10}},
      {{1, 10}, {2, 20}},
  });
  auto payloadMaps = makeMapVector<int64_t, int64_t>({
      {{2, 20}, {1, 10}},
      {{1, 10}, {2, 20}},
  });
  auto arrays = makeNullableArrayVector<double>(
      std::vector<std::vector<std::optional<double>>>{
          {1.0, std::nullopt, std::nan("")}, {}});
  auto input = makeRowVector({keyMaps, payloadMaps, arrays});
  BmRowContainer container(
      {keyMaps->type(), payloadMaps->type(), arrays->type()},
      {false, false, false},
      1,
      bufferManager_,
      MemoryTag::kTesting);

  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());

  auto keyResult = BaseVector::create(keyMaps->type(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 0, keyResult, true);
  auto payloadResult =
      BaseVector::create(payloadMaps->type(), rows.size(), pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 1, payloadResult, true);
  auto arrayResult = BaseVector::create(arrays->type(), rows.size(), pool());
  container.extractColumnResident(
      rows.data(), rows.size(), 2, arrayResult, true);

  auto* keyResultMap = keyResult->as<MapVector>();
  auto* payloadResultMap = payloadResult->as<MapVector>();
  ASSERT_NE(nullptr, keyResultMap);
  ASSERT_NE(nullptr, payloadResultMap);
  EXPECT_EQ(1, keyResultMap->mapKeys()->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(
      2, payloadResultMap->mapKeys()->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(
      1, payloadResultMap->mapKeys()->asFlatVector<int64_t>()->valueAt(2));
  test::assertEqualVectors(arrays, arrayResult);

  EXPECT_EQ(0, container.compare(rows[0], rows[1], 0));
  EXPECT_EQ(container.hash(rows[0], 0), container.hash(rows[1], 0));

  std::vector<char> rowCopy;
  std::vector<char> variableCopy;
  const std::vector<int32_t> deepColumns{0, 1, 2};
  container.copyRowWithDeepColumns(
      rows[0],
      folly::Range<const int32_t*>(deepColumns.data(), deepColumns.size()),
      rowCopy,
      variableCopy);
  const char* copiedRow = rowCopy.data();
  auto copiedArray = BaseVector::create(arrays->type(), 1, pool());
  container.extractColumnResident(&copiedRow, 1, 2, copiedArray, true);
  const char* originalRow = rows[0];
  auto expectedArray = BaseVector::create(arrays->type(), 1, pool());
  container.extractColumnResident(&originalRow, 1, 2, expectedArray, true);
  test::assertEqualVectors(expectedArray, copiedArray);
}

TEST_F(BmRowContainerTest, ComplexArrayAndRowCompareHashNestedValues) {
  auto arrays = makeNullableArrayVector<double>(
      std::vector<std::vector<std::optional<double>>>{
          {1.0, std::nullopt, std::nan("")},
          {1.0, std::nullopt, std::nan("")}});
  auto nestedRows = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
      arrays,
  });
  auto input = makeRowVector({arrays, nestedRows});
  BmRowContainer container(
      {arrays->type(), nestedRows->type()},
      {false, false},
      2,
      bufferManager_,
      MemoryTag::kTesting);

  auto rows = storeAll(container, input);
  ASSERT_EQ(2, rows.size());
  for (int32_t column = 0; column < 2; ++column) {
    EXPECT_EQ(0, container.compare(rows[0], rows[1], column));
    EXPECT_EQ(container.hash(rows[0], column), container.hash(rows[1], column));
  }

  auto result = BaseVector::create(nestedRows->type(), rows.size(), pool());
  container.extractColumnResident(rows.data(), rows.size(), 1, result, true);
  test::assertEqualVectors(nestedRows, result);
}

TEST_F(BmRowContainerTest, InlineComplexValuesRemainValidDuringCompareAndHash) {
  auto nestedRows = makeRowVector({makeFlatVector<int8_t>({1, 2, 1})});
  const ContainerRowSerdeOptions options{.isKey = true};
  for (vector_size_t row = 0; row < nestedRows->size(); ++row) {
    ASSERT_LE(
        BmContainerRowSerde::serializedSize(*nestedRows, row, options),
        StringView::kInlineSize);
  }
  auto input = makeRowVector({nestedRows});
  BmRowContainer container(
      {nestedRows->type()}, {false}, 1, bufferManager_, MemoryTag::kTesting);

  auto rows = storeAll(container, input);
  ASSERT_EQ(3, rows.size());
  BmRowLayout layout(
      {nestedRows->type()},
      {false},
      1,
      static_cast<uint32_t>(
          memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)));
  for (const auto* row : rows) {
    EXPECT_TRUE(reinterpret_cast<const StringView*>(layout.valueAddress(row, 0))
                    ->isInline());
  }

  EXPECT_LT(container.compare(rows[0], rows[1], 0), 0);
  EXPECT_EQ(0, container.compare(rows[0], rows[2], 0));
  EXPECT_NE(container.hash(rows[0], 0), container.hash(rows[1], 0));
  EXPECT_EQ(container.hash(rows[0], 0), container.hash(rows[2], 0));
}

TEST_F(BmRowContainerTest, ComplexCompareHonorsFlags) {
  auto arrays = makeNullableArrayVector<int8_t>({
      {std::nullopt},
      {1},
      {1, 2},
      {1, 3},
  });
  BmRowContainer container(
      {arrays->type()}, {false}, 1, bufferManager_, MemoryTag::kTesting);
  auto rows = storeAll(container, makeRowVector({arrays}));

  CompareFlags nullsLast;
  nullsLast.nullsFirst = false;
  EXPECT_GT(container.compare(rows[0], rows[1], 0, nullsLast), 0);

  CompareFlags descending;
  descending.ascending = false;
  EXPECT_GT(container.compare(rows[2], rows[3], 0, descending), 0);
}

TEST_F(BmRowContainerTest, ResidentStoreCompareAndExtract) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, false},
      0,
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
      0,
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
      0,
      bufferManager_,
      MemoryTag::kTesting);
  auto context = container.appendRow();

  ASSERT_NE(nullptr, context.segment());
  ASSERT_NE(nullptr, context.chunk());
  EXPECT_EQ(context.segment()->meta.id, context.chunk()->meta.segmentId);
  EXPECT_EQ(context.row(), context.chunk()->rowBlock.ptr);
}

TEST_F(BmRowContainerTest, RowLayoutMatchesOldRowContainerPacking) {
  BmRowLayout layout({BIGINT(), INTEGER()}, {true, false}, 0, 4 << 20);

  EXPECT_EQ(1, layout.column(0).offset);
  EXPECT_EQ(9, layout.column(1).offset);
  EXPECT_EQ(13, layout.rowSize());
}

TEST_F(BmRowContainerTest, RowLayoutBuildsTypedFixedRowBlockDescriptor) {
  BmRowLayout layout(
      {BOOLEAN(),
       TINYINT(),
       SMALLINT(),
       INTEGER(),
       BIGINT(),
       REAL(),
       DOUBLE(),
       VARCHAR(),
       HUGEINT()},
      {true, false, false, false, false, false, false, false, false},
      0,
      4 << 20);

  auto descriptor = layout.makeBlockDescriptor(17);

  ASSERT_NE(nullptr, descriptor);
  EXPECT_EQ(memory::bm::BlockSchemaKind::kFixedRow, descriptor->schemaKind);
  EXPECT_EQ(17, descriptor->elementCount);
  const auto& schema =
      std::get<memory::bm::FixedRowBlockSchema>(descriptor->schema);
  EXPECT_EQ(layout.rowSize(), schema.rowStride);
  ASSERT_EQ(10, schema.fields.size());
  EXPECT_EQ(memory::bm::BlockFieldKind::kOpaque, schema.fields[0].kind);
  EXPECT_EQ(0, schema.fields[0].offset);
  EXPECT_EQ(1, schema.fields[0].width);
  EXPECT_EQ(
      memory::bm::BlockFieldKind::kUnsignedInteger, schema.fields[1].kind);
  for (size_t i = 2; i <= 5; ++i) {
    EXPECT_EQ(memory::bm::BlockFieldKind::kSignedInteger, schema.fields[i].kind)
        << "field=" << i;
  }
  EXPECT_EQ(memory::bm::BlockFieldKind::kFloatingPoint, schema.fields[6].kind);
  EXPECT_EQ(memory::bm::BlockFieldKind::kFloatingPoint, schema.fields[7].kind);
  EXPECT_EQ(memory::bm::BlockFieldKind::kOpaque, schema.fields[8].kind);
  EXPECT_EQ(memory::bm::BlockFieldKind::kOpaque, schema.fields[9].kind);
}

TEST_F(BmRowContainerTest, RowLayoutInitializesOnlyNulls) {
  {
    BmRowLayout layout({BIGINT(), INTEGER()}, {false, false}, 0, 4 << 20);
    std::vector<char> row(layout.rowSize(), static_cast<char>(0x7f));

    layout.initializeNulls(row.data());

    for (auto byte : row) {
      EXPECT_EQ(static_cast<char>(0x7f), byte);
    }
  }

  {
    BmRowLayout layout({BIGINT(), VARCHAR()}, {true, false}, 0, 4 << 20);
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
      0,
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
      0,
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

TEST_F(BmRowContainerDiskIoTest, NullableStringNullSurvivesSpillRead) {
  BmRowContainer container(
      {BIGINT(), VARCHAR()},
      {false, true},
      0,
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

TEST_F(BmRowContainerTest, RejectsTooManyKeyColumns) {
  EXPECT_THROW(
      BmRowContainer(
          {ARRAY(BIGINT())}, {false}, 2, bufferManager_, MemoryTag::kTesting),
      BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
