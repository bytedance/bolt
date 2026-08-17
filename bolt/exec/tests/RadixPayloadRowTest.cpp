/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/PayloadRowReader.h"
#include "bolt/exec/radixsort/PayloadRowWriter.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

class RadixPayloadRowTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-payload-test")};

  template <typename T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<T>>& values) {
    auto vector =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, *values[row]);
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const TypePtr& type,
      const std::vector<std::optional<std::string>>& values) {
    auto vector = BaseVector::create<FlatVector<StringView>>(
        type, values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, StringView(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  VectorPtr makeUnknownVector(vector_size_t size) {
    auto vector = BaseVector::create(UNKNOWN(), size, pool_.get());
    for (vector_size_t row = 0; row < size; ++row) {
      vector->setNull(row, true);
    }
    return vector;
  }

  RowVectorPtr makeRows(const std::vector<VectorPtr>& children) {
    std::vector<TypePtr> types;
    types.reserve(children.size());
    for (const auto& child : children) {
      types.push_back(child->type());
    }
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW(std::move(types)),
        nullptr,
        children.empty() ? 0 : children.front()->size(),
        children);
  }

  ArrayVectorPtr makeArrays() {
    const std::array<vector_size_t, 4> offsets{0, 2, 4, 4};
    const std::array<vector_size_t, 4> sizes{2, 2, 0, 0};
    auto offsetBuffer =
        AlignedBuffer::allocate<vector_size_t>(offsets.size(), pool_.get());
    auto sizeBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool_.get());
    std::memcpy(
        offsetBuffer->asMutable<vector_size_t>(),
        offsets.data(),
        sizeof(offsets));
    std::memcpy(
        sizeBuffer->asMutable<vector_size_t>(), sizes.data(), sizeof(sizes));
    auto result = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        makeVector<int32_t>(INTEGER(), {1, std::nullopt, 3, 4}));
    result->setNull(3, true);
    return result;
  }

  RowVectorPtr makeNestedRows() {
    auto result = std::make_shared<RowVector>(
        pool_.get(),
        ROW({"number", "text"}, {BIGINT(), VARCHAR()}),
        nullptr,
        4,
        std::vector<VectorPtr>{
            makeVector<int64_t>(BIGINT(), {1, 2, 3, 4}),
            makeStringVector(
                VARCHAR(),
                {std::string(80, 'a'),
                 std::nullopt,
                 std::string("short"),
                 std::string(31, 'd')})});
    result->setNull(2, true);
    return result;
  }

  MapVectorPtr makeMaps() {
    const std::array<vector_size_t, 4> offsets{0, 2, 4, 4};
    const std::array<vector_size_t, 4> sizes{2, 2, 0, 0};
    auto offsetBuffer =
        AlignedBuffer::allocate<vector_size_t>(offsets.size(), pool_.get());
    auto sizeBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool_.get());
    std::memcpy(
        offsetBuffer->asMutable<vector_size_t>(),
        offsets.data(),
        sizeof(offsets));
    std::memcpy(
        sizeBuffer->asMutable<vector_size_t>(), sizes.data(), sizeof(sizes));
    auto result = std::make_shared<MapVector>(
        pool_.get(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        makeVector<int32_t>(INTEGER(), {2, 1, 4, 3}),
        makeStringVector(
            VARCHAR(),
            {std::string(48, 'b'),
             std::string("one"),
             std::string("four"),
             std::string(37, 'c')}));
    result->setNull(3, true);
    return result;
  }

  RowVectorPtr makeTimestampWithTimeZones() {
    auto result = std::make_shared<RowVector>(
        pool_.get(),
        TIMESTAMP_WITH_TIME_ZONE(),
        nullptr,
        4,
        std::vector<VectorPtr>{
            makeVector<int64_t>(
                BIGINT(),
                {int64_t{0}, int64_t{123456789}, int64_t{-1}, int64_t{42}}),
            makeVector<int16_t>(
                SMALLINT(),
                {int16_t{1}, int16_t{840}, int16_t{1680}, int16_t{7}})});
    result->setNull(2, true);
    return result;
  }

  RowVectorPtr makeNestedVariants() {
    auto variants = VariantVector::create(pool_.get(), VARIANT(), 4);
    auto* values =
        variants->valueChildVector()->asUnchecked<FlatVector<StringView>>();
    auto* metadata =
        variants->metadataChildVector()->asUnchecked<FlatVector<StringView>>();
    const std::string longValue(48, 'v');
    const std::string longMetadata(36, 'm');
    values->set(0, StringView("short"));
    metadata->set(0, StringView("meta"));
    values->set(1, StringView(longValue));
    metadata->set(1, StringView(longMetadata));
    variants->setNull(2, true);
    values->set(3, StringView());
    metadata->set(3, StringView());
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW({"variant"}, {VARIANT()}),
        nullptr,
        4,
        std::vector<VectorPtr>{variants});
  }

  static RadixSortKeyLayout keyLayout() {
    return RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  }

  static std::shared_ptr<const PayloadRowLayout> payloadLayout(
      const RowTypePtr& rowType) {
    auto layout = PayloadRowLayout::create(rowType);
    EXPECT_NE(layout, nullptr);
    return layout;
  }

  static void expectEquivalent(
      const RowVector& expected,
      const RowVector& actual) {
    ASSERT_EQ(expected.size(), actual.size());
    ASSERT_EQ(expected.childrenSize(), actual.childrenSize());
    const CompareFlags flags{
        .nullsFirst = true,
        .ascending = true,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
    for (uint32_t column = 0; column < expected.childrenSize(); ++column) {
      for (vector_size_t row = 0; row < expected.size(); ++row) {
        const auto result = expected.childAt(column)->compare(
            actual.childAt(column).get(), row, row, flags);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 0) << "column=" << column << ", row=" << row;
      }
    }
  }

  static std::vector<char*> rowPointers(const PayloadRowBatch& batch) {
    std::vector<char*> rows(batch.size());
    for (vector_size_t row = 0; row < batch.size(); ++row) {
      rows[row] = batch.rowAt(row);
    }
    return rows;
  }

  static void gatherPayloadBatch(
      const PayloadRowLayout& layout,
      const PayloadRowBatch& batch,
      memory::MemoryPool* pool,
      RowVectorPtr& result) {
    auto rows = rowPointers(batch);
    PayloadRowReader::gather(
        layout, std::span<char* const>(rows), pool, result);
  }
};

TEST_F(RadixPayloadRowTest, packedLayoutHasNoPadding) {
  auto rowType =
      ROW({"tiny",
           "big",
           "string",
           "integer",
           "timestamp",
           "huge",
           "decimal",
           "binary",
           "boolean",
           "unknown"},
          {TINYINT(),
           BIGINT(),
           VARCHAR(),
           INTEGER(),
           TIMESTAMP(),
           HUGEINT(),
           DECIMAL(18, 4),
           VARBINARY(),
           BOOLEAN(),
           UNKNOWN()});
  auto layout = payloadLayout(rowType);

  EXPECT_EQ(layout->nullBytes(), 2);
  ASSERT_TRUE(layout->variableSizeOffset().has_value());
  EXPECT_EQ(*layout->variableSizeOffset(), 2);
  const std::array<uint64_t, 10> expectedOffsets{
      10, 11, 19, 35, 39, 55, 71, 79, 95, 96};
  const std::array<uint32_t, 10> expectedWidths{
      1, 8, 16, 4, 16, 16, 8, 16, 1, 0};
  ASSERT_EQ(layout->columns().size(), expectedOffsets.size());
  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    EXPECT_EQ(layout->columns()[column].offset, expectedOffsets[column]);
    EXPECT_EQ(layout->columns()[column].width, expectedWidths[column]);
    EXPECT_EQ(layout->columns()[column].nullByte, column / 8);
    EXPECT_EQ(
        layout->columns()[column].nullMask,
        static_cast<uint8_t>(1U << (column % 8)));
  }
  EXPECT_EQ(layout->rowWidth(), 96);
  EXPECT_NE(layout->columns()[1].offset % alignof(int64_t), 0);
  EXPECT_NE(layout->columns()[2].offset % alignof(StringView), 0);
  EXPECT_NE(layout->columns()[4].offset % alignof(Timestamp), 0);
  EXPECT_NE(layout->columns()[5].offset % alignof(int128_t), 0);
  EXPECT_EQ(
      layout->rowWidth(),
      layout->nullBytes() + sizeof(uint64_t) +
          std::accumulate(
              expectedWidths.begin(), expectedWidths.end(), uint64_t{0}));

  auto fixedLayout = payloadLayout(
      ROW({"tiny", "big", "timestamp"}, {TINYINT(), BIGINT(), TIMESTAMP()}));
  EXPECT_EQ(fixedLayout->nullBytes(), 1);
  EXPECT_FALSE(fixedLayout->variableSizeOffset().has_value());
  EXPECT_EQ(fixedLayout->columns()[0].offset, 1);
  EXPECT_EQ(fixedLayout->columns()[1].offset, 2);
  EXPECT_EQ(fixedLayout->columns()[2].offset, 10);
  EXPECT_EQ(fixedLayout->rowWidth(), 26);
}

TEST_F(RadixPayloadRowTest, capabilityAndKeyOnlySchema) {
  const std::vector<TypePtr> supported{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      DECIMAL(18, 4),
      DECIMAL(38, 18),
      TIMESTAMP(),
      DATE(),
      INTERVAL_DAY_TIME(),
      INTERVAL_YEAR_MONTH(),
      UNKNOWN(),
      VARCHAR(),
      VARBINARY(),
      JSON(),
      HYPERLOGLOG(),
      TIMESTAMP_WITH_TIME_ZONE(),
      ARRAY(BIGINT()),
      MAP(INTEGER(), ROW({BIGINT(), VARCHAR()})),
      ROW({ARRAY(INTEGER()), MAP(BIGINT(), VARCHAR())})};
  for (const auto& type : supported) {
    EXPECT_TRUE(PayloadRowLayout::supports(*type)) << type->toString();
  }
  EXPECT_FALSE(PayloadRowLayout::supports(*VARIANT()));
  EXPECT_FALSE(PayloadRowLayout::supports(*OPAQUE<int32_t>()));
  EXPECT_FALSE(PayloadRowLayout::supports(*FUNCTION({BIGINT()}, BOOLEAN())));
  EXPECT_TRUE(PayloadRowLayout::supports(*ARRAY(VARIANT())));
  EXPECT_FALSE(PayloadRowLayout::supports(*ROW({BIGINT(), OPAQUE<int32_t>()})));

  auto emptyLayout = PayloadRowLayout::create(
      ROW(std::vector<std::string>{}, std::vector<TypePtr>{}));
  EXPECT_EQ(emptyLayout, nullptr);

  EXPECT_THROW(
      PayloadRowLayout::create(ROW({"opaque"}, {OPAQUE<int32_t>()})),
      BoltException);
}

TEST_F(RadixPayloadRowTest, scalarStringRoundTripAndDeepCopy) {
  uint32_t floatNanBits = 0x7fc12345;
  float floatNan;
  std::memcpy(&floatNan, &floatNanBits, sizeof(floatNan));
  uint64_t doubleNanBits = 0x7ff8123456789abcULL;
  double doubleNan;
  std::memcpy(&doubleNan, &doubleNanBits, sizeof(doubleNan));
  const auto decimal38 =
      HugeInt::fromString("99999999999999999999999999999999999999");
  const std::string invalidUtf8{"\xc3\x28\xff", 3};
  const std::string binary{"\x00\x01\xff", 3};
  const std::string longA(128, 'a');
  const std::string longB(96, 'b');

  auto input = makeRows(
      {makeVector<bool>(BOOLEAN(), {false, true, std::nullopt, true}),
       makeVector<int8_t>(
           TINYINT(),
           {std::numeric_limits<int8_t>::min(),
            0,
            std::numeric_limits<int8_t>::max(),
            std::nullopt}),
       makeVector<int16_t>(
           SMALLINT(),
           {std::numeric_limits<int16_t>::min(),
            0,
            std::numeric_limits<int16_t>::max(),
            std::nullopt}),
       makeVector<int32_t>(
           INTEGER(),
           {std::numeric_limits<int32_t>::min(),
            0,
            std::numeric_limits<int32_t>::max(),
            std::nullopt}),
       makeVector<int64_t>(
           BIGINT(),
           {std::numeric_limits<int64_t>::min(),
            0,
            std::numeric_limits<int64_t>::max(),
            std::nullopt}),
       makeVector<int128_t>(
           HUGEINT(),
           {HugeInt::build(uint64_t{1} << 63, 0),
            static_cast<int128_t>(0),
            HugeInt::build(
                (uint64_t{1} << 63) - 1, std::numeric_limits<uint64_t>::max()),
            std::nullopt}),
       makeVector<float>(REAL(), {-0.0f, 0.0f, floatNan, std::nullopt}),
       makeVector<double>(DOUBLE(), {-0.0, 0.0, doubleNan, std::nullopt}),
       makeVector<int64_t>(
           DECIMAL(18, 4),
           {-999999999999999999LL, 0, 999999999999999999LL, std::nullopt}),
       makeVector<int128_t>(
           DECIMAL(38, 18),
           {-decimal38, static_cast<int128_t>(0), decimal38, std::nullopt}),
       makeVector<Timestamp>(
           TIMESTAMP(),
           {Timestamp::min(), Timestamp(0, 1), Timestamp::max(), std::nullopt}),
       makeStringVector(
           VARCHAR(), {std::string(), invalidUtf8, longA, std::nullopt}),
       makeStringVector(
           VARBINARY(), {binary, std::string(12, 'x'), longB, std::nullopt}),
       makeUnknownVector(4)});
  auto layout = payloadLayout(asRowType(input->type()));
  auto arenaPool = rootPool_->addLeafChild("payload-roundtrip-arena");
  RadixSortRunStorage arena(
      arenaPool.get(), keyLayout(), 4, 64, layout, 2, 512);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);
  ASSERT_EQ(batch.size(), input->size());
  ASSERT_EQ(arena.payloadSize(), input->size());
  ASSERT_EQ(arena.payloadFixedBlocks().size(), 2);

  for (vector_size_t row = 0; row < batch.size(); ++row) {
    EXPECT_EQ(
        loadUnaligned<uint64_t>(
            batch.rowAt(row) + *layout->variableSizeOffset()),
        batch.heapSizeAt(row));
  }
  EXPECT_EQ(batch.heapSizeAt(0), 0);
  EXPECT_EQ(batch.heapSizeAt(1), 0);
  EXPECT_EQ(batch.heapSizeAt(2), longA.size() + longB.size());
  EXPECT_EQ(batch.heapSizeAt(3), 0);
  ASSERT_NE(batch.heapAt(2), nullptr);
  const auto firstString =
      loadUnaligned<StringView>(batch.rowAt(2) + layout->columns()[11].offset);
  const auto secondString =
      loadUnaligned<StringView>(batch.rowAt(2) + layout->columns()[12].offset);
  EXPECT_EQ(firstString.data(), batch.heapAt(2));
  EXPECT_EQ(secondString.data(), batch.heapAt(2) + longA.size());
  EXPECT_EQ(
      secondString.data() + secondString.size(),
      batch.heapAt(2) + batch.heapSizeAt(2));

  auto outputPool = rootPool_->addLeafChild("payload-roundtrip-output");
  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, outputPool.get(), output);
  expectEquivalent(*input, *output);

  const auto outputFloat =
      output->childAt(6)->asUnchecked<SimpleVector<float>>()->valueAt(2);
  uint32_t outputFloatBits;
  std::memcpy(&outputFloatBits, &outputFloat, sizeof(outputFloatBits));
  EXPECT_EQ(outputFloatBits, floatNanBits);
  const auto outputDouble =
      output->childAt(7)->asUnchecked<SimpleVector<double>>()->valueAt(2);
  uint64_t outputDoubleBits;
  std::memcpy(&outputDoubleBits, &outputDouble, sizeof(outputDoubleBits));
  EXPECT_EQ(outputDoubleBits, doubleNanBits);
  EXPECT_TRUE(std::signbit(
      output->childAt(6)->asUnchecked<SimpleVector<float>>()->valueAt(0)));
  EXPECT_TRUE(std::signbit(
      output->childAt(7)->asUnchecked<SimpleVector<double>>()->valueAt(0)));

  std::memset(batch.heapAt(2), 'z', batch.heapSizeAt(2));
  expectEquivalent(*input, *output);
  batch = PayloadRowBatch{};
  arena.clear();
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(arenaPool->currentBytes(), 0);
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, logicalAndCustomScalarRoundTrip) {
  auto input = makeRows(
      {makeVector<int32_t>(
           DATE(),
           {DATE()->toDays("1970-01-01"),
            DATE()->toDays("2024-02-29"),
            std::nullopt}),
       makeVector<int64_t>(
           INTERVAL_DAY_TIME(),
           {int64_t{0}, int64_t{-123456789}, std::nullopt}),
       makeVector<int32_t>(
           INTERVAL_YEAR_MONTH(), {int32_t{0}, int32_t{-25}, std::nullopt}),
       makeStringVector(
           JSON(),
           {std::string("{\"a\":1}"), std::string(80, 'j'), std::nullopt}),
       makeStringVector(
           HYPERLOGLOG(),
           {std::string("\x00\x01\xff", 3),
            std::string(64, 'h'),
            std::nullopt})});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 512);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);

  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  expectEquivalent(*input, *output);
  for (uint32_t column = 0; column < input->childrenSize(); ++column) {
    EXPECT_TRUE(output->childAt(column)->type()->equivalent(
        *input->childAt(column)->type()));
  }
}

TEST_F(RadixPayloadRowTest, complexRoundTripAndContiguousHeap) {
  auto arrays = makeArrays();
  auto nestedRows = makeNestedRows();
  auto maps = makeMaps();
  auto timestampWithTimeZones = makeTimestampWithTimeZones();
  auto nestedVariants = makeNestedVariants();
  auto input = makeRows(
      {arrays, nestedRows, maps, timestampWithTimeZones, nestedVariants});
  auto layout = payloadLayout(asRowType(input->type()));
  ASSERT_TRUE(layout->hasVariableFields());
  ASSERT_EQ(layout->columns().size(), 5);
  for (const auto& column : layout->columns()) {
    EXPECT_TRUE(column.variable);
    EXPECT_TRUE(column.complex);
    EXPECT_EQ(column.width, sizeof(PayloadVarlenRef));
  }

  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 1024);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);
  const exec::ContainerRowSerdeOptions options{.isKey = false};
  for (vector_size_t row = 0; row < input->size(); ++row) {
    auto* cursor = batch.heapAt(row);
    for (uint32_t column = 0; column < layout->columns().size(); ++column) {
      const auto& metadata = layout->columns()[column];
      const auto* slot = batch.rowAt(row) + metadata.offset;
      if (input->childAt(column)->isNullAt(row)) {
        EXPECT_EQ(loadUnaligned<uint64_t>(slot), 0);
        EXPECT_EQ(loadUnaligned<uint64_t>(slot + sizeof(uint64_t)), 0);
        continue;
      }
      const auto expectedSize = exec::ContainerRowSerde::serializedSize(
          *input->childAt(column), row, options);
      const auto descriptor = loadUnaligned<PayloadVarlenRef>(slot);
      EXPECT_EQ(descriptor.size, expectedSize);
      if (expectedSize == 0) {
        EXPECT_EQ(descriptor.data, nullptr);
      } else {
        EXPECT_EQ(descriptor.data, cursor);
        cursor += expectedSize;
      }
    }
    EXPECT_EQ(
        cursor,
        batch.heapAt(row) == nullptr
            ? nullptr
            : batch.heapAt(row) + batch.heapSizeAt(row));
  }

  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  const auto expectComplexEquivalent = [&]() {
    const auto compareFlags = CompareFlags{
        .nullsFirst = true,
        .ascending = true,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
    for (uint32_t column = 0; column < 4; ++column) {
      for (vector_size_t row = 0; row < input->size(); ++row) {
        const auto result = input->childAt(column)->compare(
            output->childAt(column).get(), row, row, compareFlags);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 0) << "column=" << column << ", row=" << row;
      }
    }
    const auto* expectedRow = input->childAt(4)->asUnchecked<RowVector>();
    const auto* actualRow = output->childAt(4)->asUnchecked<RowVector>();
    const auto* expectedVariant =
        expectedRow->childAt(0)->asUnchecked<VariantVector>();
    const auto* actualVariant =
        actualRow->childAt(0)->asUnchecked<VariantVector>();
    for (vector_size_t row = 0; row < input->size(); ++row) {
      EXPECT_EQ(expectedVariant->isNullAt(row), actualVariant->isNullAt(row));
      if (!expectedVariant->isNullAt(row)) {
        const auto expected = expectedVariant->valueAt(row);
        const auto actual = actualVariant->valueAt(row);
        EXPECT_EQ(expected.value, actual.value);
        EXPECT_EQ(expected.metadata, actual.metadata);
      }
    }
  };
  expectComplexEquivalent();
  const auto* outputMaps = output->childAt(2)->asUnchecked<MapVector>();
  const auto* inputKeys = maps->mapKeys()->asUnchecked<SimpleVector<int32_t>>();
  const auto* outputKeys =
      outputMaps->mapKeys()->asUnchecked<SimpleVector<int32_t>>();
  for (vector_size_t row = 0; row < maps->size(); ++row) {
    if (maps->isNullAt(row)) {
      continue;
    }
    ASSERT_EQ(maps->sizeAt(row), outputMaps->sizeAt(row));
    for (vector_size_t entry = 0; entry < maps->sizeAt(row); ++entry) {
      EXPECT_EQ(
          inputKeys->valueAt(maps->offsetAt(row) + entry),
          outputKeys->valueAt(outputMaps->offsetAt(row) + entry));
    }
  }

  for (const auto& group : arena.payloadHeapGroups()) {
    std::memset(group.base, 0xa5, group.used);
  }
  expectComplexEquivalent();
}

TEST_F(RadixPayloadRowTest, stringBoundaryRoundTrip) {
  const std::string invalidUtf8{"\xc3\x28\xff", 3};
  const std::string controls{"\x00\x01\x02\xff", 4};
  const std::vector<std::optional<std::string>> values{
      std::string(),
      std::string(12, 'a'),
      std::string(13, 'b'),
      std::string(4096, 'c'),
      invalidUtf8,
      controls,
      std::nullopt};
  auto input = makeRows(
      {makeStringVector(VARCHAR(), values),
       makeStringVector(VARBINARY(), values)});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 8, 64, layout, 8, 16 * 1024);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);
  EXPECT_EQ(batch.heapSizeAt(0), 0);
  EXPECT_EQ(batch.heapSizeAt(1), 0);
  EXPECT_EQ(batch.heapSizeAt(2), 26);
  EXPECT_EQ(batch.heapSizeAt(3), 8192);
  EXPECT_EQ(batch.heapSizeAt(4), 0);
  EXPECT_EQ(batch.heapSizeAt(5), 0);
  EXPECT_EQ(batch.heapSizeAt(6), 0);

  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, tiledStringRoundTrip) {
  constexpr vector_size_t kRows = 97;
  std::vector<std::optional<std::string>> nullableLong;
  std::vector<std::optional<std::string>> nonNullMixed;
  std::vector<std::optional<std::string>> nullableMixed;
  std::vector<std::optional<std::string>> nonNullLong;
  nullableLong.reserve(kRows);
  nonNullMixed.reserve(kRows);
  nullableMixed.reserve(kRows);
  nonNullLong.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    nullableLong.push_back(
        row % 11 == 0 ? std::nullopt
                      : std::optional<std::string>(std::string(
                            48 + row % 17, static_cast<char>('a' + row % 26))));
    nonNullMixed.push_back(
        row % 3 == 0 ? std::string("short-" + std::to_string(row))
                     : std::string(64 + row % 13, 'm'));
    nullableMixed.push_back(
        row % 7 == 0 ? std::nullopt
                     : std::optional<std::string>(
                           row % 2 == 0 ? std::string("v" + std::to_string(row))
                                        : std::string(33 + row % 19, 'n')));
    nonNullLong.push_back(std::string(80 + row % 23, 'z'));
  }
  auto input = makeRows(
      {makeStringVector(VARCHAR(), nullableLong),
       makeStringVector(VARBINARY(), nonNullMixed),
       makeStringVector(VARCHAR(), nullableMixed),
       makeStringVector(VARBINARY(), nonNullLong)});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 32, 4096, layout, 32, 32 * 1024);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);

  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  expectEquivalent(*input, *output);

  for (const auto& group : arena.payloadHeapGroups()) {
    std::memset(group.base, 0, group.used);
  }
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, fixedSeedRoundTripProperty) {
  constexpr uint32_t kSeeds = 32;
  constexpr vector_size_t kRows = 257;
  for (uint32_t seed = 0; seed < kSeeds; ++seed) {
    std::mt19937_64 random(seed);
    SCOPED_TRACE("seed=" + std::to_string(seed));
    std::vector<std::optional<int64_t>> integers;
    std::vector<std::optional<double>> doubles;
    std::vector<std::optional<Timestamp>> timestamps;
    std::vector<std::optional<std::string>> strings;
    std::vector<std::optional<std::string>> binaries;
    integers.reserve(kRows);
    doubles.reserve(kRows);
    timestamps.reserve(kRows);
    strings.reserve(kRows);
    binaries.reserve(kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      if (row % 17 == 0) {
        integers.push_back(std::nullopt);
        doubles.push_back(std::nullopt);
        timestamps.push_back(std::nullopt);
        strings.push_back(std::nullopt);
        binaries.push_back(std::nullopt);
        continue;
      }
      integers.push_back(static_cast<int64_t>(random()));
      uint64_t doubleBits = random();
      double doubleValue;
      std::memcpy(&doubleValue, &doubleBits, sizeof(doubleValue));
      doubles.push_back(doubleValue);
      timestamps.push_back(Timestamp(
          static_cast<int64_t>(
              random() % static_cast<uint64_t>(Timestamp::kMaxSeconds)),
          random() % (Timestamp::kMaxNanos + 1)));
      const auto stringSize = random() % 65;
      std::string stringValue(stringSize, '\0');
      std::string binaryValue(stringSize, '\0');
      for (uint32_t byte = 0; byte < stringSize; ++byte) {
        stringValue[byte] = static_cast<char>(random());
        binaryValue[byte] = static_cast<char>(random());
      }
      strings.push_back(std::move(stringValue));
      binaries.push_back(std::move(binaryValue));
    }

    auto input = makeRows(
        {makeVector<int64_t>(BIGINT(), integers),
         makeVector<double>(DOUBLE(), doubles),
         makeVector<Timestamp>(TIMESTAMP(), timestamps),
         makeStringVector(VARCHAR(), strings),
         makeStringVector(VARBINARY(), binaries)});
    auto layout = payloadLayout(asRowType(input->type()));
    RadixSortRunStorage arena(
        pool_.get(), keyLayout(), 127, 4096, layout, 127, 4096);
    PayloadRowBatch batch;
    PayloadRowWriter::append(*input, arena, batch);
    RowVectorPtr output;
    gatherPayloadBatch(*layout, batch, pool_.get(), output);
    expectEquivalent(*input, *output);
  }
}

TEST_F(RadixPayloadRowTest, nullBitmapAndNullSlots) {
  std::vector<VectorPtr> children;
  for (uint32_t column = 0; column < 10; ++column) {
    children.push_back(
        makeVector<int64_t>(BIGINT(), {std::nullopt, int64_t{column}}));
  }
  auto input = makeRows(children);
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);

  const auto* allNull = reinterpret_cast<const uint8_t*>(batch.rowAt(0));
  EXPECT_EQ(allNull[0], 0);
  EXPECT_EQ(allNull[1], 0xfc);
  for (const auto& column : layout->columns()) {
    for (uint32_t byte = 0; byte < column.width; ++byte) {
      EXPECT_EQ(batch.rowAt(0)[column.offset + byte], 0);
    }
  }
  const auto* noNull = reinterpret_cast<const uint8_t*>(batch.rowAt(1));
  EXPECT_EQ(noNull[0], 0xff);
  EXPECT_EQ(noNull[1], 0xff);
}

TEST_F(RadixPayloadRowTest, dictionaryAndConstantInput) {
  auto integers = makeVector<int32_t>(INTEGER(), {10, 20, 30});
  auto strings = makeStringVector(
      VARCHAR(),
      {std::string("first"), std::string(64, 's'), std::string("third")});
  auto indices = AlignedBuffer::allocate<vector_size_t>(4, pool_.get());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  rawIndices[0] = 2;
  rawIndices[1] = 0;
  rawIndices[2] = 1;
  rawIndices[3] = 2;
  auto dictionaryIntegers =
      BaseVector::wrapInDictionary(nullptr, indices, 4, integers);
  auto dictionaryStrings =
      BaseVector::wrapInDictionary(nullptr, indices, 4, strings);
  auto constants =
      BaseVector::wrapInConstant(4, 1, makeVector<int64_t>(BIGINT(), {7, 11}));
  auto input = makeRows({dictionaryIntegers, dictionaryStrings, constants});

  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 128);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);
  RowVectorPtr output;
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, fixedOnlyAndEmptyInputHaveNoHeap) {
  auto fixedInput = makeRows(
      {makeVector<int8_t>(TINYINT(), {1, 2, 3}),
       makeVector<int64_t>(BIGINT(), {4, 5, 6}),
       makeVector<Timestamp>(
           TIMESTAMP(), {Timestamp(0, 1), Timestamp(0, 2), Timestamp(0, 3)})});
  auto layout = payloadLayout(asRowType(fixedInput->type()));
  ASSERT_FALSE(layout->hasVariableFields());
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*fixedInput, arena, batch);
  EXPECT_TRUE(arena.payloadHeapGroups().empty());
  EXPECT_EQ(batch.rowAt(1) - batch.rowAt(0), layout->rowWidth());
  EXPECT_EQ(batch.rowAt(2) - batch.rowAt(1), layout->rowWidth());
  EXPECT_EQ(batch.heapAt(0), nullptr);
  EXPECT_EQ(batch.heapAt(1), nullptr);
  EXPECT_EQ(batch.heapAt(2), nullptr);
  for (vector_size_t row = 0; row < fixedInput->size(); ++row) {
    EXPECT_EQ(
        static_cast<uint8_t>(batch.rowAt(row)[0]), static_cast<uint8_t>(0xff));
    EXPECT_EQ(
        loadUnaligned<int8_t>(batch.rowAt(row) + layout->columns()[0].offset),
        row + 1);
    EXPECT_EQ(
        loadUnaligned<int64_t>(batch.rowAt(row) + layout->columns()[1].offset),
        row + 4);
    EXPECT_EQ(
        loadUnaligned<Timestamp>(
            batch.rowAt(row) + layout->columns()[2].offset),
        Timestamp(0, row + 1));
  }

  RadixSortRunStorage sizedArena(
      pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  PayloadRowSizes sizes;
  PayloadRowWriter::measure(*fixedInput, *layout, pool_.get(), sizes);
  PayloadRowBatch sizedBatch;
  PayloadRowWriter::append(*fixedInput, sizedArena, sizes, sizedBatch);
  EXPECT_TRUE(sizedArena.payloadHeapGroups().empty());
  RowVectorPtr sizedOutput;
  gatherPayloadBatch(*layout, sizedBatch, pool_.get(), sizedOutput);
  expectEquivalent(*fixedInput, *sizedOutput);

  auto emptyInput = std::make_shared<RowVector>(
      pool_.get(),
      fixedInput->type(),
      nullptr,
      0,
      std::vector<VectorPtr>{
          BaseVector::create(TINYINT(), 0, pool_.get()),
          BaseVector::create(BIGINT(), 0, pool_.get()),
          BaseVector::create(TIMESTAMP(), 0, pool_.get())});
  RadixSortRunStorage emptyArena(
      pool_.get(), keyLayout(), 4, 64, layout, 2, 64);
  PayloadRowBatch emptyBatch;
  PayloadRowWriter::append(*emptyInput, emptyArena, emptyBatch);
  EXPECT_EQ(emptyBatch.size(), 0);
  EXPECT_TRUE(emptyArena.payloadFixedBlocks().empty());
  EXPECT_TRUE(emptyArena.payloadHeapGroups().empty());
  EXPECT_EQ(emptyArena.allocatedBytes(), 0);
}

TEST_F(RadixPayloadRowTest, reversedUnalignedBigintGather) {
  constexpr vector_size_t kRows = 17;
  std::vector<std::optional<int8_t>> tinyValues;
  std::vector<std::optional<int64_t>> firstBigintValues;
  std::vector<std::optional<double>> doubleValues;
  std::vector<std::optional<int64_t>> secondBigintValues;
  tinyValues.reserve(kRows);
  firstBigintValues.reserve(kRows);
  doubleValues.reserve(kRows);
  secondBigintValues.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    tinyValues.push_back(static_cast<int8_t>(row));
    firstBigintValues.push_back(
        static_cast<int64_t>(0x1020304050607000ULL + row));
    doubleValues.push_back(
        row % 5 == 0 ? std::optional<double>{}
                     : std::optional<double>{row * 1.25 - 7.0});
    secondBigintValues.push_back(
        static_cast<int64_t>(0x7060504030201000ULL - row));
  }
  auto input = makeRows(
      {makeVector<int8_t>(TINYINT(), tinyValues),
       makeVector<int64_t>(BIGINT(), firstBigintValues),
       makeVector<double>(DOUBLE(), doubleValues),
       makeVector<int64_t>(BIGINT(), secondBigintValues)});
  auto layout = payloadLayout(asRowType(input->type()));
  ASSERT_NE(layout->columns()[1].offset % alignof(int64_t), 0);
  ASSERT_NE(layout->columns()[2].offset % alignof(int64_t), 0);
  ASSERT_NE(layout->columns()[3].offset % alignof(int64_t), 0);
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  PayloadRowBatch batch;
  PayloadRowWriter::append(*input, arena, batch);
  auto rows = rowPointers(batch);
  std::reverse(rows.begin(), rows.end());

  RowVectorPtr output;
  PayloadRowReader::gather(*layout, rows, pool_.get(), output);
  const auto* tiny = output->childAt(0)->asUnchecked<FlatVector<int8_t>>();
  const auto* firstBigint =
      output->childAt(1)->asUnchecked<FlatVector<int64_t>>();
  const auto* doubles = output->childAt(2)->asUnchecked<FlatVector<double>>();
  const auto* secondBigint =
      output->childAt(3)->asUnchecked<FlatVector<int64_t>>();
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto inputRow = kRows - row - 1;
    EXPECT_EQ(tiny->valueAt(row), *tinyValues[inputRow]);
    EXPECT_EQ(firstBigint->valueAt(row), *firstBigintValues[inputRow]);
    EXPECT_EQ(doubles->isNullAt(row), !doubleValues[inputRow].has_value());
    if (doubleValues[inputRow].has_value()) {
      EXPECT_EQ(doubles->valueAt(row), *doubleValues[inputRow]);
    }
    EXPECT_EQ(secondBigint->valueAt(row), *secondBigintValues[inputRow]);
  }
}

TEST_F(RadixPayloadRowTest, heapGroupsAndOversizedRows) {
  auto layout = payloadLayout(ROW({"value"}, {VARCHAR()}));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  const std::array<uint64_t, 7> heapSizes{17, 0, 20, 27, 100, 0, 17};
  PayloadRowBatch batch;
  arena.allocatePayloadRowBatch(heapSizes, batch);

  ASSERT_EQ(arena.payloadFixedBlocks().size(), 2);
  EXPECT_EQ(arena.payloadFixedBlocks()[0].count, 4);
  EXPECT_EQ(arena.payloadFixedBlocks()[1].count, 3);
  ASSERT_EQ(arena.payloadHeapGroups().size(), 3);
  EXPECT_EQ(arena.payloadHeapGroups()[0].used, 64);
  EXPECT_EQ(arena.payloadHeapGroups()[0].rowCount, 3);
  EXPECT_EQ(arena.payloadHeapGroups()[1].used, 100);
  EXPECT_EQ(arena.payloadHeapGroups()[1].rowCount, 1);
  EXPECT_EQ(arena.payloadHeapGroups()[2].used, 17);
  EXPECT_EQ(arena.payloadHeapGroups()[2].rowCount, 1);
  EXPECT_EQ(batch.heapAt(0), arena.payloadHeapGroups()[0].base);
  EXPECT_EQ(batch.heapAt(1), nullptr);
  EXPECT_EQ(batch.heapAt(2), arena.payloadHeapGroups()[0].base + 17);
  EXPECT_EQ(batch.heapAt(3), arena.payloadHeapGroups()[0].base + 37);
  EXPECT_EQ(batch.heapAt(4), arena.payloadHeapGroups()[1].base);
  EXPECT_EQ(batch.heapAt(5), nullptr);
  EXPECT_EQ(batch.heapAt(6), arena.payloadHeapGroups()[2].base);
  uint64_t heapBytes = 0;
  for (const auto& group : arena.payloadHeapGroups()) {
    heapBytes += group.used;
  }
  const auto fixedBytes = heapSizes.size() * layout->rowWidth();
  EXPECT_EQ(heapBytes, 181);
  EXPECT_GE(
      static_cast<uint64_t>(arena.allocatedBytes()), fixedBytes + heapBytes);
}

TEST_F(RadixPayloadRowTest, payloadAllocationRangeBoundaryAndClear) {
  auto leaf = rootPool_->addLeafChild("payload-range-boundary");
  auto layout = payloadLayout(ROW({"value"}, {VARCHAR()}));
  RadixSortRunStorage arena(
      leaf.get(), keyLayout(), 2048, 64 * 1024, layout, 2048, 32 * 1024);
  std::array<uint64_t, 2048> heapSizes{};
  heapSizes.fill(4096);
  PayloadRowBatch batch;
  arena.allocatePayloadRowBatch(heapSizes, batch);
  EXPECT_GT(arena.numRanges(), 1);
  EXPECT_FALSE(arena.payloadFixedBlocks().empty());
  EXPECT_GT(arena.payloadHeapGroups().size(), 1);
  EXPECT_GT(leaf->currentBytes(), 0);

  batch = PayloadRowBatch{};
  arena.clear();
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(arena.payloadSize(), 0);
  EXPECT_TRUE(arena.payloadFixedBlocks().empty());
  EXPECT_TRUE(arena.payloadHeapGroups().empty());
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(RadixPayloadRowTest, payloadFixedBlockRangeBoundary) {
  auto leaf = rootPool_->addLeafChild("payload-fixed-range-boundary");
  auto layout = payloadLayout(ROW({"value"}, {BIGINT()}));
  RadixSortRunStorage arena(
      leaf.get(), keyLayout(), 2048, 64 * 1024, layout, 2048, 64 * 1024);
  const std::array<uint64_t, 2048> heapSizes{};
  while (arena.numRanges() < 2) {
    PayloadRowBatch batch;
    arena.allocatePayloadRowBatch(heapSizes, batch);
  }
  EXPECT_GT(arena.payloadFixedBlocks().size(), 1);
  EXPECT_TRUE(arena.payloadHeapGroups().empty());
  EXPECT_GT(leaf->currentBytes(), 0);

  arena.clear();
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(RadixPayloadRowTest, fixedKeyAndPayloadShareAllocationPoolRange) {
  auto leaf = rootPool_->addLeafChild("payload-shared-range");
  auto layout = payloadLayout(ROW({"value"}, {BIGINT()}));
  auto physicalLayout = RadixSortKeyLayout::fromKind(
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage arena(
      leaf.get(),
      physicalLayout,
      RadixSortRunStorage::kDefaultKeysPerBlock,
      64 * 1024,
      layout,
      RadixSortRunStorage::kDefaultKeysPerBlock,
      64 * 1024);
  const std::array<uint64_t, 2048> heapSizes{};
  PayloadRowBatch batch;
  arena.allocatePayloadRowBatch(heapSizes, batch);

  std::vector<std::string_view> keys(2048, std::string_view("12345678", 8));
  std::vector<char*> payloads(2048);
  for (vector_size_t row = 0; row < batch.size(); ++row) {
    payloads[row] = batch.rowAt(row);
  }
  arena.appendBatch(keys, payloads);

  ASSERT_EQ(arena.payloadFixedBlocks().size(), 1);
  ASSERT_EQ(arena.keyBlocks().size(), 1);
  EXPECT_EQ(arena.numRanges(), 1);
  EXPECT_EQ(arena.allocatedBytes(), 64 * 1024);
  EXPECT_NE(arena.payloadFixedBlocks()[0].base, nullptr);
  EXPECT_NE(arena.keyBlocks()[0].base, nullptr);
  EXPECT_EQ(
      arena.size() * arena.layout().width() +
          arena.payloadSize() * layout->rowWidth(),
      2048 * (arena.layout().width() + layout->rowWidth()));
}

TEST_F(RadixPayloadRowTest, invalidInputs) {
  auto layout = payloadLayout(ROW({"value"}, {VARCHAR()}));
  RadixSortRunStorage noPayloadArena(pool_.get(), keyLayout(), 4, 64);
  PayloadRowBatch batch;
  EXPECT_THROW(
      noPayloadArena.allocatePayloadRowBatch(std::array<uint64_t, 1>{0}, batch),
      BoltException);

  auto fixedLayout = payloadLayout(ROW({"value"}, {BIGINT()}));
  RadixSortRunStorage fixedArena(
      pool_.get(), keyLayout(), 4, 64, fixedLayout, 4, 64);
  EXPECT_THROW(
      fixedArena.allocatePayloadRowBatch(std::array<uint64_t, 1>{1}, batch),
      BoltException);

  auto wrongInput = makeRows({makeVector<int64_t>(BIGINT(), {1})});
  RadixSortRunStorage variableArena(
      pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  EXPECT_THROW(
      PayloadRowWriter::append(*wrongInput, variableArena, batch),
      BoltException);

  RowVectorPtr output;
  PayloadRowBatch emptyBatch;
  RadixSortRunStorage emptyArena(
      pool_.get(), keyLayout(), 4, 64, layout, 4, 64);
  emptyArena.allocatePayloadRowBatch(std::span<const uint64_t>{}, emptyBatch);
  gatherPayloadBatch(*layout, emptyBatch, pool_.get(), output);
  EXPECT_EQ(output->size(), 0);

  EXPECT_THROW(
      PayloadRowReader::gather(
          *layout, std::span<char* const>{}, nullptr, output),
      BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
