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

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
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

class FixedSizeStreamArena final : public StreamArena {
 public:
  explicit FixedSizeStreamArena(char* buffer)
      : StreamArena(nullptr), buffer_(buffer) {}

  void newRange(int32_t bytes, ByteRange*, ByteRange* range) override {
    range->buffer = reinterpret_cast<uint8_t*>(buffer_);
    range->size = bytes;
    range->position = 0;
  }

 private:
  char* buffer_;
};

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

  template <typename T>
  FlatVectorPtr<T> makeLimitsVector(const TypePtr& type) {
    return makeVector<T>(
        type,
        {std::numeric_limits<T>::min(),
         0,
         std::numeric_limits<T>::max(),
         std::nullopt});
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

  template <typename T, typename F>
  std::vector<T> generate(vector_size_t size, F valueAt) {
    std::vector<T> values;
    values.reserve(size);
    for (vector_size_t row = 0; row < size; ++row) {
      values.push_back(valueAt(row));
    }
    return values;
  }

  std::vector<VectorPtr> makeNullConstants(
      vector_size_t size,
      std::initializer_list<TypePtr> types) {
    std::vector<VectorPtr> result;
    result.reserve(types.size());
    for (const auto& type : types) {
      result.push_back(BaseVector::createNullConstant(type, size, pool_.get()));
    }
    return result;
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

  std::vector<VectorPtr> wrapDictionaries(
      const std::vector<VectorPtr>& bases,
      const BufferPtr& indices,
      vector_size_t size,
      const BufferPtr& nulls = nullptr) {
    std::vector<VectorPtr> result;
    result.reserve(bases.size());
    for (const auto& base : bases) {
      result.push_back(
          BaseVector::wrapInDictionary(nulls, indices, size, base));
    }
    return result;
  }

  ArrayVectorPtr makeArrays() {
    const std::vector<vector_size_t> offsets{0, 2, 4, 4};
    const std::vector<vector_size_t> sizes{2, 2, 0, 0};
    auto result = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        offsets.size(),
        makeBuffer(pool_.get(), offsets),
        makeBuffer(pool_.get(), sizes),
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
    const std::vector<vector_size_t> offsets{0, 2, 4, 4};
    const std::vector<vector_size_t> sizes{2, 2, 0, 0};
    auto result = std::make_shared<MapVector>(
        pool_.get(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        offsets.size(),
        makeBuffer(pool_.get(), offsets),
        makeBuffer(pool_.get(), sizes),
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

  MapVectorPtr makeLargeStringStringMaps(vector_size_t rows) {
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> sizes;
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<std::string>> values;
    offsets.reserve(rows);
    sizes.reserve(rows);
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < rows; ++row) {
      offsets.push_back(offset);
      if (row % 17 == 0) {
        sizes.push_back(0);
        continue;
      }
      const vector_size_t entries = row % 11 == 0 ? 0
          : row % 5 == 0                          ? 24
          : row % 3 == 0                          ? 12
                                                  : 5;
      sizes.push_back(entries);
      for (vector_size_t entry = 0; entry < entries; ++entry) {
        keys.push_back(
            "param_" + std::to_string(entry % 23) + "_" +
            std::to_string(row % 7));
        if (entry % 10 == 0) {
          values.push_back(
              std::string(48 + row % 17, static_cast<char>('a' + entry % 26)));
        } else {
          values.push_back(
              "value_" + std::to_string(row) + "_" + std::to_string(entry));
        }
      }
      offset += entries;
    }

    auto result = std::make_shared<MapVector>(
        pool_.get(),
        MAP(VARCHAR(), VARCHAR()),
        nullptr,
        rows,
        makeBuffer(pool_.get(), offsets),
        makeBuffer(pool_.get(), sizes),
        makeStringVector(VARCHAR(), keys),
        makeStringVector(VARCHAR(), values));
    for (vector_size_t row = 0; row < rows; row += 17) {
      result->setNull(row, true);
    }
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

  std::vector<VectorPtr> makeSupportedValues() {
#define SUPPORTED_SCALAR(cppType, type, first, second, fourth) \
  makeVector<cppType>(type, {first, second, std::nullopt, fourth})
#define SUPPORTED_STRING(type, first, second, fourth) \
  makeStringVector(type, {first, second, std::nullopt, fourth})
    constexpr vector_size_t kRows = 4;
    std::vector<VectorPtr> values{
        SUPPORTED_SCALAR(bool, BOOLEAN(), true, false, true),
        SUPPORTED_SCALAR(int8_t, TINYINT(), 1, -2, 4),
        SUPPORTED_SCALAR(int16_t, SMALLINT(), 10, -20, 40),
        SUPPORTED_SCALAR(int32_t, INTEGER(), 100, -200, 400),
        SUPPORTED_SCALAR(int64_t, BIGINT(), 1000, -2000, 4000),
        SUPPORTED_SCALAR(int128_t, HUGEINT(), 1, -2, 4),
        SUPPORTED_SCALAR(float, REAL(), 1.5F, -2.5F, 4.5F),
        SUPPORTED_SCALAR(double, DOUBLE(), 1.5, -2.5, 4.5),
        SUPPORTED_SCALAR(int64_t, DECIMAL(18, 4), 100, -200, 400),
        SUPPORTED_SCALAR(int128_t, DECIMAL(38, 18), 100, -200, 400),
        SUPPORTED_SCALAR(
            int32_t,
            DATE(),
            DATE()->toDays("1970-01-01"),
            DATE()->toDays("2024-02-29"),
            DATE()->toDays("1969-12-31")),
        SUPPORTED_SCALAR(int64_t, INTERVAL_DAY_TIME(), 0, -123456789, 400),
        SUPPORTED_SCALAR(int32_t, INTERVAL_YEAR_MONTH(), 0, -25, 400),
        SUPPORTED_SCALAR(
            Timestamp,
            TIMESTAMP(),
            Timestamp(1, 2),
            Timestamp(-2, 3),
            Timestamp(4, 5)),
        SUPPORTED_STRING(VARCHAR(), "one", std::string(80, 'x'), "four"),
        SUPPORTED_STRING(
            VARBINARY(),
            std::string("\x00", 1),
            std::string("\xff", 1),
            std::string("\x01\x02", 2)),
        SUPPORTED_STRING(JSON(), "{\"a\":1}", std::string(80, 'j'), "[4]"),
        SUPPORTED_STRING(
            HYPERLOGLOG(),
            std::string("\x00\x01\xff", 3),
            std::string(64, 'h'),
            "four"),
        makeUnknownVector(kRows),
        makeNestedVariants(),
        makeArrays(),
        makeMaps(),
        makeRows(
            {makeVector<int32_t>(INTEGER(), {10, 20, 30, 40}),
             makeStringVector(
                 VARCHAR(),
                 {"ten", "twenty", std::nullopt, std::string(64, 'r')})}),
        makeTimestampWithTimeZones(),
    };
#undef SUPPORTED_STRING
#undef SUPPORTED_SCALAR
    return values;
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

  template <typename T>
  static BufferPtr makeBuffer(
      memory::MemoryPool* pool,
      const std::vector<T>& values) {
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool);
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
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
      EXPECT_TRUE(expected.childAt(column)->type()->equivalent(
          *actual.childAt(column)->type()));
      if (expected.childAt(column)->typeKind() == TypeKind::ROW &&
          expected.childAt(column)->type()->size() == 1 &&
          expected.childAt(column)->type()->childAt(0)->kind() ==
              TypeKind::VARIANT) {
        const auto* expectedVariants = expected.childAt(column)
                                           ->asUnchecked<RowVector>()
                                           ->childAt(0)
                                           ->asUnchecked<VariantVector>();
        const auto* actualVariants = actual.childAt(column)
                                         ->asUnchecked<RowVector>()
                                         ->childAt(0)
                                         ->asUnchecked<VariantVector>();
        for (vector_size_t row = 0; row < expected.size(); ++row) {
          EXPECT_EQ(
              expectedVariants->isNullAt(row), actualVariants->isNullAt(row));
          if (!expectedVariants->isNullAt(row)) {
            const auto expectedValue = expectedVariants->valueAt(row);
            const auto actualValue = actualVariants->valueAt(row);
            EXPECT_EQ(expectedValue.value, actualValue.value);
            EXPECT_EQ(expectedValue.metadata, actualValue.metadata);
          }
        }
        continue;
      }
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

  static uint64_t totalHeapSize(const PayloadRowBatch& batch) {
    uint64_t size = 0;
    for (vector_size_t row = 0; row < batch.size(); ++row) {
      size += batch.heapSizeAt(row);
    }
    return size;
  }

  RowVectorPtr roundTrip(
      const RowVectorPtr& input,
      RadixSortRunStorage& arena,
      PayloadRowBatch& batch,
      bool verifyHeap = false) {
    PayloadRowWriter writer;
    writer.append(*input, arena, batch);
    if (verifyHeap) {
      verifyPayloadHeapLayout(*input, *arena.payloadLayout(), batch);
    }
    RowVectorPtr output;
    gatherPayloadBatch(*arena.payloadLayout(), batch, pool_.get(), output);
    expectEquivalent(*input, *output);
    return output;
  }

  void poisonHeap(
      const RadixSortRunStorage& arena,
      uint8_t byte,
      const RowVector& input,
      const RowVector& output) {
    for (const auto& group : arena.payloadHeapGroups()) {
      std::memset(group.base, byte, group.used);
    }
    expectEquivalent(input, output);
  }

  void expectArenaCleared(
      const RadixSortRunStorage& arena,
      const memory::MemoryPool& pool) {
    EXPECT_EQ(arena.allocatedBytes(), 0);
    EXPECT_EQ(arena.numRanges(), 0);
    EXPECT_EQ(pool.currentBytes(), 0);
  }

  char* poisonNextRows(
      RadixSortRunStorage& arena,
      const PayloadRowLayout& layout,
      vector_size_t rows) {
    PayloadRowBatch used;
    arena.allocateFixedPayloadRowBatch(rows, used);
    auto* next = arena.payloadFixedBlocks().front().base +
        static_cast<uint64_t>(used.size()) * layout.rowWidth();
    std::memset(next, 0xa5, static_cast<uint64_t>(rows) * layout.rowWidth());
    return next;
  }

  uint64_t serializedComplexSize(const BaseVector& vector, vector_size_t row) {
    std::vector<char> buffer(1 << 20);
    FixedSizeStreamArena arena(buffer.data());
    ByteOutputStream stream(&arena, false, false);
    stream.startWrite(buffer.size());
    ContainerRowSerde::serialize(
        vector, row, stream, ContainerRowSerdeOptions{.isKey = false});
    return stream.size();
  }

  void verifyPayloadHeapLayout(
      const RowVector& input,
      const PayloadRowLayout& layout,
      const PayloadRowBatch& batch) {
    for (vector_size_t row = 0; row < input.size(); ++row) {
      auto* cursor = batch.heapAt(row);
      for (uint32_t column = 0; column < layout.columns().size(); ++column) {
        const auto& metadata = layout.columns()[column];
        const auto isNull = input.childAt(column)->isNullAt(row);
        EXPECT_EQ(
            (static_cast<uint8_t>(batch.rowAt(row)[metadata.nullByte]) &
             metadata.nullMask) == 0,
            isNull)
            << "row=" << row << ", column=" << column;
        if (isNull) {
          for (uint32_t byte = 0; byte < metadata.width; ++byte) {
            EXPECT_EQ(
                static_cast<uint8_t>(batch.rowAt(row)[metadata.offset + byte]),
                0)
                << "row=" << row << ", column=" << column << ", byte=" << byte;
          }
        }
        if (!metadata.variable) {
          continue;
        }
        const auto* slot = batch.rowAt(row) + metadata.offset;
        if (isNull) {
          if (metadata.complex) {
            const auto value = loadUnaligned<PayloadVarlenRef>(slot);
            EXPECT_EQ(value.size, 0);
            EXPECT_EQ(value.data, nullptr);
          } else {
            const auto value = loadUnaligned<StringView>(slot);
            EXPECT_EQ(value.size(), 0);
          }
          continue;
        }
        if (metadata.complex) {
          const auto value = loadUnaligned<PayloadVarlenRef>(slot);
          EXPECT_EQ(
              value.size, serializedComplexSize(*input.childAt(column), row))
              << "row=" << row << ", column=" << column;
          EXPECT_EQ(value.data, value.size == 0 ? nullptr : cursor);
          if (value.size > 0) {
            cursor += value.size;
          }
        } else {
          const auto value = loadUnaligned<StringView>(slot);
          if (!value.isInline()) {
            EXPECT_EQ(value.data(), cursor);
            cursor += value.size();
          }
        }
      }
      EXPECT_EQ(
          cursor,
          batch.heapAt(row) == nullptr
              ? nullptr
              : batch.heapAt(row) + batch.heapSizeAt(row));
      EXPECT_EQ(batch.heapAt(row) == nullptr, batch.heapSizeAt(row) == 0);
      ASSERT_TRUE(layout.variableSizeOffset().has_value());
      EXPECT_EQ(
          loadUnaligned<uint64_t>(
              batch.rowAt(row) + *layout.variableSizeOffset()),
          batch.heapSizeAt(row));
    }
  }
};

TEST_F(RadixPayloadRowTest, packedLayoutHasNoPadding) {
  auto rowType = ROW(
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

  auto fixedLayout = payloadLayout(ROW({TINYINT(), BIGINT(), TIMESTAMP()}));
  EXPECT_EQ(fixedLayout->nullBytes(), 1);
  EXPECT_FALSE(fixedLayout->variableSizeOffset().has_value());
  EXPECT_EQ(fixedLayout->columns()[0].offset, 1);
  EXPECT_EQ(fixedLayout->columns()[1].offset, 2);
  EXPECT_EQ(fixedLayout->columns()[2].offset, 10);
  EXPECT_EQ(fixedLayout->rowWidth(), 26);
}

TEST_F(RadixPayloadRowTest, capabilityAndKeyOnlySchema) {
  for (const auto& value : makeSupportedValues()) {
    EXPECT_TRUE(PayloadRowLayout::supports(*value->type()))
        << value->type()->toString();
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
       makeLimitsVector<int8_t>(TINYINT()),
       makeLimitsVector<int16_t>(SMALLINT()),
       makeLimitsVector<int32_t>(INTEGER()),
       makeLimitsVector<int64_t>(BIGINT()),
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
  PayloadRowWriter writer;
  writer.append(*input, arena, batch);
  ASSERT_EQ(batch.size(), input->size());
  ASSERT_EQ(arena.payloadSize(), input->size());
  ASSERT_EQ(arena.payloadFixedBlocks().size(), 2);

  verifyPayloadHeapLayout(*input, *layout, batch);
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
  expectArenaCleared(arena, *arenaPool);
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, complexRoundTripAndContiguousHeap) {
  auto arrays = makeArrays();
  auto maps = makeMaps();
  auto emptyRows = std::make_shared<RowVector>(
      pool_.get(), ROW({}), nullptr, 4, std::vector<VectorPtr>{});
  emptyRows->setNull(2, true);
  auto input = makeRows(
      {arrays,
       makeNestedRows(),
       maps,
       makeTimestampWithTimeZones(),
       makeNestedVariants(),
       emptyRows});
  auto layout = payloadLayout(asRowType(input->type()));
  ASSERT_TRUE(layout->hasVariableFields());
  ASSERT_EQ(layout->columns().size(), 6);
  for (const auto& column : layout->columns()) {
    EXPECT_TRUE(column.variable);
    EXPECT_TRUE(column.complex);
    EXPECT_EQ(column.width, sizeof(PayloadVarlenRef));
  }

  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 1024);
  PayloadRowBatch batch;
  auto output = roundTrip(input, arena, batch, true);
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

  poisonHeap(arena, 0xa5, *input, *output);
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
  roundTrip(input, arena, batch);
  EXPECT_EQ(batch.heapSizeAt(0), 0);
  EXPECT_EQ(batch.heapSizeAt(1), 0);
  EXPECT_EQ(batch.heapSizeAt(2), 26);
  EXPECT_EQ(batch.heapSizeAt(3), 8192);
  EXPECT_EQ(batch.heapSizeAt(4), 0);
  EXPECT_EQ(batch.heapSizeAt(5), 0);
  EXPECT_EQ(batch.heapSizeAt(6), 0);
}

TEST_F(RadixPayloadRowTest, multiColumnStringRoundTrip) {
  constexpr vector_size_t kRows = 97;
  using OptionalString = std::optional<std::string>;
  auto nullableLong = generate<OptionalString>(kRows, [](auto row) {
    return row % 11 == 0
        ? OptionalString{}
        : OptionalString(
              std::string(48 + row % 17, static_cast<char>('a' + row % 26)));
  });
  auto nonNullMixed = generate<OptionalString>(kRows, [](auto row) {
    return row % 3 == 0 ? "short-" + std::to_string(row)
                        : std::string(64 + row % 13, 'm');
  });
  auto nullableMixed = generate<OptionalString>(kRows, [](auto row) {
    return row % 7 == 0 ? OptionalString{}
                        : OptionalString(
                              row % 2 == 0 ? "v" + std::to_string(row)
                                           : std::string(33 + row % 19, 'n'));
  });
  auto nonNullLong = generate<OptionalString>(
      kRows, [](auto row) { return std::string(80 + row % 23, 'z'); });
  auto input = makeRows(
      {makeStringVector(VARCHAR(), nullableLong),
       makeStringVector(VARBINARY(), nonNullMixed),
       makeStringVector(VARCHAR(), nullableMixed),
       makeStringVector(VARBINARY(), nonNullLong)});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 32, 4096, layout, 32, 32 * 1024);
  PayloadRowBatch batch;
  auto output = roundTrip(input, arena, batch);

  poisonHeap(arena, 0, *input, *output);
}

TEST_F(RadixPayloadRowTest, mixedStringAndComplexPayloadHeapOrder) {
  auto input = makeRows({
      makeStringVector(
          VARCHAR(),
          {std::string(48, 'a'),
           std::string("inline"),
           std::nullopt,
           std::string(64, 'b')}),
      makeArrays(),
      makeStringVector(
          VARBINARY(),
          {std::string(33, 'c'),
           std::string(80, 'd'),
           std::string("short"),
           std::nullopt}),
      makeMaps(),
  });
  auto layout = payloadLayout(asRowType(input->type()));
  ASSERT_TRUE(layout->hasVariableFields());
  ASSERT_EQ(layout->columns().size(), 4);

  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 4, 1024);
  PayloadRowBatch batch;
  roundTrip(input, arena, batch, true);
}

TEST_F(RadixPayloadRowTest, allSupportedPayloadTypesRoundTrip) {
  auto input = makeRows(makeSupportedValues());
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 8, 64, layout, 8, 16 * 1024);
  PayloadRowBatch batch;
  auto output = roundTrip(input, arena, batch);

  poisonHeap(arena, 0x3c, *input, *output);
}

TEST_F(RadixPayloadRowTest, gatherResultReuse) {
  auto input = makeRows(
      {makeVector<int64_t>(
           BIGINT(), {10, std::nullopt, 30, 40, 50, std::nullopt, 70}),
       makeStringVector(
           VARCHAR(),
           {std::string(80, 'a'),
            std::nullopt,
            std::string("short"),
            std::string(96, 'b'),
            std::string("tiny"),
            std::nullopt,
            std::string(112, 'c')})});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 8, 64, layout, 8, 1024);
  PayloadRowBatch batch;
  PayloadRowWriter writer;
  writer.append(*input, arena, batch);

  RowVectorPtr output;
  auto rows = rowPointers(batch);
  PayloadRowReader::gather(*layout, rows, pool_.get(), output);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), input->size());
  ASSERT_NE(output->childAt(0)->rawNulls(), nullptr);
  ASSERT_NE(output->childAt(1)->rawNulls(), nullptr);

  rows = {batch.rowAt(2), batch.rowAt(3), batch.rowAt(4)};
  PayloadRowReader::gather(*layout, rows, pool_.get(), output);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 3);
  expectEquivalent(
      *makeRows(
          {makeVector<int64_t>(BIGINT(), {30, 40, 50}),
           makeStringVector(
               VARCHAR(),
               {std::string("short"), std::string(96, 'b'), "tiny"})}),
      *output);
  for (uint32_t column = 0; column < output->childrenSize(); ++column) {
    for (vector_size_t row = 0; row < output->size(); ++row) {
      EXPECT_FALSE(output->childAt(column)->isNullAt(row))
          << "column=" << column << ", row=" << row;
    }
  }

  constexpr vector_size_t kBitmapRows = 129;
  auto first = generate<std::optional<int64_t>>(kBitmapRows, [](auto row) {
    return row % 17 == 0 ? std::optional<int64_t>{}
                         : std::optional<int64_t>(row * 3);
  });
  auto second = generate<std::optional<double>>(kBitmapRows, [](auto row) {
    return row % 19 == 0 ? std::optional<double>{}
                         : std::optional<double>(row + 0.25);
  });
  auto bitmapInput = makeRows(
      {makeVector<int64_t>(BIGINT(), first),
       makeVector<double>(DOUBLE(), second)});
  auto bitmapLayout = payloadLayout(asRowType(bitmapInput->type()));
  RadixSortRunStorage bitmapArena(
      pool_.get(), keyLayout(), 64, 1024, bitmapLayout, 64, 64);
  PayloadRowBatch bitmapBatch;
  PayloadRowWriter bitmapWriter;
  bitmapWriter.append(*bitmapInput, bitmapArena, bitmapBatch);

  auto bitmapRows = rowPointers(bitmapBatch);
  RowVectorPtr bitmapOutput;
  PayloadRowReader::gather(
      *bitmapLayout,
      std::span<char* const>(bitmapRows.data(), 65),
      pool_.get(),
      bitmapOutput);
  expectEquivalent(
      *makeRows(
          {makeVector<int64_t>(
               BIGINT(), std::vector(first.begin(), first.begin() + 65)),
           makeVector<double>(
               DOUBLE(), std::vector(second.begin(), second.begin() + 65))}),
      *bitmapOutput);

  PayloadRowReader::gather(
      *bitmapLayout, bitmapRows, pool_.get(), bitmapOutput);
  expectEquivalent(*bitmapInput, *bitmapOutput);
}

TEST_F(RadixPayloadRowTest, flatNullFreeWriteDoesNotDependOnClearedRows) {
  auto layout = payloadLayout(ROW({"first", "second"}, {BIGINT(), INTEGER()}));
  RadixSortRunStorage arena(pool_.get(), keyLayout(), 4, 64, layout, 8, 64);
  auto* nextRows = poisonNextRows(arena, *layout, 4);
  ASSERT_EQ(arena.payloadFixedBlocks().size(), 1);
  ASSERT_EQ(arena.payloadFixedBlocks()[0].capacity, 8);
  ASSERT_EQ(arena.payloadFixedBlocks()[0].count, 4);

  auto input = makeRows(
      {makeVector<int64_t>(BIGINT(), {11, 22, 33, 44}),
       makeVector<int32_t>(INTEGER(), {1, 2, 3, 4})});
  PayloadRowBatch batch;
  PayloadRowWriter writer;
  writer.append(*input, arena, batch);
  EXPECT_EQ(batch.rowAt(0), nextRows);
  for (vector_size_t row = 0; row < input->size(); ++row) {
    EXPECT_EQ(static_cast<uint8_t>(batch.rowAt(row)[0]), 0xff);
    EXPECT_EQ(
        loadUnaligned<int64_t>(batch.rowAt(row) + layout->columns()[0].offset),
        11 * (row + 1));
    EXPECT_EQ(
        loadUnaligned<int32_t>(batch.rowAt(row) + layout->columns()[1].offset),
        row + 1);
  }
}

TEST_F(RadixPayloadRowTest, wrappedComplexPayloads) {
  constexpr vector_size_t kRows = 4;
  auto arrays = makeArrays();
  auto maps = makeMaps();
  auto largeMaps = makeLargeStringStringMaps(kRows);
  auto rows = makeNestedRows();
  auto indices = makeBuffer<vector_size_t>(pool_.get(), {2, 0, 3, 1});
  auto wrapperIndices = makeBuffer<vector_size_t>(pool_.get(), {1, 2, 3, 0});
  auto nulls =
      AlignedBuffer::allocate<bool>(kRows, pool_.get(), bits::kNotNull);
  bits::setNull(nulls->asMutable<uint64_t>(), 1, true);
  bits::setNull(nulls->asMutable<uint64_t>(), 3, true);
  auto columns = wrapDictionaries(
      {arrays,
       rows,
       makeVector<int32_t>(INTEGER(), {10, 20, 30, 40}),
       makeStringVector(
           VARCHAR(), {"first", std::string(80, 's'), "third", "fourth"})},
      indices,
      kRows);
  columns.insert(
      columns.begin() + 1, BaseVector::wrapInConstant(kRows, 1, maps));
  columns.push_back(
      BaseVector::wrapInDictionary(nulls, wrapperIndices, kRows, largeMaps));
  columns.push_back(BaseVector::wrapInConstant(
      kRows, 1, makeVector<int64_t>(BIGINT(), {7, 11})));
  auto nullConstants =
      makeNullConstants(kRows, {BIGINT(), VARCHAR(), arrays->type()});
  columns.insert(columns.end(), nullConstants.begin(), nullConstants.end());
  auto nullDictionaries = wrapDictionaries(
      {arrays,
       rows,
       makeStringVector(
           VARCHAR(),
           {std::string(96, 'a'),
            std::string(80, 'b'),
            std::string(64, 'c'),
            std::string(48, 'd')})},
      wrapperIndices,
      kRows,
      nulls);
  columns.insert(
      columns.end(), nullDictionaries.begin(), nullDictionaries.end());
  nullConstants =
      makeNullConstants(kRows, {BOOLEAN(), maps->type(), rows->type()});
  columns.insert(columns.end(), nullConstants.begin(), nullConstants.end());
  columns.push_back(makeUnknownVector(kRows));
  auto input = makeRows(columns);
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 4, 64, layout, 8, 16 * 1024);
  auto* nextRows = poisonNextRows(arena, *layout, kRows);
  ASSERT_EQ(arena.payloadFixedBlocks().size(), 1);
  PayloadRowBatch batch;
  auto output = roundTrip(input, arena, batch, true);
  EXPECT_EQ(batch.rowAt(0), nextRows);
  poisonHeap(arena, 0x7f, *input, *output);

  auto allNullColumns = makeNullConstants(
      kRows,
      {BOOLEAN(),
       BIGINT(),
       VARCHAR(),
       arrays->type(),
       maps->type(),
       rows->type()});
  allNullColumns.push_back(makeUnknownVector(kRows));
  auto allNull = makeRows(allNullColumns);
  auto allNullLayout = payloadLayout(asRowType(allNull->type()));
  RadixSortRunStorage allNullArena(
      pool_.get(), keyLayout(), 4, 64, allNullLayout, 4, 64);
  PayloadRowBatch allNullBatch;
  PayloadRowWriter allNullWriter;
  allNullWriter.append(*allNull, allNullArena, allNullBatch);
  verifyPayloadHeapLayout(*allNull, *allNullLayout, allNullBatch);
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_EQ(allNullBatch.heapSizeAt(row), 0);
    EXPECT_EQ(allNullBatch.heapAt(row), nullptr);
  }
}

TEST_F(RadixPayloadRowTest, writerReuseAndBatchCopyOnWrite) {
  auto input = makeRows({makeArrays(), makeMaps()});
  auto layout = payloadLayout(asRowType(input->type()));
  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 32, 4096, layout, 32, 16 * 1024);
  PayloadRowWriter writer;
  PayloadRowBatch batch;

  writer.append(*input, arena, batch);
  ASSERT_NE(batch.rows(), nullptr);
  ASSERT_NE(batch.heaps(), nullptr);
  ASSERT_NE(batch.heapSizes(), nullptr);
  EXPECT_EQ(
      batch.heapSizes()->size(),
      static_cast<uint64_t>(input->size()) * sizeof(uint64_t));
  const auto* rowsBuffer = batch.rows().get();
  const auto* heapsBuffer = batch.heaps().get();
  const auto* sizesBuffer = batch.heapSizes().get();

  writer.append(*input, arena, batch);
  EXPECT_EQ(batch.rows().get(), rowsBuffer);
  EXPECT_EQ(batch.heaps().get(), heapsBuffer);
  EXPECT_EQ(batch.heapSizes().get(), sizesBuffer);

  const auto retained = batch;
  std::vector<uint64_t> retainedHeapSizes(retained.size());
  for (vector_size_t row = 0; row < retained.size(); ++row) {
    retainedHeapSizes[row] = retained.heapSizeAt(row);
  }

  writer.append(*input, arena, batch);
  EXPECT_NE(batch.rows().get(), retained.rows().get());
  EXPECT_NE(batch.heaps().get(), retained.heaps().get());
  EXPECT_NE(batch.heapSizes().get(), retained.heapSizes().get());
  for (vector_size_t row = 0; row < retained.size(); ++row) {
    EXPECT_EQ(retained.heapSizeAt(row), retainedHeapSizes[row]);
  }
  RowVectorPtr output;
  gatherPayloadBatch(*layout, retained, pool_.get(), output);
  expectEquivalent(*input, *output);
  gatherPayloadBatch(*layout, batch, pool_.get(), output);
  expectEquivalent(*input, *output);
}

TEST_F(RadixPayloadRowTest, logPatternPayloadRoundTrip) {
  constexpr vector_size_t kRows = 48;
  using OptionalInt = std::optional<int64_t>;
  using OptionalString = std::optional<std::string>;
  auto deviceIds =
      generate<OptionalInt>(kRows, [](auto row) { return 10'000 + row; });
  auto userIds =
      generate<OptionalInt>(kRows, [](auto row) { return 20'000 + row * 3; });
  auto groupIds = generate<OptionalInt>(kRows, [](auto row) {
    return row % 4 == 0 ? OptionalInt{} : OptionalInt(row % 97);
  });
  auto localTimes = generate<OptionalInt>(
      kRows, [](auto row) { return 1'787'000'000'000 + row * 1000; });
  auto enterFrom = generate<OptionalString>(kRows, [](auto row) {
    return row % 5 == 0 ? OptionalString{}
                        : OptionalString("enter_" + std::to_string(row % 7));
  });
  auto relationTag = generate<OptionalString>(kRows, [](auto row) {
    return row % 3 == 0 ? OptionalString{}
                        : OptionalString("relation_" + std::to_string(row % 5));
  });

  auto event = BaseVector::wrapInDictionary(
      nullptr,
      makeBuffer(
          pool_.get(),
          generate<vector_size_t>(
              kRows,
              [](auto row) { return row % 10 < 6 ? row % 3 : row % 6; })),
      kRows,
      makeStringVector(
          VARCHAR(),
          {"video_play",
           "like",
           "follow",
           "share",
           "comment",
           "enter_homepage"}));

  auto input = makeRows({
      makeVector<int64_t>(BIGINT(), deviceIds),
      makeVector<int64_t>(BIGINT(), userIds),
      makeVector<int64_t>(BIGINT(), groupIds),
      makeVector<int64_t>(BIGINT(), localTimes),
      event,
      makeStringVector(VARCHAR(), enterFrom),
      makeStringVector(VARCHAR(), relationTag),
      BaseVector::wrapInConstant(
          kRows, 0, makeStringVector(VARCHAR(), {std::string("12")})),
      makeLargeStringStringMaps(kRows),
  });
  auto layout = payloadLayout(asRowType(input->type()));
  ASSERT_EQ(layout->columns().size(), 9);
  ASSERT_TRUE(layout->hasVariableFields());

  RadixSortRunStorage arena(
      pool_.get(), keyLayout(), 64, 4096, layout, 64, 64 * 1024);
  PayloadRowBatch batch;
  roundTrip(input, arena, batch);

  EXPECT_GT(totalHeapSize(batch), 8 * 1024);
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
    roundTrip(input, arena, batch);
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
  PayloadRowWriter writer;
  writer.append(*input, arena, batch);

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
  roundTrip(fixedInput, arena, batch);
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
  PayloadRowWriter emptyWriter;
  emptyWriter.append(*emptyInput, emptyArena, emptyBatch);
  EXPECT_EQ(emptyBatch.size(), 0);
  EXPECT_TRUE(emptyArena.payloadFixedBlocks().empty());
  EXPECT_TRUE(emptyArena.payloadHeapGroups().empty());
  EXPECT_EQ(emptyArena.allocatedBytes(), 0);
}

TEST_F(RadixPayloadRowTest, reversedUnalignedBigintGather) {
  constexpr vector_size_t kRows = 17;
  auto tinyValues = generate<std::optional<int8_t>>(
      kRows, [](auto row) { return static_cast<int8_t>(row); });
  auto firstBigintValues =
      generate<std::optional<int64_t>>(kRows, [](auto row) {
        return static_cast<int64_t>(0x1020304050607000ULL + row);
      });
  auto doubleValues = generate<std::optional<double>>(kRows, [](auto row) {
    return row % 5 == 0 ? std::optional<double>{}
                        : std::optional<double>{row * 1.25 - 7.0};
  });
  auto secondBigintValues =
      generate<std::optional<int64_t>>(kRows, [](auto row) {
        return static_cast<int64_t>(0x7060504030201000ULL - row);
      });
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
  PayloadRowWriter writer;
  writer.append(*input, arena, batch);
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

TEST_F(RadixPayloadRowTest, fixedPayloadBlockCapacityIsByteBounded) {
  constexpr uint64_t kMaxPayloadFixedBlockBytes = 64 * 1024;
  for (const auto columnCount : {1U, 256U, 8193U}) {
    std::vector<TypePtr> types(columnCount, BIGINT());
    auto layout = payloadLayout(ROW(std::move(types)));
    RadixSortRunStorage arena(
        pool_.get(),
        keyLayout(),
        RadixSortRunStorage::kDefaultKeysPerBlock,
        64 * 1024,
        layout,
        RadixSortRunStorage::kDefaultKeysPerBlock,
        64 * 1024);
    PayloadRowBatch batch;
    arena.allocateFixedPayloadRowBatch(1, batch);

    ASSERT_EQ(arena.payloadFixedBlocks().size(), 1);
    const auto& block = arena.payloadFixedBlocks().front();
    const auto byteBoundedCapacity =
        kMaxPayloadFixedBlockBytes / layout->rowWidth();
    const auto expectedCapacity = static_cast<uint32_t>(std::min<uint64_t>(
        RadixSortRunStorage::kDefaultKeysPerBlock,
        std::max<uint64_t>(1, byteBoundedCapacity)));
    EXPECT_EQ(block.capacity, expectedCapacity) << "columns=" << columnCount;
    EXPECT_EQ(block.count, 1);
    if (layout->rowWidth() <= kMaxPayloadFixedBlockBytes) {
      EXPECT_LE(
          static_cast<uint64_t>(block.capacity) * layout->rowWidth(),
          kMaxPayloadFixedBlockBytes);
    } else {
      EXPECT_EQ(block.capacity, 1);
    }
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
  const auto fixedBytes = heapSizes.size() * layout->rowWidth();
  const auto heapBytes = totalHeapSize(batch);
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
  expectArenaCleared(arena, *leaf);
  EXPECT_EQ(arena.payloadSize(), 0);
  EXPECT_TRUE(arena.payloadFixedBlocks().empty());
  EXPECT_TRUE(arena.payloadHeapGroups().empty());
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
  expectArenaCleared(arena, *leaf);
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
