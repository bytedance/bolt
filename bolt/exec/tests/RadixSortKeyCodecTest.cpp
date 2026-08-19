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
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>

#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/exec/radixsort/RadixSortKeyCodec.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

constexpr vector_size_t kFuzzPairsPerSeed = 10'000;
constexpr uint32_t kFuzzSeeds = 100;

class RadixSortKeyCodecTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-sort-key-codec-test")};

  static CompareFlags flags(bool ascending, bool nullsFirst) {
    return CompareFlags{
        .nullsFirst = nullsFirst,
        .ascending = ascending,
        .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  }

  static std::vector<CompareFlags> allFlags() {
    return {
        flags(true, true),
        flags(true, false),
        flags(false, true),
        flags(false, false)};
  }

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

  ArrayVectorPtr makeIntegerArrays(
      const std::vector<std::optional<std::vector<std::optional<int32_t>>>>&
          values) {
    auto array = std::dynamic_pointer_cast<ArrayVector>(
        BaseVector::create(ARRAY(INTEGER()), values.size(), pool_.get()));
    BOLT_CHECK_NOT_NULL(array);
    vector_size_t elementCount = 0;
    for (vector_size_t row = 0; row < values.size(); ++row) {
      array->setOffsetAndSize(
          row, elementCount, values[row].has_value() ? values[row]->size() : 0);
      if (!values[row].has_value()) {
        array->setNull(row, true);
        continue;
      }
      elementCount += values[row]->size();
    }
    array->elements()->resize(elementCount);
    auto* elements = array->elements()->asUnchecked<FlatVector<int32_t>>();
    vector_size_t element = 0;
    for (const auto& value : values) {
      if (!value.has_value()) {
        continue;
      }
      for (const auto& item : *value) {
        if (item.has_value()) {
          elements->set(element, *item);
        } else {
          elements->setNull(element, true);
        }
        ++element;
      }
    }
    return array;
  }

  ArrayVectorPtr makeIntegerArraysWithElements(
      const std::vector<std::optional<vector_size_t>>& sizes,
      VectorPtr elements) {
    auto offsetBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool_.get());
    auto sizeBuffer =
        AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool_.get());
    auto* rawOffsets = offsetBuffer->asMutable<vector_size_t>();
    auto* rawSizes = sizeBuffer->asMutable<vector_size_t>();
    vector_size_t offset = 0;
    for (vector_size_t row = 0; row < sizes.size(); ++row) {
      rawOffsets[row] = offset;
      rawSizes[row] = sizes[row].value_or(0);
      if (sizes[row].has_value()) {
        offset += *sizes[row];
      }
    }
    BOLT_CHECK_LE(offset, elements->size());
    auto arrays = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(INTEGER()),
        nullptr,
        sizes.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        std::move(elements));
    for (vector_size_t row = 0; row < sizes.size(); ++row) {
      if (!sizes[row].has_value()) {
        arrays->setNull(row, true);
      }
    }
    return arrays;
  }

  RowVectorPtr makeNestedRows() {
    auto rows = std::make_shared<RowVector>(
        pool_.get(),
        ROW({"number", "text"}, {INTEGER(), VARCHAR()}),
        nullptr,
        7,
        std::vector<VectorPtr>{
            makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 1, std::nullopt, 1}),
            makeStringVector(
                VARCHAR(),
                {std::string("a"),
                 std::string("b"),
                 std::string("a"),
                 std::nullopt,
                 std::string("a"),
                 std::string("a"),
                 std::string("a")})});
    rows->setNull(6, true);
    return rows;
  }

  ArrayVectorPtr makeRowArrays() {
    auto elements = makeNestedRows();
    const std::array<vector_size_t, 7> offsets{0, 0, 1, 3, 4, 6, 7};
    const std::array<vector_size_t, 7> sizes{0, 1, 2, 1, 2, 1, 0};
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
    auto arrays = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(elements->type()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        elements);
    arrays->setNull(6, true);
    return arrays;
  }

  MapVectorPtr makeIntegerStringMaps() {
    const std::array<vector_size_t, 7> offsets{0, 0, 1, 3, 5, 7, 8};
    const std::array<vector_size_t, 7> sizes{0, 1, 2, 2, 2, 1, 0};
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
    auto keys = makeVector<int32_t>(INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1});
    auto values = makeStringVector(
        VARCHAR(),
        {std::string("a"),
         std::string("b"),
         std::string("a"),
         std::string("a"),
         std::string("b"),
         std::string("c"),
         std::string("a"),
         std::string("z")});
    auto maps = std::make_shared<MapVector>(
        pool_.get(),
        MAP(INTEGER(), VARCHAR()),
        nullptr,
        offsets.size(),
        std::move(offsetBuffer),
        std::move(sizeBuffer),
        keys,
        values);
    maps->setNull(6, true);
    return maps;
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

  std::unique_ptr<RadixSortKeyCodec> bind(
      const std::vector<TypePtr>& types,
      const std::vector<CompareFlags>& compareFlags) {
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind(types, compareFlags, codec);
    EXPECT_NE(codec, nullptr);
    return codec;
  }

  void verifyProperty(
      const RowVectorPtr& rows,
      const std::vector<CompareFlags>& compareFlags,
      bool verifyAllPairs = true) {
    std::vector<TypePtr> types;
    std::vector<column_index_t> channels;
    for (uint32_t column = 0; column < rows->childrenSize(); ++column) {
      types.push_back(rows->childAt(column)->type());
      channels.push_back(column);
    }
    auto codec = bind(types, compareFlags);
    ASSERT_TRUE(codec->canEncodeDecode());

    EncodedKeyBatch keys;
    codec->encode(*rows, pool_.get(), keys);

    RowVectorPtr decoded;
    decodeBatch(*codec, keys, decoded);
    ASSERT_EQ(decoded->size(), rows->size());

    for (vector_size_t row = 0; row < rows->size(); ++row) {
      EXPECT_EQ(
          SortComparatorOracle::compareRows(
              *rows, row, *decoded, row, channels, compareFlags),
          0)
          << "round-trip mismatch at row " << row << ", key "
          << keyHex(keys, row);
    }

    const auto comparePair = [&](vector_size_t left, vector_size_t right) {
      const auto expected = SortComparatorOracle::compareRows(
          *rows, left, *rows, right, channels, compareFlags);
      const auto actual = compareEncodedKeys(keys, left, right);
      EXPECT_EQ((actual > 0) - (actual < 0), (expected > 0) - (expected < 0))
          << "left " << left << ", right " << right << ", left-values "
          << rows->toString(left) << ", right-values " << rows->toString(right)
          << ", left-key " << keyHex(keys, left) << ", right-key "
          << keyHex(keys, right);
    };
    if (verifyAllPairs) {
      for (vector_size_t left = 0; left < rows->size(); ++left) {
        for (vector_size_t right = 0; right < rows->size(); ++right) {
          comparePair(left, right);
        }
      }
    } else {
      for (vector_size_t left = 0; left + 1 < rows->size(); ++left) {
        comparePair(left, left + 1);
      }
    }
  }

  void decodeBatch(
      const RadixSortKeyCodec& codec,
      const EncodedKeyBatch& keys,
      RowVectorPtr& decoded) {
    std::vector<std::array<char, sizeof(uint64_t)>> fixedBytes;
    std::vector<EncodedKeyView> views(keys.size());
    if (keys.format() == EncodedKeyFormat::kFixed64) {
      fixedBytes.resize(keys.size());
      for (vector_size_t row = 0; row < keys.size(); ++row) {
        auto word = keys.fixedKeyAt(row);
        if constexpr (std::endian::native == std::endian::little) {
          word = byteSwap(word);
        }
        storeUnaligned(fixedBytes[row].data(), word);
        views[row] = {
            std::string_view(fixedBytes[row].data(), fixedBytes[row].size()),
            true};
      }
    } else {
      for (vector_size_t row = 0; row < keys.size(); ++row) {
        views[row] = {keys.variableKeyAt(row), false};
      }
    }
    BufferPtr cursorScratch;
    codec.decode(
        std::span<const EncodedKeyView>(views.data(), views.size()),
        {},
        {},
        pool_.get(),
        cursorScratch,
        decoded);
  }

  static int32_t compareEncodedKeys(
      const EncodedKeyBatch& keys,
      vector_size_t left,
      vector_size_t right) {
    BOLT_CHECK_GE(left, 0);
    BOLT_CHECK_GE(right, 0);
    BOLT_CHECK_LT(left, keys.size());
    BOLT_CHECK_LT(right, keys.size());
    if (keys.format() == EncodedKeyFormat::kFixed64) {
      const auto leftKey = keys.fixedKeyAt(left);
      const auto rightKey = keys.fixedKeyAt(right);
      return (leftKey > rightKey) - (leftKey < rightKey);
    }
    const auto leftKey = keys.variableKeyAt(left);
    const auto rightKey = keys.variableKeyAt(right);
    const auto commonSize = std::min(leftKey.size(), rightKey.size());
    const auto result =
        std::memcmp(leftKey.data(), rightKey.data(), commonSize);
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
    return (leftKey.size() > rightKey.size()) -
        (leftKey.size() < rightKey.size());
  }

  static std::string hex(std::string_view bytes) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (const auto byte : bytes) {
      out << std::setw(2) << static_cast<uint32_t>(static_cast<uint8_t>(byte));
    }
    return out.str();
  }

  static std::string keyHex(const EncodedKeyBatch& keys, vector_size_t row) {
    if (keys.format() == EncodedKeyFormat::kVariableBinary) {
      return hex(keys.variableKeyAt(row));
    }
    auto word = keys.fixedKeyAt(row);
    if constexpr (std::endian::native == std::endian::little) {
      word = byteSwap(word);
    }
    std::array<char, sizeof(word)> bytes{};
    storeUnaligned(bytes.data(), word);
    return hex(std::string_view(bytes.data(), bytes.size()));
  }
};

TEST_F(RadixSortKeyCodecTest, bindMetadataAndCapability) {
  auto codec = bind(
      {INTEGER(), ARRAY(BIGINT()), ROW({{"a", VARCHAR()}})},
      {flags(true, true), flags(false, false), flags(true, false)});
  EXPECT_TRUE(codec->canEncodeDecode());

  const std::vector<TypePtr> supportedTypes{
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
      UNKNOWN(),
      VARCHAR(),
      VARBINARY(),
      JSON(),
      HYPERLOGLOG(),
      TIMESTAMP_WITH_TIME_ZONE(),
      ARRAY(MAP(INTEGER(), BIGINT())),
      MAP(INTEGER(), ROW({BIGINT(), VARCHAR()}))};
  for (const auto& type : supportedTypes) {
    EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*type))
        << type->toString();
  }
  EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*DATE()));
  EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*INTERVAL_DAY_TIME()));
  EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*INTERVAL_YEAR_MONTH()));
  EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*ARRAY(BIGINT())));
  EXPECT_TRUE(
      RadixSortKeyCodec::supportsEncodeDecode(*ROW({INTEGER(), VARCHAR()})));
  EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(
      *ARRAY(MAP(INTEGER(), BIGINT()))));
  EXPECT_TRUE(
      RadixSortKeyCodec::supportsEncodeDecode(*MAP(INTEGER(), BIGINT())));
  EXPECT_FALSE(RadixSortKeyCodec::supportsEncodeDecode(*VARIANT()));
  EXPECT_FALSE(RadixSortKeyCodec::supportsEncodeDecode(*OPAQUE<int32_t>()));
  EXPECT_FALSE(RadixSortKeyCodec::supportsEncodeDecode(
      *FUNCTION({BIGINT()}, BOOLEAN())));
}

TEST_F(RadixSortKeyCodecTest, invalidBindAndInputContracts) {
  std::unique_ptr<RadixSortKeyCodec> codec;
  EXPECT_THROW(RadixSortKeyCodec::bind({}, {}, codec), BoltException);
  EXPECT_EQ(codec, nullptr);
  EXPECT_THROW(RadixSortKeyCodec::bind({BIGINT()}, {}, codec), BoltException);
  EXPECT_EQ(codec, nullptr);

  auto invalidFlags = flags(true, true);
  invalidFlags.equalsOnly = true;
  EXPECT_THROW(
      RadixSortKeyCodec::bind({BIGINT()}, {invalidFlags}, codec),
      BoltException);
  EXPECT_EQ(codec, nullptr);
  invalidFlags = flags(true, true);
  invalidFlags.compareSizeFirst = true;
  EXPECT_THROW(
      RadixSortKeyCodec::bind({BIGINT()}, {invalidFlags}, codec),
      BoltException);
  EXPECT_EQ(codec, nullptr);

  codec = bind({BIGINT()}, {flags(true, true)});
  EncodedKeyBatch keys;
  auto wrongCount = makeRows(
      {makeVector<int64_t>(BIGINT(), {1}), makeVector<int64_t>(BIGINT(), {2})});
  EXPECT_THROW(codec->encode(*wrongCount, pool_.get(), keys), BoltException);
  auto wrongType = makeRows({makeVector<int32_t>(INTEGER(), {1})});
  EXPECT_THROW(codec->encode(*wrongType, pool_.get(), keys), BoltException);
  EXPECT_THROW(codec->encode(*wrongType, nullptr, keys), BoltException);

  auto unsupportedCodec = bind({VARIANT()}, {flags(true, true)});
  EXPECT_FALSE(unsupportedCodec->canEncodeDecode());
  EXPECT_THROW(
      unsupportedCodec->encode(
          *makeRows({BaseVector::create(VARIANT(), 1, pool_.get())}),
          pool_.get(),
          keys),
      BoltException);
}

TEST_F(RadixSortKeyCodecTest, leadingSkippableValidityOffsets) {
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind(
      {TINYINT(), SMALLINT(), INTEGER(), TINYINT()},
      {flags(true, true),
       flags(true, true),
       flags(true, true),
       flags(true, true)},
      codec);
  const std::vector<uint8_t> allNonNull{0, 0, 0, 0};
  EXPECT_EQ(
      codec->leadingSkippableValidityOffsets(allNonNull),
      (std::vector<uint32_t>{0, 2, 5}));

  RadixSortKeyCodec::bind(
      {TINYINT(), SMALLINT(), TINYINT()},
      {flags(true, true), flags(true, true), flags(true, true)},
      codec);
  const std::vector<uint8_t> secondMayHaveNulls{0, 1, 0};
  EXPECT_EQ(
      codec->leadingSkippableValidityOffsets(secondMayHaveNulls),
      (std::vector<uint32_t>{0}));

  RadixSortKeyCodec::bind(
      {VARCHAR(), TINYINT()}, {flags(true, true), flags(true, true)}, codec);
  const std::vector<uint8_t> variableNonNull{0, 0};
  EXPECT_TRUE(codec->leadingSkippableValidityOffsets(variableNonNull).empty());
}

TEST_F(RadixSortKeyCodecTest, dynamicLeadingSkippableValidityOffsets) {
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind(
      {TINYINT(), SMALLINT(), INTEGER(), TINYINT()},
      {flags(true, true),
       flags(true, true),
       flags(true, true),
       flags(true, true)},
      codec);
  const std::vector<uint8_t> allNonNull{0, 0, 0, 0};
  EXPECT_EQ(
      codec->leadingSkippableValidityOffsets(allNonNull),
      (std::vector<uint32_t>{0, 2, 5}));
  const std::vector<uint8_t> secondMayHaveNulls{0, 1, 0, 0};
  EXPECT_EQ(
      codec->leadingSkippableValidityOffsets(secondMayHaveNulls),
      (std::vector<uint32_t>{0}));

  RadixSortKeyCodec::bind(
      {VARCHAR(), TINYINT()}, {flags(true, true), flags(true, true)}, codec);
  const std::vector<uint8_t> variableNonNull{0, 0};
  EXPECT_TRUE(codec->leadingSkippableValidityOffsets(variableNonNull).empty());
}

TEST_F(RadixSortKeyCodecTest, nullFreeInt32KeysSkipBothValidityBytes) {
  auto codec =
      bind({INTEGER(), INTEGER()}, {flags(true, true), flags(true, true)});
  const std::vector<uint8_t> nullFreeKeys{0, 0};
  EXPECT_EQ(
      codec->leadingSkippableValidityOffsets(nullFreeKeys),
      (std::vector<uint32_t>{0, 5}));
}

TEST_F(RadixSortKeyCodecTest, knownEncodedKeyBytes) {
  auto integerRows =
      makeRows({makeVector<int32_t>(INTEGER(), {0, -1, std::nullopt})});
  auto integerCodec = bind({INTEGER()}, {flags(true, true)});
  EncodedKeyBatch integerKeys;
  integerCodec->encode(*integerRows, pool_.get(), integerKeys);
  EXPECT_EQ(keyHex(integerKeys, 0), "0280000000000000");
  EXPECT_EQ(keyHex(integerKeys, 1), "027fffffff000000");
  EXPECT_EQ(keyHex(integerKeys, 2), "0100000000000000");

  const std::string bytes{"\x00\x01\x02\xff", 4};
  auto stringRows =
      makeRows({makeStringVector(VARBINARY(), {bytes, std::nullopt})});
  auto ascCodec = bind({VARBINARY()}, {flags(true, true)});
  EncodedKeyBatch ascKeys;
  ascCodec->encode(*stringRows, pool_.get(), ascKeys);
  EXPECT_EQ(keyHex(ascKeys, 0), "020100010102ff00");
  EXPECT_EQ(keyHex(ascKeys, 1), "01");

  auto descCodec = bind({VARBINARY()}, {flags(false, true)});
  EncodedKeyBatch descKeys;
  descCodec->encode(*stringRows, pool_.get(), descKeys);
  EXPECT_EQ(keyHex(descKeys, 0), "02fefffefefd00ff");
  EXPECT_EQ(keyHex(descKeys, 1), "01");
}

TEST_F(RadixSortKeyCodecTest, fixed64BoundaryAndAllocation) {
  auto fixedRows = makeRows(
      {makeVector<int16_t>(SMALLINT(), {1, -1, std::nullopt}),
       makeVector<int32_t>(INTEGER(), {2, -2, 0})});
  auto fixedCodec =
      bind({SMALLINT(), INTEGER()}, {flags(true, true), flags(true, true)});
  EXPECT_EQ(fixedCodec->maximumEncodedSize(), 8);

  auto before = pool_->stats();
  EncodedKeyBatch fixedKeys;
  fixedCodec->encode(*fixedRows, pool_.get(), fixedKeys);
  auto after = pool_->stats();
  EXPECT_EQ(fixedKeys.format(), EncodedKeyFormat::kFixed64);
  EXPECT_EQ(after.numAllocs - before.numAllocs, 1);
  EXPECT_NE(fixedKeys.fixedKeys(), nullptr);
  EXPECT_EQ(fixedKeys.offsets(), nullptr);
  EXPECT_EQ(fixedKeys.data(), nullptr);

  auto variableRows =
      makeRows({makeVector<int64_t>(BIGINT(), {1, -1, std::nullopt})});
  auto variableCodec = bind({BIGINT()}, {flags(true, true)});
  EXPECT_EQ(variableCodec->maximumEncodedSize(), 9);

  before = pool_->stats();
  EncodedKeyBatch variableKeys;
  variableCodec->encode(*variableRows, pool_.get(), variableKeys);
  after = pool_->stats();
  EXPECT_EQ(variableKeys.format(), EncodedKeyFormat::kVariableBinary);
  EXPECT_EQ(after.numAllocs - before.numAllocs, 2);
  EXPECT_EQ(variableKeys.fixedKeys(), nullptr);
  EXPECT_NE(variableKeys.offsets(), nullptr);
  EXPECT_NE(variableKeys.data(), nullptr);
}

TEST_F(RadixSortKeyCodecTest, selectiveDecodeSkipsFixedPrefix) {
  auto rows = makeRows(
      {makeVector<int64_t>(BIGINT(), {10, std::nullopt, -7, 42, std::nullopt}),
       makeStringVector(
           VARCHAR(),
           {std::string("alpha"),
            std::string("\x00", 1) + std::string("\x01", 1) + "beta",
            std::nullopt,
            std::string(128, 'z'),
            std::string()}),
       makeVector<double>(
           DOUBLE(),
           {1.5,
            -0.0,
            std::numeric_limits<double>::quiet_NaN(),
            std::nullopt,
            -7.25})});
  const std::vector<CompareFlags> compareFlags{
      flags(true, true), flags(false, false), flags(false, true)};
  auto codec = bind({BIGINT(), VARCHAR(), DOUBLE()}, compareFlags);

  EncodedKeyBatch keys;
  codec->encode(*rows, pool_.get(), keys);
  std::vector<EncodedKeyView> views(rows->size());
  for (vector_size_t row = 0; row < rows->size(); ++row) {
    views[row] = {keys.variableKeyAt(row), false};
  }

  BufferPtr cursorScratch;
  RowVectorPtr decoded;
  const std::vector<uint8_t> decodedColumns{0, 1, 1};
  const std::vector<uint8_t> mayHaveNulls{1, 1, 1};
  codec->decode(
      std::span<const EncodedKeyView>(views.data(), views.size()),
      decodedColumns,
      mayHaveNulls,
      pool_.get(),
      cursorScratch,
      decoded);

  ASSERT_EQ(decoded->childAt(0), nullptr);
  ASSERT_NE(decoded->childAt(1), nullptr);
  ASSERT_NE(decoded->childAt(2), nullptr);
  for (vector_size_t row = 0; row < rows->size(); ++row) {
    EXPECT_EQ(
        SortComparatorOracle::compare(
            *rows->childAt(1), row, *decoded->childAt(1), row, compareFlags[1]),
        0);
    EXPECT_EQ(
        SortComparatorOracle::compare(
            *rows->childAt(2), row, *decoded->childAt(2), row, compareFlags[2]),
        0);
  }
}

TEST_F(RadixSortKeyCodecTest, emptyBatchAndVariableAllocation) {
  auto codec = bind({VARCHAR()}, {flags(true, true)});
  auto emptyRows = makeRows(
      {BaseVector::create<FlatVector<StringView>>(VARCHAR(), 0, pool_.get())});
  EncodedKeyBatch emptyKeys;
  codec->encode(*emptyRows, pool_.get(), emptyKeys);
  EXPECT_EQ(emptyKeys.size(), 0);
  EXPECT_NE(emptyKeys.offsets(), nullptr);
  EXPECT_EQ(emptyKeys.data(), nullptr);
  RowVectorPtr decoded;
  decodeBatch(*codec, emptyKeys, decoded);
  EXPECT_EQ(decoded->size(), 0);

  std::vector<std::optional<std::string>> values;
  values.reserve(1024);
  for (uint32_t row = 0; row < 1024; ++row) {
    values.emplace_back(std::string(4096, static_cast<char>(2 + row % 200)));
  }
  auto rows = makeRows({makeStringVector(VARCHAR(), values)});
  auto before = pool_->stats();
  EncodedKeyBatch keys;
  codec->encode(*rows, pool_.get(), keys);
  auto after = pool_->stats();
  EXPECT_EQ(after.numAllocs - before.numAllocs, 2);
  EXPECT_EQ(keys.variableKeyAt(0).size(), 4098);
  EXPECT_EQ(keys.variableKeyAt(1023).size(), 4098);
}

TEST_F(RadixSortKeyCodecTest, integralAndDecimalRoundTrip) {
  const auto int128Min = HugeInt::build(uint64_t{1} << 63, 0);
  const auto int128Max = HugeInt::build(
      (uint64_t{1} << 63) - 1, std::numeric_limits<uint64_t>::max());
  const auto decimal38Max =
      HugeInt::fromString("99999999999999999999999999999999999999");
  struct TestCase {
    TypePtr type;
    VectorPtr vector;
  };
  const std::vector<TestCase> cases{
      {BOOLEAN(), makeVector<bool>(BOOLEAN(), {false, true, std::nullopt})},
      {TINYINT(),
       makeVector<int8_t>(
           TINYINT(),
           {std::numeric_limits<int8_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int8_t>::max(),
            std::nullopt})},
      {SMALLINT(),
       makeVector<int16_t>(
           SMALLINT(),
           {std::numeric_limits<int16_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int16_t>::max(),
            std::nullopt})},
      {INTEGER(),
       makeVector<int32_t>(
           INTEGER(),
           {std::numeric_limits<int32_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {BIGINT(),
       makeVector<int64_t>(
           BIGINT(),
           {std::numeric_limits<int64_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int64_t>::max(),
            std::nullopt})},
      {HUGEINT(),
       makeVector<int128_t>(
           HUGEINT(),
           {int128Min,
            static_cast<int128_t>(-1),
            static_cast<int128_t>(0),
            static_cast<int128_t>(1),
            int128Max,
            std::nullopt})},
      {DECIMAL(18, 4),
       makeVector<int64_t>(
           DECIMAL(18, 4),
           {-999999999999999999LL,
            -1,
            0,
            1,
            999999999999999999LL,
            std::nullopt})},
      {DECIMAL(38, 18),
       makeVector<int128_t>(
           DECIMAL(38, 18),
           {-decimal38Max,
            static_cast<int128_t>(-1),
            static_cast<int128_t>(0),
            static_cast<int128_t>(1),
            decimal38Max,
            std::nullopt})}};

  for (const auto& testCase : cases) {
    for (const auto compareFlags : allFlags()) {
      SCOPED_TRACE(
          testCase.type->toString() +
          (compareFlags.ascending ? " ASC" : " DESC") +
          (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));
      verifyProperty(makeRows({testCase.vector}), {compareFlags});
    }
  }
}

TEST_F(RadixSortKeyCodecTest, dateAndIntervalRoundTrip) {
  struct TestCase {
    TypePtr type;
    VectorPtr vector;
  };
  const std::vector<TestCase> cases{
      {DATE(),
       makeVector<int32_t>(
           DATE(),
           {std::numeric_limits<int32_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {INTERVAL_YEAR_MONTH(),
       makeVector<int32_t>(
           INTERVAL_YEAR_MONTH(),
           {std::numeric_limits<int32_t>::min(),
            -13,
            0,
            13,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {INTERVAL_DAY_TIME(),
       makeVector<int64_t>(
           INTERVAL_DAY_TIME(),
           {std::numeric_limits<int64_t>::min(),
            -1,
            0,
            1,
            std::numeric_limits<int64_t>::max(),
            std::nullopt})}};

  for (const auto& testCase : cases) {
    for (const auto compareFlags : allFlags()) {
      SCOPED_TRACE(testCase.type->toString());
      verifyProperty(makeRows({testCase.vector}), {compareFlags});
    }
  }
}

TEST_F(RadixSortKeyCodecTest, arrayRoundTripAndOrdering) {
  auto arrays = makeIntegerArrays(
      {std::vector<std::optional<int32_t>>{},
       std::vector<std::optional<int32_t>>{1},
       std::vector<std::optional<int32_t>>{1, 2},
       std::vector<std::optional<int32_t>>{1, 3},
       std::vector<std::optional<int32_t>>{1, std::nullopt},
       std::vector<std::optional<int32_t>>{2},
       std::nullopt});
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({arrays}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, fixedArrayRoundTripWithWrappedElements) {
  const std::vector<std::optional<vector_size_t>> sizes{
      0, 2, 3, std::nullopt, 1};

  auto dictionaryBase = makeVector<int32_t>(
      INTEGER(), {7, std::nullopt, -3, 11, 5, std::nullopt});
  auto dictionaryIndices =
      AlignedBuffer::allocate<vector_size_t>(6, pool_.get());
  const std::array<vector_size_t, 6> rawIndices{2, 1, 4, 0, 5, 3};
  std::memcpy(
      dictionaryIndices->asMutable<vector_size_t>(),
      rawIndices.data(),
      sizeof(rawIndices));
  auto dictionaryElements = BaseVector::wrapInDictionary(
      nullptr, dictionaryIndices, rawIndices.size(), dictionaryBase);
  auto dictionaryArrays =
      makeIntegerArraysWithElements(sizes, std::move(dictionaryElements));

  auto constantBase = makeVector<int32_t>(INTEGER(), {42});
  auto constantElements = BaseVector::wrapInConstant(6, 0, constantBase);
  auto constantArrays =
      makeIntegerArraysWithElements(sizes, std::move(constantElements));

  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({dictionaryArrays}), {compareFlags});
    verifyProperty(makeRows({constantArrays}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, rowRoundTripAndOrdering) {
  auto rows = makeNestedRows();
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({rows}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, customOrderableTypesRoundTrip) {
  auto json = makeStringVector(
      JSON(),
      {std::string(R"({"a":1})"),
       std::string(R"({"a":2})"),
       std::string(),
       std::nullopt});
  auto hll = makeStringVector(
      HYPERLOGLOG(),
      {std::string("\x00\x01\xff", 3),
       std::string("\x00\x02", 2),
       std::string(),
       std::nullopt});
  auto timestampWithTimeZone = std::make_shared<RowVector>(
      pool_.get(),
      TIMESTAMP_WITH_TIME_ZONE(),
      nullptr,
      5,
      std::vector<VectorPtr>{
          makeVector<int64_t>(BIGINT(), {-1, 0, 0, 1, std::nullopt}),
          makeVector<int16_t>(SMALLINT(), {1, 1, 2, 1, 1})});

  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({json}), {compareFlags});
    verifyProperty(makeRows({hll}), {compareFlags});
    verifyProperty(makeRows({timestampWithTimeZone}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, recursiveArrayAndRowRoundTrip) {
  auto arrays = makeIntegerArrays(
      {std::vector<std::optional<int32_t>>{},
       std::vector<std::optional<int32_t>>{1},
       std::vector<std::optional<int32_t>>{1, 2},
       std::vector<std::optional<int32_t>>{1, std::nullopt},
       std::vector<std::optional<int32_t>>{2},
       std::vector<std::optional<int32_t>>{2, 0},
       std::nullopt});
  auto nestedRows = makeNestedRows();
  auto rowOfNested = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"array", "row"}, {arrays->type(), nestedRows->type()}),
      nullptr,
      arrays->size(),
      std::vector<VectorPtr>{arrays, nestedRows});
  rowOfNested->setNull(6, true);
  auto arrayOfRows = makeRowArrays();

  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({rowOfNested}), {compareFlags});
    verifyProperty(makeRows({arrayOfRows}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, mapRoundTripAndCanonicalOrdering) {
  auto maps = makeIntegerStringMaps();
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({maps}), {compareFlags});
  }

  auto codec = bind({maps->type()}, {flags(true, true)});
  EncodedKeyBatch keys;
  codec->encode(*makeRows({maps}), pool_.get(), keys);
  EXPECT_EQ(compareEncodedKeys(keys, 2, 3), 0);
  EXPECT_EQ(
      SortComparatorOracle::compare(*maps, 2, *maps, 3, flags(true, true)), 0);
}

TEST_F(RadixSortKeyCodecTest, mapVarcharVarcharKeyCanonicalOrdering) {
  const std::array<vector_size_t, 5> offsets{0, 0, 2, 4, 6};
  const std::array<vector_size_t, 5> sizes{0, 2, 2, 2, 0};
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
  auto mapKeys = makeStringVector(VARCHAR(), {"b", "a", "a", "b", "b", "a"});
  auto mapValues = makeStringVector(VARCHAR(), {"2", "1", "1", "2", "2", "1"});
  auto maps = std::make_shared<MapVector>(
      pool_.get(),
      MAP(VARCHAR(), VARCHAR()),
      nullptr,
      offsets.size(),
      std::move(offsetBuffer),
      std::move(sizeBuffer),
      mapKeys,
      mapValues);
  maps->setNull(4, true);

  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({maps}), {compareFlags});
  }

  auto codec = bind({maps->type()}, {flags(true, true)});
  EncodedKeyBatch keys;
  codec->encode(*makeRows({maps}), pool_.get(), keys);
  EXPECT_EQ(compareEncodedKeys(keys, 1, 2), 0);
  EXPECT_EQ(
      SortComparatorOracle::compare(*maps, 1, *maps, 2, flags(true, true)), 0);
}

TEST_F(RadixSortKeyCodecTest, floatingPointRoundTrip) {
  const auto floatNan = std::numeric_limits<float>::quiet_NaN();
  const auto doubleNan = std::numeric_limits<double>::quiet_NaN();
  auto floats = makeVector<float>(
      REAL(),
      {-std::numeric_limits<float>::infinity(),
       -std::numeric_limits<float>::max(),
       -1.0f,
       -0.0f,
       0.0f,
       1.0f,
       std::numeric_limits<float>::max(),
       std::numeric_limits<float>::infinity(),
       floatNan,
       std::nullopt});
  auto doubles = makeVector<double>(
      DOUBLE(),
      {-std::numeric_limits<double>::infinity(),
       -std::numeric_limits<double>::max(),
       -1.0,
       -0.0,
       0.0,
       1.0,
       std::numeric_limits<double>::max(),
       std::numeric_limits<double>::infinity(),
       doubleNan,
       std::nullopt});
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({floats}), {compareFlags});
    verifyProperty(makeRows({doubles}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, timestampNanosRoundTrip) {
  auto timestamps = makeVector<Timestamp>(
      TIMESTAMP(),
      {Timestamp::min(),
       Timestamp(-1, Timestamp::kMaxNanos),
       Timestamp(0, 0),
       Timestamp(0, 1),
       Timestamp(0, Timestamp::kMaxNanos),
       Timestamp(1, 0),
       Timestamp::max(),
       std::nullopt});
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({timestamps}), {compareFlags});
  }
}

TEST_F(RadixSortKeyCodecTest, binarySafeStringsRoundTrip) {
  const std::string invalidUtf8{"\xc3\x28\xff", 3};
  const std::string controls{"\x00\x01\x02\xff", 4};
  const std::vector<std::optional<std::string>> values{
      std::string(),
      std::string(12, 'a'),
      std::string(13, 'a'),
      std::string(4096, 'x'),
      controls,
      invalidUtf8,
      std::string("prefix"),
      std::string("prefix\x00", 7),
      std::nullopt};
  for (const auto& type : std::vector<TypePtr>{VARCHAR(), VARBINARY()}) {
    for (const auto compareFlags : allFlags()) {
      SCOPED_TRACE(type->toString());
      verifyProperty(
          makeRows({makeStringVector(type, values)}), {compareFlags});
    }
  }
}

TEST_F(RadixSortKeyCodecTest, longCommonPrefixAndWrappedInput) {
  std::string prefix(8192, 'p');
  auto strings = makeStringVector(
      VARCHAR(), {prefix + "a", prefix + "b", prefix + "\x00", std::nullopt});
  auto indices = AlignedBuffer::allocate<vector_size_t>(4, pool_.get());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  rawIndices[0] = 2;
  rawIndices[1] = 0;
  rawIndices[2] = 3;
  rawIndices[3] = 1;
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 4, strings);
  verifyProperty(makeRows({dictionary}), {flags(true, true)});

  auto integers = makeVector<int32_t>(INTEGER(), {7, 11});
  auto constant = BaseVector::wrapInConstant(4, 1, integers);
  verifyProperty(makeRows({constant}), {flags(false, false)});
}

TEST_F(
    RadixSortKeyCodecTest,
    lowCardinalityDictionaryStringKeyRoundTripAndOrdering) {
  constexpr vector_size_t kRows = 96;
  auto base = makeStringVector(
      VARCHAR(),
      {"video_play",
       "video_play_pause",
       "like",
       "follow",
       "share",
       "comment",
       "enter_homepage",
       "click_music"});
  auto indices = AlignedBuffer::allocate<vector_size_t>(kRows, pool_.get());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t row = 0; row < kRows; ++row) {
    rawIndices[row] = row % 10 < 6 ? row % 3 : row % base->size();
  }
  auto events = BaseVector::wrapInDictionary(nullptr, indices, kRows, base);
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({events}), {compareFlags}, false);
  }
}

TEST_F(RadixSortKeyCodecTest, multiKeyStringPrefixWithNullableTieBreaker) {
  auto events = makeStringVector(
      VARCHAR(),
      {"play", "play", "play", "click", "click", "share", "share", "share"});
  auto ids = makeVector<int64_t>(
      BIGINT(), {3, std::nullopt, 1, 2, std::nullopt, 9, 7, 8});
  auto groups = makeVector<int32_t>(
      INTEGER(), {1, 1, std::nullopt, 0, 1, 2, std::nullopt, 2});
  verifyProperty(
      makeRows({events, ids, groups}),
      {flags(true, true), flags(false, false), flags(true, false)});
}

TEST_F(RadixSortKeyCodecTest, unknownAndMultipleColumns) {
  for (const auto compareFlags : allFlags()) {
    verifyProperty(makeRows({makeUnknownVector(3)}), {compareFlags});
  }

  auto rows = makeRows(
      {makeVector<int32_t>(INTEGER(), {1, 1, 2, std::nullopt}),
       makeStringVector(
           VARCHAR(),
           {std::string("b"),
            std::string("a"),
            std::string("a"),
            std::string("z")}),
       makeVector<double>(
           DOUBLE(),
           {0.0, -0.0, std::numeric_limits<double>::quiet_NaN(), 1.0})});
  verifyProperty(
      rows, {flags(true, false), flags(false, true), flags(true, true)});
}

TEST_F(RadixSortKeyCodecTest, fixedSeedPropertyFuzz) {
  for (uint32_t seed = 0; seed < kFuzzSeeds; ++seed) {
    std::mt19937_64 random(seed);
    const auto compareFlags =
        allFlags()[(seed / 4) % static_cast<uint32_t>(allFlags().size())];
    SCOPED_TRACE(
        "seed=" + std::to_string(seed) +
        (compareFlags.ascending ? " ASC" : " DESC") +
        (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));

    if (seed % 4 == 0) {
      SCOPED_TRACE("type=BIGINT");
      std::vector<std::optional<int64_t>> values;
      values.reserve(kFuzzPairsPerSeed + 1);
      for (vector_size_t index = 0; index <= kFuzzPairsPerSeed; ++index) {
        values.push_back(
            index % 29 == 0
                ? std::nullopt
                : std::optional<int64_t>(static_cast<int64_t>(random())));
      }
      verifyProperty(
          makeRows({makeVector<int64_t>(BIGINT(), values)}),
          {compareFlags},
          false);
    } else if (seed % 4 == 1) {
      SCOPED_TRACE("type=DOUBLE");
      std::vector<std::optional<double>> values;
      values.reserve(kFuzzPairsPerSeed + 1);
      for (vector_size_t index = 0; index <= kFuzzPairsPerSeed; ++index) {
        if (index % 31 == 0) {
          values.push_back(std::nullopt);
        } else {
          uint64_t bits = random();
          double value;
          std::memcpy(&value, &bits, sizeof(value));
          values.push_back(value);
        }
      }
      verifyProperty(
          makeRows({makeVector<double>(DOUBLE(), values)}),
          {compareFlags},
          false);
    } else if (seed % 4 == 2) {
      SCOPED_TRACE("type=DECIMAL(38, 7)");
      std::vector<std::optional<int128_t>> values;
      values.reserve(kFuzzPairsPerSeed + 1);
      for (vector_size_t index = 0; index <= kFuzzPairsPerSeed; ++index) {
        auto value =
            HugeInt::build(random() & ((uint64_t{1} << 62) - 1), random());
        if ((random() & 1) != 0) {
          value = -value;
        }
        values.push_back(
            index % 37 == 0 ? std::nullopt : std::optional<int128_t>(value));
      }
      verifyProperty(
          makeRows({makeVector<int128_t>(DECIMAL(38, 7), values)}),
          {compareFlags},
          false);
    } else {
      SCOPED_TRACE("type=VARBINARY");
      std::vector<std::optional<std::string>> values;
      values.reserve(kFuzzPairsPerSeed + 1);
      for (vector_size_t index = 0; index <= kFuzzPairsPerSeed; ++index) {
        if (index % 41 == 0) {
          values.push_back(std::nullopt);
          continue;
        }
        const auto size = random() % 33;
        std::string value(size, '\0');
        for (auto& byte : value) {
          byte = static_cast<char>(random());
        }
        values.push_back(std::move(value));
      }
      verifyProperty(
          makeRows({makeStringVector(VARBINARY(), values)}),
          {compareFlags},
          false);
    }
  }
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
