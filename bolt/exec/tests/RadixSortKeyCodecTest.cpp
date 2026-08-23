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
  using TypeVector = VectorPtr;

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

  template <typename T, typename U = T>
  FlatVectorPtr<T> makeVector(
      const TypePtr& type,
      const std::vector<std::optional<U>>& values) {
    auto vector =
        BaseVector::create<FlatVector<T>>(type, values.size(), pool_.get());
    for (vector_size_t row = 0; row < values.size(); ++row) {
      if (values[row].has_value()) {
        vector->set(row, T(*values[row]));
      } else {
        vector->setNull(row, true);
      }
    }
    return vector;
  }

  FlatVectorPtr<StringView> makeStringVector(
      const TypePtr& type,
      const std::vector<std::optional<std::string>>& values) {
    return makeVector<StringView, std::string>(type, values);
  }

  template <typename Container>
  BufferPtr makeBuffer(const Container& values) {
    using T = typename Container::value_type;
    auto buffer = AlignedBuffer::allocate<T>(values.size(), pool_.get());
    std::copy(values.begin(), values.end(), buffer->template asMutable<T>());
    return buffer;
  }

  template <typename T>
  static std::vector<std::optional<T>> signedValues(
      T inner = T{1},
      T min = std::numeric_limits<T>::min(),
      T max = std::numeric_limits<T>::max()) {
    return {min, -inner, 0, inner, max, std::nullopt};
  }

  template <typename T>
  static std::vector<std::optional<T>> floatingValues() {
    return {
        -std::numeric_limits<T>::infinity(),
        -std::numeric_limits<T>::max(),
        -T{1},
        -T{0},
        T{0},
        T{1},
        std::numeric_limits<T>::max(),
        std::numeric_limits<T>::infinity(),
        std::numeric_limits<T>::quiet_NaN(),
        std::nullopt};
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
    std::vector<std::optional<vector_size_t>> sizes;
    std::vector<std::optional<int32_t>> elements;
    for (const auto& value : values) {
      if (!value.has_value()) {
        sizes.push_back(std::nullopt);
        continue;
      }
      sizes.push_back(value->size());
      elements.insert(elements.end(), value->begin(), value->end());
    }
    return makeArrays(
        sizes, makeVector<int32_t>(INTEGER(), std::move(elements)));
  }

  ArrayVectorPtr makeArrays(
      const std::vector<std::optional<vector_size_t>>& sizes,
      VectorPtr elements) {
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> rawSizes;
    vector_size_t offset = 0;
    for (const auto size : sizes) {
      offsets.push_back(offset);
      rawSizes.push_back(size.value_or(0));
      offset += size.value_or(0);
    }
    BOLT_CHECK_LE(offset, elements->size());
    auto arrays = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(elements->type()),
        nullptr,
        sizes.size(),
        makeBuffer(offsets),
        makeBuffer(rawSizes),
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
    return makeArrays({0, 1, 2, 1, 2, 1, std::nullopt}, makeNestedRows());
  }

  MapVectorPtr makeMaps(
      const std::vector<std::optional<vector_size_t>>& sizes,
      VectorPtr keys,
      VectorPtr values) {
    std::vector<vector_size_t> offsets;
    std::vector<vector_size_t> rawSizes;
    vector_size_t offset = 0;
    for (const auto size : sizes) {
      offsets.push_back(offset);
      rawSizes.push_back(size.value_or(0));
      offset += size.value_or(0);
    }
    auto maps = std::make_shared<MapVector>(
        pool_.get(),
        MAP(keys->type(), values->type()),
        nullptr,
        sizes.size(),
        makeBuffer(offsets),
        makeBuffer(rawSizes),
        std::move(keys),
        std::move(values));
    for (vector_size_t row = 0; row < sizes.size(); ++row) {
      if (!sizes[row].has_value()) {
        maps->setNull(row, true);
      }
    }
    return maps;
  }

  MapVectorPtr makeIntegerStringMaps() {
    return makeMaps(
        {0, 1, 2, 2, 2, 1, std::nullopt},
        makeVector<int32_t>(INTEGER(), {1, 2, 1, 1, 2, 2, 1, 1}),
        makeStringVector(VARCHAR(), {"a", "b", "a", "a", "b", "c", "a", "z"}));
  }

  MapVectorPtr makeStringStringMaps() {
    return makeMaps(
        {0, 2, 2, 1, std::nullopt, 0, 3},
        makeStringVector(
            VARCHAR(),
            {"b", "a", "a", "b", std::string(64, 'k'), "c", "a", "d"}),
        makeStringVector(
            VARCHAR(),
            {"2",
             "1",
             "1",
             "2",
             std::string(80, 'v'),
             "3",
             "1",
             std::string(33, 'z')}));
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
        for (vector_size_t right = left + 1; right < rows->size(); ++right) {
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
      RowVectorPtr& decoded,
      std::span<const uint8_t> decodedColumns = {},
      std::span<const uint8_t> mayHaveNulls = {},
      uint32_t encodedPrefixSize = 0,
      uint32_t firstColumn = 0) {
    std::vector<std::array<char, sizeof(uint64_t)>> fixedBytes;
    auto views = makeViews(keys, fixedBytes, encodedPrefixSize);
    BufferPtr cursorScratch;
    codec.decode(
        std::span<const EncodedKeyView>(views.data(), views.size()),
        decodedColumns,
        mayHaveNulls,
        pool_.get(),
        cursorScratch,
        decoded,
        firstColumn);
  }

  static void expectColumnEqual(
      const RowVector& input,
      const RowVector& decoded,
      uint32_t column,
      const CompareFlags& compareFlags) {
    ASSERT_NE(decoded.childAt(column), nullptr);
    for (vector_size_t row = 0; row < input.size(); ++row) {
      EXPECT_EQ(
          SortComparatorOracle::compare(
              *input.childAt(column),
              row,
              *decoded.childAt(column),
              row,
              compareFlags),
          0)
          << "row=" << row;
    }
  }

  void verifySelectiveDecode(
      const std::vector<TypeVector>& cases,
      bool canSkip) {
    constexpr std::array<uint8_t, 1> kSkipColumn{0};
    constexpr std::array<uint8_t, 1> kMayHaveNulls{1};
    const auto compareFlags = flags(false, true);
    for (const auto& vector : cases) {
      const auto& type = vector->type();
      SCOPED_TRACE(type->toString());
      auto input = makeRows({vector});
      auto codec = bind({type}, {compareFlags});
      EncodedKeyBatch keys;
      codec->encode(*input, pool_.get(), keys);
      RowVectorPtr decoded;
      decodeBatch(*codec, keys, decoded, kSkipColumn, kMayHaveNulls);
      ASSERT_EQ(decoded->size(), input->size());
      if (canSkip) {
        EXPECT_EQ(decoded->childAt(0), nullptr);
      } else {
        expectColumnEqual(*input, *decoded, 0, compareFlags);
      }
    }
  }

  void verifyAllFlags(
      const std::vector<TypeVector>& cases,
      bool verifyAllPairs = true) {
    for (const auto& vector : cases) {
      for (const auto compareFlags : allFlags()) {
        SCOPED_TRACE(
            vector->type()->toString() +
            (compareFlags.ascending ? " ASC" : " DESC") +
            (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));
        verifyProperty(makeRows({vector}), {compareFlags}, verifyAllPairs);
      }
    }
  }

  static void expectBuffersEqual(
      const BufferPtr& buffer,
      const std::vector<uint64_t>& expected) {
    EXPECT_EQ(
        std::vector<uint64_t>(
            buffer->as<uint64_t>(), buffer->as<uint64_t>() + expected.size()),
        expected);
  }

  static std::vector<EncodedKeyView> makeViews(
      const EncodedKeyBatch& keys,
      std::vector<std::array<char, sizeof(uint64_t)>>& fixedBytes,
      uint32_t prefixSize = 0) {
    std::vector<EncodedKeyView> views(keys.size());
    if (keys.format() == EncodedKeyFormat::kFixed64) {
      fixedBytes.resize(keys.size());
      for (vector_size_t row = 0; row < keys.size(); ++row) {
        auto word = keys.fixedKeyAt(row);
        if constexpr (std::endian::native == std::endian::little) {
          word = byteSwap(word);
        }
        storeUnaligned(fixedBytes[row].data(), word);
        views[row] = EncodedKeyView(
            std::string_view(fixedBytes[row].data(), fixedBytes[row].size()));
      }
      return views;
    }
    for (vector_size_t row = 0; row < keys.size(); ++row) {
      const auto key = keys.variableKeyAt(row);
      BOLT_CHECK_LE(prefixSize, key.size());
      views[row] = {key.substr(prefixSize)};
    }
    return views;
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
      DATE(),
      INTERVAL_DAY_TIME(),
      INTERVAL_YEAR_MONTH(),
      JSON(),
      HYPERLOGLOG(),
      TIMESTAMP_WITH_TIME_ZONE(),
      ARRAY(BIGINT()),
      ROW({INTEGER(), VARCHAR()}),
      ARRAY(MAP(INTEGER(), BIGINT())),
      MAP(INTEGER(), BIGINT()),
      MAP(INTEGER(), ROW({BIGINT(), VARCHAR()})),
  };
  for (const auto& type : supportedTypes) {
    EXPECT_TRUE(RadixSortKeyCodec::supportsEncodeDecode(*type))
        << type->toString();
  }
  for (const auto& type : std::vector<TypePtr>{
           VARIANT(), OPAQUE<int32_t>(), FUNCTION({BIGINT()}, BOOLEAN())}) {
    EXPECT_FALSE(RadixSortKeyCodec::supportsEncodeDecode(*type))
        << type->toString();
  }
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
  EXPECT_THROW(
      codec->encode(
          *makeRows({makeVector<int64_t>(BIGINT(), {1})}), nullptr, keys),
      BoltException);
}

TEST_F(RadixSortKeyCodecTest, leadingSkippableValidityOffsets) {
  struct Case {
    const char* name;
    std::vector<TypePtr> types;
    std::vector<uint8_t> mayHaveNulls;
    uint32_t radixWidth;
    std::vector<uint32_t> expected;
  };
  const std::vector<Case> cases{
      {"mixedWidths",
       {TINYINT(), SMALLINT(), INTEGER(), TINYINT()},
       {0, 0, 0, 0},
       8,
       {0, 2, 5}},
      {"nullableSecond", {TINYINT(), SMALLINT(), TINYINT()}, {0, 1, 0}, 8, {0}},
      {"variableFirst", {VARCHAR(), TINYINT()}, {0, 0}, 16, {0}},
      {"nullableVariableFirst", {VARCHAR(), TINYINT()}, {1, 0}, 16, {}},
      {"wordBoundary",
       {TINYINT(), TINYINT(), TINYINT(), TINYINT(), TINYINT()},
       {0, 0, 0, 0, 0},
       8,
       {0, 2, 4, 6}},
      {"nullableMiddle",
       {TINYINT(), TINYINT(), TINYINT(), TINYINT(), TINYINT()},
       {0, 0, 1, 0, 0},
       8,
       {0, 2}},
      {"nullableFirst",
       {TINYINT(), TINYINT(), TINYINT(), TINYINT(), TINYINT()},
       {1, 0, 0, 0, 0},
       8,
       {}},
      {"secondMarkerInsideWord", {INTEGER(), INTEGER()}, {0, 0}, 8, {0, 5}},
      {"secondMarkerBeyondWord",
       {BIGINT(), BIGINT(), VARCHAR()},
       {0, 0, 0},
       16,
       {0, 9}},
      {"fixedPrefixThenVariable",
       {INTEGER(), INTEGER(), VARCHAR()},
       {0, 0, 0},
       16,
       {0, 5, 10}},
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(testCase.name);
    auto codec = bind(
        testCase.types,
        std::vector<CompareFlags>(testCase.types.size(), flags(true, true)));
    EXPECT_EQ(
        codec->leadingSkippableValidityOffsets(
            testCase.mayHaveNulls, testCase.radixWidth),
        testCase.expected);
  }
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

TEST_F(RadixSortKeyCodecTest, encodedBuffersReuseWithCopyOnWrite) {
  auto fixedCodec =
      bind({SMALLINT(), INTEGER()}, {flags(true, true), flags(true, true)});
  auto firstFixed = makeRows(
      {makeVector<int16_t>(SMALLINT(), {1, -1, std::nullopt}),
       makeVector<int32_t>(INTEGER(), {2, -2, 0})});
  auto secondFixed = makeRows(
      {makeVector<int16_t>(SMALLINT(), {7, std::nullopt, -9}),
       makeVector<int32_t>(INTEGER(), {-4, 5, 6})});

  EncodedKeyBatch fixedKeys;
  fixedCodec->encode(*firstFixed, pool_.get(), fixedKeys);
  const auto* firstFixedBuffer = fixedKeys.fixedKeys().get();
  auto before = pool_->stats();
  fixedCodec->encode(*secondFixed, pool_.get(), fixedKeys);
  auto after = pool_->stats();
  EXPECT_EQ(after.numAllocs - before.numAllocs, 0);
  EXPECT_EQ(fixedKeys.fixedKeys().get(), firstFixedBuffer);
  RowVectorPtr decoded;
  decodeBatch(*fixedCodec, fixedKeys, decoded);
  expectColumnEqual(*secondFixed, *decoded, 0, flags(true, true));
  expectColumnEqual(*secondFixed, *decoded, 1, flags(true, true));

  BufferPtr retainedFixed = fixedKeys.fixedKeys();
  const std::vector<uint64_t> retainedWords(
      retainedFixed->as<uint64_t>(),
      retainedFixed->as<uint64_t>() + secondFixed->size());
  fixedCodec->encode(*firstFixed, pool_.get(), fixedKeys);
  EXPECT_NE(fixedKeys.fixedKeys().get(), retainedFixed.get());
  expectBuffersEqual(retainedFixed, retainedWords);

  auto variableCodec = bind({BIGINT()}, {flags(true, true)});
  auto firstVariable =
      makeRows({makeVector<int64_t>(BIGINT(), {1, -1, std::nullopt})});
  auto secondVariable =
      makeRows({makeVector<int64_t>(BIGINT(), {7, std::nullopt, -9})});
  EncodedKeyBatch variableKeys;
  variableCodec->encode(*firstVariable, pool_.get(), variableKeys);
  const auto* firstOffsets = variableKeys.offsets().get();
  before = pool_->stats();
  variableCodec->encode(*secondVariable, pool_.get(), variableKeys);
  after = pool_->stats();
  EXPECT_EQ(after.numAllocs - before.numAllocs, 1);
  EXPECT_EQ(variableKeys.offsets().get(), firstOffsets);
  decodeBatch(*variableCodec, variableKeys, decoded);
  expectColumnEqual(*secondVariable, *decoded, 0, flags(true, true));

  BufferPtr retainedOffsets = variableKeys.offsets();
  const std::vector<uint64_t> retainedOffsetValues(
      retainedOffsets->as<uint64_t>(),
      retainedOffsets->as<uint64_t>() + secondVariable->size() + 1);
  variableCodec->encode(*firstVariable, pool_.get(), variableKeys);
  EXPECT_NE(variableKeys.offsets().get(), retainedOffsets.get());
  expectBuffersEqual(retainedOffsets, retainedOffsetValues);

  auto emptyVariable = makeRows(
      {BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool_.get())});
  const auto* reusableOffsets = variableKeys.offsets().get();
  variableCodec->encode(*emptyVariable, pool_.get(), variableKeys);
  EXPECT_EQ(variableKeys.offsets().get(), reusableOffsets);
  ASSERT_EQ(variableKeys.offsets()->size(), sizeof(uint64_t));
  EXPECT_EQ(variableKeys.offsets()->as<uint64_t>()[0], 0);
  EXPECT_EQ(variableKeys.data(), nullptr);
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
  RowVectorPtr decoded;
  const std::vector<uint8_t> decodedColumns{0, 1, 1};
  const std::vector<uint8_t> mayHaveNulls{1, 1, 1};
  decodeBatch(*codec, keys, decoded, decodedColumns, mayHaveNulls);

  ASSERT_EQ(decoded->childAt(0), nullptr);
  expectColumnEqual(*rows, *decoded, 1, compareFlags[1]);
  expectColumnEqual(*rows, *decoded, 2, compareFlags[2]);
}

TEST_F(RadixSortKeyCodecTest, selectiveDecodeSkipsFixedTypes) {
  verifySelectiveDecode(
      {makeVector<bool>(BOOLEAN(), {true, std::nullopt}),
       makeVector<int8_t>(TINYINT(), {1, std::nullopt}),
       makeVector<int16_t>(SMALLINT(), {1, std::nullopt}),
       makeVector<int32_t>(INTEGER(), {1, std::nullopt}),
       makeVector<int64_t>(BIGINT(), {1, std::nullopt}),
       makeVector<int128_t>(HUGEINT(), {1, std::nullopt}),
       makeVector<float>(REAL(), {1.5F, std::nullopt}),
       makeVector<double>(DOUBLE(), {1.5, std::nullopt}),
       makeVector<int64_t>(DECIMAL(18, 4), {1, std::nullopt}),
       makeVector<int128_t>(DECIMAL(38, 18), {1, std::nullopt}),
       makeVector<int32_t>(DATE(), {1, std::nullopt}),
       makeVector<int64_t>(INTERVAL_DAY_TIME(), {1, std::nullopt}),
       makeVector<int32_t>(INTERVAL_YEAR_MONTH(), {1, std::nullopt}),
       makeVector<Timestamp>(TIMESTAMP(), {Timestamp(1, 2), std::nullopt}),
       makeUnknownVector(2)},
      true);
}

TEST_F(
    RadixSortKeyCodecTest,
    selectiveDecodeCannotSkipVariableAndComplexTypes) {
  auto timestampWithTimeZone = std::make_shared<RowVector>(
      pool_.get(),
      TIMESTAMP_WITH_TIME_ZONE(),
      nullptr,
      4,
      std::vector<VectorPtr>{
          makeVector<int64_t>(BIGINT(), {-1, 0, 1, std::nullopt}),
          makeVector<int16_t>(SMALLINT(), {1, 2, 3, 4})});
  auto arrays = makeIntegerArrays(
      {std::vector<std::optional<int32_t>>{},
       std::vector<std::optional<int32_t>>{1, std::nullopt},
       std::vector<std::optional<int32_t>>{2, 3},
       std::nullopt});
  auto rows = makeNestedRows();
  auto rowOfNested = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"array", "row"}, {arrays->type(), rows->type()}),
      nullptr,
      4,
      std::vector<VectorPtr>{arrays, rows});
  rowOfNested->setNull(3, true);

  verifySelectiveDecode(
      {makeStringVector(
           VARCHAR(), {"one", std::string(80, 'x'), std::nullopt, "four"}),
       makeStringVector(
           VARBINARY(),
           {std::string("\x00", 1),
            std::string("\xff", 1),
            std::nullopt,
            std::string("\x01\x02", 2)}),
       makeStringVector(JSON(), {"1", "{\"a\":2}", std::nullopt, "[4]"}),
       makeStringVector(
           HYPERLOGLOG(), {"one", std::string(80, 'h'), std::nullopt, "four"}),
       timestampWithTimeZone,
       arrays,
       makeIntegerStringMaps(),
       makeStringStringMaps(),
       rows,
       rowOfNested},
      false);
}

TEST_F(RadixSortKeyCodecTest, selectiveDecodeFixedPrefixAndComplexSuffix) {
  auto arrays = makeIntegerArrays(
      {std::vector<std::optional<int32_t>>{1, 2},
       std::vector<std::optional<int32_t>>{1, std::nullopt},
       std::vector<std::optional<int32_t>>{},
       std::nullopt});
  auto rows = makeRows(
      {makeVector<int32_t>(INTEGER(), {10, 20, 30, 40}),
       makeVector<int64_t>(BIGINT(), {100, std::nullopt, 300, 400}),
       arrays});
  const std::vector<CompareFlags> compareFlags{
      flags(true, true), flags(false, false), flags(true, true)};
  auto codec = bind({INTEGER(), BIGINT(), arrays->type()}, compareFlags);
  EncodedKeyBatch keys;
  codec->encode(*rows, pool_.get(), keys);
  RowVectorPtr decoded;
  const std::array<uint8_t, 3> decodedColumns{0, 0, 1};
  const std::array<uint8_t, 3> mayHaveNulls{0, 1, 1};
  decodeBatch(*codec, keys, decoded, decodedColumns, mayHaveNulls);
  ASSERT_EQ(decoded->size(), rows->size());
  EXPECT_EQ(decoded->childAt(0), nullptr);
  EXPECT_EQ(decoded->childAt(1), nullptr);
  expectColumnEqual(*rows, *decoded, 2, compareFlags[2]);
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
  const std::vector<TypeVector> cases{
      makeVector<bool>(BOOLEAN(), {false, true, std::nullopt}),
      makeVector<int8_t>(TINYINT(), signedValues<int8_t>()),
      makeVector<int16_t>(SMALLINT(), signedValues<int16_t>()),
      makeVector<int32_t>(INTEGER(), signedValues<int32_t>()),
      makeVector<int64_t>(BIGINT(), signedValues<int64_t>()),
      makeVector<int128_t>(
          HUGEINT(), signedValues<int128_t>(1, int128Min, int128Max)),
      makeVector<int64_t>(
          DECIMAL(18, 4),
          signedValues<int64_t>(
              1, -999999999999999999LL, 999999999999999999LL)),
      makeVector<int128_t>(
          DECIMAL(38, 18),
          signedValues<int128_t>(1, -decimal38Max, decimal38Max))};
  verifyAllFlags(cases);
}

TEST_F(RadixSortKeyCodecTest, dateAndIntervalRoundTrip) {
  verifyAllFlags({
      makeVector<int32_t>(DATE(), signedValues<int32_t>()),
      makeVector<int32_t>(INTERVAL_YEAR_MONTH(), signedValues<int32_t>(13)),
      makeVector<int64_t>(INTERVAL_DAY_TIME(), signedValues<int64_t>()),
  });
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
  verifyAllFlags({arrays});
}

TEST_F(RadixSortKeyCodecTest, fixedArrayRoundTripWithWrappedElements) {
  const std::vector<std::optional<vector_size_t>> sizes{
      0, 2, 3, std::nullopt, 1};

  auto dictionaryBase = makeVector<int32_t>(
      INTEGER(), {7, std::nullopt, -3, 11, 5, std::nullopt});
  const std::array<vector_size_t, 6> rawIndices{2, 1, 4, 0, 5, 3};
  auto dictionaryElements = BaseVector::wrapInDictionary(
      nullptr, makeBuffer(rawIndices), rawIndices.size(), dictionaryBase);
  auto dictionaryArrays = makeArrays(sizes, std::move(dictionaryElements));

  auto constantBase = makeVector<int32_t>(INTEGER(), {42});
  auto constantElements = BaseVector::wrapInConstant(6, 0, constantBase);
  auto constantArrays = makeArrays(sizes, std::move(constantElements));

  verifyAllFlags({dictionaryArrays, constantArrays});
}

TEST_F(RadixSortKeyCodecTest, rowRoundTripAndOrdering) {
  verifyAllFlags({makeNestedRows()});
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

  verifyAllFlags({json, hll, timestampWithTimeZone});
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

  verifyAllFlags({rowOfNested, arrayOfRows});
}

TEST_F(RadixSortKeyCodecTest, mapRoundTripAndCanonicalOrdering) {
  verifyAllFlags({makeIntegerStringMaps()});
}

TEST_F(RadixSortKeyCodecTest, mapVarcharVarcharKeyCanonicalOrdering) {
  verifyAllFlags({makeStringStringMaps()});
}

TEST_F(RadixSortKeyCodecTest, nestedComplexVariableKeySizes) {
  auto mapElements = makeMaps(
      {2, std::nullopt, 0, 1},
      makeStringVector(VARCHAR(), {"b", "a", std::string("\x00", 1)}),
      makeVector<int64_t>(BIGINT(), {2, 1, std::nullopt}));
  auto arrayMaps = makeArrays({2, 2, std::nullopt}, mapElements);
  auto rowKey = makeRows(
      {arrayMaps,
       makeStringVector(
           VARCHAR(),
           {std::string("x"), std::nullopt, std::string("\x01z", 2)})});
  auto input = makeRows({rowKey});
  const std::vector<CompareFlags> compareFlags{flags(true, true)};

  verifyProperty(input, compareFlags);

  auto codec = bind({rowKey->type()}, compareFlags);
  EncodedKeyBatch keys;
  codec->encode(*input, pool_.get(), keys);

  ASSERT_EQ(keys.format(), EncodedKeyFormat::kVariableBinary);
  ASSERT_EQ(keys.size(), 3);
  EXPECT_EQ(keys.variableKeyAt(0).size(), 34);
  EXPECT_EQ(keys.variableKeyAt(1).size(), 15);
  EXPECT_EQ(keys.variableKeyAt(2).size(), 7);
}

TEST_F(RadixSortKeyCodecTest, floatingPointRoundTrip) {
  verifyAllFlags(
      {makeVector<float>(REAL(), floatingValues<float>()),
       makeVector<double>(DOUBLE(), floatingValues<double>())});
}

TEST_F(RadixSortKeyCodecTest, timestampNanosRoundTrip) {
  verifyAllFlags({makeVector<Timestamp>(
      TIMESTAMP(),
      {Timestamp::min(),
       Timestamp(-1, Timestamp::kMaxNanos),
       Timestamp(0, 0),
       Timestamp(0, 1),
       Timestamp(0, Timestamp::kMaxNanos),
       Timestamp(1, 0),
       Timestamp::max(),
       std::nullopt})});
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
  verifyAllFlags(
      {makeStringVector(VARCHAR(), values),
       makeStringVector(VARBINARY(), values)});
}

TEST_F(RadixSortKeyCodecTest, longCommonPrefixAndWrappedInput) {
  std::string prefix(8192, 'p');
  auto strings = makeStringVector(
      VARCHAR(), {prefix + "a", prefix + "b", prefix + "\x00", std::nullopt});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr,
      makeBuffer(std::array<vector_size_t, 4>{2, 0, 3, 1}),
      4,
      strings);
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
  verifyAllFlags({events}, false);
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
  verifyAllFlags({makeUnknownVector(3)});

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
