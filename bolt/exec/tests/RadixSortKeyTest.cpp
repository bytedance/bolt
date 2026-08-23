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
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {
using LayoutKind = RadixSortKeyLayoutKind;

struct LayoutDescriptor {
  LayoutKind kind;
  uint32_t width;
  uint32_t inlineCapacity;
  bool variable;
  bool hasPayload;
  std::optional<uint32_t> sizeOffset;
  std::optional<uint32_t> dataOffset;
  std::optional<uint32_t> payloadOffset;
  int32_t (*compare)(const char*, const char*);
};

// clang-format off
#define LAYOUT(KIND, RECORD, CAPACITY, VARIABLE, PAYLOAD, SIZE, DATA, PAYLOAD_OFFSET) \
  {LayoutKind::KIND, sizeof(RECORD), CAPACITY, VARIABLE, PAYLOAD, SIZE, DATA, PAYLOAD_OFFSET, \
   &RadixSortKeyOps<LayoutKind::KIND>::compare}
constexpr std::array<LayoutDescriptor, 9> kLayouts{{
    LAYOUT(kKeyOnlyFixed8, KeyOnlyFixed8Record, 8, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed16, KeyOnlyFixed16Record, 16, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed24, KeyOnlyFixed24Record, 24, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed32, KeyOnlyFixed32Record, 32, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyVariable32, KeyOnlyVariable32Record, 16, true, false, 16, 24, {}),
    LAYOUT(kKeyWithPayloadFixed16, KeyWithPayloadFixed16Record, 8, false, true, {}, {}, 8),
    LAYOUT(kKeyWithPayloadFixed24, KeyWithPayloadFixed24Record, 16, false, true, {}, {}, 16),
    LAYOUT(kKeyWithPayloadFixed32, KeyWithPayloadFixed32Record, 24, false, true, {}, {}, 24),
    LAYOUT(kKeyWithPayloadVariable32, KeyWithPayloadVariable32Record, 8, true, true, 8, 16, 24),
}};
#undef LAYOUT
// clang-format on

using StorageBoundaryScenario = std::pair<uint32_t, std::array<uint32_t, 3>>;

constexpr std::array<StorageBoundaryScenario, 5> kStorageBoundaries{{
    {1, {1, 1, 1}},
    {2, {2, 2, 1}},
    {31, {31, 31, 1}},
    {32, {32, 32, 1}},
    {33, {33, 33, 1}},
}};
struct PayloadPointers {
  explicit PayloadPointers(vector_size_t size) : storage(size), pointers(size) {
    for (vector_size_t row = 0; row < size; ++row) {
      pointers[row] = reinterpret_cast<char*>(&storage[row]);
    }
  }
  std::span<char* const> span(bool enabled) {
    return enabled ? std::span<char* const>{pointers}
                   : std::span<char* const>{};
  }
  std::vector<uint64_t> storage;
  std::vector<char*> pointers;
};

class RadixSortKeyTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-physical-sort-key-test")};

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
  FlatVectorPtr<T> makeMinMaxVector(const TypePtr& type) {
    const auto limits = std::numeric_limits<T>{};
    return makeVector<T>(type, {limits.min(), 0, limits.max(), {}});
  }
  template <typename T>
  FlatVectorPtr<T> makeFloatVector(const TypePtr& type) {
    using Limits = std::numeric_limits<T>;
    const auto infinity = Limits::infinity();
    return makeVector<T>(
        type, {-infinity, -T{0}, T{0}, infinity, Limits::quiet_NaN(), {}});
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

  RowVectorPtr makeRows(const VectorPtr& child) {
    return makeRows(std::vector<VectorPtr>{child});
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

  std::pair<std::unique_ptr<RadixSortKeyCodec>, EncodedKeyBatch> bindAndEncode(
      const RowVectorPtr& rows,
      const std::vector<CompareFlags>& compareFlags) {
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind(
        rows->type()->asRow().children(), compareFlags, codec);
    EncodedKeyBatch keys;
    codec->encode(*rows, pool_.get(), keys);
    return {std::move(codec), std::move(keys)};
  }

  static std::string encodedKeyAt(
      const EncodedKeyBatch& keys,
      vector_size_t row) {
    if (keys.format() == EncodedKeyFormat::kVariableBinary) {
      return std::string(keys.variableKeyAt(row));
    }
    auto word = keys.fixedKeyAt(row);
    if constexpr (std::endian::native == std::endian::little) {
      word = byteSwap(word);
    }
    std::string result(sizeof(word), '\0');
    storeUnaligned(result.data(), word);
    return result;
  }

  static int32_t encodedCompare(std::string_view left, std::string_view right) {
    const auto commonSize = std::min(left.size(), right.size());
    const auto result = std::memcmp(left.data(), right.data(), commonSize);
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
    return (left.size() > right.size()) - (left.size() < right.size());
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
      return (keys.fixedKeyAt(left) > keys.fixedKeyAt(right)) -
          (keys.fixedKeyAt(left) < keys.fixedKeyAt(right));
    }
    return encodedCompare(keys.variableKeyAt(left), keys.variableKeyAt(right));
  }

  template <typename Expected>
  static void expectPairwiseCompare(
      const RadixSortRunStorage& arena,
      vector_size_t size,
      Expected expected) {
    for (vector_size_t left = 0; left < size; ++left) {
      for (vector_size_t right = 0; right < size; ++right) {
        const auto actual = arena.keyAt(left).compare(arena.keyAt(right));
        const auto reference = expected(left, right);
        EXPECT_EQ(
            (actual > 0) - (actual < 0), (reference > 0) - (reference < 0));
      }
    }
  }

  static void expectPhysicalCompare(
      const RadixSortRunStorage& arena,
      const std::vector<std::string>& keys) {
    expectPairwiseCompare(arena, keys.size(), [&](auto left, auto right) {
      return encodedCompare(keys[left], keys[right]);
    });
  }

  static void verifyPhysicalStorage(
      const RadixSortKeyLayout& layout,
      const char* record,
      const RadixSortKey& physical,
      std::string_view original) {
    if (layout.isVariable()) {
      EXPECT_EQ(
          std::string_view(record, layout.heapKeyOffset()),
          original.substr(0, layout.heapKeyOffset()));
      EXPECT_EQ(physical.heapKey(), original.substr(layout.heapKeyOffset()));
      return;
    }
    RadixSortInlineKeyBuffer buffer;
    EncodedKeyView view;
    physical.deconstruct(buffer, view);
    EXPECT_EQ(view.bytes.substr(0, original.size()), original);
    EXPECT_EQ(
        view.bytes.substr(original.size()),
        std::string(layout.inlineCapacity() - original.size(), '\0'));
  }

  static void verifyStoredKey(
      const RadixSortRunStorage& arena,
      uint64_t row,
      std::string_view original,
      char* payload = nullptr) {
    const auto key = arena.keyAt(row);
    const auto& layout = arena.layout();
    verifyPhysicalStorage(layout, arena.keyDataAt(row), key, original);
    EXPECT_EQ(key.payload(), payload);
  }

  RowVectorPtr decodeArena(
      const RadixSortKeyCodec& codec,
      const RadixSortRunStorage& arena) {
    std::vector<std::string> logicalKeys(arena.size());
    std::vector<RadixSortInlineKeyBuffer> inlineBuffers(arena.size());
    std::vector<EncodedKeyView> views(arena.size());
    for (uint64_t row = 0; row < arena.size(); ++row) {
      if (arena.layout().isVariable()) {
        logicalKeys[row].assign(
            arena.keyDataAt(row), arena.layout().heapKeyOffset());
        logicalKeys[row].append(arena.keyAt(row).heapKey());
        views[row] = {logicalKeys[row]};
      } else {
        arena.keyAt(row).deconstruct(inlineBuffers[row], views[row]);
      }
    }
    RowVectorPtr decoded;
    BufferPtr cursorScratch;
    codec.decode(views, {}, {}, pool_.get(), cursorScratch, decoded);
    return decoded;
  }

  template <typename ExpectedCompare, typename VerifyDecoded>
  void verifyCodecStorage(
      const RowVectorPtr& rows,
      const std::vector<CompareFlags>& compareFlags,
      uint32_t keysPerBlock,
      bool withPayloadCase,
      bool expectVariable,
      ExpectedCompare expectedCompare,
      VerifyDecoded verifyDecoded) {
    auto [codec, encoded] = bindAndEncode(rows, compareFlags);
    const auto verify = [&](bool hasPayload) {
      auto layout =
          RadixSortKeyLayout::select(codec->maximumEncodedSize(), hasPayload);
      if (expectVariable) {
        ASSERT_TRUE(layout.isVariable());
      }
      PayloadPointers payloads(rows->size());
      RadixSortRunStorage arena(pool_.get(), layout, keysPerBlock, 64);
      arena.appendBatch(encoded, payloads.span(hasPayload));
      ASSERT_EQ(arena.size(), rows->size());
      for (vector_size_t row = 0; row < rows->size(); ++row) {
        verifyStoredKey(
            arena,
            row,
            encodedKeyAt(encoded, row),
            hasPayload ? payloads.pointers[row] : nullptr);
      }
      expectPairwiseCompare(arena, rows->size(), [&](auto left, auto right) {
        return expectedCompare(encoded, left, right);
      });
      auto decoded = decodeArena(*codec, arena);
      for (vector_size_t row = 0; row < rows->size(); ++row) {
        EXPECT_EQ(verifyDecoded(*decoded, row), 0);
      }
    };
    verify(false);
    if (withPayloadCase) {
      verify(true);
    }
  }

  template <typename Groups>
  static uint64_t groupBytes(const Groups& groups, bool capacity = false) {
    uint64_t bytes = 0;
    for (const auto& group : groups) {
      bytes += capacity ? group.capacity : group.used;
    }
    return bytes;
  }

  template <typename Blocks>
  static uint64_t blockBytes(const Blocks& blocks, uint64_t width) {
    uint64_t bytes = 0;
    for (const auto& block : blocks) {
      bytes += static_cast<uint64_t>(block.count) * width;
    }
    return bytes;
  }
};

TEST_F(RadixSortKeyTest, layoutAbiAndSelection) {
  for (const auto& expected : kLayouts) {
    auto layout = RadixSortKeyLayout::fromKind(expected.kind);
    EXPECT_EQ(layout.width(), expected.width);
    EXPECT_EQ(layout.inlineCapacity(), expected.inlineCapacity);
    EXPECT_EQ(layout.isVariable(), expected.variable);
    EXPECT_EQ(layout.hasPayload(), expected.hasPayload);
    EXPECT_EQ(layout.sizeOffset(), expected.sizeOffset);
    EXPECT_EQ(layout.dataOffset(), expected.dataOffset);
    EXPECT_EQ(layout.payloadOffset(), expected.payloadOffset);
  }

  using Selection =
      std::tuple<std::optional<uint64_t>, bool, RadixSortKeyLayoutKind>;
  const std::array<Selection, 19> selections{{
      {8, false, RadixSortKeyLayoutKind::kKeyOnlyFixed8},
      {9, false, RadixSortKeyLayoutKind::kKeyOnlyFixed16},
      {16, false, RadixSortKeyLayoutKind::kKeyOnlyFixed16},
      {17, false, RadixSortKeyLayoutKind::kKeyOnlyFixed24},
      {24, false, RadixSortKeyLayoutKind::kKeyOnlyFixed24},
      {25, false, RadixSortKeyLayoutKind::kKeyOnlyFixed32},
      {32, false, RadixSortKeyLayoutKind::kKeyOnlyFixed32},
      {33, false, RadixSortKeyLayoutKind::kKeyOnlyVariable32},
      {std::nullopt, false, RadixSortKeyLayoutKind::kKeyOnlyVariable32},
      {8, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16},
      {9, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {16, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {17, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {24, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {25, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
      {32, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
      {33, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
      {73, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
      {std::nullopt, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
  }};
  for (const auto& [maximumSize, hasPayload, kind] : selections) {
    EXPECT_EQ(RadixSortKeyLayout::select(maximumSize, hasPayload).kind(), kind);
  }

  EXPECT_THROW(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kInvalid),
      BoltException);
  EXPECT_THROW(RadixSortKeyLayout::select(0, true), BoltException);
}

TEST_F(RadixSortKeyTest, variableHeapKeyOffsetUsesTopLevelFixedBoundaries) {
  const auto assertOffset = [&](const std::vector<TypePtr>& types,
                                bool hasPayload,
                                uint32_t expectedOffset) {
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind(
        types,
        std::vector<CompareFlags>(types.size(), flags(true, true)),
        codec);
    auto layout =
        RadixSortKeyLayout::select(codec->maximumEncodedSize(), hasPayload);
    ASSERT_TRUE(layout.isVariable());
    EXPECT_EQ(
        codec->heapKeyOffsetForVariableLayout(layout.inlineCapacity()),
        expectedOffset);
  };

  assertOffset({INTEGER(), VARCHAR()}, false, 5);
  assertOffset({BIGINT(), BIGINT(), VARCHAR()}, false, 9);
  assertOffset({VARCHAR(), INTEGER()}, false, 0);
  assertOffset({INTEGER(), VARCHAR()}, true, 5);
  assertOffset({BIGINT(), VARCHAR()}, true, 0);
}

TEST_F(RadixSortKeyTest, allLayoutsRoundTripAndCompare) {
  for (const auto& descriptor : kLayouts) {
    const auto layout = RadixSortKeyLayout::fromKind(descriptor.kind);
    std::vector<std::string> keys;
    keys.emplace_back(layout.inlineCapacity() - 1, 'a');
    keys.emplace_back(layout.inlineCapacity(), 'b');
    if (layout.isVariable()) {
      keys.emplace_back(layout.inlineCapacity() + 1, 'c');
      std::string commonPrefix(layout.inlineCapacity() + 128, 'p');
      keys.push_back(commonPrefix + 'a');
      keys.push_back(commonPrefix + 'b');
    }
    PayloadPointers payloads(keys.size());

    RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
    for (uint64_t index = 0; index < keys.size(); ++index) {
      auto* payload = layout.hasPayload() ? payloads.pointers[index] : nullptr;
      arena.append(keys[index], payload);
      verifyStoredKey(arena, index, keys[index], payload);
    }
    expectPhysicalCompare(arena, keys);
  }
}

TEST_F(RadixSortKeyTest, variableLayoutStoresHeapFromColumnBoundary) {
  auto layout = RadixSortKeyLayout::select(std::nullopt, false, 5);
  ASSERT_EQ(layout.kind(), LayoutKind::kKeyOnlyVariable32);
  ASSERT_EQ(layout.heapKeyOffset(), 5);
  const std::vector<std::string> keys{
      std::string("abcdef"),
      std::string("abcde") + std::string(32, 'x'),
      std::string("abcdf") + std::string(32, 'x'),
  };
  RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
  for (const auto& key : keys) {
    arena.append(key);
  }
  ASSERT_EQ(arena.size(), keys.size());
  EXPECT_EQ(arena.keyAt(0).heapSize(), keys[0].size() - layout.heapKeyOffset());
  EXPECT_EQ(arena.keyAt(0).heapKey(), "f");
  ASSERT_NE(arena.keyAt(1).heapKeyData(), nullptr);
  EXPECT_EQ(arena.keyAt(1).heapSize(), keys[1].size() - layout.heapKeyOffset());
  EXPECT_EQ(
      std::string_view(arena.keyAt(1).heapKeyData(), arena.keyAt(1).heapSize()),
      std::string_view(keys[1]).substr(layout.heapKeyOffset()));
  EXPECT_EQ(
      arena.keyAt(0).compare(arena.keyAt(1)), encodedCompare(keys[0], keys[1]));
  EXPECT_EQ(
      arena.keyAt(1).compare(arena.keyAt(2)), encodedCompare(keys[1], keys[2]));
  for (uint64_t row = 0; row < arena.size(); ++row) {
    verifyStoredKey(arena, row, keys[row]);
  }
}

TEST_F(RadixSortKeyTest, suffixCompareDoesNotReadRadixPrefix) {
  auto layout = RadixSortKeyLayout::select(std::nullopt, false, 9);
  std::string left(40, 's');
  std::string right = left;
  left[12] = 'a';
  right[12] = 'z';

  RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
  arena.append(left);
  arena.append(right);

  ASSERT_LT(arena.keyAt(0).compare(arena.keyAt(1)), 0);
  EXPECT_EQ(
      RadixSortKeyOps<LayoutKind::kKeyOnlyVariable32>::compareSuffix(
          arena.keyDataAt(0),
          arena.keyDataAt(1),
          layout.heapKeyOffset(),
          layout.radixWidth()),
      0);
}

TEST_F(RadixSortKeyTest, radixSortUtilsBoundaryChecks) {
  EXPECT_EQ(checkedAlignUp<uint64_t>(0, 8), 0);
  EXPECT_EQ(checkedAlignUp<uint64_t>(9, 8), 16);
  EXPECT_EQ(checkedAlignUp<uint64_t>(9, 0), std::nullopt);
  EXPECT_EQ(checkedAlignUp<uint64_t>(9, 3), std::nullopt);
  EXPECT_EQ(
      checkedAlignUp<uint64_t>(std::numeric_limits<uint64_t>::max(), 8),
      std::nullopt);

  EXPECT_TRUE(isValidRecordRelativeRange(16, 1, 15));
  EXPECT_FALSE(isValidRecordRelativeRange(16, 0, 0));
  EXPECT_TRUE(isValidRecordRelativeRange(16, 0, 0, 1, true));
  EXPECT_FALSE(isValidRecordRelativeRange(16, 0, 1, 1, true));
  EXPECT_FALSE(isValidRecordRelativeRange(16, 15, 2));
  EXPECT_FALSE(isValidRecordRelativeRange(16, 16, 0));
  EXPECT_FALSE(
      isValidRecordRelativeRange(16, std::numeric_limits<uint64_t>::max(), 2));
}

TEST_F(RadixSortKeyTest, knownPerWordByteSwap) {
  auto layout = RadixSortKeyLayout::fromKind(LayoutKind::kKeyOnlyFixed16);
  alignas(uint64_t) std::array<char, 16> storage{};
  const std::array<char, 16> encoded{
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08,
      0x11,
      0x12,
      0x13,
      0x14,
      0x15,
      0x16,
      0x17,
      0x18};
  RadixSortKey key(layout, storage.data());
  key.construct(std::string_view(encoded.data(), encoded.size()), nullptr);
  EXPECT_EQ(loadUnaligned<uint64_t>(storage.data()), 0x0102030405060708ULL);
  EXPECT_EQ(loadUnaligned<uint64_t>(storage.data() + 8), 0x1112131415161718ULL);
  verifyPhysicalStorage(
      layout,
      storage.data(),
      key,
      std::string_view(encoded.data(), encoded.size()));
}

TEST_F(RadixSortKeyTest, templatedCompareMatchesGenericCompare) {
  for (const auto& descriptor : kLayouts) {
    const auto layout = RadixSortKeyLayout::fromKind(descriptor.kind);
    std::vector<std::string> storage{
        std::string(layout.inlineCapacity() - 1, 'a'),
        std::string(layout.inlineCapacity(), 'b')};
    if (layout.isVariable()) {
      storage.push_back(std::string(layout.inlineCapacity() + 1, 'c'));
      storage.push_back(std::string(layout.inlineCapacity() + 64, 'p') + "a");
      storage.push_back(std::string(layout.inlineCapacity() + 64, 'p') + "b");
    }
    PayloadPointers payloads(storage.size());
    RadixSortRunStorage arena(pool_.get(), layout, 3, 64);
    std::vector<std::string_view> keys(storage.begin(), storage.end());
    arena.appendBatch(keys, payloads.span(layout.hasPayload()));
    expectPairwiseCompare(arena, arena.size(), [&](auto left, auto right) {
      return descriptor.compare(arena.keyDataAt(left), arena.keyDataAt(right));
    });
  }
}

TEST_F(RadixSortKeyTest, codecPhysicalCompareProperty) {
  const auto decimal38Max =
      HugeInt::fromString("99999999999999999999999999999999999999");
  const std::vector<VectorPtr> cases{
      makeVector<bool>(BOOLEAN(), {false, true, {}}),
      makeMinMaxVector<int8_t>(TINYINT()),
      makeMinMaxVector<int16_t>(SMALLINT()),
      makeMinMaxVector<int32_t>(INTEGER()),
      makeMinMaxVector<int64_t>(BIGINT()),
      makeMinMaxVector<int32_t>(DATE()),
      makeMinMaxVector<int32_t>(INTERVAL_YEAR_MONTH()),
      makeMinMaxVector<int64_t>(INTERVAL_DAY_TIME()),
      makeVector<int128_t>(
          HUGEINT(),
          {HugeInt::build(uint64_t{1} << 63, 0),
           0,
           HugeInt::build(
               (uint64_t{1} << 63) - 1, std::numeric_limits<uint64_t>::max()),
           {}}),
      makeFloatVector<float>(REAL()),
      makeFloatVector<double>(DOUBLE()),
      makeVector<int64_t>(
          DECIMAL(18, 4), {-999999999999999999LL, 0, 999999999999999999LL, {}}),
      makeVector<int128_t>(
          DECIMAL(38, 18), {-decimal38Max, 0, decimal38Max, {}}),
      makeVector<Timestamp>(
          TIMESTAMP(),
          {Timestamp::min(),
           Timestamp(0, 0),
           Timestamp(0, 1),
           Timestamp::max(),
           {}}),
      makeUnknownVector(4),
      makeStringVector(
          VARCHAR(),
          {std::string(),
           std::string("\xc3\x28\xff", 3),
           std::string(128, 'x'),
           {}}),
      makeStringVector(
          VARBINARY(),
          {std::string(),
           std::string("\x00\x01\xff", 3),
           std::string(128, 'y'),
           {}})};

  for (const auto& input : cases) {
    for (const auto compareFlags : allFlags()) {
      SCOPED_TRACE(
          input->type()->toString() +
          (compareFlags.ascending ? " ASC" : " DESC") +
          (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));
      auto rows = makeRows(input);
      verifyCodecStorage(
          rows,
          {compareFlags},
          3,
          true,
          false,
          [](const auto& encoded, auto left, auto right) {
            return compareEncodedKeys(encoded, left, right);
          },
          [&](const RowVector& decoded, auto row) {
            return SortComparatorOracle::compare(
                *input, row, *decoded.childAt(0), row, compareFlags);
          });
    }
  }
}

TEST_F(RadixSortKeyTest, multipleColumnCodecPhysicalCompare) {
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
  const std::vector<CompareFlags> compareFlags{
      flags(true, false), flags(false, true), flags(true, true)};
  const std::vector<column_index_t> channels{0, 1, 2};
  verifyCodecStorage(
      rows,
      compareFlags,
      2,
      true,
      false,
      [](const auto& encoded, auto left, auto right) {
        return compareEncodedKeys(encoded, left, right);
      },
      [&](const RowVector& decoded, auto row) {
        return SortComparatorOracle::compareRows(
            *rows, row, decoded, row, channels, compareFlags);
      });
}

TEST_F(RadixSortKeyTest, nestedCodecPhysicalCompare) {
  auto elements =
      makeVector<int32_t>(INTEGER(), {1, 1, 2, 1, 3, 1, std::nullopt, 2});
  auto offsets = AlignedBuffer::allocate<vector_size_t>(6, pool_.get());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(6, pool_.get());
  const std::array<vector_size_t, 6> rawOffsets{0, 0, 1, 3, 5, 7};
  const std::array<vector_size_t, 6> rawSizes{0, 1, 2, 2, 2, 1};
  std::memcpy(
      offsets->asMutable<vector_size_t>(),
      rawOffsets.data(),
      sizeof(rawOffsets));
  std::memcpy(
      sizes->asMutable<vector_size_t>(), rawSizes.data(), sizeof(rawSizes));
  auto arrays = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(INTEGER()),
      nullptr,
      rawOffsets.size(),
      offsets,
      sizes,
      elements);
  auto rows = makeRows(
      {arrays, makeVector<int64_t>(BIGINT(), {5, 4, 3, 2, 1, std::nullopt})});
  const std::vector<CompareFlags> compareFlags{
      flags(true, true), flags(false, false)};
  const std::vector<column_index_t> channels{0, 1};
  verifyCodecStorage(
      rows,
      compareFlags,
      3,
      false,
      true,
      [&](const auto&, auto left, auto right) {
        return SortComparatorOracle::compareRows(
            *rows, left, *rows, right, channels, compareFlags);
      },
      [&](const RowVector& decoded, auto row) {
        return SortComparatorOracle::compareRows(
            *rows, row, decoded, row, channels, compareFlags);
      });
}

TEST_F(RadixSortKeyTest, blockAndHeapGroups) {
  auto layout = RadixSortKeyLayout::fromKind(LayoutKind::kKeyOnlyVariable32);
  RadixSortRunStorage arena(pool_.get(), layout, 3, 64);
  const std::vector<std::string> keys{
      std::string(16, 'i'),
      std::string(17, 'a'),
      std::string(20, 'b'),
      std::string(27, 'c'),
      std::string(100, 'd'),
      std::string(17, 'e')};
  for (const auto& key : keys) {
    arena.append(key);
  }

  ASSERT_EQ(arena.keyBlocks().size(), 2);
  EXPECT_EQ(arena.keyBlocks()[0].capacity, 3);
  EXPECT_EQ(arena.keyBlocks()[0].count, 3);
  EXPECT_EQ(arena.keyBlocks()[1].capacity, 3);
  EXPECT_EQ(arena.keyBlocks()[1].count, 3);
  ASSERT_EQ(arena.keyHeapGroups().size(), 4);
  EXPECT_EQ(arena.keyHeapGroups()[0].capacity, 64);
  EXPECT_EQ(arena.keyHeapGroups()[0].used, 53);
  EXPECT_EQ(arena.keyHeapGroups()[0].keyCount, 3);
  EXPECT_EQ(arena.keyHeapGroups()[1].capacity, 64);
  EXPECT_EQ(arena.keyHeapGroups()[1].used, 27);
  EXPECT_EQ(arena.keyHeapGroups()[1].keyCount, 1);
  EXPECT_EQ(arena.keyHeapGroups()[2].capacity, 100);
  EXPECT_EQ(arena.keyHeapGroups()[2].used, 100);
  EXPECT_EQ(arena.keyHeapGroups()[2].keyCount, 1);
  EXPECT_EQ(arena.keyHeapGroups()[3].capacity, 64);
  EXPECT_EQ(arena.keyHeapGroups()[3].used, 17);
  EXPECT_EQ(arena.keyHeapGroups()[3].keyCount, 1);

  const auto heapBytes = groupBytes(arena.keyHeapGroups());
  const auto keyBytes = keys.size() * layout.width();
  EXPECT_EQ(heapBytes, 197);
  EXPECT_GE(
      static_cast<uint64_t>(arena.allocatedBytes()), keyBytes + heapBytes);
}

TEST_F(RadixSortKeyTest, storageBlockAllocationAndAddressingBoundaries) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(LayoutKind::kKeyWithPayloadFixed16);
  auto payloadLayout = PayloadRowLayout::create(ROW({"value"}, {BIGINT()}));

  for (const auto& [rowsPerBlock, blockCounts] : kStorageBoundaries) {
    SCOPED_TRACE("rowsPerBlock=" + std::to_string(rowsPerBlock));
    RadixSortRunStorage arena(
        pool_.get(),
        keyLayout,
        rowsPerBlock,
        64,
        payloadLayout,
        rowsPerBlock,
        64);
    const auto rowCount = static_cast<vector_size_t>(rowsPerBlock * 2 + 1);
    PayloadRowBatch payloadBatch;
    arena.allocateFixedPayloadRowBatch(rowCount, payloadBatch);

    std::vector<std::string> keyStorage(rowCount);
    std::vector<std::string_view> keys(rowCount);
    std::vector<char*> payloads(rowCount);
    for (vector_size_t row = 0; row < rowCount; ++row) {
      keyStorage[row] = std::string(8, static_cast<char>('a' + row % 26));
      keyStorage[row][7] = static_cast<char>(row);
      keys[row] = keyStorage[row];
      payloads[row] = payloadBatch.rowAt(row);
    }
    arena.appendBatch(keys, payloads);

    ASSERT_EQ(arena.size(), rowCount);
    ASSERT_EQ(arena.payloadSize(), rowCount);
    ASSERT_EQ(arena.keyBlocks().size(), 3);
    ASSERT_EQ(arena.payloadFixedBlocks().size(), 3);
    for (uint32_t block = 0; block < 3; ++block) {
      const auto expectedCount = blockCounts[block];
      EXPECT_EQ(arena.keyBlocks()[block].capacity, rowsPerBlock);
      EXPECT_EQ(arena.keyBlocks()[block].count, expectedCount);
      EXPECT_EQ(arena.payloadFixedBlocks()[block].capacity, rowsPerBlock);
      EXPECT_EQ(arena.payloadFixedBlocks()[block].count, expectedCount);
    }

    for (vector_size_t row = 0; row < rowCount; ++row) {
      const auto block = static_cast<uint32_t>(row) / rowsPerBlock;
      const auto indexInBlock = static_cast<uint32_t>(row) % rowsPerBlock;
      EXPECT_EQ(
          arena.keyDataAt(row),
          arena.keyBlocks()[block].base +
              static_cast<uint64_t>(indexInBlock) * keyLayout.width());
      EXPECT_EQ(
          payloadBatch.rowAt(row),
          arena.payloadFixedBlocks()[block].base +
              static_cast<uint64_t>(indexInBlock) * payloadLayout->rowWidth());
      EXPECT_EQ(arena.keyAt(row).payload(), payloadBatch.rowAt(row));
      verifyPhysicalStorage(
          keyLayout, arena.keyDataAt(row), arena.keyAt(row), keys[row]);
    }

    for (uint64_t begin = 0; begin < arena.size();) {
      const auto range = arena.keyRangeAt(begin, rowCount);
      const auto expectedCount = std::min<vector_size_t>(
          rowsPerBlock - begin % rowsPerBlock, rowCount - begin);
      ASSERT_EQ(range.count, expectedCount);
      EXPECT_EQ(range.data, arena.keyDataAt(begin));
      begin += range.count;
    }
    EXPECT_EQ(arena.keyRangeAt(arena.size(), rowCount).count, 0);
  }
}

TEST_F(RadixSortKeyTest, storageKeyRangeConstAccessAndEncodedAppend) {
  auto rows = makeRows(makeVector<int32_t>(INTEGER(), {5, 1, 7, 3, 9}));
  auto [codec, encoded] = bindAndEncode(rows, {flags(true, true)});

  auto layout = RadixSortKeyLayout::select(codec->maximumEncodedSize(), true);
  PayloadPointers payloads(rows->size());
  RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
  arena.appendBatch(encoded, payloads.pointers);
  ASSERT_EQ(arena.size(), rows->size());

  const auto& constArena = arena;
  auto firstRange = constArena.keyRangeAt(0, 5);
  ASSERT_NE(firstRange.data, nullptr);
  EXPECT_EQ(firstRange.count, 2);
  EXPECT_EQ(firstRange.data, constArena.keyDataAt(0));
  auto secondRange = constArena.keyRangeAt(2, 2);
  ASSERT_NE(secondRange.data, nullptr);
  EXPECT_EQ(secondRange.count, 2);
  EXPECT_EQ(secondRange.data, constArena.keyDataAt(2));
  auto tailRange = constArena.keyRangeAt(4, 5);
  ASSERT_NE(tailRange.data, nullptr);
  EXPECT_EQ(tailRange.count, 1);
  EXPECT_EQ(constArena.keyRangeAt(arena.size(), 3).count, 0);
  EXPECT_EQ(constArena.keyRangeAt(0, 0).count, 0);

  for (uint64_t row = 0; row < arena.size(); ++row) {
    EXPECT_EQ(arena.keyAt(row).payload(), payloads.pointers[row]);
    EXPECT_EQ(constArena.keyAt(row).payload(), payloads.pointers[row]);
  }
}

TEST_F(RadixSortKeyTest, storageAppendsInlineVariableKeysToFixedLayouts) {
  struct Case {
    std::vector<VectorPtr> columns;
    RadixSortKeyLayoutKind kind;
  };
  auto strings = makeStringVector(VARCHAR(), {"a", "bb", "ccc"});
  auto integers = makeVector<int32_t>(INTEGER(), {3, 2, 1});
  const std::vector<Case> cases{
      {{strings}, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {{strings, integers}, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {{strings, integers}, RadixSortKeyLayoutKind::kKeyOnlyFixed24},
      {{strings, integers, makeVector<int32_t>(INTEGER(), {6, 5, 4})},
       RadixSortKeyLayoutKind::kKeyOnlyFixed32},
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(static_cast<uint8_t>(testCase.kind));
    auto rows = makeRows(testCase.columns);
    auto [codec, encoded] = bindAndEncode(
        rows,
        std::vector<CompareFlags>(rows->childrenSize(), flags(true, true)));
    ASSERT_EQ(encoded.format(), EncodedKeyFormat::kVariableBinary);
    std::vector<std::string> expectedKeys;
    expectedKeys.reserve(rows->size());
    for (vector_size_t row = 0; row < rows->size(); ++row) {
      expectedKeys.push_back(std::string(encoded.variableKeyAt(row)));
    }

    auto layout = RadixSortKeyLayout::fromKind(testCase.kind);
    PayloadPointers payloads(rows->size());
    RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
    arena.appendBatch(encoded, payloads.span(layout.hasPayload()));
    ASSERT_EQ(arena.size(), rows->size());
    for (uint64_t row = 0; row < arena.size(); ++row) {
      verifyStoredKey(
          arena,
          row,
          expectedKeys[row],
          layout.hasPayload() ? payloads.pointers[row] : nullptr);
    }
    expectPhysicalCompare(arena, expectedKeys);
  }
}

TEST_F(RadixSortKeyTest, fixedSeedPhysicalCompareProperty) {
  constexpr uint32_t kSeeds = 32;
  constexpr uint32_t kPairsPerSeed = 2'000;
  for (uint32_t seed = 0; seed < kSeeds; ++seed) {
    std::mt19937_64 random(seed);
    const auto kind = kLayouts[seed % 9].kind;
    const auto layout = RadixSortKeyLayout::fromKind(kind);
    SCOPED_TRACE(
        "seed=" + std::to_string(seed) +
        ", layout=" + std::to_string(static_cast<uint8_t>(kind)));

    std::vector<std::string> storage;
    std::vector<std::string_view> keys;
    storage.reserve(kPairsPerSeed + 1);
    keys.reserve(kPairsPerSeed + 1);
    for (uint32_t index = 0; index <= kPairsPerSeed; ++index) {
      const auto maxSize = layout.isVariable() ? layout.inlineCapacity() + 96
                                               : layout.inlineCapacity();
      const auto size = 1 + random() % maxSize;
      storage.emplace_back(size, '\0');
      for (auto& byte : storage.back()) {
        byte = static_cast<char>(1 + random() % 255);
      }
      keys.push_back(storage.back());
    }

    PayloadPointers payloads(keys.size());
    RadixSortRunStorage arena(pool_.get(), layout, 127, 4096);
    arena.appendBatch(keys, payloads.span(layout.hasPayload()));

    for (uint32_t index = 0; index < kPairsPerSeed; ++index) {
      const auto expected = encodedCompare(keys[index], keys[index + 1]);
      const auto actual = arena.keyAt(index).compare(arena.keyAt(index + 1));
      EXPECT_EQ((actual > 0) - (actual < 0), expected)
          << "left=" << index << ", right=" << index + 1;
    }
  }
}

TEST_F(RadixSortKeyTest, allocationRangeBoundaryAndClear) {
  auto leaf = rootPool_->addLeafChild("radix-sort-arena-clear-test");
  EXPECT_EQ(leaf->currentBytes(), 0);
  auto layout = RadixSortKeyLayout::fromKind(LayoutKind::kKeyOnlyFixed32);
  RadixSortRunStorage arena(leaf.get(), layout, 2048, 64 * 1024);
  EXPECT_EQ(arena.size(), 0);
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_TRUE(arena.keyBlocks().empty());
  EXPECT_TRUE(arena.keyHeapGroups().empty());
  arena.appendBatch(std::span<const std::string_view>{});
  EXPECT_EQ(leaf->currentBytes(), 0);
  const std::string key(32, 'k');
  while (arena.numRanges() < 2) {
    arena.append(key);
  }

  ASSERT_GE(arena.keyBlocks().size(), 2);
  EXPECT_EQ(arena.keyBlocks()[0].count, 2048);
  EXPECT_GE(arena.numRanges(), 2);
  EXPECT_GE(arena.allocatedBytes(), arena.size() * arena.layout().width());
  EXPECT_GT(leaf->currentBytes(), 0);

  arena.clear();
  EXPECT_EQ(arena.size(), 0);
  EXPECT_TRUE(arena.keyBlocks().empty());
  EXPECT_TRUE(arena.keyHeapGroups().empty());
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(RadixSortKeyTest, heapAllocationRangeBoundary) {
  auto leaf = rootPool_->addLeafChild("radix-sort-heap-range-test");
  auto layout = RadixSortKeyLayout::fromKind(LayoutKind::kKeyOnlyVariable32);
  RadixSortRunStorage arena(leaf.get(), layout, 4096, 32 * 1024);
  const std::string key(4096, 'h');
  arena.append(key);
  const auto initialRanges = arena.numRanges();
  ASSERT_GT(initialRanges, 0);

  while (arena.numRanges() == initialRanges && arena.size() < 10'000) {
    arena.append(key);
  }
  ASSERT_GT(arena.numRanges(), initialRanges);
  ASSERT_GT(arena.keyHeapGroups().size(), 1);
  for (uint64_t index = 0; index < arena.size(); ++index) {
    verifyStoredKey(arena, index, key);
  }

  arena.clear();
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(
    RadixSortKeyTest,
    storageEstimatedOutputBytesIncludesVariableKeyAndPayload) {
  constexpr vector_size_t kRows = 33;
  constexpr uint32_t kRowsPerBlock = 32;
  auto layout =
      RadixSortKeyLayout::fromKind(LayoutKind::kKeyWithPayloadVariable32);
  auto payloadLayout = PayloadRowLayout::create(
      ROW({"payload_string", "payload_map", "payload_bigint"},
          {VARCHAR(), MAP(INTEGER(), VARCHAR()), BIGINT()}));
  RadixSortRunStorage arena(
      pool_.get(), layout, kRowsPerBlock, 64, payloadLayout, kRowsPerBlock, 64);

  std::vector<std::optional<std::string>> payloadStrings;
  std::vector<std::optional<int64_t>> payloadBigints;
  std::vector<vector_size_t> rawOffsets(kRows);
  std::vector<vector_size_t> rawSizes(kRows);
  std::vector<std::optional<int32_t>> mapKeys;
  std::vector<std::optional<std::string>> mapValues;
  payloadStrings.reserve(kRows);
  payloadBigints.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    payloadStrings.emplace_back(
        row % 3 == 1 ? std::optional<std::string>{}
                     : std::optional<std::string>(
                           std::string(48 + row * 3, 'a' + row % 26)));
    payloadBigints.emplace_back(row * 10);
    rawOffsets[row] = mapKeys.size();
    rawSizes[row] = row % 4 == 0 ? 2 : 0;
    for (vector_size_t entry = 0; entry < rawSizes[row]; ++entry) {
      mapKeys.emplace_back(row * 2 + entry);
      mapValues.emplace_back(std::string(40 + row + entry * 17, 'm' + entry));
    }
  }
  auto mapOffsets = AlignedBuffer::allocate<vector_size_t>(kRows, pool_.get());
  auto mapSizes = AlignedBuffer::allocate<vector_size_t>(kRows, pool_.get());
  std::memcpy(
      mapOffsets->asMutable<vector_size_t>(),
      rawOffsets.data(),
      rawOffsets.size() * sizeof(vector_size_t));
  std::memcpy(
      mapSizes->asMutable<vector_size_t>(),
      rawSizes.data(),
      rawSizes.size() * sizeof(vector_size_t));
  auto maps = std::make_shared<MapVector>(
      pool_.get(),
      MAP(INTEGER(), VARCHAR()),
      nullptr,
      kRows,
      std::move(mapOffsets),
      std::move(mapSizes),
      makeVector<int32_t>(INTEGER(), mapKeys),
      makeStringVector(VARCHAR(), mapValues));
  auto payload = makeRows(
      {makeStringVector(VARCHAR(), payloadStrings),
       maps,
       makeVector<int64_t>(BIGINT(), payloadBigints)});
  PayloadRowBatch payloadBatch;
  PayloadRowWriter payloadWriter;
  payloadWriter.append(*payload, arena, payloadBatch);

  std::vector<std::string> keyStorage(kRows);
  std::vector<std::string_view> keys(kRows);
  std::vector<char*> payloadPointers(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keyStorage[row] = std::string(
        row % 5 == 1 ? layout.inlineCapacity()
                     : layout.inlineCapacity() + 17 + row * 5,
        'k' + row % 11);
    keys[row] = keyStorage[row];
    payloadPointers[row] = payloadBatch.rowAt(row);
  }
  arena.appendBatch(keys, payloadPointers);

  ASSERT_EQ(arena.size(), kRows);
  ASSERT_EQ(arena.payloadSize(), kRows);
  ASSERT_EQ(arena.keyBlocks().size(), 2);
  ASSERT_EQ(arena.payloadFixedBlocks().size(), 2);
  EXPECT_EQ(arena.keyBlocks()[0].count, kRowsPerBlock);
  EXPECT_EQ(arena.keyBlocks()[1].count, 1);
  EXPECT_EQ(arena.payloadFixedBlocks()[0].count, kRowsPerBlock);
  EXPECT_EQ(arena.payloadFixedBlocks()[1].count, 1);
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_EQ(arena.keyAt(row).payload(), payloadBatch.rowAt(row));
  }

  const auto keyFixedBytes =
      blockBytes(arena.keyBlocks(), *layout.payloadOffset());
  const auto payloadFixedBytes =
      blockBytes(arena.payloadFixedBlocks(), payloadLayout->rowWidth());
  const auto keyHeapUsed = groupBytes(arena.keyHeapGroups());
  const auto keyHeapCapacity = groupBytes(arena.keyHeapGroups(), true);
  const auto expectedKeyHeapUsed = std::accumulate(
      keys.begin(), keys.end(), uint64_t{0}, [&](uint64_t sum, auto key) {
        return sum + layout.heapSize(key.size());
      });
  const auto payloadHeapUsed = groupBytes(arena.payloadHeapGroups());
  const auto payloadHeapCapacity = groupBytes(arena.payloadHeapGroups(), true);
  const auto expectedPayloadHeapUsed = std::accumulate(
      payloadBatch.heapSizes()->as<uint64_t>(),
      payloadBatch.heapSizes()->as<uint64_t>() + kRows,
      uint64_t{0});

  EXPECT_EQ(keyHeapUsed, expectedKeyHeapUsed);
  EXPECT_EQ(payloadHeapUsed, expectedPayloadHeapUsed);
  EXPECT_EQ(
      arena.estimatedOutputBytes(),
      keyFixedBytes + keyHeapUsed + payloadFixedBytes + payloadHeapUsed);
  EXPECT_GT(arena.keyHeapGroups().size(), 1);
  EXPECT_GT(arena.payloadHeapGroups().size(), 1);
  EXPECT_GE(keyHeapCapacity, keyHeapUsed);
  EXPECT_GE(payloadHeapCapacity, payloadHeapUsed);
  const auto reservedBytes = static_cast<uint64_t>(arena.keyBlocks().size()) *
          kRowsPerBlock * layout.width() +
      keyHeapCapacity +
      static_cast<uint64_t>(arena.payloadFixedBlocks().size()) * kRowsPerBlock *
          payloadLayout->rowWidth() +
      payloadHeapCapacity;
  EXPECT_GT(reservedBytes, arena.estimatedOutputBytes());
  EXPECT_GE(static_cast<uint64_t>(arena.allocatedBytes()), reservedBytes);
  EXPECT_GT(
      static_cast<uint64_t>(arena.allocatedBytes()),
      arena.estimatedOutputBytes());
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
