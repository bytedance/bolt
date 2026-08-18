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
#include <cstring>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/exec/tests/utils/RadixSortComparatorOracle.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

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
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW({child->type()}),
        nullptr,
        child->size(),
        std::vector<VectorPtr>{child});
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
      const auto leftKey = keys.fixedKeyAt(left);
      const auto rightKey = keys.fixedKeyAt(right);
      return (leftKey > rightKey) - (leftKey < rightKey);
    }
    return encodedCompare(keys.variableKeyAt(left), keys.variableKeyAt(right));
  }

  void decodeViews(
      const RadixSortKeyCodec& codec,
      std::span<const EncodedKeyView> views,
      RowVectorPtr& decoded) {
    BufferPtr cursorScratch;
    codec.decode(views, {}, {}, pool_.get(), cursorScratch, decoded);
  }

  static RadixSortKeyLayout layoutFromKind(RadixSortKeyLayoutKind kind) {
    return RadixSortKeyLayout::fromKind(kind);
  }

  static void verifyDeconstructed(
      const RadixSortKeyLayout& layout,
      const RadixSortKey& physical,
      std::string_view original) {
    RadixSortInlineKeyBuffer buffer{};
    EncodedKeyView deconstructed;
    physical.deconstruct(buffer, deconstructed);
    if (original.size() > layout.inlineCapacity()) {
      EXPECT_EQ(deconstructed.bytes, original);
      EXPECT_FALSE(deconstructed.zeroPadded);
      return;
    }
    ASSERT_EQ(deconstructed.bytes.size(), layout.inlineCapacity());
    EXPECT_TRUE(deconstructed.zeroPadded);
    EXPECT_EQ(deconstructed.bytes.substr(0, original.size()), original);
    for (uint64_t index = original.size(); index < deconstructed.bytes.size();
         ++index) {
      EXPECT_EQ(deconstructed.bytes[index], 0);
    }
  }

  static std::vector<EncodedKeyView> deconstructArena(
      const RadixSortRunStorage& arena,
      std::vector<RadixSortInlineKeyBuffer>& buffers) {
    buffers.resize(arena.size());
    std::vector<EncodedKeyView> views(arena.size());
    for (uint64_t row = 0; row < arena.size(); ++row) {
      arena.keyAt(row).deconstruct(buffers[row], views[row]);
    }
    return views;
  }
};

TEST_F(RadixSortKeyTest, layoutAbiAndSelection) {
  struct ExpectedLayout {
    RadixSortKeyLayoutKind kind;
    uint32_t width;
    uint32_t inlineCapacity;
    bool variable;
    bool hasPayload;
    std::optional<uint32_t> sizeOffset;
    std::optional<uint32_t> dataOffset;
    std::optional<uint32_t> payloadOffset;
  };
  const std::array<ExpectedLayout, 9> expectedLayouts{{
      {RadixSortKeyLayoutKind::kKeyOnlyFixed8, 8, 8, false, false, {}, {}, {}},
      {RadixSortKeyLayoutKind::kKeyOnlyFixed16,
       16,
       16,
       false,
       false,
       {},
       {},
       {}},
      {RadixSortKeyLayoutKind::kKeyOnlyFixed24,
       24,
       24,
       false,
       false,
       {},
       {},
       {}},
      {RadixSortKeyLayoutKind::kKeyOnlyFixed32,
       32,
       32,
       false,
       false,
       {},
       {},
       {}},
      {RadixSortKeyLayoutKind::kKeyOnlyVariable32,
       32,
       16,
       true,
       false,
       16,
       24,
       {}},
      {RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
       16,
       8,
       false,
       true,
       {},
       {},
       8},
      {RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
       24,
       16,
       false,
       true,
       {},
       {},
       16},
      {RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
       32,
       24,
       false,
       true,
       {},
       {},
       24},
      {RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
       32,
       8,
       true,
       true,
       8,
       16,
       24},
  }};
  for (const auto& expected : expectedLayouts) {
    auto layout = layoutFromKind(expected.kind);
    EXPECT_EQ(layout.width(), expected.width);
    EXPECT_EQ(layout.inlineCapacity(), expected.inlineCapacity);
    EXPECT_EQ(layout.isVariable(), expected.variable);
    EXPECT_EQ(layout.hasPayload(), expected.hasPayload);
    EXPECT_EQ(layout.sizeOffset(), expected.sizeOffset);
    EXPECT_EQ(layout.dataOffset(), expected.dataOffset);
    EXPECT_EQ(layout.payloadOffset(), expected.payloadOffset);
  }

  EXPECT_EQ(sizeof(KeyOnlyFixed8Record), 8);
  EXPECT_EQ(sizeof(KeyOnlyFixed16Record), 16);
  EXPECT_EQ(sizeof(KeyOnlyFixed24Record), 24);
  EXPECT_EQ(sizeof(KeyOnlyFixed32Record), 32);
  EXPECT_EQ(sizeof(KeyOnlyVariable32Record), 32);
  EXPECT_EQ(sizeof(KeyWithPayloadFixed16Record), 16);
  EXPECT_EQ(sizeof(KeyWithPayloadFixed24Record), 24);
  EXPECT_EQ(sizeof(KeyWithPayloadFixed32Record), 32);
  EXPECT_EQ(sizeof(KeyWithPayloadVariable32Record), 32);
  EXPECT_EQ(sizeof(KeyWithPayloadVariable56Record), 56);
  EXPECT_EQ(sizeof(KeyWithPayloadVariable64Record), 64);

  struct Selection {
    std::optional<uint64_t> maximumSize;
    bool hasPayload;
    RadixSortKeyLayoutKind kind;
  };
  const std::array<Selection, 18> selections{{
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
      {std::nullopt, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
  }};
  for (const auto& selection : selections) {
    auto layout =
        RadixSortKeyLayout::select(selection.maximumSize, selection.hasPayload);
    EXPECT_EQ(layout.kind(), selection.kind);
  }
  EXPECT_EQ(
      RadixSortKeyLayout::select(std::nullopt, true, 3).kind(),
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable56);
  EXPECT_EQ(
      RadixSortKeyLayout::select(std::nullopt, true, 4).kind(),
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable64);

  EXPECT_THROW(
      RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind::kInvalid),
      BoltException);
  EXPECT_THROW(RadixSortKeyLayout::select(0, false), BoltException);
  EXPECT_THROW(RadixSortKeyLayout::select(0, true), BoltException);
  EXPECT_THROW(
      RadixSortKeyLayout::select(std::numeric_limits<uint64_t>::max(), true),
      BoltException);
}

TEST_F(RadixSortKeyTest, allLayoutsRoundTripAndCompare) {
  const std::array<RadixSortKeyLayoutKind, 11> kinds{
      RadixSortKeyLayoutKind::kKeyOnlyFixed8,
      RadixSortKeyLayoutKind::kKeyOnlyFixed16,
      RadixSortKeyLayoutKind::kKeyOnlyFixed24,
      RadixSortKeyLayoutKind::kKeyOnlyFixed32,
      RadixSortKeyLayoutKind::kKeyOnlyVariable32,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable56,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable64};
  std::array<uint64_t, 4> payloadStorage{};
  for (const auto kind : kinds) {
    auto layout = layoutFromKind(kind);
    std::vector<std::string> keys;
    keys.emplace_back(layout.inlineCapacity() - 1, 'a');
    keys.emplace_back(layout.inlineCapacity(), 'b');
    if (layout.isVariable()) {
      keys.emplace_back(layout.inlineCapacity() + 1, 'c');
      std::string commonPrefix(layout.inlineCapacity() + 128, 'p');
      keys.push_back(commonPrefix + 'a');
      keys.push_back(commonPrefix + 'b');
    }

    RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
    for (uint64_t index = 0; index < keys.size(); ++index) {
      auto* payload = layout.hasPayload()
          ? reinterpret_cast<char*>(&payloadStorage[index])
          : nullptr;
      arena.append(keys[index], payload);
      auto physical = arena.keyAt(index);
      verifyDeconstructed(layout, physical, keys[index]);
      EXPECT_EQ(
          physical.heapSize(),
          keys[index].size() > layout.inlineCapacity() ? keys[index].size()
                                                       : 0);
      EXPECT_EQ(
          physical.fullKeyData() != nullptr,
          keys[index].size() > layout.inlineCapacity());
      EXPECT_EQ(physical.payload(), payload);
      if (layout.hasPayload()) {
        uint64_t replacementPayload = index;
        auto* replacement = reinterpret_cast<char*>(&replacementPayload);
        physical.setPayload(replacement);
        EXPECT_EQ(physical.payload(), replacement);
        physical.setPayload(payload);
      }
    }
    for (uint64_t left = 0; left < keys.size(); ++left) {
      for (uint64_t right = 0; right < keys.size(); ++right) {
        const auto expected = encodedCompare(keys[left], keys[right]);
        const auto actual = arena.keyAt(left).compare(arena.keyAt(right));
        EXPECT_EQ((actual > 0) - (actual < 0), expected);
      }
    }
    if (!layout.isVariable()) {
      EXPECT_THROW(
          arena.append(std::string(layout.inlineCapacity() + 1, 'x')),
          BoltException);
      EXPECT_EQ(arena.size(), keys.size());
    }
  }
}

TEST_F(RadixSortKeyTest, knownPerWordByteSwap) {
  auto layout = layoutFromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed16);
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
  verifyDeconstructed(
      layout, key, std::string_view(encoded.data(), encoded.size()));
}

TEST_F(RadixSortKeyTest, templatedCompareMatchesGenericCompare) {
  const auto verify = [&]<RadixSortKeyLayoutKind KIND>() {
    const auto layout = layoutFromKind(KIND);
    std::vector<std::string> storage{
        std::string(layout.inlineCapacity() - 1, 'a'),
        std::string(layout.inlineCapacity(), 'b')};
    if (layout.isVariable()) {
      storage.push_back(std::string(layout.inlineCapacity() + 1, 'c'));
      storage.push_back(std::string(layout.inlineCapacity() + 64, 'p') + "a");
      storage.push_back(std::string(layout.inlineCapacity() + 64, 'p') + "b");
    }
    std::vector<char*> payloads(storage.size(), nullptr);
    std::vector<uint64_t> payloadStorage(storage.size());
    if (layout.hasPayload()) {
      for (uint64_t row = 0; row < storage.size(); ++row) {
        payloads[row] = reinterpret_cast<char*>(&payloadStorage[row]);
      }
    }
    RadixSortRunStorage arena(pool_.get(), layout, 3, 64);
    std::vector<std::string_view> keys;
    keys.reserve(storage.size());
    for (const auto& key : storage) {
      keys.push_back(key);
    }
    arena.appendBatch(
        keys,
        layout.hasPayload() ? std::span<char* const>(payloads)
                            : std::span<char* const>{});
    for (uint64_t left = 0; left < arena.size(); ++left) {
      for (uint64_t right = 0; right < arena.size(); ++right) {
        const auto generic = arena.keyAt(left).compare(arena.keyAt(right));
        const auto templated = RadixSortKeyOps<KIND>::compare(
            arena.keyDataAt(left), arena.keyDataAt(right));
        EXPECT_EQ(
            (generic > 0) - (generic < 0), (templated > 0) - (templated < 0));
      }
    }
  };

  verify.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyOnlyVariable32>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
  verify.template operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
  verify
      .template operator()<RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>();
}

TEST_F(RadixSortKeyTest, codecPhysicalCompareProperty) {
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
            0,
            std::numeric_limits<int8_t>::max(),
            std::nullopt})},
      {SMALLINT(),
       makeVector<int16_t>(
           SMALLINT(),
           {std::numeric_limits<int16_t>::min(),
            0,
            std::numeric_limits<int16_t>::max(),
            std::nullopt})},
      {INTEGER(),
       makeVector<int32_t>(
           INTEGER(),
           {std::numeric_limits<int32_t>::min(),
            0,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {BIGINT(),
       makeVector<int64_t>(
           BIGINT(),
           {std::numeric_limits<int64_t>::min(),
            0,
            std::numeric_limits<int64_t>::max(),
            std::nullopt})},
      {DATE(),
       makeVector<int32_t>(
           DATE(),
           {std::numeric_limits<int32_t>::min(),
            0,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {INTERVAL_YEAR_MONTH(),
       makeVector<int32_t>(
           INTERVAL_YEAR_MONTH(),
           {std::numeric_limits<int32_t>::min(),
            0,
            std::numeric_limits<int32_t>::max(),
            std::nullopt})},
      {INTERVAL_DAY_TIME(),
       makeVector<int64_t>(
           INTERVAL_DAY_TIME(),
           {std::numeric_limits<int64_t>::min(),
            0,
            std::numeric_limits<int64_t>::max(),
            std::nullopt})},
      {HUGEINT(),
       makeVector<int128_t>(
           HUGEINT(),
           {HugeInt::build(uint64_t{1} << 63, 0),
            static_cast<int128_t>(0),
            HugeInt::build(
                (uint64_t{1} << 63) - 1, std::numeric_limits<uint64_t>::max()),
            std::nullopt})},
      {REAL(),
       makeVector<float>(
           REAL(),
           {-std::numeric_limits<float>::infinity(),
            -0.0f,
            0.0f,
            std::numeric_limits<float>::infinity(),
            std::numeric_limits<float>::quiet_NaN(),
            std::nullopt})},
      {DOUBLE(),
       makeVector<double>(
           DOUBLE(),
           {-std::numeric_limits<double>::infinity(),
            -0.0,
            0.0,
            std::numeric_limits<double>::infinity(),
            std::numeric_limits<double>::quiet_NaN(),
            std::nullopt})},
      {DECIMAL(18, 4),
       makeVector<int64_t>(
           DECIMAL(18, 4),
           {-999999999999999999LL, 0, 999999999999999999LL, std::nullopt})},
      {DECIMAL(38, 18),
       makeVector<int128_t>(
           DECIMAL(38, 18),
           {-decimal38Max,
            static_cast<int128_t>(0),
            decimal38Max,
            std::nullopt})},
      {TIMESTAMP(),
       makeVector<Timestamp>(
           TIMESTAMP(),
           {Timestamp::min(),
            Timestamp(0, 0),
            Timestamp(0, 1),
            Timestamp::max(),
            std::nullopt})},
      {UNKNOWN(), makeUnknownVector(4)},
      {VARCHAR(),
       makeStringVector(
           VARCHAR(),
           {std::string(),
            std::string("\xc3\x28\xff", 3),
            std::string(128, 'x'),
            std::nullopt})},
      {VARBINARY(),
       makeStringVector(
           VARBINARY(),
           {std::string(),
            std::string("\x00\x01\xff", 3),
            std::string(128, 'y'),
            std::nullopt})}};

  for (const auto& testCase : cases) {
    for (const auto compareFlags : allFlags()) {
      SCOPED_TRACE(
          testCase.type->toString() +
          (compareFlags.ascending ? " ASC" : " DESC") +
          (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));
      std::unique_ptr<RadixSortKeyCodec> codec;
      RadixSortKeyCodec::bind({testCase.type}, {compareFlags}, codec);
      auto rows = makeRows(testCase.vector);
      EncodedKeyBatch encodedKeys;
      codec->encode(*rows, pool_.get(), encodedKeys);

      for (const bool hasPayload : {false, true}) {
        auto layout =
            RadixSortKeyLayout::select(codec->maximumEncodedSize(), hasPayload);
        std::vector<uint64_t> payloadStorage(rows->size());
        std::vector<char*> payloads(rows->size());
        for (vector_size_t row = 0; row < rows->size(); ++row) {
          payloads[row] = reinterpret_cast<char*>(&payloadStorage[row]);
        }
        RadixSortRunStorage arena(pool_.get(), layout, 3, 64);
        arena.appendBatch(
            encodedKeys,
            hasPayload ? std::span<char* const>(payloads)
                       : std::span<char* const>{});
        ASSERT_EQ(arena.size(), rows->size());

        for (vector_size_t row = 0; row < rows->size(); ++row) {
          const auto original = encodedKeyAt(encodedKeys, row);
          verifyDeconstructed(layout, arena.keyAt(row), original);
          EXPECT_EQ(
              arena.keyAt(row).payload(), hasPayload ? payloads[row] : nullptr);
        }
        for (vector_size_t left = 0; left < rows->size(); ++left) {
          for (vector_size_t right = 0; right < rows->size(); ++right) {
            const auto expected = compareEncodedKeys(encodedKeys, left, right);
            const auto actual = arena.keyAt(left).compare(arena.keyAt(right));
            EXPECT_EQ(
                (actual > 0) - (actual < 0), (expected > 0) - (expected < 0));
          }
        }

        std::vector<RadixSortInlineKeyBuffer> buffers;
        auto views = deconstructArena(arena, buffers);
        RowVectorPtr decoded;
        decodeViews(
            *codec,
            std::span<const EncodedKeyView>(views.data(), views.size()),
            decoded);
        for (vector_size_t row = 0; row < rows->size(); ++row) {
          EXPECT_EQ(
              SortComparatorOracle::compare(
                  *testCase.vector,
                  row,
                  *decoded->childAt(0),
                  row,
                  compareFlags),
              0);
        }
      }
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
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind(
      {INTEGER(), VARCHAR(), DOUBLE()}, compareFlags, codec);
  EncodedKeyBatch encodedKeys;
  codec->encode(*rows, pool_.get(), encodedKeys);

  for (const bool hasPayload : {false, true}) {
    auto layout =
        RadixSortKeyLayout::select(codec->maximumEncodedSize(), hasPayload);
    RadixSortRunStorage arena(pool_.get(), layout, 2, 64);
    std::vector<uint64_t> payloadStorage(rows->size());
    std::vector<char*> payloads(rows->size());
    for (vector_size_t row = 0; row < rows->size(); ++row) {
      payloads[row] = reinterpret_cast<char*>(&payloadStorage[row]);
    }
    arena.appendBatch(
        encodedKeys,
        hasPayload ? std::span<char* const>(payloads)
                   : std::span<char* const>{});
    for (vector_size_t left = 0; left < rows->size(); ++left) {
      for (vector_size_t right = 0; right < rows->size(); ++right) {
        const auto expected = compareEncodedKeys(encodedKeys, left, right);
        const auto actual = arena.keyAt(left).compare(arena.keyAt(right));
        EXPECT_EQ((actual > 0) - (actual < 0), (expected > 0) - (expected < 0));
      }
    }

    std::vector<RadixSortInlineKeyBuffer> buffers;
    auto views = deconstructArena(arena, buffers);
    RowVectorPtr decoded;
    decodeViews(
        *codec,
        std::span<const EncodedKeyView>(views.data(), views.size()),
        decoded);
    const std::vector<column_index_t> channels{0, 1, 2};
    for (vector_size_t row = 0; row < rows->size(); ++row) {
      EXPECT_EQ(
          SortComparatorOracle::compareRows(
              *rows, row, *decoded, row, channels, compareFlags),
          0);
    }
  }
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
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind({arrays->type(), BIGINT()}, compareFlags, codec);
  EncodedKeyBatch encodedKeys;
  codec->encode(*rows, pool_.get(), encodedKeys);

  auto layout = RadixSortKeyLayout::select(codec->maximumEncodedSize(), false);
  ASSERT_TRUE(layout.isVariable());
  RadixSortRunStorage arena(pool_.get(), layout, 3, 64);
  arena.appendBatch(encodedKeys);
  const std::vector<column_index_t> channels{0, 1};
  for (vector_size_t left = 0; left < rows->size(); ++left) {
    for (vector_size_t right = 0; right < rows->size(); ++right) {
      const auto expected = SortComparatorOracle::compareRows(
          *rows, left, *rows, right, channels, compareFlags);
      const auto actual = arena.keyAt(left).compare(arena.keyAt(right));
      EXPECT_EQ((actual > 0) - (actual < 0), (expected > 0) - (expected < 0));
    }
  }

  std::vector<RadixSortInlineKeyBuffer> buffers;
  auto views = deconstructArena(arena, buffers);
  RowVectorPtr decoded;
  decodeViews(
      *codec,
      std::span<const EncodedKeyView>(views.data(), views.size()),
      decoded);
  for (vector_size_t row = 0; row < rows->size(); ++row) {
    EXPECT_EQ(
        SortComparatorOracle::compareRows(
            *rows, row, *decoded, row, channels, compareFlags),
        0);
  }
}

TEST_F(RadixSortKeyTest, blockAndHeapGroups) {
  auto layout = layoutFromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
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
  ASSERT_EQ(arena.keyHeapGroups().size(), 3);
  EXPECT_EQ(arena.keyHeapGroups()[0].capacity, 64);
  EXPECT_EQ(arena.keyHeapGroups()[0].used, 64);
  EXPECT_EQ(arena.keyHeapGroups()[0].keyCount, 3);
  EXPECT_EQ(arena.keyHeapGroups()[1].capacity, 100);
  EXPECT_EQ(arena.keyHeapGroups()[1].used, 100);
  EXPECT_EQ(arena.keyHeapGroups()[1].keyCount, 1);
  EXPECT_EQ(arena.keyHeapGroups()[2].capacity, 64);
  EXPECT_EQ(arena.keyHeapGroups()[2].used, 17);
  EXPECT_EQ(arena.keyHeapGroups()[2].keyCount, 1);

  EXPECT_EQ(arena.keyAt(0).fullKeyData(), nullptr);
  EXPECT_EQ(arena.keyAt(1).fullKeyData(), arena.keyHeapGroups()[0].base);
  EXPECT_EQ(arena.keyAt(2).fullKeyData(), arena.keyHeapGroups()[0].base + 17);
  EXPECT_EQ(arena.keyAt(3).fullKeyData(), arena.keyHeapGroups()[0].base + 37);
  EXPECT_EQ(arena.keyAt(4).fullKeyData(), arena.keyHeapGroups()[1].base);
  EXPECT_EQ(arena.keyAt(5).fullKeyData(), arena.keyHeapGroups()[2].base);
  uint64_t heapBytes = 0;
  for (const auto& group : arena.keyHeapGroups()) {
    heapBytes += group.used;
  }
  const auto keyBytes = keys.size() * layout.width();
  EXPECT_EQ(heapBytes, 181);
  EXPECT_EQ(keyBytes, keys.size() * layout.width());
  EXPECT_GE(
      static_cast<uint64_t>(arena.allocatedBytes()), keyBytes + heapBytes);
}

TEST_F(RadixSortKeyTest, fixedSeedPhysicalCompareProperty) {
  constexpr uint32_t kSeeds = 32;
  constexpr uint32_t kPairsPerSeed = 2'000;
  const std::array<RadixSortKeyLayoutKind, 9> kinds{
      RadixSortKeyLayoutKind::kKeyOnlyFixed8,
      RadixSortKeyLayoutKind::kKeyOnlyFixed16,
      RadixSortKeyLayoutKind::kKeyOnlyFixed24,
      RadixSortKeyLayoutKind::kKeyOnlyFixed32,
      RadixSortKeyLayoutKind::kKeyOnlyVariable32,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed32,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32};

  for (uint32_t seed = 0; seed < kSeeds; ++seed) {
    std::mt19937_64 random(seed);
    const auto kind = kinds[seed % kinds.size()];
    const auto layout = layoutFromKind(kind);
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

    std::vector<uint64_t> payloadStorage(keys.size());
    std::vector<char*> payloads(keys.size());
    for (uint64_t index = 0; index < keys.size(); ++index) {
      payloads[index] = reinterpret_cast<char*>(&payloadStorage[index]);
    }
    RadixSortRunStorage arena(pool_.get(), layout, 127, 4096);
    arena.appendBatch(
        keys,
        layout.hasPayload() ? std::span<char* const>(payloads)
                            : std::span<char* const>{});

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
  auto layout = layoutFromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed32);
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
  auto layout = layoutFromKind(RadixSortKeyLayoutKind::kKeyOnlyVariable32);
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
    const auto physical = arena.keyAt(index);
    ASSERT_NE(physical.fullKeyData(), nullptr);
    EXPECT_EQ(std::string_view(physical.fullKeyData(), key.size()), key);
  }

  arena.clear();
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.numRanges(), 0);
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(RadixSortKeyTest, invalidArenaInputs) {
  auto fixedLayout = layoutFromKind(RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  RadixSortRunStorage fixedArena(pool_.get(), fixedLayout, 4, 64);
  EXPECT_THROW(fixedArena.append({}), BoltException);
  EXPECT_THROW(fixedArena.append(std::string(9, 'x')), BoltException);
  uint64_t payload = 0;
  EXPECT_THROW(
      fixedArena.append("x", reinterpret_cast<char*>(&payload)), BoltException);
  EXPECT_EQ(fixedArena.size(), 0);
  EXPECT_TRUE(fixedArena.keyBlocks().empty());

  auto variableLayout =
      layoutFromKind(RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
  RadixSortRunStorage variableArena(pool_.get(), variableLayout, 4, 64);
  const std::array<std::string_view, 2> keys{"a", "b"};
  std::array<char*, 1> payloads{nullptr};
  EXPECT_THROW(variableArena.appendBatch(keys, payloads), BoltException);
  EXPECT_EQ(variableArena.size(), 0);

  std::array<char, 8> storage{};
  RadixSortKey noPayload(fixedLayout, storage.data());
  EXPECT_THROW(noPayload.construct({}, nullptr), BoltException);
  EXPECT_THROW(noPayload.setPayload(nullptr), BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
