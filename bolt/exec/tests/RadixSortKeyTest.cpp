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
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/radixsort/RadixSortRun.h"
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
  int32_t (*compare)(const char*, const char*, uint32_t);
};

static_assert(sizeof(KeyOnlyVariable32Record) == 32);
static_assert(alignof(KeyOnlyVariable32Record) == 8);
static_assert(offsetof(KeyOnlyVariable32Record, size) == 18);
static_assert(offsetof(KeyOnlyVariable32Record, data) == 26);
static_assert(std::is_trivially_copyable_v<KeyOnlyVariable32Record>);
static_assert(sizeof(KeyWithPayloadVariable32Record) == 32);
static_assert(alignof(KeyWithPayloadVariable32Record) == 8);
static_assert(offsetof(KeyWithPayloadVariable32Record, size) == 12);
static_assert(offsetof(KeyWithPayloadVariable32Record, data) == 20);
static_assert(offsetof(KeyWithPayloadVariable32Record, payload) == 26);
static_assert(std::is_trivially_copyable_v<KeyWithPayloadVariable32Record>);
static_assert(sizeof(KeyWithPayloadFixed16Record) == 16);
static_assert(alignof(KeyWithPayloadFixed16Record) == 8);
static_assert(offsetof(KeyWithPayloadFixed16Record, payload) == 10);
static_assert(std::is_trivially_copyable_v<KeyWithPayloadFixed16Record>);
static_assert(sizeof(KeyWithPayloadFixed24Record) == 24);
static_assert(alignof(KeyWithPayloadFixed24Record) == 8);
static_assert(offsetof(KeyWithPayloadFixed24Record, payload) == 18);
static_assert(std::is_trivially_copyable_v<KeyWithPayloadFixed24Record>);
static_assert(sizeof(KeyWithPayloadFixed32Record) == 32);
static_assert(alignof(KeyWithPayloadFixed32Record) == 8);
static_assert(offsetof(KeyWithPayloadFixed32Record, payload) == 26);
static_assert(std::is_trivially_copyable_v<KeyWithPayloadFixed32Record>);

// clang-format off
#define LAYOUT(KIND, RECORD, CAPACITY, VARIABLE, PAYLOAD, SIZE, DATA, PAYLOAD_OFFSET) \
  {LayoutKind::KIND, sizeof(RECORD), CAPACITY, VARIABLE, PAYLOAD, SIZE, DATA, PAYLOAD_OFFSET, \
   &RadixSortKeyOps<LayoutKind::KIND>::compare}
constexpr std::array<LayoutDescriptor, 9> kLayouts{{
    LAYOUT(kKeyOnlyFixed8, KeyOnlyFixed8Record, 8, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed16, KeyOnlyFixed16Record, 16, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed24, KeyOnlyFixed24Record, 24, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyFixed32, KeyOnlyFixed32Record, 32, false, false, {}, {}, {}),
    LAYOUT(kKeyOnlyVariable32, KeyOnlyVariable32Record, 18, true, false, 18, 26, {}),
    LAYOUT(kKeyWithPayloadFixed16, KeyWithPayloadFixed16Record, 10, false, true, {}, {}, 10),
    LAYOUT(kKeyWithPayloadFixed24, KeyWithPayloadFixed24Record, 18, false, true, {}, {}, 18),
    LAYOUT(kKeyWithPayloadFixed32, KeyWithPayloadFixed32Record, 26, false, true, {}, {}, 26),
    LAYOUT(kKeyWithPayloadVariable32, KeyWithPayloadVariable32Record, 12, true, true, 12, 20, 26),
}};
#undef LAYOUT
// clang-format on

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

  std::unique_ptr<RadixSortRun> makeRun(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<CompareFlags>& compareFlags) {
    std::vector<std::string> keyNames;
    std::vector<TypePtr> keyTypes;
    keyNames.reserve(keyChannels.size());
    keyTypes.reserve(keyChannels.size());
    for (const auto channel : keyChannels) {
      keyNames.push_back(input->type()->asRow().nameOf(channel));
      keyTypes.push_back(input->type()->asRow().childAt(channel));
    }
    auto run = RadixSortRun::create(
        pool_.get(),
        std::static_pointer_cast<const RowType>(input->type()),
        ROW(std::move(keyNames), std::move(keyTypes)),
        compareFlags,
        keyChannels,
        {});
    run->append(*input);
    return run;
  }

  std::unique_ptr<RadixSortRun> makeRun(
      const RowVectorPtr& rows,
      const std::vector<CompareFlags>& compareFlags,
      bool hasPayload) {
    std::vector<column_index_t> keyChannels(rows->childrenSize());
    std::iota(keyChannels.begin(), keyChannels.end(), 0);
    std::vector<std::string> outputNames(rows->childrenSize());
    for (uint32_t column = 0; column < outputNames.size(); ++column) {
      outputNames[column] = "key" + std::to_string(column);
    }
    std::vector<std::string> keyNames = outputNames;
    std::vector<TypePtr> keyTypes = rows->type()->asRow().children();
    std::vector<TypePtr> outputTypes = rows->type()->asRow().children();
    std::vector<VectorPtr> inputChildren = rows->children();
    if (hasPayload) {
      outputNames.push_back("payload");
      outputTypes.push_back(BIGINT());
      inputChildren.push_back(makeVector<int64_t>(
          BIGINT(),
          std::vector<std::optional<int64_t>>(rows->size(), int64_t{0})));
    }
    auto input = std::make_shared<RowVector>(
        pool_.get(),
        ROW(std::move(outputNames), std::move(outputTypes)),
        nullptr,
        rows->size(),
        std::move(inputChildren));
    return makeRun(input, keyChannels, compareFlags);
  }

  static const char* recordAt(
      const RadixSortRunStorage& storage,
      uint64_t row) {
    const auto range = storage.keyRangeAt(row, 1);
    BOLT_CHECK_EQ(range.count, 1);
    return range.data;
  }

  static void constructPhysicalKey(
      const RadixSortKeyLayout& layout,
      std::string_view encodedKey,
      char* heap,
      char* payload,
      char* record) {
    if (layout.isVariable()) {
      std::memset(record, 0, layout.inlineCapacity());
      std::memcpy(
          record,
          encodedKey.data(),
          std::min<size_t>(encodedKey.size(), layout.inlineCapacity()));
      storeUnaligned<uint64_t>(
          record + *layout.sizeOffset(), encodedKey.size());
      const auto heapSize = encodedKey.size() - layout.heapKeyOffset();
      if (heapSize > 0) {
        std::memcpy(heap, encodedKey.data() + layout.heapKeyOffset(), heapSize);
      }
      storeCompactPointer(record + *layout.dataOffset(), heap);
    } else {
      RadixSortInlineKeyBuffer bytes{};
      std::memcpy(
          bytes.data(),
          encodedKey.data(),
          std::min<size_t>(encodedKey.size(), layout.inlineCapacity()));
      for (uint32_t word = 0; word < layout.inlineWordCount(); ++word) {
        auto value =
            loadUnaligned<uint64_t>(bytes.data() + word * sizeof(uint64_t));
        if constexpr (std::endian::native == std::endian::little) {
          value = byteSwap(value);
        }
        storeUnaligned<uint64_t>(record + word * sizeof(uint64_t), value);
      }
      std::memcpy(
          record + layout.inlineWordBytes(),
          bytes.data() + layout.inlineWordBytes(),
          layout.inlineTailBytes());
    }
    if (layout.hasPayload()) {
      storeCompactPointer(record + *layout.payloadOffset(), payload);
    }
  }

  static uint64_t storedSize(
      const RadixSortKeyLayout& layout,
      const char* record) {
    return loadUnaligned<uint64_t>(record + *layout.sizeOffset());
  }

  static std::string_view heapKey(
      const RadixSortKeyLayout& layout,
      const char* record) {
    return {
        loadCompactPointer(record + *layout.dataOffset()),
        storedSize(layout, record) - layout.heapKeyOffset()};
  }

  static char* payloadAt(const RadixSortKeyLayout& layout, const char* record) {
    return layout.hasPayload()
        ? loadCompactPointer(record + *layout.payloadOffset())
        : nullptr;
  }

  static RowVectorPtr
  output(RadixSortRun& run, vector_size_t maxRows, memory::MemoryPool* pool) {
    RowVectorPtr result;
    return run.getOutput(maxRows, pool, result);
  }

  static int32_t comparePhysical(
      const RadixSortKeyLayout& layout,
      const char* left,
      const char* right) {
#define COMPARE_KIND(kind)                             \
  case LayoutKind::kind:                               \
    return RadixSortKeyOps<LayoutKind::kind>::compare( \
        left, right, layout.heapKeyOffset())
    switch (layout.kind()) {
      COMPARE_KIND(kKeyOnlyFixed8);
      COMPARE_KIND(kKeyOnlyFixed16);
      COMPARE_KIND(kKeyOnlyFixed24);
      COMPARE_KIND(kKeyOnlyFixed32);
      COMPARE_KIND(kKeyOnlyVariable32);
      COMPARE_KIND(kKeyWithPayloadFixed16);
      COMPARE_KIND(kKeyWithPayloadFixed24);
      COMPARE_KIND(kKeyWithPayloadFixed32);
      COMPARE_KIND(kKeyWithPayloadVariable32);
      default:
        BOLT_UNREACHABLE();
    }
#undef COMPARE_KIND
  }

  static std::vector<std::unique_ptr<std::string>> appendPhysicalKeys(
      RadixSortRunStorage& storage,
      std::span<const std::string_view> keys,
      std::span<char* const> payloads = {}) {
    BOLT_CHECK(payloads.empty() || payloads.size() == keys.size());
    std::vector<std::unique_ptr<std::string>> heaps(keys.size());
    storage.appendKeyBlocks(
        keys.size(),
        [&](vector_size_t source, vector_size_t count, char* records) {
          for (vector_size_t row = 0; row < count; ++row) {
            const auto inputRow = source + row;
            auto* heap = static_cast<char*>(nullptr);
            const auto heapSize = storage.layout().isVariable()
                ? keys[inputRow].size() - storage.layout().heapKeyOffset()
                : 0;
            if (heapSize > 0) {
              heaps[inputRow] = std::make_unique<std::string>(
                  keys[inputRow].substr(storage.layout().heapKeyOffset()));
              heap = heaps[inputRow]->data();
            }
            constructPhysicalKey(
                storage.layout(),
                keys[inputRow],
                heap,
                payloads.empty() ? nullptr : payloads[inputRow],
                records +
                    static_cast<uint64_t>(row) * storage.layout().width());
          }
        });
    return heaps;
  }

  static std::vector<std::unique_ptr<std::string>> appendPhysicalKeys(
      RadixSortRunStorage& storage,
      const std::vector<std::string>& keys,
      std::span<char* const> payloads = {}) {
    const std::vector<std::string_view> views(keys.begin(), keys.end());
    return appendPhysicalKeys(storage, views, payloads);
  }

  template <typename Expected>
  static void expectPairwiseCompare(
      const RadixSortRunStorage& arena,
      vector_size_t size,
      Expected expected) {
    for (vector_size_t left = 0; left < size; ++left) {
      for (vector_size_t right = 0; right < size; ++right) {
        const auto actual = comparePhysical(
            arena.layout(), recordAt(arena, left), recordAt(arena, right));
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
      return SortComparatorOracle::compareUnsignedBytes(
          keys[left], keys[right]);
    });
  }

  static void verifyPhysicalStorage(
      const RadixSortKeyLayout& layout,
      const char* record,
      std::string_view original) {
    if (layout.isVariable()) {
      EXPECT_EQ(
          std::string_view(record, layout.heapKeyOffset()),
          original.substr(0, layout.heapKeyOffset()));
      EXPECT_EQ(
          heapKey(layout, record), original.substr(layout.heapKeyOffset()));
      return;
    }
    RadixSortInlineKeyBuffer bytes{};
    for (uint32_t word = 0; word < layout.inlineWordCount(); ++word) {
      auto value = loadUnaligned<uint64_t>(
          record + static_cast<uint64_t>(word) * sizeof(uint64_t));
      if constexpr (std::endian::native == std::endian::little) {
        value = byteSwap(value);
      }
      storeUnaligned<uint64_t>(
          bytes.data() + static_cast<uint64_t>(word) * sizeof(uint64_t), value);
    }
    std::memcpy(
        bytes.data() + layout.inlineWordBytes(),
        record + layout.inlineWordBytes(),
        layout.inlineTailBytes());
    const std::string_view encoded(bytes.data(), layout.inlineCapacity());
    EXPECT_EQ(encoded.substr(0, original.size()), original);
    EXPECT_EQ(
        encoded.substr(original.size()),
        std::string(layout.inlineCapacity() - original.size(), '\0'));
  }

  static void verifyStoredKey(
      const RadixSortRunStorage& arena,
      uint64_t row,
      std::string_view original,
      char* payload = nullptr) {
    const auto* record = recordAt(arena, row);
    const auto& layout = arena.layout();
    verifyPhysicalStorage(layout, record, original);
    EXPECT_EQ(payloadAt(layout, record), payload);
  }

  void verifyCodecStorage(
      const RowVectorPtr& rows,
      const std::vector<CompareFlags>& compareFlags,
      bool withPayloadCase,
      bool expectVariable) {
    std::vector<column_index_t> channels(rows->childrenSize());
    std::iota(channels.begin(), channels.end(), 0);
    const auto verify = [&](bool hasPayload) {
      auto run = makeRun(rows, compareFlags, hasPayload);
      const auto& arena = *run->storage();
      const auto& layout = arena.layout();
      if (expectVariable) {
        ASSERT_TRUE(layout.isVariable());
      }
      ASSERT_EQ(arena.size(), rows->size());
      expectPairwiseCompare(arena, rows->size(), [&](auto left, auto right) {
        return SortComparatorOracle::compareRows(
            *rows, left, *rows, right, channels, compareFlags);
      });
      std::vector<vector_size_t> expectedRows(rows->size());
      std::iota(expectedRows.begin(), expectedRows.end(), 0);
      std::stable_sort(
          expectedRows.begin(), expectedRows.end(), [&](auto left, auto right) {
            return SortComparatorOracle::compareRows(
                       *rows, left, *rows, right, channels, compareFlags) < 0;
          });
      run->finalize();
      auto decoded = output(*run, rows->size(), pool_.get());
      ASSERT_NE(decoded, nullptr);
      for (vector_size_t row = 0; row < rows->size(); ++row) {
        EXPECT_EQ(
            SortComparatorOracle::compareRows(
                *rows,
                expectedRows[row],
                *decoded,
                row,
                channels,
                compareFlags),
            0)
            << "row=" << row;
      }
    };
    verify(false);
    if (withPayloadCase) {
      verify(true);
    }
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
  const std::array<Selection, 20> selections{{
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
      {10, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16},
      {11, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {18, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {19, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {26, true, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {27, true, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32},
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
  EXPECT_THROW(
      RadixSortKeyLayout::select(std::numeric_limits<uint64_t>::max(), true),
      BoltException);
  EXPECT_THROW(RadixSortKeyLayout::select(8, false, 1), BoltException);
  EXPECT_THROW(
      RadixSortKeyLayout::select(std::nullopt, false, 19), BoltException);
  EXPECT_THROW(
      RadixSortKeyLayout::select(std::nullopt, true, 13), BoltException);
  EXPECT_EQ(
      RadixSortKeyLayout::select(std::nullopt, false, 18).heapKeyOffset(), 18);
  EXPECT_EQ(
      RadixSortKeyLayout::select(std::nullopt, true, 12).heapKeyOffset(), 12);
}

TEST_F(RadixSortKeyTest, variableHeapKeyOffsetUsesTopLevelFixedBoundaries) {
  const auto assertOffset = [&](const std::vector<TypePtr>& types,
                                bool hasPayload,
                                uint32_t expectedOffset) {
    std::unique_ptr<RadixSortKeyCodec> codec;
    RadixSortKeyCodec::bind(
        types,
        std::vector<CompareFlags>(
            types.size(), SortComparatorOracle::makeSortFlags(true, true)),
        codec);
    auto layout =
        RadixSortKeyLayout::select(codec->maximumEncodedSize(), hasPayload);
    ASSERT_TRUE(layout.isVariable());
    EXPECT_EQ(
        codec->heapKeyOffsetForVariableLayout(layout.inlineCapacity()),
        expectedOffset);
  };

  assertOffset({INTEGER(), VARCHAR()}, false, 5);
  assertOffset({BIGINT(), BIGINT(), VARCHAR()}, false, 18);
  assertOffset({VARCHAR(), INTEGER()}, false, 0);
  assertOffset({INTEGER(), VARCHAR()}, true, 5);
  assertOffset({BIGINT(), VARCHAR()}, true, 9);
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

    RadixSortRunStorage arena(pool_.get(), layout);
    const auto heaps =
        appendPhysicalKeys(arena, keys, payloads.span(layout.hasPayload()));
    for (uint64_t index = 0; index < keys.size(); ++index) {
      auto* payload = layout.hasPayload() ? payloads.pointers[index] : nullptr;
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
  RadixSortRunStorage arena(pool_.get(), layout);
  const auto heaps = appendPhysicalKeys(arena, keys);
  ASSERT_EQ(arena.size(), keys.size());
  const auto first = heapKey(layout, recordAt(arena, 0));
  const auto second = heapKey(layout, recordAt(arena, 1));
  EXPECT_EQ(first.size(), keys[0].size() - layout.heapKeyOffset());
  EXPECT_EQ(first, "f");
  ASSERT_NE(second.data(), nullptr);
  EXPECT_EQ(second.size(), keys[1].size() - layout.heapKeyOffset());
  EXPECT_EQ(second, std::string_view(keys[1]).substr(layout.heapKeyOffset()));
  EXPECT_EQ(
      comparePhysical(layout, recordAt(arena, 0), recordAt(arena, 1)),
      SortComparatorOracle::compareUnsignedBytes(keys[0], keys[1]));
  EXPECT_EQ(
      comparePhysical(layout, recordAt(arena, 1), recordAt(arena, 2)),
      SortComparatorOracle::compareUnsignedBytes(keys[1], keys[2]));
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

  RadixSortRunStorage arena(pool_.get(), layout);
  const std::vector<std::string_view> keys{left, right};
  const auto heaps = appendPhysicalKeys(arena, keys);

  ASSERT_LT(comparePhysical(layout, recordAt(arena, 0), recordAt(arena, 1)), 0);
  EXPECT_EQ(
      RadixSortKeyOps<LayoutKind::kKeyOnlyVariable32>::compareSuffix(
          recordAt(arena, 0),
          recordAt(arena, 1),
          layout.heapKeyOffset(),
          layout.radixWidth()),
      0);
}

TEST_F(RadixSortKeyTest, suffixCompareCoversLengthDataAndEquality) {
  using CompareSuffix =
      int32_t (*)(const char*, const char*, uint32_t, uint32_t);
  const std::array<std::tuple<LayoutKind, uint32_t, CompareSuffix>, 2> cases{{
      {LayoutKind::kKeyOnlyVariable32,
       5,
       &RadixSortKeyOps<LayoutKind::kKeyOnlyVariable32>::compareSuffix},
      {LayoutKind::kKeyWithPayloadVariable32,
       0,
       &RadixSortKeyOps<LayoutKind::kKeyWithPayloadVariable32>::compareSuffix},
  }};
  for (const auto& [kind, heapOffset, compareSuffix] : cases) {
    const auto layout = RadixSortKeyLayout::select(
        std::nullopt,
        kind == LayoutKind::kKeyWithPayloadVariable32,
        heapOffset);
    const auto radixWidth = layout.radixWidth();
    std::vector<std::string> keys{
        std::string(radixWidth - 1, 'a'),
        std::string(radixWidth + 1, 'a'),
        std::string(radixWidth + 3, 'p'),
        std::string(radixWidth + 3, 'p'),
        std::string(radixWidth + 2, 'q'),
        std::string(radixWidth + 3, 'q'),
        std::string(radixWidth + 3, 'z'),
        std::string(radixWidth + 3, 'z'),
    };
    keys[2][radixWidth + 1] = 'a';
    keys[3][radixWidth + 1] = 'z';
    PayloadPointers payloads(keys.size());
    RadixSortRunStorage arena(pool_.get(), layout);
    std::vector<std::string_view> views(keys.begin(), keys.end());
    const auto heaps =
        appendPhysicalKeys(arena, views, payloads.span(layout.hasPayload()));

    const auto compare = [&](uint64_t left, uint64_t right) {
      return compareSuffix(
          recordAt(arena, left),
          recordAt(arena, right),
          layout.heapKeyOffset(),
          radixWidth);
    };
    EXPECT_LT(compare(0, 1), 0);
    EXPECT_LT(compare(2, 3), 0);
    EXPECT_LT(compare(4, 5), 0);
    EXPECT_EQ(compare(6, 7), 0);
  }
}

TEST_F(RadixSortKeyTest, compactPointerAccessIsExactAndRangeChecked) {
  std::array<uint8_t, kCompactPointerBytes + 2> storage;
  storage.fill(0xa5);
  constexpr uint64_t value = 0xfedcba987654;
  storeCompactUInt48(storage.data() + 1, value);
  EXPECT_EQ(loadCompactUInt48(storage.data() + 1), value);
  EXPECT_EQ(storage.front(), 0xa5);
  EXPECT_EQ(storage.back(), 0xa5);

  storeCompactUInt48(storage.data() + 1, kCompactPointerMask);
  EXPECT_EQ(loadCompactUInt48(storage.data() + 1), kCompactPointerMask);
  EXPECT_EQ(storage.front(), 0xa5);
  EXPECT_EQ(storage.back(), 0xa5);

  auto* pointer = reinterpret_cast<char*>(static_cast<uintptr_t>(value));
  storeCompactPointer(storage.data() + 1, pointer);
  EXPECT_EQ(loadCompactPointer(storage.data() + 1), pointer);
  uint64_t stackValue = 0;
  storeCompactPointer(storage.data() + 1, &stackValue);
  EXPECT_EQ(
      loadCompactPointer(storage.data() + 1),
      reinterpret_cast<char*>(&stackValue));
  storeCompactPointer(storage.data() + 1, nullptr);
  EXPECT_EQ(loadCompactPointer(storage.data() + 1), nullptr);

  std::array<uint8_t, kCompactPointerBytes + 2> invalidStorage;
  invalidStorage.fill(0xa5);
#ifndef NDEBUG
  EXPECT_THROW(
      storeCompactUInt48(
          invalidStorage.data() + 1, kCompactPointerMask + uint64_t{1}),
      BoltException);
#endif
  EXPECT_EQ(invalidStorage.front(), 0xa5);
  EXPECT_EQ(invalidStorage.back(), 0xa5);

  EXPECT_NO_THROW(checkCompactPointerRange(nullptr, 0));
  EXPECT_NO_THROW(checkCompactPointerRange(
      reinterpret_cast<void*>(static_cast<uintptr_t>(kCompactPointerMask)), 1));
  EXPECT_THROW(
      checkCompactPointerRange(
          reinterpret_cast<void*>(static_cast<uintptr_t>(kCompactPointerMask)),
          2),
      BoltException);
  EXPECT_THROW(
      checkCompactPointerRange(
          reinterpret_cast<void*>(
              static_cast<uintptr_t>(kCompactPointerMask + 1)),
          1),
      BoltException);
}

TEST_F(RadixSortKeyTest, nonWordAlignedPrefixTailParticipatesInComparison) {
  const std::array<std::pair<LayoutKind, std::vector<uint32_t>>, 5> cases{{
      {LayoutKind::kKeyWithPayloadFixed16, {7, 8, 9}},
      {LayoutKind::kKeyWithPayloadFixed24, {15, 16, 17}},
      {LayoutKind::kKeyWithPayloadFixed32, {23, 24, 25}},
      {LayoutKind::kKeyOnlyVariable32, {7, 8, 15, 16, 17}},
      {LayoutKind::kKeyWithPayloadVariable32, {7, 8, 9, 10, 11}},
  }};
  for (const auto& [kind, offsets] : cases) {
    const auto layout = RadixSortKeyLayout::fromKind(kind);
    const auto descriptor = std::find_if(
        kLayouts.begin(), kLayouts.end(), [&](const auto& candidate) {
          return candidate.kind == kind;
        });
    ASSERT_NE(descriptor, kLayouts.end());
    for (const auto offset : offsets) {
      SCOPED_TRACE(
          "kind=" + std::to_string(static_cast<uint8_t>(kind)) +
          ", offset=" + std::to_string(offset));
      std::string left(
          layout.inlineCapacity() + (layout.isVariable() ? 8 : 0), 'p');
      auto right = left;
      left[offset] = '\x10';
      right[offset] = '\x20';
      PayloadPointers payloads(2);
      RadixSortRunStorage arena(pool_.get(), layout);
      const std::vector<std::string_view> keys{left, right};
      const auto heaps =
          appendPhysicalKeys(arena, keys, payloads.span(layout.hasPayload()));
      EXPECT_LT(
          comparePhysical(layout, recordAt(arena, 0), recordAt(arena, 1)), 0);
      EXPECT_LT(
          descriptor->compare(
              recordAt(arena, 0), recordAt(arena, 1), layout.heapKeyOffset()),
          0);
    }
  }
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
  constructPhysicalKey(
      layout,
      std::string_view(encoded.data(), encoded.size()),
      nullptr,
      nullptr,
      storage.data());
  EXPECT_EQ(loadUnaligned<uint64_t>(storage.data()), 0x0102030405060708ULL);
  EXPECT_EQ(loadUnaligned<uint64_t>(storage.data() + 8), 0x1112131415161718ULL);
  verifyPhysicalStorage(
      layout, storage.data(), std::string_view(encoded.data(), encoded.size()));
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
    RadixSortRunStorage arena(pool_.get(), layout);
    std::vector<std::string_view> keys(storage.begin(), storage.end());
    const auto heaps =
        appendPhysicalKeys(arena, keys, payloads.span(layout.hasPayload()));
    expectPairwiseCompare(arena, arena.size(), [&](auto left, auto right) {
      return descriptor.compare(
          recordAt(arena, left),
          recordAt(arena, right),
          layout.heapKeyOffset());
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
    for (const auto compareFlags : SortComparatorOracle::allSortFlags()) {
      SCOPED_TRACE(
          input->type()->toString() +
          (compareFlags.ascending ? " ASC" : " DESC") +
          (compareFlags.nullsFirst ? " NULLS FIRST" : " NULLS LAST"));
      auto rows = makeRows(input);
      verifyCodecStorage(rows, {compareFlags}, true, false);
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
      SortComparatorOracle::makeSortFlags(true, false),
      SortComparatorOracle::makeSortFlags(false, true),
      SortComparatorOracle::makeSortFlags(true, true)};
  verifyCodecStorage(rows, compareFlags, true, false);
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
      SortComparatorOracle::makeSortFlags(true, true),
      SortComparatorOracle::makeSortFlags(false, false)};
  verifyCodecStorage(rows, compareFlags, false, true);
}

TEST_F(RadixSortKeyTest, defaultKeyBlocksUseDefaultByteStorageTarget) {
  constexpr uint64_t kDefaultBlockBytes = 64 * 1024;
  for (const auto& descriptor : kLayouts) {
    const auto kind = descriptor.kind;
    SCOPED_TRACE(static_cast<uint8_t>(kind));
    auto layout = RadixSortKeyLayout::fromKind(kind);
    RadixSortRunStorage arena(pool_.get(), layout);
    const auto expectedRows =
        static_cast<uint32_t>(kDefaultBlockBytes / layout.width());
    EXPECT_EQ(arena.keysPerBlock(), expectedRows);
    EXPECT_LE(
        static_cast<uint64_t>(expectedRows) * layout.width(),
        kDefaultBlockBytes);

    const auto rows = expectedRows + 1;
    const std::vector<std::string> keys(
        rows, std::string(layout.inlineCapacity(), 'a'));
    PayloadPointers payloads(rows);
    const auto heaps =
        appendPhysicalKeys(arena, keys, payloads.span(layout.hasPayload()));
    EXPECT_EQ(arena.keyRangeAt(0, rows).count, expectedRows);
    EXPECT_EQ(arena.keyRangeAt(expectedRows, rows).count, 1);
  }
}

TEST_F(RadixSortKeyTest, storageBlockAllocationAndAddressingBoundaries) {
  auto keyLayout =
      RadixSortKeyLayout::fromKind(LayoutKind::kKeyWithPayloadFixed16);
  RadixSortRunStorage arena(pool_.get(), keyLayout);
  const auto rowsPerBlock = arena.keysPerBlock();
  const auto rowCount = static_cast<vector_size_t>(rowsPerBlock * 2 + 1);
  const std::array<uint32_t, 3> blockCounts{rowsPerBlock, rowsPerBlock, 1};
  PayloadPointers payloadPointers(rowCount);

  std::vector<std::string> keyStorage(rowCount);
  std::vector<std::string_view> keys(rowCount);
  std::vector<char*> payloads(rowCount);
  for (vector_size_t row = 0; row < rowCount; ++row) {
    keyStorage[row] = std::string(8, static_cast<char>('a' + row % 26));
    keyStorage[row][7] = static_cast<char>(row);
    keys[row] = keyStorage[row];
    payloads[row] = payloadPointers.pointers[row];
  }
  const auto heaps = appendPhysicalKeys(arena, keys, payloads);

  ASSERT_EQ(arena.size(), rowCount);
  for (uint32_t block = 0; block < 3; ++block) {
    const auto expectedCount = blockCounts[block];
    EXPECT_EQ(
        arena.keyRangeAt(static_cast<uint64_t>(block) * rowsPerBlock, rowCount)
            .count,
        expectedCount);
  }

  for (vector_size_t row = 0; row < rowCount; ++row) {
    const auto block = static_cast<uint32_t>(row) / rowsPerBlock;
    const auto indexInBlock = static_cast<uint32_t>(row) % rowsPerBlock;
    const auto blockRange =
        arena.keyRangeAt(static_cast<uint64_t>(block) * rowsPerBlock, rowCount);
    EXPECT_EQ(
        recordAt(arena, row),
        blockRange.data +
            static_cast<uint64_t>(indexInBlock) * keyLayout.width());
    EXPECT_EQ(
        payloadAt(keyLayout, recordAt(arena, row)),
        payloadPointers.pointers[row]);
    verifyPhysicalStorage(keyLayout, recordAt(arena, row), keys[row]);
  }

  for (uint64_t begin = 0; begin < arena.size();) {
    const auto range = arena.keyRangeAt(begin, rowCount);
    const auto expectedCount = std::min<vector_size_t>(
        rowsPerBlock - begin % rowsPerBlock, rowCount - begin);
    ASSERT_EQ(range.count, expectedCount);
    EXPECT_EQ(range.data, recordAt(arena, begin));
    begin += range.count;
  }
  EXPECT_EQ(arena.keyRangeAt(arena.size(), rowCount).count, 0);

  const std::array<uint64_t, 6> starts{
      0,
      rowsPerBlock - 1,
      rowsPerBlock,
      rowsPerBlock * 2 - 1,
      rowsPerBlock * 2,
      static_cast<uint64_t>(rowCount)};
  const std::array<vector_size_t, 4> requested{
      1,
      static_cast<vector_size_t>(rowsPerBlock - 1),
      static_cast<vector_size_t>(rowsPerBlock),
      static_cast<vector_size_t>(rowsPerBlock + 1)};
  for (const auto start : starts) {
    for (const auto maxCount : requested) {
      const auto range = arena.keyRangeAt(start, maxCount);
      const auto expectedCount = start == arena.size() || maxCount == 0
          ? 0
          : std::min<vector_size_t>(
                maxCount,
                blockCounts[start / rowsPerBlock] - start % rowsPerBlock);
      EXPECT_EQ(range.count, expectedCount);
      EXPECT_EQ(
          range.data, expectedCount == 0 ? nullptr : recordAt(arena, start));
    }
  }
}

TEST_F(RadixSortKeyTest, storageKeyRangeConstAccessAndEncodedAppend) {
  auto rows = makeRows(makeVector<int32_t>(INTEGER(), {5, 1, 7, 3, 9}));
  auto run =
      makeRun(rows, {SortComparatorOracle::makeSortFlags(true, true)}, true);
  const auto& arena = *run->storage();
  ASSERT_EQ(arena.size(), rows->size());

  const auto& constArena = arena;
  auto firstRange = constArena.keyRangeAt(0, 5);
  ASSERT_NE(firstRange.data, nullptr);
  EXPECT_EQ(firstRange.count, 5);
  EXPECT_EQ(firstRange.data, recordAt(constArena, 0));
  auto secondRange = constArena.keyRangeAt(2, 2);
  ASSERT_NE(secondRange.data, nullptr);
  EXPECT_EQ(secondRange.count, 2);
  EXPECT_EQ(secondRange.data, recordAt(constArena, 2));
  auto tailRange = constArena.keyRangeAt(4, 5);
  ASSERT_NE(tailRange.data, nullptr);
  EXPECT_EQ(tailRange.count, 1);
  EXPECT_EQ(constArena.keyRangeAt(arena.size(), 3).count, 0);
  EXPECT_EQ(constArena.keyRangeAt(0, 0).count, 0);

  for (uint64_t row = 0; row < arena.size(); ++row) {
    EXPECT_NE(payloadAt(arena.layout(), recordAt(arena, row)), nullptr);
  }
}

TEST_F(RadixSortKeyTest, storageAppendsInlineVariableKeysToFixedLayouts) {
  struct Case {
    std::vector<VectorPtr> columns;
    RadixSortKeyLayoutKind kind;
  };
  auto bigints = makeVector<int64_t>(BIGINT(), {3, -2, 1});
  auto integers = makeVector<int32_t>(INTEGER(), {6, -5, 4});
  const std::vector<Case> cases{
      {{bigints}, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16},
      {{bigints}, RadixSortKeyLayoutKind::kKeyOnlyFixed16},
      {{bigints, bigints}, RadixSortKeyLayoutKind::kKeyWithPayloadFixed24},
      {{bigints, bigints, integers},
       RadixSortKeyLayoutKind::kKeyWithPayloadFixed32},
      {{bigints, bigints}, RadixSortKeyLayoutKind::kKeyOnlyFixed24},
      {{bigints, bigints, bigints}, RadixSortKeyLayoutKind::kKeyOnlyFixed32},
  };

  for (const auto& testCase : cases) {
    SCOPED_TRACE(static_cast<uint8_t>(testCase.kind));
    auto rows = makeRows(testCase.columns);
    const auto flags = std::vector<CompareFlags>(
        rows->childrenSize(), SortComparatorOracle::makeSortFlags(true, true));
    const auto requested = RadixSortKeyLayout::fromKind(testCase.kind);
    auto run = makeRun(rows, flags, requested.hasPayload());
    const auto& arena = *run->storage();
    ASSERT_EQ(arena.layout().kind(), testCase.kind);
    ASSERT_EQ(arena.size(), rows->size());
    expectPairwiseCompare(arena, rows->size(), [&](auto left, auto right) {
      std::vector<column_index_t> keyChannels(rows->childrenSize());
      std::iota(keyChannels.begin(), keyChannels.end(), 0);
      return SortComparatorOracle::compareRows(
          *rows, left, *rows, right, keyChannels, flags);
    });
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
    RadixSortRunStorage arena(pool_.get(), layout);
    const auto heaps =
        appendPhysicalKeys(arena, keys, payloads.span(layout.hasPayload()));

    for (uint32_t index = 0; index < kPairsPerSeed; ++index) {
      const auto expected = SortComparatorOracle::compareUnsignedBytes(
          keys[index], keys[index + 1]);
      const auto actual = comparePhysical(
          layout, recordAt(arena, index), recordAt(arena, index + 1));
      EXPECT_EQ((actual > 0) - (actual < 0), expected)
          << "left=" << index << ", right=" << index + 1;
    }
  }
}

TEST_F(RadixSortKeyTest, allocationRangeBoundaryAndClear) {
  auto leaf = rootPool_->addLeafChild("radix-sort-arena-clear-test");
  EXPECT_EQ(leaf->currentBytes(), 0);
  auto layout = RadixSortKeyLayout::fromKind(LayoutKind::kKeyOnlyFixed32);
  RadixSortRunStorage arena(leaf.get(), layout);
  EXPECT_EQ(arena.size(), 0);
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(arena.keyRangeAt(0, 1).count, 0);
  arena.appendKeyBlocks(0, [](auto, auto, auto*) {});
  EXPECT_EQ(leaf->currentBytes(), 0);
  arena.appendKeyBlocks(
      arena.keysPerBlock() + 1,
      [&](vector_size_t, vector_size_t count, char* records) {
        std::memset(
            records, 'k', static_cast<uint64_t>(count) * layout.width());
      });

  EXPECT_EQ(arena.keyRangeAt(0, arena.size()).count, 2048);
  EXPECT_GE(arena.allocatedBytes(), arena.size() * arena.layout().width());
  EXPECT_GT(leaf->currentBytes(), 0);

  arena.clear();
  EXPECT_EQ(arena.size(), 0);
  EXPECT_EQ(arena.keyRangeAt(0, 1).count, 0);
  EXPECT_EQ(arena.allocatedBytes(), 0);
  EXPECT_EQ(leaf->currentBytes(), 0);
}

TEST_F(RadixSortKeyTest, heapAllocationRangeBoundary) {
  constexpr vector_size_t kRows = 33;
  const std::vector<std::optional<std::string>> values(
      kRows, std::string(4096, 'h'));
  auto rows = makeRows(makeStringVector(VARCHAR(), values));
  auto run =
      makeRun(rows, {SortComparatorOracle::makeSortFlags(true, true)}, false);
  ASSERT_TRUE(run->keyLayout().isVariable());
  EXPECT_GT(run->retainedBytes(), run->estimatedOutputBytes());

  run->finalize();
  auto output = RadixSortKeyTest::output(*run, kRows, pool_.get());
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_EQ(
        output->childAt(0)
            ->asUnchecked<SimpleVector<StringView>>()
            ->valueAt(row)
            .str(),
        *values[row]);
  }
  EXPECT_EQ(run->retainedBytes(), 0);
}

TEST_F(
    RadixSortKeyTest,
    storageEstimatedOutputBytesIncludesVariableKeyAndPayload) {
  constexpr vector_size_t kRows = 33;
  std::vector<std::optional<std::string>> keyStrings;
  std::vector<std::optional<std::string>> payloadStrings;
  std::vector<std::optional<int64_t>> payloadBigints;
  std::vector<vector_size_t> rawOffsets(kRows);
  std::vector<vector_size_t> rawSizes(kRows);
  std::vector<std::optional<int32_t>> mapKeys;
  std::vector<std::optional<std::string>> mapValues;
  keyStrings.reserve(kRows);
  payloadStrings.reserve(kRows);
  payloadBigints.reserve(kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    keyStrings.emplace_back(std::string(48 + row * 5, 'k'));
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
  std::vector<VectorPtr> children{
      makeStringVector(VARCHAR(), keyStrings),
      makeStringVector(VARCHAR(), payloadStrings),
      maps,
      makeVector<int64_t>(BIGINT(), payloadBigints)};
  std::vector<std::string> names{
      "key", "payload_string", "payload_map", "payload_bigint"};
  std::vector<TypePtr> types;
  types.reserve(children.size());
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  auto input = std::make_shared<RowVector>(
      pool_.get(),
      ROW(std::move(names), std::move(types)),
      nullptr,
      kRows,
      std::move(children));
  auto run =
      makeRun(input, {0}, {SortComparatorOracle::makeSortFlags(true, true)});

  ASSERT_EQ(run->size(), kRows);
  EXPECT_GT(run->retainedBytes(), run->estimatedOutputBytes());

  run->finalize();
  auto output = RadixSortKeyTest::output(*run, kRows, pool_.get());
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), kRows);
  for (vector_size_t row = 0; row < kRows; ++row) {
    EXPECT_TRUE(input->equalValueAt(output.get(), row, row));
  }
  EXPECT_EQ(run->retainedBytes(), 0);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
