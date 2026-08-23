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
#include <bit>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "bolt/exec/radixsort/RadixSortRunSorter.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::test {
namespace {

struct PayloadIdentity {
  uint64_t index;
};

class RadixSortRunSorterTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

 protected:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("radix-run-sort-test")};

  static RadixSortKeyLayout layout(
      RadixSortKeyLayoutKind kind = RadixSortKeyLayoutKind::kKeyOnlyFixed8) {
    return RadixSortKeyLayout::fromKind(kind);
  }

  static std::string fixedKey(uint64_t value, uint8_t prefix = 0) {
    std::string key(8, '\0');
    key[0] = static_cast<char>(prefix);
    for (uint32_t byte = 1; byte < key.size(); ++byte) {
      key[byte] = static_cast<char>(value >> ((key.size() - byte - 1) * 8));
    }
    return key;
  }

  static std::string
  fixedWideKey(uint64_t suffix, uint32_t width, char prefix = 'p') {
    std::string key(width, prefix);
    for (uint32_t byte = 0; byte < sizeof(suffix); ++byte) {
      key[key.size() - 1 - byte] = static_cast<char>(suffix >> (byte * 8));
    }
    return key;
  }

  template <typename MakeKey>
  static std::vector<std::string> makeKeys(uint64_t size, MakeKey&& makeKey) {
    std::vector<std::string> keys;
    keys.reserve(size);
    for (uint64_t row = 0; row < size; ++row) {
      keys.push_back(makeKey(row));
    }
    return keys;
  }

  static std::vector<std::string_view> views(
      const std::vector<std::string>& keys) {
    return {keys.begin(), keys.end()};
  }

  static void append(
      RadixSortRunStorage& arena,
      const std::vector<std::string>& keys,
      std::vector<PayloadIdentity>* payloads = nullptr) {
    auto keyViews = views(keys);
    if (payloads == nullptr) {
      arena.appendBatch(keyViews);
      return;
    }

    payloads->resize(keys.size());
    std::vector<char*> pointers(keys.size());
    for (uint64_t index = 0; index < keys.size(); ++index) {
      (*payloads)[index].index = index;
      pointers[index] = reinterpret_cast<char*>(&(*payloads)[index]);
    }
    arena.appendBatch(keyViews, pointers);
  }

  static int32_t unsignedByteCompare(
      std::string_view left,
      std::string_view right) {
    const auto commonSize = std::min(left.size(), right.size());
    for (uint64_t index = 0; index < commonSize; ++index) {
      const auto leftByte = static_cast<uint8_t>(left[index]);
      const auto rightByte = static_cast<uint8_t>(right[index]);
      if (leftByte != rightByte) {
        return (leftByte > rightByte) - (leftByte < rightByte);
      }
    }
    return (left.size() > right.size()) - (left.size() < right.size());
  }

  static std::string normalizedKeyForLayout(
      const RadixSortKeyLayout& layout,
      std::string_view key) {
    if (layout.isVariable()) {
      return std::string(key);
    }
    std::string normalized(layout.inlineCapacity(), '\0');
    std::memcpy(
        normalized.data(),
        key.data(),
        std::min<uint64_t>(key.size(), normalized.size()));
    return normalized;
  }

  static std::string logicalKey(
      const RadixSortKey& key,
      const RadixSortKeyLayout& layout,
      const char* record) {
    if (layout.isVariable()) {
      return std::string(record, layout.heapKeyOffset()) +
          std::string(key.heapKey());
    }
    RadixSortInlineKeyBuffer buffer;
    EncodedKeyView view;
    key.deconstruct(buffer, view);
    return std::string(view.bytes);
  }

  static void expectMatchesIndependentOracle(
      const RadixSortRunStorage& actual,
      const std::vector<std::string>& originalKeys) {
    ASSERT_EQ(actual.size(), originalKeys.size());
    std::vector<std::string> expected;
    expected.reserve(originalKeys.size());
    for (const auto& key : originalKeys) {
      expected.push_back(normalizedKeyForLayout(actual.layout(), key));
    }
    std::sort(
        expected.begin(),
        expected.end(),
        [](const auto& left, const auto& right) {
          return unsignedByteCompare(left, right) < 0;
        });
    std::vector<std::string> actualKeys;
    actualKeys.reserve(actual.size());
    for (uint64_t index = 0; index < actual.size(); ++index) {
      actualKeys.emplace_back(logicalKey(
          actual.keyAt(index), actual.layout(), actual.keyDataAt(index)));
    }
    EXPECT_EQ(actualKeys, expected);
  }

  static void expectPayloadCoupled(
      const RadixSortRunStorage& arena,
      const std::vector<std::string>& originalKeys) {
    ASSERT_TRUE(arena.layout().hasPayload());
    std::vector<bool> seen(originalKeys.size(), false);
    for (uint64_t index = 0; index < arena.size(); ++index) {
      const auto* identity = reinterpret_cast<const PayloadIdentity*>(
          arena.keyAt(index).payload());
      ASSERT_NE(identity, nullptr);
      ASSERT_LT(identity->index, originalKeys.size());
      EXPECT_FALSE(seen[identity->index])
          << "duplicate payload identity=" << identity->index;
      seen[identity->index] = true;
      const auto key = logicalKey(
          arena.keyAt(index), arena.layout(), arena.keyDataAt(index));
      const auto expected =
          normalizedKeyForLayout(arena.layout(), originalKeys[identity->index]);
      EXPECT_EQ(unsignedByteCompare(key, expected), 0)
          << "index=" << index << ", identity=" << identity->index;
    }
    EXPECT_TRUE(std::all_of(
        seen.begin(), seen.end(), [](bool value) { return value; }));
  }

  void verifyAgainstOracle(
      const std::vector<std::string>& keys,
      RadixSortKeyLayoutKind kind,
      std::span<const uint32_t> skippableByteOffsets = {},
      uint32_t keysPerBlock = 31,
      uint64_t keyHeapGroupBytes = 4096,
      std::optional<RadixSortKeyLayout> layoutOverride = std::nullopt) {
    auto selectedLayout = layoutOverride.value_or(layout(kind));
    std::vector<PayloadIdentity> payloads;
    RadixSortRunStorage actual(
        pool_.get(), selectedLayout, keysPerBlock, keyHeapGroupBytes);
    append(actual, keys, selectedLayout.hasPayload() ? &payloads : nullptr);
    RadixSortRunSorter sorter(actual);
    sorter.sort(skippableByteOffsets);

    expectMatchesIndependentOracle(actual, keys);
    if (selectedLayout.hasPayload()) {
      expectPayloadCoupled(actual, keys);
    }
  }

  static std::vector<std::string> makeNaturalRuns(
      std::span<const uint64_t> runLengths,
      bool descending,
      int32_t unstableRun = -1) {
    const auto total =
        std::accumulate(runLengths.begin(), runLengths.end(), uint64_t{0});
    const auto stride = total + 1;
    std::vector<std::string> keys;
    keys.reserve(total);
    for (uint64_t run = 0; run < runLengths.size(); ++run) {
      const auto length = runLengths[run];
      const auto base =
          descending ? run * stride : (runLengths.size() - run) * stride;
      for (uint64_t offset = 0; offset < length; ++offset) {
        const auto value = static_cast<int32_t>(run) == unstableRun
            ? ((offset % 2 != 0 || offset + 1 == length)
                   ? (runLengths.size() + 1) * stride + offset
                   : offset)
            : base + (descending ? length - offset : offset);
        keys.push_back(fixedKey(value));
      }
    }
    return keys;
  }
};

TEST_F(RadixSortRunSorterTest, randomFixedAndVariableMatchComparisonOracle) {
  std::mt19937_64 random(20260809);
  auto fixedKeys = makeKeys(
      4096, [&](auto) { return fixedKey(random() % 2048, random() % 16); });
  for (const auto kind :
       {RadixSortKeyLayoutKind::kKeyOnlyFixed8,
        RadixSortKeyLayoutKind::kKeyOnlyFixed16,
        RadixSortKeyLayoutKind::kKeyOnlyFixed24,
        RadixSortKeyLayoutKind::kKeyOnlyFixed32,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed24,
        RadixSortKeyLayoutKind::kKeyWithPayloadFixed32}) {
    verifyAgainstOracle(fixedKeys, kind);
  }

  auto variableKeys = makeKeys(4096, [&](auto) {
    const auto size = 17 + random() % 160;
    std::string key(size, 'p');
    for (uint32_t byte = 8; byte < key.size(); ++byte) {
      key[byte] = static_cast<char>(random());
    }
    return key;
  });
  verifyAgainstOracle(variableKeys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  verifyAgainstOracle(
      variableKeys, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
}

TEST_F(RadixSortRunSorterTest, fixedSeedPropertyMatchesComparisonOracle) {
  constexpr uint32_t kSeeds = 32;
  constexpr uint32_t kRows = 1024;
  const std::array<RadixSortKeyLayoutKind, 4> kinds{
      RadixSortKeyLayoutKind::kKeyOnlyFixed8,
      RadixSortKeyLayoutKind::kKeyWithPayloadFixed16,
      RadixSortKeyLayoutKind::kKeyOnlyVariable32,
      RadixSortKeyLayoutKind::kKeyWithPayloadVariable32};

  for (uint32_t seed = 0; seed < kSeeds; ++seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    std::mt19937_64 random(seed);
    const auto kind = kinds[seed % kinds.size()];
    const auto selectedLayout = layout(kind);
    std::vector<std::string> keys;
    keys.reserve(kRows);
    for (uint32_t row = 0; row < kRows; ++row) {
      if (selectedLayout.isVariable()) {
        const auto size = 1 + random() % 192;
        std::string key(size, '\0');
        const auto prefix = seed % 3 == 0 ? std::min<uint64_t>(size, 48) : 0;
        std::fill(key.begin(), key.begin() + prefix, 'q');
        for (uint64_t byte = prefix; byte < size; ++byte) {
          key[byte] = static_cast<char>(random());
        }
        keys.push_back(std::move(key));
      } else {
        keys.push_back(fixedKey(random() % 257, random() % 8));
      }
    }

    verifyAgainstOracle(keys, kind);
  }
}

TEST_F(RadixSortRunSorterTest, sortedReverseEqualAndDuplicateKeys) {
  auto sorted = makeKeys(2048, [](auto value) { return fixedKey(value); });
  verifyAgainstOracle(sorted, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  std::reverse(sorted.begin(), sorted.end());
  verifyAgainstOracle(sorted, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);

  std::vector<std::string> equal(2048, fixedKey(7));
  verifyAgainstOracle(equal, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  auto duplicates =
      makeKeys(4096, [](auto value) { return fixedKey(value % 13); });
  verifyAgainstOracle(
      duplicates, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
}

TEST_F(RadixSortRunSorterTest, codecKeysWithManyNullsMatchBoltComparator) {
  constexpr vector_size_t kRows = 4096;
  auto values =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), kRows, pool_.get());
  std::mt19937 random(1729);
  for (vector_size_t row = 0; row < kRows; ++row) {
    if (row % 3 == 0) {
      values->setNull(row, true);
    } else {
      values->set(row, static_cast<int32_t>(random() % 257));
    }
  }
  auto rows = std::make_shared<RowVector>(
      pool_.get(),
      ROW({INTEGER()}),
      nullptr,
      kRows,
      std::vector<VectorPtr>{values});
  const CompareFlags compareFlags{
      .nullsFirst = false,
      .ascending = true,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind({INTEGER()}, {compareFlags}, codec);
  EncodedKeyBatch encodedKeys;
  codec->encode(*rows, pool_.get(), encodedKeys);

  auto selectedLayout =
      RadixSortKeyLayout::select(codec->maximumEncodedSize(), true);
  RadixSortRunStorage arena(pool_.get(), selectedLayout, 31, 4096);
  std::vector<uint64_t> rowIds(kRows);
  std::vector<char*> payloads(kRows);
  for (uint64_t row = 0; row < kRows; ++row) {
    rowIds[row] = row;
    payloads[row] = reinterpret_cast<char*>(&rowIds[row]);
  }
  arena.appendBatch(encodedKeys, payloads);

  RadixSortRunSorter sorter(arena);
  const std::vector<uint8_t> keyMayHaveNulls{1};
  sorter.sort(codec->leadingSkippableValidityOffsets(
      keyMayHaveNulls, selectedLayout.radixWidth()));

  for (uint64_t index = 1; index < arena.size(); ++index) {
    const auto left =
        *reinterpret_cast<const uint64_t*>(arena.keyAt(index - 1).payload());
    const auto right =
        *reinterpret_cast<const uint64_t*>(arena.keyAt(index).payload());
    const auto result =
        values->compare(values.get(), left, right, compareFlags);
    ASSERT_TRUE(result.has_value());
    EXPECT_LE(*result, 0);
  }
}

TEST_F(RadixSortRunSorterTest, alternatingShortRunsSortCorrectly) {
  constexpr uint64_t kSize = 8192;
  auto keys = makeKeys(kSize, [](auto row) {
    const auto begin = row / 8 * 8;
    return fixedKey((begin / 8) % 2 == 0 ? row : begin + 7 - row % 8);
  });
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8);
}

TEST_F(RadixSortRunSorterTest, naturalRunsMatchOracle) {
  struct Case {
    std::string name;
    std::vector<uint64_t> runLengths;
    bool descending;
    int32_t unstableRun;
  };
  std::vector<Case> cases{
      {"long ascending", {2048, 2048, 2048}, false, -1},
      {"long descending", {2048, 2048, 2048}, true, -1},
      {"unstable prefix", {16, 2048}, false, 0},
      {"unstable middle", {2048, 16, 2048}, false, 1},
      {"unstable suffix", {1024, 1024, 1024, 1024, 16}, false, 4}};

  constexpr uint64_t kBoundaryTotal = 4096;
  const auto floorLog2 = std::bit_width(kBoundaryTotal) - 1;
  const auto limit = kBoundaryTotal / floorLog2;
  for (const auto runLength : {limit - 1, limit, limit + 1}) {
    for (const bool descending : {false, true}) {
      cases.push_back(
          {"boundary " + std::to_string(runLength),
           {runLength, kBoundaryTotal - runLength},
           descending,
           -1});
    }
  }

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name + (test.descending ? " descending" : " ascending"));
    verifyAgainstOracle(
        makeNaturalRuns(test.runLengths, test.descending, test.unstableRun),
        test.descending || test.unstableRun >= 0
            ? RadixSortKeyLayoutKind::kKeyWithPayloadFixed16
            : RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  }
}

TEST_F(RadixSortRunSorterTest, radixSingleBucketAllBucketsAndHighSkew) {
  auto oneBucket =
      makeKeys(1024, [](auto row) { return fixedKey(1024 - row, 17); });
  verifyAgainstOracle(oneBucket, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  auto allBuckets =
      makeKeys(1024, [](auto row) { return fixedKey(3 - row % 4, row / 4); });
  std::reverse(allBuckets.begin(), allBuckets.end());
  verifyAgainstOracle(allBuckets, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  std::vector<std::string> skewed(4000, fixedKey(1, 9));
  for (uint32_t bucket = 0; bucket < 256; ++bucket) {
    skewed.push_back(fixedKey(bucket, bucket));
  }
  verifyAgainstOracle(skewed, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);
}

TEST_F(RadixSortRunSorterTest, algorithmThresholdBoundaries) {
  std::mt19937_64 random(20260809);
  for (const uint64_t size : {23, 24, 127, 128, 129, 1023, 1024, 1025}) {
    SCOPED_TRACE("size=" + std::to_string(size));
    auto keys =
        makeKeys(size, [&](auto) { return fixedKey(random(), random()); });
    verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  }
}

TEST_F(RadixSortRunSorterTest, blockSizeBoundariesSortAcrossSegments) {
  auto keys = makeKeys(
      257, [](auto row) { return fixedKey((row * 37) % 257, row % 5); });
  for (const uint32_t keysPerBlock : {2, 31, 32, 33}) {
    SCOPED_TRACE("keysPerBlock=" + std::to_string(keysPerBlock));
    verifyAgainstOracle(
        keys, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16, {}, keysPerBlock);
  }
}

TEST_F(RadixSortRunSorterTest, largeBucketThresholdsUseSharedRadixPrefix) {
  for (const uint64_t size : {1023, 1024, 1025}) {
    SCOPED_TRACE("size=" + std::to_string(size));
    auto keys = makeKeys(size, [&](auto row) {
      auto key = fixedKey(size - row, 0x42);
      key[1] = 0x11;
      key[2] = 0x22;
      return key;
    });
    verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  }
}

TEST_F(RadixSortRunSorterTest, longCommonPrefixFallsBackToFullKey) {
  auto keys = makeKeys(2048, [](auto row) {
    const auto value = 2048 - row;
    std::string key(96, 'x');
    for (uint32_t byte = 0; byte < sizeof(value); ++byte) {
      key[key.size() - 1 - byte] = static_cast<char>(value >> (byte * 8));
    }
    return key;
  });
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
}

TEST_F(RadixSortRunSorterTest, variablePrefixTiesAtInlineAndWordBoundaries) {
  std::vector<std::string> keys{
      std::string("prefix") + std::string(10, '\0'),
      std::string("prefix") + std::string(10, '\xff'),
      std::string("prefix") + std::string(9, '\xff'),
      std::string("prefix") + std::string(10, '\0') + "suffix",
      std::string(16, 'a'),
      std::string(16, 'a') + '\0',
      std::string(16, 'a') + '\xff',
      std::string(24, 'b'),
      std::string(24, 'b') + '\0',
      std::string(24, 'b') + '\xff',
      std::string(32, 'c'),
      std::string(32, 'c') + '\0',
      std::string(32, 'c') + '\xff'};
  for (uint32_t repeat = 0; repeat < 16; ++repeat) {
    keys.push_back(std::string(64, 'x') + static_cast<char>(repeat));
    keys.push_back(std::string(64, 'x'));
  }
  std::reverse(keys.begin(), keys.end());

  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
}

TEST_F(RadixSortRunSorterTest, variableHeapOffsetSortsBySuffix) {
  auto layout = RadixSortKeyLayout::select(std::nullopt, false, 9);
  std::vector<std::string> keys;
  for (uint32_t row = 0; row < 512; ++row) {
    std::string key = "fixed-pre";
    key += static_cast<char>('z' - row % 26);
    key += std::string(40 + row % 17, static_cast<char>('a' + row % 5));
    key += static_cast<char>(row);
    keys.push_back(std::move(key));
  }
  std::reverse(keys.begin(), keys.end());
  verifyAgainstOracle(
      keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32, {}, 31, 4096, layout);
}

TEST_F(RadixSortRunSorterTest, variableRadixCoversEntireRecordPrefix) {
  auto layout = RadixSortKeyLayout::select(std::nullopt, false, 9);
  std::vector<std::string> keys;
  keys.reserve(96);
  for (uint32_t value = 96; value > 0; --value) {
    std::string key(40, 's');
    key[12] = static_cast<char>(value);
    keys.push_back(std::move(key));
  }
  verifyAgainstOracle(
      keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32, {}, 31, 4096, layout);
}

TEST_F(RadixSortRunSorterTest, lowCardinalityVariableStringKeyMatchesOracle) {
  static constexpr std::array<const char*, 8> kEvents{
      "video_play",
      "video_play_pause",
      "like",
      "follow",
      "share",
      "comment",
      "enter_homepage",
      "click_music"};
  auto keys = makeKeys(512, [&](auto row) {
    const auto eventIndex = row % 10 < 6 ? row % 3 : row % kEvents.size();
    return std::string(kEvents[eventIndex]) + "_" + std::to_string(row % 17);
  });
  std::reverse(keys.begin(), keys.end());

  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
}

TEST_F(RadixSortRunSorterTest, wideVariablePayloadKeysMatchOracle) {
  std::vector<std::string> keys;
  keys.reserve(2048);
  for (uint64_t row = 0; row < 1024; ++row) {
    std::string threeKeyColumns(
        40 + row % 37, static_cast<char>('a' + row % 5));
    threeKeyColumns[0] = static_cast<char>(row % 17);
    threeKeyColumns[8] = static_cast<char>((1023 - row) % 251);
    threeKeyColumns.back() = static_cast<char>(row % 251);
    keys.push_back(std::move(threeKeyColumns));

    std::string fourKeyColumns(48 + row % 41, static_cast<char>('k' + row % 7));
    fourKeyColumns[0] = static_cast<char>(row % 19);
    fourKeyColumns[16] = static_cast<char>((row * 13) % 251);
    fourKeyColumns[32] = static_cast<char>((1023 - row) % 251);
    fourKeyColumns.back() = static_cast<char>(row % 251);
    keys.push_back(std::move(fourKeyColumns));
  }
  std::reverse(keys.begin(), keys.end());

  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
}

TEST_F(RadixSortRunSorterTest, fixedWideSuffixBytesSortBeforeFallback) {
  auto keys = makeKeys(
      4096, [](auto row) { return fixedWideKey((4096 - row) % 257, 32); });
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed32);

  auto payloadKeys = makeKeys(4096, [](auto row) {
    const auto value = 4096 - row;
    return fixedWideKey(value % 257, 24, static_cast<char>('a' + value % 4));
  });
  verifyAgainstOracle(
      payloadKeys, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32);
}

TEST_F(RadixSortRunSorterTest, radixPrefixBeyondEightBytesSortsLateBytes) {
  for (const uint32_t passCount : {8, 9}) {
    SCOPED_TRACE("passCount=" + std::to_string(passCount));
    const bool lowFanout = passCount == 8;
    const uint64_t size = lowFanout ? 4096 : 1536;
    std::vector<std::string> keys;
    keys.reserve(size);
    for (uint64_t value = size; value > 0; --value) {
      std::string key(32, '\0');
      for (uint32_t byte = 0; byte < passCount; ++byte) {
        key[byte] = static_cast<char>(
            lowFanout ? (value >> (2 * (7 - byte))) & 3
                      : (value >> byte) & 0x1f);
      }
      const auto suffix =
          value ^ (lowFanout ? 0x9e3779b97f4a7c15ULL : 0xa5a5a5a5a5a5a5a5ULL);
      for (uint32_t byte = 0; byte < sizeof(suffix); ++byte) {
        key[key.size() - 1 - byte] = static_cast<char>(suffix >> (byte * 8));
      }
      keys.push_back(std::move(key));
    }
    verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed32);
  }
}

TEST_F(RadixSortRunSorterTest, leadingValiditySkipAvoidsConstantPass) {
  auto keys = makeKeys(4096, [](auto row) {
    auto key = fixedKey(4096 - row);
    key[0] = 1;
    return key;
  });

  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  const std::array<uint32_t, 1> skippable{0};
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8, skippable);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
