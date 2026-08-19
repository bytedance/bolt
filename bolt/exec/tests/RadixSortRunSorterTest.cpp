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

  static std::vector<std::string_view> views(
      const std::vector<std::string>& keys) {
    std::vector<std::string_view> result;
    result.reserve(keys.size());
    for (const auto& key : keys) {
      result.push_back(key);
    }
    return result;
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

  static void oracleSort(RadixSortRunStorage& arena) {
    std::vector<RadixSortInlineKeyBuffer> keys(arena.size());
    for (uint64_t index = 0; index < arena.size(); ++index) {
      std::memcpy(
          keys[index].data(), arena.keyDataAt(index), arena.layout().width());
    }
    std::sort(
        keys.begin(), keys.end(), [&](const auto& left, const auto& right) {
          return RadixSortKey(arena.layout(), left.data())
                     .compare(RadixSortKey(arena.layout(), right.data())) < 0;
        });
    for (uint64_t index = 0; index < arena.size(); ++index) {
      std::memcpy(
          arena.keyDataAt(index), keys[index].data(), arena.layout().width());
    }
  }

  static void expectSameOrder(
      const RadixSortRunStorage& actual,
      const RadixSortRunStorage& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (uint64_t index = 0; index < actual.size(); ++index) {
      EXPECT_EQ(actual.keyAt(index).compare(expected.keyAt(index)), 0)
          << "index=" << index;
    }
  }

  static void expectSorted(const RadixSortRunStorage& arena) {
    for (uint64_t index = 1; index < arena.size(); ++index) {
      EXPECT_LE(arena.keyAt(index - 1).compare(arena.keyAt(index)), 0)
          << "index=" << index;
    }
  }

  static void expectPayloadCoupled(
      const RadixSortRunStorage& arena,
      const std::vector<std::string>& originalKeys) {
    ASSERT_TRUE(arena.layout().hasPayload());
    for (uint64_t index = 0; index < arena.size(); ++index) {
      const auto* identity = reinterpret_cast<const PayloadIdentity*>(
          arena.keyAt(index).payload());
      ASSERT_NE(identity, nullptr);
      ASSERT_LT(identity->index, originalKeys.size());

      RadixSortRunStorage expected(
          arena.pool(),
          arena.layout(),
          1,
          originalKeys[identity->index].size());
      expected.append(
          originalKeys[identity->index],
          const_cast<char*>(arena.keyAt(index).payload()));
      EXPECT_EQ(arena.keyAt(index).compare(expected.keyAt(0)), 0);
    }
  }

  void verifyAgainstOracle(
      const std::vector<std::string>& keys,
      RadixSortKeyLayoutKind kind,
      std::span<const uint32_t> skippableByteOffsets = {}) {
    auto selectedLayout = layout(kind);
    std::vector<PayloadIdentity> payloads;
    std::vector<PayloadIdentity> oraclePayloads;
    RadixSortRunStorage actual(pool_.get(), selectedLayout, 31, 4096);
    RadixSortRunStorage expected(pool_.get(), selectedLayout, 29, 4096);
    append(actual, keys, selectedLayout.hasPayload() ? &payloads : nullptr);
    append(
        expected,
        keys,
        selectedLayout.hasPayload() ? &oraclePayloads : nullptr);

    RadixSortRunSorter sorter(actual);
    sorter.sort(skippableByteOffsets);
    oracleSort(expected);

    expectSameOrder(actual, expected);
    expectSorted(actual);
    if (selectedLayout.hasPayload()) {
      expectPayloadCoupled(actual, keys);
    }
  }
};

TEST_F(RadixSortRunSorterTest, randomFixedAndVariableMatchComparisonOracle) {
  std::mt19937_64 random(20260809);
  std::vector<std::string> fixedKeys;
  fixedKeys.reserve(4096);
  for (uint32_t index = 0; index < 4096; ++index) {
    fixedKeys.push_back(fixedKey(random() % 2048, random() % 16));
  }
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

  std::vector<std::string> variableKeys;
  variableKeys.reserve(4096);
  for (uint32_t index = 0; index < 4096; ++index) {
    const auto size = 17 + random() % 160;
    std::string key(size, 'p');
    for (uint32_t byte = 8; byte < key.size(); ++byte) {
      key[byte] = static_cast<char>(random());
    }
    variableKeys.push_back(std::move(key));
  }
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
  std::vector<std::string> sorted;
  for (uint64_t value = 0; value < 2048; ++value) {
    sorted.push_back(fixedKey(value));
  }
  verifyAgainstOracle(sorted, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  std::reverse(sorted.begin(), sorted.end());
  verifyAgainstOracle(sorted, RadixSortKeyLayoutKind::kKeyWithPayloadFixed16);

  std::vector<std::string> equal(2048, fixedKey(7));
  verifyAgainstOracle(equal, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  std::vector<std::string> duplicates;
  for (uint64_t value = 0; value < 4096; ++value) {
    duplicates.push_back(fixedKey(value % 13));
  }
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
  sorter.sort(codec->leadingSkippableValidityOffsets(keyMayHaveNulls));

  expectSorted(arena);
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
  std::vector<std::string> keys;
  keys.reserve(kSize);
  for (uint64_t begin = 0; begin < kSize; begin += 8) {
    if ((begin / 8) % 2 == 0) {
      for (uint64_t offset = 0; offset < 8; ++offset) {
        keys.push_back(fixedKey(begin + offset));
      }
    } else {
      for (uint64_t offset = 0; offset < 8; ++offset) {
        keys.push_back(fixedKey(begin + 7 - offset));
      }
    }
  }

  auto selectedLayout = layout();
  RadixSortRunStorage arena(pool_.get(), selectedLayout, 37, 4096);
  append(arena, keys);
  RadixSortRunSorter sorter(arena);
  sorter.sort();

  expectSorted(arena);
}

TEST_F(RadixSortRunSorterTest, longNaturalRunsMatchOracle) {
  std::vector<std::string> keys;
  for (uint64_t value = 2048; value < 4096; ++value) {
    keys.push_back(fixedKey(value));
  }
  for (uint64_t value = 0; value < 2048; ++value) {
    keys.push_back(fixedKey(value));
  }
  for (uint64_t value = 4096; value < 6144; ++value) {
    keys.push_back(fixedKey(value));
  }

  auto selectedLayout = layout();
  RadixSortRunStorage adaptive(pool_.get(), selectedLayout, 41, 4096);
  RadixSortRunStorage oracle(pool_.get(), selectedLayout, 43, 4096);
  append(adaptive, keys);
  append(oracle, keys);

  RadixSortRunSorter sorter(adaptive);
  sorter.sort();
  oracleSort(oracle);

  expectSameOrder(adaptive, oracle);
}

TEST_F(RadixSortRunSorterTest, longRunsSeparatedBySmallDisorderMatchOracle) {
  std::vector<std::string> keys;
  for (uint64_t value = 2048; value < 4096; ++value) {
    keys.push_back(fixedKey(value));
  }
  for (uint64_t value = 0; value < 16; ++value) {
    keys.push_back(fixedKey(8192 - value * 7));
  }
  for (uint64_t value = 0; value < 2048; ++value) {
    keys.push_back(fixedKey(value));
  }

  auto selectedLayout = layout();
  RadixSortRunStorage actual(pool_.get(), selectedLayout, 41, 4096);
  RadixSortRunStorage expected(pool_.get(), selectedLayout, 43, 4096);
  append(actual, keys);
  append(expected, keys);

  RadixSortRunSorter sorter(actual);
  sorter.sort();
  oracleSort(expected);

  expectSameOrder(actual, expected);
}

TEST_F(RadixSortRunSorterTest, radixSingleBucketAllBucketsAndHighSkew) {
  std::vector<std::string> oneBucket;
  for (uint64_t value = 1024; value > 0; --value) {
    oneBucket.push_back(fixedKey(value, 17));
  }
  verifyAgainstOracle(oneBucket, RadixSortKeyLayoutKind::kKeyOnlyFixed8);

  std::vector<std::string> allBuckets;
  for (uint32_t bucket = 0; bucket < 256; ++bucket) {
    for (uint64_t value = 0; value < 4; ++value) {
      allBuckets.push_back(fixedKey(3 - value, bucket));
    }
  }
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
    std::vector<std::string> keys;
    keys.reserve(size);
    for (uint64_t row = 0; row < size; ++row) {
      keys.push_back(fixedKey(random(), random()));
    }
    verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed8);
  }
}

TEST_F(RadixSortRunSorterTest, longCommonPrefixFallsBackToFullKey) {
  std::vector<std::string> keys;
  for (uint64_t value = 2048; value > 0; --value) {
    std::string key(96, 'x');
    for (uint32_t byte = 0; byte < sizeof(value); ++byte) {
      key[key.size() - 1 - byte] = static_cast<char>(value >> (byte * 8));
    }
    keys.push_back(std::move(key));
  }
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
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
  std::vector<std::string> keys;
  keys.reserve(512);
  for (uint32_t row = 0; row < 512; ++row) {
    const auto eventIndex = row % 10 < 6 ? row % 3 : row % kEvents.size();
    keys.push_back(
        std::string(kEvents[eventIndex]) + "_" + std::to_string(row % 17));
  }
  std::reverse(keys.begin(), keys.end());

  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyVariable32);
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyWithPayloadVariable32);
}

TEST_F(RadixSortRunSorterTest, fixedWideSuffixBytesSortBeforeFallback) {
  std::vector<std::string> keys;
  for (uint64_t value = 4096; value > 0; --value) {
    keys.push_back(fixedWideKey(value % 257, 32));
  }
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed32);

  std::vector<std::string> payloadKeys;
  for (uint64_t value = 4096; value > 0; --value) {
    payloadKeys.push_back(
        fixedWideKey(value % 257, 24, static_cast<char>('a' + value % 4)));
  }
  verifyAgainstOracle(
      payloadKeys, RadixSortKeyLayoutKind::kKeyWithPayloadFixed32);
}

TEST_F(RadixSortRunSorterTest, fewBucketRadixPassesDoNotConsumeBudget) {
  std::vector<std::string> keys;
  keys.reserve(4096);
  for (uint64_t value = 4096; value > 0; --value) {
    std::string key(32, '\0');
    for (uint32_t byte = 0; byte < 8; ++byte) {
      key[byte] = static_cast<char>((value >> (2 * (7 - byte))) & 3);
    }
    const auto suffix = value ^ 0x9e3779b97f4a7c15ULL;
    for (uint32_t byte = 0; byte < sizeof(suffix); ++byte) {
      key[key.size() - 1 - byte] = static_cast<char>(suffix >> (byte * 8));
    }
    keys.push_back(std::move(key));
  }
  verifyAgainstOracle(keys, RadixSortKeyLayoutKind::kKeyOnlyFixed32);
}

TEST_F(RadixSortRunSorterTest, leadingValiditySkipAvoidsConstantPass) {
  std::vector<std::string> keys;
  for (uint64_t value = 4096; value > 0; --value) {
    auto key = fixedKey(value);
    key[0] = 1;
    keys.push_back(std::move(key));
  }

  auto selectedLayout = layout();
  RadixSortRunStorage withoutSkip(pool_.get(), selectedLayout, 31, 4096);
  RadixSortRunStorage withSkip(pool_.get(), selectedLayout, 31, 4096);
  append(withoutSkip, keys);
  append(withSkip, keys);

  RadixSortRunSorter withoutSkipSorter(withoutSkip);
  withoutSkipSorter.sort();
  RadixSortRunSorter withSkipSorter(withSkip);
  const std::array<uint32_t, 1> skippable{0};
  withSkipSorter.sort(skippable);

  expectSameOrder(withSkip, withoutSkip);
}

} // namespace
} // namespace bytedance::bolt::exec::radixsort::test
