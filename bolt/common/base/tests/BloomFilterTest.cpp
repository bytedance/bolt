/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/BloomFilter.h"

#include <folly/Hash.h>
#include <folly/Random.h>
#include <gtest/gtest.h>
#include <unordered_set>
using namespace bytedance::bolt;

class BloomFilterTest : public ::testing::Test {};

TEST_F(BloomFilterTest, basic) {
  constexpr int32_t kSize = 1024;
  BloomFilter bloom;
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(folly::hasher<int32_t>()(i));
  }
  int32_t numFalsePositives = 0;
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_TRUE(bloom.mayContain(folly::hasher<int32_t>()(i)));
    numFalsePositives += bloom.mayContain(folly::hasher<int32_t>()(i + kSize));
    numFalsePositives +=
        bloom.mayContain(folly::hasher<int32_t>()((i + kSize) * 123451));
  }
  EXPECT_GT(2, 100 * numFalsePositives / kSize);
}

TEST_F(BloomFilterTest, serialize) {
  constexpr int32_t kSize = 1024;
  BloomFilter bloom;
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(folly::hasher<int32_t>()(i));
  }
  std::string data;
  data.resize(bloom.serializedSize());
  bloom.serialize(data.data());
  BloomFilter deserialized;
  deserialized.merge(data.data());
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_TRUE(deserialized.mayContain(folly::hasher<int32_t>()(i)));
  }
  EXPECT_FALSE(
      deserialized.mayContain(folly::hasher<int32_t>()(kSize + 123451)));

  EXPECT_EQ(bloom.serializedSize(), deserialized.serializedSize());
}

TEST_F(BloomFilterTest, staticMayContain) {
  constexpr int32_t kSize = 1024;
  std::string serializedBloom;
  BloomFilter bloom;
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(folly::hasher<int32_t>()(i));
  }
  serializedBloom.resize(bloom.serializedSize());
  bloom.serialize(serializedBloom.data());
  int32_t numFalsePositives = 0;
  for (auto i = 0; i < kSize; ++i) {
    EXPECT_TRUE(BloomFilter<>::mayContain(
        serializedBloom.data(), folly::hasher<int32_t>()(i)));

    const uint64_t smallValueHash = folly::hasher<int32_t>()(i + kSize);
    const bool isFalsePositiveForSmallValue =
        BloomFilter<>::mayContain(serializedBloom.data(), smallValueHash);
    EXPECT_EQ(isFalsePositiveForSmallValue, bloom.mayContain(smallValueHash));
    numFalsePositives += isFalsePositiveForSmallValue;

    const uint64_t largeValueHash =
        folly::hasher<int32_t>()((i + kSize) * 123451);
    const bool isFalsePositiveForLargeValue =
        BloomFilter<>::mayContain(serializedBloom.data(), largeValueHash);
    EXPECT_EQ(isFalsePositiveForLargeValue, bloom.mayContain(largeValueHash));
    numFalsePositives += isFalsePositiveForLargeValue;
  }
  EXPECT_GT(2, 100 * numFalsePositives / kSize);
}

TEST_F(BloomFilterTest, merge) {
  constexpr int32_t kSize = 10;
  BloomFilter bloom;
  bloom.reset(kSize);
  for (auto i = 0; i < kSize; ++i) {
    bloom.insert(folly::hasher<int32_t>()(i));
  }

  BloomFilter merge;
  merge.reset(kSize);
  for (auto i = kSize; i < kSize + kSize; i++) {
    merge.insert(folly::hasher<int32_t>()(i));
  }

  std::string data;
  data.resize(bloom.serializedSize());
  merge.serialize(data.data());

  bloom.merge(data.data());

  for (auto i = 0; i < kSize + kSize; ++i) {
    EXPECT_TRUE(bloom.mayContain(folly::hasher<int32_t>()(i)));
  }
  EXPECT_FALSE(bloom.mayContain(folly::hasher<int32_t>()(kSize + 123451)));

  EXPECT_EQ(bloom.serializedSize(), merge.serializedSize());
}

TEST(NGramBloomFilterTest, basic) {
  constexpr int32_t kSize = 512;
  constexpr int32_t kHashes = 3;
  constexpr int32_t kSeed = 100;
  NGramBloomFilter bloom(512, 3, 100);
  std::vector<std::string> vals = {
      "中国",
      "国人",
      "阿烽",
      "烽赶",
      "赶海",
      "第二",
      "二波",
      "今日",
      "日头",
      "头条"};
  std::vector<std::string> finds = {"中国", "赶海", "第二", "头条"};
  for (auto val : vals) {
    bloom.insert(val);
  }
  int32_t numFalsePositives = 0;
  for (auto find : finds) {
    EXPECT_TRUE(bloom.mayContain(find));
  }
}
