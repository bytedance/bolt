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

#include "bolt/common/base/SpillConfig.h"
#include <gtest/gtest.h>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/exec/HashBitRange.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::exec;

TEST(SpillConfig, spillLevel) {
  const uint8_t kInitialBitOffset = 16;
  const uint8_t kNumPartitionsBits = 3;
  static const std::string emptyPath = "";
  GetSpillDirectoryPathCB getPathCb = [&]() -> const std::string& {
    return emptyPath;
  };
  UpdateAndCheckSpillLimitCB checkLimitCb = [&](uint64_t) {};
  const SpillConfig config(
      getPathCb,
      checkLimitCb,
      "fakeSpillPath",
      0,
      false,
      0,
      nullptr,
      0,
      0,
      kInitialBitOffset,
      kNumPartitionsBits,
      0,
      0,
      0,
      0,
      "none");
  struct {
    uint8_t bitOffset;
    // Indicates an invalid if 'expectedLevel' is negative.
    int32_t expectedLevel;

    std::string debugString() const {
      return fmt::format(
          "bitOffset:{}, expectedLevel:{}", bitOffset, expectedLevel);
    }
  } testSettings[] = {
      {0, -1},
      {kInitialBitOffset - 1, -1},
      {kInitialBitOffset - kNumPartitionsBits, -1},
      {kInitialBitOffset, 0},
      {kInitialBitOffset + 1, -1},
      {kInitialBitOffset + kNumPartitionsBits, 1},
      {kInitialBitOffset + 3 * kNumPartitionsBits, 3},
      {kInitialBitOffset + 15 * kNumPartitionsBits, 15},
      {kInitialBitOffset + 16 * kNumPartitionsBits, -1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedLevel == -1) {
      ASSERT_ANY_THROW(config.joinSpillLevel(testData.bitOffset));
    } else {
      ASSERT_EQ(
          config.joinSpillLevel(testData.bitOffset), testData.expectedLevel);
    }
  }
}

TEST(SpillConfig, spillLevelLimit) {
  struct {
    uint8_t startBitOffset;
    int32_t numBits;
    uint8_t bitOffset;
    int32_t maxSpillLevel;
    bool expectedExceeds;

    std::string debugString() const {
      return fmt::format(
          "startBitOffset:{}, numBits:{}, bitOffset:{}, maxSpillLevel:{}, expectedExceeds:{}",
          startBitOffset,
          numBits,
          bitOffset,
          maxSpillLevel,
          expectedExceeds);
    }
  } testSettings[] = {
      {0, 2, 2, 0, true},
      {0, 2, 2, 1, false},
      {0, 2, 4, 0, true},
      {0, 2, 0, -1, false},
      {0, 2, 62, -1, false},
      {0, 2, 63, -1, true},
      {0, 2, 64, -1, true},
      {0, 2, 65, -1, true},
      {30, 3, 30, 0, false},
      {30, 3, 33, 0, true},
      {30, 3, 30, 1, false},
      {30, 3, 33, 1, false},
      {30, 3, 36, 1, true},
      {30, 3, 0, -1, false},
      {30, 3, 60, -1, false},
      {30, 3, 63, -1, true},
      {30, 3, 66, -1, true}};
  static const std::string emptyPath = "";
  GetSpillDirectoryPathCB getPathCb = [&]() -> const std::string& {
    return emptyPath;
  };
  UpdateAndCheckSpillLimitCB checkLimitCb = [&](uint64_t) {};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    const HashBitRange partitionBits(
        testData.startBitOffset, testData.startBitOffset + testData.numBits);
    const SpillConfig config(
        getPathCb,
        checkLimitCb,
        "fakeSpillPath",
        0,
        false,
        0,
        nullptr,
        0,
        0,
        testData.startBitOffset,
        testData.numBits,
        testData.maxSpillLevel,
        0,
        0,
        0,
        "none");

    ASSERT_EQ(
        testData.expectedExceeds,
        config.exceedJoinSpillLevelLimit(testData.bitOffset));
  }
}

// ---- Serialization helpers --------------------------------------------------

namespace {

// Returns a SpillConfig with every serializable field set to a non-default
// value, making it easy to detect fields that were silently dropped.
SpillConfig makeFullConfig() {
  SpillConfig cfg;
  cfg.fileNamePrefix = "test_spill";
  cfg.maxFileSize = 512UL * 1024 * 1024;
  cfg.spillUringEnabled = true;
  cfg.writeBufferSize = 8UL * 1024 * 1024;
  cfg.minSpillableReservationPct = 10;
  cfg.spillableReservationGrowthPct = 25;
  cfg.startPartitionBit = 29;
  cfg.joinPartitionBits = 4;
  cfg.joinRepartitionBits = 2;
  cfg.maxSpillLevel = 3;
  cfg.maxSpillRunRows = 100'000;
  cfg.writerFlushThresholdSize = 64UL * 1024 * 1024;
  cfg.testSpillPct = 5;
  cfg.compressionKind = CompressionKind_ZSTD;
  cfg.fileCreateConfig = R"({"option":"value"})";
  cfg.rowBasedSpillMode = RowBasedSpillMode::RAW;
  cfg.singlePartitionSerdeKind = "Arrow";
  cfg.spillPartitionsAdaptiveThreshold = 64;
  cfg.jitEnabled = false;
  cfg.needSetNextEqual = true;
  cfg.aggBypassHTEqualNum = 42;
  return cfg;
}

void assertFieldsEqual(const SpillConfig& e, const SpillConfig& a) {
  EXPECT_EQ(e.fileNamePrefix, a.fileNamePrefix);
  EXPECT_EQ(e.maxFileSize, a.maxFileSize);
  EXPECT_EQ(e.spillUringEnabled, a.spillUringEnabled);
  EXPECT_EQ(e.writeBufferSize, a.writeBufferSize);
  EXPECT_EQ(e.minSpillableReservationPct, a.minSpillableReservationPct);
  EXPECT_EQ(e.spillableReservationGrowthPct, a.spillableReservationGrowthPct);
  EXPECT_EQ(e.startPartitionBit, a.startPartitionBit);
  EXPECT_EQ(e.joinPartitionBits, a.joinPartitionBits);
  EXPECT_EQ(e.joinRepartitionBits, a.joinRepartitionBits);
  EXPECT_EQ(e.maxSpillLevel, a.maxSpillLevel);
  EXPECT_EQ(e.maxSpillRunRows, a.maxSpillRunRows);
  EXPECT_EQ(e.writerFlushThresholdSize, a.writerFlushThresholdSize);
  EXPECT_EQ(e.testSpillPct, a.testSpillPct);
  EXPECT_EQ(e.compressionKind, a.compressionKind);
  EXPECT_EQ(e.fileCreateConfig, a.fileCreateConfig);
  EXPECT_EQ(e.rowBasedSpillMode, a.rowBasedSpillMode);
  EXPECT_EQ(e.singlePartitionSerdeKind, a.singlePartitionSerdeKind);
  EXPECT_EQ(
      e.spillPartitionsAdaptiveThreshold, a.spillPartitionsAdaptiveThreshold);
  EXPECT_EQ(e.jitEnabled, a.jitEnabled);
  EXPECT_EQ(e.needSetNextEqual, a.needSetNextEqual);
  EXPECT_EQ(e.aggBypassHTEqualNum, a.aggBypassHTEqualNum);
}

} // namespace

// ---- Serialization tests ----------------------------------------------------

TEST(SpillConfig, serializeContainsNameField) {
  folly::dynamic dyn = makeFullConfig().serialize();
  ASSERT_TRUE(dyn.isObject());
  ASSERT_EQ("SpillConfig", dyn["name"].asString());
}

TEST(SpillConfig, roundTripAllFields) {
  SpillConfig cfg = makeFullConfig();
  auto rt = SpillConfig::deserialize(cfg.serialize());
  ASSERT_NE(nullptr, rt);
  assertFieldsEqual(cfg, *rt);
}

TEST(SpillConfig, callbacksAndExecutorNullAfterRoundTrip) {
  SpillConfig cfg = makeFullConfig();
  cfg.getSpillDirPathCb = []() -> const std::string& {
    static const std::string path = "/tmp/spill";
    return path;
  };
  cfg.updateAndCheckSpillLimitCb = [](uint64_t) {};

  auto rt = SpillConfig::deserialize(cfg.serialize());
  ASSERT_NE(nullptr, rt);
  EXPECT_FALSE(static_cast<bool>(rt->getSpillDirPathCb));
  EXPECT_FALSE(static_cast<bool>(rt->updateAndCheckSpillLimitCb));
  EXPECT_EQ(nullptr, rt->executor);
}

TEST(SpillConfig, roundTripViaISerializableRegistry) {
  SpillConfig cfg = makeFullConfig();
  auto rt = ISerializable::deserialize<SpillConfig>(cfg.serialize());
  ASSERT_NE(nullptr, rt);
  assertFieldsEqual(cfg, *rt);
}

TEST(SpillConfig, roundTripDefaultConstructed) {
  SpillConfig cfg;
  auto rt = SpillConfig::deserialize(cfg.serialize());
  ASSERT_NE(nullptr, rt);
  assertFieldsEqual(cfg, *rt);
}

TEST(SpillConfig, roundTripRowBasedSpillModes) {
  for (auto mode :
       {RowBasedSpillMode::DISABLE,
        RowBasedSpillMode::RAW,
        RowBasedSpillMode::COMPRESSION}) {
    SpillConfig cfg = makeFullConfig();
    cfg.rowBasedSpillMode = mode;
    auto rt = SpillConfig::deserialize(cfg.serialize());
    ASSERT_NE(nullptr, rt);
    EXPECT_EQ(mode, rt->rowBasedSpillMode);
  }
}

TEST(SpillConfig, roundTripCompressionKinds) {
  for (auto kind :
       {CompressionKind_NONE,
        CompressionKind_ZLIB,
        CompressionKind_SNAPPY,
        CompressionKind_ZSTD,
        CompressionKind_LZ4,
        CompressionKind_GZIP}) {
    SpillConfig cfg = makeFullConfig();
    cfg.compressionKind = kind;
    auto rt = SpillConfig::deserialize(cfg.serialize());
    ASSERT_NE(nullptr, rt);
    EXPECT_EQ(kind, rt->compressionKind);
  }
}

TEST(SpillConfig, maxSpillLevelUnlimited) {
  SpillConfig cfg = makeFullConfig();
  cfg.maxSpillLevel = -1; // -1 means no limit
  auto rt = SpillConfig::deserialize(cfg.serialize());
  ASSERT_NE(nullptr, rt);
  EXPECT_EQ(-1, rt->maxSpillLevel);
}

// ---- spillableReservationPercentages ----------------------------------------

TEST(SpillConfig, spillableReservationPercentages) {
  struct {
    uint32_t growthPct;
    int32_t minPct;
    bool expectedError;

    std::string debugString() const {
      return fmt::format(
          "growthPct:{}, minPct:{}, expectedError:{}",
          growthPct,
          minPct,
          expectedError);
    }
  } testSettings[] = {
      {100, 50, false},
      {50, 50, false},
      {50, 100, true},
      {1, 50, true},
      {1, 1, false}};
  const std::string emptySpillFolder = "";
  GetSpillDirectoryPathCB getPathCb = [&]() -> const std::string& {
    return emptySpillFolder;
  };
  UpdateAndCheckSpillLimitCB checkLimitCb = [&](uint64_t) {};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto createConfigFn = [&]() {
      const SpillConfig config(
          getPathCb,
          checkLimitCb,
          "spillableReservationPercentages",
          0,
          false,
          0,
          nullptr,
          testData.minPct,
          testData.growthPct,
          0,
          0,
          0,
          1'000'000,
          0,
          0,
          "none");
    };

    if (testData.expectedError) {
      BOLT_ASSERT_THROW(
          createConfigFn(),
          "Spillable memory reservation growth pct should not be lower than minimum available pct");
    } else {
      createConfigFn();
    }
  }
}
