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

#include <fmt/format.h>
#include <folly/dynamic.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SuccinctPrinter.h"
#include "bolt/common/compression/Compression.h"

namespace bytedance::bolt::common {

RowBasedSpillMode strToRowBasedSpillMode(const std::string& str) {
  if (str == "raw") {
    return RowBasedSpillMode::RAW;
  } else if (str == "compression") {
    return RowBasedSpillMode::COMPRESSION;
  } else {
    return RowBasedSpillMode::DISABLE;
  }
}

SpillConfig::SpillConfig(
    GetSpillDirectoryPathCB _getSpillDirPathCb,
    UpdateAndCheckSpillLimitCB _updateAndCheckSpillLimitCb,
    const std::string& _fileNamePrefix,
    uint64_t _maxFileSize,
    bool _spillUringEnabled,
    uint64_t _writeBufferSize,
    folly::Executor* _executor,
    int32_t _minSpillableReservationPct,
    int32_t _spillableReservationGrowthPct,
    uint8_t _startPartitionBit,
    uint8_t _joinPartitionBits,
    int32_t _maxSpillLevel,
    uint64_t _maxSpillRunRows,
    uint64_t _writerFlushThresholdSize,
    int32_t _testSpillPct,
    const std::string& _compressionKind,
    const std::string& _fileCreateConfig,
    const std::string& _rowBasedSpillMode,
    const std::string& _singlePartitionSerdeKind,
    bool jitEnabled)
    : getSpillDirPathCb(std::move(_getSpillDirPathCb)),
      updateAndCheckSpillLimitCb(std::move(_updateAndCheckSpillLimitCb)),
      fileNamePrefix(std::move(_fileNamePrefix)),
      maxFileSize(
          _maxFileSize == 0 ? std::numeric_limits<int64_t>::max()
                            : _maxFileSize),
      spillUringEnabled(_spillUringEnabled),
      writeBufferSize(_writeBufferSize),
      executor(_executor),
      minSpillableReservationPct(_minSpillableReservationPct),
      spillableReservationGrowthPct(_spillableReservationGrowthPct),
      startPartitionBit(_startPartitionBit),
      joinPartitionBits(_joinPartitionBits),
      joinRepartitionBits(_joinPartitionBits),
      maxSpillLevel(_maxSpillLevel),
      maxSpillRunRows(_maxSpillRunRows),
      writerFlushThresholdSize(_writerFlushThresholdSize),
      testSpillPct(_testSpillPct),
      compressionKind(common::stringToCompressionKind(_compressionKind)),
      fileCreateConfig(_fileCreateConfig),
      rowBasedSpillMode(strToRowBasedSpillMode(_rowBasedSpillMode)),
      singlePartitionSerdeKind(_singlePartitionSerdeKind),
      jitEnabled(jitEnabled) {
  BOLT_USER_CHECK_GE(
      spillableReservationGrowthPct,
      minSpillableReservationPct,
      "Spillable memory reservation growth pct should not be lower than minimum available pct");
}

int32_t SpillConfig::joinSpillLevel(uint8_t startBitOffset) const {
  const auto numPartitionBits = joinPartitionBits;
  BOLT_CHECK_LE(
      startBitOffset + numPartitionBits,
      64,
      "startBitOffset:{} numPartitionsBits:{}",
      startBitOffset,
      numPartitionBits);
  const int32_t deltaBits = startBitOffset - startPartitionBit;
  BOLT_CHECK_GE(deltaBits, 0, "deltaBits:{}", deltaBits);
  BOLT_CHECK_EQ(
      deltaBits % numPartitionBits,
      0,
      "deltaBits:{} numPartitionsBits{}",
      deltaBits,
      numPartitionBits);
  return deltaBits / numPartitionBits;
}

bool SpillConfig::exceedJoinSpillLevelLimit(uint8_t startBitOffset) const {
  if (startBitOffset + joinPartitionBits > 64) {
    return true;
  }
  if (maxSpillLevel == -1) {
    return false;
  }
  return joinSpillLevel(startBitOffset) > maxSpillLevel;
}

int32_t SpillConfig::joinSpillLevel(
    std::map<uint8_t, uint8_t>& offsetToJoinBits) const {
  if (offsetToJoinBits.empty()) {
    return 0;
  }
  auto mapToString = [&]() -> std::string {
    std::vector<std::string> pairs;
    pairs.reserve(offsetToJoinBits.size());

    for (const auto& [offset, bits] : offsetToJoinBits) {
      pairs.emplace_back(fmt::format("[{}:{}]", offset, bits));
    }

    return fmt::format("{{{}}}", fmt::join(pairs, ","));
  };
  // should start from startPartitionBit
  auto firstIt = offsetToJoinBits.begin();
  BOLT_CHECK_EQ(
      startPartitionBit,
      firstIt->first,
      "startPartitionBit:{} offsetToJoinBits:{}",
      startPartitionBit,
      mapToString());

  auto it = offsetToJoinBits.begin();
  uint8_t prevStart = it->first;
  uint8_t prevBits = it->second;
  ++it;
  for (; it != offsetToJoinBits.end(); ++it) {
    BOLT_CHECK_EQ(
        it->first, prevStart + prevBits, "offsetToJoinBits:{}", mapToString());
    prevStart = it->first;
    prevBits = it->second;
  }
  BOLT_CHECK_LE(
      prevStart + prevBits,
      64,
      "startBitOffset:{} numPartitionsBits:{} offsetToJoinBits:{}",
      prevStart,
      prevBits,
      mapToString());
  return offsetToJoinBits.size() - 1;
}

bool SpillConfig::exceedJoinSpillLevelLimit(
    std::map<uint8_t, uint8_t>& offsetToJoinBits) const {
  if (offsetToJoinBits.empty()) {
    return false;
  }
  auto lastIt = offsetToJoinBits.end();
  --lastIt;
  if (lastIt->first + lastIt->second > 64) {
    LOG(ERROR) << __FUNCTION__ << " : start from " << lastIt->first
               << ", joinBits = " << lastIt->second;
    return true;
  }
  if (maxSpillLevel == -1) {
    return false;
  }
  return joinSpillLevel(offsetToJoinBits) > maxSpillLevel;
}

std::string SpillConfig::toString() const {
  return fmt::format(
      "SpillConfig:\n\t"
      "fileNamePrefix:{},\n\t"
      "maxFileSize:{},\n\t"
      "writeBufferSize:{},\n\t"
      "minSpillableReservationPct:{},\n\t"
      "spillableReservationGrowthPct:{},\n\t"
      "startPartitionBit:{},\n\t"
      "joinPartitionBits:{},\n\t"
      "maxSpillLevel:{},\n\t"
      "maxSpillRunRows:{},\n\t"
      "writerFlushThresholdSize:{},\n\t"
      "testSpillPct:{},\n\t"
      "compressionKind:{},\n\t"
      "fileCreateConfig:{}\n"
      "rowBasedSpillMode:{}\n"
      "singlePartitionSerdeKind:{}\n"
      "jitEnabled:{}\n",
      fileNamePrefix,
      succinctBytes(maxFileSize),
      succinctBytes(writeBufferSize),
      minSpillableReservationPct,
      spillableReservationGrowthPct,
      startPartitionBit,
      joinPartitionBits,
      maxSpillLevel,
      maxSpillRunRows,
      succinctBytes(writerFlushThresholdSize),
      testSpillPct,
      compressionKind,
      fileCreateConfig,
      rowBasedSpillMode,
      singlePartitionSerdeKind,
      jitEnabled);
}

folly::dynamic SpillConfig::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "SpillConfig";
  obj["fileNamePrefix"] = fileNamePrefix;
  obj["maxFileSize"] = static_cast<int64_t>(maxFileSize);
  obj["spillUringEnabled"] = spillUringEnabled;
  obj["writeBufferSize"] = static_cast<int64_t>(writeBufferSize);
  obj["minSpillableReservationPct"] = minSpillableReservationPct;
  obj["spillableReservationGrowthPct"] = spillableReservationGrowthPct;
  obj["startPartitionBit"] = static_cast<int64_t>(startPartitionBit);
  obj["joinPartitionBits"] = static_cast<int64_t>(joinPartitionBits);
  obj["joinRepartitionBits"] = static_cast<int64_t>(joinRepartitionBits);
  obj["maxSpillLevel"] = maxSpillLevel;
  obj["maxSpillRunRows"] = static_cast<int64_t>(maxSpillRunRows);
  obj["writerFlushThresholdSize"] =
      static_cast<int64_t>(writerFlushThresholdSize);
  obj["testSpillPct"] = testSpillPct;
  obj["compressionKind"] = static_cast<int>(compressionKind);
  obj["fileCreateConfig"] = fileCreateConfig;
  obj["rowBasedSpillMode"] = static_cast<int>(rowBasedSpillMode);
  obj["singlePartitionSerdeKind"] = singlePartitionSerdeKind;
  obj["spillPartitionsAdaptiveThreshold"] =
      static_cast<int64_t>(spillPartitionsAdaptiveThreshold);
  obj["jitEnabled"] = jitEnabled;
  obj["needSetNextEqual"] = needSetNextEqual;
  obj["aggBypassHTEqualNum"] = static_cast<int64_t>(aggBypassHTEqualNum);
  // getSpillDirPathCb, updateAndCheckSpillLimitCb, and executor are omitted;
  // they must be re-injected by the host after deserialization.
  return obj;
}

std::shared_ptr<SpillConfig> SpillConfig::deserialize(
    const folly::dynamic& obj) {
  auto spillConfig = std::make_shared<SpillConfig>();
  spillConfig->fileNamePrefix = obj["fileNamePrefix"].asString();
  spillConfig->maxFileSize = static_cast<uint64_t>(obj["maxFileSize"].asInt());
  spillConfig->spillUringEnabled = obj["spillUringEnabled"].asBool();
  spillConfig->writeBufferSize =
      static_cast<uint64_t>(obj["writeBufferSize"].asInt());
  spillConfig->minSpillableReservationPct =
      static_cast<int32_t>(obj["minSpillableReservationPct"].asInt());
  spillConfig->spillableReservationGrowthPct =
      static_cast<int32_t>(obj["spillableReservationGrowthPct"].asInt());
  spillConfig->startPartitionBit =
      static_cast<uint8_t>(obj["startPartitionBit"].asInt());
  spillConfig->joinPartitionBits =
      static_cast<uint8_t>(obj["joinPartitionBits"].asInt());
  spillConfig->joinRepartitionBits =
      static_cast<uint8_t>(obj["joinRepartitionBits"].asInt());
  spillConfig->maxSpillLevel =
      static_cast<int32_t>(obj["maxSpillLevel"].asInt());
  spillConfig->maxSpillRunRows =
      static_cast<uint64_t>(obj["maxSpillRunRows"].asInt());
  spillConfig->writerFlushThresholdSize =
      static_cast<uint64_t>(obj["writerFlushThresholdSize"].asInt());
  spillConfig->testSpillPct = static_cast<int32_t>(obj["testSpillPct"].asInt());
  spillConfig->compressionKind =
      static_cast<common::CompressionKind>(obj["compressionKind"].asInt());
  spillConfig->fileCreateConfig = obj["fileCreateConfig"].asString();
  spillConfig->rowBasedSpillMode =
      static_cast<RowBasedSpillMode>(obj["rowBasedSpillMode"].asInt());
  spillConfig->singlePartitionSerdeKind =
      obj["singlePartitionSerdeKind"].asString();
  spillConfig->spillPartitionsAdaptiveThreshold =
      static_cast<uint32_t>(obj["spillPartitionsAdaptiveThreshold"].asInt());
  spillConfig->jitEnabled = obj["jitEnabled"].asBool();
  spillConfig->needSetNextEqual = obj["needSetNextEqual"].asBool();
  spillConfig->aggBypassHTEqualNum =
      static_cast<size_t>(obj["aggBypassHTEqualNum"].asInt());
  // getSpillDirPathCb, updateAndCheckSpillLimitCb, and executor remain
  // default-initialized (null); callers must re-inject them as needed.
  return spillConfig;
}

void SpillConfig::registerSerDe() {
  auto& registry = bolt::DeserializationRegistryForSharedPtr();
  registry.Register("SpillConfig", SpillConfig::deserialize);
}

namespace {
const bool kSpillConfigSerdeRegistered = []() {
  SpillConfig::registerSerDe();
  return true;
}();
} // namespace

SpillConfig& SpillConfig::setJITenableForSpill(bool enabled) noexcept {
  jitEnabled = enabled;
  return *this;
}

bool SpillConfig::getJITenabledForSpill() const noexcept {
  return jitEnabled;
}
} // namespace bytedance::bolt::common
