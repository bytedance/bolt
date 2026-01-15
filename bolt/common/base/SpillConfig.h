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

#pragma once

#include <stdint.h>
#include <string.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <optional>
#include "bolt/common/compression/Compression.h"
#include "bolt/vector/VectorStream.h"
namespace bytedance::bolt::common {

#define BOLT_SPILL_LIMIT_EXCEEDED(errorMessage)                     \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kSpillLimitExceeded.c_str(),   \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

/// Defining type for a callback function that returns the spill directory path.
/// Implementations can use it to ensure the path exists before returning.
using GetSpillDirectoryPathCB = std::function<const std::string&()>;

/// The callback used to update the aggregated spill bytes of a query. If the
/// query spill limit is set, the callback throws if the aggregated spilled
/// bytes exceed the set limit.
using UpdateAndCheckSpillLimitCB = std::function<void(uint64_t)>;

/// Specifies the options for spill to disk.
struct SpillDiskOptions {
  std::string spillDirPath;
  bool spillDirCreated{true};
  std::function<std::string()> spillDirCreateCb{nullptr};
};

enum class RowBasedSpillMode {
  DISABLE, // disable row based spill
  RAW, // row based spill with raw data
  COMPRESSION // row based spill with compression
};

RowBasedSpillMode strToRowBasedSpillMode(const std::string& str);

/// Specifies the config for spilling.
struct SpillConfig {
  SpillConfig() = default;
  SpillConfig(
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
      const std::string& _fileCreateConfig = {},
      const std::string& _rowBasedSpillMode = "disabled",
      const std::string& _singlePartitionSerdeKind = {},
      bool _jitEnabled = true);

  /// Returns the hash join spilling level with given 'startBitOffset'.
  ///
  /// NOTE: we advance (or right shift) the partition bit offset when goes to
  /// the next level of recursive spilling.
  int32_t joinSpillLevel(uint8_t startBitOffset) const;

  /// Returns the hash join spilling level with given 'offsetToJoinBits' map.
  int32_t joinSpillLevel(std::map<uint8_t, uint8_t>& offsetToJoinBits) const;

  /// Checks if the given 'startBitOffset' has exceeded the max hash join
  /// spill limit.
  bool exceedJoinSpillLevelLimit(uint8_t startBitOffset) const;

  std::string toString() const;

  /// A callback function that returns the spill directory path. Implementations
  /// can use it to ensure the path exists before returning.
  GetSpillDirectoryPathCB getSpillDirPathCb;

  /// The callback used to update the aggregated spill bytes of a query. If the
  /// query spill limit is set, the callback throws if the aggregated spilled
  /// bytes exceed the set limit.
  UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb;

  /// Prefix for spill files.
  std::string fileNamePrefix;
  bool exceedJoinSpillLevelLimit(
      std::map<uint8_t, uint8_t>& offsetToJoinBits) const;

  SpillConfig& setJITenableForSpill(bool enabled) noexcept;
  bool getJITenabledForSpill() const noexcept;

  struct SpillIOConfig {
    GetSpillDirectoryPathCB getSpillDirPathCb;
    UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb;
    std::string fileNamePrefix;
    uint64_t maxFileSize;
    bool spillUringEnabled;
    uint64_t writeBufferSize;
    common::CompressionKind compressionKind;
    std::string fileCreateConfig;
    std::optional<VectorSerde::Kind> spillSerdeKind;
  };

  SpillIOConfig spillIOConfig(int32_t maxPartitions) const {
    std::optional<VectorSerde::Kind> kind;
    if (maxPartitions == 1 && !singlePartitionSerdeKind.empty()) {
      kind = VectorSerde::kindByName(singlePartitionSerdeKind);
    }
    return SpillIOConfig{
        getSpillDirPathCb,
        updateAndCheckSpillLimitCb,
        fileNamePrefix,
        maxFileSize,
        spillUringEnabled,
        writeBufferSize,
        compressionKind,
        fileCreateConfig,
        kind};
  }

  /// The max spill file size. If it is zero, there is no limit on the spill
  /// file size.
  uint64_t maxFileSize;

  // io_uring control
  bool spillUringEnabled;

  /// Specifies the size to buffer the serialized spill data before write to
  /// storage system for io efficiency.
  uint64_t writeBufferSize;

  /// Executor for spilling. If nullptr spilling writes on the Driver's thread.
  folly::Executor* executor; // Not owned.

  /// The minimal spillable memory reservation in percentage of the current
  /// memory usage.
  int32_t minSpillableReservationPct;

  /// The spillable memory reservation growth in percentage of the current
  /// memory usage.
  int32_t spillableReservationGrowthPct;

  /// Used to calculate spill partition number.
  uint8_t startPartitionBit;

  /// Used to calculate the spill hash partition number for hash join with
  /// 'startPartitionBit'.
  uint8_t joinPartitionBits;

  /// partition bits for spill level > 0
  uint8_t joinRepartitionBits;

  /// The max allowed spilling level with zero being the initial spilling
  /// level. This only applies for hash build spilling which needs recursive
  /// spilling when the build table is too big. If it is set to -1, then there
  /// is no limit and then some extreme large query might run out of spilling
  /// partition bits at the end.
  int32_t maxSpillLevel;

  /// The max row numbers to fill and spill for each spill run. This is used to
  /// cap the memory used for spilling. If it is zero, then there is no limit
  /// and spilling might run out of memory.
  uint64_t maxSpillRunRows;

  /// Minimum memory footprint size required to reclaim memory from a file
  /// writer by flushing its buffered data to disk.
  uint64_t writerFlushThresholdSize;

  /// Percentage of input batches to be spilled for testing. 0 means no
  /// spilling for test.
  int32_t testSpillPct;

  /// CompressionKind when spilling, CompressionKind_NONE means no compression.
  common::CompressionKind compressionKind;

  /// Custom options passed to bolt::FileSystem to create spill WriteFile.
  std::string fileCreateConfig;

  // spill direct with row format
  RowBasedSpillMode rowBasedSpillMode;

  // Optional serde kind for single-partition row-vector spill (e.g., "Arrow").
  std::string singlePartitionSerdeKind;

  /// The max allowed number of partitions to be adjusted upwards.
  uint32_t spillPartitionsAdaptiveThreshold{128};

  /// Enable JIT for Spill
  bool jitEnabled{true};

  /// set equal flag for row[i] when row[i] == row[i+1]
  bool needSetNextEqual{false};

  /// equal num in the sorted runs to be spilled
  size_t aggBypassHTEqualNum{0};
};
} // namespace bytedance::bolt::common

template <>
struct fmt::formatter<bytedance::bolt::common::RowBasedSpillMode>
    : fmt::formatter<std::string_view> {
  auto format(bytedance::bolt::common::RowBasedSpillMode c, format_context& ctx)
      const {
    switch (c) {
      case bytedance::bolt::common::RowBasedSpillMode::DISABLE:
        return fmt::formatter<std::string_view>::format("DISABLE", ctx);
      case bytedance::bolt::common::RowBasedSpillMode::RAW:
        return fmt::formatter<std::string_view>::format("RAW", ctx);
      case bytedance::bolt::common::RowBasedSpillMode::COMPRESSION:
        return fmt::formatter<std::string_view>::format("COMPRESSION", ctx);
      default:
        return fmt::format_to(ctx.out(), "unknown[{}]", static_cast<int>(c));
    }
  }
};
