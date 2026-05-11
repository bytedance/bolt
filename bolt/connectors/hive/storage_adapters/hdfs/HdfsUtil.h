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

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include <folly/Conv.h>

#include "bolt/common/config/Config.h"
#include "bolt/common/file/FileSystems.h"

namespace bytedance::bolt::filesystems {

struct HdfsOpenFileOptions {
  static constexpr const char* kBufferSize = "bolt.io.file.buffer.size";
  static constexpr const char* kReplication = "bolt.dfs.replication";
  static constexpr const char* kBlockSize = "bolt.dfs.blocksize";

  int bufferSize{0};
  short replication{0};
  int blockSize{0};
};

inline std::string getHdfsPath(
    const std::string& filePath,
    const std::string_view& kScheme) {
  auto endOfAuthority = filePath.find('/', kScheme.size());
  std::string hdfsAuthority{
      filePath, kScheme.size(), endOfAuthority - kScheme.size()};
  if (hdfsAuthority.empty()) {
    return std::string(filePath.substr(kScheme.size()));
  }

  return std::string(filePath.substr(endOfAuthority));
}

inline int parseHdfsOpenFileIntOption(
    const FileOptions& options,
    const char* key) {
  auto it = options.values.find(key);
  if (it == options.values.end()) {
    return 0;
  }

  auto value = folly::tryTo<int64_t>(it->second);
  BOLT_CHECK(
      value.hasValue(),
      "Invalid HDFS open file option '{}': '{}'. Expected an integer.",
      key,
      it->second);
  BOLT_CHECK_GE(
      value.value(),
      0,
      "Invalid HDFS open file option '{}': '{}'. Expected a non-negative integer.",
      key,
      it->second);
  BOLT_CHECK_LE(
      value.value(),
      std::numeric_limits<int>::max(),
      "Invalid HDFS open file option '{}': '{}'. Exceeds int range.",
      key,
      it->second);
  return static_cast<int>(value.value());
}

inline HdfsOpenFileOptions getHdfsOpenFileOptions(const FileOptions& options) {
  HdfsOpenFileOptions hdfsOptions;
  hdfsOptions.bufferSize =
      parseHdfsOpenFileIntOption(options, HdfsOpenFileOptions::kBufferSize);
  hdfsOptions.blockSize =
      parseHdfsOpenFileIntOption(options, HdfsOpenFileOptions::kBlockSize);

  const auto replication =
      parseHdfsOpenFileIntOption(options, HdfsOpenFileOptions::kReplication);
  BOLT_CHECK_LE(
      replication,
      std::numeric_limits<short>::max(),
      "Invalid HDFS open file option '{}': '{}'. Exceeds short range.",
      HdfsOpenFileOptions::kReplication,
      replication);
  hdfsOptions.replication = static_cast<short>(replication);
  return hdfsOptions;
}

inline void setHdfsOpenFileOptionsFromConfig(
    const config::ConfigBase* config,
    FileOptions& options) {
  if (config == nullptr) {
    return;
  }

  for (const auto* key :
       {HdfsOpenFileOptions::kBufferSize,
        HdfsOpenFileOptions::kReplication,
        HdfsOpenFileOptions::kBlockSize}) {
    auto value = config->get<std::string>(key);
    if (value.hasValue()) {
      options.values[key] = value.value();
    }
  }
}

} // namespace bytedance::bolt::filesystems
