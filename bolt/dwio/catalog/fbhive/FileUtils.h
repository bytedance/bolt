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

#include <algorithm>
#include <functional>
#include <string>
#include <vector>
namespace bytedance {
namespace bolt {
namespace dwio {
namespace catalog {
namespace fbhive {

class FileUtils {
 public:
  using EncodeFunction = std::function<std::string(const std::string&)>;

  /// Converts the path name to be hive metastore compliant, will do
  /// url-encoding when needed.
  static std::string escapePathName(const std::string& data);

  /// Converts the hive-metastore-compliant path name back to its original path
  /// name.
  static std::string unescapePathName(const std::string& data);

  /// Creates the partition directory path from the list of partition key/value
  /// pairs, will do url-encoding when needed.
  static std::string makePartName(
      const std::vector<std::pair<std::string, std::string>>& entries,
      bool partitionPathAsLowerCase);

  static std::string makePartName(
      const std::vector<std::pair<std::string, std::string>>& entries,
      bool partitionPathAsLowerCase,
      bool useDefaultPartitionValue,
      const EncodeFunction& encodeFunc);

  /// Converts the hive-metastore-compliant path name back to the corresponding
  /// partition key/value pairs.
  static std::vector<std::pair<std::string, std::string>> parsePartKeyValues(
      const std::string& partName);

  /// Converts a path name to a hive-metastore-compliant path name.
  static std::string extractPartitionName(const std::string& filePath);

  inline static const std::string kDefaultPartitionValue =
      "__HIVE_DEFAULT_PARTITION__";
};

} // namespace fbhive
} // namespace catalog
} // namespace dwio
} // namespace bolt
} // namespace bytedance
