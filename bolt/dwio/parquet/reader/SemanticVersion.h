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

#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

#include <re2/re2.h>
#include <optional>
#include <string>

namespace bytedance::bolt::parquet {

class SemanticVersion {
 public:
  SemanticVersion();

  SemanticVersion(int major, int minor, int patch);

  SemanticVersion(std::string application, int major, int minor, int patch);

  static std::optional<SemanticVersion> parse(const std::string& input);

  bool shouldIgnoreStatistics(thrift::Type::type type) const;

  std::string toString() const;

  bool operator==(const SemanticVersion& other) const;

  bool operator<(const SemanticVersion& other) const;

 private:
  std::string application_;
  int majorVersion_;
  int minorVersion_;
  int patchVersion_;

  static const re2::RE2 pattern_;
};

} // namespace bytedance::bolt::parquet
