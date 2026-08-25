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

#include "bolt/common/base/SparkCompatibility.h"

#include <re2/re2.h>

#include "bolt/common/config/Config.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/type/tz/TimeZoneMap.h"
namespace bytedance::bolt::core {

QueryConfig::QueryConfig(
    const std::unordered_map<std::string, std::string>& values)
    : config_{std::make_unique<config::ConfigBase>(
          std::unordered_map<std::string, std::string>(values))} {
  VLOG(1) << "Morsel-driven Bolt enabled: " << this->morselDrivenEnabled()
          << " (morselSize=" << this->morselSize()
          << ", queueSize=" << this->morselDrivenPrimedQueueSize() << ")";
}

QueryConfig::QueryConfig(std::unordered_map<std::string, std::string>&& values)
    : config_{std::make_unique<config::ConfigBase>(std::move(values))} {
  VLOG(1) << "Morsel-driven Bolt enabled: " << this->morselDrivenEnabled()
          << " (morselSize=" << this->morselSize()
          << ", queueSize=" << this->morselDrivenPrimedQueueSize() << ")";
}

uint64_t QueryConfig::maxSpillBytes() const {
  static constexpr uint64_t kDefault =
      ::bytedance::bolt::kSparkCompatible ? 0UL : 100UL << 30;
  return get<uint64_t>(kMaxSpillBytes, kDefault);
}

void QueryConfig::testingOverrideConfigUnsafe(
    std::unordered_map<std::string, std::string>&& values) {
  config_ = std::make_unique<config::ConfigBase>(std::move(values));
  VLOG(1) << "Morsel-driven Bolt enabled: " << this->morselDrivenEnabled()
          << " (morselSize=" << this->morselSize()
          << ", queueSize=" << this->morselDrivenPrimedQueueSize() << ")";
}

std::unordered_map<std::string, std::string> QueryConfig::rawConfigsCopy()
    const {
  return config_->rawConfigsCopy();
}

} // namespace bytedance::bolt::core
