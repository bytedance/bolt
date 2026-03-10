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

#include "bolt/connectors/hive/storage_adapters/abfs/AbfsUtil.h"
#include "bolt/common/config/Config.h"
#include "bolt/connectors/hive/storage_adapters/abfs/AbfsPath.h"

namespace bytedance::bolt::filesystems {

std::vector<CacheKey> extractCacheKeyFromConfig(
    const config::ConfigBase& config) {
  std::vector<CacheKey> cacheKeys;
  constexpr std::string_view authTypePrefix{kAzureAccountAuthType};
  for (const auto& [key, value] : config.rawConfigs()) {
    if (key.find(authTypePrefix) == 0) {
      // Extract the accountName after "fs.azure.account.auth.type.".
      auto remaining = std::string_view(key).substr(authTypePrefix.size() + 1);
      auto dot = remaining.find(".");
      BOLT_USER_CHECK_NE(
          dot,
          std::string_view::npos,
          "Invalid Azure account auth type key: {}",
          key);
      cacheKeys.emplace_back(CacheKey{remaining.substr(0, dot), value});
    }
  }
  return cacheKeys;
}

} // namespace bytedance::bolt::filesystems
