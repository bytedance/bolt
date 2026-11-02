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

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include <folly/container/EvictingCacheMap.h>

#include "bolt/type/StringView.h"

namespace bytedance::bolt::functions::regex {

// Own keys and pin patterns during matching, including across cache eviction.
// Large, infrequently reused patterns do not retain unbounded key storage.
template <typename Pattern>
class RegexCache {
 public:
  template <typename Compile>
  std::shared_ptr<const Pattern> get(StringView pattern, Compile&& compile)
      const {
    if (pattern.size() > 4096) {
      return compile(pattern);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key(pattern.data(), pattern.size());
    auto it = patterns_.find(key);
    if (it != patterns_.end()) {
      return it->second;
    }
    auto compiled = compile(pattern);
    patterns_.insert(std::move(key), compiled);
    return compiled;
  }

 private:
  mutable std::mutex mutex_;
  mutable folly::EvictingCacheMap<std::string, std::shared_ptr<const Pattern>>
      patterns_{40};
};

struct NoRegexCache {};

} // namespace bytedance::bolt::functions::regex
