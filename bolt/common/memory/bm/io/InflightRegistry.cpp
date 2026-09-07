/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/common/memory/bm/io/InflightRegistry.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

void InflightRegistry::add(uint64_t requestId, InflightIoRequest request) {
  requests_.emplace(requestId, std::move(request));
}

std::optional<InflightIoRequest> InflightRegistry::take(uint64_t requestId) {
  auto it = requests_.find(requestId);
  if (it == requests_.end()) {
    return std::nullopt;
  }

  auto request = std::move(it->second);
  requests_.erase(it);
  return request;
}

bool InflightRegistry::empty() const {
  return requests_.empty();
}

size_t InflightRegistry::size() const {
  return requests_.size();
}

} // namespace bytedance::bolt::memory::bm
