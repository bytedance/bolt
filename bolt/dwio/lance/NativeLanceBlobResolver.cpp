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

#include "bolt/dwio/lance/NativeLanceBlobResolver.h"

#include "bolt/common/base/Exceptions.h"
#include "folly/hash/Hash.h"

namespace bytedance::bolt::lance::reader {

CachingNativeLanceBlobResolver::CachingNativeLanceBlobResolver(
    std::shared_ptr<const NativeLanceBlobResolver> delegate,
    size_t maxEntries)
    : delegate_(std::move(delegate)), maxEntries_(maxEntries) {
  BOLT_CHECK_NOT_NULL(delegate_);
  BOLT_CHECK_GT(maxEntries_, 0);
}

size_t CachingNativeLanceBlobResolver::KeyHash::operator()(
    const Key& key) const {
  return folly::hash::hash_combine(
      static_cast<uint8_t>(key.kind), key.sourceDataFile, key.blobId, key.uri);
}

std::unique_ptr<dwio::common::BufferedInput>
CachingNativeLanceBlobResolver::resolve(
    const Request& request,
    memory::MemoryPool& pool) const {
  Key key{request.kind, request.sourceDataFile, request.blobId, request.uri};
  std::lock_guard<std::mutex> lock(mutex_);
  if (const auto cached = entries_.find(key); cached != entries_.end()) {
    lru_.splice(lru_.begin(), lru_, cached->second.lruPosition);
    return cached->second.input->clone();
  }

  auto resolved = delegate_->resolve(request, pool);
  BOLT_CHECK_NOT_NULL(resolved);
  auto shared =
      std::shared_ptr<dwio::common::BufferedInput>(std::move(resolved));
  auto clone = shared->clone();
  lru_.push_front(std::move(key));
  entries_.emplace(
      lru_.front(),
      Entry{.input = std::move(shared), .lruPosition = lru_.begin()});
  while (entries_.size() > maxEntries_) {
    entries_.erase(lru_.back());
    lru_.pop_back();
  }
  return clone;
}

} // namespace bytedance::bolt::lance::reader
