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

#include "bolt/dwio/lance/NativeLanceDecodedPageCache.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

VectorPtr NativeLanceDecodedPageCache::get(Key key) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = entries_.find(key);
  if (it == entries_.end()) {
    ++misses_;
    return nullptr;
  }
  ++hits_;
  lru_.splice(lru_.begin(), lru_, it->second.lruPosition);
  return it->second.vector;
}

VectorPtr NativeLanceDecodedPageCache::getOrLoad(
    Key key,
    const std::function<VectorPtr()>& load) {
  std::shared_ptr<Pending> pending;
  bool loader = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto cached = entries_.find(key);
    if (cached != entries_.end()) {
      ++hits_;
      lru_.splice(lru_.begin(), lru_, cached->second.lruPosition);
      return cached->second.vector;
    }
    ++misses_;
    const auto [it, inserted] =
        pending_.try_emplace(key, std::make_shared<Pending>());
    pending = it->second;
    loader = inserted;
    if (loader) {
      ++loads_;
    } else {
      ++waits_;
    }
  }
  if (!loader) {
    std::unique_lock<std::mutex> lock(pending->mutex);
    pending->ready.wait(lock, [&] { return pending->done; });
    if (pending->error != nullptr) {
      std::rethrow_exception(pending->error);
    }
    return pending->vector;
  }

  try {
    auto vector = load();
    put(key, vector);
    {
      std::lock_guard<std::mutex> lock(pending->mutex);
      pending->vector = vector;
      pending->done = true;
    }
    pending->ready.notify_all();
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.erase(key);
    return vector;
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(pending->mutex);
      pending->error = std::current_exception();
      pending->done = true;
    }
    pending->ready.notify_all();
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.erase(key);
    throw;
  }
}

void NativeLanceDecodedPageCache::put(Key key, VectorPtr vector) {
  BOLT_CHECK_NOT_NULL(vector);
  const auto retainedBytes = vector->retainedSize();
  if (retainedBytes > maxBytes_) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (const auto existing = entries_.find(key); existing != entries_.end()) {
    lru_.splice(lru_.begin(), lru_, existing->second.lruPosition);
    return;
  }
  lru_.push_front(key);
  sizeBytes_ += retainedBytes;
  entries_.emplace(
      key,
      Entry{
          .vector = std::move(vector),
          .retainedBytes = retainedBytes,
          .lruPosition = lru_.begin()});
  evictLocked();
}

bool NativeLanceDecodedPageCache::contains(Key key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.contains(key);
}

uint64_t NativeLanceDecodedPageCache::sizeBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return sizeBytes_;
}

NativeLanceDecodedPageCache::Stats NativeLanceDecodedPageCache::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {
      .hits = hits_,
      .misses = misses_,
      .loads = loads_,
      .waits = waits_,
      .evictions = evictions_,
      .sizeBytes = sizeBytes_,
  };
}

void NativeLanceDecodedPageCache::evictLocked() {
  while (sizeBytes_ > maxBytes_ && !lru_.empty()) {
    const auto key = lru_.back();
    const auto it = entries_.find(key);
    BOLT_CHECK(it != entries_.end());
    sizeBytes_ -= it->second.retainedBytes;
    entries_.erase(it);
    lru_.pop_back();
    ++evictions_;
  }
}

} // namespace bytedance::bolt::lance::reader
