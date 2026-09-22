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

#pragma once

#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <list>
#include <mutex>
#include <unordered_map>

#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

/// Reader-scoped LRU of fully decoded pages. All decoders created for a file,
/// including prefetch clones, share this cache.
class NativeLanceDecodedPageCache {
 public:
  static constexpr uint64_t kDefaultMaxBytes = 512UL << 20;

  struct Key {
    uint32_t physicalColumnIndex;
    int32_t pageIndex;

    bool operator==(const Key& other) const {
      return physicalColumnIndex == other.physicalColumnIndex &&
          pageIndex == other.pageIndex;
    }
  };

  struct KeyHash {
    size_t operator()(Key key) const {
      return std::hash<uint32_t>{}(key.physicalColumnIndex) ^
          (std::hash<int32_t>{}(key.pageIndex) << 1);
    }
  };

  explicit NativeLanceDecodedPageCache(uint64_t maxBytes)
      : maxBytes_(maxBytes) {}

  VectorPtr get(Key key);

  VectorPtr getOrLoad(Key key, const std::function<VectorPtr()>& load);

  void put(Key key, VectorPtr vector);

  bool contains(Key key) const;

  uint64_t sizeBytes() const;

 private:
  struct Entry {
    VectorPtr vector;
    uint64_t retainedBytes;
    std::list<Key>::iterator lruPosition;
  };

  struct Pending {
    std::mutex mutex;
    std::condition_variable ready;
    bool done{false};
    VectorPtr vector;
    std::exception_ptr error;
  };

  void evictLocked();

  const uint64_t maxBytes_;
  mutable std::mutex mutex_;
  std::list<Key> lru_;
  std::unordered_map<Key, Entry, KeyHash> entries_;
  std::unordered_map<Key, std::shared_ptr<Pending>, KeyHash> pending_;
  uint64_t sizeBytes_{0};
};

} // namespace bytedance::bolt::lance::reader
