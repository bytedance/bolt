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

/*
 * Copyright (c) ByteDance, Inc. and its affiliates.
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

#include <list>
#include <optional>
#include <unordered_map>

namespace bytedance::bolt::jit {

/// A cache which evicts the least recently used item when it is full
template <typename Key, typename Value, typename EvictPolicy>
class LRUCache {
 public:
  using key_type = Key;
  using value_type = Value;
  using list_type = std::list<key_type>;
  using map_type = std::unordered_map<
      key_type,
      std::pair<value_type, typename list_type::iterator>>;

  LRUCache() = default;
  ~LRUCache() = default;

  size_t size() const noexcept {
    return map_.size();
  }

  bool empty() const noexcept {
    return map_.empty();
  }

  bool contains(const key_type& key) const {
    return map_.find(key) != map_.end();
  }

  void insert(const key_type& key, const value_type& value) {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full

      while (!empty() && evictPolicy_()) {
        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      list_.push_front(key);
      map_[key] = std::make_pair(value, list_.begin());
    }
  }

  std::optional<value_type> get(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // value not in cache
      return std::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator j = i->second.second;
    if (j != list_.begin()) {
      // move item to the front of the most recently used list
      list_.erase(j);
      list_.push_front(key);

      // update iterator in map
      j = list_.begin();
      const value_type& value = i->second.first;
      map_[key] = std::make_pair(value, j);

      // return the value
      return value;
    }

    // the item is already at the front of the most recently
    // used list so just return it
    return i->second.first;
  }

  void clear() {
    map_.clear();
    list_.clear();
  }

 private:
  void evict() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --list_.end();
    map_.erase(*i);
    list_.erase(i);
  }

  map_type map_;
  list_type list_;
  EvictPolicy evictPolicy_;
};

} // namespace bytedance::bolt::jit
