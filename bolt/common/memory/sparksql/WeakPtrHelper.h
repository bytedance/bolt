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

#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::memory::sparksql {

struct TransparentWeakPtrComparator {
  template <typename T>
  bool operator()(const std::weak_ptr<T>& lhs, const std::weak_ptr<T>& rhs)
      const {
    return lhs.owner_before(rhs);
  }
};

template <typename T>
FOLLY_ALWAYS_INLINE std::shared_ptr<T> lock_or_throw(std::weak_ptr<T> weak) {
  if (auto shared = weak.lock()) {
    return shared;
  }
  BOLT_FAIL("weak ptr is invalid for type: " + std::string(typeid(T).name()));
}

template <typename T, typename Func>
FOLLY_ALWAYS_INLINE void with_locked(std::weak_ptr<T> weak, Func&& f) {
  if (auto shared = weak.lock()) {
    // if lock success, call func
    std::forward<Func>(f)(shared);
  }
}

} // namespace bytedance::bolt::memory::sparksql
