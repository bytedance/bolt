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

#include <array>
#include <cstddef>
#include <cstdint>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

enum class AllocateSize : uint8_t {
  kSmall = 0,
  kMedium = 1,
  kLarge = 2,
};

constexpr std::array<size_t, 3> kAllocateSizeBytes{
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024};

inline size_t allocateSizeBytes(AllocateSize size) {
  const auto index = static_cast<size_t>(size);
  BOLT_CHECK_LT(index, kAllocateSizeBytes.size());
  return kAllocateSizeBytes[index];
}

inline const char* toString(AllocateSize size) {
  switch (size) {
    case AllocateSize::kSmall:
      return "small";
    case AllocateSize::kMedium:
      return "medium";
    case AllocateSize::kLarge:
      return "large";
  }
  BOLT_FAIL("unknown BM allocate size {}", static_cast<int>(size));
}

} // namespace bytedance::bolt::memory::bm
