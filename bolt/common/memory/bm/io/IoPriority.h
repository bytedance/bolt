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

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm {

enum class IoOpcode : uint8_t {
  Read,
  Write,
};

enum class IoPriority : uint8_t {
  High = 0,
  Medium = 1,
  Low = 2,
  Count,
};

constexpr size_t kIoPriorityCount = static_cast<size_t>(IoPriority::Count);

inline size_t priorityIndex(IoPriority priority) {
  return static_cast<size_t>(priority);
}

inline bool validOpcode(IoOpcode opcode) {
  return opcode == IoOpcode::Read || opcode == IoOpcode::Write;
}

inline bool validPriority(IoPriority priority) {
  return priorityIndex(priority) < kIoPriorityCount;
}

} // namespace bytedance::bolt::memory::bm
