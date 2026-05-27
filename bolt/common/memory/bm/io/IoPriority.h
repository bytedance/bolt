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
