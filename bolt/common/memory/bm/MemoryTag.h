#pragma once

#include <cstddef>
#include <cstdint>

namespace bytedance::bolt::memory::bm {

enum class MemoryTag : uint8_t {
  kUnknown,
  kHashBuild,
  kAggregation,
  kSort,
  kWindow,
  kExchange,
  kTesting,
  kCount,
};

constexpr size_t kMemoryTagCount = static_cast<size_t>(MemoryTag::kCount);

inline const char* toString(MemoryTag tag) {
  switch (tag) {
    case MemoryTag::kUnknown:
      return "Unknown";
    case MemoryTag::kHashBuild:
      return "HashBuild";
    case MemoryTag::kAggregation:
      return "Aggregation";
    case MemoryTag::kSort:
      return "Sort";
    case MemoryTag::kWindow:
      return "Window";
    case MemoryTag::kExchange:
      return "Exchange";
    case MemoryTag::kTesting:
      return "Testing";
    case MemoryTag::kCount:
      return "Unknown";
  }
  return "Unknown";
}

} // namespace bytedance::bolt::memory::bm
