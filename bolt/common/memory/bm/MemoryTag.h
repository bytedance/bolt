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
};

constexpr size_t kMemoryTagCount = 7;

const char* toString(MemoryTag tag);

} // namespace bytedance::bolt::memory::bm
