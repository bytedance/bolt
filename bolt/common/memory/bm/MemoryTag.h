#pragma once

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

const char* toString(MemoryTag tag);

} // namespace bytedance::bolt::memory::bm
