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
