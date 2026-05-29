#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

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

size_t allocateSizeBytes(AllocateSize size);
const char* toString(AllocateSize size);

} // namespace bytedance::bolt::memory::bm
