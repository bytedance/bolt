#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct IoBuffer {
  std::unique_ptr<char[]> data;
  size_t size{0};
  size_t offset{0};
  size_t length{0};
};

struct IoRequest {
  IoOpcode opcode{IoOpcode::Read};
  IoPriority priority{IoPriority::Medium};
  int fd{-1};
  uint64_t fileOffset{0};
  IoBuffer buffer;
};

} // namespace bytedance::bolt::memory::bm
