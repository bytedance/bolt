#pragma once

#include <cstdint>

#include "bolt/common/memory/bm/io/IoBuffer.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

namespace bytedance::bolt::memory::bm {

struct IoRequest {
  IoOpcode opcode{IoOpcode::Read};
  IoPriority priority{IoPriority::Medium};
  int fd{-1};
  uint64_t fileOffset{0};
  IoBuffer buffer;
};

} // namespace bytedance::bolt::memory::bm
