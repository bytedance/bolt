#pragma once

#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

struct SpillStoreConfig {
  FileBlockAllocatorConfig fileAllocatorConfig;
  compress::CompressionConfig compressionConfig;
};

} // namespace bytedance::bolt::memory::bm
