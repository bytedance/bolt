#pragma once

#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

struct SpillStoreConfig {
  FileSegmentAllocatorConfig fileAllocatorConfig;
  compress::CompressionConfig compressionConfig;
};

} // namespace bytedance::bolt::memory::bm
