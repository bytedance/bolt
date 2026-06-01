#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/compress/CompressionManager.h"
#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace bytedance::bolt::memory::bm {

class SpillCodec {
 public:
  explicit SpillCodec(compress::CompressionConfig config);

  compress::CompressionRecordResult Build(std::span<const char> raw);
  IoBuffer Decode(
      std::span<const char> record,
      size_t expectedRawSize,
      MemoryPool* pool,
      uint64_t* decompressionTimeUs);

 private:
  compress::CompressionManager compression_;
};

} // namespace bytedance::bolt::memory::bm
