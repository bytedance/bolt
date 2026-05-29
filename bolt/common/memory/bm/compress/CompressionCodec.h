#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <cstdint>
#include <memory>

namespace bytedance::bolt::memory::bm::compress {

struct CompressResult {
  IoBuffer buffer;
  uint64_t rawSize{0};
  uint64_t storedSize{0};
  CompressionKind storedKind{CompressionKind::kNone};
  uint64_t compressionTimeUs{0};
  bool compressed{false};
};

class CompressionCodec {
 public:
  CompressionCodec();
  ~CompressionCodec();

  CompressionCodec(const CompressionCodec&) = delete;
  CompressionCodec& operator=(const CompressionCodec&) = delete;

  CompressResult TryCompress(
      IoBuffer payload,
      const CompressionConfig& config,
      MemoryPool* pool);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

CompressResult TryCompress(
    IoBuffer payload,
    const CompressionConfig& config,
    MemoryPool* pool);

IoBuffer Decompress(
    IoBuffer storedPayload,
    uint64_t rawSize,
    uint64_t storedSize,
    CompressionKind storedKind,
    MemoryPool* pool,
    uint64_t* decompressionTimeUs);

} // namespace bytedance::bolt::memory::bm::compress
