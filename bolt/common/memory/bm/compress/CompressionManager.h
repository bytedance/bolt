#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <cstdint>
#include <memory>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

struct CompressionRecordResult {
  IoBuffer record;
  uint64_t rawSize{0};
  uint64_t physicalSize{0};
  CompressionKind storedKind{CompressionKind::kNone};
  uint64_t compressionTimeUs{0};
  bool compressed{false};
};

class CompressionManager {
 public:
  explicit CompressionManager(CompressionConfig config);
  ~CompressionManager();

  CompressionManager(const CompressionManager&) = delete;
  CompressionManager& operator=(const CompressionManager&) = delete;

  CompressionRecordResult BuildSpillRecord(std::span<const char> payload);

  IoBuffer DecodeSpillRecord(
      std::span<const char> record,
      uint64_t expectedRawSize,
      MemoryPool* outputPool,
      uint64_t* decompressionTimeUs);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace bytedance::bolt::memory::bm::compress
