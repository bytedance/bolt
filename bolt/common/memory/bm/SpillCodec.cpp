#include "bolt/common/memory/bm/SpillCodec.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

SpillCodec::SpillCodec(compress::CompressionConfig config)
    : compression_(std::move(config)) {}

compress::CompressionRecordResult SpillCodec::Build(std::span<const char> raw) {
  return compression_.BuildSpillRecord(raw);
}

IoBuffer SpillCodec::Decode(
    std::span<const char> record,
    size_t expectedRawSize,
    MemoryPool* pool,
    uint64_t* decompressionTimeUs) {
  return compression_.DecodeSpillRecord(
      record, expectedRawSize, pool, decompressionTimeUs);
}

} // namespace bytedance::bolt::memory::bm
