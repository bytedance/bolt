#pragma once

#include "bolt/common/memory/bm/compress/CompressionConfig.h"

#include <array>
#include <cstdint>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

constexpr uint32_t kSpillRecordMagic = 0x424d5350; // BMSP
constexpr uint16_t kSpillRecordVersion = 1;

struct SpillRecordHeader {
  uint32_t magic{kSpillRecordMagic};
  uint16_t version{kSpillRecordVersion};
  uint16_t headerSize{sizeof(SpillRecordHeader)};
  uint32_t compressionKind{static_cast<uint32_t>(CompressionKind::kNone)};
  uint32_t reserved{0};
  uint64_t rawSize{0};
  uint64_t storedSize{0};
};

std::array<char, sizeof(SpillRecordHeader)> EncodeSpillRecordHeader(
    const SpillRecordHeader& header);

SpillRecordHeader DecodeSpillRecordHeader(
    const char* data,
    size_t size,
    uint64_t expectedRawSize);

} // namespace bytedance::bolt::memory::bm::compress
