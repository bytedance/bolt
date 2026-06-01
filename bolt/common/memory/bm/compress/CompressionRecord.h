#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace bytedance::bolt::memory::bm::compress {

size_t SpillRecordHeaderSize();

IoBuffer AllocateSpillRecord(size_t bodyCapacity);

char* SpillRecordBody(IoBuffer& record);

void FinalizeSpillRecord(
    IoBuffer& record,
    CompressionKind storedKind,
    uint64_t rawSize,
    uint64_t storedSize);

IoBuffer AllocateDecodedPayload(MemoryPool* outputPool, uint64_t rawSize);

std::span<const char> StoredPayloadSpan(
    std::span<const char> record,
    size_t headerSize,
    uint64_t storedSize);

} // namespace bytedance::bolt::memory::bm::compress
