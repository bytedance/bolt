#include "bolt/common/memory/bm/compress/CompressionRecord.h"

#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include <cstring>

namespace bytedance::bolt::memory::bm::compress {

size_t SpillRecordHeaderSize() {
  return sizeof(SpillRecordHeader);
}

IoBuffer AllocateSpillRecord(size_t bodyCapacity) {
  return IoBuffer::allocateFromMalloc(SpillRecordHeaderSize() + bodyCapacity);
}

char* SpillRecordBody(IoBuffer& record) {
  return record.data() + SpillRecordHeaderSize();
}

void FinalizeSpillRecord(
    IoBuffer& record,
    CompressionKind storedKind,
    uint64_t rawSize,
    uint64_t storedSize) {
  SpillRecordHeader header;
  header.compressionKind = static_cast<uint32_t>(storedKind);
  header.rawSize = rawSize;
  header.storedSize = storedSize;

  const auto encoded = EncodeSpillRecordHeader(header);
  std::memcpy(record.data(), encoded.data(), encoded.size());
  record.setLength(SpillRecordHeaderSize() + storedSize);
}

IoBuffer AllocateDecodedPayload(MemoryPool* outputPool, uint64_t rawSize) {
  if (outputPool == nullptr) {
    return IoBuffer::allocateFromMalloc(rawSize);
  }
  return IoBuffer::allocateFromPool(outputPool, rawSize);
}

std::span<const char> StoredPayloadSpan(
    std::span<const char> record,
    size_t headerSize,
    uint64_t storedSize) {
  return record.subspan(headerSize, storedSize);
}

} // namespace bytedance::bolt::memory::bm::compress
