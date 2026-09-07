/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
