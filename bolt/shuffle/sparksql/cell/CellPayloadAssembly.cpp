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

#include "bolt/shuffle/sparksql/cell/CellPayloadAssembly.h"

#include <algorithm>
#include <chrono>
#include <cstring>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

uint64_t currentTimeNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool) {
  if (!buffer) {
    if (size == 0) {
      return;
    }
    buffer = AlignedBuffer::allocate<char>(std::max<size_t>(64, size), pool);
  } else if (size > buffer->capacity()) {
    AlignedBuffer::reallocate<char>(
        &buffer,
        std::max(size, checkedMultiply<size_t>(2, buffer->capacity())));
  }
  buffer->setSize(size);
}

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool) {
  if (bytes == 0) {
    return;
  }
  const size_t at = out ? out->size() : 0;
  resizeBuffer(out, checkedPlus(at, bytes), pool);
  ::memcpy(out->asMutable<char>() + at, data, bytes);
}

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool) {
  appendBytes(out, &value, sizeof(value), pool);
}

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool) {
  appendBytes(out, &value, sizeof(value), pool);
}

void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool) {
  const uint32_t numColumns = in.layout->numWireColumns();
  if (numColumns == 0) {
    return;
  }
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const size_t tagStart = out ? out->size() : 0;
  resizeBuffer(out, checkedPlus<size_t>(tagStart, tagBytes), pool);
  auto* tags = out->asMutable<uint8_t>() + tagStart;
  ::memset(tags, 0, tagBytes);
  const uint32_t bitmapBytes = (rows + 7) / 8;
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto summary = in.nulls->summarize(pid, col, rows);
    setNullTag(tags, col, summary.tag);
  }
  // Bitmaps follow the tags for RAW_NULL columns, in column order. The tag
  // byte array may move as `out` grows, so tags were fully written first.
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (getNullTag(out->as<uint8_t>() + tagStart, col) == NullTag::kRawNull) {
      const size_t at = out->size();
      resizeBuffer(out, checkedPlus<size_t>(at, bitmapBytes), pool);
      in.nulls->emitBitmap(pid, col, rows, out->asMutable<uint8_t>() + at);
    }
  }
}

void appendPayloadPrefix(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t runCount,
    BufferPtr& out,
    memory::MemoryPool* pool) {
  const uint32_t rows = in.rowCounts[pid];
  appendLe32(out, rows, pool);
  appendLe32(out, runCount, pool);
  appendLe64(out, in.variableBytes[pid], pool);
  const size_t nullSizeAt = out->size();
  appendLe32(out, 0, pool); // patched below
  appendLe32(out, 0, pool); // uncompressed
  const size_t nullBodyAt = out->size();
  buildNullBody(in, pid, rows, out, pool);
  const uint32_t nullLength = static_cast<uint32_t>(out->size() - nullBodyAt);
  ::memcpy(out->asMutable<char>() + nullSizeAt, &nullLength, 4);
  appendBytes(
      out, in.encodingTags, (in.layout->numWireColumns() + 7) / 8, pool);
}

uint64_t residentBytes(const CellWindowInput& in, uint32_t pid) {
  uint64_t total = 0;
  for (uint32_t s = 0; s < in.layout->numStreams(); ++s) {
    total += in.cells->bytes(pid, s);
  }
  return total;
}

void gatherResident(
    const CellWindowInput& in,
    uint32_t pid,
    char* out,
    uint64_t* streamSizes) {
  size_t offset = 0;
  for (uint32_t s = 0; s < in.layout->numStreams(); ++s) {
    streamSizes[s] = in.cells->bytes(pid, s);
    in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
      ::memcpy(out + offset, data, bytes);
      offset += bytes;
    });
  }
}

void writeRunHeader(
    char* out,
    RunLayout layout,
    uint64_t stored,
    const uint64_t* decodedSizes,
    uint32_t numStreams) {
  out[0] = static_cast<char>(layout);
  ::memcpy(out + 1, &stored, 8);
  ::memcpy(out + 9, decodedSizes, 8ull * numStreams);
}

RunLayout maybeCompressRun(
    Codec* codec,
    const char* data,
    uint64_t dataBytes,
    BufferPtr& scratch,
    memory::MemoryPool* pool,
    const char*& body,
    uint64_t& stored,
    uint64_t& compressTimeNs) {
  body = data;
  stored = dataBytes;
  if (codec != nullptr && dataBytes >= kMinCompressRunBytes) {
    const uint64_t start = currentTimeNs();
    resizeBuffer(scratch, codec->maxCompressedLen(dataBytes), pool);
    const int64_t written = codec->compress(
        reinterpret_cast<const uint8_t*>(data),
        static_cast<int64_t>(dataBytes),
        scratch->asMutable<uint8_t>(),
        static_cast<int64_t>(scratch->size()));
    compressTimeNs += currentTimeNs() - start;
    // Spec section 5: fall back to the stored form when compression does
    // not pay.
    if (written > 0 && static_cast<uint64_t>(written) < dataBytes) {
      body = scratch->as<char>();
      stored = static_cast<uint64_t>(written);
      return RunLayout::kCombined;
    }
  }
  return RunLayout::kCombinedStored;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
