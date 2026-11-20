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

#include "bolt/shuffle/sparksql/common/ShuffleBlockFormat.h"

#include <folly/lang/Bits.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

/// Serializes fields explicitly so native struct padding never enters the wire.
struct RowBlockHeader {
  ShuffleBlockKind kind;
  uint32_t rowCount;
  uint64_t decodedBytes;
  uint64_t storedBytes;
  RunLayout layout;

  void writeTo(char* out) const {
    out[0] = static_cast<char>(kind);
    folly::storeUnaligned<uint32_t>(out + 1, rowCount);
    folly::storeUnaligned<uint64_t>(out + 5, decodedBytes);
    folly::storeUnaligned<uint64_t>(out + 13, storedBytes);
    out[21] = static_cast<char>(layout);
  }
};

} // namespace

uint64_t currentTimeNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

char* resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool) {
  if (buffer == nullptr) {
    if (size == 0) {
      return nullptr;
    }
    buffer = AlignedBuffer::allocate<char>(std::max<size_t>(64, size), pool);
  } else if (size > buffer->capacity()) {
    AlignedBuffer::reallocate<char>(
        &buffer,
        std::max(size, checkedMultiply<size_t>(2, buffer->capacity())));
  }
  buffer->setSize(size);
  return buffer->asMutable<char>();
}

char* resetBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool) {
  if (buffer == nullptr || size > buffer->capacity()) {
    buffer.reset();
    if (size == 0) {
      return nullptr;
    }
    buffer = AlignedBuffer::allocate<char>(size, pool);
  }
  buffer->setSize(size);
  return buffer->asMutable<char>();
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
  static_assert(folly::kIsLittleEndian, "little-endian wire format assumed");
  appendBytes(out, &value, sizeof(value), pool);
}

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool) {
  static_assert(folly::kIsLittleEndian, "little-endian wire format assumed");
  appendBytes(out, &value, sizeof(value), pool);
}

void buildRowBlock(
    ShuffleBlockKind kind,
    const uint8_t* const* rows,
    size_t numRows,
    uint64_t rawBytes,
    Codec* codec,
    BufferPtr& block,
    BufferPtr& compressedBuffer,
    memory::MemoryPool* pool,
    uint64_t& compressTimeNs) {
  BOLT_CHECK_LE(
      numRows,
      std::numeric_limits<uint32_t>::max(),
      "too many rows for one row block");
  resetBuffer(block, checkedPlus<size_t>(kRowBlockHeaderBytes, rawBytes), pool);
  char* body = block->asMutable<char>() + kRowBlockHeaderBytes;
  uint64_t offset = 0;
  for (size_t i = 0; i < numRows; ++i) {
    const uint64_t bytes = framedRowBytes(rows[i]);
    ::memcpy(body + offset, rows[i], bytes);
    offset += bytes;
  }

  // Compressed when the codec shrinks the body, else stored as is.
  RowBlockHeader header{
      kind,
      static_cast<uint32_t>(numRows),
      rawBytes,
      rawBytes,
      RunLayout::kCombinedUncompressed};
  if (codec != nullptr && rawBytes >= kMinCompressBytes) {
    const uint64_t start = currentTimeNs();
    resetBuffer(
        compressedBuffer,
        codec->maxCompressedLen(rawBytes),
        memory::spillMemoryPool());
    const int64_t written = codec->compress(
        reinterpret_cast<const uint8_t*>(body),
        static_cast<int64_t>(rawBytes),
        compressedBuffer->asMutable<uint8_t>(),
        static_cast<int64_t>(compressedBuffer->size()));
    compressTimeNs += currentTimeNs() - start;
    if (written > 0 && static_cast<uint64_t>(written) < rawBytes) {
      header.storedBytes = static_cast<uint64_t>(written);
      header.layout = RunLayout::kCombined;
      ::memcpy(body, compressedBuffer->as<char>(), header.storedBytes);
    }
  }

  header.writeTo(block->asMutable<char>());
  block->setSize(kRowBlockHeaderBytes + header.storedBytes);
}

} // namespace bytedance::bolt::shuffle::sparksql
