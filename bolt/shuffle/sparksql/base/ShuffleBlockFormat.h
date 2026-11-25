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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/MemoryCopy.h"
#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::shuffle::sparksql {

class Codec;

/// Most rows one block or payload holds. Keeps u32 row fields exact and a
/// single block decodable as one vector.
inline constexpr uint32_t kMaxBlockRows = (1u << 31) - 1;

/// Bytes below which a frame is stored rather than compressed: the codec's
/// frame would not pay for itself.
inline constexpr uint64_t kMinCompressBytes = 256;

/// What a block of a block-based shuffle stream holds: every block on the
/// wire starts with this byte. Values start at 0x80 so they never collide with
/// legacy V1/V2 Payload::Type (1..3) or RowVectorLayout (9..12); a mismatched
/// legacy reader therefore fails immediately instead of misinterpreting bytes.
enum class ShuffleBlockKind : uint8_t {
  kCellPayload = 0x80,
  kCompositeRows = 0x81,
  // TODO: Wire up row-based block shuffle writer/reader using kRows.
  kRows = 0x82,
};

inline constexpr uint32_t format_as(ShuffleBlockKind kind) {
  return static_cast<uint32_t>(kind);
}

/// Human-readable label for a block kind (used in error diagnostics).
inline constexpr std::string_view shuffleBlockKindName(ShuffleBlockKind kind) {
  switch (kind) {
    case ShuffleBlockKind::kCellPayload:
      return "cell payload";
    case ShuffleBlockKind::kCompositeRows:
      return "composite";
    case ShuffleBlockKind::kRows:
      return "row";
  }
  return "unknown";
}

/// A run's or block's compression layout: all streams/body compressed as one
/// buffer, each stream compressed on its own, or all bytes stored uncompressed.
/// A writer falls back to the stored form when compression does not pay, except
/// streaming compression of a large frame that has already flushed compressed
/// output to its sink (the reader accepts compressed frames of any size).
enum class RunLayout : uint8_t {
  kCombined = 0x00,
  kSeparate = 0x01,
  kCombinedUncompressed = 0x02,
};

/// A framed row block carries length-prefixed rows back to back:
///
///   kind         u8   e.g. ShuffleBlockKind::kCompositeRows
///   rowCount     u32
///   rawBytes     u64  uncompressed body bytes
///   storedBytes  u64  body bytes that follow
///   layout       u8   RunLayout::kCombined (compressed) or
///                     kCombinedUncompressed
///   body              rows back to back, each [size i32][size bytes]
///
/// The reader requires rowCount in [1, kMaxBlockRows], rawBytes at least
/// 4 * rowCount, a non-negative size per row, rows consuming the body
/// exactly, storedBytes = rawBytes for kCombinedUncompressed, and a codec for
/// kCombined. The header size counts the kind byte.
/// Wire header of a framed row block (packed, little-endian, no padding).
struct __attribute__((packed)) RowBlockHeader {
  ShuffleBlockKind kind;
  uint32_t rowCount;
  uint64_t rawBytes;
  uint64_t storedBytes;
  RunLayout layout;
};

static_assert(std::is_trivially_copyable_v<RowBlockHeader>);
inline constexpr uint64_t kRowBlockHeaderBytes = sizeof(RowBlockHeader);

/// The size prefix of a framed row. Rows lie back to back, so the prefix is
/// in general unaligned.
inline int32_t framedRowSize(const void* row) {
  return memory::loadUnaligned<int32_t>(row);
}

/// Bytes of a framed row with its size prefix.
inline uint64_t framedRowBytes(const void* row) {
  return sizeof(int32_t) + static_cast<uint64_t>(framedRowSize(row));
}

/// Sets the size of `buffer`, allocating or growing it from `pool`, and
/// returns its data. Keeps the bytes - growth copies them and doubles the
/// capacity - for buffers appended to; and the capacity on shrink, so one
/// buffer serves every partition or payload.
char* resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

/// resizeBuffer() for a workspace about to be overwritten: the bytes are
/// not kept. Within the capacity it only sets the size; past it, the old
/// buffer goes back to `pool` before one of exactly `size` is allocated, so
/// growing neither copies nor holds both, nor over-allocates.
char* resetBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool);

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool);

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool);

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool);

/// Assembles one framed row block of `rows` (each [size i32][size bytes])
/// into `block` with the given `kind`. The body is gathered in place, so a
/// stored block needs no second copy and a compressed one is copied back over
/// it. The compression workspace comes from memory::spillMemoryPool() so
/// blocks can be written when the task's pool is short.
void buildRowBlock(
    ShuffleBlockKind kind,
    const uint8_t* const* rows,
    size_t numRows,
    uint64_t rawBytes,
    Codec* codec,
    BufferPtr& block,
    BufferPtr& compressedBuffer,
    memory::MemoryPool* pool,
    uint64_t& compressTimeNs);

} // namespace bytedance::bolt::shuffle::sparksql
