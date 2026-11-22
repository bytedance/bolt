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

#include <chrono>
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
/// A writer falls back to the stored form when compression does not pay.
enum class RunLayout : uint8_t {
  kCombined = 0x00,
  kSeparate = 0x01,
  kCombinedUncompressed = 0x02,
};

/// A framed row block carries length-prefixed rows back to back:
///
///   kind         u8   e.g. ShuffleBlockKind::kCompositeRows
///   rowCount     u32
///   decodedSize  u64  body bytes before compression
///   stored       u64  body bytes that follow
///   layout       u8   RunLayout::kCombined (compressed) or
///                     kCombinedUncompressed
///   body              rows back to back, each [size i32][size bytes]
///
/// The reader requires rowCount in [1, kMaxBlockRows], decodedSize at least
/// 4 * rowCount, a non-negative size per row, rows consuming the body
/// exactly, stored = decodedSize for kCombinedUncompressed, and a codec for
/// kCombined. The header size counts the kind byte.
/// Wire header of a framed row block (packed, little-endian, no padding).
struct __attribute__((packed)) RowBlockHeader {
  ShuffleBlockKind kind;
  uint32_t rowCount;
  uint64_t decodedBytes;
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

/// Measures steady_clock wall time and adds the elapsed nanoseconds, minus the
/// delta of up to two excluded nanosecond counters (e.g. nested spill,
/// decompression, or shred timers), to `*target` on destruction.
template <typename Target>
class ExclusiveNanosecondTimer {
 public:
  explicit ExclusiveNanosecondTimer(
      Target* target,
      const uint64_t* excluded1 = nullptr,
      const uint64_t* excluded2 = nullptr)
      : target_(target),
        excluded1_(excluded1),
        excluded2_(excluded2),
        excluded1Start_(excluded1 != nullptr ? *excluded1 : 0),
        excluded2Start_(excluded2 != nullptr ? *excluded2 : 0),
        start_(std::chrono::steady_clock::now()) {}

  ~ExclusiveNanosecondTimer() {
    if (target_ == nullptr) {
      return;
    }
    const auto elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start_)
            .count());
    uint64_t excluded = 0;
    if (excluded1_ != nullptr && *excluded1_ > excluded1Start_) {
      excluded += *excluded1_ - excluded1Start_;
    }
    if (excluded2_ != nullptr && *excluded2_ > excluded2Start_) {
      excluded += *excluded2_ - excluded2Start_;
    }
    if (elapsed > excluded) {
      *target_ += static_cast<Target>(elapsed - excluded);
    }
  }

  ExclusiveNanosecondTimer(const ExclusiveNanosecondTimer&) = delete;
  ExclusiveNanosecondTimer& operator=(const ExclusiveNanosecondTimer&) = delete;

 private:
  Target* const target_;
  const uint64_t* const excluded1_;
  const uint64_t* const excluded2_;
  const uint64_t excluded1Start_;
  const uint64_t excluded2Start_;
  const std::chrono::steady_clock::time_point start_;
};

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
