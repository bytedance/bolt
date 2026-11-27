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

#include <atomic>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include <folly/CPortability.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/MemoryCopy.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"

namespace bytedance::bolt {
class RowType;
} // namespace bytedance::bolt

namespace bytedance::bolt::shuffle::sparksql {
struct ShuffleWriterOptions;
} // namespace bytedance::bolt::shuffle::sparksql

namespace bytedance::bolt::shuffle::sparksql::cell {

struct SplitBatch;

/// Samples task attempts once per writer and checks their non-null cache values
/// with per-partition ordered CRC32C and exact counts across all batches.
/// CRC32C can miss collisions and is not cryptographic. Does not check null
/// positions or encoded output, trigger flushes, or retain input values. Cache
/// base addresses registered via setColumnCache() remain borrowed through
/// finishStream().
class CellShuffleCheck {
 public:
  /// Returns null for disabled, unselected or unsupported writers. Keeps all
  /// check setup and sampling decisions out of the producer.
  static std::unique_ptr<CellShuffleCheck> create(
      const RowType& type,
      uint32_t numPartitions,
      const ShuffleWriterOptions& options,
      bool supportedSchema,
      memory::MemoryPool* pool);

  /// Selects by taskAttemptId % 100 < 100 * ratio, independently of partitions.
  /// Negative task IDs are selected whenever ratio is positive.
  /// Returns null when disabled, unselected, or no supported columns exist.
  static std::unique_ptr<CellShuffleCheck> create(
      const RowType& type,
      uint32_t numPartitions,
      double ratio,
      int32_t maxColumns,
      memory::MemoryPool* pool,
      int64_t taskAttemptId = -1);

  bool checksColumn(uint32_t col) const;

  /// Registers the staging cache base for a checked column (fixed for the
  /// lifetime of the splitter).
  void setColumnCache(uint32_t col, const char* streamCacheBase);

  /// Validates the batch and accumulates expected digests and counts for every
  /// checked column without retaining any references into `batch`.
  void observeBatch(const SplitBatch& batch);

  /// Called after the store at an existing full-cache flush point. Validates
  /// the observer type selected independently from the producer's kernel.
  template <typename T>
  FOLLY_ALWAYS_INLINE void beforeFullFlush(
      uint32_t col,
      uint32_t pid,
      const char* line,
      uint32_t usedBytes);

  /// Observes natural partial flushes (e.g. during seal or final flush).
  void beforeStreamFlush(
      uint32_t col,
      uint32_t pid,
      const char* line,
      uint32_t usedBytes);

  /// Compares after every final cache flush, before output finalization.
  void finishStream();

 private:
  using ObserveInput = void (*)(CellShuffleCheck&, uint32_t, const SplitBatch&);
  using StreamObserve =
      void (*)(CellShuffleCheck&, uint32_t, uint32_t, const char*, uint32_t);
  struct ColumnCheck {
    ObserveInput observeInput{nullptr};
    StreamObserve streamObserve{nullptr};
    uint32_t streamIndex{0};
    const char* streamCache{nullptr};
  };

  struct StreamState {
    uint32_t expectedCrc{0};
    uint32_t actualCrc{0};
    uint64_t expectedCount{0};
    uint64_t actualCount{0};
  };

  CellShuffleCheck(
      uint32_t numPartitions,
      std::vector<ColumnCheck> columns,
      uint32_t selectedColumns,
      memory::MemoryPool* pool);

  template <typename T>
  static void observeInputColumn(
      CellShuffleCheck& check,
      uint32_t col,
      const SplitBatch& batch);

  // Keep cache loads out of the producer so they cannot reuse its stored
  // values.
  template <typename T>
  FOLLY_NOINLINE static void observeStream(
      CellShuffleCheck& check,
      uint32_t col,
      uint32_t pid,
      const char* line,
      uint32_t usedBytes);

  const uint32_t numPartitions_;
  std::vector<ColumnCheck> columns_;
  std::vector<StreamState, memory::StlAllocator<StreamState>> streamStates_;
};

template <typename T>
FOLLY_ALWAYS_INLINE void CellShuffleCheck::beforeFullFlush(
    uint32_t col,
    uint32_t pid,
    const char* line,
    uint32_t usedBytes) {
  BOLT_CHECK_LT(col, columns_.size());
  const auto& column = columns_[col];
  BOLT_CHECK(column.streamObserve == &observeStream<T>);
  BOLT_CHECK_LT(pid, numPartitions_);
  BOLT_CHECK_NOT_NULL(column.streamCache);
  BOLT_CHECK(line == column.streamCache + size_t{pid} * kCacheLineBytes);
  BOLT_CHECK_EQ(usedBytes, kCacheLineBytes);
#if defined(__GNUC__) || defined(__clang__)
  // Force subsequent digest operands to come from the post-store cache bytes.
  // This is a compiler barrier; hardware store-buffer forwarding is valid.
  asm volatile("" : : "r"(line) : "memory");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
  if constexpr (std::is_integral_v<T>) {
    auto& state =
        streamStates_[size_t{column.streamIndex} * numPartitions_ + pid];
    constexpr uint32_t kCount = kCacheLineBytes / sizeof(T);
    state.actualCount += kCount;
    BOLT_CHECK_LE(
        state.actualCount,
        state.expectedCount,
        "Cell streaming check extra non-null value: column={} partition={}",
        col,
        pid);
    uint32_t crc = state.actualCrc;
    using UInt = std::make_unsigned_t<T>;
    for (uint32_t at = 0; at < kCacheLineBytes; at += sizeof(T)) {
      crc = simd::crc32U64(
          crc, static_cast<uint64_t>(memory::loadUnaligned<UInt>(line + at)));
    }
    state.actualCrc = crc;
    return;
  }
  observeStream<T>(*this, col, pid, line, usedBytes);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
