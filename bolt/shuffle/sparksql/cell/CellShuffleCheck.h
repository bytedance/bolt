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
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <vector>

#include <folly/CPortability.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace bytedance::bolt {
class RowType;
class RowVector;
} // namespace bytedance::bolt

namespace bytedance::bolt::shuffle::sparksql {
struct ShuffleWriterOptions;
} // namespace bytedance::bolt::shuffle::sparksql

namespace bytedance::bolt::shuffle::sparksql::cell {

struct SplitBatch;

/// Samples task attempts once per writer and checks their non-null cache values
/// with per-partition ordered CRC32C and exact counts across all batches.
/// CRC32C can miss collisions and is not cryptographic. Does not check null
/// positions or encoded output, trigger flushes, or retain input values. The
/// input is borrowed until endBatch(); cache addresses remain borrowed through
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
  /// Selected writers check the first maxColumns supported columns in every
  /// partition across all batches and seals.
  CellShuffleCheck(
      const RowType& type,
      uint32_t numPartitions,
      double ratio,
      int32_t maxColumns,
      memory::MemoryPool* pool,
      int64_t taskAttemptId = -1);

  /// Borrows validated, normalized original hash pids independently of the
  /// producer's mapping. Does not change partitioning or allocate row buffers.
  /// The input must remain alive until endBatch(), even if beginBatch fails.
  void prepareInput(const RowVector& input, bool isHash);
  /// Returns false for an empty batch or an unselected writer.
  bool beginBatch(const SplitBatch& batch);
  bool checksColumn(uint32_t col) const;
  bool checksBatchColumn(uint32_t col) const {
    return batch_ != nullptr && checksColumn(col);
  }
  bool enabled() const {
    return selectedColumns_ != 0;
  }
  void beginColumn(uint32_t col, const char* streamCacheBase);

  /// Called after the store at an existing full-cache flush point. Validates
  /// the observer type selected independently from the producer's kernel.
  template <typename T>
  FOLLY_ALWAYS_INLINE void
  beforeFullFlush(uint32_t pid, const char* line, uint32_t usedBytes);
  /// Observes natural partial flushes, including after endBatch().
  void beforeStreamFlush(
      uint32_t col,
      uint32_t pid,
      const char* line,
      uint32_t usedBytes);
  /// Safe during exception unwinding. Clears all borrowed input references.
  void endBatch() noexcept;
  /// Compares after every final cache flush, before output finalization.
  void finishStream();

 private:
  enum class InputMapping { kIdentity, kDictionary, kConstant };
  using Select = void (*)(CellShuffleCheck&);
  using StreamObserve =
      void (*)(CellShuffleCheck&, uint32_t, uint32_t, const char*, uint32_t);
  struct ColumnCheck {
    Select select{nullptr};
    uint32_t streamIndex{0};
    StreamObserve streamObserve{nullptr};
    const char* streamCache{nullptr};
  };

  struct StreamState {
    uint32_t expectedCrc{0};
    uint32_t actualCrc{0};
    // Input adds credits; each cache observation consumes them exactly once.
    uint64_t remainingCount{0};
  };

  template <typename T>
  static void prepareColumn(CellShuffleCheck& check);
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
  uint32_t selectedColumns_{0};

  // Original validated pids, independent of the producer's copy when available.
  const int32_t* sourcePids_{nullptr};
  uint32_t preparedNumRows_{0};
  const SplitBatch* batch_{nullptr};
  uint32_t column_{0};
  StreamObserve observe_{nullptr};
  const char* cacheBase_{nullptr};
  std::vector<StreamState, memory::StlAllocator<StreamState>> streamStates_;
  StreamState* streamColumn_{nullptr};
};

template <typename T>
FOLLY_ALWAYS_INLINE void CellShuffleCheck::beforeFullFlush(
    uint32_t pid,
    const char* line,
    uint32_t usedBytes) {
  BOLT_CHECK(observe_ == &observeStream<T>);
  BOLT_CHECK_LT(pid, numPartitions_);
  BOLT_CHECK(line == cacheBase_ + size_t{pid} * kCacheLineBytes);
  BOLT_CHECK_EQ(usedBytes, kCacheLineBytes);
#if defined(__GNUC__) || defined(__clang__)
  // Force subsequent digest operands to come from the post-store cache bytes.
  // This is a compiler barrier; hardware store-buffer forwarding is valid.
  asm volatile("" : : "r"(line) : "memory");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
#if defined(__x86_64__) && defined(__SSE4_2__)
  if constexpr (
      std::is_integral_v<T> && std::endian::native == std::endian::little) {
    auto& state = streamColumn_[pid];
    constexpr uint32_t kCount = kCacheLineBytes / sizeof(T);
    BOLT_CHECK_LE(kCount, state.remainingCount);
    uint64_t crc = state.actualCrc;
    for (uint32_t at = 0; at < kCacheLineBytes; at += sizeof(uint64_t)) {
      uint64_t word;
      std::memcpy(&word, line + at, sizeof(word));
      crc = _mm_crc32_u64(crc, word);
    }
    state.actualCrc = static_cast<uint32_t>(crc);
    state.remainingCount -= kCount;
    return;
  }
#endif
  observeStream<T>(*this, column_, pid, line, usedBytes);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
