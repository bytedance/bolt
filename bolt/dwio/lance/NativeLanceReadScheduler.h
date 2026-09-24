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
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/common/io/Options.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/lance/NativeLanceMemoryBudget.h"
#include "bolt/dwio/lance/NativeLancePageReader.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceBufferRole : uint8_t {
  kValues,
  kValidity,
  kOffsets,
  kRepetition,
  kDefinition,
  kDictionary,
  kPayload,
  kBlobDescriptor,
  kBlobPayload,
  kUnknown,
};

class NativeLanceReadScheduler {
 public:
  struct Options {
    uint64_t maxReadBytes{io::ReaderOptions::kDefaultLoadQuantum};
    uint64_t maxInFlightBytes{io::ReaderOptions::kDefaultCoalesceBytes};
  };

  struct ReadKey {
    uint64_t offset;
    uint64_t length;

    bool operator==(const ReadKey& other) const {
      return offset == other.offset && length == other.length;
    }
  };

  struct ReadKeyHash {
    size_t operator()(ReadKey key) const {
      return std::hash<uint64_t>{}(key.offset) ^
          (std::hash<uint64_t>{}(key.length) << 1);
    }
  };

  struct PageBufferKey {
    NativeLancePageKey page;
    NativeLanceBufferRole role;
    uint32_t ordinal;

    bool operator==(const PageBufferKey& other) const {
      return page == other.page && role == other.role &&
          ordinal == other.ordinal;
    }
  };

  struct PageBufferKeyHash {
    size_t operator()(const PageBufferKey& key) const {
      return std::hash<uint32_t>{}(key.page.physicalColumn) ^
          (std::hash<int32_t>{}(key.page.pageIndex) << 1) ^
          (std::hash<uint8_t>{}(static_cast<uint8_t>(key.role)) << 2) ^
          (std::hash<uint32_t>{}(key.ordinal) << 3);
    }
  };

  struct PageBuffer {
    BufferPtr data;
    std::optional<NativeLanceMemoryBudget::Reservation> reservation;
  };

  explicit NativeLanceReadScheduler(memory::MemoryPool& pool);

  NativeLanceReadScheduler(
      memory::MemoryPool& pool,
      Options options,
      NativeLanceMemoryBudget* memoryBudget = nullptr);

  ~NativeLanceReadScheduler();

  /// Drops staged-but-not-submitted reads. Submitted or materialized reads are
  /// retained so later decode stages can consume prefetched bytes.
  void clearStage();

  void schedule(
      dwio::common::BufferedInput& input,
      uint64_t offset,
      uint64_t length);

  void schedulePage(
      dwio::common::BufferedInput& input,
      PageBufferKey pageBuffer,
      uint64_t offset,
      uint64_t length);

  /// Issues the current staged requests to BufferedInput. Async-capable inputs
  /// may start background I/O here while the returned streams stay owned by the
  /// plan until decode calls take().
  void submit(dwio::common::BufferedInput& input);

  /// Submits and materializes the current stage into owned Bolt buffers.
  void load(dwio::common::BufferedInput& input);

  /// Materializes all submitted streams into owned Bolt buffers.
  void materialize();

  /// Cancels staged and submitted work, releases materialized buffers, and
  /// prevents new requests from being admitted. Idempotent.
  void cancel(dwio::common::BufferedInput* input = nullptr);

  bool cancelled() const {
    return cancelled_.load(std::memory_order_acquire);
  }

  /// Test/debug counters. Planning and materialization are single-threaded.
  uint64_t inFlightBytes() const {
    return submittedBytes_;
  }

  uint64_t peakInFlightBytes() const {
    return peakSubmittedBytes_;
  }

  /// Returns an exact prefetched range or a slice from a wider prefetched
  /// range. Submitted streams are materialized on demand.
  BufferPtr take(uint64_t offset, uint64_t length);

  /// Returns the bytes owned by a specific page buffer request.
  PageBuffer takePage(PageBufferKey pageBuffer);

 private:
  struct ScheduledRead {
    ReadKey key;
  };

  struct SubmittedRead {
    uint64_t length;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };

  struct PrefetchedRead {
    BufferPtr data;
    std::optional<NativeLanceMemoryBudget::Reservation> reservation;
  };

  BufferPtr materializeSubmitted(
      std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash>::iterator it);
  void scheduleChunk(
      dwio::common::BufferedInput& input,
      uint64_t offset,
      uint64_t length);
  void materializeUntilAdmitted(uint64_t incomingBytes);
  std::optional<NativeLanceMemoryBudget::Reservation> reserve(uint64_t bytes);

  memory::MemoryPool& pool_;
  const Options options_;
  // Streams returned by BufferedInput::enqueue must stay alive after submit().
  // DirectBufferedInput associates an async/coalesced load with the exact
  // stream pointer, so submitted streams remain owned here until decode asks
  // for the bytes.
  std::vector<ScheduledRead, memory::StlAllocator<ScheduledRead>> stagedReads_;
  std::unordered_set<ReadKey, ReadKeyHash> scheduledReadKeys_;
  std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash> submittedReads_;
  std::unordered_map<ReadKey, PrefetchedRead, ReadKeyHash> prefetchedReads_;
  std::unordered_map<PageBufferKey, ReadKey, PageBufferKeyHash> pageReads_;
  uint64_t submittedBytes_{0};
  uint64_t peakSubmittedBytes_{0};
  std::atomic<bool> cancelled_{false};
  dwio::common::BufferedInput* input_{nullptr};
  NativeLanceMemoryBudget* const memoryBudget_;
  // The row-aligned decode stage consumes independent entries concurrently.
  // Planning and materialization remain single-threaded.
  std::mutex consumeMutex_;
};

} // namespace bytedance::bolt::lance::reader
