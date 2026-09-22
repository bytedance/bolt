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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/vector/BaseVector.h"

namespace bytedance::bolt::lance::reader {

class NativeLanceReadPlan {
 public:
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

  explicit NativeLanceReadPlan(memory::MemoryPool& pool);

  /// Drops staged-but-not-submitted reads. Submitted or materialized reads are
  /// retained so later decode stages can consume prefetched bytes.
  void clearStage();

  void schedule(
      dwio::common::BufferedInput& input,
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

  /// Returns an exact prefetched range or a slice from a wider prefetched
  /// range. Submitted streams are materialized on demand.
  BufferPtr take(uint64_t offset, uint64_t length);

 private:
  struct ScheduledRead {
    ReadKey key;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };

  struct SubmittedRead {
    uint64_t length;
    std::unique_ptr<dwio::common::SeekableInputStream> stream;
  };

  BufferPtr materializeSubmitted(
      std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash>::iterator it);

  memory::MemoryPool& pool_;
  // Streams returned by BufferedInput::enqueue must stay alive after submit().
  // DirectBufferedInput associates an async/coalesced load with the exact
  // stream pointer, so submitted streams remain owned here until decode asks
  // for the bytes.
  std::vector<ScheduledRead, memory::StlAllocator<ScheduledRead>> stagedReads_;
  std::unordered_set<ReadKey, ReadKeyHash> scheduledReadKeys_;
  std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash> submittedReads_;
  std::unordered_map<ReadKey, BufferPtr, ReadKeyHash> prefetchedReads_;
  // The row-aligned decode stage consumes independent entries concurrently.
  // Planning and materialization remain single-threaded.
  std::mutex consumeMutex_;
};

} // namespace bytedance::bolt::lance::reader
