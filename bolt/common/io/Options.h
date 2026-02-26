/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/common/memory/Memory.h"
namespace bytedance::bolt::io {

constexpr uint64_t DEFAULT_AUTO_PRELOAD_SIZE =
    (static_cast<const uint64_t>((1ul << 20) * 72));

/**
 * Mode for prefetching data.
 *
 * This mode may be ignored for a reader, such as DWRF, where it does not
 * make sense.
 *
 * To enable single-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PRELOAD);
 * To enable double-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PREFETCH);
 * To select unbuffered reading:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::NOT_SET);
 *
 * Single-buffered reading (as in dwio::PreloadableInputStream)
 * reads ahead into a buffer.   Double-buffered reading additionally reads
 * asynchronously into a second buffer, swaps the buffers when the
 * first is fully consumed and the second has been filled, and then starts
 * a new parallel read.  For clients with a slow network connection to
 * Warm Storage, enabling PREFETCH reduces elapsed time by 10% or more,
 * at the cost of a second buffer.   The relative improvement would be greater
 * for cases where the network throughput is higher.
 */
enum class PrefetchMode {
  NOT_SET = 0,
  PRELOAD = 1, // read a buffer of autoPreloadLength bytes on a read beyond the
               // current buffer, if any.
  PREFETCH = 2, // read a second buffer of autoPreloadLength bytes ahead of
                // actual reads.
};

class ReaderOptions {
 protected:
  bolt::memory::MemoryPool* memoryPool;
  uint64_t autoPreloadLength;
  PrefetchMode prefetchMode;
  size_t loadQuantum_{kDefaultLoadQuantum};
  size_t maxCoalesceDistance_{kDefaultCoalesceDistance};
  size_t maxCoalesceBytes_{kDefaultCoalesceBytes};
  int32_t prefetchRowGroups_{kDefaultPrefetchRowGroups};
  int32_t prefetchMemoryPercent_{kDefaultPrefetchMemoryPercent};

 public:
  static constexpr size_t kDefaultLoadQuantum = 8 << 20; // 8MB
  static constexpr size_t kDefaultCoalesceDistance = 512 << 10; // 512K
  static constexpr size_t kDefaultCoalesceBytes = 128 << 20; // 128M
  static constexpr int32_t kDefaultPrefetchRowGroups = 1;
  static constexpr int32_t kDefaultPrefetchMemoryPercent = 30;

  explicit ReaderOptions(bolt::memory::MemoryPool* pool)
      : memoryPool(pool),
        autoPreloadLength(DEFAULT_AUTO_PRELOAD_SIZE),
        prefetchMode(PrefetchMode::PREFETCH) {}

  ReaderOptions& operator=(const ReaderOptions& other) {
    memoryPool = other.memoryPool;
    autoPreloadLength = other.autoPreloadLength;
    prefetchMode = other.prefetchMode;
    maxCoalesceDistance_ = other.maxCoalesceDistance_;
    maxCoalesceBytes_ = other.maxCoalesceBytes_;
    prefetchRowGroups_ = other.prefetchRowGroups_;
    loadQuantum_ = other.loadQuantum_;
    prefetchMemoryPercent_ = other.prefetchMemoryPercent_;
    return *this;
  }

  ReaderOptions(const ReaderOptions& other) {
    *this = other;
  }

  /**
   * Set the memory allocator.
   */
  ReaderOptions& setMemoryPool(bolt::memory::MemoryPool& pool) {
    memoryPool = &pool;
    return *this;
  }

  /**
   * Modify the autoPreloadLength
   */
  ReaderOptions& setAutoPreloadLength(uint64_t len) {
    autoPreloadLength = len;
    return *this;
  }

  /**
   * Modify the prefetch mode.
   */
  ReaderOptions& setPrefetchMode(PrefetchMode mode) {
    prefetchMode = mode;
    return *this;
  }

  /**
   * Modify the load quantum.
   */
  ReaderOptions& setLoadQuantum(int32_t quantum) {
    loadQuantum_ = quantum;
    return *this;
  }
  /**
   * Modify the maximum load coalesce distance.
   */
  ReaderOptions& setMaxCoalesceDistance(int32_t distance) {
    maxCoalesceDistance_ = distance;
    return *this;
  }
  /**
   * Modify the maximum load coalesce bytes.
   */
  ReaderOptions& setMaxCoalesceBytes(int64_t bytes) {
    maxCoalesceBytes_ = bytes;
    return *this;
  }

  /**
   * Modify the number of row groups to prefetch.
   */
  ReaderOptions& setPrefetchRowGroups(int32_t numPrefetch) {
    prefetchRowGroups_ = numPrefetch;
    return *this;
  }

  ReaderOptions& setPrefetchMemoryPercent(int32_t percent) {
    if (percent < 0 || percent > 100) {
      percent = kDefaultPrefetchMemoryPercent;
      LOG(WARNING)
          << "prefetchMemoryPercent must be between 0 and 100, use default value: "
          << percent;
    }
    prefetchMemoryPercent_ = percent;
    return *this;
  }

  /**
   * Get the memory allocator.
   */
  bolt::memory::MemoryPool& getMemoryPool() const {
    return *memoryPool;
  }

  uint64_t getAutoPreloadLength() const {
    return autoPreloadLength;
  }

  PrefetchMode getPrefetchMode() const {
    return prefetchMode;
  }

  int32_t loadQuantum() const {
    return loadQuantum_;
  }

  int32_t maxCoalesceDistance() const {
    return maxCoalesceDistance_;
  }

  int64_t maxCoalesceBytes() const {
    return maxCoalesceBytes_;
  }

  int64_t prefetchRowGroups() const {
    return prefetchRowGroups_;
  }

  int32_t prefetchMemoryPercent() const {
    return prefetchMemoryPercent_;
  }
};
} // namespace bytedance::bolt::io
