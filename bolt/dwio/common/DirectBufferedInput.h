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

#include <folly/Executor.h>
#include <cstdint>
#include <memory>
#include <mutex>

#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/caching/FileGroupStats.h"
#include "bolt/common/caching/ScanTracker.h"
#include "bolt/common/io/IoStatistics.h"
#include "bolt/common/io/Options.h"
#include "bolt/connectors/Connector.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/CacheInputStream.h"
#include "bolt/dwio/common/InputStream.h"
namespace bytedance::bolt::dwio::common {

struct LoadRequest {
  LoadRequest() = default;
  LoadRequest(bolt::common::Region& _region, cache::TrackingId _trackingId)
      : region(_region), trackingId(_trackingId) {}

  bool operator<(const LoadRequest& other) const {
    return region.offset < other.region.offset ||
        (region.offset == other.region.offset &&
         region.length > other.region.length);
  }

  bolt::common::Region region;
  cache::TrackingId trackingId;

  const SeekableInputStream* stream;

  /// Buffers to be handed to 'stream' after load.
  memory::Allocation data;
  std::string tinyData;
  // Number of bytes in 'data/tinyData'.
  int64_t loadSize{0};
};

/// Represents planned loads that should be performed as a single IO.
class DirectCoalescedLoad : public cache::CoalescedLoad {
 public:
  DirectCoalescedLoad(
      std::shared_ptr<ReadFileInputStream> input,
      std::shared_ptr<IoStatistics> ioStats,
      uint64_t groupId,
      const std::vector<LoadRequest*>& requests,
      std::shared_ptr<memory::MemoryPool> pool,
      int32_t loadQuantum)
      : CoalescedLoad({}, {}),
        ioStats_(ioStats),
        groupId_(groupId),
        input_(std::move(input)),
        loadQuantum_(loadQuantum),
        pool_(pool) {
    BOLT_DCHECK_NOT_NULL(pool_);
    BOLT_DCHECK(
        std::is_sorted(requests.begin(), requests.end(), [](auto* x, auto* y) {
          return x->region.offset < y->region.offset;
        }));
    requests_.reserve(requests.size());
    for (auto i = 0; i < requests.size(); ++i) {
      requests_.push_back(std::move(*requests[i]));
      preloadBytes_ += requests_[i].region.length;
    }
  };

  ~DirectCoalescedLoad() override {
    if (pool_) {
      requests_.clear();
      pool_.reset();
    }
  }

  // Loads the regions. Returns {} since no cache entries are made. The loaded
  // data is retrieved with getData().
  std::vector<cache::CachePin> loadData(bool isPrefetch) override;

  // Returns the buffer for 'region' in either 'data' or 'tinyData'. 'region'
  // must match a region given to SelectiveBufferedInput::enqueue().
  int32_t
  getData(int64_t offset, memory::Allocation& data, std::string& tinyData);

  const auto& requests() {
    return requests_;
  }

  uint64_t getMemoryBytes() override {
    std::lock_guard<std::mutex> l(mutex_);
    if (pool_) {
      return pool_->currentBytes();
    }
    return 0;
  }

  void cancel() override {
    std::lock_guard<std::mutex> l(mutex_);
    if ((state_ != State::kLoading) && pool_) {
      requests_.clear();
      pool_.reset();
    }
    state_ = State::kCancelled;
    if (promise_ != nullptr) {
      promise_->setValue(true);
      promise_.reset();
    }
  }

  int64_t size() const override {
    int64_t size = 0;
    for (auto& request : requests_) {
      size += request.region.length;
    }
    return size;
  }

 private:
  const std::shared_ptr<IoStatistics> ioStats_;
  const uint64_t groupId_;
  const std::shared_ptr<ReadFileInputStream> input_;
  const int32_t loadQuantum_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<LoadRequest> requests_;
};

class DirectBufferedInput : public BufferedInput {
 public:
  static constexpr int32_t kTinySize = 2'000;

  DirectBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      const MetricsLogPtr& metricsLog,
      uint64_t fileNum,
      std::shared_ptr<cache::ScanTracker> tracker,
      uint64_t groupId,
      std::shared_ptr<IoStatistics> ioStats,
      folly::Executor* executor,
      const io::ReaderOptions& readerOptions,
      std::shared_ptr<connector::AsyncThreadCtx> asyncThreadCtx)
      : BufferedInput(
            std::move(readFile),
            readerOptions.getMemoryPool(),
            metricsLog,
            ioStats ? ioStats.get() : nullptr),
        fileNum_(fileNum),
        tracker_(std::move(tracker)),
        groupId_(groupId),
        ioStats_(std::move(ioStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions),
        asyncThreadCtx_(asyncThreadCtx) {}

  ~DirectBufferedInput() override {
    streamToCoalescedLoad_.wlock()->clear();
    for (auto& load : coalescedLoads_) {
      load->cancel();
    }
  }

  std::unique_ptr<SeekableInputStream> enqueue(
      bolt::common::Region region,
      const StreamIdentifier* sid) override;

  bool supportSyncLoad() const override {
    return false;
  }

  void updatePreloadingBytes(int64_t bytes) {
    if (asyncThreadCtx_) {
      asyncThreadCtx_->addPreloadingBytes(bytes);
    }
  }

  void load(const LogType /*unused*/) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  bool shouldPreload(int32_t numPages = 0) override;

  bool shouldPrefetchStripes() const override {
    return false;
  }

  void setNumStripes(int32_t numStripes) override {
    auto* stats = tracker_->fileGroupStats();
    if (stats) {
      stats->recordFile(fileNum_, groupId_, numStripes);
    }
  }

  virtual std::unique_ptr<BufferedInput> clone() const override {
    std::unique_ptr<DirectBufferedInput> input(new DirectBufferedInput(
        input_,
        fileNum_,
        tracker_,
        groupId_,
        ioStats_,
        executor_,
        options_,
        asyncThreadCtx_));
    return input;
  }

  memory::MemoryPool* pool() {
    return &pool_;
  }

  /// Returns the CoalescedLoad that contains the correlated loads for
  /// 'stream' or nullptr if none. Returns nullptr on all but first
  /// call for 'stream' since the load is to be triggered by the first
  /// access.
  std::shared_ptr<DirectCoalescedLoad> coalescedLoad(
      const SeekableInputStream* stream);

  std::unique_ptr<SeekableInputStream>
  read(uint64_t offset, uint64_t length, LogType logType) const override;

  folly::Executor* executor() const override {
    return executor_;
  }

 private:
  /// Constructor used by clone().
  DirectBufferedInput(
      std::shared_ptr<ReadFileInputStream> input,
      uint64_t fileNum,
      std::shared_ptr<cache::ScanTracker> tracker,
      uint64_t groupId,
      std::shared_ptr<IoStatistics> ioStats,
      folly::Executor* executor,
      const io::ReaderOptions& readerOptions,
      std::shared_ptr<connector::AsyncThreadCtx> asyncThreadCtx)
      : BufferedInput(std::move(input), readerOptions.getMemoryPool()),
        fileNum_(fileNum),
        tracker_(std::move(tracker)),
        groupId_(groupId),
        ioStats_(std::move(ioStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions),
        asyncThreadCtx_(asyncThreadCtx) {}

  std::vector<int32_t> groupRequests(
      const std::vector<LoadRequest*>& requests,
      bool prefetch) const;

  // Makes a CoalescedLoad for 'requests' to be read together, coalescing
  // IO if appropriate. If 'prefetch' is set, schedules the CoalescedLoad
  // on 'executor_'. Links the CoalescedLoad  to all DirectInputStreams that
  // it covers.
  void readRegion(const std::vector<LoadRequest*>& requests, bool prefetch);

  // Read coalesced regions.  Regions are grouped together using `groupEnds'.
  // For example if there are 5 regions, 1 and 2 are coalesced together and 3,
  // 4, 5 are coalesced together, we will have {2, 5} in `groupEnds'.
  void readRegions(
      const std::vector<LoadRequest*>& requests,
      bool prefetch,
      const std::vector<int32_t>& groupEnds);

  // Holds the reference on the memory pool for async load in case of early
  // task terminate.
  struct AsyncLoadHolder {
    explicit AsyncLoadHolder(
        std::shared_ptr<cache::CoalescedLoad> load,
        int32_t prefetchMemoryPercent,
        std::shared_ptr<connector::AsyncThreadCtx> asyncThrCtx)
        : load(std::move(load)),
          prefetchMemoryPercent_(prefetchMemoryPercent),
          asyncThreadCtx(std::move(asyncThrCtx)) {
      BOLT_CHECK(asyncThreadCtx);
      preloadBytesLimit_ = asyncThreadCtx->preloadBytesLimit();
    }

    AsyncLoadHolder(const AsyncLoadHolder&) = delete;
    AsyncLoadHolder& operator=(const AsyncLoadHolder&) = delete;

    AsyncLoadHolder(AsyncLoadHolder&& other) noexcept
        : load(std::move(other.load)),
          prefetchMemoryPercent_(other.prefetchMemoryPercent_),
          asyncThreadCtx(std::move(other.asyncThreadCtx)),
          preloadBytesLimit_(other.preloadBytesLimit_),
          addedBytes_(other.addedBytes_) {
      other.asyncThreadCtx = nullptr;
      other.addedBytes_ = 0;
    }

    bool canPreload() const {
      static int maxAttempt = 1000;
      static int sleepMs = 500;
      if (load->state() != DirectCoalescedLoad::State::kPlanned ||
          prefetchMemoryPercent_ == 0 || !asyncThreadCtx->allowPreload()) {
        return false;
      }
      for (auto attempt = 1; attempt <= maxAttempt; ++attempt) {
        if (load->state() != DirectCoalescedLoad::State::kPlanned) {
          return false;
        }
        auto memoryBytes =
            std::max(load->getMemoryBytes(), preloadBytesLimit_ / 10);
        // there are in preloading with high memory usage, sleep to avoid OOM
        {
          std::lock_guard<std::mutex> lock(asyncThreadCtx->getMutex());
          // must preserve memory for other part of scan, i.e decompressed data
          if ((asyncThreadCtx->inPreloadingBytesUntracked() +
                       load->preloadBytes() <
                   preloadBytesLimit_ * prefetchMemoryPercent_ / 100.0 &&
               asyncThreadCtx->inPreloadingBytesUntracked() +
                       load->preloadBytes() + memoryBytes <
                   preloadBytesLimit_ / 2)) {
            asyncThreadCtx->addPreloadingBytesUntracked(load->preloadBytes());
            addedBytes_ = load->preloadBytes();
            return true;
          }
        }
        if (!asyncThreadCtx->allowPreload()) {
          return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        if (!asyncThreadCtx->allowPreload()) {
          return false;
        }
        if (attempt % 100 == 0) {
          LOG(WARNING) << "async IO thread sleep: " << attempt * sleepMs / 1000
                       << " s, pool_.currentBytes(): " << memoryBytes
                       << " preloadBytesLimit: " << preloadBytesLimit_
                       << " inPreloadingBytes: "
                       << asyncThreadCtx->inPreloadingBytesUntracked()
                       << " preloadBytes: " << load->preloadBytes()
                       << " prefetchMemoryPercent: " << prefetchMemoryPercent_
                       << ", pls reduce preload IO threads or add memory";
        }
      }
      LOG(WARNING)
          << "async IO thread sleep: " << maxAttempt * sleepMs / 1000
          << " s, still can not preload, pls reduce preload IO threads or add memory";
      return false;
    }

    std::shared_ptr<cache::CoalescedLoad> load;
    int32_t prefetchMemoryPercent_{30};
    std::shared_ptr<connector::AsyncThreadCtx> asyncThreadCtx;
    uint64_t preloadBytesLimit_{0};

    mutable int64_t addedBytes_{0};

    ~AsyncLoadHolder() {
      if (asyncThreadCtx && addedBytes_ > 0) {
        asyncThreadCtx->addPreloadingBytes(-addedBytes_);
      }
      // Release the load reference before the memory pool reference.
      // This is to make sure the memory pool is not destroyed before we free
      // up the allocated buffers. This is to handle the case that the
      // associated task has already destroyed before the async load is done.
      // The async load holds the last reference to the memory pool in that
      // case.
      load.reset();
    }
  };

  const uint64_t fileNum_;
  const std::shared_ptr<cache::ScanTracker> tracker_;
  const uint64_t groupId_;
  const std::shared_ptr<IoStatistics> ioStats_;
  folly::Executor* const executor_;
  const uint64_t fileSize_;

  // Regions that are candidates for loading.
  std::vector<LoadRequest> requests_;

  // Coalesced loads spanning multiple streams in one IO.
  folly::Synchronized<folly::F14FastMap<
      const SeekableInputStream*,
      std::shared_ptr<DirectCoalescedLoad>>>
      streamToCoalescedLoad_;

  // Distinct coalesced loads in 'coalescedLoads_'.
  std::vector<std::shared_ptr<cache::CoalescedLoad>> coalescedLoads_;

  io::ReaderOptions options_;
  std::shared_ptr<connector::AsyncThreadCtx> asyncThreadCtx_ = nullptr;
};

} // namespace bytedance::bolt::dwio::common
