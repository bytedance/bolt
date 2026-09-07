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

#include <folly/GLog.h>
#include <folly/ScopeGuard.h>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <stdexcept>

#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/flags/BoltFlags.h"
#include "bolt/common/memory/Allocation.h"
#include "bolt/common/process/ThreadNameHolder.h"
#include "bolt/common/process/TraceContext.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/dwio/common/DirectBufferedInput.h"
#include "bolt/dwio/common/DirectInputStream.h"

using ::bytedance::bolt::common::Region;
using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::dwio::common {

using cache::CoalescedLoad;
using cache::ScanTracker;
using cache::TrackingId;

namespace {

struct SharedSeekableInputChunk {
  uint64_t offset;
  BufferPtr data;
};

class SharedSeekableInputData {
 public:
  SharedSeekableInputData(
      std::unique_ptr<SeekableInputStream> input,
      Region region,
      std::shared_ptr<memory::MemoryPool> pool,
      uint64_t loadQuantum)
      : input_(std::move(input)),
        region_(region),
        pool_(std::move(pool)),
        loadQuantum_(loadQuantum) {
    BOLT_CHECK_GT(loadQuantum_, 0);
  }

  std::shared_ptr<const SharedSeekableInputChunk> load(uint64_t position) {
    BOLT_CHECK_LT(position, region_.length);
    const auto offset = position / loadQuantum_ * loadQuantum_;

    std::lock_guard<std::mutex> lock(mutex_);
    if (failure_) {
      std::rethrow_exception(failure_);
    }
    for (auto i = 0; i < chunks_.size(); ++i) {
      if (chunks_[i] && chunks_[i]->offset == offset) {
        auto chunk = chunks_[i];
        if (i == 0) {
          std::swap(chunks_[0], chunks_[1]);
        }
        return chunk;
      }
    }

    const auto size = std::min(loadQuantum_, region_.length - offset);
    auto buffer = AlignedBuffer::allocate<char>(size, pool_.get());
    const std::vector<uint64_t> positions{offset};
    PositionProvider positionProvider(positions);
    try {
      input_->seekToPosition(positionProvider);
      input_->readFully(buffer->asMutable<char>(), size);
    } catch (...) {
      failure_ = std::current_exception();
      throw;
    }

    auto chunk = std::make_shared<SharedSeekableInputChunk>(
        SharedSeekableInputChunk{offset, std::move(buffer)});
    chunks_[0] = std::move(chunks_[1]);
    chunks_[1] = chunk;
    return chunk;
  }

 private:
  const std::unique_ptr<SeekableInputStream> input_;
  const Region region_;
  const std::shared_ptr<memory::MemoryPool> pool_;
  const uint64_t loadQuantum_;
  std::mutex mutex_;
  std::exception_ptr failure_;
  std::array<std::shared_ptr<const SharedSeekableInputChunk>, 2> chunks_;
};

class SharedSeekableInputStream final : public SeekableInputStream {
 public:
  explicit SharedSeekableInputStream(
      std::shared_ptr<SharedSeekableInputData> data,
      uint64_t size)
      : data_(std::move(data)), size_(size) {}

  bool Next(const void** buffer, int32_t* size) override {
    currentChunk_.reset();
    lastNextSize_ = 0;
    if (position_ >= size_) {
      *size = 0;
      return false;
    }

    currentChunk_ = data_->load(position_);
    const auto offsetInChunk = position_ - currentChunk_->offset;
    const auto available = currentChunk_->data->size() - offsetInChunk;
    BOLT_CHECK_LE(available, std::numeric_limits<int32_t>::max());
    *buffer = currentChunk_->data->as<char>() + offsetInChunk;
    *size = static_cast<int32_t>(available);
    position_ += available;
    lastNextSize_ = *size;
    return true;
  }

  void BackUp(int32_t count) override {
    BOLT_CHECK_GE(count, 0, "can't backup negative distances");
    BOLT_CHECK_LE(count, lastNextSize_, "Can't backup that much!");
    currentChunk_.reset();
    position_ -= count;
    lastNextSize_ = 0;
  }

  bool SkipInt64(int64_t count) override {
    currentChunk_.reset();
    lastNextSize_ = 0;
    if (count < 0) {
      return false;
    }
    const auto unsignedCount = static_cast<uint64_t>(count);
    if (unsignedCount <= size_ - position_) {
      position_ += unsignedCount;
      return true;
    }
    position_ = size_;
    return false;
  }

  google::protobuf::int64 ByteCount() const override {
    return static_cast<google::protobuf::int64>(position_);
  }

  void seekToPosition(PositionProvider& position) override {
    const auto newPosition = position.next();
    BOLT_CHECK_LE(newPosition, size_);
    currentChunk_.reset();
    lastNextSize_ = 0;
    position_ = newPosition;
  }

  std::string getName() const override {
    return fmt::format("SharedSeekableInputStream {} of {}", position_, size_);
  }

  size_t positionSize() override {
    return 1;
  }

 private:
  const std::shared_ptr<SharedSeekableInputData> data_;
  const uint64_t size_;
  uint64_t position_{0};
  int32_t lastNextSize_{0};
  std::shared_ptr<const SharedSeekableInputChunk> currentChunk_;
};

SeekableInputStreamPair makeSharedSeekableInputStreamPair(
    std::unique_ptr<SeekableInputStream> input,
    Region region,
    std::shared_ptr<memory::MemoryPool> pool,
    uint64_t loadQuantum) {
  auto data = std::make_shared<SharedSeekableInputData>(
      std::move(input), region, std::move(pool), loadQuantum);
  auto makeStream = [&]() {
    return std::make_unique<SharedSeekableInputStream>(data, region.length);
  };
  return {makeStream(), makeStream()};
}

} // namespace

std::unique_ptr<SeekableInputStream> DirectBufferedInput::enqueue(
    Region region,
    const StreamIdentifier* sid = nullptr) {
  if (!coalescedLoads_.empty()) {
    // Results of previous load are no more available here.
    coalescedLoads_.clear();
    streamToCoalescedLoad_.wlock()->clear();
  }
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  TrackingId id;
  if (sid) {
    id = TrackingId(sid->getId());
  }
  BOLT_CHECK_LE(region.offset + region.length, fileSize_);
  if (tracker_) {
    tracker_->recordReference(id, region.length, fileNum_, groupId_);
  }
  auto stream = std::make_unique<DirectInputStream>(
      this,
      ioStats_.get(),
      region,
      input_,
      fileNum_,
      tracker_,
      id,
      groupId_,
      options_.loadQuantum());
  if (!preloaded()) {
    requests_.emplace_back(region, id);
    requests_.back().stream = stream.get();
  }
  return stream;
}

SeekableInputStreamPair DirectBufferedInput::enqueuePair(
    Region region,
    const StreamIdentifier* sid) {
  if (region.length == 0 || preloaded()) {
    return BufferedInput::enqueuePair(region, sid);
  }

  BOLT_CHECK_LE(region.offset, fileSize_);
  BOLT_CHECK_LE(region.length, fileSize_ - region.offset);
  BOLT_CHECK_GT(options_.loadQuantum(), 0);
  auto input = enqueue(region, sid);
  BOLT_CHECK(!requests_.empty());
  // Only preload the first quantum. Subsequent chunks are fetched on demand,
  // keeping the shared pair bounded while retaining initial coalescing.
  requests_.back().region.length =
      std::min<uint64_t>(region.length, options_.loadQuantum());
  return makeSharedSeekableInputStreamPair(
      std::move(input),
      region,
      pool_.shared_from_this(),
      options_.loadQuantum());
}

bool DirectBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/)
    const {
  return preloaded();
}

bool DirectBufferedInput::shouldPreload(int32_t numPages) {
  return false;
}

namespace {

// True if the percentage is high enough to warrant prefetch.
bool isPrefetchablePct(int32_t pct) {
  return pct >= FLAGS_cache_prefetch_min_pct;
}

bool lessThan(const LoadRequest* left, const LoadRequest* right) {
  return *left < *right;
}

void appendRanges(
    memory::Allocation& allocation,
    size_t length,
    std::vector<folly::Range<char*>>& buffers) {
  BOLT_CHECK_LE(
      length,
      memory::AllocationTraits::pageBytes(allocation.numPages()),
      "Length exceeds allocation size");
  uint64_t offsetInRuns = 0;
  for (int i = 0; i < allocation.numRuns(); ++i) {
    BOLT_CHECK_GE(length, offsetInRuns);
    auto run = allocation.runAt(i);
    const uint64_t bytes = memory::AllocationTraits::pageBytes(run.numPages());
    const uint64_t readSize = std::min(bytes, length - offsetInRuns);
    buffers.emplace_back(run.data<char>(), readSize);
    offsetInRuns += readSize;
    if (offsetInRuns >= length) {
      break;
    }
  }
}

} // namespace

void DirectBufferedInput::load(const LogType /*unused*/) {
  // After load, new requests cannot be merged into pre-load ones.
  auto requests = std::move(requests_);
  std::vector<LoadRequest*> storageLoad[2];
  for (auto& request : requests) {
    cache::TrackingData trackingData;
    const bool prefetchAnyway = request.trackingId.empty() ||
        request.trackingId.id() == StreamIdentifier::sequentialFile().id_;
    if (!prefetchAnyway && tracker_) {
      trackingData = tracker_->trackingData(request.trackingId);
    }
    const int loadIndex =
        (prefetchAnyway || isPrefetchablePct(adjustedReadPct(trackingData)))
        ? 1
        : 0;
    storageLoad[loadIndex].push_back(&request);
  }
  std::sort(storageLoad[1].begin(), storageLoad[1].end(), lessThan);
  std::sort(storageLoad[0].begin(), storageLoad[0].end(), lessThan);
  std::vector<int32_t> groupEnds[2];
  groupEnds[1] = groupRequests(storageLoad[1], true);
  moveCoalesced(
      storageLoad[1],
      groupEnds[1],
      storageLoad[0],
      [](auto* request) { return request->region.offset; },
      [](auto* request) {
        return request->region.offset + request->region.length;
      });
  groupEnds[0] = groupRequests(storageLoad[0], false);
  readRegions(storageLoad[1], true, groupEnds[1]);
  readRegions(storageLoad[0], false, groupEnds[0]);
}

std::vector<int32_t> DirectBufferedInput::groupRequests(
    const std::vector<LoadRequest*>& requests,
    bool prefetch) const {
  if (requests.empty() || (requests.size() < 2 && !prefetch)) {
    // A single request has no other requests to coalesce with and is not
    // eligible to prefetch. This will be loaded by itself on first use.
    return {};
  }
  const int32_t maxDistance = options_.maxCoalesceDistance();
  const auto loadQuantum = options_.loadQuantum();
  // If reading densely accessed, coalesce into large for best throughput, if
  // for sparse, coalesce to quantum to reduce overread. Not all sparse access
  // is correlated.
  const auto maxCoalesceBytes =
      prefetch ? options_.maxCoalesceBytes() : loadQuantum;

  // Combine adjacent short reads.
  int64_t coalescedBytes = 0;
  std::vector<int32_t> ends;
  ends.reserve(requests.size());
  std::vector<char> ranges;
  coalesceIo<LoadRequest*, char>(
      requests,
      maxDistance,
      // Break batches up. Better load more short ones i parallel.
      std::numeric_limits<int32_t>::max(), // limit coalesce by size, not count.
      [&](int32_t index) { return requests[index]->region.offset; },
      [&](int32_t index) -> int32_t {
        auto size = requests[index]->region.length;
        if (size > loadQuantum) {
          coalescedBytes += loadQuantum;
          return loadQuantum;
        }
        coalescedBytes += size;
        return size;
      },
      [&](int32_t index) {
        if (coalescedBytes > maxCoalesceBytes) {
          coalescedBytes = 0;
          return kNoCoalesce;
        }
        return 1;
      },
      [&](LoadRequest* /*request*/, std::vector<char>& ranges) {
        // ranges.size() is used in coalesceIo so we cannot leave it empty.
        ranges.push_back(0);
      },
      [&](int32_t /*gap*/, std::vector<char> /*ranges*/) { /*no op*/ },
      [&](const std::vector<LoadRequest*>& /*requests*/,
          int32_t /*begin*/,
          int32_t end,
          uint64_t /*offset*/,
          const std::vector<char>& /*ranges*/) { ends.push_back(end); });
  return ends;
}

void DirectBufferedInput::readRegion(
    const std::vector<LoadRequest*>& requests,
    bool prefetch) {
  if (requests.empty() || (requests.size() == 1 && !prefetch)) {
    return;
  }
  auto load = std::make_shared<DirectCoalescedLoad>(
      input_,
      ioStats_,
      groupId_,
      requests,
      pool_.shared_from_this(),
      options_.loadQuantum());
  coalescedLoads_.push_back(load);
  streamToCoalescedLoad_.withWLock([&](auto& loads) {
    for (auto& request : requests) {
      loads[request->stream] = load;
    }
  });
}

void DirectBufferedInput::readRegions(
    const std::vector<LoadRequest*>& requests,
    bool prefetch,
    const std::vector<int32_t>& groupEnds) {
  int i = 0;
  std::vector<LoadRequest*> group;
  for (auto end : groupEnds) {
    while (i < end) {
      group.push_back(requests[i++]);
    }
    readRegion(group, prefetch);
    group.clear();
  }
  if (prefetch && executor_) {
    for (auto i = 0; i < coalescedLoads_.size(); ++i) {
      auto& load = coalescedLoads_[i];
      if (load->state() == CoalescedLoad::State::kPlanned) {
        // hack way to set preload memory percent and total memory percent
        AsyncLoadHolder loadHolder(
            load, options_.prefetchMemoryPercent(), asyncThreadCtx_);
        executor_->add([asyncLoad = std::move(loadHolder)]() {
          if (asyncLoad.load->state() != DirectCoalescedLoad::State::kPlanned) {
            return;
          }
          connector::AsyncThreadCtx::Guard guard(
              asyncLoad.asyncThreadCtx.get(), 0);
          if (!guard) {
            return;
          }

          // first check available memory allows to preload data, even if not,
          // the non-preload load will be sync loaded on the main thread.
          if (asyncLoad.canPreload(guard)) {
            process::TraceContext trace("Read Ahead");
            BOLT_CHECK_NOT_NULL(asyncLoad.load);
            auto res = asyncLoad.load->loadOrFuture(nullptr);
            if (!res) {
              LOG(INFO) << "Preload fails to load "
                        << (uint64_t)asyncLoad.load.get() << " by async thread "
                        << folly::getCurrentThreadName().value();
              asyncLoad.asyncThreadCtx->disallowPreload();
            }
          }
        });
      }
    }
  }
}

std::shared_ptr<DirectCoalescedLoad> DirectBufferedInput::coalescedLoad(
    const SeekableInputStream* stream) {
  return streamToCoalescedLoad_.withWLock(
      [&](auto& loads) -> std::shared_ptr<DirectCoalescedLoad> {
        auto it = loads.find(stream);
        if (it == loads.end()) {
          return nullptr;
        }
        auto load = std::move(it->second);
        loads.erase(it);
        return load;
      });
}

void DirectBufferedInput::preload() {
  BOLT_CHECK(!preloadData_.has_value(), "preload() called more than once");
  BOLT_CHECK(requests_.empty(), "preload() must be called before enqueue()");

  preloadData_.emplace(fileSize_);

  std::vector<folly::Range<char*>> buffers;
  if (fileSize_ <= kTinySize) {
    preloadData_->tinyData.resize(fileSize_);
    buffers.emplace_back(preloadData_->tinyData.data(), fileSize_);
  } else {
    const auto numPages = memory::AllocationTraits::numPages(fileSize_);
    pool_.allocateNonContiguous(numPages, preloadData_->data);
    appendRanges(preloadData_->data, fileSize_, buffers);
  }

  uint64_t usecs = 0;
  {
    MicrosecondTimer timer(&usecs);
    input_->read(buffers, 0, LogType::FILE);
  }
  if (ioStats_) {
    ioStats_->read().increment(fileSize_);
    ioStats_->incRawBytesRead(fileSize_);
    ioStats_->queryThreadIoLatencySync().increment(usecs);
    ioStats_->queryThreadIoLatency().increment(usecs);
    ioStats_->incTotalScanTime(usecs * 1'000);
  }
}

folly::Range<const char*> DirectBufferedInput::preloadedData(
    uint64_t offset,
    uint64_t length) const {
  BOLT_CHECK(
      preloadData_.has_value(), "preloadedData() called without preload");
  BOLT_CHECK_LT(offset, preloadData_->size, "Offset exceeds preloaded size");
  const auto available =
      std::min<uint64_t>(length, preloadData_->size - offset);
  if (preloadData_->data.numPages() == 0) {
    return {preloadData_->tinyData.data() + offset, available};
  }

  int32_t runIndex;
  int32_t offsetInRun;
  preloadData_->data.findRun(offset, &runIndex, &offsetInRun);
  auto run = preloadData_->data.runAt(runIndex);
  const auto runBytes = memory::AllocationTraits::pageBytes(run.numPages());
  const auto contiguousBytes =
      std::min<uint64_t>(available, runBytes - offsetInRun);
  return {run.data<const char>() + offsetInRun, contiguousBytes};
}

std::unique_ptr<SeekableInputStream> DirectBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    LogType /*logType*/) const {
  BOLT_CHECK_LE(offset + length, fileSize_);
  return std::make_unique<DirectInputStream>(
      const_cast<DirectBufferedInput*>(this),
      ioStats_.get(),
      Region{offset, length},
      input_,
      fileNum_,
      nullptr,
      TrackingId(),
      0,
      options_.loadQuantum());
}

std::vector<cache::CachePin> DirectCoalescedLoad::loadData(bool isPrefetch) {
  std::vector<folly::Range<char*>> buffers;
  int64_t lastEnd = requests_[0].region.offset;
  int64_t size = 0;
  int64_t overread = 0;
  int tryCount = 0;
  int64_t requestBytes = 0;
  for (auto& request : requests_) {
    if (isPrefetch && state_ == cache::CoalescedLoad::State::kCancelled) {
      return {};
    }
    auto& region = request.region;
    if (region.offset > lastEnd) {
      buffers.push_back(folly::Range<char*>(
          nullptr,
          reinterpret_cast<char*>(
              static_cast<uint64_t>(region.offset - lastEnd))));
      overread += buffers.back().size();
    }
    if (region.length > DirectBufferedInput::kTinySize) {
      if (&request != &requests_.back()) {
        // Case where request is a little over quantum but is followed by
        // another within the max distance. Coalesces and allows reading the
        // region of max quantum + max distance in one piece.
        request.loadSize = region.length;
      } else {
        request.loadSize = std::min<int64_t>(region.length, loadQuantum_);
      }
      const auto numPages =
          memory::AllocationTraits::numPages(request.loadSize);
      if (isAsyncPreloadThread()) {
        try {
          pool_->allocateNonContiguous(numPages, request.data);
        } catch (const std::exception& e) { // wrap any memory failure exception
          throw std::runtime_error(fmt::format(
              "{} failed with memory allocation {} pages, size {}, true size {} exception {}",
              folly::getCurrentThreadName().value(),
              numPages,
              request.loadSize,
              region.length,
              e.what()));
        }
      } else {
        pool_->allocateNonContiguous(numPages, request.data);
      }
#if PRELOAD_DEBUG
      bool isAsyncThread = isAsyncPreloadThread();
      requestBytes += request.data.byteSize();
      if (isAsyncThread &&
          ((++tryCount == requests_.size() && std::rand() % 10 < 6) ||
           std::rand() % 10 < 4)) {
        throw std::runtime_error(fmt::format(
            "{} async load mock failed with memory {} , total target {}",
            folly::getCurrentThreadName().value(),
            requestBytes,
            this->size()));
      }
#endif
      appendRanges(request.data, request.loadSize, buffers);
    } else {
      request.loadSize = region.length;
      request.tinyData.resize(region.length);
      buffers.push_back(folly::Range(request.tinyData.data(), region.length));
    }
    lastEnd = region.offset + request.loadSize;
    size += request.loadSize;
  }
  if (isPrefetch && state_ == cache::CoalescedLoad::State::kCancelled) {
    return {};
  }
  uint64_t usecs = 0;
  {
    MicrosecondTimer timer(&usecs);
    input_->read(buffers, requests_[0].region.offset, LogType::FILE);
  }
  if (isPrefetch && state_ == cache::CoalescedLoad::State::kCancelled) {
    return {};
  }
  ioStats_->read().increment(size + overread);
  ioStats_->incRawBytesRead(size);
  ioStats_->incTotalScanTime(usecs * 1'000);
  ioStats_->incRawOverreadBytes(overread);

  if (isPrefetch) {
    ioStats_->prefetch().increment(size + overread);
  }
  BOLT_TEST_ADJUST(
      "bytedance::bolt::cache::DirectCoalescedLoad::loadData", this);
  return {};
}

int32_t DirectCoalescedLoad::getData(
    int64_t offset,
    memory::Allocation& data,
    std::string& tinyData) {
  auto it = std::lower_bound(
      requests_.begin(), requests_.end(), offset, [](auto& x, auto offset) {
        return x.region.offset < offset;
      });
  if (it == requests_.end() || it->region.offset != offset) {
    return 0;
  }
  data = std::move(it->data);
  tinyData = std::move(it->tinyData);
  return it->loadSize;
}

} // namespace bytedance::bolt::dwio::common
