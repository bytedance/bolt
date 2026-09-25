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

#include "bolt/dwio/lance/NativeLanceReadScheduler.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {
namespace {

template <typename Map>
bool containsRange(const Map& ranges, uint64_t offset, uint64_t length) {
  for (const auto& entry : ranges) {
    const auto& key = entry.first;
    if (offset >= key.offset && offset - key.offset <= key.length &&
        length <= key.length - (offset - key.offset)) {
      return true;
    }
  }
  return false;
}

bool containsRange(
    const NativeLanceReadScheduler::ReadKey& key,
    uint64_t offset,
    uint64_t length) {
  return offset >= key.offset && offset - key.offset <= key.length &&
      length <= key.length - (offset - key.offset);
}

template <typename Map>
std::optional<NativeLanceReadScheduler::ReadKey> containingKey(
    const Map& ranges,
    uint64_t offset) {
  std::optional<NativeLanceReadScheduler::ReadKey> best;
  for (const auto& entry : ranges) {
    const auto& key = entry.first;
    if (key.offset <= offset && offset - key.offset < key.length &&
        (!best.has_value() ||
         key.offset + key.length > best->offset + best->length)) {
      best = key;
    }
  }
  return best;
}

} // namespace

NativeLanceReadScheduler::NativeLanceReadScheduler(memory::MemoryPool& pool)
    : NativeLanceReadScheduler(pool, Options{}) {}

NativeLanceReadScheduler::NativeLanceReadScheduler(
    memory::MemoryPool& pool,
    Options options)
    : pool_(pool),
      options_(options),
      stagedReads_(memory::StlAllocator<ScheduledRead>(&pool)) {
  BOLT_CHECK_GT(options_.maxReadBytes, 0);
  BOLT_CHECK_GT(options_.maxInFlightBytes, 0);
}

NativeLanceReadScheduler::~NativeLanceReadScheduler() {
  cancel();
}

void NativeLanceReadScheduler::clearStage() {
  stagedReads_.clear();
  scheduledReadKeys_.clear();
  if (submittedReads_.empty() && prefetchedReads_.empty()) {
    input_ = nullptr;
  }
}

void NativeLanceReadScheduler::schedule(
    dwio::common::BufferedInput& input,
    uint64_t offset,
    uint64_t length) {
  BOLT_CHECK(!cancelled(), "Cannot schedule a cancelled Lance read scheduler");
  BOLT_CHECK(
      input_ == nullptr || input_ == &input,
      "A Lance read scheduler cannot span multiple BufferedInput instances");
  input_ = &input;
  BOLT_CHECK_LE(offset, std::numeric_limits<uint64_t>::max() - length);
  const auto maxChunkBytes =
      std::min(options_.maxReadBytes, options_.maxInFlightBytes);
  for (uint64_t chunkOffset = 0; chunkOffset < length;) {
    const auto chunkBytes = std::min(maxChunkBytes, length - chunkOffset);
    scheduleChunk(input, offset + chunkOffset, chunkBytes);
    chunkOffset += chunkBytes;
  }
}

void NativeLanceReadScheduler::scheduleChunk(
    dwio::common::BufferedInput& input,
    uint64_t offset,
    uint64_t length) {
  if (length == 0) {
    return;
  }
  const ReadKey key{offset, length};
  if (prefetchedReads_.find(key) != prefetchedReads_.end()) {
    return;
  }
  if (submittedReads_.find(key) != submittedReads_.end()) {
    return;
  }
  if (containsRange(prefetchedReads_, offset, length) ||
      containsRange(submittedReads_, offset, length)) {
    return;
  }
  if (!scheduledReadKeys_.insert(key).second) {
    return;
  }
  if (input.isBuffered(offset, length)) {
    auto buffer = AlignedBuffer::allocate<char>(length, &pool_);
    auto stream = input.enqueue({offset, length});
    stream->readFully(buffer->asMutable<char>(), length);
    prefetchedReads_.insert_or_assign(
        key, PrefetchedRead{.data = std::move(buffer)});
    scheduledReadKeys_.erase(key);
    return;
  }
  stagedReads_.push_back({key});
}

void NativeLanceReadScheduler::submit(dwio::common::BufferedInput& input) {
  BOLT_CHECK(!cancelled(), "Cannot submit a cancelled Lance read scheduler");
  BOLT_CHECK(input_ == nullptr || input_ == &input);
  input_ = &input;
  if (stagedReads_.empty()) {
    return;
  }
  size_t next = 0;
  while (next < stagedReads_.size()) {
    materializeUntilAdmitted(stagedReads_[next].key.length);
    uint64_t batchBytes = 0;
    std::vector<
        std::pair<ReadKey, std::unique_ptr<dwio::common::SeekableInputStream>>>
        batch;
    while (next < stagedReads_.size()) {
      const auto key = stagedReads_[next].key;
      if (!batch.empty() &&
          (batchBytes > options_.maxInFlightBytes - key.length ||
           submittedBytes_ >
               options_.maxInFlightBytes - batchBytes - key.length)) {
        break;
      }
      batch.emplace_back(key, input.enqueue({key.offset, key.length}));
      batchBytes += key.length;
      ++next;
    }
    input.load(dwio::common::LogType::BLOCK);
    for (auto& [key, stream] : batch) {
      submittedReads_.insert_or_assign(
          key,
          SubmittedRead{.length = key.length, .stream = std::move(stream)});
      submittedBytes_ += key.length;
      peakSubmittedBytes_ = std::max(peakSubmittedBytes_, submittedBytes_);
    }
    if (input.supportSyncLoad()) {
      materialize();
    }
  }
  clearStage();
}

void NativeLanceReadScheduler::load(dwio::common::BufferedInput& input) {
  submit(input);
  materialize();
}

void NativeLanceReadScheduler::materialize() {
  while (!submittedReads_.empty()) {
    materializeSubmitted(submittedReads_.begin());
  }
}

void NativeLanceReadScheduler::finishBatch() {
  std::lock_guard<std::mutex> guard(consumeMutex_);
  clearStage();
  submittedReads_.clear();
  prefetchedReads_.clear();
  submittedBytes_ = 0;
  input_ = nullptr;
}

void NativeLanceReadScheduler::materializeUntilAdmitted(
    uint64_t incomingBytes) {
  while (!submittedReads_.empty() &&
         (incomingBytes > options_.maxInFlightBytes ||
          submittedBytes_ > options_.maxInFlightBytes - incomingBytes)) {
    materializeSubmitted(submittedReads_.begin());
  }
}

BufferPtr NativeLanceReadScheduler::materializeSubmitted(
    std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash>::iterator it) {
  auto key = it->first;
  auto submitted = std::move(it->second);
  submittedReads_.erase(it);
  BOLT_CHECK_GE(submittedBytes_, submitted.length);
  submittedBytes_ -= submitted.length;
  auto buffer = AlignedBuffer::allocate<char>(submitted.length, &pool_);
  submitted.stream->readFully(buffer->asMutable<char>(), submitted.length);
  prefetchedReads_.insert_or_assign(key, PrefetchedRead{.data = buffer});
  if (submittedReads_.empty() && stagedReads_.empty()) {
    input_ = nullptr;
  }
  return buffer;
}

BufferPtr NativeLanceReadScheduler::take(uint64_t offset, uint64_t length) {
  std::lock_guard<std::mutex> guard(consumeMutex_);
  if (cancelled()) {
    return nullptr;
  }
  BOLT_CHECK_LE(offset, std::numeric_limits<uint64_t>::max() - length);
  const ReadKey key{offset, length};
  const auto prefetched = prefetchedReads_.find(key);
  if (prefetched != prefetchedReads_.end()) {
    auto buffer = std::move(prefetched->second.data);
    prefetchedReads_.erase(prefetched);
    return buffer;
  }
  const auto submitted = submittedReads_.find(key);
  if (submitted != submittedReads_.end()) {
    auto buffer = materializeSubmitted(submitted);
    prefetchedReads_.erase(key);
    return buffer;
  }
  for (const auto& [prefetchedKey, read] : prefetchedReads_) {
    if (containsRange(prefetchedKey, offset, length)) {
      return Buffer::slice<char>(
          read.data, offset - prefetchedKey.offset, length, &pool_);
    }
  }
  for (auto it = submittedReads_.begin(); it != submittedReads_.end(); ++it) {
    const auto submittedKey = it->first;
    if (containsRange(submittedKey, offset, length)) {
      auto buffer = materializeSubmitted(it);
      return Buffer::slice<char>(
          buffer, offset - submittedKey.offset, length, &pool_);
    }
  }

  uint64_t position = offset;
  const auto end = offset + length;
  while (position < end) {
    auto key = containingKey(prefetchedReads_, position);
    if (!key.has_value()) {
      key = containingKey(submittedReads_, position);
    }
    if (!key.has_value()) {
      return nullptr;
    }
    position = std::min(end, key->offset + key->length);
  }

  auto result = AlignedBuffer::allocate<char>(length, &pool_);
  position = offset;
  while (position < end) {
    auto key = containingKey(prefetchedReads_, position);
    if (!key.has_value()) {
      const auto submittedKey = containingKey(submittedReads_, position);
      BOLT_CHECK(submittedKey.has_value());
      auto submitted = submittedReads_.find(*submittedKey);
      BOLT_CHECK(submitted != submittedReads_.end());
      materializeSubmitted(submitted);
      key = submittedKey;
    }
    const auto bytes =
        std::min(end - position, key->offset + key->length - position);
    const auto& source = prefetchedReads_.at(*key);
    std::memcpy(
        result->asMutable<char>() + position - offset,
        source.data->as<char>() + position - key->offset,
        bytes);
    position += bytes;
  }
  return result;
}

void NativeLanceReadScheduler::cancel(dwio::common::BufferedInput* input) {
  std::lock_guard<std::mutex> guard(consumeMutex_);
  if (cancelled_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  if (input != nullptr) {
    input->cancelPendingLoads();
  }
  clearStage();
  submittedReads_.clear();
  prefetchedReads_.clear();
  submittedBytes_ = 0;
  input_ = nullptr;
}

} // namespace bytedance::bolt::lance::reader
