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

#include "bolt/dwio/lance/NativeLanceReadPlan.h"

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
    const NativeLanceReadPlan::ReadKey& key,
    uint64_t offset,
    uint64_t length) {
  return offset >= key.offset && offset - key.offset <= key.length &&
      length <= key.length - (offset - key.offset);
}

} // namespace

NativeLanceReadPlan::NativeLanceReadPlan(memory::MemoryPool& pool)
    : pool_(pool), stagedReads_(memory::StlAllocator<ScheduledRead>(&pool)) {}

void NativeLanceReadPlan::clearStage() {
  stagedReads_.clear();
  scheduledReadKeys_.clear();
}

void NativeLanceReadPlan::schedule(
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
    prefetchedReads_.insert_or_assign(key, std::move(buffer));
    scheduledReadKeys_.erase(key);
    return;
  }
  stagedReads_.push_back({key, input.enqueue({offset, length})});
}

void NativeLanceReadPlan::submit(dwio::common::BufferedInput& input) {
  if (stagedReads_.empty()) {
    scheduledReadKeys_.clear();
    return;
  }
  input.load(dwio::common::LogType::BLOCK);
  for (auto& staged : stagedReads_) {
    submittedReads_.insert_or_assign(
        staged.key,
        SubmittedRead{
            .length = staged.key.length, .stream = std::move(staged.stream)});
  }
  clearStage();
  if (input.supportSyncLoad()) {
    materialize();
  }
}

void NativeLanceReadPlan::load(dwio::common::BufferedInput& input) {
  submit(input);
  materialize();
}

void NativeLanceReadPlan::materialize() {
  while (!submittedReads_.empty()) {
    materializeSubmitted(submittedReads_.begin());
  }
}

BufferPtr NativeLanceReadPlan::materializeSubmitted(
    std::unordered_map<ReadKey, SubmittedRead, ReadKeyHash>::iterator it) {
  auto key = it->first;
  auto submitted = std::move(it->second);
  submittedReads_.erase(it);
  auto buffer = AlignedBuffer::allocate<char>(submitted.length, &pool_);
  submitted.stream->readFully(buffer->asMutable<char>(), submitted.length);
  prefetchedReads_.insert_or_assign(key, buffer);
  return buffer;
}

BufferPtr NativeLanceReadPlan::take(uint64_t offset, uint64_t length) {
  std::lock_guard<std::mutex> guard(consumeMutex_);
  const ReadKey key{offset, length};
  const auto prefetched = prefetchedReads_.find(key);
  if (prefetched != prefetchedReads_.end()) {
    auto buffer = std::move(prefetched->second);
    prefetchedReads_.erase(prefetched);
    return buffer;
  }
  const auto submitted = submittedReads_.find(key);
  if (submitted != submittedReads_.end()) {
    auto buffer = materializeSubmitted(submitted);
    prefetchedReads_.erase(key);
    return buffer;
  }
  for (const auto& [prefetchedKey, buffer] : prefetchedReads_) {
    if (containsRange(prefetchedKey, offset, length)) {
      return Buffer::slice<char>(
          buffer, offset - prefetchedKey.offset, length, &pool_);
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
  return nullptr;
}

} // namespace bytedance::bolt::lance::reader
