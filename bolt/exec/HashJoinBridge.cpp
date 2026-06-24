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

#include "bolt/exec/HashJoinBridge.h"
namespace bytedance::bolt::exec {
void HashJoinBridge::start() {
  std::lock_guard<std::mutex> l(mutex_);
  started_ = true;

  // In morsel-driven mode, then it is possible that no builder exists when
  // hashbridge is started when the task is initiated.
  if (!isMorselDriven_) {
    BOLT_CHECK_GT(numBuilders_, 0);
  }
}

void HashJoinBridge::addBuilder() {
  std::lock_guard<std::mutex> l(mutex_);

  // In morsel-driven mode, then it is possible to add more builders after the
  // hashbridge is started when the task is initiated.
  if (!isMorselDriven_) {
    BOLT_CHECK(!started_);
  }
  ++numBuilders_;
}

bool HashJoinBridge::setHashTable(
    std::unique_ptr<BaseHashTable> table,
    SpillPartitionSet spillPartitionSet,
    bool hasNullKeys,
    SpillOffsetToBitsSet offsetToJoinBits) {
  BOLT_CHECK_NOT_NULL(table, "setHashTable called with null table");

  auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);

  bool hasSpillData;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_CHECK(started_);
    BOLT_CHECK(!buildResult_.has_value());
    BOLT_CHECK(restoringSpillShards_.empty());

    if (restoringSpillPartitionId_.has_value()) {
      for (const auto& id : spillPartitionIdSet) {
        BOLT_DCHECK_LT(
            restoringSpillPartitionId_->partitionBitOffset(),
            id.partitionBitOffset());
      }
    }

    for (auto& partitionEntry : spillPartitionSet) {
      const auto id = partitionEntry.first;
      BOLT_CHECK_EQ(spillPartitionSets_.count(id), 0);
      spillPartitionSets_.emplace(id, std::move(partitionEntry.second));
    }
    buildResult_ = HashBuildResult(
        std::move(table),
        std::move(restoringSpillPartitionId_),
        std::move(spillPartitionIdSet),
        hasNullKeys,
        offsetToJoinBits);
    restoringSpillPartitionId_.reset();

    hasSpillData = !spillPartitionSets_.empty();
    promises = std::move(promises_);
  }
  notify(std::move(promises));
  return hasSpillData;
}

void HashJoinBridge::setHashTable(
    std::shared_ptr<BaseHashTable> table,
    bool hasNullKeys) {
  BOLT_CHECK_NOT_NULL(table, "setHashTable called with null table");

  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_CHECK(started_);
    BOLT_CHECK(!buildResult_.has_value());
    BOLT_CHECK(restoringSpillShards_.empty());

    buildResult_ = HashBuildResult(std::move(table), hasNullKeys);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

void HashJoinBridge::setAntiJoinHasNullKeys() {
  std::vector<ContinuePromise> promises;
  SpillPartitionSet spillPartitions;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_CHECK(started_);
    BOLT_CHECK(!buildResult_.has_value());
    BOLT_CHECK(restoringSpillShards_.empty());

    buildResult_ = HashBuildResult{};
    restoringSpillPartitionId_.reset();
    spillPartitions.swap(spillPartitionSets_);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<HashJoinBridge::HashBuildResult> HashJoinBridge::tableOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_CHECK(started_);
  BOLT_CHECK(!cancelled_, "Getting hash table after join is aborted");
  BOLT_CHECK(
      !buildResult_.has_value() ||
      (!restoringSpillPartitionId_.has_value() &&
       restoringSpillShards_.empty()));

  if (buildResult_.has_value()) {
    return buildResult_.value();
  }
  promises_.emplace_back("HashJoinBridge::tableOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

bool HashJoinBridge::probeFinished() {
  std::vector<ContinuePromise> promises;
  bool hasSpillInput = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    BOLT_CHECK(started_);
    BOLT_CHECK(buildResult_.has_value());
    BOLT_CHECK(
        !restoringSpillPartitionId_.has_value() &&
        restoringSpillShards_.empty());
    BOLT_CHECK_GT(numBuilders_, 0);

    // NOTE: we are clearing the hash table as it has been fully processed and
    // not needed anymore. We'll wait for the HashBuild operator to build a new
    // table from the next spill partition now.
    buildResult_.reset();

    if (!spillPartitionSets_.empty()) {
      hasSpillInput = true;
      restoringSpillPartitionId_ = spillPartitionSets_.begin()->first;
      restoringSpillShards_ =
          spillPartitionSets_.begin()->second->split(numBuilders_);
      BOLT_CHECK_EQ(restoringSpillShards_.size(), numBuilders_);
      spillPartitionSets_.erase(spillPartitionSets_.begin());
      promises = std::move(promises_);
    } else {
      BOLT_CHECK(promises_.empty());
    }
  }
  notify(std::move(promises));
  return hasSpillInput;
}

std::optional<HashJoinBridge::SpillInput> HashJoinBridge::spillInputOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  BOLT_CHECK(started_);
  BOLT_CHECK(!cancelled_, "Getting spill input after join is aborted");
  BOLT_DCHECK(
      !restoringSpillPartitionId_.has_value() || !buildResult_.has_value());

  if (!restoringSpillPartitionId_.has_value()) {
    if (spillPartitionSets_.empty()) {
      return HashJoinBridge::SpillInput{};
    } else {
      promises_.emplace_back("HashJoinBridge::spillInputOrFuture");
      *future = promises_.back().getSemiFuture();
      return std::nullopt;
    }
  }
  BOLT_CHECK(!restoringSpillShards_.empty());
  auto spillShard = std::move(restoringSpillShards_.back());
  restoringSpillShards_.pop_back();
  return SpillInput(std::move(spillShard));
}

bool isLeftNullAwareJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  return (joinNode->isAntiJoin() || joinNode->isLeftSemiProjectJoin() ||
          joinNode->isLeftSemiFilterJoin()) &&
      joinNode->isNullAware() && (joinNode->filter() != nullptr);
}

bool canDropDuplicates(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  // Left semi and anti join with no extra filter only needs to know whether
  // there is a match. Hence, no need to store entries with duplicate keys.
  return !joinNode->filter() &&
      (joinNode->isLeftSemiFilterJoin() || joinNode->isLeftSemiProjectJoin() ||
       joinNode->isAntiJoin());
}

uint64_t HashJoinMemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  uint64_t reclaimedBytes{0};
  pool->visitChildren([&](memory::MemoryPool* child) {
    BOLT_CHECK_EQ(child->kind(), memory::MemoryPool::Kind::kLeaf);
    // The hash probe operator do not support memory reclaim.
    if (!isHashBuildMemoryPool(*child)) {
      return true;
    }
    // We only need to reclaim from any one of the hash build operators
    // which will reclaim from all the peer hash build operators.
    reclaimedBytes = child->reclaim(targetBytes, maxWaitMs, stats);
    return false;
  });
  return reclaimedBytes;
}

bool isHashBuildMemoryPool(const memory::MemoryPool& pool) {
  return folly::StringPiece(pool.name()).endsWith("HashBuild");
}

bool isHashProbeMemoryPool(const memory::MemoryPool& pool) {
  return folly::StringPiece(pool.name()).endsWith("HashProbe");
}
} // namespace bytedance::bolt::exec
