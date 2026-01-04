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

#include "bolt/exec/Spill.h"
#include "bolt/common/base/CompareFlags.h"
#include "bolt/common/base/RuntimeMetrics.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/serializers/PrestoSerializer.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {
void SpillMergeStream::pop() {
  if (++index_ >= size_) {
    setNextBatch();
  }
}

int32_t SpillMergeStream::compare(const MergeStream& other) const {
  auto& otherStream = static_cast<const SpillMergeStream&>(other);
  auto& children = rowVector_->children();
  auto& otherChildren = otherStream.current().children();
  int32_t key = 0;
  if (sortCompareFlags().empty()) {
    do {
      auto result = children[key]
                        ->compare(
                            otherChildren[key].get(),
                            index_,
                            otherStream.index_,
                            CompareFlags())
                        .value();
      if (result != 0) {
        return result;
      }
    } while (++key < numSortKeys());
  } else {
    do {
      auto result = children[key]
                        ->compare(
                            otherChildren[key].get(),
                            index_,
                            otherStream.index_,
                            sortCompareFlags()[key])
                        .value();
      if (result != 0) {
        return result;
      }
    } while (++key < numSortKeys());
  }
  return 0;
}

SpillState::SpillState(
    const common::SpillConfig::SpillIOConfig& ioConfig,
    int32_t maxPartitions,
    int32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    uint64_t targetFileSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : ioConfig_(ioConfig),
      maxPartitions_(maxPartitions),
      numSortKeys_(numSortKeys),
      sortCompareFlags_(sortCompareFlags),
      targetFileSize_(targetFileSize),
      pool_(pool),
      stats_(stats),
      partitionWriters_(maxPartitions_) {
  spilledRowCount_.resize(maxPartitions_, 0);
}

void SpillState::setPartitionSpilled(uint32_t partition) {
  BOLT_DCHECK_LT(partition, maxPartitions_);
  BOLT_DCHECK_LT(spilledPartitionSet_.size(), maxPartitions_);
  BOLT_DCHECK(!spilledPartitionSet_.contains(partition));
  spilledPartitionSet_.insert(partition);
  ++stats_->wlock()->spilledPartitions;
  common::incrementGlobalSpilledPartitionStats();
}

void SpillState::updateSpilledInputBytes(uint64_t bytes) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledInputBytes += bytes;
  common::updateGlobalSpillMemoryBytes(bytes);
}

uint64_t SpillState::appendToPartition(
    uint32_t partition,
    const RowVectorPtr& rows) {
  BOLT_CHECK(
      isPartitionSpilled(partition), "Partition {} is not spilled", partition);

  TestValue::adjust(
      "bytedance::bolt::exec::SpillState::appendToPartition", this);

  BOLT_CHECK_NOT_NULL(
      ioConfig_.getSpillDirPathCb, "Spill directory callback not specified.");
  const std::string& spillDir = ioConfig_.getSpillDirPathCb();
  BOLT_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (partitionWriters_.at(partition) == nullptr) {
    partitionWriters_[partition] = std::make_unique<SpillWriter>(
        std::static_pointer_cast<const RowType>(rows->type()),
        numSortKeys_,
        sortCompareFlags_,
        fmt::format(
            "{}/{}-spill-{}{}",
            spillDir,
            ioConfig_.fileNamePrefix,
            partition,
            (immediateFlush_ ? "-flags" : "")),
        targetFileSize_,
        ioConfig_,
        pool_,
        stats_,
        maxBatchRows_,
        std::nullopt);
  }

  updateSpilledInputBytes(rows->estimateFlatSize());
  spilledRowCount_[partition] += rows->size();

  IndexRange range{0, rows->size()};
  if (immediateFlush_) {
    return partitionWriters_[partition]->writeAndFlush(
        rows, folly::Range<IndexRange*>(&range, 1));
  } else {
    return partitionWriters_[partition]->write(
        rows, folly::Range<IndexRange*>(&range, 1));
  }
}

uint64_t SpillState::appendToPartition(
    uint32_t partition,
    const std::vector<char*, memory::StlAllocator<char*>>& rows,
    RowTypePtr type,
    const RowFormatInfo& info) {
  BOLT_CHECK(
      isPartitionSpilled(partition), "Partition {} is not spilled", partition);

  TestValue::adjust(
      "bytedance::bolt::exec::SpillState::appendToPartition", this);

  BOLT_CHECK_NOT_NULL(
      ioConfig_.getSpillDirPathCb, "Spill directory callback not specified.");
  const std::string& spillDir = ioConfig_.getSpillDirPathCb();
  BOLT_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (partitionWriters_.at(partition) == nullptr) {
    partitionWriters_[partition] = std::make_unique<SpillWriter>(
        type,
        numSortKeys_,
        sortCompareFlags_,
        fmt::format(
            "{}/{}-spill-{}", spillDir, ioConfig_.fileNamePrefix, partition),
        targetFileSize_,
        ioConfig_,
        pool_,
        stats_,
        maxBatchRows_,
        info);
  }

  spilledRowCount_[partition] += rows.size();

  auto spillBytes = partitionWriters_[partition]->write(rows, info);
  // for row based spill, spilled size is very close to input size
  updateSpilledInputBytes(spillBytes);
  return spillBytes;
}

SpillWriter* SpillState::partitionWriter(uint32_t partition) const {
  BOLT_DCHECK(isPartitionSpilled(partition));
  return partitionWriters_[partition].get();
}

void SpillState::finishFile(uint32_t partition) {
  auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return;
  }
  writer->finishFile();
}

size_t SpillState::numFinishedFiles(uint32_t partition) const {
  if (!isPartitionSpilled(partition)) {
    return 0;
  }
  const auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return 0;
  }
  return writer->numFinishedFiles();
}

SpillFiles SpillState::finish(uint32_t partition) {
  auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return {};
  }
  return writer->finish();
}

const SpillPartitionNumSet& SpillState::spilledPartitionSet() const {
  return spilledPartitionSet_;
}

std::vector<std::string> SpillState::testingSpilledFilePaths() const {
  std::vector<std::string> spilledFiles;
  for (const auto& writer : partitionWriters_) {
    if (writer != nullptr) {
      const auto partitionSpilledFiles = writer->testingSpilledFilePaths();
      spilledFiles.insert(
          spilledFiles.end(),
          partitionSpilledFiles.begin(),
          partitionSpilledFiles.end());
    }
  }
  return spilledFiles;
}

std::vector<uint32_t> SpillState::testingSpilledFileIds(
    int32_t partitionNum) const {
  return partitionWriters_[partitionNum]->testingSpilledFileIds();
}

SpillPartitionNumSet SpillState::testingNonEmptySpilledPartitionSet() const {
  SpillPartitionNumSet partitionSet;
  for (uint32_t partition = 0; partition < maxPartitions_; ++partition) {
    if (partitionWriters_[partition] != nullptr) {
      partitionSet.insert(partition);
    }
  }
  return partitionSet;
}

std::vector<std::unique_ptr<SpillPartition>> SpillPartition::split(
    int numShards) {
  std::vector<std::unique_ptr<SpillPartition>> shards(numShards);
  const auto numFilesPerShard = files_.size() / numShards;
  int32_t numRemainingFiles = files_.size() % numShards;
  int fileIdx{0};
  for (int shard = 0; shard < numShards; ++shard) {
    SpillFiles files;
    auto numFiles = numFilesPerShard;
    if (numRemainingFiles-- > 0) {
      ++numFiles;
    }
    files.reserve(numFiles);
    while (files.size() < numFiles) {
      files.push_back(std::move(files_[fileIdx++]));
    }
    shards[shard] = std::make_unique<SpillPartition>(id_, std::move(files));
  }
  BOLT_CHECK_EQ(fileIdx, files_.size());
  files_.clear();
  return shards;
}

std::string SpillPartition::toString() const {
  return fmt::format(
      "SPILLED PARTITION[ID:{} FILES:{} SIZE:{} ROWCOUNT:{}]",
      id_.toString(),
      files_.size(),
      succinctBytes(size_),
      rowCount_);
}

std::unique_ptr<UnorderedStreamReader<BatchStream>>
SpillPartition::createUnorderedReader(
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    bool isRowBased) {
  BOLT_CHECK_NOT_NULL(pool);
  std::vector<std::unique_ptr<BatchStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    if (isRowBased) {
      streams.push_back(RowBasedFileSpillBatchStream::create(
          RowBasedSpillReadFile::create(fileInfo, pool, spillUringEnabled)));
    } else {
      streams.push_back(FileSpillBatchStream::create(
          SpillReadFile::create(fileInfo, pool, spillUringEnabled)));
    }
  }
  files_.clear();
  return std::make_unique<UnorderedStreamReader<BatchStream>>(
      std::move(streams));
}

uint32_t FileSpillMergeStream::id() const {
  return spillFile_->id();
}

void FileSpillMergeStream::nextBatch() {
  MicrosecondTimer timer(&spillReadTimeUs_);
  index_ = 0;
  if (!spillFile_->nextBatch(rowVector_)) {
    spillReadIOTimeUs_ += spillFile_->getSpillReadIOTime();
    size_ = 0;
    return;
  }
  size_ = rowVector_->size();
}

std::unique_ptr<TreeOfLosers<SpillMergeStream>>
SpillPartition::createOrderedReader(
    memory::MemoryPool* pool,
    bool spillUringEnabled) {
  std::vector<std::unique_ptr<SpillMergeStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    auto startCreateReadFile = getCurrentTimeMicro();
    auto spillReadFile =
        SpillReadFile::create(fileInfo, pool, spillUringEnabled);
    streams.push_back(FileSpillMergeStream::createWithInitTime(
        std::move(spillReadFile), getCurrentTimeMicro() - startCreateReadFile));
  }
  files_.clear();
  // Check if the partition is empty or not.
  if (FOLLY_UNLIKELY(streams.empty())) {
    return nullptr;
  }
  return std::make_unique<TreeOfLosers<SpillMergeStream>>(std::move(streams));
}

std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
SpillPartition::createRowBasedOrderedReader(
    memory::MemoryPool* pool,
    RowContainer* const rows,
    bool canJit,
    bool spillUringEnabled) {
#ifdef ENABLE_BOLT_JIT
  if (rows != nullptr && canJit && RowContainer::JITable(rows->keyTypes())) {
    auto cmpFlags = files_[0].sortFlags;
    if (cmpFlags.empty()) {
      cmpFlags.resize(rows->keyTypes().size(), CompareFlags());
    }
    auto [jitMod, fn] = rows->codegenCompare(
        rows->keyTypes(),
        cmpFlags,
        bytedance::bolt::jit::CmpType::CMP_SPILL,
        true);
    jitModuleRow_ = std::move(jitMod);
    rowCmpRowFunc_ = (RowRowCompare)jitModuleRow_->getFuncPtr(fn);
    LOG(INFO) << "JIT enabled for row based spill ordered reader!";
  }
#endif

  std::vector<std::unique_ptr<RowBasedSpillMergeStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    BOLT_CHECK(fileInfo.rowInfo.has_value());
    streams.push_back(RowBasedFileSpillMergeStream::create(
        RowBasedSpillReadFile::create(fileInfo, pool, spillUringEnabled),
        rowCmpRowFunc_));
  }
  files_.clear();
  // Check if the partition is empty or not.
  if (FOLLY_UNLIKELY(streams.empty())) {
    return nullptr;
  }
  return std::make_unique<TreeOfLosers<RowBasedSpillMergeStream>>(
      std::move(streams));
}

std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
SpillPartition::createRowBasedOrderedReaderWithLength(
    memory::MemoryPool* pool,
    RowContainer* const rows,
    bool canJit,
    bool spillUringEnabled) {
#ifdef ENABLE_BOLT_JIT
  if (rows != nullptr && canJit && RowContainer::JITable(rows->keyTypes())) {
    auto cmpFlags = files_[0].sortFlags;
    if (cmpFlags.empty()) {
      cmpFlags.resize(rows->keyTypes().size(), CompareFlags());
    }
    auto [jitMod, fn] = rows->codegenCompare(
        rows->keyTypes(),
        cmpFlags,
        bytedance::bolt::jit::CmpType::CMP_SPILL,
        true);
    jitModuleRow_ = std::move(jitMod);
    rowCmpRowFunc_ = (RowRowCompare)jitModuleRow_->getFuncPtr(fn);
    LOG(INFO) << "JIT enabled for row based spill ordered reader!";
  }
#endif

  std::vector<std::unique_ptr<RowBasedSpillMergeStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    BOLT_CHECK(fileInfo.rowInfo.has_value());
    streams.push_back(RowBasedFileSpillMergeStream::createWithLength(
        RowBasedSpillReadFile::create(fileInfo, pool, spillUringEnabled),
        rowCmpRowFunc_));
  }
  files_.clear();
  // Check if the partition is empty or not.
  if (FOLLY_UNLIKELY(streams.empty())) {
    return nullptr;
  }
  return std::make_unique<TreeOfLosers<RowBasedSpillMergeStream>>(
      std::move(streams));
}

SpillPartitionIdSet toSpillPartitionIdSet(
    const SpillPartitionSet& partitionSet) {
  SpillPartitionIdSet partitionIdSet;
  partitionIdSet.reserve(partitionSet.size());
  for (auto& partitionEntry : partitionSet) {
    partitionIdSet.insert(partitionEntry.first);
  }
  return partitionIdSet;
}

tsan_atomic<int32_t>& testingSpillPct() {
  static tsan_atomic<int32_t> spillPct = 0;
  return spillPct;
}

tsan_atomic<int32_t>& testingSpillCounter() {
  static tsan_atomic<int32_t> spillCounter = 0;
  return spillCounter;
}

TestScopedSpillInjection::TestScopedSpillInjection(
    int32_t spillPct,
    int32_t maxInjections) {
  BOLT_CHECK_EQ(testingSpillCounter(), 0);
  testingSpillPct() = spillPct;
  testingSpillCounter() = maxInjections;
}

TestScopedSpillInjection::~TestScopedSpillInjection() {
  testingSpillPct() = 0;
  testingSpillCounter() = 0;
}

bool testingTriggerSpill() {
  // Do not evaluate further if trigger is not set.
  if (testingSpillCounter() <= 0 || testingSpillPct() <= 0) {
    return false;
  }
  if (folly::Random::rand32() % 100 < testingSpillPct()) {
    return testingSpillCounter()-- > 0;
  }
  return false;
}
} // namespace bytedance::bolt::exec
