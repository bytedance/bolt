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
#include "bolt/exec/ContainerRow2RowSerde.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/serializers/PrestoSerializer.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::exec {
namespace {
void removeSpillFileNoThrow(const std::string& path) noexcept {
  if (path.empty()) {
    return;
  }
  try {
    auto fs = filesystems::getFileSystem(path, nullptr);
    if (fs->exists(path)) {
      fs->remove(path);
    }
  } catch (const std::exception& error) {
    LOG(WARNING) << "Failed to remove spill file '" << path
                 << "': " << error.what();
  } catch (...) {
    LOG(WARNING) << "Failed to remove spill file '" << path << "'";
  }
}

void removeSpillFilesNoThrow(const SpillFiles& files) noexcept {
  for (const auto& file : files) {
    removeSpillFileNoThrow(file.path);
  }
}
} // namespace

void SpillMergeStream::pop() {
  BOLT_CHECK(!closed_);
  if (++index_ >= size_) {
    setNextBatch();
  }
}

int32_t SpillMergeStream::compare(const MergeStream& other) const {
  BOLT_CHECK(!closed_);
  const auto& otherStream = static_cast<const SpillMergeStream&>(other);
  const auto& children = rowVector_->children();
  const auto& otherChildren = otherStream.current().children();
  for (const auto& [key, compareFlags] : sortingKeys()) {
    const auto result = children[key]
                            ->compare(
                                otherChildren[key].get(),
                                index_,
                                otherStream.index_,
                                compareFlags)
                            .value();
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

void SpillMergeStream::close() {
  BOLT_CHECK(!closed_);
  closed_ = true;
  rowVector_.reset();
  decoded_.clear();
  rows_.resize(0);
  index_ = 0;
  size_ = 0;
}

SpillState::SpillState(
    const common::SpillConfig::SpillIOConfig& ioConfig,
    int32_t maxPartitions,
    const std::vector<SpillSortKey>& sortingKeys,
    uint64_t targetFileSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : SpillState(
          ioConfig,
          maxPartitions,
          sortingKeys,
          targetFileSize,
          pool,
          stats,
          true) {}

SpillState::SpillState(
    const common::SpillConfig::SpillIOConfig& ioConfig,
    int32_t maxPartitions,
    const std::vector<SpillSortKey>& sortingKeys,
    uint64_t targetFileSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    bool countSpilledPartition)
    : ioConfig_(ioConfig),
      maxPartitions_(maxPartitions),
      sortingKeys_(sortingKeys),
      targetFileSize_(targetFileSize),
      countSpilledPartition_(countSpilledPartition),
      pool_(pool),
      stats_(stats),
      partitionWriters_(maxPartitions_) {
  spilledRowCount_.resize(maxPartitions_, 0);
}

std::vector<SpillSortKey> SpillState::makeSortingKeys(
    const std::vector<CompareFlags>& compareFlags) {
  std::vector<SpillSortKey> sortingKeys;
  sortingKeys.reserve(compareFlags.size());
  for (column_index_t i = 0; i < compareFlags.size(); ++i) {
    sortingKeys.emplace_back(i, compareFlags[i]);
  }
  return sortingKeys;
}

std::vector<SpillSortKey> SpillState::makeSortingKeys(
    const std::vector<column_index_t>& indices,
    const std::vector<CompareFlags>& compareFlags) {
  BOLT_CHECK(!indices.empty());
  BOLT_CHECK_EQ(indices.size(), compareFlags.size());
  std::vector<SpillSortKey> sortingKeys;
  sortingKeys.reserve(indices.size());
  for (auto i = 0; i < indices.size(); i++) {
    sortingKeys.emplace_back(indices[i], compareFlags[i]);
  }
  return sortingKeys;
}

void SpillState::setPartitionSpilled(uint32_t partition) {
  BOLT_DCHECK_LT(partition, maxPartitions_);
  BOLT_DCHECK_LT(spilledPartitionSet_.size(), maxPartitions_);
  BOLT_DCHECK(!spilledPartitionSet_.contains(partition));
  spilledPartitionSet_.insert(partition);
  if (countSpilledPartition_) {
    ++stats_->wlock()->spilledPartitions;
    common::incrementGlobalSpilledPartitionStats();
  }
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

  BOLT_TEST_ADJUST(
      "bytedance::bolt::exec::SpillState::appendToPartition", this);

  BOLT_CHECK_NOT_NULL(
      ioConfig_.getSpillDirPathCb, "Spill directory callback not specified.");
  const std::string& spillDir = ioConfig_.getSpillDirPathCb();
  BOLT_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (partitionWriters_.at(partition) == nullptr) {
    partitionWriters_[partition] = std::make_unique<SpillWriter>(
        std::static_pointer_cast<const RowType>(rows->type()),
        sortingKeys_,
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
  return appendToPartition(
      partition,
      folly::Range<char* const*>(rows.data(), rows.size()),
      std::move(type),
      info);
}

uint64_t SpillState::appendToPartition(
    uint32_t partition,
    folly::Range<char* const*> rows,
    RowTypePtr type,
    const RowFormatInfo& info) {
  BOLT_CHECK(
      isPartitionSpilled(partition), "Partition {} is not spilled", partition);

  BOLT_TEST_ADJUST(
      "bytedance::bolt::exec::SpillState::appendToPartition", this);

  BOLT_CHECK_NOT_NULL(
      ioConfig_.getSpillDirPathCb, "Spill directory callback not specified.");
  const std::string& spillDir = ioConfig_.getSpillDirPathCb();
  BOLT_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (partitionWriters_.at(partition) == nullptr) {
    partitionWriters_[partition] = std::make_unique<SpillWriter>(
        type,
        sortingKeys_,
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

void SpillState::cleanupPartitionFilesNoThrow(uint32_t partition) noexcept {
  if (partition >= partitionWriters_.size() ||
      partitionWriters_[partition] == nullptr) {
    return;
  }
  partitionWriters_[partition]->cleanupFilesNoThrow();
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
  BOLT_CHECK(!closed_);
  return spillFile_->id();
}

std::unique_ptr<SpillMergeStream> ConcatFilesSpillMergeStream::create(
    uint32_t id,
    std::vector<std::unique_ptr<SpillReadFile>> spillFiles) {
  auto spillStream = std::unique_ptr<ConcatFilesSpillMergeStream>(
      new ConcatFilesSpillMergeStream(id, std::move(spillFiles)));
  spillStream->nextBatch();
  return spillStream;
}

uint32_t ConcatFilesSpillMergeStream::id() const {
  return id_;
}

void ConcatFilesSpillMergeStream::nextBatch() {
  BOLT_CHECK(!closed_);
  index_ = 0;
  for (; fileIndex_ < spillFiles_.size(); ++fileIndex_) {
    BOLT_CHECK_NOT_NULL(spillFiles_[fileIndex_]);
    if (spillFiles_[fileIndex_]->nextBatch(rowVector_)) {
      BOLT_CHECK_NOT_NULL(rowVector_);
      size_ = rowVector_->size();
      return;
    }
    spillFiles_[fileIndex_].reset();
  }
  size_ = 0;
  close();
}

void ConcatFilesSpillMergeStream::close() {
  BOLT_CHECK(!closed_);
  SpillMergeStream::close();
  spillFiles_.clear();
}

const std::vector<SpillSortKey>& ConcatFilesSpillMergeStream::sortingKeys()
    const {
  BOLT_CHECK(!closed_);
  return spillFiles_[fileIndex_]->sortingKeys();
}

std::unique_ptr<BatchStream> ConcatFilesSpillBatchStream::create(
    std::vector<std::unique_ptr<SpillReadFile>> spillFiles) {
  auto* spillStream = new ConcatFilesSpillBatchStream(std::move(spillFiles));
  return std::unique_ptr<BatchStream>(spillStream);
}

bool ConcatFilesSpillBatchStream::nextBatch(RowVectorPtr& batch) {
  TestValue::adjust(
      "bytedance::bolt::exec::ConcatFilesSpillBatchStream::nextBatch", nullptr);
  BOLT_CHECK_NULL(batch);
  BOLT_CHECK(!atEnd_);
  for (; fileIndex_ < spillFiles_.size(); ++fileIndex_) {
    BOLT_CHECK_NOT_NULL(spillFiles_[fileIndex_]);
    if (spillFiles_[fileIndex_]->nextBatch(batch)) {
      BOLT_CHECK_NOT_NULL(batch);
      return true;
    }
    spillFiles_[fileIndex_].reset();
  }
  spillFiles_.clear();
  atEnd_ = true;
  return false;
}

FileSpillMergeStream::~FileSpillMergeStream() {
  if (spillFile_ != nullptr) {
    const auto path = spillFile_->testingFilePath();
    spillFile_.reset();
    removeSpillFileNoThrow(path);
  }
}

void FileSpillMergeStream::nextBatch() {
  BOLT_CHECK(!closed_);
  MicrosecondTimer timer(&spillReadTimeUs_);
  index_ = 0;
  if (!spillFile_->nextBatch(rowVector_)) {
    spillReadIOTimeUs_ += spillFile_->getSpillReadIOTime();
    size_ = 0;
    close();
    return;
  }
  size_ = rowVector_->size();
}

void FileSpillMergeStream::close() {
  if (closed_) {
    return;
  }
  SpillMergeStream::close();
  std::string filePath = spillFile_->testingFilePath();
  spillFile_.reset();
  removeSpillFileNoThrow(filePath);
}

RowBasedSpillMergeStream::RowBasedSpillMergeStream(
    RowTypePtr rowType,
    std::vector<RowColumn> rowColumns,
    std::vector<SpillSortKey> sortingKeys,
    DistinctProvenance provenance,
    AggregationRowOrigin origin,
    RowRowCompare compare)
    : rowType_(std::move(rowType)),
      rowColumns_(std::move(rowColumns)),
      sortingKeys_(std::move(sortingKeys)),
      provenance_(provenance),
      origin_(origin),
      compare_(compare) {
  BOLT_CHECK_NOT_NULL(rowType_);
  BOLT_CHECK_EQ(rowColumns_.size(), rowType_->size());
}

int32_t RowBasedSpillMergeStream::compare(const MergeStream& other) const {
  const auto& otherStream = static_cast<const RowBasedSpillMergeStream&>(other);
  char* left = rowVector_[index_];
  char* right = otherStream.current()[otherStream.currentIndex()];
  if (compare_ != nullptr && origin_ == otherStream.origin_) {
    return compare_(left, right);
  }
  const bool hasRowContainer = origin_ == AggregationRowOrigin::kRowContainer ||
      otherStream.origin_ == AggregationRowOrigin::kRowContainer;
  for (const auto& [key, flags] : sortingKeys_) {
    const auto& type = rowType_->childAt(key);
    if (!type->isFixedWidth() && hasRowContainer) {
      const auto leftColumn = rowColumns_[key];
      const auto rightColumn = otherStream.rowColumns_[key];
      const bool leftNull = RowContainer::isNullAt(left, leftColumn);
      const bool rightNull = RowContainer::isNullAt(right, rightColumn);
      if (leftNull || rightNull) {
        if (leftNull != rightNull) {
          return leftNull == flags.nullsFirst ? -1 : 1;
        }
        continue;
      }
      int32_t result;
      if (type->kind() == TypeKind::VARCHAR ||
          type->kind() == TypeKind::VARBINARY) {
        auto leftValue =
            RowContainer::valueAt<StringView>(left, leftColumn.offset());
        auto rightValue =
            RowContainer::valueAt<StringView>(right, rightColumn.offset());
        std::string leftStorage, rightStorage;
        if (origin_ == AggregationRowOrigin::kRowContainer) {
          leftValue =
              HashStringAllocator::contiguousString(leftValue, leftStorage);
        }
        if (otherStream.origin_ == AggregationRowOrigin::kRowContainer) {
          rightValue =
              HashStringAllocator::contiguousString(rightValue, rightStorage);
        }
        result = leftValue.compare(rightValue);
        if (!flags.ascending) {
          result = -result;
        }
      } else {
        const auto streamFor =
            [](char* row, RowColumn column, AggregationRowOrigin origin) {
              if (origin == AggregationRowOrigin::kRowContainer) {
                return RowContainer::prepareRead(row, column.offset());
              }
              const auto value =
                  RowContainer::valueAt<std::string_view>(row, column.offset());
              return std::make_unique<ByteInputStream>(std::vector<ByteRange>{
                  {reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
                   static_cast<int32_t>(value.size()),
                   0}});
            };
        auto leftStream = streamFor(left, leftColumn, origin_);
        auto rightStream = streamFor(right, rightColumn, otherStream.origin_);
        result = ContainerRowSerde::compare(
            *leftStream, *rightStream, type.get(), flags);
      }
      if (result != 0) {
        return result;
      }
      continue;
    }
    const auto result = BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
        compareByRow,
        type->kind(),
        left,
        right,
        rowColumns_[key],
        otherStream.rowColumns_[key],
        flags,
        type.get());
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

uint64_t RowBasedSpillMergeStream::serializedRowSize() const {
  if (!rowLengths_.empty()) {
    return rowLengths_[index_];
  }
  return origin_ == AggregationRowOrigin::kRowContainer
      ? info().getRowSize(rowVector_[index_])
      : ContainerRow2RowSerde::rowSize(rowVector_[index_], info());
}

RowBasedFileSpillMergeStream::~RowBasedFileSpillMergeStream() {
  close();
}

void RowBasedFileSpillMergeStream::close() noexcept {
  if (spillFile_ == nullptr) {
    return;
  }
  spillDecompressTimeUs_ += spillFile_->getSpillDecompressTime();
  spillReadIOTimeUs_ += spillFile_->getSpillReadIOTime();
  const auto path = spillFile_->testingFilePath();
  spillFile_.reset();
  rowVector_.clear();
  rowLengths_.clear();
  index_ = 0;
  removeSpillFileNoThrow(path);
}

void extractRowContainerSpillVector(
    RowContainer& container,
    const RowTypePtr& rowType,
    folly::Range<char* const*> rows,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  if (result == nullptr) {
    result = BaseVector::create<RowVector>(rowType, rows.size(), pool);
  } else {
    result->prepareForReuse();
    result->resize(rows.size());
  }
  auto** mutableRows = const_cast<char**>(rows.data());
  const auto mutableRange = folly::Range<char**>(mutableRows, rows.size());
  const auto& types = container.columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container.extractColumn(mutableRows, rows.size(), i, result->childAt(i));
  }
  const auto& accumulators = container.accumulators();
  for (auto i = 0; i < accumulators.size(); ++i) {
    accumulators[i].extractForSpill(
        mutableRange, result->childAt(i + types.size()));
  }
}

namespace {
class RowContainerSpillMergeStream final : public SpillMergeStream {
 public:
  RowContainerSpillMergeStream(
      RowContainer& container,
      RowTypePtr rowType,
      folly::Range<char* const*> rows,
      int32_t numSortKeys,
      std::vector<CompareFlags> sortCompareFlags,
      memory::MemoryPool* pool,
      DistinctProvenance provenance)
      : SpillMergeStream(provenance),
        container_(container),
        rowType_(std::move(rowType)),
        rows_(rows),
        sortCompareFlags_(std::move(sortCompareFlags)),
        pool_(pool) {
    BOLT_CHECK(
        sortCompareFlags_.empty() || sortCompareFlags_.size() == numSortKeys);
    sortingKeys_.reserve(numSortKeys);
    for (int32_t key = 0; key < numSortKeys; ++key) {
      sortingKeys_.emplace_back(
          key,
          sortCompareFlags_.empty() ? CompareFlags() : sortCompareFlags_[key]);
    }
    if (!rows_.empty()) {
      nextBatch();
    }
  }

  uint32_t id() const override {
    return std::numeric_limits<uint32_t>::max();
  }

  std::optional<uint64_t> memoryPosition() const override {
    return batchBegin_ + index_;
  }

  bool isNextEqual() const override {
    const auto position = *memoryPosition();
    return position + 1 < rows_.size() &&
        container_.compareRows(
            rows_[position], rows_[position + 1], sortCompareFlags_) == 0;
  }

 private:
  const std::vector<SpillSortKey>& sortingKeys() const override {
    return sortingKeys_;
  }

  void nextBatch() override {
    static constexpr vector_size_t kMaxRows = 64;
    BOLT_CHECK(!closed_);
    batchBegin_ += size_;
    if (batchBegin_ >= rows_.size()) {
      SpillMergeStream::close();
      return;
    }
    const auto count = std::min<uint64_t>(kMaxRows, rows_.size() - batchBegin_);
    extractRowContainerSpillVector(
        container_,
        rowType_,
        rows_.subpiece(batchBegin_, count),
        pool_,
        rowVector_);
    index_ = 0;
    size_ = count;
  }

  RowContainer& container_;
  const RowTypePtr rowType_;
  const folly::Range<char* const*> rows_;
  const std::vector<CompareFlags> sortCompareFlags_;
  std::vector<SpillSortKey> sortingKeys_;
  memory::MemoryPool* const pool_;
  uint64_t batchBegin_{0};
};

class RowContainerRowBasedSpillMergeStream final
    : public RowBasedSpillMergeStream {
 public:
  RowContainerRowBasedSpillMergeStream(
      RowContainer& container,
      RowTypePtr rowType,
      folly::Range<char* const*> rows,
      int32_t numSortKeys,
      std::vector<CompareFlags> sortCompareFlags,
      RowFormatInfo rowInfo,
      DistinctProvenance provenance)
      : RowBasedSpillMergeStream(
            std::move(rowType),
            rowInfo.rowColumns,
            SpillState::makeSortingKeys([&]() {
              if (sortCompareFlags.empty()) {
                sortCompareFlags.resize(numSortKeys);
              }
              return sortCompareFlags;
            }()),
            provenance,
            AggregationRowOrigin::kRowContainer),
        container_(container),
        rows_(rows),
        sortCompareFlags_(std::move(sortCompareFlags)),
        rowInfo_(std::move(rowInfo)) {
    if (!rows_.empty()) {
      nextBatch();
    }
  }

  uint32_t id() const override {
    return std::numeric_limits<uint32_t>::max();
  }

  std::optional<uint64_t> memoryPosition() const override {
    return batchBegin_ + index_;
  }

  bool isNextEqual() const override {
    const auto position = *memoryPosition();
    return position + 1 < rows_.size() &&
        container_.compareRows(
            rows_[position], rows_[position + 1], sortCompareFlags_) == 0;
  }

 private:
  void nextBatch() override {
    loadBatch(false);
  }

  void nextBatchWithLengths() override {
    loadBatch(true);
  }

  void loadBatch(bool withLengths) {
    static constexpr vector_size_t kMaxRows = 64;
    batchBegin_ += rowVector_.size();
    index_ = 0;
    rowVector_.clear();
    rowLengths_.clear();
    if (batchBegin_ >= rows_.size()) {
      return;
    }
    const auto count = std::min<uint64_t>(kMaxRows, rows_.size() - batchBegin_);
    rowVector_.reserve(count);
    if (withLengths) {
      rowLengths_.reserve(count);
    }
    for (uint64_t i = 0; i < count; ++i) {
      auto* row = rows_[batchBegin_ + i];
      rowVector_.push_back(row);
      if (withLengths) {
        rowLengths_.push_back(rowInfo_.getRowSize(row));
      }
    }
  }

  const RowFormatInfo& info() const override {
    return rowInfo_;
  }

  RowContainer& container_;
  const folly::Range<char* const*> rows_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const RowFormatInfo rowInfo_;
  uint64_t batchBegin_{0};
};
} // namespace

std::unique_ptr<SpillMergeStream> makeRowContainerSpillMergeStream(
    RowContainer& container,
    const RowTypePtr& rowType,
    folly::Range<char* const*> rows,
    int32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    memory::MemoryPool* pool,
    DistinctProvenance provenance) {
  return std::make_unique<RowContainerSpillMergeStream>(
      container,
      rowType,
      rows,
      numSortKeys,
      sortCompareFlags,
      pool,
      provenance);
}

std::unique_ptr<RowBasedSpillMergeStream>
makeRowContainerRowBasedSpillMergeStream(
    RowContainer& container,
    const RowTypePtr& rowType,
    folly::Range<char* const*> rows,
    int32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const RowFormatInfo& rowInfo,
    DistinctProvenance provenance) {
  return std::make_unique<RowContainerRowBasedSpillMergeStream>(
      container,
      rowType,
      rows,
      numSortKeys,
      sortCompareFlags,
      rowInfo,
      provenance);
}

OwnedSpillPartition::OwnedSpillPartition(SpillPartition&& partition)
    : files_(partition.takeFiles()) {}

void OwnedSpillPartition::addFiles(SpillFiles files) {
  auto cleanup = folly::makeGuard([&]() { removeSpillFilesNoThrow(files); });
  files_.insert(
      files_.end(),
      std::make_move_iterator(files.begin()),
      std::make_move_iterator(files.end()));
  cleanup.dismiss();
}

OwnedSpillPartition::~OwnedSpillPartition() {
  for (const auto& file : files_) {
    removeSpillFileNoThrow(file.path);
  }
}

std::vector<std::unique_ptr<SpillMergeStream>>
OwnedSpillPartition::takeOrderedStreams(
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    DistinctProvenance provenance,
    size_t alreadyOutputFiles) {
  std::vector<std::unique_ptr<SpillMergeStream>> streams;
  streams.reserve(files_.size());
  size_t fileIndex = 0;
  for (auto& file : files_) {
    auto start = getCurrentTimeMicro();
    auto reader = SpillReadFile::create(file, pool, spillUringEnabled);
    auto stream = FileSpillMergeStream::createWithInitTime(
        std::move(reader),
        getCurrentTimeMicro() - start,
        fileIndex++ < alreadyOutputFiles ? DistinctProvenance::kAlreadyOutput
                                         : provenance);
    streams.push_back(std::move(stream));
    file.path.clear();
  }
  files_.clear();
  return streams;
}

std::vector<std::unique_ptr<RowBasedSpillMergeStream>>
OwnedSpillPartition::takeRowBasedOrderedStreams(
    memory::MemoryPool* pool,
    RowContainer* rows,
    bool canJit,
    bool spillUringEnabled,
    bool withLengths,
    DistinctProvenance provenance,
    size_t alreadyOutputFiles) {
#ifdef ENABLE_BOLT_JIT
  bolt::jit::CompiledModuleSP jitModule;
  if (rows != nullptr && canJit && RowContainer::JITable(rows->keyTypes()) &&
      !files_.empty()) {
    std::vector<CompareFlags> flags;
    flags.reserve(files_.front().sortingKeys.size());
    for (const auto& [_, compareFlags] : files_.front().sortingKeys) {
      flags.push_back(compareFlags);
    }
    if (flags.empty()) {
      flags.resize(rows->keyTypes().size(), CompareFlags());
    }
    jitModule = std::get<0>(rows->codegenCompare(
        rows->keyTypes(),
        flags,
        bytedance::bolt::jit::CmpType::CMP_SPILL,
        true));
  }
#endif
  std::vector<std::unique_ptr<RowBasedSpillMergeStream>> streams;
  streams.reserve(files_.size());
  size_t fileIndex = 0;
  for (auto& file : files_) {
    BOLT_CHECK(file.rowInfo.has_value());
    auto reader = RowBasedSpillReadFile::create(file, pool, spillUringEnabled);
    const auto fileProvenance = fileIndex++ < alreadyOutputFiles
        ? DistinctProvenance::kAlreadyOutput
        : provenance;
    auto stream = withLengths ? RowBasedFileSpillMergeStream::createWithLength(
                                    std::move(reader)
#ifdef ENABLE_BOLT_JIT
                                        ,
                                    jitModule
#endif
                                    ,
                                    fileProvenance)
                              : RowBasedFileSpillMergeStream::create(
                                    std::move(reader)
#ifdef ENABLE_BOLT_JIT
                                        ,
                                    jitModule
#endif
                                    ,
                                    fileProvenance);
    streams.push_back(std::move(stream));
    file.path.clear();
  }
  files_.clear();
  return streams;
}

std::unique_ptr<TreeOfLosers<SpillMergeStream>>
SpillPartition::createOrderedReader(
    memory::MemoryPool* pool,
    bool spillUringEnabled) {
  if (FOLLY_UNLIKELY(files_.empty())) {
    return nullptr;
  }
  OwnedSpillPartition owned(std::move(*this));
  auto streams = owned.takeOrderedStreams(
      pool, spillUringEnabled, DistinctProvenance::kNew);
  return std::make_unique<TreeOfLosers<SpillMergeStream>>(std::move(streams));
}

std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
SpillPartition::createRowBasedOrderedReader(
    memory::MemoryPool* pool,
    RowContainer* const rows,
    bool canJit,
    bool spillUringEnabled) {
  if (FOLLY_UNLIKELY(files_.empty())) {
    return nullptr;
  }
  OwnedSpillPartition owned(std::move(*this));
  auto streams = owned.takeRowBasedOrderedStreams(
      pool, rows, canJit, spillUringEnabled, false, DistinctProvenance::kNew);
  return std::make_unique<TreeOfLosers<RowBasedSpillMergeStream>>(
      std::move(streams));
}

std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
SpillPartition::createRowBasedOrderedReaderWithLength(
    memory::MemoryPool* pool,
    RowContainer* const rows,
    bool canJit,
    bool spillUringEnabled) {
  if (FOLLY_UNLIKELY(files_.empty())) {
    return nullptr;
  }
  OwnedSpillPartition owned(std::move(*this));
  auto streams = owned.takeRowBasedOrderedStreams(
      pool, rows, canJit, spillUringEnabled, true, DistinctProvenance::kNew);
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
