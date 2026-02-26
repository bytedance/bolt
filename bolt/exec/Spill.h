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

#include <folly/container/F14Set.h>
#include <cstdint>
#include <optional>
#include <vector>
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/RowBasedCompare.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/SpillFile.h"
#include "bolt/exec/TreeOfLosers.h"
#include "bolt/exec/UnorderedStreamReader.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/VectorStream.h"
namespace bytedance::bolt::exec {

// A source of sorted spilled RowVectors coming either from a file or memory.
class SpillMergeStream : public MergeStream {
 public:
  SpillMergeStream() = default;
  virtual ~SpillMergeStream() = default;

  /// Returns the id of a spill merge stream which is unique in the merge set.
  virtual uint32_t id() const = 0;

  bool hasData() const final {
    return index_ < size_;
  }

  bool operator<(const MergeStream& other) const final {
    return compare(other) < 0;
  }

  int32_t compare(const MergeStream& other) const override;

  void pop();

  const RowVector& current() const {
    return *rowVector_;
  }

  virtual void prefetch(){};

  /// Invoked to get the current row index in 'rowVector_'. If 'isLastRow' is
  /// not null, it is set to true if current row is the last one in the current
  /// batch, in which case the caller must call copy out current batch data if
  /// required before calling pop().
  vector_size_t currentIndex(bool* isLastRow = nullptr) const {
    if (isLastRow != nullptr) {
      *isLastRow = (index_ == (rowVector_->size() - 1));
    }
    return index_;
  }

  /// Returns a DecodedVector set decoding the 'index'th child of 'rowVector_'
  DecodedVector& decoded(int32_t index) {
    ensureDecodedValid(index);
    return decoded_[index];
  }

  int64_t getSpillReadTime() const {
    return spillReadTimeUs_;
  }

  int64_t getSpillDecompressTime() const {
    return spillDecompressTimeUs_;
  }

  int64_t getSpillReadIOTime() const {
    return spillReadIOTimeUs_;
  }

 protected:
  virtual int32_t numSortKeys() const = 0;

  virtual const std::vector<CompareFlags>& sortCompareFlags() const = 0;

  virtual void nextBatch() = 0;

  // loads the next 'rowVector' and sets 'decoded_' if this is initialized.
  void setNextBatch() {
    nextBatch();
    if (!decoded_.empty()) {
      ensureRows();
      for (auto i = 0; i < decoded_.size(); ++i) {
        decoded_[i].decode(*rowVector_->childAt(i), rows_);
      }
    }
  }

  void ensureDecodedValid(int32_t index) {
    int32_t oldSize = decoded_.size();
    if (index < oldSize) {
      return;
    }
    ensureRows();
    decoded_.resize(index + 1);
    for (auto i = oldSize; i <= index; ++i) {
      decoded_[index].decode(*rowVector_->childAt(index), rows_);
    }
  }

  void ensureRows() {
    if (rows_.size() != rowVector_->size()) {
      rows_.resize(size_);
    }
  }

  // Current batch of rows.
  RowVectorPtr rowVector_;

  // The current row in 'rowVector_'
  vector_size_t index_{0};

  // Number of rows in 'rowVector_'
  vector_size_t size_{0};

  uint64_t avgRowSize_{0};

  // Decoded vectors for leading parts of 'rowVector_'. Initialized on first
  // use and maintained when updating 'rowVector_'.
  std::vector<DecodedVector> decoded_;

  // Covers all rows inn 'rowVector_' Set if 'decoded_' is non-empty.
  SelectivityVector rows_;
};

// A source of spilled RowVectors coming from a file.
class FileSpillMergeStream : public SpillMergeStream {
 public:
  static std::unique_ptr<SpillMergeStream> create(
      std::unique_ptr<SpillReadFile> spillFile,
      size_t initTime) {
    auto* spillStream = new FileSpillMergeStream(std::move(spillFile));
    spillStream->spillReadTimeUs_ += initTime;
    spillStream->nextBatch();
    return std::unique_ptr<SpillMergeStream>(spillStream);
  }

  static std::unique_ptr<SpillMergeStream> createWithInitTime(
      std::unique_ptr<SpillReadFile> spillFile,
      uint64_t initTime) {
    auto* spillStream = new FileSpillMergeStream(std::move(spillFile));
    spillStream->spillReadIOTimeUs_ += initTime;
    spillStream->nextBatch();
    return std::unique_ptr<SpillMergeStream>(spillStream);
  }

  void prefetch() override {
    spillFile_->prefetch();
  }

  uint32_t id() const override;

  ~FileSpillMergeStream() {
    std::string filePath = spillFile_->testingFilePath();
    spillFile_.reset();
    auto fs = filesystems::getFileSystem(filePath, nullptr);
    fs->remove(filePath);
  }

 private:
  explicit FileSpillMergeStream(std::unique_ptr<SpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    BOLT_CHECK_NOT_NULL(spillFile_);
  }

  int32_t numSortKeys() const override {
    return spillFile_->numSortKeys();
  }

  const std::vector<CompareFlags>& sortCompareFlags() const override {
    return spillFile_->sortCompareFlags();
  }

  void nextBatch() override;

  std::unique_ptr<SpillReadFile> spillFile_;
};

// A source of sorted spilled rows coming either from a file or memory.
class RowBasedSpillMergeStream : public MergeStream {
 public:
  RowBasedSpillMergeStream() = default;
  virtual ~RowBasedSpillMergeStream() = default;

  /// Returns the id of a spill merge stream which is unique in the merge set.
  virtual uint32_t id() const = 0;

  virtual void prefetch(){};

  bool hasData() const final {
    return index_ < rowVector_.size();
  }

  bool operator<(const MergeStream& other) const final {
    return compare(other) < 0;
  }

  int32_t compare(const MergeStream& other) const override {
    BOLT_NYI("Not implemented RowBasedSpillMergeStream's compare");
  }

  virtual const std::vector<RowColumn>& rowColumns() const = 0;

  virtual const RowTypePtr rowType() const = 0;

  void pop() {
    if (++index_ >= rowVector_.size()) {
      setNextBatch();
    }
  }

  void popWithLengths() {
    if (++index_ >= rowVector_.size()) {
      setNextBatchWithLengths();
    }
  }

  const RowFormatInfo& getRowFormatInfo() {
    return info();
  }

  // return reference
  const std::vector<char*>& current() const {
    return rowVector_;
  }

  virtual uint64_t getSpillReadIOTime() {
    return 0;
  }

  const std::vector<size_t>& currentLengths() const {
    return rowLengths_;
  }

  /// Invoked to get the current row index in 'rowVector_'. If 'isLastRow' is
  /// not null, it is set to true if current row is the last one in the current
  /// batch, in which case the caller must call copy out current batch data if
  /// required before calling pop().
  vector_size_t currentIndex(bool* isLastRow = nullptr) const {
    if (isLastRow != nullptr) {
      *isLastRow = (index_ == rowVector_.size() - 1);
    }
    return index_;
  }

  vector_size_t currentIndex(bool* isLastRow, int32_t& rowSize) const {
    if (LIKELY(isLastRow != nullptr)) {
      *isLastRow = (index_ == rowVector_.size() - 1);
      rowSize = *isLastRow ? 0 : rowVector_[index_ + 1] - rowVector_[index_];
    }
    return index_;
  }

  /// Returns a DecodedVector set decoding the 'index'th child of 'rowVector_'
  DecodedVector& decoded(int32_t index) {
    BOLT_NYI("RowBasedSpillMergeStream decode isn't implemented");
  }

  int64_t getSpillReadTime() const {
    return spillReadTimeUs_;
  }

  int64_t getSpillDecompressTime() const {
    return spillDecompressTimeUs_;
  }

 protected:
  virtual int32_t numSortKeys() const = 0;

  virtual const std::vector<CompareFlags>& sortCompareFlags() const = 0;

  virtual void nextBatch() = 0;

  virtual void nextBatchWithLengths() = 0;

  virtual const RowFormatInfo& info() const = 0;

  // loads the next 'rowVector'
  void setNextBatch() {
    nextBatch();
  }

  void setNextBatchWithLengths() {
    nextBatchWithLengths();
  }

  void ensureDecodedValid(int32_t index) {
    // noop
  }

  void ensureRows() {
    // noop
  }

  // Current batch of rows.
  std::vector<char*> rowVector_;

  // Lengths of rows in current batch.
  std::vector<size_t> rowLengths_;

  // The current row in 'rowVector_'
  vector_size_t index_{0};
};

// A source of spilled rows coming from a file.
class RowBasedFileSpillMergeStream : public RowBasedSpillMergeStream {
 public:
  static std::unique_ptr<RowBasedSpillMergeStream> create(
      std::unique_ptr<RowBasedSpillReadFile> spillFile,
      RowRowCompare cmp) {
    auto* spillStream =
        new RowBasedFileSpillMergeStream(std::move(spillFile), cmp);
    spillStream->nextBatch();
    return std::unique_ptr<RowBasedSpillMergeStream>(spillStream);
  }

  static std::unique_ptr<RowBasedSpillMergeStream> createWithLength(
      std::unique_ptr<RowBasedSpillReadFile> spillFile,
      RowRowCompare cmp) {
    auto* spillStream =
        new RowBasedFileSpillMergeStream(std::move(spillFile), cmp);
    spillStream->nextBatchWithLengths();
    return std::unique_ptr<RowBasedSpillMergeStream>(spillStream);
  }

  int32_t compare(const MergeStream& other) const override {
    auto& otherStream = static_cast<const RowBasedFileSpillMergeStream&>(other);
    const std::vector<RowColumn>& leftRowColumns = spillFile_->rowColumns();
    const std::vector<RowColumn>& rightRowColumns =
        otherStream.spillFile_->rowColumns();
    RowTypePtr rowType = spillFile_->type();
    int32_t key = 0;
    char* left = rowVector_[index_];
    char* right = otherStream.current()[otherStream.currentIndex()];
    if (cmp_) {
      return cmp_(left, right);
    } else {
      if (sortCompareFlags().empty()) {
        do {
          auto result = BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
              compareByRow,
              rowType->childAt(key)->kind(),
              left,
              right,
              leftRowColumns[key],
              rightRowColumns[key],
              CompareFlags(),
              rowType->childAt(key).get());
          if (result != 0) {
            return result;
          }
        } while (++key < numSortKeys());
      } else {
        do {
          auto result = BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
              compareByRow,
              rowType->childAt(key)->kind(),
              left,
              right,
              leftRowColumns[key],
              rightRowColumns[key],
              sortCompareFlags()[key],
              rowType->childAt(key).get());
          if (result != 0) {
            return result;
          }
        } while (++key < numSortKeys());
      }
    }
    return 0;
  }

  void prefetch() override {
    spillFile_->prefetch();
  }

  uint32_t id() const override {
    return spillFile_->id();
  }

  bool isNextEqual() const final {
    const auto offset = spillFile_->nextEqualOffset();
    return offset > 0 && bits::isBitSet(rowVector_[index_], offset);
  }

  uint64_t getSpillReadIOTime() override {
    return spillFile_->getSpillReadIOTime();
  }

 private:
  explicit RowBasedFileSpillMergeStream(
      std::unique_ptr<RowBasedSpillReadFile> spillFile,
      RowRowCompare cmp)
      : spillFile_(std::move(spillFile)), cmp_(cmp) {
    BOLT_CHECK_NOT_NULL(spillFile_);
  }

  int32_t numSortKeys() const override {
    return spillFile_->numSortKeys();
  }

  const std::vector<CompareFlags>& sortCompareFlags() const override {
    return spillFile_->sortCompareFlags();
  }

  void nextBatch() override {
    MicrosecondTimer timer(&spillReadTimeUs_);
    index_ = 0;
    if (!spillFile_->nextBatch(rowVector_)) {
      spillDecompressTimeUs_ += spillFile_->getSpillDecompressTime();
      return;
    }
  }

  void nextBatchWithLengths() override {
    MicrosecondTimer timer(&spillReadTimeUs_);
    index_ = 0;
    if (!spillFile_->nextBatch(rowVector_, rowLengths_)) {
      spillDecompressTimeUs_ += spillFile_->getSpillDecompressTime();
      return;
    }
  }

  const RowFormatInfo& info() const override {
    return spillFile_->info();
  }

  const std::vector<RowColumn>& rowColumns() const override {
    return spillFile_->rowColumns();
  }

  const RowTypePtr rowType() const override {
    return spillFile_->type();
  }

  std::unique_ptr<RowBasedSpillReadFile> spillFile_;
#ifdef ENABLE_BOLT_JIT
  bolt::jit::CompiledModuleSP jitModule_;
#endif
  RowRowCompare cmp_{nullptr};
};

/// A source of spilled RowVectors coming from a file. The spill data might not
/// be sorted.
///
/// NOTE: this object is not thread-safe.
class FileSpillBatchStream : public BatchStream {
 public:
  static std::unique_ptr<BatchStream> create(
      std::unique_ptr<SpillReadFile> spillFile) {
    auto* spillStream = new FileSpillBatchStream(std::move(spillFile));
    return std::unique_ptr<BatchStream>(spillStream);
  }

  bool nextBatch(RowVectorPtr& batch) override {
    return spillFile_->nextBatch(batch);
  }

  void reuse() override {
    spillFile_->reuse();
  }

  void prefetch() override {
    spillFile_->prefetch();
  }

  uint64_t getSpillReadIOTime() override {
    return spillFile_->getSpillReadIOTime();
  }

  ~FileSpillBatchStream() {
    std::string filePath = spillFile_->testingFilePath();
    spillFile_.reset();
    auto fs = filesystems::getFileSystem(filePath, nullptr);
    fs->remove(filePath);
  }

 private:
  explicit FileSpillBatchStream(std::unique_ptr<SpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    BOLT_CHECK_NOT_NULL(spillFile_);
  }

  std::unique_ptr<SpillReadFile> spillFile_;
};

class RowBasedFileSpillBatchStream : public BatchStream {
 public:
  static std::unique_ptr<BatchStream> create(
      std::unique_ptr<RowBasedSpillReadFile> spillFile) {
    auto* spillStream = new RowBasedFileSpillBatchStream(std::move(spillFile));
    return std::unique_ptr<BatchStream>(spillStream);
  }

  uint32_t nextBatch(std::vector<char*>& rows) override {
    return spillFile_->nextBatch(rows);
  }

  bool nextBatch(RowVectorPtr& batch) override {
    BOLT_UNREACHABLE("nextBatch: Can't called for row based spill");
  }

  void reuse() override {
    BOLT_UNREACHABLE("reuse: Can't called for row based spill");
  }

  ~RowBasedFileSpillBatchStream() {
    std::string filePath = spillFile_->testingFilePath();
    spillFile_.reset();
    auto fs = filesystems::getFileSystem(filePath, nullptr);
    fs->remove(filePath);
  }

 private:
  explicit RowBasedFileSpillBatchStream(
      std::unique_ptr<RowBasedSpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    BOLT_CHECK_NOT_NULL(spillFile_);
  }

  std::unique_ptr<RowBasedSpillReadFile> spillFile_;
};

/// Identifies a spill partition generated from a given spilling operator. It
/// consists of partition start bit offset and the actual partition number. The
/// start bit offset is used to calculate the partition number of spill data. It
/// is required for the recursive spilling handling as we advance the start bit
/// offset when we go to the next level of recursive spilling.
///
/// NOTE: multiple shards created from the same SpillPartition by split()
/// will share the same id.
class SpillPartitionId {
 public:
  static constexpr int32_t dummySubRangePartitionNumber = -1;

  SpillPartitionId(
      uint8_t partitionBitOffset,
      int32_t partitionNumber,
      int32_t subRangePartitionNumber = dummySubRangePartitionNumber,
      bool isLast = false)
      : partitionBitOffset_(partitionBitOffset),
        partitionNumber_(partitionNumber),
        subRangePartitionNumber_(subRangePartitionNumber),
        isLastSubRange_(isLast) {}

  bool operator==(const SpillPartitionId& other) const {
    return std::tie(
               partitionBitOffset_,
               partitionNumber_,
               subRangePartitionNumber_,
               isLastSubRange_) ==
        std::tie(
               other.partitionBitOffset_,
               other.partitionNumber_,
               other.subRangePartitionNumber_,
               other.isLastSubRange_);
  }

  bool operator!=(const SpillPartitionId& other) const {
    return !(*this == other);
  }

  /// Customize the compare operator for recursive spilling control. It ensures
  /// the partition with higher partition bit is handled prior than one with
  /// lower partition bit. With the same partition bit, the one with smaller
  /// partition number is handled first. We put all spill partitions in an
  /// ordered map sorted based on the partition id. The recursive spilling will
  /// advance the partition start bit when go to the next level of recursive
  /// spilling.
  bool operator<(const SpillPartitionId& other) const {
    if (partitionBitOffset_ != other.partitionBitOffset_) {
      return partitionBitOffset_ > other.partitionBitOffset_;
    }
    if (partitionNumber_ != other.partitionNumber_) {
      return partitionNumber_ < other.partitionNumber_;
    }
    return subRangePartitionNumber_ < other.subRangePartitionNumber_;
  }

  bool operator>(const SpillPartitionId& other) const {
    return (*this != other) && !(*this < other);
  }

  std::string toString() const {
    if (subRangePartitionNumber_ == dummySubRangePartitionNumber) {
      return fmt::format("[{},{}]", partitionBitOffset_, partitionNumber_);
    } else {
      return fmt::format(
          "[{},{},{},{}]",
          partitionBitOffset_,
          partitionNumber_,
          subRangePartitionNumber_,
          isLastSubRange_);
    }
  }

  uint8_t partitionBitOffset() const {
    return partitionBitOffset_;
  }

  int32_t partitionNumber() const {
    return partitionNumber_;
  }

  void setSubRangePartitionNumber(int32_t rangePartition, bool isLast) {
    subRangePartitionNumber_ = rangePartition;
    isLastSubRange_ = isLast;
  }

  int32_t subRangePartitionNumber() const {
    return subRangePartitionNumber_;
  }

  bool isLastSubRangePartition() const {
    return isLastSubRange_;
  }

  bool isSubRangePartiton() const {
    return subRangePartitionNumber_ != dummySubRangePartitionNumber;
  }

 private:
  uint8_t partitionBitOffset_{0};
  int32_t partitionNumber_{0};
  int32_t subRangePartitionNumber_{dummySubRangePartitionNumber};
  bool isLastSubRange_{false};
};

inline std::ostream& operator<<(std::ostream& os, SpillPartitionId id) {
  os << id.toString();
  return os;
}

using SpillPartitionIdSet = folly::F14FastSet<SpillPartitionId>;
using SpillPartitionNumSet = folly::F14FastSet<uint32_t>;
using SpillOffsetToBitsSet = const std::map<uint8_t, uint8_t>*;

/// Contains a spill partition data which includes the partition id and
/// corresponding spill files.
class SpillPartition {
 public:
  explicit SpillPartition(const SpillPartitionId& id)
      : SpillPartition(id, {}) {}

  SpillPartition(const SpillPartitionId& id, SpillFiles files) : id_(id) {
    addFiles(std::move(files));
  }

  void addFiles(SpillFiles files) {
    files_.reserve(files_.size() + files.size());
    for (auto& file : files) {
      size_ += file.size;
      rowCount_ += file.rowCount;
      files_.push_back(std::move(file));
    }
  }

  const SpillPartitionId& id() const {
    return id_;
  }

  int numFiles() const {
    return files_.size();
  }

  // TODO for testing, should remove before merge
  const SpillFiles& files() const {
    return files_;
  }

  /// Returns the total file byte size of this spilled partition.
  uint64_t size() const {
    return size_;
  }

  uint64_t rowCount() const {
    return rowCount_;
  }

  void setSubRangePartitionNumber(
      int32_t subRangePartitionNumber,
      bool isLast) {
    id_.setSubRangePartitionNumber(subRangePartitionNumber, isLast);
  }
  /// Invoked to split this spill partition into 'numShards' to process in
  /// parallel.
  ///
  /// NOTE: the split spill partition shards will have the same id as this.
  std::vector<std::unique_ptr<SpillPartition>> split(int numShards);

  /// Invoked to create an unordered stream reader from this spill partition.
  /// The created reader will take the ownership of the spill files.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> createUnorderedReader(
      memory::MemoryPool* pool,
      bool spillUringEnabled = false,
      bool isRowBased = false);

  /// Invoked to create an ordered stream reader from this spill partition.
  /// The created reader will take the ownership of the spill files.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> createOrderedReader(
      memory::MemoryPool* pool,
      bool spillUringEnabled = false);

  std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
  createRowBasedOrderedReader(
      memory::MemoryPool* pool,
      RowContainer* const container,
      bool canJit,
      bool spillUringEnabled);

  std::unique_ptr<TreeOfLosers<RowBasedSpillMergeStream>>
  createRowBasedOrderedReaderWithLength(
      memory::MemoryPool* pool,
      RowContainer* const container,
      bool canJit,
      bool spillUringEnabled);

  std::string toString() const;

 private:
  SpillPartitionId id_;
  SpillFiles files_;
  // Counts the total file size in bytes from this spilled partition.
  uint64_t size_{0};
  // Total row count from this spilled partition.
  uint64_t rowCount_{0};
  RowRowCompare rowCmpRowFunc_{nullptr};
#ifdef ENABLE_BOLT_JIT
  bolt::jit::CompiledModuleSP jitModuleRow_;
#endif
};

using SpillPartitionSet =
    std::map<SpillPartitionId, std::unique_ptr<SpillPartition>>;

/// Represents all spilled data of an operator, e.g. order by or group
/// by. This has one SpillFileList per partition of spill data.
class SpillState {
 public:
  /// Constructs a SpillState. 'type' is the content RowType. 'path' is the file
  /// system path prefix. 'bits' is the hash bit field for partitioning data
  /// between files. This also gives the maximum number of partitions.
  /// 'numSortKeys' is the number of leading columns on which the data is
  /// sorted, 0 if only hash partitioning is used. 'targetFileSize' is the
  /// target size of a single file.  'pool' owns the memory for state and
  /// results.
  SpillState(
      const common::SpillConfig::SpillIOConfig& ioConfig,
      int32_t maxPartitions,
      int32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      uint64_t targetFileSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  /// Indicates if a given 'partition' has been spilled or not.
  bool isPartitionSpilled(uint32_t partition) const {
    BOLT_DCHECK_LT(partition, maxPartitions_);
    return spilledPartitionSet_.contains(partition);
  }

  // Sets a partition as spilled.
  void setPartitionSpilled(uint32_t partition);

  // Returns how many ways spilled data can be partitioned.
  int32_t maxPartitions() const {
    return maxPartitions_;
  }

  uint64_t targetFileSize() const {
    return targetFileSize_;
  }

  common::CompressionKind compressionKind() const {
    return ioConfig_.compressionKind;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const {
    return sortCompareFlags_;
  }

  bool isAnyPartitionSpilled() const {
    return !spilledPartitionSet_.empty();
  }

  bool isAllPartitionSpilled() const {
    BOLT_CHECK_LE(spilledPartitionSet_.size(), maxPartitions_);
    return spilledPartitionSet_.size() == maxPartitions_;
  }

  /// Appends data to 'partition'. The rows given by 'indices' must be sorted
  /// for a sorted spill and must hash to 'partition'. It is safe to call this
  /// on multiple threads if all threads specify a different partition. Returns
  /// the size to append to partition.
  uint64_t appendToPartition(uint32_t partition, const RowVectorPtr& rows);

  // append row based data to partition
  uint64_t appendToPartition(
      uint32_t partition,
      const std::vector<char*, memory::StlAllocator<char*>>& rows,
      RowTypePtr type,
      const RowFormatInfo& info);

  /// Finishes a sorted run for 'partition'. If write is called for 'partition'
  /// again, the data does not have to be sorted relative to the data written so
  /// far.
  void finishFile(uint32_t partition);

  /// Returns the current number of finished files from a given partition.
  ///
  /// NOTE: the function returns zero if the state has finished or the partition
  /// is not spilled yet.
  size_t numFinishedFiles(uint32_t partition) const;

  /// Returns the spill file objects from a given 'partition'. The function
  /// returns an empty list if either the partition has not been spilled or has
  /// no spilled data.
  SpillFiles finish(uint32_t partition);

  /// Returns the spilled partition number set.
  const SpillPartitionNumSet& spilledPartitionSet() const;

  std::optional<VectorSerde::Kind> testingSpillSerdeKind() const {
    return ioConfig_.spillSerdeKind;
  }

  /// Returns the spilled file paths from all the partitions.
  std::vector<std::string> testingSpilledFilePaths() const;

  /// Returns the file ids from a given partition.
  std::vector<uint32_t> testingSpilledFileIds(int32_t partitionNum) const;

  /// Returns the set of partitions that have spilled data.
  SpillPartitionNumSet testingNonEmptySpilledPartitionSet() const;

  void setMaxBatchRows(const uint32_t maxBatchRows) {
    maxBatchRows_ = maxBatchRows;
  }

  uint64_t maxPartitionRowCount() const {
    return *std::max_element(spilledRowCount_.begin(), spilledRowCount_.end());
  }

  uint64_t sumPartitionRowCount() const {
    return std::accumulate(spilledRowCount_.begin(), spilledRowCount_.end(), 0);
  }

  uint64_t spilledRowCountSize() const {
    return spilledRowCount_.size();
  }

  const std::vector<uint64_t>& spilledRowCount() {
    return spilledRowCount_;
  }

  uint64_t spilledRowCount(uint32_t partition) const {
    BOLT_DCHECK_LT(partition, spilledRowCount_.size());
    return spilledRowCount_[partition];
  }

  void setImmediateFlush(bool immediateFlush) {
    immediateFlush_ = immediateFlush;
  }

  uint64_t rowsInCurrentFile(uint32_t partition) {
    return partitionWriters_[partition]->rowsInCurrentFile();
  }

  bool isUringEnabled() {
    return ioConfig_.spillUringEnabled;
  }

 private:
  void updateSpilledInputBytes(uint64_t bytes);

  SpillWriter* partitionWriter(uint32_t partition) const;

  const RowTypePtr type_;

  const common::SpillConfig::SpillIOConfig ioConfig_;

  const int32_t maxPartitions_;
  const int32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const uint64_t targetFileSize_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  // A set of spilled partition numbers.
  SpillPartitionNumSet spilledPartitionSet_;

  // A file list for each spilled partition. Only partitions that have
  // started spilling have an entry here.
  std::vector<std::unique_ptr<SpillWriter>> partitionWriters_;

  uint32_t maxBatchRows_{0};

  std::vector<uint64_t> spilledRowCount_;

  bool immediateFlush_{false};
};

/// Generate partition id set from given spill partition set.
SpillPartitionIdSet toSpillPartitionIdSet(
    const SpillPartitionSet& partitionSet);

/// Scoped spill percentage utility that allows user to set the behavior of
/// triggered spill.
/// 'spillPct' indicates the chance of triggering spilling. 100% means spill
/// will always be triggered.
/// 'maxInjections' indicates the max number of actual triggering. e.g. when
/// 'spillPct' is 20 and 'maxInjections' is 10, continuous calls to
/// testingTriggerSpill() will keep rolling the dice that has a chance of 20%
/// triggering until 10 triggers have been invoked.
class TestScopedSpillInjection {
 public:
  explicit TestScopedSpillInjection(
      int32_t spillPct,
      int32_t maxInjections = std::numeric_limits<int32_t>::max());

  ~TestScopedSpillInjection();
};

/// Test utility that returns true if triggered spill is evaluated to happen,
/// false otherwise.
bool testingTriggerSpill();
} // namespace bytedance::bolt::exec

// Adding the custom hash for SpillPartitionId to std::hash to make it usable
// with maps and other standard data structures.
namespace std {
template <>
struct hash<::bytedance::bolt::exec::SpillPartitionId> {
  size_t operator()(const ::bytedance::bolt::exec::SpillPartitionId& id) const {
    return bytedance::bolt::bits::hashMix(
        id.partitionBitOffset(), id.partitionNumber());
  }
};
} // namespace std

template <>
struct fmt::formatter<bytedance::bolt::exec::SpillPartitionId>
    : formatter<std::string> {
  auto format(bytedance::bolt::exec::SpillPartitionId s, format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};
