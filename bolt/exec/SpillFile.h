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

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/TreeOfLosers.h"
#include "bolt/exec/UnorderedStreamReader.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/VectorStream.h"
namespace bytedance::bolt::exec {

/// Represents a spill file for writing the serialized spilled data into a disk
/// file.
class SpillWriteFile {
 public:
  static std::unique_ptr<SpillWriteFile> create(
      uint32_t id,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig,
      const bool spillUringEnabled);

  uint32_t id() const {
    return id_;
  }

  /// Returns the file size in bytes.
  uint64_t size() const;

  const std::string& path() const {
    return path_;
  }

  uint64_t write(std::unique_ptr<folly::IOBuf> iobuf);

  uint64_t write(std::string_view buf);

  WriteFile* file() {
    return file_.get();
  }

  /// Finishes writing and flushes any unwritten data.
  void finish();

 private:
  static inline std::atomic<int32_t> ordinalCounter_{0};

  SpillWriteFile(
      uint32_t id,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig,
      const bool spillUringEnabled);

  // The spill file id which is monotonically increasing and unique for each
  // associated spill partition.
  const uint32_t id_;
  const std::string path_;

  std::unique_ptr<WriteFile> file_;
  // Byte size of the backing file. Set when finishing writing.
  uint64_t size_{0};
  std::atomic_uint64_t taskId_{0};
  bool spillUringEnabled_;

  // Write buffers to maintain the lifecycle of data to be written via io_uring,
  // since the data must remain valid until completion of uring write.
  std::shared_ptr<WriteBuffers> writeBuffers_;
};

/// Records info of a finished spill file which is used for read.
struct SpillFileInfo {
  uint32_t id;
  RowTypePtr type;
  std::string path;
  /// The file size in bytes.
  uint64_t size;
  uint64_t rowCount;
  uint32_t numSortKeys;
  std::vector<CompareFlags> sortFlags;
  common::CompressionKind compressionKind;
  std::optional<VectorSerde::Kind> serdeKind;
  std::optional<RowFormatInfo> rowInfo;
};

using SpillFiles = std::vector<SpillFileInfo>;

/// Used to write the spilled data to a sequence of files for one partition. If
/// data is sorted, each file is sorted. The globally sorted order is produced
/// by merging the constituent files.
class SpillWriter {
 public:
  /// 'type' is a RowType describing the content. 'numSortKeys' is the number
  /// of leading columns on which the data is sorted. 'path' is a file path
  /// prefix. ' 'targetFileSize' is the target byte size of a single file.
  /// 'writeBufferSize' specifies the size limit of the buffered data before
  /// write to file. 'fileOptions' specifies the file layout on remote storage
  /// which is storage system specific. 'pool' is used for buffering and
  /// constructing the result data read from 'this'. 'stats' is used to collect
  /// the spill write stats.
  ///
  /// When writing sorted spill runs, the caller is responsible for buffering
  /// and sorting the data. write is called multiple times, followed by flush().
  SpillWriter(
      const RowTypePtr& type,
      const uint32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const std::string& pathPrefix,
      uint64_t targetFileSize,
      const common::SpillConfig::SpillIOConfig& ioConfig,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats,
      uint32_t maxBatchRows = 0,
      std::optional<RowFormatInfo> rowInfo = std::nullopt);

  /// Adds 'rows' for the positions in 'indices' into 'this'. The indices
  /// must produce a view where the rows are sorted if sorting is desired.
  /// Consecutive calls must have sorted data so that the first row of the
  /// next call is not less than the last row of the previous call.
  /// Returns the size to write.
  uint64_t write(
      const RowVectorPtr& rows,
      const folly::Range<IndexRange*>& indices);

  uint64_t writeAndFlush(
      const RowVectorPtr& rows,
      const folly::Range<IndexRange*>& indices);

  uint64_t write(
      const std::vector<char*, memory::StlAllocator<char*>>& rows,
      const RowFormatInfo& info);

  /// Closes the current output file if any. Subsequent calls to write will
  /// start a new one.
  void finishFile();

  /// Returns the number of current finished files.
  size_t numFinishedFiles() const;

  /// Finishes this file writer and returns the written spill files info.
  ///
  /// NOTE: we don't allow write to a spill writer after t
  SpillFiles finish();

  std::vector<std::string> testingSpilledFilePaths() const;

  std::vector<uint32_t> testingSpilledFileIds() const;

  uint64_t rowsInCurrentFile() const {
    return rowsInCurrentFile_;
  }

 private:
  FOLLY_ALWAYS_INLINE void checkNotFinished() const {
    BOLT_CHECK(!finished_, "SpillWriter has finished");
  }

  // Returns an open spill file for write. If there is no open spill file, then
  // the function creates a new one. If the current open spill file exceeds the
  // target file size limit, then it first closes the current one and then
  // creates a new one. 'currentFile_' points to the current open spill file.
  SpillWriteFile* ensureFile();

  // Closes the current open spill file pointed by 'currentFile_'.
  void closeFile();

  // Writes data from 'batch_' to the current output file. Returns the actual
  // written size.
  uint64_t flush();

  // Invoked to increment the number of spilled files and the file size.
  void updateSpilledFileStats(uint64_t fileSize);

  // Invoked to update the number of spilled rows.
  void updateAppendStats(uint64_t numRows, uint64_t serializationTimeUs);

  // Invoked to update the disk write stats. "isControlGroup" is used for abtest
  // mode. If it's no abtest mode, then it defaults to control group.
  void updateWriteStats(
      uint64_t spilledBytes,
      uint64_t flushTimeUs,
      uint64_t writeTimeUs);

  const RowTypePtr type_;
  const uint32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const common::CompressionKind compressionKind_;
  const std::string pathPrefix_;
  const uint64_t targetFileSize_;
  const bool spillUringEnabled_;
  const uint64_t writeBufferSize_;
  const std::string fileCreateConfig_;

  // Updates the aggregated spill bytes of this query, and throws if exceeds
  // the max spill bytes limit.
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  bool finished_{false};
  uint32_t nextFileId_{0};
  std::unique_ptr<VectorStreamGroup> batch_;
  std::unique_ptr<SpillWriteFile> currentFile_;
  SpillFiles finishedFiles_;
  uint32_t maxBatchRows_{0};
  uint32_t unflushedRows_{0};
  uint64_t unflushedSizeInRowVector_{0};

  std::optional<RowFormatInfo> rowInfo_;
  const std::optional<VectorSerde::Kind> spillSerdeKind_;
  VectorSerde* serde_{nullptr};
  uint64_t rowsInCurrentFile_{0};
};

/// Input stream backed by spill file.
///
/// TODO Usage of ByteInputStream as base class is hacky and just happens to
/// work. For example, ByteInputStream::size(), seekp(), tellp(),
/// remainingSize() APIs do not work properly.
class SpillInputStream : public ByteInputStream {
 public:
  /// Reads from 'input' using 'buffer' for buffering reads.
  SpillInputStream(
      std::unique_ptr<ReadFile>&& file,
      std::vector<BufferPtr> buffers,
      const bool spillUringEnabled = false)
      : file_(std::move(file)),
        size_(file_->size()),
        readBuffers_(std::move(buffers)),
        spillUringEnabled_(spillUringEnabled) {
    bufferNum_ = readBuffers_.size();
    bufferSizes_.resize(bufferNum_, 0);
    if (spillUringEnabled_ && file_->uringEnabled()) {
      init(true);
    } else {
      next(true);
    }
  }
  /// True if all of the file has been read into vectors.
  bool atEnd() const override {
    if (spillUringEnabled_ && file_->uringEnabled()) {
      return completed_ >= size_ && ranges()[0].position >= ranges()[0].size;
    } else {
      return offset_ >= size_ && ranges()[0].position >= ranges()[0].size;
    }
  }

  void reuse() {
    offset_ = 0;
  }

  uint64_t getSpillReadIOTime() {
    return spillReadIOTimeUs_;
  }
  void prefetch(bool throwIfPastEnd);

 private:
  void next(bool throwIfPastEnd) override;

  void init(bool throwIfPastEnd);

  const std::unique_ptr<ReadFile> file_;
  const uint64_t size_;
  std::vector<BufferPtr> readBuffers_;
  std::vector<size_t> bufferSizes_;

  int bufferNum_;
  int idx_ = 0;
  uint64_t spillReadIOTimeUs_ = 0;
  // Offset of first byte not in 'buffer_'
  uint64_t offset_ = 0;
  uint64_t completed_ = 0;
  bool spillUringEnabled_;
};

/// Represents a spill file for read which turns the serialized spilled data on
/// disk back into a sequence of spilled row vectors.
///
/// NOTE: The class will not delete spill file upon destruction, so the user
/// needs to remove the unused spill files at some point later. For example, a
/// query Task deletes all the generated spill files in one operation using
/// rmdir() call.
class SpillReadFileBase {
 public:
  uint32_t id() const {
    return id_;
  }

  int32_t numSortKeys() const {
    return numSortKeys_;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const {
    return sortCompareFlags_;
  }

  /// Returns the file size in bytes.
  uint64_t size() const {
    return size_;
  }

  const std::string& testingFilePath() const {
    return path_;
  }

  const RowTypePtr type() const {
    return type_;
  }

  uint64_t getSpillReadIOTime() {
    return spillReadIOTimeUs_;
  }

  void prefetch() {
    if (spillUringEnabled_)
      input_->prefetch(true);
  }

 protected:
  SpillReadFileBase(
      const SpillFileInfo& fileInfo,
      memory::MemoryPool* pool,
      const bool spillUringEnabled = false);

  // The spill file id which is monotonically increasing and unique for each
  // associated spill partition.
  const uint32_t id_;
  const std::string path_;
  // The file size in bytes.
  const uint64_t size_;
  // The data type of spilled data.
  const RowTypePtr type_;
  const uint32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const common::CompressionKind compressionKind_;
  const VectorSerde::Options readOptions_;
  const std::optional<VectorSerde::Kind> serdeKind_;
  VectorSerde* const serde_{nullptr};
  bool spillUringEnabled_;
  memory::MemoryPool* const pool_;

  std::unique_ptr<SpillInputStream> input_;
  uint64_t spillReadIOTimeUs_{0};
};

class SpillReadFile : public SpillReadFileBase {
  SpillReadFile(
      const SpillFileInfo& fileInfo,
      memory::MemoryPool* pool,
      const bool spillUringEnabled)
      : SpillReadFileBase(fileInfo, pool, spillUringEnabled) {}

 public:
  static std::unique_ptr<SpillReadFile> create(
      const SpillFileInfo& fileInfo,
      memory::MemoryPool* pool,
      const bool spillUringEnabled) {
    return std::unique_ptr<SpillReadFile>(
        new SpillReadFile(fileInfo, pool, spillUringEnabled));
  }

  void reuse();
  bool nextBatch(RowVectorPtr& rowVector);
};

class RowBasedSpillReadFile : public SpillReadFileBase {
  RowBasedSpillReadFile(
      const SpillFileInfo& fileInfo,
      memory::MemoryPool* pool,
      bool spillUringEnabled)
      : SpillReadFileBase(fileInfo, pool, spillUringEnabled),
        info_(fileInfo.rowInfo.value()),
        rowBuffer_(AlignedBuffer::allocate<char>(
            (1UL << 20) - AlignedBuffer::kPaddedSize,
            pool_)),
        compressedBuffer_(nullptr) {}

 public:
  static std::unique_ptr<RowBasedSpillReadFile> create(
      const SpillFileInfo& fileInfo,
      memory::MemoryPool* pool,
      bool spillUringEnabled) {
    return std::unique_ptr<RowBasedSpillReadFile>(
        new RowBasedSpillReadFile(fileInfo, pool, spillUringEnabled));
  }

  uint32_t nextBatch(std::vector<char*>& rowVector);
  bool nextBatch(
      std::vector<char*>& rowVector,
      std::vector<size_t>& rowLengths);

  const std::vector<RowColumn>& rowColumns() {
    return info_.rowColumns;
  }

  const RowFormatInfo& info() const {
    return info_;
  }

  const int32_t nextEqualOffset() const {
    return info_.nextEqualOffset;
  }

  int64_t getSpillDecompressTime() const {
    return spillDecompressTimeUs_;
  }

 protected:
  uint64_t spillDecompressTimeUs_{0};

 private:
  const RowFormatInfo info_;
  BufferPtr rowBuffer_;
  BufferPtr compressedBuffer_;
};

} // namespace bytedance::bolt::exec
