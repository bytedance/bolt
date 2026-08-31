/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#pragma once

#include <limits>
#include <optional>
#include <span>

#include <folly/Function.h>
#include <folly/Synchronized.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/exec/SpillFile.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortSpillSections.h"

namespace bytedance::bolt::exec::radixsort {
constexpr uint64_t kRadixSortSpillBufferSize =
    (1UL << 20) - AlignedBuffer::kPaddedSize;

struct RadixSortSpillFile {
  uint32_t id;
  std::string path;
  uint64_t size{0};
  uint64_t rowCount{0};
  common::CompressionKind compressionKind{common::CompressionKind_NONE};
};

struct RadixSortSpillRun {
  std::vector<RadixSortSpillFile> files;
};

struct RadixSortSpillReadBufferCache {
  BufferPtr serializedBuffer;
};

class RadixSortSpillWriter {
 public:
  RadixSortSpillWriter(
      std::string pathPrefix,
      const common::SpillConfig& ioConfig,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  ~RadixSortSpillWriter();

  std::vector<RadixSortSpillFile> writeRun(
      const RadixSortRunStorage& storage,
      const PayloadRowLayout* payloadLayout,
      uint64_t beginRow = 0);

  uint64_t inputBytes() const {
    return inputBytes_;
  }

 private:
  void prepareWriteBuffer();

  void ensureBuffer(uint64_t bytes);

  void resetBuffer(uint64_t bytes);

  void ensureRecordFits(uint64_t recordSize);

  void clearPendingBlock();

  void appendKeyRange(const char* keyBase, vector_size_t count);

  std::vector<RadixSortSpillFile> finish();

  void appendSizedKeyRange(
      const char* keyBase,
      const RadixSortSpillSectionBatchSize& batchSize,
      std::span<const RadixSortSpillSectionSize> rowSizes);

  void flush();

  const common::CompressionKind compressionKind_;
  const uint64_t writeBufferSize_;
  memory::MemoryPool* const pool_;
  std::unique_ptr<SpillWriter> spillWriter_;

  struct PendingRange {
    const char* keyBase{nullptr};
    uint32_t rowCount{0};
    uint64_t keyRecordBytes{0};
    uint64_t keyHeapBytes{0};
    uint64_t payloadFixedBytes{0};
    uint64_t payloadHeapBytes{0};
    size_t rowSizeOffset{0};

    uint64_t totalBytes() const {
      return keyRecordBytes + keyHeapBytes + payloadFixedBytes +
          payloadHeapBytes;
    }
  };

  struct PendingBlock {
    std::vector<PendingRange> ranges;
    std::vector<RadixSortSpillSectionSize> rowSizes;
    uint64_t totalBytes{0};
  };

  RadixSortSpillSectionMeta meta_;
  PendingBlock pendingBlock_;
  std::vector<RadixSortSpillSectionSize> sizingSectionSizes_;
  BufferPtr buffer_;
  uint64_t normalBufferSize_{0};
  uint64_t pendingBodyCapacity_{0};
  uint64_t inputBytes_{0};
  bool finished_{false};
};

class RadixSortSpillReader : protected SpillReadFileInput {
 public:
  RadixSortSpillReader(
      RadixSortSpillFile file,
      RadixSortSpillSectionMeta meta,
      const PayloadRowLayout* payloadLayout,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache = nullptr);

  bool nextBatch(std::vector<const char*>& keys);

  uint64_t spillReadTimeUs() const {
    return spillReadTimeUs_;
  }

  uint64_t spillDecompressTimeUs() const {
    return spillDecompressTimeUs_;
  }

  uint64_t spillReadIOTimeUs() const;

  void close();

 private:
  void acquireSerializedBuffer(uint64_t size);

  void recycleSerializedBuffer();

  void finishReading();

  RadixSortSpillFile file_;
  RadixSortSpillSectionMeta meta_;
  const PayloadRowLayout* const payloadLayout_;
  const uint64_t maxReusableSerializedBufferSize_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  BufferPtr serializedBuffer_;
  BufferPtr compressedBuffer_;
  uint64_t spillReadTimeUs_{0};
  uint64_t spillDecompressTimeUs_{0};
};

class RadixSortMergeStream {
 public:
  explicit RadixSortMergeStream(const RadixSortKeyLayout& keyLayout)
      : keyWidth_(keyLayout.width()),
        hasPayload_(keyLayout.hasPayload()),
        payloadOffset_(keyLayout.payloadOffset().value_or(0)) {}

  virtual ~RadixSortMergeStream() = default;

  const char* key() const {
    return key_;
  }

  char* payload() const {
    return payload_;
  }

  virtual bool hasData() const = 0;

  /// Advances past the current row when the next row has the same backing
  /// storage. Returns false without changing state if advancing could
  /// invalidate the current key or payload.
  virtual bool tryAdvance() = 0;

  virtual void advanceAfterFlush() {
    BOLT_UNREACHABLE("advanceAfterFlush called without a spill boundary");
  }

  virtual uint64_t getSpillReadTime() const {
    return 0;
  }

  virtual uint64_t getSpillDecompressTime() const {
    return 0;
  }

  virtual uint64_t getSpillReadIOTime() const {
    return 0;
  }

 protected:
  const uint32_t keyWidth_;
  const bool hasPayload_;
  const uint32_t payloadOffset_;
  const char* key_{nullptr};
  char* payload_{nullptr};
};

class RadixSortMemoryRunMergeStream : public RadixSortMergeStream {
 public:
  explicit RadixSortMemoryRunMergeStream(const RadixSortRunStorage& storage);

  bool hasData() const override;

  bool tryAdvance() override;

  uint64_t position() const {
    return index_;
  }

 private:
  void loadCurrent();

  const RadixSortRunStorage& storage_;
  uint64_t index_{0};
  RadixSortKeyRange range_{nullptr, 0};
  vector_size_t rangeIndex_{0};
};

class RadixSortSpillFileMergeStream final : public RadixSortMergeStream {
 public:
  RadixSortSpillFileMergeStream(
      RadixSortSpillFile file,
      RadixSortSpillSectionMeta meta,
      const PayloadRowLayout* payloadLayout,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache = nullptr);

  ~RadixSortSpillFileMergeStream() override;

  bool hasData() const override;

  bool tryAdvance() override;

  void advanceAfterFlush() override;

  uint64_t getSpillReadTime() const {
    return reader_.spillReadTimeUs();
  }

  uint64_t getSpillDecompressTime() const {
    return reader_.spillDecompressTimeUs();
  }

  uint64_t getSpillReadIOTime() const {
    return reader_.spillReadIOTimeUs();
  }

 private:
  class SpillFileGuard {
   public:
    explicit SpillFileGuard(std::string path) : path_(std::move(path)) {}

    ~SpillFileGuard();

    void remove();

    void removeNoThrow() noexcept;

   private:
    std::string path_;
  };

  void loadBatch();

  void updateCurrent();

  void finishReading();

  void closeNoThrow() noexcept;

  SpillFileGuard fileGuard_;
  RadixSortSpillReader reader_;
  std::vector<const char*> keys_;
  uint32_t index_{0};
};

class RadixSortConcatFilesSpillMergeStream final : public RadixSortMergeStream {
 public:
  RadixSortConcatFilesSpillMergeStream(
      std::vector<RadixSortSpillFile> files,
      RadixSortSpillSectionMeta meta,
      const PayloadRowLayout* payloadLayout,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache = nullptr);

  ~RadixSortConcatFilesSpillMergeStream() override;

  bool hasData() const override;

  bool tryAdvance() override;

  void advanceAfterFlush() override;

  uint64_t getSpillReadTime() const override;

  uint64_t getSpillDecompressTime() const override;

  uint64_t getSpillReadIOTime() const override;

 private:
  void loadNextFile();

  void updateCurrent();

  void retireCurrentFileStream();

  void cleanupUnreadFilesNoThrow() noexcept;

  void closeNoThrow() noexcept;

  RadixSortSpillSectionMeta meta_;
  const PayloadRowLayout* const payloadLayout_;
  memory::MemoryPool* const pool_;
  const bool spillUringEnabled_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  std::vector<RadixSortSpillFile> files_;
  size_t nextFileIndex_{0};
  std::unique_ptr<RadixSortSpillFileMergeStream> current_;
  uint64_t completedSpillReadTimeUs_{0};
  uint64_t completedSpillDecompressTimeUs_{0};
  uint64_t completedSpillReadIOTimeUs_{0};
};

class RadixSortMerger {
 public:
  using CompareKeys = int32_t (*)(const char*, const char*, uint32_t);
  using FlushRows = folly::FunctionRef<void(vector_size_t)>;

  RadixSortMerger(
      RadixSortKeyLayout keyLayout,
      std::vector<std::unique_ptr<RadixSortMergeStream>> streams,
      std::optional<size_t> memoryIndex = std::nullopt,
      std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache = nullptr);

  vector_size_t collectRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      FlushRows flushRows);

  uint64_t getSpillReadTime() const;

  uint64_t getSpillDecompressTime() const;

  uint64_t getSpillReadIOTime() const;

  std::optional<uint64_t> memoryPosition() const;

  void replaceMemory(
      RadixSortSpillRun run,
      RadixSortSpillSectionMeta meta,
      const PayloadRowLayout* payloadLayout,
      memory::MemoryPool* pool,
      bool spillUringEnabled);

  void removeMemory();

  size_t testingNumStreams() const {
    return streams_.size();
  }

 private:
  using StreamIndex = uint16_t;
  static constexpr StreamIndex kEmpty = std::numeric_limits<StreamIndex>::max();

  template <bool HasPayload>
  vector_size_t collectSingleStreamRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      FlushRows flushRows);

  template <bool HasPayload>
  vector_size_t collectTwoWayRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      FlushRows flushRows);

  template <bool HasPayload>
  vector_size_t collectLoserTreeRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      FlushRows flushRows);

  bool less(StreamIndex left, StreamIndex right) const;

  StreamIndex nextLoserTreeStream();

  StreamIndex first(int32_t node);

  StreamIndex propagate(int32_t node, StreamIndex value);

  void resetSelection();

  int32_t compareKeys(const char* left, const char* right) const {
    return compareKeys_(left, right, keyLayout_.heapKeyOffset());
  }

  static int32_t parent(int32_t node) {
    return (node - 1) / 2;
  }

  static int32_t leftChild(int32_t node) {
    return node * 2 + 1;
  }

  static int32_t rightChild(int32_t node) {
    return node * 2 + 2;
  }

  RadixSortKeyLayout keyLayout_;
  CompareKeys compareKeys_{nullptr};
  std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache_;
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams_;
  std::optional<size_t> memoryIndex_;
  std::vector<StreamIndex> losers_;
  StreamIndex lastIndex_{kEmpty};
  int32_t firstStream_{0};
};

} // namespace bytedance::bolt::exec::radixsort
