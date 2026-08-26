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

#include <folly/Synchronized.h>

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/base/SpillStats.h"
#include "bolt/exec/SpillFile.h"
#include "bolt/exec/TreeOfLosers.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortSpillSections.h"

namespace bytedance::bolt::exec::radixsort {

struct RadixSortSpillFile {
  uint32_t id;
  std::string path;
  uint64_t size{0};
  uint64_t rowCount{0};
  common::CompressionKind compressionKind{common::CompressionKind_NONE};
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
      const PayloadRowLayout* payloadLayout);

  void writeKeyRange(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* keyBase,
      vector_size_t count);

  std::vector<RadixSortSpillFile> finish();

  uint64_t inputBytes() const {
    return inputBytes_;
  }

 private:
  void resetWriteState();

  void prepareWriteBuffer();

  void ensureBuffer(uint64_t bytes);

  void resetBuffer(uint64_t bytes);

  void ensureRecordFits(uint64_t recordSize);

  void clearPendingRange();

  void appendKeyRange(const char* keyBase, vector_size_t count);

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

    uint64_t totalBytes() const {
      return keyRecordBytes + keyHeapBytes + payloadFixedBytes +
          payloadHeapBytes;
    }
  };

  RadixSortSpillSectionMeta meta_;
  PendingRange pendingRange_;
  std::vector<RadixSortSpillSectionSize> pendingSectionSizes_;
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

  void releaseRetainedBuffers(bool releaseCurrentBuffer) {
    if (releaseCurrentBuffer) {
      if (bufferCache_ == nullptr) {
        serializedBuffer_.reset();
      } else {
        recycleSerializedBuffer();
      }
      compressedBuffer_.reset();
      recycleRetainedSerializedBuffer();
      return;
    }
    recycleRetainedSerializedBuffer();
  }

 private:
  void acquireSerializedBuffer(uint64_t size);

  void recycleSerializedBuffer();

  void recycleRetainedSerializedBuffer();

  RadixSortSpillFile file_;
  RadixSortSpillSectionMeta meta_;
  const PayloadRowLayout* const payloadLayout_;
  const uint64_t maxReusableSerializedBufferSize_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  BufferPtr serializedBuffer_;
  std::vector<BufferPtr> retainedSerializedBuffers_;
  BufferPtr compressedBuffer_;
  uint64_t spillReadTimeUs_{0};
  uint64_t spillDecompressTimeUs_{0};
};

class RadixSortMergeStream : public MergeStream {
 public:
  explicit RadixSortMergeStream(const RadixSortKeyLayout& keyLayout)
      : keyLayout_(keyLayout) {}

  const char* key() const {
    return key_;
  }

  char* payload() const {
    return payload_;
  }

  int32_t compare(const MergeStream& other) const override;

  virtual void pop() = 0;

  virtual uint64_t getSpillReadTime() const {
    return 0;
  }

  virtual uint64_t getSpillDecompressTime() const {
    return 0;
  }

  virtual uint64_t getSpillReadIOTime() const {
    return 0;
  }

  virtual void releaseRetainedBuffers() {}

 protected:
  RadixSortKeyLayout keyLayout_;
  const char* key_{nullptr};
  char* payload_{nullptr};
};

class RadixSortMemoryRunMergeStream : public RadixSortMergeStream {
 public:
  explicit RadixSortMemoryRunMergeStream(const RadixSortRunStorage& storage);

  bool hasData() const override;

  void pop() override;

 private:
  void loadCurrent();

  const RadixSortRunStorage& storage_;
  uint64_t index_{0};
  RadixSortKeyRange range_{nullptr, 0};
  vector_size_t rangeIndex_{0};
};

class RadixSortSpillFileMergeStream : public RadixSortMergeStream {
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

  void pop() override;

  uint64_t getSpillReadTime() const {
    return reader_.spillReadTimeUs();
  }

  uint64_t getSpillDecompressTime() const {
    return reader_.spillDecompressTimeUs();
  }

  uint64_t getSpillReadIOTime() const {
    return reader_.spillReadIOTimeUs();
  }

  void releaseRetainedBuffers() override {
    reader_.releaseRetainedBuffers(key_ == nullptr);
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

  void finishReading();

  void closeNoThrow() noexcept;

  SpillFileGuard fileGuard_;
  RadixSortSpillReader reader_;
  std::vector<const char*> keys_;
  uint32_t index_{0};
};

class RadixSortMerger {
 public:
  using CompareKeys = int32_t (*)(const char*, const char*, uint32_t);

  RadixSortMerger(
      RadixSortKeyLayout keyLayout,
      std::vector<std::unique_ptr<RadixSortMergeStream>> streams,
      std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache = nullptr);

  vector_size_t
  collectRows(vector_size_t count, const char** keys, char** payloads);

  uint64_t getSpillReadTime() const;

  uint64_t getSpillDecompressTime() const;

  uint64_t getSpillReadIOTime() const;

  void releaseRetainedBuffers();

 private:
  using StreamIndex = uint16_t;
  static constexpr StreamIndex kEmpty = std::numeric_limits<StreamIndex>::max();

  vector_size_t collectSingleStreamRows(
      vector_size_t count,
      const char** keys,
      char** payloads);

  vector_size_t
  collectTwoWayRows(vector_size_t count, const char** keys, char** payloads);

  vector_size_t
  collectLoserTreeRows(vector_size_t count, const char** keys, char** payloads);

  bool less(StreamIndex left, StreamIndex right) const;

  StreamIndex nextLoserTreeStream();

  StreamIndex first(int32_t node);

  StreamIndex propagate(int32_t node, StreamIndex value);

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
  std::vector<StreamIndex> losers_;
  StreamIndex lastIndex_{kEmpty};
  int32_t firstStream_{0};
};

} // namespace bytedance::bolt::exec::radixsort
