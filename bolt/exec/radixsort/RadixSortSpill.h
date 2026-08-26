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
#include "bolt/exec/radixsort/RadixSortSpillRow.h"

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
  BufferPtr rowBuffer;
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

  void writeRows(
      const RadixSortKeyLayout& keyLayout,
      const PayloadRowLayout* payloadLayout,
      const char* const* keys,
      char* const* payloads,
      vector_size_t count);

  std::vector<RadixSortSpillFile> finishRows();

  uint64_t inputBytes() const {
    return inputBytes_;
  }

 private:
  void cleanupFilesNoThrow() noexcept;

  void resetWriteState();

  void prepareWriteBuffer();

  void ensureBuffer(uint64_t bytes);

  void resetBuffer(uint64_t bytes);

  void ensureRowFits(uint64_t rowSize);

  void appendRow(const char* key);

  void appendRow(const char* key, char* payload);

  template <RadixSortKeyLayoutKind KIND>
  void appendFixedRows(const char* keys, vector_size_t count);

  void flush();

  void closeFile();

  SpillWriteFile* ensureFile();

  const std::string pathPrefix_;
  const common::SpillConfig ioConfig_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  struct PendingRow {
    const char* key;
    RadixSortSpillRowSize size;
  };

  RadixRow2RowSerdeMeta meta_;
  std::vector<PendingRow> pendingRows_;
  BufferPtr buffer_;
  BufferPtr compressedBuffer_;
  uint64_t normalBufferSize_{0};
  uint64_t pendingBodyCapacity_{0};
  uint64_t pendingBodyBytes_{0};
  uint64_t pendingKeyHeapBytes_{0};
  uint64_t pendingPayloadHeapBytes_{0};
  uint32_t nextFileId_{0};
  std::unique_ptr<SpillWriteFile> currentFile_;
  std::vector<RadixSortSpillFile> files_;
  uint64_t currentFileRows_{0};
  uint64_t inputBytes_{0};
};

class RadixSortSpillReader {
 public:
  RadixSortSpillReader(
      RadixSortSpillFile file,
      RadixSortSpillRunMeta meta,
      const PayloadRowLayout* payloadLayout,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache = nullptr);

  bool nextBatch(std::vector<char*>& keys, std::vector<char*>& payloads);

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
      if (rowBuffer_ != nullptr) {
        retainedRowBuffers_.push_back(std::move(rowBuffer_));
      }
      recycleRetainedRowBuffer();
      if (bufferCache_ == nullptr) {
        serializedBuffer_.reset();
      } else {
        recycleSerializedBuffer();
      }
      compressedBuffer_.reset();
      recycleRetainedSerializedBuffer();
      return;
    }
    recycleRetainedRowBuffer();
    recycleRetainedSerializedBuffer();
  }

 private:
  void acquireSerializedBuffer(uint64_t size);

  void recycleSerializedBuffer();

  void recycleRetainedSerializedBuffer();

  void acquireRowBuffer(uint64_t size);

  void recycleRetainedRowBuffer();

  bool nextFixedBatch(
      char* block,
      int32_t uncompressedSize,
      char* output,
      std::vector<char*>& keys,
      std::vector<char*>& payloads);

  RadixSortSpillFile file_;
  RadixSortSpillRunMeta meta_;
  const PayloadRowLayout* const payloadLayout_;
  memory::MemoryPool* const pool_;
  const uint64_t maxReusableRowBufferSize_;
  std::unique_ptr<SpillInputStream> input_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  BufferPtr serializedBuffer_;
  BufferPtr rowBuffer_;
  std::vector<BufferPtr> retainedSerializedBuffers_;
  std::vector<BufferPtr> retainedRowBuffers_;
  BufferPtr compressedBuffer_;
  uint64_t spillReadTimeUs_{0};
  uint64_t spillDecompressTimeUs_{0};
  uint64_t spillReadIOTimeUs_{0};
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
      RadixSortSpillRunMeta meta,
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
  std::vector<char*> keys_;
  std::vector<char*> payloads_;
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
