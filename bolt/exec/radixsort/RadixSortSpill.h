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
#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortSpillSections.h"

namespace bytedance::bolt::exec::radixsort {
constexpr uint64_t kRadixSortSpillBufferSize =
    (1UL << 20) - AlignedBuffer::kPaddedSize;
constexpr uint32_t kCurrentRadixSortSpillFormat = 2;

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

struct RadixSortSpillBlockView {
  const char* keyRecordsBegin{nullptr};
  const char* keyHeapBegin{nullptr};
  char* payloadFixedBegin{nullptr};
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
      const RadixSortSpillSectionBatchSize& batchSize);

  void flush();

  const common::CompressionKind compressionKind_;
  const uint64_t writeBufferSize_;
  memory::MemoryPool* const pool_;
  std::unique_ptr<SpillWriter> spillWriter_;

  struct PendingRange {
    const char* keyBase{nullptr};
    uint32_t rowCount{0};
    uint64_t keyHeapBytes{0};
    uint64_t payloadHeapBytes{0};
  };

  struct PendingBlock {
    std::vector<PendingRange> ranges;
    uint64_t rowCount{0};
    uint64_t keyHeapBytes{0};
    uint64_t payloadHeapBytes{0};

    uint64_t totalBytes(uint64_t fixedBytesPerRow) const {
      return rowCount * fixedBytesPerRow + keyHeapBytes + payloadHeapBytes;
    }
  };

  RadixSortSpillSectionMeta meta_;
  PendingBlock pendingBlock_;
  BufferPtr buffer_;
  uint64_t normalBufferSize_{0};
  uint64_t pendingBodyCapacity_{0};
  uint64_t inputBytes_{0};
  bool finished_{false};
};

class RadixSortSpillReader : protected SpillReadFileInput {
 public:
  // Metadata describes the whole logical run and must outlive this
  // physical-file reader. RadixSortSpillLogicalRunCore owns it in production.
  RadixSortSpillReader(
      const RadixSortSpillFile& file,
      const RadixSortSpillSectionMeta& meta,
      memory::MemoryPool* pool,
      bool spillUringEnabled,
      RadixSortSpillReadBufferCache* bufferCache = nullptr);

  RadixSortSpillReader(
      const RadixSortSpillFile&,
      const RadixSortSpillSectionMeta&&,
      memory::MemoryPool*,
      bool,
      RadixSortSpillReadBufferCache* = nullptr) = delete;

  RadixSortSpillReader(const RadixSortSpillReader&) = delete;
  RadixSortSpillReader& operator=(const RadixSortSpillReader&) = delete;
  RadixSortSpillReader(RadixSortSpillReader&&) = delete;
  RadixSortSpillReader& operator=(RadixSortSpillReader&&) = delete;

  std::optional<RadixSortSpillBlockView> nextBatch();

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

  const common::CompressionKind compressionKind_;
  const RadixSortSpillSectionMeta& meta_;
  RadixSortSpillReadBufferCache* const bufferCache_;
  BufferPtr serializedBuffer_;
  BufferPtr compressedBuffer_;
  uint64_t spillReadTimeUs_{0};
  uint64_t spillDecompressTimeUs_{0};
};

class RadixSortMergeStream {
 public:
  explicit RadixSortMergeStream(const RadixSortKeyLayout& keyLayout)
      : RadixSortMergeStream(keyLayout, keyLayout.width()) {}

  RadixSortMergeStream(
      const RadixSortKeyLayout& keyLayout,
      uint32_t recordStride)
      : recordStride_(recordStride),
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
  const uint32_t recordStride_;
  const bool hasPayload_;
  const uint32_t payloadOffset_;
  const char* key_{nullptr};
  char* payload_{nullptr};
};

class RadixSortVariableMergeStream : public RadixSortMergeStream {
 public:
  RadixSortVariableMergeStream(
      const RadixSortKeyLayout& keyLayout,
      uint32_t recordStride)
      : RadixSortMergeStream(keyLayout, recordStride),
        keyHeapOffset_(keyLayout.heapKeyOffset()),
        keySizeOffset_(*keyLayout.sizeOffset()),
        keyDataOffset_(*keyLayout.dataOffset()) {}

  const EncodedKeyView& encodedSuffix() const;

  FOLLY_ALWAYS_INLINE const EncodedKeyView& encodedSuffixInline() const {
    if (key_ != nullptr && encodedSuffix_.bytes.data() == nullptr) {
      const auto encodedSize = loadUnaligned<uint64_t>(key_ + keySizeOffset_);
      BOLT_DCHECK_GT(encodedSize, keyHeapOffset_);
      encodedSuffix_ = EncodedKeyView{std::string_view(
          loadCompactPointer(key_ + keyDataOffset_),
          encodedSize - keyHeapOffset_)};
    }
    return encodedSuffix_;
  }

 protected:
  // Bytes from keyLayout.heapKeyOffset() through the end of the encoded key.
  // Spill streams set this while advancing their sequential heap cursor.
  // Memory streams leave it empty until comparison or output actually needs
  // the suffix, then cache it for the rest of the current row's lifetime.
  mutable EncodedKeyView encodedSuffix_{};
  const uint32_t keyHeapOffset_;

 private:
  const uint32_t keySizeOffset_;
  const uint32_t keyDataOffset_;
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

class RadixSortVariableMemoryRunMergeStream final
    : public RadixSortVariableMergeStream {
 public:
  explicit RadixSortVariableMemoryRunMergeStream(
      const RadixSortRunStorage& storage);

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

std::unique_ptr<RadixSortMergeStream> makeRadixSortMemoryRunMergeStream(
    const RadixSortRunStorage& storage);

std::unique_ptr<RadixSortMergeStream> makeRadixSortSpillMergeStream(
    RadixSortSpillRun run,
    RadixSortSpillSectionMeta meta,
    memory::MemoryPool* pool,
    bool spillUringEnabled,
    RadixSortSpillReadBufferCache* bufferCache = nullptr);

class RadixSortMerger {
 public:
  using CompareFixedKeys = int32_t (*)(const char*, const char*, uint32_t);
  using CompareVariableKeys = int32_t (*)(
      const char*,
      const char*,
      const RadixSortVariableMergeStream&,
      const RadixSortVariableMergeStream&,
      uint32_t);
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
      std::span<EncodedKeyView> selectedViews,
      FlushRows flushRows);

  uint64_t getSpillReadTime() const;

  uint64_t getSpillDecompressTime() const;

  uint64_t getSpillReadIOTime() const;

  std::optional<uint64_t> memoryPosition() const;

  void replaceMemory(
      RadixSortSpillRun run,
      RadixSortSpillSectionMeta meta,
      memory::MemoryPool* pool,
      bool spillUringEnabled);

  void removeMemory();

  size_t testingNumStreams() const {
    return streams_.size();
  }

 private:
  using StreamIndex = uint16_t;
  static constexpr StreamIndex kEmpty = std::numeric_limits<StreamIndex>::max();

  template <bool HasPayload, bool CaptureViews>
  vector_size_t collectSingleStreamRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      std::span<EncodedKeyView> selectedViews,
      FlushRows flushRows);

  template <bool HasPayload, bool Variable, bool CaptureViews>
  vector_size_t collectTwoWayRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      std::span<EncodedKeyView> selectedViews,
      FlushRows flushRows);

  template <bool HasPayload, bool Variable, bool CaptureViews>
  vector_size_t collectLoserTreeRows(
      vector_size_t count,
      const char** keys,
      char** payloads,
      std::span<EncodedKeyView> selectedViews,
      FlushRows flushRows);

  template <bool Variable>
  bool less(StreamIndex left, StreamIndex right) const;

  template <bool Variable>
  StreamIndex nextLoserTreeStream();

  template <bool Variable>
  StreamIndex first(int32_t node);

  template <bool Variable>
  StreamIndex propagate(int32_t node, StreamIndex value);

  void resetSelection();

  template <bool Variable>
  int32_t compareStreams(
      const char* left,
      const char* right,
      const RadixSortMergeStream& leftStream,
      const RadixSortMergeStream& rightStream) const;

  void captureSelectedView(
      const RadixSortMergeStream& stream,
      EncodedKeyView& view) const;

  void validateVariableStreams() const;

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
  union CompareFn {
    constexpr CompareFn() : fixed(nullptr) {}

    CompareFixedKeys fixed;
    CompareVariableKeys variable;
  } compare_;
  uint32_t variableSuffixSkip_{0};
  std::unique_ptr<RadixSortSpillReadBufferCache> bufferCache_;
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams_;
  std::optional<size_t> memoryIndex_;
  std::vector<StreamIndex> losers_;
  StreamIndex lastIndex_{kEmpty};
  int32_t firstStream_{0};
};

} // namespace bytedance::bolt::exec::radixsort
