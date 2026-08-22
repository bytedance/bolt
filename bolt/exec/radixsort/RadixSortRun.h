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

#include <algorithm>
#include <chrono>

#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortRunSorter.h"

namespace bytedance::bolt::exec::radixsort {

enum class RadixSortOutputSource : uint8_t {
  kDecodedKey = 0,
  kPayload = 1,
};

struct RadixSortOutputColumn {
  RadixSortOutputSource source;
  uint32_t sourceIndex;
};

class RadixSortOutputProjection {
 public:
  static std::unique_ptr<RadixSortOutputProjection> create(
      const RowTypePtr& outputType,
      const RowTypePtr& keyType,
      const std::vector<column_index_t>& directKeyChannels);

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  const RowTypePtr& keyType() const {
    return keyType_;
  }

  const RowTypePtr& payloadType() const {
    return payloadType_;
  }

  const std::vector<RadixSortOutputColumn>& columns() const {
    return columns_;
  }

  const std::vector<column_index_t>& payloadChannels() const {
    return payloadChannels_;
  }

  const std::vector<column_index_t>& directKeyChannels() const {
    return directKeyChannels_;
  }

  bool hasPayload() const {
    return payloadType_ != nullptr;
  }

  bool needsDecodedKeys() const {
    return needsDecodedKeys_;
  }

  const std::vector<uint8_t>& decodedKeyMask() const {
    return decodedKeyMask_;
  }

  void projectKeys(const RowVector& input, std::vector<VectorPtr>& children)
      const;

  void projectPayload(const RowVector& input, std::vector<VectorPtr>& children)
      const;

  RowVectorPtr reconstruct(
      const RowVectorPtr& decodedKeys,
      const RowVectorPtr& payload,
      memory::MemoryPool* pool) const;

 private:
  RadixSortOutputProjection(
      RowTypePtr outputType,
      RowTypePtr keyType,
      RowTypePtr payloadType,
      std::vector<RadixSortOutputColumn> columns,
      std::vector<column_index_t> payloadChannels,
      std::vector<column_index_t> directKeyChannels)
      : outputType_(std::move(outputType)),
        keyType_(std::move(keyType)),
        payloadType_(std::move(payloadType)),
        columns_(std::move(columns)),
        payloadChannels_(std::move(payloadChannels)),
        directKeyChannels_(std::move(directKeyChannels)),
        decodedKeyMask_(keyType_->size(), 0),
        needsDecodedKeys_(std::any_of(
            columns_.begin(),
            columns_.end(),
            [](const auto& column) {
              return column.source == RadixSortOutputSource::kDecodedKey;
            })) {
    for (const auto& column : columns_) {
      if (column.source == RadixSortOutputSource::kDecodedKey) {
        decodedKeyMask_[column.sourceIndex] = 1;
      }
    }
  }

  RowTypePtr outputType_;
  RowTypePtr keyType_;
  RowTypePtr payloadType_;
  std::vector<RadixSortOutputColumn> columns_;
  std::vector<column_index_t> payloadChannels_;
  std::vector<column_index_t> directKeyChannels_;
  std::vector<uint8_t> decodedKeyMask_;
  bool needsDecodedKeys_;
};

enum class RadixSortRunState : uint8_t {
  kBuilding = 0,
  kFinalizing = 1,
  kSortedInMemory = 2,
  kConsumed = 3,
};

struct RadixSortRunStats {
  uint64_t inputRows{0};
  uint64_t outputRows{0};
  uint64_t encodeTimeUs{0};
  uint64_t appendTimeUs{0};
  uint64_t sortTimeUs{0};
  uint64_t outputTimeUs{0};
};

struct RadixSortRunOptions {
  std::vector<uint8_t> initialKeyMayHaveNulls;
  std::vector<uint8_t> initialPayloadMayHaveNulls;
  uint32_t keysPerBlock{RadixSortRunStorage::kDefaultKeysPerBlock};
  uint64_t preferredKeyHeapGroupBytes{64 * 1024};
  uint32_t payloadRowsPerBlock{RadixSortRunStorage::kDefaultKeysPerBlock};
  uint64_t preferredPayloadHeapGroupBytes{64 * 1024};
};

class RadixSortRun {
 public:
  static std::unique_ptr<RadixSortRun> create(
      memory::MemoryPool* pool,
      const RowTypePtr& outputType,
      const RowTypePtr& keyType,
      const std::vector<CompareFlags>& keyFlags,
      const std::vector<column_index_t>& directKeyChannels,
      RadixSortRunOptions options);

  RadixSortRunState state() const {
    return state_;
  }

  uint64_t size() const {
    return storage_ == nullptr ? 0 : storage_->size();
  }

  int64_t retainedBytes() const {
    return storage_ == nullptr ? 0 : storage_->allocatedBytes();
  }

  uint64_t estimatedOutputBytes() const {
    return storage_ == nullptr ? 0 : storage_->estimatedOutputBytes();
  }

  const RadixSortRunStats& metrics() const {
    return metrics_;
  }

  const RadixSortOutputProjection& projection() const {
    return *projection_;
  }

  const std::shared_ptr<const PayloadRowLayout>& payloadLayout() const {
    return payloadLayout_;
  }

  const RadixSortKeyLayout& keyLayout() const {
    return keyLayout_;
  }

  const RadixSortRunStorage* storage() const {
    return storage_.get();
  }

  const std::vector<uint8_t>& keyMayHaveNulls() const {
    return keyMayHaveNulls_;
  }

  const std::vector<uint8_t>& payloadMayHaveNulls() const {
    return payloadMayHaveNulls_;
  }

  void append(const RowVector& input);

  void finalize();

  RowVectorPtr getOutput(vector_size_t maxRows, memory::MemoryPool* outputPool);

  RowVectorPtr getOutput(
      std::span<const char* const> keys,
      std::span<char* const> payloads,
      memory::MemoryPool* outputPool);

  vector_size_t collectRemainingRows(
      vector_size_t maxRows,
      const char** keys,
      char** payloads);

  void clear();

 private:
  RadixSortRun(
      memory::MemoryPool* pool,
      std::unique_ptr<RadixSortOutputProjection> projection,
      std::unique_ptr<RadixSortKeyCodec> keyCodec,
      std::shared_ptr<const PayloadRowLayout> payloadLayout,
      RadixSortKeyLayout keyLayout,
      std::unique_ptr<RadixSortRunStorage> arena,
      std::vector<uint8_t> keyMayHaveNulls,
      std::vector<uint8_t> payloadMayHaveNulls)
      : pool_(pool),
        projection_(std::move(projection)),
        keyCodec_(std::move(keyCodec)),
        payloadLayout_(std::move(payloadLayout)),
        keyLayout_(std::move(keyLayout)),
        storage_(std::move(arena)),
        keyMayHaveNulls_(std::move(keyMayHaveNulls)),
        currentRunKeyMayHaveNulls_(keyMayHaveNulls_.size(), 0),
        payloadMayHaveNulls_(std::move(payloadMayHaveNulls)) {}

  RowVectorPtr decodeKeys(
      uint64_t begin,
      vector_size_t count,
      memory::MemoryPool* outputPool);

  RowVectorPtr gatherPayload(
      uint64_t begin,
      vector_size_t count,
      memory::MemoryPool* outputPool);

  RowVectorPtr decodeKeyPointers(
      std::span<const char* const> keys,
      memory::MemoryPool* outputPool);

  RowVectorPtr gatherPayloadPointers(
      std::span<char* const> payloads,
      memory::MemoryPool* outputPool);

  static uint64_t elapsedUs(const std::chrono::steady_clock::time_point& begin);

  memory::MemoryPool* pool_;
  std::unique_ptr<RadixSortOutputProjection> projection_;
  std::unique_ptr<RadixSortKeyCodec> keyCodec_;
  std::shared_ptr<const PayloadRowLayout> payloadLayout_;
  RadixSortKeyLayout keyLayout_;
  std::unique_ptr<RadixSortRunStorage> storage_;
  EncodedKeyBatch encodeOutput_;
  PayloadRowWriter payloadWriter_;
  PayloadRowBatch payloadBatch_;
  RowVectorPtr decodedKeysOutput_;
  RowVectorPtr payloadOutput_;
  BufferPtr decodeCursorOutput_;
  BufferPtr decodeInlineOutput_;
  BufferPtr decodeViewsOutput_;
  BufferPtr payloadRowsOutput_;
  // Global across spilled and in-memory runs; used by output decode.
  std::vector<uint8_t> keyMayHaveNulls_;
  // Current in-memory run only; used by finalize radix pass skipping.
  std::vector<uint8_t> currentRunKeyMayHaveNulls_;
  std::vector<uint8_t> payloadMayHaveNulls_;
  RadixSortRunState state_{RadixSortRunState::kBuilding};
  uint64_t outputPosition_{0};
  RadixSortRunStats metrics_;
};

} // namespace bytedance::bolt::exec::radixsort
