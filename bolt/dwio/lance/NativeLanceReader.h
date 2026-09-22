/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <mutex>
#include <utility>

#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceColumnReader.h"
#include "bolt/dwio/lance/NativeLanceDecoder.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "folly/synchronization/Baton.h"

namespace bytedance::bolt::lance::reader {

class NativeLanceReaderBase {
 public:
  NativeLanceReaderBase(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver = nullptr);

  NativeLanceReaderBase(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter);

  dwio::common::BufferedInput& input() const {
    return *input_;
  }

  memory::MemoryPool& pool() const {
    return pool_;
  }

  const NativeLanceMetadata& metadata() const {
    return metadata_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId() const {
    return typeWithId_;
  }

  const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver() const {
    return blobResolver_;
  }

 private:
  memory::MemoryPool& pool_;
  std::shared_ptr<dwio::common::BufferedInput> input_;
  NativeLanceMetadata metadata_;
  std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
};

class NativeLanceRowReader : public dwio::common::RowReader {
 public:
  NativeLanceRowReader(
      std::shared_ptr<NativeLanceReaderBase> readerBase,
      dwio::common::RowReaderOptions options);

  int64_t nextRowNumber() override;
  int64_t nextReadSize(uint64_t size) override;
  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation = nullptr) override;
  uint64_t skip(uint64_t skipSize) override;
  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;
  void resetFilterCaches() override;
  std::optional<size_t> estimatedRowSize() const override;

  bool allPrefetchIssued() const override;

  std::optional<std::vector<PrefetchUnit>> prefetchUnits() override;

 private:
  enum class FetchStatus { kNotStarted, kInProgress, kFinished };

  struct PrefetchRange {
    uint64_t begin;
    uint64_t end;
  };

  void advancePastFinishedRange();
  uint64_t capReadSize(uint64_t size) const;
  FetchResult prefetchRange(size_t rangeIndex);
  void initializePrefetchRanges();
  void markPrefetchRangesFinished(uint64_t begin, uint64_t end);
  std::optional<size_t> prefetchRangeIndex(uint64_t begin, uint64_t end) const;
  NativeLanceDecoder* prefetchedDecoderForRange(uint64_t begin, uint64_t end);

  std::shared_ptr<NativeLanceReaderBase> readerBase_;
  dwio::common::RowReaderOptions options_;
  NativeLanceDecoder decoder_;
  std::unique_ptr<NativeLanceStructColumnReader> rootColumnReader_;
  std::vector<std::pair<uint64_t, uint64_t>> rowRanges_;
  std::vector<PrefetchRange> prefetchRanges_;
  std::vector<FetchStatus> prefetchStatuses_;
  std::vector<std::unique_ptr<dwio::common::BufferedInput>> prefetchInputs_;
  std::vector<std::unique_ptr<NativeLanceDecoder>> prefetchDecoders_;
  std::vector<std::shared_ptr<folly::Baton<>>> prefetchBatons_;
  mutable std::mutex prefetchMutex_;
  mutable std::mutex decoderMutex_;
  size_t currentRange_{0};
  uint64_t currentRow_{0};
  uint64_t batchesRead_{0};
  uint64_t decodeTimeNs_{0};
  int64_t maxBatchBytes_{0};
  mutable uint64_t estimatedBytesPerRow_{0};
};

class NativeLanceReader : public dwio::common::Reader {
 public:
  NativeLanceReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver = nullptr);

  NativeLanceReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter);

  std::optional<uint64_t> numberOfRows() const override;
  const RowTypePtr& rowType() const override;
  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;
  NativeLanceMetadata::DebugStats debugStats() const;
  size_t loadedColumnMetadataCount() const;
  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;
  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

 private:
  std::shared_ptr<NativeLanceReaderBase> readerBase_;
};

class NativeLanceReaderFactory : public dwio::common::ReaderFactory {
 public:
  explicit NativeLanceReaderFactory(
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver = nullptr,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter =
          defaultNativeLanceTypeAdapter())
      : ReaderFactory(dwio::common::FileFormat::LANCE),
        blobResolver_(std::move(blobResolver)),
        typeAdapter_(std::move(typeAdapter)) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override;

 private:
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter_;
};

} // namespace bytedance::bolt::lance::reader
