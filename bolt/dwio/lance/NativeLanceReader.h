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

#include <atomic>
#include <mutex>
#include <utility>

#include <folly/Portability.h>

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

  NativeLanceReadPlan::Options readPlanOptions() const {
    return readPlanOptions_;
  }

  const std::shared_ptr<NativeLanceDecodedPageCache>& decodedPageCache() const {
    return decodedPageCache_;
  }

  void recordDecodedWindow(uint64_t rows) {
    ++decodedWindowBuilds_;
    decodedWindowRows_ += rows;
  }

  void recordDecodedWindowSlice() {
    ++decodedWindowSlices_;
  }

  void addDecodedWindowStats(NativeLanceMetadata::DebugStats& stats) const {
    stats.decodedWindowBuilds = decodedWindowBuilds_;
    stats.decodedWindowRows = decodedWindowRows_;
    stats.decodedWindowSlices = decodedWindowSlices_;
    stats.selectiveWindowBuilds = selectiveWindowBuilds_;
    stats.selectiveWindowSourceRows = selectiveWindowSourceRows_;
    stats.selectiveWindowOutputRows = selectiveWindowOutputRows_;
    stats.selectiveWindowSlices = selectiveWindowSlices_;
  }

  void recordSelectiveWindow(uint64_t sourceRows, uint64_t outputRows) {
    ++selectiveWindowBuilds_;
    selectiveWindowSourceRows_ += sourceRows;
    selectiveWindowOutputRows_ += outputRows;
  }

  void recordSelectiveWindowSlice() {
    ++selectiveWindowSlices_;
  }

 private:
  memory::MemoryPool& pool_;
  std::shared_ptr<dwio::common::BufferedInput> input_;
  NativeLanceMetadata metadata_;
  std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  NativeLanceReadPlan::Options readPlanOptions_;
  std::shared_ptr<NativeLanceDecodedPageCache> decodedPageCache_;
  std::atomic<uint64_t> decodedWindowBuilds_{0};
  std::atomic<uint64_t> decodedWindowRows_{0};
  std::atomic<uint64_t> decodedWindowSlices_{0};
  std::atomic<uint64_t> selectiveWindowBuilds_{0};
  std::atomic<uint64_t> selectiveWindowSourceRows_{0};
  std::atomic<uint64_t> selectiveWindowOutputRows_{0};
  std::atomic<uint64_t> selectiveWindowSlices_{0};
};

class NativeLanceRowReader : public dwio::common::RowReader {
 public:
  NativeLanceRowReader(
      std::shared_ptr<NativeLanceReaderBase> readerBase,
      dwio::common::RowReaderOptions options);

  ~NativeLanceRowReader() override;

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

  struct PipelineState {
    uint64_t begin;
    uint64_t end;
    std::unique_ptr<dwio::common::BufferedInput> input;
    std::unique_ptr<NativeLanceDecoder> decoder;
  };

  struct DenseWindow {
    uint64_t begin;
    uint64_t end;
    VectorPtr rows;
  };

  struct SelectiveWindow {
    uint64_t begin;
    uint64_t end;
    VectorPtr rows;
    std::vector<vector_size_t> selectedRows;
  };

  void advancePastFinishedRange();
  uint64_t capReadSize(uint64_t size) const;
  FetchResult prefetchRange(size_t rangeIndex);
  void initializePrefetchRanges();
  void markPrefetchRangesFinished(uint64_t begin, uint64_t end);
  void prepareNextBatchPipeline(uint64_t readEnd, uint64_t requestedRows);
  std::optional<PipelineState> takePipeline(
      uint64_t readBegin,
      uint64_t readEnd);
  std::optional<size_t> prefetchRangeIndex(uint64_t begin, uint64_t end) const;
  NativeLanceDecoder* prefetchedDecoderForRange(uint64_t begin, uint64_t end);
  FOLLY_NOINLINE void readFiltered(
      uint64_t readBegin,
      uint64_t readEnd,
      uint64_t requestedRows,
      const common::ScanSpec& scanSpec,
      const dwio::common::Mutation* mutation,
      VectorPtr& result);

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
  std::optional<PipelineState> pipeline_;
  std::optional<DenseWindow> denseWindow_;
  std::optional<SelectiveWindow> selectiveWindow_;
  std::optional<double> observedFilterSelectivity_;
  mutable std::mutex prefetchMutex_;
  mutable std::mutex decoderMutex_;
  bool denseWindowEnabled_{false};
  bool selectiveWindowEnabled_{false};
  size_t currentRange_{0};
  uint64_t currentRow_{0};
  uint64_t batchesRead_{0};
  uint64_t decodeTimeNs_{0};
  int64_t maxBatchBytes_{0};
  uint64_t windowBytesPerRow_{0};
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
