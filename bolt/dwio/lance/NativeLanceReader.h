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

#include <exception>
#include <mutex>
#include <utility>

#include <folly/Portability.h>

#include "bolt/dwio/common/Reader.h"
#include "bolt/dwio/common/ReaderFactory.h"
#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceColumnReader.h"
#include "bolt/dwio/lance/NativeLanceDecoder.h"
#include "bolt/dwio/lance/NativeLanceFileContext.h"
#include "bolt/dwio/lance/NativeLanceScanPlan.h"
#include "bolt/dwio/lance/NativeLanceScanWindow.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "folly/synchronization/Baton.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceScanState : uint8_t {
  kIdle,
  kPlanningWindow,
  kDecodingFilters,
  kDecodingValues,
  kAssemblingBatch,
  kOutputReady,
  kDrainingOutput,
  kFinished,
  kFailed,
  kCancelled,
};

/// Owns all scan-local state and drives one output batch at a time.
class NativeLanceScanCoordinator {
 public:
  static constexpr int64_t kAtEnd = dwio::common::RowReader::kAtEnd;
  using FetchResult = dwio::common::RowReader::FetchResult;
  using PrefetchUnit = dwio::common::RowReader::PrefetchUnit;

  NativeLanceScanCoordinator(
      std::shared_ptr<const NativeLanceFileContext> fileContext,
      dwio::common::RowReaderOptions options);

  ~NativeLanceScanCoordinator();

  int64_t nextRowNumber();
  int64_t nextReadSize(uint64_t size);
  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation);
  uint64_t skip(uint64_t skipSize);
  void cancel();
  void updateRuntimeStats(dwio::common::RuntimeStatistics& stats) const;
  void resetFilterCaches();
  std::optional<size_t> estimatedRowSize() const;

  bool allPrefetchIssued() const;

  std::optional<std::vector<dwio::common::RowReader::PrefetchUnit>>
  prefetchUnits();

  NativeLanceScanState state() const;

 private:
  enum class FetchStatus { kNotStarted, kInProgress, kFinished };

  struct PrefetchRange {
    uint64_t begin;
    uint64_t end;
  };

  void advancePastFinishedRange();
  const std::vector<std::pair<uint64_t, uint64_t>>& rowRanges() const {
    return scanPlan_->rowRanges();
  }
  uint64_t capReadSize(uint64_t size) const;
  dwio::common::RowReader::FetchResult prefetchRange(size_t rangeIndex);
  void initializePrefetchRanges();
  void markPrefetchRangesFinished(uint64_t begin, uint64_t end);
  void prepareNextBatchPipeline(uint64_t readEnd, uint64_t requestedRows);
  std::optional<size_t> prefetchRangeIndex(uint64_t begin, uint64_t end) const;
  bool waitForPrefetchedRange(uint64_t begin, uint64_t end);
  FOLLY_NOINLINE void readFiltered(
      uint64_t readBegin,
      uint64_t readEnd,
      uint64_t requestedRows,
      const common::ScanSpec& scanSpec,
      const dwio::common::Mutation* mutation,
      VectorPtr& result);
  uint64_t nextImpl(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation);
  void transition(NativeLanceScanState expected, NativeLanceScanState desired);
  void fail(std::exception_ptr error);
  [[noreturn]] void rethrowFailure() const;

  std::shared_ptr<const NativeLanceFileContext> fileContext_;
  dwio::common::RowReaderOptions options_;
  std::unique_ptr<dwio::common::BufferedInput> input_;
  NativeLanceDecoder decoder_;
  std::unique_ptr<NativeLanceScanPlan> scanPlan_;
  std::optional<NativeLanceScanWindow> window_;
  uint64_t generation_{0};
  std::vector<PrefetchRange> prefetchRanges_;
  std::vector<FetchStatus> prefetchStatuses_;
  std::vector<std::shared_ptr<folly::Baton<>>> prefetchBatons_;
  mutable std::mutex prefetchMutex_;
  mutable std::mutex decoderMutex_;
  mutable std::mutex stateMutex_;
  NativeLanceScanState state_{NativeLanceScanState::kIdle};
  std::exception_ptr failure_;
  size_t currentRange_{0};
  uint64_t currentRow_{0};
  uint64_t batchesRead_{0};
  uint64_t decodeTimeNs_{0};
  int64_t maxBatchBytes_{0};
  mutable uint64_t estimatedBytesPerRow_{0};
};

class NativeLanceRowReader : public dwio::common::RowReader {
 public:
  NativeLanceRowReader(
      std::shared_ptr<const NativeLanceFileContext> fileContext,
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
  std::unique_ptr<NativeLanceScanCoordinator> coordinator_;
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
  size_t loadedColumnMetadataCount() const;
  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;
  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

 private:
  std::shared_ptr<const NativeLanceFileContext> fileContext_;
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
