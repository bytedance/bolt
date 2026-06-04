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

#include <optional>

#include "bolt/connectors/hive/HiveSplitReaderBase.h"
#include "bolt/connectors/hive/SplitReader.h"
#include "bolt/connectors/hive/bytelake/BytelakeConstants.h"
#include "bolt/connectors/hive/bytelake/BytelakeHashDedup.h"
#include "bolt/connectors/hive/bytelake/BytelakeScanSpecUtil.h"

namespace bytedance::bolt {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace bytedance::bolt

namespace bytedance::bolt::connector::hive {

class BytelakeSplitReader : public HiveSplitReaderBase {
 public:
  BytelakeSplitReader(
      std::vector<std::unique_ptr<SplitReader>> splitReaders,
      const std::shared_ptr<io::IoStatistics> ioStats,
      // One key-only SplitReader per file, reading PK + precombine + isDeleted.
      std::vector<std::unique_ptr<SplitReader>> keyOnlySplitReaders,
      std::optional<BytelakeKeyOnlySchema> keyOnlySchema);

  virtual uint64_t next(int64_t size, VectorPtr& output) override;

  virtual bool allPrefetchIssued() const override;

  virtual bool emptySplit() const override;

  virtual void resetFilterCaches() override;

  virtual int64_t estimatedRowSize() const override;

  virtual void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  virtual void resetSplit() override;

  // Phase 1+2: stream key-only batches through BytelakeHashDedup, populating
  // losersPerFile_ and fileRowCounts_. Called once per split (gated by
  // losersBuilt_).
  void buildLosers();

  // Build a batch-local skip bitmap for splitReaders_[fileIdx]'s next read
  // of [baseRow, baseRow + sliceSize). Returns empty vector if no losers
  // fall in that range (caller should pass nullptr to Mutation).
  std::vector<uint64_t>
  buildBatchBitmap(size_t fileIdx, uint64_t baseRow, uint64_t sliceSize) const;

 protected:
  std::vector<std::unique_ptr<SplitReader>> splitReaders_;

  // Phase 1 readers + reduced key schema (parallel to splitReaders_).
  std::vector<std::unique_ptr<SplitReader>> keyOnlySplitReaders_;
  std::optional<BytelakeKeyOnlySchema> keyOnlySchema_;

  // Per-file sorted rowInFile positions to skip in Phase 3.
  std::vector<std::vector<uint32_t>> losersPerFile_;

  // Per-file total row count from Phase 1 (needs filter-free ScanSpec).
  std::vector<uint64_t> fileRowCounts_;

  // Set true after buildLosers() runs; reset by resetSplit().
  bool losersBuilt_ = false;

  // Phase 3 streaming cursor: current file index and consumed raw rows in it.
  size_t currentFileIdx_ = 0;
  uint64_t currentFileRow_ = 0;

  const std::shared_ptr<io::IoStatistics> ioStats_;
};

} // namespace bytedance::bolt::connector::hive
