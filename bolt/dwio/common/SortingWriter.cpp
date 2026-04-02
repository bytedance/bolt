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

#include "bolt/dwio/common/SortingWriter.h"
#include <limits>
namespace bytedance::bolt::dwio::common {

SortingWriter::SortingWriter(
    std::unique_ptr<Writer> writer,
    std::unique_ptr<exec::SortBuffer> sortBuffer,
    vector_size_t maxOutputRowsConfig,
    uint64_t maxOutputBytesConfig,
    bolt::common::SpillStats* spillStats)
    : outputWriter_(std::move(writer)),
      maxOutputRowsConfig_(maxOutputRowsConfig),
      maxOutputBytesConfig_(maxOutputBytesConfig),
      sortPool_(sortBuffer->pool()),
      canReclaim_(sortBuffer->canSpill()),
      spillStats_(spillStats),
      sortBuffer_(std::move(sortBuffer)) {
  BOLT_CHECK_GT(maxOutputRowsConfig_, 0);
  BOLT_CHECK_GT(maxOutputBytesConfig_, 0);
  BOLT_CHECK_NOT_NULL(spillStats_);
  if (sortPool_->parent()->reclaimer() != nullptr) {
    sortPool_->setReclaimer(MemoryReclaimer::create(this));
  }
  setState(State::kRunning);
}

SortingWriter::~SortingWriter() {
  sortPool_->release();
}

void SortingWriter::write(const VectorPtr& data) {
  checkRunning();
  sortBuffer_->addInput(data);
}

void SortingWriter::flush() {
  checkRunning();
  outputWriter_->flush();
}

void SortingWriter::close() {
  setState(State::kClosed);

  sortBuffer_->noMoreInput();
  const auto maxOutputBatchRows = outputBatchRows();
  RowVectorPtr output = sortBuffer_->getOutput(maxOutputBatchRows);
  while (output != nullptr) {
    outputWriter_->write(output);
    output = sortBuffer_->getOutput(maxOutputBatchRows);
  }
  auto spillStatsOr = sortBuffer_->spilledStats();
  if (spillStatsOr.has_value()) {
    BOLT_CHECK(canReclaim_);
    *spillStats_ = spillStatsOr.value();
  }
  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->close();
}

void SortingWriter::abort() {
  setState(State::kAborted);

  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->abort();
}

bool SortingWriter::canReclaim() const {
  return canReclaim_;
}

uint64_t SortingWriter::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  if (!canReclaim_) {
    return 0;
  }

  if (!isRunning()) {
    LOG(WARNING) << "Can't reclaim from a not running hive sort writer pool: "
                 << sortPool_->name() << ", state: " << state()
                 << "used memory: " << succinctBytes(sortPool_->currentBytes())
                 << ", reserved memory: "
                 << succinctBytes(sortPool_->reservedBytes());
    ++stats.numNonReclaimableAttempts;
    return 0;
  }
  BOLT_CHECK_NOT_NULL(sortBuffer_);

  auto reclaimBytes = memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(
              sortPool_, &reclaimedBytes);
          sortBuffer_->spill();
          sortPool_->release();
        }
        return reclaimedBytes;
      },
      stats);

  return reclaimBytes;
}

vector_size_t SortingWriter::outputBatchRows() {
  vector_size_t estimatedMaxOutputRows =
      std::numeric_limits<vector_size_t>::max();
  if (sortBuffer_->estimateOutputRowSize().has_value() &&
      sortBuffer_->estimateOutputRowSize().value() != 0) {
    const uint64_t maxOutputRows =
        maxOutputBytesConfig_ / sortBuffer_->estimateOutputRowSize().value();

    if (UNLIKELY(maxOutputRows > std::numeric_limits<vector_size_t>::max())) {
      return maxOutputRowsConfig_;
    }

    estimatedMaxOutputRows = maxOutputRows;
  }
  return std::min(estimatedMaxOutputRows, maxOutputRowsConfig_);
}

std::unique_ptr<memory::MemoryReclaimer> SortingWriter::MemoryReclaimer::create(
    SortingWriter* writer) {
  return std::unique_ptr<memory::MemoryReclaimer>(new MemoryReclaimer(writer));
}

bool SortingWriter::MemoryReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  BOLT_CHECK_EQ(pool.name(), writer_->sortPool_->name());

  reclaimableBytes = 0;
  if (!writer_->canReclaim()) {
    return false;
  }
  reclaimableBytes = pool.currentBytes();
  return true;
}

uint64_t SortingWriter::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  BOLT_CHECK_EQ(pool->name(), writer_->sortPool_->name());

  return writer_->reclaim(targetBytes, stats);
}
} // namespace bytedance::bolt::dwio::common
