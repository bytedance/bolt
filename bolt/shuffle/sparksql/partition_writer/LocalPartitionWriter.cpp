/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include <cstdint>
#include <filesystem>
#include <random>
#include <thread>

#include <bolt/common/time/Timer.h>
#include <boost/stacktrace.hpp>
#include <glog/logging.h>
#include "bolt/shuffle/sparksql/CompressionStream.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/Spill.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/partition_writer/LocalPartitionWriter.h"
namespace bytedance::bolt::shuffle::sparksql {

class LocalPartitionWriter::LocalSpiller {
 public:
  LocalSpiller(
      uint32_t numPartitions,
      const std::string& spillFile,
      uint32_t compressionThreshold,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec)
      : numPartitions_(numPartitions),
        spillFile_(spillFile),
        compressionThreshold_(compressionThreshold),
        pool_(pool),
        codec_(codec) {}

  arrow::Status spill(
      uint32_t partitionId,
      std::unique_ptr<BlockPayload> payload) {
    // Check spill Type.
    if (payload->type() != Payload::kUncompressed) {
      return arrow::Status::Invalid(
          "Cannot spill payload of type: " + payload->toString() +
          ", must be Payload::kUncompressed.");
    }

    if (!opened_) {
      opened_ = true;
      ARROW_ASSIGN_OR_RAISE(
          os_, arrow::io::FileOutputStream::Open(spillFile_, true));
      diskSpill_ = std::make_unique<Spill>(
          Spill::SpillType::kSequentialSpill, numPartitions_, spillFile_);
    }

    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(payload->serialize(os_.get()));
    // Because payload is uncompressed, no compress time.
    spillTime_ += payload->getWriteTime();
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    DLOG(INFO) << "LocalSpiller: Spilled partition " << partitionId
               << " file start: " << start << ", file end: " << end
               << ", file: " << spillFile_;

    auto payloadType =
        codec_ != nullptr && payload->numRows() >= compressionThreshold_
        ? Payload::kToBeCompressed
        : Payload::kUncompressed;
    diskSpill_->insertPayload(
        partitionId,
        payloadType,
        payload->numRows(),
        payload->isValidityBuffer(),
        end - start,
        pool_,
        codec_);
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> finish() {
    if (finished_) {
      return arrow::Status::Invalid(
          "Calling toBlockPayload() on a finished SpillEvictor.");
    }
    finished_ = true;

    if (!opened_) {
      return arrow::Status::Invalid("SpillEvictor has no data spilled.");
    }
    RETURN_NOT_OK(os_->Close());
    return std::move(diskSpill_);
  }

  bool finished() const {
    return finished_;
  }

  int64_t getSpillTime() {
    return spillTime_;
  }

 private:
  uint32_t numPartitions_;
  std::string spillFile_;
  uint32_t compressionThreshold_;
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  bool opened_{false};
  bool finished_{false};
  std::shared_ptr<Spill> diskSpill_{nullptr};
  std::shared_ptr<arrow::io::FileOutputStream> os_;
  int64_t spillTime_{0};
};

class LocalPartitionWriter::PayloadMerger {
 public:
  PayloadMerger(
      const PartitionWriterOptions& options,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      bool hasComplexType)
      : pool_(pool),
        codec_(codec),
        hasComplexType_(hasComplexType),
        compressionThreshold_(options.compressionThreshold),
        mergeBufferSize_(options.mergeBufferSize),
        mergeBufferMinSize_(options.mergeBufferSize * options.mergeThreshold),
        rowvectorModeCompressionMinColumns_(
            options.rowvectorModeCompressionMinColumns),
        rowvectorModeCompressionMaxBufferSize_(
            options.rowvectorModeCompressionMaxBufferSize) {}

  arrow::Result<std::vector<std::unique_ptr<BlockPayload>>> merge(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> append,
      bool reuseBuffers) {
    std::vector<std::unique_ptr<BlockPayload>> merged{};
    if (hasComplexType_) {
      // TODO: Merging complex type is currently not supported.
      merged.emplace_back();
      ARROW_ASSIGN_OR_RAISE(
          merged.back(), createBlockPayload(std::move(append), reuseBuffers));
      return merged;
    }

    MergeGuard mergeGuard(partitionInMerge_, partitionId);

    auto cacheOrFinish = [&]() {
      if (append->numRows() <= mergeBufferMinSize_) {
        // Save for merge.
        if (reuseBuffers) {
          // This is the first append, therefore need copy.
          RETURN_NOT_OK(append->copyBuffers(pool_));
        }
        partitionMergePayload_[partitionId] = std::move(append);
        return arrow::Status::OK();
      }
      merged.emplace_back();
      // If current buffer rows reaches merging threshold, create BlockPayload.
      ARROW_ASSIGN_OR_RAISE(
          merged.back(), createBlockPayload(std::move(append), reuseBuffers));
      return arrow::Status::OK();
    };

    if (!hasMerged(partitionId)) {
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    auto lastPayload = std::move(partitionMergePayload_[partitionId]);
    auto mergedRows = append->numRows() + lastPayload->numRows();
    auto mergedBytes = append->getBufferSize() + lastPayload->getBufferSize();
    if (mergedRows > mergeBufferSize_ ||
        append->numRows() > mergeBufferMinSize_ ||
        mergedBytes > kDefaultMergeBufferBytesThreshold) {
      merged.emplace_back();
      ARROW_ASSIGN_OR_RAISE(
          merged.back(),
          lastPayload->toBlockPayload(
              codec_ != nullptr &&
                      lastPayload->numRows() >= compressionThreshold_
                  ? Payload::kCompressed
                  : Payload::kUncompressed,
              pool_,
              codec_,
              hasComplexType_));
      RETURN_NOT_OK(cacheOrFinish());
      return merged;
    }

    // Merge buffers.
    DLOG(INFO) << "Merged partition: " << partitionId
               << ", numRows before: " << lastPayload->numRows()
               << ", numRows appended: " << append->numRows()
               << ", numRows after: " << mergedRows;
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        InMemoryPayload::merge(
            std::move(lastPayload),
            std::move(append),
            pool_,
            rowvectorModeCompressionMinColumns_,
            rowvectorModeCompressionMaxBufferSize_));
    if (mergedRows < mergeBufferSize_) {
      // Still not reach merging threshold, save for next merge.
      partitionMergePayload_[partitionId] = std::move(payload);
      return merged;
    }
    // mergedRows == mergeBufferSize_
    merged.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        merged.back(),
        payload->toBlockPayload(
            codec_ != nullptr && payload->numRows() >= compressionThreshold_
                ? Payload::kCompressed
                : Payload::kUncompressed,
            pool_,
            codec_,
            hasComplexType_));
    return merged;
  }

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> finishForSpill(
      uint32_t partitionId) {
    // We need to check whether the spill source is from compressing/copying the
    // merged buffers.
    if ((partitionInMerge_.has_value() && *partitionInMerge_ == partitionId) ||
        !hasMerged(partitionId)) {
      return std::nullopt;
    }
    auto payload = std::move(partitionMergePayload_[partitionId]);
    return payload->toBlockPayload(
        Payload::kUncompressed, pool_, codec_, hasComplexType_);
  }

  arrow::Result<std::optional<std::unique_ptr<BlockPayload>>> finish(
      uint32_t partitionId) {
    if (!hasMerged(partitionId)) {
      return std::nullopt;
    }
    auto numRows = partitionMergePayload_[partitionId]->numRows();
    // Because this is the last BlockPayload, delay the compression before
    // writing to the final data file.
    auto payloadType = (codec_ != nullptr && numRows >= compressionThreshold_)
        ? Payload::kToBeCompressed
        : Payload::kUncompressed;
    auto payload = std::move(partitionMergePayload_[partitionId]);
    return payload->toBlockPayload(payloadType, pool_, codec_, hasComplexType_);
  }

  bool hasMerged(uint32_t partitionId) {
    return partitionMergePayload_.find(partitionId) !=
        partitionMergePayload_.end() &&
        partitionMergePayload_[partitionId] != nullptr;
  }

 private:
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  bool hasComplexType_;
  int32_t compressionThreshold_;
  int32_t mergeBufferSize_;
  int32_t mergeBufferMinSize_;
  std::unordered_map<uint32_t, std::unique_ptr<InMemoryPayload>>
      partitionMergePayload_;
  std::optional<uint32_t> partitionInMerge_;

  int64_t rowvectorModeCompressionMinColumns_;
  int64_t rowvectorModeCompressionMaxBufferSize_;

  class MergeGuard {
   public:
    MergeGuard(std::optional<uint32_t>& partitionInMerge, uint32_t partitionId)
        : partitionInMerge_(partitionInMerge) {
      partitionInMerge_ = partitionId;
    }

    ~MergeGuard() {
      partitionInMerge_ = std::nullopt;
    }

   private:
    std::optional<uint32_t>& partitionInMerge_;
  };

  arrow::Status copyBuffers(
      std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
    // Copy.
    std::vector<std::shared_ptr<arrow::Buffer>> copies;
    for (auto& buffer : buffers) {
      if (!buffer) {
        continue;
      }
      if (buffer->size() == 0) {
        buffer = zeroLengthNullBuffer();
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(
          auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool_));
      memcpy(copy->mutable_data(), buffer->data(), buffer->size());
      buffer = std::move(copy);
    }
    return arrow::Status::OK();
  }

  arrow::Result<std::unique_ptr<BlockPayload>> createBlockPayload(
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      bool reuseBuffers) {
    auto createCompressed = codec_ != nullptr &&
        inMemoryPayload->numRows() >= compressionThreshold_;
    if (reuseBuffers && !createCompressed) {
      // For uncompressed buffers, need to copy before caching.
      RETURN_NOT_OK(inMemoryPayload->copyBuffers(pool_));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        inMemoryPayload->toBlockPayload(
            createCompressed ? Payload::kCompressed : Payload::kUncompressed,
            pool_,
            codec_,
            hasComplexType_));
    return payload;
  }
};

class LocalPartitionWriter::PayloadCache {
 public:
  PayloadCache(uint32_t numPartitions) : numPartitions_(numPartitions) {}

  arrow::Status cache(
      uint32_t partitionId,
      std::unique_ptr<BlockPayload> payload) {
    if (partitionCachedPayload_.find(partitionId) ==
        partitionCachedPayload_.end()) {
      partitionCachedPayload_[partitionId] =
          std::list<std::unique_ptr<BlockPayload>>{};
    }
    partitionCachedPayload_[partitionId].push_back(std::move(payload));
    return arrow::Status::OK();
  }

  bool hasCachedPayloads(uint32_t partitionId) {
    return partitionCachedPayload_.find(partitionId) !=
        partitionCachedPayload_.end() &&
        !partitionCachedPayload_[partitionId].empty();
  }

  arrow::Status write(uint32_t partitionId, arrow::io::OutputStream* os) {
    if (hasCachedPayloads(partitionId)) {
      auto& payloads = partitionCachedPayload_[partitionId];
      while (!payloads.empty()) {
        auto payload = std::move(payloads.front());
        payloads.pop_front();
        // Write the cached payload to disk.
        RETURN_NOT_OK(payload->serialize(os));
        compressTime_ += payload->getCompressTime();
        writeTime_ += payload->getWriteTime();
      }
    }
    return arrow::Status::OK();
  }

  bool canSpill() {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (hasCachedPayloads(pid)) {
        return true;
      }
    }
    return false;
  }

  arrow::Result<std::shared_ptr<Spill>> spill(
      const std::string& spillFile,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec) {
    std::shared_ptr<Spill> diskSpill = nullptr;
    ARROW_ASSIGN_OR_RAISE(
        auto os, arrow::io::FileOutputStream::Open(spillFile, true));
    ARROW_ASSIGN_OR_RAISE(auto start, os->Tell());
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (hasCachedPayloads(pid)) {
        auto& payloads = partitionCachedPayload_[pid];
        while (!payloads.empty()) {
          auto payload = std::move(payloads.front());
          payloads.pop_front();
          // Spill the cached payload to disk.
          RETURN_NOT_OK(payload->serialize(os.get()));
          compressTime_ += payload->getCompressTime();
          spillTime_ += payload->getWriteTime();

          if (UNLIKELY(!diskSpill)) {
            diskSpill = std::make_unique<Spill>(
                Spill::SpillType::kBatchedSpill, numPartitions_, spillFile);
          }
          ARROW_ASSIGN_OR_RAISE(auto end, os->Tell());
          DLOG(INFO) << "PayloadCache: Spilled partition " << pid
                     << " file start: " << start << ", file end: " << end
                     << ", file: " << spillFile;
          diskSpill->insertPayload(
              pid,
              payload->type(),
              payload->numRows(),
              payload->isValidityBuffer(),
              end - start,
              pool,
              codec);
          start = end;
        }
      }
    }
    RETURN_NOT_OK(os->Close());
    return diskSpill;
  }

  // for V2
  arrow::Status startSpill(const std::string& spillFile) {
    diskSpill_ = std::make_shared<Spill>(
        Spill::SpillType::kBatchedSpill, numPartitions_, spillFile);
    ARROW_ASSIGN_OR_RAISE(
        os_, arrow::io::FileOutputStream::Open(spillFile, true));
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> stopSpill() {
    RETURN_NOT_OK(os_->Close());
    os_ = nullptr;
    return std::move(diskSpill_);
  }

  arrow::Status
  spill(uint32_t pid, arrow::MemoryPool* pool, arrow::util::Codec* codec) {
    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    if (hasCachedPayloads(pid)) {
      auto& payloads = partitionCachedPayload_[pid];
      while (!payloads.empty()) {
        auto payload = std::move(payloads.front());
        payloads.pop_front();
        // Spill the cached payload to disk.
        RETURN_NOT_OK(payload->serialize(os_.get()));
        compressTime_ += payload->getCompressTime();
        spillTime_ += payload->getWriteTime();

        ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
        DLOG(INFO) << "PayloadCache: Spilled partition " << pid
                   << " file start: " << start << ", file end: " << end;
        diskSpill_->insertPayload(
            pid,
            payload->type(),
            payload->numRows(),
            payload->isValidityBuffer(),
            end - start,
            pool,
            codec);
        start = end;
      }
    }
    return arrow::Status::OK();
  }

  int64_t getCompressTime() {
    return compressTime_;
  }

  int64_t getSpillTime() {
    return spillTime_;
  }

  int64_t getWriteTime() {
    return writeTime_;
  }

 private:
  uint32_t numPartitions_;
  int64_t compressTime_{0};
  int64_t spillTime_{0};
  int64_t writeTime_{0};
  std::unordered_map<uint32_t, std::list<std::unique_ptr<BlockPayload>>>
      partitionCachedPayload_;
  // for V2
  std::shared_ptr<Spill> diskSpill_;
  std::shared_ptr<arrow::io::FileOutputStream> os_;
};

// class for BoltRowBasedSortShuffleWriter
class LocalPartitionWriter::PartitionRowWriter {
 public:
  PartitionRowWriter() = default;

  arrow::Status startSpill(
      uint32_t numPartitions,
      const std::string& spillFile) {
    diskSpill_ = std::make_shared<Spill>(
        Spill::SpillType::kBatchedSpill, numPartitions, spillFile);
    ARROW_ASSIGN_OR_RAISE(
        os_, arrow::io::FileOutputStream::Open(spillFile, true));
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<Spill>> stopSpill() {
    RETURN_NOT_OK(os_->Close());
    os_ = nullptr;
    return std::move(diskSpill_);
  }

  arrow::Status spill(
      uint32_t pid,
      std::vector<uint8_t*>& rows,
      const int64_t rawSize,
      ZstdStreamCodec* codec,
      RowVectorLayout layout,
      arrow::MemoryPool* pool) {
    auto payload = std::make_unique<RowBlockPayload>(
        folly::Range<uint8_t**>(rows.data(), rows.size()),
        rawSize,
        pool,
        codec,
        layout);
    ARROW_ASSIGN_OR_RAISE(auto start, os_->Tell());
    RETURN_NOT_OK(payload->serialize(os_.get()));
    ARROW_ASSIGN_OR_RAISE(auto end, os_->Tell());
    diskSpill_->insertPayload(pid, rows.size(), end - start);
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<Spill> diskSpill_;
  std::shared_ptr<arrow::io::FileOutputStream> os_;
};

LocalPartitionWriter::LocalPartitionWriter(
    uint32_t numPartitions,
    PartitionWriterOptions options,
    arrow::MemoryPool* pool,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs)
    : PartitionWriter(numPartitions, std::move(options), pool),
      dataFile_(dataFile),
      localDirs_(localDirs) {
  init();
}

std::string LocalPartitionWriter::nextSpilledFileDir() {
  auto spilledFileDir = getSpilledShuffleFileDir(
      localDirs_[dirSelection_], subDirSelection_[dirSelection_]);
  subDirSelection_[dirSelection_] =
      (subDirSelection_[dirSelection_] + 1) % options_.numSubDirs;
  dirSelection_ = (dirSelection_ + 1) % localDirs_.size();
  return spilledFileDir;
}

arrow::Status LocalPartitionWriter::openDataFile() {
  // open data file output stream
  std::shared_ptr<arrow::io::FileOutputStream> fout;
  ARROW_ASSIGN_OR_RAISE(fout, arrow::io::FileOutputStream::Open(dataFile_));
  if (options_.bufferedWrite) {
    // Output stream buffer is neither partition buffer memory nor ipc memory.
    ARROW_ASSIGN_OR_RAISE(
        dataFileOs_,
        arrow::io::BufferedOutputStream::Create(16384, pool_, fout));
  } else {
    dataFileOs_ = fout;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::clearResource() {
  RETURN_NOT_OK(dataFileOs_->Close());
  // When bufferedWrite = true, dataFileOs_->Close doesn't release underlying
  // buffer.
  dataFileOs_.reset();

  // release small shuffle file
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  arrow::Status status;
  for (auto& spill : spills_) {
    auto spillFile = spill->spillFile();
    spill.reset();
    arrow::Status status(fs->DeleteFile(spillFile));
    if (!status.ok()) {
      return status;
    }
  }
  return arrow::Status::OK();
}

void LocalPartitionWriter::init() {
  partitionLengths_.resize(numPartitions_, 0);
  rawPartitionLengths_.resize(numPartitions_, 0);

  // Shuffle the configured local directories. This prevents each task from
  // using the same directory for spilled files.
  std::random_device rd;
  std::default_random_engine engine(rd());
  std::shuffle(localDirs_.begin(), localDirs_.end(), engine);
  subDirSelection_.assign(localDirs_.size(), 0);
}

arrow::Status LocalPartitionWriter::mergeSpills(uint32_t partitionId) {
  auto spillId = 0;
  auto spillIter = spills_.begin();
  while (spillIter != spills_.end()) {
    ARROW_ASSIGN_OR_RAISE(auto st, dataFileOs_->Tell());
    // Read if partition exists in the spilled file and write to the final file.
    while (auto payload = (*spillIter)->nextPayload(partitionId)) {
      // May trigger spill during compression.
      RETURN_NOT_OK(payload->serialize(dataFileOs_.get()));
      compressTime_ += payload->getCompressTime();
      writeTime_ += payload->getWriteTime();
    }
    ++spillIter;
    ARROW_ASSIGN_OR_RAISE(auto ed, dataFileOs_->Tell());
    DLOG(INFO) << "Partition " << partitionId << " spilled from spillResult "
               << spillId++ << " of bytes " << ed - st;
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stop(ShuffleWriterMetrics* metrics) {
  if (stopped_) {
    return arrow::Status::OK();
  }
  stopped_ = true;
  if (isRowFormat_) {
    return stopInRowFormat(metrics);
  }

  RETURN_NOT_OK(finishSpill());

  // Open final data file.
  // If options_.bufferedWrite is set, it will acquire 16KB memory that can
  // trigger spill.
  RETURN_NOT_OK(openDataFile());

  int64_t endInFinalFile = 0;
  LOG(INFO) << "LocalPartitionWriter stopped. Total spills: " << spills_.size();
  // Iterator over pid.
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    // Record start offset.
    auto startInFinalFile = endInFinalFile;
    // Iterator over all spilled files.
    // Reading and compressing toBeCompressed payload can trigger spill.
    RETURN_NOT_OK(mergeSpills(pid));
    if (payloadCache_ && payloadCache_->hasCachedPayloads(pid)) {
      RETURN_NOT_OK(payloadCache_->write(pid, dataFileOs_.get()));
    }
    if (merger_) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finish(pid));
      if (merged) {
        // Compressing merged payload can trigger spill.
        RETURN_NOT_OK((*merged)->serialize(dataFileOs_.get()));
        compressTime_ += (*merged)->getCompressTime();
        writeTime_ += (*merged)->getWriteTime();
      }
    }
    ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
    partitionLengths_[pid] = endInFinalFile - startInFinalFile;
  }

  for (const auto& spill : spills_) {
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      if (spill->hasNextPayload(pid)) {
        return arrow::Status::Invalid("Merging from spill is not exhausted.");
      }
    }
  }

  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());

  // Close Final file. Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));

  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::requestSpill() {
  if (!spiller_ || spiller_->finished()) {
    ARROW_ASSIGN_OR_RAISE(
        auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
    spiller_ = std::make_unique<LocalSpiller>(
        numPartitions_,
        spillFile,
        options_.compressionThreshold,
        payloadPool_.get(),
        codec_.get());
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::finishSpill() {
  // Finish the spiller. No compression, no spill.
  if (spiller_ && !spiller_->finished()) {
    auto spiller = std::move(spiller_);
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(spills_.back(), spiller->finish());
    spillTime_ += spiller->getSpillTime();
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evict(
    uint32_t partitionId,
    std::unique_ptr<InMemoryPayload> inMemoryPayload,
    Evict::type evictType,
    bool reuseBuffers,
    bool hasComplexType) {
  rawPartitionLengths_[partitionId] += inMemoryPayload->getBufferSize();

  if (evictType == Evict::kSpill) {
    RETURN_NOT_OK(requestSpill());
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        inMemoryPayload->toBlockPayload(
            Payload::kUncompressed,
            payloadPool_.get(),
            nullptr,
            hasComplexType));
    RETURN_NOT_OK(spiller_->spill(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  if (evictType == Evict::kCacheNoMerge) {
    ARROW_ASSIGN_OR_RAISE(
        auto payload,
        inMemoryPayload->toBlockPayload(
            Payload::kCompressed,
            payloadPool_.get(),
            codec_.get(),
            hasComplexType));
    if (UNLIKELY(!payloadCache_)) {
      payloadCache_ = std::make_shared<PayloadCache>(numPartitions_);
    }
    RETURN_NOT_OK(payloadCache_->cache(partitionId, std::move(payload)));
    return arrow::Status::OK();
  }

  if (!merger_) {
    merger_ = std::make_shared<PayloadMerger>(
        options_,
        payloadPool_.get(),
        codec_ ? codec_.get() : nullptr,
        hasComplexType);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto merged,
      merger_->merge(partitionId, std::move(inMemoryPayload), reuseBuffers));
  if (!merged.empty()) {
    if (UNLIKELY(!payloadCache_)) {
      payloadCache_ = std::make_shared<PayloadCache>(numPartitions_);
    }
    for (auto& payload : merged) {
      RETURN_NOT_OK(payloadCache_->cache(partitionId, std::move(payload)));
    }
    merged.clear();
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill());

  int64_t reclaimed = 0;
  // Reclaim memory from payloadCache.
  if (payloadCache_ && payloadCache_->canSpill()) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    ARROW_ASSIGN_OR_RAISE(
        auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        spills_.back(),
        payloadCache_->spill(spillFile, payloadPool_.get(), codec_.get()));
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
    if (reclaimed >= size) {
      *actual = reclaimed;
      return arrow::Status::OK();
    }
  }
  // Then spill payloads from merger. Create uncompressed payloads.
  if (merger_) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      ARROW_ASSIGN_OR_RAISE(auto merged, merger_->finishForSpill(pid));
      if (merged.has_value()) {
        RETURN_NOT_OK(requestSpill());
        RETURN_NOT_OK(spiller_->spill(pid, std::move(*merged)));
      }
    }
    // This is not accurate. When the evicted partition buffers are not copied,
    // the merged ones are resized from the original buffers thus allocated from
    // partitionBufferPool.
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
    RETURN_NOT_OK(finishSpill());
  }
  *actual = reclaimed;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::populateMetrics(
    ShuffleWriterMetrics* metrics) {
  if (payloadCache_) {
    spillTime_ += payloadCache_->getSpillTime();
    writeTime_ += payloadCache_->getWriteTime();
    compressTime_ += payloadCache_->getCompressTime();
  }

  metrics->totalCompressTime += compressTime_;
  metrics->totalEvictTime += spillTime_;
  metrics->totalWriteTime += writeTime_;
  metrics->totalBytesEvicted += totalBytesEvicted_;
  metrics->totalBytesWritten += totalBytesWritten_;
  metrics->partitionLengths = std::move(partitionLengths_);
  metrics->rawPartitionLengths = std::move(rawPartitionLengths_);
  return arrow::Status::OK();
}

// for BoltShuffleWriterV2
arrow::Status LocalPartitionWriter::reclaimFixedSizeNoMerge(
    int64_t size,
    int64_t* actual) {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill());

  int64_t reclaimed = 0;
  // Reclaim memory from payloadCache.
  if (payloadCache_ && payloadCache_->canSpill()) {
    auto beforeSpill = payloadPool_->bytes_allocated();
    ARROW_ASSIGN_OR_RAISE(
        auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        spills_.back(),
        payloadCache_->spill(spillFile, payloadPool_.get(), codec_.get()));
    reclaimed += beforeSpill - payloadPool_->bytes_allocated();
    if (reclaimed >= size) {
      *actual = reclaimed;
      return arrow::Status::OK();
    }
  }
  *actual = reclaimed;
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::evictPayLoadCache() {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill());

  // spill all cached payloads
  if (payloadCache_ && payloadCache_->canSpill()) {
    ARROW_ASSIGN_OR_RAISE(
        auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
    spills_.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        spills_.back(),
        payloadCache_->spill(spillFile, payloadPool_.get(), codec_.get()));
  }
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::startClearPayLoadCacheSequential() {
  // Finish last spiller.
  RETURN_NOT_OK(finishSpill());
  ARROW_ASSIGN_OR_RAISE(
      auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
  if (UNLIKELY(!payloadCache_)) {
    payloadCache_ = std::make_shared<PayloadCache>(numPartitions_);
  }
  RETURN_NOT_OK(payloadCache_->startSpill(spillFile));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stopClearPayLoadCacheSequential() {
  spills_.emplace_back();
  ARROW_ASSIGN_OR_RAISE(spills_.back(), payloadCache_->stopSpill());
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::clearSpecificPayLoadCache(uint32_t pid) {
  RETURN_NOT_OK(payloadCache_->spill(pid, payloadPool_.get(), codec_.get()));
  return arrow::Status::OK();
}

bool LocalPartitionWriter::canSpill() {
  return payloadCache_ && payloadCache_->canSpill();
}

// for BoltRowBasedSortShuffleWriter
arrow::Status LocalPartitionWriter::evict(
    std::vector<std::vector<uint8_t*>>& rows,
    std::vector<int64_t>& partitionBytes,
    const bool isCompositeVector) {
  // evict rows in all partitions
  if (!zstdCodec_) {
    zstdCodec_ = std::make_shared<ZstdStreamCodec>(
        options_.compressionLevel, true, payloadPool_.get());
    partitionRowWriter_ = std::make_shared<PartitionRowWriter>();
  }
  RowVectorLayout layout = isCompositeVector ? RowVectorLayout::kComposite
                                             : RowVectorLayout::kColumnar;
  // open file and inputStream
  RETURN_NOT_OK(startEvictRowsSequential());
  int64_t pBytes = 0;
  // compress and flush
  for (auto pid = 0; pid < rows.size(); ++pid) {
    if (isCompositeVector) {
      pBytes = std::accumulate(
          rows[pid].begin(),
          rows[pid].end(),
          0,
          [](uint64_t sum, uint8_t* row) { return sum + *(int32_t*)row; });
    } else {
      pBytes = partitionBytes[pid];
      partitionBytes[pid] = 0;
    }
    if (!rows[pid].empty()) {
      BOLT_DCHECK(
          pBytes != 0,
          "rows = " + std::to_string(rows[pid].size()) +
              ", but bytes = " + std::to_string(pBytes));
      RETURN_NOT_OK(partitionRowWriter_->spill(
          pid,
          rows[pid],
          pBytes,
          zstdCodec_.get(),
          layout,
          payloadPool_.get()));
      rawPartitionLengths_[pid] += pBytes;
      rows[pid].clear();
    }
  }
  // closefile
  RETURN_NOT_OK(stopEvictRowsSequential());
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::startEvictRowsSequential() {
  ARROW_ASSIGN_OR_RAISE(
      auto spillFile, createTempShuffleFile(nextSpilledFileDir()));
  RETURN_NOT_OK(partitionRowWriter_->startSpill(numPartitions_, spillFile));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stopEvictRowsSequential() {
  spills_.emplace_back();
  ARROW_ASSIGN_OR_RAISE(spills_.back(), partitionRowWriter_->stopSpill());
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::stopInRowFormat(
    ShuffleWriterMetrics* metrics) {
  // Open final data file.
  // If options_.bufferedWrite is set, it will acquire 16KB memory that can
  // trigger spill.
  RETURN_NOT_OK(openDataFile());

  int64_t endInFinalFile = 0;
  LOG(INFO) << "LocalPartitionWriter stopped. Total spills: " << spills_.size();
  // Iterator over pid.
  {
    bytedance::bolt::NanosecondTimer timer(&writeTime_);
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      // Record start offset.
      auto startInFinalFile = endInFinalFile;
      RETURN_NOT_OK(mergeRowSpills(pid));
      ARROW_ASSIGN_OR_RAISE(endInFinalFile, dataFileOs_->Tell());
      partitionLengths_[pid] = endInFinalFile - startInFinalFile;
    }
  }

  ARROW_ASSIGN_OR_RAISE(totalBytesWritten_, dataFileOs_->Tell());

  // Close Final file. Clear buffered resources.
  RETURN_NOT_OK(clearResource());
  compressTime_ += zstdCodec_->getCompressTime();
  writeTime_ += zstdCodec_->getWriteTime();
  // Populate shuffle writer metrics.
  RETURN_NOT_OK(populateMetrics(metrics));
  return arrow::Status::OK();
}

arrow::Status LocalPartitionWriter::mergeRowSpills(uint32_t partitionId) {
  auto spillIter = spills_.begin();
  while (spillIter != spills_.end()) {
    // Read if partition exists in the spilled file and write to the final file.
    while (auto&& payload = (*spillIter)->nextPayload(partitionId)) {
      RETURN_NOT_OK(payload->serialize(dataFileOs_.get()));
    }
    ++spillIter;
  }
  return arrow::Status::OK();
}

} // namespace bytedance::bolt::shuffle::sparksql
