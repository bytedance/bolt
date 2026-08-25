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

#include "bolt/exec/radixsort/RadixSortBuffer.h"

#include <algorithm>
#include <limits>

#include <folly/ScopeGuard.h>

#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/Spill.h"

namespace bytedance::bolt::exec::radixsort {

namespace {

bool supportsOrderKey(const Type& type) {
  if (!RadixSortKeyCodec::supportsEncodeDecode(type)) {
    return false;
  }
  switch (type.kind()) {
    case TypeKind::UNKNOWN:
      return true;
    case TypeKind::ARRAY:
      return supportsOrderKey(*type.childAt(0));
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (!supportsOrderKey(*type.childAt(child))) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return supportsOrderKey(*type.childAt(0)) &&
          supportsOrderKey(*type.childAt(1));
    case TypeKind::VARIANT:
    case TypeKind::OPAQUE:
    case TypeKind::FUNCTION:
      return false;
    default:
      return type.isOrderable();
  }
}

void cleanupSpillFilesNoThrow(
    const std::vector<RadixSortSpillFile>& files) noexcept {
  for (const auto& file : files) {
    if (file.path.empty()) {
      continue;
    }
    try {
      auto fs = filesystems::getFileSystem(file.path, nullptr);
      if (fs->exists(file.path)) {
        fs->remove(file.path);
      }
    } catch (const std::exception& error) {
      LOG(WARNING) << "Failed to remove radix sort spill file '" << file.path
                   << "': " << error.what();
    } catch (...) {
      LOG(WARNING) << "Failed to remove radix sort spill file '" << file.path
                   << "'";
    }
  }
}

} // namespace

RadixSortBuffer::RadixSortBuffer(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& sortColumnIndices,
    const std::vector<CompareFlags>& sortCompareFlags,
    memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    uint64_t spillMemoryThreshold,
    OperatorCtx* operatorCtx,
    tsan_atomic<bool>* nonReclaimableSection)
    : inputType_(inputType),
      pool_(pool),
      nonReclaimableSection_(nonReclaimableSection),
      sortCompareFlags_(sortCompareFlags),
      spillConfig_(spillConfig),
      spillMemoryThreshold_(spillMemoryThreshold),
      operatorCtx_(operatorCtx) {
  std::vector<std::string> keyNames;
  std::vector<TypePtr> keyTypes;
  keyNames.reserve(sortColumnIndices.size());
  keyTypes.reserve(sortColumnIndices.size());
  directKeyChannels_.reserve(sortColumnIndices.size());
  bool validKeyChannels = true;
  bool supportedKeyTypes = true;
  std::string unsupportedKeyType;
  for (const auto channel : sortColumnIndices) {
    if (channel >= inputType_->size()) {
      validKeyChannels = false;
      continue;
    }
    const auto& type = inputType_->childAt(channel);
    if (!supportsOrderKey(*type)) {
      supportedKeyTypes = false;
      unsupportedKeyType = type->toString();
    }
    keyNames.push_back(inputType_->nameOf(channel));
    keyTypes.push_back(type);
    directKeyChannels_.emplace_back(channel);
  }
  BOLT_CHECK(validKeyChannels, "RadixSortBuffer key channel is out of range");
  BOLT_CHECK(
      supportedKeyTypes,
      "RadixSortBuffer does not support ORDER BY key type {}",
      unsupportedKeyType);

  keyType_ = ROW(std::move(keyNames), std::move(keyTypes));
  keyMayHaveNulls_.resize(keyType_->size(), 0);
  run_ = makeRun();
}

RadixSortBuffer::~RadixSortBuffer() {
  merger_.reset();
  run_.reset();
  cleanupSpillFilesNoThrow(spilledFiles_);
  pool_->release();
}

void RadixSortBuffer::addInput(const VectorPtr& input) {
  BOLT_CHECK(!noMoreInput_, "RadixSortBuffer cannot add input after sorting");
  BOLT_CHECK_NOT_NULL(input);
  const auto* rows = input->as<RowVector>();
  BOLT_CHECK_NOT_NULL(rows, "RadixSortBuffer input must be a RowVector");
  ensureInputFits(input);

  run_->append(*rows);
  inputRows_ += input->size();
}

void RadixSortBuffer::noMoreInput() {
  if (run_->state() == RadixSortRunState::kBuilding) {
    run_->finalize();
  }
  noMoreInput_ = true;
  if (!spilledFiles_.empty()) {
    prepareMerge();
  }
  pool_->release();
}

RowVectorPtr RadixSortBuffer::getOutput(vector_size_t maxOutputRows) {
  BOLT_CHECK(noMoreInput_, "RadixSortBuffer output requires noMoreInput");
  if (outputRows_ == inputRows_) {
    return nullptr;
  }
  RowVectorPtr result;
  if (merger_ == nullptr) {
    result = run_->getOutput(maxOutputRows, pool_);
  } else {
    const auto begin = std::chrono::steady_clock::now();
    const auto count = static_cast<vector_size_t>(std::min<uint64_t>(
        inputRows_ - outputRows_, static_cast<uint64_t>(maxOutputRows)));
    if (mergeKeyRows_ == nullptr ||
        mergeKeyRows_->capacity() <
            static_cast<uint64_t>(count) * sizeof(char*)) {
      mergeKeyRows_ =
          AlignedBuffer::allocate<const char*>(count, pool_, nullptr);
      mergePayloadRows_ = AlignedBuffer::allocate<char*>(count, pool_, nullptr);
    } else {
      mergeKeyRows_->setSize(static_cast<uint64_t>(count) * sizeof(char*));
      mergePayloadRows_->setSize(static_cast<uint64_t>(count) * sizeof(char*));
    }
    auto** rawKeys = mergeKeyRows_->asMutable<const char*>();
    auto** rawPayloads = mergePayloadRows_->asMutable<char*>();
    const auto outputCount = merger_->collectRows(count, rawKeys, rawPayloads);
    if (outputCount > 0) {
      result = run_->getOutput(
          std::span<const char* const>(rawKeys, outputCount),
          std::span<char* const>(rawPayloads, outputCount),
          pool_);
      merger_->releaseRetainedBuffers();
    }
    outputTimeUs_ += std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  }
  if (result != nullptr) {
    outputRows_ += result->size();
  }
  return result;
}

std::optional<common::SortStats> RadixSortBuffer::sortStats() const {
  const auto& metrics = run_->metrics();
  common::SortStats stats;
  stats.sortColToRowTimeUs = encodeTimeUs_ + appendTimeUs_ +
      metrics.encodeTimeUs + metrics.appendTimeUs;
  stats.sortInSortTimeUs = sortTimeUs_ + metrics.sortTimeUs;
  stats.sortOutputTimeUs = outputTimeUs_ + metrics.outputTimeUs;
  return stats;
}

std::optional<common::SpillReadStats> RadixSortBuffer::spillReadStats() const {
  if (merger_ == nullptr && completedSpillReadStats_.spillReadTimeUs == 0 &&
      completedSpillReadStats_.spillDecompressTimeUs == 0 &&
      completedSpillReadStats_.spillReadIOTimeUs == 0) {
    return std::nullopt;
  }
  auto stats = completedSpillReadStats_;
  if (merger_ != nullptr) {
    stats.spillReadTimeUs += merger_->getSpillReadTime();
    stats.spillDecompressTimeUs += merger_->getSpillDecompressTime();
    stats.spillReadIOTimeUs += merger_->getSpillReadIOTime();
  }
  return stats;
}

size_t RadixSortBuffer::numInputRows() const {
  return inputRows_;
}

size_t RadixSortBuffer::numOutputRows() const {
  return outputRows_;
}

std::optional<uint64_t> RadixSortBuffer::estimateOutputRowSize() const {
  const auto rows = storedRows_ + run_->size();
  const auto bytes = storedBytes_ + run_->estimatedOutputBytes();
  if (rows == 0 || bytes == 0) {
    return std::nullopt;
  }
  return std::max<uint64_t>(bytes / rows, 1);
}

std::unique_ptr<RadixSortRun> RadixSortBuffer::makeRun() const {
  auto options = runOptions_;
  options.initialKeyMayHaveNulls = keyMayHaveNulls_;
  options.initialPayloadMayHaveNulls = payloadMayHaveNulls_;
  options.initialVariableKeysFitRadixPrefix = variableKeysFitRadixPrefix_;
  return RadixSortRun::create(
      pool_,
      inputType_,
      keyType_,
      sortCompareFlags_,
      directKeyChannels_,
      std::move(options));
}

void RadixSortBuffer::ensureInputFits(const VectorPtr& input) {
  if (spillConfig_ == nullptr || run_->size() == 0) {
    return;
  }
  if (testingTriggerSpill()) {
    spill();
    return;
  }
  if (spillMemoryThreshold_ != 0 &&
      pool_->currentBytes() > spillMemoryThreshold_) {
    spill();
    return;
  }

  const auto currentMemoryUsage =
      static_cast<uint64_t>(std::max<int64_t>(pool_->currentBytes(), 0));
  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = static_cast<uint64_t>(
      std::max<int64_t>(pool_->availableReservation(), 0));
  const auto flatInputBytes = input->estimateFlatSize();
  const auto retainedBytes =
      static_cast<uint64_t>(std::max<int64_t>(run_->retainedBytes(), 0));
  const auto averageRetainedBytes =
      run_->size() == 0 ? 0 : retainedBytes / run_->size();
  const auto estimatedRunIncrementBytes =
      checkedMultiply<uint64_t>(averageRetainedBytes, input->size())
          .value_or(std::numeric_limits<uint64_t>::max());
  const auto estimatedIncrementalBytes =
      std::max<uint64_t>(flatInputBytes, estimatedRunIncrementBytes);
  if (estimatedIncrementalBytes == 0) {
    return;
  }
  const auto estimatedReservationBytes =
      estimatedIncrementalBytes > std::numeric_limits<uint64_t>::max() / 2
      ? std::numeric_limits<uint64_t>::max()
      : estimatedIncrementalBytes * 2;
  if (availableReservationBytes > minReservationBytes &&
      availableReservationBytes > estimatedReservationBytes) {
    return;
  }

  const auto growthReservationBytes =
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100;
  const auto targetIncrementBytes =
      std::max(estimatedReservationBytes, growthReservationBytes);
  bool reserved = false;
  if (nonReclaimableSection_ != nullptr) {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    reserved = pool_->maybeReserve(targetIncrementBytes);
  } else {
    reserved = pool_->maybeReserve(targetIncrementBytes);
  }
  if (reserved) {
    return;
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool_->name()
               << ", usage: " << succinctBytes(pool_->currentBytes())
               << ", reservation: " << succinctBytes(pool_->reservedBytes())
               << ", spill current radix sort run before adding input";
  spill();
}

void RadixSortBuffer::spill() {
  BOLT_CHECK_NOT_NULL(spillConfig_, "RadixSortBuffer spill requires config");
  if (operatorCtx_ != nullptr) {
    auto* spillConf = const_cast<common::SpillConfig*>(spillConfig_);
    operatorCtx_->adjustSpillCompressionKind(spillConf);
  }
  if (noMoreInput_) {
    spillRemainingOutput();
    return;
  }
  spillBuildingRun();
}

void RadixSortBuffer::spillBuildingRun() {
  if (run_->size() == 0) {
    return;
  }
  const auto spillBegin = std::chrono::steady_clock::now();
  run_->finalize();
  const auto metrics = run_->metrics();
  encodeTimeUs_ += metrics.encodeTimeUs;
  appendTimeUs_ += metrics.appendTimeUs;
  sortTimeUs_ += metrics.sortTimeUs;
  const auto mergeNullability = [](auto& target, const auto& source) {
    if (target.empty()) {
      target = source;
      return;
    }
    for (uint32_t column = 0; column < target.size(); ++column) {
      target[column] |= source[column];
    }
  };
  mergeNullability(keyMayHaveNulls_, run_->keyMayHaveNulls());
  mergeNullability(payloadMayHaveNulls_, run_->payloadMayHaveNulls());
  if (run_->keyLayout().isVariable()) {
    variableKeysFitRadixPrefix_ &= run_->variableKeysFitRadixPrefix();
  }
  const auto* storage = run_->storage();
  const auto runRows = run_->size();
  const auto runBytes = run_->estimatedOutputBytes();
  const auto spilledInputBytes = storage->allocatedBytes();
  storedRows_ += runRows;
  storedBytes_ += runBytes;

  const auto directory = spillConfig_->getSpillDirPathCb();
  RadixSortSpillWriter writer(
      fmt::format("{}/{}", directory, spillConfig_->fileNamePrefix),
      *spillConfig_,
      memory::spillMemoryPool(),
      &stats_);
  const auto writeBegin = std::chrono::steady_clock::now();
  auto files = writer.writeRun(*storage, run_->payloadLayout().get());
  auto cleanupUncommittedFiles =
      folly::makeGuard([&files]() { cleanupSpillFilesNoThrow(files); });
  const auto serializationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - writeBegin)
          .count();
  const auto totalTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - spillBegin)
          .count();
  const auto committedFileCount = files.size();
  spilledFiles_.insert(
      spilledFiles_.end(),
      std::make_move_iterator(files.begin()),
      std::make_move_iterator(files.end()));
  cleanupUncommittedFiles.dismiss();
  run_->clear();
  run_ = makeRun();

  bool firstSpilledPartition;
  {
    auto locked = stats_.wlock();
    firstSpilledPartition = locked->spilledPartitions == 0;
    ++locked->spillRuns;
    locked->spilledInputBytes += spilledInputBytes;
    locked->spilledRows += runRows;
    locked->spilledPartitions = 1;
    locked->spilledFiles += committedFileCount;
    locked->spillFillTimeUs += metrics.encodeTimeUs + metrics.appendTimeUs;
    locked->spillSortTimeUs += metrics.sortTimeUs;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs += totalTimeUs;
  }
  common::updateGlobalSpillMemoryBytes(spilledInputBytes);
  common::updateGlobalSpillAppendStats(runRows, serializationTimeUs);
  if (firstSpilledPartition) {
    common::incrementGlobalSpilledPartitionStats();
  }
  for (uint64_t file = 0; file < committedFileCount; ++file) {
    common::incrementGlobalSpilledFiles();
  }
  common::updateGlobalSpillFillTime(
      metrics.encodeTimeUs + metrics.appendTimeUs);
  common::updateGlobalSpillSortTime(metrics.sortTimeUs);
  common::updateGlobalSpillTotalTime(totalTimeUs);
}

void RadixSortBuffer::spillRemainingOutput() {
  if (outputRows_ == inputRows_ || outputStageSpilled_) {
    pool_->release();
    return;
  }

  constexpr vector_size_t kSpillBatchRows = 2048;
  BufferPtr keyRows =
      AlignedBuffer::allocate<const char*>(kSpillBatchRows, pool_, nullptr);
  BufferPtr payloadRows =
      AlignedBuffer::allocate<char*>(kSpillBatchRows, pool_, nullptr);
  auto** rawKeys = keyRows->asMutable<const char*>();
  auto** rawPayloads = payloadRows->asMutable<char*>();

  const auto directory = spillConfig_->getSpillDirPathCb();
  RadixSortSpillWriter writer(
      fmt::format("{}/{}-output", directory, spillConfig_->fileNamePrefix),
      *spillConfig_,
      memory::spillMemoryPool(),
      &stats_);

  const auto spillBegin = std::chrono::steady_clock::now();
  uint64_t spilledRows = 0;
  uint64_t serializationTimeUs = 0;
  const auto remainingRows = inputRows_ - outputRows_;
  const auto keyLayout = run_->keyLayout();
  const auto* payloadLayout = run_->payloadLayout().get();
  auto writeRows = [&](vector_size_t count) {
    const auto writeBegin = std::chrono::steady_clock::now();
    writer.writeRows(
        keyLayout,
        payloadLayout,
        rawKeys,
        payloadLayout == nullptr ? nullptr : rawPayloads,
        count);
    serializationTimeUs +=
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - writeBegin)
            .count();
    spilledRows += count;
  };
  while (spilledRows < remainingRows) {
    const auto requested = static_cast<vector_size_t>(std::min<uint64_t>(
        remainingRows - spilledRows, static_cast<uint64_t>(kSpillBatchRows)));
    const auto count = merger_ != nullptr
        ? merger_->collectRows(requested, rawKeys, rawPayloads)
        : run_->collectRemainingRows(requested, rawKeys, rawPayloads);
    writeRows(count);
    if (merger_ != nullptr) {
      merger_->releaseRetainedBuffers();
    }
  }
  auto files = writer.finishRows();
  const auto spilledInputBytes = writer.inputBytes();
  const auto committedFileCount = files.size();

  accumulateSpillReadStats();
  run_->clear();
  merger_.reset();
  mergeKeyRows_.reset();
  mergePayloadRows_.reset();
  keyRows.reset();
  payloadRows.reset();
  spilledFiles_ = std::move(files);
  outputStageSpilled_ = true;
  const auto totalTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - spillBegin)
          .count();
  bool firstSpilledPartition;
  {
    auto locked = stats_.wlock();
    firstSpilledPartition = locked->spilledPartitions == 0;
    ++locked->spillRuns;
    locked->spilledInputBytes += spilledInputBytes;
    locked->spilledRows += spilledRows;
    locked->spilledPartitions = 1;
    locked->spilledFiles += committedFileCount;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs += totalTimeUs;
  }
  common::updateGlobalSpillAppendStats(spilledRows, serializationTimeUs);
  common::updateGlobalSpillMemoryBytes(spilledInputBytes);
  if (firstSpilledPartition) {
    common::incrementGlobalSpilledPartitionStats();
  }
  for (uint64_t file = 0; file < committedFileCount; ++file) {
    common::incrementGlobalSpilledFiles();
  }
  common::updateGlobalSpillTotalTime(totalTimeUs);
  prepareMerge();
  pool_->release();
}

void RadixSortBuffer::accumulateSpillReadStats() {
  if (merger_ == nullptr) {
    return;
  }
  completedSpillReadStats_.spillReadTimeUs += merger_->getSpillReadTime();
  completedSpillReadStats_.spillDecompressTimeUs +=
      merger_->getSpillDecompressTime();
  completedSpillReadStats_.spillReadIOTimeUs += merger_->getSpillReadIOTime();
}

void RadixSortBuffer::prepareMerge() {
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.reserve(spilledFiles_.size() + (run_->size() == 0 ? 0 : 1));
  auto meta = RadixRow2RowSerdeMeta::create(
      run_->keyLayout(), run_->payloadLayout().get());
  auto readBufferCache = std::make_unique<RadixSortSpillReadBufferCache>();
  for (const auto& file : spilledFiles_) {
    streams.push_back(std::make_unique<RadixSortSpillFileMergeStream>(
        file,
        meta,
        run_->payloadLayout().get(),
        memory::spillMemoryPool(),
        spillConfig_->spillUringEnabled,
        readBufferCache.get()));
  }
  if (run_->size() > 0) {
    streams.push_back(
        std::make_unique<RadixSortMemoryRunMergeStream>(*run_->storage()));
  }
  merger_ = std::make_unique<RadixSortMerger>(
      run_->keyLayout(), std::move(streams), std::move(readBufferCache));
  // The merger streams own the spill files after successful construction.
  // Clear buffer-level metadata to avoid duplicate cleanup and filesystem
  // probes after each stream removes its file at EOF.
  spilledFiles_.clear();
}

} // namespace bytedance::bolt::exec::radixsort
