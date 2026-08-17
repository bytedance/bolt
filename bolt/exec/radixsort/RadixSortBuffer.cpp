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
    case TypeKind::VARIANT:
    case TypeKind::OPAQUE:
    case TypeKind::FUNCTION:
      return false;
    default:
      return type.isOrderable();
  }
}

bool requiresBitExactOutput(const Type& type) {
  switch (type.kind()) {
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return true;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (requiresBitExactOutput(*type.childAt(child))) {
          return true;
        }
      }
      return false;
    default:
      return false;
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
  BOLT_CHECK_NOT_NULL(inputType_);
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK_GT(sortColumnIndices.size(), 0);
  BOLT_CHECK_EQ(sortColumnIndices.size(), sortCompareFlags.size());

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

  bitExactRequired_.resize(inputType_->size(), false);
  for (const auto channel : sortColumnIndices) {
    bitExactRequired_[channel] =
        requiresBitExactOutput(*inputType_->childAt(channel));
  }

  keyType_ = ROW(std::move(keyNames), std::move(keyTypes));
  keyMayHaveNulls_.resize(keyType_->size(), 0);
  run_ = makeRun();
}

RadixSortBuffer::~RadixSortBuffer() {
  run_.reset();
  pool_->release();
}

void RadixSortBuffer::addInput(const VectorPtr& input) {
  BOLT_CHECK(!noMoreInput_, "RadixSortBuffer cannot add input after sorting");
  BOLT_CHECK_NOT_NULL(input);
  const auto* rows = input->as<RowVector>();
  BOLT_CHECK_NOT_NULL(rows, "RadixSortBuffer input must be a RowVector");
  BOLT_CHECK(
      input->type()->equivalent(*inputType_),
      "RadixSortBuffer input type does not match");
  ensureInputFits(input);

  run_->append(*rows);
  inputRows_ += input->size();
  inputFlatBytes_ += input->estimateFlatSize();
}

void RadixSortBuffer::noMoreInput() {
  BOLT_CHECK(!noMoreInput_, "RadixSortBuffer can be finalized only once");
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
  BOLT_CHECK_GT(maxOutputRows, 0);
  RowVectorPtr result;
  if (merger_ == nullptr) {
    result = run_->getOutput(maxOutputRows, pool_);
  } else {
    const auto begin = std::chrono::steady_clock::now();
    if (mergeKeyRows_ == nullptr ||
        mergeKeyRows_->capacity() <
            static_cast<uint64_t>(maxOutputRows) * sizeof(char*)) {
      mergeKeyRows_ =
          AlignedBuffer::allocate<const char*>(maxOutputRows, pool_, nullptr);
      mergePayloadRows_ =
          AlignedBuffer::allocate<char*>(maxOutputRows, pool_, nullptr);
    } else {
      mergeKeyRows_->setSize(
          static_cast<uint64_t>(maxOutputRows) * sizeof(char*));
      mergePayloadRows_->setSize(
          static_cast<uint64_t>(maxOutputRows) * sizeof(char*));
    }
    auto** rawKeys = mergeKeyRows_->asMutable<const char*>();
    auto** rawPayloads = mergePayloadRows_->asMutable<char*>();
    const auto count = static_cast<vector_size_t>(std::min<uint64_t>(
        inputRows_ - outputRows_, static_cast<uint64_t>(maxOutputRows)));
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
  if (merger_ == nullptr) {
    return std::nullopt;
  }
  return common::SpillReadStats{
      merger_->getSpillReadTime(),
      merger_->getSpillDecompressTime(),
      merger_->getSpillReadIOTime()};
}

size_t RadixSortBuffer::numInputRows() const {
  return inputRows_;
}

size_t RadixSortBuffer::numOutputRows() const {
  return outputRows_;
}

std::optional<uint64_t> RadixSortBuffer::estimateOutputRowSize() const {
  if (inputRows_ == 0 || inputFlatBytes_ == 0) {
    return std::nullopt;
  }
  return std::max<uint64_t>(inputFlatBytes_ / inputRows_, 1);
}

std::unique_ptr<RadixSortRun> RadixSortBuffer::makeRun() const {
  auto options = runOptions_;
  options.initialKeyMayHaveNulls = keyMayHaveNulls_;
  options.initialPayloadMayHaveNulls = payloadMayHaveNulls_;
  return RadixSortRun::create(
      pool_,
      inputType_,
      keyType_,
      sortCompareFlags_,
      directKeyChannels_,
      bitExactRequired_,
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
  const auto& metrics = run_->metrics();
  encodeTimeUs_ += metrics.encodeTimeUs;
  appendTimeUs_ += metrics.appendTimeUs;
  sortTimeUs_ += metrics.sortTimeUs;
  const auto mergeNullability = [](auto& target, const auto& source) {
    if (target.empty()) {
      target = source;
      return;
    }
    BOLT_CHECK_EQ(target.size(), source.size());
    for (uint32_t column = 0; column < target.size(); ++column) {
      target[column] |= source[column];
    }
  };
  mergeNullability(keyMayHaveNulls_, run_->keyMayHaveNulls());
  mergeNullability(payloadMayHaveNulls_, run_->payloadMayHaveNulls());
  const auto* storage = run_->storage();
  const auto directory = spillConfig_->getSpillDirPathCb();
  RadixSortSpillWriter writer(
      fmt::format("{}/{}", directory, spillConfig_->fileNamePrefix),
      spillConfig_->spillIOConfig(1),
      memory::spillMemoryPool(),
      &stats_);
  const auto writeBegin = std::chrono::steady_clock::now();
  auto files = writer.writeRun(*storage, run_->payloadLayout().get());
  const auto serializationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - writeBegin)
          .count();
  spilledFiles_.insert(spilledFiles_.end(), files.begin(), files.end());
  {
    auto locked = stats_.wlock();
    ++locked->spillRuns;
    locked->spilledInputBytes += storage->allocatedBytes();
    locked->spilledRows += run_->size();
    locked->spilledPartitions = 1;
    locked->spillFillTimeUs += metrics.encodeTimeUs + metrics.appendTimeUs;
    locked->spillSortTimeUs += metrics.sortTimeUs;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs +=
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - spillBegin)
            .count();
  }
  run_->clear();
  run_ = makeRun();
}

void RadixSortBuffer::spillRemainingOutput() {
  if (outputRows_ == inputRows_ || outputStageSpilled_) {
    pool_->release();
    return;
  }
  BOLT_CHECK_GT(inputRows_, outputRows_);
  BOLT_CHECK(
      run_->state() == RadixSortRunState::kSortedInMemory ||
          run_->state() == RadixSortRunState::kConsumed,
      "RadixSortBuffer output-stage spill requires finalized output state");

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
      spillConfig_->spillIOConfig(1),
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
    BOLT_CHECK_GT(count, 0);
    writeRows(count);
    if (merger_ != nullptr) {
      merger_->releaseRetainedBuffers();
    }
  }
  BOLT_CHECK_EQ(spilledRows, remainingRows);
  auto files = writer.finishRows();
  BOLT_CHECK(!files.empty());

  run_->clear();
  merger_.reset();
  mergeKeyRows_.reset();
  mergePayloadRows_.reset();
  keyRows.reset();
  payloadRows.reset();
  spilledFiles_ = std::move(files);
  outputStageSpilled_ = true;
  {
    auto locked = stats_.wlock();
    ++locked->spillRuns;
    locked->spilledRows += spilledRows;
    locked->spilledPartitions = 1;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs +=
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - spillBegin)
            .count();
  }
  prepareMerge();
  pool_->release();
}

void RadixSortBuffer::prepareMerge() {
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  streams.reserve(spilledFiles_.size() + (run_->size() == 0 ? 0 : 1));
  RadixSortSpillRunMeta meta{
      run_->keyLayout(),
      static_cast<uint32_t>(
          run_->payloadLayout() == nullptr
              ? 0
              : run_->payloadLayout()->rowWidth())};
  for (const auto& file : spilledFiles_) {
    streams.push_back(std::make_unique<RadixSortSpillFileMergeStream>(
        file,
        meta,
        run_->payloadLayout().get(),
        memory::spillMemoryPool(),
        spillConfig_->spillUringEnabled));
  }
  if (run_->size() > 0) {
    streams.push_back(
        std::make_unique<RadixSortMemoryRunMergeStream>(*run_->storage()));
  }
  BOLT_CHECK(!streams.empty());
  merger_ =
      std::make_unique<RadixSortMerger>(run_->keyLayout(), std::move(streams));
}

} // namespace bytedance::bolt::exec::radixsort
