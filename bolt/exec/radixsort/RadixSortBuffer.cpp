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
#include <optional>
#include <span>

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

void cleanupSpillFileNoThrow(const std::string& path) noexcept {
  if (path.empty()) {
    return;
  }
  try {
    auto fs = filesystems::getFileSystem(path, nullptr);
    if (fs->exists(path)) {
      fs->remove(path);
    }
  } catch (const std::exception& error) {
    LOG(WARNING) << "Failed to remove radix sort spill file '" << path
                 << "': " << error.what();
  } catch (...) {
    LOG(WARNING) << "Failed to remove radix sort spill file '" << path << "'";
  }
}

void cleanupSpillFilesNoThrow(
    const std::vector<RadixSortSpillFile>& files) noexcept {
  for (const auto& file : files) {
    cleanupSpillFileNoThrow(file.path);
  }
}

void cleanupSpillRunsNoThrow(
    const std::vector<RadixSortSpillRun>& runs) noexcept {
  for (const auto& run : runs) {
    cleanupSpillFilesNoThrow(run.files);
  }
}

void appendSpillRun(
    std::vector<RadixSortSpillRun>& spillRuns,
    std::vector<RadixSortSpillFile>& files) {
  if (files.empty()) {
    return;
  }
  spillRuns.emplace_back();
  spillRuns.back().files.swap(files);
}

std::vector<std::string> copySpillFilePaths(
    const std::vector<RadixSortSpillFile>& files) {
  std::vector<std::string> paths;
  paths.reserve(files.size());
  for (const auto& file : files) {
    paths.push_back(file.path);
  }
  return paths;
}

void cleanupSpillFilePathsNoThrow(
    const std::vector<std::string>& paths) noexcept {
  for (const auto& path : paths) {
    cleanupSpillFileNoThrow(path);
  }
}

uint64_t fixedWidthValueBytes(const Type& type, vector_size_t rows) {
  if (type.kind() == TypeKind::UNKNOWN) {
    return 0;
  }
  if (type.kind() == TypeKind::BOOLEAN) {
    return BaseVector::byteSize<bool>(rows);
  }
  return checkedByteSize(rows, type.cppSizeInBytes(), "output value");
}

uint64_t spillReadBytesPerRun(common::CompressionKind compressionKind) {
  uint64_t bytes = 2 * kRadixSortSpillBufferSize;
  if (isSpillCompressionEnabled(compressionKind)) {
    bytes += kRadixSortSpillBufferSize / 10;
  }
  return bytes;
}

bool maybeReserve(
    memory::MemoryPool* pool,
    uint64_t bytes,
    tsan_atomic<bool>* nonReclaimableSection) {
  if (nonReclaimableSection == nullptr) {
    return pool->maybeReserve(bytes);
  }
  memory::ReclaimableSectionGuard guard(nonReclaimableSection);
  return pool->maybeReserve(bytes);
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
  cleanupSpillRunsNoThrow(spilledRuns_);
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
  ensureMergeFits();
  if (run_->state() == RadixSortRunState::kBuilding) {
    run_->finalize();
  }
  estimatedOutputRowSize_ = estimateOutputRowSize();
  noMoreInput_ = true;
  if (!spilledRuns_.empty()) {
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
  const auto count = static_cast<vector_size_t>(std::min<uint64_t>(
      inputRows_ - outputRows_, static_cast<uint64_t>(maxOutputRows)));

  RowVectorPtr result;
  ensureOutputFits(count);
  if (merger_ == nullptr) {
    prepareOutputShell(count);
    result = run_->getOutput(count, pool_, output_);
  } else {
    result = getMergedOutput(count);
  }
  if (result != nullptr) {
    outputRows_ += result->size();
  }
  return result;
}

bool RadixSortBuffer::canReuseOutput(vector_size_t batchSize) const {
  if (output_ == nullptr || output_.use_count() != 1 ||
      !output_->type()->kindEquals(inputType_) ||
      output_->encoding() != VectorEncoding::Simple::ROW ||
      (output_->nulls() != nullptr && !output_->nulls()->isMutable())) {
    return false;
  }

  const auto& projection = run_->projection();
  for (uint32_t channel = 0; channel < output_->childrenSize(); ++channel) {
    const auto& outputChild = output_->childAt(channel);
    const auto& childType = inputType_->childAt(channel);
    if (outputChild == nullptr || !childType->isFixedWidth() ||
        !outputChild->isFlatEncoding()) {
      // Variable-width and complex children may need new backing storage even
      // when the aggregate retained size looks large enough. Count the full
      // estimated output for these cases instead of under-reserving.
      return false;
    }

    const auto& column = projection.columns()[channel];
    const auto& materialized =
        column.source == RadixSortOutputSource::kDecodedKey
        ? run_->decodedKeysOutput_
        : run_->payloadOutput_;
    const auto& child = materialized == nullptr
        ? outputChild
        : materialized->childAt(column.sourceIndex);
    if (merger_ == nullptr) {
      // Direct output shares each materialized child between the run and the
      // RowVector shell. The shell releases this known alias before reuse.
      if (materialized == nullptr || materialized.use_count() != 1 ||
          child != outputChild || child.use_count() != 2) {
        return false;
      }
    } else if (child.use_count() != 1) {
      return false;
    }
    if (!child->isWritable()) {
      return false;
    }

    const auto valueBytes = fixedWidthValueBytes(*childType, batchSize);
    if (valueBytes != 0 &&
        (child->values() == nullptr ||
         child->values()->capacity() < valueBytes)) {
      return false;
    }
    const bool mayHaveNulls =
        column.source == RadixSortOutputSource::kDecodedKey
        ? run_->keyMayHaveNulls()[column.sourceIndex] != 0
        : run_->payloadMayHaveNulls()[column.sourceIndex] != 0;
    if (!mayHaveNulls) {
      continue;
    }
    const auto nullCount = child->getNullCount();
    if (child->nulls() == nullptr ||
        child->nulls()->capacity() < BaseVector::byteSize<bool>(batchSize) ||
        !nullCount.has_value() || *nullCount == 0) {
      // prepareForReuse() drops an all-not-null bitmap. Count output growth
      // unless cached metadata proves that the bitmap survives reuse.
      return false;
    }
  }
  return true;
}

void RadixSortBuffer::prepareOutputShell(vector_size_t outputBatchSize) {
  if (output_ != nullptr && output_.use_count() == 1 &&
      output_->type()->kindEquals(inputType_) &&
      output_->encoding() == VectorEncoding::Simple::ROW) {
    for (auto& child : output_->children()) {
      child.reset();
    }
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, outputBatchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
    return;
  }
  output_ = std::make_shared<RowVector>(
      pool_,
      inputType_,
      nullptr,
      outputBatchSize,
      std::vector<VectorPtr>(inputType_->size()));
}

void RadixSortBuffer::prepareMergeOutputVector(vector_size_t outputBatchSize) {
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, outputBatchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
    for (auto& child : output_->children()) {
      BOLT_CHECK_NOT_NULL(child);
      child->resize(outputBatchSize);
    }
    return;
  }
  output_ = BaseVector::create<RowVector>(inputType_, outputBatchSize, pool_);
}

RowVectorPtr RadixSortBuffer::getMergedOutput(vector_size_t count) {
  const auto begin = std::chrono::steady_clock::now();
  prepareMergeOutputVector(count);
  const auto selectedViews = run_->prepareMergeOutput(*output_);
  auto abortMergeOutput =
      folly::makeGuard([this]() noexcept { run_->abortMergeOutput(); });
  ensureMergeRowPointerBuffers(count);

  auto** rawKeys = mergeKeyRows_->asMutable<const char*>();
  auto** rawPayloads = mergePayloadRows_ == nullptr
      ? nullptr
      : mergePayloadRows_->asMutable<char*>();
  vector_size_t outputOffset = 0;
  const auto outputCount = merger_->collectRows(
      count,
      rawKeys,
      rawPayloads,
      selectedViews,
      [&](vector_size_t segmentSize) {
        BOLT_DCHECK_GT(segmentSize, 0);
        const auto payloads = rawPayloads == nullptr
            ? std::span<char* const>{}
            : std::span<char* const>(rawPayloads, segmentSize);
        std::span<const EncodedKeyView> externalViews;
        if (!selectedViews.empty()) {
          externalViews = selectedViews.first(segmentSize);
        }
        run_->writeMergeOutput(
            std::span<const char* const>(rawKeys, segmentSize),
            payloads,
            externalViews,
            outputOffset,
            *output_);
        outputOffset += segmentSize;
      });
  BOLT_CHECK_EQ(outputCount, count);
  BOLT_CHECK_EQ(outputOffset, count);
  run_->finishMergeOutput(*output_, count);
  abortMergeOutput.dismiss();
  const auto memoryPosition = merger_->memoryPosition();
  if (memoryPosition.has_value() && *memoryPosition == run_->size()) {
    merger_->removeMemory();
    mergeKeyRows_.reset();
    mergePayloadRows_.reset();
    run_->clear();
  }
  outputTimeUs_ += std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - begin)
                       .count();
  return output_;
}

void RadixSortBuffer::ensureMergeRowPointerBuffers(vector_size_t count) {
  const auto bytes = checkedByteSize(
      static_cast<uint64_t>(count), sizeof(char*), "merge pointer scratch");
  if (mergeKeyRows_ == nullptr || !mergeKeyRows_->isMutable() ||
      mergeKeyRows_->capacity() < bytes) {
    mergeKeyRows_ = AlignedBuffer::allocate<const char*>(count, pool_, nullptr);
  } else {
    mergeKeyRows_->setSize(bytes);
  }

  if (run_->payloadLayout() == nullptr) {
    mergePayloadRows_.reset();
    return;
  }
  if (mergePayloadRows_ == nullptr || !mergePayloadRows_->isMutable() ||
      mergePayloadRows_->capacity() < bytes) {
    mergePayloadRows_ = AlignedBuffer::allocate<char*>(count, pool_, nullptr);
  } else {
    mergePayloadRows_->setSize(bytes);
  }
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
  common::SpillReadStats stats;
  stats.spillReadTimeUs = merger_->getSpillReadTime();
  stats.spillDecompressTimeUs = merger_->getSpillDecompressTime();
  stats.spillReadIOTimeUs = merger_->getSpillReadIOTime();
  return stats;
}

size_t RadixSortBuffer::numInputRows() const {
  return inputRows_;
}

size_t RadixSortBuffer::numOutputRows() const {
  return outputRows_;
}

std::optional<uint64_t> RadixSortBuffer::estimateOutputRowSize() const {
  if (noMoreInput_) {
    return estimatedOutputRowSize_;
  }
  const auto rows = storedRows_ + run_->size();
  const auto bytes = storedBytes_ + run_->estimatedOutputBytes();
  if (rows == 0 || bytes == 0) {
    return std::nullopt;
  }
  return std::max<uint64_t>(bytes / rows, 1);
}

void RadixSortBuffer::ensureMergeFits() {
  if (spillConfig_ == nullptr || spilledRuns_.empty()) {
    return;
  }

  uint64_t spillRunCount = 0;
  for (const auto& spillRun : spilledRuns_) {
    if (!spillRun.files.empty()) {
      ++spillRunCount;
    }
  }
  if (spillRunCount == 0) {
    return;
  }

  const auto incrementBytes = checkedMultiply<uint64_t>(
      spillRunCount, spillReadBytesPerRun(spillConfig_->compressionKind));
  if (!incrementBytes.has_value() || *incrementBytes == 0) {
    return;
  }
  const auto targetIncrementBytes =
      checkedMultiply<uint64_t>(*incrementBytes, uint64_t{2});
  if (!targetIncrementBytes.has_value() || *targetIncrementBytes == 0) {
    return;
  }

  const auto availableReservationBytes = static_cast<uint64_t>(
      std::max<int64_t>(pool_->availableReservation(), 0));
  if (availableReservationBytes > *targetIncrementBytes) {
    return;
  }

  if (maybeReserve(pool_, *targetIncrementBytes, nonReclaimableSection_)) {
    return;
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(*targetIncrementBytes)
               << " before preparing radix sort merge, spill runs: "
               << spillRunCount << ", pool: " << pool_->name()
               << ", usage: " << succinctBytes(pool_->currentBytes())
               << ", reservation: " << succinctBytes(pool_->reservedBytes());
}

void RadixSortBuffer::ensureOutputFits(vector_size_t batchSize) {
  if (batchSize <= 0 || spillConfig_ == nullptr) {
    return;
  }
  const auto retainedBytesBefore = run_->retainedBytes();
  reserveOutputForCurrentState(batchSize);
  // A synchronous reclaim clears the resident run and may change the output
  // path and scratch requirements. Recompute once in that case.
  if (run_->retainedBytes() != retainedBytesBefore) {
    reserveOutputForCurrentState(batchSize);
  }
}

uint64_t RadixSortBuffer::OutputAdmissionEstimate::total() const {
  const auto paddedOutputGrowth = checkedByteSum(
      outputGrowth, outputGrowth / 5, "padded output allocation");
  return checkedByteSum(paddedOutputGrowth, scratchGrowth, "output admission");
}

RadixSortBuffer::OutputAdmissionEstimate
RadixSortBuffer::outputAdmissionEstimate(vector_size_t batchSize) {
  const auto rowSize = estimateOutputRowSize();
  if (!rowSize.has_value()) {
    return {};
  }

  const auto rows = static_cast<uint64_t>(batchSize);
  const auto outputBytes = checkedByteSize(*rowSize, rows, "output allocation");
  const auto outputGrowth = canReuseOutput(batchSize) ? 0 : outputBytes;

  uint64_t scratchGrowth;
  if (merger_ == nullptr) {
    scratchGrowth = run_->directOutputScratchAllocationBytes(batchSize);
  } else {
    scratchGrowth = run_->mergeOutputScratchAllocationBytes(batchSize);
    scratchGrowth = checkedByteSum(
        scratchGrowth,
        bufferAllocationNeed(
            mergeKeyRows_,
            pool_,
            rows,
            sizeof(char*),
            "merge key pointer scratch"),
        "merge output scratch");
    if (run_->payloadLayout() != nullptr) {
      scratchGrowth = checkedByteSum(
          scratchGrowth,
          bufferAllocationNeed(
              mergePayloadRows_,
              pool_,
              rows,
              sizeof(char*),
              "merge payload pointer scratch"),
          "merge output scratch");
    }
  }

  return {outputGrowth, scratchGrowth};
}

void RadixSortBuffer::reserveOutputForCurrentState(vector_size_t batchSize) {
  const auto need = outputAdmissionEstimate(batchSize).total();
  if (need == 0) {
    return;
  }

  const auto availableReservationBytes = static_cast<uint64_t>(
      std::max<int64_t>(pool_->availableReservation(), 0));
  if (availableReservationBytes >= need) {
    return;
  }

  if (maybeReserve(pool_, need, nonReclaimableSection_)) {
    return;
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(need)
               << " before producing radix sort output, pool: " << pool_->name()
               << ", usage: " << succinctBytes(pool_->currentBytes())
               << ", reservation: " << succinctBytes(pool_->reservedBytes());
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
    spillMemoryRun();
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
  storedRows_ += runRows;
  storedBytes_ += runBytes;

  const auto directory = spillConfig_->getSpillDirPathCb();
  RadixSortSpillWriter writer(
      fmt::format("{}/{}", directory, spillConfig_->fileNamePrefix),
      *spillConfig_,
      memory::spillMemoryPool(),
      &stats_);
  const auto writeStatsBefore = stats_.copy();
  const auto writeBegin = std::chrono::steady_clock::now();
  auto files = writer.writeRun(*storage, run_->payloadLayout().get());
  const auto writeEnd = std::chrono::steady_clock::now();
  auto cleanupUncommittedFiles =
      folly::makeGuard([&files]() { cleanupSpillFilesNoThrow(files); });
  auto serializationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          writeEnd - writeBegin)
          .count();
  const auto spilledInputBytes = writer.inputBytes();
  const auto writeStatsAfter = stats_.copy();
  const auto writerTimeUs = writeStatsAfter.spillFlushTimeUs -
      writeStatsBefore.spillFlushTimeUs + writeStatsAfter.spillWriteTimeUs -
      writeStatsBefore.spillWriteTimeUs;
  serializationTimeUs = serializationTimeUs > writerTimeUs
      ? serializationTimeUs - writerTimeUs
      : 0;
  const auto totalTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          writeEnd - spillBegin)
          .count();
  appendSpillRun(spilledRuns_, files);
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
    BOLT_CHECK_LE(locked->spilledRows, inputRows_);
    locked->spilledPartitions = 1;
    locked->spillSortTimeUs += metrics.sortTimeUs;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs += totalTimeUs;
  }
  common::updateGlobalSpillMemoryBytes(spilledInputBytes);
  common::updateGlobalSpillAppendStats(runRows, serializationTimeUs);
  if (firstSpilledPartition) {
    common::incrementGlobalSpilledPartitionStats();
  }
  common::updateGlobalSpillSortTime(metrics.sortTimeUs);
  common::updateGlobalSpillTotalTime(totalTimeUs);
}

void RadixSortBuffer::spillMemoryRun() {
  BOLT_CHECK(noMoreInput_, "Radix memory spill requires noMoreInput");
  if (run_->retainedBytes() == 0) {
    pool_->release();
    return;
  }

  const auto end = run_->size();
  uint64_t begin;
  if (merger_ == nullptr) {
    begin = run_->outputPosition();
  } else {
    const auto memoryPosition = merger_->memoryPosition();
    BOLT_CHECK(memoryPosition.has_value(), "Missing radix memory merge stream");
    begin = *memoryPosition;
  }
  BOLT_CHECK_LE(begin, end);
  BOLT_CHECK_LT(begin, end, "Radix memory spill has no remaining rows");

  const auto keyLayout = run_->keyLayout();
  const auto payloadLayout = run_->payloadLayout();
  const auto spillBegin = std::chrono::steady_clock::now();
  const auto directory = spillConfig_->getSpillDirPathCb();
  RadixSortSpillWriter writer(
      fmt::format("{}/{}-output", directory, spillConfig_->fileNamePrefix),
      *spillConfig_,
      memory::spillMemoryPool(),
      &stats_);
  const auto writeStatsBefore = stats_.copy();
  const auto writeBegin = std::chrono::steady_clock::now();
  auto files = writer.writeRun(*run_->storage(), payloadLayout.get(), begin);
  const auto writeEnd = std::chrono::steady_clock::now();
  auto cleanupReturnedFiles =
      folly::makeGuard([&files]() { cleanupSpillFilesNoThrow(files); });
  auto cleanupFiles = copySpillFilePaths(files);
  auto cleanupUncommittedFiles = folly::makeGuard(
      [&cleanupFiles]() { cleanupSpillFilePathsNoThrow(cleanupFiles); });
  cleanupReturnedFiles.dismiss();
  auto serializationTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          writeEnd - writeBegin)
          .count();
  const auto writeStatsAfter = stats_.copy();
  const auto writerTimeUs = writeStatsAfter.spillFlushTimeUs -
      writeStatsBefore.spillFlushTimeUs + writeStatsAfter.spillWriteTimeUs -
      writeStatsBefore.spillWriteTimeUs;
  serializationTimeUs = serializationTimeUs > writerTimeUs
      ? serializationTimeUs - writerTimeUs
      : 0;
  const auto spilledInputBytes = writer.inputBytes();
  const auto spilledRows = end - begin;
  const auto totalTimeUs =
      std::chrono::duration_cast<std::chrono::microseconds>(
          writeEnd - spillBegin)
          .count();

  if (merger_ == nullptr) {
    mergeKeyRows_.reset();
    mergePayloadRows_.reset();
    output_.reset();
    run_->clear();
    appendSpillRun(spilledRuns_, files);
    prepareMerge();
    cleanupUncommittedFiles.dismiss();
  } else {
    merger_->replaceMemory(
        RadixSortSpillRun{std::move(files)},
        RadixSortSpillSectionMeta::create(keyLayout, payloadLayout.get()),
        pool_,
        spillConfig_->spillUringEnabled);
    cleanupUncommittedFiles.dismiss();
    mergeKeyRows_.reset();
    mergePayloadRows_.reset();
    output_.reset();
    run_->clear();
  }

  bool firstSpilledPartition;
  {
    auto locked = stats_.wlock();
    firstSpilledPartition = locked->spilledPartitions == 0;
    ++locked->spillRuns;
    locked->spilledInputBytes += spilledInputBytes;
    locked->spilledRows += spilledRows;
    BOLT_CHECK_LE(locked->spilledRows, inputRows_);
    locked->spilledPartitions = 1;
    locked->spillSerializationTimeUs += serializationTimeUs;
    locked->spillTotalTimeUs += totalTimeUs;
  }
  common::updateGlobalSpillAppendStats(spilledRows, serializationTimeUs);
  common::updateGlobalSpillMemoryBytes(spilledInputBytes);
  if (firstSpilledPartition) {
    common::incrementGlobalSpilledPartitionStats();
  }
  common::updateGlobalSpillTotalTime(totalTimeUs);
  pool_->release();
}

void RadixSortBuffer::prepareMerge() {
  auto readBufferCache = std::make_unique<RadixSortSpillReadBufferCache>();
  std::vector<std::unique_ptr<RadixSortMergeStream>> streams;
  std::optional<size_t> memoryIndex;
  streams.reserve(spilledRuns_.size() + (run_->size() == 0 ? 0 : 1));
  auto meta = RadixSortSpillSectionMeta::create(
      run_->keyLayout(), run_->payloadLayout().get());
  for (auto& spillRun : spilledRuns_) {
    if (spillRun.files.empty()) {
      continue;
    }
    streams.push_back(makeRadixSortSpillMergeStream(
        std::move(spillRun),
        meta,
        pool_,
        spillConfig_->spillUringEnabled,
        readBufferCache.get()));
  }
  if (run_->size() > 0) {
    memoryIndex = streams.size();
    streams.push_back(makeRadixSortMemoryRunMergeStream(*run_->storage()));
  }
  merger_ = std::make_unique<RadixSortMerger>(
      run_->keyLayout(),
      std::move(streams),
      memoryIndex,
      std::move(readBufferCache));
  // The merger streams own the spill files after successful construction.
  // Clear buffer-level metadata to avoid duplicate cleanup and filesystem
  // probes after each stream removes its file at EOF.
  spilledRuns_.clear();
}

} // namespace bytedance::bolt::exec::radixsort
