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

#include "bolt/shuffle/sparksql/cell/CelebornCellOutput.h"

#include <cstring>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellPayloadAssembly.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

CelebornCellOutput::CelebornCellOutput(
    PartitionWriterOptions options,
    const CellLayout* layout,
    memory::MemoryPool* pool,
    bool framed)
    : options_(std::move(options)),
      layout_(layout),
      pool_(pool),
      client_(options_.rssClient),
      framed_(framed) {
  BOLT_CHECK_NOT_NULL(
      client_, "CellShuffleWriter on Celeborn requires an RSS client");
  if (options_.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        options_.compressionType,
        CodecOptions{
            getCodecBackend(options_.codecBackend),
            options_.compressionLevel,
            options_.checksumEnabled});
  }
  partitionLengths_.assign(options_.numPartitions, 0);
  rawPartitionLengths_.assign(options_.numPartitions, 0);
}

void CelebornCellOutput::spillRun(const CellWindowInput& /*in*/) {
  BOLT_FAIL("CelebornCellOutput never spills: windows are pushed whole");
}

void CelebornCellOutput::sealWindow(const CellWindowInput& in) {
  pushWindow(in);
  ++numSealed_;
}

void CelebornCellOutput::pushWindow(const CellWindowInput& in) {
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.rowCounts[pid] > 0) {
      pushPartition(in, pid);
    }
  }
  // A seal exists to free memory: hand the workspace capacity back too.
  payload_.reset();
  compressScratch_.reset();
}

void CelebornCellOutput::pushPartition(
    const CellWindowInput& in,
    uint32_t pid) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t total = residentBytes(in, pid);
  resizeBuffer(payload_, 0, pool_);
  if (framed_) {
    const auto kind = static_cast<uint8_t>(CellBlockKind::kPayload);
    appendBytes(payload_, &kind, 1, pool_);
  }
  appendPayloadPrefix(in, pid, total > 0 ? 1 : 0, payload_, pool_);
  uint64_t raw = payload_->size();
  if (total > 0) {
    // The run is gathered straight into its place in the payload, so a
    // stored run needs no second copy and a compressed one is copied back
    // over it. When the compression workspace cannot be funded - a seal is
    // often the moment the pool is exhausted - the stored run goes out as
    // it is rather than failing the operation meant to free memory.
    const size_t headerAt = payload_->size();
    const size_t bodyAt = headerAt + runHeaderBytes(numStreams);
    resizeBuffer(payload_, checkedPlus<size_t>(bodyAt, total), pool_);
    streamSizes_.resize(numStreams);
    gatherResident(
        in, pid, payload_->asMutable<char>() + bodyAt, streamSizes_.data());
    raw += runHeaderBytes(numStreams) + total;
    auto runLayout = RunLayout::kCombinedStored;
    uint64_t stored = total;
    try {
      const char* body = nullptr;
      runLayout = maybeCompressRun(
          codec_.get(),
          payload_->as<char>() + bodyAt,
          total,
          compressScratch_,
          pool_,
          body,
          stored,
          compressTimeNs_);
      if (runLayout == RunLayout::kCombined) {
        ::memcpy(payload_->asMutable<char>() + bodyAt, body, stored);
      }
    } catch (const std::exception&) {
      compressScratch_.reset();
      runLayout = RunLayout::kCombinedStored;
      stored = total;
    }
    writeRunHeader(
        payload_->asMutable<char>() + headerAt,
        runLayout,
        stored,
        streamSizes_.data(),
        numStreams);
    payload_->setSize(bodyAt + stored);
  }
  rawPartitionLengths_[pid] += static_cast<int64_t>(raw);
  push(pid, payload_);
}

void CelebornCellOutput::spillRaw(const RawBlockSource& next) {
  BufferPtr block;
  for (uint32_t pid = 0; pid < options_.numPartitions; ++pid) {
    while (next(pid, block)) {
      push(pid, block);
    }
  }
}

void CelebornCellOutput::push(uint32_t pid, BufferPtr& bytes) {
  const uint64_t start = currentTimeNs();
  const int32_t pushed = client_->pushPartitionData(
      static_cast<int32_t>(pid),
      bytes->asMutable<char>(),
      static_cast<int64_t>(bytes->size()));
  pushTimeNs_ += currentTimeNs() - start;
  partitionLengths_[pid] += pushed;
  pushedBytes_ += pushed;
  ++numPushes_;
}

void CelebornCellOutput::finalize(
    const CellWindowInput& in,
    bool windowHasData,
    ShuffleWriterMetrics& metrics) {
  if (windowHasData) {
    pushWindow(in);
  }
  client_->stop();
  LOG(INFO) << "CellShuffleWriter celeborn: sealed windows=" << numSealed_
            << ", pushes=" << numPushes_ << ", pushed bytes=" << pushedBytes_;

  metrics.partitionLengths = std::move(partitionLengths_);
  metrics.rawPartitionLengths = std::move(rawPartitionLengths_);
  metrics.totalBytesWritten = pushedBytes_;
  metrics.totalBytesEvicted = pushedBytes_;
  metrics.totalWriteTime = 0;
  metrics.totalEvictTime = static_cast<int64_t>(pushTimeNs_);
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSealed_);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
