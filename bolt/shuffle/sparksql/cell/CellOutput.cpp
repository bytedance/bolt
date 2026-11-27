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

#include "bolt/shuffle/sparksql/cell/CellOutput.h"

#include <folly/ScopeGuard.h>
#include <algorithm>
#include <limits>
#include <utility>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

/// Runs go to the spill file...
class SpillRunSink final : public RunSink {
 public:
  explicit SpillRunSink(ShuffleSpillFile& file) : file_(file) {}
  uint64_t reserve(size_t bytes) override {
    return file_.reserve(bytes);
  }
  void write(const void* data, size_t bytes) override {
    file_.append(data, bytes);
  }
  void patch(uint64_t position, const void* data, size_t bytes) override {
    file_.patch(position, data, bytes);
  }

 private:
  ShuffleSpillFile& file_;
};

/// ...or to the data file.
class DataRunSink final : public RunSink {
 public:
  explicit DataRunSink(DataFileWriter& file) : file_(file) {}
  uint64_t reserve(size_t bytes) override {
    return file_.reserve(bytes);
  }
  void write(const void* data, size_t bytes) override {
    file_.write(data, bytes);
  }
  void patch(uint64_t position, const void* data, size_t bytes) override {
    file_.patch(position, data, bytes);
  }

 private:
  DataFileWriter& file_;
};

/// Logs compressor totals once at finalize when compression was active.
void logCompression(const RunCompressor& compressor) {
  if (compressor.codec() == nullptr) {
    return;
  }
  const auto& stats = compressor.stats();
  if (stats.storedRuns + stats.combinedRuns + stats.separateRuns +
          stats.compressedNullBodies + stats.storedNullBodies ==
      0) {
    return;
  }
  LOG(INFO) << "CellShuffleWriter compression: " << stats.toString();
}

/// Uncompressed wire size of a run holding `payloadBytes` across `numStreams`:
/// UNCOMPRESSED runs always use kCombinedUncompressed, even above
/// kMinSeparateRunBytes, so rawPartitionLengths stays codec-independent.
uint64_t uncompressedRunBytes(uint32_t numStreams, uint64_t payloadBytes) {
  return combinedRunHeaderBytes(numStreams) + payloadBytes;
}

/// Writes partition `pid`'s resident cells to `sink` as one run and returns its
/// uncompressed payload bytes; nothing written and returns 0 when it has none.
uint64_t writeCells(
    RunCompressor& compressor,
    RunSink& sink,
    const CellWindowInput& in,
    uint32_t pid) {
  return compressor.write(
      sink,
      in.layout->numStreams(),
      [&](uint32_t s) { return in.cells->bytes(pid, s); },
      [&](uint32_t s, RunPieceFn fn) { in.cells->scan(pid, s, fn); });
}

} // namespace

LocalCellOutput::LocalCellOutput(
    PartialSpiller& composite,
    ShuffleSpillFile& spill,
    const CellLayout* layout,
    CellShuffleOptions cellOptions,
    PartitionWriterOptions options,
    memory::MemoryPool* pool)
    : PartialSpiller(spill),
      composite_(composite),
      options_(std::move(options)),
      layout_(layout),
      pool_(pool),
      compressor_(options_, cellOptions, &compressTimeNs_) {}

void LocalCellOutput::appendSegment(uint32_t pid, SpillSegment segment) {
  BOLT_CHECK_LT(numSegments_, kNoSegment, "too many cell spill segments");
  const uint32_t idx = numSegments_++;
  const uint32_t chunkIdx = idx >> kSegmentChunkShift;
  const uint32_t slot = idx & kSegmentChunkMask;
  if (chunkIdx == segmentChunks_.size()) {
    segmentChunks_.emplace_back(new SegmentChunk);
  }
  auto& chunk = *segmentChunks_[chunkIdx];
  chunk.segments[slot] = segment;
  chunk.next[slot] = kNoSegment;
  auto& chain = pendingSegments_[pid];
  if (chain.count == 0) {
    chain.head = idx;
  } else {
    segmentChunks_[chain.tail >> kSegmentChunkShift]
        ->next[chain.tail & kSegmentChunkMask] = idx;
  }
  chain.tail = idx;
  ++chain.count;
}

void LocalCellOutput::spillCells(const CellWindowInput& in) {
  auto writeGuard = scopedWrite();
  // Runs are compressed as they spill, in their final form: both disk
  // passes write compressed bytes and the merge copies the segments
  // verbatim.
  SpillRunSink sink(spill_);
  struct WrittenSegment {
    uint32_t pid;
    SpillSegment segment;
    uint64_t rawBytes;
  };
  std::vector<WrittenSegment> written;
  written.reserve(std::min<size_t>(in.numPartitions, 256));
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.runPartitions == nullptr || in.runPartitions[pid] != 0) {
      const uint64_t begin = spill_.end();
      const uint64_t rawBytes = writeCells(compressor_, sink, in, pid);
      const uint64_t end = spill_.end();
      if (end > begin) {
        BOLT_DCHECK_GT(rawBytes, 0);
        written.push_back({pid, {begin, end}, rawBytes});
      }
    }
  }
  // A spill exists to free memory: hand the workspace capacity back too.
  compressor_.releaseBuffers();
  scratch_.reset();
  spill_.flush();
  if (!written.empty()) {
    if (pendingSegments_.empty()) {
      pendingSegments_.resize(options_.numPartitions);
    }
    if (rawPartitionLengths_.empty()) {
      rawPartitionLengths_.assign(options_.numPartitions, 0);
    }
    const uint32_t numStreams = layout_->numStreams();
    for (const auto& entry : written) {
      appendSegment(entry.pid, entry.segment);
      rawPartitionLengths_[entry.pid] += static_cast<int64_t>(
          uncompressedRunBytes(numStreams, entry.rawBytes));
    }
    ++numRuns_;
  }
}

void LocalCellOutput::sealPayloads(const CellWindowInput& in) {
  auto writeGuard = scopedWrite();
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.sealPartitions != nullptr && in.sealPartitions[pid] == 0) {
      continue;
    }
    if (in.rowCounts[pid] == 0) {
      BOLT_DCHECK(pendingSegments_.empty() || pendingSegments_[pid].count == 0);
      continue;
    }
    if (sealed_.empty()) {
      sealed_.resize(options_.numPartitions);
    }
    // The partition's open payload ends here: its spilled segments since the
    // last seal become the sealed payload's.
    SealedPayload payload;
    if (!pendingSegments_.empty()) {
      payload.firstSegment = pendingSegments_[pid].head;
      payload.segmentCount = pendingSegments_[pid].count;
      pendingSegments_[pid] = {};
    }
    payload.rows = in.rowCounts[pid];
    payload.variableBytes = in.variableBytes[pid];
    resetBuffer(scratch_, 0, pool_);
    buildNullBody(in, pid, payload.rows, scratch_, pool_);
    payload.nullDecodedSize = compressor_.compressNullBody(scratch_, 0);
    payload.nullOffset = spill_.end();
    const size_t nullBytes = scratch_ ? scratch_->size() : 0;
    BOLT_CHECK_LE(
        nullBytes,
        std::numeric_limits<uint32_t>::max(),
        "null body exceeds u32 size limit");
    payload.nullLength = static_cast<uint32_t>(nullBytes);
    if (payload.nullLength > 0) {
      spill_.append(scratch_->as<char>(), nullBytes);
    }
    sealed_[pid].push_back(std::move(payload));
  }
  // A seal exists to free memory: hand the workspace capacity back too.
  compressor_.releaseBuffers();
  spill_.flush();
  ++numSeals_;
}

void LocalCellOutput::writeSealedPayload(
    DataFileWriter& out,
    uint32_t pid,
    const SealedPayload& payload,
    std::span<const uint8_t> encodingTags) {
  const uint32_t rows = payload.rows;
  BOLT_CHECK_LE(
      payload.segmentCount,
      kMaxPayloadRuns,
      "Cell payload of partition {} holds {} runs, more than maxRunCount ({})",
      pid,
      payload.segmentCount,
      kMaxPayloadRuns);
  const uint32_t runCount = payload.segmentCount;
  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  appendLe32(scratch_, rows, pool_);
  appendLe32(scratch_, runCount, pool_);
  appendLe64(scratch_, payload.variableBytes, pool_);
  appendLe32(scratch_, payload.nullLength, pool_);
  appendLe32(scratch_, payload.nullDecodedSize, pool_);
  const size_t nullAt = scratch_->size();
  resizeBuffer(
      scratch_, checkedPlus<size_t>(nullAt, payload.nullLength), pool_);
  spill_.read(
      payload.nullOffset,
      scratch_->asMutable<char>() + nullAt,
      payload.nullLength);
  appendBytes(scratch_, encodingTags.data(), encodingTags.size(), pool_);
  // The raw length counts the null body at its decompressed size.
  rawBytesWritten_ += scratch_->size() +
      (payload.nullDecodedSize == 0
           ? 0
           : payload.nullDecodedSize - payload.nullLength);
  out.write(scratch_->as<char>(), scratch_->size());

  // Each spilled segment is a run of its own, oldest first.
  copySegmentChain(out, payload.firstSegment, payload.segmentCount);
}

void LocalCellOutput::copySegmentChain(
    DataFileWriter& out,
    uint32_t head,
    uint32_t count) {
  uint32_t idx = head;
  for (uint32_t i = 0; i < count; ++i) {
    BOLT_DCHECK_NE(idx, kNoSegment);
    const auto& chunk = *segmentChunks_[idx >> kSegmentChunkShift];
    const uint32_t slot = idx & kSegmentChunkMask;
    const SpillSegment& segment = chunk.segments[slot];
    const uint64_t segmentBytes = segment.end - segment.begin;
    BOLT_CHECK_GE(segmentBytes, 1, "corrupt cell spill segment");
    ++copiedSegments_;
    out.copyFrom(spill_, segment.begin, segmentBytes);
    idx = chunk.next[slot];
  }
  BOLT_DCHECK_EQ(idx, kNoSegment);
}

void LocalCellOutput::writeOpenPayload(
    DataFileWriter& out,
    const CellWindowInput& in,
    uint32_t pid) {
  const uint64_t total = residentBytes(in, pid);
  const SegmentChain chain =
      pendingSegments_.empty() ? SegmentChain{} : pendingSegments_[pid];
  const uint64_t runs =
      static_cast<uint64_t>(chain.count) + (total > 0 ? 1 : 0);
  BOLT_CHECK_LE(
      runs,
      kMaxPayloadRuns,
      "Cell payload of partition {} holds {} runs, more than maxRunCount ({})",
      pid,
      runs,
      kMaxPayloadRuns);
  const auto runCount = static_cast<uint32_t>(runs);

  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  rawBytesWritten_ +=
      appendPayloadPrefix(in, pid, runCount, compressor_, scratch_, pool_);
  rawBytesWritten_ += scratch_->size();
  out.write(scratch_->as<char>(), scratch_->size());
  // Mid-window spilled runs first (they hold the older blocks), the
  // resident cells last.
  copySegmentChain(out, chain.head, chain.count);
  if (total > 0) {
    rawBytesWritten_ += uncompressedRunBytes(layout_->numStreams(), total);
    DataRunSink sink(out);
    const uint64_t writtenBytes = writeCells(compressor_, sink, in, pid);
    BOLT_DCHECK_EQ(writtenBytes, total);
  }
}

void LocalCellOutput::appendBlockKind() {
  const auto kind = static_cast<uint8_t>(ShuffleBlockKind::kCellPayload);
  appendBytes(scratch_, &kind, 1, pool_);
}

void LocalCellOutput::finalize(
    const CellWindowInput& in,
    ShuffleWriterMetrics& metrics) {
  // The residual window never takes a spill round-trip: whatever is still
  // in memory is written straight into the data file by the merge,
  // alongside any runs the window already spilled mid-stream.
  mergeInput_ = &in;
  if (rawPartitionLengths_.empty()) {
    rawPartitionLengths_.assign(in.numPartitions, 0);
  }
  {
    SCOPE_EXIT {
      mergeInput_ = nullptr;
    };
    // Per partition: the Cell payloads, then the composite blocks.
    mergeToDataFile(options_, spill_, {this, &composite_}, metrics);
  }
  compressor_.releaseBuffers();
  scratch_.reset();
  segmentChunks_.clear();
  segmentChunks_.shrink_to_fit();
  numSegments_ = 0;
  std::vector<SegmentChain>().swap(pendingSegments_);
  std::vector<std::vector<SealedPayload>>().swap(sealed_);
  metrics.rawPartitionLengths = std::move(rawPartitionLengths_);
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
  LOG(INFO) << "CellShuffleWriter merge: seals=" << numSeals_
            << " runs=" << numRuns_ << " segmentsCopied=" << copiedSegments_;
  logCompression(compressor_);
}

void LocalCellOutput::writePartitionShare(uint32_t pid, DataFileWriter& out) {
  BOLT_CHECK_NOT_NULL(mergeInput_, "Cell payloads merged outside finalize()");
  const uint64_t rawStart = rawBytesWritten_;
  if (!sealed_.empty()) {
    for (const auto& payload : sealed_[pid]) {
      writeSealedPayload(out, pid, payload, mergeInput_->encodingTags);
    }
    std::vector<SealedPayload>().swap(sealed_[pid]);
  }
  if (mergeInput_->rowCounts[pid] > 0) {
    writeOpenPayload(out, *mergeInput_, pid);
  }
  if (!pendingSegments_.empty()) {
    pendingSegments_[pid] = {};
  }
  rawPartitionLengths_[pid] +=
      static_cast<int64_t>(rawBytesWritten_ - rawStart);
}

RssCellOutput::RssCellOutput(
    PartitionBlockOutput& blocks,
    const CellLayout* layout,
    const PartitionWriterOptions& options,
    const CellShuffleOptions& cellOptions,
    memory::MemoryPool* pool)
    : blocks_(blocks),
      layout_(layout),
      pool_(pool),
      compressor_(options, cellOptions, &compressTimeNs_),
      rawPartitionLengths_(options.numPartitions, 0) {}

void RssCellOutput::spillCells(const CellWindowInput& /*in*/) {
  BOLT_FAIL("RssCellOutput never spills: payloads are sent whole");
}

void RssCellOutput::sealPayloads(const CellWindowInput& in) {
  sendPayloads(in);
  ++numSeals_;
}

void RssCellOutput::sendPayloads(const CellWindowInput& in) {
  BufferPtr block;
  blocks_.writeBlocks([&](uint32_t pid, BlockEmitter emit) {
    if (in.rowCounts[pid] == 0 ||
        (in.sealPartitions != nullptr && in.sealPartitions[pid] == 0)) {
      return;
    }
    buildPayload(in, pid, block);
    emit(block->asMutable<char>(), block->size());
  });
  // A seal exists to free memory: hand the workspace capacity back too.
  compressor_.releaseBuffers();
}

void RssCellOutput::buildPayload(
    const CellWindowInput& in,
    uint32_t pid,
    BufferPtr& payload) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t total = residentBytes(in, pid);
  resetBuffer(payload, 0, pool_);
  const auto kind = static_cast<uint8_t>(ShuffleBlockKind::kCellPayload);
  appendBytes(payload, &kind, 1, pool_);
  uint64_t raw = appendPayloadPrefix(
      in, pid, total > 0 ? 1 : 0, compressor_, payload, pool_);
  raw += payload->size();
  if (total > 0) {
    BufferRunSink sink(payload, pool_);
    writeCells(compressor_, sink, in, pid);
    raw += uncompressedRunBytes(numStreams, total);
  }
  rawPartitionLengths_[pid] += static_cast<int64_t>(raw);
}

void RssCellOutput::finalize(
    const CellWindowInput& in,
    ShuffleWriterMetrics& metrics) {
  sendPayloads(in);
  LOG(INFO) << "CellShuffleWriter rss: seals=" << numSeals_;
  logCompression(compressor_);
  blocks_.finish(metrics);
  metrics.rawPartitionLengths = rawPartitionLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
