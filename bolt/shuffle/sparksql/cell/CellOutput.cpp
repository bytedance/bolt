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
#include <sstream>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
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
  uint64_t position() const override {
    return file_.end();
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
  uint64_t position() const override {
    return file_.position();
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

/// Bytes of a SEPARATE run header: layout byte, stored and decoded size
/// per stream.
uint64_t separateRunHeaderBytes(uint32_t numStreams) {
  return 1 + 16ull * numStreams;
}

/// The compressor's totals at INFO, its per-stream detail at VLOG(1).
void logCompression(const RunCompressor& compressor, const CellLayout& layout) {
  LOG(INFO) << "CellShuffleWriter compression: "
            << compressor.stats().toString();
  if (!VLOG_IS_ON(1)) {
    return;
  }
  const auto& streams = compressor.streamStats();
  for (uint32_t s = 0; s < streams.size() && s < layout.numStreams(); ++s) {
    const auto& stats = streams[s];
    if (stats.tried + stats.skipped == 0) {
      continue;
    }
    VLOG(1) << "CellShuffleWriter compression stream " << s << " (column "
            << layout.stream(s).column << ", kind "
            << static_cast<int>(layout.stream(s).kind)
            << "): tried=" << stats.tried << " compressed=" << stats.compressed
            << " skipped=" << stats.skipped << " ratio="
            << (stats.inBytes > 0 ? double(stats.outBytes) / stats.inBytes
                                  : 1.0);
  }
}

/// The per-stream sizes of partition `pid`'s resident cells.
std::vector<uint64_t> residentSizes(const CellWindowInput& in, uint32_t pid) {
  std::vector<uint64_t> sizes(in.layout->numStreams());
  for (uint32_t s = 0; s < sizes.size(); ++s) {
    sizes[s] = in.cells->bytes(pid, s);
  }
  return sizes;
}

} // namespace

LocalCellOutput::LocalCellOutput(
    PartialSpiller& composite,
    const CellLayout* layout,
    CellShuffleOptions cellOptions,
    PartitionWriterOptions options,
    memory::MemoryPool* pool)
    : PartialSpiller(options, "cell"),
      composite_(composite),
      options_(std::move(options)),
      layout_(layout),
      cellOptions_(cellOptions),
      pool_(pool),
      compressor_(options_, cellOptions_, pool, &compressTimeNs_) {
  openFrom_.assign(options_.numPartitions, 0);
  sealed_.resize(options_.numPartitions);
}

void LocalCellOutput::spillRun(const CellWindowInput& in) {
  const uint32_t numStreams = layout_->numStreams();
  auto& run = runs_.emplace_back();
  // Runs are compressed as they spill, so both disk passes write compressed
  // bytes and the merge copies the segments verbatim. A run a pressure spill
  // had to store for want of a workspace is not final: the merge compresses
  // it.
  run.storedIsFinal = true;
  auto& ends = run.pidEnds;
  ends.resize(in.numPartitions + 1);
  ends[0] = spill_.end();

  SpillRunSink sink(spill_);
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.runPartitions == nullptr || in.runPartitions[pid] != 0) {
      const auto sizes = residentSizes(in, pid);
      if (residentBytes(in, pid) > 0) {
        const auto visit = [&](uint32_t s, RunPieceFn fn) {
          in.cells->scan(pid, s, fn);
        };
        run.storedIsFinal &=
            compressor_.write(sink, numStreams, sizes.data(), visit);
      }
    }
    ends[pid + 1] = spill_.end();
  }
  countRun(ends[0]);
  // A spill exists to free memory: hand the workspace capacity back too.
  compressor_.releaseWorkspace();
  scratch_.reset();
  spill_.flush();
}

void LocalCellOutput::sealWindow(const CellWindowInput& in) {
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.sealPartitions != nullptr && in.sealPartitions[pid] == 0) {
      continue;
    }
    // The partition's open payload ends here: its segments since the last
    // seal become the sealed payload's, and later Runs open the next one.
    const size_t from = openFrom_[pid];
    openFrom_[pid] = runs_.size();
    if (in.rowCounts[pid] == 0) {
      continue;
    }
    SealedPayload payload;
    payload.segments = partitionSegments(runs_, pid, from);
    payload.rows = in.rowCounts[pid];
    payload.variableBytes = in.variableBytes[pid];
    resetBuffer(scratch_, 0, pool_);
    buildNullBody(in, pid, payload.rows, scratch_, pool_);
    payload.nullDecodedSize = compressor_.compressNullBody(scratch_, 0);
    payload.nullOffset = spill_.end();
    payload.nullLength = scratch_ ? static_cast<uint32_t>(scratch_->size()) : 0;
    if (payload.nullLength > 0) {
      spill_.append(scratch_->as<char>(), scratch_->size());
    }
    sealed_[pid].push_back(std::move(payload));
    ++numSealedPayloads_;
  }
  // A seal exists to free memory: hand the workspace capacity back too.
  compressor_.releaseWorkspace();
  spill_.flush();
  ++numSeals_;
}

void LocalCellOutput::writeDiskPayload(
    DataFileWriter& out,
    const SealedPayload& payload,
    const uint8_t* encodingTags) {
  const uint32_t rows = payload.rows;
  const auto& segments = payload.segments;
  const uint32_t runCount = static_cast<uint32_t>(segments.size());
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
  appendBytes(
      scratch_, encodingTags, (layout_->numWireColumns() + 7) / 8, pool_);
  // Raw, the null body counts decompressed.
  rawAccum_ += scratch_->size() +
      (payload.nullDecodedSize == 0
           ? 0
           : payload.nullDecodedSize - payload.nullLength);
  out.write(scratch_->as<char>(), scratch_->size());

  // Each spilled segment is a run of its own, oldest first.
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment);
  }
}

void LocalCellOutput::writeSpilledSegment(
    DataFileWriter& out,
    const SpillSegment& segment) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t runHeaderBytes = cell::runHeaderBytes(numStreams);
  const uint64_t segmentBytes = segment.end - segment.begin;
  // The spill segment is a run in wire form.
  BOLT_CHECK_GE(segmentBytes, 1, "corrupt cell spill segment");
  uint8_t layoutByte;
  spill_.read(segment.begin, &layoutByte, 1);
  if (layoutByte == static_cast<uint8_t>(RunLayout::kSeparate)) {
    // Compressed stream by stream at spill time: the final run.
    const uint64_t headerBytes = separateRunHeaderBytes(numStreams);
    BOLT_CHECK_GE(segmentBytes, headerBytes, "corrupt cell spill segment");
    std::vector<uint64_t> sizes(2ull * numStreams);
    spill_.read(segment.begin + 1, sizes.data(), 16ull * numStreams);
    uint64_t decodedSum = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      const uint64_t decoded = sizes[numStreams + s];
      decodedSum += decoded != 0 ? decoded : sizes[s];
    }
    rawAccum_ += runHeaderBytes + decodedSum;
    ++copiedSegments_;
    out.copyFrom(spill_, segment.begin, segmentBytes);
    return;
  }
  BOLT_CHECK(
      layoutByte == static_cast<uint8_t>(RunLayout::kCombined) ||
          layoutByte == static_cast<uint8_t>(RunLayout::kCombinedUncompressed),
      "corrupt cell spill segment");
  BOLT_CHECK_GE(segmentBytes, runHeaderBytes, "corrupt cell spill segment");
  std::vector<uint64_t> decodedSizes(numStreams);
  spill_.read(segment.begin + 9, decodedSizes.data(), 8ull * numStreams);
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes) {
    decodedSum += size;
  }
  rawAccum_ += runHeaderBytes + decodedSum;
  // A compressed segment, and a stored one the spill already made final,
  // is copied as it is. Any other stored segment - one a pressure spill
  // could not compress - is compressed now, read back from the spill file
  // stream by stream.
  if (layoutByte == static_cast<uint8_t>(RunLayout::kCombined) ||
      segment.storedIsFinal) {
    ++copiedSegments_;
    out.copyFrom(spill_, segment.begin, segmentBytes);
    return;
  }
  std::vector<uint64_t> streamAt(numStreams);
  uint64_t at = segment.begin + runHeaderBytes;
  for (uint32_t s = 0; s < numStreams; ++s) {
    streamAt[s] = at;
    at += decodedSizes[s];
  }
  ++compressedSegments_;
  DataRunSink sink(out);
  compressor_.write(
      sink, numStreams, decodedSizes.data(), [&](uint32_t s, RunPieceFn fn) {
        char buffer[16 << 10];
        for (uint64_t done = 0; done < decodedSizes[s];) {
          const uint64_t chunk =
              std::min<uint64_t>(sizeof(buffer), decodedSizes[s] - done);
          spill_.read(streamAt[s] + done, buffer, chunk);
          fn(buffer, static_cast<uint32_t>(chunk));
          done += chunk;
        }
      });
}

void LocalCellOutput::writeCurrentWindowPayload(
    DataFileWriter& out,
    const CellWindowInput& in,
    uint32_t pid) {
  const uint64_t total = residentBytes(in, pid);
  const auto segments = partitionSegments(runs_, pid, openFrom_[pid]);
  const uint32_t runCount =
      (total > 0 ? 1 : 0) + static_cast<uint32_t>(segments.size());

  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  rawAccum_ +=
      appendPayloadPrefix(in, pid, runCount, compressor_, scratch_, pool_);
  rawAccum_ += scratch_->size();
  out.write(scratch_->as<char>(), scratch_->size());
  // Mid-window spilled runs first (they hold the older blocks), the
  // resident cells last.
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment);
  }
  if (total > 0) {
    writeResidentRun(out, in, pid, total);
  }
}

namespace {

/// "12.3MB"-style size for the diagnostics log.
std::string mb(uint64_t bytes) {
  char buf[32];
  ::snprintf(buf, sizeof(buf), "%.2fMB", bytes / (1024.0 * 1024.0));
  return buf;
}

} // namespace

uint64_t LocalCellOutput::describeRuns(
    const std::vector<SpilledRun>& runs,
    uint32_t numPartitions,
    std::ostream& os) {
  constexpr size_t kMaxListedRuns = 16;
  uint64_t total = 0;
  os << runs.size() << " runs [";
  for (size_t i = 0; i < runs.size(); ++i) {
    const uint64_t bytes = runs[i].pidEnds[numPartitions] - runs[i].pidEnds[0];
    total += bytes;
    if (i < kMaxListedRuns) {
      os << (i > 0 ? ", " : "") << mb(bytes);
    } else if (i == kMaxListedRuns) {
      os << ", +" << (runs.size() - kMaxListedRuns) << " more";
    }
  }
  os << "]";
  return total;
}

std::vector<LocalCellOutput::SpillSegment> LocalCellOutput::partitionSegments(
    const std::vector<SpilledRun>& runs,
    uint32_t pid,
    size_t from) {
  std::vector<SpillSegment> segments;
  for (size_t i = from; i < runs.size(); ++i) {
    const auto& run = runs[i];
    if (run.pidEnds[pid + 1] > run.pidEnds[pid]) {
      segments.push_back(
          {run.pidEnds[pid], run.pidEnds[pid + 1], run.storedIsFinal});
    }
  }
  return segments;
}

void LocalCellOutput::logWindowDiagnostics(
    const CellWindowInput& in,
    bool windowHasData) {
  uint64_t sealedRows = 0;
  uint64_t nullBytes = 0;
  for (const auto& payloads : sealed_) {
    for (const auto& payload : payloads) {
      sealedRows += payload.rows;
      nullBytes += payload.nullLength;
    }
  }
  uint64_t rows = 0;
  uint64_t resident = 0;
  if (windowHasData) {
    const uint32_t numStreams = layout_->numStreams();
    for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
      rows += in.rowCounts[pid];
      for (uint32_t s = 0; s < numStreams; ++s) {
        resident += in.cells->bytes(pid, s);
      }
    }
  }
  std::ostringstream os;
  os << "CellShuffleWriter residual window: rows=" << rows
     << ", resident=" << mb(resident) << ", spilled ";
  const uint64_t runBytes = describeRuns(runs_, in.numPartitions, os);
  LOG(INFO) << os.str();
  LOG(INFO) << "CellShuffleWriter totals: " << numSeals_ << " seals closing "
            << numSealedPayloads_ << " partition payloads (" << sealedRows
            << " rows, nullBytes=" << mb(nullBytes) << "), " << runs_.size()
            << " runs, " << mb(runBytes) << " spilled";
}

void LocalCellOutput::writeResidentRun(
    DataFileWriter& out,
    const CellWindowInput& in,
    uint32_t pid,
    uint64_t total) {
  const uint32_t numStreams = layout_->numStreams();
  const auto sizes = residentSizes(in, pid);
  rawAccum_ += runHeaderBytes(numStreams) + total;
  DataRunSink sink(out);
  compressor_.write(
      sink, numStreams, sizes.data(), [&](uint32_t s, RunPieceFn fn) {
        in.cells->scan(pid, s, fn);
      });
}

void LocalCellOutput::appendBlockKind() {
  const auto kind = static_cast<uint8_t>(CellBlockKind::kPayload);
  appendBytes(scratch_, &kind, 1, pool_);
}

void LocalCellOutput::finalize(
    const CellWindowInput& in,
    bool windowHasData,
    ShuffleWriterMetrics& metrics) {
  // The residual window never takes a spill round-trip: whatever is still
  // in memory is written straight into the data file by the merge,
  // alongside any runs the window already spilled mid-stream.
  logWindowDiagnostics(in, windowHasData);
  residual_ = &in;
  residualHasData_ = windowHasData;
  rawLengths_.assign(in.numPartitions, 0);
  {
    SCOPE_EXIT {
      residual_ = nullptr;
    };
    // Per partition: the Cell payloads, then the composite blocks.
    mergeToDataFile(options_, {this, &composite_}, metrics);
  }
  compressor_.releaseWorkspace();
  scratch_.reset();
  metrics.rawPartitionLengths = rawLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
  LOG(INFO) << "CellShuffleWriter merge: segments copied=" << copiedSegments_
            << " compressed=" << compressedSegments_;
  logCompression(compressor_, *layout_);
}

void LocalCellOutput::mergeToFinal(uint32_t pid, DataFileWriter& out) {
  BOLT_CHECK_NOT_NULL(residual_, "Cell payloads merged outside finalize()");
  const uint64_t rawStart = rawAccum_;
  for (const auto& payload : sealed_[pid]) {
    writeDiskPayload(out, payload, residual_->encodingTags);
  }
  if (residualHasData_ && residual_->rowCounts[pid] > 0) {
    writeCurrentWindowPayload(out, *residual_, pid);
  }
  rawLengths_[pid] += static_cast<int64_t>(rawAccum_ - rawStart);
}

RssCellOutput::RssCellOutput(
    RawPartitionOutput& raw,
    const CellLayout* layout,
    const PartitionWriterOptions& options,
    const CellShuffleOptions& cellOptions,
    memory::MemoryPool* pool)
    : raw_(raw),
      layout_(layout),
      pool_(pool),
      compressor_(options, cellOptions, pool, &compressTimeNs_),
      rawPartitionLengths_(options.numPartitions, 0) {}

void RssCellOutput::spillRun(const CellWindowInput& /*in*/) {
  BOLT_FAIL("RssCellOutput never spills: windows are sent whole");
}

void RssCellOutput::sealWindow(const CellWindowInput& in) {
  sendWindow(in);
  ++numSealed_;
}

void RssCellOutput::sendWindow(const CellWindowInput& in) {
  uint32_t built = std::numeric_limits<uint32_t>::max();
  raw_.spillRaw([&](uint32_t pid, BufferPtr& block) {
    if (pid == built || in.rowCounts[pid] == 0 ||
        (in.sealPartitions != nullptr && in.sealPartitions[pid] == 0)) {
      return false;
    }
    buildPayload(in, pid, block);
    built = pid;
    return true;
  });
  // A seal exists to free memory: hand the workspace capacity back too.
  compressor_.releaseWorkspace();
}

void RssCellOutput::buildPayload(
    const CellWindowInput& in,
    uint32_t pid,
    BufferPtr& payload) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t total = residentBytes(in, pid);
  resetBuffer(payload, 0, pool_);
  const auto kind = static_cast<uint8_t>(CellBlockKind::kPayload);
  appendBytes(payload, &kind, 1, pool_);
  uint64_t raw = appendPayloadPrefix(
      in, pid, total > 0 ? 1 : 0, compressor_, payload, pool_);
  raw += payload->size();
  if (total > 0) {
    const auto sizes = residentSizes(in, pid);
    BufferRunSink sink(payload, pool_);
    compressor_.write(
        sink, numStreams, sizes.data(), [&](uint32_t s, RunPieceFn fn) {
          in.cells->scan(pid, s, fn);
        });
    raw += runHeaderBytes(numStreams) + total;
  }
  rawPartitionLengths_[pid] += static_cast<int64_t>(raw);
}

void RssCellOutput::finalize(
    const CellWindowInput& in,
    bool windowHasData,
    ShuffleWriterMetrics& metrics) {
  if (windowHasData) {
    sendWindow(in);
  }
  LOG(INFO) << "CellShuffleWriter rss: sealed windows=" << numSealed_;
  logCompression(compressor_, *layout_);
  raw_.finish(metrics);
  metrics.rawPartitionLengths = rawPartitionLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSealed_);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
