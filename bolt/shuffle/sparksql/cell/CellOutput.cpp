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
#include <cstring>
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
      compressor_(
          options_,
          cellOptions_.streamCompression,
          pool,
          &compressTimeNs_) {
  openFrom_.assign(options_.numPartitions, 0);
  sealed_.resize(options_.numPartitions);
}

void LocalCellOutput::spillRun(const CellWindowInput& in) {
  const uint32_t numStreams = layout_->numStreams();
  auto& run = runs_.emplace_back();
  const bool compress = compressor_.compresses() && cellOptions_.compressSpill;
  // With compressSpill off, the merge compresses the stored segments.
  run.storedIsFinal = !compressor_.compresses() || compress;
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
        if (compress) {
          run.storedIsFinal &=
              compressor_.write(sink, numStreams, sizes.data(), visit);
        } else {
          compressor_.writeStored(sink, numStreams, sizes.data(), visit);
        }
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
    payload.nullOffset = spill_.end();
    payload.nullLength = scratch_ ? static_cast<uint32_t>(scratch_->size()) : 0;
    if (payload.nullLength > 0) {
      spill_.append(scratch_->as<char>(), scratch_->size());
    }
    sealed_[pid].push_back(std::move(payload));
    ++numSealedPayloads_;
  }
  spill_.flush();
  ++numSeals_;
}

uint64_t LocalCellOutput::gatherPartitionRuns(
    const std::vector<SpillSegment>& segments,
    const CellWindowInput* resident,
    uint32_t pid,
    std::vector<uint64_t>& streamSizes) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t runHeaderBytes = 1 + 8 + 8ull * numStreams;
  streamSizes.assign(numStreams, 0);

  // Pass 1: per-stream totals from the segment headers and the cells.
  // A SEPARATE segment's stored size per stream goes to `separateStored`,
  // 0 where the stream is stored as it is.
  std::vector<uint8_t> layouts(segments.size());
  std::vector<uint64_t> storedSizes(segments.size());
  std::vector<uint64_t> segSizes(segments.size() * numStreams);
  std::vector<uint64_t> separateStored(segments.size() * numStreams);
  for (size_t i = 0; i < segments.size(); ++i) {
    uint64_t* sizes = segSizes.data() + i * numStreams;
    spill_.read(segments[i].begin, &layouts[i], 1);
    if (layouts[i] == static_cast<uint8_t>(RunLayout::kSeparate)) {
      uint64_t* stored = separateStored.data() + i * numStreams;
      spill_.read(segments[i].begin + 1, stored, 8ull * numStreams);
      spill_.read(
          segments[i].begin + 1 + 8ull * numStreams, sizes, 8ull * numStreams);
      for (uint32_t s = 0; s < numStreams; ++s) {
        if (sizes[s] == 0) {
          sizes[s] = stored[s];
          stored[s] = 0;
        }
      }
    } else {
      spill_.read(segments[i].begin + 1, &storedSizes[i], 8);
      spill_.read(segments[i].begin + 9, sizes, 8ull * numStreams);
    }
    for (uint32_t s = 0; s < numStreams; ++s) {
      streamSizes[s] += sizes[s];
    }
  }
  if (resident != nullptr) {
    for (uint32_t s = 0; s < numStreams; ++s) {
      streamSizes[s] += resident->cells->bytes(pid, s);
    }
  }
  uint64_t total = 0;
  std::vector<uint64_t> cursor(numStreams);
  for (uint32_t s = 0; s < numStreams; ++s) {
    cursor[s] = total;
    total += streamSizes[s];
  }
  resetBuffer(gather_, total, pool_);

  // Pass 2: one sequential read per segment, then scatter per stream.
  for (size_t i = 0; i < segments.size(); ++i) {
    uint64_t dataBytes = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      dataBytes += segSizes[i * numStreams + s];
    }
    if (layouts[i] == static_cast<uint8_t>(RunLayout::kSeparate)) {
      uint64_t at = segments[i].begin + separateRunHeaderBytes(numStreams);
      for (uint32_t s = 0; s < numStreams; ++s) {
        const uint64_t bytes = segSizes[i * numStreams + s];
        const uint64_t stored = separateStored[i * numStreams + s];
        char* target = gather_->asMutable<char>() + cursor[s];
        if (stored == 0) {
          spill_.read(at, target, bytes);
          at += bytes;
        } else {
          resetBuffer(runScratch_, stored, pool_);
          spill_.read(at, runScratch_->asMutable<char>(), stored);
          const uint64_t decompressStart = currentTimeNs();
          const int64_t decoded = compressor_.codec()->decompress(
              runScratch_->as<uint8_t>(),
              static_cast<int64_t>(stored),
              reinterpret_cast<uint8_t*>(target),
              static_cast<int64_t>(bytes));
          compressTimeNs_ += currentTimeNs() - decompressStart;
          BOLT_CHECK_EQ(
              decoded,
              static_cast<int64_t>(bytes),
              "corrupt cell spill segment");
          at += stored;
        }
        cursor[s] += bytes;
      }
      continue;
    }
    const char* body;
    if (layouts[i] == static_cast<uint8_t>(RunLayout::kCombined)) {
      resetBuffer(runScratch_, storedSizes[i], pool_);
      spill_.read(
          segments[i].begin + runHeaderBytes,
          runScratch_->asMutable<char>(),
          storedSizes[i]);
      resetBuffer(compressScratch_, dataBytes, pool_);
      // Codec time, reported with compression: the merge's decompression
      // is the price of spill-time compression.
      const uint64_t decompressStart = currentTimeNs();
      const int64_t decoded = compressor_.codec()->decompress(
          runScratch_->as<uint8_t>(),
          static_cast<int64_t>(storedSizes[i]),
          compressScratch_->asMutable<uint8_t>(),
          static_cast<int64_t>(dataBytes));
      compressTimeNs_ += currentTimeNs() - decompressStart;
      BOLT_CHECK_EQ(
          decoded,
          static_cast<int64_t>(dataBytes),
          "corrupt cell spill segment");
      body = compressScratch_->as<char>();
    } else {
      BOLT_CHECK_EQ(
          layouts[i],
          static_cast<uint8_t>(RunLayout::kCombinedUncompressed),
          "corrupt cell spill segment");
      resetBuffer(runScratch_, dataBytes, pool_);
      spill_.read(
          segments[i].begin + runHeaderBytes,
          runScratch_->asMutable<char>(),
          dataBytes);
      body = runScratch_->as<char>();
    }
    uint64_t off = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      const uint64_t bytes = segSizes[i * numStreams + s];
      ::memcpy(gather_->asMutable<char>() + cursor[s], body + off, bytes);
      cursor[s] += bytes;
      off += bytes;
    }
  }
  if (resident != nullptr) {
    for (uint32_t s = 0; s < numStreams; ++s) {
      resident->cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
        ::memcpy(gather_->asMutable<char>() + cursor[s], data, bytes);
        cursor[s] += bytes;
      });
    }
  }
  return total;
}

void LocalCellOutput::writeDiskPayload(
    DataFileWriter& out,
    const SealedPayload& payload,
    const uint8_t* encodingTags,
    uint32_t pid) {
  const uint32_t rows = payload.rows;
  const auto& segments = payload.segments;
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  const uint32_t runCount = coalesce ? (segments.empty() ? 0 : 1)
                                     : static_cast<uint32_t>(segments.size());
  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  appendLe32(scratch_, rows, pool_);
  appendLe32(scratch_, runCount, pool_);
  appendLe64(scratch_, payload.variableBytes, pool_);
  appendLe32(scratch_, payload.nullLength, pool_);
  appendLe32(scratch_, 0, pool_); // null body stored uncompressed
  const size_t nullAt = scratch_->size();
  resizeBuffer(
      scratch_, checkedPlus<size_t>(nullAt, payload.nullLength), pool_);
  spill_.read(
      payload.nullOffset,
      scratch_->asMutable<char>() + nullAt,
      payload.nullLength);
  appendBytes(
      scratch_, encodingTags, (layout_->numWireColumns() + 7) / 8, pool_);
  rawAccum_ += scratch_->size();
  out.write(scratch_->as<char>(), scratch_->size());

  if (coalesce && segments.size() > 1) {
    std::vector<uint64_t> streamSizes;
    const uint64_t total =
        gatherPartitionRuns(segments, nullptr, pid, streamSizes);
    writeGatheredRun(out, total, streamSizes);
    return;
  }
  // Without coalescing, or with a single segment that already is the one
  // run: copied verbatim when in final form, never re-encoded.
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
  // is copied as it is. Any other stored segment is compressed now, read
  // back from the spill file stream by stream, into the run a spill-time
  // compression gives the same bytes: compressSpill never changes the
  // final file.
  if (layoutByte == static_cast<uint8_t>(RunLayout::kCombined) ||
      segment.storedIsFinal) {
    out.copyFrom(spill_, segment.begin, segmentBytes);
    return;
  }
  std::vector<uint64_t> streamAt(numStreams);
  uint64_t at = segment.begin + runHeaderBytes;
  for (uint32_t s = 0; s < numStreams; ++s) {
    streamAt[s] = at;
    at += decodedSizes[s];
  }
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
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t total = residentBytes(in, pid);
  const auto segments = partitionSegments(runs_, pid, openFrom_[pid]);
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  uint32_t runCount;
  if (coalesce) {
    runCount = (total > 0 || !segments.empty()) ? 1 : 0;
  } else {
    runCount = (total > 0 ? 1 : 0) + static_cast<uint32_t>(segments.size());
  }

  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  appendPayloadPrefix(in, pid, runCount, scratch_, pool_);
  rawAccum_ += scratch_->size();
  out.write(scratch_->as<char>(), scratch_->size());
  if (coalesce && (segments.size() > 1 || (total > 0 && !segments.empty()))) {
    std::vector<uint64_t> streamSizes;
    const uint64_t gathered =
        gatherPartitionRuns(segments, &in, pid, streamSizes);
    writeGatheredRun(out, gathered, streamSizes);
    return;
  }
  // Without coalescing, or when the payload already is a single run (one
  // spilled segment, copied verbatim when in final form, or only
  // resident cells), the runs are written as they are: mid-window spilled
  // runs first (they hold the older blocks), the resident cells last.
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

void LocalCellOutput::writeGatheredRun(
    DataFileWriter& out,
    uint64_t total,
    const std::vector<uint64_t>& streamSizes) {
  const uint32_t numStreams = layout_->numStreams();
  rawAccum_ += runHeaderBytes(numStreams) + total;
  std::vector<uint64_t> streamAt(numStreams);
  uint64_t at = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    streamAt[s] = at;
    at += streamSizes[s];
  }
  DataRunSink sink(out);
  compressor_.write(
      sink, numStreams, streamSizes.data(), [&](uint32_t s, RunPieceFn fn) {
        // In pieces, so a single stream past 4GiB still fits the callback.
        const char* data = gather_->as<char>() + streamAt[s];
        for (uint64_t done = 0; done < streamSizes[s];) {
          const uint64_t chunk =
              std::min<uint64_t>(1u << 30, streamSizes[s] - done);
          fn(data + done, static_cast<uint32_t>(chunk));
          done += chunk;
        }
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
  runScratch_.reset();
  compressScratch_.reset();
  gather_.reset();
  scratch_.reset();
  metrics.rawPartitionLengths = rawLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
}

void LocalCellOutput::mergeToFinal(uint32_t pid, DataFileWriter& out) {
  BOLT_CHECK_NOT_NULL(residual_, "Cell payloads merged outside finalize()");
  const uint64_t rawStart = rawAccum_;
  for (const auto& payload : sealed_[pid]) {
    writeDiskPayload(out, payload, residual_->encodingTags, pid);
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
    bool streamCompression,
    memory::MemoryPool* pool)
    : raw_(raw),
      layout_(layout),
      pool_(pool),
      compressor_(options, streamCompression, pool, &compressTimeNs_),
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
  appendPayloadPrefix(in, pid, total > 0 ? 1 : 0, payload, pool_);
  uint64_t raw = payload->size();
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
  raw_.finish(metrics);
  metrics.rawPartitionLengths = rawPartitionLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSealed_);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
