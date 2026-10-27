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
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

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
      pool_(pool) {
  if (options_.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        options_.compressionType,
        CodecOptions{
            getCodecBackend(options_.codecBackend),
            options_.compressionLevel,
            options_.checksumEnabled});
  }
  openFrom_.assign(options_.numPartitions, 0);
  sealed_.resize(options_.numPartitions);
}

void LocalCellOutput::spillRun(const CellWindowInput& in) {
  const uint32_t numStreams = layout_->numStreams();
  auto& run = runs_.emplace_back();
  run.storedIsFinal = codec_ == nullptr || cellOptions_.compressSpill;
  auto& ends = run.pidEnds;
  ends.resize(in.numPartitions + 1);
  ends[0] = spill_.end();

  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    uint64_t total = 0;
    if (in.runPartitions == nullptr || in.runPartitions[pid] != 0) {
      for (uint32_t s = 0; s < numStreams; ++s) {
        total += in.cells->bytes(pid, s);
      }
    }
    if (total > 0) {
      // Store runs in wire format so merge can copy them without re-encoding
      // when run coalescing is disabled.
      const char* body = nullptr;
      uint64_t stored = total;
      auto runLayout = RunLayout::kCombinedUncompressed;
      if (codec_ != nullptr && cellOptions_.compressSpill) {
        // The gather/compress workspaces come from the task pool, and a
        // spill is often the moment that pool is exhausted; when they cannot
        // be funded, degrade to streaming the run out uncompressed rather
        // than failing the very operation meant to relieve the pressure.
        try {
          resetBuffer(runScratch_, total, pool_);
          size_t offset = 0;
          for (uint32_t s = 0; s < numStreams; ++s) {
            in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
              ::memcpy(runScratch_->asMutable<char>() + offset, data, bytes);
              offset += bytes;
            });
          }
          runLayout =
              maybeCompressRun(runScratch_->as<char>(), total, body, stored);
        } catch (const std::exception&) {
          runLayout = RunLayout::kCombinedUncompressed;
          body = nullptr;
          stored = total;
          run.storedIsFinal = false;
        }
      }
      resetBuffer(scratch_, 1, pool_);
      scratch_->asMutable<uint8_t>()[0] = static_cast<uint8_t>(runLayout);
      appendLe64(scratch_, stored, pool_);
      for (uint32_t s = 0; s < numStreams; ++s) {
        appendLe64(scratch_, in.cells->bytes(pid, s), pool_);
      }
      spill_.append(scratch_->as<char>(), scratch_->size());
      if (body != nullptr) {
        spill_.append(body, stored);
      } else {
        for (uint32_t s = 0; s < numStreams; ++s) {
          in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
            spill_.append(data, bytes);
          });
        }
      }
    }
    ends[pid + 1] = spill_.end();
  }
  countRun(ends[0]);
  // A spill exists to free memory: hand the workspace capacity back too.
  runScratch_.reset();
  compressScratch_.reset();
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

RunLayout LocalCellOutput::maybeCompressRun(
    const char* data,
    uint64_t dataBytes,
    const char*& body,
    uint64_t& stored) {
  return cell::maybeCompressRun(
      codec_.get(),
      data,
      dataBytes,
      compressScratch_,
      pool_,
      body,
      stored,
      compressTimeNs_);
}

void LocalCellOutput::writeRun(
    DataFileWriter& out,
    const char* data,
    uint64_t dataBytes,
    const uint64_t* decodedSizes,
    bool separable) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t headerBytes = 1 + 8 + 8ull * numStreams;
  rawAccum_ += headerBytes + dataBytes;

  if (separable && codec_ != nullptr && dataBytes >= kMinSeparateRunBytes) {
    writeSeparateRun(out, data, decodedSizes);
    return;
  }

  const char* body = nullptr;
  uint64_t stored = 0;
  const auto runLayout = maybeCompressRun(data, dataBytes, body, stored);

  resetBuffer(scratch_, 1, pool_);
  scratch_->asMutable<uint8_t>()[0] = static_cast<uint8_t>(runLayout);
  appendLe64(scratch_, stored, pool_);
  for (uint32_t stream = 0; stream < numStreams; ++stream) {
    appendLe64(scratch_, decodedSizes[stream], pool_);
  }
  out.write(scratch_->as<char>(), scratch_->size());
  out.write(body, stored);
}

void LocalCellOutput::writeSeparateRun(
    DataFileWriter& out,
    const char* data,
    const uint64_t* decodedSizes) {
  const uint32_t numStreams = layout_->numStreams();
  uint64_t capacity = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    if (decodedSizes[s] >= kMinCompressRunBytes) {
      capacity += codec_->maxCompressedLen(decodedSizes[s]);
    }
  }
  resetBuffer(compressScratch_, capacity, pool_);
  // Header: the layout, the stored size per stream, then the decoded size
  // per stream - 0 for a stream stored as it is.
  std::vector<uint64_t> stored(numStreams);
  std::vector<uint64_t> decoded(numStreams, 0);
  std::vector<const char*> bodies(numStreams);
  char* compressed = compressScratch_->asMutable<char>();
  const char* source = data;
  for (uint32_t s = 0; s < numStreams; ++s) {
    const uint64_t bytes = decodedSizes[s];
    stored[s] = bytes;
    bodies[s] = source;
    if (bytes >= kMinCompressRunBytes) {
      const uint64_t start = currentTimeNs();
      const int64_t written = codec_->compress(
          reinterpret_cast<const uint8_t*>(source),
          static_cast<int64_t>(bytes),
          reinterpret_cast<uint8_t*>(compressed),
          codec_->maxCompressedLen(bytes));
      compressTimeNs_ += currentTimeNs() - start;
      if (written > 0 && static_cast<uint64_t>(written) < bytes) {
        stored[s] = static_cast<uint64_t>(written);
        decoded[s] = bytes;
        bodies[s] = compressed;
        compressed += written;
      }
    }
    source += bytes;
  }
  resetBuffer(scratch_, 1, pool_);
  scratch_->asMutable<uint8_t>()[0] =
      static_cast<uint8_t>(RunLayout::kSeparate);
  for (uint32_t s = 0; s < numStreams; ++s) {
    appendLe64(scratch_, stored[s], pool_);
  }
  for (uint32_t s = 0; s < numStreams; ++s) {
    appendLe64(scratch_, decoded[s], pool_);
  }
  out.write(scratch_->as<char>(), scratch_->size());
  for (uint32_t s = 0; s < numStreams; ++s) {
    if (stored[s] > 0) {
      out.write(bodies[s], stored[s]);
    }
  }
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
  std::vector<uint8_t> layouts(segments.size());
  std::vector<uint64_t> storedSizes(segments.size());
  std::vector<uint64_t> segSizes(segments.size() * numStreams);
  for (size_t i = 0; i < segments.size(); ++i) {
    uint8_t head[9];
    spill_.read(segments[i].begin, head, sizeof(head));
    layouts[i] = head[0];
    ::memcpy(&storedSizes[i], head + 1, 8);
    spill_.read(
        segments[i].begin + sizeof(head),
        segSizes.data() + i * numStreams,
        8ull * numStreams);
    for (uint32_t s = 0; s < numStreams; ++s) {
      streamSizes[s] += segSizes[i * numStreams + s];
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
      const int64_t decoded = codec_->decompress(
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
    writeRun(
        out,
        gather_->as<char>(),
        total,
        streamSizes.data(),
        /*separable=*/true);
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
  const uint64_t runHeaderBytes = 1 + 8 + 8ull * numStreams;
  const uint64_t segmentBytes = segment.end - segment.begin;
  // The spill segment is a run body in wire form.
  BOLT_CHECK_GE(segmentBytes, runHeaderBytes, "corrupt cell spill segment");
  resetBuffer(scratch_, runHeaderBytes, pool_);
  spill_.read(segment.begin, scratch_->asMutable<char>(), runHeaderBytes);
  const auto segmentLayout = static_cast<RunLayout>(scratch_->as<uint8_t>()[0]);
  BOLT_CHECK(
      segmentLayout == RunLayout::kCombined ||
          segmentLayout == RunLayout::kCombinedUncompressed,
      "corrupt cell spill segment");
  std::vector<uint64_t> decodedSizes(numStreams);
  ::memcpy(decodedSizes.data(), scratch_->as<char>() + 9, 8ull * numStreams);
  const uint64_t dataBytes = segmentBytes - runHeaderBytes;
  // A compressed segment, and a stored one whose spill already made the
  // compression attempt, is the final run. Any other stored segment gets
  // the attempt here - unless its workspace cannot be funded, in which case
  // the stored form, a valid run as well, is copied as it is.
  if (segmentLayout == RunLayout::kCombinedUncompressed &&
      !segment.storedIsFinal && codec_ != nullptr &&
      dataBytes >= kMinCompressRunBytes) {
    bool funded = true;
    try {
      resetBuffer(runScratch_, dataBytes, pool_);
      resetBuffer(compressScratch_, codec_->maxCompressedLen(dataBytes), pool_);
    } catch (const std::exception&) {
      funded = false;
      runScratch_.reset();
      compressScratch_.reset();
    }
    if (funded) {
      spill_.read(
          segment.begin + runHeaderBytes,
          runScratch_->asMutable<char>(),
          dataBytes);
      // The form a spill-time compression would have given it, so that
      // compressSpill never changes the final file.
      writeRun(
          out,
          runScratch_->as<char>(),
          dataBytes,
          decodedSizes.data(),
          /*separable=*/false);
      return;
    }
  }
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes) {
    decodedSum += size;
  }
  rawAccum_ += runHeaderBytes + decodedSum;
  out.write(scratch_->as<char>(), runHeaderBytes);
  out.copyFrom(spill_, segment.begin + runHeaderBytes, dataBytes);
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
    writeRun(
        out,
        gather_->as<char>(),
        gathered,
        streamSizes.data(),
        /*separable=*/true);
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
    std::vector<uint64_t> decodedSizes(numStreams);
    resetBuffer(runScratch_, total, pool_);
    gatherResident(
        in, pid, runScratch_->asMutable<char>(), decodedSizes.data());
    writeRun(
        out,
        runScratch_->as<char>(),
        total,
        decodedSizes.data(),
        /*separable=*/true);
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
    memory::MemoryPool* pool)
    : raw_(raw),
      layout_(layout),
      pool_(pool),
      rawPartitionLengths_(options.numPartitions, 0) {
  if (options.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        options.compressionType,
        CodecOptions{
            getCodecBackend(options.codecBackend),
            options.compressionLevel,
            options.checksumEnabled});
  }
}

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
  compressScratch_.reset();
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
    // The run is gathered straight into its place in the payload, so a
    // stored run needs no second copy and a compressed one is copied back
    // over it. When the compression workspace cannot be funded - a seal is
    // often the moment the pool is exhausted - the stored run goes out as
    // it is rather than failing the operation meant to free memory.
    const size_t headerAt = payload->size();
    const size_t bodyAt = headerAt + runHeaderBytes(numStreams);
    resizeBuffer(payload, checkedPlus<size_t>(bodyAt, total), pool_);
    streamSizes_.resize(numStreams);
    gatherResident(
        in, pid, payload->asMutable<char>() + bodyAt, streamSizes_.data());
    raw += runHeaderBytes(numStreams) + total;
    auto runLayout = RunLayout::kCombinedUncompressed;
    uint64_t stored = total;
    try {
      const char* body = nullptr;
      runLayout = maybeCompressRun(
          codec_.get(),
          payload->as<char>() + bodyAt,
          total,
          compressScratch_,
          pool_,
          body,
          stored,
          compressTimeNs_);
      if (runLayout == RunLayout::kCombined) {
        ::memcpy(payload->asMutable<char>() + bodyAt, body, stored);
      }
    } catch (const std::exception&) {
      compressScratch_.reset();
      runLayout = RunLayout::kCombinedUncompressed;
      stored = total;
    }
    writeRunHeader(
        payload->asMutable<char>() + headerAt,
        runLayout,
        stored,
        streamSizes_.data(),
        numStreams);
    payload->setSize(bodyAt + stored);
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
