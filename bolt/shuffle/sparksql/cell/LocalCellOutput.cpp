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

#include "bolt/shuffle/sparksql/cell/LocalCellOutput.h"

#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <sstream>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void appendLe32(PoolBytes& out, uint32_t value) {
  out.append(&value, 4);
}

void appendLe64(PoolBytes& out, uint64_t value) {
  out.append(&value, 8);
}

/// Builds the spec section 4.2 null body of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    PoolBytes& out) {
  const uint32_t numColumns = in.layout->numColumns();
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const size_t tagStart = out.size();
  out.resize(tagStart + tagBytes);
  auto* tags = out.udata() + tagStart;
  ::memset(tags, 0, tagBytes);
  const uint32_t bitmapBytes = (rows + 7) / 8;
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto summary = in.nulls->summarize(pid, col, rows);
    setNullTag(tags, col, summary.tag);
  }
  // Bitmaps follow the tags for RAW_NULL columns, in column order. The tag
  // byte array may move as `out` grows, so tags were fully written first.
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (getNullTag(out.udata() + tagStart, col) == NullTag::kRawNull) {
      const size_t at = out.size();
      out.resize(at + bitmapBytes);
      in.nulls->emitBitmap(pid, col, rows, out.udata() + at);
    }
  }
}

} // namespace

LocalCellOutput::LocalCellOutput(
    PartitionWriterOptions options,
    const CellLayout* layout,
    CellShuffleOptions cellOptions,
    memory::MemoryPool* pool)
    : options_(std::move(options)),
      layout_(layout),
      cellOptions_(cellOptions),
      runScratch_(pool),
      compressScratch_(pool),
      gather_(pool),
      scratch_(pool) {
  if (options_.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        options_.compressionType,
        CodecOptions{
            getCodecBackend(options_.codecBackend),
            options_.compressionLevel,
            options_.checksumEnabled});
  }
}

LocalCellOutput::~LocalCellOutput() {
  if (spillFile_ != nullptr) {
    ::fclose(spillFile_);
    ::unlink(spillPath_.c_str());
  }
}

void LocalCellOutput::ensureSpillFile() {
  if (spillFile_ != nullptr) {
    return;
  }
  if (!options_.configuredDirs.empty()) {
    const auto dir = getSpilledShuffleFileDir(
        options_.configuredDirs[0], 0 % options_.numSubDirs);
    auto maybePath = createTempShuffleFile(dir);
    BOLT_CHECK(
        maybePath.ok(),
        "Failed to create cell spill file: {}",
        maybePath.status().ToString());
    spillPath_ = *maybePath;
  } else {
    // Test convenience: sit next to the data file.
    spillPath_ = options_.dataFile + ".cellspill";
  }
  spillFile_ = ::fopen(spillPath_.c_str(), "wb+");
  BOLT_CHECK_NOT_NULL(spillFile_, "Failed to open {}", spillPath_);
  spillFd_ = ::fileno(spillFile_);
}

void LocalCellOutput::spillWrite(const void* data, size_t bytes) {
  const uint64_t start = nowNs();
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, spillFile_),
      bytes,
      "cell spill write failed");
  evictTimeNs_ += nowNs() - start;
  spillOffset_ += bytes;
  bytesEvicted_ += bytes;
}

void LocalCellOutput::readSpill(uint64_t offset, void* out, size_t bytes)
    const {
  size_t done = 0;
  auto* dst = reinterpret_cast<char*>(out);
  while (done < bytes) {
    const auto n = ::pread(spillFd_, dst + done, bytes - done, offset + done);
    BOLT_CHECK_GT(n, 0, "cell spill read failed");
    done += n;
  }
}

void LocalCellOutput::spillRun(const CellWindowInput& in) {
  ensureSpillFile();
  const uint32_t numStreams = layout_->numStreams();
  auto& ends = openWindowRuns_.emplace_back();
  ends.resize(in.numPartitions + 1);
  ends[0] = spillOffset_;

  const bool compressHere = cellOptions_.compressSpill && codec_ != nullptr;
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    uint64_t total = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      total += in.cells->bytes(pid, s);
    }
    if (total > 0) {
      // Run body (spec section 5): layout byte, the single stored size, the
      // decoded size of every stream, then the bytes. With spill
      // compression on, the segment is already the final COMBINED wire form
      // and the merge copies it verbatim - both disk passes write the
      // compressed bytes (the SSD-endurance path).
      const char* body = nullptr;
      uint64_t stored = total;
      auto runLayout = RunLayout::kCombinedStored;
      if (compressHere) {
        // The gather/compress workspaces come from the task pool, and a
        // spill is often the moment that pool is exhausted; when they cannot
        // be funded, degrade to streaming the run out uncompressed rather
        // than failing the very operation meant to relieve the pressure.
        try {
          runScratch_.clear();
          runScratch_.reserve(total);
          for (uint32_t s = 0; s < numStreams; ++s) {
            in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
              runScratch_.append(data, bytes);
            });
          }
          runLayout =
              maybeCompressRun(runScratch_.data(), total, body, stored);
        } catch (const std::exception&) {
          runLayout = RunLayout::kCombinedStored;
          body = nullptr;
          stored = total;
        }
      }
      scratch_.clear();
      scratch_.push_back(static_cast<char>(static_cast<uint8_t>(runLayout)));
      appendLe64(scratch_, stored);
      for (uint32_t s = 0; s < numStreams; ++s) {
        appendLe64(scratch_, in.cells->bytes(pid, s));
      }
      spillWrite(scratch_.data(), scratch_.size());
      if (body != nullptr) {
        spillWrite(body, stored);
      } else {
        for (uint32_t s = 0; s < numStreams; ++s) {
          in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
            spillWrite(data, bytes);
          });
        }
      }
    }
    ends[pid + 1] = spillOffset_;
  }
  // A spill exists to free memory: hand the workspace capacity back too.
  runScratch_.reset();
  compressScratch_.reset();
  scratch_.reset();
  const uint64_t flushStart = nowNs();
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  evictTimeNs_ += nowNs() - flushStart;
}

void LocalCellOutput::sealWindow(const CellWindowInput& in) {
  ensureSpillFile();
  SealedWindow window;
  window.runPidEnds = std::move(openWindowRuns_);
  openWindowRuns_.clear();
  window.nullOffset.resize(in.numPartitions, 0);
  window.nullLength.resize(in.numPartitions, 0);
  window.rowCounts.assign(in.rowCounts, in.rowCounts + in.numPartitions);
  window.variableBytes.assign(
      in.variableBytes, in.variableBytes + in.numPartitions);

  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (window.rowCounts[pid] == 0) {
      continue;
    }
    scratch_.clear();
    buildNullBody(in, pid, window.rowCounts[pid], scratch_);
    window.nullOffset[pid] = spillOffset_;
    window.nullLength[pid] = static_cast<uint32_t>(scratch_.size());
    spillWrite(scratch_.data(), scratch_.size());
  }
  const uint64_t flushStart = nowNs();
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  evictTimeNs_ += nowNs() - flushStart;
  sealed_.push_back(std::move(window));
}

void LocalCellOutput::writeOut(std::FILE* out, const void* data, size_t bytes) {
  const uint64_t start = nowNs();
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, out), bytes, "shuffle data file write failed");
  writeTimeNs_ += nowNs() - start;
  finalBytes_ += bytes;
}

RunLayout LocalCellOutput::maybeCompressRun(
    const char* data,
    uint64_t dataBytes,
    const char*& body,
    uint64_t& stored) {
  body = data;
  stored = dataBytes;
  if (codec_ != nullptr &&
      dataBytes >= static_cast<uint64_t>(cellOptions_.compressMinRunBytes)) {
    const uint64_t start = nowNs();
    compressScratch_.resize(codec_->maxCompressedLen(dataBytes));
    const int64_t written = codec_->compress(
        reinterpret_cast<const uint8_t*>(data),
        static_cast<int64_t>(dataBytes),
        reinterpret_cast<uint8_t*>(compressScratch_.data()),
        static_cast<int64_t>(compressScratch_.size()));
    compressTimeNs_ += nowNs() - start;
    // Spec section 5: fall back to the stored form when compression does
    // not pay.
    if (written > 0 && static_cast<uint64_t>(written) < dataBytes) {
      body = compressScratch_.data();
      stored = static_cast<uint64_t>(written);
      return RunLayout::kCombined;
    }
  }
  return RunLayout::kCombinedStored;
}

void LocalCellOutput::writeRun(
    std::FILE* out,
    const char* data,
    uint64_t dataBytes,
    const uint64_t* decodedSizes) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t headerBytes = 1 + 8 + 8ull * numStreams;
  rawAccum_ += headerBytes + dataBytes;

  const char* body = nullptr;
  uint64_t stored = 0;
  const auto runLayout = maybeCompressRun(data, dataBytes, body, stored);

  scratch_.clear();
  scratch_.push_back(static_cast<char>(static_cast<uint8_t>(runLayout)));
  appendLe64(scratch_, stored);
  for (uint32_t stream = 0; stream < numStreams; ++stream) {
    appendLe64(scratch_, decodedSizes[stream]);
  }
  writeOut(out, scratch_.data(), scratch_.size());
  writeOut(out, body, stored);
}

uint64_t LocalCellOutput::gatherPartitionRuns(
    const std::vector<std::pair<uint64_t, uint64_t>>& segments,
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
    readSpill(segments[i].first, head, sizeof(head));
    layouts[i] = head[0];
    ::memcpy(&storedSizes[i], head + 1, 8);
    readSpill(
        segments[i].first + sizeof(head),
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
  gather_.clear();
  gather_.resize(total);

  // Pass 2: one sequential read per segment, then scatter per stream.
  for (size_t i = 0; i < segments.size(); ++i) {
    uint64_t dataBytes = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      dataBytes += segSizes[i * numStreams + s];
    }
    const char* body;
    if (layouts[i] == static_cast<uint8_t>(RunLayout::kCombined)) {
      runScratch_.clear();
      runScratch_.resize(storedSizes[i]);
      readSpill(
          segments[i].first + runHeaderBytes,
          runScratch_.data(),
          storedSizes[i]);
      compressScratch_.clear();
      compressScratch_.resize(dataBytes);
      const int64_t decoded = codec_->decompress(
          reinterpret_cast<const uint8_t*>(runScratch_.data()),
          static_cast<int64_t>(storedSizes[i]),
          reinterpret_cast<uint8_t*>(compressScratch_.data()),
          static_cast<int64_t>(dataBytes));
      BOLT_CHECK_EQ(
          decoded,
          static_cast<int64_t>(dataBytes),
          "corrupt cell spill segment");
      body = compressScratch_.data();
    } else {
      BOLT_CHECK_EQ(
          layouts[i],
          static_cast<uint8_t>(RunLayout::kCombinedStored),
          "corrupt cell spill segment");
      runScratch_.clear();
      runScratch_.resize(dataBytes);
      readSpill(segments[i].first + runHeaderBytes, runScratch_.data(),
                dataBytes);
      body = runScratch_.data();
    }
    uint64_t off = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      const uint64_t bytes = segSizes[i * numStreams + s];
      ::memcpy(gather_.data() + cursor[s], body + off, bytes);
      cursor[s] += bytes;
      off += bytes;
    }
  }
  if (resident != nullptr) {
    for (uint32_t s = 0; s < numStreams; ++s) {
      resident->cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
        ::memcpy(gather_.data() + cursor[s], data, bytes);
        cursor[s] += bytes;
      });
    }
  }
  return total;
}

void LocalCellOutput::writeDiskPayload(
    std::FILE* out,
    const SealedWindow& w,
    const uint8_t* encodingTags,
    uint32_t pid) {
  const uint32_t rows = w.rowCounts[pid];
  if (rows == 0) {
    return;
  }
  std::vector<std::pair<uint64_t, uint64_t>> segments;
  segments.reserve(w.runPidEnds.size());
  for (const auto& ends : w.runPidEnds) {
    if (ends[pid + 1] > ends[pid]) {
      segments.emplace_back(ends[pid], ends[pid + 1]);
    }
  }
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  const uint32_t runCount = coalesce
      ? (segments.empty() ? 0 : 1)
      : static_cast<uint32_t>(segments.size());
  scratch_.clear();
  appendLe32(scratch_, rows);
  appendLe32(scratch_, runCount);
  appendLe64(scratch_, w.variableBytes[pid]);
  appendLe32(scratch_, w.nullLength[pid]);
  appendLe32(scratch_, 0); // null body stored uncompressed
  const size_t nullAt = scratch_.size();
  scratch_.resize(nullAt + w.nullLength[pid]);
  readSpill(w.nullOffset[pid], scratch_.data() + nullAt, w.nullLength[pid]);
  scratch_.append(encodingTags, (layout_->numColumns() + 7) / 8);
  rawAccum_ += scratch_.size();
  writeOut(out, scratch_.data(), scratch_.size());

  if (coalesce) {
    if (!segments.empty()) {
      std::vector<uint64_t> streamSizes;
      const uint64_t total =
          gatherPartitionRuns(segments, nullptr, pid, streamSizes);
      writeRun(out, gather_.data(), total, streamSizes.data());
    }
    return;
  }
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment.first, segment.second);
  }
}

void LocalCellOutput::writeSpilledSegment(
    std::FILE* out,
    uint64_t begin,
    uint64_t end) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t runHeaderBytes = 1 + 8 + 8ull * numStreams;
  const uint64_t segmentBytes = end - begin;
  // The spill segment is a run body in wire form.
  BOLT_CHECK_GE(segmentBytes, runHeaderBytes, "corrupt cell spill segment");
  scratch_.resize(runHeaderBytes);
  readSpill(begin, scratch_.data(), runHeaderBytes);
  const auto segmentLayout =
      static_cast<RunLayout>(static_cast<uint8_t>(scratch_.data()[0]));
  std::vector<uint64_t> decodedSizes(numStreams);
  ::memcpy(decodedSizes.data(), scratch_.data() + 9, 8ull * numStreams);
  if (segmentLayout == RunLayout::kCombined) {
    // Compressed at spill time: already the final form, copy verbatim.
    uint64_t decodedSum = 0;
    for (const auto size : decodedSizes) {
      decodedSum += size;
    }
    rawAccum_ += runHeaderBytes + decodedSum;
    writeOut(out, scratch_.data(), runHeaderBytes);
    char copyBuffer[64 << 10];
    uint64_t offset = begin + runHeaderBytes;
    uint64_t left = segmentBytes - runHeaderBytes;
    while (left > 0) {
      const size_t chunk = left < sizeof(copyBuffer) ? left : sizeof(copyBuffer);
      readSpill(offset, copyBuffer, chunk);
      writeOut(out, copyBuffer, chunk);
      offset += chunk;
      left -= chunk;
    }
    return;
  }
  BOLT_CHECK_EQ(
      static_cast<uint8_t>(segmentLayout),
      static_cast<uint8_t>(RunLayout::kCombinedStored),
      "corrupt cell spill segment");
  const uint64_t dataBytes = segmentBytes - runHeaderBytes;
  runScratch_.resize(dataBytes);
  readSpill(begin + runHeaderBytes, runScratch_.data(), dataBytes);
  writeRun(out, runScratch_.data(), dataBytes, decodedSizes.data());
}

void LocalCellOutput::writeCurrentWindowPayload(
    std::FILE* out,
    const CellWindowInput& in,
    uint32_t pid) {
  const uint32_t rows = in.rowCounts[pid];
  const uint32_t numStreams = layout_->numStreams();
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    total += in.cells->bytes(pid, s);
  }
  std::vector<std::pair<uint64_t, uint64_t>> segments;
  segments.reserve(openWindowRuns_.size());
  for (const auto& ends : openWindowRuns_) {
    if (ends[pid + 1] > ends[pid]) {
      segments.emplace_back(ends[pid], ends[pid + 1]);
    }
  }
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  uint32_t runCount;
  if (coalesce) {
    runCount = (total > 0 || !segments.empty()) ? 1 : 0;
  } else {
    runCount =
        (total > 0 ? 1 : 0) + static_cast<uint32_t>(segments.size());
  }

  scratch_.clear();
  appendLe32(scratch_, rows);
  appendLe32(scratch_, runCount);
  appendLe64(scratch_, in.variableBytes[pid]);
  const size_t nullSizeAt = scratch_.size();
  appendLe32(scratch_, 0); // patched below
  appendLe32(scratch_, 0); // uncompressed
  const size_t nullBodyAt = scratch_.size();
  buildNullBody(in, pid, rows, scratch_);
  const uint32_t nullLength =
      static_cast<uint32_t>(scratch_.size() - nullBodyAt);
  ::memcpy(scratch_.data() + nullSizeAt, &nullLength, 4);
  scratch_.append(in.encodingTags, (layout_->numColumns() + 7) / 8);
  rawAccum_ += scratch_.size();
  writeOut(out, scratch_.data(), scratch_.size());
  if (coalesce) {
    if (runCount != 0) {
      std::vector<uint64_t> streamSizes;
      const uint64_t gathered =
          gatherPartitionRuns(segments, &in, pid, streamSizes);
      writeRun(out, gather_.data(), gathered, streamSizes.data());
    }
    return;
  }
  // Mid-window spilled runs come first (they hold the older blocks), the
  // still-resident cells form the final run.
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment.first, segment.second);
  }
  if (total > 0) {
    std::vector<uint64_t> decodedSizes(numStreams);
    runScratch_.clear();
    runScratch_.reserve(total);
    for (uint32_t s = 0; s < numStreams; ++s) {
      decodedSizes[s] = in.cells->bytes(pid, s);
      in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
        runScratch_.append(data, bytes);
      });
    }
    writeRun(out, runScratch_.data(), total, decodedSizes.data());
  }
}

namespace {

/// "12.3MB"-style size for the diagnostics log.
std::string mb(uint64_t bytes) {
  char buf[32];
  ::snprintf(buf, sizeof(buf), "%.2fMB", bytes / (1024.0 * 1024.0));
  return buf;
}

/// Appends "N runs [a, b, ...]" for one window's run offset tables and
/// returns the runs' total spill bytes.
uint64_t describeRuns(
    const std::vector<std::vector<uint64_t>>& runPidEnds,
    uint32_t numPartitions,
    std::ostringstream& os) {
  constexpr size_t kMaxListedRuns = 16;
  uint64_t total = 0;
  os << runPidEnds.size() << " runs [";
  for (size_t i = 0; i < runPidEnds.size(); ++i) {
    const uint64_t bytes =
        runPidEnds[i][numPartitions] - runPidEnds[i][0];
    total += bytes;
    if (i < kMaxListedRuns) {
      os << (i > 0 ? ", " : "") << mb(bytes);
    } else if (i == kMaxListedRuns) {
      os << ", +" << (runPidEnds.size() - kMaxListedRuns) << " more";
    }
  }
  os << "]";
  return total;
}

} // namespace

void LocalCellOutput::logWindowDiagnostics(
    const CellWindowInput& in,
    bool windowHasData) {
  constexpr size_t kMaxListedWindows = 64;
  uint64_t totalRuns = 0;
  uint64_t totalRunBytes = 0;
  for (size_t w = 0; w < sealed_.size(); ++w) {
    const auto& window = sealed_[w];
    uint64_t rows = 0;
    uint32_t nonEmpty = 0;
    for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
      rows += window.rowCounts[pid];
      nonEmpty += window.rowCounts[pid] > 0 ? 1 : 0;
    }
    uint64_t nullBytes = 0;
    for (const auto length : window.nullLength) {
      nullBytes += length;
    }
    std::ostringstream os;
    os << "CellShuffleWriter window " << w << ": rows=" << rows
       << ", partitions=" << nonEmpty << "/" << in.numPartitions
       << ", nullBytes=" << mb(nullBytes) << ", ";
    const uint64_t runBytes =
        describeRuns(window.runPidEnds, in.numPartitions, os);
    totalRuns += window.runPidEnds.size();
    totalRunBytes += runBytes;
    if (w < kMaxListedWindows) {
      LOG(INFO) << os.str();
    } else if (w == kMaxListedWindows) {
      LOG(INFO) << "CellShuffleWriter ... " << (sealed_.size() - w)
                << " more sealed windows elided";
    }
  }
  {
    uint64_t rows = 0;
    uint64_t resident = 0;
    if (windowHasData) {
      for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
        rows += in.rowCounts[pid];
      }
      const uint32_t numStreams = layout_->numStreams();
      for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
        for (uint32_t s = 0; s < numStreams; ++s) {
          resident += in.cells->bytes(pid, s);
        }
      }
    }
    std::ostringstream os;
    os << "CellShuffleWriter residual window: rows=" << rows
       << ", resident=" << mb(resident) << ", spilled ";
    const uint64_t runBytes =
        describeRuns(openWindowRuns_, in.numPartitions, os);
    totalRuns += openWindowRuns_.size();
    totalRunBytes += runBytes;
    LOG(INFO) << os.str();
  }
  LOG(INFO) << "CellShuffleWriter totals: " << sealed_.size()
            << " sealed windows, " << totalRuns << " runs, "
            << mb(totalRunBytes) << " spilled";
}

void LocalCellOutput::finalize(
    const CellWindowInput& in,
    bool windowHasData,
    ShuffleWriterMetrics& metrics) {
  // The residual window never takes a spill round-trip: whatever is still
  // in memory is written straight into the data file, alongside any runs
  // the window already spilled mid-stream.

  logWindowDiagnostics(in, windowHasData);
  std::FILE* out = ::fopen(options_.dataFile.c_str(), "wb");
  BOLT_CHECK_NOT_NULL(
      out, "Failed to open shuffle data file {}", options_.dataFile);
  finalBytes_ = 0;

  metrics.partitionLengths.assign(in.numPartitions, 0);
  metrics.rawPartitionLengths.assign(in.numPartitions, 0);
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    const uint64_t partitionStart = finalBytes_;
    const uint64_t rawStart = rawAccum_;
    for (const auto& window : sealed_) {
      writeDiskPayload(out, window, in.encodingTags, pid);
    }
    if (windowHasData && in.rowCounts[pid] > 0) {
      writeCurrentWindowPayload(out, in, pid);
    }
    metrics.partitionLengths[pid] =
        static_cast<int64_t>(finalBytes_ - partitionStart);
    metrics.rawPartitionLengths[pid] =
        static_cast<int64_t>(rawAccum_ - rawStart);
  }
  const uint64_t flushStart = nowNs();
  BOLT_CHECK_EQ(::fflush(out), 0, "shuffle data file flush failed");
  writeTimeNs_ += nowNs() - flushStart;
  BOLT_CHECK_EQ(::fclose(out), 0, "shuffle data file close failed");

  if (spillFile_ != nullptr) {
    ::fclose(spillFile_);
    ::unlink(spillPath_.c_str());
    spillFile_ = nullptr;
    spillFd_ = -1;
  }
  runScratch_.reset();
  compressScratch_.reset();
  gather_.reset();
  scratch_.reset();

  metrics.totalBytesWritten = static_cast<int64_t>(finalBytes_);
  metrics.totalBytesEvicted = bytesEvicted_;
  metrics.totalWriteTime = static_cast<int64_t>(writeTimeNs_);
  metrics.totalEvictTime = static_cast<int64_t>(evictTimeNs_);
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(sealed_.size());
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
