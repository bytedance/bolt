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

void appendLe32(std::string& out, uint32_t value) {
  out.append(reinterpret_cast<const char*>(&value), 4);
}

void appendLe64(std::string& out, uint64_t value) {
  out.append(reinterpret_cast<const char*>(&value), 8);
}

/// Builds the spec section 4.2 null body of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    std::string& out) {
  const uint32_t numColumns = in.layout->numColumns();
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const size_t tagStart = out.size();
  out.append(tagBytes, '\0');
  auto* tags = reinterpret_cast<uint8_t*>(out.data() + tagStart);
  const uint32_t bitmapBytes = (rows + 7) / 8;
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto summary = in.nulls->summarize(pid, col, rows);
    setNullTag(tags, col, summary.tag);
  }
  // Bitmaps follow the tags for RAW_NULL columns, in column order. The tag
  // byte array may move as `out` grows, so tags were fully written first.
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (getNullTag(
            reinterpret_cast<const uint8_t*>(out.data() + tagStart), col) ==
        NullTag::kRawNull) {
      const size_t at = out.size();
      out.resize(at + bitmapBytes);
      in.nulls->emitBitmap(
          pid, col, rows, reinterpret_cast<uint8_t*>(out.data() + at));
    }
  }
}

} // namespace

LocalCellOutput::LocalCellOutput(
    PartitionWriterOptions options,
    const CellLayout* layout,
    int64_t compressMinRunBytes)
    : options_(std::move(options)),
      layout_(layout),
      compressMinRunBytes_(compressMinRunBytes) {
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
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, spillFile_),
      bytes,
      "cell spill write failed");
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
  const uint64_t start = nowNs();
  ensureSpillFile();
  const uint32_t numStreams = layout_->numStreams();
  auto& ends = openWindowRuns_.emplace_back();
  ends.resize(in.numPartitions + 1);
  ends[0] = spillOffset_;

  scratch_.clear();
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    uint64_t total = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      total += in.cells->bytes(pid, s);
    }
    if (total > 0) {
      // Run body, COMBINED_STORED (spec section 5): layout byte, the single
      // stored size, the decoded size of every stream, then the bytes.
      scratch_.clear();
      scratch_.push_back(
          static_cast<char>(static_cast<uint8_t>(RunLayout::kCombinedStored)));
      appendLe64(scratch_, total);
      for (uint32_t s = 0; s < numStreams; ++s) {
        appendLe64(scratch_, in.cells->bytes(pid, s));
      }
      spillWrite(scratch_.data(), scratch_.size());
      for (uint32_t s = 0; s < numStreams; ++s) {
        in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
          spillWrite(data, bytes);
        });
      }
    }
    ends[pid + 1] = spillOffset_;
  }
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  evictTimeNs_ += nowNs() - start;
}

void LocalCellOutput::sealWindow(const CellWindowInput& in) {
  const uint64_t start = nowNs();
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
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  sealed_.push_back(std::move(window));
  evictTimeNs_ += nowNs() - start;
}

void LocalCellOutput::writeOut(std::FILE* out, const void* data, size_t bytes) {
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, out), bytes, "shuffle data file write failed");
  finalBytes_ += bytes;
}

void LocalCellOutput::writeRun(
    std::FILE* out,
    const char* data,
    uint64_t dataBytes,
    const uint64_t* decodedSizes) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t headerBytes = 1 + 8 + 8ull * numStreams;
  rawAccum_ += headerBytes + dataBytes;

  const char* body = data;
  uint64_t stored = dataBytes;
  auto runLayout = RunLayout::kCombinedStored;
  if (codec_ != nullptr &&
      dataBytes >= static_cast<uint64_t>(compressMinRunBytes_)) {
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
      runLayout = RunLayout::kCombined;
    }
  }

  scratch_.clear();
  scratch_.push_back(static_cast<char>(static_cast<uint8_t>(runLayout)));
  appendLe64(scratch_, stored);
  for (uint32_t stream = 0; stream < numStreams; ++stream) {
    appendLe64(scratch_, decodedSizes[stream]);
  }
  writeOut(out, scratch_.data(), scratch_.size());
  writeOut(out, body, stored);
}

void LocalCellOutput::writeDiskPayload(
    std::FILE* out,
    const SealedWindow& w,
    uint32_t pid) {
  const uint32_t rows = w.rowCounts[pid];
  if (rows == 0) {
    return;
  }
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t runHeaderBytes = 1 + 8 + 8ull * numStreams;
  uint32_t runCount = 0;
  for (const auto& ends : w.runPidEnds) {
    runCount += ends[pid + 1] > ends[pid] ? 1 : 0;
  }
  scratch_.clear();
  appendLe32(scratch_, rows);
  appendLe32(scratch_, runCount);
  appendLe64(scratch_, w.variableBytes[pid]);
  appendLe32(scratch_, w.nullLength[pid]);
  appendLe32(scratch_, 0); // null body stored uncompressed
  const size_t nullAt = scratch_.size();
  scratch_.resize(nullAt + w.nullLength[pid]);
  readSpill(w.nullOffset[pid], scratch_.data() + nullAt, w.nullLength[pid]);
  scratch_.append((layout_->numColumns() + 7) / 8, '\0'); // encoding tags
  rawAccum_ += scratch_.size();
  writeOut(out, scratch_.data(), scratch_.size());

  std::vector<uint64_t> decodedSizes(numStreams);
  for (const auto& ends : w.runPidEnds) {
    const uint64_t segmentBytes = ends[pid + 1] - ends[pid];
    if (segmentBytes == 0) {
      continue;
    }
    // The spill segment is a COMBINED_STORED run body; lift its stream
    // sizes and hand the data to the (possibly compressing) run writer.
    BOLT_CHECK_GE(segmentBytes, runHeaderBytes, "corrupt cell spill segment");
    scratch_.resize(runHeaderBytes);
    readSpill(ends[pid], scratch_.data(), runHeaderBytes);
    BOLT_CHECK_EQ(
        static_cast<uint8_t>(scratch_[0]),
        static_cast<uint8_t>(RunLayout::kCombinedStored),
        "corrupt cell spill segment");
    ::memcpy(decodedSizes.data(), scratch_.data() + 9, 8ull * numStreams);
    const uint64_t dataBytes = segmentBytes - runHeaderBytes;
    runScratch_.resize(dataBytes);
    readSpill(ends[pid] + runHeaderBytes, runScratch_.data(), dataBytes);
    writeRun(out, runScratch_.data(), dataBytes, decodedSizes.data());
  }
}

void LocalCellOutput::writeLivePayload(
    std::FILE* out,
    const CellWindowInput& in,
    uint32_t pid) {
  const uint32_t rows = in.rowCounts[pid];
  const uint32_t numStreams = layout_->numStreams();
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    total += in.cells->bytes(pid, s);
  }
  const uint32_t runCount = total > 0 ? 1 : 0;

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
  scratch_.append((layout_->numColumns() + 7) / 8, '\0'); // encoding tags
  rawAccum_ += scratch_.size();
  writeOut(out, scratch_.data(), scratch_.size());
  if (runCount > 0) {
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

void LocalCellOutput::finalize(
    const CellWindowInput& in,
    bool windowHasData,
    ShuffleWriterMetrics& metrics) {
  bool liveWindow = false;
  if (windowHasData) {
    if (hasDiskState()) {
      spillRun(in);
      in.cells->releaseAll();
      sealWindow(in);
    } else {
      liveWindow = true;
    }
  }

  const uint64_t start = nowNs();
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
      writeDiskPayload(out, window, pid);
    }
    if (liveWindow && in.rowCounts[pid] > 0) {
      writeLivePayload(out, in, pid);
    }
    metrics.partitionLengths[pid] =
        static_cast<int64_t>(finalBytes_ - partitionStart);
    metrics.rawPartitionLengths[pid] =
        static_cast<int64_t>(rawAccum_ - rawStart);
  }
  BOLT_CHECK_EQ(::fclose(out), 0, "shuffle data file close failed");
  writeTimeNs_ += nowNs() - start;

  if (spillFile_ != nullptr) {
    ::fclose(spillFile_);
    ::unlink(spillPath_.c_str());
    spillFile_ = nullptr;
    spillFd_ = -1;
  }

  metrics.totalBytesWritten = static_cast<int64_t>(finalBytes_);
  metrics.totalBytesEvicted = bytesEvicted_;
  metrics.totalWriteTime = static_cast<int64_t>(writeTimeNs_);
  metrics.totalEvictTime = static_cast<int64_t>(evictTimeNs_);
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(sealed_.size());
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
