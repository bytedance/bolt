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
#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>
#include <sstream>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/Utils.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

constexpr uint64_t kMinCompressRunBytes = 1 << 10;

uint64_t currentTimeNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Keep existing capacity on clear or shrink; growth is shared across
// partitions.
void resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool) {
  if (!buffer) {
    if (size == 0) {
      return;
    }
    buffer = AlignedBuffer::allocate<char>(std::max<size_t>(64, size), pool);
  } else if (size > buffer->capacity()) {
    AlignedBuffer::reallocate<char>(
        &buffer,
        std::max(size, checkedMultiply<size_t>(2, buffer->capacity())));
  }
  buffer->setSize(size);
}

void appendBytes(
    BufferPtr& out,
    const void* data,
    size_t bytes,
    memory::MemoryPool* pool) {
  if (bytes == 0) {
    return;
  }
  const size_t at = out ? out->size() : 0;
  resizeBuffer(out, checkedPlus(at, bytes), pool);
  ::memcpy(out->asMutable<char>() + at, data, bytes);
}

void appendLe32(BufferPtr& out, uint32_t value, memory::MemoryPool* pool) {
  appendBytes(out, &value, sizeof(value), pool);
}

void appendLe64(BufferPtr& out, uint64_t value, memory::MemoryPool* pool) {
  appendBytes(out, &value, sizeof(value), pool);
}

/// Builds the spec section 4.2 null body of one partition.
void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool) {
  const uint32_t numColumns = in.layout->numWireColumns();
  if (numColumns == 0) {
    return;
  }
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const size_t tagStart = out ? out->size() : 0;
  resizeBuffer(out, checkedPlus<size_t>(tagStart, tagBytes), pool);
  auto* tags = out->asMutable<uint8_t>() + tagStart;
  ::memset(tags, 0, tagBytes);
  const uint32_t bitmapBytes = (rows + 7) / 8;
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto summary = in.nulls->summarize(pid, col, rows);
    setNullTag(tags, col, summary.tag);
  }
  // Bitmaps follow the tags for RAW_NULL columns, in column order. The tag
  // byte array may move as `out` grows, so tags were fully written first.
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (getNullTag(out->as<uint8_t>() + tagStart, col) == NullTag::kRawNull) {
      const size_t at = out->size();
      resizeBuffer(out, checkedPlus<size_t>(at, bitmapBytes), pool);
      in.nulls->emitBitmap(pid, col, rows, out->asMutable<uint8_t>() + at);
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
      pool_(pool) {
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
    // One spill file per task, in a random directory as the local partition
    // writer does, so concurrent tasks spread their spill I/O over the disks.
    const auto& dirs = options_.configuredDirs;
    std::default_random_engine engine(std::random_device{}());
    const auto dir = getSpilledShuffleFileDir(
        dirs[std::uniform_int_distribution<size_t>(0, dirs.size() - 1)(engine)],
        std::uniform_int_distribution<int32_t>(
            0, std::max<int32_t>(options_.numSubDirs, 1) - 1)(engine));
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
  const uint64_t start = currentTimeNs();
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, spillFile_), bytes, "cell spill write failed");
  evictTimeNs_ += currentTimeNs() - start;
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
  auto& run = openWindowRuns_.emplace_back();
  run.storedIsFinal = codec_ == nullptr || cellOptions_.compressSpill;
  auto& ends = run.pidEnds;
  ends.resize(in.numPartitions + 1);
  ends[0] = spillOffset_;

  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    uint64_t total = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      total += in.cells->bytes(pid, s);
    }
    if (total > 0) {
      // Store runs in wire format so merge can copy them without re-encoding
      // when run coalescing is disabled.
      const char* body = nullptr;
      uint64_t stored = total;
      auto runLayout = RunLayout::kCombinedStored;
      if (codec_ != nullptr && cellOptions_.compressSpill) {
        // The gather/compress workspaces come from the task pool, and a
        // spill is often the moment that pool is exhausted; when they cannot
        // be funded, degrade to streaming the run out uncompressed rather
        // than failing the very operation meant to relieve the pressure.
        try {
          resizeBuffer(runScratch_, 0, pool_);
          resizeBuffer(runScratch_, total, pool_);
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
          runLayout = RunLayout::kCombinedStored;
          body = nullptr;
          stored = total;
          run.storedIsFinal = false;
        }
      }
      resizeBuffer(scratch_, 1, pool_);
      scratch_->asMutable<uint8_t>()[0] = static_cast<uint8_t>(runLayout);
      appendLe64(scratch_, stored, pool_);
      for (uint32_t s = 0; s < numStreams; ++s) {
        appendLe64(scratch_, in.cells->bytes(pid, s), pool_);
      }
      spillWrite(scratch_->as<char>(), scratch_->size());
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
  const uint64_t flushStart = currentTimeNs();
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  evictTimeNs_ += currentTimeNs() - flushStart;
}

void LocalCellOutput::sealWindow(const CellWindowInput& in) {
  ensureSpillFile();
  SealedWindow window;
  window.runs = std::move(openWindowRuns_);
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
    resizeBuffer(scratch_, 0, pool_);
    buildNullBody(in, pid, window.rowCounts[pid], scratch_, pool_);
    window.nullOffset[pid] = spillOffset_;
    window.nullLength[pid] =
        scratch_ ? static_cast<uint32_t>(scratch_->size()) : 0;
    if (window.nullLength[pid] > 0) {
      spillWrite(scratch_->as<char>(), scratch_->size());
    }
  }
  const uint64_t flushStart = currentTimeNs();
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "cell spill flush failed");
  evictTimeNs_ += currentTimeNs() - flushStart;
  sealed_.push_back(std::move(window));
}

void LocalCellOutput::writeOut(std::FILE* out, const void* data, size_t bytes) {
  const uint64_t start = currentTimeNs();
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, out), bytes, "shuffle data file write failed");
  writeTimeNs_ += currentTimeNs() - start;
  finalBytes_ += bytes;
}

RunLayout LocalCellOutput::maybeCompressRun(
    const char* data,
    uint64_t dataBytes,
    const char*& body,
    uint64_t& stored) {
  body = data;
  stored = dataBytes;
  if (codec_ != nullptr && dataBytes >= kMinCompressRunBytes) {
    const uint64_t start = currentTimeNs();
    resizeBuffer(compressScratch_, codec_->maxCompressedLen(dataBytes), pool_);
    const int64_t written = codec_->compress(
        reinterpret_cast<const uint8_t*>(data),
        static_cast<int64_t>(dataBytes),
        compressScratch_->asMutable<uint8_t>(),
        static_cast<int64_t>(compressScratch_->size()));
    compressTimeNs_ += currentTimeNs() - start;
    // Spec section 5: fall back to the stored form when compression does
    // not pay.
    if (written > 0 && static_cast<uint64_t>(written) < dataBytes) {
      body = compressScratch_->as<char>();
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

  resizeBuffer(scratch_, 1, pool_);
  scratch_->asMutable<uint8_t>()[0] = static_cast<uint8_t>(runLayout);
  appendLe64(scratch_, stored, pool_);
  for (uint32_t stream = 0; stream < numStreams; ++stream) {
    appendLe64(scratch_, decodedSizes[stream], pool_);
  }
  writeOut(out, scratch_->as<char>(), scratch_->size());
  writeOut(out, body, stored);
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
    readSpill(segments[i].begin, head, sizeof(head));
    layouts[i] = head[0];
    ::memcpy(&storedSizes[i], head + 1, 8);
    readSpill(
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
  resizeBuffer(gather_, 0, pool_);
  resizeBuffer(gather_, total, pool_);

  // Pass 2: one sequential read per segment, then scatter per stream.
  for (size_t i = 0; i < segments.size(); ++i) {
    uint64_t dataBytes = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      dataBytes += segSizes[i * numStreams + s];
    }
    const char* body;
    if (layouts[i] == static_cast<uint8_t>(RunLayout::kCombined)) {
      resizeBuffer(runScratch_, 0, pool_);
      resizeBuffer(runScratch_, storedSizes[i], pool_);
      readSpill(
          segments[i].begin + runHeaderBytes,
          runScratch_->asMutable<char>(),
          storedSizes[i]);
      resizeBuffer(compressScratch_, 0, pool_);
      resizeBuffer(compressScratch_, dataBytes, pool_);
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
          static_cast<uint8_t>(RunLayout::kCombinedStored),
          "corrupt cell spill segment");
      resizeBuffer(runScratch_, 0, pool_);
      resizeBuffer(runScratch_, dataBytes, pool_);
      readSpill(
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
    std::FILE* out,
    const SealedWindow& w,
    const uint8_t* encodingTags,
    uint32_t pid) {
  const uint32_t rows = w.rowCounts[pid];
  if (rows == 0) {
    return;
  }
  const auto segments = partitionSegments(w.runs, pid);
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  const uint32_t runCount = coalesce ? (segments.empty() ? 0 : 1)
                                     : static_cast<uint32_t>(segments.size());
  resizeBuffer(scratch_, 0, pool_);
  appendLe32(scratch_, rows, pool_);
  appendLe32(scratch_, runCount, pool_);
  appendLe64(scratch_, w.variableBytes[pid], pool_);
  appendLe32(scratch_, w.nullLength[pid], pool_);
  appendLe32(scratch_, 0, pool_); // null body stored uncompressed
  const size_t nullAt = scratch_->size();
  resizeBuffer(scratch_, checkedPlus<size_t>(nullAt, w.nullLength[pid]), pool_);
  readSpill(
      w.nullOffset[pid],
      scratch_->asMutable<char>() + nullAt,
      w.nullLength[pid]);
  appendBytes(
      scratch_, encodingTags, (layout_->numWireColumns() + 7) / 8, pool_);
  rawAccum_ += scratch_->size();
  writeOut(out, scratch_->as<char>(), scratch_->size());

  if (coalesce && segments.size() > 1) {
    std::vector<uint64_t> streamSizes;
    const uint64_t total =
        gatherPartitionRuns(segments, nullptr, pid, streamSizes);
    writeRun(out, gather_->as<char>(), total, streamSizes.data());
    return;
  }
  // Without coalescing, or with a single segment that already is the one
  // run: copied verbatim when in final form, never re-encoded.
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment);
  }
}

void LocalCellOutput::writeSpilledSegment(
    std::FILE* out,
    const SpillSegment& segment) {
  const uint32_t numStreams = layout_->numStreams();
  const uint64_t runHeaderBytes = 1 + 8 + 8ull * numStreams;
  const uint64_t segmentBytes = segment.end - segment.begin;
  // The spill segment is a run body in wire form.
  BOLT_CHECK_GE(segmentBytes, runHeaderBytes, "corrupt cell spill segment");
  resizeBuffer(scratch_, runHeaderBytes, pool_);
  readSpill(segment.begin, scratch_->asMutable<char>(), runHeaderBytes);
  const auto segmentLayout = static_cast<RunLayout>(scratch_->as<uint8_t>()[0]);
  BOLT_CHECK(
      segmentLayout == RunLayout::kCombined ||
          segmentLayout == RunLayout::kCombinedStored,
      "corrupt cell spill segment");
  std::vector<uint64_t> decodedSizes(numStreams);
  ::memcpy(decodedSizes.data(), scratch_->as<char>() + 9, 8ull * numStreams);
  const uint64_t dataBytes = segmentBytes - runHeaderBytes;
  // A compressed segment, and a stored one whose spill already made the
  // compression attempt, is the final run. Any other stored segment gets
  // the attempt here - unless its workspace cannot be funded, in which case
  // the stored form, a valid run as well, is copied as it is.
  if (segmentLayout == RunLayout::kCombinedStored && !segment.storedIsFinal &&
      codec_ != nullptr && dataBytes >= kMinCompressRunBytes) {
    bool funded = true;
    try {
      resizeBuffer(runScratch_, 0, pool_);
      resizeBuffer(runScratch_, dataBytes, pool_);
      resizeBuffer(
          compressScratch_, codec_->maxCompressedLen(dataBytes), pool_);
    } catch (const std::exception&) {
      funded = false;
      runScratch_.reset();
      compressScratch_.reset();
    }
    if (funded) {
      readSpill(
          segment.begin + runHeaderBytes,
          runScratch_->asMutable<char>(),
          dataBytes);
      writeRun(out, runScratch_->as<char>(), dataBytes, decodedSizes.data());
      return;
    }
  }
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes) {
    decodedSum += size;
  }
  rawAccum_ += runHeaderBytes + decodedSum;
  writeOut(out, scratch_->as<char>(), runHeaderBytes);
  char copyBuffer[64 << 10];
  uint64_t offset = segment.begin + runHeaderBytes;
  uint64_t left = dataBytes;
  while (left > 0) {
    const size_t chunk = left < sizeof(copyBuffer) ? left : sizeof(copyBuffer);
    readSpill(offset, copyBuffer, chunk);
    writeOut(out, copyBuffer, chunk);
    offset += chunk;
    left -= chunk;
  }
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
  const auto segments = partitionSegments(openWindowRuns_, pid);
  const bool coalesce = cellOptions_.coalesceMergedRuns;
  uint32_t runCount;
  if (coalesce) {
    runCount = (total > 0 || !segments.empty()) ? 1 : 0;
  } else {
    runCount = (total > 0 ? 1 : 0) + static_cast<uint32_t>(segments.size());
  }

  resizeBuffer(scratch_, 0, pool_);
  appendLe32(scratch_, rows, pool_);
  appendLe32(scratch_, runCount, pool_);
  appendLe64(scratch_, in.variableBytes[pid], pool_);
  const size_t nullSizeAt = scratch_->size();
  appendLe32(scratch_, 0, pool_); // patched below
  appendLe32(scratch_, 0, pool_); // uncompressed
  const size_t nullBodyAt = scratch_->size();
  buildNullBody(in, pid, rows, scratch_, pool_);
  const uint32_t nullLength =
      static_cast<uint32_t>(scratch_->size() - nullBodyAt);
  ::memcpy(scratch_->asMutable<char>() + nullSizeAt, &nullLength, 4);
  appendBytes(
      scratch_, in.encodingTags, (layout_->numWireColumns() + 7) / 8, pool_);
  rawAccum_ += scratch_->size();
  writeOut(out, scratch_->as<char>(), scratch_->size());
  if (coalesce && (segments.size() > 1 || (total > 0 && !segments.empty()))) {
    std::vector<uint64_t> streamSizes;
    const uint64_t gathered =
        gatherPartitionRuns(segments, &in, pid, streamSizes);
    writeRun(out, gather_->as<char>(), gathered, streamSizes.data());
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
    resizeBuffer(runScratch_, 0, pool_);
    resizeBuffer(runScratch_, total, pool_);
    size_t offset = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      decodedSizes[s] = in.cells->bytes(pid, s);
      in.cells->scan(pid, s, [&](const char* data, uint32_t bytes) {
        ::memcpy(runScratch_->asMutable<char>() + offset, data, bytes);
        offset += bytes;
      });
    }
    writeRun(out, runScratch_->as<char>(), total, decodedSizes.data());
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
    uint32_t pid) {
  std::vector<SpillSegment> segments;
  segments.reserve(runs.size());
  for (const auto& run : runs) {
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
    const uint64_t runBytes = describeRuns(window.runs, in.numPartitions, os);
    totalRuns += window.runs.size();
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
  const uint64_t flushStart = currentTimeNs();
  BOLT_CHECK_EQ(::fflush(out), 0, "shuffle data file flush failed");
  writeTimeNs_ += currentTimeNs() - flushStart;
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
