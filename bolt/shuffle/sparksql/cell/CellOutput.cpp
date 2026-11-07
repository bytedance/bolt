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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/lang/Bits.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <sstream>
#include <string_view>

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

/// Writes partition `pid`'s resident cells to `sink` as one run; nothing
/// when it has none.
void writeCells(
    RunCompressor& compressor,
    RunSink& sink,
    const CellWindowInput& in,
    uint32_t pid) {
  compressor.write(
      sink,
      in.layout->numStreams(),
      [&](uint32_t s) { return in.cells->bytes(pid, s); },
      [&](uint32_t s, RunPieceFn fn) { in.cells->scan(pid, s, fn); });
}

/// The data file read forward for verifyCellDataFile(), through a window
/// that each miss reads afresh from the missed offset. A pread costs about
/// as much as copying 4 KiB from the page cache, so a miss reads 1 KiB -
/// the headers of a payload, or of a run, after the walk skipped a body -
/// unless it lands across the window's end or within 1 KiB of it, as the
/// headers of small partitions do: then twice the last read, up to 64 KiB.
class DataFileReader {
 public:
  explicit DataFileReader(const std::string& path)
      : path_(path), fd_(::open(path.c_str(), O_RDONLY | O_CLOEXEC)) {
    BOLT_CHECK_GE(
        fd_,
        0,
        "cell data file verification failed: cannot open {}: {}",
        path_,
        folly::errnoStr(errno));
    struct stat st;
    const int status = ::fstat(fd_, &st);
    BOLT_CHECK_EQ(
        status,
        0,
        "cell data file verification failed: cannot stat {}: {}",
        path_,
        folly::errnoStr(errno));
    size_ = static_cast<uint64_t>(st.st_size);
  }

  ~DataFileReader() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  DataFileReader(const DataFileReader&) = delete;
  DataFileReader& operator=(const DataFileReader&) = delete;

  uint64_t size() const {
    return size_;
  }

  /// Copies [offset, offset + bytes), which lies within the file, to `out`.
  void read(uint64_t offset, void* out, size_t bytes) {
    if (offset < windowOffset_ ||
        offset + bytes > windowOffset_ + windowBytes_) {
      fill(offset, bytes);
    }
    ::memcpy(out, window_.get() + (offset - windowOffset_), bytes);
  }

  /// Up to 32 bytes from `offset` as hex, read as far as the file gives
  /// them: for a failure's message, it never throws.
  std::string hexAt(uint64_t offset) const {
    uint8_t bytes[32];
    size_t got = 0;
    while (offset < size_ && got < sizeof(bytes)) {
      const ssize_t n = ::pread(fd_, bytes + got, sizeof(bytes) - got, offset);
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n <= 0) {
        break;
      }
      got += static_cast<size_t>(n);
      offset += static_cast<uint64_t>(n);
    }
    if (got == 0) {
      return "none";
    }
    std::string out;
    for (size_t i = 0; i < got; ++i) {
      fmt::format_to(
          std::back_inserter(out), "{}{:02x}", i == 0 ? "" : " ", bytes[i]);
    }
    return out;
  }

 private:
  static constexpr size_t kMinFillBytes = 1 << 10;
  static constexpr size_t kMaxFillBytes = 64 << 10;

  void fill(uint64_t offset, size_t bytes) {
    const uint64_t windowEnd = windowOffset_ + windowBytes_;
    fillBytes_ = windowBytes_ > 0 && offset >= windowOffset_ &&
            offset < windowEnd + kMinFillBytes
        ? std::min(2 * fillBytes_, kMaxFillBytes)
        : kMinFillBytes;
    const auto want = static_cast<size_t>(
        std::min<uint64_t>(std::max(bytes, fillBytes_), size_ - offset));
    if (want > capacity_) {
      window_ = std::make_unique<char[]>(want);
      capacity_ = want;
    }
    windowBytes_ = 0;
    size_t done = 0;
    while (done < want) {
      const ssize_t n =
          ::pread(fd_, window_.get() + done, want - done, offset + done);
      if (n < 0 && errno == EINTR) {
        continue;
      }
      BOLT_CHECK_GT(
          n,
          0,
          "cell data file verification failed: reading {} at {} failed: {}",
          path_,
          offset + done,
          n < 0 ? folly::errnoStr(errno) : std::string("end of file"));
      done += static_cast<size_t>(n);
    }
    windowOffset_ = offset;
    windowBytes_ = want;
  }

  const std::string path_;
  const int fd_;
  uint64_t size_{0};
  std::unique_ptr<char[]> window_;
  size_t capacity_{0};
  uint64_t windowOffset_{0};
  size_t windowBytes_{0};
  size_t fillBytes_{kMinFillBytes};
};

/// A header field the walk checks: `name`, of run `run` and its stream
/// `stream` when they are not -1.
struct WalkField {
  const char* name;
  int64_t run{-1};
  int64_t stream{-1};

  std::string toString() const {
    std::string out;
    if (run >= 0) {
      out += fmt::format("run {} ", run);
    }
    if (stream >= 0) {
      out += fmt::format("stream {} ", stream);
    }
    return out + name;
  }
};

/// verifyCellDataFile()'s walk, block by block, with where it is for a
/// failure's message.
class DataFileWalk {
 public:
  DataFileWalk(
      const std::string& path,
      uint32_t numWireColumns,
      uint32_t numStreams,
      const std::vector<CellPayloadRecord>* written)
      : file_(path),
        numWireColumns_(numWireColumns),
        numStreams_(numStreams),
        tagBytes_((uint64_t{numWireColumns} + 7) / 8),
        written_(written) {}

  CellDataFileSummary walk(std::span<const int64_t> partitionLengths) {
    uint64_t total = 0;
    for (size_t pid = 0; pid < partitionLengths.size(); ++pid) {
      if (partitionLengths[pid] < 0) {
        failFile(
            0,
            fmt::format("partition {} length", pid),
            "at least 0",
            std::to_string(partitionLengths[pid]));
      }
      total += static_cast<uint64_t>(partitionLengths[pid]);
    }
    if (total != file_.size()) {
      failFile(
          std::min(total, file_.size()),
          "file size",
          fmt::format("{}, the partition lengths' sum", total),
          std::to_string(file_.size()));
    }
    uint64_t start = 0;
    for (size_t pid = 0; pid < partitionLengths.size(); ++pid) {
      const uint64_t end = start + static_cast<uint64_t>(partitionLengths[pid]);
      walkPartition(static_cast<uint32_t>(pid), start, end);
      start = end;
    }
    if (written_ != nullptr && nextWritten_ != written_->size()) {
      failFile(
          (*written_)[nextWritten_].offset,
          "payloads",
          fmt::format(
              "{} written, the next at file offset {}",
              written_->size(),
              (*written_)[nextWritten_].offset),
          fmt::format("{} in the file", nextWritten_));
    }
    summary_.partitions = partitionLengths.size();
    return summary_;
  }

 private:
  /// The partition's blocks, back to back from `start`, the last ending at
  /// `end`.
  void walkPartition(uint32_t pid, uint64_t start, uint64_t end) {
    pid_ = pid;
    end_ = end;
    blockIndex_ = 0;
    payloadIndex_ = 0;
    compositeIndex_ = 0;
    // Each block stays within the partition, so the last one ends at its
    // end.
    for (uint64_t at = start; at < end; ++blockIndex_) {
      blockAt_ = at;
      blockKind_ = -1;
      uint8_t kind;
      read(at, &kind, 1, {"block kind"});
      if (kind == static_cast<uint8_t>(CellBlockKind::kPayload)) {
        blockKind_ = kind;
        at = walkPayload(at);
        ++payloadIndex_;
      } else if (kind == static_cast<uint8_t>(CellBlockKind::kCompositeRows)) {
        blockKind_ = kind;
        at = walkComposite(at);
        ++compositeIndex_;
      } else {
        fail(
            at,
            {"block kind"},
            "0 (payload) or 1 (composite rows)",
            std::to_string(kind));
      }
    }
  }

  /// The payload whose kind byte is at `at`; returns its end.
  uint64_t walkPayload(uint64_t at) {
    uint8_t header[kPayloadFixedHeaderBytes];
    read(at + 1, header, sizeof(header), {"payload header"});
    const auto rows = folly::loadUnaligned<uint32_t>(header);
    const auto runCount = folly::loadUnaligned<uint32_t>(header + 4);
    const auto variableBytes = folly::loadUnaligned<uint64_t>(header + 8);
    const auto nullStoredSize = folly::loadUnaligned<uint32_t>(header + 16);
    const auto nullDecodedSize = folly::loadUnaligned<uint32_t>(header + 20);
    // What the writer wrote first: a patch over a payload's header reads as
    // a header of other counts.
    if (written_ != nullptr) {
      if (nextWritten_ == written_->size()) {
        fail(at, {"payload"}, "none, the writer wrote no more", "a payload");
      }
      const auto& record = (*written_)[nextWritten_++];
      if (record.offset != at) {
        fail(
            at,
            {"payload offset"},
            fmt::format("{}, the writer's next payload", record.offset),
            std::to_string(at));
      }
      if (record.rows != rows) {
        fail(
            at + 1,
            {"row count"},
            std::to_string(record.rows),
            std::to_string(rows));
      }
      if (record.runCount != runCount) {
        fail(
            at + 5,
            {"run count"},
            std::to_string(record.runCount),
            std::to_string(runCount));
      }
    }
    if (rows == 0 || rows > kMaxPayloadRows) {
      fail(
          at + 1,
          {"row count"},
          fmt::format("1 to {}", kMaxPayloadRows),
          std::to_string(rows));
    }
    if (runCount > kMaxRunCount) {
      fail(
          at + 5,
          {"run count"},
          fmt::format("at most {}", kMaxRunCount),
          std::to_string(runCount));
    }
    if (numWireColumns_ == 0) {
      if (runCount != 0 || variableBytes != 0 || nullStoredSize != 0 ||
          nullDecodedSize != 0) {
        fail(
            at + 5,
            {"header without wire columns"},
            "runCount, variableBytes, nullStoredSize and nullDecodedSize 0",
            fmt::format(
                "{}, {}, {} and {}",
                runCount,
                variableBytes,
                nullStoredSize,
                nullDecodedSize));
      }
    } else if (nullStoredSize == 0) {
      fail(at + 17, {"null stored size"}, "at least 1", "0");
    }
    const uint64_t nullAt = at + 1 + kPayloadFixedHeaderBytes;
    const uint64_t tagsAt =
        skip(nullAt, nullStoredSize, at + 17, {"null stored size"});
    uint64_t runAt = skip(tagsAt, tagBytes_, tagsAt, {"encoding tags"});
    for (uint32_t run = 0; run < runCount; ++run) {
      runAt = walkRun(runAt, run);
    }
    ++summary_.payloads;
    summary_.runs += runCount;
    return runAt;
  }

  /// Run `run` of a payload, at `at`; returns its end.
  uint64_t walkRun(uint64_t at, uint32_t run) {
    uint8_t layout;
    read(at, &layout, 1, {"layout", run});
    const bool separate = layout == static_cast<uint8_t>(RunLayout::kSeparate);
    if (!separate && layout != static_cast<uint8_t>(RunLayout::kCombined) &&
        layout != static_cast<uint8_t>(RunLayout::kCombinedUncompressed)) {
      fail(
          at,
          {"layout", run},
          "0 (combined), 1 (separate) or 2 (combined uncompressed)",
          std::to_string(layout));
    }
    // The stored sizes - one per stream, or one - then a decoded size per
    // stream.
    const uint32_t numStored = separate ? numStreams_ : 1;
    const uint64_t sizesAt = at + 1;
    sizes_.resize(numStored + numStreams_);
    read(
        sizesAt,
        sizes_.data(),
        sizes_.size() * sizeof(uint64_t),
        {"header", run});
    uint64_t stored = 0;
    for (uint32_t s = 0; s < numStored; ++s) {
      if (__builtin_add_overflow(stored, sizes_[s], &stored)) {
        fail(
            sizesAt + 8 * s,
            {"stored size", run, separate ? int64_t{s} : int64_t{-1}},
            "stored sizes that add up within 64 bits",
            std::to_string(sizes_[s]));
      }
    }
    // The run's bytes uncompressed: a stream of a separate run stored as it
    // is has decoded size 0.
    uint64_t raw = 0;
    for (uint32_t s = 0; s < numStreams_; ++s) {
      const uint64_t decoded = sizes_[numStored + s];
      const uint64_t decodedAt = sizesAt + 8 * (numStored + s);
      if (separate && sizes_[s] == 0 && decoded != 0) {
        fail(
            decodedAt,
            {"decoded size", run, s},
            "0, the stream being stored empty",
            std::to_string(decoded));
      }
      const uint64_t bytes = separate && decoded == 0 ? sizes_[s] : decoded;
      if (__builtin_add_overflow(raw, bytes, &raw)) {
        fail(
            decodedAt,
            {"decoded size", run, s},
            "decoded sizes that add up within 64 bits",
            std::to_string(decoded));
      }
    }
    // The writer writes no run of a partition without bytes.
    if (raw == 0) {
      fail(at, {"sizes", run}, "a run of at least one byte", "no bytes");
    }
    if (layout == static_cast<uint8_t>(RunLayout::kCombinedUncompressed) &&
        stored != raw) {
      fail(
          sizesAt,
          {"stored size", run},
          fmt::format("{}, its decoded sizes' sum", raw),
          std::to_string(stored));
    }
    if (layout == static_cast<uint8_t>(RunLayout::kCombined) && stored == 0) {
      fail(sizesAt, {"stored size", run}, "at least 1", "0");
    }
    const uint64_t bodyAt = sizesAt + sizes_.size() * sizeof(uint64_t);
    return skip(bodyAt, stored, sizesAt, {"stored size", run});
  }

  /// The CompositeRows block whose kind byte is at `at`; returns its end.
  uint64_t walkComposite(uint64_t at) {
    uint8_t header[kCompositeBlockHeaderBytes - 1];
    read(at + 1, header, sizeof(header), {"composite header"});
    const auto rows = folly::loadUnaligned<uint32_t>(header);
    const auto decoded = folly::loadUnaligned<uint64_t>(header + 4);
    const auto stored = folly::loadUnaligned<uint64_t>(header + 12);
    const uint8_t layout = header[20];
    if (rows == 0 || rows > kMaxPayloadRows) {
      fail(
          at + 1,
          {"composite row count"},
          fmt::format("1 to {}", kMaxPayloadRows),
          std::to_string(rows));
    }
    if (decoded < 4ull * rows) {
      fail(
          at + 5,
          {"composite decoded size"},
          fmt::format("at least {}, 4 per row", 4ull * rows),
          std::to_string(decoded));
    }
    if (layout == static_cast<uint8_t>(RunLayout::kCombinedUncompressed)) {
      if (stored != decoded) {
        fail(
            at + 13,
            {"composite stored size"},
            fmt::format("{}, its decoded size", decoded),
            std::to_string(stored));
      }
    } else if (layout != static_cast<uint8_t>(RunLayout::kCombined)) {
      fail(
          at + 21,
          {"composite layout"},
          "0 (combined) or 2 (combined uncompressed)",
          std::to_string(layout));
    }
    ++summary_.compositeBlocks;
    return skip(
        at + kCompositeBlockHeaderBytes,
        stored,
        at + 13,
        {"composite stored size"});
  }

  /// Reads `bytes` at `at` of the field, which must lie within the
  /// partition.
  void read(uint64_t at, void* out, size_t bytes, const WalkField& field) {
    if (bytes > end_ - at) {
      fail(
          at,
          field,
          fmt::format(
              "{} bytes within the partition, which ends at file offset {}",
              bytes,
              end_),
          fmt::format("{} bytes left", end_ - at));
    }
    file_.read(at, out, bytes);
  }

  /// `at` + `bytes`, which must lie within the partition: the bytes the
  /// field at `fieldAt` gives the size of, skipped.
  uint64_t
  skip(uint64_t at, uint64_t bytes, uint64_t fieldAt, const WalkField& field) {
    if (bytes > end_ - at) {
      fail(
          fieldAt,
          field,
          fmt::format(
              "at most {}, the partition's bytes left at file offset {}",
              end_ - at,
              at),
          std::to_string(bytes));
    }
    return at + bytes;
  }

  /// Fails on `field` at file offset `at`, of the block being walked.
  [[noreturn]] void fail(
      uint64_t at,
      const WalkField& field,
      const std::string& expected,
      const std::string& actual) const {
    std::string block;
    if (blockKind_ == static_cast<int>(CellBlockKind::kPayload)) {
      block = fmt::format("payload {} (block {})", payloadIndex_, blockIndex_);
    } else if (blockKind_ == static_cast<int>(CellBlockKind::kCompositeRows)) {
      block = fmt::format(
          "composite block {} (block {})", compositeIndex_, blockIndex_);
    } else {
      block = fmt::format("block {}", blockIndex_);
    }
    BOLT_FAIL(
        "cell data file verification failed: partition {}, {} at file "
        "offset {}: {} at file offset {}: expected {}, actual {}; 32 bytes at "
        "{}: {}",
        pid_,
        block,
        blockAt_,
        field.toString(),
        at,
        expected,
        actual,
        at,
        file_.hexAt(at));
  }

  /// Fails on `what`, a property of the whole file, at file offset `at`.
  [[noreturn]] void failFile(
      uint64_t at,
      const std::string& what,
      const std::string& expected,
      const std::string& actual) const {
    BOLT_FAIL(
        "cell data file verification failed: {}: expected {}, actual {}; 32 "
        "bytes at {}: {}",
        what,
        expected,
        actual,
        at,
        file_.hexAt(at));
  }

  /// The reader's default bound on a payload's runs.
  static constexpr uint32_t kMaxRunCount = CellDecodeLimits{}.maxRunCount;

  DataFileReader file_;
  const uint32_t numWireColumns_;
  const uint32_t numStreams_;
  const uint64_t tagBytes_;
  const std::vector<CellPayloadRecord>* const written_;
  size_t nextWritten_{0};
  std::vector<uint64_t> sizes_;
  CellDataFileSummary summary_;

  // Where the walk is.
  uint32_t pid_{0};
  uint64_t end_{0};
  uint64_t blockAt_{0};
  int blockKind_{-1};
  uint32_t blockIndex_{0};
  uint32_t payloadIndex_{0};
  uint32_t compositeIndex_{0};
};

} // namespace

CellDataFileSummary verifyCellDataFile(
    const std::string& path,
    std::span<const int64_t> partitionLengths,
    uint32_t numWireColumns,
    uint32_t numStreams,
    const std::vector<CellPayloadRecord>* written) {
  return DataFileWalk(path, numWireColumns, numStreams, written)
      .walk(partitionLengths);
}

LocalCellOutput::LocalCellOutput(
    PartialSpiller& composite,
    const CellLayout* layout,
    CellShuffleOptions cellOptions,
    PartitionWriterOptions options,
    memory::MemoryPool* pool,
    SpillMemory* memory)
    : PartialSpiller(options, "cell", memory),
      composite_(composite),
      options_(std::move(options)),
      layout_(layout),
      pool_(pool),
      verifyDataFile_(cellOptions.verifyDataFile),
      compressor_(options_, cellOptions, &compressTimeNs_) {
  openFrom_.assign(options_.numPartitions, 0);
  sealed_.resize(options_.numPartitions);
}

void LocalCellOutput::spillRun(const CellWindowInput& in) {
  auto& run = runs_.emplace_back();
  // Runs are compressed as they spill, in their final form: both disk
  // passes write compressed bytes and the merge copies the segments
  // verbatim.
  auto& ends = run.pidEnds;
  ends.resize(in.numPartitions + 1);
  ends[0] = spill_.end();

  SpillRunSink sink(spill_);
  for (uint32_t pid = 0; pid < in.numPartitions; ++pid) {
    if (in.runPartitions == nullptr || in.runPartitions[pid] != 0) {
      writeCells(compressor_, sink, in, pid);
    }
    ends[pid + 1] = spill_.end();
  }
  countSpill(ends[0]);
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

void LocalCellOutput::writeSealedPayload(
    DataFileWriter& out,
    const SealedPayload& payload,
    std::span<const uint8_t> encodingTags) {
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
  appendBytes(scratch_, encodingTags.data(), encodingTags.size(), pool_);
  // The raw length counts the null body at its decompressed size.
  rawBytesWritten_ += scratch_->size() +
      (payload.nullDecodedSize == 0
           ? 0
           : payload.nullDecodedSize - payload.nullLength);
  recordPayload(out, rows, runCount);
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
  const uint64_t combinedRunHeaderBytes =
      cell::combinedRunHeaderBytes(numStreams);
  const uint64_t segmentBytes = segment.end - segment.begin;
  // The spill segment is a run in wire form.
  BOLT_CHECK_GE(segmentBytes, 1, "corrupt cell spill segment");
  uint8_t layoutByte;
  spill_.read(segment.begin, &layoutByte, 1);
  if (layoutByte == static_cast<uint8_t>(RunLayout::kSeparate)) {
    // Compressed stream by stream at spill time: the final run.
    // A kSeparate run header: layout byte, stored and decoded size per
    // stream.
    const uint64_t headerBytes = 1 + 16ull * numStreams;
    BOLT_CHECK_GE(segmentBytes, headerBytes, "corrupt cell spill segment");
    std::vector<uint64_t> sizes(2ull * numStreams);
    spill_.read(segment.begin + 1, sizes.data(), 16ull * numStreams);
    uint64_t decodedSum = 0;
    for (uint32_t s = 0; s < numStreams; ++s) {
      const uint64_t decoded = sizes[numStreams + s];
      decodedSum += decoded != 0 ? decoded : sizes[s];
    }
    rawBytesWritten_ += combinedRunHeaderBytes + decodedSum;
    ++copiedSegments_;
    out.copyFrom(spill_, segment.begin, segmentBytes);
    return;
  }
  BOLT_CHECK(
      layoutByte == static_cast<uint8_t>(RunLayout::kCombined) ||
          layoutByte == static_cast<uint8_t>(RunLayout::kCombinedUncompressed),
      "corrupt cell spill segment");
  BOLT_CHECK_GE(
      segmentBytes, combinedRunHeaderBytes, "corrupt cell spill segment");
  std::vector<uint64_t> decodedSizes(numStreams);
  // Past the layout byte and the u64 stored size.
  spill_.read(segment.begin + 9, decodedSizes.data(), 8ull * numStreams);
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes) {
    decodedSum += size;
  }
  rawBytesWritten_ += combinedRunHeaderBytes + decodedSum;
  ++copiedSegments_;
  out.copyFrom(spill_, segment.begin, segmentBytes);
}

void LocalCellOutput::writeResidentPayload(
    DataFileWriter& out,
    const CellWindowInput& in,
    uint32_t pid) {
  const uint64_t total = residentBytes(in, pid);
  const auto segments = partitionSegments(runs_, pid, openFrom_[pid]);
  const uint32_t runCount =
      (total > 0 ? 1 : 0) + static_cast<uint32_t>(segments.size());

  resetBuffer(scratch_, 0, pool_);
  appendBlockKind();
  rawBytesWritten_ +=
      appendPayloadPrefix(in, pid, runCount, compressor_, scratch_, pool_);
  rawBytesWritten_ += scratch_->size();
  recordPayload(out, in.rowCounts[pid], runCount);
  out.write(scratch_->as<char>(), scratch_->size());
  // Mid-window spilled runs first (they hold the older blocks), the
  // resident cells last.
  for (const auto& segment : segments) {
    writeSpilledSegment(out, segment);
  }
  if (total > 0) {
    rawBytesWritten_ += combinedRunHeaderBytes(layout_->numStreams()) + total;
    DataRunSink sink(out);
    writeCells(compressor_, sink, in, pid);
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

std::vector<LocalCellOutput::SpillSegment> LocalCellOutput::partitionSegments(
    const std::vector<SpilledRun>& runs,
    uint32_t pid,
    size_t from) {
  std::vector<SpillSegment> segments;
  for (size_t i = from; i < runs.size(); ++i) {
    const auto& run = runs[i];
    if (run.pidEnds[pid + 1] > run.pidEnds[pid]) {
      segments.push_back({run.pidEnds[pid], run.pidEnds[pid + 1]});
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
  // "N runs [a, b, ...]", the first runs' spill bytes listed.
  constexpr size_t kMaxListedRuns = 16;
  uint64_t runBytes = 0;
  os << runs_.size() << " runs [";
  for (size_t i = 0; i < runs_.size(); ++i) {
    const uint64_t bytes =
        runs_[i].pidEnds[in.numPartitions] - runs_[i].pidEnds[0];
    runBytes += bytes;
    if (i < kMaxListedRuns) {
      os << (i > 0 ? ", " : "") << mb(bytes);
    } else if (i == kMaxListedRuns) {
      os << ", +" << (runs_.size() - kMaxListedRuns) << " more";
    }
  }
  os << "]";
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
  resident_ = &in;
  residentHasData_ = windowHasData;
  rawPartitionLengths_.assign(in.numPartitions, 0);
  {
    SCOPE_EXIT {
      resident_ = nullptr;
    };
    // Per partition: the Cell payloads, then the composite blocks.
    mergeToDataFile(options_, {this, &composite_}, metrics);
  }
  compressor_.releaseWorkspace();
  scratch_.reset();
  if (verifyDataFile_) {
    verifyDataFile(metrics);
  }
  metrics.rawPartitionLengths = rawPartitionLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
  LOG(INFO) << "CellShuffleWriter merge: segments copied=" << copiedSegments_;
  logCompression(compressor_, *layout_);
}

void LocalCellOutput::verifyDataFile(const ShuffleWriterMetrics& metrics) {
  const uint64_t start = currentTimeNs();
  const auto summary = verifyCellDataFile(
      options_.dataFile,
      metrics.partitionLengths,
      layout_->numWireColumns(),
      layout_->numStreams(),
      &writtenPayloads_);
  const uint64_t elapsedNs = currentTimeNs() - start;
  writtenPayloads_ = {};
  LOG(INFO) << fmt::format(
      "CellShuffleWriter data file verified: {} partitions, {} payloads, {} "
      "runs{} in {:.3f} ms",
      summary.partitions,
      summary.payloads,
      summary.runs,
      summary.compositeBlocks > 0
          ? fmt::format(", {} composite blocks", summary.compositeBlocks)
          : "",
      elapsedNs / 1e6);
}

void LocalCellOutput::writePartitionShare(uint32_t pid, DataFileWriter& out) {
  BOLT_CHECK_NOT_NULL(resident_, "Cell payloads merged outside finalize()");
  const uint64_t rawStart = rawBytesWritten_;
  for (const auto& payload : sealed_[pid]) {
    writeSealedPayload(out, payload, resident_->encodingTags);
  }
  if (residentHasData_ && resident_->rowCounts[pid] > 0) {
    writeResidentPayload(out, *resident_, pid);
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

void RssCellOutput::spillRun(const CellWindowInput& /*in*/) {
  BOLT_FAIL("RssCellOutput never spills: windows are sent whole");
}

void RssCellOutput::sealWindow(const CellWindowInput& in) {
  sendWindow(in);
  ++numSeals_;
}

void RssCellOutput::sendWindow(const CellWindowInput& in) {
  uint32_t built = std::numeric_limits<uint32_t>::max();
  blocks_.writeBlocks([&](uint32_t pid, BufferPtr& block) {
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
    BufferRunSink sink(payload, pool_);
    writeCells(compressor_, sink, in, pid);
    raw += combinedRunHeaderBytes(numStreams) + total;
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
  LOG(INFO) << "CellShuffleWriter rss: seals=" << numSeals_;
  logCompression(compressor_, *layout_);
  blocks_.finish(metrics);
  metrics.rawPartitionLengths = rawPartitionLengths_;
  metrics.totalCompressTime = static_cast<int64_t>(compressTimeNs_);
  metrics.spillCount = static_cast<int64_t>(numSeals_);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
