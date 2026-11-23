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

#include "bolt/shuffle/sparksql/base/PartitionBlockOutput.h"

#include <unistd.h>
#include <algorithm>
#include <cstring>
#include <random>

#include <folly/ScopeGuard.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/Utils.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {

/// Large enough that a block is one pool allocation per few runs, small
/// enough that a file's last, partly filled block wastes little.
constexpr uint64_t kMaxSpillBlockBytes = 4 << 20;

/// The largest power of two up to kMaxSpillBlockBytes that fits four times
/// within `capBytes`, so that what the memory keeps moves in steps of a
/// quarter of it at most; 0 when not even a machine page does.
uint64_t spillBlockBytes(int64_t capBytes) {
  const int64_t quarter = capBytes / 4;
  if (quarter < static_cast<int64_t>(memory::AllocationTraits::kPageSize)) {
    return 0;
  }
  return std::min<uint64_t>(
      kMaxSpillBlockBytes, folly::prevPowTwo(static_cast<uint64_t>(quarter)));
}

} // namespace

ShuffleSpillFile::WriteGuard::WriteGuard(
    ShuffleSpillFile& file,
    const void* owner)
    : file_(file), owner_(owner) {
  BOLT_CHECK_NOT_NULL(owner_, "spill write owner must not be null");
  BOLT_CHECK(!file_.frozen_, "shuffle spill file written after it was frozen");
  const void* expected = nullptr;
  BOLT_CHECK(
      file_.activeWriter_.compare_exchange_strong(
          expected, owner_, std::memory_order_acq_rel),
      "concurrent or nested write to shared ShuffleSpillFile");
}

ShuffleSpillFile::WriteGuard::~WriteGuard() {
  const void* expected = owner_;
  const bool released = file_.activeWriter_.compare_exchange_strong(
      expected, nullptr, std::memory_order_acq_rel);
  BOLT_DCHECK(released, "ShuffleSpillFile write guard released out of order");
}

ShuffleSpillFile::ShuffleSpillFile(
    PartitionWriterOptions options,
    std::string name,
    memory::MemoryPool* pool,
    int64_t memoryCapBytes)
    : options_(std::move(options)),
      name_(std::move(name)),
      pool_(pool),
      capBytes_(pool_ != nullptr ? std::max<int64_t>(memoryCapBytes, 0) : 0),
      blockBytes_(pool_ != nullptr ? spillBlockBytes(capBytes_) : 0),
      blockShift_(blockBytes_ == 0 ? 0 : folly::findLastSet(blockBytes_) - 1) {}

ShuffleSpillFile::~ShuffleSpillFile() {
  remove();
}

bool ShuffleSpillFile::takeBlock(memory::ContiguousAllocation& block) {
  if (growing_ && heldBytes_ + static_cast<int64_t>(blockBytes_) <= capBytes_) {
    // Reserved first, as a cell chunk is. The reservation may reclaim the
    // writer, which may write blocks of this file out: the last block is full,
    // so they are whole.
    if (pool_->maybeReserve(blockBytes_)) {
      pool_->allocateContiguous(
          memory::AllocationTraits::numPages(blockBytes_), block);
      heldBytes_ += static_cast<int64_t>(blockBytes_);
      peakBytes_ = std::max(peakBytes_, heldBytes_);
      return true;
    }
    growing_ = false;
  }
  if (blocks_.empty()) {
    return false;
  }
  writeOutOldest(block);
  return true;
}

bool ShuffleSpillFile::releaseOne() {
  if (blocks_.empty()) {
    return false;
  }
  BOLT_CHECK(!frozen_, "shuffle spill memory released during the merge");
  memory::ContiguousAllocation block;
  writeOutOldest(block);
  // `block` goes back to the pool as it leaves.
  heldBytes_ -= static_cast<int64_t>(blockBytes_);
  return true;
}

int64_t ShuffleSpillFile::release(int64_t bytes) {
  growing_ = false;
  int64_t freed = 0;
  while (freed < bytes && releaseOne()) {
    freed += static_cast<int64_t>(blockBytes_);
  }
  return freed;
}

int64_t ShuffleSpillFile::releaseAll() {
  growing_ = false;
  int64_t freed = 0;
  while (releaseOne()) {
    freed += static_cast<int64_t>(blockBytes_);
  }
  return freed;
}

void ShuffleSpillFile::recordSpillRun(uint64_t begin) {
  if (end_ > begin) {
    BOLT_CHECK(
        spillStarts_.empty() || begin >= spillStarts_.back(),
        "overlapping spill runs in ShuffleSpillFile");
    spillStarts_.push_back(begin);
  }
}

int64_t ShuffleSpillFile::spilledRuns() const {
  return static_cast<int64_t>(
      std::lower_bound(spillStarts_.begin(), spillStarts_.end(), diskEnd_) -
      spillStarts_.begin());
}

void ShuffleSpillFile::freeze() {
  BOLT_CHECK(
      activeWriter_.load(std::memory_order_acquire) == nullptr,
      "shuffle spill file frozen while a write is in progress");
  frozen_ = true;
}

void ShuffleSpillFile::openFile() {
  if (!options_.configuredDirs.empty()) {
    // In a random directory as the local partition writer does, so
    // concurrent tasks spread their spill I/O over the disks.
    const auto& dirs = options_.configuredDirs;
    std::default_random_engine engine(std::random_device{}());
    const auto dir = getSpilledShuffleFileDir(
        dirs[std::uniform_int_distribution<size_t>(0, dirs.size() - 1)(engine)],
        std::uniform_int_distribution<int32_t>(
            0, std::max<int32_t>(options_.numSubDirs, 1) - 1)(engine));
    auto maybePath = createTempShuffleFile(dir);
    BOLT_CHECK(
        maybePath.ok(),
        "Failed to create shuffle spill file: {}",
        maybePath.status().ToString());
    path_ = *maybePath;
  } else {
    // Test convenience: sit next to the data file.
    path_ = options_.dataFile + "." + name_ + ".spill";
  }
  file_ = ::fopen(path_.c_str(), "wb+");
  BOLT_CHECK_NOT_NULL(file_, "Failed to open {}", path_);
  fd_ = ::fileno(file_);
}

uint64_t ShuffleSpillFile::append(const void* data, size_t bytes) {
  BOLT_CHECK(!frozen_, "shuffle spill file appended after it was frozen");
  BOLT_CHECK(!appending_, "reentrant append to ShuffleSpillFile");
  appending_ = true;
  SCOPE_EXIT {
    appending_ = false;
  };
  const uint64_t offset = end_;
  if (bytes == 0) {
    return offset;
  }
  if (blockBytes_ == 0) {
    appendToDisk(data, bytes);
    return offset;
  }
  const auto* source = static_cast<const char*>(data);
  while (bytes > 0) {
    // Recomputed after a block is taken: taking one may write blocks of
    // this file out.
    const uint64_t room = blocks_.size() * blockBytes_ - (end_ - diskEnd_);
    if (room == 0) {
      // Taken with the last block full only, so that what a reclaim reached
      // from the reservation writes out of this file is whole.
      memory::ContiguousAllocation block;
      if (!takeBlock(block)) {
        // No block held: none of this file's bytes is in memory either.
        appendToDisk(source, bytes);
        break;
      }
      blocks_.push_back(std::move(block));
      continue;
    }
    const size_t piece = std::min<uint64_t>(room, bytes);
    ::memcpy(blocks_.back().data<char>() + (blockBytes_ - room), source, piece);
    end_ += piece;
    source += piece;
    bytes -= piece;
  }
  return offset;
}

void ShuffleSpillFile::appendToDisk(const void* data, size_t bytes) {
  // The bytes in memory always end the file: nothing goes to disk behind
  // them.
  BOLT_CHECK_EQ(
      diskEnd_, end_, "shuffle spill file written to disk behind memory");
  if (file_ == nullptr) {
    openFile();
  }
  // The I/O calls stay out of the check macros: those evaluate an operand
  // again to format the failure message.
  size_t written;
  {
    NanosecondTimer timer(&writeTimeNs_);
    written = ::fwrite(data, 1, bytes, file_);
  }
  BOLT_CHECK_EQ(written, bytes, "shuffle spill write failed");
  end_ += bytes;
  diskEnd_ = end_;
  if (pool_ != nullptr) {
    directBytes_ += bytes;
  }
}

void ShuffleSpillFile::writeOutOldest(memory::ContiguousAllocation& block) {
  const uint64_t bytes = std::min<uint64_t>(blockBytes_, end_ - diskEnd_);
  if (bytes > 0) {
    if (file_ == nullptr) {
      openFile();
    }
    size_t written;
    int status;
    {
      NanosecondTimer timer(&writeTimeNs_);
      written = ::fwrite(blocks_.front().data<char>(), 1, bytes, file_);
      // Readable at once, as they were in memory.
      status = ::fflush(file_);
    }
    BOLT_CHECK_EQ(written, bytes, "shuffle spill write failed");
    BOLT_CHECK_EQ(status, 0, "shuffle spill flush failed");
    diskEnd_ += bytes;
    flushedBytes_ += bytes;
    ++flushes_;
  }
  block = std::move(blocks_.front());
  blocks_.pop_front();
}

template <typename Visit>
void ShuffleSpillFile::visitMemory(
    uint64_t offset,
    uint64_t bytes,
    Visit&& visit) const {
  if (bytes == 0) {
    return;
  }
  BOLT_CHECK(
      offset >= diskEnd_ && offset <= end_ && bytes <= end_ - offset &&
          !blocks_.empty(),
      "shuffle spill memory read out of range");
  uint64_t at = offset - diskEnd_;
  while (bytes > 0) {
    const uint64_t inBlock = at & (blockBytes_ - 1);
    const uint64_t piece = std::min(bytes, blockBytes_ - inBlock);
    visit(blocks_[at >> blockShift_].data<char>() + inBlock, piece);
    at += piece;
    bytes -= piece;
  }
}

void ShuffleSpillFile::flush() {
  if (file_ == nullptr) {
    return;
  }
  int status;
  {
    NanosecondTimer timer(&writeTimeNs_);
    status = ::fflush(file_);
    for (const auto& [offset, bytes] : pendingPatches_) {
      const ssize_t written =
          ::pwrite(fd_, bytes.data(), bytes.size(), static_cast<off_t>(offset));
      BOLT_CHECK_EQ(
          written,
          static_cast<ssize_t>(bytes.size()),
          "shuffle spill patch failed");
    }
  }
  pendingPatches_.clear();
  BOLT_CHECK_EQ(status, 0, "shuffle spill flush failed");
}

void ShuffleSpillFile::patch(uint64_t offset, const void* data, size_t bytes) {
  BOLT_CHECK(!frozen_, "shuffle spill file patched after it was frozen");
  BOLT_CHECK(
      offset <= end_ && bytes <= end_ - offset,
      "shuffle spill patch beyond its end");
  if (bytes == 0) {
    return;
  }
  const auto* source = static_cast<const char*>(data);
  if (offset < diskEnd_) {
    const size_t onDisk = std::min<uint64_t>(bytes, diskEnd_ - offset);
    pendingPatches_.emplace_back(offset, std::string(source, onDisk));
    offset += onDisk;
    source += onDisk;
    bytes -= onDisk;
  }
  visitMemory(offset, bytes, [&](char* to, size_t piece) {
    ::memcpy(to, source, piece);
    source += piece;
  });
}

void ShuffleSpillFile::read(uint64_t offset, void* out, size_t bytes) const {
  BOLT_CHECK(
      offset <= end_ && bytes <= end_ - offset,
      "shuffle spill read beyond what was written");
  if (bytes == 0) {
    return;
  }
  auto* dst = static_cast<char*>(out);
  if (offset < diskEnd_) {
    const size_t onDisk = std::min<uint64_t>(bytes, diskEnd_ - offset);
    readFromDisk(offset, dst, onDisk);
    offset += onDisk;
    dst += onDisk;
    bytes -= onDisk;
  }
  visitMemory(offset, bytes, [&](const char* data, size_t piece) {
    ::memcpy(dst, data, piece);
    dst += piece;
  });
}

void ShuffleSpillFile::copyTo(
    uint64_t offset,
    uint64_t bytes,
    const std::function<void(const char* data, size_t bytes)>& out) const {
  BOLT_CHECK(
      offset <= end_ && bytes <= end_ - offset,
      "shuffle spill read beyond what was written");
  if (bytes == 0) {
    return;
  }
  if (offset < diskEnd_) {
    char buffer[64 << 10];
    uint64_t onDisk = std::min(bytes, diskEnd_ - offset);
    bytes -= onDisk;
    while (onDisk > 0) {
      const size_t piece = std::min<uint64_t>(onDisk, sizeof(buffer));
      readFromDisk(offset, buffer, piece);
      out(buffer, piece);
      offset += piece;
      onDisk -= piece;
    }
  }
  visitMemory(offset, bytes, out);
}

void ShuffleSpillFile::readFromDisk(uint64_t offset, char* out, size_t bytes)
    const {
  BOLT_CHECK(pendingPatches_.empty(), "shuffle spill read before flush()");
  size_t done = 0;
  while (done < bytes) {
    const auto n = ::pread(fd_, out + done, bytes - done, offset + done);
    BOLT_CHECK_GT(n, 0, "shuffle spill read failed");
    done += n;
  }
}

void ShuffleSpillFile::remove() {
  BOLT_CHECK(
      activeWriter_.load(std::memory_order_acquire) == nullptr,
      "shuffle spill file removed while a write is in progress");
  if (!blocks_.empty()) {
    heldBytes_ -= static_cast<int64_t>(blocks_.size() * blockBytes_);
    blocks_.clear();
  }
  if (file_ != nullptr) {
    ::fclose(file_);
    ::unlink(path_.c_str());
    file_ = nullptr;
    fd_ = -1;
  }
}

DataFileWriter::DataFileWriter(const std::string& path, uint32_t numPartitions)
    : file_(::fopen(path.c_str(), "wb")), lengths_(numPartitions, 0) {
  BOLT_CHECK_NOT_NULL(file_, "Failed to open shuffle data file {}", path);
}

DataFileWriter::~DataFileWriter() {
  if (file_ != nullptr) {
    ::fclose(file_);
  }
}

void DataFileWriter::beginPartition(uint32_t pid) {
  BOLT_CHECK_LT(pid, lengths_.size());
  pid_ = pid;
}

void DataFileWriter::write(const void* data, size_t bytes) {
  if (bytes == 0) {
    return;
  }
  size_t written;
  {
    NanosecondTimer timer(&writeTimeNs_);
    written = ::fwrite(data, 1, bytes, file_);
  }
  BOLT_CHECK_EQ(written, bytes, "shuffle data file write failed");
  lengths_[pid_] += static_cast<int64_t>(bytes);
  bytes_ += static_cast<int64_t>(bytes);
}

void DataFileWriter::patch(uint64_t position, const void* data, size_t bytes) {
  const auto end = static_cast<uint64_t>(bytes_);
  BOLT_CHECK(
      position <= end && bytes <= end - position,
      "shuffle data file patch beyond its end");
  if (bytes == 0) {
    return;
  }
  pendingPatches_.emplace_back(
      position, std::string(static_cast<const char*>(data), bytes));
}

void DataFileWriter::copyFrom(
    const ShuffleSpillFile& spill,
    uint64_t begin,
    uint64_t bytes) {
  spill.copyTo(begin, bytes, [this](const char* data, size_t piece) {
    write(data, piece);
  });
}

void DataFileWriter::close(ShuffleWriterMetrics& metrics) {
  int status;
  {
    NanosecondTimer timer(&writeTimeNs_);
    status = ::fflush(file_);
    for (const auto& [position, bytes] : pendingPatches_) {
      const ssize_t written = ::pwrite(
          ::fileno(file_),
          bytes.data(),
          bytes.size(),
          static_cast<off_t>(position));
      BOLT_CHECK_EQ(
          written,
          static_cast<ssize_t>(bytes.size()),
          "shuffle data file patch failed");
    }
  }
  pendingPatches_.clear();
  BOLT_CHECK_EQ(status, 0, "shuffle data file flush failed");
  std::FILE* file = file_;
  file_ = nullptr;
  status = ::fclose(file);
  BOLT_CHECK_EQ(status, 0, "shuffle data file close failed");
  metrics.partitionLengths = lengths_;
  metrics.totalBytesWritten = bytes_;
  metrics.totalWriteTime = static_cast<int64_t>(writeTimeNs_);
}

void PartialSpiller::mergeToDataFile(
    const PartitionWriterOptions& options,
    ShuffleSpillFile& spill,
    std::initializer_list<PartialSpiller*> spillers,
    ShuffleWriterMetrics& metrics) {
  BOLT_CHECK_GE(options.numPartitions, 0, "numPartitions must be non-negative");
  BOLT_CHECK(!spill.isFrozen(), "partial spiller merged twice");
  for (auto* spiller : spillers) {
    BOLT_CHECK(
        &spiller->spill_ == &spill,
        "all partial spillers must share the same ShuffleSpillFile");
  }
  // Nothing may be appended behind the merge: it would break the
  // partition-major order or be missed by partitions already written.
  spill.flush();
  spill.freeze();
  const auto numPartitions = static_cast<uint32_t>(options.numPartitions);
  DataFileWriter out(options.dataFile, numPartitions);
  for (uint32_t pid = 0; pid < numPartitions; ++pid) {
    out.beginPartition(pid);
    for (auto* spiller : spillers) {
      spiller->writePartitionShare(pid, out);
    }
  }
  out.close(metrics);
  spill.remove();
  metrics.totalBytesEvicted = spill.bytesWritten();
  metrics.totalSpillFiles = spill.spilledRuns();
  metrics.totalEvictTime = static_cast<int64_t>(spill.writeTimeNs());
}

LocalBlockOutput::LocalBlockOutput(
    PartitionWriterOptions options,
    ShuffleSpillFile& spill)
    : PartialSpiller(spill), options_(std::move(options)) {
  BOLT_CHECK_GE(
      options_.numPartitions, 0, "numPartitions must be non-negative");
}

void LocalBlockOutput::writeBlocks(const BlockSource& next) {
  auto writeGuard = scopedWrite();
  const auto numPartitions = static_cast<uint32_t>(options_.numPartitions);
  std::vector<uint64_t> ends(numPartitions + 1);
  ends[0] = spill_.end();
  BufferPtr block;
  for (uint32_t pid = 0; pid < numPartitions; ++pid) {
    while (next(pid, block)) {
      spill_.append(block->as<char>(), block->size());
    }
    ends[pid + 1] = spill_.end();
  }
  countSpill(ends[0]);
  spill_.flush();
  spillOffsets_.push_back(std::move(ends));
}

void LocalBlockOutput::finish(ShuffleWriterMetrics& metrics) {
  mergeToDataFile(options_, spill_, {this}, metrics);
}

void LocalBlockOutput::writePartitionShare(uint32_t pid, DataFileWriter& out) {
  for (const auto& ends : spillOffsets_) {
    const uint64_t bytes = ends[pid + 1] - ends[pid];
    if (bytes > 0) {
      out.copyFrom(spill_, ends[pid], bytes);
    }
  }
}

RssBlockOutput::RssBlockOutput(PartitionWriterOptions options)
    : options_(std::move(options)),
      client_(options_.rssClient),
      partitionLengths_(std::max(options_.numPartitions, 0), 0) {
  BOLT_CHECK_GE(
      options_.numPartitions, 0, "numPartitions must be non-negative");
  BOLT_CHECK_NOT_NULL(
      client_, "a remote shuffle output requires an RSS client");
}

void RssBlockOutput::writeBlocks(const BlockSource& next) {
  BOLT_CHECK(!finished_, "writeBlocks after finish");
  const auto numPartitions = static_cast<uint32_t>(options_.numPartitions);
  BufferPtr block;
  for (uint32_t pid = 0; pid < numPartitions; ++pid) {
    while (next(pid, block)) {
      int32_t pushed;
      {
        NanosecondTimer timer(&pushTimeNs_);
        pushed = client_->pushPartitionData(
            static_cast<int32_t>(pid),
            block->asMutable<char>(),
            static_cast<int64_t>(block->size()));
      }
      BOLT_CHECK_GE(pushed, 0, "RSS pushPartitionData failed");
      partitionLengths_[pid] += pushed;
      pushedBytes_ += pushed;
      ++numPushes_;
    }
  }
}

void RssBlockOutput::finish(ShuffleWriterMetrics& metrics) {
  BOLT_CHECK(!finished_, "finish called twice");
  finished_ = true;
  client_->stop();
  LOG(INFO) << "Shuffle RSS output: pushes=" << numPushes_
            << ", pushed bytes=" << pushedBytes_;
  metrics.partitionLengths = partitionLengths_;
  metrics.totalBytesWritten = pushedBytes_;
  metrics.totalBytesEvicted = pushedBytes_;
  metrics.totalWriteTime = 0;
  metrics.totalEvictTime = static_cast<int64_t>(pushTimeNs_);
}

} // namespace bytedance::bolt::shuffle::sparksql
