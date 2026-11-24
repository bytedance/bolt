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

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <random>

#include <folly/FileUtil.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/Utils.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace detail {

void BufferedFile::open(const std::string& path, int flags, mode_t mode) {
  BOLT_CHECK_LT(fd_, 0, "BufferedFile is already open");
  fd_ = ::open(path.c_str(), flags, mode);
  BOLT_CHECK_GE(fd_, 0, "Failed to open {}: {}", path, folly::errnoStr(errno));
  flushedEnd_ = 0;
  end_ = 0;
  buffer_.clear();
}

void BufferedFile::writeAllFd(
    const void* data,
    size_t bytes,
    uint64_t& writeTimeNs) {
  ssize_t written;
  {
    NanosecondTimer timer(&writeTimeNs);
    written = folly::writeFull(fd_, data, bytes);
  }
  BOLT_CHECK_EQ(
      written,
      static_cast<ssize_t>(bytes),
      "shuffle file write failed: {}",
      written < 0 ? folly::errnoStr(errno) : "short write");
}

void BufferedFile::write(
    const void* data,
    size_t bytes,
    uint64_t& writeTimeNs) {
  if (bytes == 0) {
    return;
  }
  if (bytes >= kWriteBufferBytes) {
    flush(writeTimeNs);
    writeAllFd(data, bytes, writeTimeNs);
    flushedEnd_ += bytes;
    end_ = flushedEnd_;
    return;
  }
  if (buffer_.size() + bytes > kWriteBufferBytes) {
    flush(writeTimeNs);
  }
  if (buffer_.capacity() < kWriteBufferBytes) {
    buffer_.reserve(kWriteBufferBytes);
  }
  const auto* src = static_cast<const char*>(data);
  buffer_.insert(buffer_.end(), src, src + bytes);
  end_ += bytes;
}

uint64_t BufferedFile::reserve(size_t bytes, uint64_t& writeTimeNs) {
  const uint64_t offset = end_;
  if (bytes == 0) {
    return offset;
  }
  if (bytes < kWriteBufferBytes) {
    if (buffer_.size() + bytes > kWriteBufferBytes) {
      flush(writeTimeNs);
    }
    if (buffer_.capacity() < kWriteBufferBytes) {
      buffer_.reserve(kWriteBufferBytes);
    }
    buffer_.resize(buffer_.size() + bytes, '\0');
    end_ += bytes;
    return offset;
  }
  static constexpr char kZeros[4096]{};
  while (bytes > 0) {
    const size_t piece = std::min(bytes, sizeof(kZeros));
    write(kZeros, piece, writeTimeNs);
    bytes -= piece;
  }
  return offset;
}

void BufferedFile::patch(
    uint64_t offset,
    const void* data,
    size_t bytes,
    uint64_t& writeTimeNs) {
  BOLT_CHECK(
      offset <= end_ && bytes <= end_ - offset,
      "shuffle file patch beyond its end");
  if (bytes == 0) {
    return;
  }
  const auto* src = static_cast<const char*>(data);
  if (offset < flushedEnd_) {
    const size_t onFd = std::min<uint64_t>(bytes, flushedEnd_ - offset);
    ssize_t written;
    {
      NanosecondTimer timer(&writeTimeNs);
      written = folly::pwriteFull(fd_, src, onFd, static_cast<off_t>(offset));
    }
    BOLT_CHECK_EQ(
        written,
        static_cast<ssize_t>(onFd),
        "shuffle file patch failed: {}",
        written < 0 ? folly::errnoStr(errno) : "short write");
    offset += onFd;
    src += onFd;
    bytes -= onFd;
  }
  if (bytes > 0) {
    ::memcpy(buffer_.data() + (offset - flushedEnd_), src, bytes);
  }
}

void BufferedFile::flush(uint64_t& writeTimeNs) {
  if (buffer_.empty()) {
    return;
  }
  writeAllFd(buffer_.data(), buffer_.size(), writeTimeNs);
  flushedEnd_ += buffer_.size();
  buffer_.clear();
}

void BufferedFile::read(uint64_t offset, void* out, size_t bytes) const {
  BOLT_CHECK(
      offset <= end_ && bytes <= end_ - offset,
      "shuffle file read beyond what was written");
  if (bytes == 0) {
    return;
  }
  auto* dst = static_cast<char*>(out);
  if (offset < flushedEnd_) {
    const size_t onFd = std::min<uint64_t>(bytes, flushedEnd_ - offset);
    const ssize_t n =
        folly::preadFull(fd_, dst, onFd, static_cast<off_t>(offset));
    BOLT_CHECK_EQ(
        n,
        static_cast<ssize_t>(onFd),
        "shuffle file read failed: {}",
        n < 0 ? folly::errnoStr(errno) : "unexpected EOF");
    offset += onFd;
    dst += onFd;
    bytes -= onFd;
  }
  if (bytes > 0) {
    ::memcpy(dst, buffer_.data() + (offset - flushedEnd_), bytes);
  }
}

void BufferedFile::copyFrom(
    const BufferedFile& src,
    uint64_t srcOffset,
    uint64_t bytes,
    uint64_t& writeTimeNs) {
  BOLT_CHECK(&src != this, "BufferedFile::copyFrom self-copy is not supported");
  BOLT_CHECK(
      srcOffset <= src.end_ && bytes <= src.end_ - srcOffset,
      "BufferedFile::copyFrom out of range");
  if (bytes == 0) {
    return;
  }
  if (srcOffset < src.flushedEnd_) {
    uint64_t onFd = std::min(bytes, src.flushedEnd_ - srcOffset);
    bytes -= onFd;
#ifdef __linux__
    if (onFd >= kCopyFileRangeMinBytes) {
      flush(writeTimeNs);
      while (onFd > 0) {
        loff_t inOff = static_cast<loff_t>(srcOffset);
        ssize_t copied;
        {
          NanosecondTimer timer(&writeTimeNs);
          copied = ::copy_file_range(
              src.fd_, &inOff, fd_, nullptr, static_cast<size_t>(onFd), 0);
        }
        if (copied < 0) {
          if (errno == EINTR) {
            continue;
          }
          if (errno == EXDEV || errno == EINVAL || errno == ENOSYS ||
              errno == EOPNOTSUPP) {
            break;
          }
          BOLT_FAIL(
              "shuffle copy_file_range failed: {}", folly::errnoStr(errno));
        }
        BOLT_CHECK_GT(copied, 0, "shuffle copy_file_range hit unexpected EOF");
        srcOffset += static_cast<uint64_t>(copied);
        onFd -= static_cast<uint64_t>(copied);
        flushedEnd_ += static_cast<uint64_t>(copied);
        end_ = flushedEnd_;
      }
    }
#endif
    if (onFd > 0) {
      if (buffer_.capacity() < kWriteBufferBytes) {
        buffer_.reserve(kWriteBufferBytes);
      }
      while (onFd > 0) {
        if (buffer_.size() == kWriteBufferBytes) {
          flush(writeTimeNs);
        }
        const size_t space = kWriteBufferBytes - buffer_.size();
        const size_t piece =
            static_cast<size_t>(std::min<uint64_t>(onFd, space));
        const size_t prevSize = buffer_.size();
        buffer_.resize(prevSize + piece);
        const ssize_t n = folly::preadFull(
            src.fd_,
            buffer_.data() + prevSize,
            piece,
            static_cast<off_t>(srcOffset));
        BOLT_CHECK_EQ(
            n,
            static_cast<ssize_t>(piece),
            "shuffle spill read failed: {}",
            n < 0 ? folly::errnoStr(errno) : "unexpected EOF");
        srcOffset += piece;
        onFd -= piece;
        end_ += piece;
      }
    }
  }
  if (bytes > 0) {
    write(
        src.buffer_.data() + (srcOffset - src.flushedEnd_),
        static_cast<size_t>(bytes),
        writeTimeNs);
  }
}

void BufferedFile::close(uint64_t& writeTimeNs) {
  if (fd_ < 0) {
    return;
  }
  flush(writeTimeNs);
  const int fd = fd_;
  fd_ = -1;
  int status;
  {
    NanosecondTimer timer(&writeTimeNs);
    status = folly::closeNoInt(fd);
  }
  BOLT_CHECK_EQ(
      status, 0, "shuffle file close failed: {}", folly::errnoStr(errno));
}

void BufferedFile::closeNoThrow() {
  if (fd_ >= 0) {
    const int fd = fd_;
    fd_ = -1;
    folly::closeNoInt(fd);
  }
  buffer_.clear();
}

} // namespace detail

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
  file_.open(path_, O_RDWR | O_CREAT | O_TRUNC);
  ++numFiles_;
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

uint64_t ShuffleSpillFile::reserve(size_t bytes) {
  BOLT_CHECK(!frozen_, "shuffle spill file reserved after it was frozen");
  const uint64_t offset = end_;
  static constexpr char kZeros[4096]{};
  while (bytes > 0) {
    const size_t piece = std::min(bytes, sizeof(kZeros));
    append(kZeros, piece);
    bytes -= piece;
  }
  return offset;
}

void ShuffleSpillFile::appendToDisk(const void* data, size_t bytes) {
  // The bytes in memory always end the file: nothing goes to disk behind
  // them.
  BOLT_CHECK_EQ(
      diskEnd_, end_, "shuffle spill file written to disk behind memory");
  if (!file_.isOpen()) {
    openFile();
  }
  file_.write(data, bytes, writeTimeNs_);
  end_ += bytes;
  diskEnd_ = end_;
  if (pool_ != nullptr) {
    directBytes_ += bytes;
  }
}

void ShuffleSpillFile::writeOutOldest(memory::ContiguousAllocation& block) {
  const uint64_t bytes = std::min<uint64_t>(blockBytes_, end_ - diskEnd_);
  if (bytes > 0) {
    if (!file_.isOpen()) {
      openFile();
    }
    file_.write(blocks_.front().data<char>(), bytes, writeTimeNs_);
    // Readable at once on fd_, as they were in memory.
    file_.flush(writeTimeNs_);
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
  if (!file_.isOpen()) {
    return;
  }
  file_.flush(writeTimeNs_);
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
    file_.patch(offset, source, onDisk, writeTimeNs_);
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
  file_.read(offset, out, bytes);
}

void ShuffleSpillFile::remove() {
  BOLT_CHECK(
      activeWriter_.load(std::memory_order_acquire) == nullptr,
      "shuffle spill file removed while a write is in progress");
  if (!blocks_.empty()) {
    heldBytes_ -= static_cast<int64_t>(blocks_.size() * blockBytes_);
    blocks_.clear();
  }
  if (file_.isOpen()) {
    file_.closeNoThrow();
    ::unlink(path_.c_str());
  }
}

DataFileWriter::DataFileWriter(const std::string& path, uint32_t numPartitions)
    : lengths_(numPartitions, 0) {
  file_.open(path, O_WRONLY | O_CREAT | O_TRUNC);
}

void DataFileWriter::beginPartition(uint32_t pid) {
  BOLT_CHECK_LT(pid, lengths_.size());
  pid_ = pid;
}

void DataFileWriter::write(const void* data, size_t bytes) {
  if (bytes == 0) {
    return;
  }
  file_.write(data, bytes, writeTimeNs_);
  lengths_[pid_] += static_cast<int64_t>(bytes);
}

uint64_t DataFileWriter::reserve(size_t bytes) {
  const uint64_t offset = file_.reserve(bytes, writeTimeNs_);
  lengths_[pid_] += static_cast<int64_t>(bytes);
  return offset;
}

void DataFileWriter::patch(uint64_t position, const void* data, size_t bytes) {
  file_.patch(position, data, bytes, writeTimeNs_);
}

void DataFileWriter::copyFrom(
    const ShuffleSpillFile& spill,
    uint64_t begin,
    uint64_t bytes) {
  BOLT_CHECK(
      begin <= spill.end_ && bytes <= spill.end_ - begin,
      "shuffle spill read beyond what was written");
  if (bytes == 0) {
    return;
  }
  uint64_t offset = begin;
  if (offset < spill.diskEnd_) {
    const uint64_t onDisk = std::min(bytes, spill.diskEnd_ - offset);
    file_.copyFrom(spill.file_, offset, onDisk, writeTimeNs_);
    lengths_[pid_] += static_cast<int64_t>(onDisk);
    offset += onDisk;
    bytes -= onDisk;
  }
  spill.visitMemory(offset, bytes, [this](const char* data, size_t piece) {
    write(data, piece);
  });
}

void DataFileWriter::close(ShuffleWriterMetrics& metrics) {
  file_.close(writeTimeNs_);
  metrics.partitionLengths = lengths_;
  metrics.totalBytesWritten = static_cast<int64_t>(file_.end());
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
  metrics.totalSpillFiles = spill.numFiles();
  metrics.totalEvictTime = static_cast<int64_t>(spill.writeTimeNs());
}

LocalBlockOutput::LocalBlockOutput(
    PartitionWriterOptions options,
    ShuffleSpillFile& spill)
    : PartialSpiller(spill), options_(std::move(options)) {
  BOLT_CHECK_GE(
      options_.numPartitions, 0, "numPartitions must be non-negative");
}

void LocalBlockOutput::writeBlocks(BlockProducer produce) {
  auto writeGuard = scopedWrite();
  const auto numPartitions = static_cast<uint32_t>(options_.numPartitions);
  std::vector<uint64_t> ends(numPartitions + 1);
  ends[0] = spill_.end();
  for (uint32_t pid = 0; pid < numPartitions; ++pid) {
    produce(pid, [&](char* data, size_t bytes) { spill_.append(data, bytes); });
    ends[pid + 1] = spill_.end();
  }
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

void RssBlockOutput::writeBlocks(BlockProducer produce) {
  BOLT_CHECK(!finished_, "writeBlocks after finish");
  const auto numPartitions = static_cast<uint32_t>(options_.numPartitions);
  for (uint32_t pid = 0; pid < numPartitions; ++pid) {
    produce(pid, [&](char* data, size_t bytes) {
      int32_t pushed;
      {
        NanosecondTimer timer(&pushTimeNs_);
        pushed = client_->pushPartitionData(
            static_cast<int32_t>(pid), data, static_cast<int64_t>(bytes));
      }
      BOLT_CHECK_GE(pushed, 0, "RSS pushPartitionData failed");
      partitionLengths_[pid] += pushed;
      pushedBytes_ += pushed;
      ++numPushes_;
    });
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
