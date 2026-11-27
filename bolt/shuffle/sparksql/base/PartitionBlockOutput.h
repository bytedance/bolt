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

#pragma once

#include <sys/types.h>
#include <atomic>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <folly/Function.h>
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace detail {

/// Buffered POSIX file descriptor supporting sequential writes, in-buffer
/// patching, pread, and zero-copy fd-to-fd transfers without stdio buffering
/// conflicts.
class BufferedFile {
 public:
  BufferedFile() = default;
  ~BufferedFile() {
    closeNoThrow();
  }

  BufferedFile(const BufferedFile&) = delete;
  BufferedFile& operator=(const BufferedFile&) = delete;

  void open(const std::string& path, int flags, mode_t mode = 0644);

  bool isOpen() const {
    return fd_ >= 0;
  }

  uint64_t end() const {
    return end_;
  }

  /// Appends `bytes` sequentially, buffering writes smaller than the write
  /// buffer and bypassing it for larger writes.
  void write(const void* data, size_t bytes, uint64_t& writeTimeNs);

  /// Advances the file by `bytes` zero-initialized placeholder bytes and
  /// returns the starting offset.
  uint64_t reserve(size_t bytes, uint64_t& writeTimeNs);

  /// Overwrites `bytes` at `offset` (within [0, end())). Bytes still in the
  /// user-space buffer are patched in place; bytes already written to `fd_`
  /// are patched directly via pwrite().
  void
  patch(uint64_t offset, const void* data, size_t bytes, uint64_t& writeTimeNs);

  /// Flushes the user-space buffer to `fd_`.
  void flush(uint64_t& writeTimeNs);

  /// Reads `bytes` starting at `offset` from `fd_` and/or the unflushed buffer.
  void read(uint64_t offset, void* out, size_t bytes) const;

  /// Copies `bytes` starting at `srcOffset` from `src` into this file.
  /// Flushed bytes use Linux copy_file_range() for large copies (falling back
  /// on cross-filesystem/unsupported errors) or pread directly into the write
  /// buffer; unflushed bytes in `src`'s buffer are written directly without a
  /// temporary copy.
  void copyFrom(
      const BufferedFile& src,
      uint64_t srcOffset,
      uint64_t bytes,
      uint64_t& writeTimeNs);

  /// Flushes and closes `fd_`.
  void close(uint64_t& writeTimeNs);

  /// Closes `fd_` without throwing (for destructors / remove()).
  void closeNoThrow();

 private:
  static constexpr size_t kWriteBufferBytes = 512 << 10;
  static constexpr uint64_t kCopyFileRangeMinBytes = 256 << 10;

  void writeAllFd(const void* data, size_t bytes, uint64_t& writeTimeNs);

  int fd_{-1};
  std::vector<char> buffer_;
  uint64_t flushedEnd_{0};
  uint64_t end_{0};
};

} // namespace detail

class DataFileWriter;

/// Temporary shuffle storage shared by a task's local spillers: appended in
/// time order, read back at any offset. On local disk, the file is created on
/// the first byte it gets, in a random configured directory so concurrent tasks
/// spread their I/O over the disks, and removed by remove() or on destruction.
///
/// When configured with a memory pool and cap (`memoryCapBytes`), it keeps its
/// newest bytes in fixed-size blocks from `pool` instead of on disk: a spill
/// that the merge copies into the data file anyway then costs no disk write and
/// read. Offsets stay continuous: [0, diskEnd) on disk and [diskEnd, end()) in
/// memory, packed oldest first. While still growing within `capBytes()`, it
/// reserves new blocks from `pool`; otherwise it reuses its oldest block after
/// writing it to disk, or appends directly to disk when no block is held.
/// Memory pressure (`release()` / `releaseAll()`) or a failed reservation ends
/// growth for good.
///
/// A new block's reservation may reclaim the writer, and that reclaim may
/// release() blocks of this file. Blocks are only taken when the last block is
/// full, so blocks written out are whole and the file stays consistent. During
/// the merge nothing may be released. Producers sharing one file must serialize
/// writes through scopedWrite().
class ShuffleSpillFile {
 public:
  /// RAII exclusive-write guard preventing concurrent or nested writes into a
  /// shared ShuffleSpillFile (e.g. between ordinary and composite paths).
  class WriteGuard {
   public:
    WriteGuard(ShuffleSpillFile& file, const void* owner);
    ~WriteGuard();

    WriteGuard(const WriteGuard&) = delete;
    WriteGuard& operator=(const WriteGuard&) = delete;

   private:
    ShuffleSpillFile& file_;
    const void* owner_;
  };

  /// `name` tells a task's spill files apart when, with no configured
  /// directory, they sit next to the data file: <dataFile>.<name>.spill.
  /// When `pool` is non-null, keeps blocks of up to 4 MiB (at least four within
  /// `memoryCapBytes`; a cap below four machine pages keeps nothing in memory).
  /// `pool`, if not null, must outlive this file.
  ShuffleSpillFile(
      PartitionWriterOptions options,
      std::string name,
      memory::MemoryPool* pool = nullptr,
      int64_t memoryCapBytes = 0);
  ~ShuffleSpillFile();

  ShuffleSpillFile(const ShuffleSpillFile&) = delete;
  ShuffleSpillFile& operator=(const ShuffleSpillFile&) = delete;

  /// Acquires exclusive write access for `owner` until the returned guard is
  /// destroyed.
  WriteGuard scopedWrite(const void* owner) {
    return WriteGuard(*this, owner);
  }

  /// Appends `bytes` and returns the offset they start at.
  uint64_t append(const void* data, size_t bytes);

  /// Advances the spill file by `bytes` zero-initialized placeholder bytes
  /// (e.g. a header patched after writing its body) and returns the starting
  /// offset.
  uint64_t reserve(size_t bytes);

  /// Makes every append visible to read(), which bypasses stdio buffering.
  void flush();

  /// Overwrites `bytes` already appended at `offset`, e.g. a header whose
  /// sizes are known only once its body is written. In memory at once; on
  /// disk at the next flush(), so a patch per run does not break up the
  /// buffered writes.
  void patch(uint64_t offset, const void* data, size_t bytes);

  void read(uint64_t offset, void* out, size_t bytes) const;

  /// Hands the `bytes` from `offset` to `out` in order, in pieces: those on
  /// disk through a 64 KiB buffer, those in memory straight from the blocks.
  void copyTo(
      uint64_t offset,
      uint64_t bytes,
      const std::function<void(const char* data, size_t bytes)>& out) const;

  /// Number of physical spill files created on disk.
  int64_t numFiles() const {
    return numFiles_;
  }

  /// The offset the next append starts at.
  uint64_t end() const {
    return end_;
  }

  /// No append is accepted afterwards; reads still are.
  void freeze();

  bool isFrozen() const {
    return frozen_;
  }

  /// Closes and deletes the file, and frees its memory blocks.
  void remove();

  /// Memory pressure: writes the oldest blocks out and frees them until at
  /// least `bytes` are freed - none for 0 or less - and ends growth. Returns
  /// the bytes freed.
  int64_t release(int64_t bytes);

  /// release() of every held block.
  int64_t releaseAll();

  /// The most block bytes this file may hold in memory.
  int64_t capBytes() const {
    return capBytes_;
  }

  /// Block bytes held in memory now, and the most held.
  int64_t heldBytes() const {
    return heldBytes_;
  }
  int64_t peakBytes() const {
    return peakBytes_;
  }

  /// Bytes written out of memory blocks to disk, and how many blocks.
  uint64_t flushedBytes() const {
    return flushedBytes_;
  }
  uint64_t flushes() const {
    return flushes_;
  }

  /// Bytes appended to disk directly, for want of a memory block.
  uint64_t directBytes() const {
    return directBytes_;
  }

  /// Bytes written to disk: the evict metric.
  int64_t bytesWritten() const {
    return static_cast<int64_t>(diskEnd_);
  }

  /// Disk write and flush time: the evict metric.
  uint64_t writeTimeNs() const {
    return writeTimeNs_;
  }

 private:
  friend class DataFileWriter;

  void openFile();

  /// Takes a block when the last block is full: a new one from `pool_`, or the
  /// oldest held block written out first. False when none is available.
  bool takeBlock(memory::ContiguousAllocation& block);

  /// Writes the oldest held block to disk and frees it; false when none is
  /// held.
  bool releaseOne();

  /// Appends to disk, behind every byte: none may be in memory.
  void appendToDisk(const void* data, size_t bytes);

  /// Writes the oldest block's bytes out to disk, readable at once, and
  /// moves the block into `block`.
  void writeOutOldest(memory::ContiguousAllocation& block);

  void readFromDisk(uint64_t offset, char* out, size_t bytes) const;

  /// Calls `visit(char* data, size_t bytes)` on the pieces of [offset,
  /// offset + bytes), which lies in memory, in order.
  template <typename Visit>
  void visitMemory(uint64_t offset, uint64_t bytes, Visit&& visit) const;

  const PartitionWriterOptions options_;
  const std::string name_;
  memory::MemoryPool* const pool_;
  const int64_t capBytes_;
  /// A power of two; 0 keeps nothing in memory.
  const uint64_t blockBytes_;
  const uint32_t blockShift_;
  /// False once memory ran short: the blocks held are reused from then on.
  bool growing_{true};
  int64_t heldBytes_{0};
  int64_t peakBytes_{0};
  uint64_t flushedBytes_{0};
  uint64_t flushes_{0};
  uint64_t directBytes_{0};

  detail::BufferedFile file_;
  std::string path_;
  uint64_t end_{0};
  /// Where the bytes in memory start: everything before is on disk.
  uint64_t diskEnd_{0};
  /// The bytes from diskEnd_ on, oldest first: all blocks but the last
  /// are full.
  std::deque<memory::ContiguousAllocation> blocks_;
  bool frozen_{false};
  bool appending_{false};
  std::atomic<const void*> activeWriter_{nullptr};
  uint64_t writeTimeNs_{0};
  int64_t numFiles_{0};
};

/// The task's final data file, written once, partition by partition.
/// Closed on destruction when a write failed part-way.
class DataFileWriter {
 public:
  DataFileWriter(const std::string& path, uint32_t numPartitions);
  ~DataFileWriter() = default;

  /// Subsequent writes count towards partition `pid`.
  void beginPartition(uint32_t pid);

  void write(const void* data, size_t bytes);

  /// Advances the data file by `bytes` zero-initialized placeholder bytes and
  /// returns the starting offset.
  uint64_t reserve(size_t bytes);

  /// Bytes written so far: where the next write lands.
  uint64_t position() const {
    return file_.end();
  }

  /// Overwrites `bytes` already written at `position`. Patches still in the
  /// user-space write buffer apply in place without a syscall; only bytes
  /// already flushed to the OS fall back to pwrite().
  void patch(uint64_t position, const void* data, size_t bytes);

  /// Copies `bytes` of `spill` from `begin`: the part on disk via
  /// copy_file_range() or direct-to-buffer pread (no bounce copy), the part in
  /// memory straight from its blocks.
  void copyFrom(const ShuffleSpillFile& spill, uint64_t begin, uint64_t bytes);

  /// Flushes and closes; fills partitionLengths, totalBytesWritten and
  /// totalWriteTime.
  void close(ShuffleWriterMetrics& metrics);

 private:
  detail::BufferedFile file_;
  std::vector<int64_t> lengths_;
  uint32_t pid_{0};
  uint64_t writeTimeNs_{0};
};

/// A local output that spills into a shared ShuffleSpillFile, in pieces that
/// only become final output in the merge: there, partition by partition, it
/// writes its share into the task's data file. Several spillers - a task's
/// ordinary and composite outputs - share one ShuffleSpillFile and one data
/// file through mergeToDataFile().
class PartialSpiller {
 public:
  /// Writes the task's data file from `spillers`, all sharing `spill`: for each
  /// partition, every spiller's share in the order given. Seals `spill` first -
  /// nothing may be appended behind the merge, and a spill file is merged once
  /// only - and removes it afterwards, freeing its memory. Fills
  /// partitionLengths, totalBytesWritten, totalBytesEvicted - the bytes that
  /// reached disk - totalSpillFiles - the number of physical spill files
  /// created - and the write and evict times.
  static void mergeToDataFile(
      const PartitionWriterOptions& options,
      ShuffleSpillFile& spill,
      std::initializer_list<PartialSpiller*> spillers,
      ShuffleWriterMetrics& metrics);

  virtual ~PartialSpiller() = default;

  /// Bytes written to disk in the shared spill file.
  int64_t spilledBytes() const {
    return spill_.bytesWritten();
  }

 protected:
  /// `spill` is shared by the task's local spillers and must outlive this
  /// object.
  explicit PartialSpiller(ShuffleSpillFile& spill) : spill_(spill) {}

  /// Acquires an exclusive write guard on `spill_` so ordinary and composite
  /// paths cannot interleave or concurrently append to the shared spill file.
  ShuffleSpillFile::WriteGuard scopedWrite() {
    return spill_.scopedWrite(this);
  }

  /// Partition `pid`'s share of the data file. Called by mergeToDataFile()
  /// only, for partition 0, 1, ..., with the spill file frozen.
  virtual void writePartitionShare(uint32_t pid, DataFileWriter& out) = 0;

  ShuffleSpillFile& spill_;
};

/// Consumes one opaque, already-framed block for the current partition.
/// The bytes are written or pushed before returning, so the caller may reuse
/// its buffer immediately.
using BlockEmitter = folly::FunctionRef<void(char* data, size_t bytes)>;

/// Emits zero or more opaque blocks for partition `pid` via `emit`.
using BlockProducer = folly::FunctionRef<void(uint32_t pid, BlockEmitter emit)>;

/// The output of a shuffle writer as whole, already-framed blocks per
/// partition: a local spill file merged partition by partition into the
/// data file at the end, or pushes to a remote shuffle service, which does
/// that merge itself. The owner frames and compresses the blocks; this layer
/// only places them.
class PartitionBlockOutput {
 public:
  virtual ~PartitionBlockOutput() = default;

  /// Writes one unit over all partitions: `produce` is called once for each
  /// partition 0, 1, ..., emitting zero or more blocks via `emit`. Every block
  /// is written before `emit` returns, so the producer may reuse its buffer.
  virtual void writeBlocks(BlockProducer produce) = 0;

  /// Ends the output on its own - writes a data file of its blocks alone, or
  /// ends the map attempt - and fills partitionLengths, totalBytesWritten,
  /// totalBytesEvicted and the write and evict times. Once only; nothing may
  /// be written afterwards.
  virtual void finish(ShuffleWriterMetrics& metrics) = 0;

  /// Bytes written to spill storage on disk, or pushed, so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// Local disk. Each writeBlocks() call becomes a block run of P + 1 offsets
/// in the shared spill file; the merge copies each partition's blocks
/// verbatim, in call order. Merged with another output's share by
/// mergeToDataFile(), or on its own by finish().
class LocalBlockOutput final : public PartitionBlockOutput,
                               public PartialSpiller {
 public:
  /// `spill` must outlive this object.
  LocalBlockOutput(PartitionWriterOptions options, ShuffleSpillFile& spill);

  void writeBlocks(BlockProducer produce) override;

  /// mergeToDataFile() of this output alone.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return spilledBytes();
  }

 private:
  void writePartitionShare(uint32_t pid, DataFileWriter& out) override;

  const PartitionWriterOptions options_;
  /// Per block run: P + 1 absolute spill-file offsets.
  std::vector<std::vector<uint64_t>> spillOffsets_;
};

/// A remote shuffle service. Every block is pushed as it comes, so each
/// must be whole: the service may reorder a mapper's pushes and interleave
/// other mappers' between them. A push hands the bytes to the client, which
/// copies them before it returns. Nothing touches local disk, and with
/// nothing to merge, a task's producers can share one: finish() ends the
/// attempt once for all of them.
class RssBlockOutput final : public PartitionBlockOutput {
 public:
  explicit RssBlockOutput(PartitionWriterOptions options);

  void writeBlocks(BlockProducer produce) override;

  /// Ends the map attempt; partition lengths are the bytes pushed.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return pushedBytes_;
  }

 private:
  const PartitionWriterOptions options_;
  const std::shared_ptr<RssClient> client_;
  std::vector<int64_t> partitionLengths_;
  int64_t pushedBytes_{0};
  uint64_t numPushes_{0};
  uint64_t pushTimeNs_{0};
  bool finished_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
