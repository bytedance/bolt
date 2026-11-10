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

#include <cstdio>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql {

class ShuffleSpillFile;

/// Memory in which the spill files of one writer keep their newest bytes
/// instead of on disk: a spill that the merge copies into the data file
/// anyway then costs no disk write and read. Blocks of one size from the
/// writer's pool, accounted to it, at most capBytes() of them over every
/// file together. A file keeps its blocks oldest first, and its bytes in
/// memory always end it: a block written out continues the file on disk.
///
/// A file that needs a block takes a new one while the memory still grows
/// and stays within the cap. Otherwise it reuses the oldest block of the
/// file holding the most, the asking one included, written out first, so
/// the memory keeps the newest bytes at a constant size; with no block
/// anywhere, the file appends to disk. Memory pressure - release() - writes
/// the oldest blocks out and frees them, and so does a failed reservation
/// of a new block: either ends the growth for good, and the memory only
/// reuses what it holds from then on.
///
/// A new block's reservation may reclaim the writer, and that reclaim may
/// release() blocks of any file, the asking one included. A file asks only
/// once its last block is full, so the blocks written out are whole and
/// the file is consistent whenever that happens. During the merge nothing
/// is released: the writer cannot be reclaimed while it stops.
class SpillMemory {
 public:
  /// Blocks of up to 4 MiB from `pool`, at least four of them within
  /// `capBytes`; `pool` must outlive this object, as must every file
  /// attached to it. A cap below four machine pages keeps nothing in memory.
  SpillMemory(memory::MemoryPool* pool, int64_t capBytes);

  SpillMemory(const SpillMemory&) = delete;
  SpillMemory& operator=(const SpillMemory&) = delete;

  /// Memory pressure: writes the oldest blocks out, of the file holding the
  /// most first, and frees them until at least `bytes` are freed - none for
  /// 0 or less - and ends the growth. Returns the bytes freed.
  int64_t release(int64_t bytes);

  /// release() of every block.
  int64_t releaseAll();

  /// The most block bytes the files hold together.
  int64_t capBytes() const {
    return capBytes_;
  }

  /// Block bytes the files hold now, and the most they held.
  int64_t heldBytes() const {
    return heldBytes_;
  }
  int64_t peakBytes() const {
    return peakBytes_;
  }

  /// Bytes the files wrote out of their blocks, and how many blocks.
  uint64_t flushedBytes() const {
    return flushedBytes_;
  }
  uint64_t flushes() const {
    return flushes_;
  }

  /// Bytes the files appended to disk directly, for want of a block.
  uint64_t directBytes() const {
    return directBytes_;
  }

 private:
  friend class ShuffleSpillFile;

  /// A block for a file whose last block is full: a new one, or the oldest
  /// of the file holding the most, written out. False when there is none.
  bool takeBlock(memory::ContiguousAllocation& block);

  /// The file holding the most blocks; null when none holds one.
  ShuffleSpillFile* largestFile() const;

  /// Writes the oldest block of the file holding the most out and frees it;
  /// false when no file holds a block.
  bool releaseOne();

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
  std::vector<ShuffleSpillFile*> files_;
};

/// Temporary shuffle storage of one output: appended in time order, read
/// back at any offset. On local disk, the file is created on the first byte
/// it gets, in a random configured directory so concurrent tasks spread
/// their I/O over the disks, and removed by remove() or on destruction.
/// Attached to a SpillMemory, it keeps its newest bytes in blocks of it:
/// the offsets stay the same, [0, diskEnd) on disk and [diskEnd, end()) in
/// memory, packed from the oldest block's start.
class ShuffleSpillFile {
 public:
  /// `name` tells a task's spill files apart when, with no configured
  /// directory, they sit next to the data file: <dataFile>.<name>.spill.
  /// `memory`, if not null, must outlive this file.
  ShuffleSpillFile(
      PartitionWriterOptions options,
      std::string name,
      SpillMemory* memory = nullptr);
  ~ShuffleSpillFile();

  ShuffleSpillFile(const ShuffleSpillFile&) = delete;
  ShuffleSpillFile& operator=(const ShuffleSpillFile&) = delete;

  /// Appends `bytes` and returns the offset they start at.
  uint64_t append(const void* data, size_t bytes);

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

  /// The offset the next append starts at.
  uint64_t end() const {
    return end_;
  }

  /// No append is accepted afterwards; reads still are.
  void freeze() {
    frozen_ = true;
  }

  bool isFrozen() const {
    return frozen_;
  }

  /// Closes and deletes the file, and frees its blocks.
  void remove();

  /// Bytes written to disk: the evict metric.
  int64_t bytesWritten() const {
    return static_cast<int64_t>(diskEnd_);
  }

  /// Disk write and flush time: the evict metric.
  uint64_t writeTimeNs() const {
    return writeTimeNs_;
  }

 private:
  friend class SpillMemory;

  void openFile();

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
  SpillMemory* const memory_;
  std::FILE* file_{nullptr};
  int fd_{-1};
  std::string path_;
  uint64_t end_{0};
  /// Where the bytes in memory start: everything before is on disk.
  uint64_t diskEnd_{0};
  /// The bytes from diskEnd_ on, oldest first: all blocks but the last
  /// are full.
  std::deque<memory::ContiguousAllocation> blocks_;
  bool frozen_{false};
  uint64_t writeTimeNs_{0};
  /// patch()es not applied yet: offset and bytes.
  std::vector<std::pair<uint64_t, std::string>> pendingPatches_;
};

/// The task's final data file, written once, partition by partition.
/// Closed on destruction when a write failed part-way.
class DataFileWriter {
 public:
  DataFileWriter(const std::string& path, uint32_t numPartitions);
  ~DataFileWriter();

  /// Subsequent writes count towards partition `pid`.
  void beginPartition(uint32_t pid);

  void write(const void* data, size_t bytes);

  /// Bytes written so far: where the next write lands.
  uint64_t position() const {
    return static_cast<uint64_t>(bytes_);
  }

  /// Overwrites `bytes` already written at `position`. Applied at close(),
  /// so a patch per run does not break up the buffered writes.
  void patch(uint64_t position, const void* data, size_t bytes);

  /// Copies `bytes` of `spill` from `begin`: the part on disk through a
  /// bounce buffer, the part in memory straight from its blocks.
  void copyFrom(const ShuffleSpillFile& spill, uint64_t begin, uint64_t bytes);

  /// Flushes and closes; fills partitionLengths, totalBytesWritten and
  /// totalWriteTime.
  void close(ShuffleWriterMetrics& metrics);

 private:
  std::FILE* file_;
  std::vector<int64_t> lengths_;
  uint32_t pid_{0};
  int64_t bytes_{0};
  uint64_t writeTimeNs_{0};
  /// patch()es not applied yet: position and bytes.
  std::vector<std::pair<uint64_t, std::string>> pendingPatches_;
};

/// A local output that spills to a ShuffleSpillFile of its own, in pieces
/// that only become final output in the merge: there, partition by
/// partition, it writes its share into the task's data file. Several
/// spillers - a task's ordinary and composite outputs - share one data file
/// through mergeToDataFile() and nothing else.
class PartialSpiller {
 public:
  /// Writes the task's data file from `spillers`: for each partition, every
  /// spiller's share in the order given. Seals their spill files first -
  /// nothing may be appended behind the merge, and a spiller is merged once
  /// only - and removes them afterwards, freeing their memory. Fills
  /// partitionLengths, totalBytesWritten, totalBytesEvicted - the bytes that
  /// reached disk - and the write and evict times, and totalSpillFiles with
  /// the spill runs: every run goes into the one spill file, so the run is
  /// what the local partition writer's file per spill corresponds to.
  static void mergeToDataFile(
      const PartitionWriterOptions& options,
      std::initializer_list<PartialSpiller*> spillers,
      ShuffleWriterMetrics& metrics);

  virtual ~PartialSpiller() = default;

  /// Bytes written to disk.
  int64_t spilledBytes() const {
    return spill_.bytesWritten();
  }

 protected:
  /// `memory`, if not null, may keep the spill file's newest bytes and must
  /// outlive this object.
  PartialSpiller(
      PartitionWriterOptions options,
      std::string name,
      SpillMemory* memory = nullptr)
      : spill_(std::move(options), std::move(name), memory) {}

  /// Partition `pid`'s share of the data file. Called by mergeToDataFile()
  /// only, for partition 0, 1, ..., with the spill file frozen.
  virtual void writePartitionShare(uint32_t pid, DataFileWriter& out) = 0;

  /// Counts one spill run if `begin`, the spill end before it, has moved.
  void countSpill(uint64_t begin) {
    numSpills_ += spill_.end() > begin ? 1 : 0;
  }

  ShuffleSpillFile spill_;

 private:
  /// Spills that wrote bytes: totalSpillFiles.
  int64_t numSpills_{0};
};

/// Produces the next opaque block of partition `pid` into `block`; false
/// once that partition has no more.
using BlockSource = std::function<bool(uint32_t pid, BufferPtr& block)>;

/// The output of a shuffle writer as whole, already-framed blocks per
/// partition: a local spill file merged partition by partition into the
/// data file at the end, or pushes to a remote shuffle service, which does
/// that merge itself. The owner frames and compresses the blocks; this layer
/// only places them.
class PartitionBlockOutput {
 public:
  virtual ~PartitionBlockOutput() = default;

  /// Writes one unit over all partitions: `next` is called for partition 0,
  /// 1, ... until it returns false for each, and every block is written
  /// before the next call, so the producer may reuse the buffer.
  virtual void writeBlocks(const BlockSource& next) = 0;

  /// Ends the output on its own - writes a data file of its blocks alone, or
  /// ends the map attempt - and fills partitionLengths, totalBytesWritten,
  /// totalBytesEvicted and the write and evict times. Once only; nothing may
  /// be written afterwards.
  virtual void finish(ShuffleWriterMetrics& metrics) = 0;

  /// Bytes written to spill storage on disk, or pushed, so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// Local disk. Each writeBlocks() call becomes a block run of P + 1 offsets
/// in its own spill file; the merge copies each partition's blocks
/// verbatim, in call order. Merged with another output's share by
/// mergeToDataFile(), or on its own by finish().
class LocalBlockOutput final : public PartitionBlockOutput,
                               public PartialSpiller {
 public:
  /// `memory`, if not null, may keep the spill file's newest bytes and must
  /// outlive this object.
  explicit LocalBlockOutput(
      PartitionWriterOptions options,
      SpillMemory* memory = nullptr);

  void writeBlocks(const BlockSource& next) override;

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

  void writeBlocks(const BlockSource& next) override;

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
