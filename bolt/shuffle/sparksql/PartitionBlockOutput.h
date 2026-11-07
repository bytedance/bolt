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
/// until memory runs short, instead of on disk: a spill that the merge
/// copies into the data file anyway then costs no disk write and read.
/// Pages of the writer's pool, accounted to it: at most capBytes() of them
/// over every file together, each taken only while the pool holds at most
/// totalCapBytes() with it, the writer's cells and everything else it holds
/// there included. A page past either cap is no memory pressure: the file
/// writes its pages out and starts them afresh or, when even fresh ones
/// pass a cap, appends a page's bytes to disk before it asks again. A
/// memory pressure event is a Pressure: it writes every file's pages out
/// first, and keeps appends on disk while it lives. A file's bytes in
/// memory always end it, as it writes its pages out before it writes
/// anything else to disk.
class SpillMemory {
 public:
  /// Pages of up to 4 MiB from `pool`, which must outlive this object, as
  /// must every file attached to it. A cap below one machine page keeps
  /// nothing in memory.
  SpillMemory(
      memory::MemoryPool* pool,
      int64_t capBytes,
      int64_t totalCapBytes);

  SpillMemory(const SpillMemory&) = delete;
  SpillMemory& operator=(const SpillMemory&) = delete;

  /// One memory pressure event, for as long as it lives. It first writes
  /// out the pages of every file but one whose bytes must stay where they
  /// are - in the middle of an append, or frozen for its merge - then keeps
  /// every append on disk. Does nothing for a null `memory`.
  class Pressure {
   public:
    explicit Pressure(SpillMemory* memory);
    ~Pressure();

    Pressure(const Pressure&) = delete;
    Pressure& operator=(const Pressure&) = delete;

    /// The page bytes the first step freed.
    int64_t freedBytes() const {
      return freedBytes_;
    }

   private:
    SpillMemory* const memory_;
    int64_t freedBytes_{0};
  };

  /// The most page bytes the files hold together.
  int64_t capBytes() const {
    return capBytes_;
  }

  /// The most the pool may hold, a new page included, for that page to be
  /// taken. The pool's usedBytes() counts every allocation from it at its
  /// size, a page as one page more; read once per page.
  int64_t totalCapBytes() const {
    return totalCapBytes_;
  }

  /// Page bytes the files hold now, and the most they held.
  int64_t heldBytes() const {
    return heldBytes_;
  }
  int64_t peakBytes() const {
    return peakBytes_;
  }

  /// Bytes the files wrote out of their pages, and how many times.
  uint64_t flushedBytes() const {
    return flushedBytes_;
  }
  uint64_t flushes() const {
    return flushes_;
  }

  /// Bytes the files appended to disk directly: under pressure, past a cap,
  /// or for want of a page.
  uint64_t directBytes() const {
    return directBytes_;
  }

 private:
  friend class ShuffleSpillFile;

  /// Whether `bytes` more of pages stay within both caps.
  bool fits(int64_t bytes) const;

  /// A page for a file, reserved through the pool first as a cell chunk is;
  /// false when the reservation fails.
  bool fundPage(memory::ContiguousAllocation& page);

  memory::MemoryPool* const pool_;
  const int64_t capBytes_;
  const int64_t totalCapBytes_;
  /// A power of two; 0 keeps nothing in memory.
  const uint64_t pageBytes_;
  const uint32_t pageShift_;
  int64_t heldBytes_{0};
  int64_t peakBytes_{0};
  uint64_t flushedBytes_{0};
  uint64_t flushes_{0};
  uint64_t directBytes_{0};
  /// Live Pressure events: appends go to disk while there is one.
  uint32_t pressureDepth_{0};
  std::vector<ShuffleSpillFile*> files_;
};

/// Temporary shuffle storage of one output: appended in time order, read
/// back at any offset. On local disk, the file is created on the first byte
/// it gets, in a random configured directory so concurrent tasks spread
/// their I/O over the disks, and removed by remove() or on destruction.
/// Attached to a SpillMemory, it keeps its newest bytes in pages while that
/// has room and no pressure: the offsets stay the same, [0, diskEnd) on disk
/// and [diskEnd, end()) in memory.
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
  /// disk through a 64 KiB buffer, those in memory straight from the pages.
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

  /// Closes and deletes the file, and frees its pages.
  void remove();

  /// Writes the bytes in memory out to disk, in order, and frees their
  /// pages; returns the page bytes freed. Does nothing - returns 0 - in the
  /// middle of an append, or once frozen: the merge reads the pages.
  int64_t flushMemory();

  /// Bytes written to disk: the evict metric.
  int64_t bytesWritten() const {
    return static_cast<int64_t>(diskEnd_);
  }

  /// Bytes in memory now, and the most there were.
  uint64_t memoryBytes() const {
    return pages_.empty() ? 0 : end_ - diskEnd_;
  }
  uint64_t peakMemoryBytes() const {
    return peakMemoryBytes_;
  }

  /// Bytes flushMemory() and the cap wrote out of memory, and how many
  /// times.
  uint64_t memoryFlushedBytes() const {
    return memoryFlushedBytes_;
  }
  uint64_t memoryFlushes() const {
    return memoryFlushes_;
  }

  /// Disk write and flush time: the evict metric.
  uint64_t writeTimeNs() const {
    return writeTimeNs_;
  }

 private:
  void openFile();

  /// Appends to disk, behind every byte: none may be in memory.
  void appendToDisk(const void* data, size_t bytes);

  /// Appends to the pages, writing them out first when the pages it needs
  /// would pass a cap; false when it must go to disk instead.
  bool appendToMemory(const char* data, size_t bytes);

  /// flushMemory() of an append, which may.
  int64_t writeOutMemory();

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
  /// The bytes from diskEnd_ on, packed from the first page's start.
  std::vector<memory::ContiguousAllocation> pages_;
  uint64_t peakMemoryBytes_{0};
  uint64_t memoryFlushedBytes_{0};
  uint64_t memoryFlushes_{0};
  bool frozen_{false};
  bool appending_{false};
  /// A page could not be funded: memory is short, so appends go to disk
  /// until the next flush() ends the run they belong to.
  bool unfunded_{false};
  /// The caps left no room even for fresh pages: appends go to disk until
  /// the file reaches this offset, a page's bytes on, before they ask for a
  /// page again.
  uint64_t diskUntil_{0};
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
  /// bounce buffer, the part in memory straight from its pages.
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
