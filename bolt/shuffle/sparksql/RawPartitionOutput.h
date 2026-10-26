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
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Temporary shuffle storage of one output on local disk: appended in time
/// order, read back at any offset. The file is created on the first append,
/// in a random configured directory so concurrent tasks spread their I/O
/// over the disks, and removed by remove() or on destruction.
class ShuffleSpillFile {
 public:
  /// `name` tells a task's spill files apart when, with no configured
  /// directory, they sit next to the data file: <dataFile>.<name>.spill.
  ShuffleSpillFile(PartitionWriterOptions options, std::string name);
  ~ShuffleSpillFile();

  /// Appends `bytes` and returns the offset they start at.
  uint64_t append(const void* data, size_t bytes);

  /// Makes every append visible to read(), which bypasses stdio buffering.
  void flush();

  void read(uint64_t offset, void* out, size_t bytes) const;

  /// The offset the next append starts at.
  uint64_t end() const {
    return end_;
  }

  /// No append is accepted afterwards; reads still are.
  void seal() {
    sealed_ = true;
  }

  bool sealed() const {
    return sealed_;
  }

  /// Closes and deletes the file.
  void remove();

  int64_t bytesWritten() const {
    return static_cast<int64_t>(end_);
  }

  /// Append and flush time: the evict metric.
  uint64_t writeTimeNs() const {
    return writeTimeNs_;
  }

 private:
  const PartitionWriterOptions options_;
  const std::string name_;
  std::FILE* file_{nullptr};
  int fd_{-1};
  std::string path_;
  uint64_t end_{0};
  bool sealed_{false};
  uint64_t writeTimeNs_{0};
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

  /// Copies `bytes` of `spill` from `begin`.
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
  /// only - and removes them afterwards. Fills partitionLengths,
  /// totalBytesWritten, totalBytesEvicted and the write and evict times, and
  /// totalSpillFiles with the spill runs: every run goes into the one spill
  /// file, so the run is what the local partition writer's file per spill
  /// corresponds to.
  static void mergeToDataFile(
      const PartitionWriterOptions& options,
      std::initializer_list<PartialSpiller*> spillers,
      ShuffleWriterMetrics& metrics);

  virtual ~PartialSpiller() = default;

  int64_t spilledBytes() const {
    return spill_.bytesWritten();
  }

 protected:
  PartialSpiller(PartitionWriterOptions options, std::string name)
      : spill_(std::move(options), std::move(name)) {}

  /// Partition `pid`'s share of the data file. Called by mergeToDataFile()
  /// only, for partition 0, 1, ..., with the spill file sealed.
  virtual void mergeToFinal(uint32_t pid, DataFileWriter& out) = 0;

  /// Counts one spill run if `begin`, the spill end before it, has moved.
  void countRun(uint64_t begin) {
    spillRuns_ += spill_.end() > begin ? 1 : 0;
  }

  ShuffleSpillFile spill_;

 private:
  /// Spills that wrote bytes: totalSpillFiles.
  int64_t spillRuns_{0};
};

/// Produces the next opaque block of partition `pid` into `block`; false
/// once that partition has no more.
using RawBlockSource = std::function<bool(uint32_t pid, BufferPtr& block)>;

/// The output of a shuffle writer as whole, already-framed blocks per
/// partition: a local spill file merged partition by partition into the
/// data file at the end, or pushes to a remote shuffle service, which does
/// that merge itself. The owner frames and compresses the blocks; this layer
/// only places them.
class RawPartitionOutput {
 public:
  /// The local or the remote output, by options.partitionWriterType.
  static std::unique_ptr<RawPartitionOutput> create(
      PartitionWriterOptions options);

  virtual ~RawPartitionOutput() = default;

  /// Writes one unit over all partitions: `next` is called for partition 0,
  /// 1, ... until it returns false for each, and every block is written
  /// before the next call, so the producer may reuse the buffer.
  virtual void spillRaw(const RawBlockSource& next) = 0;

  /// Ends the output on its own - writes a data file of its blocks alone, or
  /// ends the map attempt - and fills partitionLengths, totalBytesWritten,
  /// totalBytesEvicted and the write and evict times. Once only; nothing may
  /// be written afterwards.
  virtual void finish(ShuffleWriterMetrics& metrics) = 0;

  /// Bytes written to spill storage, or pushed, so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// Local disk. Each spillRaw() call becomes a raw run of P + 1 offsets in
/// its own spill file; the merge copies each partition's raw blocks
/// verbatim, in call order. Merged with another output's share by
/// mergeToDataFile(), or on its own by finish().
class LocalRawOutput final : public RawPartitionOutput, public PartialSpiller {
 public:
  explicit LocalRawOutput(PartitionWriterOptions options);

  void spillRaw(const RawBlockSource& next) override;

  /// mergeToDataFile() of this output alone.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return spilledBytes();
  }

 private:
  void mergeToFinal(uint32_t pid, DataFileWriter& out) override;

  const PartitionWriterOptions options_;
  /// Per raw run: P + 1 absolute spill-file offsets.
  std::vector<std::vector<uint64_t>> rawRuns_;
};

/// A remote shuffle service. Every block is pushed as it comes, so each
/// must be whole: the service may reorder a mapper's pushes and interleave
/// other mappers' between them. A push hands the bytes to the client, which
/// copies them before it returns. Nothing touches local disk, and with
/// nothing to merge, a task's producers can share one: finish() ends the
/// attempt once for all of them.
class RssRawOutput final : public RawPartitionOutput {
 public:
  explicit RssRawOutput(PartitionWriterOptions options);

  void spillRaw(const RawBlockSource& next) override;

  /// Ends the map attempt; partition lengths are the bytes pushed.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return pushedBytes_;
  }

 private:
  void push(uint32_t pid, BufferPtr& bytes);

  const PartitionWriterOptions options_;
  const std::shared_ptr<RssClient> client_;
  std::vector<int64_t> partitionLengths_;
  int64_t pushedBytes_{0};
  uint64_t numPushes_{0};
  uint64_t pushTimeNs_{0};
  bool finished_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
