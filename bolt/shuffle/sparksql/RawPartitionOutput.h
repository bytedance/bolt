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
#include <memory>
#include <string>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Produces the next opaque block of partition `pid` into `block`; false
/// once that partition has no more.
using RawBlockSource = std::function<bool(uint32_t pid, BufferPtr& block)>;

/// The output of a shuffle writer as opaque, already-framed blocks per
/// partition: a local spill file merged partition by partition into the
/// data file at the end, or pushes to a remote shuffle service. The owner
/// frames and compresses the blocks; this layer only places them.
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

  /// Ends the output - writes the data file, or ends the map attempt - and
  /// fills partitionLengths, totalBytesWritten, totalBytesEvicted and the
  /// write and evict times.
  virtual void finish(ShuffleWriterMetrics& metrics) = 0;

  /// Bytes written to spill storage, or pushed, so far.
  virtual int64_t bytesEvicted() const = 0;
};

/// Local disk. One temporary spill file per task, in a random configured
/// directory so concurrent tasks spread their I/O over the disks; each
/// spillRaw() call becomes a raw run of P + 1 offsets in it. The data file
/// is written once, partition by partition: an owner's own data first, then
/// the partition's raw blocks, copied verbatim. An owner may keep its own
/// spilled data in the same spill file.
class LocalRawOutput : public RawPartitionOutput {
 public:
  explicit LocalRawOutput(PartitionWriterOptions options);
  ~LocalRawOutput() override;

  void spillRaw(const RawBlockSource& next) override;

  /// Writes a data file of the raw blocks alone.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return bytesEvicted_;
  }

 protected:
  /// Writes the data file: for each partition `writeOwn(out, pid)`, when
  /// set, then its raw blocks. Closes and removes the spill file.
  void writeDataFile(
      const std::function<void(std::FILE* out, uint32_t pid)>& writeOwn,
      ShuffleWriterMetrics& metrics);

  void ensureSpillFile();
  void spillWrite(const void* data, size_t bytes);
  void flushSpill();
  void readSpill(uint64_t offset, void* out, size_t bytes) const;

  /// Copies `bytes` of the spill file from `begin` into the data file.
  void copySpill(std::FILE* out, uint64_t begin, uint64_t bytes);

  /// Appends to the data file, counted in its length and write time.
  void writeOut(std::FILE* out, const void* data, size_t bytes);

  uint64_t spillOffset() const {
    return spillOffset_;
  }

  const PartitionWriterOptions options_;

 private:
  std::FILE* spillFile_{nullptr};
  int spillFd_{-1};
  std::string spillPath_;
  uint64_t spillOffset_{0};
  int64_t bytesEvicted_{0};
  /// Per raw run: P + 1 absolute spill-file offsets.
  std::vector<std::vector<uint64_t>> rawRuns_;
  uint64_t finalBytes_{0};
  uint64_t evictTimeNs_{0};
  uint64_t writeTimeNs_{0};
};

/// A remote shuffle service. Every block is pushed as it comes, so each
/// must be whole: the service may reorder a mapper's pushes and interleave
/// other mappers' between them. A push hands the bytes to the client, which
/// copies them before it returns. Nothing touches local disk.
class RssRawOutput : public RawPartitionOutput {
 public:
  explicit RssRawOutput(PartitionWriterOptions options);

  void spillRaw(const RawBlockSource& next) override;

  /// Ends the map attempt; partition lengths are the bytes pushed.
  void finish(ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return pushedBytes_;
  }

 protected:
  /// Pushes one whole block of partition `pid`.
  void push(uint32_t pid, BufferPtr& bytes);

  uint64_t numPushes() const {
    return numPushes_;
  }

  const PartitionWriterOptions options_;

 private:
  const std::shared_ptr<RssClient> client_;
  std::vector<int64_t> partitionLengths_;
  int64_t pushedBytes_{0};
  uint64_t numPushes_{0};
  uint64_t pushTimeNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql
