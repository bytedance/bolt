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

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/RssClient.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The Celeborn backend. The service may reorder a mapper's pushes and
/// interleaves other mappers' pushes between them, so every push is exactly
/// one complete ColumnarPayload: a window is only ever output whole, at a
/// seal, one partition after another, and nothing is spilled - no local
/// file is ever created. Each payload carries the partition's resident
/// cells as one run, compressed when that pays.
///
/// A push hands the bytes to the client, which copies them before it
/// returns, as for the V1 Celeborn writer; the workspace is reused for the
/// next partition right away.
class CelebornCellOutput final : public CellOutput {
 public:
  /// `pool` backs the payload workspaces so they stay inside task memory
  /// accounting; it must outlive this object.
  CelebornCellOutput(
      PartitionWriterOptions options,
      const CellLayout* layout,
      memory::MemoryPool* pool);

  bool sealsResidentCells() const override {
    return true;
  }

  void spillRun(const CellWindowInput& in) override;
  void sealWindow(const CellWindowInput& in) override;
  /// Pushes every block as it comes; a block is whole, so it may land
  /// anywhere among the partition's other pushes.
  void spillRaw(const RawBlockSource& next) override;
  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) override;

  /// Bytes pushed so far: a remote shuffle's evictions, as for V1.
  int64_t bytesEvicted() const override {
    return pushedBytes_;
  }

 private:
  void pushWindow(const CellWindowInput& in);

  /// Assembles one partition's payload into payload_ and pushes it.
  void pushPartition(const CellWindowInput& in, uint32_t pid);

  void push(uint32_t pid, BufferPtr& bytes);

  const PartitionWriterOptions options_;
  const CellLayout* const layout_;
  memory::MemoryPool* const pool_;
  const std::shared_ptr<RssClient> client_;
  /// Null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;

  BufferPtr payload_;
  BufferPtr compressScratch_;
  std::vector<uint64_t> streamSizes_;

  std::vector<int64_t> partitionLengths_;
  std::vector<int64_t> rawPartitionLengths_;
  int64_t pushedBytes_{0};
  uint64_t numPushes_{0};
  uint64_t numSealed_{0};
  uint64_t compressTimeNs_{0};
  uint64_t pushTimeNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
