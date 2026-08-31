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

#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/PoolBytes.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The ESS backend: Runs and sealed windows go to one temporary spill file;
/// stop() merges everything into the shuffle data file, partition-major.
///
/// Spill segments are stored in the exact Run body form of the wire format
/// (COMBINED_STORED, uncompressed), so the final merge is a header plus a
/// sequential byte copy — nothing is re-parsed or re-assembled. A task whose
/// data never needed a spill writes payloads straight from the live cells
/// and touches no temporary file at all.
class LocalCellOutput final : public CellOutput {
 public:
  /// `pool` backs the gather/compress workspaces so they stay inside task
  /// memory accounting; it must outlive this object.
  LocalCellOutput(
      PartitionWriterOptions options,
      const CellLayout* layout,
      CellShuffleOptions cellOptions,
      memory::MemoryPool* pool);
  ~LocalCellOutput() override;

  void spillRun(const CellWindowInput& in) override;
  void sealWindow(const CellWindowInput& in) override;
  void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) override;

  int64_t bytesEvicted() const override {
    return bytesEvicted_;
  }

 private:
  struct SealedWindow {
    /// Per run: P + 1 absolute spill-file offsets; equal neighbours mean an
    /// empty segment for that partition.
    std::vector<std::vector<uint64_t>> runPidEnds;
    std::vector<uint64_t> nullOffset;
    std::vector<uint32_t> nullLength;
    std::vector<uint32_t> rowCounts;
    std::vector<uint64_t> variableBytes;
  };

  bool hasDiskState() const {
    return spillFile_ != nullptr;
  }

  void ensureSpillFile();
  void spillWrite(const void* data, size_t bytes);
  void readSpill(uint64_t offset, void* out, size_t bytes) const;

  /// Appends one partition's payload assembled from a sealed window.
  void writeDiskPayload(
      std::FILE* out,
      const SealedWindow& w,
      const uint8_t* encodingTags,
      uint32_t pid);

  /// Copies one spilled run segment into the data file: verbatim when it
  /// was compressed at spill time, else through the compressing run writer.
  void writeSpilledSegment(std::FILE* out, uint64_t begin, uint64_t end);

  /// Compression policy shared by spill and merge: points `body`/`stored`
  /// at the COMBINED form when the codec shrinks the bytes, else at the
  /// input (spec section 5: fall back to the stored form when compression
  /// does not pay). Returns the layout to declare.
  RunLayout maybeCompressRun(
      const char* data,
      uint64_t dataBytes,
      const char*& body,
      uint64_t& stored);

  /// Writes one run to the data file, compressed per the policy above.
  /// `data` is the concatenated stream bytes; sizes are the per-stream
  /// decoded lengths.
  void writeRun(
      std::FILE* out,
      const char* data,
      uint64_t dataBytes,
      const uint64_t* decodedSizes);

  /// Appends one partition's payload for the current (unsealed) window:
  /// header, null body and row counts straight from memory, any mid-window
  /// spilled runs copied from the spill file, and the still-resident cells
  /// as the final run. The residual never takes a spill round-trip.
  void writeCurrentWindowPayload(
      std::FILE* out,
      const CellWindowInput& in,
      uint32_t pid);

  void writeOut(std::FILE* out, const void* data, size_t bytes);

  const PartitionWriterOptions options_;
  const CellLayout* const layout_;
  const CellShuffleOptions cellOptions_;
  /// Final-merge codec; null when compressionType is UNCOMPRESSED.
  std::unique_ptr<Codec> codec_;
  /// Time metrics align with the V1/V2 partition writers and never
  /// overlap: compress = codec calls only; write = fwrite/fflush moments
  /// on the data file; evict = fwrite/fflush moments on the spill file.
  /// Assembly glue (chain scans, null bodies, spill read-back) is counted
  /// nowhere, as in V1 where it rides inside split.
  uint64_t compressTimeNs_{0};
  /// Pool-backed workspaces, released after every spill and after finalize
  /// so their capacity never sits on the reservation between uses. When the
  /// pool cannot fund them during a pressure spill, spillRun degrades to
  /// streaming the run out uncompressed instead of failing.
  PoolBytes runScratch_;
  PoolBytes compressScratch_;

  std::FILE* spillFile_{nullptr};
  int spillFd_{-1};
  std::string spillPath_;
  uint64_t spillOffset_{0};
  int64_t bytesEvicted_{0};

  /// Runs of the still-open window, then folded into a SealedWindow.
  std::vector<std::vector<uint64_t>> openWindowRuns_;
  std::vector<SealedWindow> sealed_;

  uint64_t finalBytes_{0};
  uint64_t rawAccum_{0};
  uint64_t evictTimeNs_{0};
  uint64_t writeTimeNs_{0};
  PoolBytes scratch_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
