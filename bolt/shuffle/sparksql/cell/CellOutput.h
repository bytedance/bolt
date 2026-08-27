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

#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/cell/CellDirectory.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// What an output backend reads from the writer when draining or sealing:
/// the live window state. Borrowed for the duration of a call.
struct CellWindowInput {
  DataCells* cells;
  NullCells* nulls;
  const CellLayout* layout;
  /// Per partition: rows and raw variable bytes of the current window.
  const uint32_t* rowCounts;
  const uint64_t* variableBytes;
  uint32_t numPartitions;
};

/// L4 of the Cell shuffle writer: the output backend. Run = physical
/// swap-out (may happen at any allocation point, never closes the logical
/// window); sealed window = payload boundary. Local and RSS backends differ
/// only here.
class CellOutput {
 public:
  virtual ~CellOutput() = default;

  /// Physically drains every DataCell chain into a Run of the current
  /// window. Does not release the cells: the caller recycles them (so a
  /// spill fired from inside an append never frees held ids twice).
  virtual void spillRun(const CellWindowInput& in) = 0;

  /// Closes the logical window: records the null region and row counts.
  /// Precondition (writer-orchestrated): caches flushed, cells drained.
  virtual void sealWindow(const CellWindowInput& in) = 0;

  /// Final merge. `windowHasData` says whether the current (unsealed)
  /// window still carries rows; the backend either seals it through disk or
  /// assembles it live when nothing was ever spilled. Fills the metrics
  /// fields it owns (partitionLengths, rawPartitionLengths,
  /// totalBytesWritten, totalBytesEvicted, write/evict times).
  virtual void finalize(
      const CellWindowInput& in,
      bool windowHasData,
      ShuffleWriterMetrics& metrics) = 0;

  /// Bytes written to spill storage so far (the evict metric).
  virtual int64_t bytesEvicted() const = 0;

  // RSS seam (二期): flush selected partitions as complete payloads and
  // release their cells, leaving the rest of the window in place.
  // virtual void flushPartitions(const std::vector<uint32_t>& pids,
  //                              const CellWindowInput& in) = 0;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
