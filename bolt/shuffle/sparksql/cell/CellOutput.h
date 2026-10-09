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

#include <functional>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/cell/CellBuffer.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// What an output backend reads from the splitter when draining or sealing:
/// the live window state. Borrowed for the duration of a call.
struct CellWindowInput {
  const DataCells* cells;
  const NullCells* nulls;
  const CellLayout* layout;
  /// Per partition: rows and raw variable bytes of the current window.
  const uint32_t* rowCounts;
  const uint64_t* variableBytes;
  /// Payload encoding tags (spec section 3.2): ceil(C/8) bytes, one bit per
  /// column, set when the column is written in dictionary form. Constant
  /// for the writer's lifetime (the probe decides once).
  const uint8_t* encodingTags;
  uint32_t numPartitions;
};

/// Produces the next opaque block of partition `pid` into `block`; false
/// once that partition has no more.
using RawBlockSource = std::function<bool(uint32_t pid, BufferPtr& block)>;

/// L4 of the Cell shuffle writer: the output backend. Run = physical
/// swap-out (may happen at any allocation point, never closes the logical
/// window); sealed window = payload boundary. Local and RSS backends differ
/// only here.
class CellOutput {
 public:
  virtual ~CellOutput() = default;

  /// Whether sealWindow() consumes the window's resident cells itself. A
  /// backend that does (RSS: every payload is pushed whole at the seal) is
  /// never asked for a Run; otherwise the splitter drains the cells into a
  /// Run before sealing.
  virtual bool sealsResidentCells() const {
    return false;
  }

  /// Physically drains every DataCell chain into a Run of the current
  /// window. Does not release the cells: the splitter recycles them (so a
  /// spill fired from inside an append never frees held ids twice).
  virtual void spillRun(const CellWindowInput& in) = 0;

  /// Closes the logical window: records the null region and row counts.
  /// Precondition (splitter-orchestrated): caches flushed, and the cells
  /// drained unless sealsResidentCells().
  virtual void sealWindow(const CellWindowInput& in) = 0;

  /// Writes opaque, already-framed blocks - the composite rows of a
  /// CompositeShuffleWriter - as one unit, like a Run: `next` is called
  /// for partition 0, 1, ... until it returns false for each, and every
  /// block is written before the next call, so the producer may reuse the
  /// buffer. The blocks belong to no window; each partition's land after
  /// its payloads in the final output.
  virtual void spillRaw(const RawBlockSource& next) = 0;

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
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
