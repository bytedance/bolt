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

#include "bolt/vector/DecodedVector.h"

#include "bolt/shuffle/sparksql/cell/CellDirectory.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Per-column null shape of one batch. Resolved by the writer with a
/// word-level scan of the decoded nulls, because mayHaveNulls() only says a
/// nulls buffer EXISTS - upstream operators routinely attach defensively
/// allocated, all-set buffers, and trusting them would tax every row of the
/// loop for nulls that are not there.
enum class BatchNullClass : uint8_t {
  kNoNulls = 0, // buffer absent, all-set, or constant non-null
  kSomeNulls = 1, // genuinely mixed: the per-row null path applies
  kAllNull = 2, // every batch row null: one counted run per partition
};

/// One decoded input batch, pid column already excluded. Borrowed for the
/// duration of a split call.
struct SplitBatch {
  /// One DecodedVector per logical column of the layout.
  std::vector<DecodedVector>* decoded;
  const uint32_t* row2Partition;
  /// Rows this batch adds per partition (the partitioner's output); lets a
  /// whole-batch fast path work per partition instead of per row.
  const uint32_t* partition2RowCount;
  /// Per column, see BatchNullClass.
  const BatchNullClass* nullClass;
  uint32_t numRows;
  /// Null-bit position of row r in its partition's window:
  /// windowRowStart[pid] + rowIndexInPid[r]. rowIndexInPid may be null when
  /// no column of this batch can carry nulls.
  const uint32_t* rowIndexInPid;
  const uint32_t* windowRowStart;
};

/// L3 of the Cell shuffle writer: the split front end. Consumes decoded
/// batches and fills DataCells (already in wire form: encoded blocks for
/// Encoding Loop streams, raw bytes otherwise) and NullCells.
///
/// The A/B seam of the design: this front end stages values in 64-byte
/// cache lines and encodes on fill (CachedCellFrontend); a later variant may
/// write raw cells and encode at the output boundary, with L4/L6 unchanged.
class SplitFrontend {
 public:
  virtual ~SplitFrontend() = default;

  virtual void split(const SplitBatch& batch) = 0;

  /// Switches one string column to dictionary form (spec section 8) for the
  /// writer's lifetime. Called by the probe before the first split; the
  /// caller owns the matching encoding_tags bit.
  virtual void enableDictionary(uint32_t col) = 0;

  /// Encodes cache residues into DataCells as stream tail blocks. Only legal
  /// right before the window closes (a tail block must be the last block of
  /// its payload stream, spec section 7.2).
  virtual void flushAll() = 0;

  /// Bytes appended to (pid, *) since the window opened; drives the
  /// checkpoint trigger and approximates the partition's payload size.
  virtual uint64_t partitionBytes(uint32_t pid) const = 0;
  virtual uint64_t maxPartitionBytes() const = 0;

  /// Raw variable-length bytes of pid's window (the payload's variable_size
  /// field, spec section 3.1).
  virtual uint64_t variableBytes(uint32_t pid) const = 0;
  virtual const uint64_t* variableBytesArray() const = 0;

  /// Clears per-window statistics after a seal.
  virtual void resetWindowStats() = 0;

  /// Fixed memory held by the front end (caches, cursors).
  virtual int64_t residentBytes() const = 0;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
