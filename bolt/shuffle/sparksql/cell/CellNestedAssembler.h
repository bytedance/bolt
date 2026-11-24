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
#include <memory>
#include <vector>

#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The reader side of a nested layout: batches of its payloads in the
/// logical row type. One decoder reads each payload; a decoder per column group
/// opens the group's columns at the group's positions, which the lengths of
/// its owner's entries add up to, counted from their stream before any
/// value is read. A batch takes the rows' next positions, then of each
/// container's group as many as the lengths just appended call for, and so
/// down, and comes out as ARRAY, MAP and ROW vectors over the groups'
/// batches, a ROW null where its presence is.
///
/// Every check of a flat payload holds per column group; besides, a length must
/// not be negative, a length stream must hold exactly its entries' lengths,
/// and a column group at most CellNestedLayout::kMaxPositions positions. A
/// batch closes before a new payload's positions would take some group's batch
/// past that. As for a flat payload, the rows' batch opens at the caller's
/// capacity.
class CellNestedAssembler {
 public:
  /// Positions one append covers at most, so that a decoder's
  /// per-append scratch stays what a flat batch's takes.
  static constexpr uint32_t kAppendPositions = 64 << 10;

  /// `type` must take a nested layout.
  CellNestedAssembler(
      RowTypePtr type,
      ShuffleDecompressor* decompressor,
      memory::MemoryPool* pool);

  /// Reads one payload, its kind byte consumed. Throws on malformed input.
  void beginPayload(ShuffleByteSource& in);

  /// The next batch: at most `capacity` rows of about `byteBudget` bytes,
  /// one append when that is <= 0. `nextPayload` begins the next payload,
  /// false at the end or before a CompositeRows block. nullptr: no row.
  RowVectorPtr nextBatch(
      vector_size_t capacity,
      int64_t byteBudget,
      const std::function<bool()>& nextPayload);

 private:
  /// The positions of container x's column group: its lengths in the payload
  /// just opened, added up.
  void countPositions(uint32_t x);

  /// Whether the batch takes the payload just begun as well: every column
  /// group's positions.
  bool takesPayload() const;

  /// Bytes the batch's values take, every column group's together.
  uint64_t batchBytes() const;

  /// Appends the next `positions` positions of column group g, then the
  /// elements their containers' entries hold.
  void appendGroup(uint32_t g, uint64_t positions, vector_size_t capacity);

  /// The offsets of container x's entries in the batch, room for `entries`.
  vector_size_t* offsets(uint32_t x, uint64_t entries);

  /// The batch so far in the logical row type, closing it.
  RowVectorPtr assemble();

  const CellNestedLayout layout_;
  memory::MemoryPool* const pool_;
  CellPayloadDecoder payloadDecoder_;
  std::vector<std::unique_ptr<CellPayloadDecoder>> decoders_;
  /// Per column group, the payload's positions.
  std::vector<uint64_t> payloadPositions_;
  /// Per container node, its batch entries' offsets.
  std::vector<BufferPtr> offsets_;
  /// A payload row's decoded bytes, every column group's together: what the
  /// byte budget counts by.
  uint64_t payloadRowBytes_{1};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
