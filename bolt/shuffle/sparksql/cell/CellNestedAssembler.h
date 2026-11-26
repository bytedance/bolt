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

/// Assembles physical column groups into batches of the logical row type.
/// Opening a payload decodes each container's length vector. Its sum gives
/// the element group size. Each batch uses a slice of the lengths and the
/// corresponding elements, recursively.
///
/// Every check of a flat payload holds per column group; besides, a length must
/// not be negative, a length stream must hold exactly its entries' lengths,
/// and a column group at most CellNestedLayout::kMaxPositions positions. A
/// batch closes before a new payload would take any group past that limit.
class CellNestedAssembler {
 public:
  /// `type` must take a nested layout.
  CellNestedAssembler(
      RowTypePtr type,
      ShuffleDecompressor* decompressor,
      memory::MemoryPool* pool);

  /// Reads a payload after its kind byte and opens its column groups for
  /// nextBatch(). Throws on malformed input.
  void openPayload(ShuffleByteSource& in);

  /// Returns up to maxBatchRows root rows, aiming for a positive byteBudget
  /// bytes. nextPayload returns false at EOF or a CompositeRows block. Returns
  /// nullptr if no rows were appended.
  RowVectorPtr nextBatch(
      vector_size_t maxBatchRows,
      int64_t byteBudget,
      const std::function<bool()>& nextPayload);

 private:
  struct ContainerColumn {
    BufferPtr offsets;

    /// A batch borrows one contiguous slice until it spans a second payload.
    VectorPtr firstPayload;
    vector_size_t firstOffset{0};
    BufferPtr mergedSizes;
    BufferPtr mergedNulls;

    void appendLengths(
        const VectorPtr& payloadLengths,
        vector_size_t from,
        vector_size_t at,
        vector_size_t count);
    /// The output sizes and nulls, including zero sizes for null containers.
    std::pair<BufferPtr, BufferPtr> finishLengths(
        vector_size_t count,
        memory::MemoryPool* pool);
  };

  /// Appends rows and their nested elements. Returns the decoded bytes added.
  uint64_t appendGroup(uint32_t groupId, uint32_t numRows);

  /// The offsets of a container's entries in the batch, room for `entries`.
  vector_size_t* offsets(BufferPtr& buffer, uint64_t entries);

  /// The batch so far in the logical row type, closing it.
  RowVectorPtr assemble();

  const CellNestedLayout layout_;
  memory::MemoryPool* const pool_;
  CellPayloadDecoder payloadDecoder_;
  std::vector<std::unique_ptr<CellPayloadDecoder>> decoders_;
  /// One whole-payload length projection for each group that owns containers.
  std::vector<std::unique_ptr<CellPayloadDecoder>> lengthDecoders_;
  /// Decoded length columns in each group's expansion order.
  std::vector<RowVectorPtr> payloadLengths_;
  std::vector<uint32_t> stringDataStreams_;
  /// Root rows for group 0, or the parent container's element count.
  std::vector<uint32_t> payloadGroupRowCounts_;
  /// Set when opening a payload, before adding its rows to the current batch.
  bool payloadFitsBatch_{true};
  /// Indexed by the container's element group. Group zero has no container.
  std::vector<ContainerColumn> containers_;
  /// A payload row's decoded bytes, every column group's together: what the
  /// byte budget counts by.
  uint64_t payloadRowBytes_{1};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
