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

#include <span>
#include <vector>

#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Writes root rows and bounded blocks of ARRAY/MAP elements through the
/// ordinary splitter. Each element inherits its parent partition. ROW fields
/// share the parent's positions and inherit its nulls.
class CellNestedSplitter {
 public:
  /// Maximum elements per block when splitting an ARRAY or MAP.
  static constexpr uint32_t kElementBatchPositions = 64 << 10;

  /// `layout`, `splitter` and `pool` must outlive this nested splitter.
  CellNestedSplitter(
      const CellNestedLayout& layout,
      CellSplitter& splitter,
      uint32_t numPartitions,
      memory::MemoryPool& pool);

  /// Takes data columns starting at columnOffset: decodes them, classifies the
  /// nulls of the rows' columns and counts each column group's positions in
  /// this batch alone. Throws a user error once some group passes
  /// CellNestedLayout::kMaxPositions: no payload can carry the batch.
  void prepare(const RowVector& rv, uint32_t columnOffset);

  /// Whether the prepared batch's positions in every column group, added to
  /// the most a partition holds there, stay within kMaxPositions. Follows any
  /// seal first - loading lazy vectors and taking scratch in prepare() may
  /// have pushed an RSS window - so after a seal of the window the batch fits.
  bool fitsWindow();

  /// The rows' columns as prepared, per layout column: what the dictionary
  /// probe reads.
  std::span<const DecodedVector> decoded() const {
    return decoded_;
  }

  std::span<const NullClass> nullClass() const {
    return nullClass_;
  }

  /// Counts for an output call's null bodies, indexed by wire column.
  /// Columns in the same column group share a count array. The view and its
  /// arrays remain valid until destruction, including across releaseScratch().
  ColumnPositionCounts columnPositionCounts() const {
    return columnPositionCounts_;
  }

  /// Splits the prepared batch: the rows' columns at once, committing the
  /// rows, then every container's elements. `row2Partition` has an entry
  /// per prepared row, `partition2RowCount` one per partition.
  void split(
      std::span<const uint32_t> row2Partition,
      std::span<const uint32_t> partition2RowCount);

  /// Total wall time spent in prepare() and split(), including the ordinary
  /// splitter and any nested reclaims.
  uint64_t splitWallNs() const {
    return splitWallNs_;
  }

  /// Time spent in CellSplitter::split() writing the physical columns.
  uint64_t columnSplitWallNs() const {
    return columnSplitWallNs_;
  }

  /// True during nested preparation or expansion, excluding ordinary column
  /// writes, so spills can be attributed to the active phase.
  bool isSplitting() const {
    return inPrepareOrSplit_ && !inColumnSplit_;
  }

  /// Gives back the scratch kept across batches and returns the bytes the
  /// pool got back; the next batch takes it anew. Does nothing inside
  /// prepare() or split(), where a reclaim may come while a vector loads or
  /// scratch is taken.
  int64_t releaseScratch();

 private:
  using ContainerExpansion = CellNestedLayout::ContainerExpansion;

  struct PreparedNode {
    /// Keeps the source vector and its decoded base alive for this batch.
    VectorPtr source;
    DecodedVector sourceMapping;
    /// Value, ROW presence, or container length indexed by the decoded
    /// position.
    VectorPtr physicalValueBase;
    const vector_size_t* offsets{nullptr};
    const vector_size_t* sizes{nullptr};
    bool allNull{false};
    BufferPtr fieldNulls;
    BufferPtr structNulls;
    BufferPtr structIndices;
  };

  /// Selects input positions in output order and assigns each one a partition.
  /// Consecutive positions use only firstSourceIndex.
  struct ElementSelection {
    std::span<const uint32_t> partitionIds;
    vector_size_t firstSourceIndex{0};
    BufferPtr sourceIndices{nullptr};

    vector_size_t sourcePosition(uint32_t k) const {
      return sourceIndices ? sourceIndices->as<vector_size_t>()[k]
                           : firstSourceIndex + static_cast<vector_size_t>(k);
    }
  };

  /// Parent and child selections use separate buffers while recursing.
  struct ElementBuffers {
    BufferPtr partitionIds;
    BufferPtr sourceIndices;
  };

  /// Builds one selection at a time in the current depth's buffers.
  struct ElementBlock {
    ElementBlock(
        std::span<uint32_t> partitionIds,
        BufferPtr& sourceIndexBuffer,
        memory::MemoryPool& pool)
        : partitionIds_(partitionIds),
          sourceIndexBuffer_(sourceIndexBuffer),
          pool_(pool) {}
    void appendRange(vector_size_t offset, uint32_t count, uint32_t pid);
    ElementSelection takeSelection();

    uint32_t size{0};

   private:
    std::span<uint32_t> partitionIds_;
    BufferPtr& sourceIndexBuffer_;
    memory::MemoryPool& pool_;
    vector_size_t* sourceIndices_{nullptr};
    vector_size_t firstSourceIndex_{0};
  };

  /// Re-reads the seals: a sealed partition's counts restart at zero.
  void followSeals();

  /// Decodes `vector`, the entries of node n, and below n.
  void decodeNode(uint32_t n, const VectorPtr& vector);

  /// decodeNode() of a ROW: its presence and every field at its entries.
  void decodeRow(uint32_t n, const VectorPtr& vector);

  /// Decodes the vector of a leaf's column or of a ROW's presence.
  void decodeColumn(uint32_t n, const VectorPtr& vector);

  /// Field i of ROW n at the structs' positions, null wherever its struct
  /// is. `nulls` holds the structs' nulls, or is null when no struct is
  /// null.
  VectorPtr foldStructNulls(uint32_t n, uint32_t i, const BufferPtr& nulls);

  /// Marks node n and below as holding no entry this batch.
  void clearNode(uint32_t n);

  void countRange(
      const ContainerExpansion& expansion,
      vector_size_t begin,
      vector_size_t end);

  BOLT_CELL_KERNEL void expandContainer(
      const ContainerExpansion& expansion,
      const ElementSelection& parents);

  BOLT_CELL_KERNEL void splitElementBlock(
      uint32_t groupId,
      const ElementSelection& elements);

  NullClass viewRows(uint32_t col);
  NullClass prepareElementColumn(
      uint32_t col,
      const ElementSelection& elements);

  void splitGroupColumns(
      uint32_t groupId,
      std::span<const uint32_t> partitionIds,
      std::span<const uint32_t> positionsPerPartition);

  std::span<const uint32_t> indicesInPartition(
      std::span<const uint32_t> partitionIds,
      bool rootGroup);

  void commitElementCounts(uint32_t groupId);

  /// `n` elements of a pool buffer kept across batches.
  template <typename T>
  T* scratch(BufferPtr& buffer, size_t n);

  /// `buffer`, as scratch(), holding a copy of the `n` elements at `data`.
  template <typename T>
  const BufferPtr& copyToScratch(BufferPtr& buffer, const T* data, size_t n);

  const CellNestedLayout& layout_;
  CellSplitter& splitter_;
  const uint32_t numPartitions_;
  memory::MemoryPool& pool_;

  std::vector<PreparedNode> preparedNodes_;
  /// Per layout column: the rows' decoded column, or an element batch's view
  /// decoded.
  std::vector<DecodedVector> decoded_;
  std::vector<NullClass> nullClass_;
  std::vector<VectorPtr> columnViews_;
  /// A dictionary view's indices and nulls, per layout column.
  std::vector<BufferPtr> viewIndices_;
  std::vector<BufferPtr> viewNulls_;
  vector_size_t numRows_{0};

  /// Per element group, the window's counts by partition and their maximum.
  BufferPtr windowElementCountsBuffer_;
  std::vector<uint32_t*> windowElementsPerPartition_;
  std::vector<uint32_t> maxWindowElementsPerPartition_;
  /// One entry per physical column. Root columns use the splitter's row counts.
  std::vector<const uint32_t*> columnPositionCounts_;
  /// Elements counted in prepare() and committed in split(), per group.
  std::vector<uint64_t> batchElementCounts_;
  std::vector<uint64_t> committedElementCounts_;
  /// The splitter's sealCount() as last followed: a new seal zeroes the
  /// counts of the partitions it emptied.
  uint64_t lastSealCount_{0};

  /// A parent's element block stays live while splitting its children.
  std::vector<ElementBuffers> elementBuffersByDepth_;
  BufferPtr blockElementsPerPartition_;
  BufferPtr nextIndexInPartition_;
  std::vector<uint32_t> distinctBlockPartitionIds_;
  /// Shared by root rows and element blocks during synchronous column writes.
  BufferPtr indicesInPartitionBuffer_;
  /// The presence columns' values, and the identity indices.
  BufferPtr rowPresenceValues_;
  BufferPtr identityIndices_;
  /// Inside prepare() or split().
  bool inPrepareOrSplit_{false};
  /// Inside the ordinary splitter, nested within split().
  bool inColumnSplit_{false};

  uint64_t splitWallNs_{0};
  uint64_t columnSplitWallNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
