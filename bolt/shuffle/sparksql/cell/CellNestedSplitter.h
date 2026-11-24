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
  /// Elements one split of a container column group covers at most.
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
  uint64_t shredWallNs() const {
    return shredWallNs_;
  }

  /// Total wall time spent in the ordinary splitter (the splitter portion of
  /// shredWallNs()).
  uint64_t wireWallNs() const {
    return wireWallNs_;
  }

  /// True while inside prepare() or split(), excluding ordinary column writes.
  /// Used so nested reclaims attribute their spill time to the shredding phase.
  bool isShredding() const {
    return busy_ && !inWireColumns_;
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
  /// A null index buffer represents consecutive positions starting at first.
  struct ElementSelection {
    std::span<const uint32_t> partitionIds;
    vector_size_t first{0};
    BufferPtr sourceIndices{nullptr};

    vector_size_t sourcePosition(uint32_t k) const {
      return sourceIndices ? sourceIndices->as<vector_size_t>()[k]
                           : first + static_cast<vector_size_t>(k);
    }
  };

  /// Child writes borrow a separate pair of buffers at the next depth.
  struct ElementBuffers {
    BufferPtr partitionIds;
    BufferPtr sourceIndices;
  };

  /// Builds one selection at a time in the current depth's buffers.
  struct ElementBlock {
    explicit ElementBlock(ElementBuffers& buffers);
    void appendRange(vector_size_t offset, uint32_t count, uint32_t pid);
    ElementSelection takeSelection();

    uint32_t size{0};

   private:
    uint32_t* const partitionIds_;
    vector_size_t* const indices_;
    const BufferPtr& indexBuffer_;
    bool contiguous_{true};
    vector_size_t first_{0};
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

  BOLT_CELL_KERNEL void writeElementBlock(
      uint32_t groupId,
      const ElementSelection& elements);

  NullClass viewRows(uint32_t col);
  NullClass prepareElementColumn(
      uint32_t col,
      const ElementSelection& elements);

  void writeGroupColumns(
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
  std::vector<VectorPtr> views_;
  /// A dictionary view's indices and nulls, per layout column.
  std::vector<BufferPtr> viewIndices_;
  std::vector<BufferPtr> viewNulls_;
  vector_size_t numRows_{0};

  /// Per column group: committed positions per partition (none for the rows),
  /// the largest, this batch's exact total and what was committed of it.
  BufferPtr countsBuffer_;
  std::vector<uint32_t*> counts_;
  /// Output view of counts_: nullptr entries use the splitter's row counts.
  std::vector<const uint32_t*> columnPositionCounts_;
  std::vector<uint32_t> maxCount_;
  std::vector<uint64_t> totals_;
  std::vector<uint64_t> committed_;
  /// The splitter's sealCount() as last followed: a new seal zeroes the
  /// counts of the partitions it emptied.
  uint64_t seals_{0};

  /// One element batch per depth: a batch stays open while the batches of
  /// the column groups below it split. Per partition, the current batch's
  /// positions and an element's running index; `touched_` lists the partitions
  /// the batch holds.
  std::vector<ElementBuffers> buffersByDepth_;
  BufferPtr elementCounts_;
  BufferPtr runningIndex_;
  std::vector<uint32_t> touched_;
  /// Shared by root rows and element blocks during synchronous column writes.
  BufferPtr rowIndex_;
  /// The presence columns' values, and the identity indices.
  BufferPtr ones_;
  BufferPtr identity_;
  /// Inside prepare() or split().
  bool busy_{false};
  /// Inside the ordinary splitter, nested within split().
  bool inWireColumns_{false};

  uint64_t shredWallNs_{0};
  uint64_t wireWallNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
