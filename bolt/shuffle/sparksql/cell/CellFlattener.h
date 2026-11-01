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

#include <vector>

#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/cell/CellSplitter.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// The writer side of a nested layout: splits a batch's rows, and then the
/// elements of its containers space by space, through the splitter's column
/// subsets. The rows are the window's, which the splitter counts; the
/// positions of every other space it counts here, per partition, and hands
/// to the splitter as the columns' position bases and position counts.
///
/// A ROW is read as columns at its own positions: its presence, 1 at each
/// non-null struct, and its fields with the structs' nulls folded in, so
/// that every column below a null struct is null there and every container
/// below it holds no element. A flat field of flat structs, as long as
/// they are, keeps its values under its nulls and theirs, a field of a
/// constant struct is a constant, and any other field takes the structs'
/// indices and nulls through a dictionary.
///
/// A container space is split in chunks of up to `chunkPositions` elements,
/// in the order of the entries holding them, and each chunk's counts are
/// committed before the next chunk of the space is split, and before the
/// elements of the chunk's own containers are. A chunk's columns are views
/// of the input: a slice where its elements lie in a row in a flat vector,
/// else a dictionary over the decoded base. After a seal the counts of the
/// partitions it emptied restart at zero.
///
/// Scratch comes from the writer's pool. What a batch takes past the larger
/// of its rows and a chunk - a ROW below a container takes it per element -
/// goes back with the batch; the rest is kept across batches until
/// releaseScratch().
class CellFlattener {
 public:
  /// Elements one split of a container space covers at most, by default.
  static constexpr uint32_t kChunkPositions = 64 << 10;

  /// Sets the splitter's column positions. `layout`, `splitter` and `pool`
  /// must outlive the flattener, and the flattener every output call of the
  /// splitter.
  CellFlattener(
      const CellNestedLayout& layout,
      CellSplitter& splitter,
      uint32_t numPartitions,
      memory::MemoryPool& pool,
      uint32_t chunkPositions = kChunkPositions);

  /// Takes the next batch, pid column first: decodes it, classifies the
  /// nulls of the rows' columns and counts each space's positions. The
  /// count follows any seal the decoding brought about.
  void prepare(const RowVector& rv);

  /// Whether the prepared batch's positions in some space, added to the
  /// most a partition holds there, would pass CellNestedLayout's
  /// kMaxPositions.
  bool exceedsPositions() const {
    return exceeds_;
  }

  /// The rows' columns as prepared, per layout column: what the dictionary
  /// probe reads.
  const DecodedVector* decoded() const {
    return decoded_.data();
  }

  const BatchNullClass* nullClass() const {
    return nullClass_.data();
  }

  /// Whether a layout column has the rows' positions.
  bool inRowSpace(uint32_t col) const {
    return layout_.node(col).space == 0;
  }

  /// Splits the prepared batch: the rows' columns at once, committing the
  /// rows, then every container's elements.
  void split(const uint32_t* row2Partition, const uint32_t* partition2RowCount);

  /// Time spent flattening since the last call, the splitter's own excluded.
  uint64_t takeFlattenTimeNs() {
    const uint64_t ns = flattenNs_;
    flattenNs_ = 0;
    return ns;
  }

  /// Gives back the scratch kept across batches and returns the bytes the
  /// pool got back; the next batch takes it anew. Does nothing inside
  /// prepare() or split(), where a reclaim may come while a vector loads or
  /// scratch is taken.
  int64_t releaseScratch();

 private:
  /// What a node's column reads in the current batch.
  struct NodeInput {
    /// The vector the node's entries index: a top-level column, an element,
    /// key or value vector of its parent's base, or a field of its parent
    /// ROW's, the structs' nulls folded in; a ROW's presence. Null when the
    /// parent holds no element in this batch.
    VectorPtr vector;
    DecodedVector decoded;
    /// What a view of the column wraps: a leaf's or a ROW's decoded base; a
    /// container's lengths, an Integer vector over its base's sizes.
    VectorPtr values;
    /// A container's base offsets and sizes.
    const vector_size_t* offsets{nullptr};
    const vector_size_t* sizes{nullptr};
    /// Every entry null, so no view is needed.
    bool allNull{false};
    /// A field's nulls and its structs', combined, where the field stays
    /// flat; a ROW's structs' nulls and indices, where its fields index
    /// them through a dictionary.
    BufferPtr fieldNulls;
    BufferPtr structNulls;
    BufferPtr structIndices;
  };

  /// The entries of a space being split: the rows, or a chunk.
  struct Entries {
    const uint32_t* pid;
    /// Entry k's index into the vectors of the space's nodes: src[k], or
    /// start + k when src is null.
    const vector_size_t* src;
    vector_size_t start;
    uint32_t size;
    uint32_t depth;

    vector_size_t index(uint32_t k) const {
      return src != nullptr ? src[k] : start + static_cast<vector_size_t>(k);
    }
  };

  /// One chunk of a space at some depth.
  struct Chunk {
    BufferPtr pids;
    BufferPtr rels;
    BufferPtr srcs;
    uint32_t size{0};
    /// The elements lie in a row from `start`; src is unwritten then.
    bool contiguous{true};
    vector_size_t start{0};
  };

  /// Re-reads the seals: a sealed partition's counts restart at zero.
  void followSeals();

  /// Decodes `vector`, the entries of node n, and below n.
  void inputNode(uint32_t n, const VectorPtr& vector);

  /// inputNode() of a ROW: its presence and every field at its entries.
  void inputRow(uint32_t n, const VectorPtr& vector);

  /// Decodes the vector of a leaf's column or of a ROW's presence.
  void inputColumn(uint32_t n, const VectorPtr& vector);

  /// Field i of ROW n's structs, as decoded, at the structs' entries and
  /// with their nulls there, `nulls`, folded in; `nulls` is null when no
  /// struct is.
  VectorPtr fold(uint32_t n, uint32_t i, const BufferPtr& nulls);

  /// `size` SmallInt values of 1, and 0 to `size` - 1: buffers that grow
  /// as a batch needs, shared by every vector over them.
  const BufferPtr& ones(vector_size_t size);
  const BufferPtr& identity(vector_size_t size);

  /// Marks node n and below as holding no entry this batch.
  void clearNode(uint32_t n);

  /// Adds to each space below container x the positions of x's entries at
  /// [begin, end) of its vector; false once some space exceeds.
  bool countRange(uint32_t x, vector_size_t begin, vector_size_t end);

  /// Splits the elements of x's non-null entries among `parent`'s.
  void emit(uint32_t x, const Entries& parent);

  /// Splits a full or last chunk of space t, commits it, then its
  /// containers' elements.
  void flush(uint32_t t, Chunk& chunk, uint32_t depth);

  /// Builds and decodes the view of `chunk` in column col.
  BatchNullClass view(uint32_t col, const Chunk& chunk);

  void splitColumns(const SplitBatch& batch);

  /// `n` elements of a pool buffer kept across batches.
  template <typename T>
  T* scratch(BufferPtr& buffer, size_t n);

  const CellNestedLayout& layout_;
  CellSplitter& splitter_;
  const uint32_t numPartitions_;
  memory::MemoryPool& pool_;
  const uint32_t chunkPositions_;

  std::vector<NodeInput> inputs_;
  /// Per layout column: the rows' decoded column, or a chunk's view decoded.
  std::vector<DecodedVector> decoded_;
  std::vector<BatchNullClass> nullClass_;
  std::vector<VectorPtr> views_;
  /// A dictionary view's indices and nulls, per layout column.
  std::vector<BufferPtr> viewIndices_;
  std::vector<BufferPtr> viewNulls_;
  vector_size_t numRows_{0};

  /// Per space: committed positions per partition (none for the rows), the
  /// largest, this batch's exact total and what was committed of it.
  BufferPtr countsBuffer_;
  std::vector<uint32_t*> counts_;
  std::vector<uint32_t> maxCount_;
  std::vector<uint64_t> totals_;
  std::vector<uint64_t> committed_;
  bool exceeds_{false};
  uint64_t seals_{0};

  /// Chunks by depth, and per partition a chunk's entries and their running
  /// index; `touched` lists the partitions a chunk holds.
  std::vector<Chunk> chunks_;
  BufferPtr counters_;
  BufferPtr ordinals_;
  std::vector<uint32_t> touched_;
  /// The rows' index within their partition, when a column needs it.
  BufferPtr rowIndex_;
  /// The presence columns' values, and the identity indices.
  BufferPtr ones_;
  BufferPtr identity_;
  /// Inside prepare() or split().
  bool busy_{false};

  uint64_t flattenNs_{0};
  uint64_t splitNs_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
