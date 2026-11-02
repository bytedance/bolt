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

#include <cstdint>
#include <vector>

#include "bolt/shuffle/sparksql/cell/CellFormat.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// A row type with ARRAY, MAP and ROW columns as plain Cell columns: a
/// nested layout of CellFormat.h. Its type tree is shredded in pre-order,
/// one node per column: node n is column n of layout(), a leaf a column of
/// its type, an ARRAY or MAP an Integer column of its entries' lengths, null
/// where an entry is, a ROW a SmallInt column holding 1 at each non-null
/// struct. The payload's rows are space 0, and every container owns a space
/// of its own, numbered in pre-order, holding the elements of its non-null
/// entries; a ROW owns none. A node's column has the positions of the space
/// its parent owns or, below a ROW, the ROW's positions. Describes the
/// shredding only: no data passes through here.
class CellNestedLayout {
 public:
  static constexpr uint32_t kNone = ~0u;

  /// Positions a container's space holds in one payload at most, 2^31 - 8:
  /// they are read into one vector, whose null bits must round up to whole
  /// bytes within an int32 count.
  static constexpr uint32_t kMaxPositions = kMaxPayloadRows - 7;

  enum class Kind : uint8_t {
    kLeaf,
    kArray,
    kMap,
    kRow,
  };

  struct Node {
    Kind kind;
    /// The logical type: a leaf's is its column's.
    TypePtr type;
    /// The space the node's column has its positions in.
    uint32_t space;
    /// The column's index among its space's columns.
    uint32_t spaceColumn;
    /// A container's space; kNone for a leaf or a ROW.
    uint32_t ownedSpace{kNone};
    /// An ARRAY's element; a MAP's key, then its value; a ROW's fields.
    std::vector<uint32_t> children;
  };

  struct Space {
    /// The container whose elements the space holds; kNone for the rows.
    uint32_t owner;
    /// Containers between the space and the rows.
    uint32_t depth;
    /// The layout columns at the space's positions, ascending, UNKNOWN
    /// columns included.
    std::vector<uint32_t> columns;
    /// Those with a wire column: the columns a split of the space covers.
    std::vector<uint32_t> splitColumns;
    /// The containers among the columns, in node order.
    std::vector<uint32_t> containers;
    /// The row type of `columns`, a decoder's for the space.
    RowTypePtr rowType;
  };

  /// Whether `rowType` takes a nested layout: it has ARRAY, MAP or ROW
  /// columns and shreds into at most 65535 columns. Its leaf types are
  /// the callers' to check first (CellShuffleTypeAdapter::isSupported).
  /// Writer and reader decide alike from the same logical row type, unless
  /// both are configured to pack nested types instead.
  static bool supports(const RowTypePtr& rowType);

  /// `rowType` must be supported.
  explicit CellNestedLayout(RowTypePtr rowType);

  const RowTypePtr& logicalType() const {
    return logicalType_;
  }

  /// The shredded columns' layout, a plain one.
  const CellLayout& layout() const {
    return layout_;
  }

  uint32_t numNodes() const {
    return static_cast<uint32_t>(nodes_.size());
  }

  const Node& node(uint32_t n) const {
    return nodes_[n];
  }

  /// The node of each top-level column.
  const std::vector<uint32_t>& topNodes() const {
    return topNodes_;
  }

  uint32_t numSpaces() const {
    return static_cast<uint32_t>(spaces_.size());
  }

  const Space& space(uint32_t s) const {
    return spaces_[s];
  }

  /// The deepest space's depth.
  uint32_t maxDepth() const {
    return maxDepth_;
  }

  /// The space of each wire column's positions.
  const std::vector<uint32_t>& wireSpaces() const {
    return wireSpaces_;
  }

 private:
  /// Adds `type`'s node, at the positions of `space`, and its subtree.
  uint32_t addNode(const TypePtr& type, uint32_t space);

  RowTypePtr logicalType_;
  std::vector<Node> nodes_;
  std::vector<uint32_t> topNodes_;
  std::vector<Space> spaces_;
  uint32_t maxDepth_{0};
  CellLayout layout_;
  std::vector<uint32_t> wireSpaces_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
