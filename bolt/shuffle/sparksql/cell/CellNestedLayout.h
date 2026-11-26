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

/// The nested layout (CellFormat.h) of a row type with ARRAY, MAP or ROW
/// columns. Each node except an UNKNOWN leaf contributes one physical column
/// in pre-order.
/// A leaf is a column of its own type; an ARRAY or MAP an Integer column of
/// each value's length, null where the value is; a ROW a SmallInt presence
/// column, 1 at each non-null struct.
///
/// Columns that share the same row/element cardinality are grouped into a
/// ColumnGroup:
/// - Group 0 holds the payload's top-level rows.
/// - Each ARRAY or MAP owns a new child ColumnGroup (numbered in pre-order)
///   holding the elements of its non-null entries.
/// - A ROW owns no group; its fields stay in the ROW's group.
///
/// Example:
///   ROW<id: BIGINT, tags: ARRAY<VARCHAR>,
///       items: ARRAY<ROW<k: INT, m: MAP<INT, DOUBLE>>>>
///
///   Pre-order nodes -> flat CellLayout columns (c0..c8):
///     n0 (kLeaf,  id)    -> c0: BIGINT              [Group 0]
///     n1 (kArray, tags)  -> c1: INT (lengths)       [Group 0] -> owns Group 1
///     n2 (kLeaf,  tag)   -> c2: VARCHAR             [Group 1]
///     n3 (kArray, items) -> c3: INT (lengths)       [Group 0] -> owns Group 2
///     n4 (kRow,   item)  -> c4: SMALLINT (presence) [Group 2] (owns no group)
///     n5 (kLeaf,  k)     -> c5: INT                 [Group 2]
///     n6 (kMap,   m)     -> c6: INT (lengths)       [Group 2] -> owns Group 3
///     n7 (kLeaf,  key)   -> c7: INT                 [Group 3]
///     n8 (kLeaf,  val)   -> c8: DOUBLE              [Group 3]
///
///   ColumnGroups (sibling ARRAYs share depth=1 but have separate groups
///   because their element counts M1 and M2 are independent):
///     Group 0 (depth=0, N rows): columns=[c0, c1, c3]
///       |- sum(c1) = M1 -> Group 1 (depth=1): columns=[c2]
///       `- sum(c3) = M2 -> Group 2 (depth=1): columns=[c4, c5, c6]
///                            `- sum(c6) = M3 -> Group 3 (depth=2): [c7, c8]
class CellNestedLayout {
 public:
  static constexpr uint32_t kNone = ~0u;

  /// Positions a container's column group holds in one payload at most,
  /// 2^31 - 8: they are read into one vector, whose null bits must round up
  /// to whole bytes within an int32 count.
  static constexpr uint32_t kMaxPositions = kMaxPayloadRows - 7;

  enum class Kind : uint8_t {
    kLeaf,
    kArray,
    kMap,
    kRow,
  };

  struct Node {
    Kind kind;
    /// The logical type: a non-UNKNOWN leaf's is its column's.
    TypePtr type;
    /// The column group the node's column has its positions in.
    uint32_t group;
    /// The node's column index in layout(), or kNone for UNKNOWN leaves.
    uint32_t layoutColumn{kNone};
    /// The column's index in the group's value projection (leaves and ROWs).
    uint32_t valueColumn{kNone};
    /// An ARRAY's element; a MAP's key, then its value; a ROW's fields.
    std::vector<uint32_t> children;
  };

  /// An ARRAY or MAP node and the group containing its elements.
  struct ContainerExpansion {
    uint32_t container;
    uint32_t elementGroup;
  };

  /// Physical columns sharing one row/element position count.
  struct ColumnGroup {
    /// Containers between this column group and the top-level rows.
    uint32_t depth;
    /// The layout() columns at this group's positions, ascending (UNKNOWN
    /// leaves omitted).
    std::vector<uint32_t> columns;
    /// Expansions from this group to its element groups, in node order.
    std::vector<ContainerExpansion> expansions;
    /// Leaves and ROW presence columns that the reader appends batch by batch.
    std::vector<uint32_t> valueColumns;
    /// ARRAY/MAP lengths in the same order as expansions.
    std::vector<uint32_t> lengthColumns;
  };

  /// Whether `rowType` takes a nested layout: it has ARRAY, MAP or ROW
  /// columns and at most 65535 nodes below the top-level row. Callers check
  /// the leaf types first with CellShuffleTypeAdapter::isSupported().
  /// Writer and reader decide alike from the same logical row type, unless
  /// both are configured to pack nested types instead.
  static bool supports(const RowTypePtr& rowType);

  /// `rowType` must be supported.
  explicit CellNestedLayout(RowTypePtr rowType);

  const RowTypePtr& logicalType() const {
    return logicalType_;
  }

  /// Cell layout of the physical columns.
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

  uint32_t numGroups() const {
    return static_cast<uint32_t>(groups_.size());
  }

  const ColumnGroup& group(uint32_t g) const {
    return groups_[g];
  }

  /// The deepest column group's depth.
  uint32_t maxDepth() const {
    return maxDepth_;
  }

  /// Node index owning layout() column `col`.
  uint32_t nodeIndexOfColumn(uint32_t col) const {
    return nodeOfColumn_[col];
  }

  /// Node owning layout() column `col`.
  const Node& nodeOfColumn(uint32_t col) const {
    return nodes_[nodeOfColumn_[col]];
  }

  /// Whether a layout column has the rows' positions (group 0).
  bool inRowGroup(uint32_t col) const {
    return nodes_[nodeOfColumn_[col]].group == 0;
  }

 private:
  /// Adds `type`'s node, at the positions of `group`, and its subtree.
  uint32_t addNode(const TypePtr& type, uint32_t group);

  RowTypePtr logicalType_;
  std::vector<Node> nodes_;
  std::vector<uint32_t> nodeOfColumn_;
  std::vector<uint32_t> topNodes_;
  std::vector<ColumnGroup> groups_;
  uint32_t maxDepth_{0};
  CellLayout layout_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
