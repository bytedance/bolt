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

#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"

#include <algorithm>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

CellNestedLayout::Kind kindOf(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return CellNestedLayout::Kind::kArray;
    case TypeKind::MAP:
      return CellNestedLayout::Kind::kMap;
    case TypeKind::ROW:
      return CellNestedLayout::Kind::kRow;
    default:
      return CellNestedLayout::Kind::kLeaf;
  }
}

bool isComplex(const TypePtr& type) {
  return kindOf(type) != CellNestedLayout::Kind::kLeaf;
}

/// Counts every node below the top-level row, including UNKNOWN leaves.
void countColumns(const TypePtr& type, uint64_t& columns) {
  ++columns;
  if (isComplex(type)) {
    for (uint32_t i = 0; i < type->size(); ++i) {
      countColumns(type->childAt(i), columns);
    }
  }
}

} // namespace

// static
bool CellNestedLayout::supports(const RowTypePtr& rowType) {
  bool nested = false;
  uint64_t columns = 0;
  for (const auto& child : rowType->children()) {
    nested = nested || isComplex(child);
    countColumns(child, columns);
  }
  return nested && columns <= UINT16_MAX;
}

CellNestedLayout::CellNestedLayout(RowTypePtr rowType)
    : logicalType_(std::move(rowType)) {
  BOLT_CHECK(
      supports(logicalType_),
      "no nested Cell layout for {}",
      logicalType_->toString());
  groups_.push_back(ColumnGroup{0, {}, {}, {}, {}});
  for (const auto& child : logicalType_->children()) {
    topNodes_.push_back(addNode(child, 0));
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(nodes_.size());
  types.reserve(nodes_.size());
  nodeOfColumn_.reserve(nodes_.size());
  for (uint32_t n = 0; n < nodes_.size(); ++n) {
    auto& node = nodes_[n];
    if (node.kind == Kind::kLeaf && node.type->kind() == TypeKind::UNKNOWN) {
      continue;
    }
    const uint32_t layoutCol = static_cast<uint32_t>(types.size());
    node.layoutColumn = layoutCol;
    nodeOfColumn_.push_back(n);
    auto& group = groups_[node.group];
    group.columns.push_back(layoutCol);
    if (node.kind == Kind::kLeaf || node.kind == Kind::kRow) {
      node.valueColumn = group.valueColumns.size();
      group.valueColumns.push_back(layoutCol);
    } else {
      group.lengthColumns.push_back(layoutCol);
    }
    names.push_back(fmt::format("n{}", n));
    // A leaf's column has its own type, a container's its lengths, a
    // ROW's its presence.
    if (node.kind == Kind::kLeaf) {
      types.push_back(node.type);
    } else if (node.kind == Kind::kRow) {
      types.push_back(SMALLINT());
    } else {
      types.push_back(INTEGER());
    }
  }
  layout_ = CellLayout::create(ROW(std::move(names), std::move(types)));
}

uint32_t CellNestedLayout::addNode(const TypePtr& type, uint32_t group) {
  const auto id = static_cast<uint32_t>(nodes_.size());
  Node node;
  node.kind = kindOf(type);
  node.type = type;
  node.group = group;
  uint32_t childGroup = group;
  if (node.kind == Kind::kArray || node.kind == Kind::kMap) {
    childGroup = static_cast<uint32_t>(groups_.size());
    groups_[group].expansions.push_back({id, childGroup});
    const uint32_t depth = groups_[group].depth + 1;
    maxDepth_ = std::max(maxDepth_, depth);
    groups_.push_back(ColumnGroup{depth, {}, {}, {}, {}});
  }
  nodes_.push_back(std::move(node));
  for (uint32_t i = 0; i < type->size(); ++i) {
    // Adding the child may reallocate nodes_.
    const uint32_t child = addNode(type->childAt(i), childGroup);
    nodes_[id].children.push_back(child);
  }
  return id;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
