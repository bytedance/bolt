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

bool isComplex(const TypePtr& type) {
  return type->kind() == TypeKind::ARRAY || type->kind() == TypeKind::MAP ||
      type->kind() == TypeKind::ROW;
}

/// Adds the columns `type` flattens into to `columns`; false when it holds a
/// leaf no Cell column carries.
bool countColumns(const TypePtr& type, uint64_t& columns) {
  ++columns;
  if (!isComplex(type)) {
    return CellLayout::isSupportedType(type);
  }
  for (uint32_t i = 0; i < type->size(); ++i) {
    if (!countColumns(type->childAt(i), columns)) {
      return false;
    }
  }
  return true;
}

/// The type of a node's column: a leaf's own, a container's lengths, a
/// ROW's presence.
TypePtr columnType(const CellNestedLayout::Node& node) {
  switch (node.kind) {
    case CellNestedLayout::Kind::kLeaf:
      return node.type;
    case CellNestedLayout::Kind::kRow:
      return SMALLINT();
    default:
      return INTEGER();
  }
}

} // namespace

// static
bool CellNestedLayout::supports(const RowTypePtr& rowType) {
  bool nested = false;
  uint64_t columns = 0;
  for (const auto& child : rowType->children()) {
    nested = nested || isComplex(child);
    if (!countColumns(child, columns)) {
      return false;
    }
  }
  return nested && columns <= UINT16_MAX;
}

CellNestedLayout::CellNestedLayout(RowTypePtr rowType)
    : logicalType_(std::move(rowType)) {
  BOLT_CHECK(
      supports(logicalType_),
      "no nested Cell layout for {}",
      logicalType_->toString());
  spaces_.push_back(Space{kNone, 0, {}, {}, {}, nullptr});
  for (const auto& child : logicalType_->children()) {
    topNodes_.push_back(addNode(child, 0));
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(nodes_.size());
  types.reserve(nodes_.size());
  for (uint32_t n = 0; n < nodes_.size(); ++n) {
    names.push_back(fmt::format("n{}", n));
    types.push_back(columnType(nodes_[n]));
  }
  for (auto& space : spaces_) {
    std::vector<std::string> spaceNames;
    std::vector<TypePtr> spaceTypes;
    for (const auto col : space.columns) {
      spaceNames.push_back(names[col]);
      spaceTypes.push_back(types[col]);
      if (types[col]->kind() != TypeKind::UNKNOWN) {
        space.splitColumns.push_back(col);
      }
    }
    space.rowType = ROW(std::move(spaceNames), std::move(spaceTypes));
  }
  layout_ = CellLayout::create(ROW(std::move(names), std::move(types)));
  wireSpaces_.reserve(layout_.numWireColumns());
  for (uint32_t wire = 0; wire < layout_.numWireColumns(); ++wire) {
    wireSpaces_.push_back(nodes_[layout_.logicalColumn(wire)].space);
  }
}

uint32_t CellNestedLayout::addNode(const TypePtr& type, uint32_t space) {
  const auto id = static_cast<uint32_t>(nodes_.size());
  Node node;
  node.kind = type->kind() == TypeKind::ARRAY ? Kind::kArray
      : type->kind() == TypeKind::MAP         ? Kind::kMap
      : type->kind() == TypeKind::ROW         ? Kind::kRow
                                              : Kind::kLeaf;
  node.type = type;
  node.space = space;
  node.spaceColumn = static_cast<uint32_t>(spaces_[space].columns.size());
  spaces_[space].columns.push_back(id);
  if (node.kind == Kind::kArray || node.kind == Kind::kMap) {
    node.ownedSpace = static_cast<uint32_t>(spaces_.size());
    spaces_[space].containers.push_back(id);
    const uint32_t depth = spaces_[space].depth + 1;
    maxDepth_ = std::max(maxDepth_, depth);
    spaces_.push_back(Space{id, depth, {}, {}, {}, nullptr});
  }
  nodes_.push_back(std::move(node));
  if (nodes_[id].kind != Kind::kLeaf) {
    // A ROW's fields have its positions, a container's children those of
    // its space.
    const uint32_t childSpace =
        nodes_[id].kind == Kind::kRow ? space : nodes_[id].ownedSpace;
    for (uint32_t i = 0; i < type->size(); ++i) {
      const uint32_t child = addNode(type->childAt(i), childSpace);
      nodes_[id].children.push_back(child);
    }
  }
  return id;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
