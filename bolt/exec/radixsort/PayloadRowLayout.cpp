/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/exec/radixsort/PayloadRowLayout.h"

#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

bool isComplex(const Type& type) {
  return type.kind() == TypeKind::ARRAY || type.kind() == TypeKind::MAP ||
      type.kind() == TypeKind::ROW;
}

bool isSupportedType(const Type& type, bool nested) {
  if (type.isDecimal()) {
    return true;
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (!isSupportedType(*type.childAt(child), true)) {
          return false;
        }
      }
      return true;
    case TypeKind::VARIANT:
      return nested;
    default:
      return false;
  }
}

std::optional<uint32_t> slotWidth(const Type& type) {
  if (type.isShortDecimal()) {
    return sizeof(int64_t);
  }
  if (type.isLongDecimal()) {
    return sizeof(int128_t);
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
      return sizeof(int128_t);
    case TypeKind::TIMESTAMP:
      return sizeof(Timestamp);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return sizeof(PayloadVarlenRef);
    case TypeKind::UNKNOWN:
      return 0;
    default:
      return std::nullopt;
  }
}

} // namespace

bool PayloadRowLayout::supports(const Type& type) {
  return isSupportedType(type, false);
}

std::shared_ptr<const PayloadRowLayout> PayloadRowLayout::create(
    const RowTypePtr& rowType) {
  if (rowType->size() == 0) {
    return nullptr;
  }

  const auto nullBytes = static_cast<uint32_t>((rowType->size() + 7) / 8);
  bool hasVariableFields = false;
  bool supportedTypes = true;
  std::string unsupportedType;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    if (!supports(*type)) {
      supportedTypes = false;
      unsupportedType = type->toString();
    }
    hasVariableFields |= type->kind() == TypeKind::VARCHAR ||
        type->kind() == TypeKind::VARBINARY || isComplex(*type);
  }
  BOLT_CHECK(
      supportedTypes,
      "Payload row layout is not implemented for ",
      unsupportedType);

  std::optional<uint64_t> variableSizeOffset;
  uint64_t rowWidth = nullBytes;
  if (hasVariableFields) {
    variableSizeOffset = rowWidth;
    rowWidth += sizeof(uint64_t);
  }

  std::vector<PayloadRowColumnLayout> columns;
  columns.reserve(rowType->size());
  bool supportedSlots = true;
  std::string unsupportedSlotType;
  bool rowWidthValid = true;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    const auto width = slotWidth(*type);
    if (!width.has_value()) {
      supportedSlots = false;
      unsupportedSlotType = type->toString();
      continue;
    }
    columns.push_back(PayloadRowColumnLayout{
        type,
        rowWidth,
        *width,
        column / 8,
        static_cast<uint8_t>(uint8_t{1} << (column % 8)),
        type->kind() == TypeKind::VARCHAR ||
            type->kind() == TypeKind::VARBINARY || isComplex(*type),
        isComplex(*type)});
    auto next = checkedAdd<uint64_t>(rowWidth, *width);
    rowWidthValid &= next.has_value();
    if (next.has_value()) {
      rowWidth = *next;
    }
  }
  BOLT_CHECK(
      supportedSlots,
      "Payload row slot is not implemented for ",
      unsupportedSlotType);
  BOLT_CHECK(rowWidthValid, "Payload row row width overflows");

  return std::shared_ptr<const PayloadRowLayout>(new PayloadRowLayout(
      rowType, std::move(columns), nullBytes, variableSizeOffset, rowWidth));
}

} // namespace bytedance::bolt::exec::radixsort
