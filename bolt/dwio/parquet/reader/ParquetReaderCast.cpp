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

#include "bolt/dwio/parquet/reader/ParquetReaderCast.h"

#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/type/filter/MapSubscriptFilter.h"

namespace bytedance::bolt::parquet {
namespace {

void checkReaderCastFilter(
    const TypePtr& fileType,
    const TypePtr& requestedType,
    const common::Filter* filter,
    const std::string& path) {
  if (isReaderCastFilterMismatch(fileType, requestedType) && filter &&
      !filter->isValueIndependent()) {
    BOLT_USER_FAIL(
        "Cannot apply {} filter to physical {} Parquet column {}",
        requestedType->kindName(),
        fileType->kindName(),
        path);
  }
}

} // namespace

bool isReaderCastFilterMismatch(
    const TypePtr& fileType,
    const TypePtr& requestedType) {
  const auto requestedKind = requestedType->kind();
  switch (fileType->kind()) {
    case TypeKind::REAL:
      // VARCHAR filters cannot be evaluated against REAL statistics.
      return requestedType->isVarchar();
    case TypeKind::DOUBLE:
      // VARCHAR and BIGINT filters cannot be evaluated against DOUBLE
      // statistics.
      return requestedType->isVarchar() || requestedKind == TypeKind::BIGINT;
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
      // DOUBLE filters cannot be evaluated against integer statistics.
      return requestedKind == TypeKind::DOUBLE;
    case TypeKind::INTEGER:
      // Reader casts change the interpretation of DATE-to-VARCHAR and
      // integer-to-DOUBLE filters.
      return requestedKind == TypeKind::DOUBLE ||
          (fileType->isDate() && requestedType->isVarchar());
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT: {
      if (!fileType->isDecimal() || !requestedType->isDecimal()) {
        return false;
      }
      const auto fileScale = getDecimalPrecisionScale(*fileType).second;
      const auto requestedScale =
          getDecimalPrecisionScale(*requestedType).second;
      // Precision-only widening preserves both raw values and storage width.
      return fileScale != requestedScale ||
          fileType->isShortDecimal() != requestedType->isShortDecimal();
    }
    default:
      return false;
  }
}

void validateReaderCastFilter(
    const TypePtr& fileType,
    const TypePtr& requestedType,
    const common::ScanSpec& scanSpec,
    const std::string& path) {
  auto* filter = scanSpec.filter();
  checkReaderCastFilter(fileType, requestedType, filter, path);
  if (isReaderCastFilterMismatch(fileType, requestedType) ||
      fileType->kind() != requestedType->kind()) {
    return;
  }

  if (fileType->isMap() && filter &&
      filter->kind() == common::FilterKind::kMapSubscript) {
    const auto* mapFilter =
        dynamic_cast<const common::MapSubscriptFilter*>(filter);
    BOLT_CHECK_NOT_NULL(mapFilter);
    const auto keyPath = path.empty()
        ? common::ScanSpec::kMapKeysFieldName
        : path + "." + common::ScanSpec::kMapKeysFieldName;
    checkReaderCastFilter(
        fileType->childAt(0),
        requestedType->childAt(0),
        mapFilter->keyFilter(),
        keyPath);
    const auto valuePath = path.empty()
        ? common::ScanSpec::kMapValuesFieldName
        : path + "." + common::ScanSpec::kMapValuesFieldName;
    checkReaderCastFilter(
        fileType->childAt(1),
        requestedType->childAt(1),
        mapFilter->valueFilter(),
        valuePath);
  }

  for (const auto& child : scanSpec.children()) {
    std::optional<uint32_t> fileChildIndex;
    std::optional<uint32_t> requestedChildIndex;
    if (fileType->isRow()) {
      fileChildIndex =
          fileType->asRow().getChildIdxIfExists(child->fieldName());
      requestedChildIndex =
          requestedType->asRow().getChildIdxIfExists(child->fieldName());
      if (!fileChildIndex || !requestedChildIndex) {
        continue;
      }
    } else if (fileType->isArray()) {
      fileChildIndex = requestedChildIndex = 0;
    } else if (fileType->isMap()) {
      fileChildIndex = requestedChildIndex =
          child->fieldName() == common::ScanSpec::kMapKeysFieldName ? 0 : 1;
    } else {
      continue;
    }

    const auto childPath =
        path.empty() ? child->fieldName() : path + "." + child->fieldName();
    validateReaderCastFilter(
        fileType->childAt(*fileChildIndex),
        requestedType->childAt(*requestedChildIndex),
        *child,
        childPath);
  }
}

} // namespace bytedance::bolt::parquet
