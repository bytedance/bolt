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

#include "bolt/dwio/lance/NativeLanceScanPlan.h"

#include <algorithm>

namespace bytedance::bolt::lance::reader {
namespace {

uint64_t estimateTypeBytesPerRow(const TypePtr& type) {
  constexpr uint64_t kNullOverhead = 1;
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView) + 16 + kNullOverhead;
    case TypeKind::ARRAY:
      return 2 * sizeof(vector_size_t) +
          estimateTypeBytesPerRow(type->childAt(0)) + kNullOverhead;
    case TypeKind::MAP:
      return 2 * sizeof(vector_size_t) +
          estimateTypeBytesPerRow(type->childAt(0)) +
          estimateTypeBytesPerRow(type->childAt(1)) + kNullOverhead;
    case TypeKind::ROW: {
      uint64_t size = kNullOverhead;
      for (uint32_t i = 0; i < type->size(); ++i) {
        size += estimateTypeBytesPerRow(type->childAt(i));
      }
      return size;
    }
    case TypeKind::UNKNOWN:
      return kNullOverhead;
    default:
      return type->cppSizeInBytes() + kNullOverhead;
  }
}

uint64_t estimateReadBytesPerRow(
    const RowTypePtr& fileType,
    const dwio::common::RowReaderOptions& options) {
  uint64_t estimate = 0;
  if (const auto& scanSpec = options.getScanSpec()) {
    for (const auto& child : scanSpec->children()) {
      if (!child->isConstant()) {
        estimate +=
            estimateTypeBytesPerRow(fileType->findChild(child->fieldName()));
      }
    }
  } else if (const auto& selector = options.getSelector()) {
    const auto selectedType = selector->buildSelectedReordered();
    for (const auto& type : selectedType->children()) {
      estimate += estimateTypeBytesPerRow(type);
    }
  } else {
    estimate = estimateTypeBytesPerRow(fileType);
  }
  return std::max<uint64_t>(estimate, 1);
}

} // namespace

std::unique_ptr<NativeLanceScanPlan> NativeLanceScanPlan::build(
    const NativeLanceFileContext& context,
    const dwio::common::RowReaderOptions& options) {
  const auto& metadata = context.metadata();
  auto rootColumnReader =
      NativeLanceRootColumnReader::buildRoot(metadata, options);
  auto requiredColumns = rootColumnReader->fileColumnIndices();
  if (const auto& scanSpec = options.getScanSpec()) {
    for (const auto& child : scanSpec->children()) {
      if (!child->isConstant() && child->hasFilter()) {
        requiredColumns.push_back(
            metadata.rowType()->getChildIdx(child->fieldName()));
      }
    }
  }
  std::sort(requiredColumns.begin(), requiredColumns.end());
  requiredColumns.erase(
      std::unique(requiredColumns.begin(), requiredColumns.end()),
      requiredColumns.end());
  metadata.loadLogicalColumns(requiredColumns);

  std::unordered_map<uint32_t, std::unique_ptr<NativeLanceColumnReader>>
      filterColumnReaders;
  if (const auto& scanSpec = options.getScanSpec()) {
    for (const auto& child : scanSpec->children()) {
      if (child->isConstant() || !child->hasFilter()) {
        continue;
      }
      const auto columnIndex =
          metadata.rowType()->getChildIdx(child->fieldName());
      filterColumnReaders.emplace(
          columnIndex,
          NativeLanceColumnReader::buildFileColumn(metadata, columnIndex));
    }
  }

  return std::unique_ptr<NativeLanceScanPlan>(new NativeLanceScanPlan(
      std::move(rootColumnReader),
      std::move(filterColumnReaders),
      metadata.rowRangesForFileRange(options.getOffset(), options.getLimit()),
      estimateReadBytesPerRow(metadata.rowType(), options)));
}

NativeLanceScanPlan::NativeLanceScanPlan(
    std::unique_ptr<NativeLanceRootColumnReader> rootColumnReader,
    std::unordered_map<uint32_t, std::unique_ptr<NativeLanceColumnReader>>
        filterColumnReaders,
    std::vector<std::pair<uint64_t, uint64_t>> rowRanges,
    uint64_t estimatedBytesPerRow)
    : rootColumnReader_(std::move(rootColumnReader)),
      filterColumnReaders_(std::move(filterColumnReaders)),
      rowRanges_(std::move(rowRanges)),
      estimatedBytesPerRow_(estimatedBytesPerRow) {}

} // namespace bytedance::bolt::lance::reader
