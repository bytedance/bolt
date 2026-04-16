/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/dwio/parquet/reader/VariantColumnReader.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::parquet {

VariantColumnReader::VariantColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool)
    : StructColumnReader(
          columnReaderOptions,
          requestedType,
          fileType,
          params,
          scanSpec,
          pool) {
  BOLT_CHECK(fileType->type()->isVariant() || fileType->type()->isRow());
}

void VariantColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  // If the requested type is not VARIANT, fallback to standard struct reading.
  if (!requestedType_->type()->isVariant()) {
    StructColumnReader::getValues(rows, result);
    return;
  }

  // Ensure result is a VariantVector
  if (!*result || !(*result)->type()->isVariant() ||
      (*result).use_count() > 1) {
    *result = VariantVector::create(
        &memoryPool_, requestedType_->type(), rows.size());
  }

  auto* variantVector = static_cast<VariantVector*>(result->get());
  variantVector->resize(rows.size());

  // Copy-paste logic from SelectiveStructColumnReaderBase::getValues
  // but specialized for VariantVector.

  if (!rows.size()) {
    resetWhenFilterAll();
    return;
  }

  if (nullsInReadRange_) {
    auto readerNulls = nullsInReadRange_->as<uint64_t>();
    auto* nulls =
        variantVector->mutableNulls(rows.size())->asMutable<uint64_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      bits::setBit(nulls, i, bits::isBitSet(readerNulls, rows[i]));
    }
  } else {
    variantVector->clearNulls(0, rows.size());
  }

  auto* valueSpec = scanSpec_->childByName(VariantVector::kValueChildName);
  auto* metadataSpec =
      scanSpec_->childByName(VariantVector::kMetadataChildName);
  auto valueChild =
      valueSpec ? valueSpec->subscript() : VariantVector::kValueChildIndex;
  auto metadataChild = metadataSpec ? metadataSpec->subscript()
                                    : VariantVector::kMetadataChildIndex;
  BOLT_CHECK_GE(valueChild, 0);
  BOLT_CHECK_GE(metadataChild, 0);
  BOLT_CHECK_LT(valueChild, children_.size());
  BOLT_CHECK_LT(metadataChild, children_.size());

  // Read children using the correct ScanSpec subscript ordering.
  // valueChild/metadataChild are the indices into children_ (the physical file
  // columns). We read each into the correct VariantVector child slot directly,
  // avoiding any post-hoc swap heuristic.
  auto& valueResult = variantVector->childAt(VariantVector::kValueChildIndex);
  children_[valueChild]->getValues(rows, &valueResult);
  auto& metadataResult =
      variantVector->childAt(VariantVector::kMetadataChildIndex);
  children_[metadataChild]->getValues(rows, &metadataResult);
}

} // namespace bytedance::bolt::parquet
