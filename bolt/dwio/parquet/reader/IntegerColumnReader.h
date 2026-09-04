/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/dwio/common/SelectiveIntegerColumnReader.h"
#include "bolt/dwio/parquet/reader/ParquetReaderCast.h"
namespace bytedance::bolt::parquet {

class IntegerColumnReader : public dwio::common::SelectiveIntegerColumnReader {
 public:
  IntegerColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(
            requestedType->type(),
            params,
            scanSpec,
            std::move(fileType)) {
    switch (requestedType->type()->kind()) {
      case TypeKind::VARCHAR: {
        makeCastExpr(fileType_->type()->isDate() ? INTEGER() : nullptr);
        break;
      }
      case TypeKind::VARBINARY: {
        makeCastExpr();
        break;
      }
      default:
        break;
    }
    const auto& physicalType = fileType_->type();
    const auto& outputType = requestedType->type();
    const auto readerCastFilterMismatch =
        isReaderCastFilterMismatch(physicalType, outputType);
    // Requested-type metadata filters cannot be evaluated against physical
    // decimal values when the scale or native storage width changes.
    if (requiresDecimalValueConversion(physicalType, outputType) ||
        (params.disableFloatingPointToVarcharMetadataFilter() &&
         readerCastFilterMismatch)) {
      formatData_->as<ParquetData>().disableTypeDependentMetadataFilters();
    }
  }

  bool hasBulkPath() const override {
    return !formatData_->as<ParquetData>().isDeltaBinaryPacked() &&
        !this->fileType().type()->isLongDecimal() &&
        ((this->fileType().type()->isShortDecimal())
             ? formatData_->as<ParquetData>().hasDictionary()
             : true);
  }

  void seekToRowGroup(int64_t index) override {
    SelectiveIntegerColumnReader::seekToRowGroup(index);
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override {
    formatData_->as<ParquetData>().skip(numValues);
    return numValues;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override;

  void read(
      int64_t offset,
      const RowSet& rows,
      const uint64_t* /*incomingNulls*/) override {
    BOLT_WIDTH_DISPATCH(
        parquetSizeOfIntKind(fileType_->type()->kind()),
        prepareRead,
        offset,
        rows,
        nullptr);
    readCommon<IntegerColumnReader>(rows);
    readOffset_ += rows.back() + 1;
  }

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor) {
    formatData_->as<ParquetData>().readWithVisitor(visitor);
  }
};

} // namespace bytedance::bolt::parquet
