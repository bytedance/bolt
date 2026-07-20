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
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        makeCastExpr();
        break;
      }
      default:
        break;
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

  void getValues(const RowSet& rows, VectorPtr* result) override {
    bool needConvertion = (castExprSet_ && castExprSet_->size() != 0);
    auto& requestedType = needConvertion ? fileType_->type() : requestedType_;
    auto& fileType = static_cast<const ParquetTypeWithId&>(*fileType_);
    bool isUnsigned = false;
    if (fileType.logicalType_.has_value() &&
        fileType.logicalType_.value().__isset.INTEGER) {
      isUnsigned = !fileType.logicalType_.value().INTEGER.isSigned;
    }
#ifdef SPARK_COMPATIBLE
    if (!fileType.logicalType_.has_value() &&
        fileType.convertedType_ == thrift::ConvertedType::UINT_64) {
      // Legacy Parquet files may carry only the UINT_64 converted type. In
      // particular, convertType accepts these files as DECIMAL(20, 0). Without
      // this fallback, getIntValues() would use its HUGEINT path and interpret
      // each 8-byte physical UINT64 as a 16-byte int128_t. Use the unsigned
      // path to preserve the UINT64 bits and widen each value correctly.
      isUnsigned = true;
    }
#endif

    if (isUnsigned) {
      getUnsignedIntValues(rows, requestedType, result);
    } else {
      getIntValues(rows, requestedType, result);
    }

    if (needConvertion) {
      doCastEvaluate(result);
    }
  }

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
