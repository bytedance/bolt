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

//
// Created by Ying Su on 2/14/22.
//

#include "bolt/common/base/SparkCompatibility.h"

#include "bolt/dwio/parquet/reader/ParquetColumnReader.h"

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
#include "bolt/dwio/parquet/reader/BooleanColumnReader.h"
#include "bolt/dwio/parquet/reader/FloatingPointColumnReader.h"
#include "bolt/dwio/parquet/reader/IntegerColumnReader.h"
#include "bolt/dwio/parquet/reader/RepeatedColumnReader.h"
#include "bolt/dwio/parquet/reader/Statistics.h"
#include "bolt/dwio/parquet/reader/StringColumnReader.h"
#include "bolt/dwio/parquet/reader/StructColumnReader.h"
#include "bolt/dwio/parquet/reader/TimestampColumnReader.h"
#include "bolt/dwio/parquet/reader/VariantColumnReader.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::parquet {

void IntegerColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  bool needConvertion = (castExprSet_ && castExprSet_->size() != 0);
  auto& requestedType = needConvertion ? castSourceType_ : requestedType_;
  auto& fileType = static_cast<const ParquetTypeWithId&>(*fileType_);
  bool isUnsigned = false;
  if (fileType.logicalType_.has_value() &&
      fileType.logicalType_.value().__isset.INTEGER) {
    isUnsigned = !fileType.logicalType_.value().INTEGER.isSigned;
  }
  if (::bytedance::bolt::kSparkCompatible &&
      !fileType.logicalType_.has_value() &&
      fileType.convertedType_ == thrift::ConvertedType::UINT_64) {
    // Legacy Parquet files may carry only the UINT_64 converted type. In
    // particular, convertType accepts these files as DECIMAL(20, 0). Without
    // this fallback, getIntValues() would use its HUGEINT path and interpret
    // each 8-byte physical UINT64 as a 16-byte int128_t. Use the unsigned
    // path to preserve the UINT64 bits and widen each value correctly.
    isUnsigned = true;
  }

  if (isUnsigned) {
    getUnsignedIntValues(rows, requestedType, result);
  } else {
    getIntValues(rows, requestedType, result);
  }

  if (needConvertion) {
    doCastEvaluate(result);
  }
}

/* type matching restriction, only allow following type convert
 * real -> real/double/varchar
 * double -> double/varchar
 * varchar/varbinary -> varchar/varbinary
 * timestamp -> timestamp
 */
bool matchType(TypeKind schemaType, TypeKind requestType) {
  switch (schemaType) {
    case TypeKind::REAL:
      return requestType == TypeKind::REAL || requestType == TypeKind::DOUBLE ||
          requestType == TypeKind::VARCHAR;
    case TypeKind::DOUBLE:
      return requestType == TypeKind::DOUBLE ||
          requestType == TypeKind::VARCHAR;
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      if (requestType == TypeKind::VARCHAR ||
          requestType == TypeKind::VARBINARY) {
        return true;
      }
      return false;
    case TypeKind::TIMESTAMP:
      return requestType == TypeKind::TIMESTAMP;
    default:
      break;
  }
  return true;
}

bool isIntegerType(const Type& type) {
  return type.equivalent(*TINYINT()) || type.equivalent(*SMALLINT()) ||
      type.equivalent(*INTEGER()) || type.equivalent(*BIGINT()) ||
      type.equivalent(*HUGEINT());
}

bool maskSparkImplicitCast(
    const Type& fileType,
    const Type& requestedType,
    int64_t castMask) {
  using CastMask = dwio::common::ParquetReaderImplicitCastMask;
  if (!fileType.isPrimitiveType() || !requestedType.isPrimitiveType()) {
    return false;
  }
  if (fileType.equivalent(requestedType) ||
      castMask == static_cast<int64_t>(CastMask::kNone)) {
    return false;
  }
  if (castMask == static_cast<int64_t>(CastMask::kAll)) {
    return true;
  }
  const auto varcharIntegerMask =
      static_cast<int64_t>(CastMask::kVarcharInteger);
  const auto varcharDateMask = static_cast<int64_t>(CastMask::kVarcharDate);
  const auto fileKind = fileType.kind();
  const auto requestedKind = requestedType.kind();

  // varchar <-> integer
  const bool blockVarcharInteger = (castMask & varcharIntegerMask) != 0 &&
      ((fileKind == TypeKind::VARCHAR && isIntegerType(requestedType)) ||
       (isIntegerType(fileType) && requestedKind == TypeKind::VARCHAR));

  // varchar <-> date
  const bool blockVarcharDate = (castMask & varcharDateMask) != 0 &&
      ((fileKind == TypeKind::VARCHAR && requestedType.isDate()) ||
       (fileType.isDate() && requestedKind == TypeKind::VARCHAR));

  return blockVarcharInteger || blockVarcharDate;
}

// static
std::unique_ptr<dwio::common::SelectiveColumnReader> ParquetColumnReader::build(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool) {
  auto colName = scanSpec.fieldName();
  const bool canReadVariantStructAsVariant =
      requestedType->type()->isVariant() && fileType->type()->isRow() &&
      fileType->size() == 2 && fileType->containsChild("value") &&
      fileType->containsChild("metadata") &&
      fileType->childByName("value")->type()->isVarbinary() &&
      fileType->childByName("metadata")->type()->isVarbinary();

  if (::bytedance::bolt::kSparkCompatible) {
    BOLT_CHECK(
        canReadVariantStructAsVariant ||
            !maskSparkImplicitCast(
                *fileType->type(),
                *requestedType->type(),
                params.parquetReaderImplicitCastMask),
        "file schema type {} can not convert to ddl type {}",
        mapTypeKindToName(fileType->type()->kind()),
        mapTypeKindToName(requestedType->type()->kind()));
  } else {
    BOLT_CHECK(
        canReadVariantStructAsVariant ||
            matchType(fileType->type()->kind(), requestedType->type()->kind()),
        "file schema type {} can not convert to ddl type {}",
        mapTypeKindToName(fileType->type()->kind()),
        mapTypeKindToName(requestedType->type()->kind()));
  }

  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::HUGEINT:
      return std::make_unique<IntegerColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::REAL:
      if (requestedType->type()->kind() == TypeKind::VARCHAR) {
        return std::make_unique<FloatingPointColumnReader<float, float>>(
            requestedType->type(), fileType, params, scanSpec, REAL());
      } else if (requestedType->type()->kind() == TypeKind::REAL) {
        return std::make_unique<FloatingPointColumnReader<float, float>>(
            requestedType->type(), fileType, params, scanSpec);
      } else {
        return std::make_unique<FloatingPointColumnReader<float, double>>(
            requestedType->type(), fileType, params, scanSpec);
      }
    case TypeKind::DOUBLE:
      if ((::bytedance::bolt::kSparkCompatible &&
           requestedType->type()->kind() == TypeKind::BIGINT) ||
          requestedType->type()->kind() == TypeKind::VARCHAR) {
        return std::make_unique<FloatingPointColumnReader<double, double>>(
            requestedType->type(), fileType, params, scanSpec, DOUBLE());
      }
      return std::make_unique<FloatingPointColumnReader<double, double>>(
          requestedType->type(), fileType, params, scanSpec);

    case TypeKind::ROW:
      if (canReadVariantStructAsVariant) {
        return std::make_unique<VariantColumnReader>(
            columnReaderOptions,
            requestedType,
            fileType,
            params,
            scanSpec,
            pool);
      }
      return std::make_unique<StructColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::VARIANT:
      return std::make_unique<VariantColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return std::make_unique<StringColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::ARRAY:
      return std::make_unique<ListColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::MAP:
      return std::make_unique<MapColumnReader>(
          columnReaderOptions, requestedType, fileType, params, scanSpec, pool);

    case TypeKind::BOOLEAN:
      return std::make_unique<BooleanColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::TIMESTAMP: {
      const auto parquetType =
          std::static_pointer_cast<const ParquetTypeWithId>(fileType)
              ->parquetType_;
      BOLT_CHECK(parquetType);
      switch (parquetType.value()) {
        case thrift::Type::INT64:
          return std::make_unique<TimestampColumnReader<int64_t>>(
              requestedType, fileType, params, scanSpec);
        case thrift::Type::INT96:
          return std::make_unique<TimestampColumnReader<int128_t>>(
              requestedType, fileType, params, scanSpec);
        default:
          BOLT_UNREACHABLE();
      }
    }

    default:
      BOLT_FAIL(
          "buildReader unhandled type: " +
          mapTypeKindToName(fileType->type()->kind()));
  }
}

} // namespace bytedance::bolt::parquet
