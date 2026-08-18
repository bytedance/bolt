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

#include "bolt/duckdb/conversion/DuckWrapper.h"
#include "bolt/duckdb/DuckDbCompat.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/duckdb/conversion/DuckConversion.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::duckdb {
using ::duckdb::Connection;
using ::duckdb::DataChunk;
using ::duckdb::DuckDB;
using ::duckdb::Hugeint;
using ::duckdb::hugeint_t;
using ::duckdb::LogicalTypeId;
using ::duckdb::PhysicalType;
using ::duckdb::QueryResult;

namespace {

class DuckDBBufferReleaser {
 public:
  explicit DuckDBBufferReleaser(
      ::duckdb::buffer_ptr<::duckdb::VectorBuffer> buffer)
      : buffer_(std::move(buffer)) {}

  void addRef() const {}
  void release() const {}

 private:
  const ::duckdb::buffer_ptr<::duckdb::VectorBuffer> buffer_;
};

class DuckDBValidityReleaser {
 public:
  explicit DuckDBValidityReleaser(const ::duckdb::ValidityMask& validity)
      : validity_(validity) {}

  void addRef() const {}
  void release() const {}

 private:
  const ::duckdb::ValidityMask validity_;
};

} // namespace

DuckDBWrapper::DuckDBWrapper(core::ExecCtx* context, const char* path)
    : context_(context) {
  db_ = std::make_unique<DuckDB>(path);
  connection_ = std::make_unique<Connection>(*db_);
}

DuckDBWrapper::~DuckDBWrapper() {}

std::unique_ptr<DuckResult> DuckDBWrapper::execute(const std::string& query) {
  auto duckResult = connection_->Query(query);
  return std::make_unique<DuckResult>(context_, std::move(duckResult));
}

void DuckDBWrapper::print(const std::string& query) {
  auto result = connection_->Query(query);
  result->Print();
}

DuckResult::DuckResult(
    core::ExecCtx* context,
    std::unique_ptr<QueryResult> queryResult)
    : context_(context), queryResult_(std::move(queryResult)) {
  auto columnCount = queryResult_->types.size();

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnCount);
  types.reserve(columnCount);
  for (auto i = 0; i < columnCount; i++) {
    types.push_back(getType(i));
    names.push_back(getName(i));
  }
  type_ = std::make_shared<RowType>(std::move(names), std::move(types));
}

DuckResult::~DuckResult() {}

bool DuckResult::success() {
  return queryResult_->success;
}

std::string DuckResult::errorMessage() {
  return queryResult_->error;
}

RowVectorPtr DuckResult::getVector() {
  auto rowType = getType();
  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(columnCount());
  for (auto i = 0; i < columnCount(); i++) {
    outputColumns.push_back(getVector(i));
  }

  return std::make_shared<RowVector>(
      context_->pool(),
      rowType,
      BufferPtr(nullptr),
      currentChunk_->size(),
      outputColumns);
}

TypePtr DuckResult::getType(size_t columnIdx) {
  assert(columnIdx < queryResult_->types.size());
  return toBoltType(queryResult_->types[columnIdx]);
}

std::string DuckResult::getName(size_t columnIdx) {
  assert(columnIdx < queryResult_->names.size());
  return queryResult_->names[columnIdx];
}

inline bool isZeroCopyEligible(const ::duckdb::LogicalType& duckType) {
  if (duckType.id() == LogicalTypeId::DECIMAL) {
    if (duckType.InternalType() == PhysicalType::INT64 ||
        duckType.InternalType() == PhysicalType::INT128) {
      return true;
    }
    return false;
  }

  if (duckType.id() == LogicalTypeId::HUGEINT ||
      duckType.id() == LogicalTypeId::TIMESTAMP ||
      duckType.id() == LogicalTypeId::BOOLEAN ||
      duckType.id() == LogicalTypeId::BLOB ||
      duckType.id() == LogicalTypeId::VARCHAR) {
    return false;
  }
  return true;
}

template <class OP>
VectorPtr convert(
    ::duckdb::Vector& duckVector,
    const TypePtr& boltType,
    size_t size,
    memory::MemoryPool* pool,
    uint8_t* validity = nullptr) {
  auto vectorType = duckVector.GetVectorType();
  switch (vectorType) {
    case ::duckdb::VectorType::FLAT_VECTOR: {
      VectorPtr result;
      auto& duckValidity = ::duckdb::FlatVector::Validity(duckVector);
      auto* duckData =
          ::duckdb::FlatVector::GetData<typename OP::DUCK_TYPE>(duckVector);

      // Some DuckDB vectors have different internal layout and cannot be
      // trivially copied.
      if (!isZeroCopyEligible(duckVector.GetType())) {
        // TODO Figure out how to perform a zero-copy conversion.
        result = BaseVector::create(boltType, size, pool);
        auto flatResult = result->as<FlatVector<typename OP::BOLT_TYPE>>();

        for (auto i = 0; i < size; i++) {
          if (duckValidity.RowIsValid(i) &&
              (!validity || bits::isBitSet(validity, i))) {
            flatResult->set(i, OP::toBolt(duckData[i]));
          }
        }

        if (!duckValidity.AllValid()) {
          auto rawNulls = flatResult->mutableRawNulls();
          memcpy(rawNulls, duckValidity.GetData(), bits::nbytes(size));
        }
      } else {
        auto valuesView = BufferView<DuckDBBufferReleaser>::create(
            reinterpret_cast<const uint8_t*>(duckData),
            size * sizeof(typename OP::BOLT_TYPE),
            DuckDBBufferReleaser(duckVector.GetBuffer()));

        BufferPtr nullsView(nullptr);
        if (!duckValidity.AllValid()) {
          nullsView = BufferView<DuckDBValidityReleaser>::create(
              reinterpret_cast<const uint8_t*>(duckValidity.GetData()),
              bits::nbytes(size),
              DuckDBValidityReleaser(duckValidity));
        }

        result = std::make_shared<FlatVector<typename OP::BOLT_TYPE>>(
            pool,
            boltType,
            nullsView,
            size,
            valuesView,
            std::vector<BufferPtr>());
      }

      return result;
    }
    case ::duckdb::VectorType::DICTIONARY_VECTOR: {
      auto& child = ::duckdb::DictionaryVector::Child(duckVector);
      auto& selection = ::duckdb::DictionaryVector::SelVector(duckVector);

      // DuckDB vectors doesn't tell what their size is. We are going to use max
      // index + 1 instead as the vector is guaranteed to be at least that
      // large.
      vector_size_t maxIndex = 0;
      for (auto i = 0; i < size; i++) {
        maxIndex = std::max(maxIndex, (vector_size_t)selection.get_index(i));
      }
      VectorPtr base;
      // Unused dictionary elements can be uninitialized. That can cause
      // errors if we try to decode them. Here we create a bitmap of
      // used values to avoid that.
      if (child.GetType() == LogicalTypeId::HUGEINT ||
          child.GetType() == LogicalTypeId::TIMESTAMP ||
          child.GetType() == LogicalTypeId::VARCHAR) {
        std::vector<uint8_t> validityVector(bits::nbytes(maxIndex + 1), 0);
        auto validity_ptr = validityVector.data();
        for (auto i = 0; i < size; i++) {
          bits::setBit(validity_ptr, selection.get_index(i));
        }
        base = convert<OP>(child, boltType, maxIndex + 1, pool, validity_ptr);
      } else {
        base = convert<OP>(child, boltType, maxIndex + 1, pool);
      }

      auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
      memcpy(
          indices->asMutable<vector_size_t>(),
          selection.data(),
          size * sizeof(vector_size_t));

      return BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indices, size, base);
    }
    default:
      BOLT_UNSUPPORTED(
          "Unsupported DuckDB vector encoding: {}",
          ::duckdb::VectorTypeToString(vectorType));
  }
}

struct NumericCastToDouble {
  template <class T>
  static double operation(T input) {
    return double(input);
  }
};

template <>
double NumericCastToDouble::operation(hugeint_t input) {
  return Hugeint::Cast<double>(input);
}

VectorPtr toBoltVector(
    int32_t size,
    ::duckdb::Vector& duckVector,
    const TypePtr& boltType,
    memory::MemoryPool* pool) {
  VectorPtr boltFlatVector;

  auto type = duckVector.GetType();
  switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
      return convert<DuckNumericConversion<bool>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::TINYINT:
      return convert<DuckNumericConversion<int8_t>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::SMALLINT:
      return convert<DuckNumericConversion<int16_t>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::INTEGER:
      return convert<DuckNumericConversion<int32_t>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::BIGINT:
      return convert<DuckNumericConversion<int64_t>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::HUGEINT:
      return convert<DuckHugeintConversion>(duckVector, boltType, size, pool);
    case LogicalTypeId::FLOAT:
      return convert<DuckNumericConversion<float>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::DOUBLE:
      return convert<DuckNumericConversion<double>>(
          duckVector, boltType, size, pool);
    case LogicalTypeId::DECIMAL: {
      uint8_t width;
      uint8_t scale;
      type.GetDecimalProperties(width, scale);
      switch (type.InternalType()) {
        case PhysicalType::INT16:
          return convert<DuckInt16DecimalConversion>(
              duckVector, boltType, size, pool);
        case PhysicalType::INT32:
          return convert<DuckInt32DecimalConversion>(
              duckVector, boltType, size, pool);
        case PhysicalType::INT64:
          return convert<DuckInt64DecimalConversion>(
              duckVector, boltType, size, pool);
        case PhysicalType::INT128:
          return convert<DuckLongDecimalConversion>(
              duckVector, boltType, size, pool);
        default:
          throw std::runtime_error(
              "unrecognized internal type for decimal (this shouldn't happen");
      }
    }
    case LogicalTypeId::VARCHAR:
      return convert<DuckStringConversion>(duckVector, boltType, size, pool);
    case LogicalTypeId::BLOB:
      return convert<DuckBlobConversion>(duckVector, boltType, size, pool);
    case LogicalTypeId::DATE:
      return convert<DuckDateConversion>(duckVector, boltType, size, pool);
    case LogicalTypeId::TIMESTAMP:
      return convert<DuckTimestampConversion>(duckVector, boltType, size, pool);
    default:
      throw std::runtime_error(
          "Unsupported vector type for conversion: " + type.ToString());
  }
}

VectorPtr DuckResult::getVector(size_t columnIdx) {
  BOLT_CHECK_LT(columnIdx, columnCount());
  BOLT_CHECK(
      currentChunk_,
      "no chunk available: did you call next() and did it return true?");
  auto& duckVector = currentChunk_->data[columnIdx];
  auto resultType = getType(columnIdx);
  return toBoltVector(
      currentChunk_->size(), duckVector, resultType, context_->pool());
}

bool DuckResult::next() {
  currentChunk_ = queryResult_->Fetch();
  if (!currentChunk_) {
    return false;
  }
  currentChunk_->Normalify();
  return currentChunk_->size() > 0;
}

} // namespace bytedance::bolt::duckdb
