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

#include "bolt/shuffle/sparksql/cell/CellShuffleTypeAdapter.h"

#include <cstring>

#include "bolt/row/CompactRow.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

// Bolt's Type::isPrimitiveType() also excludes VARIANT, FUNCTION, OPAQUE, and
// INVALID, so check the supported container kinds explicitly.
bool isComplex(const TypePtr& type) {
  return type->isArray() || type->isMap() || type->isRow();
}

} // namespace

bool CellShuffleTypeAdapter::isSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::UNKNOWN || CellLayout::isSupportedType(type)) {
    return true;
  }
  if (!isComplex(type)) {
    return false;
  }
  for (uint32_t i = 0; i < type->size(); ++i) {
    if (!isSupported(type->childAt(i))) {
      return false;
    }
  }
  return true;
}

CellShuffleTypeAdapter::CellShuffleTypeAdapter(RowTypePtr logicalType)
    : logicalType_(std::move(logicalType)) {
  BOLT_CHECK(
      isSupported(logicalType_),
      "Unsupported Cell logical schema: {}",
      logicalType_->toString());
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<std::string> complexNames;
  std::vector<TypePtr> complexTypes;
  for (uint32_t i = 0; i < logicalType_->size(); ++i) {
    const auto& type = logicalType_->childAt(i);
    if (type->kind() == TypeKind::UNKNOWN) {
      unknownColumns_.push_back(i);
    } else if (isComplex(type)) {
      complexColumns_.push_back(i);
      complexNames.push_back(logicalType_->nameOf(i));
      complexTypes.push_back(type);
    } else {
      simpleColumns_.push_back(i);
      names.push_back(logicalType_->nameOf(i));
      types.push_back(type);
    }
  }
  if (hasComplexColumns()) {
    packedType_ = ROW(std::move(complexNames), std::move(complexTypes));
    names.push_back("__cell_packed_complex");
    types.push_back(VARBINARY());
  }
  if (needsAdaptation()) {
    physicalType_ = ROW(std::move(names), std::move(types));
  } else {
    physicalType_ = logicalType_;
  }
}

RowVectorPtr CellShuffleTypeAdapter::pack(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) const {
  BOLT_CHECK(
      !RowVector::isComposite(input),
      "CellShuffleTypeAdapter does not take composite rows");
  BOLT_DCHECK(
      input->type()->equivalent(*logicalType_), "Cell input schema changed");
  if (!needsAdaptation()) {
    return input;
  }
  const vector_size_t numRows = input->size();
  if (!hasComplexColumns()) {
    std::vector<VectorPtr> children;
    children.reserve(simpleColumns_.size());
    for (const auto col : simpleColumns_) {
      children.push_back(input->childAt(col));
    }
    return std::make_shared<RowVector>(
        pool, physicalType_, nullptr, numRows, std::move(children));
  }
  std::vector<VectorPtr> complex;
  for (const auto col : complexColumns_) {
    complex.push_back(BaseVector::loadedVectorShared(input->childAt(col)));
  }
  auto grouped = std::make_shared<RowVector>(
      pool, packedType_, nullptr, numRows, std::move(complex));
  row::CompactRow serde(grouped);
  const auto fixedSize = row::CompactRow::fixedRowSize(packedType_);
  std::vector<int32_t> sizes(numRows);
  uint64_t bytes = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    sizes[i] = fixedSize ? *fixedSize : serde.rowSize(i);
    BOLT_CHECK_GE(sizes[i], 0);
    bytes += sizes[i];
  }
  auto binary =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);
  // One pool-owned buffer, retained by the binary vector until split completes.
  auto buffer = AlignedBuffer::allocate<char>(bytes, pool);
  auto* data = buffer->asMutable<char>();
  size_t at = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    // CompactRow only sets null bits and skips null values: each row starts
    // zeroed, just before it is written, while it is in cache.
    ::memset(data + at, 0, sizes[i]);
    BOLT_CHECK_EQ(serde.serialize(i, data + at), sizes[i]);
    binary->setNoCopy(i, StringView(data + at, sizes[i]));
    at += sizes[i];
  }
  binary->setStringBuffers({buffer});
  std::vector<VectorPtr> children;
  children.reserve(simpleColumns_.size() + 1);
  for (const auto col : simpleColumns_) {
    children.push_back(BaseVector::loadedVectorShared(input->childAt(col)));
  }
  children.push_back(std::move(binary));
  return std::make_shared<RowVector>(
      pool, physicalType_, nullptr, numRows, std::move(children));
}

RowVectorPtr CellShuffleTypeAdapter::unpack(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) const {
  if (!needsAdaptation()) {
    return input;
  }
  BOLT_DCHECK(
      input->type()->equivalent(*physicalType_),
      "Invalid Cell physical schema");
  const vector_size_t numRows = input->size();
  std::vector<VectorPtr> children(logicalType_->size());
  for (uint32_t i = 0; i < simpleColumns_.size(); ++i) {
    children[simpleColumns_[i]] = input->childAt(i);
  }
  for (const auto col : unknownColumns_) {
    children[col] = BaseVector::createNullConstant(
        logicalType_->childAt(col), numRows, pool);
  }
  if (hasComplexColumns()) {
    DecodedVector binary(*input->childAt(simpleColumns_.size()));
    std::vector<std::string_view> rows;
    rows.reserve(numRows);
    for (vector_size_t i = 0; i < numRows; ++i) {
      BOLT_CHECK(!binary.isNullAt(i), "Cell complex payload must be non-null");
      // Keep a reference into the vector's buffer: inline StringView::data()
      // points into the StringView object itself, so a by-value copy would
      // dangle once this iteration ends.
      const auto& value = binary.data<StringView>()[binary.index(i)];
      rows.emplace_back(value.data(), value.size());
    }
    auto complex = row::CompactRow::deserialize(rows, packedType_, pool);
    for (uint32_t i = 0; i < complexColumns_.size(); ++i) {
      children[complexColumns_[i]] = complex->childAt(i);
    }
  }
  return std::make_shared<RowVector>(
      pool, logicalType_, nullptr, numRows, std::move(children));
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
