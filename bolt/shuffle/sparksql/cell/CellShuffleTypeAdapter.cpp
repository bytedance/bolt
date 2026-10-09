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

#include "bolt/row/CompactRow.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

// Batch targets apply to serialized rows, not decoded vector memory. A
// single row larger than the target still forms a batch of its own.
constexpr vector_size_t kMaxBatchRows = 1024;
constexpr uint64_t kTargetBatchBytes = 8ULL << 20;

bool isComplex(const TypePtr& type) {
  return type->kind() == TypeKind::ARRAY || type->kind() == TypeKind::MAP ||
      type->kind() == TypeKind::ROW;
}

} // namespace

bool CellShuffleTypeAdapter::isSupported(const TypePtr& type) {
  if (CellLayout::isSupportedType(type)) {
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
    if (isComplex(type)) {
      complexColumns_.push_back(i);
      complexNames.push_back(logicalType_->nameOf(i));
      complexTypes.push_back(type);
    } else {
      retainedColumns_.push_back(i);
      names.push_back(logicalType_->nameOf(i));
      types.push_back(type);
    }
  }
  if (hasComplexColumns()) {
    complexType_ = ROW(std::move(complexNames), std::move(complexTypes));
    names.push_back("__cell_complex_payload");
    types.push_back(VARBINARY());
    physicalType_ = ROW(std::move(names), std::move(types));
  } else {
    physicalType_ = logicalType_;
  }
}

RowVectorPtr CellShuffleTypeAdapter::encodeNext(
    const RowVectorPtr& input,
    vector_size_t& offset,
    memory::MemoryPool* pool) const {
  BOLT_CHECK(
      !RowVector::isComposite(input),
      "Cell complex adapter requires ordinary rows");
  BOLT_CHECK(
      input->type()->equivalent(*logicalType_), "Cell input schema changed");
  if (!hasComplexColumns()) {
    BOLT_CHECK_EQ(offset, 0);
    offset = input->size();
    return input;
  }
  BOLT_CHECK_GE(offset, 0);
  BOLT_CHECK_LE(offset, input->size());
  const auto start = offset;
  const auto candidateRows = std::min(kMaxBatchRows, input->size() - start);
  std::vector<VectorPtr> complex;
  for (const auto col : complexColumns_) {
    complex.push_back(BaseVector::loadedVectorShared(input->childAt(col))
                          ->slice(start, candidateRows));
  }
  auto grouped = std::make_shared<RowVector>(
      pool, complexType_, nullptr, candidateRows, std::move(complex));
  row::CompactRow serde(grouped);
  const auto fixedSize = row::CompactRow::fixedRowSize(complexType_);
  std::vector<int32_t> sizes;
  uint64_t bytes = 0;
  for (vector_size_t i = 0; i < candidateRows; ++i) {
    const int32_t size = fixedSize ? *fixedSize : serde.rowSize(i);
    BOLT_CHECK_GE(size, 0);
    if (!sizes.empty() && bytes + size > kTargetBatchBytes) {
      break;
    }
    sizes.push_back(size);
    bytes += size;
  }
  const vector_size_t rows = sizes.size();
  auto binary =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), rows, pool);
  // One pool-owned buffer, retained by the binary vector until split completes.
  auto buffer = AlignedBuffer::allocate<char>(bytes, pool, 0);
  auto* data = buffer->asMutable<char>();
  size_t at = 0;
  for (vector_size_t i = 0; i < rows; ++i) {
    BOLT_CHECK_EQ(serde.serialize(i, data + at), sizes[i]);
    binary->setNoCopy(i, StringView(data + at, sizes[i]));
    at += sizes[i];
  }
  binary->setStringBuffers({buffer});
  std::vector<VectorPtr> children;
  for (const auto col : retainedColumns_) {
    children.push_back(BaseVector::loadedVectorShared(input->childAt(col))
                           ->slice(start, rows));
  }
  children.push_back(std::move(binary));
  offset += rows;
  return std::make_shared<RowVector>(
      pool, physicalType_, nullptr, rows, std::move(children));
}

RowVectorPtr CellShuffleTypeAdapter::decodeNext(
    const RowVectorPtr& input,
    vector_size_t& offset,
    memory::MemoryPool* pool) const {
  if (!input || !hasComplexColumns()) {
    if (input) {
      BOLT_CHECK_EQ(offset, 0);
      offset = input->size();
    }
    return input;
  }
  BOLT_CHECK(
      input->type()->equivalent(*physicalType_),
      "Invalid Cell physical schema");
  BOLT_CHECK_GE(offset, 0);
  BOLT_CHECK_LE(offset, input->size());
  const auto start = offset;
  const auto count = std::min(kMaxBatchRows, input->size() - start);
  DecodedVector binary(*input->childAt(retainedColumns_.size()));
  std::vector<std::string_view> rows;
  rows.reserve(count);
  uint64_t bytes = 0;
  for (vector_size_t i = start; i < start + count; ++i) {
    BOLT_CHECK(!binary.isNullAt(i), "Cell complex payload must be non-null");
    const auto& value = binary.data<StringView>()[binary.index(i)];
    if (!rows.empty() && bytes + value.size() > kTargetBatchBytes) {
      break;
    }
    bytes += value.size();
    rows.emplace_back(value.data(), value.size());
  }
  auto complex = row::CompactRow::deserialize(rows, complexType_, pool);
  std::vector<VectorPtr> children(logicalType_->size());
  for (uint32_t i = 0; i < retainedColumns_.size(); ++i) {
    children[retainedColumns_[i]] = start == 0 && rows.size() == input->size()
        ? input->childAt(i)
        : input->childAt(i)->slice(start, rows.size());
  }
  for (uint32_t i = 0; i < complexColumns_.size(); ++i) {
    children[complexColumns_[i]] = complex->childAt(i);
  }
  auto output = std::make_shared<RowVector>(
      pool, logicalType_, nullptr, rows.size(), std::move(children));
  offset += rows.size();
  return output;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
