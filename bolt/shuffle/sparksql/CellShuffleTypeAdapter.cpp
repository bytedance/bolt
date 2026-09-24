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

#include "bolt/shuffle/sparksql/CellShuffleTypeAdapter.h"

#include "bolt/row/CompactRow.h"
#include "bolt/shuffle/sparksql/cell/CellTypes.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql {
namespace {

bool isComplex(const TypePtr& type) {
  return type->kind() == TypeKind::ARRAY || type->kind() == TypeKind::MAP ||
      type->kind() == TypeKind::ROW;
}

// Check the complete CompactRow before invoking its vectorized deserializer.
// Charge conservative workspace per element as well as bytes, so tiny/null
// nested values cannot turn a small payload into unbounded allocations.
class CompactRowBounds {
 public:
  explicit CompactRowBounds(uint64_t& budget) : budget_(budget) {}

  size_t value(
      const TypePtr& type,
      std::string_view bytes,
      size_t at = 0,
      uint32_t depth = 0) {
    BOLT_CHECK_LT(depth, 128, "Cell complex value nesting exceeds limit");
    charge(64);
    if (type->isFixedWidth() || type->isUnKnown()) {
      const size_t width = type->isUnKnown()    ? 0
          : type->kind() == TypeKind::TIMESTAMP ? 8
                                                : type->cppSizeInBytes();
      require(bytes, at, width);
      if (type->kind() == TypeKind::BOOLEAN) {
        BOLT_CHECK_LE(
            static_cast<uint8_t>(bytes[at]), 1, "Invalid CompactRow boolean");
      }
      return at + width;
    }
    switch (type->kind()) {
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto length = number(bytes, at);
        require(bytes, at, length);
        charge(length);
        return at + length;
      }
      case TypeKind::ROW: {
        const auto count = type->size();
        const auto nullAt = at;
        const auto nullBytes = (count + 7) / 8;
        require(bytes, at, nullBytes);
        at += nullBytes;
        for (uint32_t i = 0; i < count; ++i) {
          const auto& child = type->childAt(i);
          if (child->isFixedWidth() || child->isUnKnown() ||
              !isNull(bytes, nullAt, i)) {
            // Null fixed-width fields still occupy their full width.
            if (isNull(bytes, nullAt, i) &&
                child->kind() == TypeKind::BOOLEAN) {
              require(bytes, at, 1);
              ++at;
            } else {
              at = value(child, bytes, at, depth + 1);
            }
          }
        }
        return at;
      }
      case TypeKind::ARRAY:
        return array(type->childAt(0), bytes, at, depth + 1).first;
      case TypeKind::MAP: {
        const auto keys = array(type->childAt(0), bytes, at, depth + 1);
        const auto values =
            array(type->childAt(1), bytes, keys.first, depth + 1);
        BOLT_CHECK_EQ(
            keys.second, values.second, "Invalid CompactRow map counts");
        return values.first;
      }
      default:
        BOLT_FAIL("Unsupported Cell complex field {}", type->toString());
    }
  }

 private:
  static void require(std::string_view bytes, size_t at, size_t size) {
    BOLT_CHECK(
        at <= bytes.size() && size <= bytes.size() - at,
        "Truncated Cell complex payload");
  }

  static uint32_t number(std::string_view bytes, size_t& at) {
    require(bytes, at, 4);
    int32_t result;
    ::memcpy(&result, bytes.data() + at, 4);
    BOLT_CHECK_GE(result, 0, "Negative Cell complex size or offset");
    at += 4;
    return result;
  }

  static bool isNull(std::string_view bytes, size_t at, uint32_t index) {
    return (static_cast<uint8_t>(bytes[at + index / 8]) >> (index % 8)) & 1;
  }

  void charge(uint64_t bytes) {
    BOLT_CHECK_LE(bytes, budget_, "Cell complex decoded size exceeds limit");
    budget_ -= bytes;
  }

  std::pair<size_t, uint32_t> array(
      const TypePtr& element,
      std::string_view bytes,
      size_t at,
      uint32_t depth) {
    const auto count = number(bytes, at);
    charge(uint64_t(count) * 64);
    const auto nullAt = at;
    const size_t nullBytes = (uint64_t(count) + 7) / 8;
    require(bytes, at, nullBytes);
    at += nullBytes;
    if (!isComplex(element)) {
      for (uint32_t i = 0; i < count; ++i) {
        if (element->isFixedWidth() || element->isUnKnown() ||
            !isNull(bytes, nullAt, i)) {
          if (isNull(bytes, nullAt, i) &&
              element->kind() == TypeKind::BOOLEAN) {
            require(bytes, at, 1);
            ++at;
          } else {
            at = value(element, bytes, at, depth);
          }
        }
      }
    } else if (count != 0) {
      const auto size = number(bytes, at);
      const auto base = at;
      require(bytes, base, size);
      const uint64_t tableBytes = uint64_t(count) * 4;
      BOLT_CHECK_GE(size, tableBytes, "Invalid Cell complex offset table");
      size_t end = base + tableBytes;
      for (uint32_t i = 0; i < count; ++i) {
        const auto offset = number(bytes, at);
        if (!isNull(bytes, nullAt, i)) {
          BOLT_CHECK_EQ(
              base + offset, end, "Invalid Cell complex element offset");
          end = value(element, bytes.substr(0, base + size), end, depth);
        }
      }
      BOLT_CHECK_EQ(end, base + size, "Trailing Cell complex array bytes");
      at = end;
    }
    return {at, count};
  }

  uint64_t& budget_;
};

} // namespace

bool CellShuffleTypeAdapter::isSupported(const TypePtr& type) {
  if (cell::CellLayout::isSupportedType(type)) {
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
  BOLT_CHECK(isSupported(logicalType_), "Unsupported Cell logical schema");
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
    BOLT_CHECK_LE(size, kMaxRowBytes, "Cell complex row exceeds size limit");
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
    uint64_t budget = kMaxDecodedBytes;
    CompactRowBounds bounds(budget);
    BOLT_CHECK_EQ(
        bounds.value(complexType_, std::string_view(data + at, sizes[i])),
        sizes[i]);
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

RowVectorPtr CellShuffleTypeAdapter::decode(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) const {
  if (!input || !hasComplexColumns()) {
    return input;
  }
  vector_size_t offset = 0;
  auto output = decodeBatch(input, offset, input->size(), UINT64_MAX, pool);
  BOLT_CHECK_EQ(
      offset, input->size(), "Cell complex decoded size exceeds limit");
  return output;
}

RowVectorPtr CellShuffleTypeAdapter::decodeNext(
    const RowVectorPtr& input,
    vector_size_t& offset,
    memory::MemoryPool* pool) const {
  return decodeBatch(input, offset, kMaxBatchRows, kTargetBatchBytes, pool);
}

RowVectorPtr CellShuffleTypeAdapter::decodeBatch(
    const RowVectorPtr& input,
    vector_size_t& offset,
    vector_size_t maxRows,
    uint64_t maxBytes,
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
  const auto count = std::min(maxRows, input->size() - start);
  DecodedVector binary(*input->childAt(retainedColumns_.size()));
  std::vector<std::string_view> rows;
  rows.reserve(count);
  uint64_t bytes = 0;
  uint64_t budget = kMaxDecodedBytes;
  for (vector_size_t i = start; i < start + count; ++i) {
    BOLT_CHECK(!binary.isNullAt(i), "Cell complex payload must be non-null");
    const auto& value = binary.data<StringView>()[binary.index(i)];
    BOLT_CHECK_LE(
        value.size(), kMaxRowBytes, "Cell complex row exceeds size limit");
    if (!rows.empty() && bytes + value.size() > maxBytes) {
      break;
    }
    uint64_t rowBudget = kMaxDecodedBytes;
    CompactRowBounds bounds(rowBudget);
    const std::string_view view(value.data(), value.size());
    BOLT_CHECK_EQ(
        bounds.value(complexType_, view),
        view.size(),
        "Trailing Cell complex row bytes");
    const auto decodedBytes = kMaxDecodedBytes - rowBudget;
    if (!rows.empty() && decodedBytes > budget) {
      break;
    }
    budget -= decodedBytes;
    bytes += value.size();
    rows.push_back(view);
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

} // namespace bytedance::bolt::shuffle::sparksql
