/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/exec/radixsort/PayloadRowWriter.h"

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/PayloadRowWriterInternal.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

void addHeapBytes(
    uint64_t bytes,
    vector_size_t row,
    uint64_t* heapSizes,
    uint64_t& heapBytes) {
  auto next = checkedAdd<uint64_t>(heapSizes[row], bytes);
  heapSizes[row] = *next;
  next = checkedAdd<uint64_t>(heapBytes, bytes);
  heapBytes = *next;
}

void measureFlatScalar(
    const RowVector& input,
    const PayloadRowLayout& layout,
    uint64_t* heapSizes,
    uint64_t& heapBytes) {
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& metadata = layout.columns()[column];
    const auto& vector = *input.childAt(column);
    if (metadata.type->kind() == TypeKind::UNKNOWN) {
      continue;
    }
    if (!metadata.variable) {
      continue;
    }
    const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
    const auto* values = flat->rawValues();
    const auto* nulls = flat->rawNulls();
    for (vector_size_t row = 0; row < input.size(); ++row) {
      if (nulls != nullptr && bits::isBitNull(nulls, row)) {
        continue;
      }
      const auto value = values[row];
      if (value.isInline()) {
        continue;
      }
      addHeapBytes(value.size(), row, heapSizes, heapBytes);
    }
  }
}

void measureStringColumn(
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* heapSizes,
    uint64_t& heapBytes) {
  if (vector.encoding() == VectorEncoding::Simple::FLAT) {
    const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
    const auto* values = flat->rawValues();
    const auto* nulls = flat->rawNulls();
    for (vector_size_t row = 0; row < size; ++row) {
      if (nulls != nullptr && bits::isBitNull(nulls, row)) {
        continue;
      }
      const auto value = values[row];
      if (!value.isInline()) {
        addHeapBytes(value.size(), row, heapSizes, heapBytes);
      }
    }
    return;
  }

  DecodedVector decoded(vector);
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(0)) {
      return;
    }
    const auto value = decoded.valueAt<StringView>(0);
    if (value.isInline()) {
      return;
    }
    for (vector_size_t row = 0; row < size; ++row) {
      addHeapBytes(value.size(), row, heapSizes, heapBytes);
    }
    return;
  }

  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    const auto value = decoded.valueAt<StringView>(row);
    if (!value.isInline()) {
      addHeapBytes(value.size(), row, heapSizes, heapBytes);
    }
  }
}

void measureComplexColumn(
    const BaseVector& vector,
    vector_size_t size,
    uint64_t* heapSizes,
    uint64_t& heapBytes) {
  DecodedVector decoded(vector);
  for (vector_size_t row = 0; row < size; ++row) {
    if (decoded.isNullAt(row)) {
      continue;
    }
    const auto valueSize = exec::ContainerRowSerde::serializedSize(
        *decoded.base(),
        decoded.index(row),
        exec::ContainerRowSerdeOptions{.isKey = false});
    addHeapBytes(valueSize, row, heapSizes, heapBytes);
  }
}

} // namespace

uint64_t PayloadRowSizes::heapSizeAt(vector_size_t row) const {
  BOLT_CHECK_GE(row, 0);
  BOLT_CHECK_LT(row, size_);
  return heapSizes_ == nullptr ? 0 : heapSizes_->as<uint64_t>()[row];
}

void PayloadRowWriter::measure(
    const RowVector& input,
    const PayloadRowLayout& layout,
    memory::MemoryPool* pool,
    PayloadRowSizes& sizes) {
  sizes = PayloadRowSizes{};
  BOLT_CHECK_NOT_NULL(
      pool, "Payload row size pass memory pool must not be null");
  validatePayloadRowInput(layout, input);
  auto fixedBytes = checkedMultiply<uint64_t>(input.size(), layout.rowWidth());
  BOLT_CHECK(fixedBytes.has_value(), "Payload row input fixed bytes overflow");
  sizes.size_ = input.size();
  sizes.fixedBytes_ = *fixedBytes;
  if (input.size() == 0) {
    return;
  }

  if (!layout.hasVariableFields()) {
    return;
  }
  sizes.heapSizes_ =
      AlignedBuffer::allocate<uint64_t>(input.size(), pool, uint64_t{0});
  if (canUseFlatScalarFastPath(layout, input)) {
    return measureFlatScalar(
        input,
        layout,
        sizes.heapSizes_->asMutable<uint64_t>(),
        sizes.heapBytes_);
  }
  auto* rawHeapSizes = sizes.heapSizes_->asMutable<uint64_t>();
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& metadata = layout.columns()[column];
    if (!metadata.variable) {
      continue;
    }
    if (metadata.complex) {
      measureComplexColumn(
          *input.childAt(column), input.size(), rawHeapSizes, sizes.heapBytes_);
    } else {
      measureStringColumn(
          *input.childAt(column), input.size(), rawHeapSizes, sizes.heapBytes_);
    }
  }
}

} // namespace bytedance::bolt::exec::radixsort
