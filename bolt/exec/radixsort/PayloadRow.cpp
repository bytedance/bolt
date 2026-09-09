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

#include "bolt/exec/radixsort/PayloadRow.h"

#include <folly/small_vector.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/process/ProcessBase.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/radixsort/RadixSortRunStorage.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/SimpleVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::radixsort {

namespace {
enum class VariableWriterKind : uint8_t {
  kComplex,
  kFlatString,
  kConstantString,
  kDecodedString,
};

struct VariableColumnWriter {
  const PayloadRowColumnLayout* column;
  const DecodedVector* decoded;
  const StringView* flatStringValues;
  const uint64_t* flatStringNulls;
  const StringView* decodedStringValues;
  const vector_size_t* decodedStringIndices;
  const uint64_t* decodedStringNulls;
  const uint64_t* complexSizes;
  VariableWriterKind kind;
  StringView constantStringValue;
  bool constantStringNull;
};
} // namespace

struct PayloadRowWriter::Impl {
  BufferPtr complexSizes;
  std::vector<std::optional<DecodedVector>> decoded;
  folly::small_vector<VariableColumnWriter, 32> variableColumns;
};

namespace {

bool isComplex(const Type& type) {
  return type.kind() == TypeKind::ARRAY || type.kind() == TypeKind::MAP ||
      type.kind() == TypeKind::ROW;
}

bool isSupportedType(const Type& type, bool nested) {
  if (type.isDecimal()) {
    return true;
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      for (uint32_t child = 0; child < type.size(); ++child) {
        if (!isSupportedType(*type.childAt(child), true)) {
          return false;
        }
      }
      return true;
    case TypeKind::VARIANT:
      return nested;
    default:
      return false;
  }
}

std::optional<uint32_t> slotWidth(const Type& type) {
  if (type.isShortDecimal()) {
    return sizeof(int64_t);
  }
  if (type.isLongDecimal()) {
    return sizeof(int128_t);
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
      return sizeof(int128_t);
    case TypeKind::TIMESTAMP:
      return sizeof(Timestamp);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return sizeof(PayloadVarlenRef);
    case TypeKind::UNKNOWN:
      return 0;
    default:
      return std::nullopt;
  }
}

} // namespace

bool PayloadRowLayout::supports(const Type& type) {
  return isSupportedType(type, false);
}

std::shared_ptr<const PayloadRowLayout> PayloadRowLayout::create(
    const RowTypePtr& rowType) {
  if (rowType->size() == 0) {
    return nullptr;
  }

  const auto nullBytes = static_cast<uint32_t>((rowType->size() + 7) / 8);
  bool hasVariableFields = false;
  bool supportedTypes = true;
  std::string unsupportedType;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    if (!supports(*type)) {
      supportedTypes = false;
      unsupportedType = type->toString();
    }
    hasVariableFields |= type->kind() == TypeKind::VARCHAR ||
        type->kind() == TypeKind::VARBINARY || isComplex(*type);
  }
  BOLT_CHECK(
      supportedTypes,
      "Payload row layout is not implemented for ",
      unsupportedType);

  uint64_t rowWidth = nullBytes;

  std::vector<PayloadRowColumnLayout> columns;
  std::vector<PayloadRowColumnLayout> variableColumns;
  columns.reserve(rowType->size());
  bool supportedSlots = true;
  std::string unsupportedSlotType;
  bool rowWidthValid = true;
  for (uint32_t column = 0; column < rowType->size(); ++column) {
    const auto& type = rowType->childAt(column);
    const auto width = slotWidth(*type);
    if (!width.has_value()) {
      supportedSlots = false;
      unsupportedSlotType = type->toString();
      continue;
    }
    PayloadRowColumnLayout layout{
        type,
        rowWidth,
        *width,
        column / 8,
        static_cast<uint8_t>(uint8_t{1} << (column % 8)),
        type->kind() == TypeKind::VARCHAR ||
            type->kind() == TypeKind::VARBINARY || isComplex(*type),
        isComplex(*type)};
    if (layout.variable) {
      variableColumns.push_back(layout);
    }
    columns.push_back(std::move(layout));
    auto next = checkedAdd<uint64_t>(rowWidth, *width);
    rowWidthValid &= next.has_value();
    if (next.has_value()) {
      rowWidth = *next;
    }
  }
  BOLT_CHECK(
      supportedSlots,
      "Payload row slot is not implemented for ",
      unsupportedSlotType);
  BOLT_CHECK(rowWidthValid, "Payload row row width overflows");

  return std::shared_ptr<const PayloadRowLayout>(new PayloadRowLayout(
      rowType,
      std::move(columns),
      std::move(variableColumns),
      nullBytes,
      hasVariableFields,
      rowWidth));
}

namespace {

void prepareResult(
    const PayloadRowLayout& layout,
    vector_size_t size,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  if (result != nullptr && result->pool() == pool &&
      result->type()->equivalent(*layout.rowType())) {
    VectorPtr reusable(std::move(result));
    BaseVector::prepareForReuse(reusable, size);
    result = std::static_pointer_cast<RowVector>(std::move(reusable));
    for (auto& child : result->children()) {
      child->resize(size);
    }
    return;
  }

  std::vector<VectorPtr> children;
  children.reserve(layout.columns().size());
  for (const auto& column : layout.columns()) {
    children.push_back(BaseVector::create(column.type, size, pool));
  }
  result = std::make_shared<RowVector>(
      pool, layout.rowType(), nullptr, size, std::move(children));
}

FOLLY_ALWAYS_INLINE bool
isNull(const char* row, uint32_t nullByte, uint8_t nullMask) {
  return (static_cast<uint8_t>(row[nullByte]) & nullMask) == 0;
}

void writeValidityBits(
    uint32_t nullByte,
    uint8_t nullMask,
    std::span<char* const> rows,
    vector_size_t outputOffset,
    uint64_t* nulls) {
  constexpr vector_size_t kRowsPerWord = 64;
  vector_size_t row = 0;
  while (row < rows.size() && (outputOffset + row) % kRowsPerWord != 0) {
    bits::setNull(
        nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
    ++row;
  }
  while (row + kRowsPerWord <= rows.size()) {
    uint64_t validityWord = 0;
    for (vector_size_t lane = 0; lane < kRowsPerWord; ++lane) {
      validityWord |=
          static_cast<uint64_t>(!isNull(rows[row + lane], nullByte, nullMask))
          << lane;
    }
    nulls[(outputOffset + row) / kRowsPerWord] = validityWord;
    row += kRowsPerWord;
  }
  for (; row < rows.size(); ++row) {
    bits::setNull(
        nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
  }
}

template <typename T, bool MayHaveNulls>
void gatherFixedColumn(
    uint64_t offset,
    uint32_t nullByte,
    uint8_t nullMask,
    std::span<char* const> rows,
    vector_size_t outputOffset,
    BaseVector* result,
    T* values,
    uint64_t* nulls,
    bool hasAvx2) {
  values += outputOffset;
  if constexpr (!MayHaveNulls) {
    if (result->rawNulls() != nullptr) {
      result->clearNulls(outputOffset, outputOffset + rows.size());
    }
  }
  if constexpr (MayHaveNulls) {
    BOLT_DCHECK_NOT_NULL(nulls);
  } else {
    BOLT_DCHECK(nulls == nullptr);
  }
  if constexpr (MayHaveNulls) {
    constexpr vector_size_t kRowsPerWord = 64;
    vector_size_t row = 0;
    while (row < rows.size() && (outputOffset + row) % kRowsPerWord != 0) {
      bits::setNull(
          nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
      values[row] = loadUnaligned<T>(rows[row] + offset);
      ++row;
    }
    while (row + kRowsPerWord <= rows.size()) {
      uint64_t validityWord = 0;
      for (vector_size_t lane = 0; lane < kRowsPerWord; ++lane) {
        validityWord |=
            static_cast<uint64_t>(!isNull(rows[row + lane], nullByte, nullMask))
            << lane;
        values[row + lane] = loadUnaligned<T>(rows[row + lane] + offset);
      }
      nulls[(outputOffset + row) / kRowsPerWord] = validityWord;
      row += kRowsPerWord;
    }
    for (; row < rows.size(); ++row) {
      bits::setNull(
          nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
      values[row] = loadUnaligned<T>(rows[row] + offset);
    }
    return;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    constexpr vector_size_t kBatchSize = xsimd::batch<int64_t>::size;
    if (rows.size() >= kBatchSize && hasAvx2) {
      const auto baseAddress =
          reinterpret_cast<intptr_t>(rows.front() + offset);
      const auto* base =
          reinterpret_cast<const int64_t*>(rows.front() + offset);
      vector_size_t row = 0;
      constexpr vector_size_t kStep = 4 * kBatchSize;
      for (; row + kStep <= rows.size(); row += kStep) {
        int64_t indices[kStep];
        for (vector_size_t lane = 0; lane < kStep; ++lane) {
          const auto address =
              reinterpret_cast<intptr_t>(rows[row + lane] + offset);
          indices[lane] = address - baseAddress;
        }
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(reinterpret_cast<int64_t*>(values + row));
        simd::gather<int64_t, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + kBatchSize));
        simd::gather<int64_t, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + 2 * kBatchSize));
        simd::gather<int64_t, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(
                reinterpret_cast<int64_t*>(values + row + 3 * kBatchSize));
      }
      for (; row + kBatchSize <= rows.size(); row += kBatchSize) {
        int64_t indices[kBatchSize];
        for (vector_size_t lane = 0; lane < kBatchSize; ++lane) {
          const auto address =
              reinterpret_cast<intptr_t>(rows[row + lane] + offset);
          indices[lane] = address - baseAddress;
        }
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(reinterpret_cast<int64_t*>(values + row));
      }
      for (; row < rows.size(); ++row) {
        values[row] = loadUnaligned<T>(rows[row] + offset);
      }
      return;
    }
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    values[row] = loadUnaligned<T>(rows[row] + offset);
  }
}

enum class FixedGatherKind : uint8_t {
  kBoolean,
  kInt8,
  kInt16,
  kInt32,
  kInt64,
  kInt128,
  kFloat,
  kDouble,
  kTimestamp,
  kUnknown,
};

struct FixedGatherState {
  uint64_t offset;
  uint32_t nullByte;
  uint8_t nullMask;
  column_index_t outputChannel;
  FixedGatherKind kind;
  bool mayHaveNulls;
  bool grouped64;
  BaseVector* result{nullptr};
  void* values{nullptr};
  uint64_t* nulls{nullptr};
};

bool isFixed64Column(const PayloadRowColumnLayout& column) {
  return column.type->isShortDecimal() ||
      column.type->kind() == TypeKind::BIGINT ||
      column.type->kind() == TypeKind::DOUBLE;
}

const FixedGatherState& asFixedGatherState(const FixedGatherState& state) {
  return state;
}

const FixedGatherState& asFixedGatherState(const FixedGatherState* state) {
  return *state;
}

template <typename States>
void gatherFixed64Columns(
    std::span<char* const> rows,
    vector_size_t outputOffset,
    const States& states) {
  constexpr vector_size_t kBatchSize = xsimd::batch<int64_t>::size;
  constexpr vector_size_t kStep = 4 * kBatchSize;
  const auto rowBaseAddress = reinterpret_cast<intptr_t>(rows.front());
  vector_size_t row = 0;
  for (; row + kStep <= rows.size(); row += kStep) {
    int64_t indices[kStep];
    for (vector_size_t lane = 0; lane < kStep; ++lane) {
      indices[lane] =
          reinterpret_cast<intptr_t>(rows[row + lane]) - rowBaseAddress;
    }
    for (const auto& stateEntry : states) {
      const auto& state = asFixedGatherState(stateEntry);
      if (state.kind == FixedGatherKind::kDouble) {
        const auto* base =
            reinterpret_cast<const double*>(rows.front() + state.offset);
        auto* values = static_cast<double*>(state.values) + outputOffset;
        simd::gather<double, int64_t, 1>(base, indices)
            .store_unaligned(values + row);
        simd::gather<double, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(values + row + kBatchSize);
        simd::gather<double, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(values + row + 2 * kBatchSize);
        simd::gather<double, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(values + row + 3 * kBatchSize);
      } else {
        const auto* base =
            reinterpret_cast<const int64_t*>(rows.front() + state.offset);
        auto* values = static_cast<int64_t*>(state.values) + outputOffset;
        simd::gather<int64_t, int64_t, 1>(base, indices)
            .store_unaligned(values + row);
        simd::gather<int64_t, int64_t, 1>(base, indices + kBatchSize)
            .store_unaligned(values + row + kBatchSize);
        simd::gather<int64_t, int64_t, 1>(base, indices + 2 * kBatchSize)
            .store_unaligned(values + row + 2 * kBatchSize);
        simd::gather<int64_t, int64_t, 1>(base, indices + 3 * kBatchSize)
            .store_unaligned(values + row + 3 * kBatchSize);
      }
    }
  }
  for (const auto& stateEntry : states) {
    const auto& state = asFixedGatherState(stateEntry);
    if (!state.mayHaveNulls && state.result->rawNulls() != nullptr) {
      state.result->clearNulls(outputOffset, outputOffset + rows.size());
    }
    if (state.kind == FixedGatherKind::kDouble) {
      auto* values = static_cast<double*>(state.values) + outputOffset;
      for (auto tail = row; tail < rows.size(); ++tail) {
        values[tail] = loadUnaligned<double>(rows[tail] + state.offset);
      }
    } else {
      auto* values = static_cast<int64_t*>(state.values) + outputOffset;
      for (auto tail = row; tail < rows.size(); ++tail) {
        values[tail] = loadUnaligned<int64_t>(rows[tail] + state.offset);
      }
    }
    if (!state.mayHaveNulls) {
      continue;
    }
    writeValidityBits(
        state.nullByte, state.nullMask, rows, outputOffset, state.nulls);
  }
}

template <bool MayHaveNulls>
void gatherFixedBooleanColumn(
    uint64_t offset,
    uint32_t nullByte,
    uint8_t nullMask,
    std::span<char* const> rows,
    vector_size_t outputOffset,
    BaseVector* result,
    uint64_t* values,
    uint64_t* nulls) {
  if constexpr (!MayHaveNulls) {
    if (result->rawNulls() != nullptr) {
      result->clearNulls(outputOffset, outputOffset + rows.size());
    }
  } else {
    BOLT_DCHECK_NOT_NULL(nulls);
  }
  if constexpr (!MayHaveNulls) {
    BOLT_DCHECK(nulls == nullptr);
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    if constexpr (MayHaveNulls) {
      bits::setNull(
          nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
    }
    const auto value = loadUnaligned<uint8_t>(rows[row] + offset);
    bits::setBit(values, outputOffset + row, value != 0);
  }
}

template <bool MayHaveNulls>
void gatherFixedTimestampColumn(
    uint64_t offset,
    uint32_t nullByte,
    uint8_t nullMask,
    std::span<char* const> rows,
    vector_size_t outputOffset,
    BaseVector* result,
    Timestamp* values,
    uint64_t* nulls) {
  values += outputOffset;
  if constexpr (!MayHaveNulls) {
    if (result->rawNulls() != nullptr) {
      result->clearNulls(outputOffset, outputOffset + rows.size());
    }
  } else {
    BOLT_DCHECK_NOT_NULL(nulls);
  }
  if constexpr (!MayHaveNulls) {
    BOLT_DCHECK(nulls == nullptr);
  }
  for (vector_size_t row = 0; row < rows.size(); ++row) {
    if constexpr (MayHaveNulls) {
      bits::setNull(
          nulls, outputOffset + row, isNull(rows[row], nullByte, nullMask));
    }
    const auto seconds = loadUnaligned<int64_t>(rows[row] + offset);
    const auto nanos =
        loadUnaligned<uint64_t>(rows[row] + offset + sizeof(int64_t));
    values[row] = Timestamp(seconds, nanos);
  }
}

template <bool MayHaveNulls>
void gatherFixedState(
    const FixedGatherState& state,
    std::span<char* const> rows,
    vector_size_t outputOffset,
    bool hasAvx2) {
  switch (state.kind) {
    case FixedGatherKind::kBoolean:
      gatherFixedBooleanColumn<MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<uint64_t*>(state.values),
          state.nulls);
      return;
    case FixedGatherKind::kInt8:
      gatherFixedColumn<int8_t, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<int8_t*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kInt16:
      gatherFixedColumn<int16_t, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<int16_t*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kInt32:
      gatherFixedColumn<int32_t, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<int32_t*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kInt64:
      gatherFixedColumn<int64_t, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<int64_t*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kInt128:
      gatherFixedColumn<int128_t, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<int128_t*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kFloat:
      gatherFixedColumn<float, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<float*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kDouble:
      gatherFixedColumn<double, MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<double*>(state.values),
          state.nulls,
          hasAvx2);
      return;
    case FixedGatherKind::kTimestamp:
      gatherFixedTimestampColumn<MayHaveNulls>(
          state.offset,
          state.nullByte,
          state.nullMask,
          rows,
          outputOffset,
          state.result,
          static_cast<Timestamp*>(state.values),
          state.nulls);
      return;
    case FixedGatherKind::kUnknown:
      bits::fillBits(
          state.nulls, outputOffset, outputOffset + rows.size(), bits::kNull);
      return;
  }
  BOLT_UNREACHABLE();
}

struct StringGatherState {
  uint64_t offset;
  uint32_t nullByte;
  uint8_t nullMask;
  column_index_t outputChannel;
  bool mayHaveNulls;
  FlatVector<StringView>* result{nullptr};
  StringView* values{nullptr};
  uint64_t* nulls{nullptr};
  uint64_t stringBytes{0};
  char* output{nullptr};
};

StringGatherState& asStringGatherState(StringGatherState& state) {
  return state;
}

StringGatherState& asStringGatherState(StringGatherState* state) {
  return *state;
}

template <bool MayHaveNulls, typename States>
void gatherStringColumns(
    std::span<char* const> rows,
    vector_size_t outputOffset,
    States& states) {
  for (auto& stateEntry : states) {
    auto& state = asStringGatherState(stateEntry);
    state.stringBytes = 0;
    state.output = nullptr;
    if constexpr (MayHaveNulls) {
      bits::fillBits(
          state.nulls,
          outputOffset,
          outputOffset + rows.size(),
          bits::kNotNull);
    } else if (state.result->rawNulls() != nullptr) {
      state.result->clearNulls(outputOffset, outputOffset + rows.size());
    }
  }

  constexpr vector_size_t kRowsPerTile = 32;
  for (vector_size_t begin = 0; begin < rows.size(); begin += kRowsPerTile) {
    const auto end = std::min<vector_size_t>(rows.size(), begin + kRowsPerTile);
    for (auto& stateEntry : states) {
      auto& state = asStringGatherState(stateEntry);
      for (vector_size_t row = begin; row < end; ++row) {
        const auto target = outputOffset + row;
        if constexpr (MayHaveNulls) {
          if (isNull(rows[row], state.nullByte, state.nullMask)) {
            bits::setNull(state.nulls, target, true);
            continue;
          }
        }
        const auto value = loadUnaligned<StringView>(rows[row] + state.offset);
        state.values[target] = value;
        if (!value.isInline()) {
          state.stringBytes += value.size();
        }
      }
    }
  }

  for (auto& stateEntry : states) {
    auto& state = asStringGatherState(stateEntry);
    state.output = state.stringBytes == 0
        ? nullptr
        : state.result->getRawStringBufferWithSpace(state.stringBytes, true);
  }

  for (vector_size_t begin = 0; begin < rows.size(); begin += kRowsPerTile) {
    const auto end = std::min<vector_size_t>(rows.size(), begin + kRowsPerTile);
    for (auto& stateEntry : states) {
      auto& state = asStringGatherState(stateEntry);
      for (vector_size_t row = begin; row < end; ++row) {
        const auto target = outputOffset + row;
        if constexpr (MayHaveNulls) {
          if (bits::isBitNull(state.nulls, target)) {
            continue;
          }
        }
        const auto value = state.values[target];
        if (value.isInline()) {
          continue;
        }
        std::memcpy(state.output, value.data(), value.size());
        state.values[target] =
            StringView(state.output, static_cast<int32_t>(value.size()));
        state.output += value.size();
      }
    }
  }
}

template <bool MayHaveNulls>
void gatherStringColumn(
    StringGatherState& state,
    std::span<char* const> rows,
    vector_size_t outputOffset) {
  uint64_t stringBytes = 0;
  auto* values = state.values;
  auto* nulls = state.nulls;
  if constexpr (MayHaveNulls) {
    bits::fillBits(
        nulls, outputOffset, outputOffset + rows.size(), bits::kNotNull);
  } else if (state.result->rawNulls() != nullptr) {
    state.result->clearNulls(outputOffset, outputOffset + rows.size());
  }

  for (vector_size_t row = 0; row < rows.size(); ++row) {
    const auto target = outputOffset + row;
    if constexpr (MayHaveNulls) {
      if (isNull(rows[row], state.nullByte, state.nullMask)) {
        bits::setNull(nulls, target, true);
        continue;
      }
    }
    const auto value = loadUnaligned<StringView>(rows[row] + state.offset);
    values[target] = value;
    if (!value.isInline()) {
      stringBytes += value.size();
    }
  }

  char* output = stringBytes == 0
      ? nullptr
      : state.result->getRawStringBufferWithSpace(stringBytes, true);

  for (vector_size_t row = 0; row < rows.size(); ++row) {
    const auto target = outputOffset + row;
    if constexpr (MayHaveNulls) {
      if (bits::isBitNull(nulls, target)) {
        continue;
      }
    }
    const auto value = values[target];
    if (value.isInline()) {
      continue;
    }
    std::memcpy(output, value.data(), value.size());
    values[target] = StringView(output, static_cast<int32_t>(value.size()));
    output += value.size();
  }
}

class SingleRangeByteInputStream final : public ByteInputStream {
 public:
  SingleRangeByteInputStream()
      : ByteInputStream(std::vector<ByteRange>{{nullptr, 0, 0}}) {}

  void reset(ByteRange range) {
    setRange(range);
  }
};

struct ComplexGatherState {
  uint64_t offset;
  uint32_t nullByte;
  uint8_t nullMask;
  column_index_t outputChannel;
  BaseVector* result{nullptr};
  uint64_t* nulls{nullptr};
};

void gatherComplexColumn(
    ComplexGatherState& state,
    std::span<char* const> rows,
    vector_size_t outputOffset) {
  SingleRangeByteInputStream input;
  for (vector_size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
    const auto* row = rows[rowIndex];
    const auto target = outputOffset + rowIndex;
    if (isNull(row, state.nullByte, state.nullMask)) {
      if (state.nulls == nullptr) {
        state.nulls = state.result->mutableRawNulls();
      }
      bits::setNull(state.nulls, target, true);
      continue;
    }
    const auto value = loadUnaligned<PayloadVarlenRef>(row + state.offset);
    if (value.size == 0) {
      if (state.nulls != nullptr) {
        bits::setNull(state.nulls, target, false);
      }
      continue;
    }
    input.reset(ByteRange{
        reinterpret_cast<uint8_t*>(value.data),
        static_cast<int32_t>(value.size),
        0});
    exec::ContainerRowSerde::deserialize(input, target, state.result, true);
  }
}

} // namespace

struct PayloadRowReader::Plan::Impl {
  explicit Impl(const PayloadRowLayout& rowLayout) : layout(&rowLayout) {}

  void gather(std::span<char* const> rows, vector_size_t outputOffset);

  const PayloadRowLayout* layout;
  std::vector<column_index_t> outputChannels;
  std::vector<FixedGatherState> fixedColumns;
  std::vector<FixedGatherState*> groupedFixed64Columns;
  std::vector<StringGatherState> stringColumns;
  std::vector<StringGatherState*> nonNullStringColumns;
  std::vector<StringGatherState*> nullableStringColumns;
  std::vector<ComplexGatherState> complexColumns;
  bool hasAvx2{false};
  RowVector* output{nullptr};
};

namespace {

FixedGatherKind fixedGatherKind(const Type& type) {
  if (type.isShortDecimal()) {
    return FixedGatherKind::kInt64;
  }
  if (type.isLongDecimal() || type.kind() == TypeKind::HUGEINT) {
    return FixedGatherKind::kInt128;
  }
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      return FixedGatherKind::kBoolean;
    case TypeKind::TINYINT:
      return FixedGatherKind::kInt8;
    case TypeKind::SMALLINT:
      return FixedGatherKind::kInt16;
    case TypeKind::INTEGER:
      return FixedGatherKind::kInt32;
    case TypeKind::BIGINT:
      return FixedGatherKind::kInt64;
    case TypeKind::REAL:
      return FixedGatherKind::kFloat;
    case TypeKind::DOUBLE:
      return FixedGatherKind::kDouble;
    case TypeKind::TIMESTAMP:
      return FixedGatherKind::kTimestamp;
    case TypeKind::UNKNOWN:
      return FixedGatherKind::kUnknown;
    default:
      BOLT_FAIL(
          "Sort fixed payload gather is not implemented for {}",
          type.toString());
  }
}

void* mutableFixedValues(BaseVector* result, FixedGatherKind kind) {
  switch (kind) {
    case FixedGatherKind::kBoolean:
      return result->asUnchecked<FlatVector<bool>>()
          ->mutableRawValues<uint64_t>();
    case FixedGatherKind::kInt8:
      return result->asUnchecked<FlatVector<int8_t>>()->mutableRawValues();
    case FixedGatherKind::kInt16:
      return result->asUnchecked<FlatVector<int16_t>>()->mutableRawValues();
    case FixedGatherKind::kInt32:
      return result->asUnchecked<FlatVector<int32_t>>()->mutableRawValues();
    case FixedGatherKind::kInt64:
      return result->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
    case FixedGatherKind::kInt128:
      return result->asUnchecked<FlatVector<int128_t>>()->mutableRawValues();
    case FixedGatherKind::kFloat:
      return result->asUnchecked<FlatVector<float>>()->mutableRawValues();
    case FixedGatherKind::kDouble:
      return result->asUnchecked<FlatVector<double>>()->mutableRawValues();
    case FixedGatherKind::kTimestamp:
      return result->asUnchecked<FlatVector<Timestamp>>()->mutableRawValues();
    case FixedGatherKind::kUnknown:
      return nullptr;
  }
  BOLT_UNREACHABLE();
}

FixedGatherState makeBoundFixedState(
    const PayloadRowColumnLayout& column,
    column_index_t outputChannel,
    bool mayHaveNulls,
    BaseVector* result) {
  const auto kind = fixedGatherKind(*column.type);
  return FixedGatherState{
      column.offset,
      column.nullByte,
      column.nullMask,
      outputChannel,
      kind,
      mayHaveNulls,
      isFixed64Column(column),
      result,
      mutableFixedValues(result, kind),
      mayHaveNulls || kind == FixedGatherKind::kUnknown
          ? result->mutableRawNulls()
          : nullptr};
}

StringGatherState makeBoundStringState(
    const PayloadRowColumnLayout& column,
    column_index_t outputChannel,
    bool mayHaveNulls,
    BaseVector* result) {
  auto* flat = result->asUnchecked<FlatVector<StringView>>();
  return StringGatherState{
      column.offset,
      column.nullByte,
      column.nullMask,
      outputChannel,
      mayHaveNulls,
      flat,
      flat->mutableRawValues(),
      mayHaveNulls ? flat->mutableRawNulls() : nullptr};
}

void gatherStandalone(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    std::span<const uint8_t> mayHaveNulls,
    memory::MemoryPool* pool,
    RowVectorPtr& result) {
  BOLT_CHECK_NOT_NULL(pool, "Payload row output memory pool must not be null");
  prepareResult(layout, static_cast<vector_size_t>(rows.size()), pool, result);
  const bool hasAvx2 = process::hasAvx2();

  if (!layout.hasVariableFields()) {
    folly::small_vector<FixedGatherState, 32> groupedFixed64Columns;
    if (hasAvx2 && rows.size() >= xsimd::batch<int64_t>::size) {
      for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
           ++columnIndex) {
        const auto& column = layout.columns()[columnIndex];
        if (isFixed64Column(column)) {
          groupedFixed64Columns.push_back(makeBoundFixedState(
              column,
              columnIndex,
              mayHaveNulls[columnIndex] != 0,
              result->childAt(columnIndex).get()));
        }
      }
    }
    if (groupedFixed64Columns.size() > 1) {
      gatherFixed64Columns(rows, 0, groupedFixed64Columns);
    } else {
      groupedFixed64Columns.clear();
    }
    for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
         ++columnIndex) {
      const auto& column = layout.columns()[columnIndex];
      if (!groupedFixed64Columns.empty() && isFixed64Column(column)) {
        continue;
      }
      const auto state = makeBoundFixedState(
          column,
          columnIndex,
          mayHaveNulls[columnIndex] != 0,
          result->childAt(columnIndex).get());
      if (state.mayHaveNulls) {
        gatherFixedState<true>(state, rows, 0, hasAvx2);
      } else {
        gatherFixedState<false>(state, rows, 0, hasAvx2);
      }
    }
    return;
  }

  folly::small_vector<StringGatherState, 32> stringColumns;
  for (uint32_t columnIndex = 0; columnIndex < layout.columns().size();
       ++columnIndex) {
    const auto& column = layout.columns()[columnIndex];
    auto* child = result->childAt(columnIndex).get();
    if (column.complex) {
      ComplexGatherState state{
          column.offset,
          column.nullByte,
          column.nullMask,
          static_cast<column_index_t>(columnIndex),
          child,
          child->rawNulls() == nullptr ? nullptr : child->mutableRawNulls()};
      gatherComplexColumn(state, rows, 0);
    } else if (column.variable) {
      stringColumns.push_back(makeBoundStringState(
          column, columnIndex, mayHaveNulls[columnIndex] != 0, child));
    } else {
      const auto state = makeBoundFixedState(
          column, columnIndex, mayHaveNulls[columnIndex] != 0, child);
      if (state.mayHaveNulls) {
        gatherFixedState<true>(state, rows, 0, hasAvx2);
      } else {
        gatherFixedState<false>(state, rows, 0, hasAvx2);
      }
    }
  }
  if (stringColumns.size() == 1) {
    auto& state = stringColumns.front();
    if (state.mayHaveNulls) {
      gatherStringColumn<true>(state, rows, 0);
    } else {
      gatherStringColumn<false>(state, rows, 0);
    }
    return;
  }
  folly::small_vector<StringGatherState*, 32> nonNullStringColumns;
  folly::small_vector<StringGatherState*, 32> nullableStringColumns;
  for (auto& state : stringColumns) {
    auto& states =
        state.mayHaveNulls ? nullableStringColumns : nonNullStringColumns;
    states.push_back(&state);
  }
  gatherStringColumns<false>(rows, 0, nonNullStringColumns);
  gatherStringColumns<true>(rows, 0, nullableStringColumns);
}

} // namespace

void PayloadRowReader::Plan::Impl::gather(
    std::span<char* const> rows,
    vector_size_t outputOffset) {
  const bool useGroupedFixed64 = hasAvx2 &&
      rows.size() >= xsimd::batch<int64_t>::size &&
      groupedFixed64Columns.size() > 1;
  if (useGroupedFixed64) {
    gatherFixed64Columns(rows, outputOffset, groupedFixed64Columns);
  }
  for (const auto& state : fixedColumns) {
    if (useGroupedFixed64 && state.grouped64) {
      continue;
    }
    if (state.mayHaveNulls) {
      gatherFixedState<true>(state, rows, outputOffset, hasAvx2);
    } else {
      gatherFixedState<false>(state, rows, outputOffset, hasAvx2);
    }
  }

  if (stringColumns.size() == 1) {
    auto& state = stringColumns.front();
    if (state.mayHaveNulls) {
      gatherStringColumn<true>(state, rows, outputOffset);
    } else {
      gatherStringColumn<false>(state, rows, outputOffset);
    }
  } else if (!stringColumns.empty()) {
    gatherStringColumns<false>(rows, outputOffset, nonNullStringColumns);
    gatherStringColumns<true>(rows, outputOffset, nullableStringColumns);
  }

  for (auto& state : complexColumns) {
    gatherComplexColumn(state, rows, outputOffset);
  }
}

PayloadRowReader::Plan::Plan(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

PayloadRowReader::Plan::Plan(Plan&&) noexcept = default;

PayloadRowReader::Plan& PayloadRowReader::Plan::operator=(Plan&&) noexcept =
    default;

PayloadRowReader::Plan::~Plan() = default;

PayloadRowReader::Plan PayloadRowReader::makePlan(
    const PayloadRowLayout& layout,
    std::span<const column_index_t> payloadChannels,
    std::span<const uint8_t> mayHaveNulls) {
#ifndef NDEBUG
  BOLT_DCHECK_EQ(mayHaveNulls.size(), layout.columns().size());
  BOLT_DCHECK(
      payloadChannels.empty() ||
      payloadChannels.size() == layout.columns().size());
#endif
  auto impl = std::make_unique<Plan::Impl>(layout);
  impl->outputChannels.reserve(layout.columns().size());
  impl->fixedColumns.reserve(layout.columns().size());
  impl->stringColumns.reserve(layout.variableColumns().size());
  impl->complexColumns.reserve(layout.variableColumns().size());
  for (uint32_t index = 0; index < layout.columns().size(); ++index) {
    const auto& column = layout.columns()[index];
    const auto outputChannel = payloadChannels.empty()
        ? static_cast<column_index_t>(index)
        : payloadChannels[index];
    impl->outputChannels.push_back(outputChannel);
    if (column.complex) {
      impl->complexColumns.push_back(ComplexGatherState{
          column.offset, column.nullByte, column.nullMask, outputChannel});
    } else if (column.variable) {
      impl->stringColumns.push_back(StringGatherState{
          column.offset,
          column.nullByte,
          column.nullMask,
          outputChannel,
          mayHaveNulls[index] != 0});
    } else {
      const auto kind = fixedGatherKind(*column.type);
      impl->fixedColumns.push_back(FixedGatherState{
          column.offset,
          column.nullByte,
          column.nullMask,
          outputChannel,
          kind,
          mayHaveNulls[index] != 0,
          isFixed64Column(column)});
    }
  }
  if (!layout.hasVariableFields()) {
    impl->groupedFixed64Columns.reserve(impl->fixedColumns.size());
    for (auto& state : impl->fixedColumns) {
      if (state.grouped64) {
        impl->groupedFixed64Columns.push_back(&state);
      }
    }
  }
  impl->nonNullStringColumns.reserve(impl->stringColumns.size());
  impl->nullableStringColumns.reserve(impl->stringColumns.size());
  for (auto& state : impl->stringColumns) {
    auto& states = state.mayHaveNulls ? impl->nullableStringColumns
                                      : impl->nonNullStringColumns;
    states.push_back(&state);
  }
  return Plan(std::move(impl));
}

void PayloadRowReader::bind(Plan& plan, RowVector& output) {
  auto& impl = *plan.impl_;
#ifndef NDEBUG
  BOLT_DCHECK(impl.output == nullptr);
  BOLT_DCHECK_NOT_NULL(output.pool());
  for (uint32_t index = 0; index < impl.outputChannels.size(); ++index) {
    const auto channel = impl.outputChannels[index];
    BOLT_DCHECK_LT(channel, output.childrenSize());
    const auto& child = output.childAt(channel);
    BOLT_DCHECK_NOT_NULL(child);
    BOLT_DCHECK(child->type()->equivalent(*impl.layout->columns()[index].type));
    BOLT_DCHECK_GE(child->size(), output.size());
  }
#endif
  for (auto& state : impl.fixedColumns) {
    state.result = output.childAt(state.outputChannel).get();
    state.values = mutableFixedValues(state.result, state.kind);
    state.nulls = state.mayHaveNulls || state.kind == FixedGatherKind::kUnknown
        ? state.result->mutableRawNulls()
        : nullptr;
  }
  for (auto& state : impl.stringColumns) {
    state.result = output.childAt(state.outputChannel)
                       ->asUnchecked<FlatVector<StringView>>();
    state.values = state.result->mutableRawValues();
    state.nulls =
        state.mayHaveNulls ? state.result->mutableRawNulls() : nullptr;
  }
  for (auto& state : impl.complexColumns) {
    state.result = output.childAt(state.outputChannel).get();
    state.nulls = state.result->rawNulls() == nullptr
        ? nullptr
        : state.result->mutableRawNulls();
  }
  impl.hasAvx2 = process::hasAvx2();
  impl.output = &output;
}

void PayloadRowReader::gather(
    Plan& plan,
    std::span<char* const> rows,
    vector_size_t outputOffset) {
  auto& impl = *plan.impl_;
  BOLT_DCHECK_NOT_NULL(impl.output);
  BOLT_DCHECK_GE(outputOffset, 0);
  BOLT_DCHECK_LE(outputOffset, impl.output->size());
  BOLT_DCHECK_LE(
      rows.size(), static_cast<size_t>(impl.output->size() - outputOffset));
  if (rows.empty()) {
    return;
  }
  impl.gather(rows, outputOffset);
}

void PayloadRowReader::finish(Plan& plan) {
  auto& impl = *plan.impl_;
  auto* output = impl.output;
  BOLT_DCHECK_NOT_NULL(output);
  impl.output = nullptr;
  for (const auto channel : impl.outputChannels) {
    output->childAt(channel)->resetDataDependentFlags(nullptr);
  }
}

void PayloadRowReader::gather(
    const PayloadRowLayout& layout,
    std::span<char* const> rows,
    memory::MemoryPool* pool,
    RowVectorPtr& result,
    std::span<const uint8_t> mayHaveNulls) {
  if (!mayHaveNulls.empty()) {
    gatherStandalone(layout, rows, mayHaveNulls, pool, result);
    return;
  }
  folly::small_vector<uint8_t, 32> inferredMayHaveNulls(
      layout.columns().size(), 0);
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    uint8_t hasNull = 0;
    for (const auto* row : rows) {
      const auto& metadata = layout.columns()[column];
      hasNull |= static_cast<uint8_t>(
          isNull(row, metadata.nullByte, metadata.nullMask));
    }
    inferredMayHaveNulls[column] = hasNull;
  }
  gatherStandalone(
      layout,
      rows,
      std::span<const uint8_t>(
          inferredMayHaveNulls.data(), inferredMayHaveNulls.size()),
      pool,
      result);
}

namespace {

class BoundedPayloadStreamArena final : public StreamArena {
 public:
  BoundedPayloadStreamArena() : StreamArena(nullptr) {}

  void newRange(int32_t, ByteRange*, ByteRange*) override {
    BOLT_FAIL("Sort payload contiguous output is too small");
  }
};

namespace payload_size {

class RowSizes {
 public:
  const BufferPtr& heapSizes() const {
    return heapSizes_;
  }

  const uint64_t* complexSizes(uint32_t ordinal) const {
    return complexSizes_ + static_cast<uint64_t>(ordinal) * size_;
  }

  vector_size_t size_{0};
  BufferPtr heapSizes_;
  const uint64_t* complexSizes_{nullptr};
};

FOLLY_ALWAYS_INLINE void addSize(uint64_t& size, uint64_t increment) {
  size += increment;
}

int32_t checkedComplexSerializedSize(uint64_t size) {
  BOLT_CHECK_LE(
      size,
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
      "A single complex sort payload serialization exceeds ByteRange "
      "capacity: {} bytes",
      size);
  return static_cast<int32_t>(size);
}

uint64_t nullBitmapBytes(vector_size_t size) {
  return bits::nwords(static_cast<uint64_t>(size)) * sizeof(uint64_t);
}

std::optional<uint64_t> fixedSerializedSize(const Type& type) {
  if (type.kind() == TypeKind::UNKNOWN) {
    return sizeof(UnknownValue);
  }
  return type.isFixedWidth() ? std::optional<uint64_t>{type.cppSizeInBytes()}
                             : std::nullopt;
}

struct RangeSizeMetadata {
  std::optional<uint64_t> fixedElementSize;
  bool mayHaveNulls{true};
};

RangeSizeMetadata rangeSizeMetadata(const BaseVector& elements) {
  return RangeSizeMetadata{
      fixedSerializedSize(*elements.type()), elements.mayHaveNulls()};
}

uint64_t serializedSizeForPayload(const BaseVector& vector, vector_size_t row);

uint64_t serializedRangeSizeForPayload(
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t count,
    const RangeSizeMetadata& metadata) {
  uint64_t size = sizeof(int32_t) + nullBitmapBytes(count);
  if (metadata.fixedElementSize.has_value()) {
    if (!metadata.mayHaveNulls) {
      const auto valuesSize =
          static_cast<uint64_t>(count) * *metadata.fixedElementSize;
      addSize(size, valuesSize);
      return size;
    }
    for (vector_size_t element = 0; element < count; ++element) {
      if (!elements.isNullAt(offset + element)) {
        addSize(size, *metadata.fixedElementSize);
      }
    }
    return size;
  }
  for (vector_size_t element = 0; element < count; ++element) {
    const auto index = offset + element;
    if (!elements.isNullAt(index)) {
      addSize(size, serializedSizeForPayload(elements, index));
    }
  }
  return size;
}

uint64_t serializedArraySizeForPayload(
    const ArrayVector& array,
    vector_size_t row,
    const RangeSizeMetadata& elementMetadata) {
  return serializedRangeSizeForPayload(
      *array.elements(),
      array.offsetAt(row),
      array.sizeAt(row),
      elementMetadata);
}

uint64_t serializedArraySizeForPayload(
    const ArrayVector& array,
    vector_size_t row) {
  return serializedArraySizeForPayload(
      array, row, rangeSizeMetadata(*array.elements()));
}

uint64_t serializedMapSizeForPayload(
    const MapVector& map,
    vector_size_t row,
    const RangeSizeMetadata& keyMetadata,
    const RangeSizeMetadata& valueMetadata) {
  const auto offset = map.offsetAt(row);
  const auto count = map.sizeAt(row);
  uint64_t size =
      serializedRangeSizeForPayload(*map.mapKeys(), offset, count, keyMetadata);
  addSize(
      size,
      serializedRangeSizeForPayload(
          *map.mapValues(), offset, count, valueMetadata));
  return size;
}

uint64_t serializedMapSizeForPayload(const MapVector& map, vector_size_t row) {
  return serializedMapSizeForPayload(
      map,
      row,
      rangeSizeMetadata(*map.mapKeys()),
      rangeSizeMetadata(*map.mapValues()));
}

uint64_t serializedSizeForPayload(const BaseVector& vector, vector_size_t row) {
  const auto fixedSize = fixedSerializedSize(*vector.type());
  if (fixedSize.has_value()) {
    return *fixedSize;
  }

  switch (vector.typeKind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const auto value =
          vector.asUnchecked<SimpleVector<StringView>>()->valueAt(row);
      return sizeof(int32_t) + value.size();
    }
    case TypeKind::VARIANT: {
      const auto* variant =
          vector.wrappedVector()->asUnchecked<VariantVector>();
      const auto value = variant->valueAt(vector.wrappedIndex(row));
      return 2 * sizeof(int32_t) + value.value.size() + value.metadata.size();
    }
    case TypeKind::ARRAY: {
      const auto* array = vector.wrappedVector()->asUnchecked<ArrayVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      return serializedArraySizeForPayload(*array, wrappedRow);
    }
    case TypeKind::MAP: {
      const auto* map = vector.wrappedVector()->asUnchecked<MapVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      return serializedMapSizeForPayload(*map, wrappedRow);
    }
    case TypeKind::ROW: {
      const auto* rowVector = vector.wrappedVector()->asUnchecked<RowVector>();
      const auto wrappedRow = vector.wrappedIndex(row);
      uint64_t size = nullBitmapBytes(rowVector->type()->size());
      for (uint32_t child = 0; child < rowVector->childrenSize(); ++child) {
        const auto& childVector = rowVector->childAt(child);
        if (childVector == nullptr || childVector->isNullAt(wrappedRow)) {
          continue;
        }
        addSize(size, serializedSizeForPayload(*childVector, wrappedRow));
      }
      return size;
    }
    default:
      BOLT_FAIL(
          "Payload serialized size is not implemented for ",
          vector.type()->toString());
  }
}

void measurePayloadRows(
    const RowVector& input,
    const PayloadRowLayout& layout,
    memory::MemoryPool* pool,
    std::vector<std::optional<DecodedVector>>& decoded,
    BufferPtr reusableHeapSizes,
    BufferPtr& complexSizeScratch,
    RowSizes& sizes) {
  sizes = RowSizes{};

  const auto rowCount = input.size();

  sizes.size_ = rowCount;
  if (rowCount == 0 || !layout.hasVariableFields()) {
    return;
  }

  const auto complexColumnCount = std::count_if(
      layout.columns().begin(), layout.columns().end(), [](const auto& column) {
        return column.complex;
      });
  auto* rawHeapSizes =
      prepareReusableBuffer<uint64_t>(reusableHeapSizes, rowCount, pool);
  std::fill(rawHeapSizes, rawHeapSizes + rowCount, uint64_t{0});
  sizes.heapSizes_ = std::move(reusableHeapSizes);
  if (complexColumnCount > 0) {
    const auto complexSizeCount = checkedMultiply<size_t>(
        static_cast<size_t>(rowCount), complexColumnCount);
    BOLT_CHECK(
        complexSizeCount.has_value(),
        "Payload row complex size scratch count overflows");
    auto* rawComplexSizes = prepareReusableBuffer<uint64_t>(
        complexSizeScratch, *complexSizeCount, pool);
    std::fill(
        rawComplexSizes, rawComplexSizes + *complexSizeCount, uint64_t{0});
    sizes.complexSizes_ = rawComplexSizes;
  }
  const auto addVariableBytes = [&](vector_size_t row, uint64_t bytes) {
    addSize(rawHeapSizes[row], bytes);
  };

  uint32_t complexOrdinal = 0;
  for (uint32_t column = 0; column < layout.columns().size(); ++column) {
    const auto& metadata = layout.columns()[column];
    if (!metadata.variable) {
      continue;
    }
    const auto& vector = *input.childAt(column);

    if (metadata.complex) {
      auto* rawComplexSizes =
          const_cast<uint64_t*>(sizes.complexSizes(complexOrdinal++));
      const auto addComplexBytes = [&](vector_size_t row, uint64_t bytes) {
        checkedComplexSerializedSize(bytes);
        rawComplexSizes[row] = bytes;
        addVariableBytes(row, bytes);
      };
      switch (metadata.type->kind()) {
        case TypeKind::ARRAY: {
          const auto* array =
              vector.wrappedVector()->asUnchecked<ArrayVector>();
          const auto elementMetadata = rangeSizeMetadata(*array->elements());
          if (!vector.mayHaveNulls()) {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              addComplexBytes(
                  row,
                  serializedArraySizeForPayload(
                      *array, vector.wrappedIndex(row), elementMetadata));
            }
          } else {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              if (!vector.isNullAt(row)) {
                addComplexBytes(
                    row,
                    serializedArraySizeForPayload(
                        *array, vector.wrappedIndex(row), elementMetadata));
              }
            }
          }
          break;
        }
        case TypeKind::MAP: {
          const auto* map = vector.wrappedVector()->asUnchecked<MapVector>();
          const auto keyMetadata = rangeSizeMetadata(*map->mapKeys());
          const auto valueMetadata = rangeSizeMetadata(*map->mapValues());
          if (!vector.mayHaveNulls()) {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              addComplexBytes(
                  row,
                  serializedMapSizeForPayload(
                      *map,
                      vector.wrappedIndex(row),
                      keyMetadata,
                      valueMetadata));
            }
          } else {
            for (vector_size_t row = 0; row < rowCount; ++row) {
              if (!vector.isNullAt(row)) {
                addComplexBytes(
                    row,
                    serializedMapSizeForPayload(
                        *map,
                        vector.wrappedIndex(row),
                        keyMetadata,
                        valueMetadata));
              }
            }
          }
          break;
        }
        case TypeKind::ROW: {
          const auto* rowVector =
              vector.wrappedVector()->asUnchecked<RowVector>();
          for (vector_size_t row = 0; row < rowCount; ++row) {
            if (vector.isNullAt(row)) {
              continue;
            }
            const auto wrappedRow = vector.wrappedIndex(row);
            uint64_t size = nullBitmapBytes(rowVector->type()->size());
            for (uint32_t child = 0; child < rowVector->childrenSize();
                 ++child) {
              const auto& childVector = rowVector->childAt(child);
              if (childVector == nullptr || childVector->isNullAt(wrappedRow)) {
                continue;
              }
              addSize(size, serializedSizeForPayload(*childVector, wrappedRow));
            }
            addComplexBytes(row, size);
          }
          break;
        }
        default:
          BOLT_FAIL(
              "Payload complex size is not implemented for ",
              metadata.type->toString());
      }
      continue;
    }

    if (!decoded[column].has_value()) {
      const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
      const auto* values = flat->rawValues();
      const auto* nulls = flat->rawNulls();
      for (vector_size_t row = 0; row < rowCount; ++row) {
        if (nulls != nullptr && bits::isBitNull(nulls, row)) {
          continue;
        }
        const auto value = values[row];
        if (!value.isInline()) {
          addVariableBytes(row, value.size());
        }
      }
      continue;
    }

    auto& decodedVector = *decoded[column];
    if (decodedVector.isConstantMapping()) {
      if (decodedVector.isNullAt(0)) {
        continue;
      }
      const auto value = decodedVector.valueAt<StringView>(0);
      if (!value.isInline()) {
        for (vector_size_t row = 0; row < rowCount; ++row) {
          addVariableBytes(row, value.size());
        }
      }
      continue;
    }
    const auto* values = decodedVector.data<StringView>();
    const auto* indices = decodedVector.indices();
    const auto* nulls = decodedVector.nulls();
    for (vector_size_t row = 0; row < rowCount; ++row) {
      if (nulls != nullptr && bits::isBitNull(nulls, row)) {
        continue;
      }
      const auto value = values[indices[row]];
      if (!value.isInline()) {
        addVariableBytes(row, value.size());
      }
    }
  }
}

} // namespace payload_size

void setNull(char* row, const PayloadRowColumnLayout& column) {
  row[column.nullByte] = static_cast<char>(
      static_cast<uint8_t>(row[column.nullByte]) &
      static_cast<uint8_t>(~column.nullMask));
}

bool shouldClearPayloadSlots(const RowVector& input) {
  for (const auto& vector : input.children()) {
    if (vector->encoding() != VectorEncoding::Simple::FLAT ||
        vector->typeKind() == TypeKind::UNKNOWN ||
        vector->rawNulls() != nullptr) {
      return true;
    }
  }
  return false;
}

template <typename T>
void writeFixedFlatColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  const auto* flat = vector.asUnchecked<FlatVector<T>>();
  const auto* values = flat->rawValues();
  const auto* nulls = flat->rawNulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
    } else {
      storeUnaligned<T>(rows[row] + column.offset, values[row]);
    }
  }
}

template <>
void writeFixedFlatColumn<bool>(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  const auto* flat = vector.asUnchecked<FlatVector<bool>>();
  const auto* nulls = flat->rawNulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
    } else {
      storeUnaligned<uint8_t>(
          rows[row] + column.offset,
          static_cast<uint8_t>(flat->valueAtFast(row)));
    }
  }
}

void writeFixedFlatColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  if (column.type->isShortDecimal()) {
    writeFixedFlatColumn<int64_t>(column, vector, rows, begin, end);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    writeFixedFlatColumn<int128_t>(column, vector, rows, begin, end);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN:
      writeFixedFlatColumn<bool>(column, vector, rows, begin, end);
      return;
    case TypeKind::TINYINT:
      writeFixedFlatColumn<int8_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::SMALLINT:
      writeFixedFlatColumn<int16_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::INTEGER:
      writeFixedFlatColumn<int32_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::BIGINT:
      writeFixedFlatColumn<int64_t>(column, vector, rows, begin, end);
      return;
    case TypeKind::REAL:
      writeFixedFlatColumn<float>(column, vector, rows, begin, end);
      return;
    case TypeKind::DOUBLE:
      writeFixedFlatColumn<double>(column, vector, rows, begin, end);
      return;
    case TypeKind::TIMESTAMP:
      writeFixedFlatColumn<Timestamp>(column, vector, rows, begin, end);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload fast path is not implemented for ",
          column.type->toString());
  }
}

void writeFlatStringValue(
    const PayloadRowColumnLayout& column,
    const StringView* values,
    const uint64_t* nulls,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t* heapRemaining) {
  if (nulls != nullptr && bits::isBitNull(nulls, row)) {
    setNull(payloadRow, column);
    return;
  }
  const auto value = values[row];
  auto* slot = payloadRow + column.offset;
  if (value.isInline()) {
    storeUnaligned<StringView>(slot, value);
    return;
  }
  std::memcpy(heapCursor, value.data(), value.size());
  storeUnaligned<StringView>(
      slot, StringView(heapCursor, static_cast<int32_t>(value.size())));
  heapCursor += value.size();
  if (heapRemaining != nullptr) {
    *heapRemaining -= value.size();
  }
}

template <typename T>
void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    DecodedVector& decoded,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(0)) {
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    }
    const auto value = decoded.valueAt<T>(0);
    for (vector_size_t row = begin; row < end; ++row) {
      storeUnaligned<T>(rows[row] + column.offset, value);
    }
    return;
  }
  const auto* values = decoded.data<T>();
  const auto* indices = decoded.indices();
  const auto* nulls = decoded.nulls();
  for (vector_size_t row = begin; row < end; ++row) {
    if (nulls != nullptr && bits::isBitNull(nulls, row)) {
      setNull(rows[row], column);
    } else {
      const auto index = indices[row];
      if constexpr (std::is_same_v<T, int128_t>) {
        storeUnaligned<T>(
            rows[row] + column.offset,
            HugeInt::deserialize(
                reinterpret_cast<const char*>(values) + sizeof(T) * index));
      } else {
        storeUnaligned<T>(rows[row] + column.offset, values[index]);
      }
    }
  }
}

void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    DecodedVector& decoded,
    char* const* rows,
    vector_size_t begin,
    vector_size_t end) {
  if (column.type->isShortDecimal()) {
    writeFixedDecodedColumn<int64_t>(column, decoded, rows, begin, end);
    return;
  }
  if (column.type->isLongDecimal() ||
      column.type->kind() == TypeKind::HUGEINT) {
    writeFixedDecodedColumn<int128_t>(column, decoded, rows, begin, end);
    return;
  }
  switch (column.type->kind()) {
    case TypeKind::BOOLEAN: {
      if (decoded.isConstantMapping()) {
        const bool isNull = decoded.isNullAt(0);
        const auto value = isNull
            ? uint8_t{0}
            : static_cast<uint8_t>(decoded.valueAt<bool>(0));
        for (vector_size_t row = begin; row < end; ++row) {
          if (isNull) {
            setNull(rows[row], column);
          } else {
            storeUnaligned<uint8_t>(rows[row] + column.offset, value);
          }
        }
        return;
      }
      const auto* values = decoded.data<uint64_t>();
      const auto* indices = decoded.indices();
      const auto* nulls = decoded.nulls();
      for (vector_size_t row = begin; row < end; ++row) {
        if (nulls != nullptr && bits::isBitNull(nulls, row)) {
          setNull(rows[row], column);
        } else {
          storeUnaligned<uint8_t>(
              rows[row] + column.offset,
              static_cast<uint8_t>(bits::isBitSet(values, indices[row])));
        }
      }
      return;
    }
    case TypeKind::TINYINT:
      writeFixedDecodedColumn<int8_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::SMALLINT:
      writeFixedDecodedColumn<int16_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::INTEGER:
      writeFixedDecodedColumn<int32_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::BIGINT:
      writeFixedDecodedColumn<int64_t>(column, decoded, rows, begin, end);
      return;
    case TypeKind::REAL:
      writeFixedDecodedColumn<float>(column, decoded, rows, begin, end);
      return;
    case TypeKind::DOUBLE:
      writeFixedDecodedColumn<double>(column, decoded, rows, begin, end);
      return;
    case TypeKind::TIMESTAMP:
      writeFixedDecodedColumn<Timestamp>(column, decoded, rows, begin, end);
      return;
    case TypeKind::UNKNOWN:
      for (vector_size_t row = begin; row < end; ++row) {
        setNull(rows[row], column);
      }
      return;
    default:
      BOLT_FAIL(
          "Sort fixed payload column is not implemented for ",
          column.type->toString());
  }
}

void writeFixedDecodedColumn(
    const PayloadRowColumnLayout& column,
    DecodedVector& decoded,
    char* const* rows,
    vector_size_t size) {
  writeFixedDecodedColumn(column, decoded, rows, 0, size);
}

void writeConstantStringValue(
    const PayloadRowColumnLayout& column,
    StringView value,
    bool isNull,
    char* payloadRow,
    char*& heapCursor,
    uint64_t* heapRemaining) {
  if (isNull) {
    setNull(payloadRow, column);
    return;
  }
  auto* slot = payloadRow + column.offset;
  if (value.isInline()) {
    storeUnaligned<StringView>(slot, value);
    return;
  }
  std::memcpy(heapCursor, value.data(), value.size());
  storeUnaligned<StringView>(
      slot, StringView(heapCursor, static_cast<int32_t>(value.size())));
  heapCursor += value.size();
  if (heapRemaining != nullptr) {
    *heapRemaining -= value.size();
  }
}

void writeStringDecodedValue(
    const PayloadRowColumnLayout& column,
    const StringView* values,
    const vector_size_t* indices,
    const uint64_t* nulls,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t* heapRemaining) {
  if (nulls != nullptr && bits::isBitNull(nulls, row)) {
    setNull(payloadRow, column);
    return;
  }
  const auto value = values[indices[row]];
  auto* slot = payloadRow + column.offset;
  if (value.isInline()) {
    storeUnaligned<StringView>(slot, value);
    return;
  }
  std::memcpy(heapCursor, value.data(), value.size());
  storeUnaligned<StringView>(
      slot, StringView(heapCursor, static_cast<int32_t>(value.size())));
  heapCursor += value.size();
  if (heapRemaining != nullptr) {
    *heapRemaining -= value.size();
  }
}

void writeComplexDecodedValue(
    const PayloadRowColumnLayout& column,
    const DecodedVector& decoded,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t& heapRemaining,
    uint64_t serializedSize,
    ByteOutputStream& stream) {
  if (decoded.isNullAt(row)) {
    setNull(payloadRow, column);
    return;
  }
  if (serializedSize == 0) {
    storeUnaligned<PayloadVarlenRef>(
        payloadRow + column.offset, PayloadVarlenRef{0, nullptr});
    return;
  }
  const auto decodedRow = decoded.index(row);
  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(heapCursor),
          static_cast<int32_t>(serializedSize),
          0},
      0);
  exec::ContainerRowSerde::serialize(
      *decoded.base(),
      decodedRow,
      stream,
      exec::ContainerRowSerdeOptions{.isKey = false});
  const auto valueSize = stream.size();
  BOLT_DCHECK_EQ(valueSize, serializedSize);
  storeUnaligned<PayloadVarlenRef>(
      payloadRow + column.offset,
      PayloadVarlenRef{valueSize, valueSize == 0 ? nullptr : heapCursor});
  if (valueSize > 0) {
    heapCursor += valueSize;
  }
  heapRemaining -= valueSize;
}

void writeFixedColumn(
    const PayloadRowColumnLayout& column,
    const BaseVector& vector,
    DecodedVector* decoded,
    char* const* rows,
    vector_size_t size) {
  if (decoded == nullptr) {
    writeFixedFlatColumn(column, vector, rows, 0, size);
  } else {
    writeFixedDecodedColumn(column, *decoded, rows, size);
  }
}

void writeVariableValue(
    const VariableColumnWriter& writer,
    vector_size_t row,
    char* payloadRow,
    char*& heapCursor,
    uint64_t& heapRemaining,
    ByteOutputStream& stream) {
  switch (writer.kind) {
    case VariableWriterKind::kComplex:
      writeComplexDecodedValue(
          *writer.column,
          *writer.decoded,
          row,
          payloadRow,
          heapCursor,
          heapRemaining,
          writer.complexSizes[row],
          stream);
      return;
    case VariableWriterKind::kFlatString:
      writeFlatStringValue(
          *writer.column,
          writer.flatStringValues,
          writer.flatStringNulls,
          row,
          payloadRow,
          heapCursor,
          &heapRemaining);
      return;
    case VariableWriterKind::kConstantString:
      writeConstantStringValue(
          *writer.column,
          writer.constantStringValue,
          writer.constantStringNull,
          payloadRow,
          heapCursor,
          &heapRemaining);
      return;
    case VariableWriterKind::kDecodedString:
      writeStringDecodedValue(
          *writer.column,
          writer.decodedStringValues,
          writer.decodedStringIndices,
          writer.decodedStringNulls,
          row,
          payloadRow,
          heapCursor,
          &heapRemaining);
      return;
  }
  BOLT_UNREACHABLE();
}

void appendRows(
    const RowVector& input,
    RadixSortRunStorage& arena,
    const payload_size::RowSizes* sizes,
    std::vector<std::optional<DecodedVector>>& decoded,
    folly::small_vector<VariableColumnWriter, 32>& variableColumns,
    PayloadRowBatch& batch) {
  const auto& layout = arena.payloadLayout();
  BOLT_CHECK_NOT_NULL(
      layout, "Radix sort run storage does not have a payload layout");
  const auto hasVariableFields = layout->hasVariableFields();
  const uint64_t* rawHeapSizes = nullptr;
  if (hasVariableFields) {
    rawHeapSizes = sizes->heapSizes() == nullptr
        ? nullptr
        : sizes->heapSizes()->as<uint64_t>();
    arena.allocatePayloadRowBatch(
        std::span<const uint64_t>(rawHeapSizes, input.size()),
        sizes->heapSizes(),
        batch);
  } else {
    arena.allocateFixedPayloadRowBatch(input.size(), batch);
  }
  if (input.size() == 0) {
    return;
  }

  auto** rows = batch.rows()->asMutable<char*>();
  const auto clearPayloadSlots = shouldClearPayloadSlots(input);
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* fixed = rows[row];
    if (clearPayloadSlots) {
      std::memset(fixed, 0, layout->rowWidth());
    }
    std::memset(fixed, 0xff, layout->nullBytes());
  }

  variableColumns.clear();
  uint32_t complexOrdinal = 0;
  for (uint32_t column = 0; column < layout->columns().size(); ++column) {
    const auto& metadata = layout->columns()[column];
    if (!metadata.variable) {
      writeFixedColumn(
          metadata,
          *input.childAt(column),
          decoded[column].has_value() ? &*decoded[column] : nullptr,
          rows,
          input.size());
    } else {
      auto* decodedVector =
          decoded[column].has_value() ? &*decoded[column] : nullptr;
      const auto* flatString = decodedVector == nullptr
          ? input.childAt(column)->asUnchecked<FlatVector<StringView>>()
          : nullptr;
      const auto kind = metadata.complex ? VariableWriterKind::kComplex
          : decodedVector == nullptr     ? VariableWriterKind::kFlatString
          : decodedVector->isConstantMapping()
          ? VariableWriterKind::kConstantString
          : VariableWriterKind::kDecodedString;
      const bool constantStringNull =
          kind == VariableWriterKind::kConstantString &&
          decodedVector->isNullAt(0);
      const auto constantStringValue =
          kind == VariableWriterKind::kConstantString && !constantStringNull
          ? decodedVector->valueAt<StringView>(0)
          : StringView();
      variableColumns.push_back(VariableColumnWriter{
          &metadata,
          decodedVector,
          flatString == nullptr ? nullptr : flatString->rawValues(),
          flatString == nullptr ? nullptr : flatString->rawNulls(),
          kind == VariableWriterKind::kDecodedString
              ? decodedVector->data<StringView>()
              : nullptr,
          kind == VariableWriterKind::kDecodedString ? decodedVector->indices()
                                                     : nullptr,
          kind == VariableWriterKind::kDecodedString ? decodedVector->nulls()
                                                     : nullptr,
          metadata.complex ? sizes->complexSizes(complexOrdinal++) : nullptr,
          kind,
          constantStringValue,
          constantStringNull});
    }
  }

  if (!hasVariableFields) {
    return;
  }

  BoundedPayloadStreamArena streamArena;
  ByteOutputStream stream(&streamArena, false, false);
  for (vector_size_t row = 0; row < input.size(); ++row) {
    auto* payloadRow = rows[row];
    auto* heapCursor = batch.heapAt(row);
    uint64_t heapRemaining = rawHeapSizes[row];
    for (const auto& writer : variableColumns) {
      writeVariableValue(
          writer, row, payloadRow, heapCursor, heapRemaining, stream);
    }
  }
}

void prepareDecodedColumns(
    const RowVector& input,
    std::vector<std::optional<DecodedVector>>& decoded) {
  decoded.resize(input.childrenSize());
  for (uint32_t column = 0; column < input.childrenSize(); ++column) {
    if (input.childAt(column)->encoding() != VectorEncoding::Simple::FLAT) {
      if (decoded[column].has_value()) {
        decoded[column]->decode(*input.childAt(column));
      } else {
        decoded[column].emplace(*input.childAt(column));
      }
    } else {
      decoded[column].reset();
    }
  }
}

} // namespace

PayloadRowWriter::PayloadRowWriter() : impl_(std::make_unique<Impl>()) {}

PayloadRowWriter::~PayloadRowWriter() = default;

void PayloadRowWriter::clear() {
  impl_->complexSizes.reset();
  std::vector<std::optional<DecodedVector>>{}.swap(impl_->decoded);
  folly::small_vector<VariableColumnWriter, 32>{}.swap(impl_->variableColumns);
}

void PayloadRowWriter::append(
    const RowVector& input,
    RadixSortRunStorage& arena,
    PayloadRowBatch& batch) {
  const auto& layout = arena.payloadLayout();
  prepareDecodedColumns(input, impl_->decoded);
  if (!layout->hasVariableFields()) {
    return appendRows(
        input, arena, nullptr, impl_->decoded, impl_->variableColumns, batch);
  }
  payload_size::RowSizes sizes;
  auto reusableHeapSizes = std::move(batch.heapSizes_);
  payload_size::measurePayloadRows(
      input,
      *layout,
      arena.pool(),
      impl_->decoded,
      std::move(reusableHeapSizes),
      impl_->complexSizes,
      sizes);
  appendRows(
      input, arena, &sizes, impl_->decoded, impl_->variableColumns, batch);
}

} // namespace bytedance::bolt::exec::radixsort
