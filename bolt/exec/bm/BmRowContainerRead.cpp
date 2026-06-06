#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <limits>

namespace bytedance::bolt::exec {
namespace {

bool isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
  return (row[nullByte] & nullMask) != 0;
}

} // namespace

void BmRowContainer::extractColumn(
    const RowId* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result,
    bool exactSize) {
  extractColumn(
      folly::Range<const RowId*>(rows, numRows),
      column,
      0,
      result,
      exactSize);
}

void BmRowContainer::extractColumn(
    folly::Range<const RowId*> rows,
    int32_t column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, rowColumns_.size());
  BOLT_CHECK_LE(resultOffset + rows.size(), result->size());

  switch (typeKinds_[column]) {
    // Complex types still use the generic path, which currently reports NYI.
    // The fast path below is only valid for flat fixed-width and string types.
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::VARIANT:
      break;
    default:
      extractColumnFast(
          typeKinds_[column],
          rows,
          rowColumns_[column],
          resultOffset,
          result,
          exactSize);
      return;
  }

  std::vector<const char*> rowPtrs;
  rowPtrs.reserve(rows.size());
  for (auto row : rows) {
    rowPtrs.push_back(pinRow(row));
  }
  extractDispatch(
      typeKinds_[column],
      rowPtrs.data(),
      rows.size(),
      rowColumns_[column],
      result,
      resultOffset,
      exactSize);
}

void BmRowContainer::extractNulls(
    const RowId* rows,
    int32_t numRows,
    int32_t column,
    const BufferPtr& result) {
  extractNulls(folly::Range<const RowId*>(rows, numRows), column, result);
}

void BmRowContainer::extractNulls(
    folly::Range<const RowId*> rows,
    int32_t column,
    const BufferPtr& result) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, rowColumns_.size());
  BOLT_CHECK_GE(result->size(), bits::nbytes(rows.size()));

  auto* rawResult = result->asMutable<uint64_t>();
  bits::fillBits(rawResult, 0, rows.size(), false);
  const auto rowColumn = rowColumns_[column];

  for (auto i = 0; i < rows.size(); ++i) {
    const auto* row = pinRow(rows[i]);
    if (isNullAt(row, rowColumn.nullByte(), rowColumn.nullMask())) {
      bits::setBit(rawResult, i, true);
    }
  }
}

StringView BmRowContainer::stringView(const char* row, BmRowColumn column) {
  const auto ref = *reinterpret_cast<const VarData*>(row + column.offset());
  if (ref.size == 0) {
    return StringView("", 0);
  }
  auto& block = blocks_->block(ref.blockId);
  BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
  return StringView(
      pinnedBlockDataAfterPressure(
          ref.blockId, "BmRowContainer cannot pin a heap block") +
          ref.offset,
      ref.size);
}

void BmRowContainer::extractDispatch(
    TypeKind kind,
    const char* const* rows,
    int32_t numRows,
    BmRowColumn column,
    const VectorPtr& result,
    vector_size_t resultOffset,
    bool exactSize) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      kind,
      rows,
      numRows,
      column,
      result,
      resultOffset,
      exactSize);
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    const char* const* rows,
    int32_t numRows,
    BmRowColumn column,
    const VectorPtr& result,
    vector_size_t resultOffset,
    bool exactSize) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    BOLT_NYI(
        "BmRowContainer extract does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* flatResult = result->asFlatVector<T>();
    BOLT_CHECK_NOT_NULL(flatResult);
    if constexpr (std::is_same_v<T, StringView>) {
      for (int32_t i = 0; i < numRows; ++i) {
        const auto* row = rows[i];
        const auto isNull = row == nullptr ||
            isNullAt(row, column.nullByte(), column.nullMask());
        flatResult->setNull(resultOffset + i, isNull);
        if (isNull) {
          continue;
        }
        flatResult->setStringViewValue(
            resultOffset + i,
            stringView(row, column),
            exactSize);
      }
    } else {
      for (int32_t i = 0; i < numRows; ++i) {
        const auto* row = rows[i];
        const auto isNull = row == nullptr ||
            isNullAt(row, column.nullByte(), column.nullMask());
        flatResult->setNull(resultOffset + i, isNull);
        if (isNull) {
          continue;
        }
        flatResult->set(
            resultOffset + i,
            *reinterpret_cast<const T*>(row + column.offset()));
      }
    }
  }
}

} // namespace bytedance::bolt::exec
