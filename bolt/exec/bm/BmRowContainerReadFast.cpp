#include "bolt/exec/bm/BmRowContainer.h"

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

template <TypeKind Kind>
void BmRowContainer::extractColumnFastTyped(
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW || Kind == TypeKind::VARIANT) {
    BOLT_NYI(
        "BmRowContainer fast extract does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* flatResult = result->asFlatVector<T>();
    BOLT_CHECK_NOT_NULL(flatResult);
    if constexpr (std::is_same_v<T, StringView>) {
      for (auto i = 0; i < rows.size(); ++i) {
        const auto row = rows[i];
        auto& rowBlock = blocks_->block(row.rowBlockId());
        BOLT_CHECK_LE(
            row.rowOffset() + layout_.fixedRowSize(), rowBlock.usedBytes);
        const auto* rowBlockData = pinBlockForRead(
            row.rowBlockId(),
            "BmRowContainer cannot pin a row block for fast extract");

        const auto* rowPtr = rowBlockData + row.rowOffset();
        const auto output = resultOffset + i;
        const auto isNull =
            isNullAt(rowPtr, column.nullByte(), column.nullMask());
        flatResult->setNull(output, isNull);
        if (isNull) {
          continue;
        }

        const auto ref =
            *reinterpret_cast<const VarData*>(rowPtr + column.offset());
        if (ref.size == 0) {
          flatResult->setStringViewValue(output, StringView("", 0), exactSize);
          continue;
        }

        auto& heapBlock = blocks_->block(ref.blockId);
        BOLT_CHECK_LE(ref.offset + ref.size, heapBlock.usedBytes);
        const auto* heapBlockData = pinBlockForRead(
            ref.blockId,
            "BmRowContainer cannot pin a heap block for fast extract");

        flatResult->setStringViewValue(
            output,
            StringView(heapBlockData + ref.offset, ref.size),
            exactSize);
      }
    } else {
      uint32_t rowBlockId = std::numeric_limits<uint32_t>::max();
      const char* rowBlockData = nullptr;
      for (auto i = 0; i < rows.size(); ++i) {
        const auto row = rows[i];
        if (row.rowBlockId() != rowBlockId) {
          auto& block = blocks_->block(row.rowBlockId());
          BOLT_CHECK_LE(
              row.rowOffset() + layout_.fixedRowSize(), block.usedBytes);
          rowBlockData = pinBlockForRead(
              row.rowBlockId(),
              "BmRowContainer cannot pin a row block for fast extract");
          rowBlockId = row.rowBlockId();
        } else {
          auto& block = blocks_->block(row.rowBlockId());
          BOLT_CHECK_LE(
              row.rowOffset() + layout_.fixedRowSize(), block.usedBytes);
        }

        const auto* rowPtr = rowBlockData + row.rowOffset();
        const auto output = resultOffset + i;
        const auto isNull =
            isNullAt(rowPtr, column.nullByte(), column.nullMask());
        flatResult->setNull(output, isNull);
        if (!isNull) {
          flatResult->set(
              output,
              *reinterpret_cast<const T*>(rowPtr + column.offset()));
        }
      }
    }
  }
}

void BmRowContainer::extractColumn(
    folly::Range<const RowId*> rows,
    int32_t column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, layout_.numColumns());
  BOLT_CHECK_LE(resultOffset + rows.size(), result->size());

  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnFastTyped,
      layout_.typeKindAt(column),
      rows,
      layout_.columnAt(column),
      resultOffset,
      result,
      exactSize);
}

} // namespace bytedance::bolt::exec
