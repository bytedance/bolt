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

void BmRowContainer::extractColumnFast(
    TypeKind kind,
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnFastTyped,
      kind,
      rows,
      column,
      resultOffset,
      result,
      exactSize);
}

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
      constexpr auto kInvalidBlockId = std::numeric_limits<uint32_t>::max();
      uint32_t rowBlockId = kInvalidBlockId;
      const char* rowBlockData = nullptr;
      uint32_t heapBlockId = kInvalidBlockId;
      const char* heapBlockData = nullptr;

      for (auto i = 0; i < rows.size(); ++i) {
        const auto row = rows[i];
        if (row.blockId != rowBlockId) {
          auto& block = blocks_->block(row.blockId);
          BOLT_CHECK_LE(
              row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
          rowBlockData = pinnedBlockDataAfterPressure(
              row.blockId,
              "BmRowContainer cannot pin a row block for fast extract");
          rowBlockId = row.blockId;
        } else {
          auto& block = blocks_->block(row.blockId);
          BOLT_CHECK_LE(
              row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
        }

        const auto* rowPtr = rowBlockData + row.rowOffset;
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

        if (ref.blockId != heapBlockId) {
          auto& block = blocks_->block(ref.blockId);
          BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
          heapBlockData = pinnedBlockDataAfterPressure(
              ref.blockId,
              "BmRowContainer cannot pin a heap block for fast extract");
          heapBlockId = ref.blockId;
        } else {
          auto& block = blocks_->block(ref.blockId);
          BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
        }

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
        if (row.blockId != rowBlockId) {
          auto& block = blocks_->block(row.blockId);
          BOLT_CHECK_LE(
              row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
          rowBlockData = pinnedBlockDataAfterPressure(
              row.blockId,
              "BmRowContainer cannot pin a row block for fast extract");
          rowBlockId = row.blockId;
        } else {
          auto& block = blocks_->block(row.blockId);
          BOLT_CHECK_LE(
              row.rowOffset + layout_.fixedRowSize(), block.usedBytes);
        }

        const auto* rowPtr = rowBlockData + row.rowOffset;
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

} // namespace bytedance::bolt::exec
