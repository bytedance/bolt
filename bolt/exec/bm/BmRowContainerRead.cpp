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
  BOLT_CHECK_LT(column, layout_.numColumns());
  BOLT_CHECK_GE(result->size(), bits::nbytes(rows.size()));

  auto* rawResult = result->asMutable<uint64_t>();
  bits::fillBits(rawResult, 0, rows.size(), false);
  const auto rowColumn = layout_.columnAt(column);

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
      pinBlockForRead(
          ref.blockId, "BmRowContainer cannot pin a heap block") +
          ref.offset,
      ref.size);
}

} // namespace bytedance::bolt::exec
