#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <cstring>
#include <limits>

namespace bytedance::bolt::exec {
namespace {

void clearBit(char* bits, uint32_t idx) {
  auto bitsAs8Bit = reinterpret_cast<uint8_t*>(bits);
  bitsAs8Bit[idx / 8] &= ~(1 << (idx % 8));
}

} // namespace

RowId BmRowContainer::newRow() {
  auto& block = ensureWritableRowBlock();
  const auto rowOffset = bits::roundUp(block.usedBytes, layout_.alignment());
  BOLT_CHECK_LE(rowOffset + layout_.fixedRowSize(), rowBlockSize_);
  auto* row = blocks_->activeData(activeRowBlockId_) + rowOffset;
  initializeRow(row);
  block.usedBytes = rowOffset + layout_.fixedRowSize();
  ++block.liveRows;
  ++numRows_;
  return RowId{activeRowBlockId_, static_cast<uint32_t>(rowOffset)};
}

void BmRowContainer::store(
    const DecodedVector& decoded,
    vector_size_t index,
    RowId row,
    int32_t column) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, layout_.numColumns());
  auto* rowPtr = mutableRow(row);
  const auto rowColumn = layout_.columnAt(column);
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      storeWithNulls,
      layout_.typeKindAt(column),
      decoded,
      index,
      rowPtr,
      rowColumn.offset(),
      rowColumn.nullByte(),
      rowColumn.nullMask());
}

std::vector<RowId> BmRowContainer::store(const RowVectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  BOLT_CHECK_EQ(input->childrenSize(), layout_.columnTypes().size());

  std::vector<DecodedVector> decoded;
  decoded.reserve(layout_.columnTypes().size());
  for (auto i = 0; i < layout_.columnTypes().size(); ++i) {
    BOLT_CHECK(input->childAt(i)->type()->equivalent(*layout_.typeAt(i)));
    decoded.emplace_back(*input->childAt(i));
  }

  std::vector<RowId> rows;
  rows.reserve(input->size());
  for (auto rowIndex = 0; rowIndex < input->size(); ++rowIndex) {
    auto row = newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      store(decoded[column], rowIndex, row, column);
    }
    rows.push_back(row);
  }
  return rows;
}

char* BmRowContainer::initializeRow(char* row) {
  std::memset(row, 0, layout_.fixedRowSize());
  if (!layout_.initialNulls().empty()) {
    std::memcpy(
        row + layout_.firstNullByteOffset(),
        layout_.initialNulls().data(),
        layout_.initialNulls().size());
  }
  if (layout_.hasVariableWidth()) {
    *reinterpret_cast<uint32_t*>(row + layout_.rowSizeOffset()) = 0;
  }
  clearBit(row, layout_.freeFlagOffset());
  return row;
}

VarData BmRowContainer::appendVariableWidth(StringView value) {
  BOLT_CHECK_LE(value.size(), heapBlockSize_);
  if (value.size() == 0) {
    return VarData{};
  }
  if (activeHeapBlockId_ == std::numeric_limits<uint32_t>::max() ||
      blocks_->block(activeHeapBlockId_).usedBytes + value.size() >
          heapBlockSize_) {
    activeHeapBlockId_ = blocks_->allocateReservedBlock(heapBlockSize_);
    heapBlockIds_.push_back(activeHeapBlockId_);
  }

  auto& block = blocks_->block(activeHeapBlockId_);
  const auto offset = block.usedBytes;
  std::memcpy(
      blocks_->activeData(activeHeapBlockId_) + offset,
      value.data(),
      value.size());
  block.usedBytes += value.size();
  return VarData{
      activeHeapBlockId_,
      static_cast<uint32_t>(offset),
      static_cast<uint32_t>(value.size())};
}

template <TypeKind Kind>
void BmRowContainer::storeWithNulls(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    BOLT_NYI(
        "BmRowContainer store does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    if (decoded.isNullAt(index)) {
      row[nullByte] |= nullMask;
      if constexpr (std::is_arithmetic_v<T>) {
        *reinterpret_cast<T*>(row + offset) = std::numeric_limits<T>::max();
      } else if constexpr (std::is_same_v<T, StringView>) {
        *reinterpret_cast<VarData*>(row + offset) = VarData{};
      } else {
        *reinterpret_cast<T*>(row + offset) = T();
      }
      return;
    }

    row[nullByte] &= ~nullMask;
    if constexpr (std::is_same_v<T, StringView>) {
      *reinterpret_cast<VarData*>(row + offset) =
          appendVariableWidth(decoded.valueAt<StringView>(index));
    } else {
      *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(index);
    }
  }
}

} // namespace bytedance::bolt::exec
