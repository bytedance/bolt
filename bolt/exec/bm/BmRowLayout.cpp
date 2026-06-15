#include "bolt/exec/bm/BmRowLayout.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"

#include <algorithm>

namespace bytedance::bolt::exec::bm {
namespace {

uint32_t alignUp(uint32_t value, uint32_t alignment) {
  return (value + alignment - 1) & ~(alignment - 1);
}

template <TypeKind Kind>
uint32_t scalarTypeWidth(const TypePtr& type) {
  if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
    return sizeof(StringView);
  } else if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("BmRowContainer does not support type {}", type->toString());
  } else {
    return sizeof(typename TypeTraits<Kind>::NativeType);
  }
}

uint32_t typeWidth(const TypePtr& type) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      scalarTypeWidth, type->kind(), type);
}

void appendInitializationRange(
    std::vector<RowInitializationRange>& ranges,
    uint32_t offset,
    uint32_t size) {
  if (size == 0) {
    return;
  }
  if (!ranges.empty()) {
    auto& last = ranges.back();
    const auto lastEnd = last.offset + last.size;
    if (lastEnd >= offset) {
      last.size = std::max(lastEnd, offset + size) - last.offset;
      return;
    }
  }
  ranges.push_back({offset, size});
}

} // namespace

BmRowLayout::BmRowLayout(
    const std::vector<TypePtr>& types,
    const std::vector<bool>& nullable,
    uint32_t rowBlockSize) {
  BOLT_CHECK_EQ(types.size(), nullable.size());
  uint32_t nullBits = 0;
  for (auto isNullable : nullable) {
    if (isNullable) {
      ++nullBits;
    }
  }
  nullBytes_ = bits::nbytes(nullBits);
  fixedRowSize_ = nullBytes_;
  appendInitializationRange(initializationRanges_, 0, nullBytes_);
  columns_.reserve(types.size());
  stringColumns_.reserve(types.size());
  storePlans_.reserve(types.size());
  uint32_t nullOffset = 0;
  for (auto i = 0; i < types.size(); ++i) {
    const auto& type = types[i];
    const auto width = typeWidth(type);
    const auto kind = type->kind();
    const auto alignment = std::min<uint32_t>(width, 8);
    fixedRowSize_ = alignUp(fixedRowSize_, std::max<uint32_t>(alignment, 1));
    ColumnLayout column{type, fixedRowSize_, width, nullable[i], 0, 0};
    if (nullable[i]) {
      column.nullByte = nullOffset / 8;
      column.nullMask = static_cast<uint8_t>(1u << (nullOffset & 7));
      ++nullOffset;
    }
    columns_.push_back(std::move(column));
    const auto& stored = columns_.back();
    storePlans_.push_back(
        {stored.type,
         kind,
         stored.offset,
         stored.width,
         stored.nullable,
         stored.nullByte,
         stored.nullMask,
         kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY});
    if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
      stringColumns_.push_back(
          {stored.offset, stored.nullable, stored.nullByte, stored.nullMask});
      appendInitializationRange(
          initializationRanges_, stored.offset, sizeof(StringView));
    }
    fixedRowSize_ += width;
  }
  BOLT_CHECK_LE(fixedRowSize_, rowBlockSize);
}

} // namespace bytedance::bolt::exec::bm
