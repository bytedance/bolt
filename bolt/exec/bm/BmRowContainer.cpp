#include "bolt/exec/bm/BmRowContainer.h"

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
  } else if constexpr (Kind == TypeKind::UNKNOWN) {
    BOLT_NYI("BmRowContainer does not support type {}", type->toString());
  } else {
    static_assert(TypeTraits<Kind>::isFixedWidth);
    return sizeof(typename TypeTraits<Kind>::NativeType);
  }
}

uint32_t typeWidth(const TypePtr& type) {
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      scalarTypeWidth, type->kind(), type);
}

} // namespace

const std::vector<SegmentId> BmRowContainer::kEmptySegments_{};

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> types,
    std::vector<bool> nullable,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize,
    uint32_t chunkRowCount)
    : types_(std::move(types)),
      bufferManager_(std::move(bufferManager)),
      tag_(tag),
      rowBlockSize_(rowBlockSize),
      heapBlockSize_(heapBlockSize),
      chunkRowCount_(chunkRowCount) {
  BOLT_CHECK_NOT_NULL(bufferManager_);
  BOLT_CHECK_EQ(types_.size(), nullable.size());
  uint32_t nullBits = 0;
  for (auto isNullable : nullable) {
    if (isNullable) {
      ++nullBits;
    }
  }
  nullBytes_ = bits::nbytes(nullBits);
  fixedRowSize_ = nullBytes_;
  columns_.reserve(types_.size());
  uint32_t nullOffset = 0;
  for (auto i = 0; i < types_.size(); ++i) {
    const auto& type = types_[i];
    const auto width = typeWidth(type);
    const auto alignment = std::min<uint32_t>(width, 8);
    fixedRowSize_ = alignUp(fixedRowSize_, std::max<uint32_t>(alignment, 1));
    ColumnLayout column{type, fixedRowSize_, width, nullable[i], 0, 0};
    if (nullable[i]) {
      column.nullByte = nullOffset / 8;
      column.nullMask = static_cast<uint8_t>(1u << (nullOffset & 7));
      ++nullOffset;
    }
    columns_.push_back(std::move(column));
    fixedRowSize_ += width;
  }
  BOLT_CHECK_LE(fixedRowSize_, rowBlockSize_);
}

char* BmRowContainer::newRow() {
  return newRow(kDefaultPartition);
}

char* BmRowContainer::newRow(PartitionId partition) {
  return newRowInSegment(activeSegment(partition));
}

} // namespace bytedance::bolt::exec::bm
