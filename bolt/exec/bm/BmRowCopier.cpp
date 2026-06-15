#include "bolt/exec/bm/BmRowCopier.h"

#include "bolt/common/base/Exceptions.h"

#include <cstring>

namespace bytedance::bolt::exec::bm {

BmRowCopier::BmRowCopier(
    const std::vector<TypePtr>* types,
    const BmRowLayout* layout,
    BmSegmentCollection* segments)
    : types_(types), layout_(layout), segments_(segments) {
  BOLT_CHECK_NOT_NULL(types_);
  BOLT_CHECK_NOT_NULL(layout_);
  BOLT_CHECK_NOT_NULL(segments_);
}

char* BmRowCopier::copyRowToSegment(
    SegmentData& segment,
    const char* source) {
  auto* target = segments().newRowInSegment(segment);
  BOLT_DCHECK_NOT_NULL(segment.writeCursor.chunk);
  auto& chunk = *segment.writeCursor.chunk;
  std::memcpy(target, source, layout().rowSize());

  for (int32_t column = 0; column < types().size(); ++column) {
    const auto kind = types()[column]->kind();
    if ((kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) ||
        layout().isNull(target, column)) {
      continue;
    }
    auto* value = reinterpret_cast<StringView*>(
        layout().valueAddress(target, column));
    if (value->isInline()) {
      continue;
    }
    auto& heap = segments().ensureHeapBlockInChunk(chunk, value->size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value->data(), value->size());
    heap.used += value->size();
    *value = StringView(stringTarget, value->size());
    segments().recordHeapForChunk(chunk, heap, target);
  }
  return target;
}

} // namespace bytedance::bolt::exec::bm
