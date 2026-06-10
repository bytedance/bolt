#include "bolt/exec/bm/BmRowCopier.h"

#include "bolt/common/base/Exceptions.h"

#include <cstring>

namespace bytedance::bolt::exec::bm {

BmRowCopier::BmRowCopier(
    const std::vector<TypePtr>* types,
    const BmRowLayout* layout,
    BmRowStorage* storage)
    : types_(types), layout_(layout), storage_(storage) {
  BOLT_CHECK_NOT_NULL(types_);
  BOLT_CHECK_NOT_NULL(layout_);
  BOLT_CHECK_NOT_NULL(storage_);
}

char* BmRowCopier::copyRowToSegment(
    SegmentData& segment,
    const char* source) {
  auto* target = storage().newRowInSegment(segment);
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
    auto& heap = storage().ensureHeapBlock(segment, value->size());
    auto* stringTarget = heap.ptr + heap.used;
    std::memcpy(stringTarget, value->data(), value->size());
    heap.used += value->size();
    *value = StringView(stringTarget, value->size());
    storage().recordHeapForCurrentChunk(segment, heap);
  }
  return target;
}

} // namespace bytedance::bolt::exec::bm
