/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/exec/bm/BmRowCopier.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"

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

char* BmRowCopier::copyRowToSegment(SegmentData& segment, const char* source) {
  auto* target = segments().newRowInSegment(segment);
  BOLT_DCHECK_NOT_NULL(segment.writeCursor.chunk);
  auto& chunk = *segment.writeCursor.chunk;
  simd::memcpy(target, source, static_cast<int32_t>(layout().rowSize()));

  for (int32_t column = 0; column < types().size(); ++column) {
    const auto kind = types()[column]->kind();
    if ((kind != TypeKind::VARCHAR && kind != TypeKind::VARBINARY) ||
        layout().isNull(target, column)) {
      continue;
    }
    auto* value =
        reinterpret_cast<StringView*>(layout().valueAddress(target, column));
    if (value->isInline()) {
      continue;
    }
    auto& heap = segments().ensureHeapBlockInChunk(chunk, value->size());
    auto* stringTarget = heap.ptr + heap.used;
    simd::memcpy(stringTarget, value->data(), value->size());
    heap.used += value->size();
    *value = StringView(stringTarget, value->size());
    segments().recordHeapForChunk(chunk, heap, target);
  }
  return target;
}

} // namespace bytedance::bolt::exec::bm
