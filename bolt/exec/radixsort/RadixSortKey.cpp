/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/exec/radixsort/RadixSortKey.h"

#include <algorithm>
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {
void checkCompactPointerRange(const void* data, uint64_t size) {
  if (size == 0) {
    return;
  }
  BOLT_CHECK_NOT_NULL(data, "Radix sort allocation must not be null");
  const auto begin = reinterpret_cast<uintptr_t>(data);
  BOLT_CHECK_LE(
      begin,
      kCompactPointerMask,
      "Radix sort allocation starts outside the 48-bit address range");
  BOLT_CHECK_LE(
      size - 1,
      kCompactPointerMask - begin,
      "Radix sort allocation ends outside the 48-bit address range");
}

RadixSortKeyLayout RadixSortKeyLayout::fromKind(RadixSortKeyLayoutKind kind) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kInvalid:
      BOLT_FAIL("Invalid radix sort key layout");
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      return RadixSortKeyLayout(kind, 8, 8, false, false, {}, {}, {});
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      return RadixSortKeyLayout(kind, 16, 16, false, false, {}, {}, {});
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      return RadixSortKeyLayout(kind, 24, 24, false, false, {}, {}, {});
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      return RadixSortKeyLayout(kind, 32, 32, false, false, {}, {}, {});
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return RadixSortKeyLayout(kind, 32, 18, true, false, 18, 26, {});
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return RadixSortKeyLayout(kind, 16, 10, false, true, {}, {}, 10);
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return RadixSortKeyLayout(kind, 24, 18, false, true, {}, {}, 18);
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return RadixSortKeyLayout(kind, 32, 26, false, true, {}, {}, 26);
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return RadixSortKeyLayout(kind, 32, 12, true, true, 12, 20, 26);
  }
  BOLT_FAIL("Unknown radix sort key layout");
}

RadixSortKeyLayout RadixSortKeyLayout::select(
    std::optional<uint64_t> maximumEncodedSize,
    bool hasPayload,
    uint32_t heapKeyOffset) {
  RadixSortKeyLayoutKind kind;
  if (!maximumEncodedSize.has_value()) {
    kind = hasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable32
                      : RadixSortKeyLayoutKind::kKeyOnlyVariable32;
  } else {
    if (hasPayload) {
      BOLT_CHECK_GT(
          *maximumEncodedSize, 0, "Payload radix sort key cannot be empty");
    }
    const auto physicalWidth = checkedAdd<uint64_t>(
        *maximumEncodedSize, hasPayload ? kCompactPointerBytes : 0);
    BOLT_CHECK(physicalWidth.has_value(), "Radix sort key width overflows");
    if (!hasPayload && *physicalWidth <= 8) {
      kind = RadixSortKeyLayoutKind::kKeyOnlyFixed8;
    } else if (*physicalWidth <= 16) {
      kind = hasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadFixed16
                        : RadixSortKeyLayoutKind::kKeyOnlyFixed16;
    } else if (*physicalWidth <= 24) {
      kind = hasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadFixed24
                        : RadixSortKeyLayoutKind::kKeyOnlyFixed24;
    } else if (*physicalWidth <= 32) {
      kind = hasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadFixed32
                        : RadixSortKeyLayoutKind::kKeyOnlyFixed32;
    } else {
      kind = hasPayload ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable32
                        : RadixSortKeyLayoutKind::kKeyOnlyVariable32;
    }
  }
  auto layout = fromKind(kind);
  if (!layout.isVariable() && maximumEncodedSize.has_value()) {
    layout.radixWidth_ = static_cast<uint32_t>(
        std::min<uint64_t>(*maximumEncodedSize, sizeof(uint64_t)));
  }
  if (layout.isVariable()) {
    BOLT_CHECK_LE(
        heapKeyOffset,
        layout.inlineCapacity(),
        "Radix sort key heap offset must fit in the inline prefix");
    layout.heapKeyOffset_ = heapKeyOffset;
  } else {
    BOLT_CHECK_EQ(
        heapKeyOffset,
        0,
        "Fixed radix sort key layout cannot have a heap offset");
  }
  return layout;
}

} // namespace bytedance::bolt::exec::radixsort
