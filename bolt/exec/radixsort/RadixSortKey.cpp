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
#include <cstring>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

uint64_t fromEncodedWord(const char* data) {
  auto word = loadUnaligned<uint64_t>(data);
  if constexpr (std::endian::native == std::endian::little) {
    word = byteSwap(word);
  }
  return word;
}

uint64_t toEncodedWord(uint64_t word) {
  if constexpr (std::endian::native == std::endian::little) {
    word = byteSwap(word);
  }
  return word;
}

} // namespace

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

uint64_t RadixSortKeyLayout::heapSize(uint64_t encodedSize) const {
  if (!isVariable()) {
    return 0;
  }
  BOLT_DCHECK_LE(heapKeyOffset_, inlineCapacity_);
  BOLT_DCHECK_LE(heapKeyOffset_, encodedSize);
  return encodedSize - heapKeyOffset_;
}

void RadixSortKey::construct(
    std::string_view encodedKey,
    char* overflowData,
    char* payload) const {
  if (layout_->isVariable()) {
    std::memset(mutableData_, 0, layout_->inlineCapacity());
    std::memcpy(
        mutableData_,
        encodedKey.data(),
        std::min<size_t>(encodedKey.size(), layout_->inlineCapacity()));
  } else {
    RadixSortInlineKeyBuffer inlineBytes{};
    std::memcpy(
        inlineBytes.data(),
        encodedKey.data(),
        std::min<size_t>(encodedKey.size(), layout_->inlineCapacity()));
    for (uint32_t word = 0; word < layout_->inlineWordCount(); ++word) {
      storeUnaligned(
          mutableData_ + word * sizeof(uint64_t),
          fromEncodedWord(inlineBytes.data() + word * sizeof(uint64_t)));
    }
    std::memcpy(
        mutableData_ + layout_->inlineWordBytes(),
        inlineBytes.data() + layout_->inlineWordBytes(),
        layout_->inlineTailBytes());
  }

  if (layout_->isVariable()) {
    BOLT_DCHECK_GT(
        encodedKey.size(),
        layout_->heapKeyOffset(),
        "Variable radix sort key must contain a suffix column");
    storeUnaligned<uint64_t>(
        mutableData_ + *layout_->sizeOffset(), encodedKey.size());
    const auto heapSize = layout_->heapSize(encodedKey.size());
    BOLT_DCHECK_GT(heapSize, 0);
    BOLT_DCHECK_NOT_NULL(overflowData);
    std::memcpy(
        overflowData, encodedKey.data() + layout_->heapKeyOffset(), heapSize);
    storeCompactPointer(mutableData_ + *layout_->dataOffset(), overflowData);
  }
  if (layout_->hasPayload()) {
    storeCompactPointer(mutableData_ + *layout_->payloadOffset(), payload);
  }
}

void RadixSortKey::deconstruct(
    RadixSortInlineKeyBuffer& inlineBuffer,
    EncodedKeyView& encodedKey) const {
  BOLT_DCHECK(!layout_->isVariable());
  inlineBuffer.fill(0);
  for (uint32_t word = 0; word < layout_->inlineWordCount(); ++word) {
    storeUnaligned(
        inlineBuffer.data() + word * sizeof(uint64_t),
        toEncodedWord(inlineWord(word)));
  }
  std::memcpy(
      inlineBuffer.data() + layout_->inlineWordBytes(),
      data_ + layout_->inlineWordBytes(),
      layout_->inlineTailBytes());
  encodedKey = {
      std::string_view(inlineBuffer.data(), layout_->inlineCapacity())};
}

int32_t RadixSortKey::compare(const RadixSortKey& other) const {
  if (!layout_->isVariable()) {
    for (uint32_t word = 0; word < layout_->inlineWordCount(); ++word) {
      const auto left = inlineWord(word);
      const auto right = other.inlineWord(word);
      if (left != right) {
        return (left > right) - (left < right);
      }
    }
    const auto tailResult = std::memcmp(
        data_ + layout_->inlineWordBytes(),
        other.data_ + layout_->inlineWordBytes(),
        layout_->inlineTailBytes());
    if (tailResult != 0) {
      return (tailResult > 0) - (tailResult < 0);
    }
    return 0;
  }

  const auto leftSize = storedSize();
  const auto rightSize = other.storedSize();
  const auto prefixSize =
      std::min<uint64_t>({leftSize, rightSize, layout_->inlineCapacity()});
  const auto result = std::memcmp(data_, other.data_, prefixSize);
  if (result != 0) {
    return (result > 0) - (result < 0);
  }
  if (prefixSize < layout_->inlineCapacity()) {
    return (leftSize > rightSize) - (leftSize < rightSize);
  }

  const auto heapOffset = layout_->heapKeyOffset();
  BOLT_DCHECK_LE(heapOffset, layout_->inlineCapacity());
  const auto* leftData = loadCompactPointer(data_ + *layout_->dataOffset());
  const auto* rightData =
      loadCompactPointer(other.data_ + *layout_->dataOffset());
  const auto suffixResult = std::memcmp(
      leftData + layout_->inlineCapacity() - heapOffset,
      rightData + layout_->inlineCapacity() - heapOffset,
      std::min(leftSize, rightSize) - layout_->inlineCapacity());
  if (suffixResult != 0) {
    return (suffixResult > 0) - (suffixResult < 0);
  }
  return (leftSize > rightSize) - (leftSize < rightSize);
}

uint64_t RadixSortKey::heapSize() const {
  if (!layout_->isVariable()) {
    return 0;
  }
  return layout_->heapSize(storedSize());
}

std::string_view RadixSortKey::heapKey() const {
  const auto size = heapSize();
  return std::string_view(
      size == 0 ? nullptr : loadCompactPointer(data_ + *layout_->dataOffset()),
      size);
}

char* RadixSortKey::heapKeyData() const {
  BOLT_DCHECK(layout_->isVariable());
  return loadCompactPointer(data_ + *layout_->dataOffset());
}

char* RadixSortKey::payload() const {
  if (!layout_->hasPayload()) {
    return nullptr;
  }
  return loadCompactPointer(data_ + *layout_->payloadOffset());
}

uint64_t RadixSortKey::inlineWord(uint32_t index) const {
  return loadUnaligned<uint64_t>(data_ + index * sizeof(uint64_t));
}

uint64_t RadixSortKey::storedSize() const {
  if (!layout_->isVariable()) {
    return 0;
  }
  return loadUnaligned<uint64_t>(data_ + *layout_->sizeOffset());
}

} // namespace bytedance::bolt::exec::radixsort
