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
      return RadixSortKeyLayout(kind, 32, 16, true, false, 16, 24, {});
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return RadixSortKeyLayout(kind, 16, 8, false, true, {}, {}, 8);
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return RadixSortKeyLayout(kind, 24, 16, false, true, {}, {}, 16);
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return RadixSortKeyLayout(kind, 32, 24, false, true, {}, {}, 24);
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return RadixSortKeyLayout(kind, 32, 8, true, true, 8, 16, 24);
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable56:
      return RadixSortKeyLayout(kind, 56, 32, true, true, 32, 40, 48);
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable64:
      return RadixSortKeyLayout(kind, 64, 40, true, true, 40, 48, 56);
  }
  BOLT_FAIL("Unknown radix sort key layout");
}

RadixSortKeyLayout RadixSortKeyLayout::select(
    std::optional<uint64_t> maximumEncodedSize,
    bool hasPayload,
    uint32_t keyColumnCount) {
  RadixSortKeyLayoutKind kind;
  if (!maximumEncodedSize.has_value()) {
    if (hasPayload) {
      kind = keyColumnCount > 3
          ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable64
          : keyColumnCount > 1
          ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable56
          : RadixSortKeyLayoutKind::kKeyWithPayloadVariable32;
    } else {
      kind = RadixSortKeyLayoutKind::kKeyOnlyVariable32;
    }
    return fromKind(kind);
  }
  auto physicalWidth = checkedAdd<uint64_t>(
      *maximumEncodedSize, hasPayload ? sizeof(uint64_t) : 0);
  if (*physicalWidth <= 8) {
    BOLT_CHECK(!hasPayload, "Payload radix sort key cannot fit in 8 bytes");
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
    if (hasPayload) {
      kind = keyColumnCount > 3
          ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable64
          : keyColumnCount > 1
          ? RadixSortKeyLayoutKind::kKeyWithPayloadVariable56
          : RadixSortKeyLayoutKind::kKeyWithPayloadVariable32;
    } else {
      kind = RadixSortKeyLayoutKind::kKeyOnlyVariable32;
    }
  }
  auto layout = fromKind(kind);
  layout.radixWidth_ = static_cast<uint32_t>(
      std::min<uint64_t>(*maximumEncodedSize, sizeof(uint64_t)));
  return layout;
}

void RadixSortKey::construct(
    std::string_view encodedKey,
    char* overflowData,
    char* payload) const {
  std::memset(mutableData_, 0, layout_->width());
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

  if (layout_->isVariable()) {
    storeUnaligned<uint64_t>(
        mutableData_ + *layout_->sizeOffset(), encodedKey.size());
    if (encodedKey.size() > layout_->inlineCapacity()) {
      std::memcpy(overflowData, encodedKey.data(), encodedKey.size());
      storeUnaligned<char*>(
          mutableData_ + *layout_->dataOffset(), overflowData);
    }
  }
  if (layout_->hasPayload()) {
    storeUnaligned<char*>(mutableData_ + *layout_->payloadOffset(), payload);
  }
}

void RadixSortKey::deconstruct(
    RadixSortInlineKeyBuffer& inlineBuffer,
    EncodedKeyView& encodedKey) const {
  if (layout_->isVariable() && storedSize() > layout_->inlineCapacity()) {
    const auto* fullKey = fullKeyData();
    encodedKey = {std::string_view(fullKey, storedSize()), false};
    return;
  }

  inlineBuffer.fill(0);
  for (uint32_t word = 0; word < layout_->inlineWordCount(); ++word) {
    storeUnaligned(
        inlineBuffer.data() + word * sizeof(uint64_t),
        toEncodedWord(inlineWord(word)));
  }
  encodedKey = {
      std::string_view(inlineBuffer.data(), layout_->inlineCapacity()), true};
}

int32_t RadixSortKey::compare(const RadixSortKey& other) const {
  for (uint32_t word = 0; word < layout_->inlineWordCount(); ++word) {
    const auto left = inlineWord(word);
    const auto right = other.inlineWord(word);
    if (left != right) {
      return (left > right) - (left < right);
    }
  }
  if (!layout_->isVariable()) {
    return 0;
  }

  const auto leftSize = storedSize();
  const auto rightSize = other.storedSize();
  if (leftSize <= layout_->inlineCapacity() ||
      rightSize <= layout_->inlineCapacity()) {
    return (leftSize > rightSize) - (leftSize < rightSize);
  }

  const auto commonSize = std::min(leftSize, rightSize);
  const auto* leftData = fullKeyData();
  const auto* rightData = other.fullKeyData();
  const auto result = std::memcmp(
      leftData + layout_->inlineCapacity(),
      rightData + layout_->inlineCapacity(),
      commonSize - layout_->inlineCapacity());
  if (result != 0) {
    return (result > 0) - (result < 0);
  }
  return (leftSize > rightSize) - (leftSize < rightSize);
}

uint64_t RadixSortKey::heapSize() const {
  if (!layout_->isVariable()) {
    return 0;
  }
  const auto size = storedSize();
  return size > layout_->inlineCapacity() ? size : 0;
}

char* RadixSortKey::fullKeyData() const {
  if (!layout_->isVariable() || storedSize() <= layout_->inlineCapacity()) {
    return nullptr;
  }
  return loadUnaligned<char*>(data_ + *layout_->dataOffset());
}

char* RadixSortKey::payload() const {
  if (!layout_->hasPayload()) {
    return nullptr;
  }
  return loadUnaligned<char*>(data_ + *layout_->payloadOffset());
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
