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

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>
#include <type_traits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {

enum class RadixSortKeyLayoutKind : uint8_t {
  kInvalid = 0,
  kKeyOnlyFixed8 = 1,
  kKeyOnlyFixed16 = 2,
  kKeyOnlyFixed24 = 3,
  kKeyOnlyFixed32 = 4,
  kKeyOnlyVariable32 = 5,
  kKeyWithPayloadFixed16 = 6,
  kKeyWithPayloadFixed24 = 7,
  kKeyWithPayloadFixed32 = 8,
  kKeyWithPayloadVariable32 = 9,
  kKeyWithPayloadVariable56 = 10,
  kKeyWithPayloadVariable64 = 11,
};

using RadixSortInlineKeyBuffer = std::array<char, 64>;

class RadixSortKeyLayout {
 public:
  RadixSortKeyLayout() = default;

  static RadixSortKeyLayout fromKind(RadixSortKeyLayoutKind kind);

  static RadixSortKeyLayout select(
      std::optional<uint64_t> maximumEncodedSize,
      bool hasPayload,
      uint32_t keyColumnCount = 1);

  RadixSortKeyLayoutKind kind() const {
    return kind_;
  }

  uint32_t width() const {
    return width_;
  }

  uint32_t inlineCapacity() const {
    return inlineCapacity_;
  }

  uint32_t inlineWordCount() const {
    return inlineCapacity_ / sizeof(uint64_t);
  }

  uint32_t radixWidth() const {
    return radixWidth_;
  }

  bool isVariable() const {
    return variable_;
  }

  bool hasPayload() const {
    return hasPayload_;
  }

  std::optional<uint32_t> sizeOffset() const {
    return sizeOffset_;
  }

  std::optional<uint32_t> dataOffset() const {
    return dataOffset_;
  }

  std::optional<uint32_t> payloadOffset() const {
    return payloadOffset_;
  }

 private:
  RadixSortKeyLayout(
      RadixSortKeyLayoutKind kind,
      uint32_t width,
      uint32_t inlineCapacity,
      bool variable,
      bool hasPayload,
      std::optional<uint32_t> sizeOffset,
      std::optional<uint32_t> dataOffset,
      std::optional<uint32_t> payloadOffset)
      : kind_(kind),
        width_(width),
        inlineCapacity_(inlineCapacity),
        radixWidth_(std::min<uint32_t>(inlineCapacity, sizeof(uint64_t))),
        variable_(variable),
        hasPayload_(hasPayload),
        sizeOffset_(sizeOffset),
        dataOffset_(dataOffset),
        payloadOffset_(payloadOffset) {}

  RadixSortKeyLayoutKind kind_{RadixSortKeyLayoutKind::kInvalid};
  uint32_t width_{0};
  uint32_t inlineCapacity_{0};
  uint32_t radixWidth_{0};
  bool variable_{false};
  bool hasPayload_{false};
  std::optional<uint32_t> sizeOffset_;
  std::optional<uint32_t> dataOffset_;
  std::optional<uint32_t> payloadOffset_;
};

struct EncodedKeyView {
  std::string_view bytes;
  bool zeroPadded{false};
};

union PointerSlot {
  char* pointer;
  uint64_t padding;
};

struct KeyOnlyFixed8Record {
  uint64_t part0;
};

struct KeyOnlyFixed16Record {
  uint64_t part0;
  uint64_t part1;
};

struct KeyOnlyFixed24Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
};

struct KeyOnlyFixed32Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
  uint64_t part3;
};

struct KeyOnlyVariable32Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t size;
  PointerSlot data;
};

struct KeyWithPayloadFixed16Record {
  uint64_t part0;
  PointerSlot payload;
};

struct KeyWithPayloadFixed24Record {
  uint64_t part0;
  uint64_t part1;
  PointerSlot payload;
};

struct KeyWithPayloadFixed32Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
  PointerSlot payload;
};

struct KeyWithPayloadVariable32Record {
  uint64_t part0;
  uint64_t size;
  PointerSlot data;
  PointerSlot payload;
};

struct KeyWithPayloadVariable56Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
  uint64_t part3;
  uint64_t size;
  PointerSlot data;
  PointerSlot payload;
};

struct KeyWithPayloadVariable64Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
  uint64_t part3;
  uint64_t part4;
  uint64_t size;
  PointerSlot data;
  PointerSlot payload;
};

static_assert(sizeof(PointerSlot) == 8);

#define BOLT_CHECK_RADIX_SORT_RECORD(type, width)    \
  static_assert(sizeof(type) == width);              \
  static_assert(std::is_trivially_copyable_v<type>); \
  static_assert(alignof(type) == alignof(uint64_t))

BOLT_CHECK_RADIX_SORT_RECORD(KeyOnlyFixed8Record, 8);
BOLT_CHECK_RADIX_SORT_RECORD(KeyOnlyFixed16Record, 16);
BOLT_CHECK_RADIX_SORT_RECORD(KeyOnlyFixed24Record, 24);
BOLT_CHECK_RADIX_SORT_RECORD(KeyOnlyFixed32Record, 32);
BOLT_CHECK_RADIX_SORT_RECORD(KeyOnlyVariable32Record, 32);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadFixed16Record, 16);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadFixed24Record, 24);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadFixed32Record, 32);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadVariable32Record, 32);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadVariable56Record, 56);
BOLT_CHECK_RADIX_SORT_RECORD(KeyWithPayloadVariable64Record, 64);

#undef BOLT_CHECK_RADIX_SORT_RECORD

#define BOLT_CHECK_RADIX_SORT_OFFSET(type, field, offset) \
  static_assert(offsetof(type, field) == offset)

BOLT_CHECK_RADIX_SORT_OFFSET(KeyOnlyVariable32Record, size, 16);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyOnlyVariable32Record, data, 24);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed16Record, payload, 8);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed24Record, payload, 16);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed32Record, payload, 24);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, size, 8);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, data, 16);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, payload, 24);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable56Record, size, 32);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable56Record, data, 40);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable56Record, payload, 48);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable64Record, size, 40);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable64Record, data, 48);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable64Record, payload, 56);

#undef BOLT_CHECK_RADIX_SORT_OFFSET

template <RadixSortKeyLayoutKind KIND>
struct RadixSortKeyTraits;

#define BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(                   \
    kind,                                                       \
    type,                                                       \
    width,                                                      \
    inlineCapacity,                                             \
    variable,                                                   \
    payload,                                                    \
    sizeOffset,                                                 \
    dataOffset,                                                 \
    payloadOffset)                                              \
  template <>                                                   \
  struct RadixSortKeyTraits<RadixSortKeyLayoutKind::kind> {     \
    using Type = type;                                          \
    static constexpr uint32_t kWidth = width;                   \
    static constexpr uint32_t kInlineCapacity = inlineCapacity; \
    static constexpr uint32_t kInlineWords =                    \
        inlineCapacity / sizeof(uint64_t);                      \
    static constexpr bool kVariable = variable;                 \
    static constexpr bool kHasPayload = payload;                \
    static constexpr uint32_t kSizeOffset = sizeOffset;         \
    static constexpr uint32_t kDataOffset = dataOffset;         \
    static constexpr uint32_t kPayloadOffset = payloadOffset;   \
  }

BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyOnlyFixed8,
    KeyOnlyFixed8Record,
    8,
    8,
    false,
    false,
    0,
    0,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyOnlyFixed16,
    KeyOnlyFixed16Record,
    16,
    16,
    false,
    false,
    0,
    0,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyOnlyFixed24,
    KeyOnlyFixed24Record,
    24,
    24,
    false,
    false,
    0,
    0,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyOnlyFixed32,
    KeyOnlyFixed32Record,
    32,
    32,
    false,
    false,
    0,
    0,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyOnlyVariable32,
    KeyOnlyVariable32Record,
    32,
    16,
    true,
    false,
    16,
    24,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed16,
    KeyWithPayloadFixed16Record,
    16,
    8,
    false,
    true,
    0,
    0,
    8);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed24,
    KeyWithPayloadFixed24Record,
    24,
    16,
    false,
    true,
    0,
    0,
    16);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed32,
    KeyWithPayloadFixed32Record,
    32,
    24,
    false,
    true,
    0,
    0,
    24);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadVariable32,
    KeyWithPayloadVariable32Record,
    32,
    8,
    true,
    true,
    8,
    16,
    24);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadVariable56,
    KeyWithPayloadVariable56Record,
    56,
    32,
    true,
    true,
    32,
    40,
    48);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadVariable64,
    KeyWithPayloadVariable64Record,
    64,
    40,
    true,
    true,
    40,
    48,
    56);

#undef BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS

template <RadixSortKeyLayoutKind KIND>
class RadixSortKeyOps {
 public:
  using Traits = RadixSortKeyTraits<KIND>;

  static int32_t compare(const char* left, const char* right) {
    for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
      const auto leftWord =
          loadUnaligned<uint64_t>(left + word * sizeof(uint64_t));
      const auto rightWord =
          loadUnaligned<uint64_t>(right + word * sizeof(uint64_t));
      if (leftWord != rightWord) {
        return (leftWord > rightWord) - (leftWord < rightWord);
      }
    }
    if constexpr (!Traits::kVariable) {
      return 0;
    }

    const auto leftSize = loadUnaligned<uint64_t>(left + Traits::kSizeOffset);
    const auto rightSize = loadUnaligned<uint64_t>(right + Traits::kSizeOffset);
    if (leftSize <= Traits::kInlineCapacity ||
        rightSize <= Traits::kInlineCapacity) {
      return (leftSize > rightSize) - (leftSize < rightSize);
    }

    const auto* leftData =
        loadUnaligned<const char*>(left + Traits::kDataOffset);
    const auto* rightData =
        loadUnaligned<const char*>(right + Traits::kDataOffset);
    const auto result = std::memcmp(
        leftData + Traits::kInlineCapacity,
        rightData + Traits::kInlineCapacity,
        std::min(leftSize, rightSize) - Traits::kInlineCapacity);
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
    return (leftSize > rightSize) - (leftSize < rightSize);
  }

  static uint8_t encodedByte(const char* key, uint64_t offset) {
    if constexpr (Traits::kVariable) {
      const auto size = loadUnaligned<uint64_t>(key + Traits::kSizeOffset);
      if (size > Traits::kInlineCapacity) {
        const auto* data =
            loadUnaligned<const char*>(key + Traits::kDataOffset);
        return static_cast<uint8_t>(data[offset]);
      }
    }
    const auto word = loadUnaligned<uint64_t>(
        key + (offset / sizeof(uint64_t)) * sizeof(uint64_t));
    const auto shift = (sizeof(uint64_t) - 1 - offset % sizeof(uint64_t)) * 8;
    return static_cast<uint8_t>(word >> shift);
  }
};

class RadixSortKey {
 public:
  RadixSortKey(const RadixSortKeyLayout& layout, char* data)
      : layout_(&layout), data_(data), mutableData_(data) {}

  RadixSortKey(const RadixSortKeyLayout& layout, const char* data)
      : layout_(&layout), data_(data) {}

  void construct(
      std::string_view encodedKey,
      char* overflowData,
      char* payload = nullptr) const;

  void deconstruct(
      RadixSortInlineKeyBuffer& inlineBuffer,
      EncodedKeyView& encodedKey) const;

  int32_t compare(const RadixSortKey& other) const;

  uint64_t heapSize() const;

  char* fullKeyData() const;

  char* payload() const;

  void setPayload(char* payload) const;

  uint64_t encodedSize() const;

  uint8_t encodedByte(uint64_t offset) const;

  const char* rawData() const {
    return data_;
  }

 private:
  uint64_t inlineWord(uint32_t index) const;

  uint64_t storedSize() const;

  const RadixSortKeyLayout* layout_;
  const char* data_;
  char* mutableData_{nullptr};
};

} // namespace bytedance::bolt::exec::radixsort
