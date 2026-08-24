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
};

using RadixSortInlineKeyBuffer = std::array<char, 32>;

constexpr uint32_t kCompactPointerBytes = 6;
constexpr uint32_t kCompactPointerBits = kCompactPointerBytes * 8;
constexpr uint64_t kCompactPointerMask =
    (uint64_t{1} << kCompactPointerBits) - 1;

struct CompactPointer48 {
  std::array<uint8_t, kCompactPointerBytes> bytes;
};

using UnalignedUInt64Slot = std::array<uint8_t, sizeof(uint64_t)>;

static_assert(sizeof(uintptr_t) == sizeof(uint64_t));
static_assert(sizeof(CompactPointer48) == kCompactPointerBytes);
static_assert(sizeof(UnalignedUInt64Slot) == sizeof(uint64_t));

inline uint64_t loadCompactUInt48(const void* slot) {
  const auto* bytes = static_cast<const uint8_t*>(slot);
  return loadUnaligned<uint32_t>(bytes) |
      (static_cast<uint64_t>(loadUnaligned<uint16_t>(bytes + 4)) << 32);
}

inline void storeCompactUInt48(void* slot, uint64_t value) {
  BOLT_DCHECK_EQ(value & ~kCompactPointerMask, 0);
  auto* bytes = static_cast<uint8_t*>(slot);
  storeUnaligned<uint32_t>(bytes, static_cast<uint32_t>(value));
  storeUnaligned<uint16_t>(bytes + 4, static_cast<uint16_t>(value >> 32));
}

inline char* loadCompactPointer(const void* slot) {
  return reinterpret_cast<char*>(
      static_cast<uintptr_t>(loadCompactUInt48(slot)));
}

inline void storeCompactPointer(void* slot, const void* pointer) {
  storeCompactUInt48(slot, reinterpret_cast<uintptr_t>(pointer));
}

void checkCompactPointerRange(const void* data, uint64_t size);

class RadixSortKeyLayout {
 public:
  RadixSortKeyLayout() = default;

  static RadixSortKeyLayout fromKind(RadixSortKeyLayoutKind kind);

  static RadixSortKeyLayout select(
      std::optional<uint64_t> maximumEncodedSize,
      bool hasPayload,
      uint32_t heapKeyOffset = 0);

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

  uint32_t inlineWordBytes() const {
    return inlineWordCount() * sizeof(uint64_t);
  }

  uint32_t inlineTailBytes() const {
    return inlineCapacity_ - inlineWordBytes();
  }

  uint32_t radixWidth() const {
    return radixWidth_;
  }

  // First logical encoded-key byte stored in overflow heap. Earlier bytes are
  // complete top-level fixed key columns already present in the inline record.
  uint32_t heapKeyOffset() const {
    return heapKeyOffset_;
  }

  uint64_t heapSize(uint64_t encodedSize) const;

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
      std::optional<uint32_t> payloadOffset,
      uint32_t heapKeyOffset = 0)
      : kind_(kind),
        width_(width),
        inlineCapacity_(inlineCapacity),
        radixWidth_(
            variable ? inlineCapacity
                     : std::min<uint32_t>(inlineCapacity, sizeof(uint64_t))),
        heapKeyOffset_(heapKeyOffset),
        variable_(variable),
        hasPayload_(hasPayload),
        sizeOffset_(sizeOffset),
        dataOffset_(dataOffset),
        payloadOffset_(payloadOffset) {}

  RadixSortKeyLayoutKind kind_{RadixSortKeyLayoutKind::kInvalid};
  uint32_t width_{0};
  uint32_t inlineCapacity_{0};
  uint32_t radixWidth_{0};
  uint32_t heapKeyOffset_{0};
  bool variable_{false};
  bool hasPayload_{false};
  std::optional<uint32_t> sizeOffset_;
  std::optional<uint32_t> dataOffset_;
  std::optional<uint32_t> payloadOffset_;
};

struct EncodedKeyView {
  std::string_view bytes;
};

static_assert(sizeof(EncodedKeyView) == 16);

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

struct alignas(uint64_t) KeyOnlyVariable32Record {
  uint64_t part0;
  uint64_t part1;
  std::array<uint8_t, 2> tail;
  UnalignedUInt64Slot size;
  CompactPointer48 data;
};

struct alignas(uint64_t) KeyWithPayloadFixed16Record {
  uint64_t part0;
  std::array<uint8_t, 2> tail;
  CompactPointer48 payload;
};

struct alignas(uint64_t) KeyWithPayloadFixed24Record {
  uint64_t part0;
  uint64_t part1;
  std::array<uint8_t, 2> tail;
  CompactPointer48 payload;
};

struct alignas(uint64_t) KeyWithPayloadFixed32Record {
  uint64_t part0;
  uint64_t part1;
  uint64_t part2;
  std::array<uint8_t, 2> tail;
  CompactPointer48 payload;
};

struct alignas(uint64_t) KeyWithPayloadVariable32Record {
  uint64_t part0;
  std::array<uint8_t, 4> tail;
  UnalignedUInt64Slot size;
  CompactPointer48 data;
  CompactPointer48 payload;
};

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

#undef BOLT_CHECK_RADIX_SORT_RECORD

#define BOLT_CHECK_RADIX_SORT_OFFSET(type, field, offset) \
  static_assert(offsetof(type, field) == offset)

BOLT_CHECK_RADIX_SORT_OFFSET(KeyOnlyVariable32Record, tail, 16);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyOnlyVariable32Record, size, 18);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyOnlyVariable32Record, data, 26);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed16Record, tail, 8);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed16Record, payload, 10);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed24Record, tail, 16);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed24Record, payload, 18);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed32Record, tail, 24);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadFixed32Record, payload, 26);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, tail, 8);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, size, 12);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, data, 20);
BOLT_CHECK_RADIX_SORT_OFFSET(KeyWithPayloadVariable32Record, payload, 26);

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
    static constexpr uint32_t kInlineWordBytes =                \
        kInlineWords * sizeof(uint64_t);                        \
    static constexpr uint32_t kInlineTailBytes =                \
        inlineCapacity - kInlineWordBytes;                      \
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
    18,
    true,
    false,
    18,
    26,
    0);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed16,
    KeyWithPayloadFixed16Record,
    16,
    10,
    false,
    true,
    0,
    0,
    10);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed24,
    KeyWithPayloadFixed24Record,
    24,
    18,
    false,
    true,
    0,
    0,
    18);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadFixed32,
    KeyWithPayloadFixed32Record,
    32,
    26,
    false,
    true,
    0,
    0,
    26);
BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS(
    kKeyWithPayloadVariable32,
    KeyWithPayloadVariable32Record,
    32,
    12,
    true,
    true,
    12,
    20,
    26);

#undef BOLT_DEFINE_PHYSICAL_SORT_KEY_TRAITS

template <typename Traits>
inline void storeFixedKeyPrefix(const char* encoded, char* record) {
  for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
    auto value = loadUnaligned<uint64_t>(encoded + word * sizeof(uint64_t));
    if constexpr (std::endian::native == std::endian::little) {
      value = byteSwap(value);
    }
    storeUnaligned<uint64_t>(record + word * sizeof(uint64_t), value);
  }
  std::memcpy(
      record + Traits::kInlineWordBytes,
      encoded + Traits::kInlineWordBytes,
      Traits::kInlineTailBytes);
}

template <typename Traits, bool rawEncodedWords>
inline int32_t compareInlineKeyPrefix(const char* left, const char* right) {
  for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
    auto leftWord = loadUnaligned<uint64_t>(left + word * sizeof(uint64_t));
    auto rightWord = loadUnaligned<uint64_t>(right + word * sizeof(uint64_t));
    if constexpr (
        rawEncodedWords && std::endian::native == std::endian::little) {
      leftWord = byteSwap(leftWord);
      rightWord = byteSwap(rightWord);
    }
    if (leftWord != rightWord) {
      return (leftWord > rightWord) - (leftWord < rightWord);
    }
  }
  if constexpr (Traits::kInlineTailBytes > 0) {
    const auto result = std::memcmp(
        left + Traits::kInlineWordBytes,
        right + Traits::kInlineWordBytes,
        Traits::kInlineTailBytes);
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
  }
  return 0;
}

template <typename Traits, uint32_t remaining = Traits::kInlineWords>
inline bool fixedKeyLess(const char* left, const char* right) {
  const auto leftWord = loadUnaligned<uint64_t>(left);
  const auto rightWord = loadUnaligned<uint64_t>(right);
  if constexpr (remaining > 1) {
    return (leftWord < rightWord) ||
        ((leftWord == rightWord) &&
         fixedKeyLess<Traits, remaining - 1>(
             left + sizeof(uint64_t), right + sizeof(uint64_t)));
  } else if constexpr (Traits::kInlineTailBytes > 0) {
    if (leftWord != rightWord) {
      return leftWord < rightWord;
    }
    static_assert(Traits::kInlineTailBytes == sizeof(uint16_t));
    auto leftTail = loadUnaligned<uint16_t>(left + sizeof(uint64_t));
    auto rightTail = loadUnaligned<uint16_t>(right + sizeof(uint64_t));
    if constexpr (std::endian::native == std::endian::little) {
      leftTail = byteSwap(leftTail);
      rightTail = byteSwap(rightTail);
    }
    return leftTail < rightTail;
  } else {
    return leftWord < rightWord;
  }
}

template <RadixSortKeyLayoutKind KIND>
class RadixSortKeyOps {
 public:
  using Traits = RadixSortKeyTraits<KIND>;

  static int32_t compare(const char* left, const char* right) {
    return compare(left, right, 0);
  }

  static int32_t
  compare(const char* left, const char* right, uint32_t heapKeyOffset) {
    if constexpr (!Traits::kVariable) {
      BOLT_DCHECK_EQ(heapKeyOffset, 0);
      return compareInlineKeyPrefix<Traits, false>(left, right);
    } else {
      const auto prefixResult =
          compareInlineKeyPrefix<Traits, true>(left, right);
      if (prefixResult != 0) {
        return prefixResult;
      }
      const auto leftSize = loadUnaligned<uint64_t>(left + Traits::kSizeOffset);
      const auto rightSize =
          loadUnaligned<uint64_t>(right + Traits::kSizeOffset);
      if (leftSize <= Traits::kInlineCapacity ||
          rightSize <= Traits::kInlineCapacity) {
        return (leftSize > rightSize) - (leftSize < rightSize);
      }
      return compareSuffixWithSizes(
          left,
          right,
          leftSize,
          rightSize,
          heapKeyOffset,
          Traits::kInlineCapacity);
    }
  }

  // Compares keys in a radix bucket after all record prefix bytes have been
  // consumed. No record byte is read again.
  static int32_t compareSuffix(
      const char* left,
      const char* right,
      uint32_t heapKeyOffset,
      uint32_t radixWidth) {
    static_assert(Traits::kVariable);
    BOLT_DCHECK_LE(heapKeyOffset, radixWidth);
    BOLT_DCHECK_LE(radixWidth, Traits::kInlineCapacity);
    const auto leftSize = loadUnaligned<uint64_t>(left + Traits::kSizeOffset);
    const auto rightSize = loadUnaligned<uint64_t>(right + Traits::kSizeOffset);
    if (leftSize <= radixWidth || rightSize <= radixWidth) {
      return (leftSize > rightSize) - (leftSize < rightSize);
    }
    return compareSuffixWithSizes(
        left, right, leftSize, rightSize, heapKeyOffset, radixWidth);
  }

 private:
  static int32_t compareSuffixWithSizes(
      const char* left,
      const char* right,
      uint64_t leftSize,
      uint64_t rightSize,
      uint32_t heapKeyOffset,
      uint32_t radixWidth) {
    const auto* leftData = loadCompactPointer(left + Traits::kDataOffset);
    const auto* rightData = loadCompactPointer(right + Traits::kDataOffset);
    const auto result = std::memcmp(
        leftData + radixWidth - heapKeyOffset,
        rightData + radixWidth - heapKeyOffset,
        std::min(leftSize, rightSize) - radixWidth);
    if (result != 0) {
      return (result > 0) - (result < 0);
    }
    return (leftSize > rightSize) - (leftSize < rightSize);
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

  std::string_view heapKey() const;

  char* heapKeyData() const;

  char* payload() const;

 private:
  uint64_t inlineWord(uint32_t index) const;

  uint64_t storedSize() const;

  const RadixSortKeyLayout* layout_;
  const char* data_;
  char* mutableData_{nullptr};
};

} // namespace bytedance::bolt::exec::radixsort
