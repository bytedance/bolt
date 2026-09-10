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

#include "bolt/vector/Utf8Utils.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include <folly/lang/Bits.h>

#include "bolt/common/base/SimdUtil.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/DictionaryVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::utf8 {
namespace {

// OpenJDK sun.nio.cs.UTF_8 malformed-input grouping: each maximal malformed
// subpart emits a single U+FFFD replacement (3 bytes).
constexpr char kReplacement[3] = {'\xEF', '\xBF', '\xBD'};
constexpr int32_t kReplacementSize = 3;
constexpr int32_t kReplacementsPerBlock = 64;
alignas(64) constexpr auto kReplacementBlock = [] {
  std::array<char, kReplacementSize * kReplacementsPerBlock> block{};
  for (int32_t index = 0; index < kReplacementsPerBlock; ++index) {
    for (int32_t byte = 0; byte < kReplacementSize; ++byte) {
      block[index * kReplacementSize + byte] = kReplacement[byte];
    }
  }
  return block;
}();
constexpr int32_t kMaxStringSize = std::numeric_limits<int32_t>::max();
// vector_size_t and per-row VARCHAR sizes are both bounded by INT32_MAX, so
// their vector-wide byte product fits in the required 64-bit size_t.
static_assert(
    sizeof(size_t) >= sizeof(uint64_t),
    "Vector-wide VARCHAR byte counts require 64-bit size_t");

inline bool isCont(uint8_t b) {
  return (b & 0xC0) == 0x80;
}

inline bool isRegularThreeByteLead(uint8_t b) {
  return b >= 0xE1 && b <= 0xEF && b != 0xED;
}

// Returns (validBytes, malformedBytes) for the UTF-8 code unit at p[0].
// Implements OpenJDK's "maximal malformed subpart" rule: when a leading byte
// declares N continuation bytes but the k-th continuation is missing or
// invalid, bytes [0, k-1] are consumed as a single malformed unit and replaced
// by one U+FFFD. Stray continuation bytes (0x80..0xBF) and 0xC0/0xC1/0xF5+
// are each replaced individually.
inline std::pair<int32_t, int32_t> nextSegment(
    const uint8_t* p,
    int32_t remaining) {
  const uint8_t c = p[0];
  if (c <= 0x7F) {
    return {1, 0};
  }
  if (c >= 0xC2 && c <= 0xDF) {
    if (remaining < 2)
      return {0, remaining};
    if (!isCont(p[1]))
      return {0, 1};
    return {2, 0};
  }
  if (c >= 0xE0 && c <= 0xEF) {
    if (remaining == 1) {
      return {0, 1};
    }
    if (!isCont(p[1]) || (c == 0xE0 && p[1] < 0xA0)) {
      return {0, 1};
    }
    if (remaining == 2) {
      return {0, 2};
    }
    if (!isCont(p[2])) {
      return {0, 2};
    }
    if (c == 0xED && p[1] >= 0xA0) {
      return {0, 3};
    }
    return {3, 0};
  }
  if (c >= 0xF0 && c <= 0xF4) {
    if (remaining == 1) {
      return {0, 1};
    }
    if (!isCont(p[1]) || (c == 0xF0 && p[1] < 0x90) ||
        (c == 0xF4 && p[1] > 0x8F)) {
      return {0, 1};
    }
    if (remaining == 2) {
      return {0, 2};
    }
    if (!isCont(p[2]))
      return {0, 2};
    if (remaining == 3) {
      return {0, 3};
    }
    if (!isCont(p[3]))
      return {0, 3};
    return {4, 0};
  }
  // 0x80..0xBF stray continuation, 0xC0/0xC1 overlong 2-byte, 0xF5..0xFF
  // invalid lead.
  return {0, 1};
}

// Consumes a run of ordinary 3-byte code points without returning to the outer
// malformed-input state machine for each code point. E0 and ED retain their
// specialized checks in nextSegment().
FOLLY_ALWAYS_INLINE int32_t
regularThreeBytePrefixLength(const uint8_t* data, int32_t remaining) {
  int32_t offset = 0;
  while (offset <= remaining - 3) {
    const auto lead = data[offset];
    if (!isRegularThreeByteLead(lead) || !isCont(data[offset + 1]) ||
        !isCont(data[offset + 2])) {
      break;
    }
    offset += 3;
  }
  return offset;
}

// Validates several ordinary 3-byte code points per SIMD batch. Kept out of
// scanUtf8 so binary-heavy inputs retain the compact scalar state machine.
FOLLY_NOINLINE int32_t
regularThreeBytePrefixLengthSimd(const uint8_t* data, int32_t remaining) {
  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  constexpr int32_t kTripletsPerBatch = (kBatchSize - 2) / 3;
  constexpr int32_t kBytesPerBatch = kTripletsPerBatch * 3;
  static_assert(kBatchSize <= sizeof(uint32_t) * 8);
  constexpr uint32_t kTripletStarts = [] {
    uint32_t mask = 0;
    for (int32_t index = 0; index < kBytesPerBatch; index += 3) {
      mask |= uint32_t{1} << index;
    }
    return mask;
  }();
  const auto leadLow = Batch::broadcast(0xE1);
  const auto leadHigh = Batch::broadcast(0xEF);
  const auto surrogateLead = Batch::broadcast(0xED);
  const auto continuationLow = Batch::broadcast(0x80);
  const auto continuationHigh = Batch::broadcast(0xBF);

  int32_t offset = 0;
  for (; offset <= remaining - kBatchSize; offset += kBytesPerBatch) {
    const auto bytes = Batch::load_unaligned(data + offset);
    const auto ordinaryLead =
        (bytes >= leadLow) & (bytes <= leadHigh) & (bytes != surrogateLead);
    const auto continuation =
        (bytes >= continuationLow) & (bytes <= continuationHigh);
    const auto leadMask = static_cast<uint32_t>(simd::toBitMask(ordinaryLead));
    const auto continuationMask =
        static_cast<uint32_t>(simd::toBitMask(continuation));
    const auto validTriplets =
        leadMask & (continuationMask >> 1) & (continuationMask >> 2);
    if (FOLLY_UNLIKELY((validTriplets & kTripletStarts) != kTripletStarts)) {
      break;
    }
  }

  offset += regularThreeBytePrefixLength(data + offset, remaining - offset);
  return offset;
}

// Returns the number of leading ASCII bytes. A SIMD comparison plus bit-mask
// conversion locates the first high byte without a scalar rescan of the batch.
// The UTF-8 state machine starts at that byte, so no successfully screened
// ASCII prefix is inspected again.
FOLLY_ALWAYS_INLINE int32_t asciiPrefixLength(const char* data, int32_t size) {
  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  const auto highBit = Batch::broadcast(0x80);
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);

  int32_t offset = 0;
  for (; offset <= size - kBatchSize; offset += kBatchSize) {
    const auto batch = Batch::load_unaligned(bytes + offset);
    const auto highBytes =
        static_cast<uint32_t>(simd::toBitMask(batch >= highBit));
    if (FOLLY_UNLIKELY(highBytes != 0)) {
      return offset + std::countr_zero(highBytes);
    }
  }

  constexpr uint64_t kAsciiMask64 = 0x8080808080808080ULL;
  for (; offset <= size - static_cast<int32_t>(sizeof(uint64_t));
       offset += sizeof(uint64_t)) {
    const auto highBytes =
        folly::loadUnaligned<uint64_t>(data + offset) & kAsciiMask64;
    if (FOLLY_UNLIKELY(highBytes != 0)) {
      return offset + std::countr_zero(highBytes) / 8;
    }
  }
  for (; offset < size; ++offset) {
    if (FOLLY_UNLIKELY((bytes[offset] & 0x80) != 0)) {
      break;
    }
  }
  return offset;
}

FOLLY_ALWAYS_INLINE bool isSingleByteMalformed(
    const uint8_t* data,
    int32_t remaining) {
  const auto current = data[0];
  if ((current >= 0x80 && current <= 0xC1) || current >= 0xF5) {
    return true;
  }
  return current >= 0xC2 && current <= 0xDF &&
      (remaining == 1 || !isCont(data[1]));
}

// Returns a prefix where every byte is a UTF-8 lead byte and therefore makes
// the preceding lead malformed. This is the common shape of binary payloads
// such as repeated 0xD5. The final lead is included only when its following
// byte cannot be a continuation (or the input ends), preserving valid tails
// such as D5 80.
FOLLY_ALWAYS_INLINE int32_t
denseLeadMalformedPrefixLength(const uint8_t* data, int32_t remaining) {
  if (data[0] < 0xC0 || (remaining > 1 && isCont(data[1]))) {
    return 0;
  }

  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  static_assert(kBatchSize <= sizeof(uint32_t) * 8);
  const auto leadLow = Batch::broadcast(0xC0);
  const auto kAllLanes = simd::allSetBitMask<uint8_t>();

  int32_t offset = 0;
  for (; offset <= remaining - kBatchSize; offset += kBatchSize) {
    const auto bytes = Batch::load_unaligned(data + offset);
    const auto mask = static_cast<uint32_t>(simd::toBitMask(bytes >= leadLow));
    if (FOLLY_UNLIKELY(mask != kAllLanes)) {
      const auto leadBytes = static_cast<int32_t>(std::countr_one(mask));
      BOLT_DCHECK_LT(leadBytes, kBatchSize);
      return offset + leadBytes -
          (leadBytes > 0 && isCont(data[offset + leadBytes]));
    }
    if (offset + kBatchSize == remaining) {
      return remaining;
    }
    if (FOLLY_UNLIKELY(isCont(data[offset + kBatchSize]))) {
      return offset + kBatchSize - 1;
    }
  }

  while (offset < remaining && data[offset] >= 0xC0 &&
         (offset + 1 == remaining || !isCont(data[offset + 1]))) {
    ++offset;
  }
  return offset;
}

// Coalesces consecutive one-byte malformed units, using SIMD for mixed binary
// payloads and the cheaper lead-only scan when possible.
FOLLY_ALWAYS_INLINE int32_t
singleByteMalformedPrefixLength(const uint8_t* data, int32_t remaining) {
  if (!isSingleByteMalformed(data, remaining)) {
    return 0;
  }
  if (remaining == 1 || data[1] <= 0x7F) {
    return 1;
  }

  constexpr uint32_t kFourLeadBytes = 0xC0C0C0C0U;
  if (remaining >= static_cast<int32_t>(sizeof(uint32_t)) &&
      (folly::loadUnaligned<uint32_t>(data) & kFourLeadBytes) ==
          kFourLeadBytes) {
    const auto denseLeadPrefix =
        denseLeadMalformedPrefixLength(data, remaining);
    if (denseLeadPrefix > 0) {
      return denseLeadPrefix;
    }
  }

  using Batch = xsimd::batch<uint8_t>;
  constexpr int32_t kBatchSize = Batch::size;
  static_assert(kBatchSize <= sizeof(uint32_t) * 8);
  const auto continuationLow = Batch::broadcast(0x80);
  const auto continuationHigh = Batch::broadcast(0xBF);
  const auto invalidTwoByteHigh = Batch::broadcast(0xC1);
  const auto twoByteLow = Batch::broadcast(0xC2);
  const auto twoByteHigh = Batch::broadcast(0xDF);
  const auto invalidLeadLow = Batch::broadcast(0xF5);
  const auto kAllLanes = simd::allSetBitMask<uint8_t>();

  int32_t offset = 0;
  for (; offset <= remaining - kBatchSize - 1; offset += kBatchSize) {
    const auto current = Batch::load_unaligned(data + offset);
    const auto next = Batch::load_unaligned(data + offset + 1);
    const auto alwaysMalformed =
        ((current >= continuationLow) & (current <= invalidTwoByteHigh)) |
        (current >= invalidLeadLow);
    const auto twoByteLead = (current >= twoByteLow) & (current <= twoByteHigh);
    const auto nextIsContinuation =
        (next >= continuationLow) & (next <= continuationHigh);
    const auto malformed =
        alwaysMalformed | (twoByteLead & ~nextIsContinuation);
    const auto mask = static_cast<uint32_t>(simd::toBitMask(malformed));
    if (FOLLY_UNLIKELY(mask != kAllLanes)) {
      return offset + std::countr_one(mask);
    }
  }
  while (offset < remaining &&
         isSingleByteMalformed(data + offset, remaining - offset)) {
    ++offset;
  }
  return offset;
}

FOLLY_ALWAYS_INLINE void writeReplacementRun(
    char* output,
    int32_t replacements) {
  BOLT_DCHECK_GT(replacements, 0);
  if (replacements == 1) {
    std::memcpy(output, kReplacement, kReplacementSize);
    return;
  }
  const auto seedCount = std::min(replacements, kReplacementsPerBlock);
  std::memcpy(output, kReplacementBlock.data(), seedCount * kReplacementSize);
  int32_t written = seedCount;
  while (written < replacements) {
    const auto copyCount = std::min(written, replacements - written);
    std::memcpy(
        output + written * kReplacementSize,
        output,
        copyCount * kReplacementSize);
    written += copyCount;
  }
}

// Enumerates malformed runs without retaining per-byte metadata. Analysis and
// materialization share the same scanner and OpenJDK grouping rules.
template <typename Consumer>
void forEachMalformedRun(
    const char* data,
    int32_t size,
    int32_t offset,
    Consumer&& consume) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  while (offset < size) {
    if (bytes[offset] <= 0x7F) {
      if (offset + 1 == size || bytes[offset + 1] > 0x7F) {
        ++offset;
      } else {
        offset += asciiPrefixLength(data + offset, size - offset);
      }
      continue;
    }
    const auto malformedPrefix =
        singleByteMalformedPrefixLength(bytes + offset, size - offset);
    if (malformedPrefix > 0) {
      consume(offset, malformedPrefix, malformedPrefix);
      offset += malformedPrefix;
      continue;
    }
    if (isRegularThreeByteLead(bytes[offset])) {
      const auto remaining = size - offset;
      // Short or interleaved code points do not amortize a SIMD call.
      const auto validPrefix = remaining >= xsimd::batch<uint8_t>::size &&
              isCont(bytes[offset + 1]) && isCont(bytes[offset + 2]) &&
              isRegularThreeByteLead(bytes[offset + 3])
          ? regularThreeBytePrefixLengthSimd(bytes + offset, remaining)
          : regularThreeBytePrefixLength(bytes + offset, remaining);
      if (validPrefix > 0) {
        offset += validPrefix;
        continue;
      }
    }
    const auto [validSize, malformedSize] =
        nextSegment(bytes + offset, size - offset);
    if (malformedSize > 0) {
      consume(offset, malformedSize, 1);
      offset += malformedSize;
    } else {
      offset += validSize;
    }
  }
}

struct MalformedRun {
  int32_t offset;
  int32_t length;
  int32_t replacements;
};

constexpr int32_t kMaxCachedMalformedRuns = 64;
using MalformedRuns = std::array<MalformedRun, kMaxCachedMalformedRuns>;

struct StringAnalysis {
  StringAnalysis(int32_t size, int32_t runs, bool onlyReplacement)
      : outputSize(size), replacementOnly(onlyReplacement), runCount(runs) {}

  // VARCHAR sizes fit in 31 bits. Keep the complete result in one register.
  uint32_t outputSize : 31;
  uint32_t replacementOnly : 1;
  int32_t runCount;
};
static_assert(sizeof(StringAnalysis) == sizeof(uint64_t));

FOLLY_NOINLINE StringAnalysis
analyzeStringSuffix(StringView value, MalformedRuns& runs, int32_t prefix) {
  const auto size = static_cast<int32_t>(value.size());
  int64_t outputSize = size;
  int32_t malformedBytes = 0;
  int32_t runCount = 0;
  forEachMalformedRun(
      value.data(),
      size,
      prefix,
      [&](int32_t offset, int32_t length, int32_t count) {
        if (runCount < kMaxCachedMalformedRuns) {
          runs[runCount] = {offset, length, count};
        }
        ++runCount;
        malformedBytes += length;
        outputSize += static_cast<int64_t>(count) * kReplacementSize - length;
      });
  BOLT_USER_CHECK_LE(
      outputSize,
      kMaxStringSize,
      "UTF-8 replacement result exceeds the maximum VARCHAR size");
  return {
      static_cast<int32_t>(outputSize),
      runCount,
      runCount > 0 && malformedBytes == size};
}

// Keep common complete strings out of the malformed-run state machine.
FOLLY_ALWAYS_INLINE StringAnalysis
analyzeString(const StringView& value, MalformedRuns& runs) {
  const auto size = static_cast<int32_t>(value.size());
  const auto* data = reinterpret_cast<const uint8_t*>(value.data());
  int32_t prefix = 0;
  if (size > 0 && data[0] <= 0x7F) {
    prefix = asciiPrefixLength(value.data(), size);
  } else if (
      size >= sizeof(uint32_t) &&
      (folly::loadUnaligned<uint32_t>(data) & 0xC0C0C0C0U) == 0xC0C0C0C0U &&
      denseLeadMalformedPrefixLength(data, size) == size) {
    BOLT_USER_CHECK_LE(
        size,
        kMaxStringSize / kReplacementSize,
        "UTF-8 replacement result exceeds the maximum VARCHAR size");
    runs[0] = {0, size, size};
    return {size * kReplacementSize, 1, true};
  } else if (size > 0 && isRegularThreeByteLead(data[0])) {
    prefix = regularThreeBytePrefixLengthSimd(data, size);
    if (prefix < size && data[prefix] <= 0x7F) {
      prefix += asciiPrefixLength(value.data() + prefix, size - prefix);
    }
  }
  if (prefix == size) {
    return {size, 0, false};
  }
  return analyzeStringSuffix(value, runs, prefix);
}

struct StringWriter {
  StringView source;
  char* output;
  int32_t copied{0};

  void append(int32_t offset, int32_t length, int32_t replacements) {
    const auto validSize = offset - copied;
    if (validSize > 0) {
      std::memcpy(output, source.data() + copied, validSize);
      output += validSize;
    }
    writeReplacementRun(output, replacements);
    output += replacements * kReplacementSize;
    copied = offset + length;
  }

  void finish() {
    if (copied < source.size()) {
      std::memcpy(output, source.data() + copied, source.size() - copied);
    }
  }
};

// Keep the rare cache-overflow scanner out of the cached materialization path.
FOLLY_NOINLINE void writeFromScan(StringWriter& writer, int32_t offset) {
  forEachMalformedRun(
      writer.source.data(),
      writer.source.size(),
      offset,
      [&](int32_t start, int32_t length, int32_t count) {
        writer.append(start, length, count);
      });
}

FOLLY_ALWAYS_INLINE void writeSanitizedString(
    StringView source,
    const StringAnalysis& analysis,
    const MalformedRuns& runs,
    char* output) {
  StringWriter writer{source, output};
  if (analysis.runCount > kMaxCachedMalformedRuns) {
    writeFromScan(writer, runs[0].offset);
  } else {
    for (int32_t index = 0; index < analysis.runCount; ++index) {
      const auto& run = runs[index];
      writer.append(run.offset, run.length, run.replacements);
    }
  }
  writer.finish();
}

// Inline inputs need at most 3 * kInlineSize output bytes. Materialize them
// directly on the stack instead of caching and replaying malformed runs.
StringAnalysis analyzeInlineString(const StringView& value, char* output) {
  const auto size = static_cast<int32_t>(value.size());
  const auto prefix = asciiPrefixLength(value.data(), size);
  if (prefix == size) {
    return {size, 0, false};
  }
  StringWriter writer{value, output};
  int32_t runCount = 0;
  int32_t malformedBytes = 0;
  forEachMalformedRun(
      value.data(),
      size,
      prefix,
      [&](int32_t offset, int32_t length, int32_t count) {
        writer.append(offset, length, count);
        malformedBytes += length;
        ++runCount;
      });
  const auto outputSize = writer.output - output + size - writer.copied;
  if (runCount > 0) {
    writer.finish();
  }
  return {static_cast<int32_t>(outputSize), runCount, malformedBytes == size};
}

// Returns nullptr for unchanged input. 'selected' is only used for a dictionary
// base: unreferenced values must survive unchanged, without being scanned.
VectorPtr buildSanitizedFlat(
    const VectorPtr& child,
    vector_size_t numRows,
    const uint64_t* outerNulls,
    memory::MemoryPool* pool,
    const SelectivityVector* selected = nullptr) {
  const auto* flat = child->isFlatEncoding()
      ? child->asUnchecked<FlatVector<StringView>>()
      : nullptr;
  const StringView* flatValues = flat ? flat->rawValues() : nullptr;
  const auto* childNulls = flat ? flat->rawNulls() : nullptr;
  const auto* base = flat ? child.get() : child->wrappedVector();
  const auto* baseValues = base->isConstantEncoding()
      ? base->asUnchecked<ConstantVector<StringView>>()->rawValues()
      : base->asUnchecked<FlatVector<StringView>>()->rawValues();

  auto isNull = [&](vector_size_t row) {
    return (outerNulls && bits::isBitNull(outerNulls, row)) ||
        (flat ? childNulls && bits::isBitNull(childNulls, row)
              : child->isNullAt(row));
  };
  auto valueAt = [&](vector_size_t row) -> const StringView& {
    return flat ? flatValues[row] : baseValues[child->wrappedIndex(row)];
  };

  vector_size_t nullCount = 0;
  size_t totalStringBytes = 0;
  size_t unchangedStringBytes = 0;
  uint64_t maxStringLength = 0;
  std::shared_ptr<FlatVector<StringView>> result;
  StringView* outputValues = nullptr;
  const char* replacementData = kReplacementBlock.data();
  static_assert(StringView::kInlineSize % kReplacementSize == 0);
  int32_t replacementCapacity = StringView::kInlineSize;

  auto initializeOutput = [&](vector_size_t firstChanged) FOLLY_NOINLINE {
    BufferPtr nulls;
    const bool combineNulls = outerNulls || !flat;
    if (combineNulls) {
      bool hasNull = false;
      for (vector_size_t row = 0; row < numRows; ++row) {
        if (isNull(row)) {
          hasNull = true;
          break;
        }
      }
      if (hasNull) {
        nulls = AlignedBuffer::allocate<uint64_t>(bits::nwords(numRows), pool);
        auto* rawNulls = nulls->asMutable<uint64_t>();
        std::memset(rawNulls, bits::kNotNullByte, nulls->size());
        for (vector_size_t row = 0; row < numRows; ++row) {
          if (isNull(row)) {
            bits::setNull(rawNulls, row);
          }
        }
      }
    } else {
      nulls = child->nulls();
    }

    auto values = AlignedBuffer::allocate<StringView>(numRows, pool);
    outputValues = values->asMutable<StringView>();
    if (flat) {
      const auto copyRows = selected ? numRows : firstChanged;
      std::memcpy(outputValues, flatValues, copyRows * sizeof(StringView));
    } else {
      for (vector_size_t row = 0; row < firstChanged; ++row) {
        outputValues[row] = isNull(row) ? StringView() : valueAt(row);
      }
    }
    result = std::make_shared<FlatVector<StringView>>(
        pool,
        child->type(),
        std::move(nulls),
        numRows,
        std::move(values),
        std::vector<BufferPtr>{},
        SimpleVectorStats<StringView>{});
  };

  auto writeChanged = [&](vector_size_t row,
                          StringView value,
                          StringAnalysis analysis,
                          const MalformedRuns& runs,
                          const char* inlineOutput) {
    if (!result) {
      initializeOutput(row);
    }
    if (analysis.replacementOnly) {
      if (analysis.outputSize > replacementCapacity) {
        constexpr int32_t kMaxReplacementBufferSize =
            kMaxStringSize - kMaxStringSize % kReplacementSize;
        const auto grownSize = std::min<int64_t>(
            kMaxReplacementBufferSize,
            std::max<int64_t>(
                analysis.outputSize,
                static_cast<int64_t>(replacementCapacity) * 2));
        auto* grown = result->getRawStringBufferWithSpace(grownSize, true);
        writeReplacementRun(grown, grownSize / kReplacementSize);
        replacementData = grown;
        replacementCapacity = static_cast<int32_t>(grownSize);
      }
      outputValues[row] = StringView(replacementData, analysis.outputSize);
      return;
    }
    if (StringView::isInline(analysis.outputSize)) {
      if (value.isInline()) {
        outputValues[row] = StringView(inlineOutput, analysis.outputSize);
        return;
      }
      char inlineData[StringView::kInlineSize];
      writeSanitizedString(value, analysis, runs, inlineData);
      outputValues[row] = StringView(inlineData, analysis.outputSize);
      return;
    }
    auto* output = result->getRawStringBufferWithSpace(analysis.outputSize);
    if (value.isInline()) {
      std::memcpy(output, inlineOutput, analysis.outputSize);
    } else {
      writeSanitizedString(value, analysis, runs, output);
    }
    outputValues[row] = StringView(output, analysis.outputSize);
  };

  auto probe = [&](vector_size_t row, const StringView& value)
      __attribute__((always_inline)) {
    MalformedRuns runs;
    char inlineOutput[StringView::kInlineSize * kReplacementSize];
    const auto analysis = value.isInline()
        ? analyzeInlineString(value, inlineOutput)
        : analyzeString(value, runs);
    maxStringLength = std::max<uint64_t>(maxStringLength, analysis.outputSize);
    if (!StringView::isInline(analysis.outputSize)) {
      totalStringBytes += analysis.outputSize;
    }
    if (analysis.runCount > 0) {
      if (analysis.replacementOnly && outputValues &&
          analysis.outputSize <= replacementCapacity) {
        outputValues[row] = StringView(replacementData, analysis.outputSize);
      } else {
        writeChanged(row, value, analysis, runs, inlineOutput);
      }
    } else {
      if (outputValues) {
        outputValues[row] = value;
      }
      if (!value.isInline()) {
        unchangedStringBytes += value.size();
      }
    }
  };
  if (selected) {
    selected->applyToSelected(
        [&](vector_size_t row) { probe(row, flatValues[row]); });
  } else if (flat && !childNulls && !outerNulls) {
    for (vector_size_t row = 0; row < numRows; ++row) {
      probe(row, flatValues[row]);
    }
  } else {
    for (vector_size_t row = 0; row < numRows; ++row) {
      if (isNull(row)) {
        ++nullCount;
        if (outputValues) {
          outputValues[row] = StringView();
        }
      } else {
        probe(row, valueAt(row));
      }
    }
  }
  if (!result) {
    return nullptr;
  }
  if (!selected) {
    result->setNullCount(nullCount);
    result->setStringViewStats(
        StringViewStats{totalStringBytes, maxStringLength});
  } else if (const auto sourceNullCount = child->getNullCount()) {
    result->setNullCount(sourceNullCount.value());
  }
  // Avoid retaining the whole input for a few unchanged strings in a densely
  // modified batch. Dictionary bases also retain unreferenced source values.
  if (!selected && unchangedStringBytes > 0 &&
      unchangedStringBytes < totalStringBytes - unchangedStringBytes) {
    for (vector_size_t row = 0; row < numRows; ++row) {
      if (isNull(row) || outputValues[row].isInline()) {
        continue;
      }
      const auto value = valueAt(row);
      if (outputValues[row].data() == value.data()) {
        auto* output = result->getRawStringBufferWithSpace(value.size());
        std::memcpy(output, value.data(), value.size());
        outputValues[row] = StringView(output, value.size());
      }
    }
  } else if (selected || unchangedStringBytes > 0) {
    result->acquireSharedStringBuffers(child.get());
  }
  return result;
}

// Analyze each referenced base value once and reuse the flat materializer.
std::optional<VectorPtr> buildSanitizedDictionary(
    const VectorPtr& child,
    vector_size_t numRows,
    const uint64_t* outerNulls,
    memory::MemoryPool* pool) {
  if (child->encoding() != VectorEncoding::Simple::DICTIONARY) {
    return std::nullopt;
  }
  const auto* dictionary = child->asUnchecked<DictionaryVector<StringView>>();
  const auto& base = dictionary->valueVector();
  if (!base->isFlatEncoding() || base->size() > numRows / 2) {
    return std::nullopt;
  }
  SelectivityVector referenced(base->size(), false);
  const auto* indices = dictionary->indices()->as<vector_size_t>();
  if (!outerNulls && !dictionary->mayHaveNulls()) {
    for (vector_size_t row = 0; row < numRows; ++row) {
      referenced.setValid(indices[row], true);
    }
  } else {
    for (vector_size_t row = 0; row < numRows; ++row) {
      if ((!outerNulls || !bits::isBitNull(outerNulls, row)) &&
          !dictionary->isNullAt(row)) {
        referenced.setValid(indices[row], true);
      }
    }
  }
  referenced.updateBounds();
  auto sanitized =
      buildSanitizedFlat(base, base->size(), nullptr, pool, &referenced);
  if (!sanitized) {
    return VectorPtr{};
  }
  return BaseVector::wrapInDictionary(
      child->nulls(), dictionary->indices(), numRows, std::move(sanitized));
}

} // namespace

RowVectorPtr replaceInvalidUtf8InTopLevelVarchars(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  std::optional<std::vector<VectorPtr>> children;
  const auto numRows = input->size();
  const auto& outerNulls = input->nulls();
  const uint64_t* outerRaw = outerNulls ? outerNulls->as<uint64_t>() : nullptr;

  for (auto c = 0; c < input->childrenSize(); ++c) {
    const auto& source = input->childAt(c);
    if (!source || source->typeKind() != TypeKind::VARCHAR) {
      continue;
    }
    // LazyVector is not a SimpleVector; inspect its loaded values instead.
    const auto& child =
        source->isLazy() ? BaseVector::loadedVectorShared(source) : source;
    if (child->encoding() == VectorEncoding::Simple::CONSTANT) {
      continue;
    }
    const auto* simple = child->asUnchecked<SimpleVector<StringView>>();
    if (simple->getAllIsAscii() && simple->isAscii().value_or(false)) {
      continue;
    }

    VectorPtr replacement;
    auto dictionaryReplacement =
        buildSanitizedDictionary(child, numRows, outerRaw, pool);
    if (dictionaryReplacement.has_value()) {
      replacement = std::move(dictionaryReplacement.value());
    } else {
      replacement = buildSanitizedFlat(child, numRows, outerRaw, pool);
    }
    if (!replacement) {
      continue;
    }
    if (!children.has_value()) {
      children = input->children();
    }
    children.value()[c] = std::move(replacement);
  }

  if (!children.has_value()) {
    return input;
  }
  return std::make_shared<RowVector>(
      pool,
      input->type(),
      input->nulls(),
      numRows,
      std::move(children.value()),
      input->getNullCount());
}

} // namespace bytedance::bolt::utf8
