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

// Fast validator for the common text shape of ordinary 3-byte code points
// followed by an ASCII suffix. Other valid UTF-8 shapes fall back to scanUtf8.
FOLLY_NOINLINE bool isValidThreeBytePrefixWithAsciiTail(
    const char* data,
    int32_t size) {
  const auto threeByteSize = regularThreeBytePrefixLengthSimd(
      reinterpret_cast<const uint8_t*>(data), size);
  if (threeByteSize == 0) {
    return false;
  }
  return threeByteSize +
      asciiPrefixLength(data + threeByteSize, size - threeByteSize) ==
      size;
}

struct MalformedRun {
  vector_size_t row;
  int32_t offset;
  int32_t size;
  int32_t replacements;
};

using MalformedRuns =
    std::vector<MalformedRun, memory::StlAllocator<MalformedRun>>;

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

FOLLY_ALWAYS_INLINE void appendMalformedRun(
    vector_size_t row,
    int32_t offset,
    int32_t size,
    int32_t replacements,
    MalformedRuns& malformed) {
  if (!malformed.empty()) {
    auto& previous = malformed.back();
    if (previous.row == row && previous.offset + previous.size == offset) {
      previous.size += size;
      previous.replacements += replacements;
      return;
    }
  }
  malformed.push_back({row, offset, size, replacements});
}

// Scans forward without a separate validation pass. ASCII runs use xsimd and
// ordinary 3-byte runs stay in a tight loop; other high-bit bytes enter the
// OpenJDK-compatible UTF-8 state machine. Adjacent malformed units are
// coalesced so dense invalid input uses one run per row instead of one record
// per byte.
int32_t scanUtf8(
    vector_size_t row,
    const char* data,
    int32_t size,
    MalformedRuns& malformed) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  int32_t offset = 0;
  int64_t outputSize = 0;
  while (offset < size) {
    if (bytes[offset] <= 0x7F) {
      const auto asciiSize = asciiPrefixLength(data + offset, size - offset);
      offset += asciiSize;
      outputSize += asciiSize;
      continue;
    }

    const auto malformedPrefix =
        singleByteMalformedPrefixLength(bytes + offset, size - offset);
    if (malformedPrefix > 0) {
      const int64_t replacementBytes =
          static_cast<int64_t>(malformedPrefix) * kReplacementSize;
      appendMalformedRun(
          row, offset, malformedPrefix, malformedPrefix, malformed);
      offset += malformedPrefix;
      outputSize += replacementBytes;
      continue;
    }

    const auto validPrefix =
        regularThreeBytePrefixLength(bytes + offset, size - offset);
    if (validPrefix > 0) {
      offset += validPrefix;
      outputSize += validPrefix;
      continue;
    }

    const auto [validSize, malformedSize] =
        nextSegment(bytes + offset, size - offset);
    if (malformedSize > 0) {
      appendMalformedRun(row, offset, malformedSize, 1, malformed);
      offset += malformedSize;
      outputSize += kReplacementSize;
    } else {
      offset += validSize;
      outputSize += validSize;
    }
  }

  BOLT_USER_CHECK_LE(
      outputSize,
      kMaxStringSize,
      "UTF-8 replacement result exceeds the maximum VARCHAR size");
  return static_cast<int32_t>(outputSize);
}

FOLLY_ALWAYS_INLINE void writeReplacementRun(
    char* output,
    int32_t replacements) {
  BOLT_DCHECK_GT(replacements, 0);
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

VectorPtr buildReplacementOnlyVector(
    const MalformedRuns& malformed,
    vector_size_t numRows,
    bool uniformReplacementCount,
    size_t totalStringBytes,
    uint64_t maxStringLength,
    memory::MemoryPool* pool) {
  const auto maxOutputSize = static_cast<int32_t>(maxStringLength);
  auto newValues = AlignedBuffer::allocate<StringView>(numRows, pool);
  auto* outputValues = newValues->asMutable<StringView>();

  BufferPtr newStrings;
  const char* replacementData;
  if (StringView::isInline(maxOutputSize)) {
    replacementData = kReplacementBlock.data();
  } else {
    newStrings = AlignedBuffer::allocate<char>(maxOutputSize, pool);
    newStrings->setSize(maxOutputSize);
    auto* mutableReplacementData = newStrings->asMutable<char>();
    writeReplacementRun(
        mutableReplacementData, maxOutputSize / kReplacementSize);
    replacementData = mutableReplacementData;
  }

  if (uniformReplacementCount) {
    std::fill_n(
        outputValues,
        numRows,
        StringView(
            replacementData,
            malformed.front().replacements * kReplacementSize));
  } else {
    for (vector_size_t row = 0; row < numRows; ++row) {
      outputValues[row] = StringView(
          replacementData, malformed[row].replacements * kReplacementSize);
    }
  }

  std::vector<BufferPtr> stringBuffers;
  if (newStrings) {
    stringBuffers.push_back(std::move(newStrings));
  }
  auto result = std::make_shared<FlatVector<StringView>>(
      pool,
      VARCHAR(),
      nullptr,
      numRows,
      std::move(newValues),
      std::move(stringBuffers),
      SimpleVectorStats<StringView>{},
      std::nullopt,
      0);
  result->setStringViewStats(
      StringViewStats{totalStringBytes, maxStringLength});
  return result;
}

// Constructs output using runs produced by scanUtf8. This performs memcpy and
// replacement only; it does not inspect or decode UTF-8 bytes.
FOLLY_ALWAYS_INLINE void writeFromRuns(
    const char* data,
    int32_t size,
    const MalformedRuns& malformed,
    size_t begin,
    size_t end,
    char* output) {
  int32_t inputOffset = 0;
  int32_t outputOffset = 0;
  for (auto index = begin; index < end; ++index) {
    const auto& run = malformed[index];
    BOLT_DCHECK_GE(run.offset, inputOffset);
    BOLT_DCHECK_LE(run.offset + run.size, size);
    const auto validSize = run.offset - inputOffset;
    if (validSize > 0) {
      std::memcpy(output + outputOffset, data + inputOffset, validSize);
      outputOffset += validSize;
    }
    writeReplacementRun(output + outputOffset, run.replacements);
    outputOffset += run.replacements * kReplacementSize;
    inputOffset = run.offset + run.size;
  }
  if (inputOffset < size) {
    std::memcpy(output + outputOffset, data + inputOffset, size - inputOffset);
  }
}

struct RowStr {
  const char* data;
  int32_t size;
};

// Unified reader across FLAT / CONSTANT / DICTIONARY / SEQUENCE wrappers.
// BaseVector::wrappedVector() peels through any nesting of wrappers to the
// underlying scalar storage; wrappedIndex() translates an outer row index
// through dict indirection, sequence offsets, and constant mappings into a
// position in the peeled base's raw values array. The peeled base for a
// VARCHAR is either a FLAT<StringView> or a CONSTANT<StringView> holding a
// raw value (valueVector_ = nullptr).
struct PeeledVarchar {
  const BaseVector* outer; // original (wrapped) vector, for isNullAt
  const StringView* values; // rawValues on peeled FLAT, or &value_ on CONSTANT
};

inline PeeledVarchar peelVarchar(const BaseVector& v) {
  // One wrappedVector() call recurses through all DICT / SEQUENCE /
  // CONSTANT(valueVector_) / LAZY wrappers to the underlying scalar storage,
  // which for VARCHAR must be a FLAT<StringView> or a "bare"
  // CONSTANT<StringView> (valueVector_ == nullptr).
  const BaseVector* base = v.wrappedVector();
  const StringView* values;
  if (base->encoding() == VectorEncoding::Simple::CONSTANT) {
    values = base->asUnchecked<ConstantVector<StringView>>()->rawValues();
  } else {
    BOLT_CHECK_EQ(
        base->encoding(),
        VectorEncoding::Simple::FLAT,
        "replaceInvalidUtf8InTopLevelVarchars: unexpected VARCHAR encoding {}",
        static_cast<int>(base->encoding()));
    values = base->asUnchecked<FlatVector<StringView>>()->rawValues();
  }
  return {&v, values};
}

inline RowStr
readPeeled(const PeeledVarchar& pv, vector_size_t row, bool& isNull) {
  isNull = pv.outer->isNullAt(row);
  if (isNull)
    return {nullptr, 0};
  const vector_size_t idx = pv.outer->wrappedIndex(row);
  const StringView& sv = pv.values[idx];
  return {sv.data(), static_cast<int32_t>(sv.size())};
}

// Single-function FLAT builder.
// The probe pass decodes every byte at most once and records coalesced
// malformed runs. Returns nullptr without allocating output when no
// replacement is needed. The construction pass uses these runs to copy into
// one exactly-sized, contiguous buffer without decoding UTF-8 again.
//
// A non-virtual FLAT fast path covers the common case (plain FLAT child, no
// outer nulls) used by Hive/Parquet writer for top-level varchar columns.
VectorPtr buildSanitizedFlat(
    const VectorPtr& child,
    vector_size_t numRows,
    const uint64_t* outerNulls,
    memory::MemoryPool* pool) {
  const bool plainFlatNoNulls = !outerNulls &&
      child->encoding() == VectorEncoding::Simple::FLAT &&
      !child->mayHaveNulls();

  vector_size_t nullCount = 0;
  size_t totalStringBytes = 0;
  size_t changedStringBytes = 0;
  uint64_t maxStringLength = 0;
  MalformedRuns malformed{memory::StlAllocator<MalformedRun>(pool)};
  constexpr vector_size_t kSampleRows = 8;
  constexpr vector_size_t kMaxSampledRunReserve = 16 * 1024;

  auto recordString = [&](int32_t outputSize, bool changed) {
    maxStringLength = std::max<uint64_t>(maxStringLength, outputSize);
    if (!StringView::isInline(outputSize)) {
      totalStringBytes += static_cast<size_t>(outputSize);
      if (changed) {
        changedStringBytes += static_cast<size_t>(outputSize);
      }
    }
  };

  auto probeScalar = [&](vector_size_t row, const char* data, int32_t size) {
    const auto malformedCount = malformed.size();
    const auto outputSize = scanUtf8(row, data, size, malformed);
    const bool changed = malformed.size() != malformedCount;
    recordString(outputSize, changed);
    return changed;
  };

  auto probeDenseLead = [&](vector_size_t row, const char* data, int32_t size) {
    if (size == 0) {
      probeScalar(row, data, size);
      return false;
    }
    const auto malformedSize = denseLeadMalformedPrefixLength(
        reinterpret_cast<const uint8_t*>(data), size);
    if (malformedSize != size) {
      probeScalar(row, data, size);
      return false;
    }
    BOLT_USER_CHECK_LE(
        size,
        kMaxStringSize / kReplacementSize,
        "UTF-8 replacement result exceeds the maximum VARCHAR size");
    malformed.push_back({row, 0, size, size});
    recordString(size * kReplacementSize, true);
    return true;
  };

  auto probeThreeByte = [&](vector_size_t row, const char* data, int32_t size) {
    if (isValidThreeBytePrefixWithAsciiTail(data, size)) {
      recordString(size, false);
      return true;
    }
    probeScalar(row, data, size);
    return false;
  };

  if (plainFlatNoNulls) {
    const auto* flat = child->asUnchecked<FlatVector<StringView>>();
    const StringView* __restrict__ svs = flat->rawValues();
    const auto sampleRows = std::min(numRows, kSampleRows);
    bool useThreeByteFastValidator = numRows > kSampleRows;
    bool useDenseLeadFastScanner = numRows > kSampleRows;
    bool reserveOneRunPerRow = numRows > kSampleRows;
    vector_size_t row = 0;
    for (; row < sampleRows; ++row) {
      const StringView& sv = svs[row];
      const auto* data = sv.data();
      const auto size = static_cast<int32_t>(sv.size());
      const auto malformedCount = malformed.size();
      const bool changed = probeScalar(row, data, size);
      if (changed && malformed.capacity() < sampleRows) {
        malformed.reserve(sampleRows);
      }
      reserveOneRunPerRow &= changed && malformed.size() == malformedCount + 1;
      if (useThreeByteFastValidator &&
          (changed || !isValidThreeBytePrefixWithAsciiTail(data, size))) {
        useThreeByteFastValidator = false;
      }
      if (useDenseLeadFastScanner &&
          (!changed || malformed.size() != malformedCount + 1 ||
           malformed.back().offset != 0 || malformed.back().size != size ||
           malformed.back().replacements != size)) {
        useDenseLeadFastScanner = false;
      }
    }
    if (reserveOneRunPerRow) {
      // Bound speculative capacity for batches whose first rows are not
      // representative. This still covers common writer batch sizes.
      malformed.reserve(std::min(numRows, kMaxSampledRunReserve));
    }
    if (useDenseLeadFastScanner) {
      for (; row < numRows; ++row) {
        const StringView& sv = svs[row];
        if (!probeDenseLead(row, sv.data(), static_cast<int32_t>(sv.size()))) {
          ++row;
          break;
        }
      }
    }
    if (useThreeByteFastValidator) {
      for (; row < numRows; ++row) {
        const StringView& sv = svs[row];
        if (!probeThreeByte(row, sv.data(), static_cast<int32_t>(sv.size()))) {
          ++row;
          break;
        }
      }
    }
    for (; row < numRows; ++row) {
      const StringView& sv = svs[row];
      probeScalar(row, sv.data(), static_cast<int32_t>(sv.size()));
    }
  } else {
    PeeledVarchar pv = peelVarchar(*child);
    for (vector_size_t row = 0; row < numRows; ++row) {
      if (outerNulls && bits::isBitNull(outerNulls, row)) {
        ++nullCount;
        continue;
      }
      bool isN = false;
      const auto rs = readPeeled(pv, row, isN);
      if (isN) {
        ++nullCount;
        continue;
      }
      probeScalar(row, rs.data, rs.size);
    }
  }

  if (malformed.empty()) {
    return nullptr;
  }

  // A fully malformed row materializes as a prefix of the same repeated
  // replacement sequence. When every row has this shape, build the sequence
  // once instead of allocating and filling one copy per row.
  if (plainFlatNoNulls && malformed.size() == numRows) {
    const auto* sourceValues =
        child->asUnchecked<FlatVector<StringView>>()->rawValues();
    bool allReplacementOnly = true;
    bool uniformReplacementCount = true;
    const auto firstReplacementCount = malformed.front().replacements;
    for (vector_size_t row = 0; row < numRows; ++row) {
      const auto& run = malformed[row];
      if (run.row != row || run.offset != 0 ||
          run.size != sourceValues[row].size()) {
        allReplacementOnly = false;
        break;
      }
      uniformReplacementCount &= run.replacements == firstReplacementCount;
    }

    if (allReplacementOnly) {
      return buildReplacementOnlyVector(
          malformed,
          numRows,
          uniformReplacementCount,
          totalStringBytes,
          maxStringLength,
          pool);
    }
  }

  // Retaining the source buffers avoids copying unchanged strings, but can
  // increase peak memory when most values changed. Share only when doing so
  // reduces the newly allocated non-inline bytes by at least half.
  const bool shareUnchangedStrings =
      totalStringBytes > 0 && changedStringBytes <= totalStringBytes / 2;
  const auto allocatedStringBytes =
      shareUnchangedStrings ? changedStringBytes : totalStringBytes;

  const bool hasAnyNull = (nullCount > 0);
  BufferPtr newNulls;
  uint64_t* dstNulls = nullptr;
  if (hasAnyNull) {
    const auto nBytes = bits::nbytes(numRows);
    newNulls = AlignedBuffer::allocate<char>(nBytes, pool);
    dstNulls = newNulls->asMutable<uint64_t>();
    std::memset(dstNulls, bits::kNotNullByte, newNulls->capacity());
  }

  BufferPtr newValues = AlignedBuffer::allocate<StringView>(numRows, pool);
  auto* rnv = newValues->asMutable<StringView>();

  BufferPtr newStrings;
  char* cur = nullptr;
  if (allocatedStringBytes > 0) {
    newStrings = AlignedBuffer::allocate<char>(allocatedStringBytes, pool);
    newStrings->setSize(allocatedStringBytes);
    cur = newStrings->asMutable<char>();
  }

  size_t malformedIndex = 0;
  auto writeString = [&](vector_size_t row, const char* data, int32_t size) {
    const auto begin = malformedIndex;
    int64_t outputSize = size;
    while (malformedIndex < malformed.size() &&
           malformed[malformedIndex].row == row) {
      outputSize += malformed[malformedIndex].replacements * kReplacementSize -
          malformed[malformedIndex].size;
      ++malformedIndex;
    }
    BOLT_DCHECK_LE(outputSize, kMaxStringSize);
    const auto outputSize32 = static_cast<int32_t>(outputSize);

    if (StringView::isInline(outputSize32)) {
      if (begin == malformedIndex) {
        rnv[row] = StringView(data, size);
      } else {
        char inlineData[StringView::kInlineSize];
        writeFromRuns(data, size, malformed, begin, malformedIndex, inlineData);
        rnv[row] = StringView(inlineData, outputSize32);
      }
      return;
    }

    if (shareUnchangedStrings && begin == malformedIndex) {
      rnv[row] = StringView(data, size);
      return;
    }

    char* output = cur;
    if (begin == malformedIndex) {
      std::memcpy(output, data, size);
    } else {
      writeFromRuns(data, size, malformed, begin, malformedIndex, output);
    }
    cur += outputSize32;
    rnv[row] = StringView(output, outputSize32);
  };

  if (plainFlatNoNulls) {
    const auto* flat = child->asUnchecked<FlatVector<StringView>>();
    const StringView* __restrict__ svs = flat->rawValues();
    for (vector_size_t row = 0; row < numRows; ++row) {
      const StringView& sv = svs[row];
      writeString(row, sv.data(), static_cast<int32_t>(sv.size()));
    }
  } else {
    PeeledVarchar pv = peelVarchar(*child);
    for (vector_size_t row = 0; row < numRows; ++row) {
      if (outerNulls && bits::isBitNull(outerNulls, row)) {
        if (dstNulls) {
          bits::setNull(dstNulls, row);
        }
        rnv[row] = StringView();
        continue;
      }
      bool isN = false;
      const auto rs = readPeeled(pv, row, isN);
      if (isN) {
        if (dstNulls) {
          bits::setNull(dstNulls, row);
        }
        rnv[row] = StringView();
        continue;
      }
      if (dstNulls) {
        bits::clearNull(dstNulls, row);
      }
      writeString(row, rs.data, rs.size);
    }
  }

  BOLT_DCHECK_EQ(malformedIndex, malformed.size());
  BOLT_DCHECK_EQ(
      cur,
      newStrings ? newStrings->asMutable<char>() + allocatedStringBytes
                 : nullptr);

  std::vector<BufferPtr> sbs;
  if (newStrings) {
    sbs.push_back(std::move(newStrings));
  }
  auto result = std::make_shared<FlatVector<StringView>>(
      pool,
      VARCHAR(),
      std::move(newNulls),
      numRows,
      std::move(newValues),
      std::move(sbs),
      SimpleVectorStats<StringView>{},
      std::nullopt,
      nullCount);
  result->setStringViewStats(
      StringViewStats{totalStringBytes, maxStringLength});
  if (shareUnchangedStrings) {
    result->acquireSharedStringBuffers(child.get());
  }
  return result;
}

// Sanitizes a single-layer DICTIONARY over a FLAT VARCHAR without decoding
// the same base value once per logical row. Only referenced, logically
// non-null base rows are scanned. When replacement is needed, the original
// indices and nulls are retained and unchanged StringViews keep sharing the
// source FlatVector's string buffers.
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
  if (base->encoding() != VectorEncoding::Simple::FLAT) {
    return std::nullopt;
  }

  const auto baseSize = base->size();
  if (baseSize == 0 || numRows == 0) {
    return VectorPtr{};
  }
  // Preserving the dictionary requires a bitmap and, when replacement is
  // needed, a StringView array sized to the full base. Fall back to the
  // logical-row path unless the base is known to be substantially reused.
  if (baseSize > numRows / 2) {
    return std::nullopt;
  }

  SelectivityVector referenced(baseSize, false);
  const auto* rawIndices = dictionary->indices()->as<vector_size_t>();
  for (vector_size_t row = 0; row < numRows; ++row) {
    if ((outerNulls && bits::isBitNull(outerNulls, row)) ||
        dictionary->isNullAt(row)) {
      continue;
    }
    referenced.setValid(rawIndices[row], true);
  }
  referenced.updateBounds();
  if (!referenced.hasSelections()) {
    return VectorPtr{};
  }

  const auto* flat = base->asUnchecked<FlatVector<StringView>>();
  const auto* sourceValues = flat->rawValues();
  MalformedRuns malformed{memory::StlAllocator<MalformedRun>(pool)};
  size_t changedStringBytes = 0;
  referenced.applyToSelected([&](vector_size_t baseRow) {
    const auto& value = sourceValues[baseRow];
    const auto begin = malformed.size();
    const auto outputSize = scanUtf8(
        baseRow, value.data(), static_cast<int32_t>(value.size()), malformed);
    if (malformed.size() != begin && !StringView::isInline(outputSize)) {
      changedStringBytes += static_cast<size_t>(outputSize);
    }
  });

  if (malformed.empty()) {
    return VectorPtr{};
  }

  auto newValues = AlignedBuffer::allocate<StringView>(baseSize, pool);
  auto* outputValues = newValues->asMutable<StringView>();
  std::memcpy(outputValues, sourceValues, baseSize * sizeof(StringView));

  BufferPtr newStrings;
  char* currentString = nullptr;
  if (changedStringBytes > 0) {
    newStrings = AlignedBuffer::allocate<char>(changedStringBytes, pool);
    newStrings->setSize(changedStringBytes);
    currentString = newStrings->asMutable<char>();
  }

  size_t malformedIndex = 0;
  while (malformedIndex < malformed.size()) {
    const auto baseRow = malformed[malformedIndex].row;
    const auto& source = sourceValues[baseRow];
    const auto begin = malformedIndex;
    int64_t outputSize = source.size();
    while (malformedIndex < malformed.size() &&
           malformed[malformedIndex].row == baseRow) {
      outputSize += malformed[malformedIndex].replacements * kReplacementSize -
          malformed[malformedIndex].size;
      ++malformedIndex;
    }
    BOLT_DCHECK_LE(outputSize, kMaxStringSize);
    const auto outputSize32 = static_cast<int32_t>(outputSize);
    if (StringView::isInline(outputSize32)) {
      char inlineData[StringView::kInlineSize];
      writeFromRuns(
          source.data(),
          source.size(),
          malformed,
          begin,
          malformedIndex,
          inlineData);
      outputValues[baseRow] = StringView(inlineData, outputSize32);
    } else {
      writeFromRuns(
          source.data(),
          source.size(),
          malformed,
          begin,
          malformedIndex,
          currentString);
      outputValues[baseRow] = StringView(currentString, outputSize32);
      currentString += outputSize32;
    }
  }

  BOLT_DCHECK_EQ(
      currentString,
      newStrings ? newStrings->asMutable<char>() + changedStringBytes
                 : nullptr);
  std::vector<BufferPtr> stringBuffers;
  if (newStrings) {
    stringBuffers.push_back(std::move(newStrings));
  }
  auto sanitizedBase = std::make_shared<FlatVector<StringView>>(
      pool,
      base->type(),
      base->nulls(),
      baseSize,
      std::move(newValues),
      std::move(stringBuffers),
      SimpleVectorStats<StringView>{},
      std::nullopt,
      base->getNullCount());
  sanitizedBase->acquireSharedStringBuffers(base.get());

  return BaseVector::wrapInDictionary(
      child->nulls(), dictionary->indices(), numRows, std::move(sanitizedBase));
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
    const auto& child = input->childAt(c);
    if (!child || child->typeKind() != TypeKind::VARCHAR) {
      continue;
    }
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
