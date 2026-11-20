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

#pragma once

#include "bolt/exec/HashTableSimd.h"

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace bytedance::bolt::exec::hash_table_simd {

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
#endif
inline FOLLY_NOINLINE bool
scanBucketLowLanes(
    uint64_t bucket,
    uint64_t tag,
    char* const* __restrict__ table,
    uint64_t laneEnd,
    char*& taggedPointer,
    uint64_t& slotOut) {
#if defined(__x86_64__) || defined(__i386__)
  const auto bucketVec0 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(table + bucket));
  const auto bucketVec1 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(table + bucket + 4));
  const auto tagMask = _mm256_set1_epi64x(static_cast<int64_t>(kTagMask));
  const auto wantedTag = _mm256_set1_epi64x(static_cast<int64_t>(tag));
  const auto tagVec0 = _mm256_and_si256(bucketVec0, tagMask);
  const auto tagVec1 = _mm256_and_si256(bucketVec1, tagMask);
  const auto tagMatches0 = _mm256_cmpeq_epi64(tagVec0, wantedTag);
  const auto tagMatches1 = _mm256_cmpeq_epi64(tagVec1, wantedTag);
  const auto zero = _mm256_setzero_si256();
  const auto empties0 = _mm256_cmpeq_epi64(bucketVec0, zero);
  const auto empties1 = _mm256_cmpeq_epi64(bucketVec1, zero);
  const auto stopVec0 = _mm256_or_si256(tagMatches0, empties0);
  const auto stopVec1 = _mm256_or_si256(tagMatches1, empties1);
  const uint32_t stopMask = _mm256_movemask_pd(_mm256_castsi256_pd(stopVec0)) |
      (_mm256_movemask_pd(_mm256_castsi256_pd(stopVec1)) << 4);
  const uint32_t cursorMask = (static_cast<uint32_t>(1) << laneEnd) - 1;
  const uint32_t stops = stopMask & cursorMask;
#else
  uint32_t stops = 0;
  for (uint64_t lane = 0; lane < laneEnd; ++lane) {
    const uint64_t slot = reinterpret_cast<uint64_t>(table[bucket + lane]);
    stops |= static_cast<uint32_t>((slot == 0) | ((slot & kTagMask) == tag))
        << lane;
  }
#endif
  if (LIKELY(stops != 0)) {
    const uint64_t lane = __builtin_ctz(stops);
    slotOut = bucket + lane;
    taggedPointer = table[slotOut];
    return true;
  }
  return false;
}

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
inline void
advanceSlots(uint64_t* __restrict__ activeSlots, int numSlots, uint64_t mask) {
  const auto vOne = _mm256_set1_epi64x(1);
  const auto vMask = _mm256_set1_epi64x(mask);
  int i = 0;
  for (; i + 4 <= numSlots; i += 4) {
    auto v =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(activeSlots + i));
    v = _mm256_and_si256(_mm256_add_epi64(v, vOne), vMask);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(activeSlots + i), v);
  }
  for (; i < numSlots; ++i) {
    activeSlots[i] = (activeSlots[i] + 1) & mask;
  }
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
inline int
compactNormalizedKeyMismatches(
    int numActive,
    const int32_t* activeRowsIn,
    const uint64_t* activeSlotsIn,
    char* const* __restrict__ hits,
    int32_t* activeRowsOut,
    uint64_t* activeSlotsOut,
    uint64_t mask) {
  int numNextActive = 0;
  int a = 0;
  for (; a + 8 <= numActive; a += 8) {
    uint32_t mismatchBits = 0;
    for (int j = 0; j < 8; ++j) {
      mismatchBits |=
          static_cast<uint32_t>(hits[activeRowsIn[a + j]] == nullptr) << j;
    }
    while (mismatchBits) {
      const int pos = __builtin_ctz(mismatchBits);
      activeSlotsOut[numNextActive] = activeSlotsIn[a + pos];
      activeRowsOut[numNextActive] = activeRowsIn[a + pos];
      ++numNextActive;
      mismatchBits &= mismatchBits - 1;
    }
  }
  for (; a < numActive; ++a) {
    if (hits[activeRowsIn[a]] == nullptr) {
      activeSlotsOut[numNextActive] = activeSlotsIn[a];
      activeRowsOut[numNextActive] = activeRowsIn[a];
      ++numNextActive;
    }
  }
  advanceSlots(activeSlotsOut, numNextActive, mask);
  return numNextActive;
}
#else
inline void
advanceSlots(uint64_t* __restrict__ activeSlots, int numSlots, uint64_t mask) {
  for (int i = 0; i < numSlots; ++i) {
    activeSlots[i] = (activeSlots[i] + 1) & mask;
  }
}

inline int compactNormalizedKeyMismatches(
    int numActive,
    const int32_t* activeRowsIn,
    const uint64_t* activeSlotsIn,
    char* const* __restrict__ hits,
    int32_t* activeRowsOut,
    uint64_t* activeSlotsOut,
    uint64_t mask) {
  int numNextActive = 0;
  for (int a = 0; a < numActive; ++a) {
    if (hits[activeRowsIn[a]] == nullptr) {
      activeSlotsOut[numNextActive] = activeSlotsIn[a];
      activeRowsOut[numNextActive] = activeRowsIn[a];
      ++numNextActive;
    }
  }
  advanceSlots(activeSlotsOut, numNextActive, mask);
  return numNextActive;
}
#endif // x86

// hits[activeRows[a]] must be non-null because the assembly dereferences it.
// Bucket walks discard empty slots before calling this function.
template <int32_t kKeyOffset>
FOLLY_ALWAYS_INLINE void compareNormalizedKeys(
    int numActive,
    const int32_t* __restrict__ activeRows,
    const uint64_t* __restrict__ keys,
    char** __restrict__ hits) {
  for (int a = 0; a < numActive; ++a) {
    const int32_t row = activeRows[a];
    char* group = hits[row];
    BOLT_DCHECK_NOT_NULL(group, "compareNormalizedKeys: hits[{}] is null", row);
    const uint64_t expectedNormalizedKey = keys[row];
#if defined(__x86_64__) || defined(__i386__)
    asm volatile(
        "movq %c[ko](%[p]), %%rax\n\t"
        "cmpq %%rax, %[wk]\n\t"
        "cmovneq %[z], %[p]\n\t"
        : [p] "+&r"(group)
        : [ko] "i"(kKeyOffset),
          [wk] "r"(expectedNormalizedKey),
          [z] "r"(static_cast<char*>(nullptr))
        : "rax", "cc", "memory");
#else
    uint64_t storedNormalizedKey;
    std::memcpy(
        &storedNormalizedKey, group + kKeyOffset, sizeof(storedNormalizedKey));
    if (storedNormalizedKey != expectedNormalizedKey) {
      group = nullptr;
    }
#endif
    hits[row] = group;
  }
}

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
#endif
FOLLY_ALWAYS_INLINE void
scanBucketChain(
    uint64_t cursor,
    uint64_t mask,
    uint64_t tag,
    char* const* __restrict__ table,
    char*& taggedPointer,
    uint64_t& slotOut) {
  uint64_t bucket = bucketStart(cursor);
  const uint64_t firstBucket = bucket;
  uint64_t startLane = cursor & (kBucketSize - 1);
  const uint64_t originalStartLane = startLane;
  do {
#if defined(__x86_64__) || defined(__i386__)
    // Keep both half-bucket loads unconditional: branchy variants that skipped
    // one load on partial-bucket scans regressed in fixed-core E2E benchmarks.
    const auto bucketVec0 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(table + bucket));
    const auto bucketVec1 = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(table + bucket + 4));
    const auto tagMask = _mm256_set1_epi64x(static_cast<int64_t>(kTagMask));
    const auto wantedTag = _mm256_set1_epi64x(static_cast<int64_t>(tag));
    const auto tagVec0 = _mm256_and_si256(bucketVec0, tagMask);
    const auto tagVec1 = _mm256_and_si256(bucketVec1, tagMask);
    const auto tagMatches0 = _mm256_cmpeq_epi64(tagVec0, wantedTag);
    const auto tagMatches1 = _mm256_cmpeq_epi64(tagVec1, wantedTag);
    const auto zero = _mm256_setzero_si256();
    const auto empties0 = _mm256_cmpeq_epi64(bucketVec0, zero);
    const auto empties1 = _mm256_cmpeq_epi64(bucketVec1, zero);
    const auto stopVec0 = _mm256_or_si256(tagMatches0, empties0);
    const auto stopVec1 = _mm256_or_si256(tagMatches1, empties1);
    const uint32_t stopMask =
        _mm256_movemask_pd(_mm256_castsi256_pd(stopVec0)) |
        (_mm256_movemask_pd(_mm256_castsi256_pd(stopVec1)) << 4);
    const uint32_t cursorMask =
        (static_cast<uint32_t>(0xFF) << startLane) & 0xFF;
    const uint32_t stops = stopMask & cursorMask;
#else
    uint32_t stops = 0;
    for (uint64_t lane = startLane; lane < kBucketSize; ++lane) {
      const uint64_t slot = reinterpret_cast<uint64_t>(table[bucket + lane]);
      stops |= static_cast<uint32_t>((slot == 0) | ((slot & kTagMask) == tag))
          << lane;
    }
#endif
    if (LIKELY(stops != 0)) {
      const uint64_t lane = __builtin_ctz(stops);
      slotOut = bucket + lane;
      taggedPointer = table[slotOut];
      return;
    }
    bucket = (bucket + kBucketSize) & mask;
    startLane = 0;
  } while (LIKELY(bucket != firstBucket));

  if (UNLIKELY(
          originalStartLane != 0 &&
          scanBucketLowLanes(
              bucket, tag, table, originalStartLane, taggedPointer, slotOut))) {
    return;
  }

  BOLT_FAIL("SIMD bucket walk wrapped without finding a stop slot");
}

#if defined(__x86_64__) || defined(__i386__)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
#endif
FOLLY_ALWAYS_INLINE bool
scanBucketChainInRange(
    uint64_t cursor,
    uint64_t mask,
    uint64_t tag,
    char* const* __restrict__ table,
    uint64_t rangeStart,
    uint64_t rangeEnd,
    char*& taggedPointer,
    uint64_t& slotOut) {
  uint64_t bucket = bucketStart(cursor);
  const uint64_t firstBucket = bucket;
  uint64_t startLane = cursor & (kBucketSize - 1);
  const uint64_t originalStartLane = startLane;
  do {
    if (UNLIKELY(bucket < rangeStart || bucket >= rangeEnd)) {
      taggedPointer = nullptr;
      slotOut = rangeEnd;
      return false;
    }
#if defined(__x86_64__) || defined(__i386__)
    const auto bucketVec0 =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(table + bucket));
    const auto bucketVec1 = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(table + bucket + 4));
    const auto tagMask = _mm256_set1_epi64x(static_cast<int64_t>(kTagMask));
    const auto wantedTag = _mm256_set1_epi64x(static_cast<int64_t>(tag));
    const auto tagVec0 = _mm256_and_si256(bucketVec0, tagMask);
    const auto tagVec1 = _mm256_and_si256(bucketVec1, tagMask);
    const auto tagMatches0 = _mm256_cmpeq_epi64(tagVec0, wantedTag);
    const auto tagMatches1 = _mm256_cmpeq_epi64(tagVec1, wantedTag);
    const auto zero = _mm256_setzero_si256();
    const auto empties0 = _mm256_cmpeq_epi64(bucketVec0, zero);
    const auto empties1 = _mm256_cmpeq_epi64(bucketVec1, zero);
    const auto stopVec0 = _mm256_or_si256(tagMatches0, empties0);
    const auto stopVec1 = _mm256_or_si256(tagMatches1, empties1);
    const uint32_t stopMask =
        _mm256_movemask_pd(_mm256_castsi256_pd(stopVec0)) |
        (_mm256_movemask_pd(_mm256_castsi256_pd(stopVec1)) << 4);
    const uint32_t cursorMask =
        (static_cast<uint32_t>(0xFF) << startLane) & 0xFF;
    const uint32_t stops = stopMask & cursorMask;
#else
    uint32_t stops = 0;
    for (uint64_t lane = startLane; lane < kBucketSize; ++lane) {
      const uint64_t slot = reinterpret_cast<uint64_t>(table[bucket + lane]);
      stops |= static_cast<uint32_t>((slot == 0) | ((slot & kTagMask) == tag))
          << lane;
    }
#endif
    if (LIKELY(stops != 0)) {
      const uint64_t lane = __builtin_ctz(stops);
      slotOut = bucket + lane;
      taggedPointer = table[slotOut];
      return true;
    }
    bucket = (bucket + kBucketSize) & mask;
    startLane = 0;
  } while (LIKELY(bucket != firstBucket));

  if (UNLIKELY(originalStartLane != 0)) {
    bucket = firstBucket;
    if (UNLIKELY(bucket < rangeStart || bucket >= rangeEnd)) {
      taggedPointer = nullptr;
      slotOut = rangeEnd;
      return false;
    }
    if (scanBucketLowLanes(
            bucket, tag, table, originalStartLane, taggedPointer, slotOut)) {
      return true;
    }
  }

  taggedPointer = nullptr;
  slotOut = rangeEnd;
  return false;
}

template <bool kEmitInsert>
FOLLY_ALWAYS_INLINE int walkAndSplitInPlace(
    int numActive,
    uint64_t mask,
    uint64_t* __restrict__ activeSlots,
    int32_t* __restrict__ activeRows,
    const uint64_t* __restrict__ hashes,
    char* const* __restrict__ table,
    char** __restrict__ hits,
    int32_t* __restrict__ insertRows,
    uint64_t* __restrict__ insertSlots,
    int* numInsertsOut) {
  constexpr int kPrefetchDistance = kProbePrefetchDistance;
  for (int i = 0; i < kPrefetchDistance && i < numActive; ++i) {
    __builtin_prefetch(&table[bucketStart(activeSlots[i])]);
  }
  int numSurvivors = 0;
  int numInserts = 0;
  for (int a = 0; a < numActive; ++a) {
    if (a + kPrefetchDistance < numActive) {
      __builtin_prefetch(
          &table[bucketStart(activeSlots[a + kPrefetchDistance])]);
    }
    const int32_t row = activeRows[a];
    const uint64_t tag = tagFromHash(hashes[row]);
    char* taggedPointer;
    uint64_t slotOut;
    scanBucketChain(activeSlots[a], mask, tag, table, taggedPointer, slotOut);
    char* group = stripTag(taggedPointer);
    hits[row] = group;
    const int nonEmpty = (taggedPointer != nullptr);
    activeRows[numSurvivors] = row;
    activeSlots[numSurvivors] = slotOut;
    numSurvivors += nonEmpty;
    if constexpr (kEmitInsert) {
      insertRows[numInserts] = row;
      insertSlots[numInserts] = slotOut;
      numInserts += (1 - nonEmpty);
    }
  }
  if constexpr (kEmitInsert) {
    *numInsertsOut = numInserts;
  }
  return numSurvivors;
}

template <typename Insert>
FOLLY_ALWAYS_INLINE int walkSplitAndInsertInPlace(
    int numActive,
    uint64_t mask,
    uint64_t* __restrict__ activeSlots,
    int32_t* __restrict__ activeRows,
    const uint64_t* __restrict__ hashes,
    char** table,
    char** __restrict__ hits,
    Insert&& insert) {
  constexpr int kPrefetchDistance = kProbePrefetchDistance;
  for (int i = 0; i < kPrefetchDistance && i < numActive; ++i) {
    __builtin_prefetch(&table[bucketStart(activeSlots[i])]);
  }
  int numSurvivors = 0;
  for (int a = 0; a < numActive; ++a) {
    if (a + kPrefetchDistance < numActive) {
      __builtin_prefetch(
          &table[bucketStart(activeSlots[a + kPrefetchDistance])]);
    }
    const int32_t row = activeRows[a];
    const uint64_t tag = tagFromHash(hashes[row]);
    char* taggedPointer;
    uint64_t slotOut;
    scanBucketChain(activeSlots[a], mask, tag, table, taggedPointer, slotOut);
    if (taggedPointer == nullptr) {
      insert(slotOut, row);
      continue;
    }
    hits[row] = stripTag(taggedPointer);
    activeRows[numSurvivors] = row;
    activeSlots[numSurvivors] = slotOut;
    ++numSurvivors;
  }
  return numSurvivors;
}

template <bool kEmitInsert, bool kWriteHits>
FOLLY_ALWAYS_INLINE int walkAndSplitToCandidates(
    int numActive,
    uint64_t mask,
    const uint64_t* __restrict__ activeSlots,
    const int32_t* __restrict__ activeRows,
    const uint64_t* __restrict__ hashes,
    char* const* __restrict__ table,
    char** __restrict__ hits,
    vector_size_t* __restrict__ candidateRows,
    char** __restrict__ candidateGroups,
    uint64_t* __restrict__ candidateSlots,
    int32_t* __restrict__ insertRows,
    uint64_t* __restrict__ insertSlots,
    int* numInsertsOut) {
  constexpr int kPrefetchDistance = kProbePrefetchDistance;
  for (int i = 0; i < kPrefetchDistance && i < numActive; ++i) {
    __builtin_prefetch(&table[bucketStart(activeSlots[i])]);
  }
  int numCandidates = 0;
  int numInserts = 0;
  for (int a = 0; a < numActive; ++a) {
    if (a + kPrefetchDistance < numActive) {
      __builtin_prefetch(
          &table[bucketStart(activeSlots[a + kPrefetchDistance])]);
    }
    const int32_t row = activeRows[a];
    const uint64_t tag = tagFromHash(hashes[row]);
    char* taggedPointer;
    uint64_t slotOut;
    scanBucketChain(activeSlots[a], mask, tag, table, taggedPointer, slotOut);
    char* group = stripTag(taggedPointer);
    const int nonEmpty = (taggedPointer != nullptr);
    if constexpr (kWriteHits) {
      hits[row] = group;
    }
    candidateRows[numCandidates] = row;
    candidateGroups[numCandidates] = group;
    candidateSlots[numCandidates] = slotOut;
    numCandidates += nonEmpty;
    if constexpr (kEmitInsert) {
      insertRows[numInserts] = row;
      insertSlots[numInserts] = slotOut;
      numInserts += (1 - nonEmpty);
    }
  }
  if constexpr (kEmitInsert) {
    *numInsertsOut = numInserts;
  }
  return numCandidates;
}

template <bool kWriteHits, typename Insert>
FOLLY_ALWAYS_INLINE int walkSplitAndInsertToCandidates(
    int numActive,
    uint64_t mask,
    const uint64_t* __restrict__ activeSlots,
    const int32_t* __restrict__ activeRows,
    const uint64_t* __restrict__ hashes,
    char** table,
    char** __restrict__ hits,
    vector_size_t* __restrict__ candidateRows,
    char** __restrict__ candidateGroups,
    uint64_t* __restrict__ candidateSlots,
    Insert&& insert) {
  constexpr int kPrefetchDistance = kProbePrefetchDistance;
  for (int i = 0; i < kPrefetchDistance && i < numActive; ++i) {
    __builtin_prefetch(&table[bucketStart(activeSlots[i])]);
  }
  int numCandidates = 0;
  for (int a = 0; a < numActive; ++a) {
    if (a + kPrefetchDistance < numActive) {
      __builtin_prefetch(
          &table[bucketStart(activeSlots[a + kPrefetchDistance])]);
    }
    const int32_t row = activeRows[a];
    const uint64_t tag = tagFromHash(hashes[row]);
    char* taggedPointer;
    uint64_t slotOut;
    scanBucketChain(activeSlots[a], mask, tag, table, taggedPointer, slotOut);
    if (taggedPointer == nullptr) {
      insert(slotOut, row);
      continue;
    }
    char* group = stripTag(taggedPointer);
    if constexpr (kWriteHits) {
      hits[row] = group;
    }
    candidateRows[numCandidates] = row;
    candidateGroups[numCandidates] = group;
    candidateSlots[numCandidates] = slotOut;
    ++numCandidates;
  }
  return numCandidates;
}

} // namespace bytedance::bolt::exec::hash_table_simd
