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

#include <cstdint>
#include <memory>
#include <vector>

#include "bolt/common/base/Portability.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::exec {

class VectorHasher;
struct HashLookup;

namespace hash_table_simd {

struct KeyColumn {
  const DecodedVector* decoded{nullptr};
  const void* inputValues{nullptr};
  RowColumn rowColumn{0, RowColumn::kNotNullOffset};
  TypeKind typeKind{TypeKind::INVALID};
  int32_t columnIndex{0};
  bool hasNulls{false};
  bool isIdentityMapping{false};
  const vector_size_t* indices{nullptr};
  bool useRowCompare{false};
};

bool tryHash(
    VectorHasher& hasher,
    const SelectivityVector& rows,
    bool mixWithExisting,
    uint64_t* hashes);

bool tryValueIds(
    VectorHasher& hasher,
    const SelectivityVector& rows,
    bool mixWithExisting,
    uint64_t* hashes);

void rehashNormalizedKeys(
    char** __restrict groups,
    int32_t numGroups,
    uint64_t* __restrict hashes);

// Returns false when the type requires the RowContainer fallback. A zero
// nullMask skips null handling.
bool tryRehash(
    TypeKind kind,
    char** __restrict groups,
    int32_t columnOffset,
    int32_t numGroups,
    bool mixWithExisting,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* __restrict hashes);

// matchMask and bothNullMask are caller-owned buffers of at least
// numCandidates elements. bothNullMask may be null when no key is nullable.
template <bool ignoreNullKeys>
int32_t compareProbeColumns(
    const std::vector<KeyColumn>& columns,
    vector_size_t* candidateRows,
    char** candidateGroups,
    uint64_t* candidateSlots,
    int32_t numCandidates,
    vector_size_t* mismatchRows,
    uint64_t* mismatchSlots,
    int32_t& totalMismatches,
    RowContainer* rows,
    uint64_t tableMask,
    uint8_t* matchMask,
    uint8_t* bothNullMask);

// matchMask, bothNullMask, and newGroupPointers are caller-owned buffers of at
// least numCandidates elements. bothNullMask may be null when no key is
// nullable.
template <bool ignoreNullKeys>
int32_t compareBuildColumns(
    const std::vector<KeyColumn>& columns,
    char** newGroups,
    vector_size_t* candidateRows,
    char** candidateGroups,
    uint64_t* candidateSlots,
    int32_t numCandidates,
    vector_size_t* mismatchRows,
    uint64_t* mismatchSlots,
    int32_t& totalMismatches,
    RowContainer* rows,
    uint8_t* matchMask,
    uint8_t* bothNullMask,
    char** newGroupPointers);

template <bool ignoreNullKeys>
void storeKeysToRow(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    RowContainer* rows,
    char* group,
    vector_size_t row);

template <bool ignoreNullKeys>
void buildKeyColumns(
    const HashLookup& lookup,
    std::vector<KeyColumn>& columns,
    RowContainer* rows,
    bool hasStoredNullKeys,
    const std::vector<bool>& columnHasNulls);

// SIMD slot layout: 8-slot buckets, each slot is [tag:16 | pointer:48].
// The 48-bit pointer invariant matches the existing non-kArray HashTable
// bucket layout, which stores row pointers in 6 bytes.
constexpr uint64_t kPointerMask = 0x0000FFFFFFFFFFFFULL;
constexpr uint64_t kTagMask = 0xFFFF000000000000ULL;
constexpr uint64_t kBucketSize = 8;
constexpr uint64_t kBucketAlignmentMask = ~(kBucketSize - 1);
constexpr int kProbePrefetchDistance = 32;

FOLLY_ALWAYS_INLINE uint64_t tagFromHash(uint64_t hash) {
  return hash & kTagMask;
}

FOLLY_ALWAYS_INLINE uint64_t bucketStart(uint64_t slot) {
  return slot & kBucketAlignmentMask;
}

FOLLY_ALWAYS_INLINE char* applyTag(char* row, uint64_t hash) {
  return reinterpret_cast<char*>(
      (reinterpret_cast<uintptr_t>(row) & kPointerMask) | tagFromHash(hash));
}

FOLLY_ALWAYS_INLINE char* stripTag(char* tagged) {
  return reinterpret_cast<char*>(
      reinterpret_cast<uintptr_t>(tagged) & kPointerMask);
}

} // namespace hash_table_simd
} // namespace bytedance::bolt::exec
