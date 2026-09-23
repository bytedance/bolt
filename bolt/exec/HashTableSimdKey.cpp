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

#include "bolt/exec/HashTableSimd.h"

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/HashTable.h"
#include "bolt/exec/VectorHasher.h"

#include <cstring>

namespace bytedance::bolt::exec::hash_table_simd {

template <bool ignoreNullKeys>
void storeKeysToRow(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    RowContainer* rows,
    char* group,
    vector_size_t row) {
  const int32_t numHashers = hashers.size();
  for (int32_t i = 0; i < numHashers; ++i) {
    auto& hasher = hashers[i];
    const auto& decoded = hasher->decodedVector();
    if constexpr (!ignoreNullKeys) {
      if (decoded.mayHaveNulls() && decoded.isNullAt(row)) {
        rows->store(decoded, row, group, i);
        continue;
      }
    }
    const auto offset = rows->columnAt(i).offset();
    switch (hasher->typeKind()) {
      case TypeKind::BOOLEAN:
        *reinterpret_cast<bool*>(group + offset) = decoded.valueAt<bool>(row);
        break;
      case TypeKind::TINYINT:
        *reinterpret_cast<int8_t*>(group + offset) =
            decoded.valueAt<int8_t>(row);
        break;
      case TypeKind::SMALLINT:
        *reinterpret_cast<int16_t*>(group + offset) =
            decoded.valueAt<int16_t>(row);
        break;
      case TypeKind::INTEGER:
        *reinterpret_cast<int32_t*>(group + offset) =
            decoded.valueAt<int32_t>(row);
        break;
      case TypeKind::BIGINT:
        *reinterpret_cast<int64_t*>(group + offset) =
            decoded.valueAt<int64_t>(row);
        break;
      case TypeKind::REAL:
        *reinterpret_cast<float*>(group + offset) = decoded.valueAt<float>(row);
        break;
      case TypeKind::DOUBLE:
        *reinterpret_cast<double*>(group + offset) =
            decoded.valueAt<double>(row);
        break;
      case TypeKind::TIMESTAMP: {
        const auto value = decoded.valueAt<Timestamp>(row);
        std::memcpy(group + offset, &value, sizeof(value));
        break;
      }
      case TypeKind::HUGEINT: {
        const auto value = decoded.valueAt<int128_t>(row);
        std::memcpy(group + offset, &value, sizeof(value));
        break;
      }
      default:
        rows->store(decoded, row, group, i);
        break;
    }
  }
}

template void storeKeysToRow<true>(
    const std::vector<std::unique_ptr<VectorHasher>>&,
    RowContainer*,
    char*,
    vector_size_t);
template void storeKeysToRow<false>(
    const std::vector<std::unique_ptr<VectorHasher>>&,
    RowContainer*,
    char*,
    vector_size_t);

bool canUseRawValueFastPath(const VectorHasher& hasher) {
  const auto& decoded = hasher.decodedVector();
  switch (hasher.inputVectorEncoding()) {
    case VectorEncoding::Simple::FLAT:
      return decoded.isIdentityMapping();
    case VectorEncoding::Simple::CONSTANT:
      return decoded.isConstantMapping();
    case VectorEncoding::Simple::DICTIONARY:
      return decoded.isConstantMapping() || decoded.base()->isFlatEncoding();
    default:
      return false;
  }
}

template <bool ignoreNullKeys>
void buildKeyColumns(
    const HashLookup& lookup,
    std::vector<KeyColumn>& columns,
    RowContainer* rows,
    bool hasStoredNullKeys,
    const std::vector<bool>& columnHasNulls) {
  columns.clear();
  columns.reserve(lookup.hashers.size());

  bool hasAnyNulls = false;
  if constexpr (!ignoreNullKeys) {
    for (const auto& hasher : lookup.hashers) {
      if (hasher->decodedVector().mayHaveNulls()) {
        hasAnyNulls = true;
        break;
      }
    }
    if (!hasAnyNulls) {
      if (hasStoredNullKeys) {
        hasAnyNulls = true;
      } else if (columnHasNulls.empty()) {
        for (int32_t i = 0; i < lookup.hashers.size(); ++i) {
          if (rows->columnAt(i).nullMask() != 0) {
            hasAnyNulls = true;
            break;
          }
        }
      }
    }
  }

  for (int32_t columnIndex = 0; columnIndex < lookup.hashers.size();
       ++columnIndex) {
    const auto& hasher = lookup.hashers[columnIndex];
    auto& decoded = hasher->decodedVector();
    const void* rawValues = nullptr;
    const bool useRowCompare = false;
    const bool useRawValueFastPath =
        !useRowCompare && canUseRawValueFastPath(*hasher);
    if (!useRowCompare) {
      switch (hasher->typeKind()) {
        case TypeKind::BIGINT:
          if (useRawValueFastPath) {
            rawValues = decoded.data<int64_t>();
          }
          break;
        case TypeKind::VARCHAR:
          if (useRawValueFastPath) {
            rawValues = decoded.data<StringView>();
          }
          break;
        case TypeKind::INTEGER:
          if (useRawValueFastPath) {
            rawValues = decoded.data<int32_t>();
          }
          break;
        case TypeKind::TINYINT:
          if (useRawValueFastPath) {
            rawValues = decoded.data<int8_t>();
          }
          break;
        case TypeKind::SMALLINT:
          if (useRawValueFastPath) {
            rawValues = decoded.data<int16_t>();
          }
          break;
        case TypeKind::REAL:
          if (useRawValueFastPath) {
            rawValues = decoded.data<float>();
          }
          break;
        case TypeKind::DOUBLE:
          if (useRawValueFastPath) {
            rawValues = decoded.data<double>();
          }
          break;
        case TypeKind::BOOLEAN:
          if (useRawValueFastPath) {
            rawValues = decoded.data<uint64_t>();
          }
          break;
        default:
          break;
      }
    }
    const auto rowColumn = rows->columnAt(columnIndex);
    bool hasColumnNulls = hasAnyNulls;
    if (hasColumnNulls) {
      const bool schemaNonNull = rowColumn.nullMask() == 0;
      const bool inputHasNoNull = !decoded.mayHaveNulls();
      const bool storedHasNoNull =
          (columnIndex < static_cast<int32_t>(columnHasNulls.size()))
          ? !columnHasNulls[columnIndex]
          : (rowColumn.nullMask() == 0);
      if (schemaNonNull || (inputHasNoNull && storedHasNoNull)) {
        hasColumnNulls = false;
      }
    }

    const vector_size_t* inputIndices = nullptr;
    if (rawValues != nullptr && !decoded.isIdentityMapping() &&
        !decoded.isConstantMapping()) {
      inputIndices = decoded.indices();
    }
    columns.push_back(KeyColumn{
        &decoded,
        rawValues,
        rowColumn,
        hasher->typeKind(),
        columnIndex,
        hasColumnNulls,
        decoded.isIdentityMapping(),
        inputIndices,
        useRowCompare});
  }
}

template void buildKeyColumns<true>(
    const HashLookup&,
    std::vector<KeyColumn>&,
    RowContainer*,
    bool,
    const std::vector<bool>&);
template void buildKeyColumns<false>(
    const HashLookup&,
    std::vector<KeyColumn>&,
    RowContainer*,
    bool,
    const std::vector<bool>&);

namespace {

struct NullPrePassResult {
  bool hasValueRows{false};
  bool anyMismatch{false};
};

FOLLY_ALWAYS_INLINE NullPrePassResult nullPrePass(
    const KeyColumn& column,
    const vector_size_t* candidateRows,
    char* const* candidateGroups,
    int32_t numCandidates,
    uint8_t* __restrict matchMask,
    uint8_t* __restrict bothNullMask) {
  const bool columnIsNullable = column.rowColumn.nullMask() != 0;
  const auto nullByte = column.rowColumn.nullByte();
  const auto nullMask = column.rowColumn.nullMask();
  NullPrePassResult result;
  for (int32_t i = 0; i < numCandidates; ++i) {
    bool groupIsNull =
        columnIsNullable && ((candidateGroups[i][nullByte] & nullMask) != 0);
    bool inputIsNull = column.decoded->isNullAt(candidateRows[i]);
    uint8_t exactlyOneNull = (groupIsNull != inputIsNull) ? 1 : 0;
    matchMask[i] = static_cast<uint8_t>(!exactlyOneNull);
    bothNullMask[i] = static_cast<uint8_t>(groupIsNull & inputIsNull);
    result.hasValueRows |= !groupIsNull && !inputIsNull;
    result.anyMismatch |= exactlyOneNull != 0;
  }
  return result;
}

template <bool hasNulls, bool isFlat>
FOLLY_ALWAYS_INLINE vector_size_t inputValueIndex(
    const vector_size_t* indices,
    const vector_size_t* candidateRows,
    const uint8_t* matchMask,
    const uint8_t* bothNullMask,
    int32_t i) {
  if constexpr (isFlat) {
    return candidateRows[i];
  } else {
    const auto row = candidateRows[i];
    const auto index = indices[row];
    if constexpr (hasNulls) {
      const auto valid = static_cast<vector_size_t>(
          matchMask[i] & static_cast<uint8_t>(bothNullMask[i] ^ 1));
      return index & (static_cast<vector_size_t>(0) - valid);
    } else {
      return index;
    }
  }
}

template <bool hasNulls, bool isFlat>
FOLLY_ALWAYS_INLINE const StringView& inputStringAt(
    const StringView* inputBase,
    const vector_size_t* indices,
    const uint8_t* matchMask,
    const uint8_t* bothNullMask,
    const vector_size_t* candidateRows,
    int32_t i) {
  return inputBase[inputValueIndex<hasNulls, isFlat>(
      indices, candidateRows, matchMask, bothNullMask, i)];
}

FOLLY_ALWAYS_INLINE uint8_t stringViewRawWordsEqual(
    uint64_t leftFirst,
    uint64_t leftSecond,
    uint64_t rightFirst,
    uint64_t rightSecond) {
  const auto size = static_cast<uint32_t>(leftFirst);
  return static_cast<uint8_t>(
      (leftFirst == rightFirst) &
      ((leftSecond == rightSecond) | (size <= StringView::kPrefixSize) |
       (size > StringView::kInlineSize)));
}

template <TypeKind Kind, bool hasNulls>
bool compareConstantProbeValues(
    const KeyColumn& column,
    char* const* __restrict candidateGroups,
    int32_t numCandidates,
    uint8_t* __restrict matchMask,
    const uint8_t* __restrict bothNullMask,
    vector_size_t* __restrict nonInlineRows) {
  if (numCandidates == 0) {
    return false;
  }

  const auto columnOffset = column.rowColumn.offset();
  const auto constantIndex = column.decoded->index(0);
  bool anyMismatch = false;

  if constexpr (
      Kind == TypeKind::BIGINT || Kind == TypeKind::INTEGER ||
      Kind == TypeKind::SMALLINT || Kind == TypeKind::TINYINT) {
    using T = std::conditional_t<
        Kind == TypeKind::BIGINT,
        int64_t,
        std::conditional_t<
            Kind == TypeKind::INTEGER,
            int32_t,
            std::conditional_t<Kind == TypeKind::SMALLINT, int16_t, int8_t>>>;
    const auto constantValue =
        reinterpret_cast<const T*>(column.inputValues)[constantIndex];
    for (int32_t i = 0; i < numCandidates; ++i) {
      uint8_t equal = static_cast<uint8_t>(
          *reinterpret_cast<const T*>(candidateGroups[i] + columnOffset) ==
          constantValue);
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::BOOLEAN) {
    const auto* boolBits =
        reinterpret_cast<const uint64_t*>(column.inputValues);
    const uint8_t constantValue =
        bits::isBitSet(boolBits, constantIndex) ? 1 : 0;
    for (int32_t i = 0; i < numCandidates; ++i) {
      uint8_t equal = static_cast<uint8_t>(
          ((*reinterpret_cast<const uint8_t*>(
                candidateGroups[i] + columnOffset) &
            0x1) == constantValue));
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::DOUBLE || Kind == TypeKind::REAL) {
    using T = std::conditional_t<Kind == TypeKind::DOUBLE, double, float>;
    const auto constantValue =
        reinterpret_cast<const T*>(column.inputValues)[constantIndex];
    for (int32_t i = 0; i < numCandidates; ++i) {
      T groupValue =
          *reinterpret_cast<const T*>(candidateGroups[i] + columnOffset);
      uint8_t equal = static_cast<uint8_t>(
          (groupValue == constantValue) |
          (std::isnan(groupValue) & std::isnan(constantValue)));
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::VARCHAR) {
    const auto constantValue =
        reinterpret_cast<const StringView*>(column.inputValues)[constantIndex];
    const auto* constantBytes = reinterpret_cast<const char*>(&constantValue);
    const uint64_t constantFirst =
        *reinterpret_cast<const uint64_t*>(constantBytes);
    const uint64_t constantSecond =
        *reinterpret_cast<const uint64_t*>(constantBytes + 8);
    int32_t numNonInline = 0;

    for (int32_t i = 0; i < numCandidates; ++i) {
      const char* groupPtr = candidateGroups[i] + columnOffset;
      const uint64_t groupFirst = *reinterpret_cast<const uint64_t*>(groupPtr);
      const uint64_t groupSecond =
          *reinterpret_cast<const uint64_t*>(groupPtr + 8);
      const uint32_t size = static_cast<uint32_t>(groupFirst);
      uint8_t equal = stringViewRawWordsEqual(
          groupFirst, groupSecond, constantFirst, constantSecond);
      uint8_t needsStringCompare = equal;
      if constexpr (hasNulls) {
        needsStringCompare =
            static_cast<uint8_t>(matchMask[i] & equal & (bothNullMask[i] ^ 1));
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
      if (needsStringCompare && size > StringView::kInlineSize) {
        nonInlineRows[numNonInline++] = i;
      }
    }

    if (numNonInline > 0) {
      __builtin_prefetch(constantValue.data(), 0, 2);
    }
    for (int32_t j = 0; j < numNonInline; ++j) {
      const auto i = nonInlineRows[j];
      const auto& stored = *reinterpret_cast<const StringView*>(
          candidateGroups[i] + columnOffset);
      __builtin_prefetch(stored.data(), 0, 2);
    }
    for (int32_t j = 0; j < numNonInline; ++j) {
      const auto i = nonInlineRows[j];
      const auto& stored = *reinterpret_cast<const StringView*>(
          candidateGroups[i] + columnOffset);
      if (FOLLY_LIKELY(HashStringAllocator::isContiguous(stored))) {
        const auto equal = std::memcmp(
                               stored.data() + StringView::kPrefixSize,
                               constantValue.data() + StringView::kPrefixSize,
                               stored.size() - StringView::kPrefixSize) == 0;
        matchMask[i] = static_cast<uint8_t>(equal);
        anyMismatch |= !equal;
      } else {
        std::string storedString;
        auto storedView =
            HashStringAllocator::contiguousString(stored, storedString);
        const auto equal = storedView == constantValue;
        matchMask[i] = static_cast<uint8_t>(equal);
        anyMismatch |= !equal;
      }
    }
  }

  return anyMismatch;
}

template <TypeKind Kind, bool hasNulls, bool isFlat>
bool compareProbeValues(
    const KeyColumn& column,
    const vector_size_t* __restrict candidateRows,
    char* const* __restrict candidateGroups,
    int32_t numCandidates,
    uint8_t* __restrict matchMask,
    const uint8_t* __restrict bothNullMask,
    vector_size_t* __restrict nonInlineRows) {
  const auto columnOffset = column.rowColumn.offset();
  [[maybe_unused]] const vector_size_t* indices = column.indices;
  bool anyMismatch = false;

  if constexpr (
      Kind == TypeKind::BIGINT || Kind == TypeKind::INTEGER ||
      Kind == TypeKind::SMALLINT || Kind == TypeKind::TINYINT) {
    using T = std::conditional_t<
        Kind == TypeKind::BIGINT,
        int64_t,
        std::conditional_t<
            Kind == TypeKind::INTEGER,
            int32_t,
            std::conditional_t<Kind == TypeKind::SMALLINT, int16_t, int8_t>>>;
    const auto* base = reinterpret_cast<const T*>(column.inputValues);
    for (int32_t i = 0; i < numCandidates; ++i) {
      const auto inputIndex = inputValueIndex<hasNulls, isFlat>(
          indices, candidateRows, matchMask, bothNullMask, i);
      uint8_t equal = static_cast<uint8_t>(
          *reinterpret_cast<const T*>(candidateGroups[i] + columnOffset) ==
          base[inputIndex]);
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::BOOLEAN) {
    const auto* boolBits =
        reinterpret_cast<const uint64_t*>(column.inputValues);
    for (int32_t i = 0; i < numCandidates; ++i) {
      uint8_t groupValue =
          *reinterpret_cast<const uint8_t*>(candidateGroups[i] + columnOffset) &
          0x1;
      const auto inputIndex = inputValueIndex<hasNulls, isFlat>(
          indices, candidateRows, matchMask, bothNullMask, i);
      uint8_t inputValue = bits::isBitSet(boolBits, inputIndex) ? 1 : 0;
      uint8_t equal = static_cast<uint8_t>(groupValue == inputValue);
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::DOUBLE || Kind == TypeKind::REAL) {
    using T = std::conditional_t<Kind == TypeKind::DOUBLE, double, float>;
    const auto* base = reinterpret_cast<const T*>(column.inputValues);
    for (int32_t i = 0; i < numCandidates; ++i) {
      const auto inputIndex = inputValueIndex<hasNulls, isFlat>(
          indices, candidateRows, matchMask, bothNullMask, i);
      T groupValue =
          *reinterpret_cast<const T*>(candidateGroups[i] + columnOffset);
      T inputValue = base[inputIndex];
      uint8_t equal = static_cast<uint8_t>(
          (groupValue == inputValue) |
          (std::isnan(groupValue) & std::isnan(inputValue)));
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::VARCHAR) {
    const auto* inputBase =
        reinterpret_cast<const StringView*>(column.inputValues);
    int32_t numNonInline = 0;

    if constexpr (isFlat) {
      for (int32_t i = 0; i < numCandidates; ++i) {
        const char* groupData = candidateGroups[i] + columnOffset;
        const auto inputIndex = inputValueIndex<hasNulls, isFlat>(
            indices, candidateRows, matchMask, bothNullMask, i);
        const char* inputData =
            reinterpret_cast<const char*>(&inputBase[inputIndex]);
        uint64_t groupWord0 = *reinterpret_cast<const uint64_t*>(groupData);
        uint64_t inputWord0 = *reinterpret_cast<const uint64_t*>(inputData);
        uint64_t groupWord1 = *reinterpret_cast<const uint64_t*>(groupData + 8);
        uint64_t inputWord1 = *reinterpret_cast<const uint64_t*>(inputData + 8);
        uint32_t stringSize = static_cast<uint32_t>(groupWord0);
        uint8_t equal = stringViewRawWordsEqual(
            groupWord0, groupWord1, inputWord0, inputWord1);
        uint8_t needsStringCompare = equal;
        if constexpr (hasNulls) {
          needsStringCompare = static_cast<uint8_t>(
              matchMask[i] & equal & (bothNullMask[i] ^ 1));
          equal =
              static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
        }
        matchMask[i] = equal;
        anyMismatch |= equal == 0;
        if (needsStringCompare && stringSize > StringView::kInlineSize) {
          nonInlineRows[numNonInline++] = i;
        }
      }
    } else {
      for (int32_t i = 0; i < numCandidates; ++i) {
        const char* groupData = candidateGroups[i] + columnOffset;
        const auto inputIndex = inputValueIndex<hasNulls, isFlat>(
            indices, candidateRows, matchMask, bothNullMask, i);
        const char* inputData =
            reinterpret_cast<const char*>(&inputBase[inputIndex]);
        uint64_t groupWord0 = *reinterpret_cast<const uint64_t*>(groupData);
        uint64_t inputWord0 = *reinterpret_cast<const uint64_t*>(inputData);
        uint64_t groupWord1 = *reinterpret_cast<const uint64_t*>(groupData + 8);
        uint64_t inputWord1 = *reinterpret_cast<const uint64_t*>(inputData + 8);
        uint32_t stringSize = static_cast<uint32_t>(groupWord0);
        uint8_t equal = stringViewRawWordsEqual(
            groupWord0, groupWord1, inputWord0, inputWord1);
        uint8_t needsStringCompare = equal;
        if constexpr (hasNulls) {
          needsStringCompare = static_cast<uint8_t>(
              matchMask[i] & equal & (bothNullMask[i] ^ 1));
          equal =
              static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
        }
        matchMask[i] = equal;
        anyMismatch |= equal == 0;
        if (needsStringCompare && stringSize > StringView::kInlineSize) {
          nonInlineRows[numNonInline++] = i;
        }
      }
    }

    {
      for (int32_t j = 0; j < numNonInline; ++j) {
        const auto i = nonInlineRows[j];
        const auto& stored = *reinterpret_cast<const StringView*>(
            candidateGroups[i] + columnOffset);
        __builtin_prefetch(stored.data(), 0, 2);
        const auto& input = inputStringAt<hasNulls, isFlat>(
            inputBase, indices, matchMask, bothNullMask, candidateRows, i);
        __builtin_prefetch(input.data(), 0, 2);
      }
      for (int32_t j = 0; j < numNonInline; ++j) {
        const auto i = nonInlineRows[j];
        const auto& stored = *reinterpret_cast<const StringView*>(
            candidateGroups[i] + columnOffset);
        const auto& input = inputStringAt<hasNulls, isFlat>(
            inputBase, indices, matchMask, bothNullMask, candidateRows, i);
        if (FOLLY_LIKELY(HashStringAllocator::isContiguous(stored))) {
          const auto equal = std::memcmp(
                                 stored.data() + StringView::kPrefixSize,
                                 input.data() + StringView::kPrefixSize,
                                 stored.size() - StringView::kPrefixSize) == 0;
          matchMask[i] = static_cast<uint8_t>(equal);
          anyMismatch |= !equal;
        } else {
          std::string storedStr;
          auto storedStringView =
              HashStringAllocator::contiguousString(stored, storedStr);
          const auto equal = storedStringView == input;
          matchMask[i] = static_cast<uint8_t>(equal);
          anyMismatch |= !equal;
        }
      }
    }
  }
  return anyMismatch;
}

template <bool ignoreNullKeys>
bool dispatchProbeComparison(
    const KeyColumn& column,
    const vector_size_t* candidateRows,
    char* const* candidateGroups,
    int32_t numCandidates,
    uint8_t* matchMask,
    uint8_t* bothNullMask,
    vector_size_t* nonInlineRows,
    bool& anyMismatch) {
  if (column.useRowCompare) {
    return false;
  }

  if (column.inputValues == nullptr) {
    return false;
  }

  const bool hasNulls = !ignoreNullKeys && column.hasNulls;
  NullPrePassResult nullResult;
  if (hasNulls) {
    nullResult = nullPrePass(
        column,
        candidateRows,
        candidateGroups,
        numCandidates,
        matchMask,
        bothNullMask);
    if (!nullResult.hasValueRows) {
      anyMismatch = nullResult.anyMismatch;
      return true;
    }
  }

  bool unsupported = false;

#define DISPATCH_KIND(KIND)                                              \
  case TypeKind::KIND:                                                   \
    if (column.isIdentityMapping) {                                      \
      if (hasNulls) {                                                    \
        anyMismatch = compareProbeValues<TypeKind::KIND, true, true>(    \
            column,                                                      \
            candidateRows,                                               \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            bothNullMask,                                                \
            nonInlineRows);                                              \
      } else {                                                           \
        anyMismatch = compareProbeValues<TypeKind::KIND, false, true>(   \
            column,                                                      \
            candidateRows,                                               \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            nullptr,                                                     \
            nonInlineRows);                                              \
      }                                                                  \
    } else if (column.indices) {                                         \
      if (hasNulls) {                                                    \
        anyMismatch = compareProbeValues<TypeKind::KIND, true, false>(   \
            column,                                                      \
            candidateRows,                                               \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            bothNullMask,                                                \
            nonInlineRows);                                              \
      } else {                                                           \
        anyMismatch = compareProbeValues<TypeKind::KIND, false, false>(  \
            column,                                                      \
            candidateRows,                                               \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            nullptr,                                                     \
            nonInlineRows);                                              \
      }                                                                  \
    } else if (                                                          \
        column.decoded->isConstantMapping() &&                           \
        column.inputValues != nullptr) {                                 \
      if (hasNulls) {                                                    \
        anyMismatch = compareConstantProbeValues<TypeKind::KIND, true>(  \
            column,                                                      \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            bothNullMask,                                                \
            nonInlineRows);                                              \
      } else {                                                           \
        anyMismatch = compareConstantProbeValues<TypeKind::KIND, false>( \
            column,                                                      \
            candidateGroups,                                             \
            numCandidates,                                               \
            matchMask,                                                   \
            nullptr,                                                     \
            nonInlineRows);                                              \
      }                                                                  \
    } else {                                                             \
      unsupported = true;                                                \
    }                                                                    \
    break

  switch (column.typeKind) {
    DISPATCH_KIND(BIGINT);
    DISPATCH_KIND(INTEGER);
    DISPATCH_KIND(SMALLINT);
    DISPATCH_KIND(TINYINT);
    DISPATCH_KIND(BOOLEAN);
    DISPATCH_KIND(DOUBLE);
    DISPATCH_KIND(REAL);
    DISPATCH_KIND(VARCHAR);
    default:
      unsupported = true;
      break;
  }
#undef DISPATCH_KIND
  return !unsupported;
}

template <bool kMaterialized>
FOLLY_ALWAYS_INLINE char* buildNewGroupAt(
    char* const* newGroupsOrPtrs,
    const vector_size_t* candidateRows,
    int32_t i) {
  if constexpr (kMaterialized) {
    return newGroupsOrPtrs[i];
  } else {
    return newGroupsOrPtrs[candidateRows[i]];
  }
}

template <bool kMaterialized>
FOLLY_ALWAYS_INLINE void nullPrePassBuild(
    int32_t nullByte,
    uint8_t nullMask,
    char* const* existingGroups,
    char* const* newGroupsOrPtrs,
    const vector_size_t* candidateRows,
    int32_t numCandidates,
    uint8_t* __restrict matchMask,
    uint8_t* __restrict bothNullMask) {
  for (int32_t i = 0; i < numCandidates; ++i) {
    bool existingIsNull = (existingGroups[i][nullByte] & nullMask) != 0;
    char* newGroup =
        buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
    bool newIsNull = (newGroup[nullByte] & nullMask) != 0;
    uint8_t exactlyOneNull = (existingIsNull != newIsNull) ? 1 : 0;
    matchMask[i] = static_cast<uint8_t>(!exactlyOneNull);
    bothNullMask[i] = static_cast<uint8_t>(existingIsNull & newIsNull);
  }
}

template <TypeKind Kind, bool hasNulls, bool kMaterialized>
bool compareBuildValues(
    int32_t columnOffset,
    char* const* __restrict existingGroups,
    char* const* __restrict newGroupsOrPtrs,
    const vector_size_t* __restrict candidateRows,
    int32_t numCandidates,
    uint8_t* __restrict matchMask,
    const uint8_t* __restrict bothNullMask,
    vector_size_t* __restrict nonInlineRows) {
  bool anyMismatch = false;
  if constexpr (
      Kind == TypeKind::BIGINT || Kind == TypeKind::INTEGER ||
      Kind == TypeKind::SMALLINT || Kind == TypeKind::TINYINT) {
    using T = std::conditional_t<
        Kind == TypeKind::BIGINT,
        int64_t,
        std::conditional_t<
            Kind == TypeKind::INTEGER,
            int32_t,
            std::conditional_t<Kind == TypeKind::SMALLINT, int16_t, int8_t>>>;
    for (int32_t i = 0; i < numCandidates; ++i) {
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      uint8_t equal = static_cast<uint8_t>(
          *reinterpret_cast<const T*>(existingGroups[i] + columnOffset) ==
          *reinterpret_cast<const T*>(newGroup + columnOffset));
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::BOOLEAN) {
    for (int32_t i = 0; i < numCandidates; ++i) {
      uint8_t existingValue =
          *reinterpret_cast<const uint8_t*>(existingGroups[i] + columnOffset) &
          0x1;
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      uint8_t newValue =
          *reinterpret_cast<const uint8_t*>(newGroup + columnOffset) & 0x1;
      uint8_t equal = static_cast<uint8_t>(existingValue == newValue);
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::DOUBLE || Kind == TypeKind::REAL) {
    using T = std::conditional_t<Kind == TypeKind::DOUBLE, double, float>;
    for (int32_t i = 0; i < numCandidates; ++i) {
      T existingValue =
          *reinterpret_cast<const T*>(existingGroups[i] + columnOffset);
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      T newValue = *reinterpret_cast<const T*>(newGroup + columnOffset);
      uint8_t equal = static_cast<uint8_t>(
          (existingValue == newValue) |
          (std::isnan(existingValue) & std::isnan(newValue)));
      if constexpr (hasNulls) {
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
    }
  } else if constexpr (Kind == TypeKind::VARCHAR) {
    int32_t numNonInline = 0;
    for (int32_t i = 0; i < numCandidates; ++i) {
      const char* ep = existingGroups[i] + columnOffset;
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      const char* np = newGroup + columnOffset;
      uint64_t e1 = *reinterpret_cast<const uint64_t*>(ep);
      uint64_t n1 = *reinterpret_cast<const uint64_t*>(np);
      uint64_t e2 = *reinterpret_cast<const uint64_t*>(ep + 8);
      uint64_t n2 = *reinterpret_cast<const uint64_t*>(np + 8);
      uint32_t stringSize = static_cast<uint32_t>(e1);
      uint8_t equal = stringViewRawWordsEqual(e1, e2, n1, n2);
      uint8_t needsStringCompare = equal;
      if constexpr (hasNulls) {
        needsStringCompare =
            static_cast<uint8_t>(matchMask[i] & equal & (bothNullMask[i] ^ 1));
        equal = static_cast<uint8_t>(matchMask[i] & (equal | bothNullMask[i]));
      }
      matchMask[i] = equal;
      anyMismatch |= equal == 0;
      if (needsStringCompare && stringSize > StringView::kInlineSize) {
        nonInlineRows[numNonInline++] = i;
      }
    }

    for (int32_t j = 0; j < numNonInline; ++j) {
      const auto i = nonInlineRows[j];
      const auto& existing = *reinterpret_cast<const StringView*>(
          existingGroups[i] + columnOffset);
      __builtin_prefetch(existing.data(), 0, 2);
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      const auto& newStringView =
          *reinterpret_cast<const StringView*>(newGroup + columnOffset);
      __builtin_prefetch(newStringView.data(), 0, 2);
    }
    for (int32_t j = 0; j < numNonInline; ++j) {
      const auto i = nonInlineRows[j];
      const auto& existing = *reinterpret_cast<const StringView*>(
          existingGroups[i] + columnOffset);
      char* newGroup =
          buildNewGroupAt<kMaterialized>(newGroupsOrPtrs, candidateRows, i);
      const auto& newStringView =
          *reinterpret_cast<const StringView*>(newGroup + columnOffset);
      const bool existingIsContiguous =
          HashStringAllocator::isContiguous(existing);
      const bool newIsContiguous =
          HashStringAllocator::isContiguous(newStringView);
      if (FOLLY_LIKELY(existingIsContiguous && newIsContiguous)) {
        const auto equal = std::memcmp(
                               existing.data() + StringView::kPrefixSize,
                               newStringView.data() + StringView::kPrefixSize,
                               existing.size() - StringView::kPrefixSize) == 0;
        matchMask[i] = static_cast<uint8_t>(equal);
        anyMismatch |= !equal;
      } else {
        std::string existingStringStorage;
        auto existingStringView = HashStringAllocator::contiguousString(
            existing, existingStringStorage);
        std::string newStringStorage;
        auto contiguousNewStringView = HashStringAllocator::contiguousString(
            newStringView, newStringStorage);
        const auto equal = existingStringView == contiguousNewStringView;
        matchMask[i] = static_cast<uint8_t>(equal);
        anyMismatch |= !equal;
      }
    }
  }
  return anyMismatch;
}

template <bool ignoreNullKeys, bool kMaterialized>
bool dispatchBuildComparison(
    const KeyColumn& column,
    char* const* existingGroups,
    char* const* newGroupsOrPtrs,
    const vector_size_t* candidateRows,
    int32_t numCandidates,
    uint8_t* matchMask,
    uint8_t* bothNullMask,
    vector_size_t* nonInlineRows,
    bool& anyMismatch) {
  if (column.useRowCompare) {
    return false;
  }

  const bool hasNulls = !ignoreNullKeys && column.rowColumn.nullMask() != 0;
  if (hasNulls) {
    nullPrePassBuild<kMaterialized>(
        column.rowColumn.nullByte(),
        column.rowColumn.nullMask(),
        existingGroups,
        newGroupsOrPtrs,
        candidateRows,
        numCandidates,
        matchMask,
        bothNullMask);
  }

  const auto columnOffset = column.rowColumn.offset();

#define DISPATCH_BUILD_KIND(KIND)                                             \
  case TypeKind::KIND:                                                        \
    if (hasNulls) {                                                           \
      anyMismatch = compareBuildValues<TypeKind::KIND, true, kMaterialized>(  \
          columnOffset,                                                       \
          existingGroups,                                                     \
          newGroupsOrPtrs,                                                    \
          candidateRows,                                                      \
          numCandidates,                                                      \
          matchMask,                                                          \
          bothNullMask,                                                       \
          nonInlineRows);                                                     \
    } else {                                                                  \
      anyMismatch = compareBuildValues<TypeKind::KIND, false, kMaterialized>( \
          columnOffset,                                                       \
          existingGroups,                                                     \
          newGroupsOrPtrs,                                                    \
          candidateRows,                                                      \
          numCandidates,                                                      \
          matchMask,                                                          \
          nullptr,                                                            \
          nonInlineRows);                                                     \
    }                                                                         \
    break

  switch (column.typeKind) {
    DISPATCH_BUILD_KIND(BIGINT);
    DISPATCH_BUILD_KIND(INTEGER);
    DISPATCH_BUILD_KIND(SMALLINT);
    DISPATCH_BUILD_KIND(TINYINT);
    DISPATCH_BUILD_KIND(BOOLEAN);
    DISPATCH_BUILD_KIND(DOUBLE);
    DISPATCH_BUILD_KIND(REAL);
    DISPATCH_BUILD_KIND(VARCHAR);
    default:
      return false;
  }
#undef DISPATCH_BUILD_KIND
  return true;
}

template <bool kCompactExtraGroups, bool kCompactCandidateSlots, bool kAdvance>
FOLLY_ALWAYS_INLINE int32_t compactCandidates(
    const uint8_t* __restrict matchMask,
    int32_t numCandidates,
    vector_size_t* candidateRows,
    char** candidateGroups,
    uint64_t* candidateSlots,
    char** extraGroups,
    vector_size_t* mismatchRows,
    uint64_t* mismatchSlots,
    int32_t& totalMismatches,
    uint64_t tableMask) {
  int32_t numSurvivors = 0;
  for (int32_t i = 0; i < numCandidates; ++i) {
    if (matchMask[i]) {
      candidateRows[numSurvivors] = candidateRows[i];
      candidateGroups[numSurvivors] = candidateGroups[i];
      if constexpr (kCompactCandidateSlots) {
        candidateSlots[numSurvivors] = candidateSlots[i];
      }
      if constexpr (kCompactExtraGroups) {
        extraGroups[numSurvivors] = extraGroups[i];
      }
      ++numSurvivors;
    } else {
      mismatchRows[totalMismatches] = candidateRows[i];
      if constexpr (kAdvance) {
        mismatchSlots[totalMismatches] = (candidateSlots[i] + 1) & tableMask;
      } else {
        mismatchSlots[totalMismatches] = candidateSlots[i];
      }
      ++totalMismatches;
    }
  }
  return numSurvivors;
}

} // anonymous namespace

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
    uint8_t* bothNullMask) {
  uint8_t* activeBothNullMask = nullptr;
  if constexpr (!ignoreNullKeys) {
    if (bothNullMask) {
      for (const auto& column : columns) {
        if (column.hasNulls) {
          activeBothNullMask = bothNullMask;
          break;
        }
      }
    }
  }

  totalMismatches = 0;
  const auto lastColumnIndex = static_cast<int32_t>(columns.size()) - 1;
  for (int32_t columnIndex = 0; columnIndex < columns.size(); ++columnIndex) {
    const auto& column = columns[columnIndex];
    if (numCandidates == 0) {
      break;
    }
    bool anyMismatch = false;
    if (!dispatchProbeComparison<ignoreNullKeys>(
            column,
            candidateRows,
            candidateGroups,
            numCandidates,
            matchMask,
            column.hasNulls ? activeBothNullMask : nullptr,
            mismatchRows + totalMismatches,
            anyMismatch)) {
      const auto compareFlags =
          CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);
      for (int32_t i = 0; i < numCandidates; ++i) {
        matchMask[i] = static_cast<uint8_t>(
            rows->compare(
                candidateGroups[i],
                column.rowColumn,
                *column.decoded,
                candidateRows[i],
                compareFlags) == 0);
        anyMismatch |= matchMask[i] == 0;
      }
    }
    if (anyMismatch) {
      if (columnIndex == lastColumnIndex) {
        numCandidates = compactCandidates<false, false, true>(
            matchMask,
            numCandidates,
            candidateRows,
            candidateGroups,
            candidateSlots,
            nullptr,
            mismatchRows,
            mismatchSlots,
            totalMismatches,
            tableMask);
      } else {
        numCandidates = compactCandidates<false, true, true>(
            matchMask,
            numCandidates,
            candidateRows,
            candidateGroups,
            candidateSlots,
            nullptr,
            mismatchRows,
            mismatchSlots,
            totalMismatches,
            tableMask);
      }
    }
  }

  return numCandidates;
}

template int32_t compareProbeColumns<true>(
    const std::vector<KeyColumn>&,
    vector_size_t*,
    char**,
    uint64_t*,
    int32_t,
    vector_size_t*,
    uint64_t*,
    int32_t&,
    RowContainer*,
    uint64_t,
    uint8_t*,
    uint8_t*);
template int32_t compareProbeColumns<false>(
    const std::vector<KeyColumn>&,
    vector_size_t*,
    char**,
    uint64_t*,
    int32_t,
    vector_size_t*,
    uint64_t*,
    int32_t&,
    RowContainer*,
    uint64_t,
    uint8_t*,
    uint8_t*);

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
    char** newGroupPointers) {
  const bool materializeNewGroupPointers = columns.size() > 1;
  if (materializeNewGroupPointers) {
    for (int32_t i = 0; i < numCandidates; ++i) {
      newGroupPointers[i] = newGroups[candidateRows[i]];
    }
  }

  uint8_t* activeBothNullMask = nullptr;
  if constexpr (!ignoreNullKeys) {
    if (bothNullMask) {
      for (const auto& column : columns) {
        if (column.rowColumn.nullMask() != 0) {
          activeBothNullMask = bothNullMask;
          break;
        }
      }
    }
  }

  const auto compareFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  totalMismatches = 0;
  const auto* lastColumn = columns.empty() ? nullptr : &columns.back();
  for (const auto& column : columns) {
    if (numCandidates == 0) {
      break;
    }
    const bool columnIsNullable = column.rowColumn.nullMask() != 0;
    bool anyMismatch = false;
    bool handled = materializeNewGroupPointers
        ? dispatchBuildComparison<ignoreNullKeys, true>(
              column,
              candidateGroups,
              newGroupPointers,
              candidateRows,
              numCandidates,
              matchMask,
              columnIsNullable ? activeBothNullMask : nullptr,
              mismatchRows + totalMismatches,
              anyMismatch)
        : dispatchBuildComparison<ignoreNullKeys, false>(
              column,
              candidateGroups,
              newGroups,
              candidateRows,
              numCandidates,
              matchMask,
              columnIsNullable ? activeBothNullMask : nullptr,
              mismatchRows + totalMismatches,
              anyMismatch);
    if (!handled) {
      for (int32_t i = 0; i < numCandidates; ++i) {
        char* newGroup = materializeNewGroupPointers
            ? newGroupPointers[i]
            : newGroups[candidateRows[i]];
        matchMask[i] = static_cast<uint8_t>(
            rows->compare(
                candidateGroups[i],
                newGroup,
                column.columnIndex,
                compareFlags) == 0);
        anyMismatch |= matchMask[i] == 0;
      }
    }
    if (anyMismatch) {
      const bool isLastColumn = &column == lastColumn;
      if (materializeNewGroupPointers) {
        if (isLastColumn) {
          numCandidates = compactCandidates<false, false, false>(
              matchMask,
              numCandidates,
              candidateRows,
              candidateGroups,
              candidateSlots,
              newGroupPointers,
              mismatchRows,
              mismatchSlots,
              totalMismatches,
              0);
        } else {
          numCandidates = compactCandidates<true, true, false>(
              matchMask,
              numCandidates,
              candidateRows,
              candidateGroups,
              candidateSlots,
              newGroupPointers,
              mismatchRows,
              mismatchSlots,
              totalMismatches,
              0);
        }
      } else {
        numCandidates = compactCandidates<false, false, false>(
            matchMask,
            numCandidates,
            candidateRows,
            candidateGroups,
            candidateSlots,
            nullptr,
            mismatchRows,
            mismatchSlots,
            totalMismatches,
            0);
      }
    }
  }

  return numCandidates;
}

template int32_t compareBuildColumns<true>(
    const std::vector<KeyColumn>&,
    char**,
    vector_size_t*,
    char**,
    uint64_t*,
    int32_t,
    vector_size_t*,
    uint64_t*,
    int32_t&,
    RowContainer*,
    uint8_t*,
    uint8_t*,
    char**);
template int32_t compareBuildColumns<false>(
    const std::vector<KeyColumn>&,
    char**,
    vector_size_t*,
    char**,
    uint64_t*,
    int32_t,
    vector_size_t*,
    uint64_t*,
    int32_t&,
    RowContainer*,
    uint8_t*,
    uint8_t*,
    char**);

} // namespace bytedance::bolt::exec::hash_table_simd
