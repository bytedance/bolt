/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <folly/CPortability.h>
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/functions/InlineFlatten.h"
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorTypeUtils.h"

#ifdef ENABLE_BOLT_JIT
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"
#include "bolt/jit/common.h"

#endif
namespace bytedance::bolt::exec {

class Aggregate;
class RowFormatInfo;

class Accumulator {
 public:
  Accumulator(
      bool isFixedSize,
      int32_t fixedSize,
      bool usesExternalMemory,
      int32_t alignment,
      TypePtr spillType,
      TypePtr finalType,
      std::function<void(folly::Range<char**> groups, VectorPtr& result)>
          spillExtractFunction,
      std::function<void(folly::Range<char**> groups, VectorPtr& result)>
          outputExtractFunction,
      std::function<void(folly::Range<char**> groups)> destroyFunction);

  explicit Accumulator(Aggregate* aggregate, TypePtr spillType);

  bool isFixedSize() const;

  bool serializable() const;

  int32_t fixedWidthSize() const;

  bool usesExternalMemory() const;

  int32_t alignment() const;

  const TypePtr& spillType() const;

  const TypePtr& finalOutputType() const;

  void extractForSpill(folly::Range<char**> groups, VectorPtr& result) const;

  void extractForOutput(folly::Range<char**> groups, VectorPtr& result) const;

  void destroy(folly::Range<char**> groups);

  uint32_t getSerializeSize(char* group) const;

  char* serializeAccumulator(char* group, char* dst) const;

  char* deserializeAccumulator(char* group, char* src) const;

 private:
  const bool isFixedSize_;
  const bool serializable_;
  const int32_t fixedSize_;
  const bool usesExternalMemory_;
  const int32_t alignment_;
  const TypePtr spillType_;
  const TypePtr finalType_;
  std::function<void(folly::Range<char**>, VectorPtr&)> spillExtractFunction_;
  std::function<void(folly::Range<char**>, VectorPtr&)> outputExtractFunction_;
  std::function<void(folly::Range<char**> groups)> destroyFunction_;
  const Aggregate* aggregate_;
};

using normalized_key_t = uint64_t;

typedef int8_t (*RowRowCompare)(const char*, const char*);
typedef bool (*RowEqVectors)(const char*, int32_t, char*[]);

struct RowContainerIterator {
  int32_t allocationIndex = 0;
  int32_t rowOffset = 0;
  // Number of unvisited entries that are prefixed by an uint64_t for
  // normalized key. Set in listRows() on first call.
  int64_t normalizedKeysLeft = 0;
  int normalizedKeySize = 0;

  // Ordinal position of 'currentRow' in RowContainer.
  int32_t rowNumber{0};
  char* FOLLY_NULLABLE rowBegin{nullptr};
  // First byte after the end of the range containing 'currentRow'.
  char* FOLLY_NULLABLE endOfRun{nullptr};

  // Cursor used by fast row listing with cached row pointers.
  int32_t listRowCursor{0};

  // Returns the current row, skipping a possible normalized key below the first
  // byte of row.
  inline char* currentRow() const {
    return (rowBegin && normalizedKeysLeft) ? rowBegin + normalizedKeySize
                                            : rowBegin;
  }

  void reset() {
    *this = {};
  }
};

/// Container with a 8-bit partition number field for each row in a
/// RowContainer. The partition number bytes correspond 1:1 to rows. Used only
/// for parallel hash join build.
class RowPartitions {
 public:
  /// Initializes this to hold up to 'numRows'.
  RowPartitions(int32_t numRows, memory::MemoryPool& pool);

  /// Appends 'partitions' to the end of 'this'. Throws if adding more than the
  /// capacity given at construction.
  void appendPartitions(folly::Range<const uint8_t*> partitions);

  auto& allocation() const {
    return allocation_;
  }

  int32_t size() const {
    return size_;
  }

 private:
  const int32_t capacity_;

  // Number of partition numbers added.
  int32_t size_{0};

  // Partition numbers. 1 byte each.
  memory::Allocation allocation_;
};

/// Packed representation of offset, null byte offset and null mask for
/// a column inside a RowContainer.
class RowColumn {
 public:
  /// Used as null offset for a non-null column.
  static constexpr int32_t kNotNullOffset = -1;

  RowColumn(int32_t offset, int32_t nullOffset)
      : packedOffsets_(PackOffsets(offset, nullOffset)) {}

  int32_t offset() const {
    return packedOffsets_ >> 32;
  }

  int32_t nullByte() const {
    return static_cast<uint32_t>(packedOffsets_) >> 8;
  }

  uint8_t nullMask() const {
    return packedOffsets_ & 0xff;
  }

 private:
  static uint64_t PackOffsets(int32_t offset, int32_t nullOffset) {
    if (nullOffset == kNotNullOffset) {
      // If the column is not nullable, The low word is 0, meaning
      // that a null check will AND 0 to the 0th byte of the row,
      // which is always false and always safe to do.
      return static_cast<uint64_t>(offset) << 32;
    }
    return (1UL << (nullOffset & 7)) | ((nullOffset & ~7UL) << 5) |
        static_cast<uint64_t>(offset) << 32;
  }

  const uint64_t packedOffsets_;
};

/// Collection of rows for aggregation, hash join, order by.
class RowContainer {
 public:
  static constexpr uint64_t kUnlimited = std::numeric_limits<uint64_t>::max();
  using Eraser = std::function<void(folly::Range<char**> rows)>;

  /// 'keyTypes' gives the type of row and use 'allocator' for bulk
  /// allocation.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      memory::MemoryPool* FOLLY_NONNULL pool)
      : RowContainer(keyTypes, std::vector<TypePtr>{}, pool) {}

  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      memory::MemoryPool* FOLLY_NONNULL pool)
      : RowContainer(
            keyTypes,
            dependentTypes,
            false, // useListRowIndex
            pool) {}

  // Convenience overload to enable fast row listing via cached pointers.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      bool useListRowIndex,
      memory::MemoryPool* FOLLY_NONNULL pool)
      : RowContainer(
            keyTypes,
            true, // nullableKeys
            std::vector<Accumulator>{},
            dependentTypes,
            false, // hasNext
            false, // isJoinBuild
            false, // hasProbedFlag
            false, // hasNormalizedKey
            useListRowIndex,
            pool) {}

  ~RowContainer();

  static int32_t combineAlignments(int32_t a, int32_t b);

  /// 'keyTypes' gives the type of the key of each row. For a group by,
  /// order by or right outer join build side these may be
  /// nullable. 'nullableKeys' specifies if these have a null flag.
  /// 'aggregates' is a vector of Aggregate for a group by payload,
  /// empty otherwise. 'DependentTypes' gives the types of non-key
  /// columns for a hash join build side or an order by. 'hasNext' is
  /// true for a hash join build side where keys can be
  /// non-unique. 'isJoinBuild' is true for hash join build sides. This
  /// implies that hashing of keys ignores null keys even if these were
  /// allowed. 'hasProbedFlag' indicates that an extra bit is reserved
  /// for a probed state of a full or right outer join, or means a row follows
  /// an equal row in sorted spilled streams for spilled aggregation.
  /// 'hasNormalizedKey' specifies that an extra word is left below each row for
  /// a normalized key that collapses all parts into one word for faster
  /// comparison. The bulk allocation is done from 'allocator'.
  /// ContainerRowSerde is used for serializing complex type values into the
  /// container. 'stringAllocator' allows sharing the variable length data arena
  /// with another RowContainer. This is needed for spilling where the same
  /// aggregates are used for reading one container and merging into another.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      bool nullableKeys,
      const std::vector<Accumulator>& accumulators,
      const std::vector<TypePtr>& dependentTypes,
      bool hasNext,
      bool isJoinBuild,
      bool hasProbedFlag,
      bool hasNormalizedKey,
      bool useListRowIndex,
      memory::MemoryPool* FOLLY_NONNULL pool,
      std::shared_ptr<HashStringAllocator> stringAllocator = nullptr);

  /// Allocates a new row and initializes possible aggregates to null.
  char* FOLLY_NONNULL newRow();

  uint32_t rowSize(const char* FOLLY_NONNULL row) const {
    return fixedRowSize_ +
        (rowSizeOffset_
             ? *reinterpret_cast<const uint32_t*>(row + rowSizeOffset_)
             : 0);
  }

  /// Sets all fields, aggregates, keys and dependents to null. Used when making
  /// a row with uninitialized keys for aggregates with no-op partial
  /// aggregation.
  void setAllNull(char* FOLLY_NONNULL row) {
    if (!nullOffsets_.empty()) {
      memset(row + nullByte(nullOffsets_[0]), 0xff, initialNulls_.size());
      bits::clearBit(row, freeFlagOffset_);
    }
  }

  /// The row size excluding any out-of-line stored variable length values.
  int32_t fixedRowSize() const {
    return fixedRowSize_;
  }

  // Adds 'rows' to the free rows list and frees any associated
  // variable length data.
  void eraseRows(folly::Range<char**> rows);

  /// Copies elements of 'rows' where the char* points to a row inside 'this' to
  /// 'result' and returns the number copied. 'result' should have space for
  /// 'rows.size()'.
  int32_t findRows(folly::Range<char**> rows, char** result);
  // Adds 'rows' to the free rows list and frees any associated
  // variable length data; but do not touch key columns.
  void eraseRowsSkippingKeys(folly::Range<char**> rows);

  void incrementRowSize(char* FOLLY_NONNULL row, uint64_t bytes) {
    uint32_t* ptr = reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
    uint64_t size = *ptr + bytes;
    *ptr = std::min<uint64_t>(size, std::numeric_limits<uint32_t>::max());
  }

  /// Initialize row. 'reuse' specifies whether the 'row' is reused or not. If
  /// it is reused, it will free memory associated with the row elsewhere (such
  /// as in HashStringAllocator).
  /// Note: Fields of the row are not zero-initialized. If the row contains
  /// variable-width fields, the caller must populate these fields by calling
  /// 'store' or initialize them to zero by calling 'initializeFields'.
  char* initializeRow(char* row, bool reuse);

  /// Zero out all the fields of the 'row'.
  void initializeFields(char* row) {
    ::memset(row, 0, fixedRowSize_);
  }
  // Store a single row id into the row at the reserved offset for hybrid design
  void storeSingleRowId(uint64_t& value, char* FOLLY_NONNULL row) {
    *reinterpret_cast<int64_t*>(row + rowIdOffset_) = value;
  }

  void store(const RowVectorPtr& input);

  /// Stores the 'index'th value in 'decoded' into 'row' at 'columnIndex'.
  void store(
      const DecodedVector& decoded,
      vector_size_t index,
      char* FOLLY_NONNULL row,
      int32_t column);

  template <TypeKind Kind>
  void storeColumn(
      const DecodedVector& decoded,
      size_t size,
      const std::vector<char*>& rows, // span
      size_t column) {
    auto numKeys = keyTypes_.size();
    bool isKey = column < numKeys;
    if (isKey && !nullableKeys_) {
      auto off = offsets_[column];

      for (auto r = 0; r < size; ++r) {
        storeNoNulls<Kind>(decoded, r, isKey, rows[r], off);
      }
    } else {
      BOLT_DCHECK(column < keyTypes_.size() || accumulators_.empty());
      auto rowColumn = rowColumns_[column];
      auto off = rowColumn.offset();
      auto nullByte = rowColumn.nullByte();
      auto mask = rowColumn.nullMask();

      for (auto r = 0; r < size; ++r) {
        storeWithNulls<Kind>(decoded, r, isKey, rows[r], off, nullByte, mask);
      }
    }
  }

  HashStringAllocator& stringAllocator() {
    return *stringAllocator_;
  }

  const std::shared_ptr<HashStringAllocator>& stringAllocatorShared() {
    return stringAllocator_;
  }

  /// Returns the number of used rows in 'this'. This is the number of rows a
  /// RowContainerIterator would access.
  int64_t numRows() const {
    return numRows_;
  }

  /// Copy key and dependent columns into a flat VARBINARY vector. All columns
  /// of a row are copied into a single buffer. The format of that buffer is an
  /// implementation detail. The data can be loaded back into the RowContainer
  /// using 'storeSerializedRow'.
  ///
  /// Used for spilling as it is more efficient than converting from row to
  /// columnar format.
  void extractSerializedRows(
      folly::Range<char**> rows,
      const VectorPtr& result);

  /// Copies serialized row produced by 'extractSerializedRow' into the
  /// container.
  void storeSerializedRow(
      const FlatVector<StringView>& vector,
      vector_size_t index,
      char* row);

  // copy one row from other memory pool
  void copySerializedRow(char* const, RowFormatInfo* const);

  /// Copies the values at 'col' into 'result' (starting at 'resultOffset')
  /// for the 'numRows' rows pointed to by 'rows'. If a 'row' is null, sets
  /// corresponding row in 'result' to null.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      RowColumn col,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  /// Copies the values at 'col' into 'result' for the 'numRows' rows pointed to
  /// by 'rows'. If an entry in 'rows' is null, sets corresponding row in
  /// 'result' to null.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      RowColumn col,
      const VectorPtr& result,
      bool exactSize = false) {
    extractColumn(rows, numRows, col, 0, result, exactSize);
  }

  /// Copies the values from the array pointed to by 'rows' at 'col' into
  /// 'result' (starting at 'resultOffset') for the rows at positions in
  /// the 'rowNumbers' array. If a 'row' is null, sets corresponding row in
  /// 'result' to null. The positions in 'rowNumbers' array can repeat and also
  /// appear out of order. If rowNumbers has a negative value, then the
  /// corresponding row in 'result' is set to null.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      RowColumn col,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  /// Sets in result all locations with null values in col for rows (for numRows
  /// number of rows).
  static void extractNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      RowColumn col,
      const BufferPtr& result);

  /// Copies the values at 'columnIndex' into 'result' for the 'numRows' rows
  /// pointed to by 'rows'. If an entry in 'rows' is null, sets corresponding
  /// row in 'result' to null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      const VectorPtr& result,
      bool exactSize = false) {
    extractColumn(rows, numRows, columnAt(columnIndex), result, exactSize);
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the 'numRows' rows pointed to by 'rows'. If an
  /// entry in 'rows' is null, sets corresponding row in 'result' to null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) {
    extractColumn(
        rows, numRows, columnAt(columnIndex), resultOffset, result, exactSize);
  }

  /// Copies the values at 'columnIndex' at positions in the 'rowNumbers' array
  /// for the rows pointed to by 'rows'. The values are copied into the 'result'
  /// vector at the offset pointed by 'resultOffset'. If an entry in 'rows'
  /// is null, sets corresponding row in 'result' to null. The positions in
  /// 'rowNumbers' array can repeat and also appear out of order. If rowNumbers
  /// has a negative value, then the corresponding row in 'result' is set to
  /// null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t columnIndex,
      const vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false) {
    extractColumn(
        rows,
        rowNumbers,
        columnAt(columnIndex),
        resultOffset,
        result,
        exactSize);
  }

  /// Sets in result all locations with null values in columnIndex for rows.
  void extractNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      const BufferPtr& result) {
    extractNulls(rows, numRows, columnAt(columnIndex), result);
  }

  /// Copies the 'probed' flags for the specified rows into 'result'.
  /// The 'result' is expected to be flat vector of type boolean.
  /// For rows with null keys, sets null in 'result' if 'setNullForNullKeysRow'
  /// is true and false otherwise. For rows with 'false' probed flag, sets null
  /// in 'result' if 'setNullForNonProbedRow' is true and false otherwise. This
  /// is used for null aware and regular right semi project join types.
  void extractProbedFlags(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      bool setNullForNullKeysRow,
      bool setNullForNonProbedRow,
      const VectorPtr& result);

  static inline int32_t nullByte(int32_t nullOffset) {
    return nullOffset / 8;
  }

  static inline uint8_t nullMask(int32_t nullOffset) {
    return 1 << (nullOffset & 7);
  }

  /// No tsan because probed flags may have been set by a different thread.
  /// There is a barrier but tsan does not know this.
  enum class ProbeType { kAll, kProbed, kNotProbed };

  /// Fast path for `listRows` that returns `rowPointers_` directly. Used by
  /// `SortBuffer` and `SortInputSpiller`, so it skips checking the free and
  /// probe flags.
  int32_t listRowsFast(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) const {
    int32_t count = 0;
    while (count < maxRows &&
           iter->listRowCursor < static_cast<int32_t>(rowPointers_.size())) {
      rows[count++] = rowPointers_[iter->listRowCursor];
      ++iter->listRowCursor;
    }
    return count;
  }

  template <ProbeType probeType>
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  int32_t
  listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    int32_t count = 0;
    uint64_t totalBytes = 0;
    auto numAllocations = rows_.numRanges();
    if (iter->allocationIndex == 0 && iter->rowOffset == 0) {
      iter->normalizedKeysLeft = numRowsWithNormalizedKey_;
      iter->normalizedKeySize = originalNormalizedKeySize_;
    }
    int32_t rowSize = fixedRowSize_ +
        (iter->normalizedKeysLeft > 0 ? originalNormalizedKeySize_ : 0);
    for (auto i = iter->allocationIndex; i < numAllocations; ++i) {
      auto range = rows_.rangeAt(i);
      auto* data =
          range.data() + memory::alignmentPadding(range.data(), alignment_);
      auto limit = range.size() -
          (reinterpret_cast<uintptr_t>(data) -
           reinterpret_cast<uintptr_t>(range.data()));
      auto row = iter->rowOffset;
      while (row + rowSize <= limit) {
        rows[count++] = data + row +
            (iter->normalizedKeysLeft > 0 ? originalNormalizedKeySize_ : 0);
        BOLT_DCHECK_EQ(
            reinterpret_cast<uintptr_t>(rows[count - 1]) % alignment_, 0);
        row += rowSize;
        auto newTotalBytes = totalBytes + rowSize;
        if (--iter->normalizedKeysLeft == 0) {
          rowSize -= originalNormalizedKeySize_;
        }
        if (bits::isBitSet(rows[count - 1], freeFlagOffset_)) {
          --count;
          continue;
        }
        if constexpr (probeType == ProbeType::kNotProbed) {
          if (bits::isBitSet(rows[count - 1], probedFlagOffset_)) {
            --count;
            continue;
          }
        }
        if constexpr (probeType == ProbeType::kProbed) {
          if (not(bits::isBitSet(rows[count - 1], probedFlagOffset_))) {
            --count;
            continue;
          }
        }
        totalBytes = newTotalBytes;
        if (rowSizeOffset_) {
          totalBytes += variableRowSize(rows[count - 1]);
        }
        if (count == maxRows || totalBytes > maxBytes) {
          iter->rowOffset = row;
          iter->allocationIndex = i;
          return count;
        }
      }
      iter->rowOffset = 0;
    }
    iter->allocationIndex = std::numeric_limits<int32_t>::max();
    return count;
  }

  /// Extracts up to 'maxRows' rows starting at the position of 'iter'. A
  /// default constructed or reset iter starts at the beginning. Returns the
  /// number of rows written to 'rows'. Returns 0 when at end. Stops after the
  /// total size of returned rows exceeds maxBytes.
  int32_t listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    return listRows<ProbeType::kAll>(iter, maxRows, maxBytes, rows);
  }

  int32_t listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    if (useListRowIndex_) {
      return listRowsFast(iter, maxRows, rows);
    }
    return listRows<ProbeType::kAll>(iter, maxRows, kUnlimited, rows);
  }

  /// Sets 'probed' flag for the specified rows. Used by the right and
  /// full join to mark build-side rows that matches join
  /// condition. 'rows' may contain duplicate entries for the cases
  /// where single probe row matched multiple build rows. In case of
  /// the full join, 'rows' may include null entries that correspond
  /// to probe rows with no match. No tsan because any thread can set
  /// this without synchronization. There is a barrier between setting
  /// and reading.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  void
  setProbedFlag(char* FOLLY_NONNULL* FOLLY_NONNULL rows, int32_t numRows);

  /// Returns true if 'row' at 'column' equals the value at 'index' in
  /// 'decoded'. 'mayHaveNulls' specifies if nulls need to be checked. This is a
  /// fast path for compare().
  template <bool mayHaveNulls>
  bool equals(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index);

  /// Compares the value at 'column' in 'row' with the value at 'index' in
  /// 'decoded'. Returns 0 for equal, < 0 for 'row' < 'decoded', > 0 otherwise.
  int32_t compare(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  /// Compares the value at 'columnIndex' between 'left' and 'right'. Returns
  /// 0 for equal, < 0 for left < right, > 0 otherwise.
  int32_t compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      int32_t columnIndex,
      CompareFlags flags = CompareFlags());

  /// Compares the value between 'left' at 'leftIndex' and 'right' and
  /// 'rightIndex'. Returns 0 for equal, < 0 for left < right, > 0 otherwise.
  /// Both columns should have the same type.
  int32_t compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      int leftColumnIndex,
      int rightColumnIndex,
      CompareFlags flags = CompareFlags());

  /// Allows get/set of the normalized key. If normalized keys are used, they
  /// are stored in the word immediately below the hash table row.
  static inline normalized_key_t& normalizedKey(char* FOLLY_NONNULL group) {
    return reinterpret_cast<normalized_key_t*>(group)[-1];
  }

  void disableNormalizedKeys() {
    normalizedKeySize_ = 0;
  }

  FOLLY_ALWAYS_INLINE RowColumn columnAt(int32_t index) const {
    return rowColumns_[index];
  }

  const std::vector<RowColumn>& columns() const {
    return rowColumns_;
  }

  /// Bit offset of the probed flag for a full or right outer join  payload.
  /// 0 if not applicable.
  int32_t probedFlagOffset() const {
    return probedFlagOffset_;
  }

  /// Returns the offset of a uint32_t row size or 0 if the row has no variable
  /// width fields or accumulators.
  int32_t rowSizeOffset() const {
    return rowSizeOffset_;
  }

  /// For a hash join table with possible non-unique entries, the offset of the
  /// pointer to the next row with the same key. 0 if keys are guaranteed
  /// unique, e.g. for a group by or semijoin build.
  int32_t nextOffset() const {
    return nextOffset_;
  }

  /// Hashes the values of 'columnIndex' for 'rows'.  If 'mix' is true, mixes
  /// the hash with the existing value in 'result'.
  void hash(
      int32_t columnIndex,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* FOLLY_NONNULL result);

  uint64_t allocatedBytes() const {
    return rows_.allocatedBytes() + stringAllocator_->retainedSize();
  }

  uint64_t usedBytes() const {
    return rows_.allocatedBytes() - rows_.freeBytes() +
        stringAllocator_->retainedSize() - stringAllocator_->freeSpace();
  }

  /// Returns the number of fixed size rows that can be allocated without
  /// growing the container and the number of unused bytes of reserved storage
  /// for variable length data.
  std::pair<uint64_t, uint64_t> freeSpace() const {
    return std::make_pair<uint64_t, uint64_t>(
        rows_.freeBytes() / fixedRowSize_ + numFreeRows_,
        stringAllocator_->freeSpace());
  }

  /// Returns the average size of rows in bytes stored in this container.
  std::optional<int64_t> estimateRowSize() const;

  /// Returns a cap on extra memory that may be needed when adding 'numRows'
  /// and variableLengthBytes of out-of-line variable length data.
  int64_t sizeIncrement(vector_size_t numRows, int64_t variableLengthBytes)
      const;

  /// Resets the state to be as after construction. Frees memory for payload.
  void clear();

  int32_t compareRows(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const std::vector<CompareFlags>& flags = {}) {
    BOLT_DCHECK(flags.empty() || flags.size() == keyTypes_.size());
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      auto result =
          compare(left, right, i, flags.empty() ? CompareFlags() : flags[i]);
      if (result) {
        return result;
      }
    }
    return 0;
  }

#ifdef ENABLE_BOLT_JIT
  static bool JITable(const std::vector<TypePtr>& keyTypes) {
    return std::all_of(std::begin(keyTypes), std::end(keyTypes), [](auto&& t) {
      return t->kind() <= TypeKind::ROW;
    });
  }

  // compiledModule, RowRowCmpFnName,
  std::tuple<bytedance::bolt::jit::CompiledModuleSP, std::string>
  codegenCompare(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<CompareFlags>& flags,
      bytedance::bolt::jit::CmpType cmpType,
      bool hasNullKeys,
      const std::vector<column_index_t>& sortKeyIndexs = kEmptySortKeyIndexes);

  std::tuple<bytedance::bolt::jit::CompiledModuleSP, std::string>
  codegenRowEqVectors(const std::vector<TypePtr>& keyTypes, bool haveNulls);

#endif

  const std::vector<char*, StlAllocator<char*>>& testingRowPointers() const {
    return rowPointers_;
  }

  memory::MemoryPool* FOLLY_NONNULL pool() const {
    return stringAllocator_->pool();
  }

  /// Returns the types of all non-aggregate columns of 'this', keys first.
  const auto& columnTypes() const {
    return types_;
  }

  const auto& keyTypes() const {
    return keyTypes_;
  }

  const auto& keyIndices() const {
    return keyIndices_;
  }

  const std::vector<Accumulator>& accumulators() const {
    return accumulators_;
  }

  bool hasVariableAccumulator() const {
    return hasVariableAccumulator_;
  }

  const HashStringAllocator& stringAllocator() const {
    return *stringAllocator_;
  }

  /// Checks that row and free row counts match and that free list membership is
  /// consistent with free flag.
  void checkConsistency();

  static FOLLY_ALWAYS_INLINE bool
  isNullAt(const char* FOLLY_NONNULL row, int32_t nullByte, uint8_t nullMask) {
    return (row[nullByte] & nullMask) != 0;
  }

  static inline bool isNullAt(const char* row, RowColumn rowColumn) {
    return (row[rowColumn.nullByte()] & rowColumn.nullMask()) != 0;
  }

  /// Creates a container to store a partition number for each row in this row
  /// container. This is used by parallel join build which is responsible for
  /// filling this. This function also marks this row container as immutable
  /// after this call, we expect the user only call this once.
  std::unique_ptr<RowPartitions> createRowPartitions(memory::MemoryPool& pool);

  /// Retrieves rows from 'iterator' whose partition equals 'partition'. Writes
  /// up to 'maxRows' pointers to the rows in 'result'. 'rowPartitions' contains
  /// the partition number of each row in this container. The function returns
  /// the number of rows retrieved, 0 when no more rows are found. 'iterator' is
  /// expected to be in initial state on first call.
  int32_t listPartitionRows(
      RowContainerIterator& iterator,
      uint8_t partition,
      int32_t maxRows,
      const RowPartitions& rowPartitions,
      char* FOLLY_NONNULL* FOLLY_NONNULL result);

  /// Advances 'iterator' by 'numRows'. The current row after skip is
  /// in iter.currentRow(). This is null if past end. Public for testing.
  void skip(RowContainerIterator& iterator, int32_t numRows);

  bool testingMutable() const {
    return mutable_;
  }

  bool checkFree() const {
    return checkFree_;
  }

  int alignment() const {
    return alignment_;
  }

  /// Returns a summary of the container: key types, dependent types, number of
  /// accumulators and number of rows.
  std::string toString() const;

  /// Returns a string representation of the specified row in the same format as
  /// BaseVector::toString(index).
  std::string toString(const char* row) const;

  template <typename T>
  static inline T valueAt(const char* FOLLY_NONNULL group, int32_t offset) {
    return *reinterpret_cast<const T*>(group + offset);
  }

  template <typename T>
  static inline T& valueAt(char* FOLLY_NONNULL group, int32_t offset) {
    return *reinterpret_cast<T*>(group + offset);
  }

  const std::vector<RowColumn>& getRowColumn() const {
    return rowColumns_;
  }

  static int32_t compareStringAsc(StringView left, StringView right);
  static std::unique_ptr<ByteInputStream> prepareRead(
      const char* row,
      int32_t offset);
  /// Returns the size of a string or complex types value stored in the
  /// specified row and column.
  int32_t variableSizeAt(const char* row, column_index_t column);

 private:
  static constexpr int32_t kNextFreeOffset = 0;

  static const std::vector<column_index_t> kEmptySortKeyIndexes;

  /// Copies a string or complex type value from the specified row and column
  /// @return The number of bytes written to 'destination' including the 4 bytes
  /// of the size.
  int32_t
  extractVariableSizeAt(const char* row, column_index_t column, char* output);

  /// Copies a string or complex type value from 'data' into the specified row
  /// and column. Expects first 4 bytes in 'data' to contain the size of the
  /// string or complex value.
  /// @return The number of bytes read from 'data': 4 bytes for size + that many
  /// bytes.
  int32_t
  storeVariableSizeAt(const char* data, char* row, column_index_t column);

  template <TypeKind Kind>
  static void extractColumnTyped(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      const VectorPtr& result,
      bool exactSize) {
    if (rowNumbers.size() > 0) {
      extractColumnTypedInternal<true, Kind>(
          rows,
          rowNumbers,
          rowNumbers.size(),
          column,
          resultOffset,
          result,
          exactSize);
    } else {
      extractColumnTypedInternal<false, Kind>(
          rows, rowNumbers, numRows, column, resultOffset, result, exactSize);
    }
  }

  template <bool useRowNumbers, TypeKind Kind>
  static void extractColumnTypedInternal(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      const VectorPtr& result,
      bool exactSize) {
    // Resize the result vector before all copies.
    result->resize(numRows + resultOffset);

    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      extractComplexType<useRowNumbers>(
          rows, rowNumbers, numRows, column, resultOffset, result);
      return;
    }
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto* flatResult = result->as<FlatVector<T>>();
    auto nullMask = column.nullMask();
    auto offset = column.offset();
    if (!nullMask) {
      extractValuesNoNulls<useRowNumbers, T>(
          rows,
          rowNumbers,
          numRows,
          offset,
          resultOffset,
          flatResult,
          exactSize);
    } else {
      extractValuesWithNulls<useRowNumbers, T>(
          rows,
          rowNumbers,
          numRows,
          offset,
          column.nullByte(),
          nullMask,
          resultOffset,
          flatResult,
          exactSize);
    }
  }

  char* FOLLY_NULLABLE& nextFree(char* FOLLY_NONNULL row) {
    return *reinterpret_cast<char**>(row + kNextFreeOffset);
  }

  uint32_t& variableRowSize(char* FOLLY_NONNULL row) {
    DCHECK(rowSizeOffset_);
    return *reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
  }

  template <TypeKind Kind>
  inline void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      bool isKey,
      char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask) {
    using T = typename TypeTraits<Kind>::NativeType;
    if (decoded.isNullAt(index)) {
      row[nullByte] |= nullMask;
      // For quick comparing
      // for null value, set as limits<T>::max() so that
      // no need to check nullity in some cases.
      if constexpr (std::is_arithmetic_v<T>) {
        *reinterpret_cast<T*>(row + offset) = std::numeric_limits<T>::max();
      } else if constexpr (std::is_same_v<T, StringView>) {
        // See StringView::compare()
        // so that null StringView is the max.
        *reinterpret_cast<T*>(row + offset) = StringView();
        reinterpret_cast<uint32_t*>(row + offset)[1] =
            std::numeric_limits<uint32_t>::max();
      } else {
        // Do not leave an uninitialized value in the case of a
        // null. This is an error with valgrind/asan.
        *reinterpret_cast<T*>(row + offset) = T();
      }
      return;
    }
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(row[rowSizeOffset_], *stringAllocator_);
      stringAllocator_->copyMultipart(decoded.valueAt<T>(index), row, offset);
    } else {
      *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(index);
    }
  }

  template <TypeKind Kind>
  inline void storeNoNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      bool isKey,
      char* FOLLY_NONNULL group,
      int32_t offset) {
    using T = typename TypeTraits<Kind>::NativeType;
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(group[rowSizeOffset_], *stringAllocator_);
      stringAllocator_->copyMultipart(decoded.valueAt<T>(index), group, offset);
    } else {
      *reinterpret_cast<T*>(group + offset) = decoded.valueAt<T>(index);
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesWithNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      bool exactSize) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    [[maybe_unused]] auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
        bits::setNull(nulls, resultIndex, true);
      } else {
        bits::setNull(nulls, resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(
              valueAt<StringView>(row, offset), result, resultIndex, exactSize);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesNoNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      bool exactSize) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    [[maybe_unused]] auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
      } else {
        result->setNull(resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(
              valueAt<StringView>(row, offset), result, resultIndex, exactSize);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  template <TypeKind Kind>
  void hashTyped(
      const Type* FOLLY_NONNULL type,
      RowColumn column,
      bool nullable,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* FOLLY_NONNULL result);

  template <TypeKind Kind>
  FLATTEN inline bool equalsWithNulls(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      const DecodedVector& decoded,
      vector_size_t index) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool rowIsNull = isNullAt(row, nullByte, nullMask);
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull || indexIsNull) {
      return rowIsNull == indexIsNull;
    }
    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, offset, decoded, index) == 0;
    }
    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      return compareStringAsc(
                 valueAt<StringView>(row, offset), decoded, index) == 0;
    }
    auto left = decoded.valueAt<T>(index);
    auto right = valueAt<T>(row, offset);
    return comparePrimitiveAsc<T>(left, right) == 0;
  }

  template <TypeKind Kind>
  inline bool equalsNoNulls(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index) {
    using T = typename KindToFlatVector<Kind>::HashRowType;

    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, offset, decoded, index) == 0;
    }
    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      return compareStringAsc(
                 valueAt<StringView>(row, offset), decoded, index) == 0;
    }

    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto left = valueAt<T>(row, offset);
    auto right = decoded.valueAt<T>(index);
    return comparePrimitiveAsc<T>(left, right) == 0;
  }

  template <TypeKind Kind>
  inline int compare(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool rowIsNull = isNullAt(row, column.nullByte(), column.nullMask());
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull) {
      return indexIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (indexIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, column.offset(), decoded, index, flags);
    }
    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto result = compareStringAsc(
          valueAt<StringView>(row, column.offset()), decoded, index);
      return flags.ascending ? result : result * -1;
    }
    auto left = valueAt<T>(row, column.offset());
    auto right = decoded.valueAt<T>(index);
    auto result = comparePrimitiveAsc<T>(left, right);
    return flags.ascending ? result : result * -1;
  }

  template <TypeKind Kind>
  inline int compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      RowColumn leftColumn,
      RowColumn rightColumn,
      CompareFlags flags) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool leftIsNull =
        isNullAt(left, leftColumn.nullByte(), leftColumn.nullMask());
    bool rightIsNull =
        isNullAt(right, rightColumn.nullByte(), rightColumn.nullMask());
    if (leftIsNull) {
      return rightIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (rightIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }

    auto leftOffset = leftColumn.offset();
    auto rightOffset = rightColumn.offset();
    if constexpr (
        Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(
          left, right, type, leftOffset, rightOffset, flags);
    }
    if constexpr (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto leftValue = valueAt<StringView>(left, leftOffset);
      auto rightValue = valueAt<StringView>(right, rightOffset);
      auto result = compareStringAsc(leftValue, rightValue);
      return flags.ascending ? result : result * -1;
    }

    auto leftValue = valueAt<T>(left, leftOffset);
    auto rightValue = valueAt<T>(right, rightOffset);
    auto result = comparePrimitiveAsc<T>(leftValue, rightValue);
    return flags.ascending ? result : result * -1;
  }

  template <TypeKind Kind>
  inline int compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      RowColumn column,
      CompareFlags flags) {
    return compare<Kind>(left, right, type, column, column, flags);
  }

  template <typename T>
  static inline int comparePrimitiveAsc(const T& left, const T& right) {
    if constexpr (std::is_floating_point<T>::value) {
      bool isLeftNan = std::isnan(left);
      bool isRightNan = std::isnan(right);
      if (UNLIKELY(isLeftNan)) {
        return isRightNan ? 0 : 1;
      }
      if (UNLIKELY(isRightNan)) {
        return -1;
      }
    }
    return left < right ? -1 : left == right ? 0 : 1;
  }

  void storeComplexType(
      const DecodedVector& decoded,
      vector_size_t index,
      bool isKey,
      char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte = 0,
      uint8_t nullMask = 0);

  template <bool useRowNumbers>
  static void extractComplexType(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      const VectorPtr& result) {
    auto nullByte = column.nullByte();
    auto nullMask = column.nullMask();
    auto offset = column.offset();

    BOLT_DCHECK_LE(numRows + resultOffset, result->size());
    for (int i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (!row || isNullAt(row, nullByte, nullMask)) {
        result->setNull(resultIndex, true);
      } else {
        auto stream = prepareRead(row, offset);
        ContainerRowSerde::deserialize(*stream, resultIndex, result.get());
      }
    }
  }

  static void extractString(
      StringView value,
      FlatVector<StringView>* FOLLY_NONNULL values,
      vector_size_t index,
      bool exactSize);

  static int32_t compareStringAsc(
      StringView left,
      const DecodedVector& decoded,
      vector_size_t index);

  int32_t compareComplexType(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  int32_t compareComplexType(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      int32_t offset,
      CompareFlags flags);

  int32_t compareComplexType(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      int32_t leftOffset,
      int32_t rightOffset,
      CompareFlags flags = CompareFlags());

  // Free variable-width fields at column `column_index` associated with the
  // 'rows', and if 'checkFree_' is true, zero out complex-typed field in
  // 'rows'. `FieldType` is the type of data representation of the fields in
  // row, and can be one of StringView(represents VARCHAR) and
  // std::string_view(represents ARRAY, MAP or ROW).
  template <typename FieldType>
  void freeVariableWidthFieldsAtColumn(
      size_t column_index,
      folly::Range<char**> rows) {
    static_assert(
        std::is_same_v<FieldType, StringView> ||
        std::is_same_v<FieldType, std::string_view>);

    const auto column = columnAt(column_index);
    for (auto row : rows) {
      if (isNullAt(row, column.nullByte(), column.nullMask())) {
        continue;
      }

      auto& view = valueAt<FieldType>(row, column.offset());
      if constexpr (std::is_same_v<FieldType, StringView>) {
        if (view.isInline()) {
          continue;
        }
      } else {
        if (view.empty()) {
          continue;
        }
      }
      stringAllocator_->free(HashStringAllocator::headerOf(view.data()));
      if (checkFree_) {
        view = FieldType();
      }
    }
  }

  // Free any variable-width fields associated with the 'rows' and zero out
  // complex-typed field in 'rows'.
  void freeVariableWidthFields(folly::Range<char**> rows);

  // Free any aggregates associated with the 'rows'.
  void freeAggregates(folly::Range<char**> rows);

  const bool checkFree_ = false;

  const std::vector<TypePtr> keyTypes_;
  std::vector<column_index_t> keyIndices_;
  const bool nullableKeys_;
  const bool isJoinBuild_;

  // Indicates if we can add new row to this row container. It is set to false
  // after user calls 'getRowPartitions()' to create 'rowPartitions' object for
  // parallel join build.
  bool mutable_{true};

  std::vector<Accumulator> accumulators_;

  bool usesExternalMemory_ = false;
  // Types of non-aggregate columns. Keys first. Corresponds pairwise
  // to 'typeKinds_' and 'rowColumns_'.
  std::vector<TypePtr> types_;
  std::vector<TypeKind> typeKinds_;
  int32_t nextOffset_ = 0;
  // Bit position of null bit  in the row. 0 if no null flag. Order is keys,
  // accumulators, dependent.
  std::vector<int32_t> nullOffsets_;
  // Position of field or accumulator. Corresponds 1:1 to 'nullOffset_'.
  std::vector<int32_t> offsets_;
  // Position of row ID field, used in hybrid design.
  int32_t rowIdOffset_;
  // Offset and null indicator offset of non-aggregate fields as a single word.
  // Corresponds pairwise to 'types_'.
  std::vector<RowColumn> rowColumns_;
  // Bit offset of the probed flag for a full or right outer join  payload. 0 if
  // not applicable.
  int32_t probedFlagOffset_ = 0;

  // Bit position of free bit.
  int32_t freeFlagOffset_ = 0;
  int32_t rowSizeOffset_ = 0;

  bool hasVariableAccumulator_{false};

  int32_t fixedRowSize_;
  // True if normalized keys are enabled in initial state.
  const bool hasNormalizedKeys_;
  // Whether to use cached row pointers for fast listing.
  const bool useListRowIndex_;
  // The count of entries that have an extra normalized_key_t before the
  // start.
  int64_t numRowsWithNormalizedKey_ = 0;
  // This is the original normalized key size regardless of whether
  // disableNormalizedKeys() is called or not.
  int originalNormalizedKeySize_;
  // Extra bytes to reserve before  each added row for a normalized key. Set to
  // 0 after deciding not to use normalized keys.
  int normalizedKeySize_;
  // Copied over the null bits of each row on initialization. Keys are
  // not null, aggregates are null.
  std::vector<uint8_t> initialNulls_;
  uint64_t numRows_ = 0;
  // Head of linked list of free rows.
  char* FOLLY_NULLABLE firstFreeRow_ = nullptr;
  uint64_t numFreeRows_ = 0;

  memory::AllocationPool rows_;
  std::shared_ptr<HashStringAllocator> stringAllocator_;
  std::vector<char*, StlAllocator<char*>> rowPointers_;

  int alignment_ = 1;
};

template <>
inline int128_t RowContainer::valueAt<int128_t>(
    const char* FOLLY_NONNULL group,
    int32_t offset) {
  return HugeInt::deserialize(group + offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, isKey, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, isKey, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, isKey, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool isKey,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, isKey, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::HUGEINT>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool /*isKey*/,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  if (decoded.isNullAt(index)) {
    row[nullByte] |= nullMask;
    // Do not leave an uninitialized value in the case of a
    // null. This is an error with valgrind/asan.
    memset(row + offset, 0, sizeof(int128_t));
    return;
  }
  HugeInt::serialize(decoded.valueAt<int128_t>(index), row + offset);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::HUGEINT>(
    const DecodedVector& decoded,
    vector_size_t index,
    bool /*isKey*/,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  HugeInt::serialize(decoded.valueAt<int128_t>(index), row + offset);
}

template <>
inline void RowContainer::extractColumnTyped<TypeKind::OPAQUE>(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL /*rows*/,
    folly::Range<const vector_size_t*> /*rowNumbers*/,
    int32_t /*numRows*/,
    RowColumn /*column*/,
    int32_t /*resultOffset*/,
    const VectorPtr& /*result*/,
    bool exactSize /*exactSize*/) {
  BOLT_UNSUPPORTED("RowContainer doesn't support values of type OPAQUE");
}

inline void RowContainer::extractColumn(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      {},
      numRows,
      column,
      resultOffset,
      result,
      exactSize);
}

inline void RowContainer::extractColumn(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    folly::Range<const vector_size_t*> rowNumbers,
    RowColumn column,
    int32_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      rowNumbers,
      rowNumbers.size(),
      column,
      resultOffset,
      result,
      exactSize);
}

inline void RowContainer::extractNulls(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    int32_t numRows,
    RowColumn column,
    const BufferPtr& result) {
  BOLT_DCHECK(result->size() >= bits::nbytes(numRows));
  auto* rawResult = result->asMutable<uint64_t>();
  bits::fillBits(rawResult, 0, numRows, false);

  auto nullMask = column.nullMask();
  if (!nullMask) {
    return;
  }

  auto nullByte = column.nullByte();
  for (int32_t i = 0; i < numRows; ++i) {
    const char* row = rows[i];
    if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
      bits::setBit(rawResult, i, true);
    }
  }
}

template <bool mayHaveNulls>
FLATTEN inline bool RowContainer::equals(
    const char* FOLLY_NONNULL row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index) {
  auto typeKind = decoded.base()->typeKind();
  if (UNLIKELY(typeKind == TypeKind::UNKNOWN)) {
    return isNullAt(row, column.nullByte(), column.nullMask());
  }

  if constexpr (!mayHaveNulls) {
    return BOLT_DYNAMIC_TYPE_DISPATCH(
        equalsNoNulls, typeKind, row, column.offset(), decoded, index);
  } else {
    return BOLT_DYNAMIC_TYPE_DISPATCH(
        equalsWithNulls,
        typeKind,
        row,
        column.offset(),
        column.nullByte(),
        column.nullMask(),
        decoded,
        index);
  }
}

template <>
inline int RowContainer::compare<TypeKind::OPAQUE>(
    const char* FOLLY_NONNULL /*row*/,
    RowColumn /*column*/,
    const DecodedVector& /*decoded*/,
    vector_size_t /*index*/,
    CompareFlags /*flags*/) {
  BOLT_UNSUPPORTED("Comparing Opaque types is not supported.");
}

template <>
inline int RowContainer::compare<TypeKind::OPAQUE>(
    const char* FOLLY_NONNULL /*left*/,
    const char* FOLLY_NONNULL /*right*/,
    const Type* FOLLY_NONNULL /*type*/,
    RowColumn /*leftColumn*/,
    RowColumn /*rightColumn*/,
    CompareFlags /*flags*/) {
  BOLT_UNSUPPORTED("Comparing Opaque types is not supported.");
}

inline int RowContainer::compare(
    const char* FOLLY_NONNULL row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compare, decoded.base()->typeKind(), row, column, decoded, index, flags);
}

inline int RowContainer::compare(
    const char* FOLLY_NONNULL left,
    const char* FOLLY_NONNULL right,
    int columnIndex,
    CompareFlags flags) {
  auto type = types_[columnIndex].get();
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compare, type->kind(), left, right, type, columnAt(columnIndex), flags);
}

inline int RowContainer::compare(
    const char* FOLLY_NONNULL left,
    const char* FOLLY_NONNULL right,
    int leftColumnIndex,
    int rightColumnIndex,
    CompareFlags flags) {
  auto leftType = types_[leftColumnIndex].get();
  auto rightType = types_[rightColumnIndex].get();
  BOLT_CHECK(leftType->equivalent(*rightType));
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compare,
      leftType->kind(),
      left,
      right,
      leftType,
      columnAt(leftColumnIndex),
      columnAt(rightColumnIndex),
      flags);
}

/// A comparator of rows stored in the RowContainer compatible with
/// std::priority_queue. Uses specified columns and sorting orders for
/// comparison.
class RowComparator {
 public:
  RowComparator(
      const RowTypePtr& rowType,
      const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders,
      RowContainer* rowContainer);

  /// Returns true if lhs < rhs, false otherwise.
  bool operator()(const char* lhs, const char* rhs);

  /// Returns true if decodeVectors[index] < rhs, false otherwise.
  bool operator()(
      const std::vector<DecodedVector>& decodedVectors,
      vector_size_t index,
      const char* rhs);

 private:
  std::vector<std::pair<column_index_t, core::SortOrder>> keyInfo_;
  RowContainer* rowContainer_;
};

struct RowFormatInfo {
  RowFormatInfo(RowContainer* container, bool enableCompression)
      : fixRowSize(container->fixedRowSize()),
        nextEqualOffset(container->probedFlagOffset()),
        rowSizeOffset(container->rowSizeOffset()),
        alignment(container->alignment()),
        rowColumns(container->columns()),
        enableCompression(enableCompression) {
    for (int i = 0; i < container->columnTypes().size(); i++) {
      auto type = container->columnTypes()[i];
      if (!type->isFixedWidth()) {
        bool isStringType = type->kind() == TypeKind::VARCHAR ||
            type->kind() == TypeKind::VARBINARY;
        variableColumns.emplace_back(isStringType, rowColumns[i]);
      }
    }
    for (const auto& accumulator : container->accumulators()) {
      if (accumulator.serializable()) {
        serializableAccumulators.push_back(accumulator);
      }
    }
    if (rowSizeOffset) {
      // do not include row size when spill
      // row container memory layout:
      // <keys>, <nulls>, <flag>, <accumulators>, <dependents>, <rowSize>,
      // <next>, <alignment>. so we only spill data before <rowSize> to reduce
      // spill size
      fixRowSize = rowSizeOffset;
    }
  }

  FLATTEN uint32_t getRowSize(char* row) const {
    uint32_t size = fixRowSize +
        (rowSizeOffset ? *reinterpret_cast<const uint32_t*>(row + rowSizeOffset)
                       : 0);
    for (const auto& accumulator : serializableAccumulators) {
      size += accumulator.getSerializeSize(row);
    }
    return bits::roundUp(size, alignment);
  }

  uint32_t getRowSize(folly::Range<char**> rows) const {
    uint32_t totalSize = 0;
    for (auto* row : rows) {
      totalSize += getRowSize(row);
    }
    return totalSize;
  }

  int32_t fixRowSize;
  int32_t nextEqualOffset;
  int32_t rowSizeOffset;
  int alignment;
  std::vector<std::pair<bool, RowColumn>> variableColumns;
  std::vector<RowColumn> rowColumns;
  std::vector<Accumulator> serializableAccumulators;
  bool enableCompression;
  bool serialized = false;
};

/// Hybrid container

struct HybridRowId {
  uint8_t containerId_;
  uint64_t rowId_;

  // For scattered mode: decode batchId and rowInBatch from rowId_
  // Encoding: rowId_ = (batchId << 32) | rowInBatch
  uint32_t batchId() const {
    return static_cast<uint32_t>(rowId_ >> 32);
  }
  uint32_t rowInBatch() const {
    return static_cast<uint32_t>(rowId_ & 0xFFFFFFFF);
  }
};

class HybridContainer {
 public:
  HybridContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& payloadTypes,
      RowContainer* rows);
  ~HybridContainer();

  std::optional<int64_t> estimateRowSize() const;
  int32_t fixedSizeAt(column_index_t column) const;
  int32_t estimateVariableSizeAt(const char* row, column_index_t column) const;
  void addPayload(RowVectorPtr input);
  void clear();
  std::vector<TypePtr> columnTypes() const;

  void extractNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      const BufferPtr& result,
      std::vector<HybridRowId>& outputRowIds);

  void extractPayload(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize);
  // The function to get the stored RowIds from RowContainer to materialize
  // payload columns Separating it from materialization logic because in many
  // cases the same set of RowIds will be used to materialize multiple columns.
  void getRowIds(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      std::vector<HybridRowId>& outputRowIds) {
    getRowIdsInternal<false>(rows, {}, numRows, outputRowIds);
  }
  void getRowIds(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      std::vector<HybridRowId>& outputRowIds) {
    getRowIdsInternal<true>(rows, rowNumbers, rowNumbers.size(), outputRowIds);
  }
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize = false) {
    // keys
    if (isKey(columnIndex)) {
      keys_->extractColumn(rows, numRows, columnIndex, resultOffset, result);
    } else {
      // payloads
      // getRowIds should be called out of extracting projection columns
      BOLT_CHECK_EQ(
          numRows,
          outputRowIds.size(),
          "Number of rowIds is not equal to number of rows.");
      BOLT_CHECK_GT(payloadTypes_.size(), 0, "No payload columns stored.");
      extractPayload(
          rows,
          {},
          numRows,
          columnIndex - numKeys_,
          resultOffset,
          result,
          outputRowIds,
          exactSize);
    }
  };

  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize = false) {
    extractColumn(
        rows, numRows, columnIndex, 0, result, outputRowIds, exactSize);
  };

  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize = false) {
    // keys
    if (isKey(columnIndex)) {
      // exactSize was deprecated
      keys_->extractColumn(rows, rowNumbers, columnIndex, resultOffset, result);
    } else {
      // payloads
      // getRowIds should be called out of extracting projection columns
      BOLT_CHECK_EQ(
          rowNumbers.size(),
          outputRowIds.size(),
          "Number of rowIds is not equal to number of rows.");
      BOLT_CHECK_GT(payloadTypes_.size(), 0, "No payload columns stored.");
      extractPayload(
          rows,
          rowNumbers,
          rowNumbers.size(),
          columnIndex - numKeys_,
          resultOffset,
          result,
          outputRowIds,
          exactSize);
    }
  }
  bool isKey(int32_t columnIndex) const {
    return columnIndex < numKeys_;
  }
  RowContainer* getKeys() const {
    return keys_;
  }

  uint32_t getNumRows() const {
    return totalRows_;
  }

  uint32_t getNumBatches() const {
    return totalBatches_;
  }

  /// Returns the total payload memory bytes that would be needed to coalesce
  /// all payload batches. This is the sum of estimateFlatSize() for all payload
  /// columns across all batches. Used by SortBuffer to account for the memory
  /// required for coalescing when making spill decisions.
  uint64_t payloadMemoryBytes() const {
    uint64_t total = 0;
    for (const auto& bytes : payloadFlatBytesSum_) {
      total += bytes;
    }
    return total;
  }

  void setId(uint8_t id) {
    id_ = id;
  }

  uint8_t getId() {
    return id_;
  }

  // Scattered mode: keep payload batches separate (no coalesce)
  void setScatteredModeEnabled(bool enabled) {
    scatteredModeEnabled_ = enabled;
  }

  bool isScatteredModeEnabled() const {
    return scatteredModeEnabled_;
  }

  void setAllContainers(
      std::unordered_map<uint8_t, HybridContainer*>& hybridDataChannel) {
    allContainers_ = hybridDataChannel;
    maxContainerId_ = 0;
    for (const auto& [cid, _] : allContainers_) {
      maxContainerId_ = std::max<uint8_t>(maxContainerId_, cid);
    }
  }

  uint8_t getNumContainers() const {
    return allContainers_.size();
  }

  // Fast path check for single container - avoids sorting overhead.
  // In single container case, all rows come from the same container.
  bool isSingleContainer() const {
    return allContainers_.size() == 1;
  }

  // Controls whether to reorder rows by containerId during extraction.
  // Can be disabled for testing to get deterministic output order.
  void setReorderEnabled(bool enabled) {
    reorderEnabled_ = enabled;
  }

  bool isReorderEnabled() const {
    return reorderEnabled_;
  }

  // Returns whether sorting should be used for extraction.
  // Sorting is used when: reorder is enabled AND there are multiple containers.
  bool shouldUseSorting() const {
    return reorderEnabled_ && !isSingleContainer();
  }

  // Reorder rows and rowIds by containerId to improve locality for extraction.
  // Returns reordered rows, rowIds, and optionally rowNumbers when provided.
  struct SortedRows {
    std::vector<const char*> rows;
    std::vector<vector_size_t> rowNumbers;
    std::vector<HybridRowId> rowIds;
  };

  SortedRows sortByContainerId(
      const char* const* rows,
      folly::Range<const vector_size_t*> rowNumbers,
      const std::vector<HybridRowId>& outputRowIds) {
    const int size = outputRowIds.size();
    SortedRows out;
    out.rows.resize(size);
    out.rowIds.resize(size);
    if (!rowNumbers.empty()) {
      out.rowNumbers.resize(size);
    }

    // Build permutation by counting sort on containerId.
    std::vector<int32_t> count(maxContainerId_ + 1, 0);
    for (const auto& item : outputRowIds) {
      ++count[item.containerId_];
    }
    for (int i = 1; i <= maxContainerId_; ++i) {
      count[i] += count[i - 1];
    }
    std::vector<int32_t> perm(size);
    for (int i = size - 1; i >= 0; --i) {
      auto cid = outputRowIds[i].containerId_;
      auto pos = --count[cid];
      perm[pos] = i;
    }

    // Apply permutation.
    for (int outIdx = 0; outIdx < size; ++outIdx) {
      const int srcIdx = perm[outIdx];
      out.rowIds[outIdx] = outputRowIds[srcIdx];
      if (!out.rowNumbers.empty()) {
        out.rowNumbers[outIdx] = rowNumbers[srcIdx];
      }
      out.rows[outIdx] = rows[srcIdx];
    }

    return out;
  }

  // Coalesce all payload batches into a single batch to improve locality.
  void coalesceBatches() {
    // Skip if no payload columns defined.
    if (payloadTypes_.empty()) {
      return;
    }

    auto* pool = keys_->pool();
    const auto numPayloadCols = payloadTypes_.size();

    // Handle empty container: create an empty batch to maintain single-batch
    // invariant. This ensures getSingleContainerData() works even when no data
    // was added.
    if (owningInputs_.empty()) {
      std::vector<VectorPtr> emptyChildren;
      emptyChildren.reserve(numPayloadCols);
      std::vector<std::string> payloadNames;
      payloadNames.reserve(numPayloadCols);
      for (int32_t col = 0; col < numPayloadCols; ++col) {
        payloadNames.push_back(fmt::format("c{}", col));
        emptyChildren.push_back(
            BaseVector::create(payloadTypes_[col], 0, pool));
      }
      owningInputs_.push_back(std::make_shared<RowVector>(
          pool,
          ROW(std::move(payloadNames), std::vector<TypePtr>(payloadTypes_)),
          BufferPtr(nullptr),
          0,
          std::move(emptyChildren)));
      totalBatches_ = 1;
      return;
    }

    const auto totalRows = totalRows_;

    std::vector<VectorPtr> newChildren;
    newChildren.reserve(numPayloadCols);
    std::vector<std::string> payloadNames;
    payloadNames.reserve(numPayloadCols);
    for (int32_t col = 0; col < numPayloadCols; ++col) {
      payloadNames.push_back(fmt::format("c{}", col));
    }
    // Flatten each payload column into a single FlatVector.
    for (int32_t col = 0; col < numPayloadCols; ++col) {
      auto flat = BaseVector::create(payloadTypes_[col], totalRows, pool);
      vector_size_t offset = 0;
      for (const auto& batch : owningInputs_) {
        auto* child = batch->childAt(col).get();
        const auto batchSize = batch->size();
        flat->copy(child, offset, 0, batchSize);
        offset += batchSize;
      }
      newChildren.push_back(std::move(flat));
    }

    // Rebuild owningInputs_ with a single RowVector.
    owningInputs_.clear();
    owningInputs_.push_back(std::make_shared<RowVector>(
        pool,
        ROW(std::move(payloadNames), std::vector<TypePtr>(payloadTypes_)),
        BufferPtr(nullptr),
        totalRows,
        std::move(newChildren)));

    totalBatches_ = 1;
  }

 private:
  // Get the single container's coalesced data (only valid when
  // isSingleContainer()). Validates that the single container is actually this
  // container.
  RowVector* getSingleContainerData() const {
    BOLT_DCHECK_EQ(allContainers_.size(), 1);
    BOLT_DCHECK_EQ(owningInputs_.size(), 1);
    BOLT_DCHECK_NOT_NULL(owningInputs_[0]);
    auto it = allContainers_.begin();
    // Validate that the single container is self
    BOLT_DCHECK_EQ(it->first, id_, "Single container ID mismatch with self ID");
    BOLT_DCHECK(it->second == this, "Single container is not self");
    return owningInputs_[0].get();
  }

  template <TypeKind Kind>
  void extractPayloadTyped(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize) {
    if (rowNumbers.size() > 0) {
      extractPayloadTypedInternal<Kind, true>(
          rows,
          rowNumbers,
          rowNumbers.size(),
          columnIndex,
          resultOffset,
          result,
          outputRowIds,
          exactSize);
    } else {
      extractPayloadTypedInternal<Kind, false>(
          rows,
          rowNumbers,
          numRows,
          columnIndex,
          resultOffset,
          result,
          outputRowIds,
          exactSize);
    }
  }

  template <TypeKind Kind, bool useRowNumbers>
  void extractPayloadTypedInternal(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds,
      bool exactSize) {
    result->resize(numRows + resultOffset);
    BOLT_CHECK_EQ(numRows, outputRowIds.size());
    if (Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
        Kind == TypeKind::ROW) {
      extractPayloadComplex(
          numRows, columnIndex, resultOffset, result, outputRowIds);
      return;
    }
    BOLT_CHECK(Kind != TypeKind::MAP);
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto flatResult = result->as<FlatVector<T>>();

    // Scattered mode path: use DecodedVector for extraction
    if (scatteredModeEnabled_) {
      if (isSingleContainer()) {
        if (isNullable_[columnIndex]) {
          extractPayloadScatteredWithNulls<T, useRowNumbers>(
              rows,
              rowNumbers,
              numRows,
              columnIndex,
              resultOffset,
              flatResult,
              outputRowIds);
        } else {
          extractPayloadScatteredNoNulls<T, useRowNumbers>(
              rows,
              rowNumbers,
              numRows,
              columnIndex,
              resultOffset,
              flatResult,
              outputRowIds);
        }
      } else {
        // Multi-container scattered mode
        if (isNullable_[columnIndex]) {
          extractPayloadScatteredWithNullsMulti<T, useRowNumbers>(
              rows,
              rowNumbers,
              numRows,
              columnIndex,
              resultOffset,
              flatResult,
              outputRowIds);
        } else {
          extractPayloadScatteredNoNullsMulti<T, useRowNumbers>(
              rows,
              rowNumbers,
              numRows,
              columnIndex,
              resultOffset,
              flatResult,
              outputRowIds);
        }
      }
      return;
    }

    // Fast path for single container (spilling, sort) - avoids map lookups
    if (isSingleContainer()) {
      if (isNullable_[columnIndex]) {
        extractPayloadWithNullsSingleContainer<T, useRowNumbers>(
            rows,
            rowNumbers,
            numRows,
            columnIndex,
            resultOffset,
            flatResult,
            outputRowIds);
      } else {
        extractPayloadNoNullsSingleContainer<T, useRowNumbers>(
            rows,
            rowNumbers,
            numRows,
            columnIndex,
            resultOffset,
            flatResult,
            outputRowIds);
      }
      return;
    }

    // Multi-container path (hash join after table merge)
    if (isNullable_[columnIndex]) {
      extractPayloadWithNulls<T, useRowNumbers>(
          rows,
          rowNumbers,
          numRows,
          columnIndex,
          resultOffset,
          flatResult,
          outputRowIds,
          exactSize);
    } else {
      extractPayloadNoNulls<T, useRowNumbers>(
          rows,
          rowNumbers,
          numRows,
          columnIndex,
          resultOffset,
          flatResult,
          outputRowIds,
          exactSize);
    }
  }

  void extractPayloadComplex(
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      const VectorPtr& result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    // Cache per-container child pointers; coalesced payload lives in
    // owningInputs_[0] for each container.
    std::vector<BaseVector*> sources(maxContainerId_ + 1, nullptr);
    for (const auto& entry : allContainers_) {
      // Skip containers with no data (e.g., drivers that received no input)
      if (entry.second->owningInputs_.empty()) {
        continue;
      }
      auto* child = entry.second->owningInputs_[0]->childAt(columnIndex).get();
      BOLT_CHECK_NOT_NULL(child);
      sources[entry.first] = child;
    }

    auto* rowIdPtr = outputRowIds.data();
    for (int i = 0; i < numRows; ++i) {
      const auto& rec = rowIdPtr[i];
      auto* source = sources[rec.containerId_];
      BOLT_DCHECK_NOT_NULL(source);
      auto resultIndex = resultOffset + i;
      if (source->isNullAt(rec.rowId_)) {
        result->setNull(resultIndex, true);
        continue;
      }
      result->setNull(resultIndex, false);
      result->copy(source, resultIndex, rec.rowId_, 1);
    }
  }

  // ========== Single-container fast path implementations ==========
  // These avoid map lookups and container ID checks in hot loops.
  // Uses 4-way unrolled prefetch for best performance.

  template <typename T, bool useRowNumbers>
  void extractPayloadWithNullsSingleContainer(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    // Single container - direct access without map lookup
    auto* flatChild = getSingleContainerData()
                          ->childAt(columnIndex)
                          ->template as<FlatVector<T>>();
    BOLT_CHECK_NOT_NULL(flatChild);
    const T* rawValues = flatChild->rawValues();
    const uint64_t* rawNulls = flatChild->rawNulls();

    constexpr vector_size_t kPrefetchDist = 16;

    int32_t i = 0;

    // ---- Main loop: process 4 rows per iteration ----
    for (; i + 3 < numRows; i += 4) {
      // ---- Prefetch next 4 records at distance ----
      const int32_t p = i + kPrefetchDist;
      if (FOLLY_LIKELY(p + 3 < numRows)) {
        __builtin_prefetch(rawValues + rowIdPtr[p].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 1].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 2].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 3].rowId_, 0, 1);
      }

      // ---- Process 4 rows ----
      for (int32_t u = 0; u < 4; ++u) {
        const int32_t idx = i + u;

        const char* row;
        if constexpr (useRowNumbers) {
          auto rowNumber = rowNumbers[idx];
          row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
        } else {
          row = rows[idx];
        }

        const auto resultIndex = resultOffset + idx;
        if (row == nullptr) {
          bits::setNull(nulls, resultIndex, true);
          continue;
        }

        const auto rid = rowIdPtr[idx].rowId_;
        if (rawNulls != nullptr && bits::isBitNull(rawNulls, rid)) {
          bits::setNull(nulls, resultIndex, true);
          continue;
        }

        bits::setNull(nulls, resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          result->set(resultIndex, rawValues[rid]);
        } else {
          values[resultIndex] = rawValues[rid];
        }
      }
    }

    // ---- Tail loop ----
    for (; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      const auto rid = rowIdPtr[i].rowId_;
      if (rawNulls != nullptr && bits::isBitNull(rawNulls, rid)) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      bits::setNull(nulls, resultIndex, false);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, rawValues[rid]);
      } else {
        values[resultIndex] = rawValues[rid];
      }
    }
  }

  template <typename T, bool useRowNumbers>
  void extractPayloadNoNullsSingleContainer(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    // Single container - direct access without map lookup
    auto* flatChild = getSingleContainerData()
                          ->childAt(columnIndex)
                          ->template as<FlatVector<T>>();
    BOLT_CHECK_NOT_NULL(flatChild);
    const T* rawValues = flatChild->rawValues();

    constexpr vector_size_t kPrefetchDist = 16;

    int32_t i = 0;

    // ---- Main loop: process 4 rows per iteration ----
    for (; i + 3 < numRows; i += 4) {
      // ---- Prefetch next 4 records at distance ----
      const int32_t p = i + kPrefetchDist;
      if (FOLLY_LIKELY(p + 3 < numRows)) {
        __builtin_prefetch(rawValues + rowIdPtr[p].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 1].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 2].rowId_, 0, 1);
        __builtin_prefetch(rawValues + rowIdPtr[p + 3].rowId_, 0, 1);
      }

      // ---- Process 4 rows ----
      for (int32_t u = 0; u < 4; ++u) {
        const int32_t idx = i + u;

        const char* row;
        if constexpr (useRowNumbers) {
          auto rowNumber = rowNumbers[idx];
          row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
        } else {
          row = rows[idx];
        }

        const auto resultIndex = resultOffset + idx;
        if (row == nullptr) {
          result->setNull(resultIndex, true);
          continue;
        }

        result->setNull(resultIndex, false);
        const auto rid = rowIdPtr[idx].rowId_;
        if constexpr (std::is_same_v<T, StringView>) {
          result->set(resultIndex, rawValues[rid]);
        } else {
          values[resultIndex] = rawValues[rid];
        }
      }
    }

    // ---- Tail loop ----
    for (; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
        continue;
      }

      result->setNull(resultIndex, false);
      const auto rid = rowIdPtr[i].rowId_;
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, rawValues[rid]);
      } else {
        values[resultIndex] = rawValues[rid];
      }
    }
  }

  // ========== End single-container fast path implementations ==========

  // ========== Scattered mode extraction implementations ==========
  // These use DecodedVector for extraction from non-coalesced batches.
  // rowId encodes (batchId, rowInBatch) instead of global row index.

  template <typename T, bool useRowNumbers>
  void extractPayloadScatteredNoNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
        continue;
      }

      result->setNull(resultIndex, false);
      const auto& rid = rowIdPtr[i];
      const auto batchIdx = rid.batchId();
      const auto rowInBatch = rid.rowInBatch();
      T value = decodedPayloads_[batchIdx][columnIndex]->template valueAt<T>(
          rowInBatch);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, value);
      } else {
        values[resultIndex] = value;
      }
    }
  }

  template <typename T, bool useRowNumbers>
  void extractPayloadScatteredWithNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      const auto& rid = rowIdPtr[i];
      const auto batchIdx = rid.batchId();
      const auto rowInBatch = rid.rowInBatch();
      auto* decoded = decodedPayloads_[batchIdx][columnIndex].get();
      if (decoded->isNullAt(rowInBatch)) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      bits::setNull(nulls, resultIndex, false);
      T value = decoded->template valueAt<T>(rowInBatch);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, value);
      } else {
        values[resultIndex] = value;
      }
    }
  }

  // Multi-container scattered mode extraction
  template <typename T, bool useRowNumbers>
  void extractPayloadScatteredNoNullsMulti(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    // Cache current container's decodedPayloads pointer to avoid repeated map
    // lookups
    uint8_t currentContainerId = UINT8_MAX;
    std::vector<std::vector<std::unique_ptr<DecodedVector>>>*
        currentDecodedPayloads = nullptr;

    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
        continue;
      }

      result->setNull(resultIndex, false);
      const auto& rid = rowIdPtr[i];

      // Switch container if needed
      if (rid.containerId_ != currentContainerId) {
        currentContainerId = rid.containerId_;
        currentDecodedPayloads =
            &(allContainers_[currentContainerId]->decodedPayloads_);
      }

      const auto batchIdx = rid.batchId();
      const auto rowInBatch = rid.rowInBatch();
      T value =
          (*currentDecodedPayloads)[batchIdx][columnIndex]->template valueAt<T>(
              rowInBatch);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, value);
      } else {
        values[resultIndex] = value;
      }
    }
  }

  template <typename T, bool useRowNumbers>
  void extractPayloadScatteredWithNullsMulti(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    // Cache current container's decodedPayloads pointer to avoid repeated map
    // lookups
    uint8_t currentContainerId = UINT8_MAX;
    std::vector<std::vector<std::unique_ptr<DecodedVector>>>*
        currentDecodedPayloads = nullptr;

    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      const auto& rid = rowIdPtr[i];

      // Switch container if needed
      if (rid.containerId_ != currentContainerId) {
        currentContainerId = rid.containerId_;
        currentDecodedPayloads =
            &(allContainers_[currentContainerId]->decodedPayloads_);
      }

      const auto batchIdx = rid.batchId();
      const auto rowInBatch = rid.rowInBatch();
      auto* decoded = (*currentDecodedPayloads)[batchIdx][columnIndex].get();
      if (decoded->isNullAt(rowInBatch)) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      bits::setNull(nulls, resultIndex, false);
      T value = decoded->template valueAt<T>(rowInBatch);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, value);
      } else {
        values[resultIndex] = value;
      }
    }
  }

  // ========== End scattered mode extraction implementations ==========

  template <typename T, bool useRowNumbers>
  void extractPayloadWithNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds,
      bool /*exactSize*/) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());

    BufferPtr& nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    auto* rowIdPtr = outputRowIds.data();

    constexpr vector_size_t kPrefetchDist = 16;

    std::vector<const T*> rawValuesByContainer(maxContainerId_ + 1, nullptr);
    std::vector<const uint64_t*> rawNullsByContainer(
        maxContainerId_ + 1, nullptr);
    for (const auto& entry : allContainers_) {
      // Skip containers with no data (e.g., drivers that received no input)
      if (entry.second->owningInputs_.empty()) {
        continue;
      }
      auto* flatChild = entry.second->owningInputs_[0]
                            ->childAt(columnIndex)
                            ->template as<FlatVector<T>>();
      BOLT_CHECK_NOT_NULL(flatChild);
      rawValuesByContainer[entry.first] = flatChild->rawValues();
      rawNullsByContainer[entry.first] = flatChild->rawNulls();
    }

    int32_t curCid = -1;
    const T* curRaw = nullptr;
    const uint64_t* curNulls = nullptr;

    int32_t pfCid = -1;
    const T* pfRaw = nullptr;

    if (FOLLY_LIKELY(numRows > 0)) {
      curCid = rowIdPtr[0].containerId_;
      curRaw = rawValuesByContainer[curCid];
      curNulls = rawNullsByContainer[curCid];
      BOLT_DCHECK_NOT_NULL(curRaw);
      if (kPrefetchDist < numRows) {
        pfCid = rowIdPtr[kPrefetchDist].containerId_;
        pfRaw = rawValuesByContainer[pfCid];
        BOLT_DCHECK_NOT_NULL(pfRaw);
      }
    }

    int32_t i = 0;

    for (; i + 3 < numRows; i += 4) {
      const int32_t p = i + kPrefetchDist;
      if (FOLLY_LIKELY(p + 3 < numRows)) {
        const auto& r0 = rowIdPtr[p];
        if (FOLLY_UNLIKELY(r0.containerId_ != pfCid)) {
          pfCid = r0.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r0.rowId_, 0, 1);

        const auto& r1 = rowIdPtr[p + 1];
        if (FOLLY_UNLIKELY(r1.containerId_ != pfCid)) {
          pfCid = r1.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r1.rowId_, 0, 1);

        const auto& r2 = rowIdPtr[p + 2];
        if (FOLLY_UNLIKELY(r2.containerId_ != pfCid)) {
          pfCid = r2.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r2.rowId_, 0, 1);

        const auto& r3 = rowIdPtr[p + 3];
        if (FOLLY_UNLIKELY(r3.containerId_ != pfCid)) {
          pfCid = r3.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r3.rowId_, 0, 1);
      }

      for (int32_t u = 0; u < 4; ++u) {
        const int32_t idx = i + u;

        const char* row;
        if constexpr (useRowNumbers) {
          auto rowNumber = rowNumbers[idx];
          row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
        } else {
          row = rows[idx];
        }

        const auto resultIndex = resultOffset + idx;
        if (row == nullptr) {
          bits::setNull(nulls, resultIndex, true);
          continue;
        }

        const auto& rowIdRec = rowIdPtr[idx];
        if (FOLLY_UNLIKELY(rowIdRec.containerId_ != curCid)) {
          curCid = rowIdRec.containerId_;
          curRaw = rawValuesByContainer[curCid];
          curNulls = rawNullsByContainer[curCid];
          BOLT_DCHECK_NOT_NULL(curRaw);
        }

        const auto rid = rowIdRec.rowId_;
        if (curNulls != nullptr && bits::isBitNull(curNulls, rid)) {
          bits::setNull(nulls, resultIndex, true);
          continue;
        }

        bits::setNull(nulls, resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          result->set(resultIndex, curRaw[rid]);
        } else {
          values[resultIndex] = curRaw[rid];
        }
      }
    }

    for (; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      const auto& rowIdRec = rowIdPtr[i];
      if (FOLLY_UNLIKELY(rowIdRec.containerId_ != curCid)) {
        curCid = rowIdRec.containerId_;
        curRaw = rawValuesByContainer[curCid];
        curNulls = rawNullsByContainer[curCid];
        BOLT_DCHECK_NOT_NULL(curRaw);
      }

      const auto rid = rowIdRec.rowId_;
      if (curNulls != nullptr && bits::isBitNull(curNulls, rid)) {
        bits::setNull(nulls, resultIndex, true);
        continue;
      }

      bits::setNull(nulls, resultIndex, false);
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, curRaw[rid]);
      } else {
        values[resultIndex] = curRaw[rid];
      }
    }
  }

  template <typename T, bool useRowNumbers>
  void extractPayloadNoNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result,
      std::vector<HybridRowId>& outputRowIds,
      bool /*exactSize*/) {
    auto maxRows = numRows + resultOffset;
    BOLT_DCHECK_LE(maxRows, result->size());
    BOLT_DCHECK_LT(columnIndex, payloadTypes_.size());

    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();

    auto* rowIdPtr = outputRowIds.data();

    constexpr vector_size_t kPrefetchDist = 16;

    std::vector<const T*> rawValuesByContainer(maxContainerId_ + 1, nullptr);
    for (const auto& entry : allContainers_) {
      // Skip containers with no data (e.g., drivers that received no input)
      if (entry.second->owningInputs_.empty()) {
        continue;
      }
      auto* flatChild = entry.second->owningInputs_[0]
                            ->childAt(columnIndex)
                            ->template as<FlatVector<T>>();
      BOLT_CHECK_NOT_NULL(flatChild);
      rawValuesByContainer[entry.first] = flatChild->rawValues();
    }
    // cached for load
    int32_t curCid = -1;
    const T* curRaw = nullptr;

    // cached for prefetch
    int32_t pfCid = -1;
    const T* pfRaw = nullptr;

    if (FOLLY_LIKELY(numRows > 0)) {
      curCid = rowIdPtr[0].containerId_;
      curRaw = rawValuesByContainer[curCid];
      BOLT_DCHECK_NOT_NULL(curRaw);
      if (kPrefetchDist < numRows) {
        pfCid = rowIdPtr[kPrefetchDist].containerId_;
        pfRaw = rawValuesByContainer[pfCid];
        BOLT_DCHECK_NOT_NULL(pfRaw);
      }
    }

    int32_t i = 0;

    // ---- Main loop: process 4 rows per iteration ----
    for (; i + 3 < numRows; i += 4) {
      // ---- Prefetch next 4 records at distance ----
      const int32_t p = i + kPrefetchDist;
      // Correct bound for prefetching p..p+3:
      if (FOLLY_LIKELY(p + 3 < numRows)) {
        // r0
        const auto& r0 = rowIdPtr[p];
        if (FOLLY_UNLIKELY(r0.containerId_ != pfCid)) {
          pfCid = r0.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r0.rowId_, 0, 1);

        // r1
        const auto& r1 = rowIdPtr[p + 1];
        if (FOLLY_UNLIKELY(r1.containerId_ != pfCid)) {
          pfCid = r1.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r1.rowId_, 0, 1);

        // r2
        const auto& r2 = rowIdPtr[p + 2];
        if (FOLLY_UNLIKELY(r2.containerId_ != pfCid)) {
          pfCid = r2.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r2.rowId_, 0, 1);

        // r3
        const auto& r3 = rowIdPtr[p + 3];
        if (FOLLY_UNLIKELY(r3.containerId_ != pfCid)) {
          pfCid = r3.containerId_;
          pfRaw = rawValuesByContainer[pfCid];
          BOLT_DCHECK_NOT_NULL(pfRaw);
        }
        __builtin_prefetch(pfRaw + r3.rowId_, 0, 1);
      }

      // ---- Consume 4 rows ----
      for (int32_t u = 0; u < 4; ++u) {
        const int32_t idx = i + u;

        const char* row;
        if constexpr (useRowNumbers) {
          auto rowNumber = rowNumbers[idx];
          row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
        } else {
          row = rows[idx];
        }

        const auto resultIndex = resultOffset + idx;
        if (row == nullptr) {
          result->setNull(resultIndex, true);
          continue;
        }

        result->setNull(resultIndex, false);

        const auto& rowIdRec = rowIdPtr[idx];

        // Refresh cached container pointer only when containerId changes.
        if (FOLLY_UNLIKELY(rowIdRec.containerId_ != curCid)) {
          curCid = rowIdRec.containerId_;
          curRaw = rawValuesByContainer[curCid];
          BOLT_DCHECK_NOT_NULL(curRaw);
        }

        const auto rid = rowIdRec.rowId_;
        if constexpr (std::is_same_v<T, StringView>) {
          result->set(resultIndex, curRaw[rid]);
        } else {
          values[resultIndex] = curRaw[rid];
        }
      }
    }

    // ---- Tail loop ----
    for (; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }

      const auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
        continue;
      }

      result->setNull(resultIndex, false);

      const auto& rowIdRec = rowIdPtr[i];
      if (FOLLY_UNLIKELY(rowIdRec.containerId_ != curCid)) {
        curCid = rowIdRec.containerId_;
        curRaw = rawValuesByContainer[curCid];
        BOLT_DCHECK_NOT_NULL(curRaw);
      }

      const auto rid = rowIdRec.rowId_;
      if constexpr (std::is_same_v<T, StringView>) {
        result->set(resultIndex, curRaw[rid]);
      } else {
        values[resultIndex] = curRaw[rid];
      }
    }
  }

  template <bool useRowNumbers>
  void getRowIdsInternal(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      uint32_t numRows,
      std::vector<HybridRowId>& outputRowIds) {
    BOLT_CHECK_EQ(numRows, outputRowIds.size());
    // No payload column
    if (payloadTypes_.size() == 0)
      return;
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        auto rowNumber = rowNumbers[i];
        row = rowNumber >= 0 ? rows[rowNumber] : nullptr;
      } else {
        row = rows[i];
      }
      if (row == nullptr) {
        outputRowIds[i] = {0, 0};
      } else {
        auto encodedId = keys_->valueAt<uint64_t>(row, rowIdColumnOffset_);
        uint8_t driverId = encodedId >> 56;
        uint64_t rowId = encodedId & ((1ULL << 56) - 1);
        outputRowIds[i] = {driverId, rowId};
      }
    }
  }

  int32_t rowIdColumnOffset_;
  std::vector<RowVectorPtr> owningInputs_;
  const std::vector<TypePtr> keyTypes_;
  const std::vector<TypePtr> payloadTypes_;
  std::vector<TypePtr> types_;
  std::vector<uint64_t> payloadFlatBytesSum_;

  const int numKeys_;
  RowContainer* keys_;

  // null bitmap for each column
  std::vector<char> isNullable_;

  uint64_t totalRows_{0};
  uint32_t totalBatches_{0};

  uint8_t id_{0};
  std::unordered_map<uint8_t, HybridContainer*> allContainers_;
  uint8_t maxContainerId_{0};

  // Controls whether to reorder rows by containerId during extraction.
  // Default true for better cache locality. Can be disabled for testing.
  bool reorderEnabled_{true};

  // Scattered mode: keep payload batches separate (no coalesce).
  // In scattered mode, rowId encodes (batchId, rowInBatch) instead of global
  // row index.
  bool scatteredModeEnabled_{false};

  // Pre-decoded payload vectors for scattered mode extraction.
  // Indexed by [batchIndex][columnIndex].
  std::vector<std::vector<std::unique_ptr<DecodedVector>>> decodedPayloads_;
};

template <>
inline void HybridContainer::extractPayloadTyped<TypeKind::OPAQUE>(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    int32_t columnIndex,
    int32_t resultOffset,
    const VectorPtr& result,
    std::vector<HybridRowId>& outputRowIds,
    bool exactSize) {
  BOLT_UNSUPPORTED("HybridContainer doesn't support OPAQUE payload types.");
}

inline void HybridContainer::extractPayload(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    folly::Range<const vector_size_t*> rowNumbers,
    int32_t numRows,
    int32_t columnIndex,
    int32_t resultOffset,
    const VectorPtr& result,
    std::vector<HybridRowId>& outputRowIds,
    bool exactSize) {
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractPayloadTyped,
      result->typeKind(),
      rows,
      rowNumbers,
      numRows,
      columnIndex,
      resultOffset,
      result,
      outputRowIds,
      exactSize);
}

inline void HybridContainer::extractNulls(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    int32_t numRows,
    int32_t columnIndex,
    const BufferPtr& result,
    std::vector<HybridRowId>& outputRowIds) {
  if (isKey(columnIndex)) {
    keys_->extractNulls(rows, numRows, columnIndex, result);
  } else {
    auto payloadColumnIndex = columnIndex - numKeys_;
    BOLT_DCHECK(result->size() >= bits::nbytes(numRows));
    auto* rawResult = result->asMutable<uint64_t>();
    bits::fillBits(rawResult, 0, numRows, false);
    if (!isNullable_[payloadColumnIndex]) {
      return;
    }
    std::vector<const uint64_t*> rawNullsByContainer(
        maxContainerId_ + 1, nullptr);
    for (const auto& entry : allContainers_) {
      // Skip containers with no data (e.g., drivers that received no input)
      if (entry.second->owningInputs_.empty()) {
        continue;
      }
      auto* child =
          entry.second->owningInputs_[0]->childAt(payloadColumnIndex).get();
      rawNullsByContainer[entry.first] = child->rawNulls();
    }
    auto* rowIdPtr = outputRowIds.data();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row = rows[i];
      if (row == nullptr) {
        bits::setBit(rawResult, i, true);
        continue;
      }
      const auto& rec = rowIdPtr[i];
      const auto* nulls = rawNullsByContainer[rec.containerId_];
      if (nulls != nullptr && bits::isBitNull(nulls, rec.rowId_)) {
        bits::setBit(rawResult, i, true);
      }
    }
  }
}

} // namespace bytedance::bolt::exec
