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
            true, // nullableKeys
            std::vector<Accumulator>{},
            dependentTypes,
            false, // hasNext
            false, // isJoinBuild
            false, // hasProbedFlag
            false, // hasNormalizedKey
            pool) {}

  ~RowContainer();

  std::unique_ptr<RowContainer> cloneByOrder(
      const std::vector<char*>& rows,
      memory::MemoryPool* pool = nullptr,
      std::shared_ptr<HashStringAllocator> stringAllocator = nullptr);

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
    if (column < numKeys && !nullableKeys_) {
      auto off = offsets_[column];

      for (auto r = 0; r < size; ++r) {
        storeNoNulls<Kind>(decoded, r, rows[r], off);
      }
    } else {
      BOLT_DCHECK(column < keyTypes_.size() || accumulators_.empty());
      auto rowColumn = rowColumns_[column];
      auto off = rowColumn.offset();
      auto nullByte = rowColumn.nullByte();
      auto mask = rowColumn.nullMask();

      for (auto r = 0; r < size; ++r) {
        storeWithNulls<Kind>(decoded, r, rows[r], off, nullByte, mask);
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

 private:
  static constexpr int32_t kNextFreeOffset = 0;

  static const std::vector<column_index_t> kEmptySortKeyIndexes;

  /// Returns the size of a string or complex types value stored in the
  /// specified row and column.
  int32_t variableSizeAt(const char* row, column_index_t column);

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
    auto values = valuesBuffer->asMutableRange<T>();
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
    auto values = valuesBuffer->asMutableRange<T>();
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
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::HUGEINT>(
    const DecodedVector& decoded,
    vector_size_t index,
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

} // namespace bytedance::bolt::exec
