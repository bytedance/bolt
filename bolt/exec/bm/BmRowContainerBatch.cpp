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

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/bm/BmContainerRowSerde.h"
#include "bolt/type/HugeInt.h"
#include "bolt/vector/ComplexVector.h"

#include <algorithm>
#include <limits>
#include <span>

namespace bytedance::bolt::exec::bm {
namespace {

// Estimate bytes that usedSize() misses when values are repeated. This is a
// hint for choosing a write size, not an upper bound on serialized bytes.
uint64_t estimateExtraBytes(const BaseVector& vector, bool& incomplete) {
  using Encoding = VectorEncoding::Simple;
  uint64_t extra = 0;
  switch (vector.encoding()) {
    case Encoding::LAZY:
      if (isLazyNotLoaded(vector)) {
        incomplete = true;
        return 0;
      }
      return estimateExtraBytes(*vector.loadedVector(), incomplete);
    case Encoding::DICTIONARY:
    case Encoding::SEQUENCE:
      // Dictionary indices may repeatedly select a wide value even when the
      // backing vector's average value size is small.
      incomplete |= isLazyNotLoaded(vector) ||
          (vector.encoding() == Encoding::DICTIONARY &&
           !vector.type()->isFixedWidth());
      return 0;
    case Encoding::CONSTANT: {
      const auto& backing = vector.valueVector();
      if (backing && isLazyNotLoaded(*backing)) {
        incomplete = true;
        return 0;
      }
      if (vector.size() == 0 || vector.isNullAt(0)) {
        return 0;
      }
      if (vector.typeKind() == TypeKind::VARCHAR ||
          vector.typeKind() == TypeKind::VARBINARY) {
        const auto value =
            vector.asUnchecked<SimpleVector<StringView>>()->valueAt(0);
        extra = static_cast<uint64_t>(value.size()) * (vector.size() - 1);
        break;
      }
      if (vector.type()->isFixedWidth()) {
        return 0;
      }
      if (!backing) {
        incomplete = true;
        return 0;
      }
      const auto backingExtra = estimateExtraBytes(*backing, incomplete);
      extra = backingExtra * vector.size() +
          backing->usedSize() * (vector.size() - 1);
      break;
    }
    case Encoding::ARRAY:
      return estimateExtraBytes(
          *vector.asUnchecked<ArrayVector>()->elements(), incomplete);
    case Encoding::MAP: {
      const auto* map = vector.asUnchecked<MapVector>();
      extra = estimateExtraBytes(*map->mapKeys(), incomplete);
      extra += estimateExtraBytes(*map->mapValues(), incomplete);
      break;
    }
    case Encoding::ROW:
      for (const auto& child : vector.asUnchecked<RowVector>()->children()) {
        if (child) {
          extra += estimateExtraBytes(*child, incomplete);
        }
      }
      break;
    case Encoding::FLAT: {
      if (vector.typeKind() != TypeKind::VARCHAR &&
          vector.typeKind() != TypeKind::VARBINARY) {
        return 0;
      }
      const auto* flat = vector.asUnchecked<FlatVector<StringView>>();
      const auto& stats = flat->stringStats();
      if (!stats) {
        return 0;
      }
      const auto usedBytes = flat->usedSize();
      return stats->totalBytes > usedBytes ? stats->totalBytes - usedBytes : 0;
    }
    default:
      return 0;
  }
  return extra;
}

// Carries a heap write cursor across the rows of one BatchAppendRange. Rows in
// a range append their variable-width payload into the same heap block, so the
// cursor advances locally and heap->used is only written back on
// publishCursor() instead of once per row.
//
// Two write models share this cursor:
//   - Known-size (VARCHAR/VARBINARY): the caller knows the byte count up front,
//     asks reserve() for a range, then copies into it.
//   - Speculative (complex ARRAY/MAP/ROW): the serialized size is not known
//     ahead of time. The caller serializes into writableTail() (the
//     uncommitted tail), then either commit()s the bytes that fit or, on
//     overflow, takes a fresh block via reserveInNewBlock() and reserializes.
struct BatchHeapWriter {
  BatchHeapWriter(BmSegmentCollection* segments, ChunkData* chunk)
      : segments(segments), chunk(chunk) {
    if (chunk != nullptr && !chunk->heapBlocks.empty()) {
      heap = &chunk->heapBlocks.back();
      cursor = heap->ptr + heap->used;
      limit = heap->ptr + heap->size;
      recordedHeapBlock = heap->id;
    }
  }

  BmSegmentCollection* segments{nullptr};
  ChunkData* chunk{nullptr};
  // Heap block the cursor currently writes into. A range starts from the
  // chunk's existing writable tail when one is available.
  BlockRef* heap{nullptr};
  // Next free byte in 'heap' (uncommitted for speculative writes) and one past
  // the block's last writable byte.
  char* cursor{nullptr};
  char* limit{nullptr};
  // Id of the heap block already recorded for this chunk, so recordHeapForChunk
  // runs at most once per block.
  BlockId recordedHeapBlock{kNoBlock};

  // Publishes the locally advanced cursor back into heap->used, making the
  // written bytes visible to readers. Called on block switches and once at the
  // end of each range.
  void publishCursor() {
    if (heap != nullptr) {
      heap->used = static_cast<uint32_t>(cursor - heap->ptr);
    }
  }

  // Uncommitted heap tail available for speculative writes. Empty before the
  // first block is acquired; callers must handle the zero-capacity case (it
  // simply forces an overflow in the serializer).
  std::span<char> writableTail() const {
    if (cursor == nullptr) {
      return {};
    }
    return std::span<char>(cursor, static_cast<size_t>(limit - cursor));
  }

  // Commits 'bytes' already speculatively written at the current cursor into
  // writableTail(). Records the block on first use, then advances the cursor.
  void commit(uint32_t bytes, const char* referencingRow) {
    recordBlockOnce(referencingRow);
    cursor += bytes;
  }

  // Reserves an exact (possibly oversized) range in a fresh block for a value
  // that did not fit in the current tail. Publishes the old block's used count
  // first, then resets the cursor and record state onto the new block. Used
  // both as the overflow fallback for speculative writes and as the slow path
  // of reserve().
  char* reserveInNewBlock(uint32_t bytes, const char* referencingRow) {
    BOLT_DCHECK_NOT_NULL(segments);
    BOLT_DCHECK_NOT_NULL(chunk);
    publishCursor();
    heap = &segments->ensureHeapBlockInChunk(*chunk, bytes);
    cursor = heap->ptr + heap->used;
    limit = heap->ptr + heap->size;
    recordedHeapBlock = kNoBlock;
    recordBlockOnce(referencingRow);
    auto* target = cursor;
    cursor += bytes;
    return target;
  }

  // Known-size reservation for the string path: hands back a 'bytes'-sized
  // range to memcpy into. Fast path carves from the current tail; otherwise
  // falls back to reserveInNewBlock() to switch blocks.
  char* reserve(uint32_t bytes, const char* referencingRow) {
    BOLT_DCHECK_NOT_NULL(segments);
    BOLT_DCHECK_NOT_NULL(chunk);
    if (FOLLY_LIKELY(cursor != nullptr && cursor + bytes <= limit)) {
      auto* target = cursor;
      cursor += bytes;
      recordBlockOnce(referencingRow);
      return target;
    }
    return reserveInNewBlock(bytes, referencingRow);
  }

 private:
  // Records 'heap' as backing this chunk exactly once, so serialized StringView
  // pointers into the block can be rebased on load. Idempotent per block via
  // recordedHeapBlock. 'referencingRow' is the row whose variable-width column
  // points into this block; it is only consumed by a debug ownership check in
  // recordHeapForChunk and is unused in release builds.
  void recordBlockOnce(const char* referencingRow) {
    if (heap != nullptr && recordedHeapBlock != heap->id) {
      segments->recordHeapForChunk(*chunk, *heap, referencingRow);
      recordedHeapBlock = heap->id;
    }
  }
};

} // namespace

void BmRowContainer::appendBatch(
    const RowVectorPtr& input,
    PartitionId partition,
    std::vector<char*>* rows,
    BmBatchStringStoreMode stringStoreMode) {
  BOLT_CHECK_NOT_NULL(input);
  BOLT_CHECK_EQ(input->childrenSize(), types_.size());
  auto* inputRow = input->as<RowVector>();
  BOLT_CHECK_NOT_NULL(inputRow);

  if (input->size() == 0) {
    return;
  }

  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    BOLT_CHECK_EQ(inputRow->childAt(column)->type(), types_[column]);
  }

  if (rows != nullptr) {
    rows->reserve(rows->size() + input->size());
  }
  auto& segment = segments_.activeSegment(partition);

  const auto storeColumn = [&](const DecodedVector& decoded,
                               vector_size_t column,
                               folly::Range<const BatchAppendRange*> ranges) {
    const auto& plan = layout_.storePlan(column);
    if (plan.complexKind) {
      storeComplexColumnBatchRanges(decoded, ranges, plan);
      return;
    }
    if (plan.stringKind) {
      storeStringColumnBatchRanges(decoded, ranges, plan, stringStoreMode);
      return;
    }

    if (plan.nullable) {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchRangesWithNullsTyped,
          plan.kind,
          decoded,
          ranges,
          plan);
    } else {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchRangesNoNullsTyped,
          plan.kind,
          decoded,
          ranges,
          plan);
    }
  };

  SelectivityVector allRows(input->size());
  bool hasHeapColumns = false;
  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    const auto& plan = layout_.storePlan(column);
    hasHeapColumns |= plan.complexKind ||
        (plan.stringKind &&
         stringStoreMode !=
             BmBatchStringStoreMode::kReferenceInputStringForBenchmark);
  }

  bool wholeBatch = chunkHeapTargetBytes_ == 0 || !hasHeapColumns;
  if (!wholeBatch) {
    auto estimatedBytes = input->usedSize();
    bool estimateIncomplete = false;
    estimatedBytes += estimateExtraBytes(*inputRow, estimateIncomplete);
    const auto& cursor = segment.writeCursor;
    const auto currentBytes =
        cursor.chunk != nullptr && cursor.nextRow != cursor.rowBlockEnd
        ? cursor.chunk->heapAllocatedBytes
        : 0;
    wholeBatch = !estimateIncomplete && currentBytes < chunkHeapTargetBytes_ &&
        estimatedBytes < chunkHeapTargetBytes_ - currentBytes;
  }

  if (wholeBatch) {
    std::vector<BatchAppendRange> ranges;
    ranges.reserve(input->size());
    segments_.reserveRowsInBatch(segment, 0, input->size(), ranges, rows);
    const folly::Range<const BatchAppendRange*> rangeView(
        ranges.data(), ranges.size());
    for (auto column = 0; column < inputRow->childrenSize(); ++column) {
      DecodedVector decoded(*inputRow->childAt(column), allRows);
      storeColumn(decoded, column, rangeView);
    }
    segments_.sealChunkAtBatchBoundary(segment);
    return;
  }

  std::vector<DecodedVector> decodedColumns(inputRow->childrenSize());
  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    decodedColumns[column].decode(*inputRow->childAt(column), allRows);
  }
  // Finish all columns for a bounded number of rows before sealing a chunk.
  constexpr vector_size_t kMaxPieceRows = 32;
  vector_size_t sourceBegin = 0;
  while (sourceBegin < input->size()) {
    const auto range = segments_.reserveRowsInCurrentChunk(
        segment,
        sourceBegin,
        std::min(kMaxPieceRows, input->size() - sourceBegin),
        rows);
    const folly::Range<const BatchAppendRange*> rangeView(&range, 1);
    for (auto column = 0; column < inputRow->childrenSize(); ++column) {
      storeColumn(decodedColumns[column], column, rangeView);
    }
    segments_.sealChunkAtBatchBoundary(segment);
    sourceBegin += range.rowCount;
  }
}

void BmRowContainer::storeComplexColumnBatchRanges(
    const DecodedVector& decoded,
    folly::Range<const BatchAppendRange*> ranges,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(column.complexKind);
  const auto rowStride = segments_.rowStride();
  const auto nullMask = static_cast<char>(column.nullMask);
  const ContainerRowSerdeOptions options{.isKey = column.isKey};
  const auto& source = *decoded.base();
  for (const auto& range : ranges) {
    BatchHeapWriter writer(&segments_, range.chunk);
    auto* row = range.rowBegin;
    auto sourceIndex = range.sourceBegin;
    for (vector_size_t i = 0; i < range.rowCount; ++i) {
      if (column.nullable) {
        auto& nullByte = row[column.nullByte];
        if (decoded.isNullAt(sourceIndex)) {
          nullByte |= nullMask;
          row += rowStride;
          ++sourceIndex;
          continue;
        }
        nullByte &= ~nullMask;
      } else {
        BOLT_DCHECK(
            !decoded.isNullAt(sourceIndex),
            "Column {} is not nullable",
            column.type->toString());
      }

      const auto index = decoded.index(sourceIndex);
      auto* target = reinterpret_cast<StringView*>(row + column.offset);

      // Speculative write: serialize into the uncommitted heap tail while
      // counting bytes and commit only once the value fits. The BatchHeapWriter
      // carries the cursor across rows in this range.
      //
      // IMPORTANT: the four-way decision tree below (fit+inline / fit+heap /
      // overflow+inline / overflow+new-block) must stay byte-for-byte
      // equivalent to the single-row path in
      // BmRowContainer::storeComplexValue() (BmRowContainerStore.cpp); the two
      // only differ in how the heap cursor is held (BatchHeapWriter here vs
      // RowWriteContext there). Change both together.
      const auto result = detail::trySerializeInto(
          source, index, writer.writableTail(), options);
      BOLT_CHECK_LE(
          result.size,
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()),
          "Complex value exceeds BmRowContainer StringView size limit");
      const auto bytes = static_cast<uint32_t>(result.size);

      if (FOLLY_LIKELY(result.complete)) {
        auto* written = writer.writableTail().data();
        if (FOLLY_UNLIKELY(StringView::isInline(result.size))) {
          // Inline-sized value: StringView copies the bytes; tail uncommitted.
          *target = StringView(written, result.size);
        } else {
          // Common path: the value fit the tail; commit it in place.
          writer.commit(bytes, row);
          *target = StringView(written, bytes);
        }
      } else if (StringView::isInline(result.size)) {
        // Inline-sized value that did not fit the tail: rewrite into a stack
        // buffer with no heap growth.
        char inlineBytes[StringView::kInlineSize];
        BmContainerRowSerde::serializeInto(
            source, index, std::span<char>(inlineBytes, result.size), options);
        *target = StringView(inlineBytes, result.size);
      } else {
        // Value crosses the current block: allocate an exact (possibly
        // oversized) block and serialize once more into it.
        auto* serializedTarget = writer.reserveInNewBlock(bytes, row);
        BmContainerRowSerde::serializeInto(
            source, index, std::span<char>(serializedTarget, bytes), options);
        *target = StringView(serializedTarget, bytes);
      }
      row += rowStride;
      ++sourceIndex;
    }
    writer.publishCursor();
  }
}

template <TypeKind Kind>
void BmRowContainer::storeFixedColumnBatchRangesNoNullsTyped(
    const DecodedVector& decoded,
    folly::Range<const BatchAppendRange*> ranges,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(!column.stringKind);
  BOLT_DCHECK(!column.nullable);
  if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    const auto rowStride = segments_.rowStride();
    // Boolean vectors are bit-packed, including identity-mapped inputs.
    const auto* values =
        Kind != TypeKind::BOOLEAN && decoded.isIdentityMapping()
        ? decoded.data<T>()
        : nullptr;
    for (const auto& range : ranges) {
      auto* row = range.rowBegin;
      auto source = range.sourceBegin;
      for (vector_size_t i = 0; i < range.rowCount; ++i) {
        BOLT_DCHECK(
            !decoded.isNullAt(source),
            "Column {} is not nullable",
            column.type->toString());
        const auto value =
            values == nullptr ? decoded.valueAt<T>(source) : values[source];
        if constexpr (Kind == TypeKind::HUGEINT) {
          HugeInt::serialize(value, row + column.offset);
        } else {
          *reinterpret_cast<T*>(row + column.offset) = value;
        }
        row += rowStride;
        ++source;
      }
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::storeFixedColumnBatchRangesWithNullsTyped(
    const DecodedVector& decoded,
    folly::Range<const BatchAppendRange*> ranges,
    const ColumnStorePlan& column) {
  BOLT_DCHECK(!column.stringKind);
  BOLT_DCHECK(column.nullable);
  if constexpr (
      Kind == TypeKind::UNKNOWN || !TypeTraits<Kind>::isPrimitiveType ||
      !TypeTraits<Kind>::isFixedWidth) {
    BOLT_NYI("Unsupported store type {}", column.type->toString());
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    const auto rowStride = segments_.rowStride();
    const auto nullMask = static_cast<char>(column.nullMask);
    for (const auto& range : ranges) {
      auto* row = range.rowBegin;
      auto source = range.sourceBegin;
      for (vector_size_t i = 0; i < range.rowCount; ++i) {
        auto& nullByte = row[column.nullByte];
        if (decoded.isNullAt(source)) {
          nullByte |= nullMask;
        } else {
          nullByte &= ~nullMask;
          if constexpr (Kind == TypeKind::HUGEINT) {
            HugeInt::serialize(
                decoded.valueAt<int128_t>(source), row + column.offset);
          } else {
            *reinterpret_cast<T*>(row + column.offset) =
                decoded.valueAt<T>(source);
          }
        }
        row += rowStride;
        ++source;
      }
    }
  }
}

void BmRowContainer::storeStringColumnBatchRanges(
    const DecodedVector& decoded,
    folly::Range<const BatchAppendRange*> ranges,
    const ColumnStorePlan& column,
    BmBatchStringStoreMode stringStoreMode) {
  BOLT_DCHECK(column.stringKind);
  const auto rowStride = segments_.rowStride();
  const auto nullMask = static_cast<char>(column.nullMask);
  const auto* values = decoded.isIdentityMapping() && !column.nullable
      ? decoded.data<StringView>()
      : nullptr;
  for (const auto& range : ranges) {
    BatchHeapWriter writer(&segments_, range.chunk);
    auto* row = range.rowBegin;
    auto source = range.sourceBegin;
    for (vector_size_t i = 0; i < range.rowCount; ++i) {
      if (column.nullable) {
        auto& nullByte = row[column.nullByte];
        if (decoded.isNullAt(source)) {
          nullByte |= nullMask;
          row += rowStride;
          ++source;
          continue;
        }
        nullByte &= ~nullMask;
      } else {
        BOLT_DCHECK(
            !decoded.isNullAt(source),
            "Column {} is not nullable",
            column.type->toString());
      }

      auto* target = reinterpret_cast<StringView*>(row + column.offset);
      const auto value = values == nullptr ? decoded.valueAt<StringView>(source)
                                           : values[source];
      if (value.isInline()) {
        *target = value;
      } else if (
          stringStoreMode ==
          BmBatchStringStoreMode::kReferenceInputStringForBenchmark) {
        *target = value;
      } else {
        auto* stringTarget = writer.reserve(value.size(), row);
        simd::memcpy(stringTarget, value.data(), value.size());
        *target = StringView(stringTarget, value.size());
      }
      row += rowStride;
      ++source;
    }
    writer.publishCursor();
  }
}

} // namespace bytedance::bolt::exec::bm
