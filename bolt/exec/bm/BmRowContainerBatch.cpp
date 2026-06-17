#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"

namespace bytedance::bolt::exec::bm {
namespace {

struct BatchHeapWriter {
  BmSegmentCollection* segments{nullptr};
  ChunkData* chunk{nullptr};
  BlockRef* heap{nullptr};
  char* cursor{nullptr};
  char* limit{nullptr};
  BlockId recordedHeapBlock{kNoBlock};

  void flush() {
    if (heap != nullptr) {
      heap->used = static_cast<uint32_t>(cursor - heap->ptr);
    }
  }

  char* allocate(uint32_t bytes, const char* row) {
    BOLT_DCHECK_NOT_NULL(segments);
    BOLT_DCHECK_NOT_NULL(chunk);
    if (FOLLY_LIKELY(cursor != nullptr && cursor + bytes <= limit)) {
      auto* target = cursor;
      cursor += bytes;
      return target;
    }

    flush();
    heap = &segments->ensureHeapBlockInChunk(*chunk, bytes);
    cursor = heap->ptr + heap->used;
    limit = heap->ptr + heap->size;
    if (recordedHeapBlock != heap->id) {
      segments->recordHeapForChunk(*chunk, *heap, row);
      recordedHeapBlock = heap->id;
    }
    BOLT_DCHECK_LE(cursor + bytes, limit);
    auto* target = cursor;
    cursor += bytes;
    return target;
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

  std::vector<BatchAppendRange> ranges;
  ranges.reserve(input->size());
  if (rows != nullptr) {
    rows->reserve(rows->size() + input->size());
  }
  auto& segment = segments_.activeSegment(partition);
  segments_.reserveRowsInBatch(segment, 0, input->size(), ranges, rows);

  SelectivityVector allRows(input->size());
  for (auto column = 0; column < inputRow->childrenSize(); ++column) {
    const auto& child = inputRow->childAt(column);
    BOLT_CHECK_EQ(child->type(), types_[column]);
    DecodedVector decoded(*child, allRows);
    const auto& plan = layout_.storePlan(column);
    const folly::Range<const BatchAppendRange*> rangeView(
        ranges.data(), ranges.size());
    if (plan.stringKind) {
      storeStringColumnBatchRanges(
          decoded, rangeView, plan, stringStoreMode);
      continue;
    }

    if (plan.nullable) {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchRangesWithNullsTyped,
          plan.kind,
          decoded,
          rangeView,
          plan);
    } else {
      BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
          storeFixedColumnBatchRangesNoNullsTyped,
          plan.kind,
          decoded,
          rangeView,
          plan);
    }
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
    const auto* values =
        decoded.isIdentityMapping() ? decoded.data<T>() : nullptr;
    for (const auto& range : ranges) {
      auto* row = range.rowBegin;
      auto source = range.sourceBegin;
      for (vector_size_t i = 0; i < range.rowCount; ++i) {
        BOLT_DCHECK(
            !decoded.isNullAt(source),
            "Column {} is not nullable",
            column.type->toString());
        *reinterpret_cast<T*>(row + column.offset) =
            values == nullptr ? decoded.valueAt<T>(source) : values[source];
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
          *reinterpret_cast<T*>(row + column.offset) =
              decoded.valueAt<T>(source);
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
  const auto* values =
      decoded.isIdentityMapping() && !column.nullable ? decoded.data<StringView>()
                                                      : nullptr;
  for (const auto& range : ranges) {
    BatchHeapWriter writer{&segments_, range.chunk};
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
        auto* stringTarget = writer.allocate(value.size(), row);
        simd::memcpy(stringTarget, value.data(), value.size());
        *target = StringView(stringTarget, value.size());
      }
      row += rowStride;
      ++source;
    }
    writer.flush();
  }
}

} // namespace bytedance::bolt::exec::bm
