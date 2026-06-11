#include "bolt/exec/bm/BmSegmentCollection.h"

#include "bolt/common/base/Exceptions.h"

#include <utility>

namespace bytedance::bolt::exec::bm {

ChunkData& BmSegmentCollection::chunkForRow(
    SegmentData& segment,
    RowNumber rowNumber) {
  (void)std::as_const(*this).chunkForRow(segment, rowNumber);
  return chunkForRowUnchecked(segment, rowNumber);
}

const ChunkData& BmSegmentCollection::chunkForRow(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  const auto rowsPerChunk = rowBlockSize_ / rowStride();
  BOLT_DCHECK_GT(rowsPerChunk, 0);
  const auto chunkIndex = rowNumber / rowsPerChunk;
  BOLT_CHECK_LT(
      chunkIndex,
      segment.chunks.size(),
      "Unknown row number {} in segment {}",
      rowNumber,
      segment.meta.id);
  const auto& chunk = chunkForRowUnchecked(segment, rowNumber);
  BOLT_CHECK(
      rowNumber >= chunk.meta.firstRowNumber &&
          rowNumber < chunk.meta.firstRowNumber + chunk.meta.rowCount,
      "Unknown row number {} in segment {}",
      rowNumber,
      segment.meta.id);
  return chunk;
}

RowId BmSegmentCollection::rowIdForRowNumber(
    const SegmentData& segment,
    RowNumber rowNumber) const {
  const auto& chunk = chunkForRow(segment, rowNumber);
  auto remaining = rowNumber - chunk.meta.firstRowNumber;
  return {
      segment.meta.id,
      rowNumber,
      chunk.rowBlock.id,
      static_cast<RowOffset>(remaining * rowStride())};
}

void BmSegmentCollection::appendRowIdsForSegment(
    const SegmentData& segment,
    std::vector<RowId>& rows) const {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot materialize RowIds for consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    RowNumber rowNumber = chunk.meta.firstRowNumber;
    RowOffset rowOffset = 0;
    const auto rowWidth = rowStride();
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      rows.push_back(
          {segment.meta.id,
           rowNumber++,
           chunk.rowBlock.id,
           rowOffset});
      rowOffset += rowWidth;
    }
  }
}

void BmSegmentCollection::appendRowPointersForSegment(
    SegmentData& segment,
    std::vector<char*>& rows,
    BulkLoadMetrics* metrics) {
  rows.reserve(rows.size() + segment.meta.numRows);
  for (const auto& chunk : segment.chunks) {
    BOLT_CHECK(
        !chunk.consumed,
        "Cannot materialize row pointers for consumed chunk {} in segment {}",
        chunk.meta.id,
        segment.meta.id);
    const auto& rowBlock = chunk.rowBlock;
    BOLT_DCHECK_NOT_NULL(rowBlock.ptr);
    if (metrics != nullptr) {
      metrics->pointerRows += chunk.meta.rowCount;
    }
    auto* row = rowBlock.ptr;
    for (uint32_t rowIndex = 0; rowIndex < chunk.meta.rowCount; ++rowIndex) {
      rows.push_back(row);
      row += rowStride();
    }
  }
}

char* BmSegmentCollection::rowPointer(const RowId& id) {
  auto& segment = segmentData(id.segmentId);
  auto& chunk = chunkForRowUnchecked(segment, id.rowNumber);
  BOLT_DCHECK_EQ(chunk.rowBlock.id, id.rowBlockId);
  BOLT_DCHECK(!chunk.consumed);
  BOLT_DCHECK_NOT_NULL(chunk.rowBlock.ptr);
  return chunk.rowBlock.ptr + id.rowOffset;
}

const char* BmSegmentCollection::rowPointer(const RowId& id) const {
  return const_cast<BmSegmentCollection*>(this)->rowPointer(id);
}

} // namespace bytedance::bolt::exec::bm
