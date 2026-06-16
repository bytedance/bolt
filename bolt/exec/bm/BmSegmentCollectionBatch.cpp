#include "bolt/exec/bm/BmSegmentCollection.h"

#include "bolt/common/base/Exceptions.h"

#include <algorithm>

namespace bytedance::bolt::exec::bm {

void BmSegmentCollection::reserveRowsInBatch(
    SegmentData& segment,
    vector_size_t sourceBegin,
    vector_size_t count,
    std::vector<BatchAppendRange>& ranges,
    std::vector<char*>* rows) {
  BOLT_DCHECK(segment.meta.state == SegmentState::kActiveResident);
  auto remaining = count;
  auto currentSource = sourceBegin;
  auto& cursor = segment.writeCursor;
  while (remaining > 0) {
    if (FOLLY_UNLIKELY(
            cursor.chunk == nullptr || cursor.nextRow == cursor.rowBlockEnd)) {
      ensureWritableChunk(segment);
    }

    BOLT_DCHECK_NOT_NULL(cursor.chunk);
    auto& chunk = *cursor.chunk;
    auto& rowBlock = chunk.rowBlock;
    const auto availableRows = static_cast<vector_size_t>(
        (cursor.rowBlockEnd - cursor.nextRow) / rowStride_);
    BOLT_DCHECK_GT(availableRows, 0);
    const auto rowsToAppend = std::min(remaining, availableRows);
    auto* const rowBegin = cursor.nextRow;
    layout().initializeNullsRange(rowBegin, rowsToAppend, rowStride_);

    ranges.push_back(
        BatchAppendRange{&chunk, rowBegin, currentSource, rowsToAppend});
    if (rows != nullptr) {
      auto* row = rowBegin;
      for (vector_size_t i = 0; i < rowsToAppend; ++i) {
        rows->push_back(row);
        row += rowStride_;
      }
    }

    const auto bytes = static_cast<uint32_t>(rowsToAppend * rowStride_);
    cursor.nextRow += bytes;
    rowBlock.used += bytes;
    BOLT_DCHECK_LE(rowBlock.used, rowBlock.size);
    chunk.meta.rowCount += rowsToAppend;
    segment.nextRowNumber += rowsToAppend;
    segment.meta.numRows += rowsToAppend;

    remaining -= rowsToAppend;
    currentSource += rowsToAppend;
  }
}

} // namespace bytedance::bolt::exec::bm
