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

#include "bolt/dwio/lance/NativeLanceColumnCursor.h"

#include <algorithm>
#include <limits>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceColumnCursor::NativeLanceColumnCursor(
    uint32_t physicalColumn,
    const std::vector<uint64_t>& pageRowStarts)
    : physicalColumn_(physicalColumn), pageRowStarts_(&pageRowStarts) {
  BOLT_CHECK(!pageRowStarts.empty());
  BOLT_CHECK_EQ(pageRowStarts.front(), 0);
  BOLT_CHECK(std::is_sorted(pageRowStarts.begin(), pageRowStarts.end()));
}

std::vector<NativeLancePageSpan> NativeLanceColumnCursor::spans(
    uint64_t rowStart,
    uint64_t rowCount) {
  BOLT_CHECK_GE(
      rowStart, nextRow_, "Lance column cursor cannot move backwards");
  BOLT_CHECK_LE(rowStart, std::numeric_limits<uint64_t>::max() - rowCount);
  const auto rowEnd = rowStart + rowCount;
  BOLT_CHECK_LE(rowEnd, pageRowStarts_->back());
  seek(rowStart);

  std::vector<NativeLancePageSpan> result;
  auto pageIndex = pageIndex_;
  while (pageIndex < static_cast<int32_t>(pageRowStarts_->size() - 1) &&
         (*pageRowStarts_)[pageIndex] < rowEnd) {
    const auto pageBegin = (*pageRowStarts_)[pageIndex];
    const auto pageEnd = (*pageRowStarts_)[pageIndex + 1];
    const auto overlapBegin = std::max(rowStart, pageBegin);
    const auto overlapEnd = std::min(rowEnd, pageEnd);
    if (overlapBegin < overlapEnd) {
      result.push_back(
          {.physicalColumn = physicalColumn_,
           .pageIndex = pageIndex,
           .pageRowBegin = pageBegin,
           .localRowBegin = overlapBegin - pageBegin,
           .rowCount = overlapEnd - overlapBegin});
    }
    ++pageIndex;
  }
  nextRow_ = rowEnd;
  seek(rowEnd);
  return result;
}

void NativeLanceColumnCursor::seek(uint64_t row) {
  BOLT_CHECK_LE(row, pageRowStarts_->back());
  if (row == pageRowStarts_->back()) {
    pageIndex_ = static_cast<int32_t>(pageRowStarts_->size() - 1);
    nextRow_ = row;
    return;
  }
  const auto upper =
      std::upper_bound(pageRowStarts_->begin(), pageRowStarts_->end(), row);
  BOLT_CHECK(upper != pageRowStarts_->begin());
  pageIndex_ = static_cast<int32_t>(upper - pageRowStarts_->begin() - 1);
  nextRow_ = row;
}

} // namespace bytedance::bolt::lance::reader
