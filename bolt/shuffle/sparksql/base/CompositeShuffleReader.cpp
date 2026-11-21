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

#include "bolt/shuffle/sparksql/base/CompositeShuffleReader.h"

#include <algorithm>
#include <limits>
#include <vector>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql {

CompositeShuffleReader::CompositeShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize)
    : decompressor_(
          codec == nullptr ? nullptr
                           : std::make_unique<CodecDecompressor>(codec)),
      cursor_(std::move(streams), arrowPool, decompressor_.get()),
      rowBlockReader_(decompressor_.get(), pool),
      pool_(pool),
      batchSize_(batchSize),
      batchByteSize_(batchByteSize),
      compositeConverter_(outputType, pool_) {}

void CompositeShuffleReader::readCompositeBlock() {
  nextCompositeRow_ = 0;
  rowBlockReader_.read(
      cursor_.source(), compositeBlock_, ShuffleBlockKind::kCompositeRows);
}

RowVectorPtr CompositeShuffleReader::nextComposite() {
  const uint64_t start = currentTimeNs();
  const size_t limit = std::min(
      compositeBlock_.rows.size(),
      nextCompositeRow_ + static_cast<size_t>(std::max(batchSize_, 1)));
  const uint64_t byteCap = batchByteSize_ > 0
      ? std::min<uint64_t>(
            static_cast<uint64_t>(batchByteSize_),
            std::numeric_limits<int32_t>::max())
      : static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
  size_t end = nextCompositeRow_;
  uint64_t total = 0;
  while (end < limit) {
    const uint64_t rowBytes = compositeBlock_.rows[end].size();
    if (end > nextCompositeRow_ && total + rowBytes > byteCap) {
      break;
    }
    total += rowBytes;
    ++end;
  }
  BOLT_CHECK_LE(
      total,
      std::numeric_limits<int32_t>::max(),
      "composite rows too large for one vector");
  std::vector<std::string_view> slice(
      compositeBlock_.rows.begin() + nextCompositeRow_,
      compositeBlock_.rows.begin() + end);
  nextCompositeRow_ = end;
  auto output = compositeConverter_.convertToComposite(
      slice, static_cast<int32_t>(total));
  decodeTimeNs_ += currentTimeNs() - start;
  rowsRead_ += output->size();
  return output;
}

RowVectorPtr CompositeShuffleReader::next() {
  // A composite block met while batching non-composite blocks goes out next.
  if (hasCompositeRows()) {
    return nextComposite();
  }
  RowVectorPtr output = nextNonComposite();
  if (output == nullptr && hasCompositeRows()) {
    return nextComposite();
  }
  if (output != nullptr) {
    rowsRead_ += output->size();
  }
  return output;
}

} // namespace bytedance::bolt::shuffle::sparksql
