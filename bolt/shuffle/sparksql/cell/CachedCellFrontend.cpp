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

#include "bolt/shuffle/sparksql/cell/CachedCellFrontend.h"

#include "bolt/shuffle/sparksql/cell/CellEncoding.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

CachedCellFrontend::CachedCellFrontend(
    const CellLayout* layout,
    DataCells* cells,
    NullCells* nulls,
    memory::MemoryPool* pool,
    ChunkAllocator::GrowCallback beforeGrow)
    : layout_(layout),
      cells_(cells),
      nulls_(nulls),
      beforeGrow_(std::move(beforeGrow)),
      numPartitions_(cells->numPartitions()),
      numStreams_(layout->numStreams()),
      arena_(pool),
      partitionBytes_(numPartitions_, 0),
      variableBytes_(numPartitions_, 0) {
  const size_t cacheBytes =
      static_cast<size_t>(numPartitions_) * numStreams_ * kBlockSourceBytes;
  const size_t cursorBytes =
      static_cast<size_t>(numPartitions_) * numStreams_;
  cacheBase_ = arena_.allocateFixed(cacheBytes, kBlockSourceBytes);
  cursors_ = reinterpret_cast<uint8_t*>(arena_.allocateFixed(cursorBytes, 64));
  ::memset(cursors_, 0, cursorBytes);
  residentBytes_ = static_cast<int64_t>(cacheBytes + cursorBytes) +
      static_cast<int64_t>(partitionBytes_.size() + variableBytes_.size()) * 8;
}

template <typename T>
void CachedCellFrontend::flushEncoded(
    uint32_t stream,
    uint32_t pid,
    uint8_t* cur) {
  uint8_t block[kMaxBlockBytes];
  const uint32_t count = cur[pid] / sizeof(T);
  const uint32_t bytes = encodeBlock(
      reinterpret_cast<const T*>(cacheLine(stream, pid)), count, block);
  cells_->append(pid, stream, block, bytes, beforeGrow_);
  bumpPartitionBytes(pid, bytes);
  cur[pid] = 0;
}

void CachedCellFrontend::flushRaw(uint32_t stream, uint32_t pid, uint8_t* cur) {
  cells_->append(pid, stream, cacheLine(stream, pid), cur[pid], beforeGrow_);
  bumpPartitionBytes(pid, cur[pid]);
  cur[pid] = 0;
}

template <typename T, bool kHasNulls, bool kIndexed>
void CachedCellFrontend::splitFixed(uint32_t col, const SplitBatch& batch) {
  const auto& decoded = (*batch.decoded)[col];
  const T* __restrict vals = decoded.data<T>();
  const uint32_t stream = layout_->columnStream(col);
  uint8_t* __restrict cur = cursors(stream);
  char* __restrict base = cacheBase_ +
      ((static_cast<size_t>(stream) * numPartitions_) << 6);
  const uint32_t* __restrict row2pid = batch.row2Partition;

  for (uint32_t row = 0; row < batch.numRows; ++row) {
    const uint32_t pid = row2pid[row];
    if constexpr (kHasNulls) {
      if (decoded.isNullAt(row)) {
        nulls_->setNull(
            pid, col, batch.windowRowStart[pid] + batch.rowIndexInPid[row]);
        continue;
      }
    }
    const T value = kIndexed ? vals[decoded.index(row)] : vals[row];
    char* slot = base + (static_cast<size_t>(pid) << 6) + cur[pid];
    ::memcpy(slot, &value, sizeof(T));
    cur[pid] += sizeof(T);
    if (FOLLY_UNLIKELY(cur[pid] == kBlockSourceBytes)) {
      flushEncoded<T>(stream, pid, cur);
    }
  }
}

template <typename T, bool kHasNulls, bool kIndexed>
void CachedCellFrontend::splitRawFixed(uint32_t col, const SplitBatch& batch) {
  const auto& decoded = (*batch.decoded)[col];
  const T* __restrict vals = decoded.data<T>();
  const uint32_t stream = layout_->columnStream(col);
  uint8_t* __restrict cur = cursors(stream);
  char* __restrict base = cacheBase_ +
      ((static_cast<size_t>(stream) * numPartitions_) << 6);
  const uint32_t* __restrict row2pid = batch.row2Partition;

  for (uint32_t row = 0; row < batch.numRows; ++row) {
    const uint32_t pid = row2pid[row];
    if constexpr (kHasNulls) {
      if (decoded.isNullAt(row)) {
        nulls_->setNull(
            pid, col, batch.windowRowStart[pid] + batch.rowIndexInPid[row]);
        continue;
      }
    }
    const T value = kIndexed ? vals[decoded.index(row)] : vals[row];
    char* slot = base + (static_cast<size_t>(pid) << 6) + cur[pid];
    ::memcpy(slot, &value, sizeof(T));
    cur[pid] += sizeof(T);
    if (FOLLY_UNLIKELY(cur[pid] == kBlockSourceBytes)) {
      flushRaw(stream, pid, cur);
    }
  }
}

template <bool kHasNulls, bool kIndexed>
void CachedCellFrontend::splitString(uint32_t col, const SplitBatch& batch) {
  const auto& decoded = (*batch.decoded)[col];
  const StringView* __restrict views = decoded.data<StringView>();
  const uint32_t lengthStream = layout_->columnStream(col);
  const uint32_t dataStream = lengthStream + 1;
  uint8_t* __restrict lengthCur = cursors(lengthStream);
  uint8_t* __restrict dataCur = cursors(dataStream);
  const uint32_t* __restrict row2pid = batch.row2Partition;

  for (uint32_t row = 0; row < batch.numRows; ++row) {
    const uint32_t pid = row2pid[row];
    if constexpr (kHasNulls) {
      if (decoded.isNullAt(row)) {
        nulls_->setNull(
            pid, col, batch.windowRowStart[pid] + batch.rowIndexInPid[row]);
        continue;
      }
    }
    const StringView view = kIndexed ? views[decoded.index(row)] : views[row];

    // Length stream: an int64 value in an 8-byte cache slot.
    {
      char* slot = cacheLine(lengthStream, pid) + lengthCur[pid];
      const int64_t length = view.size();
      ::memcpy(slot, &length, sizeof(int64_t));
      lengthCur[pid] += sizeof(int64_t);
      if (FOLLY_UNLIKELY(lengthCur[pid] == kBlockSourceBytes)) {
        flushEncoded<int64_t>(lengthStream, pid, lengthCur);
      }
    }

    // Data stream: raw bytes, staged through the cache line for locality;
    // long values bypass it after a flush keeps the byte order.
    const uint32_t size = view.size();
    variableBytes_[pid] += size;
    if (FOLLY_UNLIKELY(size >= kBlockSourceBytes)) {
      if (dataCur[pid] > 0) {
        flushRaw(dataStream, pid, dataCur);
      }
      cells_->append(pid, dataStream, view.data(), size, beforeGrow_);
      bumpPartitionBytes(pid, size);
      continue;
    }
    if (dataCur[pid] + size > kBlockSourceBytes) {
      flushRaw(dataStream, pid, dataCur);
    }
    ::memcpy(cacheLine(dataStream, pid) + dataCur[pid], view.data(), size);
    dataCur[pid] += static_cast<uint8_t>(size);
  }
}

template <typename T>
void CachedCellFrontend::dispatchEncoded(uint32_t col, const SplitBatch& batch) {
  const auto& decoded = (*batch.decoded)[col];
  const bool hasNulls = decoded.mayHaveNulls();
  const bool indexed = !decoded.isIdentityMapping();
  if (hasNulls) {
    indexed ? splitFixed<T, true, true>(col, batch)
            : splitFixed<T, true, false>(col, batch);
  } else {
    indexed ? splitFixed<T, false, true>(col, batch)
            : splitFixed<T, false, false>(col, batch);
  }
}

template <typename T>
void CachedCellFrontend::dispatchRaw(uint32_t col, const SplitBatch& batch) {
  const auto& decoded = (*batch.decoded)[col];
  const bool hasNulls = decoded.mayHaveNulls();
  const bool indexed = !decoded.isIdentityMapping();
  if (hasNulls) {
    indexed ? splitRawFixed<T, true, true>(col, batch)
            : splitRawFixed<T, true, false>(col, batch);
  } else {
    indexed ? splitRawFixed<T, false, true>(col, batch)
            : splitRawFixed<T, false, false>(col, batch);
  }
}

void CachedCellFrontend::split(const SplitBatch& batch) {
  const auto& rowType = layout_->rowType();
  for (uint32_t col = 0; col < layout_->numColumns(); ++col) {
    switch (rowType->childAt(col)->kind()) {
      case TypeKind::SMALLINT:
        dispatchEncoded<int16_t>(col, batch);
        break;
      case TypeKind::INTEGER:
        dispatchEncoded<int32_t>(col, batch);
        break;
      case TypeKind::BIGINT:
        dispatchEncoded<int64_t>(col, batch);
        break;
      case TypeKind::TINYINT:
        dispatchRaw<int8_t>(col, batch);
        break;
      case TypeKind::REAL:
        dispatchRaw<float>(col, batch);
        break;
      case TypeKind::DOUBLE:
        dispatchRaw<double>(col, batch);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto& decoded = (*batch.decoded)[col];
        const bool hasNulls = decoded.mayHaveNulls();
        const bool indexed = !decoded.isIdentityMapping();
        if (hasNulls) {
          indexed ? splitString<true, true>(col, batch)
                  : splitString<true, false>(col, batch);
        } else {
          indexed ? splitString<false, true>(col, batch)
                  : splitString<false, false>(col, batch);
        }
        break;
      }
      default:
        BOLT_UNREACHABLE();
    }
  }
}

void CachedCellFrontend::flushAll() {
  for (uint32_t stream = 0; stream < numStreams_; ++stream) {
    const auto& info = layout_->stream(stream);
    uint8_t* cur = cursors(stream);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      if (cur[pid] == 0) {
        continue;
      }
      if (info.kind == StreamKind::kEncoded) {
        switch (info.sourceWidth) {
          case 2:
            flushEncoded<int16_t>(stream, pid, cur);
            break;
          case 4:
            flushEncoded<int32_t>(stream, pid, cur);
            break;
          case 8:
            flushEncoded<int64_t>(stream, pid, cur);
            break;
          default:
            BOLT_UNREACHABLE();
        }
      } else {
        flushRaw(stream, pid, cur);
      }
    }
  }
}

void CachedCellFrontend::resetWindowStats() {
  std::fill(partitionBytes_.begin(), partitionBytes_.end(), 0);
  std::fill(variableBytes_.begin(), variableBytes_.end(), 0);
  maxPartitionBytes_ = 0;
}

// The fixed-width split templates are only referenced from this translation
// unit; no explicit instantiation is needed.

} // namespace bytedance::bolt::shuffle::sparksql::cell
