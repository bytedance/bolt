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
      variableBytes_(numPartitions_, 0),
      dictEnabled_(layout->numColumns(), 0),
      dictStates_(layout->numColumns()) {
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
  const auto* line = reinterpret_cast<const T*>(cacheLine(stream, pid));
  const bool full = cur[pid] == kBlockSourceBytes;
  // Common case: encode straight into the tail cell (reserve allocates
  // nothing, so nothing can spill between reserve and commit).
  if (auto* dst = reinterpret_cast<uint8_t*>(
          cells_->tryReserve(pid, stream, kMaxBlockBytes))) {
    const uint32_t bytes = full
        ? encodeBlockFull<T>(line, dst)
        : encodeBlock<T>(line, cur[pid] / sizeof(T), dst);
    cells_->commit(pid, stream, bytes);
    bumpPartitionBytes(pid, bytes);
    cur[pid] = 0;
    return;
  }
  uint8_t block[kMaxBlockBytes];
  const uint32_t bytes = full
      ? encodeBlockFull<T>(line, block)
      : encodeBlock<T>(line, cur[pid] / sizeof(T), block);
  cells_->append(pid, stream, block, bytes, beforeGrow_);
  bumpPartitionBytes(pid, bytes);
  cur[pid] = 0;
}

void CachedCellFrontend::flushRaw(uint32_t stream, uint32_t pid, uint8_t* cur) {
  cells_->append(pid, stream, cacheLine(stream, pid), cur[pid], beforeGrow_);
  bumpPartitionBytes(pid, cur[pid]);
  cur[pid] = 0;
}

void CachedCellFrontend::enableDictionary(uint32_t col) {
  BOLT_CHECK(
      layout_->isStringColumn(col),
      "dictionary form is defined for string columns only");
  dictEnabled_[col] = 1;
  dictStates_[col].assign(numPartitions_, DictState{});
  residentBytes_ +=
      static_cast<int64_t>(numPartitions_) * sizeof(DictState);
}

void CachedCellFrontend::closeDictSegment(
    uint32_t dataStream,
    uint32_t pid,
    DictState& st,
    bool last) {
  // The whole framing lands in one append: DataCells::append copies only
  // after every needed cell is held, so a spill fired inside the grow sees
  // none of these bytes and the dictionary never crosses a Run boundary
  // (spec section 5.4).
  uint8_t buf[kDictSerializedBudget + 1 + 1 + 4];
  ::memcpy(buf, st.entries, st.entryBytes);
  buf[st.entryBytes] = last ? kDictLastMarker : kDictMoreMarker;
  ::memcpy(buf + st.entryBytes + 1, &st.matched, 4);
  const uint32_t bytes = st.entryBytes + 5;
  cells_->append(pid, dataStream, buf, bytes, beforeGrow_);
  bumpPartitionBytes(pid, bytes);
  st.matched = 0;
  st.entryCount = 0;
  st.entryBytes = 0;
}

FOLLY_ALWAYS_INLINE bool CachedCellFrontend::appendDictValue(
    uint32_t lengthStream,
    uint32_t dataStream,
    uint32_t pid,
    DictState& st,
    const StringView& view,
    uint64_t key,
    uint8_t* lengthCur) {
  const uint32_t size = static_cast<uint32_t>(key);
  // An entry costs 1 + size serialized bytes and one dictionary is capped
  // at 63 (reader L1 rule): a value that can never fit alone can never be
  // an entry, and the format only allows a fallback tail after that.
  if (FOLLY_LIKELY(1 + size <= kDictSerializedBudget)) {
    const uint32_t count = st.entryCount;
    uint32_t i = 0;
    uint32_t off = 0;
    // The dictionary is stored only in its serialized [len][bytes]... form;
    // the walk over entry boundaries is inherent, so each step is made as
    // cheap as the form allows: one 8-byte window load per entry yields the
    // length byte AND up to seven content bytes, so a short value - the
    // common dictionary shape - compares whole in the same register, and
    // the walk issues exactly one load per entry.
    if (FOLLY_LIKELY(size <= 7)) {
      // Values up to 7 chars are always inline in the StringView, whose
      // bytes 4..11 hold the zero-padded characters: the whole value in
      // one register, no data() branch.
      //
      // Branchless full walk. An early-exit walk mispredicts once per row
      // (the hit position is data-dependent); walking all entries with a
      // fixed, perfectly predicted trip count and accumulating the hit
      // with a conditional move is cheaper. Entries are unique by
      // construction, so at most one step matches and a plain cmov
      // accumulation is exact. Length byte and content compare as one
      // masked word: needle = [size][chars...].
      uint64_t value;
      ::memcpy(&value, reinterpret_cast<const char*>(&view) + 4, 8);
      const uint64_t needle = (value << 8) | size;
      const uint64_t needleMask =
          (((uint64_t{1} << (size * 8)) - 1) << 8) | 0xFF;
      uint32_t hit = count;
      for (uint32_t k = 0; k < count; ++k) {
        uint64_t window;
        ::memcpy(&window, st.entries + off, 8);
        if ((window & needleMask) == needle) {
          hit = k; // at most once; compiles to a flag-carrying select
        }
        off += 1 + static_cast<uint32_t>(window & 0xFF);
      }
      i = hit;
    } else {
      for (; i < count; ++i) {
        const uint32_t len =
            static_cast<uint8_t>(st.entries[off]);
        if (len == size &&
            ::memcmp(st.entries + off + 1, view.data(), size) == 0) {
          break;
        }
        off += 1 + len;
      }
    }
    if (FOLLY_UNLIKELY(i == count)) {
      // New value. Room left: it becomes an entry. No room: the segment
      // closes, and its hit rate decides between a successor segment
      // seeded with this value and the permanent fallback tail.
      if (count == DictState::kMaxEntries ||
          st.entryBytes + 1 + size > kDictSerializedBudget) {
        if (st.matched >= kDictSegmentContinueFactor * count) {
          closeDictSegment(dataStream, pid, st, /*last=*/false);
        } else {
          closeDictSegment(dataStream, pid, st, /*last=*/true);
          st.mode = DictState::kModeFallback;
          // Pending index bytes flush raw before the first staged
          // fallback length reuses the cache line.
          if (lengthCur[pid] > 0) {
            flushRaw(lengthStream, pid, lengthCur);
          }
          return false;
        }
      }
      st.entries[st.entryBytes] = static_cast<char>(size);
      ::memcpy(st.entries + st.entryBytes + 1, view.data(), size);
      st.entryBytes += static_cast<uint8_t>(1 + size);
      i = st.entryCount++;
    }
    ++st.matched;
    cacheLine(lengthStream, pid)[lengthCur[pid]] = static_cast<char>(i);
    if (FOLLY_UNLIKELY(++lengthCur[pid] == kBlockSourceBytes)) {
      flushRaw(lengthStream, pid, lengthCur);
    }
    return true;
  }
  // A value too long for any entry: the format only allows a fallback
  // tail, so this partition demotes now (an empty sequence is still owed
  // when no segment was ever written).
  closeDictSegment(dataStream, pid, st, /*last=*/true);
  st.mode = DictState::kModeFallback;
  if (lengthCur[pid] > 0) {
    flushRaw(lengthStream, pid, lengthCur);
  }
  return false;
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
      // Raw stream bytes may split anywhere (spec section 5.4), so a huge
      // value is appended in bounded pieces: each append then pre-holds at
      // most a couple of cells and a spill can reclaim between pieces,
      // keeping the memory cap meaningful.
      constexpr uint32_t kDirectAppendPiece = 256 << 10;
      const char* src = view.data();
      uint32_t left = size;
      while (left > 0) {
        const uint32_t piece = left < kDirectAppendPiece ? left : kDirectAppendPiece;
        cells_->append(pid, dataStream, src, piece, beforeGrow_);
        src += piece;
        left -= piece;
      }
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

template <bool kHasNulls, bool kIndexed>
void CachedCellFrontend::splitStringDict(
    uint32_t col,
    const SplitBatch& batch) {
  auto& decoded = (*batch.decoded)[col]; // nulls() may materialize lazily
  const StringView* __restrict views = decoded.data<StringView>();
  const uint32_t lengthStream = layout_->columnStream(col);
  const uint32_t dataStream = lengthStream + 1;
  uint8_t* __restrict lengthCur = cursors(lengthStream);
  uint8_t* __restrict dataCur = cursors(dataStream);
  DictState* __restrict states = dictStates_[col].data();
  const uint32_t* __restrict row2pid = batch.row2Partition;
  // Identity mapping indexes the null bitmap by row: one bit test beats
  // the isNullAt call the compiler declines to inline into this loop.
  const uint64_t* __restrict rawNulls =
      kHasNulls && !kIndexed ? decoded.nulls() : nullptr;

  for (uint32_t row = 0; row < batch.numRows; ++row) {
    const uint32_t pid = row2pid[row];
    if constexpr (kHasNulls) {
      const bool isNull =
          kIndexed ? decoded.isNullAt(row) : bits::isBitNull(rawNulls, row);
      if (isNull) {
        nulls_->setNull(
            pid, col, batch.windowRowStart[pid] + batch.rowIndexInPid[row]);
        continue;
      }
    }
    const StringView& view =
        kIndexed ? views[decoded.index(row)] : views[row];
    // The first 8 StringView bytes are (size u32)(zero-padded 4-byte
    // prefix) for inline and heap values alike: one load feeds the size,
    // the byte accounting and the whole dictionary probe, and view.data()
    // with its inline-or-pointer branch stays off the hit path.
    static_assert(StringView::kPrefixSize == 4, "prefix layout assumed");
    uint64_t key;
    ::memcpy(&key, &view, sizeof(uint64_t));
    variableBytes_[pid] += static_cast<uint32_t>(key);
    DictState& st = states[pid];
    if (FOLLY_LIKELY(st.mode == DictState::kModeDict) &&
        appendDictValue(
            lengthStream, dataStream, pid, st, view, key, lengthCur)) {
      continue;
    }

    // The fallback tail of a demoted partition: byte-identical to the raw
    // string path of splitString.
    {
      char* slot = cacheLine(lengthStream, pid) + lengthCur[pid];
      const int64_t length = view.size();
      ::memcpy(slot, &length, sizeof(int64_t));
      lengthCur[pid] += sizeof(int64_t);
      if (FOLLY_UNLIKELY(lengthCur[pid] == kBlockSourceBytes)) {
        flushEncoded<int64_t>(lengthStream, pid, lengthCur);
      }
    }
    const uint32_t size = view.size();
    if (FOLLY_UNLIKELY(size >= kBlockSourceBytes)) {
      if (dataCur[pid] > 0) {
        flushRaw(dataStream, pid, dataCur);
      }
      constexpr uint32_t kDirectAppendPiece = 256 << 10;
      const char* src = view.data();
      uint32_t left = size;
      while (left > 0) {
        const uint32_t piece =
            left < kDirectAppendPiece ? left : kDirectAppendPiece;
        cells_->append(pid, dataStream, src, piece, beforeGrow_);
        src += piece;
        left -= piece;
      }
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
void CachedCellFrontend::dispatchEncoded(
    uint32_t col,
    const SplitBatch& batch,
    bool hasNulls) {
  const auto& decoded = (*batch.decoded)[col];
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
void CachedCellFrontend::dispatchRaw(
    uint32_t col,
    const SplitBatch& batch,
    bool hasNulls) {
  const auto& decoded = (*batch.decoded)[col];
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
    const auto klass = batch.nullClass[col];
    if (klass == BatchNullClass::kAllNull) {
      // The whole batch is null in this column (constant null, or a flat
      // vector whose scan came back all-null): values contribute nothing
      // (dense pack) and the nulls are a counted run per partition - the
      // column costs O(partitions) per batch and allocates no bitmap.
      for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
        const uint32_t count = batch.partition2RowCount[pid];
        if (count > 0) {
          nulls_->setNullRun(pid, col, batch.windowRowStart[pid], count);
        }
      }
      continue;
    }
    const bool hasNulls = klass == BatchNullClass::kSomeNulls;
    switch (rowType->childAt(col)->kind()) {
      case TypeKind::SMALLINT:
        dispatchEncoded<int16_t>(col, batch, hasNulls);
        break;
      case TypeKind::INTEGER:
        dispatchEncoded<int32_t>(col, batch, hasNulls);
        break;
      case TypeKind::BIGINT:
        dispatchEncoded<int64_t>(col, batch, hasNulls);
        break;
      case TypeKind::TINYINT:
        dispatchRaw<int8_t>(col, batch, hasNulls);
        break;
      case TypeKind::REAL:
        dispatchRaw<float>(col, batch, hasNulls);
        break;
      case TypeKind::DOUBLE:
        dispatchRaw<double>(col, batch, hasNulls);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const bool indexed = !(*batch.decoded)[col].isIdentityMapping();
        if (dictEnabled_[col] != 0) {
          if (hasNulls) {
            indexed ? splitStringDict<true, true>(col, batch)
                    : splitStringDict<true, false>(col, batch);
          } else {
            indexed ? splitStringDict<false, true>(col, batch)
                    : splitStringDict<false, false>(col, batch);
          }
        } else if (hasNulls) {
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
  // Dictionary columns first: a partition still in dictionary mode has raw
  // index bytes in its length cache (flushed raw, so the generic loop below
  // sees an empty cursor and cannot re-encode them) and owes the closing
  // 0xFF framing to its data stream. A partition with no indexed row wrote
  // nothing and gets no framing (an empty-stream column, spec section 9).
  for (uint32_t col = 0; col < layout_->numColumns(); ++col) {
    if (dictEnabled_[col] == 0) {
      continue;
    }
    const uint32_t lengthStream = layout_->columnStream(col);
    const uint32_t dataStream = lengthStream + 1;
    uint8_t* lengthCur = cursors(lengthStream);
    auto* states = dictStates_[col].data();
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      DictState& st = states[pid];
      if (st.mode != DictState::kModeDict) {
        continue; // demoted: framing closed at demote time
      }
      if (st.matched == 0) {
        continue; // no value this window
      }
      if (lengthCur[pid] > 0) {
        flushRaw(lengthStream, pid, lengthCur);
      }
      closeDictSegment(dataStream, pid, st, /*last=*/true);
    }
  }
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
  // A payload is self-contained: every partition re-enters dictionary mode
  // with an empty open segment for the next window.
  for (uint32_t col = 0; col < layout_->numColumns(); ++col) {
    if (dictEnabled_[col] == 0) {
      continue;
    }
    for (auto& st : dictStates_[col]) {
      st.matched = 0;
      st.mode = DictState::kModeDict;
      st.entryCount = 0;
      st.entryBytes = 0;
    }
  }
}

// The fixed-width split templates are only referenced from this translation
// unit; no explicit instantiation is needed.

} // namespace bytedance::bolt::shuffle::sparksql::cell
