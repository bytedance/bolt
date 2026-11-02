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

#include "bolt/shuffle/sparksql/cell/CellBuffer.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <utility>

#include <folly/ScopeGuard.h>
#include <folly/lang/Bits.h>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Allocation.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

ChunkAllocator::ChunkAllocator(
    memory::MemoryPool* pool,
    uint32_t chunkBytes,
    uint32_t cellBytes,
    ReserveChunkFn reserveChunk)
    : pool_(pool),
      reserveChunk_(std::move(reserveChunk)),
      chunkBytes_(chunkBytes),
      cellBytes_(cellBytes) {
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK(
      chunkBytes_ > 0 && bits::isPowerOfTwo(chunkBytes_),
      "chunkBytes must be a power of two: {}",
      chunkBytes_);
  BOLT_CHECK(
      cellBytes_ > 0 && bits::isPowerOfTwo(cellBytes_) &&
          cellBytes_ <= chunkBytes_,
      "cellBytes must be a power of two <= chunkBytes: {} vs {}",
      cellBytes_,
      chunkBytes_);
  BOLT_CHECK_EQ(
      chunkBytes_ % memory::AllocationTraits::kPageSize,
      0,
      "chunkBytes must be page-aligned");
  cellBytesShift_ = static_cast<uint32_t>(__builtin_ctz(cellBytes_));
  cellsPerChunkShift_ =
      static_cast<uint32_t>(__builtin_ctz(chunkBytes_ / cellBytes_));
  cellsPerChunkMask_ = (1u << cellsPerChunkShift_) - 1;
}

ChunkAllocator::~ChunkAllocator() {
  for (auto& chunk : chunks_) {
    if (!chunk.empty()) {
      pool_->freeContiguous(chunk);
    }
  }
}

uint32_t ChunkAllocator::allocCell() {
  if (FOLLY_LIKELY(!freeList_.empty())) {
    const uint32_t id = freeList_.back();
    freeList_.pop_back();
    ++chunkLiveCells_[id >> cellsPerChunkShift_];
    ++usedCells_;
    return id;
  }
  if (bumpChunk_ == kNoChunk) {
    findBumpChunk();
  }
  if (bumpChunk_ == kNoChunk) {
    if (reserveChunk_) {
      reserveChunk_();
      // reserveChunk may have spilled and recycled cells.
      if (!freeList_.empty()) {
        const uint32_t id = freeList_.back();
        freeList_.pop_back();
        ++chunkLiveCells_[id >> cellsPerChunkShift_];
        ++usedCells_;
        return id;
      }
      findBumpChunk();
    }
    if (bumpChunk_ == kNoChunk) {
      growChunk();
    }
  }
  const uint32_t slot = chunkBump_[bumpChunk_]++;
  const uint32_t id = (bumpChunk_ << cellsPerChunkShift_) | slot;
  ++chunkLiveCells_[bumpChunk_];
  ++usedCells_;
  if (chunkBump_[bumpChunk_] == cellsPerChunk()) {
    bumpChunk_ = kNoChunk;
  }
  return id;
}

void ChunkAllocator::packFreelist() {
  // Descending ids: allocation pops from the back, so the lowest chunks
  // are consumed first and refills stay dense.
  std::sort(freeList_.begin(), freeList_.end(), std::greater<uint32_t>());
}

void ChunkAllocator::recycle(uint32_t cellId) {
  BOLT_DCHECK_LT(cellId >> cellsPerChunkShift_, chunks_.size());
  BOLT_DCHECK_GT(chunkLiveCells_[cellId >> cellsPerChunkShift_], 0);
  // A cell recycled out of a chunk being evacuated must not be handed out
  // again by the evacuation itself.
  if (FOLLY_UNLIKELY(!evacuating_.empty()) &&
      evacuating_[cellId >> cellsPerChunkShift_] != 0) {
    parkedFree_.push_back(cellId);
  } else {
    freeList_.push_back(cellId);
  }
  --chunkLiveCells_[cellId >> cellsPerChunkShift_];
  --usedCells_;
}

void ChunkAllocator::resetAll() {
  freeList_.clear();
  std::fill(chunkBump_.begin(), chunkBump_.end(), 0);
  std::fill(chunkLiveCells_.begin(), chunkLiveCells_.end(), 0);
  usedCells_ = 0;
  bumpChunk_ = kNoChunk;
  findBumpChunk();
}

int64_t ChunkAllocator::shrink() {
  int64_t released = 0;
  bool anyReleased = false;
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (chunks_[i].empty() || chunkLiveCells_[i] != 0) {
      continue;
    }
    pool_->freeContiguous(chunks_[i]);
    chunkBump_[i] = 0;
    --liveChunks_;
    released += chunkBytes_;
    anyReleased = true;
    if (bumpChunk_ == i) {
      bumpChunk_ = kNoChunk;
    }
  }
  if (anyReleased && !freeList_.empty()) {
    // Drop freelist entries that lived in released chunks.
    size_t kept = 0;
    for (const uint32_t id : freeList_) {
      if (!chunks_[id >> cellsPerChunkShift_].empty()) {
        freeList_[kept++] = id;
      }
    }
    freeList_.resize(kept);
  }
  return released;
}

void ChunkAllocator::beginEvacuation(const std::vector<uint8_t>& evacuating) {
  BOLT_CHECK(evacuating_.empty(), "nested chunk evacuation");
  evacuating_ = evacuating;
  evacuating_.resize(chunks_.size(), 0);
  size_t kept = 0;
  for (const uint32_t id : freeList_) {
    if (evacuating_[id >> cellsPerChunkShift_] != 0) {
      parkedFree_.push_back(id);
    } else {
      freeList_[kept++] = id;
    }
  }
  freeList_.resize(kept);
}

uint32_t ChunkAllocator::allocForEvacuation() {
  if (!freeList_.empty()) {
    const uint32_t id = freeList_.back();
    freeList_.pop_back();
    ++chunkLiveCells_[id >> cellsPerChunkShift_];
    ++usedCells_;
    return id;
  }
  for (uint32_t chunk = 0; chunk < chunks_.size(); ++chunk) {
    if (chunks_[chunk].empty() || evacuating_[chunk] != 0 ||
        chunkBump_[chunk] == cellsPerChunk()) {
      continue;
    }
    const uint32_t id = (chunk << cellsPerChunkShift_) | chunkBump_[chunk]++;
    ++chunkLiveCells_[chunk];
    ++usedCells_;
    if (bumpChunk_ == chunk && chunkBump_[chunk] == cellsPerChunk()) {
      bumpChunk_ = kNoChunk;
    }
    return id;
  }
  return kInvalidCell;
}

void ChunkAllocator::endEvacuation() {
  freeList_.insert(freeList_.end(), parkedFree_.begin(), parkedFree_.end());
  parkedFree_.clear();
  evacuating_.clear();
}

void ChunkAllocator::growChunk() {
  uint32_t slot = kNoChunk;
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (chunks_[i].empty()) {
      slot = i;
      break;
    }
  }
  if (slot == kNoChunk) {
    chunks_.emplace_back();
    chunkBump_.push_back(0);
    chunkLiveCells_.push_back(0);
    slot = static_cast<uint32_t>(chunks_.size() - 1);
  }
  pool_->allocateContiguous(
      memory::AllocationTraits::numPages(chunkBytes_), chunks_[slot]);
  chunkBump_[slot] = 0;
  chunkLiveCells_[slot] = 0;
  ++liveChunks_;
  bumpChunk_ = slot;
}

void ChunkAllocator::findBumpChunk() {
  for (uint32_t i = 0; i < chunks_.size(); ++i) {
    if (!chunks_[i].empty() && chunkBump_[i] < cellsPerChunk()) {
      bumpChunk_ = i;
      return;
    }
  }
  bumpChunk_ = kNoChunk;
}

DataCells::DataCells(
    memory::MemoryPool* pool,
    ChunkAllocator* allocator,
    uint32_t numPartitions,
    uint32_t numStreams)
    : pool_(pool),
      allocator_(allocator),
      numPartitions_(numPartitions),
      numStreams_(numStreams) {
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  if (numChains == 0) {
    return;
  }
  infos_ = reinterpret_cast<ChainInfo*>(
      pool_->allocate(numChains * sizeof(ChainInfo)));
  // The pool aligns to 64 bytes: every ChainInfo stays in one cache line.
  BOLT_DCHECK_EQ(reinterpret_cast<uintptr_t>(infos_) % alignof(ChainInfo), 0);
  if (numChains != 0) {
    std::fill(infos_, infos_ + numChains, ChainInfo{});
  }
}

DataCells::~DataCells() {
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  if (infos_ != nullptr) {
    pool_->free(infos_, numChains * sizeof(ChainInfo));
  }
  if (next_ != nullptr) {
    pool_->free(next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
  }
}

void DataCells::reserveNext(uint32_t capacity) {
  if (capacity <= nextCapacity_) {
    return;
  }
  // At least doubling: next_ grows a few times per task, not once per
  // chunk, so its copies stay linear in its final size.
  const uint32_t newCapacity = static_cast<uint32_t>(std::max<uint64_t>(
      capacity,
      std::min<uint64_t>(
          2ULL * nextCapacity_, std::numeric_limits<uint32_t>::max())));
  // The pool may reclaim from this writer while it allocates; next_ is
  // copied only afterwards, so a compaction run by that reclaim is kept.
  auto* grown = reinterpret_cast<uint32_t*>(
      pool_->allocate(static_cast<size_t>(newCapacity) * sizeof(uint32_t)));
  if (next_ != nullptr) {
    ::memcpy(
        grown, next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
    pool_->free(next_, static_cast<size_t>(nextCapacity_) * sizeof(uint32_t));
  }
  next_ = grown;
  nextCapacity_ = newCapacity;
}

void DataCells::appendSlow(
    uint32_t pid,
    uint32_t stream,
    const void* data,
    uint32_t bytes) {
  if (bytes == 0) {
    return;
  }
  const uint32_t cellBytes = allocator_->cellBytes();
  // Phase 1: allocate every cell this append could need, before touching the
  // chain or copying a byte. The allocator's reserveChunk or pool arbitration
  // during chunk growth may spill and release all chains here; the held ids
  // are unlinked and survive. One spare covers the tail the spill may have
  // taken away.
  const uint32_t maxNeeded = bytes / cellBytes + 2;
  uint32_t held[2];
  std::vector<uint32_t> heldOverflow;
  // Reserved up front, so that no held id is lost to a failing push_back.
  heldOverflow.reserve(maxNeeded > 2 ? maxNeeded - 2 : 0);
  uint32_t heldCount = 0;
  const auto heldAt = [&](uint32_t i) {
    return i < 2 ? held[i] : heldOverflow[i - 2];
  };
  {
    // An allocation failing here leaves the chain as it was: the cells
    // already held go back.
    auto release = folly::makeGuard([&] {
      for (uint32_t i = 0; i < heldCount; ++i) {
        allocator_->recycle(heldAt(i));
      }
    });
    for (uint32_t i = 0; i < maxNeeded; ++i) {
      const uint32_t id = allocator_->allocCell();
      if (heldCount < 2) {
        held[heldCount] = id;
      } else {
        heldOverflow.push_back(id);
      }
      ++heldCount;
    }
    // next_ must cover the held ids before phase 2 links them: growing it
    // there would allocate - and possibly reclaim, spilling this chain -
    // with part of the bytes already copied, splitting one append across
    // Runs.
    reserveNext(allocator_->cellIdCapacity());
    release.dismiss();
  }

  // Phase 2: re-read the chain (a spill may have emptied it), link held
  // cells as needed, copy, and recycle the surplus.
  auto& info = infos_[chainIndex(pid, stream)];
  const char* src = reinterpret_cast<const char*>(data);
  totalBytes_ += bytes;
  uint32_t nextHeld = 0;
  while (bytes > 0) {
    if (info.tailLeft == 0) {
      // Link the next held cell to the end of the chain; next_ covers it,
      // reserved in phase 1.
      const uint32_t id = heldAt(nextHeld++);
      BOLT_DCHECK_LT(id, nextCapacity_);
      next_[id] = ChunkAllocator::kInvalidCell;
      if (info.numCells == 0) {
        info.firstCell = id;
      } else {
        next_[info.lastCell] = id;
      }
      info.lastCell = id;
      info.tail = allocator_->cellData(id);
      info.tailLeft = allocator_->cellBytes();
      ++info.numCells;
    }
    const uint32_t copy = bytes < info.tailLeft ? bytes : info.tailLeft;
    ::memcpy(info.tail, src, copy);
    info.tail += copy;
    info.tailLeft -= copy;
    src += copy;
    bytes -= copy;
  }
  for (uint32_t i = nextHeld; i < heldCount; ++i) {
    allocator_->recycle(heldAt(i));
  }
}

void DataCells::releaseAll() {
  for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
    releasePartition(pid);
  }
}

uint64_t DataCells::compact(uint64_t targetBytes) {
  const uint32_t numChunks = allocator_->numChunkSlots();
  if (numChunks < 2 || targetBytes == 0) {
    return 0;
  }
  const size_t numChains = static_cast<size_t>(numPartitions_) * numStreams_;
  // Chained cells per chunk: a chunk whose live cells are all on chains can
  // be emptied; one with a cell an append holds unlinked cannot.
  std::vector<uint32_t> chained(numChunks, 0);
  for (size_t c = 0; c < numChains; ++c) {
    const auto& info = infos_[c];
    uint32_t id = info.firstCell;
    for (uint32_t i = 0; i < info.numCells; ++i) {
      ++chained[allocator_->chunkOf(id)];
      id = next_[id];
    }
  }
  const uint32_t perChunk = allocator_->cellsPerChunk();
  const uint64_t chunkBytes =
      static_cast<uint64_t>(perChunk) * allocator_->cellBytes();
  // 1 marks a chunk to empty; 2 a chunk that only receives no cells: its
  // ids lie past next_, which compact() must not grow from the pool.
  std::vector<uint8_t> excluded(numChunks, 0);
  std::vector<uint32_t> candidates;
  uint64_t room = 0;
  for (uint32_t chunk = 0; chunk < numChunks; ++chunk) {
    if (!allocator_->chunkRetained(chunk)) {
      continue;
    }
    const uint32_t live = allocator_->chunkLiveCells(chunk);
    if (static_cast<uint64_t>(chunk + 1) * perChunk > nextCapacity_) {
      excluded[chunk] = 2;
    } else {
      room += perChunk - live;
    }
    if (live == chained[chunk]) {
      candidates.push_back(chunk);
    }
  }
  std::sort(candidates.begin(), candidates.end(), [&](uint32_t a, uint32_t b) {
    return allocator_->chunkLiveCells(a) < allocator_->chunkLiveCells(b);
  });
  uint64_t moving = 0;
  uint64_t freed = 0;
  for (const uint32_t chunk : candidates) {
    if (freed >= targetBytes) {
      break;
    }
    const uint32_t live = allocator_->chunkLiveCells(chunk);
    // Emptied, the chunk's own free cells no longer count as room.
    const uint64_t own = excluded[chunk] == 0 ? perChunk - live : 0;
    if (moving + live + own > room) {
      continue;
    }
    room -= own;
    moving += live;
    excluded[chunk] = 1;
    freed += chunkBytes;
  }
  if (freed == 0) {
    return 0;
  }
  const uint32_t cellBytes = allocator_->cellBytes();
  allocator_->beginEvacuation(excluded);
  SCOPE_EXIT {
    allocator_->endEvacuation();
  };
  for (size_t c = 0; c < numChains; ++c) {
    auto& info = infos_[c];
    uint32_t prev = ChunkAllocator::kInvalidCell;
    uint32_t id = info.firstCell;
    for (uint32_t i = 0; i < info.numCells; ++i) {
      const uint32_t following = next_[id];
      if (excluded[allocator_->chunkOf(id)] == 1) {
        const uint32_t moved = allocator_->allocForEvacuation();
        BOLT_CHECK_NE(
            moved,
            ChunkAllocator::kInvalidCell,
            "cell compaction ran out of room");
        BOLT_DCHECK_LT(moved, nextCapacity_);
        ::memcpy(
            allocator_->cellData(moved), allocator_->cellData(id), cellBytes);
        next_[moved] = following;
        if (prev == ChunkAllocator::kInvalidCell) {
          info.firstCell = moved;
        } else {
          next_[prev] = moved;
        }
        if (id == info.lastCell) {
          info.lastCell = moved;
          info.tail = allocator_->cellData(moved) + (cellBytes - info.tailLeft);
        }
        allocator_->recycle(id);
        id = moved;
      }
      prev = id;
      id = following;
    }
  }
  return freed;
}

void DataCells::releasePartition(uint32_t pid) {
  for (uint32_t stream = 0; stream < numStreams_; ++stream) {
    auto& info = infos_[chainIndex(pid, stream)];
    uint32_t id = info.firstCell;
    while (id != ChunkAllocator::kInvalidCell) {
      const uint32_t following = next_[id];
      allocator_->recycle(id);
      id = following;
    }
    totalBytes_ -= chainBytes(info);
    info = ChainInfo{};
  }
}

namespace {

/// Clears bits [begin, end) of a little-endian bit array.
void clearBitRange(uint8_t* bytes, uint64_t begin, uint64_t end) {
  if (begin >= end) {
    return;
  }
  const uint64_t first = begin >> 3;
  const uint64_t last = (end - 1) >> 3;
  const auto firstMask = static_cast<uint8_t>(0xFFu << (begin & 7));
  const auto lastMask = static_cast<uint8_t>(0xFFu >> (7 - ((end - 1) & 7)));
  if (first == last) {
    bytes[first] &= static_cast<uint8_t>(~(firstMask & lastMask));
    return;
  }
  bytes[first] &= static_cast<uint8_t>(~firstMask);
  ::memset(bytes + first + 1, 0, last - first - 1);
  bytes[last] &= static_cast<uint8_t>(~lastMask);
}

/// Chunks of the moved null cells: 2048 cells each.
constexpr uint32_t kNullChunkBytes = 64 << 10;

} // namespace

NullCells::NullCells(
    memory::MemoryPool* pool,
    uint32_t numPartitions,
    uint32_t numColumns)
    : pool_(pool),
      numPartitions_(numColumns == 0 ? 0 : numPartitions),
      numColumns_(numColumns),
      states_(numColumns),
      columns_(numColumns),
      chainCells_(pool, kNullChunkBytes, sizeof(Cell)) {}

NullCells::~NullCells() {
  reset();
}

bool NullCells::observeBatch(
    uint32_t col,
    NullClass batch,
    const uint32_t* windowRows) {
  // A column's class joins its batches': it stays kNoNulls or kAllNull only
  // while every batch is.
  std::optional<NullClass>& state = states_[col];
  if (!state.has_value()) {
    state = batch;
  }
  if (*state == batch) {
    return batch == NullClass::kSomeNulls;
  }
  if (*state == NullClass::kSomeNulls) {
    return true;
  }
  const bool nullSoFar = *state == NullClass::kAllNull;
  // The column turns mixed. Its rows so far are all non-null - what
  // unrecorded rows are, needing no storage yet - or all null: each
  // partition's become a null run of its own.
  state = NullClass::kSomeNulls;
  if (nullSoFar) {
    Column& column = columnFor(col);
    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      appendRows(column, pid, true, windowRows[pid]);
    }
  }
  return true;
}

NullCells::Cell NullCells::emptyCell(uint32_t head) {
  Cell cell;
  cell.head = head;
  cell.run = 0;
  cell.fill = 0;
  ::memset(cell.bits, 0xFF, sizeof(cell.bits));
  return cell;
}

uint32_t NullCells::cellNulls(const Cell& cell) {
  // The bitmap's bits past `fill` are 1, so its zero bits are its nulls.
  uint32_t ones = 0;
  for (uint32_t at = 0; at + sizeof(uint64_t) <= sizeof(cell.bits);
       at += sizeof(uint64_t)) {
    ones +=
        __builtin_popcountll(folly::loadUnaligned<uint64_t>(cell.bits + at));
  }
  for (uint32_t at = sizeof(cell.bits) / sizeof(uint64_t) * sizeof(uint64_t);
       at < sizeof(cell.bits);
       ++at) {
    ones += __builtin_popcount(cell.bits[at]);
  }
  const uint32_t runNulls =
      (cell.run & kNullRun) != 0 ? cell.run & kMaxRunRows : 0;
  return runNulls + (kCellBits - ones);
}

size_t NullCells::columnBlockBytes() const {
  // Room to align the cells to their size, so none spans two cache lines.
  return sizeof(Cell) + (sizeof(Cell) + sizeof(Progress)) * numPartitions_;
}

void NullCells::openColumn(Column& column) {
  const size_t bytes = columnBlockBytes();
  void* block = pool_->allocate(bytes);
  auto* cells = reinterpret_cast<Cell*>(
      (reinterpret_cast<uintptr_t>(block) + sizeof(Cell) - 1) &
      ~(uintptr_t{sizeof(Cell)} - 1));
  auto* progress = reinterpret_cast<Progress*>(cells + numPartitions_);
  std::fill(cells, cells + numPartitions_, emptyCell(0));
  std::fill(
      progress,
      progress + numPartitions_,
      Progress{ChunkAllocator::kInvalidCell, 0, 0});
  column = Column{cells, progress, block};
  tableBytes_ += static_cast<int64_t>(bytes);
}

void NullCells::appendNulls(
    Column& column,
    uint32_t pid,
    uint32_t row,
    uint32_t count) {
  const uint32_t end = column.cells[pid].head;
  BOLT_DCHECK_GE(row, end, "null rows out of order");
  appendRows(column, pid, false, row - end);
  appendRows(column, pid, true, count);
}

void NullCells::appendRows(
    Column& column,
    uint32_t pid,
    bool null,
    uint32_t rows) {
  while (rows > 0) {
    Cell& cell = column.cells[pid];
    const uint32_t runRows = cell.run & kMaxRunRows;
    const bool runNull = (cell.run & kNullRun) != 0;
    uint32_t taken;
    if (cell.fill == 0 &&
        (runRows == 0 || (runNull == null && runRows < kMaxRunRows))) {
      // The rows start or extend the cell's run.
      taken = std::min(rows, kMaxRunRows - runRows);
      cell.run =
          static_cast<uint16_t>((null ? kNullRun : 0) | (runRows + taken));
    } else if (
        (cell.fill != 0 || runNull != null) && rows <= kCellBits - cell.fill) {
      // They fit the bitmap, whose bits past `fill` are already non-null.
      taken = rows;
      if (null) {
        clearBitRange(cell.bits, cell.fill, cell.fill + taken);
      }
      cell.fill = static_cast<uint8_t>(cell.fill + taken);
    } else {
      // A full run, or more rows than the bitmap can take: they start the
      // next cell.
      moveCell(column, pid);
      continue;
    }
    rows -= taken;
    cell.head += taken;
  }
}

void NullCells::moveCell(Column& column, uint32_t pid) {
  const uint32_t id = chainCells_.allocCell();
  Cell& cell = column.cells[pid];
  Progress& progress = column.progress[pid];
  auto* moved = reinterpret_cast<Cell*>(chainCells_.cellData(id));
  moved->head = progress.last;
  ::memcpy(&moved->run, &cell.run, kNullRecordBytes);
  progress.last = id;
  progress.nulls += cellNulls(cell);
  ++progress.moved;
  cell = emptyCell(cell.head);
}

NullCells::Summary
NullCells::summarize(uint32_t pid, uint32_t col, uint32_t rowCount) const {
  if (rowCount == 0) {
    return {NullTag::kNoNull, 0};
  }
  switch (states_[col].value_or(NullClass::kNoNulls)) {
    case NullClass::kNoNulls:
      return {NullTag::kNoNull, rowCount};
    case NullClass::kAllNull:
      return {NullTag::kAllNull, 0};
    case NullClass::kSomeNulls:
      break;
  }
  const Column& column = columns_[col];
  if (column.cells == nullptr) {
    return {NullTag::kNoNull, rowCount};
  }
  const Cell& cell = column.cells[pid];
  BOLT_CHECK_LE(
      cell.head, rowCount, "nulls recorded past the partition's rows");
  const uint32_t nulls = column.progress[pid].nulls + cellNulls(cell);
  if (nulls == 0) {
    return {NullTag::kNoNull, rowCount};
  }
  if (nulls == rowCount) {
    return {NullTag::kAllNull, 0};
  }
  return {NullTag::kRawNull, rowCount - nulls};
}

void NullCells::emitRecords(
    uint32_t pid,
    const uint32_t* cols,
    uint32_t numCols,
    uint8_t* out) const {
  // The current cell's record last, then the chain read from its last
  // moved cell back, each cell written as a record just before the one
  // after it.
  struct Walk {
    const Cell* cell;
    uint8_t* record;
    /// For the current cell, whose head counts rows, the cell before it:
    /// the partition's last moved cell. A moved cell's head links it.
    uint32_t currentPrev;
    bool current;
  };
  constexpr uint32_t kMaxWalks = 16;
  Walk walks[kMaxWalks];
  uint32_t numWalks = 0;
  const auto walkInStep = [&] {
    while (numWalks > 0) {
      for (uint32_t i = 0; i < numWalks;) {
        Walk& walk = walks[i];
        const Cell& cell = *walk.cell;
        const uint32_t prevId = walk.current ? walk.currentPrev : cell.head;
        const Cell* prev = prevId == ChunkAllocator::kInvalidCell
            ? nullptr
            : &chainCell(prevId);
        if (prev != nullptr) {
          __builtin_prefetch(prev);
        }
        ::memcpy(walk.record, &cell.run, kNullRecordBytes);
        // On the wire the bits past fill are zero.
        uint8_t* bits =
            walk.record + (kNullRecordBytes - kNullRecordBitmapBytes);
        const uint32_t fullBytes = cell.fill >> 3;
        if ((cell.fill & 7) != 0) {
          bits[fullBytes] &= static_cast<uint8_t>((1u << (cell.fill & 7)) - 1);
        }
        const uint32_t usedBytes = (cell.fill + 7) >> 3;
        ::memset(bits + usedBytes, 0, kNullRecordBitmapBytes - usedBytes);
        if (prev == nullptr) {
          walk = walks[--numWalks];
          continue;
        }
        walk.cell = prev;
        walk.current = false;
        walk.record -= kNullRecordBytes;
        ++i;
      }
    }
  };
  for (uint32_t i = 0; i < numCols; ++i) {
    const Column& column = columns_[cols[i]];
    BOLT_DCHECK(column.cells != nullptr, "records of a column without nulls");
    const Progress& progress = column.progress[pid];
    const uint32_t records = progress.moved + 1;
    folly::storeUnaligned<uint32_t>(out, records);
    out += sizeof(records);
    // The current cell's record goes last.
    walks[numWalks++] = {
        &column.cells[pid],
        out + size_t{kNullRecordBytes} * (records - 1),
        progress.last,
        true};
    out += size_t{kNullRecordBytes} * records;
    if (numWalks == kMaxWalks) {
      walkInStep();
    }
  }
  walkInStep();
}

void NullCells::reset() {
  for (uint32_t col = 0; col < numColumns_; ++col) {
    Column& column = columns_[col];
    if (column.block != nullptr) {
      pool_->free(column.block, columnBlockBytes());
      column = Column{};
    }
    states_[col].reset();
  }
  chainCells_.resetAll();
  chainCells_.shrink();
  tableBytes_ = 0;
}

void NullCells::releasePartition(uint32_t pid) {
  for (uint32_t col = 0; col < numColumns_; ++col) {
    Column& column = columns_[col];
    if (column.cells == nullptr) {
      continue;
    }
    uint32_t id = column.progress[pid].last;
    while (id != ChunkAllocator::kInvalidCell) {
      const uint32_t prev = chainCell(id).head;
      chainCells_.recycle(id);
      id = prev;
    }
    column.cells[pid] = emptyCell(0);
    column.progress[pid] = Progress{ChunkAllocator::kInvalidCell, 0, 0};
  }
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
