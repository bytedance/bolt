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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include <folly/CPortability.h>
#include <folly/Likely.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

// Buffers shuffle data by partition and stream, with chunk-backed cell
// allocation, data chains, and null tracking.

/// The Cell shuffle writer's chunk allocator: the only component that talks to
/// the engine MemoryPool for cell data.
///
/// "Chunk owns memory, Cell owns data": memory is requested from and returned
/// to the pool exclusively in fixed-size chunks (default 4MB, matching the
/// pool's reservation quantum), each carved into fixed-size cells. Cells are
/// handed out by id; what a cell holds is the concern of the directory layer
/// above.
///
/// Accounting is exact and O(1): allocatedBytes() is live chunks times chunk
/// size, which is precisely the pool-visible footprint; usedCells() counts
/// cells currently handed out.
///
/// Not thread-safe; a shuffle writer is single-threaded.
class ChunkAllocator {
 public:
  static constexpr uint32_t kInvalidCell = 0xFFFFFFFFu;

  /// Called when the freelist is empty, before the allocator asks the pool
  /// for a new chunk: the caller's budget choke point. It may reserve pool
  /// memory for the chunk, or free memory by spilling and recycle()-ing
  /// cells; the allocator takes recycled cells first and grows only if none
  /// came back. It must not allocate cells from this allocator. It may also
  /// leave the growth to proceed unreserved, and the pool's own arbitration
  /// then decides.
  using ReserveChunkFn = std::function<void()>;

  /// Both sizes must be powers of two, 0 < cellBytes <= chunkBytes.
  /// Stores reserveChunk for later allocations; construction does not call
  /// it.
  ChunkAllocator(
      memory::MemoryPool* pool,
      uint32_t chunkBytes,
      uint32_t cellBytes,
      ReserveChunkFn reserveChunk = {});
  ~ChunkAllocator();

  ChunkAllocator(const ChunkAllocator&) = delete;
  ChunkAllocator& operator=(const ChunkAllocator&) = delete;

  /// Returns a cell id. Order of preference: freelist, never-yet-handed slot
  /// of a retained chunk, then a new chunk from the pool (calling
  /// reserveChunk first and re-checking the freelist after it, since it may
  /// spill and recycle).
  uint32_t allocCell();

  /// O(1) id -> address. Valid until the owning chunk is released by
  /// shrink() or destruction.
  char* cellData(uint32_t cellId) const {
    return chunks_[cellId >> cellsPerChunkShift_].data<char>() +
        (static_cast<size_t>(cellId & cellsPerChunkMask_) << cellBytesShift_);
  }

  uint32_t cellBytes() const {
    return cellBytes_;
  }

  /// Upper bound (exclusive) of ids ever handed out; sizes side arrays
  /// indexed by cell id.
  uint32_t cellIdCapacity() const {
    return static_cast<uint32_t>(chunks_.size()) << cellsPerChunkShift_;
  }

  /// Sorts the freelist so reuse consumes cells chunk by chunk (lowest
  /// chunk first). Called after a full recycle: refilling then packs the
  /// leading chunks and leaves trailing chunks untouched, so shrink()
  /// can hand idle memory back without a spill - the property that makes
  /// declining a reclaim an honest, materially compliant answer.
  void packFreelist();

  /// Returns a cell to the freelist for reuse. The chunk stays with the
  /// allocator until shrink().
  void recycle(uint32_t cellId);

  /// Logically frees every cell and invalidates all outstanding ids; chunks
  /// are retained for reuse. Used after a full drain when more input is
  /// expected.
  void resetAll();

  /// Returns fully idle chunks (no live cell) to the pool. Freelist entries
  /// pointing into released chunks are dropped. Returns bytes released.
  int64_t shrink();

  // Evacuation: moving live cells out of chunks so shrink() can return
  // them. DataCells::compact() drives it.

  /// Chunk slots, retained or holes; a chunk index is below this.
  uint32_t numChunkSlots() const {
    return static_cast<uint32_t>(chunks_.size());
  }

  /// Whether slot `chunk` holds memory (a hole left by shrink() does not).
  bool chunkRetained(uint32_t chunk) const {
    return !chunks_[chunk].empty();
  }

  /// Cells handed out of `chunk` and not recycled.
  uint32_t chunkLiveCells(uint32_t chunk) const {
    return chunkLiveCells_[chunk];
  }

  uint32_t chunkOf(uint32_t cellId) const {
    return cellId >> cellsPerChunkShift_;
  }

  uint32_t cellsPerChunk() const {
    return 1u << cellsPerChunkShift_;
  }

  /// Until endEvacuation(), allocForEvacuation() hands out cells only from
  /// retained chunks whose `evacuating` entry is 0, and nothing grows.
  void beginEvacuation(const std::vector<uint8_t>& evacuating);

  /// A free cell outside the evacuating chunks; kInvalidCell when none is
  /// left. Never calls reserveChunk or the pool.
  uint32_t allocForEvacuation();

  void endEvacuation();

  /// Live chunks times chunk size: exactly the pool-visible footprint.
  int64_t allocatedBytes() const {
    return static_cast<int64_t>(liveChunks_) * chunkBytes_;
  }

  int64_t usedCells() const {
    return usedCells_;
  }

  int64_t usedBytes() const {
    return usedCells_ * static_cast<int64_t>(cellBytes_);
  }

 private:
  static constexpr uint32_t kNoChunk = 0xFFFFFFFFu;

  /// Allocates a chunk from the pool into a hole slot or a new slot and makes
  /// it the current bump chunk.
  void growChunk();

  /// Points bumpChunk_ at a retained chunk that still has never-handed slots,
  /// or kNoChunk when none exists.
  void findBumpChunk();

  memory::MemoryPool* const pool_;
  const ReserveChunkFn reserveChunk_;
  const uint32_t chunkBytes_;
  const uint32_t cellBytes_;
  uint32_t cellBytesShift_;
  uint32_t cellsPerChunkShift_;
  uint32_t cellsPerChunkMask_;

  /// Chunk slots; an empty ContiguousAllocation is a hole left by shrink().
  std::vector<memory::ContiguousAllocation> chunks_;
  /// Next never-handed slot per chunk ("bump watermark").
  std::vector<uint32_t> chunkBump_;
  /// Live (handed, not recycled) cells per chunk.
  std::vector<uint32_t> chunkLiveCells_;
  /// Cells handed back by recycle(), LIFO.
  std::vector<uint32_t> freeList_;

  /// During an evacuation: the chunks being emptied, and the freelist
  /// entries inside them, set aside until endEvacuation().
  std::vector<uint8_t> evacuating_;
  std::vector<uint32_t> parkedFree_;

  /// Chunk currently served by bump allocation, or kNoChunk.
  uint32_t bumpChunk_{kNoChunk};

  uint32_t liveChunks_{0};
  int64_t usedCells_{0};
};

/// The Cell shuffle writer's (partition, stream) -> cell-chain
/// directory. Cells express data ownership; all real memory lives in the
/// ChunkAllocator below.
///
/// A chain's cells are all full except the last. Chain order is append
/// order, which is the byte order of the stream. Each chain keeps its tail's
/// write position as a pointer: a linked cell's chunk is never freed or
/// moved, so the hot appends reach the cell without the cell id arithmetic.
class DataCells {
 public:
  DataCells(
      memory::MemoryPool* pool,
      ChunkAllocator* allocator,
      uint32_t numPartitions,
      uint32_t numStreams);
  ~DataCells();

  DataCells(const DataCells&) = delete;
  DataCells& operator=(const DataCells&) = delete;

  /// Appends bytes to the (pid, stream) chain, allocating cells as needed.
  ///
  /// Spill-safe by construction: every cell the append may need is allocated
  /// (held unlinked) before a single byte is copied, so a spill fired from
  /// the allocator's reserveChunk or the pool's arbitration during chunk growth
  /// sees only complete prior appends. The spill must release chains via
  /// releaseAll()/releasePartition() (recycling), never the allocator's
  /// resetAll(), so held ids stay valid.
  void append(uint32_t pid, uint32_t stream, const void* data, uint32_t bytes) {
    auto& info = infos_[chainIndex(pid, stream)];
    // Fast path: the bytes fit the tail cell - the overwhelmingly common
    // case. Nothing is allocated, so no spill can fire mid-append. An empty
    // chain has tailLeft 0, and bytes == 0 wraps past it: both go slow.
    if (FOLLY_LIKELY(bytes - 1 < info.tailLeft)) {
      ::memcpy(info.tail, data, bytes);
      info.tail += bytes;
      info.tailLeft -= bytes;
      totalBytes_ += bytes;
      return;
    }
    appendSlow(pid, stream, data, bytes);
  }

  /// Contiguous scratch inside the current tail cell, or nullptr when the
  /// tail cannot hold maxBytes. Allocates nothing, so no spill can fire
  /// between a reserve and its commit; the caller writes up to maxBytes and
  /// commits the actual size.
  char* tryReserve(uint32_t pid, uint32_t stream, uint32_t maxBytes) {
    const auto& info = infos_[chainIndex(pid, stream)];
    return maxBytes <= info.tailLeft && info.tail != nullptr ? info.tail
                                                             : nullptr;
  }

  void commit(uint32_t pid, uint32_t stream, uint32_t bytes) {
    auto& info = infos_[chainIndex(pid, stream)];
    info.tail += bytes;
    info.tailLeft -= bytes;
    totalBytes_ += bytes;
  }

  /// Recycles every chain's cells back to the allocator (the Run drain's
  /// release step). Unlike ChunkAllocator::resetAll, ids not owned by any
  /// chain survive.
  void releaseAll();

  /// Total bytes buffered for (pid, stream).
  uint64_t bytes(uint32_t pid, uint32_t stream) const {
    return chainBytes(infos_[chainIndex(pid, stream)]);
  }

  /// Visits the chain in byte order: fn(const char* data, uint32_t bytes).
  template <typename F>
  void scan(uint32_t pid, uint32_t stream, F&& fn) const {
    const auto& info = infos_[chainIndex(pid, stream)];
    if (info.numCells == 0) {
      return;
    }
    uint32_t id = info.firstCell;
    for (uint32_t i = 0; i + 1 < info.numCells; ++i) {
      fn(allocator_->cellData(id), allocator_->cellBytes());
      id = next_[id];
    }
    fn(allocator_->cellData(id), allocator_->cellBytes() - info.tailLeft);
  }

  /// Clears every chain. The caller resets or shrinks the allocator; ids
  /// held by this directory are invalid afterwards.
  void reset();

  /// Recycles the cells of one partition back to the allocator: a partial
  /// Run's release step.
  void releasePartition(uint32_t pid);

  /// Empties chunks so shrink() can return at least `targetBytes`, when the
  /// other chunks have room: moves the chained cells of the least-occupied
  /// chunks into free cells elsewhere and relinks them. Cells are spread
  /// over the chunks without regard to partition, so after a partial Run
  /// every chunk is still partly live and frees nothing on its own. A chunk
  /// holding a cell an append holds unlinked stays put. Allocates nothing
  /// from the pool. Returns the bytes of the chunks emptied.
  uint64_t compact(uint64_t targetBytes);

  /// Cell ids the chain links cover: every id the allocator has handed out
  /// once an append returns.
  uint32_t nextCapacity() const {
    return nextCapacity_;
  }

  uint64_t totalBytes() const {
    return totalBytes_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  uint32_t numStreams() const {
    return numStreams_;
  }

 private:
  /// One chain, in half a cache line: the append path reads tail and
  /// tailLeft only.
  struct alignas(32) ChainInfo {
    /// Write position in the last cell; nullptr while the chain is empty.
    char* tail{nullptr};
    /// Bytes left in the last cell; 0 while the chain is empty.
    uint32_t tailLeft{0};
    uint32_t firstCell{ChunkAllocator::kInvalidCell};
    uint32_t lastCell{ChunkAllocator::kInvalidCell};
    uint32_t numCells{0};
  };
  static_assert(sizeof(ChainInfo) == 32);

  uint64_t chainBytes(const ChainInfo& info) const {
    return static_cast<uint64_t>(info.numCells) * allocator_->cellBytes() -
        info.tailLeft;
  }

  void
  appendSlow(uint32_t pid, uint32_t stream, const void* data, uint32_t bytes);

  /// Stream-major: one column's chains are contiguous, matching the cache
  /// layer's per-column flush pattern.
  size_t chainIndex(uint32_t pid, uint32_t stream) const {
    return static_cast<size_t>(stream) * numPartitions_ + pid;
  }

  /// Links an already-allocated cell to the end of the chain.
  void linkCell(ChainInfo& info, uint32_t id);

  /// Grows next_ to cover cell ids below `capacity`.
  void reserveNext(uint32_t capacity);

  memory::MemoryPool* const pool_;
  ChunkAllocator* const allocator_;
  const uint32_t numPartitions_;
  const uint32_t numStreams_;

  /// numStreams * numPartitions chain infos; pool-backed.
  ChainInfo* infos_{nullptr};
  /// cellId -> next cell in its chain; grows with the allocator's id space.
  uint32_t* next_{nullptr};
  uint32_t nextCapacity_{0};
  uint64_t totalBytes_{0};
};

/// The null shape of a column's rows.
///
/// For a batch, its classification by the writer, with a word-level scan
/// of the decoded nulls, because mayHaveNulls() only says a nulls buffer
/// EXISTS - upstream operators routinely attach defensively allocated,
/// all-set buffers, and trusting them would tax every row of the loop for
/// nulls that are not there. kSomeNulls may be conservative: wrapped nulls,
/// or a stale buffer judged row by row, may turn out to hold none.
///
/// For a window, the join of its batches' classes: kNoNulls or kAllNull
/// while every batch is, kSomeNulls once they differ.
enum class NullClass : uint8_t {
  kNoNulls = 0, // buffer absent, all-set, or constant non-null
  kSomeNulls = 1, // mixed: the per-row null path applies
  kAllNull = 2, // every row null: one counted run per partition
};

/// The null state of the current checkpoint window, per non-UNKNOWN wire
/// column and partition, in the null body's bit order: bit 1 = non-null,
/// bit 0 = null. All null information lives here.
///
/// A column first has a state of its own, kept for the whole window: while
/// every row so far is non-null, or every row so far null, the state says
/// it all and nothing else is stored or touched - an all-null column costs
/// nothing per batch. Only a column that holds both gets storage, at its
/// first recorded null: a cell per partition, the column's cells contiguous
/// for locality.
///
/// A cell holds a stretch of a partition's rows in 32 bytes, a null record
/// of the null body (CellFormat.h) after a 4-byte head: a run of up to
/// 32767 rows all null or all non-null, then a bitmap of up to 200 rows.
/// Only null rows are recorded; the non-null rows before one join the run
/// or the bitmap as it passes them, and the rows after the last recorded
/// null are non-null. The cell in the column's array is the partition's
/// current one, whose head is the rows recorded: recording a null touches
/// that cell alone. Once full it moves to a cell of a chunk allocator,
/// whose head links the cell moved before it, and the partition's progress
/// - the last cell moved, the nulls and the count of the cells moved - is
/// updated then only. A partition's rows are the chain read from its last
/// moved cell back, then the current cell. A released partition's cells go
/// back to the allocator for reuse, and everything returns to the pool when
/// the window closes.
class NullCells {
 public:
  NullCells(
      memory::MemoryPool* pool,
      uint32_t numPartitions,
      uint32_t numColumns);
  ~NullCells();

  NullCells(const NullCells&) = delete;
  NullCells& operator=(const NullCells&) = delete;

  /// Joins `batch`, one batch's class in column col, into the column's
  /// class for the window, and returns whether the batch's nulls must be
  /// recorded through setNull()/setNullRun(): only once the column's class
  /// is kSomeNulls. `windowRows` gives each partition's rows before this
  /// batch: a column turning kSomeNulls after all-null rows records those
  /// as a null run per partition, once.
  bool observeBatch(uint32_t col, NullClass batch, const uint32_t* windowRows);

  /// Marks rowInWindow (0-based since the partition's payload opened) of
  /// column col in partition pid as null. Only for a column that
  /// observeBatch() found mixed. Rows are visited in strictly increasing
  /// order per partition.
  ///
  /// Forced inline: the split loops call it per null row, and the compiler
  /// otherwise keeps it out of line behind a PLT call. A null that lands in
  /// the cell's bitmap or extends its null run is recorded here, in the
  /// partition's current cell alone; starting a run or a bitmap, moving a
  /// full cell and opening the column stay out of line.
  FOLLY_ALWAYS_INLINE void
  setNull(uint32_t pid, uint32_t col, uint32_t rowInWindow) {
    Column& column = columnFor(col);
    Cell& cell = column.cells[pid];
    if (cell.fill != 0) {
      // The rows since the last recorded one are non-null: bits already set.
      const uint32_t bit = cell.fill + (rowInWindow - cell.head);
      if (FOLLY_LIKELY(bit < kCellBits)) {
        cell.bits[bit >> 3] &= static_cast<uint8_t>(~(1u << (bit & 7)));
        cell.fill = static_cast<uint8_t>(bit + 1);
        cell.head = rowInWindow + 1;
        return;
      }
    } else if (
        rowInWindow == cell.head && cell.run >= kNullRun &&
        cell.run < kNullRun + kMaxRunRows) {
      // The next row of the cell's null run, which has room for it.
      ++cell.run;
      ++cell.head;
      return;
    }
    appendNulls(column, pid, rowInWindow, 1);
  }

  /// Bulk form for a run of consecutive rows that are all null in this
  /// column. Only for a mixed column, as setNull().
  void
  setNullRun(uint32_t pid, uint32_t col, uint32_t startRow, uint32_t count) {
    appendNulls(columnFor(col), pid, startRow, count);
  }

  struct Summary {
    NullTag tag;
    uint32_t nonNullCount;
  };

  /// Summarizes (pid, col) over the first rowCount rows of the partition,
  /// which cover every row recorded.
  Summary summarize(uint32_t pid, uint32_t col, uint32_t rowCount) const;

  /// Bytes of the null records of (pid, col), a column summarize() found
  /// kRawNull: their u32 count and the records.
  size_t recordsBytes(uint32_t pid, uint32_t col) const {
    return sizeof(uint32_t) +
        kNullRecordBytes * (size_t{columns_[col].progress[pid].moved} + 1);
  }

  /// Writes the null records of partition pid's columns cols[0..numCols),
  /// each one summarize() found kRawNull, at `out`: per column in turn,
  /// recordsBytes() of them - its record count, then its cells as records
  /// in row order. The columns' cells are read in step, each cell's
  /// predecessor fetched while the other columns' cells are written, so
  /// that their cache misses overlap.
  void emitRecords(
      uint32_t pid,
      const uint32_t* cols,
      uint32_t numCols,
      uint8_t* out) const;

  /// Drops every column's state and cells (checkpoint window close).
  void reset();

  /// Empties one partition's cells, its payload having closed; the
  /// columns' states, which hold for the rows still to come, stay.
  void releasePartition(uint32_t pid);

  /// Bytes of the partitions' moved cells; input to the checkpoint
  /// trigger. Not in it, as they stay bounded whatever the window's size:
  /// the columns' cell arrays, 44 bytes per partition of a column with
  /// nulls recorded.
  int64_t allocatedBytes() const {
    return chainCells_.usedBytes();
  }

  /// Bytes of the columns' cell arrays.
  int64_t tableBytes() const {
    return tableBytes_;
  }

  /// Every byte held from the pool: the cell arrays and the allocator's
  /// chunks, free cells included.
  int64_t footprintBytes() const {
    return tableBytes_ + chainCells_.allocatedBytes();
  }

 private:
  /// A stretch of a partition's rows: a head, then a null record whose
  /// bitmap's bits past `fill` are 1, not 0 as on the wire.
  struct Cell {
    /// In a partition's current cell, the rows recorded: the partition
    /// holds rows [0, head). In a moved cell, the cell moved before it;
    /// kInvalidCell for the first.
    uint32_t head;
    /// The run's rows, plus kNullRun when they are null.
    uint16_t run;
    /// Rows in `bits`, after the run's.
    uint8_t fill;
    /// Bit 1 = non-null; the bits past `fill` stay 1.
    uint8_t bits[kNullRecordBitmapBytes];
  };
  static_assert(sizeof(Cell) == 32);
  static_assert(
      offsetof(Cell, run) == sizeof(uint32_t) &&
          sizeof(Cell) - offsetof(Cell, run) == kNullRecordBytes,
      "a cell after its 4-byte head is a null record");
  static_assert(
      kNullRecordBitmapBytes * 8 == kNullRecordBits,
      "a cell's nulls are its bitmap's zero bits");
  static constexpr uint16_t kNullRun = kNullRecordRunNull;
  static constexpr uint32_t kMaxRunRows = kNullRecordMaxRun;
  static constexpr uint32_t kCellBits = kNullRecordBits;

  /// A partition's moved cells in a column, updated as a cell moves.
  struct Progress {
    /// The last cell moved, the head of the chain; kInvalidCell for none.
    uint32_t last;
    /// The null rows in the moved cells.
    uint32_t nulls;
    /// The cells moved.
    uint32_t moved;
  };

  /// A mixed column's storage, one current cell and one progress per
  /// partition, in one block; nullptr until the column records a null. A
  /// batch's nulls can be classified conservatively, so a mixed column may
  /// never record one.
  struct Column {
    Cell* cells{nullptr};
    Progress* progress{nullptr};
    void* block{nullptr};
  };

  FOLLY_ALWAYS_INLINE Column& columnFor(uint32_t col) {
    BOLT_DCHECK(
        states_[col] == NullClass::kSomeNulls, "nulls of an unmixed column");
    Column& column = columns_[col];
    if (FOLLY_UNLIKELY(column.cells == nullptr)) {
      openColumn(column);
    }
    return column;
  }

  const Cell& chainCell(uint32_t id) const {
    return *reinterpret_cast<const Cell*>(chainCells_.cellData(id));
  }

  /// A cell holding no row, whose head is `head`.
  static Cell emptyCell(uint32_t head);
  /// The null rows `cell` records.
  static uint32_t cellNulls(const Cell& cell);
  size_t columnBlockBytes() const;
  void openColumn(Column& column);
  /// Records the non-null rows before `row`, then `count` null rows.
  void appendNulls(Column& column, uint32_t pid, uint32_t row, uint32_t count);
  /// Records the next `rows` rows of the partition, all null or all not.
  void appendRows(Column& column, uint32_t pid, bool null, uint32_t rows);
  /// Moves the partition's full current cell to the allocator, linked to
  /// the cell moved before it, leaving an empty current cell.
  void moveCell(Column& column, uint32_t pid);

  memory::MemoryPool* const pool_;
  const uint32_t numPartitions_;
  const uint32_t numColumns_;

  /// Per column, its class over the window so far; nullopt before its
  /// first batch. Only a kSomeNulls column stores anything.
  std::vector<std::optional<NullClass>> states_;
  std::vector<Column> columns_;
  /// The moved cells of every column and partition.
  ChunkAllocator chainCells_;
  int64_t tableBytes_{0};
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
