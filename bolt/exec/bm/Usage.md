# BM RowContainer Usage Guide

This guide is for execution operators that integrate `BmRowContainer`. It
documents public interfaces and lifetimes, not internal segment, chunk, block,
`StringView` rebasing, or BufferManager pin details.

Public entry points:

- `BmRowContainer.h`
- `BmRowContainerRead.h`
- `BmRowContainerPublicTypes.h`

`BmSegmentTypes.h` contains internal storage types for the `bm` implementation
and white-box tests only.

## Model

`BmRowContainer` is a row-based temporary data container.

- The write phase appends rows in memory.
- Under memory pressure, the operator spills the active segment to
  BufferManager.
- Spill returns a `SegmentId`; reload, release, and partition management use
  segment IDs.
- Resident data uses `char*` row pointers. When the full set cannot remain
  resident, use `RowId` handles and resolve them in batches through
  `ReadOnlyWindowReadSession`.

## Construction

```cpp
using namespace bytedance::bolt::exec::bm;

BmRowContainer rows(
    {BIGINT(), VARCHAR()},
    {false, true},
    0, // Number of key columns.
    bufferManager,
    memory::bm::MemoryTag::kTesting);
```

`types` and `nullable` must have matching entries. `numKeyColumns` identifies
the leading key columns. Maps in keys are serialized in sorted-key order for
stable comparison and hashing. Nullability participates in row layout; a
non-nullable column uses a shorter fast path.

## Writing Rows

```cpp
auto context = rows.appendRow(partitionId);
rows.store(context, decodedKey, sourceIndex, keyColumn);
rows.store(context, decodedPayload, sourceIndex, payloadColumn);
char* row = context.row();
```

`RowWriteContext` describes only the current row's write location. Never retain
it across containers, spills, or asynchronous operations.

## Resident Pointer Access

Comparison and extraction require resident row pointers:

```cpp
int32_t result = rows.compare(leftRow, rightRow, column, flags);
int32_t rowResult = rows.compareRows(leftRow, rightRow, keyFlags);

rows.extractColumnResident(
    rowPtrs.data(), rowPtrs.size(), column, outputVector);
```

Pointers obtained before a spill are invalid afterward. Obtain new pointers
through `BulkReadSession`, `ReadOnlyWindowReadSession`, or `MergeReadSession`.

## Spilling

Default partition:

```cpp
SegmentId segment = rows.spillActiveSegment();
```

Explicit partition:

```cpp
SegmentId segment = rows.spillActivePartitionSegment(partitionId);
```

A partition may be spilled repeatedly, which supports partitioned Hash Build:

```cpp
const auto& segments = rows.segmentsForPartition(partitionId);
```

## Bulk Read

Use bulk read when the full working set is expected to fit in memory:

```cpp
std::vector<SegmentId> segments = ...;
folly::Range<const SegmentId*> range(segments.data(), segments.size());

if (rows.canBulkRead(range)) {
  auto bulk = rows.beginBulkReadSegments(range);
  std::vector<char*> rowPtrs = bulk.loadRows();
  // rowPtrs can be used by compare and extractColumnResident.
}
```

`canBulkRead()` is conservative. Only blocks with a retained `BufferHandle`
count as loaded. Unpinned but resident blocks count against the reload reserve,
because reclaim may change their state during the `MaybeReserve()` probe.
`loadRows()` performs the real reserve and pin operation and can still throw if
memory state changes.

Returned pointers are backed by resident blocks held by the container. Bulk
read has no partial eviction; use a read-only window session when the working
set must be released incrementally.

## Window Read

When bulk read is not possible, list `RowId`s and load the operator's current
window. `RowId` is an opaque locator and must not be decoded by callers.

```cpp
auto session = rows.beginReadOnlyWindowReadSegments(range);
std::vector<RowId> rowIds = session.listRowIds();

std::vector<RowId> needed = ...;
std::vector<const char*> rowPtrs = session.loadRows(
    folly::Range<const RowId*>(needed.data(), needed.size()));
```

Single-row loading is an explicit slow path:

```cpp
const char* row = session.loadRow(rowId);
```

The session returns only `const char*`. After the current batch is no longer in
use, call `releaseLoadedChunks(targetBytes)` to release session-owned pins. The
blocks remain managed by BufferManager and may later be reclaimed or spilled.
Release is chunk-granular: a chunk's row block and heap blocks are unpinned
together.

Use `evictLoadedChunks(targetBytes)` when resident blocks should be reclaimed
immediately. Clean blocks can be discarded; dirty row blocks are written back
to their spill backing.

## Reordered Segments and Merge Read

Sort or HashAgg can materialize rows in sorted pointer order as a mergeable
segment:

```cpp
SegmentId orderedSegment = rows.finalizeReorderedSegment(
    folly::Range<char* const*>(orderedRows.data(), orderedRows.size()));
```

Read multiple ordered segments through `MergeReadSession`:

```cpp
auto merge = rows.beginMergeReadSegments(range);

std::vector<char*> batch;
while (merge.next(batch, maxRows)) {
  rows.extractColumnResident(batch.data(), batch.size(), column, output);
}
```

`beginMergeReadSegments()` accepts only ordered segments produced by
`finalizeReorderedSegment()`. Ordinary spill segments cannot be merged
directly.

Merge read consumes data by default and releases completed chunks when safe,
preventing consumed data from being spilled again under later pressure. Disable
release-after-read for repeatable access:

```cpp
auto merge = rows.beginMergeReadSegments(range, false);
```

## Releasing and Evicting Data

Release pins while keeping data available for later reads:

```cpp
session.releaseLoadedChunks(targetBytes);
```

Attempt immediate resident-memory reclamation:

```cpp
session.evictLoadedChunks(targetBytes);
```

Release data permanently:

```cpp
rows.releaseSegment(segment);
rows.releaseSegments(range);
```

## Integration Patterns

Sort and HashAgg:

1. Write with `appendRow()` and `store()`.
2. Retain resident row pointers and sort them with `compareRows()`.
3. Call `finalizeReorderedSegment()` after sorting.
4. Produce output from multiple ordered segments with
   `beginMergeReadSegments()`.

Hash Build:

1. Write by partition with `appendRow(partition)` and `store()`.
2. Call `spillActivePartitionSegment(partition)` as often as needed.
3. Read `segmentsForPartition(partition)` during probe or later processing.
4. Use `BulkReadSession::loadRows()` when everything fits; otherwise use
   `ReadOnlyWindowReadSession::listRowIds()` and `loadRows()`.
5. Release the partition's segments when processing completes.

## Usage Constraints

- Never use a row pointer after its data has spilled.
- Treat `RowId` as opaque and return it to `ReadOnlyWindowReadSession`.
- `compare()`, `compareRows()`, and `extractColumnResident()` require resident
  pointers.
- Use `RowWriteContext` only for per-column stores into its current row.
- Common fast paths cover fixed-width values, `VARCHAR`, and `VARBINARY`.
  Verify complex-type behavior explicitly instead of assuming a fast path.

## Development and Testing Rules

Preserve hot-path performance during refactoring. Do not introduce PImpl,
virtual dispatch, heap allocations, or locks into `appendRow()`, `store()`,
`appendBatch()`, resident comparison, or extraction merely to hide
implementation details. Public headers may retain necessary hot-path details;
new code should otherwise depend on the narrowest public types.

The public API does not expose fine-grained trace metrics. Use broad
`BufferManagerStats`, `BufferManagerTagStats`, and I/O scheduler statistics for
production diagnostics. Benchmarks may measure end-to-end phases outside the
container, but must not reintroduce per-row, per-block, or nanosecond counters
into container hot paths.

## Benchmark Data Profiles

RowContainer benchmarks under `bolt/exec/bm/benchmarks` use three profiles:

- `fixed`: `BIGINT`, `INTEGER`, and `DOUBLE` only.
- `variable_small`: one `VARCHAR` whose deterministic length ranges from 1 to
  64 bytes with an average near 32 bytes. Override the maximum with
  `--bm_row_container_variable_max_string_length=64`.
- `variable_large`: one fixed 1024-byte `VARCHAR` for large-value copy, spill,
  compression, and I/O pressure. Override it with
  `--bm_row_container_large_string_length=1024`.

Both string-length flags may be provided together, but each affects only its
matching profile. The runner enumerates every registered binary case, so a
default run includes all three profiles.

Tests are split by behavior:

- `BmRowContainerResidentTest.cpp`: resident writes, comparison, extraction,
  nullability, and layout.
- `BmRowContainerReadTest.cpp`: bulk/window reads, eviction, and `StringView`
  rebasing.
- `BmRowContainerBatchTest.cpp`: fixed, string, null, and cross-chunk batch
  append behavior.
- `BmMergeReadSessionTest.cpp`: reordered segments and merge reads.
- `BmSegmentCollectionTest.cpp`: internal segment, chunk, and block storage.
- `BmPartitionTest.cpp`: partition spilling and boundaries.

Add a unit test to the matching behavior file. Create a new test file and
update `tests/CMakeLists.txt` only for a genuinely separate behavior domain.
