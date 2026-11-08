# Cell shuffle

The Cell shuffle is a columnar Spark shuffle writer and reader for hash,
range, single and round-robin partitioning. It stages every value in a
64-byte cache line of its (partition, stream), encodes each full line as one
small integer block, and keeps a whole window of payloads resident in
fixed-size cells until it is sealed or spilled. This page gives the overview and the reasoning behind the
design. The wire format is defined, field by field, in
[CellFormat.h](CellFormat.h); this page does not repeat it.

## 1. What it is and when to use it

### How it differs from the V1/V2 writers

| | V1 / V2 | Cell |
| --- | --- | --- |
| Buffering | per-partition column buffers (V1 preallocated per partition, V2 from a shared buffer pool), evicted as payloads | one 64-byte staging line per (partition, stream), flushed into chained cells of 4 MiB chunks |
| Value encoding | raw column buffers, left to the codec | per-line Encoding Blocks (constant, bit-packed, frame-of-reference, plain) and per-partition string dictionaries |
| Nulls | validity buffers | one null body per payload: a 2-bit tag per column, plus run/bitmap null records for mixed columns only |
| Payload | one per buffer eviction | one per partition per window, however many batches it spans |
| Compression | per buffer | per run, in `COMBINED` or `SEPARATE` frames, skipping data that does not compress |
| Memory accounting | buffer sizes | exact chunk counts, through one choke point (`reserveChunk`) |

The reader protocol is not compatible with the other writers: a Cell writer
must be read by the Cell reader, and both sides must select it the same way.

### Where it is not a good fit

- **Ultra-narrow, highly repetitive rows.** The codec already shrinks such
  data very well in any writer, so Cell's encoding gains little, while its
  per-row split loop and per-payload framing still cost.
- **Many streams at very high partition counts.** The staging arena is
  `64 B x partitions x streams` (for example 30000 partitions and 100 streams
  take about 192 MB), and the cell size shrinks with the same product. Wide
  nested schemas multiply the streams; for them, turning
  `nativeNestedTypes` off packs the complex columns into one column.

### How to enable it

Writer side (`ShuffleWriterOptions`, [Options.h](../Options.h)):

- `forceShuffleWriterType = 4` (`ShuffleWriterType::Cell`), or
- `preferCellShuffle = true`. It takes priority over a forced V1 (`1`) or
  V2 (`2`), and over the adaptive choice (`0`) unless that picked the
  row-based writer; a forced RowBased (`3`) stays row-based.

The rule is the same under every partitioning. Single and round-robin
partitioning never pick the row-based writer and, where Cell is not chosen,
write V1 whatever type is forced, as before.

`writesCellShuffle()` in Options.h is the one rule the writer and both
readers follow. A job that sets `preferCellShuffle` writes Cell for its
single and round-robin exchanges too, so their readers must be
`SparkShuffleReaderNode` as well.

The writer must be created through `BoltShuffleWriter::createShuffleWriter()`;
`createForType(Cell)` fails on purpose rather than silently writing V1.

`ShuffleWriterOptions::cellOptions` (`CellShuffleOptions`):

| Option | Default | Meaning |
| --- | --- | --- |
| `cellMemorySpillBytes` | `0` (off) | Spill threshold on the chunk memory plus the spill memory. Not a hard cap: past it a chunk growth spills (local) or requests a window push (Celeborn). |
| `spillMemoryBytes` | `-1` | Local only: spill data - runs, sealed payloads, composite blocks - kept in memory until memory pressure, while the whole writer stays within half its memory limit, or 512 MiB with none (see [Spill memory](#spill-memory-local)). `-1` keeps what that half leaves; a positive value caps the spill data at that many bytes as well; `0` is off. |
| `partitionSealBytes` | `40 MiB` | At a batch boundary, seal the payload of any partition whose bytes since it opened exceed this; other partitions stay open. |
| `nullMemLimitBytes` | `16 MiB` | Seal the whole window once the moved null cells exceed this. A reclaim requests a seal above a quarter of it. |
| `enableStringDictionary` | `true` | Probe the first batch to enable dictionary form per string column. |
| `streamCompression` | `true` | Compress runs through the codec's stream form, from the cells through fixed buffers. Off, each frame is gathered into a spill-pool workspace and compressed at once. Codecs without a stream form are always one-shot. |
| `adaptiveCompression` | `true` | After a frame that did not shrink, store the next frames of its kind without a codec call and retry one in ten. |
| `nativeNestedTypes` | `true` | Shred ARRAY, MAP and ROW columns into a nested layout. Off, the type adapter packs them into one binary column. |
| `verifyDataFile` | `true` | Local only, a diagnostic: once the data file is closed, walk its block and run headers and fail the task on a mismatch (see [Limits and failure modes](#7-limits-and-failure-modes)). |

The codec, level and backend come from `PartitionWriterOptions`
(`compressionType`, `compressionLevel`, `codecBackend`), as for the other
writers.

Reader side (`ShuffleReaderOptions`): the Cell reader is used by
`SparkShuffleReaderNode` when the same rule picks Cell from the reader's
`forceShuffleWriterType` and `preferCellShuffle`. `BoltShuffleReader`
refuses these configurations with a check. The reader takes `compressionType`,
`batchSize` and `shuffleBatchByteSize` as usual.

Settings that must match on both sides, because the payloads do not record
them:

| Writer | Reader |
| --- | --- |
| `forceShuffleWriterType` / `preferCellShuffle` | `forceShuffleWriterType` / `preferCellShuffle` |
| `cellOptions.nativeNestedTypes` | `cellNativeNestedTypes` |
| `compressionType` | `compressionType` |

## 2. Data flow

```text
 input RowVector (pid column first; single/round-robin: computed)
   |
   v
 CompositeShuffleWriter --- CompositeRowVector ---> held rows ----------+
   | ordinary batch                                (CompositeRows      |
   v                                                blocks)             |
 CellShuffleWriter  (slice big batches, reserveSlice on Celeborn,       |
   |                 computePartitions, null classes, dictionary probe) |
   |-- nested layout -----> CellNestedShredder (one split per space)    |
   |-- complex, adapter --> CellShuffleTypeAdapter::pack                |
   v                                                                    |
 CellSplitter  (64 B staging line per (partition, stream);              |
   |            full line -> Encoding Block / raw bytes)                |
   v                                                                    |
 DataCells (chains)  <- ChunkAllocator (4 MiB chunks of cells)          |
 NullCells (null state, own chunk allocator)                            |
   |  run (local spill)      | seal (window or partition)               |
   v                         v                                          |
 LocalCellOutput (local)                   RssCellOutput (Celeborn)     |
   | RunCompressor -> spill file             | RunCompressor -> payload |
   | finalize: merge per partition           | writeBlocks per seal     |
   v                                         v                          |
 data file  <-- composite spill --  PartitionBlockOutput: LocalBlockOutput
                                    / RssBlockOutput (push channel)  <--+
   |
   v  bytes: [kind][ColumnarPayload] ... [kind][CompositeRows] ...
 CellShuffleReader  (stream iterator, kind byte, codec)
   |-- flat or adapter ---> CellPayloadDecoder -> adapter unpack
   |-- nested layout -----> CellNestedAssembler (a CellPayloadDecoder
   |                        per space) -> ARRAY / MAP / ROW vectors
   |-- CompositeRows -----> CompositeRowVector
   v
 RowVector batches of at most batchSize rows
```

Step by step:

1. **CompositeShuffleWriter** (base class) partitions every batch by its pid
   column - computed and put in front first under single or round-robin
   partitioning - and buffers `CompositeRowVector` rows (aggregation state)
   as `CompositeRows` blocks. Only one kind holds data at a time: a batch of the
   other kind first writes the current one out.
2. **CellShuffleWriter** builds the layout on the first batch, sizes the
   cells, runs the one-time dictionary probe, classifies nulls per batch
   (`NullClass`), and decides when to spill and seal. Batches above 200 MiB
   locally, or 8 MiB on Celeborn, are sliced.
3. **CellShuffleTypeAdapter** packs complex columns into one trailing
   VARBINARY column when the schema does not take a nested layout;
   **CellNestedShredder** splits a nested-layout batch, rows first, then each
   container's elements, space by space.
4. **CellSplitter** runs the per-type row loops: each value goes into the
   staging line of its (partition, stream), and a full line is flushed as an
   Encoding Block or as raw bytes into **DataCells** chains of
   **ChunkAllocator** cells. **NullCells** keeps the null records.
5. **LocalCellOutput** writes runs and sealed null bodies to a spill file -
   whose newest bytes stay in spill memory until memory runs short - and,
   in `finalize`, merges them partition by partition into the data file,
   writing the window still in memory straight from the cells.
   **RssCellOutput** pushes one complete payload per partition at each seal.
   Both write every run and null body through **RunCompressor**. The
   composite blocks go through a **PartitionBlockOutput**: locally a
   `LocalBlockOutput` spill that the Cell merge folds into the same data file
   (each partition's payloads, then its composite blocks); on Celeborn an
   `RssBlockOutput`, the push channel both kinds share.
6. **CellShuffleReader** reads each block's kind byte and decodes payloads
   through **CellPayloadDecoder**, or **CellNestedAssembler** for a nested
   layout; `CompositeRows` blocks come out as `CompositeRowVector`s, never
   mixed with payload rows.

## 3. Core concepts

| Term | Meaning |
| --- | --- |
| Partition | A reduce partition, the pid of a row. |
| Stream | One byte sequence of a wire column: a value stream, or a String's length and data streams, or a Timestamp's seconds and nanos streams (`CellStream`, `StreamKind`). |
| Cache line / Encoding Block | `kCacheLineBytes` (64) bytes of staged values of one (partition, stream). A full line becomes one Encoding Block: 8 Bigints, 16 Integers or 32 SmallInts. A format constant, not the host's line size. |
| Cell | A fixed-size piece of a chunk (256 B to 64 KiB, a power of two), holding stream bytes of one (partition, stream). |
| Chunk | A 4 MiB allocation from the memory pool, carved into cells. The only unit in which cell memory is taken from and returned to the pool. |
| Chain | The cells of one (partition, stream) in append order; all full except the last. |
| Window | Every partition's open payload. Per-partition counts run from when that partition's payload opened. |
| Payload | A `ColumnarPayload`: some rows of one partition, a header, a null body, encoding tags and runs. |
| Seal | Closing payloads. A whole-window seal closes every partition's; a per-partition seal closes only those over `partitionSealBytes`. |
| Run | A set of cells written out to free memory. It never closes a payload; a payload can hold several runs, oldest first. |
| Segment | One partition's slice of a spilled run in the spill file, copied verbatim by the merge. |
| Frame (COMBINED / SEPARATE) | The unit of compression of a run: one frame over all streams (`kCombined`, or `kCombinedUncompressed` when stored), or one per stream (`kSeparate`). |
| Block | A unit on the wire, opened by a `CellBlockKind` byte: a payload or a `CompositeRows` block. |
| Position space | In a nested layout, the positions a column has: space 0 is the payload's rows; each ARRAY or MAP owns a space holding its non-null entries' elements. |
| Null body / null record | Per payload, a `NullTag` per wire column (`kAllNull`, `kNoNull`, `kRawNull`); for a `kRawNull` column, 28-byte records, each a run of up to 32767 positions of one null-ness followed by a bitmap of up to 200 positions. |
| Dictionary | For a string column chosen by the probe, per partition, segments of up to 16 entries in at most 63 serialized bytes; matched rows store one index byte. |

## 4. Memory model

### Cell size from the memory limit

At the first batch the writer takes the tighter of the root pool's cap
(`maxCapacity()`) and Spark's execution memory per task, each where set. The
budget is a quarter of that, at most 1 GiB (1 GiB with neither). The cell
size is

```text
cellBytes = prevPowTwo(clamp(budget / 8 / partitions / streams, 256 B, 64 KiB))
```

The partly filled tail cell of every chain is slack, so a cell sized from a
budget far above the real one would waste much of the real memory. The
`CellShuffleWriter init` log line shows the inputs and the result.

### The choke points: `reserveChunk` and `reserveSlice`

- **`reserveChunk`** is the allocator's callback, called only when the
  freelist is empty and a new chunk is needed. Locally, if the chunk passes
  `cellMemorySpillBytes` or `maybeReserve` fails, it writes the spill memory
  out and tries once more; only if that fails too does it spill the largest
  partitions until half of the used cell bytes are covered, and the growth
  then reuses their recycled cells. On Celeborn it only sets a seal request
  and lets the growth proceed. While the output is reading cells, nothing
  may spill, so growth there proceeds unreserved.
- **`reserveSlice`** (Celeborn only) runs before each slice of a batch: it
  reserves the slice's estimated bytes plus one chunk, plus twice the largest
  partition's bytes for the push. If that fails, or passes
  `cellMemorySpillBytes`, the window is pushed first (`SealReason::kReserve`).

`DataCells::append` takes every cell it needs before it copies a byte, so a
spill fired from inside a growth sees only complete earlier appends.

### Local partial spills vs. Celeborn whole-window pushes

Locally, a spill is a run of some partitions: their cells are compressed into
the spill file in their final wire form and recycled, while their payloads
stay open. A task that never spills writes the data file straight from the
cells and creates no temporary file, and neither does one whose spills all
fit in spill memory.

Celeborn may reorder a mapper's pushes and interleave other mappers' pushes,
so every pushed block must be one complete payload. A window is therefore
only pushed whole, at a batch boundary; there are no runs. Batches are sliced
to 8 MiB so a boundary comes soon, and a reclaim arriving mid-batch or
mid-seal is deferred to the next boundary.

### Spill memory (local)

What a local writer spills is copied into the data file by the merge
anyway, so while memory is plentiful it waits in memory instead of on disk:
the spill file keeps its newest bytes in 4 MiB pages of the writer's pool,
accounted to it, and the merge copies them from there. A skewed task whose
hot partitions seal payload after payload then needs no disk write and read
for them. The pages of the Cell output's spill file and of the composite
blocks' share one cap, `spillMemoryBytes`: only one kind holds data at a
time, so the kind being written gets whatever the other does not hold.

Whatever the cap, a page is taken only while the writer's whole pool, the
page included, holds at most half the writer's memory limit, as the cell
size takes it, or 512 MiB with none. The whole pool is the cells' chunks,
the null state, both outputs' pages, the staging lines and scratch: a cap
on the pages alone would let cells and pages together reach the whole
limit and, under Spark, take the task's execution memory from its other
operators, which then spill. The check reads the pool's `usedBytes()`,
once per new page. It counts every allocation at its size, so a page is
one page more, and leaves out the reservation slack, which moves in steps
of up to 8 MiB of its own and goes back at the next `release()`. By
default (`-1`) that half is all that bounds the pages; a positive
`spillMemoryBytes` caps them at that many bytes as well. The
`CellShuffleWriter init` log line shows both caps:
`spillMemory=<cap> (writer total <= <half>)`.

What decides is memory pressure, not whether the bytes are a seal or a run.
Every pressure event writes the spill memory out first, in order, and then
does what it would have done, straight to disk:

- a reclaim, which counts the pages freed towards its request;
- a chunk reservation that fails or passes `cellMemorySpillBytes`, which
  then tries once more before it spills;
- the window seal a reclaim asks for;
- composite rows short of room for the next batch, and a reclaim while
  composite rows are held.

A page past the cap or past half the limit is no memory pressure: the
file writes its pages out and starts them afresh, or, if even fresh pages
do not fit, appends to disk and asks again a page's bytes later. A page
the pool cannot reserve counts as pressure for the rest of that run. A
file's bytes in memory are always its last: it writes them out before it
writes anything else to disk. They stay put while the file is being
appended to, or merged: a reclaim reached from inside a page's
reservation, or during the merge, leaves them. Mode switches and the final
merge are not pressure. `totalBytesEvicted` counts the bytes that reached
the disk; the `CellShuffleWriter pressure` log line ends with the spill
memory's peak and the bytes it flushed and appended to disk directly.

### Reclaim policy (local)

`reclaimOrdinary` steps from cheap to expensive and stops when the request
is met:

1. Return idle chunks (`shrink()`), the shredder's scratch and reservation
   slack. This is safe even while the output reads cells.
2. Write the spill memory out, unless the output is reading cells.
3. Skip the rest if that covered the request or the writer holds fewer
   than two chunks.
4. Spill the largest partitions until the request is covered, then shrink.
5. Compact: move live cells out of the emptiest chunks and shrink again.
   After a partial run, the remaining cells are spread over every chunk, so
   no chunk would be free without this step.
6. Only then, spill every partition (a full run).

It then releases the pool reservation and requests a window seal if the
moved null cells exceed a quarter of `nullMemLimitBytes`. On Celeborn, a
reclaim pushes the whole window instead of steps 2 to 6. All of this is
counted in the `CellShuffleWriter pressure` log line.

### Why null cells are not freed by spills

A payload's null body precedes its runs and covers all of its rows, so it
can only be written when the payload closes. Spills write data streams only.
The null state therefore stays resident for the life of the window, in a
separate chunk allocator owned by `NullCells`. It is bounded by
`nullMemLimitBytes` (a `kNulls` window seal) instead. Columns that are all
null or have no nulls in the window store nothing.

### Compression workspaces

`RunCompressor`'s one-shot workspaces come from the spill memory pool, not
the task pool. Spills and seals happen when the task pool is short, and must
not fail for want of the memory they are meant to free. Every spill and seal
gives the workspaces back. Stream compression needs only fixed buffers
(256 KiB staging, about 512 KiB output).

## 5. Encoding and compression

### Encoding Blocks

Encoded streams are SmallInt, Integer, Date and Bigint values (short
decimals map to Bigint), Timestamp seconds and nanos, and string lengths.
Each block starts with an `EncodingByte`: the kind in the low 2 bits, a
parameter in the high 6.

| Kind | Body |
| --- | --- |
| `kConstNarrow` | every value equal: the value in 1 to width bytes, sign-extended |
| `kBitPack` | values in 1 to 63 bits each, sign-extended |
| `kForBitPack` | a base, then `value - base` in 0 to 63 bits |
| `kPlain` | the values as they are |

The encoder picks the smallest body; ties resolve `kPlain`, `kConstNarrow`,
`kBitPack`, `kForBitPack` in that order, so the bytes are deterministic. A
block is at most `kMaxBlockBytes` (73). Boolean, TinyInt, Hugeint, Real and
Double are raw streams: a full line is copied as it is. String bytes go to
the data stream, staged through the line; values of 64 bytes or more bypass
it.

### String dictionaries

The first batch decides, per string column and for the writer's lifetime,
whether to use dictionary form; in a nested layout only the string columns
at the rows' positions are probed. A column qualifies when every distinct
value is at most 63 bytes, they serialize into at most 63 bytes together,
and the non-null rows are at least four times the distinct values. The
decision is final because cell bytes are already in wire form. With
`enableStringDictionary` off, or a first batch below 1024 rows, no column
uses it.

In dictionary form each partition keeps an open segment of up to 16 entries
staged in its data-stream line. Matched rows write one index byte. When a
segment is full, it closes; a successor opens only if the segment matched at
least `kDictSegmentContinueFactor` (4) rows per entry. Otherwise the
partition demotes to fallback rows for the rest of its payload. Each payload
starts afresh. The `dictionaryMatchedRows` and `dictionaryFallbackRows`
metrics report the totals.

### Null records

Each batch's nulls are classified per column first (`kNoNulls`,
`kSomeNulls`, `kAllNull`) with a word-level scan, because `mayHaveNulls()`
only says that a buffer exists. A column stays free while every row of the
window is null, or none is. Only a mixed column stores 32-byte cells (a
4-byte head plus one 28-byte null record) per partition. At a seal they
become the null body, compressed when it is at least `kMinCompressBytes` and
compresses to half its size or less.

### RunCompressor

| Run size | Layout |
| --- | --- |
| below `kMinCompressBytes` (256 B), or no codec | `kCombinedUncompressed` |
| below `kMinSeparateRunBytes` (64 KiB) | one `kCombined` frame, judged on its whole size |
| 64 KiB or more | `kSeparate`: one frame per stream, so each stream sets its own ratio |

A frame is stored instead of compressed when it is below 256 B or does not
shrink (compressed size plus 1/32 is not below the input). With the stream
form, a `kSeparate` frame is judged after its first 256 KiB of input, before
anything reaches the sink, so nothing written is taken back. With `adaptiveCompression`, a frame
that did not shrink makes the next nine frames of its kind (the same stream
for `kSeparate`, all `kCombined` frames, or null bodies) stored without a
codec call, and the tenth is tried again. This keeps random keys, hashes and
packed doubles from paying for compression they do not get. Totals are in
the `CellShuffleWriter compression` log line; per-stream detail is at
`VLOG(1)`.

## 6. Nested types

**Shredding.** With `nativeNestedTypes` on, a row type with ARRAY, MAP or
ROW columns takes a nested layout (`CellNestedLayout`) when every leaf is
supported and it shreds into at most 65535 columns. The type tree is
shredded in pre-order: a leaf is a column of its type; an ARRAY or MAP is an
Integer column of lengths followed by its element's columns, or its key's and
value's; a ROW is a SmallInt presence column (1 at each non-null struct)
followed by its fields.

**Position spaces.** Space 0 holds the rows. Each ARRAY or MAP owns a space
holding the elements of its non-null entries, numbered in pre-order. A ROW
owns none: its fields have the ROW's positions. `CellNestedShredder` splits
the rows' columns, then each space in element batches of up to 64K
positions, and gives the splitter each partition's position counts as the
null-position base of those columns.

**Struct-null folding.** A field is null wherever a ROW above it at the same
positions is null, and a container below a null struct holds no element. A
flat field of flat structs keeps its values under the combined nulls; a
field of a constant struct is a constant; any other field, including one
shorter than its structs, takes the structs' indices and nulls through a
dictionary.

**Assembly.** `CellNestedAssembler` reads each payload with one decoder and
opens a decoder per space at the space's positions, counted from the length
streams before any value is read. A batch takes the rows' next positions,
then as many positions of each container's space as the lengths call for,
and comes out as ARRAY, MAP and ROW vectors.

**Adapter fallback.** `CellShuffleTypeAdapter` packs a row's complex columns
into one trailing VARBINARY column of CompactRows, and the reader unpacks
them. Which path is used:

| Schema | `nativeNestedTypes` on | off |
| --- | --- | --- |
| no complex columns | flat layout | flat layout |
| complex, at most 65535 shredded columns | nested layout | adapter |
| complex, more than 65535 shredded columns | adapter | adapter |

Writer and reader decide alike from the same logical type, which is why
`nativeNestedTypes` and `cellNativeNestedTypes` must match.

## 7. Limits and failure modes

- **Single and round-robin partitioning:** their batches carry no pid
  column. `CompositeShuffleWriter::split` computes one per ordinary batch
  with the partitioner V1 uses - every row to partition 0, or round-robin
  from `startPartitionId`, its position carried across batches, so each
  row lands where V1 puts it - and puts it in front, sharing the batch's
  children; the rest of the writer, and the reader, are the hash path's.
  This costs one INTEGER column per batch. Composite rows are refused
  there (`composite rows only under hash or range partitioning`), as V1
  refuses them.
- **Supported types:** BOOLEAN, TINYINT, SMALLINT, INTEGER (and DATE),
  BIGINT (and short DECIMAL), HUGEINT (long DECIMAL), REAL, DOUBLE, VARCHAR,
  VARBINARY, TIMESTAMP and UNKNOWN (which has no wire column), plus ARRAY,
  MAP and ROW of these at any depth. Any other type fails the first batch
  with `Unsupported Cell logical schema`.
- **65535 columns:** a layout holds at most 65535 columns (`too many Cell
  columns`). A nested schema above it falls back to the adapter.
- **`kMaxPayloadRows`** (2^31 - 1): a window is sealed before a partition
  would pass it (`SealReason::kRows`).
- **`kMaxPositions`** (2^31 - 8): the most positions one space holds in a
  payload. A window is sealed before a batch's positions would pass it.
- **Runs per payload:** a reader accepts at most
  `CellDecodeLimits::maxRunCount` (2^24) runs in a payload. Locally, every
  run that writes any of a partition's cells, however few, adds one to its
  open payload; the merge fails the task rather than write a payload of
  more runs than a reader accepts.
- **Oversized nested batches:** a single batch whose rows together hold more
  than `kMaxPositions` elements in one ARRAY or MAP column cannot be carried
  by any payload, and fails with a user error (`BOLT_USER_FAIL`).
- **Sliced batches and short struct fields (known limitation):** batches
  above the slice bound (200 MiB locally, 8 MiB on Celeborn) are cut with
  `RowVector::slice`, which slices every child to the same range. A ROW
  field shorter than its struct is supported by the shredder, but not in a
  batch that gets sliced.
- **Malformed input** on the read side is reported as an error, never
  crashed on; `CellDecodeLimits` bounds the row and run counts. A malformed
  block's error ends with where the reader was: the stream and the block's
  index and offset in it, the stream's previous payload (its offsets and
  header fields), the block's header bytes read and up to 32 bytes after
  them, in hex.
- **Data file verification** (`verifyDataFile`, local only): after the
  merge closes the data file - its deferred run-header patches applied -
  `verifyCellDataFile()` walks every partition's blocks with `pread`,
  reading only the payload, run and CompositeRows headers. Every read must
  stay in its partition and the last block end at its end, the header
  fields must be ones the writer writes, the partition lengths must add up
  to the file's size, and each payload must lie where the merge wrote it,
  with the row and run counts it wrote. A mismatch fails the task at write
  time with `cell data file verification failed`, the partition, the block,
  the field with its expected and actual values and 32 bytes at it; Spark
  then retries the map. Each verification logs `data file verified: N
  partitions, M payloads, K runs in X ms`. Celeborn needs none: every push
  is a whole payload.

## 8. Performance notes

**`BOLT_CELL_KERNEL` alignment.** The hot kernels (split loops, block
encode/decode, null expansion, nested element splits) are declared with
`BOLT_CELL_KERNEL`, which aligns them to 64 bytes. Their loops then keep
their offsets within a cache line whatever code links before them, so a size
change in an unrelated function cannot move a tight loop across a line and
shift throughput by several percent. Mark new hot kernels the same way.

**Files that need an interleaved A/B for any change:**
`CellSplitter.cpp` and `.h`, `CellBuffer.h` (the append fast path,
`NullCells::setNull`), `CellEncoding.h`, `CellPayload.cpp` (decoder),
`CellNestedShredder.cpp`, `CellNestedAssembler.cpp`, `RunCompressor.cpp`,
and the batch loop in `CellShuffleWriter.cpp`. Unrelated-looking edits here
can move code and change timings; byte-level output can also change, so
compare `bytes_written` as well as time.

**Running `bolt_shuffle_benchmark`** (built under
`_build/Release/bolt/shuffle/sparksql/benchmarks/`):

```bash
LD_LIBRARY_PATH=<build dir> taskset -c 3 ./bolt_shuffle_benchmark \
  --bolt_benchmark_shuffle_tier=full \
  --bolt_benchmark_shuffle_writers=Cell \
  --bm_regex='^(A1|B1-P30000|F1)[.].*/Cell/write$' \
  --bm_max_secs=3 --bm_json_verbose=out.json
```

- Tiers: `smoke` (A1, A5, A7, A8 at 64 MiB), `standard` (A, B, C, the
  default) and `full` (A to F; F runs under a 256 MiB task memory limit, so
  every writer spills). Case ids are frozen.
- Codec: `--bolt_benchmark_shuffle_codec=lz4|zstd` (default `lz4`) and
  `--bolt_benchmark_shuffle_codec_level` (0 keeps the codec's default).
- Other flags, all prefixed `--bolt_benchmark_shuffle_`: `writers` (default
  `Cell,V1,V2`; with Cell, the `CellNoDict`, `CellUnsealedNulls` and
  `CellAdapter` variants join the cases they apply to), `input_mb`,
  `verify`, `list`, `dir`, `cell_stream_compression`,
  `cell_adaptive_compression` and `cell_verify_data_file`. See
  `--helpfull`.
- Each benchmark reports ns per row, plus user counters (split, shred,
  seal and spill, compress, write and evict times, bytes written and raw,
  pool peak, and so on) in the `--bm_json_verbose` output. `seal_spill_ns`
  is the writer's `sealSpillTime`: the seals, spills and pushes done inside
  its splits - a batch's rows' seal, the partial spills of chunk growths,
  the seals after a batch, the spills of a reclaim reached from inside a
  split - which `splitTime` leaves out, as V1's leaves out its eviction. It
  holds the compression and the spill writes or pushes that `compress_ns`
  and `evict_ns` count. A reclaim from outside is the operator's
  `externalReclaimTime`, and `stop()` is in neither.

**A/B method.** The A/B helper scripts are not in the repository. The method:
keep a copy of the baseline build's benchmark binary and its shared
libraries; run the baseline and the candidate alternately on one pinned
core, with the same flags, for three rounds or more; take each benchmark's
best time per side; compare ratios per case and their geomean. Check that
`bytes_written` and `bytes_raw` match when the change should not alter
output.

## 9. Code map

| File | Contents |
| --- | --- |
| `CellFormat.h` / `.cpp` | Wire format definition and constants; `CellLayout` (row type to wire columns and streams); `BOLT_CELL_KERNEL` |
| `CellEncoding.h` | Encoding Block encode/decode kernels (scalar and AVX2), `StreamCursor` |
| `CellBuffer.h` / `.cpp` | `ChunkAllocator`, `DataCells` (chains), `NullClass`, `NullCells` |
| `CellSplitter.h` / `.cpp` | Per-type split loops, staging lines, dictionary form, flush, run and seal orchestration |
| `CellShuffleWriter.h` / `.cpp` | The writer: layout choice, cell sizing, dictionary probe, memory pressure, seals |
| `CompositeShuffleWriter.h` / `.cpp` | Base writer: partitioning and `CompositeRowVector` handling |
| `CellShuffleTypeAdapter.h` / `.cpp` | Packs and unpacks complex columns as one binary column |
| `CellNestedLayout.h` / `.cpp` | Nested layout: nodes and position spaces of a shredded row type |
| `CellNestedShredder.h` / `.cpp` | Writer side of the nested layout |
| `CellNestedAssembler.h` / `.cpp` | Reader side of the nested layout |
| `CellOutput.h` / `.cpp` | `CellOutput`, `LocalCellOutput` (spill file and merge), `RssCellOutput` (pushes) |
| `RunCompressor.h` / `.cpp` | Run and null-body compression policy, stream and one-shot forms |
| `CellPayload.h` / `.cpp` | `CellPayloadDecoder`, null-record expansion, payload pieces the outputs assemble, thresholds |
| `CellShuffleReader.h` / `.cpp` | The read chain: streams, block kinds, composite rows, batches |
