# Cell shuffle

The Cell shuffle is a columnar Spark shuffle writer and reader for hash,
range, single and round-robin partitioning. It uses a 64-byte staging line
per (partition, stream), integer block encoding, and fixed-size cells for
encoded and raw bytes. Payloads remain open across batches until sealed;
their data cells can spill earlier. This page explains the design. The
wire format is defined, field by field, in
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
| Memory accounting | buffer sizes | exact chunk counts, checked before growth (`prepareChunkGrowth`) |

The reader protocol is not compatible with the other writers: a Cell writer
must be read by the Cell reader, and both sides must select it the same way.

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
| `cellMemorySpillRatio` | `0` (off) | Memory-pressure threshold for cell chunks plus spill memory, as a share of the writer's memory limit; `0` disables it. See [Memory model](#4-memory-model). |
| `spillMemoryBytes` | `-1` | Local only: the newest spill data - runs, sealed payloads, composite blocks - kept in memory, at most this many bytes (see [Spill memory](#spill-memory-local)). `-1` is no cap of its own: it grows until the first memory pressure; `0` is off. |
| `reclaimRunFloorBytes` | `64 MiB` | Local only: minimum resident cell data for a sized reclaim to spill a run; `0` disables the floor. See [Reclaim policy](#reclaim-policy-local). |
| `cellSpillBytesPerStream` | `512 KiB` | Local only: per-(partition, stream) cell-data budget for batch-boundary spills, subject to `cellSpillMinBytes`. See [Cell threshold](#cell-threshold-local). |
| `cellSpillMinBytes` | `64 MiB` | Local only: the least threshold `cellSpillBytesPerStream` sets, so that a shuffle of a few partitions does not spill its cells in runs of a few MiB. A negative value is an error. |
| `partitionSealBytes` | `40 MiB` | At a batch boundary, seal the payload of any partition whose bytes since it opened exceed this; other partitions stay open. |
| `nullMemLimitBytes` | `16 MiB` | Seal the whole window once the moved null cells exceed this. A reclaim requests a seal above a quarter of it. |
| `enableStringDictionary` | `true` | Probe the first batch to enable dictionary form per string column. |
| `stringDictionaryProbeRowCount` | `64` | Minimum first-batch row count for the dictionary probe. `0` enables every eligible string column without probing, including all-null columns; runtime fallback still applies. Ignored when `enableStringDictionary` is off. |
| `streamCompression` | `true` | Compress runs through the codec's stream form, from the cells through fixed buffers. Off, each frame is gathered into a spill-pool workspace and compressed at once. Codecs without a stream form are always one-shot. |
| `adaptiveCompression` | `true` | After a frame that did not shrink, store the next frames of its kind without a codec call and retry one in ten. |
| `nativeNestedTypes` | `true` | Shred ARRAY, MAP and ROW columns into a nested layout. Off, the type adapter packs them into one binary column. |

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

### Optional shuffle check

`shuffleCheckRatio` selects whole task attempts using
`taskAttemptId % 100 < 100 * shuffleCheckRatio`; 0 disables and 1 checks every
task. For nonnegative IDs, selection repeats every 100 tasks, with 1% granularity
(positive ratios below 1% select remainder 0). Negative task IDs are selected
whenever the ratio is positive, following signed remainder semantics.
Selected writers check the first `shuffleCheckMaxColumns` supported columns
across all partitions and batches. Coverage depends on the task ID remainders,
not row/byte counts. Unselected writers have no checker attached.

`CellShuffleCheck.h/.cpp` contain the check implementation. Integer, REAL and
DOUBLE non-null values use ordered CRC32C and exact counts per column/partition.
Input values supply expected CRCs; actual cache bytes are read at natural flush
points before encoding/reuse. State survives seals; comparison runs at finish.
NaNs and signed zeros use value-equality semantics. No extra flush is requested.
NULL positions, decimals, nested/packed schemas and encoded output are outside
coverage. CRC collisions can hide corruption; this is not a cryptographic check.

## 2. Data flow

### Input and staging

```text
RowVector
    |
CompositeShuffleWriter
    +-- CompositeRowVector --> held rows --> PartitionBlockOutput
    |
    +-- ordinary batch --> CellShuffleWriter
                              |
                    flat / nested / adapter
                              |
                         CellSplitter
                          /        \
                     DataCells    NullCells
                         |
                   ChunkAllocator
```

- `CompositeShuffleWriter` routes ordinary batches and buffers composite
  aggregation states. Switching kinds flushes the previous one, so they do
  not hold data together.
- `CellShuffleWriter` partitions rows, chooses the layout, probes
  dictionaries, classifies nulls, and controls spills and seals. Large
  batches are sliced to 200 MiB locally or 8 MiB on Celeborn.
- Native nested input passes through `CellNestedShredder`, one space at a
  time. The adapter instead packs complex columns into one VARBINARY column.
- `CellSplitter` stages values in 64-byte lines and appends encoded blocks
  or raw bytes to `DataCells`. `NullCells` keeps the payload's null state.

### Output

```text
                     DataCells + NullCells
                              |
                          CellOutput
                         /          \
                LocalCellOutput    RssCellOutput
                      |                  |
                spill runs and      complete payloads
                sealed nulls             |
                      |             Celeborn pushes
                partition merge
                      |
                  data file
```

Both backends use `RunCompressor`. Local finalization copies spilled runs
and writes the remaining resident cells directly. RSS sends complete
payloads at seals. Composite blocks use the same destination through
`PartitionBlockOutput`: the local merge includes its spill; RSS shares its
push channel.

### Read

```text
partition bytes --> CellShuffleReader --> block kind
                                            |
                  +-------------------------+---------------------+
                  |                         |                     |
           flat / adapter                 nested             CompositeRows
                  |                         |                     |
          CellPayloadDecoder       CellNestedAssembler     CompositeRowVector
                  |                 (decoder per space)
                  +-------------+-----------+
                                |
                         RowVector batches
```

The adapter unpacks its binary column after decoding. Nested assembly
reconstructs ARRAY, MAP and ROW vectors. Batches contain at most `batchSize`
rows, and composite rows are never mixed with payload rows.

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
| Block | A unit on the wire, opened by a `ShuffleBlockKind` byte: a `kCellPayload`, `kCompositeRows`, or `kRows` block. |
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

- `prepareChunkGrowth()` handles local growth pressure by flushing spill
  memory, then spilling cells as needed. Celeborn requests a seal instead.
- `prepareRemoteSlice()` reserves room for the next slice and its push;
  reservation failure or memory pressure pushes the current window first
  (`SealReason::kReserveFailed`).

`DataCells::append` allocates all needed cells before copying, so growth-triggered spills see only complete prior appends.

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

### Cell threshold (local)

Cells hold uncompressed data; spill memory holds compressed runs. At a
batch boundary, the writer spills the largest partitions down to half once
cell data exceeds both:

- `partitions x streams x cellSpillBytesPerStream` (512 KiB per stream);
- `cellSpillMinBytes` (64 MiB).

Other partitions remain resident. The floor avoids many tiny runs when
there are few streams or partitions. Memory pressure may trigger a spill
first. The init log reports `cellSpillThreshold`; the pressure log reports
`threshold runs`.

### Spill memory (local)

The newest spill bytes stay in blocks of the writer's pool until memory
runs short. Finalization copies them directly into the data file, avoiding
a disk round trip. Cell runs, sealed null bodies and composite blocks
share the `spillMemoryBytes` cap:

- `-1`: grow until the first memory pressure;
- `0`: write directly to disk;
- positive: bound the retained bytes.

Blocks are normally 4 MiB; a cap below 16 MiB uses smaller blocks so four
fit. A file's in-memory bytes are always its trailing bytes. Reusing a
block first writes the file's oldest block to disk. After the first
pressure event, spill memory reuses its blocks rather than growing.

Reclaims release enough oldest blocks to cover the request; failed chunk
reservations release 8 MiB before retrying. Composite growth and failed
spill-block reservations also end growth. Flushing completed blocks is
safe while output is writing; a file asks for another only after filling
its last block. Reclaim is disabled during the final merge.

`totalBytesEvicted` and `totalSpillFiles` count only bytes and runs that
reached disk. The pressure log reports spill-memory peak, flushed bytes and
blocks, and direct disk appends.

### Reclaim policy (local)

`reclaimOrdinary` steps from cheap to expensive and stops when the request
is met:

1. Return idle chunks (`shrink()`), the shredder's scratch and reservation
   slack. This is safe even while the output reads cells.
2. Write the oldest spill memory out, as much as the request still needs.
   This is safe even while the output writes (see above).
3. Skip the rest while the output reads cells, if that covered the
   request, or if the writer holds fewer than two chunks.
4. Skip the rest too, for a reclaim of a given size, while the cells hold
   less data than the run floor (below).
5. Spill the largest partitions until the request is covered, and at least
   two chunks (8 MiB), then shrink. Memory goes back to the pool only in
   whole chunks: the cells a smaller run frees would lie scattered over
   every chunk, too few for step 6 to empty one, and a reclaim of a few
   hundred KB would end in step 7.
6. Compact: move live cells out of the emptiest chunks and shrink again.
   After a partial run, the remaining cells are spread over every chunk, so
   no chunk would be free without this step.
7. Only then, spill every partition (a full run).

It then releases the pool reservation and requests a window seal if the
moved null cells exceed a quarter of `nullMemLimitBytes`. On Celeborn, a
reclaim pushes the whole window instead of steps 2 to 7. All of this is
counted in the `CellShuffleWriter pressure` log line.

**The run floor.** Reclaiming a small amount after every batch can create
many tiny runs and repeatedly allocate mostly empty tail cells. A sized
reclaim therefore stops after freeing idle chunks and spill memory while
cell data is below `reclaimRunFloorBytes` (64 MiB by default, capped at an
eighth of the memory limit). Reclaims of everything and chunk-growth
pressure bypass this floor; `0` disables it. The init log reports
`reclaimRunFloor`, and the pressure log counts `below floor` reclaims.

### Why null cells are not freed by spills

The null body describes the complete payload and precedes its runs, so it
is written only when the payload closes. A run spill writes data streams
and keeps the null state for subsequent batches.

**Uniform nullness is cheap.** A column that stays all-null or all-non-null
keeps only a small classification state in memory and a 2-bit tag per
payload. It needs no per-partition null cells, bitmap or null records.
All-null columns also append no value bytes. Only mixed columns allocate
null cells; long runs are encoded compactly.

Mixed-column null storage has its own allocator. `nullMemLimitBytes`
triggers a window seal to release moved null cells, independently of data
spills.

### Compression workspaces

`RunCompressor`'s one-shot workspaces come from the spill memory pool, not
the task pool. Spills and seals happen when the task pool is short, and must
not fail for want of the memory they are meant to free. Every spill and seal
gives the workspaces back. Stream compression needs only fixed buffers
(256 KiB staging, about 512 KiB output).

## 5. Encoding and compression

### Encoding Blocks

Integer streams encode one staging line at a time: up to 32 SmallInts,
16 Integers or 8 Bigints. Date, short decimal, Timestamp components and
string lengths use these same encoders.

| Kind | Core idea |
| --- | --- |
| `kConstNarrow` | Store one signed value for a constant block. |
| `kBitPack` | Store each value at a reduced signed width. |
| `kForBitPack` | Store a base, then unsigned differences from it. |
| `kPlain` | Copy values at their original width. |

- Test constant blocks first; use the compact form when it saves bytes.
- Choose the remaining forms from the block's minimum and maximum.
  Small widths pack exactly; wider widths round to bytes so repeated
  values remain useful to the outer codec.
- Wide Bigints favor Plain unless FOR saves enough. This preserves
  repeated-byte matches in the outer codec.
- `kMaxBlockBytes` (73) is conservative writable buffer capacity, including
  the packer's final whole-word store; it is not the emitted block length.
- Boolean, TinyInt, Hugeint, Real and Double use raw streams. String data
  uses raw bytes, with full lines and long values copied in bulk.

See `CellEncoding.h` for selection thresholds and `CellFormat.h` for the
wire layout.

### String dictionaries

- **Selection:** the first batch chooses a column's wire form for the
  writer's lifetime. In a nested layout, only strings at the rows'
  positions are eligible. The probe requires distinct values to fit a
  63-byte serialized dictionary and at least four non-null rows per
  distinct value.
- **Controls:** `enableStringDictionary=false` disables dictionaries.
  `stringDictionaryProbeRowCount` defaults to 64; smaller first batches
  skip the probe. Setting it to `0` enables every eligible string column,
  including all-null columns, while retaining runtime fallback.
- **Writing:** each partition's open segment holds up to 16 entries in its
  data staging line. A matched row writes one index byte.
- **Fallback:** when a segment closes, a successor opens only if it matched
  at least four rows per entry. Otherwise that partition uses fallback
  rows for the rest of its payload. Every new payload starts afresh.
- **Observability:** the probe log records selected or skipped columns;
  `dictionaryMatchedRows` and `dictionaryFallbackRows` count written rows.

### Null records

- Classify each batch as `kNoNulls`, `kSomeNulls` or `kAllNull` with a
  word-level scan; `mayHaveNulls()` alone only reports a null buffer.
- Uniform columns need no null records. Mixed columns use 32-byte cells:
  a 4-byte head and a 28-byte run/bitmap record per partition.
- At seal, records form the null body. Bodies of at least
  `kMinCompressBytes` are compressed when they shrink to half or less.

### RunCompressor

| Run size | Layout |
| --- | --- |
| below `kMinCompressBytes` (256 B), or no codec | `kCombinedUncompressed` |
| below `kMinSeparateRunBytes` (64 KiB) | one `kCombined` frame, judged on its whole size |
| 64 KiB or more | `kSeparate`: one frame per stream, so each stream sets its own ratio |

- Store frames below 256 B or without enough compression gain (compressed
  size plus 1/32 must be below input size).
- Stream compression judges a Separate frame after its first 256 KiB,
  before emitting bytes, so it never needs to retract output.
- Adaptive compression skips nine attempts after a frame does not shrink,
  then retries. History is tracked per stream, combined frames, or null
  bodies as applicable.
- The `CellShuffleWriter compression` log reports totals; `VLOG(1)` adds
  per-stream detail.

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
null-position base of those columns. The shredder owns these counts and the
wire-column mapping. At seal and finish, the writer passes a read-only view
of the per-column counts to the splitter for that output call's null bodies;
the splitter keeps no space metadata or borrowed count mapping between calls.

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

- **Partitioning:** hash and range batches carry a pid column. Single and
  round-robin batches keep their original schema, with no synthetic pid
  column. The writer uses the shared partitioner: Single sends every row to
  partition 0; round-robin continues from `startPartitionId` across batches.
  Composite rows require hash or range partitioning.
- **Supported types:** BOOLEAN, TINYINT, SMALLINT, INTEGER (and DATE),
  BIGINT (and short DECIMAL), HUGEINT (long DECIMAL), REAL, DOUBLE, VARCHAR,
  VARBINARY, TIMESTAMP and UNKNOWN (which has no wire column), plus ARRAY,
  MAP and ROW of these at any depth. Any other type fails the first batch
  with `Unsupported Cell logical schema`.
- **65535 columns:** a layout holds at most 65535 columns (`too many Cell
  columns`). A nested schema above it falls back to the adapter.
- **`kMaxPayloadRows`** (2^31 - 1): a window is sealed before a partition
  would pass it (`SealReason::kPositionLimit`).
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
  crashed on; `CellDecodeLimits` bounds the row and run counts. Errors name
  the defect, such as a truncated header, an invalid block kind or an
  unsupported layout.

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
  `verify`, `list`, `dir`, `cell_stream_compression` and
  `cell_adaptive_compression`. See `--helpfull`.
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
| `../base/ShuffleBlockFormat.h` / `.cpp` | Shared block kind, compression layout, packed `RowBlockHeader`, framed row block writer, and buffer helpers |
| `../base/ShuffleByteSource.h` | Shared pull-based byte source (`ShuffleByteSource`, `MemoryByteSource`) and `ShuffleDecompressor` |
| `../base/ShuffleBlockReader.h` / `.cpp` | Shared `BlockShuffleReader` interface, `StreamByteSource`, `CodecDecompressor`, `ShuffleStreamCursor`, and `RowBlockReader` |
| `../base/CompositeShuffleReader.h` / `.cpp` | Base reader: stream traversal, decompression/read metrics, and `CompositeRows` deserialization |
| `../base/PartitionBlockOutput.h` / `.cpp` | `SpillMemory`, `ShuffleSpillFile`, `DataFileWriter`, `PartialSpiller`, `LocalBlockOutput`, `RssBlockOutput` |
| `../base/CompositeShuffleWriter.h` / `.cpp` | Base writer: partitioning and `CompositeRowVector` handling |
| `CellFormat.h` / `.cpp` | Wire format definition and constants; `CellLayout` (row type to wire columns and streams); `BOLT_CELL_KERNEL` |
| `CellEncoding.h` | Encoding Block encode/decode kernels (scalar and AVX2), `StreamCursor` |
| `CellBuffer.h` / `.cpp` | `ChunkAllocator`, `DataCells` (chains), `NullClass`, `NullCells` |
| `CellSplitter.h` / `.cpp` | Per-type split loops, staging lines, dictionary form, flush, run and seal orchestration |
| `CellShuffleWriter.h` / `.cpp` | The writer: layout choice, cell sizing, dictionary probe, memory pressure, seals |
| `CellShuffleTypeAdapter.h` / `.cpp` | Packs and unpacks complex columns as one binary column |
| `CellNestedLayout.h` / `.cpp` | Nested layout: nodes and position spaces of a shredded row type |
| `CellNestedShredder.h` / `.cpp` | Writer side of the nested layout |
| `CellNestedAssembler.h` / `.cpp` | Reader side of the nested layout |
| `CellOutput.h` / `.cpp` | `CellOutput`, `LocalCellOutput` (spill file and merge), `RssCellOutput` (pushes) |
| `RunCompressor.h` / `.cpp` | Run and null-body compression policy, stream and one-shot forms |
| `CellPayload.h` / `.cpp` | `CellPayloadDecoder`, null-record expansion, payload pieces the outputs assemble, thresholds |
| `CellShuffleReader.h` / `.cpp` | The read chain: streams, block kinds, composite rows, batches |
