# Radix Sort Direct Storage and Spill-Merge Output Design

## 1. Problem and scope

This document describes the implemented radix-sort input and spill-merge output
paths. The design has two related goals:

- encode production input keys directly into `RadixSortRunStorage`; and
- materialize spill-merge rows before advancing a reader across the storage
  boundary that owns those rows.

The output change fixes a memory-lifetime problem. A spill reader restores key
and payload pointers into its current serialized block. If a merge collects those
pointers across several blocks before consuming them, the reader must preserve
all crossed blocks. The old retaining protocol did that and allowed memory to
grow with the number of blocks and physical files crossed by one output call.
That protocol and its retained-buffer and retained-file containers have been
deleted.

The implemented replacement divides a requested output batch into synchronous
segments. A segment ends immediately before a selected stream would replace its
current spill block. The consumer decodes, gathers, or serializes that segment
while its source pointers are valid, then permits the stream to advance.

The production output path writes every segment directly into one final
`RowVector` at an offset. It does not create a temporary decoded-key or payload
`RowVector`, and it does not use `Vector::copy` to assemble the result. Strings
and complex values perform only the deep materialization required to make the
final output own its data.

The production input path also avoids a batch-sized encoded-key intermediate.
Both inline and variable keys are encoded into their final record and heap
storage. `EncodedKeyBatch` remains available only to the low-level codec/storage
interfaces and tests; `RadixSortRun::append()` does not use it.

The scope includes all production key layouts and merger shapes, key-only and
payload-bearing output, output-stage spill, every supported value type,
compressed and split-file input, reuse, metrics, and failure cleanup.

The existing non-merge `RadixSortRun::getOutput()` path remains a standalone
batch path. Its public behavior is unchanged. The direct-to-storage input change
is shared by in-memory and spill-producing runs because both enter through
`RadixSortRun::append()`.

### 1.2 Non-goals

This change does not add a second production strategy, transactional retry after
an output failure, a new vector API, a new string arena, repeated-winner merge
draining, or support for additional key types. It also does not promise identical
throughput for every spill-block distribution: unusually short boundary segments
can reduce SIMD utilization and increase string-space requests. Those effects are
measured rather than hidden behind extra machinery.

The design deliberately keeps the internal contract simple. Key and payload
offset writers are private, trusted helpers and use focused debug checks. They do
not repeat a batch-wide schema validation pass. Bytes read from spill files are a
different trust boundary and retain Release-mode fail-fast size, overflow, codec,
section, and pointer-restoration checks.

## 2. Ownership and lifecycle

### 2.1 Pointer lifetime

`RadixSortSpillReader::nextBatch()` restores all row pointers into one current
serialized block. Those pointers are valid only until that reader loads another
block, closes, or is destroyed. The merger therefore follows one rule:

> Every pointer selected from a spill block is consumed before that stream
> replaces or releases the block.

The rule applies to both consumers of `RadixSortMerger`:

- `RadixSortBuffer::getMergedOutput()` decodes and gathers rows into the final
  output vector; and
- `RadixSortBuffer::spillRemainingOutput()` serializes rows into the output-stage
  spill writer.

Both consumers run inside the `collectRows()` callback. The callback is
synchronous and must not retain the key or payload pointers after returning.

### 2.2 Stream boundary protocol

`RadixSortMergeStream` exposes two operations:

```cpp
virtual bool tryAdvance() = 0;
virtual void advanceAfterFlush();
```

`tryAdvance()` has two outcomes:

1. It returns `true` after advancing to the next row when the next row uses the
   same backing storage.
2. It returns `false` without changing the current row when advancing would
   invalidate the current key or payload pointer.

On the second outcome, `RadixSortMerger::collectRows()` invokes its callback for
the complete pending segment. Only after the callback succeeds does it call
`advanceAfterFlush()` to load the next block or physical file.

The in-memory merge stream always advances in `tryAdvance()` because its
`RadixSortRunStorage` outlives the merger. Crossing an allocation range inside
that storage is not a spill-reader lifetime boundary. Spill-file streams return
`false` on the final row of their current restored block.

### 2.3 Merger collection

`RadixSortMerger::collectRows()` preserves the specialized single-stream,
two-way, and loser-tree implementations, plus compile-time `HasPayload`
dispatch. Each implementation uses two counters:

- `totalCollected` is monotonic for the requested batch; and
- `segmentSize` indexes reusable key and payload pointer arrays from zero.

The common operation is equivalent to:

```cpp
while (totalCollected < count) {
  auto* stream = selectWinner();
  savePointers(stream, segmentSize++);
  ++totalCollected;

  if (!stream->tryAdvance()) {
    flushRows(segmentSize);
    segmentSize = 0;
    stream->advanceAfterFlush();
  }
}
if (segmentSize != 0) {
  flushRows(segmentSize);
}
```

Rows selected from different streams can share a segment. A segment is ended by
a backing-storage boundary, not by a stream switch. After every callback the
pointer arrays are reused from index zero.

The two-way path refreshes only the selected side after advancing it. The
loser-tree path updates the winner on the following tree propagation exactly
once. A request for zero rows returns zero without invoking the callback. A
non-empty request against an exhausted merger is an invalid internal state and
fails instead of returning a silent short result.

### 2.4 Reader and file ownership

Each active disk stream owns its current serialized buffer, compressed scratch,
and blocking or io_uring read state. The merger owns one shared
`RadixSortSpillReadBufferCache`; it is not one cache per reader. An exhausted
stream may return an eligible serialized buffer to that shared cache so another
stream or physical-file child can reuse it.

If the current serialized buffer can hold the next block, the reader reuses it.
If it is too small, the reader resets the old buffer before allocating the new
one, avoiding a growth-time peak that keeps both full allocations live. A block
above the reusable-size limit is released rather than cached. At EOF, the reader
recycles or releases its final serialized buffer, releases compressed scratch,
and closes its input.

A concat stream represents one logical run split across physical files. It
accumulates read, decompression, and I/O metrics from an exhausted child, destroys
that child immediately, and then opens the next file. It does not preserve old
children merely to keep pointers valid.

File ownership is guarded with RAII:

- a constructed spill-file stream owns its path through `SpillFileGuard`;
- an exhausted stream closes and removes its owned file; a failed operation
  keeps source ownership until the stream or merger is destroyed;
- a concat stream removes unopened files during destruction; and
- output-stage spill guards uncommitted files until ownership is transferred.

Destructors perform no-throw cleanup. Operational failures are not swallowed:
constructing or loading a stream catches only to clean up and then rethrows.

### 2.5 Failure semantics

A callback failure propagates directly and terminates the operator operation.
There is no rollback or retry protocol. If the callback throws at a boundary,
the boundary row has not advanced and its backing block is still alive for safe
stack unwinding. Ordinary rows earlier in the segment may already have advanced;
the API does not claim that the whole segment is transactionally unconsumed.

If the callback succeeds but loading the next block or file fails, the stream
cleans up its current ownership and rethrows. A failed merge batch does not call
its finish step and does not commit output-row or output-time metrics. The bound
payload plan is not an exception-recovery mechanism; terminal operator teardown
destroys it without dereferencing cached output pointers.

### 2.6 Memory bound

The reader-side live-memory bound depends on active logical disk runs, not on
the number of blocks or physical files already crossed. It consists primarily
of:

- one current serialized block per active disk stream;
- current compressed and read buffers per active disk stream; and
- one merger-owned reusable serialized-buffer cache.

The output vector, merge pointer arrays, decode scratch, and output-stage writer
staging are additional bounded consumers. A single oversized row or block can
still be large, so this is an asymptotic lifetime guarantee rather than a fixed
byte ceiling.

## 3. Final APIs and call flow

### 3.1 Buffer output

The merged-output entry point is:

```cpp
RowVectorPtr RadixSortBuffer::getMergedOutput(vector_size_t count);
```

`RadixSortBuffer::getOutput()` first computes `count`, then calls
`ensureOutputFits(count)`, and only then inspects `merger_`. This order is
required because reservation arbitration can reclaim the operator, spill its
remaining output, and replace the merger. The branch must observe that final
state.

The merge branch prepares or reuses `output_`, resizes reused children once,
calls `prepareMergeOutput()`, and ensures reusable pointer-array capacity. It
then calls `collectRows()`; each callback calls `writeMergeOutput()` at the
current offset. After collected rows and accumulated offsets both equal `count`,
it calls `finishMergeOutput()` once, commits buffer output time, and returns the
final vector.

`prepareOutputVector(count, true)` performs the vector reuse boundary.
`BaseVector::prepareForReuse()` can reset reused row children to size zero, so
the merge branch explicitly resizes each reused child once. Segment writers do
not resize, replace, prepare, or shrink output children.

### 3.2 Merger API

The final merger API is:

```cpp
using FlushRows = folly::FunctionRef<void(vector_size_t)>;

vector_size_t collectRows(
    vector_size_t count,
    const char** keys,
    char** payloads,
    FlushRows flushRows);
```

`FlushRows` is non-owning and synchronous. The spans represented by the pointer
arrays end at `segmentSize`; elements beyond that prefix are irrelevant. Key-only
collection leaves the payload array absent.

### 3.3 Run materialization lifecycle

`RadixSortRun` owns the private batch lifecycle:

```cpp
void prepareMergeOutput(RowVector& output);

void writeMergeOutput(
    std::span<const char* const> keys,
    std::span<char* const> payloads,
    vector_size_t outputOffset,
    RowVector& output);

void finishMergeOutput(RowVector& output, vector_size_t writtenRows);
```

`prepareMergeOutput()` lazily builds stable decode and gather plans and binds the
payload plan to the current final output. `writeMergeOutput()` processes one
whole segment. `finishMergeOutput()` finalizes child metadata and commits the
run's output-row metric once after all segments succeed.

These methods do not call `RadixSortOutputProjection::reconstruct()`. Projection
already supplies the direct key channels, decoded-key mask, and payload channels
needed to target final children. Avoiding reconstruction is what removes the
temporary vectors and their copy into `output_`.

### 3.4 Key codec API

The merge-only key helpers remain private to `RadixSortKeyCodec`, with
`RadixSortRun` as a friend: `singleFixedWordBytes()`, `decodeSingleFixedAt()`,
`decodeSuffixAt()`, `decodePrefixAt()`, and `finishDecode()`. The decode methods
take the final destination and, where applicable, `outputOffset`, projection
masks/channels, and reusable scratch.

`singleFixedWordBytes()` selects the direct physical decoder once per run plan.
The two layered methods write suffix and prefix columns into mapped final
children. The projection assigns each decoded destination to one logical key;
`finishDecode()` resets data-dependent flags once for each selected key child.

There is no separate batch-heavy schema validator. The caller, projection,
layout, and output are private components built together. Their shape, channel,
offset, and binding invariants use `BOLT_DCHECK` at focused boundaries. Checked
arithmetic and data-dependent parser failures remain Release checks.

### 3.5 Payload API

The merge payload path uses the private `PayloadRowReader::Plan` lifecycle:
`makePlan()` once, `bind()` once per output batch, `gather()` for each segment,
and `finish()` once after success.

`makePlan()` classifies columns and stores stable layout, channel, type-kind, and
nullability metadata. `bind()` attaches a plan to one output batch and caches its
destination vectors and raw value/null pointers. Repeated `gather()` calls write
segments. `finish()` unbinds the output and resets each payload child's
data-dependent flags.

The public non-spill `PayloadRowReader::gather(layout, rows, pool, result, ...)`
does not construct or use this PIMPL plan. It follows a separate lightweight
standalone path with stack-inline `folly::small_vector` state for inferred null
hints, fixed64 candidates, string columns, and string groups. The persistent
plan exists only where merge output benefits from reuse across segments and
batches.

### 3.6 Output-stage spill

`spillRemainingOutput()` is the other `collectRows()` consumer. When a merger is
present, its callback:

1. copies the segment's fixed key records into bounded staging;
2. computes section sizes;
3. repeatedly calls `writePresizedKeyRange()` until the complete segment is
   consumed; and
4. returns only after the writer has deep-copied all key and payload sections.

Only then may `advanceAfterFlush()` invalidate the source block. The no-merger
branch keeps using `collectRemainingRows()`. Successful output-stage spill
accumulates reader statistics, clears the old run and merger, and replaces their
remaining rows with one logical spill run. Uncommitted output files are removed
by a scope guard on failure.

## 4. Direct storage and output materialization

### 4.1 Production input is direct-to-storage

`RadixSortRun::append()` projects key columns, writes payload rows when needed,
and then takes one of two production key paths:

- a non-variable layout calls `RadixSortKeyCodec::encodeAndAppendInline()`;
- a variable layout calls `RadixSortKeyCodec::encodeAndAppendVariable()`.

Neither path creates a complete encoded-key batch and then copies it into run
storage.

For inline layouts, `encodeAndAppendInline()` first tries the specialized
single-flat-fixed path. The general path calls
`RadixSortRunStorage::appendKeyBlocks()` and encodes each block directly into its
final fixed-stride records. Per-row cursors are lightweight reusable metadata;
encoded key bytes are not staged elsewhere. After encoding, the implementation
normalizes inline words for comparison order and stores compact payload pointers
where the physical layout has payload.

For variable layouts, encoding has a sizing pass and a writing pass:

1. `keySizeScratch_` records the encoded suffix size for each row.
2. `appendVariableKeyBatch()` allocates final records and their overflow ranges.
3. Columns `[0, firstSuffixColumn_)` encode directly into fixed positions in the
   record.
4. Columns `[firstSuffixColumn_, keyColumnCount)` encode directly into the
   allocated heap through an indirect output cursor.
5. The leading suffix bytes needed by the radix prefix are mirrored from the
   heap into the record's remaining inline region.
6. Encoded size, heap pointer, and optional payload pointer are stored in the
   final record.

The sizing array is scratch, not an encoded-key copy. `EncodedKeyBatch` remains
for low-level `RadixSortKeyCodec::encode()`, the compatible storage append API,
and tests/oracles. It is not a member of `RadixSortRun` and is not used by the
normal production append path.

### 4.2 Final output and offsets

All merge segments target the one prepared `output_`. The indexing rule is:

```text
source key or payload row = row
segment scratch row       = row
destination row           = outputOffset + row
```

Pointer views, decode cursors, validity selections, and other scratch remain
zero-based for the current segment. Only final vector writes apply
`outputOffset`. This keeps existing column-oriented kernels batch-shaped while
preserving rows written by earlier segments.

The merge path never creates an intermediate `RowVector`, never invokes
`Vector::copy`, and never replaces a final child during a segment. Reused output
children are prepared once before collection; nested ARRAY, MAP, and ROW storage
then grows across all segments in the batch.

### 4.3 Exactly-once logical key decode

For a variable physical key, let `firstSuffix` be `firstSuffixColumn_`. The
logical key columns are partitioned exactly once:

```text
prefix columns = [0, firstSuffix)
suffix columns = [firstSuffix, end)
```

`decodeSuffixAt()` decodes the suffix. `decodePrefixAt()` overlays the fixed
prefix from physical record slots. No logical column belongs to both ranges.

The suffix bytes may have two physical representations: the heap is canonical,
and the record contains an inline mirror of the leading suffix bytes used by the
radix prefix. A finalized run chooses exactly one suffix source for decode:

- use the inline mirror when every encoded key in the run fits the available
  radix prefix; or
- use the heap otherwise.

It never decodes both copies. The mirror is a comparison/decode source choice,
not a second logical key. The run-wide choice is fixed during finalization from
the maximum encoded key size observed across all append batches.

Projection also prevents duplicate output materialization. A key appearing once
as a direct output channel is decoded there. A repeated direct-key occurrence is
kept in payload so that the same output child is not produced independently by
both key and payload paths and each selected key child is finalized once.

### 4.4 Key decode paths

The cached `MergeDecodePlan` contains:

- optional `singleFixedWordBytes`; and
- mask-aware scratch words per row for the layered decoder.

For one fixed key in a non-variable layout, `decodeSingleFixedAt()` writes the
physical values directly into the mapped final child. It preserves the existing
type-specific, byte-order, null, Boolean, and Timestamp behavior.

Other layouts create segment-local `EncodedKeyView`s. Fixed layouts use reusable
inline view scratch. Variable layouts build views over the chosen inline or heap
suffix source. `decodeSuffixAt()` retains the existing compile-time
`FirstColumn`, `MayHaveNulls`, and `Descending` dispatch for fixed words, Boolean,
int128, Timestamp, strings, and complex values.

Masked columns still advance every encoded cursor. Fixed columns use the fixed
skip kernel. String and complex columns use the checked recursive grammar skip,
so no throwaway output vector is needed merely to reach a later selected key.
ROW, ARRAY, MAP, and UNKNOWN follow the same encoded grammar as their decoders.

Key strings use the existing scan-then-write path, including delimiter scanning
and the no-escape copy shortcut. Selected complex keys append nested storage
directly into the final child. Top-level rows use `outputOffset + row`; nested
element, key, and value positions append from their current nested vector sizes.

### 4.5 Payload gather paths

The persistent payload plan classifies columns once into fixed, string, and
complex states. It also precomputes nullable and non-null string groups and, for
eligible layouts, grouped fixed64 states. Binding to an output batch caches:

- destination vectors;
- mutable value pointers;
- mutable null pointers; and
- the process AVX2 capability.

These raw pointers remain valid because segment writes do not resize or replace
top-level children. String-buffer growth does not replace the `StringView` values
or null bitmap. Complex gather caches only the top-level vector and null pointer;
nested storage is owned and grown by that vector.

Fixed scalars write directly into pre-biased value ranges. Boolean values and
nulls use separate bitmaps and apply the destination offset to both. Timestamp
loads its encoded words and constructs the final value in place. UNKNOWN writes
nulls without a value allocation.

Grouped fixed64 gather is enabled only for a fixed-only payload layout. It is not
claimed for a mixed layout containing string or complex fields. At execution it
also requires AVX2, at least one `xsimd::batch<int64_t>` of rows, and more than
one eligible BIGINT, DOUBLE, or short-decimal column. A mixed layout gathers its
fixed columns through the ordinary per-column kernels.

String gather performs two necessary passes per segment: first it records views
and totals non-inline bytes, then it obtains destination-owned string space and
copies those bytes once. Inline strings require no external byte copy. Multi-
column strings retain the 32-row tiled traversal. Every non-inline `StringView`
is rewritten to the destination-owned address before the source block advances.

Complex payload uses `ContainerRowSerde::deserialize()` directly at the target
row with reuse enabled. This is the one necessary deep materialization from the
serialized payload representation into final vector storage. Nested vectors are
not reset between segments.

### 4.6 Bitmaps and metadata

Segment code never clears a zero-based prefix of a destination bitmap. Null and
Boolean writes use the actual destination range. Nullable fixed payload paths
handle a leading partial word, full aligned words, and a trailing partial word
so bits outside the segment remain unchanged. Null-free paths clear only the
segment range when an old null buffer exists. Nullable strings initialize only
their target range to not-null and then set discovered nulls.

Raw writes make cached null counts, ASCII state, and similar data-dependent
metadata stale. The code does not reset those flags after every segment. On a
successful complete batch, `RadixSortKeyCodec::finishDecode()` and
`PayloadRowReader::finish()` reset participating child metadata once.

### 4.7 Scratch allocation

Merge decode view, inline, and cursor scratch are reusable, segment-local
buffers. When the pool changes or capacity is insufficient, growth resets the
old buffer and allocates a new one. It does not copy obsolete scratch contents
or keep an old raw pointer across growth. When capacity is sufficient, only the
logical size changes.

Decode-scratch word and byte sizing, and spill-section sizing, use checked
arithmetic. Raw pointers are acquired only after allocation or resizing is
complete. The input key sizing scratch and codec cursor metadata similarly do
not own encoded key bytes.

### 4.8 Trust boundary and checks

Merge materialization is a private pipeline. `RadixSortBuffer`, its projection,
`RadixSortRun`, codec, payload layout, and output vector are created together.
Consequently, stable internal invariants use debug checks, including plan-bound
state, channel counts, compatible child types, nonnegative offsets, and
destination range fit. Segment execution does not rescan the full schema.

Spill files are an external byte boundary and are checked in Release builds.
`RadixSortSpillReader::nextBatch()` verifies positive row and block sizes, codec
limits, compressed or uncompressed size consistency, checked fixed-section
multiplication, checked total-section addition, and exact body size. Pointer
restoration validates key-heap and payload-heap bounds and requires both cursors
to end exactly at their declared section ends. Malformed framing, section sizes,
or restored pointers fail fast before they can become trusted merge pointers.

The encoded cursor parser also keeps data-dependent bounds and marker checks
needed for safe masked-column skipping. This is targeted input validation, not a
repeated batch-wide schema-validation phase.

## 5. Performance contract

### 5.1 Work remains batched

The unit of work is a boundary-delimited segment, not an individual row. One
callback invokes key materialization and payload gather once over the complete
pointer span. Column dispatch, pointer-view construction, string sizing, and SIMD
selection remain outside inner row loops where the existing implementation
already supports that shape.

The implementation preserves these paths:

- compile-time key-only versus payload merger specialization;
- single-stream, two-way, and loser-tree merge;
- direct single-fixed key decode;
- inline or heap variable-key view construction;
- disjoint fixed-prefix and suffix decode;
- masked fixed and recursive variable cursor skip;
- fixed, Boolean, int128, Timestamp, string, and complex key kernels;
- typed fixed payload gather;
- single-column null-free int64 AVX2 gather;
- fixed-only grouped fixed64 AVX2 gather;
- single-string and tiled multi-string gather; and
- direct complex payload deserialization.

An ordinary selected row incurs one virtual `tryAdvance()` call that both tests
and performs the safe advance. Only a boundary row adds a callback and
`advanceAfterFlush()` call. The callback can contain rows from multiple streams,
which avoids turning stream changes into artificial batch boundaries.

### 5.2 Cached work

Stable work is cached at the narrowest useful lifetime:

- key layout, width, payload presence, and payload offset are cached by a merge
  stream instead of reconstructing a key wrapper for every row;
- the merger stores the selected comparison function;
- `MergeDecodePlan` caches single-fixed eligibility/word width or scratch words
  per row across output batches;
- `PayloadRowReader::Plan` caches column classification and channel metadata
  across output batches; and
- payload `bind()` caches current destination vectors, raw pointers, and AVX2
  availability once per output batch.

The output pointer arrays and merge decode scratch retain capacity and change
only logical size for smaller segments or batches. The shared spill buffer cache
allows physical files to reuse one eligible serialized allocation without
keeping exhausted readers alive.

### 5.3 Direct input benefit

Direct-to-storage key append removes the allocation and memory traffic of first
forming a complete `EncodedKeyBatch` and then copying it into record and heap
storage. Inline layouts encode by storage block. Variable layouts perform only
the required sizing pass before writing fixed-prefix bytes and suffix bytes to
their final locations, followed by the small inline mirror needed for radix
comparison.

This does not eliminate necessary payload encoding, variable-key size scratch,
or the inline suffix mirror. Those objects serve distinct layout requirements
and are not full-key intermediates.

### 5.4 Expected segmentation costs

Correct lifetime boundaries can make a segment shorter than the requested output
batch. A short segment may fall below a SIMD threshold. Each string column may
request output-owned space once per segment that contains non-inline bytes, so a
batch spanning many tiny blocks can have more requests than one monolithic
decode. Complex values remain row-deserialized because their serde API is
row-oriented.

The design accepts these bounded costs in exchange for releasing source blocks
promptly. It does not add a retaining fallback or a production switch. Benchmark
results must report the effect instead of assuming throughput parity.

## 6. Correctness contract

The following invariants define completion of the implementation.

### 6.1 Input

1. Every production `RadixSortRun::append()` key is encoded directly into final
   record/heap storage.
2. Inline layouts never need a full encoded-key intermediate.
3. Variable layouts size the suffix first, then write prefix and suffix directly
   to their final storage.
4. The inline suffix mirror contains only the bytes required by the physical
   radix prefix.
5. Payload compact pointers are stored in the same append pass after payload rows
   have stable addresses.
6. `EncodedKeyBatch` is not part of the production run state or append path.

### 6.2 Merge lifetime

1. A spill stream that returns `false` from `tryAdvance()` has not advanced.
2. The pending segment is consumed synchronously before `advanceAfterFlush()`.
3. The callback retains no source pointers.
4. Both normal merged output and output-stage spill obey the same rule.
5. No historical serialized block or exhausted physical-file child is retained.
6. The merger owns one shared reusable serialized-buffer cache; each active disk
   stream owns only its current reader state.
7. EOF closes inputs and removes exhausted paths; failure cleanup occurs during
   terminal stream or merger destruction.

### 6.3 Output

1. The final `RowVector` is prepared once for the whole requested batch.
2. Source and scratch indices start at zero for every segment; only destination
   indices add `outputOffset`.
3. Segment sizes are positive, and their sum equals the requested output count.
4. Segment writers do not resize or replace output children.
5. No temporary output `RowVector` or `Vector::copy` is used.
6. Every logical key column is decoded exactly once: prefix
   `[0, firstSuffixColumn_)`, suffix `[firstSuffixColumn, end)`.
7. Variable suffix decode selects the inline mirror or heap, never both.
8. Masked encoded columns advance cursors without materializing throwaway values.
9. String and complex output owns its data before a source block advances.
10. Nested vector contents written by earlier segments survive later growth.
11. Null and Boolean bits outside the destination segment are unchanged.
12. Data-dependent child metadata is reset once after successful batch
    completion.

### 6.4 Metrics

`writeMergeOutput()` does not commit row metrics. After every segment succeeds,
`finishMergeOutput()` uses checked addition to update
`RadixSortRun::metrics_.outputRows` exactly once with the complete batch size.

`RadixSortBuffer::outputRows_` is increased only after `getMergedOutput()`
returns successfully. Merge output wall time is owned by
`RadixSortBuffer::outputTimeUs_` and is also committed only on successful batch
completion. `RadixSortRun::metrics_.outputTimeUs` is not charged again for this
path.

If collection, materialization, finalization, or reader advancement throws, the
exception propagates and these batch metrics are not committed. Spill-reader
metrics already accumulated by completed reads remain available through the
normal ownership path.

### 6.5 Failure and validation

1. Internal offset/materialization misuse is a debug invariant, not a public
   recoverable error contract.
2. Arithmetic overflow and malformed external spill blocks fail in Release.
3. A callback exception preserves the current boundary row but does not roll back
   ordinary advances earlier in the segment.
4. Failures are terminal; the implementation does not retry a partially written
   output batch.
5. RAII preserves ownership on failure and removes owned spill files when the
   failed operator is destroyed; destructors make cleanup best-effort/no-throw.
6. Successful output-stage serialization deep-copies a full segment before the
   source reader may advance.

## 7. Tests and benchmarks

### 7.1 Reader and merger tests

Keep focused coverage in `RadixSortSpillSectionsTest.cpp` for:

- `readerReusesGrowsAndDropsBuffers`: same/smaller blocks reuse capacity, growth
  releases the old allocation first, oversized blocks are not cached, and EOF
  releases or recycles the final block;
- `mergerFlushesBoundarySegmentsForAllShapes`: single-stream, two-way, and
  loser-tree collection, key-only and payload modes, first-row boundaries,
  repeated boundaries, and the final non-boundary callback;
- `mergerCallbackThrowLeavesBoundaryRowUnadvanced`: only the boundary row is
  guaranteed not to advance when its callback throws;
- `safeBoundaryAdvanceCleansFileWhenNextBlockIsCorrupt`: successful callback
  followed by a failing next-block load still cleans up;
- `concatFileMergeStreamSafeAdvanceRetiresExhaustedFiles`: physical files retire
  immediately, metrics remain monotonic, and the merger-level cache is reused;
- empty-merger fail-fast behavior for a non-empty request; and
- compressed, uncompressed, split-file, and mixed disk/memory streams.

Corrupt-spill tests must exercise Release checks for invalid header sizes, codec
bounds, section totals, and restored heap-pointer bounds. Internal private helper
shape assertions do not need a separate heavy-validation test matrix.

### 7.2 Input append tests

Keep `RadixSortRunTest.cpp` coverage that proves direct-to-storage behavior:

- `variableKeyEncodesDirectlyIntoRecordAndHeap` compares record prefix, heap
  suffix, encoded size, and inline mirror against the low-level codec oracle;
- key-only fixed8/16/24/32 and payload-bearing fixed16/24/32 layouts round-trip;
- the single-flat-fixed fast path preserves all sort directions, null placement,
  Boolean, floating-point, decimal, and Timestamp semantics;
- appending several input batches maintains maximum encoded size and the
  run-wide inline-versus-heap decode decision;
- payload pointers refer to final payload rows; and
- sort and spill round-trips remain identical to the comparator oracle.

Low-level `EncodedKeyBatch` tests remain useful as codec tests and as an oracle.
They must not be interpreted as evidence that production append still builds
that intermediate.

### 7.3 Offset materialization tests

Keep key/run coverage for offsets `0`, `1`, `63`, `64`, and `65`, including:

- `spillOffsetSingleFixedPreservesSentinels`;
- `spillOffsetNullableBooleanPreservesValueAndNullBitmapSentinels`;
- `spillOffsetLayeredVariableSegmentsAndChildIdentity`;
- `spillOffsetSkipsMaskedKeysAndAppendsNestedOutput`;
- `spillOffsetSelectedComplexKeysGrowAcrossSegments`; and
- `spillOffsetTimestampKeyAndPayload`.

These tests verify untouched prefix and suffix sentinels, exact child identity,
nullable and null-free segments in both orders, final short segments, masked
fixed/string/ARRAY/MAP/ROW cursor advancement, inline and heap variable keys, and
the destination boundary exactly at `output.size()`.

Keep payload coverage for:

- `segmentedGatherPreservesBitmapBoundaries`;
- `segmentedGatherInvalidatesFullRangeNullCount`;
- `gatherBatchFinalizesMetadataOnce`; and
- `segmentedGatherMapsStringsAndGrowingComplexValues`.

Bitmap cases should straddle word boundaries, including `(1, 1)`, `(1, 63)`,
`(63, 2)`, `(64, 64)`, and `(65, 63)`. Boolean value bits and null bits are
checked independently. String and nested complex cases use at least two segments
and force later storage growth while validating earlier values.

### 7.4 End-to-end and ownership tests

Keep `RadixSortBufferTest.cpp` coverage for:

- `spillBatchOutputSizesAcrossBlockBoundaries`, including the requested
  `{3, 3, 2}` batch sizes;
- `fixedWidthSpillOutputCrossesBlockBoundaries`;
- `spillMergeWidePayloadStaysWithinOperatorCap`, which is the primary regression
  for historical-block memory growth;
- `outputVectorReuse`, including final child reuse when ownership permits;
- key-only, payload-bearing, compressed, split-file, and multi-run output;
- complex keys and MAP payloads;
- output-stage spill after partial output;
- spill statistics and file cleanup; and
- output data surviving buffer destruction and memory-pool churn.

`presizedWriterDeepCopiesBeforeReturning` independently proves that the
output-stage writer owns all key and payload bytes before its callback returns.
Failure injection should continue to verify both boundary ordering and cleanup,
without asserting transactional retry behavior.

### 7.5 Performance validation

Build and run the focused radix target first:

```bash
cmake --build --preset conan-release --target bolt_radix_sort_test
```

Run the relevant tests with AVX2 enabled and with
`--bolt_enable_avx2=false`. This confirms that scalar and SIMD paths produce the
same results; unit tests alone do not prove performance parity.

Compare stable pre-change and changed-build medians with the existing radix sort
buffer benchmark. At minimum include:

- wide fixed-only payload, where grouped fixed64 is eligible;
- wide string payload;
- complex payload;
- fixed and variable keys; and
- spill layouts that produce both long and short boundary segments.

Report total time and output time separately, plus peak/current operator-pool
memory where available. Use the same data, batch size, compression, spill file
size, run count, and AVX2 setting for both builds. Process RSS is not a substitute
for the constrained operator-pool regression.
