# Bolt-native Lance reader

This directory contains the Bolt-owned Lance reader. The production read path
does not call `lance_file_ffi` and does not materialize Arrow record batches.
Metadata, page buffers, decompression output, and result vectors are owned by
Bolt through `BufferedInput`, `MemoryPool`, and the DWIO reader interfaces.

## Architecture

- `NativeLanceMetadata` parses the footer, schema, column metadata, page
  layouts, physical-column mapping, and row ranges.
- `NativeLanceReadPlan` deduplicates byte ranges, submits them to
  `BufferedInput`, retains async streams, and materializes Bolt-owned buffers.
- `NativeLanceColumnReader` binds projection and constants, separates
  row-aligned from offset-dependent work, and runs independent compressed
  columns on Bolt's decode executor.
- `NativeLanceDecoder` performs page selection, two-stage planning, decoding,
  page caching, and structural sibling assembly.
- `NativeLanceStructuralDecoder` implements the v2.1-v2.3 dense and Sparse
  structural layouts and compressive encoding grammar.
- `NativeLanceTypeAdapter` preserves semantic type identity without changing
  the physical Bolt vector kind and lets applications inject additional
  fail-closed Arrow extension mappings.
- `NativeLanceBlobResolver` decouples Packed, Dedicated, and External Blob v2
  object lookup from the file-format decoder. Dataset integrations resolve a
  descriptor to a Bolt `BufferedInput`, preserving the selected cache, I/O
  executor, accounting, coalescing policy, and Bolt-owned memory.
- `NativeLanceReader` exposes the implementation as a DWIO `Reader` and
  `RowReader`, including skip, split ownership, batch memory limits, prefetch,
  selective top-level filtering, and runtime statistics.

`lance_decode_parallelism` is the maximum number of decode execution contexts
used for one batch, including the calling thread. Its default is 16; set it to
1 for deterministic single-threaded decoding.

## File-format compatibility

The reader accepts historical footer `0.3` and canonical Lance file versions
`2.0`, `2.1`, `2.2`, and `2.3`. Version 2.3 Sparse pages use their native
layer-local validity, list-offset, and FixedSizeList domains instead of being
expanded into dense repetition and definition streams.

Supported v2.0 array encodings include Flat, Nullable, Bitpacked,
BitpackedForNonNeg, Dictionary, FixedSizeBinary, FixedSizeList, List, Map,
Struct, PackedStruct, FSST, zstd/lz4 wrappers, and the legacy Blob descriptor.
List, Map, and legacy binary offsets may use Flat, Bitpacked, or the
1,024-value FastLanes `BitpackedForNonNeg` layout.
The additional Constant, Variable, Inline/OutOfLineBitpacking, RLE,
ByteStreamSplit, GeneralMiniBlock, Block, and fixed-width mini-block messages in
the shared protobuf are v2.1 grammar components; a v2.0 writer does not emit
them as top-level array encodings.

Supported v2.1-v2.3 page layouts are MiniBlock, ConstantLayout, FullZip,
BlobLayout, and SparseLayout. Supported compressive encodings are Flat, Variable, Constant,
InlineBitpacking, OutOfLineBitpacking, FSST, RLE, ByteStreamSplit, General,
FixedSizeList, PackedStruct, and v2.2 VariablePackedStruct. Dictionary buffers
in MiniBlock pages are decoded and retained as Bolt dictionary vectors.

Structural reconstruction supports nullable Struct, nested List/LargeList,
Map, recursive FixedSizeList, v2.2 FixedSizeList<Struct> whose descendants may
include List/LargeList/Map, packed structs, empty structs, null and empty lists,
and pages crossing the FastLanes 1,024-value boundary. Every physical leaf of a
projected structural field participates in the same read plan before decode.

## Type mapping

| Lance / Arrow logical type | Bolt output | Notes |
| --- | --- | --- |
| null | UNKNOWN | all values are null |
| bool | BOOLEAN | lossless |
| int8/int16/int32/int64 | TINYINT/SMALLINT/INTEGER/BIGINT | lossless |
| uint8/uint16/uint32/uint64 | SMALLINT/INTEGER/BIGINT/HUGEINT | widened, lossless |
| float16/float32/float64 | REAL/REAL/DOUBLE | float16 is widened |
| string/large_string | VARCHAR | large-offset distinction is not retained |
| binary/large_binary | VARBINARY | large-offset distinction is not retained |
| fixed_size_binary | VARBINARY | bytes are lossless; fixed-width schema constraint is not retained |
| date32/date64 | DATE | date64 must be aligned to a whole day |
| time32/time64 | BIGINT-kind semantic type | encoded unit and TIME identity are preserved |
| timestamp s/ms/us/ns | TIMESTAMP-kind semantic type | instant, unit, and timezone label are preserved |
| duration s/ms/us/ns | INTERVAL DAY TO SECOND | us/ns values must be exactly representable in milliseconds |
| decimal128 | DECIMAL | precision <= 38, non-negative scale |
| JSON extension | VARBINARY-kind semantic type | Lance JSONB storage and extension metadata are preserved |
| lance.bfloat16 | VARBINARY-kind semantic type | exact bytes and extension identity are preserved |
| Dictionary | underlying Bolt type / DictionaryVector | all eight integer key widths and all v2.0-v2.2 writer-supported scalar/string/binary value types are accepted |
| List/LargeList | ARRAY | large-offset distinction is not retained |
| FixedSizeList | ARRAY | values are lossless; fixed-size schema constraint is not retained |
| Map | MAP | Arrow-compatible non-null keys |
| Struct | ROW | nested and empty structs supported within the v2.1/v2.2 writer limits |
| Blob v2 | VARBINARY | Inline reads the data file; Packed, Dedicated, and External use an injected `NativeLanceBlobResolver` |

The following cases are explicitly rejected instead of being silently
truncated or misinterpreted:

- Decimal256, because Bolt has no lossless 256-bit decimal representation;
- Decimal128 with a negative scale or precision greater than 38;
- date64 values that are not exact whole days;
- duration us/ns values that are not exact milliseconds;
- Blob v2 Packed, Dedicated, and External descriptors when no dataset/sidecar
  object resolver was supplied;
- unknown logical types, unknown Arrow extension types, conflicting extension
  metadata, and unsupported future file versions. Unknown extensions fail
  closed instead of silently exposing only their storage type.

Types rejected by Lance's schema writer through v2.3, including Arrow Union,
RunEndEncoded, Interval, ListView/LargeListView, sorted Map, and
direct FixedSizeList<List/Map>, are outside this reader's compatibility
contract. The v2.1 writer also rejects FixedSizeList<Struct>; that structural
form is supported for v2.2 files. Bolt parses Dictionary metadata from the
right, so value logical types containing `:` are supported. Current upstream
Lance writers can serialize these types but need the corresponding
right-anchored parser fix to read them back; checked-in fixtures cover Date,
Time, Timestamp, Duration, Decimal, and FixedSizeBinary values for v2.0-v2.2.

## I/O, memory, and selective reads

- For v2.1 and newer structural files, the footer, schema, and column-offset
  table are opened eagerly while column metadata is loaded in one coalesced
  request for only the projected and filtered physical columns. Legacy v2.0
  metadata remains eager because its physical mapping depends on page encoding.
- Page ranges are scheduled through `BufferedInput`, allowing native
  coalescing and asynchronous implementations.
- The read plan splits large ranges at the Bolt `loadQuantum`, limits submitted
  but not yet materialized bytes with `maxCoalesceBytes`, and cancels pending
  Direct/Cached input loads when a reader or plan is abandoned.
- Primitive all-valid MiniBlock pages parse chunk metadata first and read only
  the contiguous chunk span covering the requested rows. Nullable, nested,
  packed, fixed-size, and rep/def-compressed layouts keep the full-page path.
- Variable-width, list, map, and Blob payloads use a second scheduling stage
  after offsets or descriptors have been decoded.
- Non-inline Blob v2 payloads are grouped by resolved object and submitted
  through its `BufferedInput`, then loaded once per object. The resolver must
  be thread-safe because prefetch may call it concurrently. External
  descriptors with `size == 0` consume the remainder of the resolved object,
  matching Lance semantics. Resolved object inputs are retained in a bounded
  reader-scoped LRU and cloned for independent batch reads.
- Prefetch units use cloned inputs so a background load cannot invalidate the
  active reader's staged buffers.
- Async-capable inputs maintain a one-batch-ahead I/O pipeline. Fully decoded
  compressed pages are cached in a reader-scoped LRU shared by the active and
  cloned prefetch decoders; MiniBlock range reads remain chunk-selective.
- Decompressed buffers are allocated from Bolt's pool and held in an adaptive
  bounded cache. `BOLT_LANCE_DECOMPRESSED_CACHE_BYTES` can override its limit.
- `maxBatchBytes` caps row batches using a conservative estimate followed by
  feedback from the retained size of produced vectors.
- Top-level filters are decoded first; projected columns are then materialized
  only for coalesced surviving row ranges. Nested selective filters and
  page/statistics pruning remain future work.
- DWIO mutation bitmaps are applied before selective filters and before
  materializing surviving projected rows. `columnStatistics()` reports the
  reliable per-column on-disk page bytes available in v2 metadata; value, null,
  min, and max statistics remain unknown because v2 files do not store them.

## Validation

Deterministic fixtures cover historical v2.0 files and Rust-current v2.1-v2.3
files, including exact-version multi-page data, nested structural types, all
supported page layouts and compression families, all four Blob v2 storage
kinds (with an in-memory resolver), dictionary key/value
matrices, complex FixedSizeList<Struct<List/Map>>, scalar semantic mappings,
v2.3 Sparse primitive/List/FixedSizeList pages, zero-width integer bitpacking,
unknown-extension rejection, and explicit lossy
type rejection paths. The fixture suite validates ranges crossing row 1,024.

The checked-in current-writer fixtures are first read back by the matching
Lance Rust reader before native tests consume them. The optional 0.37 FFI
differential oracle covers its supported legacy and ordinary structural types;
that older reader does not implement structural Dictionary, Map, or complex
FixedSizeList dispatch.

When `BOLT_ENABLE_LANCE=ON`, the differential test writes files with the legacy
Rust writer and compares a test-only direct Rust FFI adapter with the native
reader. Production native targets do not link `lance_file_ffi`.

The benchmark exposes independent `parquet`, `rust`, and `native` modes and
forces every projected child vector to load. Use a batch size of 1,024 and the
repository's `bolt-benchmark-compare --rounds 7` wrapper for alternating runs,
paired sign-flip tests, and Holm correction.

The rust mode above uses the packaged compatibility FFI. To compare against an
arbitrary current Lance checkout without linking Rust into a production target,
build the standalone test harness and use the printed executable as the
baseline command:

    bolt/dwio/lance/tests/run_current_rust_reader_benchmark.py \
      --lance-repo /path/to/lance --prepare-only

The harness accepts the same BOLT_LANCE_BENCHMARK_FILE,
BOLT_LANCE_BENCHMARK_EXPECTED_ROWS, and BOLT_LANCE_BENCHMARK_BATCH_SIZE
environment variables as the C++ benchmark. It performs a full-column
materializing scan and emits Folly-compatible JSON, so it can be passed
directly to bolt-benchmark-compare. The reported metric is picoseconds per row,
matching the work-unit normalization performed by Folly BENCHMARK_MULTI.
