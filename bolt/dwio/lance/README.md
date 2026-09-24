# Bolt-native Lance reader

This directory contains the Bolt-owned Lance reader. The production read path
does not call `lance_file_ffi` and does not materialize Arrow record batches.
Metadata, page buffers, decompression output, and result vectors are owned by
Bolt through `BufferedInput`, `MemoryPool`, and the DWIO reader interfaces.

## Architecture

- `NativeLanceFileOpenTask` constructs and validates an immutable
  `NativeLanceFileContext`. Each row reader clones its own scan input while the
  context retains the input used for lazy metadata reads.
- `NativeLanceMetadata` parses the footer, schema, column metadata, page
  layouts, physical-column mapping, page-row indexes, and split row ranges.
- `NativeLanceScanPlan` binds projection, filters, required physical columns,
  the logical column-reader tree, and split row ranges once per row reader.
- `NativeLanceReadScheduler` deduplicates byte ranges, submits them to
  `BufferedInput`, and can return page-owned buffers together with their memory
  reservations.
- `NativeLanceColumnReader` owns logical type semantics. Batch-local
  `NativeLanceColumnReadTask` instances keep execution state out of the shared
  reader tree.
- `NativeLanceColumnCursor` maps monotonically increasing row ranges to page
  spans without rescanning page metadata from the beginning.
- `NativeLanceStructuralPageReader` owns v2.1+ page state and invokes the
  structural decode kernel. `NativeLanceLegacyPageReader` owns v2.0 compressed
  Flat page state.
- `NativeLanceDecompressor` owns legacy zstd and LZ4 codec handling and exposes
  whether a codec is sequential-frame or whole-buffer based.
- `NativeLanceLegacyScalar` owns v2.0 scalar, nullable, bitmap, bitpack, and
  fixed-size binary kernels. These kernels no longer live in the monolithic
  migration adapter.
- `NativeLanceLegacyBinary` and `NativeLanceLegacyDictionary` own v2.0
  variable-width, FSST, dictionary-index, and dictionary assembly kernels.
- `NativeLanceMemoryBudget` provides move-only reservations for bounded
  transient scan memory.
- `NativeLanceDecoder` is the migration adapter for legacy page paths. It does
  not retain decoded or decompressed payloads across batches and will be
  removed after all encoding families move to page readers.
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
  matching Lance semantics. The native reader does not add a reader-scoped
  object cache; dataset integrations may provide one behind the resolver.
- Prefetch units and the one-batch-ahead pipeline submit compressed byte ranges
  into the single scan-owned scheduler. They do not clone inputs or decoders.
- Zstd pages preserve only a sequential codec cursor, one bounded compressed
  chunk, and a small unaligned tail across batches. LZ4 is explicitly handled
  as a whole-buffer codec. Neither path retains decoded or decompressed page
  caches across batches.
- `maxBatchBytes` caps row batches using a conservative estimate followed by
  feedback from the retained size of produced vectors.
- Top-level and nested Struct filters are decoded first; projected columns are
  then materialized only for coalesced surviving row ranges. Nested Struct
  projection replaces unselected children with typed null constants. Array
  subscript projection bounds each row's elements, while Map subscript
  projection filters keys and applies the same entry selection to values. The
  transformed vectors retain parent nulls and use dictionary indices instead
  of mutating decoded pages. Page/statistics pruning is unavailable because
  Lance v2 page metadata does not store value, null-count, minimum, or maximum
  statistics.
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
Lance Rust reader before native tests consume them. Production and public test
targets do not link `lance_file_ffi`; cross-version performance comparison uses
the standalone current-main Rust harness below.

The C++ benchmark exposes independent `parquet` and `native` modes and forces
every projected child vector to load. The companion generator creates a
deterministic 43-column Lance v2.2 Zstandard file and a Zstandard Parquet file
with equivalent Bolt-visible values:

    bolt/dwio/lance/tests/generate_lance_benchmark_data.py \
      /tmp/bolt-lance-all-types \
      --rows 65536 \
      --batch-rows 65536 \
      --lance-repo /path/to/lance

The generated manifest records paths, file sizes, projected column count, and
the row-count/checksum oracles. It also lists the logical storage adaptations
needed because Bolt's Parquet reader does not expose every Arrow logical type
supported by Lance. Blob v2 storage kinds are intentionally excluded because
they require a dataset/sidecar resolver and have no equivalent Parquet
representation; deterministic correctness fixtures cover them instead.

To compare against an arbitrary current Lance checkout without linking Rust
into a production or benchmark target, build the standalone test harness and
use the printed executable as the baseline command:

    bolt/dwio/lance/tests/run_current_rust_reader_benchmark.py \
      --lance-repo /path/to/lance --prepare-only

The harness accepts the same BOLT_LANCE_BENCHMARK_FILE,
BOLT_LANCE_BENCHMARK_EXPECTED_ROWS, and BOLT_LANCE_BENCHMARK_BATCH_SIZE
environment variables as the C++ benchmark.
`BOLT_LANCE_BENCHMARK_RUNTIME_THREADS` controls the harness Tokio runtime; use
it together with Lance's `LANCE_CPU_THREADS` and `LANCE_IO_THREADS` plus CPU
affinity when comparing equal CPU budgets. The harness performs a full-column
materializing scan and emits Folly-compatible JSON, so it can be passed
directly to bolt-benchmark-compare. The reported metric is picoseconds per row,
matching the work-unit normalization performed by Folly BENCHMARK_MULTI.

`run_lance_reader_benchmark_suite.py` wraps the native, Parquet, and Rust
executables behind one JSON protocol. Each invocation performs exactly one
scan for each combination of full scan or approximately 1% filtering and 1 or
16 pinned CPUs. It fixes the row batch size at 1,024 and validates all four
scans against the manifest. For example:

    python3 bolt/dwio/lance/tests/run_lance_reader_benchmark_suite.py native \
      --manifest /tmp/bolt-lance-all-types/manifest.json \
      --native-benchmark \
        _build/NativeWithFfi/bolt/dwio/lance/tests/bolt_dwio_native_lance_reader_benchmark

Pass the suite commands to `bolt-benchmark-compare --rounds 7`. The tool
alternates execution order, applies an exact paired sign-flip test on log
ratios, corrects the four case p-values with Holm's method, and writes a
self-contained HTML report plus companion JSON and per-run artifacts.

For one acceptance dataset, `run_native_rust_acceptance_benchmark.py` provides
a standalone paired runner. It alternates native/Rust order for seven rounds,
pins both readers to the same CPUs, samples process peak RSS from `/proc`, and
stores raw outputs together with an exact paired sign-flip result:

    python3 bolt/dwio/lance/tests/run_native_rust_acceptance_benchmark.py \
      --dataset /tmp/data.lance \
      --native-benchmark \
        _build/NativeWithFfi/bolt/dwio/lance/tests/bolt_dwio_native_lance_reader_benchmark \
      --rust-benchmark /tmp/bolt-current-rust-lance-benchmark/target/release/bolt-current-rust-lance-benchmark \
      --output /tmp/lance-reader-acceptance.json

The filter comparison has intentionally different execution semantics. Bolt's
native reader applies the `ScanSpec` filter before materializing surviving
projected rows. The current Rust `FileReader` API accepts only an opaque filter
expression and does not execute this predicate in the core decoder, so the
standalone Rust harness decodes all columns and applies Arrow's
`filter_record_batch` afterward. Reports must retain this distinction rather
than describe the Rust result as predicate pushdown.
