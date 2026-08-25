# Radix Spill Row Format Refactor Plan

This document records the calibrated design for the radix spill row format.

## Goal

Radix keeps its own spill row format, row serialization/deserialization, and
merge tree. The outer radix spill writer/reader code may keep its current file
and block handling, including max spill file size rolling, but the per-row disk
format and serde logic should be replaced.

The new disk row must be:

```text
key record spill part
key heap
payload fixed
payload heap
next row
```

The disk row must not write:

- per-row row size
- key data pointer
- key payload pointer
- payload variable pointer
- payload heap total size

## Disk Row Format

### 1. Key Record Spill Part

The key record spill part is a single contiguous copy from the beginning of the
runtime key record. Metadata computes the copy size once.

Rules:

- Fixed key without payload: copy the complete fixed key record,
  `keyLayout.width()`.
- Fixed key with payload: copy only the bytes before the payload pointer,
  `payloadOffset`.
- Variable key, with or without payload: copy only the bytes before the key data
  pointer, `dataOffset`.

For variable keys, this copied prefix contains the existing runtime size field.
That field is the encoded key size already stored in the runtime record, for
example in `KeyOnlyVariable32Record`. It is copied as-is and no extra heap size
field is written.

### 2. Key Heap

The key heap exists only when the runtime key has an external key heap pointer.
No extra key heap size is stored on disk.

Key heap size rules:

- No key heap: write nothing.
- Fixed-size key heap: use a metadata-computed fixed heap size.
- Variable-size key heap: load the existing encoded key size from the copied key
  record and compute:

```text
keyHeapSize = encodedKeySize - keyLayout.heapKeyOffset()
```

The source is the runtime key data pointer. The pointer itself is not written to
disk.

### 3. Payload Fixed

Payload fixed is exactly the runtime `PayloadRow` fixed area:

```text
null bitmap + payload slots
```

The fixed area is copied in one `memcpy`. There is no payload heap total size
field.

For variable payload slots, only cached variable payload columns are visited.
The loop must not scan all payload columns per row.

Variable slot rules:

- Null: write no payload heap bytes; the destination slot can be zeroed.
- Inline string: keep the slot as-is. The final eight bytes are inline data, not
  a pointer.
- Non-inline string: keep size and prefix; clear the string pointer bytes.
- Complex payload: keep `PayloadVarlenRef::size`; clear
  `PayloadVarlenRef::data`.
- Complex payload with size zero: write no heap bytes and keep the data pointer
  cleared.

### 4. Payload Heap

Payload heap contains only the bytes referenced by variable payload fields:

- complete bytes for non-inline strings
- serialized bytes for complex payload values

The order is the cached variable payload column order. For payload rows produced
by `PayloadRowWriter`, the heap bytes for one row are written in that same order
and are contiguous, so serialization should normally copy the payload heap with
one `memcpy` per row.

Do not assume payload heaps are contiguous across different rows. Spill rows are
written in sorted key order and payload row pointers may be reordered.

## Metadata

Add a serde metadata object, for example `RadixRow2RowSerdeMeta`, so schema and
layout decisions are made once instead of inside every row.

Required key metadata:

```text
keyLayout
keyRecordCopySize
runtimeKeyRecordSize
keyHeapOffset
keySizeOffset
keyDataPointerOffset
keyPayloadPointerOffset
keyHeapMode: none / fixed / variable
fixedKeyHeapSize
hasPayload
```

Required payload metadata:

```text
payloadFixedSize
payloadVariableOps
hasVariablePayload
```

`payloadVariableOps` should be a cached list of only variable payload columns,
in column order. Each entry should carry the slot offset and enough type kind
information to handle string versus complex payload slots without re-reading the
full schema per row.

Important calibration:

- `keyRecordCopySize` is always metadata-derived. Row serialization should just
  `memcpy(out, key, keyRecordCopySize)`.
- Variable key heap size is row-value dependent because it comes from the
  copied existing size field. This is not a schema branch.
- Payload null, string inline/non-inline, and complex size zero checks are row
  value checks and cannot be fully moved to metadata.
- Do not add extra `BOLT_CHECK` for this refactor.

## Serialization

The target shape is four copies plus optional pointer clearing:

```cpp
char* serializeRow(const char* key, const char* payload, char* out);
```

Conceptual flow:

```text
1. memcpy key record spill part
2. optionally memcpy key heap
3. optionally memcpy payload fixed
4. optionally clear variable payload pointers in copied fixed area
5. optionally memcpy payload heap, preferably once per row
6. return the advanced output cursor
```

Pseudo-flow:

```cpp
memcpy(out, key, meta.keyRecordCopySize);
out += meta.keyRecordCopySize;

if constexpr (HasKeyHeap) {
  const auto keyHeapSize = keyHeapSizeForRow(meta, key);
  const auto* keyHeap = loadCompactPointer(key + meta.keyDataPointerOffset);
  memcpy(out, keyHeap, keyHeapSize);
  out += keyHeapSize;
}

if constexpr (HasPayload) {
  auto* outPayloadFixed = out;
  memcpy(outPayloadFixed, payload, meta.payloadFixedSize);
  out += meta.payloadFixedSize;

  if constexpr (HasVariablePayload) {
    auto range = clearPayloadPointersAndGetHeapRange(
        meta, payload, outPayloadFixed);
    memcpy(out, range.begin, range.size);
    out += range.size;
  }
}

return out;
```

The hot path should be dispatched by metadata/compile-time cases so each row does
not repeatedly branch on schema shape.

Fast paths to preserve or reintroduce:

- fixed key record plus fixed payload row
- fixed key record plus fixed key heap plus fixed payload row
- no payload
- no variable payload

## Deserialization

The target shape is four copies plus pointer restore:

```cpp
const char* deserializeRow(const char* in, char* keyOut, char* payloadOut);
```

The exact API can return either the advanced input cursor or fill a caller-owned
result object, but it must adapt cleanly to the radix spill reader and merge
tree.

Conceptual flow:

```text
1. memcpy disk key record spill part into runtime key record
2. optionally memcpy disk key heap into runtime key heap and restore key data pointer
3. optionally memcpy disk payload fixed into runtime payload fixed
4. optionally restore key payload pointer
5. optionally compute payload heap bytes from payload slots
6. optionally memcpy disk payload heap into runtime payload heap
7. restore variable payload pointers from metadata and row values
8. return the advanced disk cursor
```

Pseudo-flow:

```cpp
memcpy(runtimeKey, disk, meta.keyRecordCopySize);
disk += meta.keyRecordCopySize;

if constexpr (HasKeyHeap) {
  const auto keyHeapSize = keyHeapSizeForRow(meta, runtimeKey);
  memcpy(runtimeKeyHeap, disk, keyHeapSize);
  storeCompactPointer(
      runtimeKey + meta.keyDataPointerOffset,
      runtimeKeyHeap);
  disk += keyHeapSize;
}

if constexpr (HasPayload) {
  memcpy(runtimePayload, disk, meta.payloadFixedSize);
  disk += meta.payloadFixedSize;

  if constexpr (KeyHasPayloadPointer) {
    storeCompactPointer(
        runtimeKey + meta.keyPayloadPointerOffset,
        runtimePayload);
  }
}

if constexpr (HasVariablePayload) {
  const auto payloadHeapBytes =
      payloadHeapSizeFromSlots(meta, runtimePayload);
  memcpy(runtimePayloadHeap, disk, payloadHeapBytes);
  restorePayloadPointers(meta, runtimePayload, runtimePayloadHeap);
  disk += payloadHeapBytes;
}

return disk;
```

Deserialize is not a complex parser. It advances through the four known regions
using metadata plus the existing row value size fields.

## Required Code Changes

### RadixSortSpillRow.h / RadixSortSpillRow.cpp

Replace the old row-header and row-relative-pointer model with the new serde
model.

Delete or replace:

- `RadixSortSpillRowHeader`
- `RadixSortSpillRow::kHeaderSize`
- `header()`
- `totalSize()`
- old `sizeForSerialize()` based on `totalSize`, `keySize`,
  `payloadFixedSize`, and `payloadHeapSize`
- old `serialize()` that writes row header and row-relative offsets
- `trustedKeySize()`
- `trustedPayloadHeapSize()`
- `trustedKeyBytes()`
- `trustedPayloadFixed()`
- `trustedPayloadHeap()`
- old `trustedRestoreKeyDataPointer()` row-relative key restore
- old `trustedRestorePayloadPointers()` row-relative payload restore
- old `swizzlePayloadPointerFields()` offset-to-pointer/pointer-to-offset
  implementation

Add or replace with:

- metadata construction
- key record copy-size calculation
- key heap size helpers
- payload variable op cache
- payload pointer clear helpers
- payload heap range/size helpers
- runtime pointer restore helpers
- `serializeRow(const char* key, const char* payload, char* out)`
- deserialize API that returns the advanced disk cursor

### PayloadRow.h / PayloadRow.cpp

Add cached variable payload column metadata if it does not already exist at the
needed level.

The goal is to allow radix spill serde to iterate only variable payload columns
and avoid checking every `PayloadRowLayout::columns()` entry per row.

Do not change runtime `PayloadRow` fixed format. It is already:

```text
null bitmap + slots
```

### RadixSortSpill.h / RadixSortSpill.cpp

Keep:

- radix spill writer/reader file/block lifecycle
- spill max file size rolling support
- radix merge stream and merge tree integration
- reader retained-buffer/cache lifetime model

Rewrite:

- writer row append path to use the new `serializeRow`
- writer row sizing/flush thresholds to use calculated serialized row size
  without a stored row header
- reader row scanning to use `deserializeRow` return cursor instead of
  `RadixSortSpillRowHeader::totalSize`
- fixed-row fast path, if kept, to use fixed disk row width without a row header

The spill reader must still output runtime-layout-compatible key rows to the
radix merge tree. In particular:

- variable key data pointer must be restored before compare/decode helpers read
  the key
- key payload pointer should be restored when the runtime key layout has one,
  even though output also carries separate payload pointers
- payload pointer array passed to radix merge/output should point at restored
  runtime payload rows

### RadixSortSpillRowTest.cpp

Rewrite tests that currently assert the old format.

Remove expectations for:

- per-row header
- row-relative key heap offsets
- row-relative payload variable offsets
- total row size stored in the row

Add expectations for:

- no row header
- variable key spill part stops before data pointer
- fixed key with payload spill part stops before payload pointer
- copied variable key size field is preserved exactly
- key heap bytes are written immediately after key record spill part
- payload fixed slot offsets match runtime payload fixed offsets
- non-inline string and complex payload pointer bytes are zeroed on disk
- inline string slots remain unchanged
- payload heap bytes are appended in variable payload column order
- deserialize restores key data pointer, key payload pointer, and payload
  variable pointers
- disk row cursor advances by computed row length, not stored row length

## Non-Goals

- Do not reintroduce the abandoned legacy `SpillFile` serde interface refactor.
- Do not add `ContainerRowBasedWriteContext`.
- Do not add the reverted `updateFileAndAppendStats_` style logic.
- Do not change legacy order-by spill file semantics.
- Do not touch unrelated benchmark edits.

## Risks And Open Points

Fixed-size key heap fast path requires reliable metadata about fixed encoded key
heap size. `RadixSortKeyLayout` alone may not be enough if nullable fixed fields
can make encoded size vary. A conservative first implementation can use the
variable-size heap path for variable key records and add a fixed-size heap fast
path only when the codec/run metadata proves the size is constant.

Payload heap one-copy serialization relies on the current radix
`PayloadRowWriter` invariant that variable payload bytes for one row are emitted
in variable-column order into one contiguous row-local heap region. That is true
for radix-produced payload rows. If future producers write payload rows
differently, the serde helper should either keep a debug-only continuity check
or fall back to per-field copying in the non-contiguous case.

Even without a stored row size, the reader still needs to know where the next
disk row starts inside a block. It should compute this by consuming the four
regions in order using metadata plus row value size fields.
