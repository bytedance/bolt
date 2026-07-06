---
layout: post
title: "DenseRow: A Compact Row Format for Shuffle"
date: 2026-07-03
author: "Zhang Xiaofeng"
parent: Blog
nav_order: 12
---

Row Based Shuffle is the shuffle path that materializes columnar `RowVector` data into row-oriented bytes before partitioning and exchanging data. Instead of moving vectors directly, it writes each row into a compact binary representation, groups rows by partition, compresses the partition payload, and later decodes the rows back into columnar vectors on the read side.

This conversion sits on a hot path. At large scale, every extra byte in the row body becomes extra memory traffic, compression input, transfer payload, and serialization work.

CompactRow already improves on UnsafeRow by removing fixed 8-byte field slots and alignment padding. However, it still keeps row/array null bits, fixed 4-byte lengths or sizes, and nested structure metadata such as offsets or serialized sizes for complex elements. For shuffle, this is often more information than necessary: both writer and reader already know the `RowType`, and the outer shuffle layer already frames each row.

DenseRow is designed to remove that remaining row-body overhead. It fuses nulls into type-specific encodings, uses varints for integers, lengths, and cardinalities, and lays out nested data in a level-hoisted form. The result is a compact, schema-driven wire format for internal shuffle transport rather than a general-purpose row object format.

The implementation was introduced in [bytedance/bolt#637](https://github.com/bytedance/bolt/pull/637).

## Core Design

DenseRow is a type-driven format:

- A row blob is the concatenation of fields in schema order.
- Variant integer encoding is the foundation: varints encode integers, lengths, cardinalities, and null markers.
- No field tags are written.
- No top-level row marker is written.
- The caller owns row framing and guarantees there are no top-level null rows.

```text
row_blob := encode(field_0) encode(field_1) ... encode(field_n)
```

Nulls are encoded as part of each type:

| Type | Encoding |
| --- | --- |
| Integer / timestamp | `0x00` means null; `INT64_MIN` has a special encoding; other values use `varint(zigzag(adjust(v)))`, where `adjust(v) = v > 0 ? v : v - 1` |
| BOOLEAN | `0 = null`, `1 = false`, `2 = true` |
| REAL / DOUBLE | Little-endian bits; a reserved sentinel means null, with reversible handling for sentinel collisions |
| VARCHAR / VARBINARY | `varint(len + 1)`, `0` means null, followed by payload bytes |
| ARRAY / MAP | `varint(cardinality + 1)`, `0` means null, empty containers encode as `1` |
| ROW | `0 = null`, `1 = present`, then recursively encoded fields |
| HUGEINT | 128-bit zigzag, split into low 64 bits and high 64 bits |

Complex types use a level-hoisted layout:

- At each nesting level, structural bytes are written first, followed by child data.
- `ARRAY<ARRAY<BIGINT>>` writes the outer cardinality, then all inner cardinalities, then the BIGINT elements.
- `MAP` always writes the keys segment before the values segment.
- At multi-position levels, `VARCHAR` / `VARBINARY` also writes all lengths before all payloads.

## Format Comparison

Use the same logical row to compare `UnsafeRow`, `CompactRow`, and `DenseRow`:

```text
ROW(
  BIGINT = 1,
  VARCHAR = "ab",
  ARRAY<INT> = [10, 20, null]
)
```

```text
UnsafeRow
---------
┌──────────────┬──────────────┬────────────────────┬───────────────────┬──────────────────────┬──────────────────────────────────────┐
│ row null set │ BIGINT slot  │ VARCHAR slot       │ ARRAY slot        │ VARCHAR payload      │ ARRAY payload                         │
│ 8B aligned   │ 8B           │ 8B offset + size   │ 8B offset + size  │ "ab" + padding       │ 8B size + null bits + fixed elements  │
└──────────────┴──────────────┴────────────────────┴───────────────────┴──────────────────────┴──────────────────────────────────────┘

CompactRow
----------
┌───────────────┬──────────────┬────────────────────────┬──────────────────────────────────────────────┐
│ row null bits │ BIGINT value │ VARCHAR                │ ARRAY                                        │
│ nbytes(3)     │ 8B           │ 4B length + "ab"       │ 4B size + element null bits + int32 values  │
└───────────────┴──────────────┴────────────────────────┴──────────────────────────────────────────────┘

DenseRow
--------
┌────────────────────────┬────────────────────────────┬──────────────────────────┬──────────────────────────────┐
│ BIGINT                 │ VARCHAR                    │ ARRAY                    │ ARRAY elements               │
│ encoded integer marker │ varint(len + 1) + "ab"     │ varint(cardinality + 1)  │ encoded INT markers          │
└────────────────────────┴────────────────────────────┴──────────────────────────┴──────────────────────────────┘
```

For the same example row, excluding the outer shuffle row-size header and comparing only the row body:

```text
UnsafeRow
  row null set                         8B
  3 field slots                    3 * 8B = 24B
  VARCHAR payload "ab", 8B aligned     8B
  ARRAY payload:
    element count                      8B
    element null bits, 8B aligned      8B
    3 int32 element slots          3 * 4B = 12B
    top-level variable padding         4B
  total                               72B

CompactRow
  row null bits                        1B
  BIGINT value                         8B
  VARCHAR length + payload          4B + 2B = 6B
  ARRAY:
    element count                      4B
    element null bits                  1B
    3 int32 values                 3 * 4B = 12B
  total                               32B

DenseRow
  BIGINT marker                        1B
  VARCHAR len+1 marker + payload    1B + 2B = 3B
  ARRAY cardinality+1 marker           1B
  3 INT element markers            3 * 1B = 3B
  total                                8B
```

| Format | Example row body | Key properties |
| --- | ---: | --- |
| `UnsafeRow` | 72B | Spark-compatible; has a null set, 8-byte slots, offset/size words, and 8-byte alignment |
| `CompactRow` | 32B | Removes 8-byte slots and alignment, but keeps row/array null bits, lengths, and sizes |
| `DenseRow` | 8B | No slots, no padding, no separate null bitmap; nulls are fused into type encodings |

## Encoding Implementation

DenseRow encoding can be understood as two passes plus type-based routing:

1. **Size pass**: compute the final encoded size for each row; complex types also build a nested slot plan.
2. **Write pass**: reuse the same slot plan to write bytes, so structure traversal matches the size pass.

Top-level fields are routed by type:

- Primitive fields use the scalar column fast path.
- `ARRAY` / `MAP` / `ROW` use the general column-batch path.
- All-scalar rows use an all-scalar fast path and skip complex-type scaffolding such as `SlotView` / `ColumnPlan`.

Key optimization points:

- **SIMD**: mainly used in the size pass to compute integer varint encoded sizes in batches.
- **BMI2**: used by varint read/write on x86 via `pdep` / `pext` for packing and unpacking payload bits.
- **Short fast path**: common 1- to 4-byte varints first use a hand-unrolled path, then fall back to BMI2/scalar if needed.
- **Mapping fast path**: identity/no-null integer columns can compute sizes over contiguous memory; constant-mapped integer columns compute the size once and apply it to all rows.

Decoding is driven by `RowType` and consumes each row blob in field order. At the end of each row, DenseRow checks that the cursor reached the exact end of the row blob, which catches schema or row-framing mismatches.

## Real-World Workload Performance Comparison

In RowBased Shuffle dual-run tests, 36 valid jobs were used for aggregate comparison between CompactRow and DenseRow.

At the end-to-end job level, enabling DenseRow reduced CPU resource usage by about 17.5% and execution time by about 21.0%.

In high-gain stages, the smaller row body benefits C2R conversion, compression, shuffle writing, and read-side deserialize/decompress work:

| Representative workload | Shuffle writer | C2R | Compress | Data size | Shuffle bytes | Deserialize |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Workload A | 2.55x | 1.82x | 2.43x | 5.98x | 1.00x | 3.87x |
| Workload B | 2.17x | 2.27x | 1.55x | 4.83x | 1.01x | 4.92x |
| Workload C | 2.59x | 2.25x | 1.53x | 3.43x | 1.05x | 4.91x |
