<!--
Copyright (c) ByteDance Ltd. and/or its affiliates.
SPDX-License-Identifier: Apache-2.0
-->

# Temporal values in the Arrow bridge

`ArrowOptions` defaults to standard Arrow timestamps. `timestampUnit` selects
seconds, milliseconds, microseconds, or nanoseconds, and `timestampTimeZone`
supplies the schema annotation. The bridge does not apply a timezone conversion.
Values outside the selected signed 64-bit unit range raise an error, including
when `exportToArrowIPC` is enabled.

## Full-range timestamps

Set `timestampEncoding = TimestampEncoding::kSecondsNanos` to preserve the full
Bolt `Timestamp` range and nanosecond fraction:

| Arrow schema property | Value |
| --- | --- |
| Storage format | `w:16` (`FixedSizeBinary(16)`) |
| `ARROW:extension:name` | `bolt.timestamp` |
| `ARROW:extension:metadata` | `1` (encoding version) |

Each non-null value has two little-endian fields:

| Byte offset | Type | Meaning |
| --- | --- | --- |
| 0 | `int64` | Seconds, in `Timestamp::kMinSeconds..Timestamp::kMaxSeconds` |
| 8 | `uint64` | Nanoseconds within the second, in `0..999999999` |

For example, one nanosecond before the epoch is `(-1, 999999999)`. The layout is
defined independently of the C++ object representation. Imports recognize this
extension from the schema and validate its version and values. Unannotated
fixed-size binary is not interpreted as a timestamp.

`timestampUnit` and `timestampTimeZone` do not apply to this encoding. Callers
retain logical precision and timezone semantics in their own schema. The bridge
does not truncate values to a logical precision. A consumer needs support for the
extension before enabling it. Enabling the option changes the Arrow schema and
payload; persisted Arrow data needs a reader that recognizes the extension. The
`Timestamp` C++ value layout is unchanged.

`ArrowVectorSerde` accepts the encoding through
`ArrowSerdeOptions::arrowOptions`. Its standard timestamp unit continues to
follow `useLosslessTimestamp`: microseconds by default, nanoseconds when enabled.
C++ callers must be rebuilt with the updated `ArrowOptions` definition.

## Time of day

Standard Arrow `Time32` and `Time64` values must fall within a single day.
`timeImportMode = TimeImportMode::kPreserveUnits`, the default, imports seconds
and milliseconds as `INTEGER`, and microseconds and nanoseconds as `BIGINT`,
preserving the input unit and value. In particular, `ttn` is time of day in
nanoseconds; a timestamp without a timezone uses `tsn:`.

`TimeImportMode::kMillisOfDay` converts every TIME unit to `INTEGER` milliseconds
since midnight. Microsecond or nanosecond values with a sub-millisecond remainder
raise an error. The option applies recursively to schema and array imports.
Use the same options with the schema-only `importFromArrow` overload.

The returned integer types do not retain the TIME annotation. Exporting them
produces ordinary Arrow integers; callers retain the logical TIME schema.

## Ownership

Temporal conversions allocate only the leaves that need conversion. Unchanged
values, validity bitmaps, and container offsets retain their existing buffer
ownership. Viewer imports borrow these buffers. Owner imports consume the input
release callbacks and release buffers when the last reference is gone, or during
cleanup if conversion fails. Nonzero `ArrowArray.offset` remains unsupported.
