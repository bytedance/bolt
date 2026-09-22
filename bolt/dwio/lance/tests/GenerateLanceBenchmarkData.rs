// Copyright (c) ByteDance Ltd. and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeListBuilder, Float32Builder, Int32Builder, LargeListBuilder, ListBuilder, MapBuilder,
    StringBuilder,
};
use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use half::f16;
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::compression_config::{CompressionFieldParams, CompressionParams};
use lance_file::versions;
use lance_file::writer::FileWriterOptions;
use lance_io::object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const DEFAULT_ROWS: u64 = 1_048_576;
const DEFAULT_BATCH_ROWS: usize = 65_536;
const FILTER_MIN: i64 = 0;
const FILTER_MAX: i64 = 99;
const FILTER_CARDINALITY: u64 = 10_000;

fn nullable(row: u64, modulus: u64) -> bool {
    row % modulus != 0
}

fn make_batch(start: u64, rows: usize) -> RecordBatch {
    let range = || start..start + rows as u64;
    let row_id = Arc::new(Int64Array::from_iter_values(range().map(|row| row as i64))) as ArrayRef;
    let filter_key = Arc::new(Int64Array::from_iter_values(
        range().map(|row| (row % FILTER_CARDINALITY) as i64),
    )) as ArrayRef;
    let bool_value = Arc::new(BooleanArray::from_iter(
        range().map(|row| nullable(row, 97).then_some(row % 2 == 0)),
    )) as ArrayRef;
    let i8_value =
        Arc::new(Int8Array::from_iter(range().map(|row| {
            nullable(row, 89).then_some((row as i8).wrapping_sub(63))
        }))) as ArrayRef;
    let u8_value = Arc::new(UInt8Array::from_iter(
        range().map(|row| nullable(row, 83).then_some(row as u8)),
    )) as ArrayRef;
    let i16_value =
        Arc::new(Int16Array::from_iter(range().map(|row| {
            nullable(row, 79).then_some((row as i16).wrapping_sub(8_000))
        }))) as ArrayRef;
    let u16_value = Arc::new(UInt16Array::from_iter(
        range().map(|row| nullable(row, 73).then_some(row as u16)),
    )) as ArrayRef;
    let i32_value = Arc::new(Int32Array::from_iter(
        range().map(|row| nullable(row, 71).then_some(row as i32 - 1_000_000)),
    )) as ArrayRef;
    let u32_value = Arc::new(UInt32Array::from_iter(
        range().map(|row| nullable(row, 67).then_some((row * 17) as u32)),
    )) as ArrayRef;
    let i64_value =
        Arc::new(Int64Array::from_iter(range().map(|row| {
            nullable(row, 61).then_some(row as i64 * 101 - 7_000_000)
        }))) as ArrayRef;
    let u64_value = Arc::new(UInt64Array::from_iter(
        range().map(|row| nullable(row, 59).then_some(row * 1_000_003)),
    )) as ArrayRef;
    let f16_value = Arc::new(Float16Array::from_iter(range().map(|row| {
        nullable(row, 53).then_some(f16::from_f32((row % 4096) as f32 * 0.25 - 512.0))
    }))) as ArrayRef;
    let f32_value =
        Arc::new(Float32Array::from_iter(range().map(|row| {
            nullable(row, 47).then_some(row as f32 * 0.125 - 1_000.0)
        }))) as ArrayRef;
    let f64_value =
        Arc::new(Float64Array::from_iter(range().map(|row| {
            nullable(row, 43).then_some(row as f64 * 0.000_125 + 10.0)
        }))) as ArrayRef;
    let string_value = Arc::new(StringArray::from_iter(range().map(|row| {
        nullable(row, 41).then(|| format!("string-{row:012}-{}", "x".repeat((row % 32) as usize)))
    }))) as ArrayRef;
    let large_string = Arc::new(LargeStringArray::from_iter(range().map(|row| {
        nullable(row, 37).then(|| format!("large-string-{row:012}-{}", "y".repeat(48)))
    }))) as ArrayRef;
    let binary = Arc::new(BinaryArray::from_iter(range().map(|row| {
        nullable(row, 31).then(|| format!("binary-{row:012}").into_bytes())
    }))) as ArrayRef;
    let large_binary = Arc::new(LargeBinaryArray::from_iter(range().map(|row| {
        nullable(row, 29).then(|| format!("large-binary-{row:012}").into_bytes())
    }))) as ArrayRef;
    let fixed_binary = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            range().map(|row| {
                nullable(row, 23).then_some([
                    row as u8,
                    (row >> 8) as u8,
                    (row >> 16) as u8,
                    (row >> 24) as u8,
                ])
            }),
            4,
        )
        .unwrap(),
    ) as ArrayRef;
    let date32 = Arc::new(Date32Array::from_iter(
        range().map(|row| nullable(row, 19).then_some(row as i32 - 10_000)),
    )) as ArrayRef;
    let date64 =
        Arc::new(Date64Array::from_iter(range().map(|row| {
            nullable(row, 17).then_some((row as i64 - 10_000) * 86_400_000)
        }))) as ArrayRef;
    let time32_s = Arc::new(Time32SecondArray::from_iter(
        range().map(|row| nullable(row, 113).then_some((row % 86_400) as i32)),
    )) as ArrayRef;
    let time32_ms =
        Arc::new(Time32MillisecondArray::from_iter(range().map(|row| {
            nullable(row, 109).then_some((row % 86_400_000) as i32)
        }))) as ArrayRef;
    let time64_us =
        Arc::new(Time64MicrosecondArray::from_iter(range().map(|row| {
            nullable(row, 107).then_some((row % 86_400_000) as i64 * 1_000)
        }))) as ArrayRef;
    let time64_ns =
        Arc::new(Time64NanosecondArray::from_iter(range().map(|row| {
            nullable(row, 103).then_some((row % 86_400_000) as i64 * 1_000_000)
        }))) as ArrayRef;
    let timestamp_s = Arc::new(TimestampSecondArray::from_iter(
        range().map(|row| nullable(row, 101).then_some(row as i64 - 500_000)),
    )) as ArrayRef;
    let timestamp_ms = Arc::new(
        TimestampMillisecondArray::from_iter(
            range().map(|row| nullable(row, 127).then_some(row as i64 * 1_001)),
        )
        .with_timezone("UTC"),
    ) as ArrayRef;
    let timestamp_us = Arc::new(
        TimestampMicrosecondArray::from_iter(
            range().map(|row| nullable(row, 131).then_some(row as i64 * 1_000_003)),
        )
        .with_timezone("Asia/Shanghai"),
    ) as ArrayRef;
    let timestamp_ns =
        Arc::new(TimestampNanosecondArray::from_iter(range().map(|row| {
            nullable(row, 137).then_some(row as i64 * 1_000_000_007)
        }))) as ArrayRef;
    let duration_s = Arc::new(DurationSecondArray::from_iter(
        range().map(|row| nullable(row, 139).then_some(row as i64 - 500_000)),
    )) as ArrayRef;
    let duration_ms = Arc::new(DurationMillisecondArray::from_iter(
        range().map(|row| nullable(row, 149).then_some(row as i64 - 500_000)),
    )) as ArrayRef;
    let duration_us =
        Arc::new(DurationMicrosecondArray::from_iter(range().map(|row| {
            nullable(row, 151).then_some((row as i64 - 500_000) * 1_000)
        }))) as ArrayRef;
    let duration_ns =
        Arc::new(DurationNanosecondArray::from_iter(range().map(|row| {
            nullable(row, 157).then_some((row as i64 - 500_000) * 1_000_000)
        }))) as ArrayRef;
    let decimal = Arc::new(
        Decimal128Array::from_iter(
            range().map(|row| nullable(row, 163).then_some((row as i128 - 500_000) * 100 + 7)),
        )
        .with_precision_and_scale(20, 2)
        .unwrap(),
    ) as ArrayRef;
    let all_null = Arc::new(NullArray::new(rows)) as ArrayRef;
    let bfloat = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            range().map(|row| nullable(row, 167).then_some((row as u16).to_le_bytes())),
            2,
        )
        .unwrap(),
    ) as ArrayRef;
    let json = Arc::new(LargeBinaryArray::from_iter(range().map(|row| {
        nullable(row, 173).then(|| format!(r#"{{"row":{row}}}"#).into_bytes())
    }))) as ArrayRef;

    let dictionary_values = Arc::new(StringArray::from(vec![
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
    ])) as ArrayRef;
    let dictionary_keys =
        Int32Array::from_iter(range().map(|row| nullable(row, 179).then_some((row % 8) as i32)));
    let dictionary = Arc::new(
        DictionaryArray::<Int32Type>::try_new(dictionary_keys, dictionary_values).unwrap(),
    ) as ArrayRef;

    let mut list_builder = ListBuilder::new(Int32Builder::new());
    let mut large_list_builder = LargeListBuilder::new(StringBuilder::new());
    let mut fixed_list_builder = FixedSizeListBuilder::new(Float32Builder::new(), 3);
    let mut map_builder = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    for row in range() {
        if nullable(row, 181) {
            for item in 0..row % 5 {
                if item == 1 && row % 11 == 0 {
                    list_builder.values().append_null();
                } else {
                    list_builder.values().append_value((row * 7 + item) as i32);
                }
            }
            list_builder.append(true);
        } else {
            list_builder.append_null();
        }
        if nullable(row, 191) {
            for item in 0..row % 3 {
                large_list_builder
                    .values()
                    .append_value(format!("item-{row}-{item}"));
            }
            large_list_builder.append(true);
        } else {
            large_list_builder.append_null();
        }
        for item in 0..3 {
            if item == 1 && row % 13 == 0 {
                fixed_list_builder.values().append_null();
            } else {
                fixed_list_builder
                    .values()
                    .append_value(row as f32 * 0.5 + item as f32);
            }
        }
        fixed_list_builder.append(nullable(row, 193));
        if nullable(row, 197) {
            for item in 0..row % 4 {
                map_builder.keys().append_value((row * 5 + item) as i32);
                if item == 2 && row % 17 == 0 {
                    map_builder.values().append_null();
                } else {
                    map_builder
                        .values()
                        .append_value(format!("value-{row}-{item}"));
                }
            }
            map_builder.append(true).unwrap();
        } else {
            map_builder.append(false).unwrap();
        }
    }
    let list = Arc::new(list_builder.finish()) as ArrayRef;
    let large_list = Arc::new(large_list_builder.finish()) as ArrayRef;
    let fixed_list = Arc::new(fixed_list_builder.finish()) as ArrayRef;
    let map = Arc::new(map_builder.finish()) as ArrayRef;

    let struct_fields = Fields::from(vec![
        Field::new("nested_i64", DataType::Int64, true),
        Field::new("nested_string", DataType::Utf8, true),
    ]);
    let struct_value = Arc::new(StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(Int64Array::from_iter(
                range().map(|row| nullable(row, 199).then_some(row as i64 * 37)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(
                range().map(|row| nullable(row, 211).then(|| format!("nested-{row}"))),
            )) as ArrayRef,
        ],
        Some(NullBuffer::from_iter(range().map(|row| nullable(row, 223)))),
    )) as ArrayRef;

    let bfloat_metadata = HashMap::from([(
        "ARROW:extension:name".to_string(),
        "lance.bfloat16".to_string(),
    )]);
    let json_metadata =
        HashMap::from([("ARROW:extension:name".to_string(), "lance.json".to_string())]);
    let fields = vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("filter_key", DataType::Int64, false),
        Field::new("bool_value", DataType::Boolean, true),
        Field::new("i8_value", DataType::Int8, true),
        Field::new("u8_value", DataType::UInt8, true),
        Field::new("i16_value", DataType::Int16, true),
        Field::new("u16_value", DataType::UInt16, true),
        Field::new("i32_value", DataType::Int32, true),
        Field::new("u32_value", DataType::UInt32, true),
        Field::new("i64_value", DataType::Int64, true),
        Field::new("u64_value", DataType::UInt64, true),
        Field::new("f16_value", DataType::Float16, true),
        Field::new("f32_value", DataType::Float32, true),
        Field::new("f64_value", DataType::Float64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("large_string", DataType::LargeUtf8, true),
        Field::new("binary", DataType::Binary, true),
        Field::new("large_binary", DataType::LargeBinary, true),
        Field::new("fixed_binary", DataType::FixedSizeBinary(4), true),
        Field::new("date32", DataType::Date32, true),
        Field::new("date64", DataType::Date64, true),
        Field::new("time32_s", DataType::Time32(TimeUnit::Second), true),
        Field::new("time32_ms", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("time64_us", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new(
            "timestamp_s",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_ms",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "timestamp_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            true,
        ),
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("duration_s", DataType::Duration(TimeUnit::Second), true),
        Field::new(
            "duration_ms",
            DataType::Duration(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "duration_us",
            DataType::Duration(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "duration_ns",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new("decimal", DataType::Decimal128(20, 2), true),
        Field::new("all_null", DataType::Null, true),
        Field::new("bfloat", DataType::FixedSizeBinary(2), true).with_metadata(bfloat_metadata),
        Field::new("json", DataType::LargeBinary, true).with_metadata(json_metadata),
        Field::new("dictionary", dictionary.data_type().clone(), true),
        Field::new("list", list.data_type().clone(), true),
        Field::new("large_list", large_list.data_type().clone(), true),
        Field::new("fixed_list", fixed_list.data_type().clone(), true),
        Field::new("map", map.data_type().clone(), true),
        Field::new("struct_value", DataType::Struct(struct_fields), true),
    ];
    let columns = vec![
        row_id,
        filter_key,
        bool_value,
        i8_value,
        u8_value,
        i16_value,
        u16_value,
        i32_value,
        u32_value,
        i64_value,
        u64_value,
        f16_value,
        f32_value,
        f64_value,
        string_value,
        large_string,
        binary,
        large_binary,
        fixed_binary,
        date32,
        date64,
        time32_s,
        time32_ms,
        time64_us,
        time64_ns,
        timestamp_s,
        timestamp_ms,
        timestamp_us,
        timestamp_ns,
        duration_s,
        duration_ms,
        duration_us,
        duration_ns,
        decimal,
        all_null,
        bfloat,
        json,
        dictionary,
        list,
        large_list,
        fixed_list,
        map,
        struct_value,
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn parquet_compatible_batch(batch: &RecordBatch) -> RecordBatch {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let (data_type, column): (DataType, ArrayRef) = match field.name().as_str() {
            // Bolt exposes these Lance logical types using the wider physical
            // scalar shown here.  Arrow's Parquet writer otherwise emits
            // annotations such as TIME_MILLIS that Bolt's Parquet reader does
            // not support.  Keep the values equivalent after Bolt conversion.
            "u8_value" => (
                DataType::Int16,
                Arc::new(Int16Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i16::from)),
                )),
            ),
            "u16_value" => (
                DataType::Int32,
                Arc::new(Int32Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<UInt16Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i32::from)),
                )),
            ),
            "u32_value" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i64::from)),
                )),
            ),
            "u64_value" => {
                let values = Decimal128Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i128::from)),
                )
                .with_precision_and_scale(20, 0)
                .unwrap();
                (DataType::Decimal128(20, 0), Arc::new(values))
            }
            "f16_value" => (
                DataType::Float32,
                Arc::new(Float32Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Float16Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(f16::to_f32)),
                )),
            ),
            "fixed_binary" | "bfloat" => (
                DataType::Binary,
                Arc::new(BinaryArray::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap()
                        .iter(),
                )),
            ),
            "date64" => (
                DataType::Date32,
                Arc::new(Date32Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Date64Array>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(|value| (value / 86_400_000) as i32)),
                )),
            ),
            "time32_s" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i64::from)),
                )),
            ),
            "time32_ms" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(i64::from)),
                )),
            ),
            "time64_us" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap()
                        .iter(),
                )),
            ),
            "time64_ns" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .unwrap()
                        .iter(),
                )),
            ),
            "duration_s" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<DurationSecondArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(|value| value * 1_000)),
                )),
            ),
            "duration_ms" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<DurationMillisecondArray>()
                        .unwrap()
                        .iter(),
                )),
            ),
            "duration_us" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<DurationMicrosecondArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(|value| value / 1_000)),
                )),
            ),
            "duration_ns" => (
                DataType::Int64,
                Arc::new(Int64Array::from_iter(
                    column
                        .as_any()
                        .downcast_ref::<DurationNanosecondArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.map(|value| value / 1_000_000)),
                )),
            ),
            "all_null" => (
                DataType::Int32,
                Arc::new(Int32Array::new_null(column.len())),
            ),
            _ => (field.data_type().clone(), column.clone()),
        };
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_metadata(HashMap::new()),
        );
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn filter_expectations(rows: u64) -> (u64, u64) {
    let mut output_rows = 0;
    let mut checksum = 0;
    for row in 0..rows {
        let key = (row % FILTER_CARDINALITY) as i64;
        if (FILTER_MIN..=FILTER_MAX).contains(&key) {
            output_rows += 1;
            checksum += row;
        }
    }
    (output_rows, checksum)
}

async fn generate(output: &Path, rows: u64, batch_rows: usize) -> anyhow::Result<()> {
    std::fs::create_dir_all(output)?;
    let lance_path = output.join("all_types_v2_2.lance");
    let parquet_path = output.join("all_types_zstd.parquet");
    let first_rows = usize::try_from(rows.min(batch_rows as u64))?;
    let first = make_batch(0, first_rows);

    let mut lance_schema = LanceSchema::try_from(first.schema().as_ref())?;
    lance_schema.set_dictionary(&first)?;
    let store = ObjectStore::local();
    File::create(&lance_path)?;
    let object_path = ObjectPath::from_filesystem_path(&lance_path)?;
    let mut compression = CompressionParams::new();
    compression.columns.insert(
        "*".to_string(),
        CompressionFieldParams {
            compression: Some("zstd".to_string()),
            compression_level: Some(3),
            ..Default::default()
        },
    );
    let mut lance_writer = versions::v2_2::create_writer_with_compression(
        store.create(&object_path).await?,
        lance_schema,
        FileWriterOptions {
            data_cache_bytes: Some(64 * 1024 * 1024),
            max_page_bytes: Some(8 * 1024 * 1024),
            keep_original_array: Some(true),
        },
        compression,
    )?;
    let parquet_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(batch_rows))
        .build();
    let mut parquet_writer = ArrowWriter::try_new(
        File::create(&parquet_path)?,
        parquet_compatible_batch(&first).schema(),
        Some(parquet_properties),
    )?;

    let mut start = 0_u64;
    while start < rows {
        let current_rows = usize::try_from((rows - start).min(batch_rows as u64))?;
        let batch = if start == 0 && current_rows == first_rows {
            first.clone()
        } else {
            make_batch(start, current_rows)
        };
        lance_writer.write_batch(&batch).await?;
        parquet_writer.write(&parquet_compatible_batch(&batch))?;
        start += current_rows as u64;
    }
    let lance_summary = lance_writer.finish().await?;
    parquet_writer.close()?;

    let lance_bytes = std::fs::metadata(&lance_path)?.len();
    let parquet_bytes = std::fs::metadata(&parquet_path)?.len();
    anyhow::ensure!(lance_summary.num_rows == rows, "Lance row count mismatch");
    anyhow::ensure!(
        lance_summary.size_bytes == lance_bytes,
        "Lance size mismatch"
    );
    let (filter_rows, filter_checksum) = filter_expectations(rows);
    let full_checksum = rows.saturating_mul(rows.saturating_sub(1)) / 2;
    let size_ratio = lance_bytes as f64 / parquet_bytes as f64;
    let manifest = format!(
        concat!(
            "{{\n",
            "  \"rows\": {rows},\n",
            "  \"batch_rows\": {batch_rows},\n",
            "  \"column_count\": {column_count},\n",
            "  \"lance_file\": \"{lance_file}\",\n",
            "  \"parquet_file\": \"{parquet_file}\",\n",
            "  \"lance_bytes\": {lance_bytes},\n",
            "  \"parquet_bytes\": {parquet_bytes},\n",
            "  \"lance_to_parquet_size_ratio\": {size_ratio:.6},\n",
            "  \"full_scan_checksum\": {full_checksum},\n",
            "  \"checksum_column\": \"row_id\",\n",
            "  \"filter_column\": \"filter_key\",\n",
            "  \"filter_min\": {filter_min},\n",
            "  \"filter_max\": {filter_max},\n",
            "  \"filter_output_rows\": {filter_rows},\n",
            "  \"filter_checksum\": {filter_checksum},\n",
            "  \"parquet_logical_type_adaptations\": [\"unsigned integers widened to Bolt signed storage\", \"float16 widened to float32\", \"fixed-size binary stored as binary\", \"date64 converted to date32\", \"time and duration converted to Bolt int64 units\", \"null stored as nullable int32\"]\n",
            "}}\n"
        ),
        rows = rows,
        batch_rows = batch_rows,
        column_count = first.num_columns(),
        lance_file = lance_path.display(),
        parquet_file = parquet_path.display(),
        lance_bytes = lance_bytes,
        parquet_bytes = parquet_bytes,
        size_ratio = size_ratio,
        full_checksum = full_checksum,
        filter_min = FILTER_MIN,
        filter_max = FILTER_MAX,
        filter_rows = filter_rows,
        filter_checksum = filter_checksum,
    );
    std::fs::write(output.join("manifest.json"), &manifest)?;
    print!("{manifest}");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("usage: generator OUTPUT_DIR [ROWS] [BATCH_ROWS]"))?;
    let rows = env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_ROWS);
    let batch_rows = env::args()
        .nth(3)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_BATCH_ROWS);
    anyhow::ensure!(rows > 0, "ROWS must be greater than zero");
    anyhow::ensure!(batch_rows > 0, "BATCH_ROWS must be greater than zero");
    generate(&output, rows, batch_rows).await
}
