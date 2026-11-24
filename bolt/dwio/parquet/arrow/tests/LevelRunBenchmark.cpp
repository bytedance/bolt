/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Benchmarks complete Parquet writes and level generation and encoding.
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "arrow/io/memory.h"
#include "arrow/util/thread_pool.h"
#include "bolt/dwio/parquet/arrow/Metadata.h"
#include "bolt/dwio/parquet/arrow/Writer.h"
#include "bolt/dwio/parquet/arrow/tests/LevelRunTestUtil.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

DEFINE_bool(bolt_benchmark_level_runs, true, "Enable run-aware level encoding");

namespace {
namespace pa = bytedance::bolt::parquet::arrow;
constexpr int kRowGroups = 4;

std::shared_ptr<::arrow::Array> makeStrings(
    int64_t rows,
    ::arrow::MemoryPool* pool) {
  ::arrow::StringBuilder strings(pool);
  int64_t value = 0;
  for (int64_t row = 0; row < rows; ++row) {
    if (row % 512 >= 256) {
      PARQUET_THROW_NOT_OK(strings.AppendNull());
    } else {
      auto text = std::to_string(value++ % 1024);
      text.resize(64, 'x');
      PARQUET_THROW_NOT_OK(strings.Append(text));
    }
  }
  return strings.Finish().ValueOrDie();
}

std::shared_ptr<::arrow::RecordBatch> makeBatch(
    const std::string& pattern,
    ::arrow::MemoryPool* pool) {
  const int64_t rows = pattern == "long_list" || pattern == "four_lists" ? 256
      : pattern == "short_list" || pattern == "mixed_dict_zstd"          ? 16384
      : pattern == "small"                                               ? 15
                           : 65536;
  auto integers = [&](const std::string& shape) {
    return pa::test::MakeLevelRunInput(shape, rows, pool);
  };
  ::arrow::ArrayVector arrays;
  std::vector<std::string> names;
  if (pattern == "varchar_dict_zstd") {
    arrays = {makeStrings(rows, pool)};
    names = {"value"};
  } else if (pattern == "mixed_dict_zstd") {
    arrays = {
        integers("blocked_null"),
        makeStrings(rows, pool),
        integers("short_list")};
    names = {"integer", "text", "items"};
  } else if (pattern == "four_lists") {
    arrays = {
        integers("long_list"),
        integers("long_list"),
        integers("short_list"),
        integers("short_list")};
    names = {"long_a", "long_b", "short_a", "short_b"};
  } else {
    arrays = {integers(pattern)};
    names = {"value"};
  }
  ::arrow::FieldVector fields;
  for (size_t i = 0; i < arrays.size(); ++i) {
    fields.push_back(
        ::arrow::field(names[i], arrays[i]->type(), pattern != "required"));
  }
  return ::arrow::RecordBatch::Make(
      ::arrow::schema(std::move(fields)), rows, std::move(arrays));
}

// Each iteration writes one complete file. Select cases with --bm_regex.
void writeFile(uint32_t iterations, const std::string& pattern, int threads) {
  folly::BenchmarkSuspender suspender;
  auto* pool = ::arrow::default_memory_pool();
  auto batch = makeBatch(pattern, pool);
  auto executor = ::arrow::internal::ThreadPool::Make(threads).ValueOrDie();
  auto properties =
      pa::ArrowWriterProperties::Builder()
          .set_enable_level_run_optimization(FLAGS_bolt_benchmark_level_runs)
          ->disable_compliant_nested_types()
          ->set_use_threads(threads > 1)
          ->set_executor(executor.get())
          ->build();
  pa::WriterProperties::Builder builder;
  builder.memory_pool(pool)
      ->data_page_version(pa::ParquetDataPageVersion::V1)
      ->data_pagesize(1024 * 1024)
      ->write_batch_size(1024)
      ->max_row_group_length(batch->num_rows())
      ->dictionary_pagesize_limit(1024 * 1024);
  if (pattern == "varchar_dict_zstd" || pattern == "mixed_dict_zstd") {
    builder.compression(pa::Compression::ZSTD)->compression_level(3);
  } else {
    builder.disable_dictionary();
  }
  auto fileProperties = builder.build();
  auto write = [&] {
    auto sink =
        ::arrow::io::BufferOutputStream::Create(1024, pool).ValueOrDie();
    auto writer = pa::arrow::FileWriter::Open(
                      *batch->schema(), pool, sink, fileProperties, properties)
                      .ValueOrDie();
    for (int group = 0; group < kRowGroups; ++group) {
      PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    PARQUET_THROW_NOT_OK(writer->Close());
    return sink->Finish().ValueOrDie();
  };
  // Validate independently before timing, including the actual row group count.
  {
    auto source = std::make_shared<::arrow::io::BufferReader>(write());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(::parquet::arrow::OpenFile(source, pool, &reader));
    if (reader->parquet_reader()->metadata()->num_row_groups() != kRowGroups) {
      throw pa::ParquetException("Unexpected benchmark row group count");
    }
    std::shared_ptr<::arrow::Table> decoded;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&decoded));
    auto expected = ::arrow::Table::FromRecordBatches(
                        std::vector<std::shared_ptr<::arrow::RecordBatch>>(
                            kRowGroups, batch))
                        .ValueOrDie();
    if (!decoded->Equals(*expected, false)) {
      throw pa::ParquetException("Decoded data differs from benchmark input");
    }
  }
  suspender.dismiss();
  while (iterations--) {
    folly::doNotOptimizeAway(write()->size());
  }
  suspender.rehire();
}

void generateLevels(uint32_t iterations, const std::string& pattern) {
  folly::BenchmarkSuspender suspender;
  auto* pool = ::arrow::default_memory_pool();
  auto batch = makeBatch(pattern, pool);
  std::vector<std::unique_ptr<pa::arrow::MultipathLevelBuilder>> builders;
  for (int column = 0; column < batch->num_columns(); ++column) {
    builders.push_back(
        pa::arrow::MultipathLevelBuilder::Make(
            *batch->column(column), batch->schema()->field(column)->nullable())
            .ValueOrDie());
  }
  auto properties =
      pa::ArrowWriterProperties::Builder()
          .set_enable_level_run_optimization(FLAGS_bolt_benchmark_level_runs)
          ->build();
  pa::ArrowWriteContext context(pool, properties.get());
  auto defBuffer = pa::AllocateBuffer(pool);
  auto repBuffer = pa::AllocateBuffer(pool);
  pa::IncrementalLevelRleEncoder def(defBuffer.get()), rep(repBuffer.get());
  int64_t bytes = 0;
  suspender.dismiss();
  while (iterations--) {
    for (int column = 0; column < batch->num_columns(); ++column) {
      const int16_t maxDef =
          batch->column(column)->type_id() == ::arrow::Type::LIST ? 3
          : batch->schema()->field(column)->nullable()            ? 1
                                                                  : 0;
      bytes += pa::test::GenerateAndEncodeLevels(
          *builders[column], context, def, rep, maxDef);
    }
  }
  folly::doNotOptimizeAway(bytes);
  suspender.rehire();
}
} // namespace

#define LEVEL_RUN_BENCHMARKS(name)                  \
  BENCHMARK_NAMED_PARAM(writeFile, name, #name, 1); \
  BENCHMARK_NAMED_PARAM(generateLevels, name, #name)

LEVEL_RUN_BENCHMARKS(all_valid);
LEVEL_RUN_BENCHMARKS(blocked_null);
LEVEL_RUN_BENCHMARKS(alternating_null);
LEVEL_RUN_BENCHMARKS(random_null);
LEVEL_RUN_BENCHMARKS(short_list);
LEVEL_RUN_BENCHMARKS(long_list);
LEVEL_RUN_BENCHMARKS(varchar_dict_zstd);
LEVEL_RUN_BENCHMARKS(mixed_dict_zstd);
LEVEL_RUN_BENCHMARKS(all_null);
LEVEL_RUN_BENCHMARKS(required);
LEVEL_RUN_BENCHMARKS(small);
BENCHMARK_NAMED_PARAM(writeFile, four_lists_serial, "four_lists", 1);
BENCHMARK_NAMED_PARAM(writeFile, four_lists_parallel, "four_lists", 4);
BENCHMARK_NAMED_PARAM(generateLevels, four_lists, "four_lists");

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
