/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

// Adapted from Apache Arrow.

#include <numeric>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/testing/gtest_compat.h"

#include "bolt/dwio/parquet/arrow/ColumnWriter.h"
#include "bolt/dwio/parquet/arrow/FileWriter.h"
#include "bolt/dwio/parquet/arrow/Platform.h"
#include "bolt/dwio/parquet/arrow/Types.h"
#include "bolt/dwio/parquet/arrow/Writer.h"
#include "bolt/dwio/parquet/arrow/tests/ColumnReader.h"
#include "bolt/dwio/parquet/arrow/tests/FileReader.h"
#include "bolt/dwio/parquet/arrow/tests/TestUtil.h"
namespace bytedance::bolt::parquet::arrow {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;
using ::testing::ElementsAre;

namespace test {

// PageBufferArena only accounts for dict/data page buffers, while
// parquet_block_size also needs to leave room for encoder-side memory.
constexpr int64_t GetExpectedPageBufferArenaSize(int64_t parquet_block_size) {
  return parquet_block_size > 0 ? parquet_block_size * 4 / 5 : 0;
}

std::pair<int64_t, int64_t> MeasureBufferedSharedScratchAfterClosingColumns(
    ParquetDataPageVersion page_version) {
  constexpr int64_t kNumRows = 4096;
  constexpr int64_t kArenaSize = 1 << 20;

  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->compression(Compression::SNAPPY)
                          ->data_page_version(page_version)
                          ->data_pagesize(1024)
                          ->set_parquet_block_size(kArenaSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col0", Repetition::REQUIRED, Type::INT32),
       PrimitiveNode::Make("col1", Repetition::REQUIRED, Type::INT32)}));

  std::vector<int32_t> values(kNumRows);
  std::iota(values.begin(), values.end(), 0);

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
  auto row_group_writer = file_writer->AppendBufferedRowGroup();

  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(kNumRows, nullptr, nullptr, values.data());
  row_group_writer->column(0)->Close();
  const int64_t scratch_after_first_column =
      row_group_writer->memory_stats().columnScratchAllocatedBytes;

  static_cast<Int32Writer*>(row_group_writer->column(1))
      ->WriteBatch(kNumRows, nullptr, nullptr, values.data());
  row_group_writer->column(1)->Close();
  const int64_t scratch_after_second_column =
      row_group_writer->memory_stats().columnScratchAllocatedBytes;

  row_group_writer->Close();
  file_writer->Close();

  return {
      scratch_after_first_column,
      scratch_after_second_column,
  };
}

template <typename TestType>
class TestSerialize : public PrimitiveTypedTest<TestType> {
 public:
  void SetUp() {
    num_columns_ = 4;
    num_rowgroups_ = 4;
    rows_per_rowgroup_ = 50;
    rows_per_batch_ = 10;
    this->SetUpSchema(Repetition::OPTIONAL, num_columns_);
  }

 protected:
  int num_columns_;
  int num_rowgroups_;
  int rows_per_rowgroup_;
  int rows_per_batch_;

  void FileSerializeTest(Compression::type codec_type) {
    FileSerializeTest(codec_type, codec_type);
  }

  void FileSerializeTest(
      Compression::type codec_type,
      Compression::type expected_codec_type) {
    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    WriterProperties::Builder prop_builder;

    for (int i = 0; i < num_columns_; ++i) {
      prop_builder.compression(this->schema_.Column(i)->name(), codec_type);
    }
    std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, writer_properties);
    this->GenerateData(rows_per_rowgroup_);
    for (int rg = 0; rg < num_rowgroups_ / 2; ++rg) {
      RowGroupWriter* row_group_writer;
      row_group_writer = file_writer->AppendRowGroup();
      for (int col = 0; col < num_columns_; ++col) {
        auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
            row_group_writer->NextColumn());
        column_writer->WriteBatch(
            rows_per_rowgroup_,
            this->def_levels_.data(),
            nullptr,
            this->values_ptr_);
        column_writer->Close();
        // Ensure column() API which is specific to BufferedRowGroup cannot be
        // called
        ASSERT_THROW(row_group_writer->column(col), ParquetException);
      }
      EXPECT_EQ(0, row_group_writer->total_compressed_bytes());
      EXPECT_NE(0, row_group_writer->total_bytes_written());
      EXPECT_NE(0, row_group_writer->total_compressed_bytes_written());
      row_group_writer->Close();
      EXPECT_EQ(0, row_group_writer->total_compressed_bytes());
      EXPECT_NE(0, row_group_writer->total_bytes_written());
      EXPECT_NE(0, row_group_writer->total_compressed_bytes_written());
    }
    // Write half BufferedRowGroups
    for (int rg = 0; rg < num_rowgroups_ / 2; ++rg) {
      RowGroupWriter* row_group_writer;
      row_group_writer = file_writer->AppendBufferedRowGroup();
      for (int batch = 0; batch < (rows_per_rowgroup_ / rows_per_batch_);
           ++batch) {
        for (int col = 0; col < num_columns_; ++col) {
          auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
              row_group_writer->column(col));
          column_writer->WriteBatch(
              rows_per_batch_,
              this->def_levels_.data() + (batch * rows_per_batch_),
              nullptr,
              this->values_ptr_ + (batch * rows_per_batch_));
          // Ensure NextColumn() API which is specific to RowGroup cannot be
          // called
          ASSERT_THROW(row_group_writer->NextColumn(), ParquetException);
        }
      }
      // total_compressed_bytes() may equal to 0 if no dictionary enabled and no
      // buffered values.
      EXPECT_EQ(0, row_group_writer->total_bytes_written());
      EXPECT_EQ(0, row_group_writer->total_compressed_bytes_written());
      for (int col = 0; col < num_columns_; ++col) {
        auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
            row_group_writer->column(col));
        column_writer->Close();
      }
      row_group_writer->Close();
      EXPECT_EQ(0, row_group_writer->total_compressed_bytes());
      EXPECT_NE(0, row_group_writer->total_bytes_written());
      EXPECT_NE(0, row_group_writer->total_compressed_bytes_written());
    }
    file_writer->Close();

    PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

    int num_rows_ = num_rowgroups_ * rows_per_rowgroup_;

    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    auto file_reader = ParquetFileReader::Open(source);
    ASSERT_EQ(num_columns_, file_reader->metadata()->num_columns());
    ASSERT_EQ(num_rowgroups_, file_reader->metadata()->num_row_groups());
    ASSERT_EQ(num_rows_, file_reader->metadata()->num_rows());

    for (int rg = 0; rg < num_rowgroups_; ++rg) {
      auto rg_reader = file_reader->RowGroup(rg);
      auto rg_metadata = rg_reader->metadata();
      ASSERT_EQ(num_columns_, rg_metadata->num_columns());
      ASSERT_EQ(rows_per_rowgroup_, rg_metadata->num_rows());
      // Check that the specified compression was actually used.
      ASSERT_EQ(
          expected_codec_type, rg_metadata->ColumnChunk(0)->compression());

      const int64_t total_byte_size = rg_metadata->total_byte_size();
      const int64_t total_compressed_size =
          rg_metadata->total_compressed_size();
      if (expected_codec_type == Compression::UNCOMPRESSED) {
        ASSERT_EQ(total_byte_size, total_compressed_size);
      } else {
        ASSERT_NE(total_byte_size, total_compressed_size);
      }

      int64_t total_column_byte_size = 0;
      int64_t total_column_compressed_size = 0;

      for (int i = 0; i < num_columns_; ++i) {
        int64_t values_read;
        ASSERT_FALSE(rg_metadata->ColumnChunk(i)->has_index_page());
        total_column_byte_size +=
            rg_metadata->ColumnChunk(i)->total_uncompressed_size();
        total_column_compressed_size +=
            rg_metadata->ColumnChunk(i)->total_compressed_size();

        std::vector<int16_t> def_levels_out(rows_per_rowgroup_);
        std::vector<int16_t> rep_levels_out(rows_per_rowgroup_);
        auto col_reader = std::static_pointer_cast<TypedColumnReader<TestType>>(
            rg_reader->Column(i));
        this->SetupValuesOut(rows_per_rowgroup_);
        col_reader->ReadBatch(
            rows_per_rowgroup_,
            def_levels_out.data(),
            rep_levels_out.data(),
            this->values_out_ptr_,
            &values_read);
        this->SyncValuesOut();
        ASSERT_EQ(rows_per_rowgroup_, values_read);
        ASSERT_EQ(this->values_, this->values_out_);
        ASSERT_EQ(this->def_levels_, def_levels_out);
      }

      ASSERT_EQ(total_byte_size, total_column_byte_size);
      ASSERT_EQ(total_compressed_size, total_column_compressed_size);
    }
  }

  void UnequalNumRows(
      int64_t max_rows,
      const std::vector<int64_t> rows_per_column) {
    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, props);

    RowGroupWriter* row_group_writer;
    row_group_writer = file_writer->AppendRowGroup();

    this->GenerateData(max_rows);
    for (int col = 0; col < num_columns_; ++col) {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->NextColumn());
      column_writer->WriteBatch(
          rows_per_column[col],
          this->def_levels_.data(),
          nullptr,
          this->values_ptr_);
      column_writer->Close();
    }
    row_group_writer->Close();
    file_writer->Close();
  }

  void UnequalNumRowsBuffered(
      int64_t max_rows,
      const std::vector<int64_t> rows_per_column) {
    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, props);

    RowGroupWriter* row_group_writer;
    row_group_writer = file_writer->AppendBufferedRowGroup();

    this->GenerateData(max_rows);
    for (int col = 0; col < num_columns_; ++col) {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->column(col));
      column_writer->WriteBatch(
          rows_per_column[col],
          this->def_levels_.data(),
          nullptr,
          this->values_ptr_);
      column_writer->Close();
    }
    row_group_writer->Close();
    file_writer->Close();
  }

  void RepeatedUnequalRows() {
    // Optional and repeated, so definition and repetition levels
    this->SetUpSchema(Repetition::REPEATED);

    const int kNumRows = 100;
    this->GenerateData(kNumRows);

    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);
    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().build();
    auto file_writer = ParquetFileWriter::Open(sink, gnode, props);

    RowGroupWriter* row_group_writer;
    row_group_writer = file_writer->AppendRowGroup();

    this->GenerateData(kNumRows);

    std::vector<int16_t> definition_levels(kNumRows, 1);
    std::vector<int16_t> repetition_levels(kNumRows, 0);

    {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->NextColumn());
      column_writer->WriteBatch(
          kNumRows,
          definition_levels.data(),
          repetition_levels.data(),
          this->values_ptr_);
      column_writer->Close();
    }

    definition_levels[1] = 0;
    repetition_levels[3] = 1;

    {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->NextColumn());
      column_writer->WriteBatch(
          kNumRows,
          definition_levels.data(),
          repetition_levels.data(),
          this->values_ptr_);
      column_writer->Close();
    }
  }

  void ZeroRowsRowGroup() {
    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, props);

    RowGroupWriter* row_group_writer;

    row_group_writer = file_writer->AppendRowGroup();
    for (int col = 0; col < num_columns_; ++col) {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->NextColumn());
      column_writer->Close();
    }
    row_group_writer->Close();

    row_group_writer = file_writer->AppendBufferedRowGroup();
    for (int col = 0; col < num_columns_; ++col) {
      auto column_writer = static_cast<TypedColumnWriter<TestType>*>(
          row_group_writer->column(col));
      column_writer->Close();
    }
    row_group_writer->Close();

    file_writer->Close();
  }
};

typedef ::testing::Types<
    Int32Type,
    Int64Type,
    Int96Type,
    FloatType,
    DoubleType,
    BooleanType,
    ByteArrayType,
    FLBAType>
    TestTypes;

TYPED_TEST_SUITE(TestSerialize, TestTypes);

TYPED_TEST(TestSerialize, SmallFileUncompressed) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::UNCOMPRESSED));
}

TYPED_TEST(TestSerialize, TooFewRows) {
  std::vector<int64_t> num_rows = {100, 100, 100, 99};
  ASSERT_THROW(this->UnequalNumRows(100, num_rows), ParquetException);
  ASSERT_THROW(this->UnequalNumRowsBuffered(100, num_rows), ParquetException);
}

TYPED_TEST(TestSerialize, TooManyRows) {
  std::vector<int64_t> num_rows = {100, 100, 100, 101};
  ASSERT_THROW(this->UnequalNumRows(101, num_rows), ParquetException);
  ASSERT_THROW(this->UnequalNumRowsBuffered(101, num_rows), ParquetException);
}

TYPED_TEST(TestSerialize, ZeroRows) {
  ASSERT_NO_THROW(this->ZeroRowsRowGroup());
}

TYPED_TEST(TestSerialize, RepeatedTooFewRows) {
  ASSERT_THROW(this->RepeatedUnequalRows(), ParquetException);
}

TYPED_TEST(TestSerialize, SmallFileSnappy) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::SNAPPY));
}

#ifdef ARROW_WITH_BROTLI
TYPED_TEST(TestSerialize, SmallFileBrotli) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::BROTLI));
}
#endif

TYPED_TEST(TestSerialize, SmallFileGzip) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::GZIP));
}

TYPED_TEST(TestSerialize, SmallFileLz4) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::LZ4));
}

TYPED_TEST(TestSerialize, SmallFileLz4Hadoop) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::LZ4_HADOOP));
}

TYPED_TEST(TestSerialize, SmallFileZstd) {
  ASSERT_NO_FATAL_FAILURE(this->FileSerializeTest(Compression::ZSTD));
}

TEST(TestBufferedRowGroupWriter, DisabledDictionary) {
  // PARQUET-1706:
  // Wrong dictionary_page_offset when writing only data pages via
  // BufferedPageWriter
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder().disable_dictionary()->build();
  schema::NodeVector fields;
  fields.push_back(
      PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
  auto file_writer = ParquetFileWriter::Open(sink, schema, writer_props);
  auto rg_writer = file_writer->AppendBufferedRowGroup();
  auto col_writer = static_cast<Int32Writer*>(rg_writer->column(0));
  int value = 0;
  col_writer->WriteBatch(1, nullptr, nullptr, &value);
  rg_writer->Close();
  file_writer->Close();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());
  auto rg_reader = file_reader->RowGroup(0);
  ASSERT_EQ(1, rg_reader->metadata()->num_columns());
  ASSERT_EQ(1, rg_reader->metadata()->num_rows());
  ASSERT_FALSE(rg_reader->metadata()->ColumnChunk(0)->has_dictionary_page());
}

TEST(TestBufferedRowGroupWriter, MultiPageDisabledDictionary) {
  constexpr int kValueCount = 10000;
  constexpr int kPageSize = 16384;
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(kPageSize)
                          ->build();
  schema::NodeVector fields;
  fields.push_back(
      PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
  auto file_writer = ParquetFileWriter::Open(sink, schema, writer_props);
  auto rg_writer = file_writer->AppendBufferedRowGroup();
  auto col_writer = static_cast<Int32Writer*>(rg_writer->column(0));
  std::vector<int32_t> values_in;
  for (int i = 0; i < kValueCount; ++i) {
    values_in.push_back((i % 100) + 1);
  }
  col_writer->WriteBatch(kValueCount, nullptr, nullptr, values_in.data());
  rg_writer->Close();
  file_writer->Close();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  auto file_metadata = file_reader->metadata();
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());
  std::vector<int32_t> values_out(kValueCount);
  for (int r = 0; r < file_metadata->num_row_groups(); ++r) {
    auto rg_reader = file_reader->RowGroup(r);
    ASSERT_EQ(1, rg_reader->metadata()->num_columns());
    ASSERT_EQ(kValueCount, rg_reader->metadata()->num_rows());
    int64_t total_values_read = 0;
    std::shared_ptr<ColumnReader> col_reader;
    ASSERT_NO_THROW(col_reader = rg_reader->Column(0));
    Int32Reader* int32_reader = static_cast<Int32Reader*>(col_reader.get());
    int64_t vn = kValueCount;
    int32_t* vx = values_out.data();
    while (int32_reader->HasNext()) {
      int64_t values_read;
      int32_reader->ReadBatch(vn, nullptr, nullptr, vx, &values_read);
      vn -= values_read;
      vx += values_read;
      total_values_read += values_read;
    }
    ASSERT_EQ(kValueCount, total_values_read);
    ASSERT_EQ(values_in, values_out);
  }
}

TEST(TestPageBufferArenaIntegration, BufferedAndNonBufferedRowGroupRoundTrip) {
  constexpr int kNumRows = 2048;
  constexpr int kPageSize = 512;

  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(kPageSize)
                          ->set_parquet_block_size(4096)
                          ->build();
  schema::NodeVector fields;
  fields.push_back(
      PrimitiveNode::Make("col0", Repetition::REQUIRED, Type::INT32));
  fields.push_back(
      PrimitiveNode::Make("col1", Repetition::REQUIRED, Type::INT32));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  std::vector<int32_t> values0(kNumRows);
  std::vector<int32_t> values1(kNumRows);
  std::iota(values0.begin(), values0.end(), 0);
  std::iota(values1.begin(), values1.end(), 10'000);

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);

  auto row_group_writer = file_writer->AppendRowGroup();
  static_cast<Int32Writer*>(row_group_writer->NextColumn())
      ->WriteBatch(kNumRows, nullptr, nullptr, values0.data());
  static_cast<Int32Writer*>(row_group_writer->NextColumn())
      ->WriteBatch(kNumRows, nullptr, nullptr, values1.data());
  row_group_writer->Close();

  row_group_writer = file_writer->AppendBufferedRowGroup();
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(kNumRows, nullptr, nullptr, values0.data());
  static_cast<Int32Writer*>(row_group_writer->column(1))
      ->WriteBatch(kNumRows, nullptr, nullptr, values1.data());
  row_group_writer->Close();

  file_writer->Close();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(2, file_reader->metadata()->num_row_groups());

  for (int row_group = 0; row_group < 2; ++row_group) {
    auto rg_reader = file_reader->RowGroup(row_group);
    for (int col = 0; col < 2; ++col) {
      std::vector<int32_t> values_out(kNumRows, -1);
      auto reader =
          std::static_pointer_cast<Int32Reader>(rg_reader->Column(col));
      int64_t total_values_read = 0;
      int64_t remaining = kNumRows;
      int32_t* out = values_out.data();
      while (reader->HasNext()) {
        int64_t values_read = 0;
        reader->ReadBatch(remaining, nullptr, nullptr, out, &values_read);
        total_values_read += values_read;
        remaining -= values_read;
        out += values_read;
      }
      ASSERT_EQ(kNumRows, total_values_read);
      ASSERT_EQ(col == 0 ? values0 : values1, values_out);
    }
  }
}

TEST(TestPageBufferArenaIntegration, BufferedRowGroupLazilyAllocatesArena) {
  constexpr int64_t kArenaSize = 1 << 20;
  constexpr int64_t kExpectedArenaSize =
      GetExpectedPageBufferArenaSize(kArenaSize);

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->disable_dictionary()
                          ->set_parquet_block_size(kArenaSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
  EXPECT_LT(pool.max_memory(), kExpectedArenaSize);

  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  EXPECT_GE(pool.max_memory(), kExpectedArenaSize);
  EXPECT_LT(pool.max_memory(), kArenaSize);

  int32_t value = 7;
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(1, nullptr, nullptr, &value);
  row_group_writer->Close();
  file_writer->Close();
}

TEST(
    TestPageBufferArenaIntegration,
    LowLevelBufferedRowGroupDefaultDoesNotAllocateArena) {
  constexpr int64_t kArenaSize = 1 << 20;
  constexpr int64_t kExpectedArenaSize =
      GetExpectedPageBufferArenaSize(kArenaSize);

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->disable_dictionary()
                          ->set_parquet_block_size(kArenaSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

  auto file_writer = ParquetFileWriter::Open(sink, schema, writer_props);
  EXPECT_LT(pool.max_memory(), kExpectedArenaSize);

  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  EXPECT_LT(pool.max_memory(), kExpectedArenaSize);

  int32_t value = 7;
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(1, nullptr, nullptr, &value);
  row_group_writer->Close();
  file_writer->Close();
}

TEST(
    TestPageBufferArenaIntegration,
    BufferedRowGroupSeriallyReusesSharedUncompressedScratchAcrossColumns) {
  if (!util::Codec::IsAvailable(Compression::SNAPPY)) {
    GTEST_SKIP() << "Snappy codec unavailable";
  }

  const auto [scratch_after_first_column, scratch_after_second_column] =
      MeasureBufferedSharedScratchAfterClosingColumns(
          ParquetDataPageVersion::V1);

  EXPECT_GT(scratch_after_first_column, 0);
  EXPECT_EQ(scratch_after_first_column, scratch_after_second_column);
}

TEST(
    TestPageBufferArenaIntegration,
    BufferedRowGroupSeriallyReusesSharedCompressedScratchAcrossColumns) {
  if (!util::Codec::IsAvailable(Compression::SNAPPY)) {
    GTEST_SKIP() << "Snappy codec unavailable";
  }

  const auto [scratch_after_first_column, scratch_after_second_column] =
      MeasureBufferedSharedScratchAfterClosingColumns(
          ParquetDataPageVersion::V2);

  EXPECT_GT(scratch_after_first_column, 0);
  EXPECT_EQ(scratch_after_first_column, scratch_after_second_column);
}

TEST(
    TestPageBufferArenaIntegration,
    NonBufferedAndThreadedWritesDoNotAllocateArena) {
  constexpr int64_t kArenaSize = 1 << 20;

  {
    ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
    auto sink = CreateOutputStream();
    auto writer_props = WriterProperties::Builder()
                            .memory_pool(&pool)
                            ->disable_dictionary()
                            ->set_parquet_block_size(kArenaSize)
                            ->build();
    auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
        "schema",
        Repetition::REQUIRED,
        {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

    auto file_writer =
        ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
    auto row_group_writer = file_writer->AppendRowGroup();
    int32_t value = 11;
    static_cast<Int32Writer*>(row_group_writer->NextColumn())
        ->WriteBatch(1, nullptr, nullptr, &value);
    row_group_writer->Close();
    file_writer->Close();

    EXPECT_LT(pool.max_memory(), kArenaSize);
  }

  {
    ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
    auto sink = CreateOutputStream();
    auto writer_props = WriterProperties::Builder()
                            .memory_pool(&pool)
                            ->disable_dictionary()
                            ->data_pagesize(512)
                            ->set_parquet_block_size(kArenaSize)
                            ->build();
    auto arrow_writer_props =
        ArrowWriterProperties::Builder().set_use_threads(true)->build();
    auto schema = ::arrow::schema(
        {::arrow::field("col0", ::arrow::int32()),
         ::arrow::field("col1", ::arrow::int32())});

    ::arrow::Int32Builder builder0;
    ::arrow::Int32Builder builder1;
    for (int i = 0; i < 32; ++i) {
      ASSERT_TRUE(builder0.Append(i).ok());
      ASSERT_TRUE(builder1.Append(10'000 + i).ok());
    }

    std::shared_ptr<::arrow::Array> array0;
    std::shared_ptr<::arrow::Array> array1;
    ASSERT_TRUE(builder0.Finish(&array0).ok());
    ASSERT_TRUE(builder1.Finish(&array1).ok());

    auto maybe_writer =
        bytedance::bolt::parquet::arrow::arrow::FileWriter::Open(
            *schema, &pool, sink, writer_props, arrow_writer_props);
    ASSERT_TRUE(maybe_writer.ok());
    auto writer = std::move(maybe_writer).ValueOrDie();
    auto batch = ::arrow::RecordBatch::Make(
        schema, 32, {std::move(array0), std::move(array1)});

    ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
    const auto stats = writer->memoryStats();
    EXPECT_EQ(0, stats.pageBufferArenaCapacityBytes);
    EXPECT_EQ(0, stats.pageBufferArenaReservedBytes);
    ASSERT_TRUE(writer->Close().ok());

    EXPECT_LT(pool.max_memory(), kArenaSize);
  }
}

TEST(TestPageBufferArenaIntegration, ArrowSerialBufferedWriteEnablesArena) {
  constexpr int64_t kArenaSize = 1 << 20;
  constexpr int64_t kExpectedArenaSize =
      GetExpectedPageBufferArenaSize(kArenaSize);

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->disable_dictionary()
                          ->data_pagesize(512)
                          ->set_parquet_block_size(kArenaSize)
                          ->build();
  auto arrow_writer_props =
      ArrowWriterProperties::Builder().set_use_threads(false)->build();
  auto schema = ::arrow::schema(
      {::arrow::field("col0", ::arrow::int32()),
       ::arrow::field("col1", ::arrow::int32())});

  ::arrow::Int32Builder builder0;
  ::arrow::Int32Builder builder1;
  for (int i = 0; i < 32; ++i) {
    ASSERT_TRUE(builder0.Append(i).ok());
    ASSERT_TRUE(builder1.Append(10'000 + i).ok());
  }

  std::shared_ptr<::arrow::Array> array0;
  std::shared_ptr<::arrow::Array> array1;
  ASSERT_TRUE(builder0.Finish(&array0).ok());
  ASSERT_TRUE(builder1.Finish(&array1).ok());

  auto maybe_writer = bytedance::bolt::parquet::arrow::arrow::FileWriter::Open(
      *schema, &pool, sink, writer_props, arrow_writer_props);
  ASSERT_TRUE(maybe_writer.ok());
  auto writer = std::move(maybe_writer).ValueOrDie();
  auto batch = ::arrow::RecordBatch::Make(
      schema, 32, {std::move(array0), std::move(array1)});

  ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
  const auto stats = writer->memoryStats();
  EXPECT_GE(stats.pageBufferArenaCapacityBytes, kExpectedArenaSize);
  ASSERT_TRUE(writer->Close().ok());
}

TEST(TestPageBufferArenaIntegration, DictionaryPageUsesArenaOnBufferedClose) {
  constexpr int64_t kNumRows = 32 * 1024;
  constexpr int64_t kArenaSize = 1 << 20;

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->enable_dictionary()
                          ->disable_statistics()
                          ->dictionary_pagesize_limit(kArenaSize)
                          ->data_pagesize(1024)
                          ->set_parquet_block_size(kArenaSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

  std::vector<int32_t> values(kNumRows);
  std::iota(values.begin(), values.end(), 0);

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(kNumRows, nullptr, nullptr, values.data());

  const int64_t peak_before_close = pool.max_memory();
  row_group_writer->Close();
  const int64_t peak_after_close = pool.max_memory();

  EXPECT_LT(peak_after_close - peak_before_close, 64 * 1024);

  file_writer->Close();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());
  ASSERT_TRUE(file_reader->RowGroup(0)
                  ->metadata()
                  ->ColumnChunk(0)
                  ->has_dictionary_page());
}

TEST(
    TestPageBufferArenaIntegration,
    BufferedCompressedV1PageUsesActualCompressedSizeForArenaAllocation) {
  if (!util::Codec::IsAvailable(Compression::SNAPPY)) {
    GTEST_SKIP() << "Snappy codec unavailable";
  }

  constexpr int64_t kNumRows = 32 * 1024;
  constexpr int64_t kParquetBlockSize = 128 * 1024;

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->disable_dictionary()
                          ->compression(Compression::SNAPPY)
                          ->data_page_version(ParquetDataPageVersion::V1)
                          ->data_pagesize(1 << 20)
                          ->set_parquet_block_size(kParquetBlockSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

  std::vector<int32_t> values(kNumRows, 7);

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(kNumRows, nullptr, nullptr, values.data());
  row_group_writer->column(0)->Close();

  const auto memoryStats = row_group_writer->memory_stats();
  EXPECT_EQ(0, memoryStats.bufferedPageFallbackAllocatedBytes);
  EXPECT_GT(memoryStats.bufferedPageWriterDataPageCount, 0);
  EXPECT_GT(memoryStats.pageBufferArenaReservedBytes, 0);
  EXPECT_LE(
      memoryStats.pageBufferArenaReservedBytes,
      memoryStats.pageBufferArenaCapacityBytes);

  row_group_writer->Close();
  file_writer->Close();

  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());

  std::vector<int32_t> values_out(kNumRows, -1);
  auto reader = std::static_pointer_cast<Int32Reader>(
      file_reader->RowGroup(0)->Column(0));
  int64_t total_values_read = 0;
  while (reader->HasNext()) {
    int64_t values_read = 0;
    reader->ReadBatch(
        kNumRows - total_values_read,
        nullptr,
        nullptr,
        values_out.data() + total_values_read,
        &values_read);
    total_values_read += values_read;
  }
  ASSERT_EQ(kNumRows, total_values_read);
  ASSERT_EQ(values, values_out);
}

TEST(
    TestPageBufferArenaIntegration,
    BufferedCompressedV2PageRetainsExactSizedArenaBehavior) {
  if (!util::Codec::IsAvailable(Compression::SNAPPY)) {
    GTEST_SKIP() << "Snappy codec unavailable";
  }

  constexpr int64_t kNumRows = 32 * 1024;
  constexpr int64_t kParquetBlockSize = 128 * 1024;

  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .memory_pool(&pool)
                          ->disable_dictionary()
                          ->compression(Compression::SNAPPY)
                          ->data_page_version(ParquetDataPageVersion::V2)
                          ->data_pagesize(1 << 20)
                          ->set_parquet_block_size(kParquetBlockSize)
                          ->build();
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema",
      Repetition::REQUIRED,
      {PrimitiveNode::Make("col", Repetition::REQUIRED, Type::INT32)}));

  std::vector<int32_t> values(kNumRows, 7);

  auto file_writer =
      ParquetFileWriter::Open(sink, schema, writer_props, NULLPTR, true);
  auto row_group_writer = file_writer->AppendBufferedRowGroup();
  static_cast<Int32Writer*>(row_group_writer->column(0))
      ->WriteBatch(kNumRows, nullptr, nullptr, values.data());
  row_group_writer->column(0)->Close();

  const auto memoryStats = row_group_writer->memory_stats();
  EXPECT_EQ(0, memoryStats.bufferedPageFallbackAllocatedBytes);
  EXPECT_GT(memoryStats.bufferedPageWriterDataPageCount, 0);

  row_group_writer->Close();
  file_writer->Close();

  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());

  std::vector<int32_t> values_out(kNumRows, -1);
  auto reader = std::static_pointer_cast<Int32Reader>(
      file_reader->RowGroup(0)->Column(0));
  int64_t total_values_read = 0;
  while (reader->HasNext()) {
    int64_t values_read = 0;
    reader->ReadBatch(
        kNumRows - total_values_read,
        nullptr,
        nullptr,
        values_out.data() + total_values_read,
        &values_read);
    total_values_read += values_read;
  }
  ASSERT_EQ(kNumRows, total_values_read);
  ASSERT_EQ(values, values_out);
}

TEST(
    TestPageBufferArenaIntegration,
    ParallelBufferedWriteRecordBatchRoundTrip) {
  constexpr int kNumRows = 2048;

  ::arrow::Int32Builder builder0;
  ::arrow::Int32Builder builder1;
  for (int i = 0; i < kNumRows; ++i) {
    ASSERT_TRUE(builder0.Append(i).ok());
    ASSERT_TRUE(builder1.Append(10'000 + i).ok());
  }

  std::shared_ptr<::arrow::Array> array0;
  std::shared_ptr<::arrow::Array> array1;
  ASSERT_TRUE(builder0.Finish(&array0).ok());
  ASSERT_TRUE(builder1.Finish(&array1).ok());

  auto schema = ::arrow::schema(
      {::arrow::field("col0", ::arrow::int32()),
       ::arrow::field("col1", ::arrow::int32())});
  auto batch = ::arrow::RecordBatch::Make(schema, kNumRows, {array0, array1});

  auto sink = CreateOutputStream();
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(512)
                          ->set_parquet_block_size(4096)
                          ->build();
  auto arrow_writer_props =
      ArrowWriterProperties::Builder().set_use_threads(true)->build();

  auto maybe_writer = bytedance::bolt::parquet::arrow::arrow::FileWriter::Open(
      *schema,
      ::arrow::default_memory_pool(),
      sink,
      writer_props,
      arrow_writer_props);
  ASSERT_TRUE(maybe_writer.ok());
  auto writer = std::move(maybe_writer).ValueOrDie();
  ASSERT_TRUE(writer->WriteRecordBatch(*batch).ok());
  ASSERT_TRUE(writer->Close().ok());

  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());
  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source);
  ASSERT_EQ(1, file_reader->metadata()->num_row_groups());

  auto rg_reader = file_reader->RowGroup(0);
  for (int col = 0; col < 2; ++col) {
    std::vector<int32_t> values_out(kNumRows, -1);
    int64_t total_values_read = 0;
    int64_t remaining = kNumRows;
    int32_t* out = values_out.data();
    auto reader = std::static_pointer_cast<Int32Reader>(rg_reader->Column(col));
    while (reader->HasNext()) {
      int64_t values_read = 0;
      reader->ReadBatch(remaining, nullptr, nullptr, out, &values_read);
      total_values_read += values_read;
      remaining -= values_read;
      out += values_read;
    }

    ASSERT_EQ(kNumRows, total_values_read);
    for (int i = 0; i < kNumRows; ++i) {
      ASSERT_EQ(col == 0 ? i : 10'000 + i, values_out[i]);
    }
  }
}

TEST(ParquetRoundtrip, AllNulls) {
  auto primitive_node =
      PrimitiveNode::Make("nulls", Repetition::OPTIONAL, nullptr, Type::INT32);
  schema::NodeVector columns({primitive_node});

  auto root_node =
      GroupNode::Make("root", Repetition::REQUIRED, columns, nullptr);

  auto sink = CreateOutputStream();

  auto file_writer = ParquetFileWriter::Open(
      sink, std::static_pointer_cast<GroupNode>(root_node));
  auto row_group_writer = file_writer->AppendRowGroup();
  auto column_writer =
      static_cast<Int32Writer*>(row_group_writer->NextColumn());

  int32_t values[3];
  int16_t def_levels[] = {0, 0, 0};

  column_writer->WriteBatch(3, def_levels, nullptr, values);

  column_writer->Close();
  row_group_writer->Close();
  file_writer->Close();

  ReaderProperties props = default_reader_properties();
  props.enable_buffered_stream();
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

  auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
  auto file_reader = ParquetFileReader::Open(source, props);
  auto row_group_reader = file_reader->RowGroup(0);
  auto column_reader =
      std::static_pointer_cast<Int32Reader>(row_group_reader->Column(0));

  int64_t values_read;
  def_levels[0] = -1;
  def_levels[1] = -1;
  def_levels[2] = -1;
  column_reader->ReadBatch(3, def_levels, nullptr, values, &values_read);
  EXPECT_THAT(def_levels, ElementsAre(0, 0, 0));
}

} // namespace test

} // namespace bytedance::bolt::parquet::arrow
