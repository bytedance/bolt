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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include <sstream>
#include <utility>

#include "bolt/dwio/parquet/arrow/Properties.h"

#include "arrow/io/buffered.h"
#include "arrow/io/memory.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
namespace bytedance::bolt::parquet::arrow {

ReaderProperties default_reader_properties() {
  static ReaderProperties default_reader_properties;
  return default_reader_properties;
}

std::ostream& operator<<(
    std::ostream& os,
    const ParquetVersion::type& version) {
  switch (version) {
    case ParquetVersion::PARQUET_1_0:
      os << "PARQUET_1_0";
      break;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    case ParquetVersion::PARQUET_2_0:
      os << "PARQUET_2_0";
      break;
#pragma GCC diagnostic pop
    case ParquetVersion::PARQUET_2_4:
      os << "PARQUET_2_4";
      break;
    case ParquetVersion::PARQUET_2_6:
      os << "PARQUET_2_6";
      break;
    default:
      os << "Unknown Version";
      break;
  }
  return os;
}

std::ostream& operator<<(
    std::ostream& os,
    const ParquetDataPageVersion& version) {
  switch (version) {
    case ParquetDataPageVersion::V1:
      os << "V1";
      break;
    case ParquetDataPageVersion::V2:
      os << "V2";
      break;
    default:
      os << "Unknown Version";
      break;
  }
  return os;
}

std::shared_ptr<ArrowInputStream> ReaderProperties::GetStream(
    std::shared_ptr<ArrowInputFile> source,
    int64_t start,
    int64_t num_bytes) {
  if (buffered_stream_enabled_) {
    // ARROW-6180 / PARQUET-1636 Create isolated reader that references segment
    // of source
    PARQUET_ASSIGN_OR_THROW(
        std::shared_ptr<::arrow::io::InputStream> safe_stream,
        ::arrow::io::RandomAccessFile::GetStream(source, start, num_bytes));
    PARQUET_ASSIGN_OR_THROW(
        auto stream,
        ::arrow::io::BufferedInputStream::Create(
            buffer_size_, pool_, safe_stream, num_bytes));
    return std::move(stream);
  } else {
    PARQUET_ASSIGN_OR_THROW(auto data, source->ReadAt(start, num_bytes));

    if (data->size() != num_bytes) {
      std::stringstream ss;
      ss << "Tried reading " << num_bytes << " bytes starting at position "
         << start << " from file but only got " << data->size();
      throw ParquetException(ss.str());
    }
    return std::make_shared<::arrow::io::BufferReader>(data);
  }
}

::arrow::internal::Executor* ArrowWriterProperties::executor() const {
  return executor_ != nullptr ? executor_
                              : ::arrow::internal::GetCpuThreadPool();
}

ArrowReaderProperties default_arrow_reader_properties() {
  static ArrowReaderProperties default_reader_props;
  return default_reader_props;
}

std::shared_ptr<ArrowWriterProperties> default_arrow_writer_properties() {
  static std::shared_ptr<ArrowWriterProperties> default_writer_properties =
      ArrowWriterProperties::Builder().build();
  return default_writer_properties;
}

} // namespace bytedance::bolt::parquet::arrow
