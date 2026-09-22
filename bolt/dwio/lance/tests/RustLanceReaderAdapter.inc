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
 */

#pragma once

#include <arrow/c/abi.h>
#include <lance_file.h>

#include <memory>
#include <string>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/File.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bytedance::bolt::lance::reader::test {

/// Test-only implementation adapter around the Rust lance_file_ffi reader.
/// Production code must use NativeLanceReader and must not link this header or
/// the FFI library.
class RustLanceReaderAdapter {
 public:
  RustLanceReaderAdapter(
      const std::string& path,
      memory::MemoryPool& pool,
      const std::vector<std::string>& projectedColumns = {})
      : state_{std::make_shared<LocalReadFile>(path), &pool},
        common_(lce_alloc_ctx_common()),
        reader_(lce_reader_alloc_ctx(common_)) {
    BOLT_CHECK_NOT_NULL(common_);
    BOLT_CHECK_NOT_NULL(reader_);
    std::vector<const char*> names;
    names.reserve(projectedColumns.size());
    for (const auto& name : projectedColumns) {
      names.push_back(name.c_str());
    }
    const auto result = lce_reader_create(
        reader_,
        {createStream,
         readStream,
         releaseStream,
         state_.file->size(),
         &state_,
         names.empty() ? nullptr : names.data(),
         names.size()});
    BOLT_CHECK_EQ(result.status, 0, "{}", error(result.error));
  }

  ~RustLanceReaderAdapter() {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }
    if (reader_ != nullptr) {
      lce_reader_free_ctx(reader_);
    }
    if (common_ != nullptr) {
      lce_free_ctx_common(common_);
    }
  }

  RustLanceReaderAdapter(const RustLanceReaderAdapter&) = delete;
  RustLanceReaderAdapter& operator=(const RustLanceReaderAdapter&) = delete;

  RowTypePtr rowType() const {
    ArrowSchema schema{};
    const auto status = lce_reader_get_schema(reader_, &schema);
    BOLT_CHECK_EQ(status.status, 0, "{}", error(status.error));
    auto type = asRowType(importFromArrow(schema));
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
    return type;
  }

  void open(uint32_t batchSize) {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }
    stream_ = {};
    const auto status = lce_reader_open_stream(reader_, batchSize, &stream_);
    BOLT_CHECK_EQ(status.status, 0, "{}", error(status.error));
  }

  bool next(VectorPtr& result) {
    ArrowSchema schema{};
    BOLT_CHECK_EQ(stream_.get_schema(&stream_, &schema), 0);
    ArrowArray array{};
    const auto status = stream_.get_next(&stream_, &array);
    BOLT_CHECK_EQ(
        status,
        0,
        "{}",
        stream_.get_last_error == nullptr ? "Rust Lance stream failed"
                                          : stream_.get_last_error(&stream_));
    if (array.release == nullptr) {
      if (schema.release != nullptr) {
        schema.release(&schema);
      }
      result.reset();
      return false;
    }
    result = importFromArrowAsOwner(schema, array, {}, state_.pool);
    return true;
  }

 private:
  struct State {
    std::shared_ptr<ReadFile> file;
    memory::MemoryPool* pool;
  };

  struct Stream {
    std::shared_ptr<ReadFile> file;
    memory::MemoryPool* pool;
    uint64_t offset;
    uint64_t remaining;
  };

  static std::string error(const char* message) {
    return message == nullptr ? "Rust Lance FFI failed" : message;
  }

  static void createStream(
      void** output,
      const void* opaque,
      uint64_t offset,
      uint64_t length) {
    const auto* state = static_cast<const State*>(opaque);
    *output = new Stream{state->file, state->pool, offset, length};
  }

  static bool
  readStream(const MemoryBuffer** output, void* opaque, const void*) {
    auto* stream = static_cast<Stream*>(opaque);
    if (stream->remaining == 0) {
      return false;
    }
    auto* data = static_cast<uint8_t*>(
        stream->pool->allocate(static_cast<int64_t>(stream->remaining)));
    stream->file->pread(stream->offset, stream->remaining, data);
    *output =
        new MemoryBuffer{data, stream->remaining, releaseBuffer, stream->pool};
    stream->offset += stream->remaining;
    stream->remaining = 0;
    return true;
  }

  static void releaseStream(const void*, void** opaque) {
    delete static_cast<Stream*>(*opaque);
    *opaque = nullptr;
  }

  static void releaseBuffer(const MemoryBuffer* buffer) {
    auto* pool =
        static_cast<memory::MemoryPool*>(const_cast<void*>(buffer->state));
    pool->free(const_cast<uint8_t*>(buffer->data), buffer->size);
    delete buffer;
  }

  State state_;
  lce_ctx_common* common_;
  lce_reader_ctx* reader_;
  ArrowArrayStream stream_{};
};

} // namespace bytedance::bolt::lance::reader::test
