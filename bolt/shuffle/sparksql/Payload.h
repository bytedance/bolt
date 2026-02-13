/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#pragma once

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <cstdint>

#include "bolt/functions/InlineFlatten.h"
#include "bolt/shuffle/sparksql/CompressionStream.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/Utils.h"
namespace bytedance::bolt::shuffle::sparksql {
class ByteBuffer;

class Payload {
 public:
  enum Type : uint8_t {
    kCompressed = 1,
    kUncompressed = 2,
    kToBeCompressed = 3,
    kPayloadTypeEnd = 4
  };

  enum Mode : uint8_t { kBuffer = 1, kRowVector = 2, kUnsafeRow = 3 };

  Payload(
      Type type,
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      Mode mode = kBuffer);

  virtual ~Payload() = default;

  virtual arrow::Status serialize(arrow::io::OutputStream* outputStream) = 0;

  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) = 0;

  int64_t getCompressTime() const {
    return compressTime_;
  }

  int64_t getWriteTime() const {
    return writeTime_;
  }

  Type type() const {
    return type_;
  }

  Mode mode() const {
    return mode_;
  }

  uint32_t numRows() const {
    return numRows_;
  }

  uint32_t numBuffers() {
    return isValidityBuffer_->size();
  }

  const std::vector<bool>* isValidityBuffer() const {
    return isValidityBuffer_;
  }

  std::string toString() const;

 protected:
  Type type_;
  uint32_t numRows_;
  const std::vector<bool>* isValidityBuffer_;
  uint64_t compressTime_{0};
  uint64_t writeTime_{0};
  Mode mode_{kBuffer};
};

// A block represents data to be cached in-memory.
// Can be compressed or uncompressed.
class BlockPayload : public Payload {
 public:
  static arrow::Result<std::unique_ptr<BlockPayload>> fromBuffers(
      Payload::Type payloadType,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      arrow::MemoryPool* pool,
      Codec* codec,
      Payload::Mode mode,
      bool hasComplexType);

  static arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> deserialize(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<Codec>& codec,
      arrow::MemoryPool* pool,
      uint32_t& numRows,
      uint64_t& decompressTime,
      std::optional<uint8_t>& payloadType,
      ByteBuffer* readAheadBuffer);

  static arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  deserializeRowVectorModeBuffers(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<Codec>& codec,
      arrow::MemoryPool* pool,
      uint32_t& numRows,
      uint64_t& decompressTime,
      ByteBuffer** readAheadBuffer);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t pos) override;

  static void concatBuffer(
      std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
      arrow::MemoryPool* pool,
      Codec* codec,
      Payload::Mode& mode,
      bool hasComplexType);

  static arrow::Status getVectorLayout(
      arrow::io::InputStream* inputStream,
      uint8_t& type,
      int64_t& bytes);

 protected:
  BlockPayload(
      Type type,
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      const std::vector<bool>* isValidityBuffer,
      arrow::MemoryPool* pool,
      Codec* codec,
      Payload::Mode mode = Payload::Mode::kBuffer)
      : Payload(type, numRows, isValidityBuffer, mode),
        buffers_(std::move(buffers)),
        pool_(pool),
        codec_(codec) {}

  void setCompressionTime(int64_t compressionTime);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
  arrow::MemoryPool* pool_;
  Codec* codec_;
};

class InMemoryPayload final : public Payload {
 public:
  InMemoryPayload(
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      bool isRowVectorMode = false)
      : Payload(
            Type::kUncompressed,
            numRows,
            isValidityBuffer,
            isRowVectorMode ? kRowVector : kBuffer),
        buffers_(std::move(buffers)) {}

  static arrow::Result<std::unique_ptr<InMemoryPayload>> merge(
      std::unique_ptr<InMemoryPayload> source,
      std::unique_ptr<InMemoryPayload> append,
      arrow::MemoryPool* pool,
      int64_t rowvectorModeCompressionMinColumns,
      int64_t rowvectorModeCompressionMaxBufferSize);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) override;

  arrow::Result<std::unique_ptr<BlockPayload>> toBlockPayload(
      Payload::Type payloadType,
      arrow::MemoryPool* pool,
      Codec* codec,
      bool hasComplexType);

  int64_t getBufferSize() const;

  arrow::Status copyBuffers(arrow::MemoryPool* pool);

 private:
  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
};

class UncompressedDiskBlockPayload : public Payload {
 public:
  UncompressedDiskBlockPayload(
      Type type,
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      arrow::io::InputStream*& inputStream,
      uint64_t rawSize,
      arrow::MemoryPool* pool,
      Codec* codec);

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) override;

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

 private:
  arrow::io::InputStream*& inputStream_;
  uint64_t rawSize_;
  arrow::MemoryPool* pool_;
  Codec* codec_;
  uint32_t readPos_{0};

  arrow::Result<std::shared_ptr<arrow::Buffer>> readUncompressedBuffer();
};

class CompressedDiskBlockPayload : public Payload {
 public:
  CompressedDiskBlockPayload(
      uint32_t numRows,
      const std::vector<bool>* isValidityBuffer,
      arrow::io::InputStream*& inputStream,
      uint64_t rawSize,
      arrow::MemoryPool* pool);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) override;

 private:
  arrow::io::InputStream*& inputStream_;
  uint64_t rawSize_;
  arrow::MemoryPool* pool_;
};

// for BoltRowBasedSortShuffleWriter
class RowBlockPayload : public Payload {
 public:
  static arrow::Status deserialize(
      arrow::io::InputStream* inputStream,
      uint8_t* dst,
      int32_t dstSize,
      int32_t& offset,
      AdaptiveParallelZstdCodec* codec,
      std::vector<std::string_view>& outputRows,
      int32_t& outputLen,
      bool& eof,
      bool& layoutEnd,
      RowVectorLayout& layout,
      uint64_t& decompressTime);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t pos) override;

  RowBlockPayload(
      folly::Range<uint8_t**> rows,
      const int64_t rawSize,
      arrow::MemoryPool* pool,
      AdaptiveParallelZstdCodec* codec,
      RowVectorLayout layout = RowVectorLayout::kColumnar)
      : Payload(kCompressed, rows.size(), nullptr, kUnsafeRow),
        rows_(rows),
        pool_(pool),
        codec_(codec),
        rawSize_(rawSize),
        layout_(layout) {}

 private:
  folly::Range<uint8_t**> rows_;
  arrow::MemoryPool* pool_;
  AdaptiveParallelZstdCodec* codec_;
  // Caution: rawSize_ is not accuracy for rss
  const int64_t rawSize_{0};
  const RowVectorLayout layout_{RowVectorLayout::kColumnar};
};

class CompressedDiskRowBlockPayload : public Payload {
 public:
  CompressedDiskRowBlockPayload(
      uint32_t numRows,
      arrow::io::InputStream*& inputStream,
      uint64_t rawSize);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) override;

 private:
  arrow::io::InputStream*& inputStream_;
  uint64_t rawSize_;
};

struct ByteBuffer {
  FLATTEN void reset() {
    data = nullptr;
    size = 0;
  }

  FLATTEN void advance(size_t len) {
    BOLT_DCHECK(
        data != nullptr && size >= len, "Illegal gluten ByteBuffer usage");
    size -= len;
    data += len;
  }
  uint8_t* data = nullptr;
  size_t size = 0;
};

} // namespace bytedance::bolt::shuffle::sparksql
