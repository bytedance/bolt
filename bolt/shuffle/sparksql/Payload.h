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

class ColumnBufferPool final {
 public:
  explicit ColumnBufferPool(arrow::MemoryPool* pool) : pool_(pool) {}

  // Returns a resizable buffer whose logical size is `size` and whose capacity
  // is at least `size + padding` (padding reserves SIMD over-read space,
  // mirroring the non-pooled arrow::AllocateResizableBuffer path). Reuses a
  // free buffer with sufficient capacity when available, otherwise allocates a
  // new one and tracks it for future reuse.
  arrow::Result<std::shared_ptr<arrow::ResizableBuffer>> allocate(
      int64_t size,
      int64_t padding) {
    const int64_t capacity = size + padding;
    if (freeList_.empty()) {
      // Rebuild the set of solely-owned buffers. This is the only O(M) step and
      // is amortized across a whole batch's worth of allocate() calls.
      refillFreeList();
    }
    while (!freeList_.empty()) {
      const size_t idx = freeList_.back();
      auto& buffer = buffers_[idx];
      // refillFreeList() only records buffers with use_count() == 1, and the
      // pool is single-threaded, so a buffer stays solely-owned until we hand
      // it out here.
      if (buffer->capacity() >= capacity) {
        // Fits within existing capacity: just adjust the logical size. Passing
        // shrink_to_fit=false guarantees no reallocation happens here, and the
        // [size, capacity) tail keeps at least `padding` bytes for SIMD.
        auto status = buffer->Resize(size, /*shrink_to_fit=*/false);
        if (!status.ok()) {
          return status;
        }
        freeList_.pop_back();
        return buffer;
      }
      // Capacity too small for this request; drop it from the free list for the
      // current batch rather than reallocating, preserving the original
      // no-realloc-on-reuse behavior.
      freeList_.pop_back();
    }
    auto result = arrow::AllocateResizableBuffer(capacity, pool_);
    if (!result.ok()) {
      return result.status();
    }
    std::shared_ptr<arrow::ResizableBuffer> buffer =
        std::move(result).ValueUnsafe();
    if (padding > 0) {
      auto status = buffer->Resize(size, /*shrink_to_fit=*/false);
      if (!status.ok()) {
        return status;
      }
    }
    if (buffers_.size() < kMaxPooledBuffers) {
      buffers_.push_back(buffer);
    }
    return buffer;
  }

  // Number of buffers currently tracked by the pool. Exposed for tests/metrics.
  size_t numBuffers() const {
    return buffers_.size();
  }

  // Drops all cached buffers and frees their memory back to `pool_`. Buffers
  // that still escaped into a live payload or RowVector (use_count() > 1) stay
  // alive until their last owner releases them. Call at close() to return
  // memory eagerly instead of waiting for the pool's own destruction.
  void release() {
    buffers_.clear();
    freeList_.clear();
  }

 private:
  // Recomputes freeList_ as the indices of buffers currently owned only by the
  // pool (use_count() == 1). Buffers still referenced elsewhere are skipped.
  void refillFreeList() {
    freeList_.clear();
    freeList_.reserve(buffers_.size());
    for (size_t i = 0; i < buffers_.size(); ++i) {
      if (buffers_[i].use_count() == 1) {
        freeList_.push_back(i);
      }
    }
  }

  std::vector<std::shared_ptr<arrow::ResizableBuffer>> buffers_;
  static constexpr size_t kMaxPooledBuffers = 5000;
  // Indices into buffers_ that are known to be solely owned by the pool and
  // thus reusable. Refilled lazily by refillFreeList() when exhausted.
  std::vector<size_t> freeList_;
  arrow::MemoryPool* pool_;
};

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
      ByteBuffer* readAheadBuffer,
      ColumnBufferPool* bufferPool = nullptr);

  static arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>>
  deserializeRowVectorModeBuffers(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<Codec>& codec,
      arrow::MemoryPool* pool,
      uint32_t& numRows,
      uint64_t& decompressTime,
      ByteBuffer** readAheadBuffer,
      ColumnBufferPool* bufferPool = nullptr);

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
      int64_t rowvectorModeCompressionMaxBufferSize,
      bool sourceBuffersResizable = false);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> readBufferAt(
      uint32_t index) override;

  int64_t bufferSizeAt(uint32_t index) const {
    return buffers_[index] ? buffers_[index]->size() : 0;
  }

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
