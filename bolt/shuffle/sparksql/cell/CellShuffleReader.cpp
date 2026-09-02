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

#include "bolt/shuffle/sparksql/cell/CellShuffleReader.h"

#include <arrow/io/interfaces.h>
#include <chrono>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

uint64_t nowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

} // namespace

bool CellShuffleReader::StreamSource::read(void* out, size_t n) {
  auto* dst = reinterpret_cast<uint8_t*>(out);
  if (peeked_ && n > 0) {
    *dst++ = peekByte_;
    --n;
    peeked_ = false;
  }
  while (n > 0) {
    const auto result = in_->Read(n, dst);
    BOLT_CHECK(
        result.ok(), "shuffle stream read failed: {}", result.status().ToString());
    const auto got = static_cast<size_t>(*result);
    if (got == 0) {
      ended_ = true;
      return false;
    }
    dst += got;
    n -= got;
  }
  return true;
}

bool CellShuffleReader::StreamSource::atEnd() const {
  if (peeked_) {
    return false;
  }
  if (ended_) {
    return true;
  }
  const auto result = in_->Read(1, &peekByte_);
  BOLT_CHECK(
      result.ok(), "shuffle stream read failed: {}", result.status().ToString());
  if (*result == 0) {
    ended_ = true;
    return true;
  }
  peeked_ = true;
  return false;
}

bool CellShuffleReader::CodecDecompressor::decompress(
    const uint8_t* data,
    size_t size,
    uint8_t* out,
    size_t decodedSize) {
  const uint64_t start = nowNs();
  const bool ok = codec_->decompress(
                      data,
                      static_cast<int64_t>(size),
                      out,
                      static_cast<int64_t>(decodedSize)) ==
      static_cast<int64_t>(decodedSize);
  decompressTimeNs_ += nowNs() - start;
  return ok;
}

CellShuffleReader::CellShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    CellLayout layout,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize)
    : streams_(std::move(streams)),
      layout_(std::move(layout)),
      decompressor_(
          codec == nullptr ? nullptr
                           : std::make_unique<CodecDecompressor>(codec)),
      arrowPool_(arrowPool),
      pool_(pool),
      batchSize_(batchSize),
      batchByteSize_(batchByteSize),
      decoder_(layout_, decompressor_.get(), pool_) {}

bool CellShuffleReader::nextDecoded(RowVectorPtr& out) {
  while (!exhausted_) {
    if (source_ == nullptr) {
      auto in = streams_->nextStream(arrowPool_);
      if (in == nullptr) {
        exhausted_ = true;
        return false;
      }
      source_ = std::make_unique<StreamSource>(std::move(in));
    }
    if (source_->atEnd()) {
      source_.reset();
      continue;
    }
    std::string error;
    const bool ok = decoder_.decode(*source_, out, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
    return true;
  }
  return false;
}

RowVectorPtr CellShuffleReader::concat(const std::vector<RowVectorPtr>& parts) {
  vector_size_t total = 0;
  for (const auto& part : parts) {
    total += part->size();
  }
  auto result = BaseVector::create<RowVector>(layout_.rowType(), total, pool_);
  vector_size_t offset = 0;
  for (const auto& part : parts) {
    result->copy(part.get(), offset, 0, part->size());
    offset += part->size();
  }
  return result;
}

RowVectorPtr CellShuffleReader::next() {
  const uint64_t start = nowNs();
  std::vector<RowVectorPtr> parts;
  int64_t pendingRows = 0;
  int64_t pendingBytes = 0;
  while (pendingRows < batchSize_ && pendingBytes < batchByteSize_) {
    RowVectorPtr decoded;
    if (!nextDecoded(decoded)) {
      break;
    }
    if (decoded->size() == 0) {
      continue;
    }
    pendingRows += decoded->size();
    pendingBytes += decoded->estimateFlatSize();
    parts.push_back(std::move(decoded));
  }
  decodeTimeNs_ += nowNs() - start;
  if (parts.empty()) {
    return nullptr;
  }
  if (parts.size() == 1) {
    return std::move(parts[0]);
  }
  return concat(parts);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
