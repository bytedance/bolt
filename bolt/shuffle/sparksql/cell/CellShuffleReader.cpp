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
#include <cstring>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

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
        result.ok(),
        "shuffle stream read failed: {}",
        result.status().ToString());
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
      result.ok(),
      "shuffle stream read failed: {}",
      result.status().ToString());
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
  const uint64_t start = currentTimeNs();
  const bool ok = codec_->decompress(
                      data,
                      static_cast<int64_t>(size),
                      out,
                      static_cast<int64_t>(decodedSize)) ==
      static_cast<int64_t>(decodedSize);
  decompressTimeNs_ += currentTimeNs() - start;
  return ok;
}

CellShuffleReader::CellShuffleReader(
    std::shared_ptr<ReaderStreamIterator> streams,
    const RowTypePtr& outputType,
    Codec* codec,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    int32_t batchSize,
    int64_t batchByteSize,
    bool compositeInput)
    : streams_(std::move(streams)),
      adapter_(outputType),
      layout_(CellLayout::create(adapter_.physicalType())),
      decompressor_(
          codec == nullptr ? nullptr
                           : std::make_unique<CodecDecompressor>(codec)),
      arrowPool_(arrowPool),
      pool_(pool),
      batchSize_(batchSize),
      batchByteSize_(batchByteSize),
      decoder_(layout_, decompressor_.get(), pool_) {
  if (compositeInput) {
    compositeConverter_ =
        std::make_unique<ShuffleRowToColumnarConverter>(outputType, pool_);
  }
}

CellShuffleReader::Decoded CellShuffleReader::nextDecoded(RowVectorPtr& out) {
  while (!exhausted_) {
    if (source_ == nullptr) {
      auto in = streams_->nextStream(arrowPool_);
      if (in == nullptr) {
        exhausted_ = true;
        return Decoded::kEnd;
      }
      source_ = std::make_unique<StreamSource>(std::move(in));
    }
    if (source_->atEnd()) {
      source_.reset();
      continue;
    }
    std::string error;
    if (compositeConverter_ != nullptr) {
      uint8_t kind;
      BOLT_CHECK(source_->read(&kind, 1), "truncated cell shuffle block");
      if (kind == static_cast<uint8_t>(CellBlockKind::kCompositeRows)) {
        const bool ok = decodeComposite(error);
        BOLT_CHECK(ok, "malformed composite shuffle block: {}", error);
        return Decoded::kComposite;
      }
      BOLT_CHECK_EQ(
          kind,
          static_cast<uint8_t>(CellBlockKind::kPayload),
          "unknown cell shuffle block kind");
    }
    const bool ok = decoder_.decode(*source_, out, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
    return Decoded::kPayload;
  }
  return Decoded::kEnd;
}

bool CellShuffleReader::decodeComposite(std::string& error) {
  const auto fail = [&](const char* message) {
    error = message;
    return false;
  };
  compositeRows_.clear();
  nextCompositeRow_ = 0;
  uint8_t header[kCompositeBlockHeaderBytes - 1];
  if (!source_->read(header, sizeof(header))) {
    return fail("truncated composite block header");
  }
  uint32_t rowCount;
  uint64_t rawBytes;
  uint64_t stored;
  ::memcpy(&rowCount, header, 4);
  ::memcpy(&rawBytes, header + 4, 8);
  ::memcpy(&stored, header + 12, 8);
  const auto layout = static_cast<RunLayout>(header[20]);
  const CellDecodeLimits limits;
  if (rowCount == 0 || rowCount > limits.maxRowCount) {
    return fail("composite block row count out of range");
  }
  if (rawBytes < 4ull * rowCount) {
    return fail("composite block smaller than its row headers");
  }
  if (stored > source_->remainingBound()) {
    return fail("composite block out of bounds");
  }
  if (layout == RunLayout::kCombinedStored) {
    if (stored != rawBytes) {
      return fail("stored composite block size mismatch");
    }
  } else if (layout != RunLayout::kCombined || decompressor_ == nullptr) {
    return fail("unsupported composite block layout");
  }

  resizeBuffer(compositeStored_, 0, pool_);
  resizeBuffer(compositeStored_, stored, pool_);
  if (stored > 0 &&
      !source_->read(compositeStored_->asMutable<char>(), stored)) {
    return fail("truncated composite block body");
  }
  const char* raw = compositeStored_->as<char>();
  if (layout == RunLayout::kCombined) {
    resizeBuffer(compositeRaw_, 0, pool_);
    resizeBuffer(compositeRaw_, rawBytes, pool_);
    if (!decompressor_->decompress(
            compositeStored_->as<uint8_t>(),
            stored,
            compositeRaw_->asMutable<uint8_t>(),
            rawBytes)) {
      return fail("composite block decompression failed");
    }
    raw = compositeRaw_->as<char>();
  }

  std::vector<std::string_view> rows;
  rows.reserve(rowCount);
  uint64_t pos = 0;
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (rawBytes - pos < 4) {
      return fail("truncated composite row header");
    }
    int32_t size;
    ::memcpy(&size, raw + pos, 4);
    pos += 4;
    if (size < 0 || static_cast<uint64_t>(size) > rawBytes - pos) {
      return fail("composite row out of bounds");
    }
    rows.emplace_back(raw + pos, static_cast<size_t>(size));
    pos += size;
  }
  if (pos != rawBytes) {
    return fail("composite block length mismatch");
  }
  compositeRows_ = std::move(rows);
  return true;
}

RowVectorPtr CellShuffleReader::nextComposite() {
  const uint64_t start = currentTimeNs();
  const size_t end = std::min(
      compositeRows_.size(),
      nextCompositeRow_ + static_cast<size_t>(std::max(batchSize_, 1)));
  std::vector<std::string_view> slice(
      compositeRows_.begin() + nextCompositeRow_, compositeRows_.begin() + end);
  uint64_t total = 0;
  for (const auto& row : slice) {
    total += row.size();
  }
  BOLT_CHECK_LE(
      total,
      std::numeric_limits<int32_t>::max(),
      "composite rows too large for one vector");
  nextCompositeRow_ = end;
  auto output = compositeConverter_->convertToComposite(
      slice, static_cast<int32_t>(total));
  decodeTimeNs_ += currentTimeNs() - start;
  return output;
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
  // A composite block met while batching payloads goes out next.
  if (hasCompositeRows()) {
    return nextComposite();
  }
  RowVectorPtr output;
  if (!adapter_.hasComplexColumns()) {
    output = nextPhysical();
  } else {
    if (pending_ == nullptr) {
      pending_ = nextPhysical();
      pendingOffset_ = 0;
    }
    if (pending_ != nullptr) {
      const uint64_t start = currentTimeNs();
      output = adapter_.decodeNext(pending_, pendingOffset_, pool_);
      if (pendingOffset_ == pending_->size()) {
        pending_.reset();
      }
      decodeTimeNs_ += currentTimeNs() - start;
    }
  }
  if (output == nullptr && hasCompositeRows()) {
    return nextComposite();
  }
  return output;
}

RowVectorPtr CellShuffleReader::nextPhysical() {
  const uint64_t start = currentTimeNs();
  std::vector<RowVectorPtr> parts;
  int64_t pendingRows = 0;
  int64_t pendingBytes = 0;
  while (pendingRows < batchSize_ && pendingBytes < batchByteSize_) {
    RowVectorPtr decoded;
    const auto kind = nextDecoded(decoded);
    if (kind != Decoded::kPayload) {
      // End of input, or a composite block that next() emits on its own.
      break;
    }
    if (decoded->size() == 0) {
      continue;
    }
    pendingRows += decoded->size();
    pendingBytes += decoded->estimateFlatSize();
    parts.push_back(std::move(decoded));
  }
  decodeTimeNs_ += currentTimeNs() - start;
  if (parts.empty()) {
    return nullptr;
  }
  if (parts.size() == 1) {
    return std::move(parts[0]);
  }
  return concat(parts);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
