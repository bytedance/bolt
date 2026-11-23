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

#include "bolt/shuffle/sparksql/base/ShuffleBlockReader.h"

#include <arrow/io/interfaces.h>
#include <folly/ScopeGuard.h>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql {

bool StreamByteSource::read(void* out, size_t n) {
  auto* dst = reinterpret_cast<uint8_t*>(out);
  if (state_ == State::kPeeked && n > 0) {
    *dst++ = peekByte_;
    --n;
    state_ = State::kOpen;
  }
  if (state_ == State::kEnded && n > 0) {
    return false;
  }
  while (n > 0) {
    const auto result = in_->Read(n, dst);
    BOLT_CHECK(
        result.ok(),
        "shuffle stream read failed: {}",
        result.status().ToString());
    const auto got = static_cast<size_t>(*result);
    if (got == 0) {
      state_ = State::kEnded;
      return false;
    }
    dst += got;
    n -= got;
  }
  return true;
}

bool StreamByteSource::atEnd() {
  if (state_ == State::kPeeked) {
    return false;
  }
  if (state_ == State::kEnded) {
    return true;
  }
  const auto result = in_->Read(1, &peekByte_);
  BOLT_CHECK(
      result.ok(),
      "shuffle stream read failed: {}",
      result.status().ToString());
  if (*result == 0) {
    state_ = State::kEnded;
    return true;
  }
  state_ = State::kPeeked;
  return false;
}

bool CodecDecompressor::decompress(
    std::span<const uint8_t> in,
    std::span<uint8_t> out) {
  NanosecondTimer timer(&decompressTimeNs_);
  const auto decodedSize = static_cast<int64_t>(out.size());
  return codec_->decompress(
             in.data(),
             static_cast<int64_t>(in.size()),
             out.data(),
             decodedSize) == decodedSize;
}

void RowBlockReader::read(
    ShuffleByteSource& in,
    RowBlock& out,
    ShuffleBlockKind kind) {
  out.rows.clear();
  out.rowBytes = 0;
  BufferPtr reusable;
  if (!out.buffers.empty()) {
    reusable = std::move(out.buffers[0]);
    out.buffers.clear();
  }
  decodeBlock(in, reusable, out.rows, out.rowBytes, kind);
  out.buffers.push_back(std::move(reusable));
}

void RowBlockReader::append(
    ShuffleByteSource& in,
    RowBlock& out,
    ShuffleBlockKind kind) {
  if (out.rows.empty()) {
    read(in, out, kind);
    return;
  }
  BufferPtr blockBuffer;
  decodeBlock(in, blockBuffer, out.rows, out.rowBytes, kind);
  out.buffers.push_back(std::move(blockBuffer));
}

void RowBlockReader::decodeBlock(
    ShuffleByteSource& in,
    BufferPtr& blockBuffer,
    std::vector<std::string_view>& rows,
    uint64_t& rowBytes,
    ShuffleBlockKind kind) {
  const std::string_view blockLabel = shuffleBlockKindName(kind);
  uint8_t rawHeader[kRowBlockHeaderBytes];
  rawHeader[0] = static_cast<uint8_t>(kind);
  if (!in.read(rawHeader + 1, kRowBlockHeaderBytes - 1)) {
    BOLT_FAIL(
        "malformed {} shuffle block: truncated {} block header",
        blockLabel,
        blockLabel);
  }
  const auto header = memory::loadUnaligned<RowBlockHeader>(rawHeader);
  const uint32_t rowCount = header.rowCount;
  const uint64_t rawBytes = header.rawBytes;
  const uint64_t stored = header.storedBytes;
  const RunLayout layout = header.layout;
  if (rowCount == 0 || rowCount > kMaxBlockRows) {
    BOLT_FAIL(
        "malformed {} shuffle block: {} block row count out of range",
        blockLabel,
        blockLabel);
  }
  if (rawBytes < 4ull * rowCount) {
    BOLT_FAIL(
        "malformed {} shuffle block: {} block smaller than its row headers",
        blockLabel,
        blockLabel);
  }
  if (layout == RunLayout::kCombinedUncompressed) {
    if (stored != rawBytes) {
      BOLT_FAIL(
          "malformed {} shuffle block: stored {} block size mismatch",
          blockLabel,
          blockLabel);
    }
  } else if (layout != RunLayout::kCombined || decompressor_ == nullptr) {
    BOLT_FAIL(
        "malformed {} shuffle block: unsupported {} block layout",
        blockLabel,
        blockLabel);
  }

  if (layout == RunLayout::kCombined) {
    if (stored == 0) {
      BOLT_FAIL(
          "malformed {} shuffle block: {} block decompression failed",
          blockLabel,
          blockLabel);
    }
    resetBuffer(compressed_, stored, pool_);
    if (!in.read(compressed_->asMutable<char>(), stored)) {
      BOLT_FAIL(
          "malformed {} shuffle block: truncated {} block body",
          blockLabel,
          blockLabel);
    }
    resetBuffer(blockBuffer, rawBytes, pool_);
    if (!decompressor_->decompress(
            {compressed_->as<uint8_t>(), stored},
            {blockBuffer->asMutable<uint8_t>(), rawBytes})) {
      BOLT_FAIL(
          "malformed {} shuffle block: {} block decompression failed",
          blockLabel,
          blockLabel);
    }
  } else {
    resetBuffer(blockBuffer, stored, pool_);
    if (!in.read(blockBuffer->asMutable<char>(), stored)) {
      BOLT_FAIL(
          "malformed {} shuffle block: truncated {} block body",
          blockLabel,
          blockLabel);
    }
  }

  const char* body = blockBuffer->as<char>();
  const size_t prevRows = rows.size();
  const uint64_t prevRowBytes = rowBytes;
  bool ok = false;
  SCOPE_EXIT {
    if (!ok) {
      rows.resize(prevRows);
      rowBytes = prevRowBytes;
    }
  };
  rows.reserve(checkedPlus(prevRows, static_cast<size_t>(rowCount)));
  uint64_t pos = 0;
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (rawBytes - pos < sizeof(int32_t)) {
      BOLT_FAIL(
          "malformed {} shuffle block: truncated {} row header",
          blockLabel,
          blockLabel);
    }
    const int32_t size = framedRowSize(body + pos);
    pos += sizeof(int32_t);
    if (size < 0 || static_cast<uint64_t>(size) > rawBytes - pos) {
      BOLT_FAIL(
          "malformed {} shuffle block: {} row out of bounds",
          blockLabel,
          blockLabel);
    }
    rows.emplace_back(body + pos, static_cast<size_t>(size));
    rowBytes += static_cast<uint64_t>(size);
    pos += size;
  }
  if (pos != rawBytes) {
    BOLT_FAIL(
        "malformed {} shuffle block: {} block length mismatch",
        blockLabel,
        blockLabel);
  }
  ok = true;
}

} // namespace bytedance::bolt::shuffle::sparksql
