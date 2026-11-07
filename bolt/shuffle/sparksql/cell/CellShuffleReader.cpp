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
#include <fmt/format.h>
#include <folly/lang/Bits.h>
#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellNestedAssembler.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

bool CellShuffleReader::StreamSource::read(void* out, size_t n) {
  auto* dst = reinterpret_cast<uint8_t*>(out);
  // Counted up front; a short read takes back the bytes it did not give.
  consumed_ += n;
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
      consumed_ -= n;
      return false;
    }
    dst += got;
    n -= got;
  }
  return true;
}

size_t CellShuffleReader::StreamSource::readAhead(
    uint8_t* out,
    size_t n) noexcept {
  size_t got = 0;
  if (peeked_ && n > 0) {
    out[got++] = peekByte_;
    peeked_ = false;
  }
  try {
    while (got < n && !ended_) {
      const auto result = in_->Read(static_cast<int64_t>(n - got), out + got);
      if (!result.ok() || *result <= 0) {
        break;
      }
      got += static_cast<size_t>(*result);
    }
  } catch (...) {
    // Best effort: the bytes read so far are all there is.
  }
  return got;
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
    std::span<const uint8_t> in,
    std::span<uint8_t> out) {
  const uint64_t start = currentTimeNs();
  const auto decodedSize = static_cast<int64_t>(out.size());
  const bool ok = codec_->decompress(
                      in.data(),
                      static_cast<int64_t>(in.size()),
                      out.data(),
                      decodedSize) == decodedSize;
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
    bool nativeNestedTypes)
    : streams_(std::move(streams)),
      adapter_(outputType),
      decompressor_(
          codec == nullptr ? nullptr
                           : std::make_unique<CodecDecompressor>(codec)),
      arrowPool_(arrowPool),
      pool_(pool),
      batchSize_(batchSize),
      batchByteSize_(batchByteSize),
      decoder_(
          CellLayout::create(adapter_.physicalType()),
          decompressor_.get(),
          pool_),
      nested_(
          nativeNestedTypes
              ? makeCellNestedAssembler(outputType, decompressor_.get(), pool_)
              : nullptr),
      compositeConverter_(outputType, pool_) {}

CellShuffleReader::NextBlock CellShuffleReader::nextBlock() {
  while (!exhausted_) {
    if (source_ == nullptr) {
      auto in = streams_->nextStream(arrowPool_);
      if (in == nullptr) {
        exhausted_ = true;
        return NextBlock::kEnd;
      }
      source_ = std::make_unique<StreamSource>(std::move(in));
      ++streamsOpened_;
      streamBlocks_ = 0;
      hasLastPayload_ = false;
    }
    if (source_->atEnd()) {
      source_.reset();
      continue;
    }
    std::string error;
    // A failed check below names where the block is and what came before
    // it: blockDiagnostics() runs only when one fails.
    const uint64_t blockStart = source_->consumed();
    uint8_t kind = 0;
    BOLT_CHECK(
        source_->read(&kind, 1),
        "truncated cell shuffle block{}",
        blockDiagnostics(blockStart, kind));
    if (kind == static_cast<uint8_t>(CellBlockKind::kCompositeRows)) {
      const bool ok = readCompositeBlock(error);
      BOLT_CHECK(
          ok,
          "malformed composite shuffle block: {}{}",
          error,
          blockDiagnostics(blockStart, kind));
      ++streamBlocks_;
      return NextBlock::kCompositeRows;
    }
    BOLT_CHECK_EQ(
        kind,
        static_cast<uint8_t>(CellBlockKind::kPayload),
        "unknown cell shuffle block kind{}",
        blockDiagnostics(blockStart, kind));
    const bool ok = nested_ != nullptr ? nested_->beginPayload(*source_, error)
                                       : decoder_.beginPayload(*source_, error);
    BOLT_CHECK(
        ok,
        "malformed cell shuffle payload: {}{}",
        error,
        blockDiagnostics(blockStart, kind));
    hasLastPayload_ = true;
    lastPayloadStart_ = blockStart;
    lastPayloadEnd_ = source_->consumed();
    const auto header = nested_ != nullptr
        ? nested_->payloadHeader()
        : std::span<const uint8_t>(decoder_.payloadHeader());
    ::memcpy(lastPayloadHeader_, header.data(), sizeof(lastPayloadHeader_));
    ++streamBlocks_;
    return NextBlock::kPayload;
  }
  return NextBlock::kEnd;
}

namespace {

/// `n` bytes as hex, space separated; "none" when there are none.
std::string hexBytes(const uint8_t* bytes, size_t n) {
  if (n == 0) {
    return "none";
  }
  std::string out;
  out.reserve(3 * n);
  for (size_t i = 0; i < n; ++i) {
    fmt::format_to(
        std::back_inserter(out), "{}{:02x}", i == 0 ? "" : " ", bytes[i]);
  }
  return out;
}

} // namespace

std::string CellShuffleReader::blockDiagnostics(
    uint64_t blockStart,
    uint8_t kind) {
  const uint64_t readTo = source_->consumed();
  auto out = fmt::format(
      " [stream {} ({} opened), block {} at offset {}, read to offset {}",
      streamsOpened_ - 1,
      streamsOpened_,
      streamBlocks_,
      blockStart,
      readTo);
  if (hasLastPayload_) {
    const auto field = [&](size_t at) {
      return folly::loadUnaligned<uint32_t>(lastPayloadHeader_ + at);
    };
    out += fmt::format(
        "; previous payload at offset {} to {}: rows={} runCount={} "
        "nullStoredSize={} nullDecodedSize={}",
        lastPayloadStart_,
        lastPayloadEnd_,
        field(0),
        field(4),
        field(16),
        field(20));
  } else {
    out += "; no previous payload in this stream";
  }
  // The block's bytes read: its kind byte, then as much of its header.
  uint8_t header[1 + kPayloadFixedHeaderBytes];
  static_assert(sizeof(header) >= 1 + sizeof(compositeHeader_));
  size_t headerBytes = 0;
  if (readTo > blockStart) {
    header[headerBytes++] = kind;
    std::span<const uint8_t> fixed;
    if (kind == static_cast<uint8_t>(CellBlockKind::kPayload)) {
      fixed = nested_ != nullptr
          ? nested_->payloadHeader()
          : std::span<const uint8_t>(decoder_.payloadHeader());
    } else if (kind == static_cast<uint8_t>(CellBlockKind::kCompositeRows)) {
      fixed = compositeHeader_;
    }
    const auto n = static_cast<size_t>(
        std::min<uint64_t>(readTo - blockStart - 1, fixed.size()));
    if (n > 0) {
      ::memcpy(header + 1, fixed.data(), n);
      headerBytes += n;
    }
  }
  uint8_t next[32];
  const size_t numNext = source_->readAhead(next, sizeof(next));
  out += fmt::format(
      "; block header bytes: {}; next {} bytes: {}]",
      hexBytes(header, headerBytes),
      numNext,
      hexBytes(next, numNext));
  return out;
}

bool CellShuffleReader::readCompositeBlock(std::string& error) {
  const auto fail = [&](const char* message) {
    error = message;
    return false;
  };
  compositeRows_.clear();
  nextCompositeRow_ = 0;
  // Read into the member, which a malformed block's error shows.
  const uint8_t* const header = compositeHeader_;
  if (!source_->read(compositeHeader_, sizeof(compositeHeader_))) {
    return fail("truncated composite block header");
  }
  const auto rowCount = folly::loadUnaligned<uint32_t>(header);
  const auto decodedBytes = folly::loadUnaligned<uint64_t>(header + 4);
  const auto stored = folly::loadUnaligned<uint64_t>(header + 12);
  const auto layout = static_cast<RunLayout>(header[20]);
  if (rowCount == 0 || rowCount > kMaxPayloadRows) {
    return fail("composite block row count out of range");
  }
  if (decodedBytes < 4ull * rowCount) {
    return fail("composite block smaller than its row headers");
  }
  if (stored > source_->remainingBound()) {
    return fail("composite block out of bounds");
  }
  if (layout == RunLayout::kCombinedUncompressed) {
    if (stored != decodedBytes) {
      return fail("stored composite block size mismatch");
    }
  } else if (layout != RunLayout::kCombined || decompressor_ == nullptr) {
    return fail("unsupported composite block layout");
  }

  resetBuffer(compositeStored_, stored, pool_);
  if (stored > 0 &&
      !source_->read(compositeStored_->asMutable<char>(), stored)) {
    return fail("truncated composite block body");
  }
  const char* body = compositeStored_->as<char>();
  if (layout == RunLayout::kCombined) {
    resetBuffer(compositeDecoded_, decodedBytes, pool_);
    if (!decompressor_->decompress(
            {compositeStored_->as<uint8_t>(), stored},
            {compositeDecoded_->asMutable<uint8_t>(), decodedBytes})) {
      return fail("composite block decompression failed");
    }
    body = compositeDecoded_->as<char>();
  }

  std::vector<std::string_view> rows;
  rows.reserve(rowCount);
  uint64_t pos = 0;
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (decodedBytes - pos < sizeof(int32_t)) {
      return fail("truncated composite row header");
    }
    const int32_t size = compositeRowSize(body + pos);
    pos += sizeof(int32_t);
    if (size < 0 || static_cast<uint64_t>(size) > decodedBytes - pos) {
      return fail("composite row out of bounds");
    }
    rows.emplace_back(body + pos, static_cast<size_t>(size));
    pos += size;
  }
  if (pos != decodedBytes) {
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
  auto output = compositeConverter_.convertToComposite(
      slice, static_cast<int32_t>(total));
  decodeTimeNs_ += currentTimeNs() - start;
  return output;
}

RowVectorPtr CellShuffleReader::next() {
  // A composite block met while batching payloads goes out next.
  if (hasCompositeRows()) {
    return nextComposite();
  }
  RowVectorPtr output;
  if (nested_ != nullptr) {
    const uint64_t start = currentTimeNs();
    output = nested_->nextBatch(std::max(batchSize_, 1), batchByteSize_, [&] {
      return nextBlock() == NextBlock::kPayload;
    });
    decodeTimeNs_ += currentTimeNs() - start;
  } else {
    output = nextPhysical();
    if (output != nullptr && adapter_.hasComplexColumns()) {
      const uint64_t start = currentTimeNs();
      output = adapter_.unpack(output, pool_);
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
  const vector_size_t capacity = std::max(batchSize_, 1);
  // One batch at a time: a large payload comes out over several calls,
  // small ones fill the same batch until it is full. An empty batch always
  // takes one append, so a byte target at or below zero reads one append
  // per batch.
  while (decoder_.batchRows() < capacity &&
         (decoder_.batchRows() == 0 ||
          static_cast<int64_t>(decoder_.batchBytes()) < batchByteSize_)) {
    const uint32_t left = decoder_.payloadRowsLeft();
    if (left == 0) {
      if (nextBlock() != NextBlock::kPayload) {
        // End of input, or a composite block that next() emits on its own.
        break;
      }
      continue;
    }
    const auto rows = static_cast<uint32_t>(
        std::min<int64_t>(left, capacity - decoder_.batchRows()));
    std::string error;
    const bool ok = decoder_.appendRows(rows, capacity, error);
    BOLT_CHECK(ok, "malformed cell shuffle payload: {}", error);
  }
  auto output = decoder_.finishBatch();
  decodeTimeNs_ += currentTimeNs() - start;
  return output;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
