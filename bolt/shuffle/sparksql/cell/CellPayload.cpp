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

#include "bolt/shuffle/sparksql/cell/CellPayload.h"

#include <algorithm>
#include <string_view>

#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

// Keep reusable storage on the reader pool and grow only when necessary.
char* resizeBuffer(BufferPtr& buffer, size_t size, memory::MemoryPool* pool) {
  if (buffer == nullptr) {
    if (size == 0) {
      return nullptr;
    }
    buffer = AlignedBuffer::allocate<char>(size, pool);
  } else if (size > buffer->capacity()) {
    AlignedBuffer::reallocate<char>(
        &buffer,
        std::max(size, checkedMultiply<size_t>(buffer->capacity(), 2)));
  }
  buffer->setSize(size);
  return buffer->asMutable<char>();
}

std::string_view bufferBytes(const BufferPtr& buffer) {
  return buffer == nullptr
      ? std::string_view{}
      : std::string_view(buffer->as<char>(), buffer->size());
}

inline uint32_t loadU32(const uint8_t* p) {
  uint32_t v;
  ::memcpy(&v, p, 4);
  return v;
}

inline uint64_t loadU64(const uint8_t* p) {
  uint64_t v;
  ::memcpy(&v, p, 8);
  return v;
}

inline bool addOverflows(uint64_t a, uint64_t b, uint64_t& out) {
  return __builtin_add_overflow(a, b, &out);
}

/// Set bits among the first `rowCount` bits.
uint32_t popcountBits(const uint8_t* bits, uint32_t rowCount) {
  uint32_t count = 0;
  const uint32_t fullBytes = rowCount / 8;
  for (uint32_t i = 0; i < fullBytes; ++i) {
    count += __builtin_popcount(bits[i]);
  }
  if ((rowCount & 7) != 0) {
    const uint8_t mask = static_cast<uint8_t>((1u << (rowCount & 7)) - 1);
    count += __builtin_popcount(bits[fullBytes] & mask);
  }
  return count;
}

inline bool bitSet(const uint8_t* bits, uint32_t row) {
  return (bits[row / 8] >> (row % 8)) & 1;
}

} // namespace

CellPayloadDecoder::CellPayloadDecoder(
    CellLayout layout,
    CellDecompressor* decompressor,
    memory::MemoryPool* pool,
    CellDecodeLimits limits)
    : layout_(std::move(layout)),
      decompressor_(decompressor),
      pool_(pool),
      limits_(limits),
      streamBytes_(layout_.numStreams()) {
  tags_.resize(layout_.numColumns());
  nonNullCount_.resize(layout_.numColumns());
  bitmaps_.resize(layout_.numColumns());
}

bool CellPayloadDecoder::parseNullRegion(
    CellByteSource& in,
    uint32_t rowCount,
    uint32_t nullStoredSize,
    uint32_t nullDecodedSize,
    std::string& error) {
  if (nullStoredSize > in.remainingBound()) {
    return fail(error, "truncated null body");
  }
  if (nullDecodedSize == 0) {
    auto* nullData = resizeBuffer(nullBody_, nullStoredSize, pool_);
    if (!in.read(nullData, nullStoredSize)) {
      return fail(error, "truncated null body");
    }
  } else {
    if (decompressor_ == nullptr) {
      return fail(error, "compressed null body without a codec");
    }
    if (limits_.maxDecodedBytes != 0 &&
        nullDecodedSize > limits_.maxDecodedBytes) {
      return fail(error, "null decoded size beyond sanity bound");
    }
    auto* stored = resizeBuffer(scratch_, nullStoredSize, pool_);
    if (!in.read(stored, nullStoredSize)) {
      return fail(error, "truncated null body");
    }
    auto* decoded = resizeBuffer(nullBody_, nullDecodedSize, pool_);
    if (!decompressor_->decompress(
            reinterpret_cast<const uint8_t*>(stored),
            nullStoredSize,
            reinterpret_cast<uint8_t*>(decoded),
            nullDecodedSize)) {
      // Rule 5: the decompressed length must match exactly.
      return fail(error, "null body decompression failed");
    }
  }

  const uint32_t numColumns = layout_.numColumns();
  const uint32_t tagBytes = nullTagBytes(layout_.numWireColumns());
  const auto nullBody = bufferBytes(nullBody_);
  const auto* nullData = reinterpret_cast<const uint8_t*>(nullBody.data());
  if (nullBody.size() < tagBytes) {
    return fail(error, "null body shorter than its tags");
  }
  uint32_t rawNullColumns = 0;
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto tag = layout_.isUnknownColumn(col)
        ? NullTag::kAllNull
        : getNullTag(nullData, layout_.wireColumn(col));
    if (tag == NullTag::kReserved) {
      return fail(error, "reserved null tag"); // rule 7
    }
    tags_[col] = tag;
    rawNullColumns += tag == NullTag::kRawNull ? 1 : 0;
  }
  // Unused high bits of the last tags byte must be zero.
  if ((layout_.numWireColumns() % 4) != 0) {
    const uint8_t tail = nullData[tagBytes - 1];
    if ((tail >> ((layout_.numWireColumns() % 4) * 2)) != 0) {
      return fail(error, "non-zero unused null tag bits");
    }
  }

  const uint64_t bitmapBytes = (static_cast<uint64_t>(rowCount) + 7) / 8;
  const uint64_t expected =
      tagBytes + static_cast<uint64_t>(rawNullColumns) * bitmapBytes;
  if (nullBody.size() != expected) {
    return fail(error, "null body length mismatch"); // rule 6
  }

  const uint8_t* cursor = tagBytes == 0 ? nullptr : nullData + tagBytes;
  for (uint32_t col = 0; col < numColumns; ++col) {
    switch (tags_[col]) {
      case NullTag::kAllNull:
        nonNullCount_[col] = 0;
        bitmaps_[col] = nullptr;
        break;
      case NullTag::kNoNull:
        nonNullCount_[col] = rowCount;
        bitmaps_[col] = nullptr;
        break;
      case NullTag::kRawNull:
        bitmaps_[col] = cursor;
        nonNullCount_[col] = popcountBits(cursor, rowCount);
        cursor += bitmapBytes;
        break;
      case NullTag::kReserved:
        break;
    }
  }
  return true;
}

bool CellPayloadDecoder::parseRun(CellByteSource& in, std::string& error) {
  uint8_t layoutByte;
  if (!in.read(&layoutByte, 1)) {
    return fail(error, "truncated run header");
  }
  if (layoutByte > static_cast<uint8_t>(RunLayout::kCombinedStored)) {
    return fail(error, "unknown run compression layout"); // rule 8
  }
  const auto runLayout = static_cast<RunLayout>(layoutByte);
  const uint32_t numStreams = layout_.numStreams();
  const uint32_t storedCount =
      runLayout == RunLayout::kSeparate ? numStreams : 1;

  storedSizes_.resize(storedCount);
  if (!in.read(storedSizes_.data(), storedCount * sizeof(uint64_t))) {
    return fail(error, "truncated run stored sizes");
  }
  decodedSizes_.resize(numStreams);
  if (!in.read(decodedSizes_.data(), numStreams * sizeof(uint64_t))) {
    return fail(error, "truncated run decoded sizes");
  }

  // Rule 9: sums must not overflow.
  uint64_t storedSum = 0;
  for (const auto size : storedSizes_) {
    if (addOverflows(storedSum, size, storedSum)) {
      return fail(error, "run stored sizes overflow");
    }
  }
  // Rule 9: the run's declared wire bytes must fit the remaining input, and
  // the check comes before any allocation sized from those fields.
  if (storedSum > in.remainingBound()) {
    return fail(error, "run larger than the remaining input");
  }
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes_) {
    if (addOverflows(decodedSum, size, decodedSum)) {
      return fail(error, "run decoded sizes overflow");
    }
  }
  if (limits_.maxDecodedBytes != 0 &&
      (decodedSum > limits_.maxDecodedBytes ||
       storedSum > limits_.maxDecodedBytes)) {
    return fail(error, "run size beyond sanity bound");
  }

  switch (runLayout) {
    case RunLayout::kSeparate:
      for (uint32_t s = 0; s < numStreams; ++s) {
        const uint64_t stored = storedSizes_[s];
        const uint64_t decoded = decodedSizes_[s];
        if (stored == 0) {
          if (decoded != 0) {
            return fail(error, "empty stored buffer with decoded size");
          }
          continue; // rule: empty stream, the codec is not invoked
        }
        auto& stream = streamBytes_[s];
        if (decoded == 0) {
          // Uncompressed: bytes go to the stream as they are.
          const size_t offset = stream == nullptr ? 0 : stream->size();
          auto* out =
              resizeBuffer(stream, checkedPlus<size_t>(offset, stored), pool_);
          if (!in.read(out + offset, stored)) {
            return fail(error, "truncated run buffer");
          }
        } else {
          if (decompressor_ == nullptr) {
            return fail(error, "compressed buffer without a codec");
          }
          auto* input = resizeBuffer(scratch_, stored, pool_);
          if (!in.read(input, stored)) {
            return fail(error, "truncated run buffer");
          }
          const size_t offset = stream == nullptr ? 0 : stream->size();
          auto* out =
              resizeBuffer(stream, checkedPlus<size_t>(offset, decoded), pool_);
          if (!decompressor_->decompress(
                  reinterpret_cast<const uint8_t*>(input),
                  stored,
                  reinterpret_cast<uint8_t*>(out) + offset,
                  decoded)) {
            return fail(error, "run buffer decompression failed"); // rule 11
          }
        }
      }
      return true;

    case RunLayout::kCombinedStored:
      // Rule 12: the single stored buffer is the concatenation itself.
      if (storedSizes_[0] != decodedSum) {
        return fail(error, "combined-stored size mismatch");
      }
      for (uint32_t s = 0; s < numStreams; ++s) {
        const uint64_t decoded = decodedSizes_[s];
        if (decoded == 0) {
          continue;
        }
        auto& stream = streamBytes_[s];
        const size_t offset = stream == nullptr ? 0 : stream->size();
        auto* out =
            resizeBuffer(stream, checkedPlus<size_t>(offset, decoded), pool_);
        if (!in.read(out + offset, decoded)) {
          return fail(error, "truncated run buffer");
        }
      }
      return true;

    case RunLayout::kCombined: {
      if (decompressor_ == nullptr) {
        return fail(error, "compressed buffer without a codec");
      }
      auto* input = resizeBuffer(scratch_, storedSizes_[0], pool_);
      if (!in.read(input, storedSizes_[0])) {
        return fail(error, "truncated run buffer");
      }
      auto* decodedData = resizeBuffer(scratch2_, decodedSum, pool_);
      if (!decompressor_->decompress(
              reinterpret_cast<const uint8_t*>(input),
              storedSizes_[0],
              reinterpret_cast<uint8_t*>(decodedData),
              decodedSum)) {
        return fail(error, "combined buffer decompression failed"); // rule 12
      }
      size_t offset = 0;
      for (uint32_t s = 0; s < numStreams; ++s) {
        if (decodedSizes_[s] == 0) {
          continue;
        }
        auto& stream = streamBytes_[s];
        const size_t streamOffset = stream == nullptr ? 0 : stream->size();
        auto* out = resizeBuffer(
            stream, checkedPlus<size_t>(streamOffset, decodedSizes_[s]), pool_);
        ::memcpy(out + streamOffset, decodedData + offset, decodedSizes_[s]);
        offset += decodedSizes_[s];
      }
      return true;
    }
  }
  return fail(error, "unreachable run layout");
}

BufferPtr CellPayloadDecoder::makeNulls(uint32_t col, uint32_t rowCount) {
  if (tags_[col] == NullTag::kNoNull) {
    return nullptr;
  }
  auto nulls = AlignedBuffer::allocate<bool>(rowCount, pool_);
  auto* raw = nulls->asMutable<uint8_t>();
  const uint32_t bytes = (rowCount + 7) / 8;
  if (tags_[col] == NullTag::kAllNull) {
    ::memset(raw, 0, bytes);
  } else {
    ::memcpy(raw, bitmaps_[col], bytes);
  }
  return nulls;
}

template <typename T>
bool CellPayloadDecoder::buildEncodedColumn(
    uint32_t col,
    uint32_t rowCount,
    const TypePtr& type,
    VectorPtr& out,
    std::string& error) {
  const auto bytes = bufferBytes(streamBytes_[layout_.columnStream(col)]);
  const uint32_t nonNull = nonNullCount_[col];
  auto values = AlignedBuffer::allocate<T>(rowCount, pool_);
  auto* rawValues = values->template asMutable<T>();

  if (nonNull == rowCount) {
    if (!decodeStream<T>(
            reinterpret_cast<const uint8_t*>(bytes.data()),
            bytes.size(),
            nonNull,
            rawValues)) {
      return fail(error, "malformed value stream");
    }
  } else {
    lengthScratch_.resize(0);
    std::vector<T> dense(nonNull);
    if (!decodeStream<T>(
            reinterpret_cast<const uint8_t*>(bytes.data()),
            bytes.size(),
            nonNull,
            dense.data())) {
      return fail(error, "malformed value stream");
    }
    const uint8_t* bitmap = bitmaps_[col];
    uint32_t next = 0;
    for (uint32_t row = 0; row < rowCount; ++row) {
      rawValues[row] =
          bitmap != nullptr && bitSet(bitmap, row) ? dense[next++] : T{};
    }
  }
  out = std::make_shared<FlatVector<T>>(
      pool_,
      type,
      makeNulls(col, rowCount),
      rowCount,
      std::move(values),
      std::vector<BufferPtr>{});
  return true;
}

template <typename T>
bool CellPayloadDecoder::buildRawColumn(
    uint32_t col,
    uint32_t rowCount,
    const TypePtr& type,
    VectorPtr& out,
    std::string& error) {
  const auto bytes = bufferBytes(streamBytes_[layout_.columnStream(col)]);
  const uint32_t nonNull = nonNullCount_[col];
  if (bytes.size() != static_cast<size_t>(nonNull) * sizeof(T)) {
    return fail(error, "raw stream length mismatch");
  }
  auto values = AlignedBuffer::allocate<T>(rowCount, pool_);
  auto* rawValues = values->template asMutable<T>();
  const T* dense = reinterpret_cast<const T*>(bytes.data());

  if (nonNull == rowCount) {
    ::memcpy(rawValues, dense, bytes.size());
  } else {
    const uint8_t* bitmap = bitmaps_[col];
    uint32_t next = 0;
    for (uint32_t row = 0; row < rowCount; ++row) {
      rawValues[row] =
          bitmap != nullptr && bitSet(bitmap, row) ? dense[next++] : T{};
    }
  }
  out = std::make_shared<FlatVector<T>>(
      pool_,
      type,
      makeNulls(col, rowCount),
      rowCount,
      std::move(values),
      std::vector<BufferPtr>{});
  return true;
}

bool CellPayloadDecoder::buildBooleanColumn(
    uint32_t col,
    uint32_t rowCount,
    const TypePtr& type,
    VectorPtr& out,
    std::string& error) {
  const auto bytes = bufferBytes(streamBytes_[layout_.columnStream(col)]);
  const uint32_t nonNull = nonNullCount_[col];
  if (bytes.size() != nonNull) {
    return fail(error, "raw stream length mismatch");
  }
  const auto* dense = reinterpret_cast<const uint8_t*>(bytes.data());
  auto values = AlignedBuffer::allocate<bool>(rowCount, pool_);
  auto* rawValues = values->asMutable<uint64_t>();
  const uint8_t* bitmap = nonNull == rowCount ? nullptr : bitmaps_[col];
  uint32_t next = 0;
  for (uint32_t row = 0; row < rowCount; ++row) {
    bool value = false;
    if (nonNull == rowCount || (bitmap != nullptr && bitSet(bitmap, row))) {
      const uint8_t byte = dense[next++];
      if (byte > 1) {
        return fail(error, "invalid boolean byte");
      }
      value = byte != 0;
    }
    bits::setBit(rawValues, row, value);
  }
  out = std::make_shared<FlatVector<bool>>(
      pool_,
      type,
      makeNulls(col, rowCount),
      rowCount,
      std::move(values),
      std::vector<BufferPtr>{});
  return true;
}

bool CellPayloadDecoder::buildTimestampColumn(
    uint32_t col,
    uint32_t rowCount,
    const TypePtr& type,
    VectorPtr& out,
    std::string& error) {
  const uint32_t secondsStream = layout_.columnStream(col);
  const auto secondsBytes = bufferBytes(streamBytes_[secondsStream]);
  const auto nanosBytes = bufferBytes(streamBytes_[secondsStream + 1]);
  const uint32_t nonNull = nonNullCount_[col];
  auto* dense = reinterpret_cast<int64_t*>(resizeBuffer(
      timestampScratch_,
      checkedMultiply<size_t>(2 * sizeof(int64_t), nonNull),
      pool_));
  int64_t* seconds = dense;
  int64_t* nanos = dense + nonNull;
  if (!decodeStream<int64_t>(
          reinterpret_cast<const uint8_t*>(secondsBytes.data()),
          secondsBytes.size(),
          nonNull,
          seconds) ||
      !decodeStream<int64_t>(
          reinterpret_cast<const uint8_t*>(nanosBytes.data()),
          nanosBytes.size(),
          nonNull,
          nanos)) {
    return fail(error, "malformed timestamp stream");
  }
  for (uint32_t i = 0; i < nonNull; ++i) {
    // Untrusted values must not reach the Timestamp constructor's range
    // checks, which abort in debug builds.
    if (seconds[i] < Timestamp::kMinSeconds ||
        seconds[i] > Timestamp::kMaxSeconds || nanos[i] < 0 ||
        nanos[i] > static_cast<int64_t>(Timestamp::kMaxNanos)) {
      return fail(error, "timestamp out of range");
    }
  }
  auto values = AlignedBuffer::allocate<Timestamp>(rowCount, pool_);
  auto* rawValues = values->asMutable<Timestamp>();
  if (nonNull == rowCount) {
    for (uint32_t row = 0; row < rowCount; ++row) {
      rawValues[row] = Timestamp(seconds[row], nanos[row]);
    }
  } else {
    const uint8_t* bitmap = bitmaps_[col];
    uint32_t next = 0;
    for (uint32_t row = 0; row < rowCount; ++row) {
      if (bitmap != nullptr && bitSet(bitmap, row)) {
        rawValues[row] = Timestamp(seconds[next], nanos[next]);
        ++next;
      } else {
        rawValues[row] = Timestamp();
      }
    }
  }
  out = std::make_shared<FlatVector<Timestamp>>(
      pool_,
      type,
      makeNulls(col, rowCount),
      rowCount,
      std::move(values),
      std::vector<BufferPtr>{});
  return true;
}

bool CellPayloadDecoder::buildStringColumn(
    uint32_t col,
    uint32_t rowCount,
    const TypePtr& type,
    bool dictionaryEncoded,
    VectorPtr& out,
    std::string& error) {
  const auto lengthBytes = bufferBytes(streamBytes_[layout_.columnStream(col)]);
  const auto dataBytes =
      bufferBytes(streamBytes_[layout_.columnStream(col) + 1]);
  const uint32_t nonNull = nonNullCount_[col];

  // Resolved values, in dense (non-null row) order, viewing borrowed bytes.
  std::vector<std::string_view> resolved;
  resolved.reserve(nonNull);
  uint64_t totalChars = 0;

  if (nonNull == 0) {
    // Spec section 9: every stream of such a column must be empty.
    if (!lengthBytes.empty() || !dataBytes.empty()) {
      return fail(error, "streams of an all-null column are not empty");
    }
  } else if (!dictionaryEncoded) {
    lengthScratch_.resize(nonNull);
    if (!decodeStream<int64_t>(
            reinterpret_cast<const uint8_t*>(lengthBytes.data()),
            lengthBytes.size(),
            nonNull,
            lengthScratch_.data())) {
      return fail(error, "malformed string length stream");
    }
    for (const int64_t length : lengthScratch_) {
      if (length < 0 || length > std::numeric_limits<int32_t>::max() ||
          addOverflows(totalChars, static_cast<uint64_t>(length), totalChars)) {
        return fail(error, "string length out of range");
      }
    }
    if (totalChars != dataBytes.size()) {
      return fail(error, "string data length mismatch");
    }
    size_t offset = 0;
    for (const int64_t length : lengthScratch_) {
      resolved.emplace_back(dataBytes.data() + offset, length);
      offset += length;
    }
  } else {
    // Spec section 8: DictionarySequence in the data stream, then the
    // index segment and fallback lengths in the length stream.
    const auto* data = reinterpret_cast<const uint8_t*>(dataBytes.data());
    const size_t dataSize = dataBytes.size();
    size_t pos = 0;
    // Every dictionary's entries in one flat table; per dictionary its
    // first entry, entry count and matched count.
    struct Dictionary {
      size_t firstEntry;
      size_t numEntries;
      uint32_t matched;
    };
    std::vector<std::string_view> entries;
    std::vector<Dictionary> dictionaries;
    uint64_t matchedSum = 0;
    while (true) {
      if (pos >= dataSize) {
        return fail(error, "unterminated dictionary sequence");
      }
      const size_t firstEntry = entries.size();
      uint32_t serialized = 0;
      uint8_t marker;
      while (true) {
        if (pos >= dataSize) {
          return fail(error, "unterminated dictionary sequence");
        }
        marker = data[pos];
        if (marker >= 0xFE) {
          ++pos;
          break;
        }
        // Entry: length byte + bytes (rule 18); the 64-byte dictionary
        // budget bounds both the entry length and the entry count.
        const uint32_t length = marker;
        if (length > 63) {
          return fail(error, "dictionary entry too long");
        }
        if (dataSize - pos < 1 + static_cast<size_t>(length)) {
          return fail(error, "dictionary entry out of bounds");
        }
        serialized += 1 + length;
        if (serialized >= 64) {
          return fail(error, "dictionary over its serialized budget");
        }
        entries.emplace_back(
            reinterpret_cast<const char*>(data) + pos + 1, length);
        pos += 1 + length;
      }
      if (dataSize - pos < 4) {
        return fail(error, "truncated dictionary matched count");
      }
      const uint32_t matchedRows = loadU32(data + pos);
      pos += 4;
      dictionaries.push_back(
          {firstEntry, entries.size() - firstEntry, matchedRows});
      if (addOverflows(matchedSum, matchedRows, matchedSum)) {
        return fail(error, "matched counts overflow");
      }
      if (marker == 0xFF) {
        break;
      }
    }
    // Rule 22.
    if (matchedSum > nonNull) {
      return fail(error, "matched counts beyond non-null rows");
    }
    const uint64_t fallbackCount = nonNull - matchedSum;
    const std::string_view fallbackRaw(dataBytes.data() + pos, dataSize - pos);

    // Length stream: matchedSum 1-byte indexes, then the fallback lengths.
    if (lengthBytes.size() < matchedSum) {
      return fail(error, "index segment out of bounds");
    }
    const auto* indexes = reinterpret_cast<const uint8_t*>(lengthBytes.data());
    lengthScratch_.resize(fallbackCount);
    if (!decodeStream<int64_t>(
            reinterpret_cast<const uint8_t*>(lengthBytes.data()) + matchedSum,
            lengthBytes.size() - matchedSum,
            fallbackCount,
            lengthScratch_.data())) {
      return fail(error, "malformed fallback length stream");
    }

    // Resolve dictionary hits per segment (rule 19), then the fallback.
    size_t indexPos = 0;
    for (const auto& dictionary : dictionaries) {
      for (uint32_t i = 0; i < dictionary.matched; ++i) {
        const uint8_t index = indexes[indexPos++];
        if (index >= dictionary.numEntries) {
          return fail(error, "dictionary index out of range");
        }
        const auto entry = entries[dictionary.firstEntry + index];
        resolved.push_back(entry);
        totalChars += entry.size();
      }
    }
    uint64_t fallbackChars = 0;
    for (const int64_t length : lengthScratch_) {
      if (length < 0 || length > std::numeric_limits<int32_t>::max() ||
          addOverflows(
              fallbackChars, static_cast<uint64_t>(length), fallbackChars)) {
        return fail(error, "fallback length out of range");
      }
    }
    // Rule 23, and the data stream must end exactly at the fallback bytes.
    if (fallbackChars != fallbackRaw.size()) {
      return fail(error, "fallback data length mismatch");
    }
    size_t offset = 0;
    for (const int64_t length : lengthScratch_) {
      resolved.emplace_back(fallbackRaw.data() + offset, length);
      offset += length;
    }
    totalChars += fallbackChars;
  }

  // Materialize: one chars buffer, one views buffer.
  auto chars = AlignedBuffer::allocate<char>(totalChars, pool_);
  auto* rawChars = chars->asMutable<char>();
  auto views = AlignedBuffer::allocate<StringView>(rowCount, pool_);
  auto* rawViews = views->asMutable<StringView>();

  const uint8_t* bitmap = bitmaps_[col];
  const bool allNonNull = nonNull == rowCount;
  size_t charOffset = 0;
  uint32_t next = 0;
  for (uint32_t row = 0; row < rowCount; ++row) {
    if (allNonNull || (bitmap != nullptr && bitSet(bitmap, row))) {
      const auto value = resolved[next++];
      ::memcpy(rawChars + charOffset, value.data(), value.size());
      rawViews[row] =
          StringView(rawChars + charOffset, static_cast<int32_t>(value.size()));
      charOffset += value.size();
    } else {
      rawViews[row] = StringView();
    }
  }

  std::vector<BufferPtr> stringBuffers;
  if (totalChars > 0) {
    stringBuffers.push_back(std::move(chars));
  }
  out = std::make_shared<FlatVector<StringView>>(
      pool_,
      type,
      makeNulls(col, rowCount),
      rowCount,
      std::move(views),
      std::move(stringBuffers));
  return true;
}

bool CellPayloadDecoder::decode(
    CellByteSource& in,
    RowVectorPtr& out,
    std::string& error) {
  uint8_t header[kPayloadFixedHeaderBytes];
  if (!in.read(header, sizeof(header))) {
    return fail(error, "truncated payload header"); // rule 1
  }
  const uint32_t rowCount = loadU32(header);
  const uint32_t runCount = loadU32(header + 4);
  // variable_size at offset 8 is a preallocation hint, never validated
  // (spec section 3.1).
  const uint32_t nullStoredSize = loadU32(header + 16);
  const uint32_t nullDecodedSize = loadU32(header + 20);
  if (layout_.numWireColumns() == 0 &&
      (nullStoredSize != 0 || nullDecodedSize != 0 || runCount != 0 ||
       loadU64(header + 8) != 0)) {
    return fail(
        error, "payload without wire columns must contain only its header");
  }
  if (layout_.numWireColumns() != 0 && nullStoredSize < 1) {
    return fail(error, "null stored size must be at least 1"); // rule 2
  }
  // Implementation-level bounds on untrusted fields (spec section 10.3).
  if (limits_.maxRowCount != 0 && rowCount > limits_.maxRowCount) {
    return fail(error, "row count beyond the configured limit");
  }
  if (limits_.maxRunCount != 0 && runCount > limits_.maxRunCount) {
    return fail(error, "run count beyond the configured limit");
  }

  if (!parseNullRegion(in, rowCount, nullStoredSize, nullDecodedSize, error)) {
    return false;
  }

  const uint32_t numColumns = layout_.numColumns();
  encodingTags_.resize((layout_.numWireColumns() + 7) / 8);
  if (!in.read(encodingTags_.data(), encodingTags_.size())) {
    return fail(error, "truncated encoding tags"); // rule 3
  }
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (layout_.isUnknownColumn(col)) {
      continue;
    }
    const auto wire = layout_.wireColumn(col);
    const bool tagged = (encodingTags_[wire / 8] >> (wire % 8)) & 1;
    if (tagged && !layout_.isStringColumn(col)) {
      return fail(error, "encoding tag on a non-string column");
    }
  }
  if ((layout_.numWireColumns() % 8) != 0 &&
      (encodingTags_.back() >> (layout_.numWireColumns() % 8)) != 0) {
    return fail(error, "non-zero unused encoding tag bits");
  }

  for (auto& stream : streamBytes_) {
    if (stream != nullptr) {
      stream->setSize(0);
    }
  }
  for (uint32_t run = 0; run < runCount; ++run) {
    if (!parseRun(in, error)) {
      return false;
    }
  }

  std::vector<VectorPtr> children(numColumns);
  const auto& rowType = layout_.rowType();
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto& type = rowType->childAt(col);
    bool ok = false;
    switch (type->kind()) {
      case TypeKind::SMALLINT:
        ok = buildEncodedColumn<int16_t>(
            col, rowCount, type, children[col], error);
        break;
      case TypeKind::INTEGER:
        ok = buildEncodedColumn<int32_t>(
            col, rowCount, type, children[col], error);
        break;
      case TypeKind::BIGINT:
        ok = buildEncodedColumn<int64_t>(
            col, rowCount, type, children[col], error);
        break;
      case TypeKind::UNKNOWN:
        children[col] = BaseVector::createNullConstant(type, rowCount, pool_);
        ok = true;
        break;
      case TypeKind::BOOLEAN:
        ok = buildBooleanColumn(col, rowCount, type, children[col], error);
        break;
      case TypeKind::TIMESTAMP:
        ok = buildTimestampColumn(col, rowCount, type, children[col], error);
        break;
      case TypeKind::HUGEINT:
        ok =
            buildRawColumn<int128_t>(col, rowCount, type, children[col], error);
        break;
      case TypeKind::TINYINT:
        ok = buildRawColumn<int8_t>(col, rowCount, type, children[col], error);
        break;
      case TypeKind::REAL:
        ok = buildRawColumn<float>(col, rowCount, type, children[col], error);
        break;
      case TypeKind::DOUBLE:
        ok = buildRawColumn<double>(col, rowCount, type, children[col], error);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY: {
        const auto wire = layout_.wireColumn(col);
        const bool dictionary = (encodingTags_[wire / 8] >> (wire % 8)) & 1;
        ok = buildStringColumn(
            col, rowCount, type, dictionary, children[col], error);
        break;
      }
      default:
        return fail(error, "unsupported column type");
    }
    if (!ok) {
      return false;
    }
  }

  out = std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(nullptr), rowCount, std::move(children));
  return true;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
