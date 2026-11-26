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

#include <folly/lang/Assume.h>
#include <algorithm>
#include <string_view>
#include <type_traits>

#include "bolt/common/base/MemoryCopy.h"
#include "bolt/core/Metaprogramming.h"
#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellOutput.h"
#include "bolt/shuffle/sparksql/cell/RunCompressor.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

constexpr uint32_t kMaxDecodeChunkRows = 64 << 10;
static_assert(kMaxDecodeChunkRows % 64 == 0);

CellLayout projectedLayout(
    const RowType& sourceType,
    std::span<const uint32_t> sourceColumns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(sourceColumns.size());
  types.reserve(sourceColumns.size());
  for (const auto col : sourceColumns) {
    names.push_back(sourceType.nameOf(col));
    types.push_back(sourceType.childAt(col));
  }
  return CellLayout::create(ROW(std::move(names), std::move(types)));
}

std::string_view bufferBytes(const BufferPtr& buffer) {
  return buffer == nullptr
      ? std::string_view{}
      : std::string_view(buffer->as<char>(), buffer->size());
}

inline bool addOverflows(uint64_t a, uint64_t b, uint64_t& out) {
  return __builtin_add_overflow(a, b, &out);
}

/// The low `n` bits set, 0 < n <= 64.
inline uint64_t lowBits(uint32_t n) {
  return n == 64 ? ~0ULL : (1ULL << n) - 1;
}

/// The 64 bits of a `bytes`-byte bitmap from bit `offset`; bits past its
/// end read 0, so a bitmap at the end of its buffer is never read beyond.
inline uint64_t
loadBitWord(const uint8_t* bits, uint64_t bytes, uint64_t offset) {
  const uint64_t byte = offset / 8;
  const uint32_t shift = offset % 8;
  if (byte + 9 <= bytes) {
    const uint64_t word = memory::loadUnaligned<uint64_t>(bits + byte);
    return shift == 0 ? word
                      : (word >> shift) |
            (static_cast<uint64_t>(bits[byte + 8]) << (64 - shift));
  }
  uint64_t word = 0;
  for (uint32_t i = 0; i < 9 && byte + i < bytes; ++i) {
    const uint64_t value = bits[byte + i];
    const uint32_t at = 8 * i;
    if (at < shift) {
      word |= value >> (shift - at);
    } else if (at - shift < 64) {
      word |= value << (at - shift);
    }
  }
  return word;
}

/// Sets the `n` bits of `target` from bit `offset` to the low bits of
/// `word`, keeping every other bit.
inline void
storeBitWord(uint64_t* target, uint64_t offset, uint64_t word, uint32_t n) {
  const uint64_t index = offset / 64;
  const uint32_t shift = offset % 64;
  const uint64_t mask = lowBits(n);
  word &= mask;
  target[index] = (target[index] & ~(mask << shift)) | (word << shift);
  if (shift != 0 && shift + n > 64) {
    const uint64_t high = lowBits(shift + n - 64);
    target[index + 1] = (target[index + 1] & ~high) | (word >> (64 - shift));
  }
}

/// Calls `visit(base, n, word)` for rows [begin, begin + numRows) of a
/// bitmap, 64 at a time: `word` holds the bits of rows base..base + n - 1
/// (relative to begin), the bits above n clear.
template <typename Visit>
inline void forEachBitWord(
    const uint8_t* bits,
    uint64_t bytes,
    uint64_t begin,
    uint32_t numRows,
    const Visit& visit) {
  for (uint32_t base = 0; base < numRows; base += 64) {
    const uint32_t n = std::min<uint32_t>(64, numRows - base);
    visit(base, n, loadBitWord(bits, bytes, begin + base) & lowBits(n));
  }
}

/// For rows [0, numRows) under a bitmap from bit `begin`: `absent(base, n)`
/// for each block of up to 64 rows holding a null, then `present(row)` for
/// each non-null row of the block, in row order.
template <typename Absent, typename Present>
inline void scatterRows(
    const uint8_t* bits,
    uint64_t bytes,
    uint64_t begin,
    uint32_t numRows,
    const Absent& absent,
    const Present& present) {
  forEachBitWord(
      bits,
      bytes,
      begin,
      numRows,
      [&](uint32_t base, uint32_t n, uint64_t word) {
        if (word != lowBits(n)) {
          absent(base, n);
        }
        while (word != 0) {
          present(base + static_cast<uint32_t>(__builtin_ctzll(word)));
          word &= word - 1;
        }
      });
}

inline void materializeString(
    StringView& out,
    const char* source,
    int32_t size,
    char*& chars) {
  // Both flat lengths and dictionary entries are validated before use.
  folly::assume(size >= 0);
  if (size > static_cast<int32_t>(StringView::kInlineSize)) {
    ::memcpy(chars, source, size);
    out.set(chars, size);
    chars += size;
  } else {
    out.set(source, size);
  }
}

/// Writes a bitmap front to back, 64 bits at a time. Each word is stored
/// whole: the last one written may cover up to 7 bytes past the bitmap,
/// which must be writable.
class BitAppender {
 public:
  explicit BitAppender(uint8_t* out) : out_(out) {}

  /// Appends the low `n` (<= 64) bits of `bits`, its bits above them clear.
  void append(uint64_t bits, uint32_t n) {
    word_ |= bits << used_;
    const uint32_t used = used_ + n;
    if (used < 64) {
      used_ = used;
      return;
    }
    store(word_);
    // The bits that did not fit in the word.
    word_ = used_ == 0 ? 0 : bits >> (64 - used_);
    used_ = used - 64;
  }

  /// Appends `n` bits, all set or all clear.
  void appendRun(bool set, uint64_t n) {
    const uint64_t fill = set ? ~0ULL : 0;
    if (used_ != 0) {
      const uint32_t room = 64 - used_;
      if (n < room) {
        append(
            fill & lowBits(static_cast<uint32_t>(n)), static_cast<uint32_t>(n));
        return;
      }
      append(fill & lowBits(room), room);
      n -= room;
    }
    const uint64_t words = n / 64;
    ::memset(out_, set ? 0xFF : 0, words * 8);
    out_ += words * 8;
    if ((n & 63) != 0) {
      append(fill & lowBits(n & 63), n & 63);
    }
  }

  /// Writes the bits appended last, the rest of their word clear.
  void finish() {
    if (used_ != 0) {
      store(word_);
    }
  }

 private:
  void store(uint64_t word) {
    memory::storeUnaligned<uint64_t>(out_, word);
    out_ += sizeof(word);
  }

  uint8_t* out_;
  uint64_t word_{0};
  uint32_t used_{0};
};

/// parseNullBody() has already checked the record count against the input.
uint32_t expandColumnNullRecords(
    const uint8_t* records,
    uint32_t positions,
    uint8_t* bitmap) {
  const uint8_t* end = records + sizeof(uint32_t) +
      uint64_t{memory::loadUnaligned<uint32_t>(records)} * kNullRecordBytes;
  uint32_t nonNullCount;
  expandNullRecords(records, end, positions, bitmap, nonNullCount);
  return nonNullCount;
}

/// Values-buffer bytes per row, a Boolean counted as one byte (an upper
/// bound); for a raw stream, also the wire width of one value.
uint32_t decodedRowBytes(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
      return 8;
    case TypeKind::HUGEINT:
    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return 16;
    default:
      return 0;
  }
}

} // namespace

/// Places successive dense groups without revisiting the null bitmap between
/// groups. A mixed word is cleared only before its first value is written.
class CellPayloadDecoder::StringRows {
 public:
  StringRows(
      StringView* out,
      const uint8_t* bitmap,
      uint64_t bitmapBytes,
      uint32_t begin,
      uint32_t numRows)
      : out_(out),
        bitmap_(bitmap),
        bitmapBytes_(bitmapBytes),
        begin_(begin),
        numRows_(numRows) {}

  template <typename Count, typename Write>
  void place(Count count, const Write& write) {
    if (bitmap_ == nullptr) {
      auto* out = out_ + next_;
      next_ += count;
      for (uint32_t i = 0; i < count; ++i) {
        write(out[i], i);
      }
      return;
    }
    for (uint32_t i = 0; i < count; ++i) {
      while (word_ == 0) {
        base_ = next_;
        const uint32_t n = std::min<uint32_t>(64, numRows_ - base_);
        word_ = loadBitWord(bitmap_, bitmapBytes_, begin_ + base_) & lowBits(n);
        if (word_ != lowBits(n)) {
          std::fill_n(out_ + base_, n, StringView());
        }
        next_ += n;
      }
      const uint32_t row =
          base_ + static_cast<uint32_t>(__builtin_ctzll(word_));
      word_ &= word_ - 1;
      write(out_[row], i);
    }
  }

  void finish() {
    // No more dense values: words not visited yet contain only nulls.
    if (bitmap_ != nullptr) {
      std::fill_n(out_ + next_, numRows_ - next_, StringView());
    }
  }

 private:
  StringView* const out_;
  const uint8_t* const bitmap_;
  const uint64_t bitmapBytes_;
  const uint32_t begin_;
  const uint32_t numRows_;
  uint32_t next_{0};
  uint32_t base_{0};
  uint64_t word_{0};
};

CellPayloadDecoder::CellPayloadDecoder(
    CellLayout layout,
    ShuffleDecompressor* decompressor,
    memory::MemoryPool* pool)
    : layout_(std::move(layout)),
      decompressor_(decompressor),
      pool_(pool),
      streamBytes_(layout_.numStreams()),
      streamViews_(layout_.numStreams()) {
  nullTags_.resize(layout_.numColumns());
  records_.resize(layout_.numColumns());
  nonNullCount_.resize(layout_.numColumns());
  bitmaps_.resize(layout_.numColumns());
  cursors_.reserve(layout_.numColumns());
  batch_.resize(layout_.numColumns());
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    const auto kind = layout_.rowType()->childAt(col)->kind();
    rowBytes_ += decodedRowBytes(kind);
    switch (kind) {
      case TypeKind::SMALLINT:
        cursors_.emplace_back(std::in_place_type<StreamCursor<int16_t>>);
        break;
      case TypeKind::INTEGER:
        cursors_.emplace_back(std::in_place_type<StreamCursor<int32_t>>);
        break;
      case TypeKind::BIGINT:
        cursors_.emplace_back(std::in_place_type<StreamCursor<int64_t>>);
        break;
      case TypeKind::TIMESTAMP:
        cursors_.emplace_back(std::in_place_type<TimestampCursor>);
        break;
      case TypeKind::BOOLEAN:
        cursors_.emplace_back(std::in_place_type<RawCursor<bool>>);
        break;
      case TypeKind::TINYINT:
        cursors_.emplace_back(std::in_place_type<RawCursor<int8_t>>);
        break;
      case TypeKind::REAL:
        cursors_.emplace_back(std::in_place_type<RawCursor<float>>);
        break;
      case TypeKind::DOUBLE:
        cursors_.emplace_back(std::in_place_type<RawCursor<double>>);
        break;
      case TypeKind::HUGEINT:
        cursors_.emplace_back(std::in_place_type<RawCursor<int128_t>>);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        cursors_.emplace_back(std::in_place_type<StringCursor>);
        break;
      default:
        BOLT_FAIL("unsupported column type");
    }
  }
  for (uint32_t s = 0; s < layout_.numStreams(); ++s) {
    if (layout_.stream(s).kind == StreamKind::kStringData) {
      stringDataStreams_.push_back(s);
    }
  }
}

CellPayloadDecoder::CellPayloadDecoder(
    CellPayloadDecoder& source,
    std::span<const uint32_t> sourceColumns)
    : CellPayloadDecoder(
          projectedLayout(*source.layout_.rowType(), sourceColumns),
          nullptr,
          source.pool_) {
  BOLT_CHECK_NULL(source.source_, "a projection requires a stream reader");
  source_ = &source;
  sourceColumns_.assign(sourceColumns.begin(), sourceColumns.end());
  sourceStreams_.reserve(layout_.numStreams());
  for (uint32_t s = 0; s < layout_.numStreams(); ++s) {
    const auto col = layout_.stream(s).column;
    const auto streamInColumn = s - layout_.firstStream(col);
    sourceStreams_.push_back(
        source.layout_.firstStream(sourceColumns[col]) + streamInColumn);
  }
}

[[noreturn]] FOLLY_NOINLINE void failMalformedPayload(const char* what) {
  BOLT_FAIL("malformed cell shuffle payload: {}", what);
}

void expandNullRecords(
    const uint8_t*& cursor,
    const uint8_t* end,
    uint32_t rowCount,
    uint8_t* bitmap,
    uint32_t& nonNullCount) {
  if (end - cursor < static_cast<ptrdiff_t>(sizeof(uint32_t))) {
    failMalformedPayload("truncated null records");
  }
  const uint32_t count = memory::loadUnaligned<uint32_t>(cursor);
  cursor += sizeof(uint32_t);
  if (count > static_cast<uint64_t>(end - cursor) / kNullRecordBytes) {
    failMalformedPayload("truncated null records");
  }
  BitAppender out(bitmap);
  uint64_t row = 0;
  uint64_t nulls = 0;
  for (uint32_t i = 0; i < count; ++i, cursor += kNullRecordBytes) {
    const auto run = memory::loadUnaligned<uint16_t>(cursor);
    const uint32_t fill = cursor[2];
    const uint8_t* bits = cursor + (kNullRecordBytes - kNullRecordBitmapBytes);
    if (fill > kNullRecordBits) {
      failMalformedPayload("null record fill beyond its bitmap");
    }
    bool clearPastFill =
        (fill & 7) == 0 || (bits[fill >> 3] >> (fill & 7)) == 0;
    for (uint32_t b = (fill + 7) >> 3; b < kNullRecordBitmapBytes; ++b) {
      clearPastFill = clearPastFill && bits[b] == 0;
    }
    if (!clearPastFill) {
      failMalformedPayload("non-zero bits past a null record's fill");
    }
    const uint32_t runRows = run & kNullRecordMaxRun;
    const uint64_t rows = runRows + fill;
    if (rows == 0) {
      failMalformedPayload("empty null record");
    }
    if (row + rows > rowCount) {
      failMalformedPayload("null records past the payload's rows");
    }
    const bool runNull = (run & kNullRecordRunNull) != 0;
    out.appendRun(!runNull, runRows);
    nulls += runNull ? runRows : 0;
    uint64_t words[4] = {};
    ::memcpy(words, bits, kNullRecordBitmapBytes);
    for (uint32_t taken = 0, w = 0; taken < fill; taken += 64, ++w) {
      const uint32_t n = std::min<uint32_t>(64, fill - taken);
      out.append(words[w], n);
      nulls += n - __builtin_popcountll(words[w]);
    }
    row += rows;
  }
  // The rows past the records are non-null.
  out.appendRun(true, rowCount - row);
  out.finish();
  nonNullCount = static_cast<uint32_t>(rowCount - nulls);
}

void CellPayloadDecoder::parseNullBody(
    ShuffleByteSource& in,
    uint32_t nullStoredSize,
    uint32_t nullDecodedSize) {
  if (nullDecodedSize == 0) {
    auto* nullData = resetBuffer(nullBody_, nullStoredSize, pool_);
    if (!in.read(nullData, nullStoredSize)) {
      failMalformedPayload("truncated null body");
    }
  } else {
    if (decompressor_ == nullptr) {
      failMalformedPayload("compressed null body without a codec");
    }
    auto* stored = resetBuffer(compressedScratch_, nullStoredSize, pool_);
    if (!in.read(stored, nullStoredSize)) {
      failMalformedPayload("truncated null body");
    }
    auto* decoded = resetBuffer(nullBody_, nullDecodedSize, pool_);
    if (!decompressor_->decompress(
            {reinterpret_cast<const uint8_t*>(stored), nullStoredSize},
            {reinterpret_cast<uint8_t*>(decoded), nullDecodedSize})) {
      // The decompressed length must match exactly.
      failMalformedPayload("null body decompression failed");
    }
  }

  const uint32_t numColumns = layout_.numColumns();
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const auto nullBody = bufferBytes(nullBody_);
  const auto* nullData = reinterpret_cast<const uint8_t*>(nullBody.data());
  if (nullBody.size() < tagBytes) {
    failMalformedPayload("null body shorter than its tags");
  }
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto tag = getNullTag(nullData, col);
    if (tag == NullTag::kReserved) {
      failMalformedPayload("reserved null tag");
    }
    nullTags_[col] = tag;
  }
  // Unused high bits of the last tags byte must be zero.
  if ((numColumns % 4) != 0) {
    const uint8_t tail = nullData[tagBytes - 1];
    if ((tail >> ((numColumns % 4) * 2)) != 0) {
      failMalformedPayload("non-zero unused null tag bits");
    }
  }

  // Each kRawNull column's records - their u32 count, then the records -
  // expanded once the column's rows are known.
  const uint8_t* cursor = nullData + tagBytes;
  const uint8_t* const end = nullData + nullBody.size();
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (nullTags_[col] != NullTag::kRawNull) {
      continue;
    }
    if (end - cursor < static_cast<ptrdiff_t>(sizeof(uint32_t)) ||
        memory::loadUnaligned<uint32_t>(cursor) >
            static_cast<uint64_t>(end - cursor - sizeof(uint32_t)) /
                kNullRecordBytes) {
      failMalformedPayload("truncated null records");
    }
    records_[col] = cursor;
    cursor += sizeof(uint32_t) +
        uint64_t{memory::loadUnaligned<uint32_t>(cursor)} * kNullRecordBytes;
  }
  if (cursor != end) {
    failMalformedPayload("null body length mismatch");
  }
}

void CellPayloadDecoder::expandNulls(uint32_t rowCount) {
  uint32_t rawNullColumns = 0;
  for (uint32_t col = 0; col < nullTags_.size(); ++col) {
    if (nullTags_[col] == NullTag::kRawNull) {
      ++rawNullColumns;
    } else {
      nonNullCount_[col] = nullTags_[col] == NullTag::kNoNull ? rowCount : 0;
      bitmaps_[col] = nullptr;
    }
  }
  if (rawNullColumns == 0) {
    return;
  }
  const uint64_t bitmapBytes = (static_cast<uint64_t>(rowCount) + 7) / 8;
  // The last record can write a whole word past the bitmap's last byte.
  auto* bitmap = reinterpret_cast<uint8_t*>(resetBuffer(
      expandedNulls_, rawNullColumns * bitmapBytes + sizeof(uint64_t), pool_));
  for (uint32_t col = 0; col < nullTags_.size(); ++col) {
    if (nullTags_[col] == NullTag::kRawNull) {
      nonNullCount_[col] =
          expandColumnNullRecords(records_[col], rowCount, bitmap);
      bitmaps_[col] = bitmap;
      bitmap += bitmapBytes;
    }
  }
}

void CellPayloadDecoder::parseRun(
    ShuffleByteSource& in,
    bool onlyRun,
    bool& streamsInCombinedDecoded) {
  uint8_t layoutByte;
  if (!in.read(&layoutByte, 1)) {
    failMalformedPayload("truncated run header");
  }
  if (layoutByte > static_cast<uint8_t>(RunLayout::kCombinedUncompressed)) {
    failMalformedPayload("unknown run compression layout");
  }
  const auto runLayout = static_cast<RunLayout>(layoutByte);
  const uint32_t numStreams = layout_.numStreams();
  const uint32_t storedCount =
      runLayout == RunLayout::kSeparate ? numStreams : 1;

  storedSizes_.resize(storedCount);
  if (!in.read(storedSizes_.data(), storedCount * sizeof(uint64_t))) {
    failMalformedPayload("truncated run stored sizes");
  }
  decodedSizes_.resize(numStreams);
  if (!in.read(decodedSizes_.data(), numStreams * sizeof(uint64_t))) {
    failMalformedPayload("truncated run decoded sizes");
  }

  uint64_t storedSum = 0;
  for (const auto size : storedSizes_) {
    if (addOverflows(storedSum, size, storedSum)) {
      failMalformedPayload("run stored sizes overflow");
    }
  }
  uint64_t decodedSum = 0;
  for (const auto size : decodedSizes_) {
    if (addOverflows(decodedSum, size, decodedSum)) {
      failMalformedPayload("run decoded sizes overflow");
    }
  }

  switch (runLayout) {
    case RunLayout::kSeparate:
      for (uint32_t s = 0; s < numStreams; ++s) {
        const uint64_t stored = storedSizes_[s];
        const uint64_t decoded = decodedSizes_[s];
        if (stored == 0) {
          if (decoded != 0) {
            failMalformedPayload("empty stored buffer with decoded size");
          }
          continue; // an empty stream: the codec is not invoked
        }
        auto& stream = streamBytes_[s];
        if (decoded == 0) {
          // Uncompressed: bytes go to the stream as they are.
          const size_t offset = stream == nullptr ? 0 : stream->size();
          uint64_t totalSize = 0;
          if (addOverflows(static_cast<uint64_t>(offset), stored, totalSize)) {
            failMalformedPayload("run stream size overflow");
          }
          auto* out =
              resizeBuffer(stream, static_cast<size_t>(totalSize), pool_);
          if (!in.read(out + offset, stored)) {
            failMalformedPayload("truncated run buffer");
          }
        } else {
          if (decompressor_ == nullptr) {
            failMalformedPayload("compressed buffer without a codec");
          }
          auto* input = resetBuffer(compressedScratch_, stored, pool_);
          if (!in.read(input, stored)) {
            failMalformedPayload("truncated run buffer");
          }
          const size_t offset = stream == nullptr ? 0 : stream->size();
          uint64_t totalSize = 0;
          if (addOverflows(static_cast<uint64_t>(offset), decoded, totalSize)) {
            failMalformedPayload("run stream size overflow");
          }
          auto* out =
              resizeBuffer(stream, static_cast<size_t>(totalSize), pool_);
          if (!decompressor_->decompress(
                  {reinterpret_cast<const uint8_t*>(input), stored},
                  {reinterpret_cast<uint8_t*>(out) + offset, decoded})) {
            failMalformedPayload("run buffer decompression failed");
          }
        }
      }
      return;

    case RunLayout::kCombinedUncompressed:
      // The single stored buffer is the concatenation itself.
      if (storedSizes_[0] != decodedSum) {
        failMalformedPayload("combined-uncompressed size mismatch");
      }
      for (uint32_t s = 0; s < numStreams; ++s) {
        const uint64_t decoded = decodedSizes_[s];
        if (decoded == 0) {
          continue;
        }
        auto& stream = streamBytes_[s];
        const size_t offset = stream == nullptr ? 0 : stream->size();
        uint64_t totalSize = 0;
        if (addOverflows(static_cast<uint64_t>(offset), decoded, totalSize)) {
          failMalformedPayload("run stream size overflow");
        }
        auto* out = resizeBuffer(stream, static_cast<size_t>(totalSize), pool_);
        if (!in.read(out + offset, decoded)) {
          failMalformedPayload("truncated run buffer");
        }
      }
      return;

    case RunLayout::kCombined: {
      if (decompressor_ == nullptr) {
        failMalformedPayload("compressed buffer without a codec");
      }
      auto* input = resetBuffer(compressedScratch_, storedSizes_[0], pool_);
      if (!in.read(input, storedSizes_[0])) {
        failMalformedPayload("truncated run buffer");
      }
      auto* decodedData = resetBuffer(combinedDecoded_, decodedSum, pool_);
      if (!decompressor_->decompress(
              {reinterpret_cast<const uint8_t*>(input), storedSizes_[0]},
              {reinterpret_cast<uint8_t*>(decodedData), decodedSum})) {
        failMalformedPayload("combined buffer decompression failed");
      }
      if (onlyRun) {
        // The streams are consecutive in the decoded buffer: view them there.
        size_t offset = 0;
        for (uint32_t s = 0; s < numStreams; ++s) {
          streamViews_[s] = std::string_view(
              reinterpret_cast<const char*>(decodedData) + offset,
              decodedSizes_[s]);
          offset += decodedSizes_[s];
        }
        streamsInCombinedDecoded = true;
        return;
      }
      size_t offset = 0;
      for (uint32_t s = 0; s < numStreams; ++s) {
        if (decodedSizes_[s] == 0) {
          continue;
        }
        auto& stream = streamBytes_[s];
        const size_t streamOffset = stream == nullptr ? 0 : stream->size();
        uint64_t totalSize = 0;
        if (addOverflows(
                static_cast<uint64_t>(streamOffset),
                decodedSizes_[s],
                totalSize)) {
          failMalformedPayload("run stream size overflow");
        }
        auto* out = resizeBuffer(stream, static_cast<size_t>(totalSize), pool_);
        ::memcpy(out + streamOffset, decodedData + offset, decodedSizes_[s]);
        offset += decodedSizes_[s];
      }
      return;
    }
  }
  failMalformedPayload("unreachable run layout");
}

void CellPayloadDecoder::openPayload(ShuffleByteSource& in) {
  BOLT_CHECK_NULL(source_, "a projection must open its source's columns");
  BOLT_CHECK_EQ(payloadRowsLeft(), 0u, "previous payload not fully read");
  rowCount_ = 0;
  rowsRead_ = 0;
  const uint32_t rowCount = parsePayload(in);
  uint64_t streamStringBytes = 0;
  for (const uint32_t stream : stringDataStreams_) {
    streamStringBytes += streamViews_[stream].size();
  }
  // Treat the header's variableBytes as a sizing hint only (never for
  // allocation), taking the larger of it and actual string data streams so
  // both dictionary-compressed and corrupted headers stay bounded.
  const uint64_t variableBytes =
      std::max(payloadVariableBytes_, streamStringBytes);
  payloadRowBytes_ = std::max<uint64_t>(
      1, rowBytes_ + variableBytes / std::max<uint32_t>(rowCount, 1u));
  openColumns(rowCount);
}

uint32_t CellPayloadDecoder::readPayloadStreams(ShuffleByteSource& in) {
  BOLT_CHECK_NULL(source_, "a projection must open its source's columns");
  BOLT_CHECK_EQ(payloadRowsLeft(), 0u, "a payload of this decoder is open");
  return parsePayload(in);
}

void CellPayloadDecoder::openProjectedColumns(uint32_t rowCount) {
  BOLT_CHECK_NOT_NULL(source_);
  BOLT_CHECK_EQ(payloadRowsLeft(), 0u, "previous payload not fully read");
  rowCount_ = 0;
  rowsRead_ = 0;
  if (rowCount > kMaxPayloadRows) {
    failMalformedPayload("projected row count beyond the configured limit");
  }
  for (uint32_t col = 0; col < sourceColumns_.size(); ++col) {
    const auto from = sourceColumns_[col];
    nullTags_[col] = source_->nullTags_[from];
    records_[col] = source_->records_[from];
  }
  for (uint32_t s = 0; s < sourceStreams_.size(); ++s) {
    streamViews_[s] = source_->streamViews_[sourceStreams_[s]];
  }
  openColumns(rowCount);
}

uint32_t CellPayloadDecoder::parsePayload(ShuffleByteSource& in) {
  uint8_t header[kPayloadFixedHeaderBytes];
  if (!in.read(header, sizeof(header))) {
    failMalformedPayload("truncated payload header");
  }
  const uint32_t rowCount = memory::loadUnaligned<uint32_t>(header);
  const uint32_t runCount = memory::loadUnaligned<uint32_t>(header + 4);
  const uint64_t variableBytes = memory::loadUnaligned<uint64_t>(header + 8);
  const uint32_t nullStoredSize = memory::loadUnaligned<uint32_t>(header + 16);
  const uint32_t nullDecodedSize = memory::loadUnaligned<uint32_t>(header + 20);
  const uint32_t numColumns = layout_.numColumns();
  if (numColumns == 0 &&
      (nullStoredSize != 0 || nullDecodedSize != 0 || runCount != 0 ||
       variableBytes != 0)) {
    failMalformedPayload(
        "payload without wire columns must contain only its header");
  }
  payloadVariableBytes_ = variableBytes;
  if (numColumns != 0 && nullStoredSize < 1) {
    failMalformedPayload("null stored size must be at least 1");
  }
  // Bounds on untrusted fields.
  if (rowCount > kMaxPayloadRows) {
    failMalformedPayload("row count beyond the configured limit");
  }
  if (runCount > kMaxPayloadRuns) {
    failMalformedPayload("run count beyond the configured limit");
  }

  parseNullBody(in, nullStoredSize, nullDecodedSize);

  encodingTags_.resize((numColumns + 7) / 8);
  if (!in.read(encodingTags_.data(), encodingTags_.size())) {
    failMalformedPayload("truncated encoding tags");
  }
  for (uint32_t col = 0; col < numColumns; ++col) {
    const bool tagged = (encodingTags_[col / 8] >> (col % 8)) & 1;
    if (tagged && !layout_.isStringColumn(col)) {
      failMalformedPayload("encoding tag on a non-string column");
    }
  }
  if ((numColumns % 8) != 0 &&
      (encodingTags_.back() >> (numColumns % 8)) != 0) {
    failMalformedPayload("non-zero unused encoding tag bits");
  }

  for (auto& stream : streamBytes_) {
    if (stream != nullptr) {
      stream->setSize(0);
    }
  }
  streamViews_.assign(streamBytes_.size(), std::string_view());
  bool streamsInCombinedDecoded = false;
  for (uint32_t run = 0; run < runCount; ++run) {
    parseRun(in, runCount == 1, streamsInCombinedDecoded);
  }
  if (!streamsInCombinedDecoded) {
    for (size_t s = 0; s < streamBytes_.size(); ++s) {
      streamViews_[s] = bufferBytes(streamBytes_[s]);
    }
  }
  return rowCount;
}

void CellPayloadDecoder::openColumns(uint32_t rowCount) {
  expandNulls(rowCount);
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    openColumn(col);
  }
  rowCount_ = rowCount;
  if (rowCount_ == 0) {
    finishPayload();
  }
}

void CellPayloadDecoder::openColumn(uint32_t col) {
  const uint32_t stream = layout_.firstStream(col);
  const auto bytes = streamViews_[stream];
  const auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
  const uint32_t nonNull = nonNullCount_[col];
  std::visit(
      util::overloaded{
          [&]<typename T>(StreamCursor<T>& cursor) {
            // Every encoded block needs a tag and at least one data byte.
            constexpr uint32_t kValuesPerBlock = kCacheLineBytes / sizeof(T);
            if (bits::divRoundUp(nonNull, kValuesPerBlock) > bytes.size() / 2) {
              failMalformedPayload("malformed value stream");
            }
            cursor.reset(data, bytes.size(), nonNull);
          },
          [&](TimestampCursor& cursor) {
            const auto nanos = streamViews_[stream + 1];
            cursor.seconds.reset(data, bytes.size(), nonNull);
            cursor.nanos.reset(
                reinterpret_cast<const uint8_t*>(nanos.data()),
                nanos.size(),
                nonNull);
          },
          [&]<typename T>(RawCursor<T>& cursor) {
            cursor.valuesRead = 0;
            if (bytes.size() != static_cast<uint64_t>(nonNull) * sizeof(T)) {
              failMalformedPayload("raw stream length mismatch");
            }
          },
          [&](StringCursor& cursor) { openStringColumn(col, cursor); }},
      cursors_[col]);
}

void CellPayloadDecoder::openStringColumn(uint32_t col, StringCursor& cursor) {
  const auto lengthBytes = streamViews_[layout_.firstStream(col)];
  const auto dataBytes = streamViews_[layout_.firstStream(col) + 1];
  const uint32_t nonNull = nonNullCount_[col];
  const auto tagCol = source_ ? sourceColumns_[col] : col;
  const auto& tags = source_ ? source_->encodingTags_ : encodingTags_;
  cursor.isDictionary = (tags[tagCol / 8] >> (tagCol % 8)) & 1;
  cursor.data = dataBytes;
  cursor.dataOffset = 0;
  cursor.entries.clear();
  cursor.dictionaries.clear();
  cursor.indexes = nullptr;
  cursor.matchedRows = 0;
  cursor.matchedRead = 0;
  cursor.currentDictionary = 0;
  cursor.currentMatchedLeft = 0;

  if (nonNull == 0) {
    // Every stream of an all-null column must be empty.
    if (!lengthBytes.empty() || !dataBytes.empty()) {
      failMalformedPayload("streams of an all-null column are not empty");
    }
    cursor.isDictionary = false;
    cursor.lengths.reset(nullptr, 0, 0);
    return;
  }
  if (!cursor.isDictionary) {
    cursor.lengths.reset(
        reinterpret_cast<const uint8_t*>(lengthBytes.data()),
        lengthBytes.size(),
        nonNull);
    return;
  }

  // The dictionary sequence in the data stream, then the index
  // segment and fallback lengths in the length stream. Every dictionary's
  // entries go into one flat table; per dictionary its first entry, entry
  // count and matched count.
  const auto* data = reinterpret_cast<const uint8_t*>(dataBytes.data());
  const size_t dataSize = dataBytes.size();
  size_t pos = 0;
  uint64_t matchedSum = 0;
  while (true) {
    if (pos >= dataSize) {
      failMalformedPayload("unterminated dictionary sequence");
    }
    const size_t firstEntry = cursor.entries.size();
    uint32_t serialized = 0;
    bool allInline = true;
    uint8_t marker;
    while (true) {
      if (pos >= dataSize) {
        failMalformedPayload("unterminated dictionary sequence");
      }
      marker = data[pos];
      static_assert(
          kDictLastMarker == 0xFF && kDictMoreMarker == kDictLastMarker - 1,
          "the markers are the two highest byte values");
      if (marker >= kDictMoreMarker) {
        ++pos;
        break;
      }
      // Entry: length byte + bytes; the serialized budget bounds both the
      // entry length and the entry count.
      const uint32_t length = marker;
      if (length > kDictEntryMaxLen) {
        failMalformedPayload("dictionary entry too long");
      }
      if (dataSize - pos < 1 + static_cast<size_t>(length)) {
        failMalformedPayload("dictionary entry out of bounds");
      }
      serialized += 1 + length;
      if (serialized > kDictSerializedBudget) {
        failMalformedPayload("dictionary over its serialized budget");
      }
      cursor.entries.emplace_back(
          reinterpret_cast<const char*>(data) + pos + 1, length);
      allInline &= length <= StringView::kInlineSize;
      pos += 1 + length;
    }
    if (dataSize - pos < 4) {
      failMalformedPayload("truncated dictionary matched count");
    }
    const uint32_t matchedRows = memory::loadUnaligned<uint32_t>(data + pos);
    pos += 4;
    cursor.dictionaries.push_back(
        {firstEntry,
         cursor.entries.size() - firstEntry,
         matchedRows,
         allInline});
    if (addOverflows(matchedSum, matchedRows, matchedSum)) {
      failMalformedPayload("matched counts overflow");
    }
    if (marker == kDictLastMarker) {
      break;
    }
  }
  // Matched rows are non-null rows.
  if (matchedSum > nonNull) {
    failMalformedPayload("matched counts beyond non-null rows");
  }
  // Length stream: matchedSum 1-byte indexes, then the fallback lengths.
  if (lengthBytes.size() < matchedSum) {
    failMalformedPayload("index bytes out of bounds");
  }
  cursor.indexes = reinterpret_cast<const uint8_t*>(lengthBytes.data());
  cursor.matchedRows = matchedSum;
  cursor.currentDictionary = 0;
  cursor.currentMatchedLeft = cursor.dictionaries[0].matchedRows;
  cursor.lengths.reset(
      cursor.indexes + matchedSum,
      lengthBytes.size() - matchedSum,
      nonNull - matchedSum);
  cursor.data = dataBytes.substr(pos);
}

void CellPayloadDecoder::finishPayload() {
  const auto finish = util::overloaded{
      []<typename T>(const StreamCursor<T>& cursor) {
        if (!cursor.finished()) {
          failMalformedPayload("malformed value stream");
        }
      },
      [](const TimestampCursor& cursor) {
        if (!cursor.seconds.finished() || !cursor.nanos.finished()) {
          failMalformedPayload("malformed timestamp stream");
        }
      },
      [](const StringCursor& cursor) {
        if (!cursor.lengths.finished()) {
          failMalformedPayload(
              cursor.isDictionary ? "malformed fallback length stream"
                                  : "malformed string length stream");
        }
        if (cursor.dataOffset != cursor.data.size()) {
          failMalformedPayload(
              cursor.isDictionary ? "fallback data length mismatch"
                                  : "string data length mismatch");
        }
      },
      // openColumn() checked the entire raw stream's length.
      []<typename T>(const RawCursor<T>&) {}};
  for (const auto& cursor : cursors_) {
    std::visit(finish, cursor);
  }
}

void CellPayloadDecoder::openBatch(vector_size_t capacity) {
  BOLT_CHECK(!batchOpen_, "a batch is already open");
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    auto& values = batch_[col].values;
    switch (layout_.rowType()->childAt(col)->kind()) {
      case TypeKind::BOOLEAN:
        values = AlignedBuffer::allocate<bool>(capacity, pool_);
        break;
      case TypeKind::TINYINT:
        values = AlignedBuffer::allocate<int8_t>(capacity, pool_);
        break;
      case TypeKind::SMALLINT:
        values = AlignedBuffer::allocate<int16_t>(capacity, pool_);
        break;
      case TypeKind::INTEGER:
        values = AlignedBuffer::allocate<int32_t>(capacity, pool_);
        break;
      case TypeKind::BIGINT:
        values = AlignedBuffer::allocate<int64_t>(capacity, pool_);
        break;
      case TypeKind::REAL:
        values = AlignedBuffer::allocate<float>(capacity, pool_);
        break;
      case TypeKind::DOUBLE:
        values = AlignedBuffer::allocate<double>(capacity, pool_);
        break;
      case TypeKind::HUGEINT:
        values = AlignedBuffer::allocate<int128_t>(capacity, pool_);
        break;
      case TypeKind::TIMESTAMP:
        values = AlignedBuffer::allocate<Timestamp>(capacity, pool_);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        values = AlignedBuffer::allocate<StringView>(capacity, pool_);
        break;
      default:
        break;
    }
  }
  batchOpen_ = true;
  batchCapacity_ = capacity;
  batchRows_ = 0;
  batchStringBytes_ = 0;
}

void CellPayloadDecoder::dropBatch() {
  for (auto& column : batch_) {
    column.values.reset();
    column.nulls.reset();
    column.strings.clear();
    column.charsFree = nullptr;
    column.charsLeft = 0;
  }
  batchOpen_ = false;
  batchCapacity_ = 0;
  batchRows_ = 0;
  batchStringBytes_ = 0;
}

void CellPayloadDecoder::reserveRows(uint64_t minCapacity) {
  if (minCapacity <= static_cast<uint64_t>(batchCapacity_)) {
    return;
  }
  BOLT_CHECK_LE(
      minCapacity,
      std::numeric_limits<vector_size_t>::max(),
      "batch beyond one vector");
  if (batchOpen_) {
    growBatch(minCapacity);
  } else {
    openBatch(static_cast<vector_size_t>(minCapacity));
  }
}

void CellPayloadDecoder::appendRows(uint32_t numRows) {
  BOLT_CHECK_LE(numRows, payloadRowsLeft());
  reserveRows(
      std::max<uint64_t>(1, static_cast<uint64_t>(batchRows_) + numRows));
  if (numRows == 0) {
    return;
  }
  // Bound temporary dense values independently of the output vector size.
  while (numRows > 0) {
    const uint32_t chunk = std::min(numRows, kMaxDecodeChunkRows);
    for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
      appendColumnChunk(
          col, chunk, appendNulls(col, chunk), *batch_[col].values);
    }
    batchRows_ += chunk;
    rowsRead_ += chunk;
    numRows -= chunk;
  }
  if (rowsRead_ == rowCount_) {
    finishPayload();
  }
}

void CellPayloadDecoder::appendColumnChunk(
    uint32_t col,
    uint32_t numRows,
    uint32_t nonNull,
    Buffer& values) {
  std::visit(
      util::overloaded{
          [&]<typename T>(StreamCursor<T>& cursor) {
            appendEncoded(col, numRows, nonNull, values, cursor);
          },
          [&]<typename T>(RawCursor<T>& cursor) {
            appendRaw(col, numRows, nonNull, values, cursor);
          },
          [&](RawCursor<bool>& cursor) {
            appendBoolean(col, numRows, nonNull, values, cursor);
          },
          [&](TimestampCursor& cursor) {
            appendTimestamp(col, numRows, nonNull, values, cursor);
          },
          [&](StringCursor& cursor) {
            appendString(col, numRows, nonNull, values, cursor);
          }},
      cursors_[col]);
}

void CellPayloadDecoder::growBatch(uint64_t rows) {
  constexpr uint64_t kMaxRows = std::numeric_limits<vector_size_t>::max();
  // Doubling stops 7 rows short of one vector, where a null buffer's int32
  // bit count no longer rounds up to whole bytes.
  const uint64_t capacity =
      std::max(rows, std::min<uint64_t>(kMaxRows - 7, 2ull * batchCapacity_));
  // Values and nulls move, the new rows' null bits set as in a new batch;
  // string bytes stay in their buffers.
  for (uint32_t col = 0; col < layout_.numColumns(); ++col) {
    auto& column = batch_[col];
    if (column.values != nullptr) {
      const auto kind = layout_.rowType()->childAt(col)->kind();
      AlignedBuffer::reallocate<char>(
          &column.values,
          kind == TypeKind::BOOLEAN ? (capacity + 7) / 8
                                    : capacity * decodedRowBytes(kind));
    }
    if (column.nulls != nullptr) {
      AlignedBuffer::reallocate<uint8_t>(
          &column.nulls, (capacity + 7) / 8, uint8_t{0xFF});
    }
  }
  batchCapacity_ = static_cast<vector_size_t>(capacity);
}

uint32_t CellPayloadDecoder::appendNulls(uint32_t col, uint32_t numRows) {
  const auto nulls = [&] {
    auto& buffer = batch_[col].nulls;
    if (buffer == nullptr) {
      buffer = allocateNulls(batchCapacity_, pool_);
    }
    return buffer->asMutable<uint64_t>();
  };
  switch (nullTags_[col]) {
    case NullTag::kAllNull:
      bits::fillBits(nulls(), batchRows_, batchRows_ + numRows, bits::kNull);
      return 0;
    case NullTag::kRawNull: {
      // The payload bitmap and the vector's nulls share the convention, a
      // set bit for a value: whole words copy across. A batch's nulls start
      // all set, so only words holding a null need writing.
      uint64_t* batchNulls = nullptr;
      uint32_t numNonNullValues = 0;
      forEachBitWord(
          bitmaps_[col],
          payloadBitmapBytes(),
          rowsRead_,
          numRows,
          [&](uint32_t base, uint32_t n, uint64_t word) {
            numNonNullValues +=
                static_cast<uint32_t>(__builtin_popcountll(word));
            if (word != lowBits(n)) {
              if (batchNulls == nullptr) {
                batchNulls = nulls();
              }
              storeBitWord(batchNulls, batchRows_ + base, word, n);
            }
          });
      return numNonNullValues;
    }
    default:
      return numRows;
  }
}

template <typename T, typename Read>
bool CellPayloadDecoder::placeValues(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    T* out,
    const Read& read) {
  if (numNonNullValues == numRows) {
    return read(numNonNullValues, out);
  }
  T* values = nullptr;
  if (numNonNullValues > 0) {
    values = reinterpret_cast<T*>(resetBuffer(
        denseScratch_,
        static_cast<size_t>(numNonNullValues) * sizeof(T),
        pool_));
    if (!read(numNonNullValues, values)) {
      return false;
    }
  }
  const uint8_t* bitmap = bitmaps_[col];
  if (bitmap == nullptr) {
    std::fill_n(out, numRows, T{});
    return true;
  }
  // A word of values is one copy; a word holding nulls clears, then places
  // its values.
  uint32_t next = 0;
  forEachBitWord(
      bitmap,
      payloadBitmapBytes(),
      rowsRead_,
      numRows,
      [&](uint32_t base, uint32_t n, uint64_t word) {
        T* block = out + base;
        if (word == lowBits(n)) {
          ::memcpy(block, values + next, static_cast<size_t>(n) * sizeof(T));
          next += n;
          return;
        }
        std::fill_n(block, n, T{});
        while (word != 0) {
          block[__builtin_ctzll(word)] = values[next++];
          word &= word - 1;
        }
      });
  return true;
}

template <typename T>
void CellPayloadDecoder::appendEncoded(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    Buffer& values,
    StreamCursor<T>& cursor) {
  T* out = values.asMutable<T>() + batchRows_;
  if (!placeValues<T>(
          col, numRows, numNonNullValues, out, [&](uint32_t count, T* dst) {
            return cursor.next(count, dst);
          })) {
    failMalformedPayload("malformed value stream");
  }
}

template <typename T>
void CellPayloadDecoder::appendRaw(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    Buffer& values,
    RawCursor<T>& cursor) {
  // openColumn() validated the stream size for all non-null values.
  const auto bytes = streamViews_[layout_.firstStream(col)];
  const char* src = bytes.data() + cursor.valuesRead * sizeof(T);
  T* out = values.asMutable<T>() + batchRows_;
  placeValues<T>(
      col, numRows, numNonNullValues, out, [&](uint32_t count, T* dst) {
        ::memcpy(dst, src, static_cast<size_t>(count) * sizeof(T));
        return true;
      });
  cursor.valuesRead += numNonNullValues;
}

void CellPayloadDecoder::appendBoolean(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    Buffer& values,
    RawCursor<bool>& cursor) {
  // openColumn() validated the stream size for all non-null values.
  const auto bytes = streamViews_[layout_.firstStream(col)];
  const auto* src =
      reinterpret_cast<const uint8_t*>(bytes.data()) + cursor.valuesRead;
  auto* out = values.asMutable<uint64_t>();
  for (uint32_t i = 0; i < numNonNullValues; ++i) {
    if (src[i] > 1) {
      failMalformedPayload("invalid boolean byte");
    }
  }
  cursor.valuesRead += numNonNullValues;
  if (numNonNullValues == numRows) {
    for (uint32_t i = 0; i < numRows; ++i) {
      bits::setBit(out, batchRows_ + i, src[i] != 0);
    }
    return;
  }
  const uint8_t* bitmap = bitmaps_[col];
  if (bitmap == nullptr) {
    bits::fillBits(out, batchRows_, batchRows_ + numRows, false);
    return;
  }
  uint32_t next = 0;
  scatterRows(
      bitmap,
      payloadBitmapBytes(),
      rowsRead_,
      numRows,
      [&](uint32_t base, uint32_t n) {
        bits::fillBits(out, batchRows_ + base, batchRows_ + base + n, false);
      },
      [&](uint32_t row) {
        bits::setBit(out, batchRows_ + row, src[next++] != 0);
      });
}

void CellPayloadDecoder::appendTimestamp(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    Buffer& values,
    TimestampCursor& cursor) {
  auto* scratch = reinterpret_cast<int64_t*>(resetBuffer(
      denseScratch_,
      2 * sizeof(int64_t) * static_cast<size_t>(numNonNullValues),
      pool_));
  int64_t* seconds = scratch;
  int64_t* nanos = scratch + numNonNullValues;
  if (numNonNullValues > 0 &&
      (!cursor.seconds.next(numNonNullValues, seconds) ||
       !cursor.nanos.next(numNonNullValues, nanos))) {
    failMalformedPayload("malformed timestamp stream");
  }
  for (uint32_t i = 0; i < numNonNullValues; ++i) {
    // Untrusted values must not reach the Timestamp constructor's range
    // checks, which abort in debug builds.
    if (seconds[i] < Timestamp::kMinSeconds ||
        seconds[i] > Timestamp::kMaxSeconds || nanos[i] < 0 ||
        nanos[i] > static_cast<int64_t>(Timestamp::kMaxNanos)) {
      failMalformedPayload("timestamp out of range");
    }
  }
  auto* out = values.asMutable<Timestamp>() + batchRows_;
  if (numNonNullValues == numRows) {
    for (uint32_t i = 0; i < numRows; ++i) {
      out[i] = Timestamp(seconds[i], nanos[i]);
    }
    return;
  }
  const uint8_t* bitmap = bitmaps_[col];
  if (bitmap == nullptr) {
    std::fill_n(out, numRows, Timestamp());
    return;
  }
  uint32_t next = 0;
  scatterRows(
      bitmap,
      payloadBitmapBytes(),
      rowsRead_,
      numRows,
      [&](uint32_t base, uint32_t n) {
        std::fill_n(out + base, n, Timestamp());
      },
      [&](uint32_t row) {
        out[row] = Timestamp(seconds[next], nanos[next]);
        ++next;
      });
}

void CellPayloadDecoder::appendString(
    uint32_t col,
    uint32_t numRows,
    uint32_t numNonNullValues,
    Buffer& values,
    StringCursor& cursor) {
  auto* out = values.asMutable<StringView>() + batchRows_;
  if (numNonNullValues == 0) {
    std::fill_n(out, numRows, StringView());
    return;
  }
  StringRows rows(
      out,
      numNonNullValues == numRows ? nullptr : bitmaps_[col],
      payloadBitmapBytes(),
      rowsRead_,
      numRows);
  if (cursor.isDictionary) {
    appendStringDictionary(col, numNonNullValues, rows, cursor);
  } else {
    appendStringFlat(col, numNonNullValues, rows, cursor);
  }
  rows.finish();
}

// Flatten only the decoding/materialization kernels: allocation, error
// reporting and length decoding keep explicit noinline boundaries.
[[gnu::flatten]] FOLLY_NOINLINE void CellPayloadDecoder::appendStringFlat(
    uint32_t col,
    uint32_t numNonNullValues,
    StringRows& rows,
    StringCursor& cursor) {
  constexpr uint32_t kLengthsPerBlock = kCacheLineBytes / sizeof(int64_t);
  static_assert(kLengthsPerBlock == 8);
  using FullBlock = std::integral_constant<uint32_t, kLengthsPerBlock>;

  // Validate and materialize one natural length block before decoding the
  // next. A complete block has a compile-time trip count of eight; a staged
  // remainder or the stream's tail uses its actual count.
  const auto take = [&](auto count, const int64_t* lengths) {
    uint64_t dataBytes = 0;
    uint64_t heapChars = 0;
    if constexpr (std::is_same_v<decltype(count), FullBlock>) {
      uint64_t lengthBits = 0;
      // Preserve the reduction loop until vectorization instead of early
      // scalar unrolling after this lambda is inlined.
#pragma GCC unroll 1
      for (uint32_t i = 0; i < kLengthsPerBlock; ++i) {
        const int64_t length = lengths[i];
        const auto value = static_cast<uint64_t>(length);
        lengthBits |= value;
        dataBytes += value;
        heapChars +=
            length > static_cast<int64_t>(StringView::kInlineSize) ? value : 0;
      }
      // Any negative or oversized length has a bit above the int32 range.
      // Unsigned sums of malformed lengths are discarded before use.
      if (lengthBits > std::numeric_limits<int32_t>::max()) {
        failMalformedPayload(
            cursor.isDictionary ? "fallback length out of range"
                                : "string length out of range");
      }
    } else {
      for (uint32_t i = 0; i < count; ++i) {
        const auto length = static_cast<uint64_t>(lengths[i]);
        if (length > std::numeric_limits<int32_t>::max()) {
          failMalformedPayload(
              cursor.isDictionary ? "fallback length out of range"
                                  : "string length out of range");
        }
        dataBytes += length;
        if (length > StringView::kInlineSize) {
          heapChars += length;
        }
      }
    }
    // At most eight validated int32 lengths: neither sum can overflow.
    if (dataBytes > cursor.data.size() - cursor.dataOffset) {
      failMalformedPayload(
          cursor.isDictionary ? "fallback data length mismatch"
                              : "string data length mismatch");
    }
    const char* source = cursor.data.data();
    if (cursor.dataOffset != 0) {
      source += cursor.dataOffset;
    }
    char* chars = heapChars == 0 ? nullptr : stringSpace(col, heapChars);
    if (heapChars != 0 && heapChars == dataBytes) {
      // Only heap strings and empty strings: their bytes are contiguous
      // in both buffers. Reserve/copy once, then build the group's views.
      ::memcpy(chars, source, dataBytes);
      rows.place(count, [&](StringView& value, uint32_t i) {
        const auto size = static_cast<int32_t>(lengths[i]);
        folly::assume(
            size == 0 || size > static_cast<int32_t>(StringView::kInlineSize));
        value.set(chars, size);
        chars += size;
      });
    } else {
      rows.place(count, [&](StringView& value, uint32_t i) {
        const auto size = static_cast<int32_t>(lengths[i]);
        materializeString(value, source, size, chars);
        if (size != 0) {
          source += size;
        }
      });
    }
    cursor.dataOffset += dataBytes;
  };

  uint32_t remaining = numNonNullValues;
  while (remaining > 0) {
    const int64_t* lengths = nullptr;
    const uint32_t count = cursor.lengths.nextBlock(remaining, lengths);
    if (count == 0) {
      failMalformedPayload(
          cursor.isDictionary ? "malformed fallback length stream"
                              : "malformed string length stream");
    }
    if (count == kLengthsPerBlock) {
      take(FullBlock{}, lengths);
    } else {
      take(count, lengths);
    }
    remaining -= count;
  }
}

[[gnu::flatten]] FOLLY_NOINLINE void CellPayloadDecoder::appendStringDictionary(
    uint32_t col,
    uint32_t numNonNullValues,
    StringRows& rows,
    StringCursor& cursor) {
  constexpr uint32_t kLengthsPerBlock = kCacheLineBytes / sizeof(int64_t);
  static_assert(kLengthsPerBlock == 8);
  using FullBlock = std::integral_constant<uint32_t, kLengthsPerBlock>;

  uint32_t remaining = numNonNullValues;
  // Matched rows precede the fallback rows; matched rows resolve against
  // their dictionary. They have no encoded lengths, but use the same small
  // groups and output row cursor as the fallback blocks.
  auto hits = static_cast<uint32_t>(std::min<uint64_t>(
      numNonNullValues, cursor.matchedRows - cursor.matchedRead));
  remaining -= hits;
  const auto takeMatched = [&](auto count, const Dictionary& dictionary) {
    const auto* indexes = cursor.indexes + cursor.matchedRead;
    uint8_t maxIndex = 0;
    for (uint32_t i = 0; i < count; ++i) {
      maxIndex = std::max(maxIndex, indexes[i]);
    }
    if (maxIndex >= dictionary.numEntries) {
      failMalformedPayload("dictionary index out of range");
    }
    const auto* entries = cursor.entries.data() + dictionary.firstEntry;
    if (dictionary.allInline) {
      // Inline entries already own their bytes, including zeroed padding.
      rows.place(count, [&](StringView& value, uint32_t i) {
        value = entries[indexes[i]];
      });
    } else {
      const StringView* values[kLengthsPerBlock];
      uint64_t heapChars = 0;
      for (uint32_t i = 0; i < count; ++i) {
        const auto& entry = entries[indexes[i]];
        if (!entry.isInline()) {
          heapChars += entry.size();
        }
        values[i] = &entry;
      }
      char* chars = heapChars == 0 ? nullptr : stringSpace(col, heapChars);
      rows.place(count, [&](StringView& value, uint32_t i) {
        const auto& entry = *values[i];
        if (entry.isInline()) {
          value = entry;
        } else {
          materializeString(value, entry.data(), entry.size(), chars);
        }
      });
    }
  };
  while (hits > 0) {
    while (cursor.currentMatchedLeft == 0) {
      ++cursor.currentDictionary;
      cursor.currentMatchedLeft =
          cursor.dictionaries[cursor.currentDictionary].matchedRows;
    }
    // Keep the entry base and matched counters fixed for the whole group.
    const auto& dictionary = cursor.dictionaries[cursor.currentDictionary];
    const uint32_t count =
        std::min({hits, cursor.currentMatchedLeft, kLengthsPerBlock});
    if (count == kLengthsPerBlock) {
      takeMatched(FullBlock{}, dictionary);
    } else {
      takeMatched(count, dictionary);
    }
    cursor.matchedRead += count;
    cursor.currentMatchedLeft -= count;
    hits -= count;
  }
  if (remaining > 0) {
    appendStringFlat(col, remaining, rows, cursor);
  }
}

FOLLY_NOINLINE char* CellPayloadDecoder::stringSpace(
    uint32_t col,
    uint64_t bytes) {
  // Small appends share one buffer per column and batch.
  constexpr uint64_t kMinStringBufferBytes = 32 << 10;
  auto& column = batch_[col];
  if (column.charsLeft < bytes) {
    const uint64_t size = std::max(bytes, kMinStringBufferBytes);
    auto buffer = AlignedBuffer::allocate<char>(size, pool_);
    column.charsFree = buffer->asMutable<char>();
    column.charsLeft = size;
    column.strings.push_back(std::move(buffer));
  }
  char* out = column.charsFree;
  column.charsFree += bytes;
  column.charsLeft -= bytes;
  batchStringBytes_ += bytes;
  return out;
}

template <typename T>
VectorPtr CellPayloadDecoder::makeFlat(uint32_t col, const TypePtr& type) {
  auto& column = batch_[col];
  return std::make_shared<FlatVector<T>>(
      pool_,
      type,
      std::move(column.nulls),
      batchRows_,
      std::move(column.values),
      std::move(column.strings));
}

RowVectorPtr CellPayloadDecoder::finishBatch(bool appendRemainingRows) {
  if (appendRemainingRows && !batchOpen_ && rowsRead_ == 0) {
    const auto& type = layout_.rowType();
    const auto rows = rowCount_;
    const auto& types = type->children();
    if (projectionBatch_.use_count() == 1 && projectionBatch_->type() == type &&
        !projectionBatch_->containsLazyNotLoaded() &&
        projectionBatch_->children().size() == types.size()) {
      // Each child is resized or replaced below, including shared slices.
      projectionBatch_->resetNulls();
      projectionBatch_->BaseVector::prepareForReuse();
      projectionBatch_->unsafeResize(rows);
    } else {
      projectionBatch_ = std::make_shared<RowVector>(
          pool_,
          type,
          nullptr,
          rows,
          std::vector<VectorPtr>(layout_.numColumns()));
    }
    auto& columns = projectionBatch_->children();
    for (uint32_t col = 0; col < columns.size(); ++col) {
      auto& output = columns[col];
      const uint32_t nonNull = nonNullCount_[col];
      const bool reusable = output.use_count() == 1 &&
          output->type() == types[col] && output->pool() == pool_;
      if (nonNull == 0) {
        if (reusable && output->isConstantEncoding() &&
            output->mayHaveNulls()) {
          output->resize(rows);
          output->setNullCount(rows);
        } else {
          output = BaseVector::createNullConstant(types[col], rows, pool_);
        }
        continue;
      }
      BufferPtr nulls;
      Buffer* values = nullptr;
      if (reusable && output->isFlatEncoding()) {
        if (output->nulls()) {
          if (nonNull < rows && output->nulls()->isMutable()) {
            nulls = output->nulls();
          }
          output->resetNulls();
        }
        output->prepareForReuse();
        // prepareForReuse discards shared values, including a retained slice.
        if (output->values()) {
          output->resize(rows);
          values = output->values().get();
        }
      }
      if (!values) {
        output = BaseVector::create(types[col], rows, pool_);
        values = output->values().get();
      }
      if (nonNull < rows) {
        const auto bytes = bits::nwords(rows) * sizeof(uint64_t);
        auto* bitmap = resetBuffer(nulls, bytes, pool_);
        std::memset(bitmap + bytes - sizeof(uint64_t), 0, sizeof(uint64_t));
        std::memcpy(bitmap, bitmaps_[col], payloadBitmapBytes());
        output->setNulls(nulls);
        output->setNullCount(rows - nonNull);
      }
      // The whole column shares one bitmap and writes directly into its vector.
      for (uint32_t offset = 0; offset < rows;) {
        const uint32_t count = std::min(rows - offset, kMaxDecodeChunkRows);
        uint32_t chunkNonNull = nonNull;
        if (count < rows) {
          chunkNonNull = nonNull == rows
              ? count
              : bits::countBits(output->rawNulls() + offset / 64, 0, count);
        }
        rowsRead_ = offset;
        batchRows_ = offset;
        appendColumnChunk(col, count, chunkNonNull, *values);
        offset += count;
      }
      if (std::holds_alternative<StringCursor>(cursors_[col])) {
        auto& column = batch_[col];
        output->asUnchecked<FlatVector<StringView>>()->setStringBuffers(
            std::move(column.strings));
        column.charsFree = nullptr;
        column.charsLeft = 0;
      }
    }
    finishPayload();
    rowsRead_ = rows;
    batchRows_ = 0;
    batchStringBytes_ = 0;
    return projectionBatch_;
  }
  if (appendRemainingRows) {
    appendRows(payloadRowsLeft());
  }
  if (!batchOpen_) {
    return nullptr;
  }
  const uint32_t numColumns = layout_.numColumns();
  const auto& rowType = layout_.rowType();
  std::vector<VectorPtr> children(numColumns);
  for (uint32_t col = 0; col < numColumns; ++col) {
    const auto& type = rowType->childAt(col);
    switch (type->kind()) {
      case TypeKind::BOOLEAN:
        children[col] = makeFlat<bool>(col, type);
        break;
      case TypeKind::TINYINT:
        children[col] = makeFlat<int8_t>(col, type);
        break;
      case TypeKind::SMALLINT:
        children[col] = makeFlat<int16_t>(col, type);
        break;
      case TypeKind::INTEGER:
        children[col] = makeFlat<int32_t>(col, type);
        break;
      case TypeKind::BIGINT:
        children[col] = makeFlat<int64_t>(col, type);
        break;
      case TypeKind::REAL:
        children[col] = makeFlat<float>(col, type);
        break;
      case TypeKind::DOUBLE:
        children[col] = makeFlat<double>(col, type);
        break;
      case TypeKind::HUGEINT:
        children[col] = makeFlat<int128_t>(col, type);
        break;
      case TypeKind::TIMESTAMP:
        children[col] = makeFlat<Timestamp>(col, type);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        children[col] = makeFlat<StringView>(col, type);
        break;
      default:
        BOLT_UNREACHABLE();
    }
  }
  auto out = std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(nullptr), batchRows_, std::move(children));
  dropBatch();
  return out;
}

// Writer side.

void buildNullBody(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t rows,
    BufferPtr& out,
    memory::MemoryPool* pool) {
  const uint32_t numColumns = in.layout->numColumns();
  if (numColumns == 0) {
    return;
  }
  const uint32_t tagBytes = nullTagBytes(numColumns);
  const size_t tagStart = out ? out->size() : 0;
  resizeBuffer(out, checkedPlus<size_t>(tagStart, tagBytes), pool);
  auto* tags = out->asMutable<uint8_t>() + tagStart;
  ::memset(tags, 0, tagBytes);
  for (uint32_t col = 0; col < numColumns; ++col) {
    const uint32_t positions =
        in.columnPositions != nullptr && in.columnPositions[col] != nullptr
        ? in.columnPositions[col][pid]
        : rows;
    const auto summary = in.nulls->summarize(pid, col, positions);
    setNullTag(tags, col, summary.tag);
  }
  // Records follow the tags for kRawNull columns, in column order, written
  // together once `out` has room for them all.
  std::vector<uint32_t> rawColumns;
  size_t recordsBytes = 0;
  for (uint32_t col = 0; col < numColumns; ++col) {
    if (getNullTag(out->as<uint8_t>() + tagStart, col) == NullTag::kRawNull) {
      rawColumns.push_back(col);
      recordsBytes += in.nulls->recordsBytes(pid, col);
    }
  }
  if (rawColumns.empty()) {
    return;
  }
  const size_t at = out->size();
  resizeBuffer(out, checkedPlus<size_t>(at, recordsBytes), pool);
  in.nulls->emitRecords(pid, rawColumns, out->asMutable<uint8_t>() + at);
}

uint64_t appendPayloadPrefix(
    const CellWindowInput& in,
    uint32_t pid,
    uint32_t runCount,
    RunCompressor& compressor,
    BufferPtr& out,
    memory::MemoryPool* pool) {
  const uint32_t rows = in.rowCounts[pid];
  appendLe32(out, rows, pool);
  appendLe32(out, runCount, pool);
  appendLe64(out, in.variableBytes[pid], pool);
  const size_t nullSizesAt = out->size();
  appendLe64(out, 0, pool); // stored and decoded sizes, patched below
  const size_t nullBodyAt = out->size();
  buildNullBody(in, pid, rows, out, pool);
  const uint32_t nullDecodedSize = compressor.compressNullBody(out, nullBodyAt);
  const size_t nullStoredBytes = out->size() - nullBodyAt;
  BOLT_CHECK_LE(
      nullStoredBytes,
      std::numeric_limits<uint32_t>::max(),
      "null body exceeds u32 size limit");
  const auto nullStoredSize = static_cast<uint32_t>(nullStoredBytes);
  memory::storeUnaligned<uint32_t>(
      out->asMutable<char>() + nullSizesAt, nullStoredSize);
  memory::storeUnaligned<uint32_t>(
      out->asMutable<char>() + nullSizesAt + 4, nullDecodedSize);
  appendBytes(out, in.encodingTags.data(), in.encodingTags.size(), pool);
  return nullDecodedSize == 0 ? 0 : nullDecodedSize - nullStoredSize;
}

uint64_t residentBytes(const CellWindowInput& in, uint32_t pid) {
  uint64_t total = 0;
  for (uint32_t s = 0; s < in.layout->numStreams(); ++s) {
    total += in.cells->bytes(pid, s);
  }
  return total;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
