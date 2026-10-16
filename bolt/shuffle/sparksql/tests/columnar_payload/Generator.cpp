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

#include "bolt/shuffle/sparksql/tests/columnar_payload/Generator.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

void appendLe(std::vector<uint8_t>& out, uint64_t value, size_t bytes) {
  for (size_t i = 0; i < bytes; ++i) {
    out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
  }
}

/// Writes `values`, each `bitWidth` bits wide, LSB-first into a fresh byte
/// run appended to `out`. Trailing bits of the last byte stay zero.
void appendBitPacked(
    const std::vector<uint64_t>& values,
    size_t bitWidth,
    std::vector<uint8_t>& out) {
  if (bitWidth == 0 || values.empty()) {
    return;
  }
  const size_t start = out.size();
  const size_t bytes = (values.size() * bitWidth + 7) / 8;
  out.resize(start + bytes, 0);

  size_t bitPos = 0;
  for (const uint64_t value : values) {
    const uint64_t mask = bitWidth >= 64 ? std::numeric_limits<uint64_t>::max()
                                         : ((1ULL << bitWidth) - 1);
    const uint64_t masked = value & mask;
    for (size_t bit = 0; bit < bitWidth; ++bit) {
      if ((masked >> bit) & 1ULL) {
        const size_t absolute = bitPos + bit;
        out[start + absolute / 8] |= static_cast<uint8_t>(1u << (absolute % 8));
      }
    }
    bitPos += bitWidth;
  }
}

/// Smallest byte count whose sign extension reproduces `value`, or 0 when no
/// width up to `maxBytes` can.
size_t narrowBytesFor(int64_t value, size_t maxBytes) {
  for (size_t bytes = 1; bytes <= maxBytes; ++bytes) {
    if (bytes >= 8) {
      return bytes;
    }
    const int64_t low = -(int64_t{1} << (8 * bytes - 1));
    const int64_t high = (int64_t{1} << (8 * bytes - 1)) - 1;
    if (value >= low && value <= high) {
      return bytes;
    }
  }
  return 0;
}

/// Smallest signed bit width representing every value, capped at `maxBits`.
/// Returns 0 when no width in range works.
size_t signedBitWidthFor(
    const std::vector<int64_t>& values,
    size_t begin,
    size_t count,
    size_t maxBits) {
  for (size_t bits = 1; bits <= maxBits; ++bits) {
    const int64_t low = -(int64_t{1} << (bits - 1));
    const int64_t high = (int64_t{1} << (bits - 1)) - 1;
    bool fits = true;
    for (size_t i = 0; i < count; ++i) {
      const int64_t value = values[begin + i];
      if (value < low || value > high) {
        fits = false;
        break;
      }
    }
    if (fits) {
      return bits;
    }
  }
  return 0;
}

size_t unsignedBitWidthFor(uint64_t maxDelta) {
  size_t bits = 0;
  while (maxDelta != 0) {
    ++bits;
    maxDelta >>= 1;
  }
  return bits;
}

/// Adds enough legal split offsets from a freely splittable byte range that
/// any requested Run count can be honoured, without listing every offset of a
/// very large range.
void addFreeRange(size_t begin, size_t end, std::vector<size_t>& out) {
  if (end <= begin) {
    return;
  }
  const size_t span = end - begin;
  const size_t stride = span <= 4096 ? 1 : span / 64;
  for (size_t offset = begin; offset < end; offset += stride) {
    out.push_back(offset);
  }
  out.push_back(end);
}

/// Picks `runCount` chunk boundaries, snapping each target to a legal split.
/// The result always starts at 0 and ends at `size`.
std::vector<size_t>
chooseSplits(size_t size, std::vector<size_t> legal, size_t runCount) {
  legal.push_back(0);
  legal.push_back(size);
  std::sort(legal.begin(), legal.end());
  legal.erase(std::unique(legal.begin(), legal.end()), legal.end());

  std::vector<size_t> splits;
  splits.reserve(runCount + 1);
  splits.push_back(0);
  for (size_t run = 1; run < runCount; ++run) {
    const size_t target = size * run / runCount;
    const auto it = std::lower_bound(legal.begin(), legal.end(), splits.back());
    auto best = it;
    for (auto candidate = it; candidate != legal.end(); ++candidate) {
      if (*candidate > target) {
        break;
      }
      best = candidate;
    }
    splits.push_back(best == legal.end() ? splits.back() : *best);
  }
  splits.push_back(size);
  return splits;
}

} // namespace

void GenerationStats::merge(const GenerationStats& other) {
  for (size_t i = 0; i < 4; ++i) {
    encodingKindBlocks[i] += other.encodingKindBlocks[i];
    nullTags[i] += other.nullTags[i];
  }
  for (size_t i = 0; i < 3; ++i) {
    runLayouts[i] += other.runLayouts[i];
  }
  fullBlocks += other.fullBlocks;
  tailBlocks += other.tailBlocks;
  dictionaries += other.dictionaries;
  dictionaryFallbackValues += other.dictionaryFallbackValues;
  compressedBuffers += other.compressedBuffers;
  storedBuffers += other.storedBuffers;
  emptyStreams += other.emptyStreams;
}

const char* GenerationStats::firstGap() const {
  for (size_t i = 0; i < 4; ++i) {
    if (encodingKindBlocks[i] == 0) {
      return toString(static_cast<EncodingKind>(i));
    }
  }
  for (size_t i = 0; i < 3; ++i) {
    if (runLayouts[i] == 0) {
      return toString(static_cast<CompressionLayout>(i));
    }
  }
  if (nullTags[static_cast<size_t>(NullTag::kAllNull)] == 0) {
    return "ALL_NULL";
  }
  if (nullTags[static_cast<size_t>(NullTag::kNoNull)] == 0) {
    return "NO_NULL";
  }
  if (nullTags[static_cast<size_t>(NullTag::kRawNull)] == 0) {
    return "RAW_NULL";
  }
  if (fullBlocks == 0) {
    return "full block";
  }
  if (tailBlocks == 0) {
    return "tail block";
  }
  if (dictionaries == 0) {
    return "dictionary";
  }
  if (dictionaryFallbackValues == 0) {
    return "dictionary RAW fallback";
  }
  if (compressedBuffers == 0) {
    return "compressed buffer";
  }
  if (storedBuffers == 0) {
    return "stored buffer";
  }
  if (emptyStreams == 0) {
    return "empty stream";
  }
  return nullptr;
}

struct ColumnarPayloadGenerator::StreamSet {
  std::vector<std::vector<uint8_t>> bytes;
  std::vector<std::vector<size_t>> legalSplits;
  uint64_t variableSize{0};
};

void ColumnarPayloadGenerator::encodeBlock(
    const std::vector<int64_t>& values,
    size_t begin,
    size_t count,
    PhysicalType type,
    std::vector<uint8_t>& out) {
  const size_t width = typeWidth(type);
  const size_t sourceBytes = count * width;

  int64_t minValue = values[begin];
  int64_t maxValue = values[begin];
  bool allEqual = true;
  for (size_t i = 1; i < count; ++i) {
    const int64_t value = values[begin + i];
    minValue = std::min(minValue, value);
    maxValue = std::max(maxValue, value);
    if (value != values[begin]) {
      allEqual = false;
    }
  }

  // Candidate bodies, sized per RFC section 7.3.
  const size_t constBytes = allEqual ? narrowBytesFor(values[begin], width) : 0;
  const size_t bitWidth =
      signedBitWidthFor(values, begin, count, std::min<size_t>(63, width * 8));
  const uint64_t maxDelta =
      static_cast<uint64_t>(maxValue) - static_cast<uint64_t>(minValue);
  const size_t deltaBits = unsignedBitWidthFor(maxDelta);

  size_t bestSize = sourceBytes;
  EncodingKind bestKind = EncodingKind::kPlain;
  if (constBytes != 0 && constBytes < bestSize) {
    bestSize = constBytes;
    bestKind = EncodingKind::kConstNarrow;
  }
  if (bitWidth != 0) {
    const size_t size = (count * bitWidth + 7) / 8;
    if (size < bestSize) {
      bestSize = size;
      bestKind = EncodingKind::kBitPack;
    }
  }
  if (deltaBits <= 63) {
    const size_t size = width + (count * deltaBits + 7) / 8;
    if (size < bestSize) {
      bestSize = size;
      bestKind = EncodingKind::kForBitPack;
    }
  }

  switch (options_.encodingPolicy) {
    case EncodingPolicy::kForcePlain:
      bestKind = EncodingKind::kPlain;
      break;
    case EncodingPolicy::kForceConstNarrow:
      if (constBytes != 0) {
        bestKind = EncodingKind::kConstNarrow;
      }
      break;
    case EncodingPolicy::kForceBitPack:
      if (bitWidth != 0) {
        bestKind = EncodingKind::kBitPack;
      }
      break;
    case EncodingPolicy::kForceForBitPack:
      if (deltaBits <= 63) {
        bestKind = EncodingKind::kForBitPack;
      }
      break;
    case EncodingPolicy::kRotate: {
      static constexpr EncodingKind kOrder[] = {
          EncodingKind::kConstNarrow,
          EncodingKind::kBitPack,
          EncodingKind::kForBitPack,
          EncodingKind::kPlain};
      for (size_t attempt = 0; attempt < 4; ++attempt) {
        const auto candidate = kOrder[(blockIndex_ + attempt) % 4];
        if (candidate == EncodingKind::kConstNarrow && constBytes == 0) {
          continue;
        }
        if (candidate == EncodingKind::kBitPack && bitWidth == 0) {
          continue;
        }
        if (candidate == EncodingKind::kForBitPack && deltaBits > 63) {
          continue;
        }
        bestKind = candidate;
        break;
      }
      break;
    }
    case EncodingPolicy::kAuto:
      break;
  }
  ++blockIndex_;

  // The RFC lets a Writer emit any legal parameter, not only the smallest
  // one. Widening here keeps the payload conforming while breaking any
  // Reader that infers body sizes from the data instead of the header.
  const size_t narrowBytes = options_.minimalEncodingWidth ? constBytes : width;
  const size_t packBits = options_.minimalEncodingWidth
      ? bitWidth
      : std::min<size_t>(63, width * 8);
  const size_t forBits = options_.minimalEncodingWidth
      ? deltaBits
      : std::min<size_t>(63, deltaBits + 5);

  stats_.encodingKindBlocks[static_cast<size_t>(bestKind)]++;
  if (sourceBytes == kEncodingBlockSourceBytes) {
    stats_.fullBlocks++;
  } else {
    stats_.tailBlocks++;
  }

  const auto emitHeader = [&](EncodingKind kind, size_t param) {
    out.push_back(static_cast<uint8_t>(
        static_cast<uint8_t>(kind) | (static_cast<uint8_t>(param) << 2)));
  };

  switch (bestKind) {
    case EncodingKind::kConstNarrow: {
      emitHeader(EncodingKind::kConstNarrow, narrowBytes);
      appendLe(out, static_cast<uint64_t>(values[begin]), narrowBytes);
      break;
    }
    case EncodingKind::kBitPack: {
      emitHeader(EncodingKind::kBitPack, packBits);
      std::vector<uint64_t> raw;
      raw.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        raw.push_back(static_cast<uint64_t>(values[begin + i]));
      }
      appendBitPacked(raw, packBits, out);
      break;
    }
    case EncodingKind::kForBitPack: {
      emitHeader(EncodingKind::kForBitPack, forBits);
      appendLe(out, static_cast<uint64_t>(minValue), width);
      std::vector<uint64_t> deltas;
      deltas.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        deltas.push_back(
            static_cast<uint64_t>(values[begin + i]) -
            static_cast<uint64_t>(minValue));
      }
      appendBitPacked(deltas, forBits, out);
      break;
    }
    case EncodingKind::kPlain: {
      emitHeader(EncodingKind::kPlain, 0);
      for (size_t i = 0; i < count; ++i) {
        appendLe(out, static_cast<uint64_t>(values[begin + i]), width);
      }
      break;
    }
  }
}

bool ColumnarPayloadGenerator::buildEncodingLoopStream(
    const std::vector<int64_t>& values,
    PhysicalType type,
    std::vector<uint8_t>& out,
    std::vector<size_t>& blockBoundaries,
    std::string& error) {
  const size_t width = typeWidth(type);
  if (width == 0 || kEncodingBlockSourceBytes % width != 0) {
    error = std::string("type_width must divide 64 for ") + toString(type);
    return false;
  }
  const size_t totalSourceBytes = values.size() * width;
  const size_t fullBlocks = totalSourceBytes / kEncodingBlockSourceBytes;
  const size_t tailSourceBytes = totalSourceBytes % kEncodingBlockSourceBytes;
  const size_t valuesPerBlock = kEncodingBlockSourceBytes / width;

  blockBoundaries.push_back(out.size());
  for (size_t block = 0; block < fullBlocks; ++block) {
    encodeBlock(values, block * valuesPerBlock, valuesPerBlock, type, out);
    blockBoundaries.push_back(out.size());
  }
  if (tailSourceBytes != 0) {
    encodeBlock(
        values,
        fullBlocks * valuesPerBlock,
        tailSourceBytes / width,
        type,
        out);
    blockBoundaries.push_back(out.size());
  }
  return true;
}

StringEncoding ColumnarPayloadGenerator::stringEncodingFor(
    size_t column) const {
  if (column < options_.stringEncodings.size()) {
    return options_.stringEncodings[column];
  }
  return options_.useDictionary ? StringEncoding::kDictionary
                                : StringEncoding::kRaw;
}

bool ColumnarPayloadGenerator::buildStringStreams(
    const FlatColumn& column,
    StringEncoding encoding,
    std::vector<uint8_t>& lengthStream,
    std::vector<size_t>& lengthBoundaries,
    std::vector<uint8_t>& dataStream,
    std::vector<size_t>& dataBoundaries,
    std::string& error) {
  const auto& values = column.stringValues;
  if (values.empty()) {
    // RFC section 9: a column with no non-null values contributes nothing.
    return true;
  }

  if (encoding == StringEncoding::kRaw) {
    std::vector<int64_t> lengths;
    lengths.reserve(values.size());
    for (const auto& value : values) {
      lengths.push_back(static_cast<int64_t>(value.size()));
      dataStream.insert(dataStream.end(), value.begin(), value.end());
    }
    addFreeRange(0, dataStream.size(), dataBoundaries);
    return buildEncodingLoopStream(
        lengths, PhysicalType::kBigint, lengthStream, lengthBoundaries, error);
  }

  struct ClosedDictionary {
    std::vector<std::string> entries;
    uint32_t matched{0};
  };

  const size_t maxDictionaries = std::max<size_t>(1, options_.maxDictionaries);
  std::vector<ClosedDictionary> closed;
  std::vector<std::string> current;
  size_t currentBytes = 0;
  uint32_t currentMatched = 0;
  bool fallback = false;

  std::vector<uint8_t> indexes;
  std::vector<int64_t> fallbackLengths;
  std::vector<uint8_t> fallbackBytes;

  const auto emitFallback = [&](const std::string& value) {
    fallbackLengths.push_back(static_cast<int64_t>(value.size()));
    fallbackBytes.insert(fallbackBytes.end(), value.begin(), value.end());
  };

  for (const auto& value : values) {
    if (fallback) {
      emitFallback(value);
      continue;
    }

    const auto it = std::find(current.begin(), current.end(), value);
    if (it != current.end()) {
      indexes.push_back(
          static_cast<uint8_t>(std::distance(current.begin(), it)));
      ++currentMatched;
      continue;
    }

    const size_t cost = 1 + value.size();
    if (current.size() < kMaxDictionaryEntries &&
        currentBytes + cost < kDictionaryMaxSerializedBytes) {
      indexes.push_back(static_cast<uint8_t>(current.size()));
      current.push_back(value);
      currentBytes += cost;
      ++currentMatched;
      continue;
    }

    // The current dictionary cannot take the value. Open another one unless
    // the budget is spent or no empty dictionary could hold it either.
    const bool fitsInEmpty = cost < kDictionaryMaxSerializedBytes;
    if (fitsInEmpty && closed.size() + 2 <= maxDictionaries) {
      closed.push_back({current, currentMatched});
      current.clear();
      current.push_back(value);
      currentBytes = cost;
      currentMatched = 1;
      indexes.push_back(0);
      continue;
    }

    fallback = true;
    emitFallback(value);
  }

  // Serialize the dictionary sequence. Every dictionary must stay inside one
  // Run, so a split is legal only after a terminator's matched_row_count.
  dataBoundaries.push_back(0);
  for (const auto& dictionary : closed) {
    for (const auto& entry : dictionary.entries) {
      dataStream.push_back(static_cast<uint8_t>(entry.size()));
      dataStream.insert(dataStream.end(), entry.begin(), entry.end());
    }
    dataStream.push_back(kDictionaryContinue);
    appendLe(dataStream, dictionary.matched, sizeof(uint32_t));
    dataBoundaries.push_back(dataStream.size());
  }
  for (const auto& entry : current) {
    dataStream.push_back(static_cast<uint8_t>(entry.size()));
    dataStream.insert(dataStream.end(), entry.begin(), entry.end());
  }
  dataStream.push_back(kDictionaryFinal);
  appendLe(dataStream, currentMatched, sizeof(uint32_t));
  stats_.dictionaries += closed.size() + 1;
  stats_.dictionaryFallbackValues += fallbackLengths.size();
  const size_t fallbackOffset = dataStream.size();
  dataStream.insert(
      dataStream.end(), fallbackBytes.begin(), fallbackBytes.end());
  addFreeRange(fallbackOffset, dataStream.size(), dataBoundaries);

  // The index region splits anywhere; the fallback length region only on
  // Encoding Block boundaries.
  lengthStream = indexes;
  addFreeRange(0, indexes.size(), lengthBoundaries);
  std::vector<size_t> tailBoundaries;
  if (!buildEncodingLoopStream(
          fallbackLengths,
          PhysicalType::kBigint,
          lengthStream,
          tailBoundaries,
          error)) {
    return false;
  }
  // tailBoundaries are already absolute offsets into lengthStream, which
  // began with the index segment; adding indexes.size() again would place a
  // legal split inside an Encoding Block.
  for (const size_t boundary : tailBoundaries) {
    lengthBoundaries.push_back(boundary);
  }
  return true;
}

bool ColumnarPayloadGenerator::buildStreams(
    const FlatTable& table,
    StreamSet& out,
    std::string& error) {
  for (size_t columnIndex = 0; columnIndex < table.columns.size();
       ++columnIndex) {
    const auto& column = table.columns[columnIndex];
    if (column.isNull.size() != table.rowCount) {
      error = "isNull size does not match rowCount";
      return false;
    }
    const size_t nonNull = column.nonNullCount();

    if (column.type == PhysicalType::kString) {
      if (column.stringValues.size() != nonNull) {
        error = "stringValues size does not match non-null count";
        return false;
      }
      for (const auto& value : column.stringValues) {
        out.variableSize += value.size();
      }
      std::vector<uint8_t> lengthStream;
      std::vector<uint8_t> dataStream;
      std::vector<size_t> lengthBoundaries;
      std::vector<size_t> dataBoundaries;
      if (!buildStringStreams(
              column,
              stringEncodingFor(columnIndex),
              lengthStream,
              lengthBoundaries,
              dataStream,
              dataBoundaries,
              error)) {
        return false;
      }
      out.bytes.push_back(std::move(lengthStream));
      out.legalSplits.push_back(std::move(lengthBoundaries));
      out.bytes.push_back(std::move(dataStream));
      out.legalSplits.push_back(std::move(dataBoundaries));
      continue;
    }

    if (column.type == PhysicalType::kFloat ||
        column.type == PhysicalType::kDouble) {
      if (column.doubleValues.size() != nonNull) {
        error = "doubleValues size does not match non-null count";
        return false;
      }
      std::vector<uint8_t> stream;
      for (const double value : column.doubleValues) {
        if (column.type == PhysicalType::kFloat) {
          const auto narrowed = static_cast<float>(value);
          uint32_t bits = 0;
          std::memcpy(&bits, &narrowed, sizeof(bits));
          appendLe(stream, bits, 4);
        } else {
          uint64_t bits = 0;
          std::memcpy(&bits, &value, sizeof(bits));
          appendLe(stream, bits, 8);
        }
      }
      std::vector<size_t> boundaries;
      addFreeRange(0, stream.size(), boundaries);
      out.bytes.push_back(std::move(stream));
      out.legalSplits.push_back(std::move(boundaries));
      continue;
    }

    if (column.intValues.size() != nonNull) {
      error = "intValues size does not match non-null count";
      return false;
    }
    const int64_t low = typeMin(column.type);
    const int64_t high = typeMax(column.type);
    for (const int64_t value : column.intValues) {
      if (value < low || value > high) {
        error = std::string("value out of range for ") + toString(column.type);
        return false;
      }
    }

    std::vector<uint8_t> stream;
    std::vector<size_t> boundaries;
    if (usesEncodingLoop(column.type)) {
      if (!buildEncodingLoopStream(
              column.intValues, column.type, stream, boundaries, error)) {
        return false;
      }
    } else {
      const size_t width = typeWidth(column.type);
      for (const int64_t value : column.intValues) {
        appendLe(stream, static_cast<uint64_t>(value), width);
      }
      addFreeRange(0, stream.size(), boundaries);
    }
    out.bytes.push_back(std::move(stream));
    out.legalSplits.push_back(std::move(boundaries));
  }
  return true;
}

bool ColumnarPayloadGenerator::generate(
    const FlatTable& table,
    GeneratedPayload& out,
    std::string& error) {
  if (table.columns.empty()) {
    error = "column_count must be >= 1";
    return false;
  }
  const bool needsCodec = options_.compress || options_.compressNullBody;
  if (needsCodec && codec_ == nullptr) {
    error = "codec is required when compression is enabled";
    return false;
  }

  blockIndex_ = 0;
  stats_ = GenerationStats{};
  StreamSet streams;
  if (!buildStreams(table, streams, error)) {
    return false;
  }

  // Null region, RFC section 4.2.
  const size_t columnCount = table.columns.size();
  std::vector<NullTag> tags(columnCount, NullTag::kNoNull);
  std::vector<uint8_t> nullBody((columnCount * 2 + 7) / 8, 0);
  std::vector<uint8_t> bitmaps;
  const size_t bitmapBytes = (table.rowCount + 7) / 8;

  for (size_t column = 0; column < columnCount; ++column) {
    const auto& source = table.columns[column];
    const size_t nonNull = source.nonNullCount();
    NullTag tag = NullTag::kRawNull;
    if (table.rowCount == 0) {
      tag = NullTag::kNoNull;
    } else if (options_.degenerateNullTags && nonNull == 0) {
      tag = NullTag::kAllNull;
    } else if (options_.degenerateNullTags && nonNull == table.rowCount) {
      tag = NullTag::kNoNull;
    }
    tags[column] = tag;
    stats_.nullTags[static_cast<size_t>(tag)]++;
    nullBody[column / 4] |=
        static_cast<uint8_t>(static_cast<uint8_t>(tag) << ((column % 4) * 2));
  }
  for (size_t column = 0; column < columnCount; ++column) {
    if (tags[column] != NullTag::kRawNull) {
      continue;
    }
    const auto& source = table.columns[column];
    std::vector<uint8_t> bitmap(bitmapBytes, 0);
    for (uint32_t row = 0; row < table.rowCount; ++row) {
      if (!source.isNull[row]) {
        bitmap[row / 8] |= static_cast<uint8_t>(1u << (row % 8));
      }
    }
    bitmaps.insert(bitmaps.end(), bitmap.begin(), bitmap.end());
  }
  nullBody.insert(nullBody.end(), bitmaps.begin(), bitmaps.end());

  std::vector<uint8_t> nullStored;
  uint32_t nullDecodedSize = 0;
  if (options_.compressNullBody) {
    nullStored = codec_->compress(nullBody.data(), nullBody.size());
    nullDecodedSize = static_cast<uint32_t>(nullBody.size());
  } else {
    nullStored = nullBody;
  }

  // Encoding tags, RFC section 3.2.
  std::vector<uint8_t> encodingTags((columnCount + 7) / 8, 0);
  for (size_t column = 0; column < columnCount; ++column) {
    // A column with no non-null value carries no stream at all, so leave its
    // tag clear rather than announcing a dictionary that is not there.
    if (table.columns[column].type == PhysicalType::kString &&
        table.columns[column].nonNullCount() != 0 &&
        stringEncodingFor(column) == StringEncoding::kDictionary) {
      encodingTags[column / 8] |= static_cast<uint8_t>(1u << (column % 8));
    }
  }

  // Run splitting, RFC sections 5.3 and 5.4.
  size_t totalStreamBytes = 0;
  for (const auto& stream : streams.bytes) {
    totalStreamBytes += stream.size();
  }
  // Section 9 allows run_count == 0 with rows present only when every column
  // is ALL_NULL. With degenerateNullTags off an all-null column is tagged
  // RAW_NULL instead, so the payload still needs one (empty) Run to be legal.
  bool everyColumnAllNull = true;
  for (const auto tag : tags) {
    if (tag != NullTag::kAllNull) {
      everyColumnAllNull = false;
      break;
    }
  }
  const bool mayOmitRuns = table.rowCount == 0 || everyColumnAllNull;
  const bool mustEmitEmptyRun = totalStreamBytes == 0 && !mayOmitRuns;

  const size_t runCount = totalStreamBytes == 0
      ? (mayOmitRuns ? 0 : 1)
      : std::max<size_t>(1, options_.runCount);

  std::vector<std::vector<size_t>> splits;
  splits.reserve(streams.bytes.size());
  for (size_t stream = 0; stream < streams.bytes.size(); ++stream) {
    splits.push_back(chooseSplits(
        streams.bytes[stream].size(), streams.legalSplits[stream], runCount));
  }

  // COMBINED needs the codec, so it is only reachable when compression is on;
  // COMBINED_STORED is its stored counterpart.
  const CompressionLayout defaultLayout =
      options_.layout == CompressionLayout::kSeparate
      ? CompressionLayout::kSeparate
      : (options_.compress ? CompressionLayout::kCombined
                           : CompressionLayout::kCombinedStored);

  std::vector<CompressionLayout> layoutChoices;
  if (options_.rotateLayoutPerRun) {
    layoutChoices.push_back(CompressionLayout::kSeparate);
    layoutChoices.push_back(CompressionLayout::kCombinedStored);
    if (options_.compress) {
      layoutChoices.push_back(CompressionLayout::kCombined);
    }
  } else {
    layoutChoices.push_back(defaultLayout);
  }

  std::vector<uint8_t> runs;
  std::vector<size_t> runOffsets;
  size_t emittedRuns = 0;
  for (size_t run = 0; run < runCount; ++run) {
    std::vector<std::vector<uint8_t>> chunks;
    chunks.reserve(streams.bytes.size());
    bool anyBytes = false;
    for (size_t stream = 0; stream < streams.bytes.size(); ++stream) {
      const size_t begin = splits[stream][run];
      const size_t end = splits[stream][run + 1];
      chunks.emplace_back(
          streams.bytes[stream].begin() + static_cast<ptrdiff_t>(begin),
          streams.bytes[stream].begin() + static_cast<ptrdiff_t>(end));
      anyBytes = anyBytes || !chunks.back().empty();
    }
    // A Run whose streams are all empty is legal but carries nothing, and
    // RFC section 9 says a Writer should not produce one. Requesting more
    // Runs than a stream has legal split points lands here. The exception is
    // a payload that has no stream bytes at all yet may not omit its Runs.
    if (!anyBytes && !mustEmitEmptyRun) {
      continue;
    }
    ++emittedRuns;
    const CompressionLayout layout =
        layoutChoices[(run + options_.variationSeed) % layoutChoices.size()];
    stats_.runLayouts[static_cast<size_t>(layout)]++;
    runOffsets.push_back(runs.size());
    runs.push_back(static_cast<uint8_t>(layout));

    if (layout == CompressionLayout::kSeparate) {
      size_t streamIndex = 0;
      std::vector<std::vector<uint8_t>> stored;
      std::vector<uint64_t> storedSizes;
      std::vector<uint64_t> decodedSizes;
      stored.reserve(chunks.size());
      for (auto& chunk : chunks) {
        ++streamIndex;
        if (chunk.empty()) {
          stats_.emptyStreams++;
          storedSizes.push_back(0);
          decodedSizes.push_back(0);
          stored.emplace_back();
        } else if (
            options_.compress &&
            (!options_.compressPerStream ||
             ((run + streamIndex + options_.variationSeed) % 2) == 0)) {
          auto compressed = codec_->compress(chunk.data(), chunk.size());
          stats_.compressedBuffers++;
          storedSizes.push_back(compressed.size());
          decodedSizes.push_back(chunk.size());
          stored.push_back(std::move(compressed));
        } else {
          stats_.storedBuffers++;
          storedSizes.push_back(chunk.size());
          decodedSizes.push_back(0);
          stored.push_back(std::move(chunk));
        }
      }
      for (const uint64_t size : storedSizes) {
        appendLe(runs, size, sizeof(uint64_t));
      }
      for (const uint64_t size : decodedSizes) {
        appendLe(runs, size, sizeof(uint64_t));
      }
      for (const auto& buffer : stored) {
        runs.insert(runs.end(), buffer.begin(), buffer.end());
      }
    } else {
      std::vector<uint8_t> combined;
      std::vector<uint64_t> decodedSizes;
      for (const auto& chunk : chunks) {
        decodedSizes.push_back(chunk.size());
        combined.insert(combined.end(), chunk.begin(), chunk.end());
      }
      std::vector<uint8_t> stored = layout == CompressionLayout::kCombined
          ? codec_->compress(combined.data(), combined.size())
          : combined;
      if (layout == CompressionLayout::kCombined) {
        stats_.compressedBuffers++;
      } else {
        stats_.storedBuffers++;
      }
      appendLe(runs, stored.size(), sizeof(uint64_t));
      for (const uint64_t size : decodedSizes) {
        appendLe(runs, size, sizeof(uint64_t));
      }
      runs.insert(runs.end(), stored.begin(), stored.end());
    }
  }

  // Assemble, RFC section 3.
  std::vector<uint8_t> bytes;
  appendLe(bytes, table.rowCount, sizeof(uint32_t));
  appendLe(bytes, emittedRuns, sizeof(uint32_t));
  appendLe(
      bytes,
      options_.variableSizeOverride < 0
          ? streams.variableSize
          : static_cast<uint64_t>(options_.variableSizeOverride),
      sizeof(uint64_t));
  appendLe(bytes, nullStored.size(), sizeof(uint32_t));
  appendLe(bytes, nullDecodedSize, sizeof(uint32_t));

  PayloadLayout resultLayout;
  resultLayout.nullBodyOffset = bytes.size();
  bytes.insert(bytes.end(), nullStored.begin(), nullStored.end());
  resultLayout.encodingTagsOffset = bytes.size();
  bytes.insert(bytes.end(), encodingTags.begin(), encodingTags.end());
  resultLayout.runsOffset = bytes.size();
  for (const size_t offset : runOffsets) {
    resultLayout.runOffsets.push_back(resultLayout.runsOffset + offset);
  }
  bytes.insert(bytes.end(), runs.begin(), runs.end());

  out.bytes = std::move(bytes);
  out.layout = std::move(resultLayout);
  out.stats = stats_;
  out.streams = std::move(streams.bytes);
  return true;
}

FlatColumn intColumn(PhysicalType type, std::vector<int64_t> values) {
  FlatColumn column;
  column.type = type;
  column.isNull.assign(values.size(), false);
  column.intValues = std::move(values);
  return column;
}

FlatColumn doubleColumn(PhysicalType type, std::vector<double> values) {
  FlatColumn column;
  column.type = type;
  column.isNull.assign(values.size(), false);
  column.doubleValues = std::move(values);
  return column;
}

FlatColumn stringColumn(std::vector<std::string> values) {
  FlatColumn column;
  column.type = PhysicalType::kString;
  column.isNull.assign(values.size(), false);
  column.stringValues = std::move(values);
  return column;
}

FlatColumn nullableIntColumn(
    PhysicalType type,
    std::vector<bool> isNull,
    std::vector<int64_t> values) {
  FlatColumn column;
  column.type = type;
  column.isNull = std::move(isNull);
  column.intValues = std::move(values);
  return column;
}

FlatColumn nullColumn(PhysicalType type, size_t rowCount) {
  FlatColumn column;
  column.type = type;
  column.isNull.assign(rowCount, true);
  return column;
}

FlatTable oneColumn(FlatColumn column) {
  FlatTable table;
  table.rowCount = static_cast<uint32_t>(column.isNull.size());
  table.columns.push_back(std::move(column));
  return table;
}

FlatTable tableOf(std::vector<FlatColumn> columns) {
  FlatTable table;
  table.rowCount =
      columns.empty() ? 0 : static_cast<uint32_t>(columns[0].isNull.size());
  table.columns = std::move(columns);
  return table;
}

namespace {

std::vector<int64_t> ramp(size_t count, int64_t base, int64_t step) {
  std::vector<int64_t> values;
  values.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    values.push_back(base + static_cast<int64_t>(i) * step);
  }
  return values;
}

} // namespace

std::vector<NamedTable> boundaryCorpus() {
  std::vector<NamedTable> corpus;
  const auto add = [&](const char* name, FlatTable table) {
    corpus.push_back({name, std::move(table)});
  };

  // Value patterns, one per Encoding Block kind the generator can pick.
  add("constantNarrow",
      oneColumn(intColumn(PhysicalType::kBigint, std::vector<int64_t>(40, 7))));
  add("constantFullWidth",
      oneColumn(intColumn(
          PhysicalType::kBigint,
          std::vector<int64_t>(40, std::numeric_limits<int64_t>::min()))));
  add("symmetricSmall",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(40, -8, 1))));
  add("offsetRamp",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(40, 1000000, 3))));
  add("fullRange",
      oneColumn(intColumn(
          PhysicalType::kBigint,
          {std::numeric_limits<int64_t>::min(),
           -1,
           0,
           1,
           std::numeric_limits<int64_t>::max()})));

  // Either side of the 64 source byte block boundary, for a 4 byte type.
  add("blockMinusOne",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(15, 0, 1))));
  add("exactlyOneBlock",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(16, 0, 1))));
  add("oneValueTail",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(17, 0, 1))));
  add("twoBlocksNoTail",
      oneColumn(intColumn(PhysicalType::kInteger, ramp(32, 0, 1))));

  // Every NullTag, plus the shapes around them.
  add("allNull", oneColumn(nullColumn(PhysicalType::kBigint, 24)));
  add("noNull", oneColumn(intColumn(PhysicalType::kBigint, ramp(24, 5, 1))));
  {
    FlatColumn column;
    column.type = PhysicalType::kBigint;
    column.isNull.assign(24, false);
    for (size_t row = 0; row < 24; ++row) {
      if (row % 2 == 0) {
        column.isNull[row] = true;
      } else {
        column.intValues.push_back(static_cast<int64_t>(row));
      }
    }
    add("alternatingNull", oneColumn(std::move(column)));
  }
  add("singleRow", oneColumn(intColumn(PhysicalType::kInteger, {42})));
  add("noRows", oneColumn(nullColumn(PhysicalType::kBigint, 0)));

  // String and dictionary limits. An entry costs 1 + length bytes and the
  // total must stay below 64, so 15 entries of length 3 fill a dictionary
  // exactly and a sixteenth distinct value has to open another one.
  add("emptyStrings", oneColumn(stringColumn({"", "", "", ""})));
  add("maxDictionaryEntryLength",
      oneColumn(stringColumn(
          {std::string(kMaxDictionaryEntryLength, 'a'), "b", "b"})));
  add("aboveDictionaryEntryLength",
      oneColumn(stringColumn(
          {std::string(kMaxDictionaryEntryLength + 1, 'a'), "b", "b"})));
  {
    std::vector<std::string> values;
    for (size_t i = 0; i < 15; ++i) {
      values.push_back("v" + std::to_string(i / 10) + std::to_string(i % 10));
    }
    for (size_t i = 0; i < 15; ++i) {
      values.push_back(values[i]);
    }
    add("dictionaryExactlyFull", oneColumn(stringColumn(std::move(values))));
  }
  {
    std::vector<std::string> values;
    for (size_t i = 0; i < 40; ++i) {
      values.push_back("w" + std::to_string(i / 10) + std::to_string(i % 10));
    }
    add("dictionaryOverflow", oneColumn(stringColumn(std::move(values))));
  }
  {
    std::vector<std::string> values;
    for (size_t i = 0; i < 60; ++i) {
      values.push_back(
          std::string(20, static_cast<char>('a' + (i % 26))) +
          std::to_string(i));
    }
    add("highCardinality", oneColumn(stringColumn(std::move(values))));
  }

  // Float payloads that must survive bit for bit.
  add("doubleSpecials",
      oneColumn(doubleColumn(
          PhysicalType::kDouble,
          {std::numeric_limits<double>::quiet_NaN(),
           std::numeric_limits<double>::infinity(),
           -std::numeric_limits<double>::infinity(),
           -0.0,
           0.0,
           std::numeric_limits<double>::denorm_min()})));
  add("floatSpecials",
      oneColumn(doubleColumn(
          PhysicalType::kFloat,
          {std::numeric_limits<float>::quiet_NaN(),
           std::numeric_limits<float>::infinity(),
           -0.0,
           1.5})));

  // Every supported type in one table, so that the Stream ordering of section
  // 1.4 is exercised with mixed widths and a variable length column.
  {
    FlatTable table;
    table.rowCount = 20;
    for (const auto type :
         {PhysicalType::kTinyInt,
          PhysicalType::kSmallInt,
          PhysicalType::kInteger,
          PhysicalType::kBigint,
          PhysicalType::kDate}) {
      table.columns.push_back(intColumn(type, ramp(20, 1, 1)));
    }
    std::vector<double> fractions;
    for (size_t row = 0; row < 20; ++row) {
      fractions.push_back(static_cast<double>(row) / 4.0);
    }
    for (const auto type : {PhysicalType::kFloat, PhysicalType::kDouble}) {
      table.columns.push_back(doubleColumn(type, fractions));
    }
    {
      std::vector<std::string> values;
      for (size_t row = 0; row < 20; ++row) {
        values.push_back("s" + std::to_string(row % 4));
      }
      table.columns.push_back(stringColumn(std::move(values)));
    }
    add("allTypes", std::move(table));
  }

  // Type range extremes, where narrowing and sign extension must agree.
  add("tinyIntExtremes",
      oneColumn(intColumn(PhysicalType::kTinyInt, {-128, -1, 0, 1, 127})));
  add("smallIntExtremes",
      oneColumn(intColumn(PhysicalType::kSmallInt, {-32768, -1, 0, 1, 32767})));
  add("integerExtremes",
      oneColumn(intColumn(
          PhysicalType::kInteger, {-2147483648LL, -1, 0, 1, 2147483647LL})));

  for (auto& entry : corpus) {
    entry.table = normalized(entry.table);
  }
  return corpus;
}

} // namespace bytedance::bolt::shuffle::sparksql::test
