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

#include "bolt/shuffle/sparksql/tests/ColumnarPayloadValidator.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

/// Bounds checked little-endian reader. Every read is guarded, so a truncated
/// or hostile payload produces a violation rather than an out of bounds read.
class Reader {
 public:
  Reader(const uint8_t* data, size_t size) : data_(data), size_(size) {}

  bool has(size_t bytes) const {
    return bytes <= size_ - std::min(size_, offset_);
  }

  bool readUint(size_t bytes, uint64_t& value) {
    if (!has(bytes)) {
      return false;
    }
    value = 0;
    for (size_t i = 0; i < bytes; ++i) {
      value |= static_cast<uint64_t>(data_[offset_ + i]) << (8 * i);
    }
    offset_ += bytes;
    return true;
  }

  bool readBytes(size_t bytes, const uint8_t*& begin) {
    if (!has(bytes)) {
      return false;
    }
    begin = data_ + offset_;
    offset_ += bytes;
    return true;
  }

  size_t offset() const {
    return offset_;
  }

  size_t remaining() const {
    return size_ - std::min(size_, offset_);
  }

 private:
  const uint8_t* data_;
  size_t size_;
  size_t offset_{0};
};

bool addOverflows(uint64_t lhs, uint64_t rhs) {
  return lhs > std::numeric_limits<uint64_t>::max() - rhs;
}

int64_t signExtend(uint64_t value, size_t bits) {
  if (bits == 0 || bits >= 64) {
    return static_cast<int64_t>(value);
  }
  const uint64_t mask = (1ULL << bits) - 1;
  const uint64_t masked = value & mask;
  const uint64_t signBit = 1ULL << (bits - 1);
  if ((masked & signBit) != 0) {
    return static_cast<int64_t>(masked | ~mask);
  }
  return static_cast<int64_t>(masked);
}

uint64_t readBitPacked(
    const uint8_t* data,
    size_t bitOffset,
    size_t bitWidth) {
  uint64_t value = 0;
  for (size_t bit = 0; bit < bitWidth; ++bit) {
    const size_t absolute = bitOffset + bit;
    if ((data[absolute / 8] >> (absolute % 8)) & 1u) {
      value |= 1ULL << bit;
    }
  }
  return value;
}

/// True when a structure spanning [begin, end) crosses a Run boundary, which
/// RFC section 5.4 forbids for Encoding Blocks and for a single Dictionary.
bool straddlesRunBoundary(
    const std::vector<size_t>& boundaries,
    size_t begin,
    size_t end) {
  for (const size_t boundary : boundaries) {
    if (boundary > begin && boundary < end) {
      return true;
    }
  }
  return false;
}

/// True when every bit at or above `validBits` is zero.
bool tailBitsZero(const uint8_t* data, size_t byteCount, size_t validBits) {
  for (size_t bit = validBits; bit < byteCount * 8; ++bit) {
    if ((data[bit / 8] >> (bit % 8)) & 1u) {
      return false;
    }
  }
  return true;
}

} // namespace

const char* toString(Check check) {
  switch (check) {
#define BOLT_COLUMNAR_PAYLOAD_CHECK_NAME(name, value, text) \
  case Check::name:                                         \
    return text;
    BOLT_COLUMNAR_PAYLOAD_CHECKS(BOLT_COLUMNAR_PAYLOAD_CHECK_NAME)
#undef BOLT_COLUMNAR_PAYLOAD_CHECK_NAME
  }
  return "?";
}

bool isLevelOne(Check check) {
  const int value = static_cast<int>(check);
  return value >= 1 && value <= 19;
}

bool ValidationResult::okAtLevelOne() const {
  for (const auto& violation : violations) {
    if (isLevelOne(violation.check) ||
        violation.check == Check::kStructural) {
      return false;
    }
  }
  return true;
}

bool ValidationResult::has(Check check) const {
  for (const auto& violation : violations) {
    if (violation.check == check) {
      return true;
    }
  }
  return false;
}

std::string ValidationResult::describe() const {
  if (violations.empty()) {
    return "conforming";
  }
  std::string text;
  for (const auto& violation : violations) {
    text += toString(violation.check);
    text += " at ";
    text += std::to_string(violation.offset);
    text += ": ";
    text += violation.message;
    text += "\n";
  }
  return text;
}

namespace {

void report(
    ValidationResult& result,
    Check check,
    size_t offset,
    std::string message) {
  result.violations.push_back({check, std::move(message), offset});
}

} // namespace

bool ColumnarPayloadValidator::decodeEnvelope(
    const uint8_t* stored,
    uint64_t storedSize,
    uint64_t decodedSize,
    size_t offset,
    Check mismatchCheck,
    std::vector<uint8_t>& out,
    ValidationResult& result) {
  if (storedSize == 0) {
    if (decodedSize != 0) {
      report(
          result,
          Check::kEmptyStreamSizes,
          offset,
          "stored size is 0 but decoded size is " +
              std::to_string(decodedSize));
      return false;
    }
    out.clear();
    return true;
  }
  if (decodedSize == 0) {
    out.assign(stored, stored + storedSize);
    return true;
  }
  if (options_.maxDecodedBytes != 0 && decodedSize > options_.maxDecodedBytes) {
    report(
        result,
        mismatchCheck,
        offset,
        "decoded size " + std::to_string(decodedSize) + " exceeds the limit");
    return false;
  }
  if (codec_ == nullptr) {
    report(
        result,
        Check::kStructural,
        offset,
        "payload contains a compressed buffer but no codec was supplied");
    return false;
  }
  if (!codec_->decompress(
          stored,
          static_cast<size_t>(storedSize),
          static_cast<size_t>(decodedSize),
          out)) {
    report(result, Check::kStructural, offset, "codec failed to decompress");
    return false;
  }
  if (out.size() != decodedSize) {
    report(
        result,
        mismatchCheck,
        offset,
        "decoded " + std::to_string(out.size()) + " bytes, declared " +
            std::to_string(decodedSize));
    return false;
  }
  return true;
}

bool ColumnarPayloadValidator::decodeEncodingLoop(
    const std::vector<uint8_t>& stream,
    const std::vector<size_t>& runBoundaries,
    size_t begin,
    PhysicalType type,
    size_t valueCount,
    std::vector<int64_t>& out,
    size_t& consumed,
    ValidationResult& result) {
  const size_t width = typeWidth(type);
  const size_t totalSourceBytes = valueCount * width;
  const size_t fullBlocks = totalSourceBytes / kEncodingBlockSourceBytes;
  const size_t tailSourceBytes = totalSourceBytes % kEncodingBlockSourceBytes;
  const size_t blockCount = fullBlocks + (tailSourceBytes != 0 ? 1 : 0);

  size_t offset = begin;
  out.reserve(valueCount);

  for (size_t block = 0; block < blockCount; ++block) {
    const size_t sourceBytes = block < fullBlocks
        ? kEncodingBlockSourceBytes
        : tailSourceBytes;
    const size_t blockValues = sourceBytes / width;

    const size_t blockBegin = offset;
    if (offset >= stream.size()) {
      report(
          result,
          Check::kBlockBodyBounds,
          offset,
          "encoding block header past end of stream");
      consumed = stream.size();
      return false;
    }
    const uint8_t header = stream[offset++];
    const auto kind = static_cast<EncodingKind>(header & 0x03);
    const size_t param = header >> 2;

    const auto bodyOverruns = [&](size_t bodyBytes) {
      if (bodyBytes > stream.size() - offset) {
        report(
            result,
            Check::kBlockBodyBounds,
            offset,
            std::string(toString(kind)) + " body of " +
                std::to_string(bodyBytes) + " bytes overruns the stream");
        return true;
      }
      return false;
    };

    switch (kind) {
      case EncodingKind::kConstNarrow: {
        if (param < 1 || param > width) {
          report(
              result,
              Check::kConstNarrowParam,
              offset - 1,
              "narrow_bytes " + std::to_string(param) + " outside [1, " +
                  std::to_string(width) + "]");
          consumed = stream.size();
          return false;
        }
        if (bodyOverruns(param)) {
          consumed = stream.size();
          return false;
        }
        uint64_t raw = 0;
        for (size_t i = 0; i < param; ++i) {
          raw |= static_cast<uint64_t>(stream[offset + i]) << (8 * i);
        }
        offset += param;
        const int64_t value = isSignedIntegral(type)
            ? signExtend(raw, param * 8)
            : static_cast<int64_t>(raw);
        for (size_t i = 0; i < blockValues; ++i) {
          out.push_back(value);
        }
        break;
      }
      case EncodingKind::kBitPack: {
        const size_t maxBits = std::min<size_t>(63, width * 8);
        if (param < 1 || param > maxBits) {
          report(
              result,
              Check::kBitPackParam,
              offset - 1,
              "bit_width " + std::to_string(param) + " outside [1, " +
                  std::to_string(maxBits) + "]");
          consumed = stream.size();
          return false;
        }
        const size_t bodyBytes = (blockValues * param + 7) / 8;
        if (bodyOverruns(bodyBytes)) {
          consumed = stream.size();
          return false;
        }
        for (size_t i = 0; i < blockValues; ++i) {
          const uint64_t raw =
              readBitPacked(stream.data() + offset, i * param, param);
          out.push_back(
              isSignedIntegral(type) ? signExtend(raw, param)
                                     : static_cast<int64_t>(raw));
        }
        if (options_.enableLevelTwo &&
            !tailBitsZero(
                stream.data() + offset, bodyBytes, blockValues * param)) {
          report(
              result,
              Check::kUnusedBitsZero,
              offset,
              "BIT_PACK trailing bits are not zero");
        }
        offset += bodyBytes;
        break;
      }
      case EncodingKind::kForBitPack: {
        if (param > 63) {
          report(
              result,
              Check::kForBitPackParam,
              offset - 1,
              "delta_bit_width " + std::to_string(param) + " exceeds 63");
          consumed = stream.size();
          return false;
        }
        const size_t deltaBytes = (blockValues * param + 7) / 8;
        if (bodyOverruns(width + deltaBytes)) {
          consumed = stream.size();
          return false;
        }
        uint64_t baseRaw = 0;
        for (size_t i = 0; i < width; ++i) {
          baseRaw |= static_cast<uint64_t>(stream[offset + i]) << (8 * i);
        }
        offset += width;
        const int64_t base = isSignedIntegral(type)
            ? signExtend(baseRaw, width * 8)
            : static_cast<int64_t>(baseRaw);
        const int64_t low = typeMin(type);
        const int64_t high = typeMax(type);
        bool rangeReported = false;
        for (size_t i = 0; i < blockValues; ++i) {
          const uint64_t delta = param == 0
              ? 0
              : readBitPacked(stream.data() + offset, i * param, param);
          const auto value =
              static_cast<int64_t>(static_cast<uint64_t>(base) + delta);
          // Section 7.3 puts the range obligation on the Writer and only
          // permits a Reader to reject, so this is a conformance check on the
          // producer rather than a safety one. It cannot fire for Bigint,
          // whose range is the whole of int64.
          if (options_.enableLevelTwo && !rangeReported &&
              (value < low || value > high)) {
            report(
                result,
                Check::kForBitPackRange,
                blockBegin,
                "FOR_BIT_PACK yields " + std::to_string(value) +
                    ", outside the range of " + toString(type));
            rangeReported = true;
          }
          out.push_back(value);
        }
        if (options_.enableLevelTwo && param != 0 &&
            !tailBitsZero(
                stream.data() + offset, deltaBytes, blockValues * param)) {
          report(
              result,
              Check::kUnusedBitsZero,
              offset,
              "FOR_BIT_PACK trailing bits are not zero");
        }
        offset += deltaBytes;
        break;
      }
      case EncodingKind::kPlain: {
        if (param != 0) {
          report(
              result,
              Check::kPlainParam,
              offset - 1,
              "PLAIN encoding_param must be 0, found " +
                  std::to_string(param));
          consumed = stream.size();
          return false;
        }
        if (bodyOverruns(sourceBytes)) {
          consumed = stream.size();
          return false;
        }
        for (size_t i = 0; i < blockValues; ++i) {
          uint64_t raw = 0;
          for (size_t b = 0; b < width; ++b) {
            raw |= static_cast<uint64_t>(stream[offset + i * width + b])
                << (8 * b);
          }
          out.push_back(
              isSignedIntegral(type) ? signExtend(raw, width * 8)
                                     : static_cast<int64_t>(raw));
        }
        offset += sourceBytes;
        break;
      }
    }

    if (options_.enableLevelTwo &&
        straddlesRunBoundary(runBoundaries, blockBegin, offset)) {
      report(
          result,
          Check::kRunBoundaryStructure,
          blockBegin,
          "encoding block spanning [" + std::to_string(blockBegin) + ", " +
              std::to_string(offset) + ") crosses a Run boundary");
    }
  }

  consumed = offset;
  return true;
}

void ColumnarPayloadValidator::decodeColumn(
    size_t column,
    size_t& streamIndex,
    const std::vector<std::vector<uint8_t>>& streams,
    const std::vector<std::vector<size_t>>& runBoundaries,
    const std::vector<size_t>& nonNullCounts,
    const std::vector<StringEncoding>& encodings,
    ValidationResult& result) {
  const PhysicalType type = schema_[column];
  const size_t nonNull = nonNullCounts[column];
  auto& target = result.decoded.columns[column];
  target.type = type;

  if (type != PhysicalType::kString) {
    const auto& stream = streams[streamIndex];
    const auto& bounds = runBoundaries[streamIndex];
    ++streamIndex;
    const size_t width = typeWidth(type);

    if (usesEncodingLoop(type)) {
      size_t consumed = 0;
      if (!decodeEncodingLoop(
              stream,
              bounds,
              0,
              type,
              nonNull,
              target.intValues,
              consumed,
              result)) {
        return;
      }
      if (options_.enableLevelTwo && consumed != stream.size()) {
        report(
            result,
            Check::kStreamExhausted,
            consumed,
            "stream has " + std::to_string(stream.size() - consumed) +
                " trailing bytes after the last block");
      }
      return;
    }

    const size_t expected = nonNull * width;
    if (stream.size() != expected) {
      report(
          result,
          options_.enableLevelTwo ? Check::kRawDataLength
                                  : Check::kStructural,
          0,
          "raw stream is " + std::to_string(stream.size()) +
              " bytes, expected " + std::to_string(expected));
      if (stream.size() < expected) {
        return;
      }
    }
    for (size_t i = 0; i < nonNull; ++i) {
      uint64_t raw = 0;
      for (size_t b = 0; b < width; ++b) {
        raw |= static_cast<uint64_t>(stream[i * width + b]) << (8 * b);
      }
      switch (type) {
        case PhysicalType::kFloat: {
          const auto bits = static_cast<uint32_t>(raw);
          float value = 0;
          std::memcpy(&value, &bits, sizeof(value));
          target.doubleValues.push_back(static_cast<double>(value));
          break;
        }
        case PhysicalType::kDouble: {
          double value = 0;
          std::memcpy(&value, &raw, sizeof(value));
          target.doubleValues.push_back(value);
          break;
        }
        default:
          target.intValues.push_back(signExtend(raw, width * 8));
          break;
      }
    }
    return;
  }

  const auto& lengthStream = streams[streamIndex];
  const auto& lengthBounds = runBoundaries[streamIndex];
  const auto& dataStream = streams[streamIndex + 1];
  const auto& dataBounds = runBoundaries[streamIndex + 1];
  streamIndex += 2;

  if (nonNull == 0) {
    if (options_.enableLevelTwo &&
        (!lengthStream.empty() || !dataStream.empty())) {
      report(
          result,
          Check::kRawDataLength,
          0,
          "column has no non-null values but its streams are not empty");
    }
    return;
  }

  if (encodings[column] == StringEncoding::kRaw) {
    std::vector<int64_t> lengths;
    size_t consumed = 0;
    if (!decodeEncodingLoop(
            lengthStream,
            lengthBounds,
            0,
            PhysicalType::kBigint,
            nonNull,
            lengths,
            consumed,
            result)) {
      return;
    }
    if (options_.enableLevelTwo && consumed != lengthStream.size()) {
      report(
          result,
          Check::kStreamExhausted,
          consumed,
          "length stream has trailing bytes");
    }
    size_t offset = 0;
    for (const int64_t length : lengths) {
      if (length < 0 ||
          static_cast<uint64_t>(length) > dataStream.size() - offset) {
        report(
            result,
            Check::kRawDataLength,
            offset,
            "string length " + std::to_string(length) +
                " overruns the data stream");
        return;
      }
      target.stringValues.emplace_back(
          reinterpret_cast<const char*>(dataStream.data() + offset),
          static_cast<size_t>(length));
      offset += static_cast<size_t>(length);
    }
    if (options_.enableLevelTwo && offset != dataStream.size()) {
      report(
          result,
          Check::kRawDataLength,
          offset,
          "data stream has " + std::to_string(dataStream.size() - offset) +
              " trailing bytes");
    }
    return;
  }

  // Dictionary encoding: the data stream must be parsed before the index
  // stream can be interpreted (RFC section 8).
  struct Dictionary {
    std::vector<std::string> entries;
    uint32_t matched{0};
  };
  std::vector<Dictionary> dictionaries;
  size_t offset = 0;
  bool sawFinal = false;

  while (!sawFinal) {
    const size_t dictionaryBegin = offset;
    Dictionary dictionary;
    bool terminated = false;
    // Serialized space of the entries in this dictionary, which section 8.1
    // caps below kDictionaryMaxSerializedBytes.
    size_t serialized = 0;
    while (!terminated) {
      if (offset >= dataStream.size()) {
        report(
            result,
            Check::kDictionaryEntryBounds,
            offset,
            "dictionary sequence ended without a terminator");
        return;
      }
      const uint8_t lead = dataStream[offset];
      if (lead == kDictionaryContinue || lead == kDictionaryFinal) {
        ++offset;
        sawFinal = lead == kDictionaryFinal;
        uint32_t matched = 0;
        if (dataStream.size() - offset < sizeof(uint32_t)) {
          report(
              result,
              Check::kDictionaryEntryBounds,
              offset,
              "matched_row_count is truncated");
          return;
        }
        std::memcpy(&matched, dataStream.data() + offset, sizeof(matched));
        offset += sizeof(matched);
        dictionary.matched = matched;
        if (options_.enableLevelTwo &&
            serialized >= kDictionaryMaxSerializedBytes) {
          report(
              result,
              Check::kDictionaryCapacity,
              offset,
              "dictionary holds " + std::to_string(serialized) +
                  " bytes of entries, which is not below " +
                  std::to_string(kDictionaryMaxSerializedBytes));
        }
        terminated = true;
        break;
      }
      const size_t length = lead;
      ++offset;
      if (options_.enableLevelTwo && length > kMaxDictionaryEntryLength) {
        report(
            result,
            Check::kDictionaryCapacity,
            offset - 1,
            "dictionary entry length " + std::to_string(length) +
                " exceeds " + std::to_string(kMaxDictionaryEntryLength));
      }
      serialized += 1 + length;
      if (length > dataStream.size() - offset) {
        report(
            result,
            Check::kDictionaryEntryBounds,
            offset,
            "dictionary entry of " + std::to_string(length) +
                " bytes overruns the stream");
        return;
      }
      dictionary.entries.emplace_back(
          reinterpret_cast<const char*>(dataStream.data() + offset), length);
      offset += length;
    }
    if (options_.enableLevelTwo &&
        straddlesRunBoundary(dataBounds, dictionaryBegin, offset)) {
      report(
          result,
          Check::kRunBoundaryStructure,
          dictionaryBegin,
          "dictionary spanning [" + std::to_string(dictionaryBegin) + ", " +
              std::to_string(offset) + ") crosses a Run boundary");
    }
    dictionaries.push_back(std::move(dictionary));
  }

  uint64_t matchedTotal = 0;
  for (const auto& dictionary : dictionaries) {
    matchedTotal += dictionary.matched;
  }
  if (matchedTotal > nonNull) {
    report(
        result,
        options_.enableLevelTwo ? Check::kMatchedRowCount
                                : Check::kStructural,
        0,
        "matched_row_count sum " + std::to_string(matchedTotal) +
            " exceeds the non-null count " + std::to_string(nonNull));
    return;
  }
  const size_t fallbackCount = nonNull - static_cast<size_t>(matchedTotal);
  const size_t fallbackOffset = offset;

  // Index segments come first in the length stream, one byte each.
  if (matchedTotal > lengthStream.size()) {
    report(
        result,
        Check::kDictionaryIndexRange,
        0,
        "length stream is too short for " + std::to_string(matchedTotal) +
            " indexes");
    return;
  }

  size_t indexOffset = 0;
  for (const auto& dictionary : dictionaries) {
    for (uint32_t i = 0; i < dictionary.matched; ++i) {
      const uint8_t index = lengthStream[indexOffset++];
      if (index >= dictionary.entries.size()) {
        report(
            result,
            Check::kDictionaryIndexRange,
            indexOffset - 1,
            "index " + std::to_string(index) + " is not below the entry count " +
                std::to_string(dictionary.entries.size()));
        return;
      }
      target.stringValues.push_back(dictionary.entries[index]);
    }
  }

  std::vector<int64_t> fallbackLengths;
  size_t consumed = indexOffset;
  if (fallbackCount != 0) {
    if (!decodeEncodingLoop(
            lengthStream,
            lengthBounds,
            indexOffset,
            PhysicalType::kBigint,
            fallbackCount,
            fallbackLengths,
            consumed,
            result)) {
      return;
    }
  }
  if (options_.enableLevelTwo && consumed != lengthStream.size()) {
    report(
        result,
        Check::kStreamExhausted,
        consumed,
        "length stream has trailing bytes after the fallback lengths");
  }

  uint64_t lengthSum = 0;
  size_t dataOffset = fallbackOffset;
  for (const int64_t length : fallbackLengths) {
    if (length < 0 ||
        static_cast<uint64_t>(length) > dataStream.size() - dataOffset) {
      report(
          result,
          Check::kFallbackLengthSum,
          dataOffset,
          "fallback length " + std::to_string(length) +
              " overruns the data stream");
      return;
    }
    target.stringValues.emplace_back(
        reinterpret_cast<const char*>(dataStream.data() + dataOffset),
        static_cast<size_t>(length));
    dataOffset += static_cast<size_t>(length);
    lengthSum += static_cast<uint64_t>(length);
  }
  if (options_.enableLevelTwo &&
      lengthSum != dataStream.size() - fallbackOffset) {
    report(
        result,
        Check::kFallbackLengthSum,
        fallbackOffset,
        "fallback lengths sum to " + std::to_string(lengthSum) +
            " but FallbackRawBytes is " +
            std::to_string(dataStream.size() - fallbackOffset) + " bytes");
  }
}

ValidationResult ColumnarPayloadValidator::validate(
    const uint8_t* data,
    size_t size) {
  ValidationResult result;
  const size_t columnCount = schema_.size();
  if (columnCount == 0) {
    report(result, Check::kStructural, 0, "column_count must be >= 1");
    return result;
  }

  if (size < kFixedHeaderBytes) {
    report(
        result,
        Check::kFixedHeaderBounds,
        0,
        "payload is " + std::to_string(size) + " bytes, shorter than the " +
            std::to_string(kFixedHeaderBytes) + " byte fixed header");
    return result;
  }

  // Rule L1.4 makes a supplied payload_size an upper bound on every read,
  // not merely something to compare against at the end. Clamping the cursor
  // here stops the parser from touching a following frame that shares the
  // buffer.
  size_t limit = size;
  if (options_.payloadSizeProvided) {
    limit = std::min(size, options_.payloadSize);
    if (options_.payloadSize > size) {
      report(
          result,
          Check::kPayloadSizeBounds,
          0,
          "payload_size " + std::to_string(options_.payloadSize) +
              " exceeds the " + std::to_string(size) + " bytes supplied");
    }
  }

  Reader reader(data, limit);

  // Fixed header, RFC section 3.
  uint64_t rowCount = 0;
  uint64_t runCount = 0;
  uint64_t variableSize = 0;
  uint64_t nullStoredSize = 0;
  uint64_t nullDecodedSize = 0;
  if (!reader.readUint(4, rowCount) || !reader.readUint(4, runCount) ||
      !reader.readUint(8, variableSize) ||
      !reader.readUint(4, nullStoredSize) ||
      !reader.readUint(4, nullDecodedSize)) {
    report(
        result,
        Check::kFixedHeaderBounds,
        0,
        "payload is shorter than the 24 byte fixed header");
    return result;
  }
  (void)variableSize; // RFC section 3.1: a hint, never a validation input.

  if (options_.maxRowCount != 0 && rowCount > options_.maxRowCount) {
    report(
        result,
        Check::kStructural,
        0,
        "row_count " + std::to_string(rowCount) + " exceeds the limit");
    return result;
  }
  if (options_.maxRunCount != 0 && runCount > options_.maxRunCount) {
    report(
        result,
        Check::kStructural,
        4,
        "run_count " + std::to_string(runCount) + " exceeds the limit");
    return result;
  }

  if (nullStoredSize < 1) {
    report(
        result,
        Check::kNullStoredSizeBounds,
        16,
        "null_stored_size must be >= 1");
    return result;
  }
  const uint8_t* nullStored = nullptr;
  const size_t nullStoredOffset = reader.offset();
  if (!reader.readBytes(static_cast<size_t>(nullStoredSize), nullStored)) {
    report(
        result,
        Check::kNullStoredSizeBounds,
        nullStoredOffset,
        "null body of " + std::to_string(nullStoredSize) +
            " bytes overruns the payload");
    return result;
  }

  std::vector<uint8_t> nullBody;
  if (!decodeEnvelope(
          nullStored,
          nullStoredSize,
          nullDecodedSize,
          nullStoredOffset,
          Check::kNullDecodedSize,
          nullBody,
          result)) {
    return result;
  }

  const size_t tagBytes = (columnCount * 2 + 7) / 8;
  if (nullBody.size() < tagBytes) {
    report(
        result,
        Check::kNullBodyExpectedSize,
        nullStoredOffset,
        "null body is too short to hold " + std::to_string(columnCount) +
            " tags");
    return result;
  }

  std::vector<NullTag> tags(columnCount);
  size_t rawNullColumnCount = 0;
  for (size_t column = 0; column < columnCount; ++column) {
    const auto tag = static_cast<NullTag>(
        (nullBody[column / 4] >> ((column % 4) * 2)) & 0x03);
    if (tag == NullTag::kReserved) {
      report(
          result,
          Check::kReservedNullTag,
          nullStoredOffset,
          "column " + std::to_string(column) + " uses the reserved NullTag");
      return result;
    }
    tags[column] = tag;
    if (tag == NullTag::kRawNull) {
      ++rawNullColumnCount;
    }
  }
  if (options_.enableLevelTwo &&
      !tailBitsZero(nullBody.data(), tagBytes, columnCount * 2)) {
    report(
        result,
        Check::kUnusedBitsZero,
        nullStoredOffset,
        "unused NullTag bits are not zero");
  }

  // No overflow check is needed on the product: rowCount is already capped by
  // options_.maxRowCount above and rawNullColumnCount is bounded by the
  // schema, which is trusted external context.
  const size_t bitmapBytes = (static_cast<size_t>(rowCount) + 7) / 8;
  const size_t expectedNullBody =
      tagBytes + rawNullColumnCount * bitmapBytes;
  if (nullBody.size() != expectedNullBody) {
    report(
        result,
        Check::kNullBodyExpectedSize,
        nullStoredOffset,
        "null body is " + std::to_string(nullBody.size()) +
            " bytes, expected " + std::to_string(expectedNullBody));
    return result;
  }

  result.decoded.rowCount = static_cast<uint32_t>(rowCount);
  result.decoded.columns.resize(columnCount);
  std::vector<size_t> nonNullCounts(columnCount, 0);
  size_t bitmapOffset = tagBytes;
  for (size_t column = 0; column < columnCount; ++column) {
    auto& target = result.decoded.columns[column];
    target.type = schema_[column];
    target.isNull.assign(static_cast<size_t>(rowCount), false);
    switch (tags[column]) {
      case NullTag::kAllNull:
        target.isNull.assign(static_cast<size_t>(rowCount), true);
        nonNullCounts[column] = 0;
        break;
      case NullTag::kNoNull:
        nonNullCounts[column] = static_cast<size_t>(rowCount);
        break;
      case NullTag::kRawNull: {
        const uint8_t* bitmap = nullBody.data() + bitmapOffset;
        size_t nonNull = 0;
        for (uint64_t row = 0; row < rowCount; ++row) {
          const bool valid = ((bitmap[row / 8] >> (row % 8)) & 1u) != 0;
          target.isNull[static_cast<size_t>(row)] = !valid;
          nonNull += valid ? 1 : 0;
        }
        if (options_.enableLevelTwo &&
            !tailBitsZero(bitmap, bitmapBytes, static_cast<size_t>(rowCount))) {
          report(
              result,
              Check::kUnusedBitsZero,
              nullStoredOffset + bitmapOffset,
              "unused RawNull bitmap bits are not zero");
        }
        nonNullCounts[column] = nonNull;
        bitmapOffset += bitmapBytes;
        break;
      }
      case NullTag::kReserved:
        break;
    }
  }

  // Encoding tags, RFC section 3.2.
  const size_t encodingTagBytes = (columnCount + 7) / 8;
  const uint8_t* encodingTags = nullptr;
  const size_t encodingTagsOffset = reader.offset();
  if (!reader.readBytes(encodingTagBytes, encodingTags)) {
    report(
        result,
        Check::kEncodingTagsBounds,
        encodingTagsOffset,
        "encoding_tags overruns the payload");
    return result;
  }
  std::vector<StringEncoding> encodings(columnCount, StringEncoding::kRaw);
  for (size_t column = 0; column < columnCount; ++column) {
    const bool dictionary =
        ((encodingTags[column / 8] >> (column % 8)) & 1u) != 0;
    encodings[column] =
        dictionary ? StringEncoding::kDictionary : StringEncoding::kRaw;
    if (options_.enableLevelTwo && dictionary &&
        schema_[column] != PhysicalType::kString) {
      report(
          result,
          Check::kEncodingTagNonString,
          encodingTagsOffset,
          "column " + std::to_string(column) +
              " is not a string but has the dictionary tag set");
    }
  }
  if (options_.enableLevelTwo &&
      !tailBitsZero(encodingTags, encodingTagBytes, columnCount)) {
    report(
        result,
        Check::kUnusedBitsZero,
        encodingTagsOffset,
        "unused EncodingTag bits are not zero");
  }

  // Runs, RFC section 5.
  size_t streamTotal = 0;
  for (const auto type : schema_) {
    streamTotal += streamCount(type);
  }
  std::vector<std::vector<uint8_t>> streams(streamTotal);
  // Offsets at which each Run's contribution to a stream ended, used to
  // enforce RFC section 5.4 once the streams are concatenated.
  std::vector<std::vector<size_t>> runBoundaries(streamTotal);

  for (uint64_t run = 0; run < runCount; ++run) {
    const size_t runOffset = reader.offset();
    uint64_t layoutRaw = 0;
    if (!reader.readUint(1, layoutRaw)) {
      report(
          result,
          Check::kStructural,
          runOffset,
          "run " + std::to_string(run) + " is truncated");
      return result;
    }
    if (layoutRaw > static_cast<uint64_t>(CompressionLayout::kCombinedStored)) {
      report(
          result,
          Check::kCompressionLayoutValue,
          runOffset,
          "compression_layout " + std::to_string(layoutRaw) + " is reserved");
      return result;
    }
    const auto layout = static_cast<CompressionLayout>(layoutRaw);
    const size_t bufferCount =
        layout == CompressionLayout::kSeparate ? streamTotal : 1;

    std::vector<uint64_t> storedSizes(bufferCount, 0);
    std::vector<uint64_t> decodedSizes(streamTotal, 0);
    uint64_t storedTotal = 0;
    for (size_t i = 0; i < bufferCount; ++i) {
      if (!reader.readUint(8, storedSizes[i])) {
        report(
            result,
            Check::kStructural,
            reader.offset(),
            "stored_sizes is truncated");
        return result;
      }
      if (addOverflows(storedTotal, storedSizes[i])) {
        report(
            result,
            Check::kRunSizeOverflow,
            runOffset,
            "sum(stored_sizes) overflows");
        return result;
      }
      storedTotal += storedSizes[i];
    }
    uint64_t decodedTotal = 0;
    for (size_t i = 0; i < streamTotal; ++i) {
      if (!reader.readUint(8, decodedSizes[i])) {
        report(
            result,
            Check::kStructural,
            reader.offset(),
            "decoded_sizes is truncated");
        return result;
      }
      if (addOverflows(decodedTotal, decodedSizes[i])) {
        report(
            result,
            Check::kRunSizeOverflow,
            runOffset,
            "sum(decoded_sizes) overflows");
        return result;
      }
      decodedTotal += decodedSizes[i];
    }
    if (storedTotal > reader.remaining()) {
      report(
          result,
          Check::kRunSizeOverflow,
          runOffset,
          "run declares " + std::to_string(storedTotal) +
              " buffer bytes but only " + std::to_string(reader.remaining()) +
              " remain");
      return result;
    }

    if (layout == CompressionLayout::kSeparate) {
      for (size_t i = 0; i < streamTotal; ++i) {
        const uint8_t* stored = nullptr;
        const size_t bufferOffset = reader.offset();
        if (!reader.readBytes(static_cast<size_t>(storedSizes[i]), stored)) {
          report(
              result,
              Check::kStructural,
              bufferOffset,
              "stored buffer is truncated");
          return result;
        }
        std::vector<uint8_t> decoded;
        if (!decodeEnvelope(
                stored,
                storedSizes[i],
                decodedSizes[i],
                bufferOffset,
                Check::kBufferDecodedSize,
                decoded,
                result)) {
          return result;
        }
        streams[i].insert(streams[i].end(), decoded.begin(), decoded.end());
        runBoundaries[i].push_back(streams[i].size());
      }
    } else {
      const uint8_t* stored = nullptr;
      const size_t bufferOffset = reader.offset();
      if (!reader.readBytes(static_cast<size_t>(storedSizes[0]), stored)) {
        report(
            result,
            Check::kStructural,
            bufferOffset,
            "combined buffer is truncated");
        return result;
      }
      std::vector<uint8_t> combined;
      if (layout == CompressionLayout::kCombinedStored) {
        if (storedSizes[0] != decodedTotal) {
          report(
              result,
              Check::kCombinedDecodedSize,
              bufferOffset,
              "COMBINED_STORED buffer is " + std::to_string(storedSizes[0]) +
                  " bytes but the streams sum to " +
                  std::to_string(decodedTotal));
          return result;
        }
        combined.assign(stored, stored + storedSizes[0]);
      } else {
        // COMBINED always goes through the codec. Here decoded_sizes carries
        // per stream lengths, so unlike SEPARATE it cannot double as an
        // uncompressed marker (RFC section 5.2).
        if (codec_ == nullptr) {
          report(
              result,
              Check::kStructural,
              bufferOffset,
              "COMBINED run needs a codec but none was supplied");
          return result;
        }
        if (options_.maxDecodedBytes != 0 &&
            decodedTotal > options_.maxDecodedBytes) {
          report(
              result,
              Check::kCombinedDecodedSize,
              bufferOffset,
              "combined decoded size exceeds the limit");
          return result;
        }
        if (!codec_->decompress(
                stored,
                static_cast<size_t>(storedSizes[0]),
                static_cast<size_t>(decodedTotal),
                combined)) {
          report(
              result,
              Check::kStructural,
              bufferOffset,
              "codec failed to decompress the combined buffer");
          return result;
        }
        if (combined.size() != decodedTotal) {
          report(
              result,
              Check::kCombinedDecodedSize,
              bufferOffset,
              "COMBINED buffer decoded to " + std::to_string(combined.size()) +
                  " bytes, expected " + std::to_string(decodedTotal));
          return result;
        }
      }
      size_t sliceOffset = 0;
      for (size_t i = 0; i < streamTotal; ++i) {
        const auto begin = static_cast<ptrdiff_t>(sliceOffset);
        const auto end =
            static_cast<ptrdiff_t>(sliceOffset + decodedSizes[i]);
        streams[i].insert(
            streams[i].end(), combined.begin() + begin, combined.begin() + end);
        runBoundaries[i].push_back(streams[i].size());
        sliceOffset += static_cast<size_t>(decodedSizes[i]);
      }
    }
  }

  if (options_.enableLevelTwo && runCount == 0 && rowCount > 0) {
    bool everyColumnAllNull = true;
    for (const auto tag : tags) {
      if (tag != NullTag::kAllNull) {
        everyColumnAllNull = false;
        break;
      }
    }
    if (!everyColumnAllNull) {
      report(
          result,
          Check::kMissingRuns,
          0,
          "run_count is 0 with row_count " + std::to_string(rowCount) +
              ", which section 9 allows only when every column is ALL_NULL");
    }
  }

  result.consumedBytes = reader.offset();
  result.streams = streams;

  if (options_.payloadSizeProvided) {
    if (limit != reader.offset()) {
      report(
          result,
          Check::kPayloadSizeBounds,
          reader.offset(),
          "consumed " + std::to_string(reader.offset()) +
              " bytes but payload_size is " +
              std::to_string(options_.payloadSize));
    }
  } else if (reader.remaining() != 0) {
    report(
        result,
        Check::kStructural,
        reader.offset(),
        std::to_string(reader.remaining()) + " trailing bytes after the runs");
  }

  size_t streamIndex = 0;
  for (size_t column = 0; column < columnCount; ++column) {
    decodeColumn(
        column,
        streamIndex,
        streams,
        runBoundaries,
        nonNullCounts,
        encodings,
        result);
  }

  return result;
}

} // namespace bytedance::bolt::shuffle::sparksql::test
