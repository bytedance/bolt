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

/// Reference Reader plus conformance checker for the Shuffle Payload format.
/// It decodes a payload back to logical data and reports every rule of RFC
/// section 10 that the payload breaks, identified by the rule's number in
/// that document.
///
/// Unlike a production Reader this one never trusts its input: it must return
/// violations rather than read out of bounds, crash, or allocate without
/// bound, so that it can be pointed at fuzzed and deliberately corrupted
/// payloads.
///
/// Normative reference: bolt/shuffle/sparksql/ColumnarPayloadFormat.md.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/tests/columnar_payload/Format.h"

namespace bytedance::bolt::shuffle::sparksql::test {

/// Rule identifiers from RFC section 10. Values 1..19 are the L1 rules a
/// Reader must implement; 20..25 are the optional L2 rules. kStructural
/// covers a malformed payload that stops parsing before any numbered rule
/// applies.
/// The rules of format spec section 10, as one list that both the enumerator
/// and its name come from. Values 1..19 are the L1 rules a Reader must
/// implement; 20 and above are the optional L2 ones.
#define BOLT_COLUMNAR_PAYLOAD_CHECKS(X)                            \
  X(kStructural, 0, "structural")                                  \
  X(kFixedHeaderBounds, 1, "L1.1 fixed header bounds")             \
  X(kNullStoredSizeBounds, 2, "L1.2 null_stored_size bounds")      \
  X(kEncodingTagsBounds, 3, "L1.3 encoding_tags bounds")           \
  X(kPayloadSizeBounds, 4, "L1.4 payload_size bounds")             \
  X(kNullDecodedSize, 5, "L1.5 null decoded size")                 \
  X(kNullBodyExpectedSize, 6, "L1.6 null body expected size")      \
  X(kReservedNullTag, 7, "L1.7 reserved NullTag")                  \
  X(kCompressionLayoutValue, 8, "L1.8 compression_layout value")   \
  X(kRunSizeOverflow, 9, "L1.9 run size overflow")                 \
  X(kEmptyStreamSizes, 10, "L1.10 empty stream sizes")             \
  X(kBufferDecodedSize, 11, "L1.11 buffer decoded size")           \
  X(kCombinedDecodedSize, 12, "L1.12 combined decoded size")       \
  X(kPlainParam, 13, "L1.13 PLAIN param")                          \
  X(kConstNarrowParam, 14, "L1.14 CONST_NARROW param")             \
  X(kBitPackParam, 15, "L1.15 BIT_PACK param")                     \
  X(kForBitPackParam, 16, "L1.16 FOR_BIT_PACK param")              \
  X(kBlockBodyBounds, 17, "L1.17 block body bounds")               \
  X(kDictionaryEntryBounds, 18, "L1.18 dictionary entry bounds")   \
  X(kDictionaryIndexRange, 19, "L1.19 dictionary index range")     \
  X(kStreamExhausted, 20, "L2.20 stream exhausted")                \
  X(kRawDataLength, 21, "L2.21 raw data length")                   \
  X(kMatchedRowCount, 22, "L2.22 matched_row_count")               \
  X(kFallbackLengthSum, 23, "L2.23 fallback length sum")           \
  X(kEncodingTagNonString, 24, "L2.24 encoding tag on non-string") \
  X(kUnusedBitsZero, 25, "L2.25 unused bits zero")                 \
  X(kDictionaryCapacity, 26, "L2.26 dictionary capacity")          \
  X(kRunBoundaryStructure, 27, "L2.27 run boundary structure")     \
  X(kMissingRuns, 28, "L2.28 missing runs")                        \
  X(kForBitPackRange, 29, "L2.29 FOR_BIT_PACK result range")

enum class Check : int {
#define BOLT_COLUMNAR_PAYLOAD_CHECK_ENUM(name, value, text) name = value,
  BOLT_COLUMNAR_PAYLOAD_CHECKS(BOLT_COLUMNAR_PAYLOAD_CHECK_ENUM)
#undef BOLT_COLUMNAR_PAYLOAD_CHECK_ENUM
};

const char* toString(Check check);

/// True for the rules a Reader must implement.
bool isLevelOne(Check check);

struct Violation {
  Check check{Check::kStructural};
  std::string message;

  /// Byte offset into the payload the violation was found at, or the size of
  /// the payload when it is not tied to one position.
  size_t offset{0};
};

struct ValidationOptions {
  /// Run the L2 rules as well. A production Reader may skip them; the
  /// validator enables them by default because its job is conformance.
  bool enableLevelTwo{true};

  /// Byte count the outer protocol claims. Enables rule 4.
  bool payloadSizeProvided{false};
  size_t payloadSize{0};

  /// Upper bounds applied to untrusted input, per RFC section 10.3. Zero
  /// disables an individual limit.
  uint32_t maxRowCount{1u << 24};
  uint32_t maxRunCount{1u << 16};
  size_t maxDecodedBytes{size_t{1} << 30};
};

struct ValidationResult {
  std::vector<Violation> violations;

  /// Populated when parsing got far enough to recover values. Compare against
  /// the generator's input for a round trip assertion.
  FlatTable decoded;

  /// Bytes the parse consumed. Equals the payload size for a conforming
  /// payload.
  size_t consumedBytes{0};

  /// Streams after Run concatenation, in RFC section 1.4 order.
  std::vector<std::vector<uint8_t>> streams;

  bool ok() const {
    return violations.empty();
  }

  /// True when no L1 rule was broken, which is the bar a production Reader
  /// has to clear.
  bool okAtLevelOne() const;

  bool has(Check check) const;

  std::string describe() const;
};

class ColumnarPayloadValidator {
 public:
  /// `schema` is the external context of RFC section 2. `codec` may be null
  /// when the payload is known to contain no compressed buffer.
  ColumnarPayloadValidator(
      Codec* codec,
      std::vector<PhysicalType> schema,
      ValidationOptions options = {})
      : codec_(codec),
        schema_(std::move(schema)),
        options_(std::move(options)) {}

  ValidationResult validate(const uint8_t* data, size_t size);

  ValidationResult validate(const std::vector<uint8_t>& payload) {
    return validate(payload.data(), payload.size());
  }

 private:
  class Cursor;
  struct StreamView;

  /// `mismatchCheck` names the rule a declared/actual size disagreement
  /// breaks, which differs between the Null envelope (L1.5) and a Run buffer
  /// (L1.11).
  bool decodeEnvelope(
      const uint8_t* stored,
      uint64_t storedSize,
      uint64_t decodedSize,
      size_t offset,
      Check mismatchCheck,
      std::vector<uint8_t>& out,
      ValidationResult& result);

  void decodeColumn(
      size_t column,
      size_t& streamIndex,
      const std::vector<std::vector<uint8_t>>& streams,
      const std::vector<std::vector<size_t>>& runBoundaries,
      const std::vector<size_t>& nonNullCounts,
      const std::vector<StringEncoding>& encodings,
      ValidationResult& result);

  /// `runBoundaries` holds the offsets at which each Run's contribution to
  /// this stream ended, so that RFC section 5.4 can be enforced: no Encoding
  /// Block may straddle one.
  bool decodeEncodingLoop(
      const std::vector<uint8_t>& stream,
      const std::vector<size_t>& runBoundaries,
      size_t begin,
      PhysicalType type,
      size_t valueCount,
      std::vector<int64_t>& out,
      size_t& consumed,
      ValidationResult& result);

  Codec* codec_;
  std::vector<PhysicalType> schema_;
  ValidationOptions options_;
};

} // namespace bytedance::bolt::shuffle::sparksql::test
