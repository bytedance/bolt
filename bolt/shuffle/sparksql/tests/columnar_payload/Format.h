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

/// Shared definitions for the Shuffle Payload binary format.
///
/// Normative reference: bolt/shuffle/sparksql/ColumnarPayloadFormat.md. Every constant and
/// enumerator here mirrors a value fixed by that document; changing one is a
/// wire format change and must follow the update rules in its section 11.
///
/// This header and its two implementations (ColumnarPayloadGenerator,
/// ColumnarPayloadValidator) deliberately depend on nothing but the standard library.
/// They are the reference oracle used to test the real Writer / Reader, so
/// they must not share code with them.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace bytedance::bolt::shuffle::sparksql::test {

// The format version is deliberately absent here. It exists only in the RFC's
// frontmatter and change history: the payload carries no version, nothing
// compares one at runtime, and consistency comes from Writer and Reader being
// built together (RFC section 11.1). A constant here would have nothing to
// check against and would drift unnoticed.

/// Length of the naturally aligned fixed header (RFC section 3).
inline constexpr size_t kFixedHeaderBytes = 24;

/// Source bytes covered by one full Encoding Block (RFC section 7.2).
inline constexpr size_t kEncodingBlockSourceBytes = 64;

/// Serialized space available to a single dictionary (RFC section 8.1).
inline constexpr size_t kDictionaryMaxSerializedBytes = 64;

/// Upper bound on entries in a single dictionary; every entry costs at least
/// the one byte holding its length.
inline constexpr size_t kMaxDictionaryEntries = 63;

/// Dictionary terminators. Both are >= 0xFE, which no entry length can reach.
inline constexpr uint8_t kDictionaryContinue = 0xFE;
inline constexpr uint8_t kDictionaryFinal = 0xFF;

/// Largest byte length an entry value may carry.
inline constexpr uint8_t kMaxDictionaryEntryLength = 63;

enum class PhysicalType : uint8_t {
  kTinyInt = 0,
  kSmallInt = 1,
  kInteger = 2,
  kBigint = 3,
  kDate = 4,
  kFloat = 5,
  kDouble = 6,
  kString = 7,
};

/// RFC section 4.2.
enum class NullTag : uint8_t {
  kAllNull = 0b00,
  kNoNull = 0b01,
  kRawNull = 0b10,
  kReserved = 0b11,
};

/// RFC section 5.
enum class CompressionLayout : uint8_t {
  kCombined = 0x00,
  kSeparate = 0x01,
  kCombinedStored = 0x02,
};

/// RFC section 7.3.
enum class EncodingKind : uint8_t {
  kConstNarrow = 0,
  kBitPack = 1,
  kForBitPack = 2,
  kPlain = 3,
};

/// RFC section 3.2.
enum class StringEncoding : uint8_t {
  kRaw = 0,
  kDictionary = 1,
};

/// Physical width in bytes. Returns 0 for kString, which is variable length.
size_t typeWidth(PhysicalType type);

/// True when the value stream uses the Encoding Loop rather than Raw Data.
bool usesEncodingLoop(PhysicalType type);

/// True for the signed integral types; false for kFloat, kDouble and kString,
/// for which signedness is not meaningful.
bool isSignedIntegral(PhysicalType type);

/// Streams contributed by one column: 2 for kString, 1 otherwise.
size_t streamCount(PhysicalType type);

/// Inclusive range representable by an integral type. Undefined for
/// non-integral types.
int64_t typeMin(PhysicalType type);
int64_t typeMax(PhysicalType type);

const char* toString(PhysicalType type);
const char* toString(EncodingKind kind);
const char* toString(CompressionLayout layout);

/// Staging form of one column: the flat arrays the encoder and decoder work
/// on. It is not a data model of its own. RowVector is the interface of this
/// library (see ColumnarPayloadVectors.h); values have to be compacted into
/// arrays like these anyway, because the format stores no placeholder for a
/// null row.
///
/// Keeping the reference implementation itself free of any vector dependency
/// is deliberate: it is what lets the encoder and the validator be built and
/// exercised without the engine, which is the property that makes them a
/// usable oracle for the engine's own Writer and Reader.
struct FlatColumn {
  PhysicalType type{PhysicalType::kInteger};

  /// Per row. Size must equal FlatTable::rowCount.
  std::vector<bool> isNull;

  /// Non-null values in row order. Exactly one of these is populated,
  /// selected by type: intValues for the integral types, doubleValues for
  /// kFloat and kDouble, stringValues for kString.
  std::vector<int64_t> intValues;
  std::vector<double> doubleValues;
  std::vector<std::string> stringValues;

  size_t nonNullCount() const;
};

struct FlatTable {
  uint32_t rowCount{0};
  std::vector<FlatColumn> columns;

  std::vector<PhysicalType> schema() const;
};

/// Rounds kFloat columns through float so that a table built from doubles
/// compares equal to the same table after a generate / validate round trip.
FlatTable normalized(const FlatTable& table);

bool operator==(const FlatColumn& lhs, const FlatColumn& rhs);
bool operator==(const FlatTable& lhs, const FlatTable& rhs);

/// The external codec context of RFC section 2. The format never branches on
/// which codec this is; it only requires that decompress inverts compress.
class Codec {
 public:
  virtual ~Codec() = default;

  virtual std::vector<uint8_t> compress(const uint8_t* data, size_t size) = 0;

  /// `decodedSize` is the length the payload declares the output will have.
  /// The format always knows it before calling (spec sections 4.1 and 5.1),
  /// and the engine's codecs need an output buffer of exactly that size, so
  /// passing it keeps an adapter to them trivial.
  ///
  /// It is a sizing hint only. An implementation must not reject a payload
  /// because the declared length disagrees with what it produced: comparing
  /// the two is the validator's job, and it owns the rule number that the
  /// disagreement breaks.
  ///
  /// Returns false when the input cannot be decoded, which a validator has to
  /// report rather than crash on.
  virtual bool decompress(
      const uint8_t* data,
      size_t size,
      size_t decodedSize,
      std::vector<uint8_t>& out) = 0;

  virtual const char* name() const = 0;
};

/// Copies its input. Useful when a test wants stored and decoded bytes to be
/// identical so that failures are readable.
class IdentityCodec : public Codec {
 public:
  std::vector<uint8_t> compress(const uint8_t* data, size_t size) override;

  bool decompress(
      const uint8_t* data,
      size_t size,
      size_t decodedSize,
      std::vector<uint8_t>& out) override;

  const char* name() const override {
    return "identity";
  }
};

/// Reversible but not identity: prefixes the decoded length and masks every
/// byte. A Reader that forgets to invoke the codec, or that invokes it on a
/// buffer marked uncompressed, produces visibly wrong bytes instead of
/// accidentally correct ones.
class MaskCodec : public Codec {
 public:
  static constexpr uint8_t kMask = 0x5A;

  std::vector<uint8_t> compress(const uint8_t* data, size_t size) override;

  bool decompress(
      const uint8_t* data,
      size_t size,
      size_t decodedSize,
      std::vector<uint8_t>& out) override;

  const char* name() const override {
    return "mask";
  }
};

} // namespace bytedance::bolt::shuffle::sparksql::test
