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

#include "bolt/shuffle/sparksql/tests/ColumnarPayloadConformance.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"

#include <limits>
#include <set>
#include <string>
#include <tuple>

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

void appendLe(std::vector<uint8_t>& out, uint64_t value, size_t bytes) {
  for (size_t i = 0; i < bytes; ++i) {
    out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
  }
}

/// Appendix B of bolt/shuffle/sparksql/ColumnarPayloadFormat.md, byte for byte. The document
/// declares that vector part of the specification, so it is spelled out here
/// rather than produced by the generator: it pins the wire format itself, and
/// a change here means the format changed.
std::vector<uint8_t> appendixBPayload() {
  std::vector<uint8_t> payload;
  appendLe(payload, 3, 4); // row_count
  appendLe(payload, 1, 4); // run_count
  appendLe(payload, 6, 8); // variable_size
  appendLe(payload, 2, 4); // null_stored_size
  appendLe(payload, 0, 4); // null_decoded_size
  payload.push_back(0x06); // tags: col0 RAW_NULL, col1 NO_NULL
  payload.push_back(0x05); // col0 bitmap: valid, null, valid
  payload.push_back(0x00); // encoding_tags
  payload.push_back(static_cast<uint8_t>(CompressionLayout::kSeparate));
  appendLe(payload, 9, 8);
  appendLe(payload, 2, 8);
  appendLe(payload, 6, 8);
  appendLe(payload, 0, 8);
  appendLe(payload, 0, 8);
  appendLe(payload, 0, 8);
  payload.push_back(0x03); // stream 0: PLAIN
  appendLe(payload, 10, 4);
  appendLe(payload, 12, 4);
  payload.push_back(0x04); // stream 1: CONST_NARROW, narrow_bytes = 1
  payload.push_back(0x02);
  for (const char c : std::string("abcdef")) { // stream 2
    payload.push_back(static_cast<uint8_t>(c));
  }
  return payload;
}

FlatTable appendixBTable() {
  FlatTable table;
  table.rowCount = 3;

  FlatColumn integers;
  integers.type = PhysicalType::kInteger;
  integers.isNull = {false, true, false};
  integers.intValues = {10, 12};

  FlatColumn strings;
  strings.type = PhysicalType::kString;
  strings.isNull = {false, false, false};
  strings.stringValues = {"ab", "cd", "ef"};

  table.columns = {integers, strings};
  return table;
}

/// Offset of the first stored buffer of a Run laid out as SEPARATE.
size_t firstBufferOffset(size_t runOffset, size_t streamTotal) {
  return runOffset + 1 + 2 * sizeof(uint64_t) * streamTotal;
}

ValidationResult validate(
    const std::vector<uint8_t>& payload,
    const std::vector<PhysicalType>& schema,
    Codec* codec) {
  ValidationOptions options;
  options.payloadSizeProvided = true;
  options.payloadSize = payload.size();
  ColumnarPayloadValidator validator(codec, schema, options);
  return validator.validate(payload);
}

TEST(ColumnarPayloadVectorTest, appendixBHasTheDocumentedSize) {
  EXPECT_EQ(appendixBPayload().size(), 93u);
}

TEST(ColumnarPayloadVectorTest, appendixBDecodes) {
  IdentityCodec codec;
  const auto payload = appendixBPayload();
  const auto result = validate(
      payload, {PhysicalType::kInteger, PhysicalType::kString}, &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_EQ(result.consumedBytes, payload.size());
  EXPECT_TRUE(result.decoded == appendixBTable());
}

class ColumnarPayloadEncodingTest
    : public testing::TestWithParam<EncodingPolicy> {};

TEST_P(ColumnarPayloadEncodingTest, everyEncodingKindRoundTrips) {
  IdentityCodec codec;

  FlatTable table;
  table.rowCount = 100;
  for (const auto type :
       {PhysicalType::kSmallInt,
        PhysicalType::kInteger,
        PhysicalType::kBigint,
        PhysicalType::kDate}) {
    FlatColumn column;
    column.type = type;
    column.isNull.assign(table.rowCount, false);
    for (uint32_t row = 0; row < table.rowCount; ++row) {
      // A constant stretch, then a tight range, then a wide one, so that all
      // four encodings are reachable inside one column. The wide stretch is
      // clamped to the type: a value outside its range is not something the
      // format can carry, and the generator rightly refuses it.
      int64_t value = row < 32 ? 7 : (row < 64 ? 1000 + row : row * 811);
      value = std::max(typeMin(type), std::min(typeMax(type), value));
      column.intValues.push_back(value);
    }
    table.columns.push_back(std::move(column));
  }

  GeneratorOptions options;
  options.encodingPolicy = GetParam();

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

INSTANTIATE_TEST_SUITE_P(
    Policies,
    ColumnarPayloadEncodingTest,
    testing::Values(
        EncodingPolicy::kAuto,
        EncodingPolicy::kForcePlain,
        EncodingPolicy::kForceConstNarrow,
        EncodingPolicy::kForceBitPack,
        EncodingPolicy::kForceForBitPack,
        EncodingPolicy::kRotate));

TEST(ColumnarPayloadVariationTest, nonMinimalEncodingWidthsAreAccepted) {
  // The RFC obliges a Reader to accept any legal encoding, not only the
  // smallest one a Writer could have chosen.
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 40;
  FlatColumn column;
  column.type = PhysicalType::kBigint;
  column.isNull.assign(table.rowCount, false);
  column.intValues.assign(table.rowCount, 7);
  table.columns = {column};

  GeneratorOptions minimal;
  GeneratedPayload minimalPayload;
  std::string error;
  ColumnarPayloadGenerator minimalGenerator(&codec, minimal);
  ASSERT_TRUE(minimalGenerator.generate(table, minimalPayload, error))
      << error;

  GeneratorOptions padded;
  padded.minimalEncodingWidth = false;
  GeneratedPayload paddedPayload;
  ColumnarPayloadGenerator paddedGenerator(&codec, padded);
  ASSERT_TRUE(paddedGenerator.generate(table, paddedPayload, error)) << error;

  // A constant column narrows to one byte when minimal and to the full type
  // width otherwise, so the padded payload must be strictly larger.
  EXPECT_GT(paddedPayload.bytes.size(), minimalPayload.bytes.size());

  const auto result = validate(paddedPayload.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadVariationTest, stringEncodingVariesPerColumn) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 12;
  for (int index = 0; index < 2; ++index) {
    FlatColumn column;
    column.type = PhysicalType::kString;
    column.isNull.assign(table.rowCount, false);
    for (uint32_t row = 0; row < table.rowCount; ++row) {
      column.stringValues.push_back("v" + std::to_string(row % 3));
    }
    table.columns.push_back(std::move(column));
  }

  GeneratorOptions options;
  options.stringEncodings = {
      StringEncoding::kRaw, StringEncoding::kDictionary};

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  // EncodingTag is a per column property: column 0 RAW, column 1 Dictionary.
  const uint8_t tags = generated.bytes[generated.layout.encodingTagsOffset];
  EXPECT_EQ(tags & 0x01, 0);
  EXPECT_EQ(tags & 0x02, 0x02);

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadVariationTest, layoutVariesAcrossRuns) {
  MaskCodec codec;
  FlatTable table;
  table.rowCount = 400;
  FlatColumn column;
  column.type = PhysicalType::kInteger;
  column.isNull.assign(table.rowCount, false);
  for (uint32_t row = 0; row < table.rowCount; ++row) {
    column.intValues.push_back(static_cast<int64_t>(row) * 13);
  }
  table.columns = {column};

  GeneratorOptions options;
  options.compress = true;
  options.rotateLayoutPerRun = true;
  options.runCount = 6;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  ASSERT_GT(generated.layout.runOffsets.size(), 1u);

  std::set<uint8_t> layouts;
  for (const size_t offset : generated.layout.runOffsets) {
    layouts.insert(generated.bytes[offset]);
  }
  // A Reader must not latch the layout of the first Run.
  EXPECT_GT(layouts.size(), 1u);

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadVariationTest, compressionVariesPerStreamWithinARun) {
  MaskCodec codec;
  FlatTable table;
  table.rowCount = 64;
  FlatColumn integers;
  integers.type = PhysicalType::kBigint;
  integers.isNull.assign(table.rowCount, false);
  FlatColumn strings;
  strings.type = PhysicalType::kString;
  strings.isNull.assign(table.rowCount, false);
  for (uint32_t row = 0; row < table.rowCount; ++row) {
    integers.intValues.push_back(static_cast<int64_t>(row));
    strings.stringValues.push_back("s" + std::to_string(row % 9));
  }
  table.columns = {integers, strings};

  GeneratorOptions options;
  options.layout = CompressionLayout::kSeparate;
  options.compress = true;
  options.compressPerStream = true;
  options.variationSeed = 7;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

/// Every option combination the corpus is run through. Deliberately smaller
/// than the round trip matrix: the corpus already varies the data, so the
/// options only need to vary the encoding.
struct CorpusVariant {
  CompressionLayout layout;
  bool compress;
  size_t runCount;
  bool useDictionary;
  EncodingPolicy policy;
  bool minimalWidth;
};

std::vector<CorpusVariant> corpusVariants() {
  std::vector<CorpusVariant> variants;
  for (const auto layout :
       {CompressionLayout::kCombined,
        CompressionLayout::kSeparate,
        CompressionLayout::kCombinedStored}) {
    for (const bool compress : {false, true}) {
      for (const size_t runCount : {size_t{1}, size_t{3}}) {
        for (const bool useDictionary : {false, true}) {
          for (const auto policy :
               {EncodingPolicy::kAuto,
                EncodingPolicy::kForcePlain,
                EncodingPolicy::kForceConstNarrow,
                EncodingPolicy::kForceBitPack,
                EncodingPolicy::kForceForBitPack,
                EncodingPolicy::kRotate}) {
            variants.push_back(
                {layout,
                 compress,
                 runCount,
                 useDictionary,
                 policy,
                 policy != EncodingPolicy::kForcePlain});
          }
        }
      }
    }
  }
  return variants;
}

/// VectorTestBase builds a memory pool in its constructor, which needs the
/// process wide manager to exist first.
class ColumnarPayloadVectorTestBase : public testing::Test,
                                      public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

class ColumnarPayloadConformanceTest : public ColumnarPayloadVectorTestBase {};

TEST_F(ColumnarPayloadConformanceTest, referenceWriterAgainstReferenceReader) {
  // The first of the four instantiations described in
  // ColumnarPayloadConformance.h. A real Writer or Reader is dropped in by
  // replacing one argument; nothing in the suite changes.
  MaskCodec codec;
  auto writer = makeReferenceWriter(codec, GeneratorOptions{});
  auto reader = makeReferenceReader(codec, pool());

  const auto report = runConformanceSuite(*writer, *reader, pool());
  EXPECT_TRUE(report.ok()) << report.describe();
  EXPECT_GT(report.casesRun, 0u) << report.describe();
  EXPECT_EQ(report.casesSkipped, 0u) << report.describe();
}

TEST_F(ColumnarPayloadConformanceTest, everyGeneratorOptionSetStaysConforming) {
  // The same suite across the encoding options, so that a Writer plugged in
  // later is compared against a reference that has itself been exercised on
  // every path rather than only the default one.
  MaskCodec codec;
  for (const auto& variant : corpusVariants()) {
    GeneratorOptions options;
    options.layout = variant.layout;
    options.compress = variant.compress;
    options.runCount = variant.runCount;
    options.useDictionary = variant.useDictionary;
    options.encodingPolicy = variant.policy;
    options.minimalEncodingWidth = variant.minimalWidth;

    auto writer = makeReferenceWriter(codec, options);
    auto reader = makeReferenceReader(codec, pool());
    ConformanceOptions suiteOptions;
    // Rejection behaviour does not vary with the encoding options, so check
    // it once in the test above rather than on every variant.
    suiteOptions.checkRejection = false;

    const auto report =
        runConformanceSuite(*writer, *reader, pool(), suiteOptions);
    EXPECT_TRUE(report.ok()) << report.describe();
  }
}

class ColumnarPayloadCorpusTest : public ColumnarPayloadVectorTestBase {};

TEST_F(ColumnarPayloadCorpusTest, boundaryVectorsRoundTrip) {
  // Going through RowVector is what turns this from a conformance check into
  // a data check: the payload has to parse *and* rebuild the exact vector the
  // encoder was handed.
  IdentityCodec identity;
  MaskCodec mask;

  size_t variantIndex = 0;
  for (const auto& entry : boundaryVectorCorpus(pool())) {
    const auto rowType = asRowType(entry.vector->type());
    for (const auto& variant : corpusVariants()) {
      GeneratorOptions options;
      options.layout = variant.layout;
      options.compress = variant.compress;
      options.compressNullBody = variant.compress && (variant.runCount == 3);
      options.runCount = variant.runCount;
      options.useDictionary = variant.useDictionary;
      options.encodingPolicy = variant.policy;
      options.minimalEncodingWidth = variant.minimalWidth;
      options.degenerateNullTags = (variantIndex % 2) == 0;
      options.variationSeed = static_cast<uint32_t>(variantIndex);
      Codec* codec = (variantIndex % 2) != 0 ? static_cast<Codec*>(&identity)
                                             : static_cast<Codec*>(&mask);
      ++variantIndex;

      GeneratedPayload generated;
      std::string error;
      ColumnarPayloadGenerator generator(codec, options);
      ASSERT_TRUE(generatePayload(generator, entry.vector, generated, error))
          << entry.name << ": " << error;

      ValidationOptions validationOptions;
      validationOptions.payloadSizeProvided = true;
      validationOptions.payloadSize = generated.bytes.size();
      std::vector<PhysicalType> schema;
      ASSERT_TRUE(schemaOf(entry.vector, schema, error)) << error;
      ColumnarPayloadValidator validator(codec, schema, validationOptions);

      const auto decoded =
          validatePayload(validator, generated.bytes, rowType, pool());
      EXPECT_TRUE(decoded.result.ok())
          << entry.name << ": " << decoded.result.describe();
      ASSERT_NE(decoded.decoded, nullptr) << entry.name;
      bytedance::bolt::test::assertEqualVectors(
          entry.vector, decoded.decoded);
    }
  }
}

TEST(ColumnarPayloadCoverageTest, theCorpusReachesEveryEncodingPath) {
  // Coverage is asserted from the constructed corpus alone, with no RNG in
  // the loop. A coverage claim that depends on random draws is itself
  // fragile: adding one call to the generator's RNG shifts every subsequent
  // value and silently changes what the suite tests.
  IdentityCodec identity;
  MaskCodec mask;
  GenerationStats total;

  size_t variantIndex = 0;
  for (const auto& entry : boundaryCorpus()) {
    for (const auto& variant : corpusVariants()) {
      GeneratorOptions options;
      options.layout = variant.layout;
      options.compress = variant.compress;
      options.runCount = variant.runCount;
      options.useDictionary = variant.useDictionary;
      options.encodingPolicy = variant.policy;
      options.minimalEncodingWidth = variant.minimalWidth;
      options.degenerateNullTags = (variantIndex % 2) == 0;
      Codec* codec = (variantIndex % 2) != 0 ? static_cast<Codec*>(&identity)
                                             : static_cast<Codec*>(&mask);
      ++variantIndex;

      GeneratedPayload generated;
      std::string error;
      ColumnarPayloadGenerator generator(codec, options);
      ASSERT_TRUE(generator.generate(entry.table, generated, error)) << error;
      total.merge(generated.stats);
    }
  }

  const char* gap = total.firstGap();
  EXPECT_EQ(gap, nullptr) << "corpus never generated: " << (gap ? gap : "");
}

TEST(ColumnarPayloadEdgeTest, emptyTable) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 0;
  FlatColumn column;
  column.type = PhysicalType::kBigint;
  table.columns = {column};

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadEdgeTest, allNullColumnEmitsNoStreamBytes) {
  IdentityCodec codec;
  auto table = oneColumn(nullColumn(PhysicalType::kString, 5));

  GeneratorOptions options;
  options.useDictionary = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  for (const auto& stream : generated.streams) {
    EXPECT_TRUE(stream.empty());
  }

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadEdgeTest, emptyStringsKeepTheirLengths) {
  IdentityCodec codec;
  auto table = oneColumn(stringColumn({"", "", "", ""}));

  for (const bool useDictionary : {false, true}) {
    GeneratorOptions options;
    options.useDictionary = useDictionary;

    GeneratedPayload generated;
    std::string error;
    ColumnarPayloadGenerator generator(&codec, options);
    ASSERT_TRUE(generator.generate(table, generated, error)) << error;

    const auto result = validate(generated.bytes, table.schema(), &codec);
    EXPECT_TRUE(result.ok()) << result.describe();
    EXPECT_TRUE(result.decoded == table);
  }
}

TEST(ColumnarPayloadEdgeTest, oversizedStringForcesRawFallback) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 3;
  FlatColumn column;
  column.type = PhysicalType::kString;
  column.isNull.assign(table.rowCount, false);
  // No dictionary can hold a value this long, so the writer must fall back.
  column.stringValues = {"a", std::string(100, 'x'), "b"};
  table.columns = {column};

  GeneratorOptions options;
  options.useDictionary = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadEdgeTest, blockBoundaryValueCounts) {
  IdentityCodec codec;
  // 16 Integers fill exactly one block; 17 adds a tail block of one value.
  for (const uint32_t rowCount : {15u, 16u, 17u, 32u, 33u}) {
    FlatTable table;
    table.rowCount = rowCount;
    FlatColumn column;
    column.type = PhysicalType::kInteger;
    column.isNull.assign(rowCount, false);
    for (uint32_t row = 0; row < rowCount; ++row) {
      column.intValues.push_back(static_cast<int64_t>(row) * 7919);
    }
    table.columns = {column};

    GeneratedPayload generated;
    std::string error;
    ColumnarPayloadGenerator generator(&codec, {});
    ASSERT_TRUE(generator.generate(table, generated, error)) << error;

    const auto result = validate(generated.bytes, table.schema(), &codec);
    EXPECT_TRUE(result.ok()) << rowCount << ": " << result.describe();
    EXPECT_TRUE(result.decoded == table) << rowCount;
  }
}

TEST(ColumnarPayloadEdgeTest, integralExtremes) {
  IdentityCodec codec;
  auto table = oneColumn(intColumn(
      PhysicalType::kBigint,
      {std::numeric_limits<int64_t>::min(),
       0,
       std::numeric_limits<int64_t>::max()}));

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadEdgeTest, valueOutsideItsTypeRangeIsRejected) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 1;
  FlatColumn column;
  column.type = PhysicalType::kTinyInt;
  column.isNull = {false};
  column.intValues = {1000};
  table.columns = {column};

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  EXPECT_FALSE(generator.generate(table, generated, error));
  EXPECT_FALSE(error.empty());
}

TEST(ColumnarPayloadRuleTest, reservedNullTagIsRejected) {
  IdentityCodec codec;
  auto table = oneColumn(nullableIntColumn(
      PhysicalType::kInteger, {false, true, false, false}, {1, 2, 3}));

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  auto corrupted = generated.bytes;
  const size_t offset = generated.layout.nullBodyOffset;
  corrupted[offset] = static_cast<uint8_t>((corrupted[offset] & ~0x03) | 0x03);

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kReservedNullTag)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, reservedCompressionLayoutIsRejected) {
  IdentityCodec codec;
  auto table = oneColumn(intColumn(PhysicalType::kInteger, {1, 2, 3, 4}));

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  ASSERT_FALSE(generated.layout.runOffsets.empty());

  auto corrupted = generated.bytes;
  corrupted[generated.layout.runOffsets[0]] = 0x03;

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kCompressionLayoutValue)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, nonZeroPlainParamIsRejected) {
  IdentityCodec codec;
  auto table = oneColumn(intColumn(PhysicalType::kInteger, {1, 2, 3, 4}));

  GeneratorOptions options;
  options.encodingPolicy = EncodingPolicy::kForcePlain;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  ASSERT_FALSE(generated.layout.runOffsets.empty());

  auto corrupted = generated.bytes;
  const size_t offset =
      firstBufferOffset(generated.layout.runOffsets[0], /*streamTotal=*/1);
  ASSERT_LT(offset, corrupted.size());
  ASSERT_EQ(
      corrupted[offset] & 0x03, static_cast<uint8_t>(EncodingKind::kPlain));
  corrupted[offset] = static_cast<uint8_t>(corrupted[offset] | (1u << 2));

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kPlainParam)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, outOfRangeDictionaryIndexIsRejected) {
  IdentityCodec codec;
  std::vector<std::string> values;
  for (uint32_t row = 0; row < 8; ++row) {
    values.push_back(row % 2 == 0 ? "aa" : "bb");
  }
  auto table = oneColumn(stringColumn(std::move(values)));

  GeneratorOptions options;
  options.useDictionary = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  ASSERT_FALSE(generated.layout.runOffsets.empty());

  auto corrupted = generated.bytes;
  // The first byte of the length/index stream is a dictionary index.
  const size_t offset =
      firstBufferOffset(generated.layout.runOffsets[0], /*streamTotal=*/2);
  ASSERT_LT(offset, corrupted.size());
  corrupted[offset] = 0x7F;

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kDictionaryIndexRange)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, inexactVariableSizeIsNotAViolation) {
  // RFC section 3.1: variable_size is a hint, never a validation input.
  IdentityCodec codec;
  auto table = oneColumn(stringColumn({"a", "bb", "ccc"}));

  GeneratorOptions options;
  options.variableSizeOverride = 4096;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

// The five cases below were found by an independent review of this file's
// first version. Each one passed the 36-way round trip matrix beforehand,
// because the generator and the validator shared the same blind spot.

TEST(ColumnarPayloadRuleTest, allNullRawTagStillEmitsARun) {
  // Section 9 permits run_count == 0 with rows present only when every column
  // is ALL_NULL. With degenerate tags off an all-null column is RAW_NULL, so
  // the payload must still carry one empty Run.
  IdentityCodec codec;
  auto table = oneColumn(nullColumn(PhysicalType::kInteger, 6));

  GeneratorOptions options;
  options.degenerateNullTags = false;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;
  EXPECT_EQ(generated.layout.runOffsets.size(), 1u);

  const auto result = validate(generated.bytes, table.schema(), &codec);
  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadRuleTest, missingRunsWithNonAllNullTagsIsRejected) {
  // The same payload with its run_count forced to 0 must now be caught.
  IdentityCodec codec;
  auto table = oneColumn(nullColumn(PhysicalType::kInteger, 6));

  GeneratorOptions options;
  options.degenerateNullTags = false;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  auto corrupted = generated.bytes;
  // run_count is the second u32 of the fixed header.
  corrupted[4] = 0;
  corrupted[5] = 0;
  corrupted[6] = 0;
  corrupted[7] = 0;
  corrupted.resize(generated.layout.runsOffset);

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kMissingRuns)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, forBitPackResultOutsideTheTypeIsRejected) {
  // A hand built SmallInt block whose base + delta leaves the type range.
  IdentityCodec codec;
  std::vector<uint8_t> stream;
  stream.push_back(static_cast<uint8_t>(
      static_cast<uint8_t>(EncodingKind::kForBitPack) | (1u << 2)));
  appendLe(stream, 32767, 2); // base = SmallInt max
  stream.push_back(0x02); // deltas: 0, 1  (LSB first, one bit each)

  auto table = oneColumn(intColumn(PhysicalType::kSmallInt, {32767, 32767}));

  GeneratorOptions options;
  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  // Splice the crafted block in place of the generated one.
  auto corrupted = std::vector<uint8_t>(
      generated.bytes.begin(),
      generated.bytes.begin() +
          static_cast<ptrdiff_t>(firstBufferOffset(
              generated.layout.runOffsets[0], /*streamTotal=*/1)));
  const size_t sizeOffset = generated.layout.runOffsets[0] + 1;
  corrupted[sizeOffset] = static_cast<uint8_t>(stream.size());
  corrupted.insert(corrupted.end(), stream.begin(), stream.end());

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kForBitPackRange)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, payloadSizeBoundsEveryRead) {
  // A buffer holding this payload plus a following frame must not be parsed
  // past the declared payload_size.
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 8;
  FlatColumn column;
  column.type = PhysicalType::kInteger;
  column.isNull.assign(8, false);
  for (uint32_t row = 0; row < 8; ++row) {
    column.intValues.push_back(static_cast<int64_t>(row));
  }
  table.columns = {column};

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, {});
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  auto framed = generated.bytes;
  const size_t declared = framed.size();
  framed.insert(framed.end(), 64, 0xAB); // the next frame

  ValidationOptions options;
  options.payloadSizeProvided = true;
  options.payloadSize = declared;
  ColumnarPayloadValidator validator(&codec, table.schema(), options);
  const auto result = validator.validate(framed);

  EXPECT_TRUE(result.ok()) << result.describe();
  EXPECT_EQ(result.consumedBytes, declared);
  EXPECT_TRUE(result.decoded == table);
}

TEST(ColumnarPayloadRuleTest, nullBodySizeMismatchReportsItsOwnRule) {
  // The Null envelope must report L1.5, not the Run buffer's L1.11.
  MaskCodec codec;
  auto table = oneColumn(nullableIntColumn(
      PhysicalType::kInteger,
      {false, true, false, true, false, true, false, true},
      {0, 1, 2, 3}));

  GeneratorOptions options;
  options.compressNullBody = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  auto corrupted = generated.bytes;
  // null_decoded_size is the fifth u32 of the fixed header, at offset 20.
  corrupted[20] = static_cast<uint8_t>(corrupted[20] + 1);

  const auto result = validate(corrupted, table.schema(), &codec);
  EXPECT_TRUE(result.has(Check::kNullDecodedSize)) << result.describe();
  EXPECT_FALSE(result.has(Check::kBufferDecodedSize)) << result.describe();
}

TEST(ColumnarPayloadRuleTest, truncationNeverValidates) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 40;
  FlatColumn integers;
  integers.type = PhysicalType::kInteger;
  integers.isNull.assign(40, false);
  FlatColumn strings;
  strings.type = PhysicalType::kString;
  strings.isNull.assign(40, false);
  for (uint32_t row = 0; row < 40; ++row) {
    integers.intValues.push_back(static_cast<int64_t>(row));
    strings.stringValues.push_back("v" + std::to_string(row % 7));
  }
  table.columns = {integers, strings};

  GeneratorOptions options;
  options.useDictionary = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  for (size_t size = 0; size < generated.bytes.size(); ++size) {
    const std::vector<uint8_t> truncated(
        generated.bytes.begin(),
        generated.bytes.begin() + static_cast<ptrdiff_t>(size));
    ValidationOptions validationOptions;
    validationOptions.payloadSizeProvided = true;
    validationOptions.payloadSize = truncated.size();
    ColumnarPayloadValidator validator(&codec, table.schema(), validationOptions);
    // A short payload must be rejected rather than read past its end; the
    // sanitizer builds enforce the second half of that.
    EXPECT_FALSE(validator.validate(truncated).ok()) << "size " << size;
  }
}

TEST(ColumnarPayloadRuleTest, singleByteMutationsNeverReadOutOfBounds) {
  IdentityCodec codec;
  FlatTable table;
  table.rowCount = 24;
  FlatColumn integers;
  integers.type = PhysicalType::kBigint;
  integers.isNull.assign(24, false);
  FlatColumn strings;
  strings.type = PhysicalType::kString;
  strings.isNull.assign(24, false);
  for (uint32_t row = 0; row < 24; ++row) {
    integers.intValues.push_back(static_cast<int64_t>(row) * 3);
    strings.stringValues.push_back("s" + std::to_string(row % 5));
  }
  table.columns = {integers, strings};

  GeneratorOptions options;
  options.useDictionary = true;

  GeneratedPayload generated;
  std::string error;
  ColumnarPayloadGenerator generator(&codec, options);
  ASSERT_TRUE(generator.generate(table, generated, error)) << error;

  size_t rejected = 0;
  for (size_t index = 0; index < generated.bytes.size(); ++index) {
    for (const uint8_t delta : {uint8_t{1}, uint8_t{0x80}, uint8_t{0xFF}}) {
      auto corrupted = generated.bytes;
      corrupted[index] = static_cast<uint8_t>(corrupted[index] + delta);
      ColumnarPayloadValidator validator(&codec, table.schema(), {});
      // Not every mutation is detectable: flipping a value byte or the
      // variable_size hint yields a different but still conforming payload.
      // What must hold is that validation terminates without reading out of
      // bounds.
      if (!validator.validate(corrupted).ok()) {
        ++rejected;
      }
    }
  }
  EXPECT_GT(rejected, 0u);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test
