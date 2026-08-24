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

/// This file is the form the engine's Writer and Reader are plugged into. It
/// is already filled in apart from three function bodies; everything else,
/// including the tests, is written and compiling.
///
/// To land a Writer or a Reader:
///
///   1. Fill in EngineCodec below, or delete it if the implementation does
///      not compress.
///   2. Fill in the write() or read() body you own.
///   3. Delete DISABLED_ from the test names that now apply.
///
/// A Reader can be built in two stages, and the tests are split to match.
/// Stage one asks only for the simplest payload the format allows: every
/// block PLAIN, no dictionary, nothing compressed, one Run. Stage two asks
/// for everything. Passing stage one is worth having on its own, so it is a
/// separate test rather than a comment about which failures to ignore.
///
/// The Writer direction is not staged, and the comment on its test explains
/// why: the reference validator accepts any conforming payload, so a Writer
/// that only ever emits PLAIN passes exactly the same test as one that uses
/// every encoding. Nothing needs relaxing for it. Types that are not done
/// yet belong in supports().
///
/// Order matters. Write the two mixed tests before the engine-to-engine one:
/// a Writer tested only against its own Reader, and a Reader tested only
/// against its own Writer, agree on any misreading of the format they happen
/// to share, and the engine-to-engine test alone cannot tell that apart from
/// both being right. Breaking that symmetry is why the reference
/// implementation exists.
///
/// Format: bolt/shuffle/sparksql/ColumnarPayloadFormat.md
/// Seam:   bolt/shuffle/sparksql/tests/columnar_payload/Conformance.h

#include "bolt/shuffle/sparksql/tests/columnar_payload/Conformance.h"

#include <gtest/gtest.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

/// Bridges the engine's compression stack to the reference Codec interface.
///
/// The payload records nothing about how it was compressed (format spec
/// section 2), so a Reader paired with a Writer that compressed differently
/// decodes garbage with nothing on the wire to catch it. The suite compares
/// name() on both sides to catch that misconfiguration directly; keep the
/// name faithful to the codec actually used.
///
/// Until an implementation compresses, MaskCodec stands in: it is reversible
/// but not the identity, so a Writer that forgets to compress or a Reader
/// that forgets to decompress produces visibly wrong bytes rather than
/// accidentally correct ones.
using EngineCodec = MaskCodec;

/// The engine's Writer, as the suite sees it.
class EngineWriter : public PayloadWriter {
 public:
  explicit EngineWriter(Codec& codec) : codec_(codec) {}

  const char* name() const override {
    return "engine";
  }

  Codec& codec() override {
    return codec_;
  }

  /// Narrow this while types are still being implemented. A shape declined
  /// here is counted as skipped rather than failed, which is what makes the
  /// suite usable from the first day rather than the last.
  bool supports(const RowTypePtr& rowType) const override {
    (void)rowType;
    return true;
  }

  bool write(
      const RowVectorPtr& input,
      std::vector<uint8_t>& out,
      std::string& error) override {
    // TODO(writer): drive the engine's Writer over `input` and hand back the
    // single payload it produced.
    //
    // Partitioning stays on this side of the line. The format says nothing
    // about how rows are distributed, so the suite deliberately knows nothing
    // about it either; configure a single partition here, or take one
    // partition's payload back out.
    (void)input;
    (void)out;
    error = "the engine Writer is not implemented yet";
    return false;
  }

 private:
  Codec& codec_;
};

/// The engine's Reader, as the suite sees it.
class EngineReader : public PayloadReader {
 public:
  explicit EngineReader(Codec& codec) : codec_(codec) {}

  const char* name() const override {
    return "engine";
  }

  Codec& codec() override {
    return codec_;
  }

  bool supports(const RowTypePtr& rowType) const override {
    (void)rowType;
    return true;
  }

  /// A production Reader implements the L1 rules of format spec section 10;
  /// the L2 ones are optional. Return false while the L1 rules are still
  /// going in, and the suite stops requiring rejections not yet signed up
  /// for.
  bool rejectsMalformed() const override {
    return true;
  }

  bool read(
      const std::vector<uint8_t>& payload,
      const RowTypePtr& rowType,
      RowVectorPtr& out,
      std::string& error) override {
    // TODO(reader): decode `payload` into `out`. `rowType` is supplied
    // because the format carries no schema of its own (spec section 2).
    (void)payload;
    (void)rowType;
    (void)out;
    error = "the engine Reader is not implemented yet";
    return false;
  }

 private:
  Codec& codec_;
};

/// The narrowest payload the format allows. Every knob that adds a decode
/// path is off: blocks are PLAIN so no narrowing, bit packing or frame of
/// reference appears; strings are RAW so there is no dictionary sequence and
/// no index segment; nothing is compressed so the codec is never called; and
/// a single SEPARATE Run means one buffer per stream with no concatenation.
///
/// What is left is still not trivial, and cannot be: null bitmaps, the tail
/// block at the end of a stream, and the string length stream are part of
/// every payload the format can produce.
GeneratorOptions plainOptions() {
  GeneratorOptions options;
  options.encodingPolicy = EncodingPolicy::kForcePlain;
  options.useDictionary = false;
  options.compress = false;
  options.compressNullBody = false;
  options.layout = CompressionLayout::kSeparate;
  options.runCount = 1;
  options.minimalEncodingWidth = true;
  options.degenerateNullTags = true;
  return options;
}

/// Configurations covering the paths plainOptions() leaves out. Together with
/// it they reach every encoding the format defines, which the coverage test
/// in PayloadTest.cpp asserts rather than assumes.
std::vector<GeneratorOptions> everyEncodingOptions() {
  std::vector<GeneratorOptions> all;
  for (const auto layout :
       {CompressionLayout::kCombined,
        CompressionLayout::kSeparate,
        CompressionLayout::kCombinedStored}) {
    for (const bool useDictionary : {false, true}) {
      for (const auto policy :
           {EncodingPolicy::kAuto,
            EncodingPolicy::kRotate,
            EncodingPolicy::kForceConstNarrow}) {
        GeneratorOptions options;
        options.layout = layout;
        options.compress = layout == CompressionLayout::kCombined;
        options.runCount = 3;
        options.useDictionary = useDictionary;
        options.encodingPolicy = policy;
        // Legal but not minimal, so a Reader cannot assume the Writer always
        // picks the smallest body.
        options.minimalEncodingWidth = policy != EncodingPolicy::kRotate;
        all.push_back(options);
      }
    }
  }
  return all;
}

class ColumnarPayloadIntegrationTest : public testing::Test,
                                       public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

/// Writer. Enable once write() is implemented; there is no staged version of
/// this one. The reference validator accepts any conforming payload, so a
/// Writer that only emits PLAIN blocks and RAW strings passes exactly the
/// test a Writer using every encoding does. Types not implemented yet belong
/// in supports(), which counts them as skipped rather than failed.
TEST_F(
    ColumnarPayloadIntegrationTest,
    DISABLED_engineWriterAgainstReferenceReader) {
  EngineCodec codec;
  EngineWriter writer{codec};
  auto reader = makeReferenceReader(codec, pool());

  const auto report = runConformanceSuite(writer, *reader, pool());
  EXPECT_TRUE(report.ok()) << report.describe();
  EXPECT_GT(report.casesRun, 0u) << report.describe();
}

/// Reader, stage one. Enable as soon as read() handles PLAIN blocks and RAW
/// strings; none of the other decode paths are exercised here.
TEST_F(ColumnarPayloadIntegrationTest, DISABLED_engineReaderOnPlainPayloads) {
  EngineCodec codec;
  EngineReader reader{codec};
  auto writer = makeReferenceWriter(codec, plainOptions());

  const auto report = runConformanceSuite(*writer, reader, pool());
  EXPECT_TRUE(report.ok()) << report.describe();
  EXPECT_GT(report.casesRun, 0u) << report.describe();
}

/// Reader, stage two. Adds constant narrowing, bit packing, frame of
/// reference, dictionaries with their RAW fallback, compression, the combined
/// layouts, several Runs, and legal but non-minimal encodings the engine's own
/// Writer may never emit.
TEST_F(ColumnarPayloadIntegrationTest, DISABLED_engineReaderOnEveryEncoding) {
  EngineCodec codec;
  EngineReader reader{codec};

  for (const auto& options : everyEncodingOptions()) {
    auto writer = makeReferenceWriter(codec, options);
    const auto report = runConformanceSuite(*writer, reader, pool());
    EXPECT_TRUE(report.ok()) << report.describe();
    EXPECT_GT(report.casesRun, 0u) << report.describe();
  }
}

/// Enable last. On its own this one cannot distinguish a correct pair from a
/// pair that agrees on the same mistake, so it is worth little until the two
/// above pass.
TEST_F(ColumnarPayloadIntegrationTest, DISABLED_engineWriterAgainstEngineReader) {
  EngineCodec codec;
  EngineWriter writer{codec};
  EngineReader reader{codec};

  const auto report = runConformanceSuite(writer, reader, pool());
  EXPECT_TRUE(report.ok()) << report.describe();
  EXPECT_GT(report.casesRun, 0u) << report.describe();
}

/// Keeps plainOptions() honest. It is the contract stage one rests on, so
/// the claim that it emits nothing but PLAIN blocks and no dictionaries is
/// checked rather than trusted; a later change to the generator's defaults
/// cannot quietly widen what stage one demands.
TEST_F(ColumnarPayloadIntegrationTest, plainOptionsEmitOnlyPlainBlocks) {
  IdentityCodec codec;
  GenerationStats total;

  for (const auto& entry : boundaryCorpus()) {
    GeneratedPayload generated;
    std::string error;
    ColumnarPayloadGenerator generator(&codec, plainOptions());
    ASSERT_TRUE(generator.generate(entry.table, generated, error))
        << entry.name << ": " << error;
    total.merge(generated.stats);
  }

  const auto kind = [&](EncodingKind value) {
    return total.encodingKindBlocks[static_cast<size_t>(value)];
  };
  EXPECT_EQ(kind(EncodingKind::kConstNarrow), 0u);
  EXPECT_EQ(kind(EncodingKind::kBitPack), 0u);
  EXPECT_EQ(kind(EncodingKind::kForBitPack), 0u);
  EXPECT_GT(kind(EncodingKind::kPlain), 0u);

  EXPECT_EQ(total.dictionaries, 0u);
  EXPECT_EQ(total.compressedBuffers, 0u);
  EXPECT_EQ(
      total.runLayouts[static_cast<size_t>(CompressionLayout::kCombined)], 0u);
  EXPECT_EQ(
      total.runLayouts[static_cast<size_t>(
          CompressionLayout::kCombinedStored)],
      0u);

  // Not everything can be switched off: these are in every payload the format
  // can produce, so stage one has to cover them.
  EXPECT_GT(total.tailBlocks, 0u);
  EXPECT_GT(total.nullTags[static_cast<size_t>(NullTag::kRawNull)], 0u);
}

/// Fails if the adapters above stop compiling, which is the point of keeping
/// them filled in: the form stays valid while nobody is using it.
TEST_F(ColumnarPayloadIntegrationTest, adaptersCompileAndReportThemselves) {
  EngineCodec codec;
  EngineWriter writer{codec};
  EngineReader reader{codec};

  EXPECT_STREQ(writer.name(), "engine");
  EXPECT_STREQ(reader.name(), "engine");
  EXPECT_STREQ(writer.codec().name(), reader.codec().name());

  // Until the bodies are filled in they must fail rather than quietly claim
  // success, so that enabling a test above cannot pass by accident.
  std::vector<uint8_t> payload;
  std::string error;
  const auto rowType = ROW({"c0"}, {BIGINT()});
  EXPECT_FALSE(writer.write(nullptr, payload, error));
  EXPECT_FALSE(error.empty());

  RowVectorPtr decoded;
  EXPECT_FALSE(reader.read(payload, rowType, decoded, error));
  EXPECT_FALSE(error.empty());
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test
