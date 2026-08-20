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

/// The seam a real Writer and a real Reader plug into.
///
/// One suite, four instantiations. As each side lands it is dropped into the
/// same suite without the suite changing:
///
///   runConformanceSuite(referenceWriter, referenceReader)  // today
///   runConformanceSuite(referenceWriter, realReader)       // Reader lands
///   runConformanceSuite(realWriter,      referenceReader)  // Writer lands
///   runConformanceSuite(realWriter,      realReader)       // both
///
/// The two middle rows are the point of the whole exercise. A Writer tested
/// only against its own Reader, and a Reader tested only against its own
/// Writer, will agree on any misreading of the format they happen to share;
/// the reference side is what breaks that symmetry. The suite also runs every
/// payload a Writer produces through the reference validator, so a Writer
/// cannot pass by emitting bytes that only its own Reader accepts.
///
/// Normative reference: bolt/shuffle/sparksql/ColumnarPayloadFormat.md.
///
/// Plugging an implementation in takes three small adapters and one test.
///
/// 1. The codec. The engine's codec writes into a caller supplied buffer,
///    which is why Codec::decompress is handed the length the payload
///    declares.
///
///      class EngineCodec : public Codec {
///       public:
///        explicit EngineCodec(CodecType type)
///            : type_(type), codec_(sparksql::Codec::create(type, {})) {}
///
///        std::vector<uint8_t> compress(const uint8_t* data, size_t size)
///            override {
///          // No exact bound is exposed, so leave room for input that grows.
///          std::vector<uint8_t> out(size + size / 8 + 64);
///          const auto written = codec_->compress(
///              data, static_cast<int64_t>(size), out.data(),
///              static_cast<int64_t>(out.size()));
///          out.resize(static_cast<size_t>(written));
///          return out;
///        }
///
///        bool decompress(const uint8_t* data, size_t size,
///                        size_t decodedSize,
///                        std::vector<uint8_t>& out) override {
///          out.resize(decodedSize);
///          const auto written = codec_->decompress(
///              data, static_cast<int64_t>(size), out.data(),
///              static_cast<int64_t>(decodedSize));
///          return written == static_cast<int64_t>(decodedSize);
///        }
///
///        const char* name() const override {
///          return sparksql::Codec::codecTypeName(type_).data();
///        }
///
///       private:
///        CodecType type_;
///        std::unique_ptr<sparksql::Codec> codec_;
///      };
///
/// 2. The Writer. Partitioning stays on your side of this line: configure a
///    single partition, or take one partition's payload back out.
///
///      class MyWriter : public PayloadWriter {
///       public:
///        MyWriter(Codec& codec, memory::MemoryPool* pool)
///            : codec_(codec), pool_(pool) {}
///
///        const char* name() const override { return "engine"; }
///        Codec& codec() override { return codec_; }
///
///        // Return false while a type is still unimplemented, so the suite
///        // counts the case as skipped instead of failed.
///        bool supports(const RowTypePtr& rowType) const override {
///          for (uint32_t i = 0; i < rowType->size(); ++i) {
///            if (rowType->childAt(i)->kind() == TypeKind::VARCHAR) {
///              return false;
///            }
///          }
///          return true;
///        }
///
///        bool write(const RowVectorPtr& input, std::vector<uint8_t>& out,
///                   std::string& error) override {
///          // ... drive the real writer, collect its bytes into `out` ...
///          return true;
///        }
///
///       private:
///        Codec& codec_;
///        memory::MemoryPool* pool_;
///      };
///
/// 3. The Reader. `rowType` is handed in because the payload carries no
///    schema.
///
///      class MyReader : public PayloadReader {
///       public:
///        const char* name() const override { return "engine"; }
///        Codec& codec() override { return codec_; }
///
///        // A production Reader implements the L1 rules of section 10 and
///        // may skip the L2 ones. Say so, and the suite stops requiring the
///        // rejections you did not sign up for.
///        bool rejectsMalformed() const override { return true; }
///
///        bool read(const std::vector<uint8_t>& payload,
///                  const RowTypePtr& rowType, RowVectorPtr& out,
///                  std::string& error) override {
///          // ... decode into `out` ...
///          return true;
///        }
///      };
///
/// 4. The test. One fixture, one call per pairing you want covered.
///
///      class MyConformanceTest : public testing::Test,
///                                public bolt::test::VectorTestBase {};
///
///      TEST_F(MyConformanceTest, writerAgainstReferenceReader) {
///        EngineCodec codec{CodecType::ZSTD};
///        MyWriter writer{codec, pool()};
///        auto reader = makeReferenceReader(codec, pool());
///        const auto report = runConformanceSuite(writer, *reader, pool());
///        EXPECT_TRUE(report.ok()) << report.describe();
///      }
///
///      TEST_F(MyConformanceTest, referenceWriterAgainstReader) {
///        EngineCodec codec{CodecType::ZSTD};
///        auto writer = makeReferenceWriter(codec, GeneratorOptions{});
///        MyReader reader{codec};
///        const auto report = runConformanceSuite(*writer, reader, pool());
///        EXPECT_TRUE(report.ok()) << report.describe();
///      }
///
///      TEST_F(MyConformanceTest, writerAgainstReader) {
///        EngineCodec codec{CodecType::ZSTD};
///        MyWriter writer{codec, pool()};
///        MyReader reader{codec};
///        const auto report = runConformanceSuite(writer, reader, pool());
///        EXPECT_TRUE(report.ok()) << report.describe();
///      }
///
/// Write the first two before the third. The third alone cannot tell a
/// correct pair from a pair that agrees on the same mistake, which is the
/// failure this whole arrangement exists to catch.
///
/// While the Writer is still incomplete, vary the reference side to widen
/// what the Reader is asked to handle: a reference Writer built with
/// GeneratorOptions{.encodingPolicy = kRotate} or
/// {.minimalEncodingWidth = false} emits legal payloads the engine's Writer
/// may never produce but the Reader still has to accept.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/tests/ColumnarPayloadVectors.h"

namespace bytedance::bolt::shuffle::sparksql::test {

/// A Writer under test: one RowVector in, one payload out.
///
/// Partitioning is out of scope here and stays the Writer implementer's
/// concern. The format says nothing about how rows are distributed, so a
/// format oracle can say nothing useful about it either; the shuffle tests
/// already cover that ground. A partitioning Writer bridges to this interface
/// in its adapter, for instance by running with a single partition and
/// handing back the payload it produced.
class PayloadWriter {
 public:
  virtual ~PayloadWriter() = default;

  virtual const char* name() const = 0;

  /// Encodes one RowVector into one payload. Returns false with `error` set
  /// when encoding failed; use supports() for shapes that are simply not
  /// implemented yet.
  virtual bool write(
      const RowVectorPtr& input,
      std::vector<uint8_t>& out,
      std::string& error) = 0;

  /// Lets a partially built Writer decline shapes it cannot encode yet, so
  /// that the suite is usable from the first day of development rather than
  /// only once every type is done.
  virtual bool supports(const RowTypePtr& rowType) const {
    (void)rowType;
    return true;
  }

  /// The codec this Writer compressed with. A Writer built on the engine's
  /// compression stack needs a thin adapter from that codec to this
  /// interface; that adapter is the second seam.
  ///
  /// The suite needs it to build a reference validator that can read this
  /// Writer's output, and to check that the Reader was given a matching one.
  virtual Codec& codec() = 0;
};

/// A Reader under test.
class PayloadReader {
 public:
  virtual ~PayloadReader() = default;

  virtual const char* name() const = 0;

  /// Decodes one payload. `rowType` is the schema the payload was produced
  /// from, since the format carries none.
  virtual bool read(
      const std::vector<uint8_t>& payload,
      const RowTypePtr& rowType,
      RowVectorPtr& out,
      std::string& error) = 0;

  virtual bool supports(const RowTypePtr& rowType) const {
    (void)rowType;
    return true;
  }

  /// Whether a payload broken in an L1 way is expected to be rejected rather
  /// than silently mis-decoded. A production Reader implements the L1 rules;
  /// the optional L2 rules are not required of it (format spec section 10).
  virtual bool rejectsMalformed() const {
    return true;
  }

  /// The codec this Reader decompresses with.
  ///
  /// The format carries no codec identity (spec section 2), so a Reader
  /// paired with a Writer that compressed differently produces garbage rather
  /// than an error. Nothing on the wire can catch that, so the suite compares
  /// the two sides here and reports the mismatch directly instead of leaving
  /// it to surface as an unreadable payload.
  virtual Codec& codec() = 0;
};

/// Wraps the reference encoder as a Writer.
std::unique_ptr<PayloadWriter> makeReferenceWriter(
    Codec& codec,
    GeneratorOptions options);

/// Wraps the reference validator as a Reader. It rebuilds the RowVector, so a
/// mismatch here is a data failure and not only a conformance one.
std::unique_ptr<PayloadReader> makeReferenceReader(
    Codec& codec,
    memory::MemoryPool* pool,
    ValidationOptions options = {});

struct ConformanceReport {
  size_t casesRun{0};
  size_t casesSkipped{0};
  size_t payloadBytes{0};
  std::vector<std::string> failures;

  bool ok() const {
    return failures.empty();
  }

  std::string describe() const;
};

struct ConformanceOptions {
  /// Also feed the Reader deliberately broken payloads and require it to
  /// reject them. Ignored when the Reader declares it does not validate.
  bool checkRejection{true};

  /// Stop after this many failures so that a broken implementation produces a
  /// readable report rather than one line per corpus entry.
  size_t maxFailures{20};
};

/// Runs the boundary corpus through `writer` and `reader`, checking three
/// things for every case:
///
///   1. the Writer's bytes conform, judged by the reference validator;
///   2. the Reader accepts them;
///   3. the Reader rebuilds exactly the vector the Writer was given.
ConformanceReport runConformanceSuite(
    PayloadWriter& writer,
    PayloadReader& reader,
    memory::MemoryPool* pool,
    ConformanceOptions options = {});

} // namespace bytedance::bolt::shuffle::sparksql::test
