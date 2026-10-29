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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/cell/RunCompressor.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

/// A RunSink over a string, recording the patches it takes.
class StringSink final : public RunSink {
 public:
  uint64_t position() const override {
    return bytes.size();
  }
  void write(const void* data, size_t size) override {
    bytes.append(static_cast<const char*>(data), size);
  }
  void patch(uint64_t position, const void* data, size_t size) override {
    ASSERT_LE(position + size, bytes.size());
    ::memcpy(bytes.data() + position, data, size);
  }

  std::string bytes;
};

/// One run read back: its layout, and per stream the decoded bytes and
/// whether they were compressed.
struct DecodedRun {
  RunLayout layout;
  std::vector<std::string> streams;
  std::vector<bool> compressed;
};

uint64_t readU64(const std::string& bytes, size_t at) {
  uint64_t value;
  ::memcpy(&value, bytes.data() + at, sizeof(value));
  return value;
}

DecodedRun
decodeRun(const std::string& run, uint32_t numStreams, Codec* codec) {
  DecodedRun decoded;
  decoded.layout = static_cast<RunLayout>(run[0]);
  decoded.streams.resize(numStreams);
  decoded.compressed.assign(numStreams, false);
  const auto inflate = [&](size_t at, uint64_t stored, uint64_t bytes) {
    std::string out(bytes, '\0');
    EXPECT_EQ(
        codec->decompress(
            reinterpret_cast<const uint8_t*>(run.data() + at),
            static_cast<int64_t>(stored),
            reinterpret_cast<uint8_t*>(out.data()),
            static_cast<int64_t>(bytes)),
        static_cast<int64_t>(bytes));
    return out;
  };
  if (decoded.layout == RunLayout::kSeparate) {
    size_t at = 1 + 16ull * numStreams;
    for (uint32_t s = 0; s < numStreams; ++s) {
      const uint64_t stored = readU64(run, 1 + 8ull * s);
      const uint64_t bytes = readU64(run, 1 + 8ull * (numStreams + s));
      if (bytes == 0) {
        decoded.streams[s] = run.substr(at, stored);
      } else {
        decoded.streams[s] = inflate(at, stored, bytes);
        decoded.compressed[s] = true;
      }
      at += stored;
    }
    EXPECT_EQ(at, run.size());
    return decoded;
  }
  const uint64_t stored = readU64(run, 1);
  std::vector<uint64_t> sizes(numStreams);
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    sizes[s] = readU64(run, 9 + 8ull * s);
    total += sizes[s];
  }
  const size_t bodyAt = runHeaderBytes(numStreams);
  EXPECT_EQ(bodyAt + stored, run.size());
  const bool compressed = decoded.layout == RunLayout::kCombined;
  const std::string body =
      compressed ? inflate(bodyAt, stored, total) : run.substr(bodyAt);
  uint64_t at = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    decoded.streams[s] = body.substr(at, sizes[s]);
    decoded.compressed[s] = compressed;
    at += sizes[s];
  }
  return decoded;
}

/// Writes `streams` as one run, each handed over in pieces of `piece`
/// bytes.
std::string compressRun(
    RunCompressor& compressor,
    const std::vector<std::string>& streams,
    bool* final = nullptr,
    size_t piece = 1000) {
  std::vector<uint64_t> sizes;
  for (const auto& stream : streams) {
    sizes.push_back(stream.size());
  }
  StringSink sink;
  const bool isFinal = compressor.write(
      sink,
      static_cast<uint32_t>(streams.size()),
      sizes.data(),
      [&](uint32_t s, RunPieceFn fn) {
        for (size_t at = 0; at < streams[s].size(); at += piece) {
          const size_t bytes = std::min(piece, streams[s].size() - at);
          fn(streams[s].data() + at, static_cast<uint32_t>(bytes));
        }
      });
  if (final != nullptr) {
    *final = isFinal;
  }
  return sink.bytes;
}

std::string randomBytes(size_t size, std::mt19937_64& rng) {
  std::string bytes(size, '\0');
  for (auto& byte : bytes) {
    byte = static_cast<char>(rng());
  }
  return bytes;
}

std::string repetitive(size_t size) {
  std::string bytes(size, '\0');
  for (size_t i = 0; i < size; ++i) {
    bytes[i] = static_cast<char>('a' + (i / 7) % 4);
  }
  return bytes;
}

class RunCompressorTest : public testing::TestWithParam<
                              std::tuple<arrow::Compression::type, bool>> {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
    options_.compressionType = std::get<0>(GetParam());
  }

  bool streaming() const {
    return std::get<1>(GetParam());
  }

  /// The mode under test; not adaptive unless a test asks for it.
  CellShuffleOptions cellOptions(bool adaptive = false) const {
    CellShuffleOptions options;
    options.streamCompression = streaming();
    options.adaptiveCompression = adaptive;
    return options;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  PartitionWriterOptions options_;
  uint64_t compressTimeNs_{0};
};

TEST_P(RunCompressorTest, layoutFollowsTheRunSize) {
  RunCompressor compressor(
      options_, cellOptions(), pool_.get(), &compressTimeNs_);
  struct Case {
    std::vector<std::string> streams;
    RunLayout layout;
  };
  const std::vector<Case> cases = {
      // Too small to pay for a frame.
      {{repetitive(120), repetitive(80)}, RunLayout::kCombinedUncompressed},
      // One frame over both streams.
      {{repetitive(20 << 10), repetitive(10 << 10)}, RunLayout::kCombined},
      // A frame per stream; the empty stream takes none.
      {{repetitive(300 << 10), "", repetitive(700 << 10)},
       RunLayout::kSeparate},
  };
  for (const auto& test : cases) {
    bool final = false;
    const auto run = compressRun(compressor, test.streams, &final);
    EXPECT_TRUE(final);
    const auto decoded = decodeRun(
        run, static_cast<uint32_t>(test.streams.size()), compressor.codec());
    EXPECT_EQ(decoded.layout, test.layout);
    EXPECT_EQ(decoded.streams, test.streams);
  }
  EXPECT_GT(compressTimeNs_, 0);
  const auto& stats = compressor.stats();
  EXPECT_EQ(stats.storedRuns, 1);
  EXPECT_EQ(stats.combinedRuns, 1);
  EXPECT_EQ(stats.separateRuns, 1);
  EXPECT_EQ(stats.tinyFrames, 1);
  EXPECT_EQ(stats.tinyBytes, 200);
  // One COMBINED frame, two SEPARATE ones: the empty stream takes none.
  EXPECT_EQ(stats.compressedFrames, 3);
  EXPECT_EQ(stats.compressedInBytes, (30 << 10) + (1000 << 10));
  EXPECT_LT(stats.compressedOutBytes, stats.compressedInBytes / 4);
}

TEST_P(RunCompressorTest, separateStreamThatDoesNotShrinkIsStored) {
  // Each stream decides alone: the random one is stored, read back as it
  // is, while the repetitive one beside it is compressed.
  RunCompressor compressor(
      options_, cellOptions(), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(7);
  const std::vector<std::string> streams = {
      randomBytes(600 << 10, rng),
      repetitive(900 << 10),
      randomBytes(100 << 10, rng), // judged on its whole size
      repetitive(200)};
  const auto run = compressRun(compressor, streams);
  const auto decoded = decodeRun(run, 4, compressor.codec());
  ASSERT_EQ(decoded.layout, RunLayout::kSeparate);
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_FALSE(decoded.compressed[0]);
  EXPECT_TRUE(decoded.compressed[1]);
  EXPECT_FALSE(decoded.compressed[2]);
  EXPECT_FALSE(decoded.compressed[3]); // below kMinCompressBytes
  EXPECT_LT(readU64(run, 9) * 4, streams[1].size());
}

TEST_P(RunCompressorTest, combinedFrameIsJudgedOnItsWholeSize) {
  // A COMBINED frame spans streams: an incompressible first stream does
  // not decide for the compressible ones after it.
  RunCompressor compressor(
      options_, cellOptions(), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(9);
  const std::vector<std::string> streams = {
      randomBytes(8 << 10, rng), repetitive(40 << 10)};
  const auto run = compressRun(compressor, streams);
  const auto decoded = decodeRun(run, 2, compressor.codec());
  EXPECT_EQ(decoded.layout, RunLayout::kCombined);
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_LT(run.size(), (48 << 10) / 2);
}

TEST_P(RunCompressorTest, frameStoredLateKeepsWhatWasWritten) {
  // A frame that shrinks over its first blocks is kept compressed even
  // when the rest does not: it has reached the sink by then, and nothing
  // written is taken back.
  RunCompressor compressor(
      options_, cellOptions(), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(11);
  const std::vector<std::string> streams = {
      repetitive(2 << 20) + randomBytes(2 << 20, rng)};
  const auto run = compressRun(compressor, streams);
  const auto decoded = decodeRun(run, 1, compressor.codec());
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_TRUE(decoded.compressed[0]);
}

/// A stream form that takes at most `maxRead` input bytes per call - or
/// none at all - in front of a real one: codec behaviour the real codecs
/// rarely show, stopping short of the input they are given.
class ShortReadCompressor final : public StreamCompressor {
 public:
  ShortReadCompressor(
      CodecType type,
      std::unique_ptr<StreamCompressor> inner,
      int64_t maxRead)
      : StreamCompressor(type, CodecOptions{CodecBackend::NONE}),
        inner_(std::move(inner)),
        maxRead_(maxRead) {}

  int32_t defaultCompressionLevel() const override {
    return inner_->defaultCompressionLevel();
  }
  StreamCompressResult compress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen) override {
    ++calls;
    const int64_t take = std::min(inputLen, maxRead_);
    if (take == 0) {
      return StreamCompressResult{0, 0};
    }
    shortReads += take < inputLen;
    return inner_->compress(input, take, output, outputLen);
  }
  StreamFlushResult flush(uint8_t* output, int64_t outputLen) override {
    return inner_->flush(output, outputLen);
  }
  StreamEndResult end(uint8_t* output, int64_t outputLen) override {
    return inner_->end(output, outputLen);
  }
  void reset() override {
    inner_->reset();
  }
  int64_t recommendedInputSize() const override {
    return inner_->recommendedInputSize();
  }
  int64_t recommendedOutputSize(int64_t inputSize) const override {
    return inner_->recommendedOutputSize(inputSize);
  }

  int64_t calls{0};
  int64_t shortReads{0};

 private:
  const std::unique_ptr<StreamCompressor> inner_;
  const int64_t maxRead_;
};

TEST_P(RunCompressorTest, streamCompressorStoppingShortIsFedTheRest) {
  // A stream form may take less than a whole staging piece per call: the
  // compressor writes out what it holds and feeds it the rest. One that
  // takes nothing and writes nothing fails the frame rather than spinning.
  if (!streaming()) {
    return;
  }
  const auto type = fromArrowCodecType(options_.compressionType);
  const auto realStreamForm = [&] {
    CodecOptions codecOptions{
        CodecBackend::NONE,
        options_.compressionLevel,
        options_.checksumEnabled,
        /*independentBlocks=*/true};
    return StreamCompressor::create(type, codecOptions);
  };
  std::mt19937_64 rng(19);
  std::string mixed;
  while (mixed.size() < (2 << 20)) {
    mixed += repetitive(20 << 10);
    mixed += randomBytes(12 << 10, rng);
  }
  const std::vector<std::string> streams = {mixed, repetitive(100 << 10)};
  for (const int64_t maxRead : {int64_t{1000}, int64_t{100} << 10}) {
    SCOPED_TRACE(maxRead);
    RunCompressor compressor(
        options_, cellOptions(), pool_.get(), &compressTimeNs_);
    auto shortReads =
        std::make_unique<ShortReadCompressor>(type, realStreamForm(), maxRead);
    auto* probe = shortReads.get();
    compressor.testingSetStreamCompressor(std::move(shortReads));
    const auto decoded =
        decodeRun(compressRun(compressor, streams), 2, compressor.codec());
    EXPECT_EQ(decoded.streams, streams);
    EXPECT_TRUE(decoded.compressed[0]);
    EXPECT_GT(probe->shortReads, 0);
  }

  RunCompressor stalled(options_, cellOptions(), pool_.get(), &compressTimeNs_);
  stalled.testingSetStreamCompressor(
      std::make_unique<ShortReadCompressor>(type, realStreamForm(), 0));
  BOLT_ASSERT_THROW(compressRun(stalled, streams), "made no progress");
}

TEST_P(RunCompressorTest, unfundedOneShotFrameIsStored) {
  // A pool with no room for the workspace: one-shot stores the frame and
  // says so; streaming needs no workspace and compresses regardless.
  auto root = memory::memoryManager()->addRootPool("runCompressor", 1 << 20);
  auto leaf = root->addLeafChild("compressor");
  RunCompressor compressor(
      options_, cellOptions(), leaf.get(), &compressTimeNs_);
  const std::vector<std::string> streams = {repetitive(3 << 20)};
  bool final = true;
  const auto run = compressRun(compressor, streams, &final);
  const auto decoded = decodeRun(run, 1, compressor.codec());
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_EQ(final, streaming());
  EXPECT_EQ(decoded.compressed[0], streaming());
}

TEST_P(RunCompressorTest, storedRunIsCombinedUncompressed) {
  RunCompressor compressor(
      options_, cellOptions(), pool_.get(), &compressTimeNs_);
  const std::vector<std::string> streams = {repetitive(100 << 10), "xyz"};
  const std::vector<uint64_t> sizes = {streams[0].size(), streams[1].size()};
  StringSink sink;
  compressor.writeStored(sink, 2, sizes.data(), [&](uint32_t s, RunPieceFn fn) {
    fn(streams[s].data(), static_cast<uint32_t>(streams[s].size()));
  });
  const auto decoded = decodeRun(sink.bytes, 2, compressor.codec());
  EXPECT_EQ(decoded.layout, RunLayout::kCombinedUncompressed);
  EXPECT_EQ(decoded.streams, streams);
}

TEST_P(RunCompressorTest, adaptiveSkipsCombinedFrames) {
  // COMBINED frames that do not shrink have the next nine stored without a
  // codec call and the tenth tried; once they shrink again they are
  // compressed every time.
  RunCompressor compressor(
      options_, cellOptions(/*adaptive=*/true), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(13);
  // COMBINED frames: random throughout, so 1 in 10 is tried.
  int compressedCalls = 0;
  for (int frame = 0; frame < 21; ++frame) {
    const std::vector<std::string> streams = {
        randomBytes(20 << 10, rng), randomBytes(10 << 10, rng)};
    const uint64_t before = compressTimeNs_;
    const auto run = compressRun(compressor, streams);
    const auto decoded = decodeRun(run, 2, compressor.codec());
    EXPECT_EQ(decoded.layout, RunLayout::kCombinedUncompressed);
    EXPECT_EQ(decoded.streams, streams);
    compressedCalls += compressTimeNs_ > before;
  }
  EXPECT_EQ(compressedCalls, 3);

  // A compressible COMBINED frame is still compressed when its turn to be
  // tried comes, and from then on every time.
  bool compressedAgain = false;
  for (int frame = 0; frame < 10; ++frame) {
    const std::vector<std::string> streams = {
        repetitive(20 << 10), repetitive(10 << 10)};
    const auto decoded =
        decodeRun(compressRun(compressor, streams), 2, compressor.codec());
    EXPECT_EQ(decoded.streams, streams);
    compressedAgain |= decoded.layout == RunLayout::kCombined;
  }
  EXPECT_TRUE(compressedAgain);
  const std::vector<std::string> streams = {
      repetitive(20 << 10), repetitive(10 << 10)};
  EXPECT_EQ(
      decodeRun(compressRun(compressor, streams), 2, compressor.codec()).layout,
      RunLayout::kCombined);
}

TEST_P(RunCompressorTest, nullBodiesCompressInPlaceWhenTheyPay) {
  RunCompressor compressor(
      options_, cellOptions(/*adaptive=*/true), pool_.get(), &compressTimeNs_);
  const std::string header(24, 'h');
  const auto withBody = [&](const std::string& body) {
    BufferPtr out;
    appendBytes(out, header.data(), header.size(), pool_.get());
    appendBytes(out, body.data(), body.size(), pool_.get());
    return out;
  };
  // A compressible body after the header: replaced by its compressed form.
  const auto body = repetitive(4000);
  auto out = withBody(body);
  EXPECT_EQ(compressor.compressNullBody(out, header.size()), body.size());
  ASSERT_LT(out->size(), header.size() + body.size());
  EXPECT_EQ(std::string(out->as<char>(), header.size()), header);
  std::string back(body.size(), '\0');
  EXPECT_EQ(
      compressor.codec()->decompress(
          out->as<uint8_t>() + header.size(),
          static_cast<int64_t>(out->size() - header.size()),
          reinterpret_cast<uint8_t*>(back.data()),
          static_cast<int64_t>(back.size())),
      static_cast<int64_t>(back.size()));
  EXPECT_EQ(back, body);

  // Below kMinCompressBytes: left as it is, without a try.
  const uint64_t before = compressTimeNs_;
  out = withBody(repetitive(200));
  EXPECT_EQ(compressor.compressNullBody(out, header.size()), 0);
  EXPECT_EQ(out->size(), header.size() + 200);
  EXPECT_EQ(compressTimeNs_, before);

  // One that does not compress to half its size stays as it is, and the
  // next 9 are not tried.
  std::mt19937_64 rng(3);
  int tried = 0;
  for (int i = 0; i < 20; ++i) {
    const auto random = randomBytes(1000, rng);
    out = withBody(random);
    const uint64_t start = compressTimeNs_;
    EXPECT_EQ(compressor.compressNullBody(out, header.size()), 0);
    EXPECT_EQ(std::string(out->as<char>() + header.size(), 1000), random);
    tried += compressTimeNs_ > start;
  }
  EXPECT_EQ(tried, 2);
  const auto& stats = compressor.stats();
  EXPECT_EQ(stats.compressedNullBodies, 1);
  EXPECT_EQ(stats.nullInBytes, body.size());
  EXPECT_EQ(stats.storedNullBodies, 21);
  EXPECT_EQ(stats.storedNullBytes, 200 + 20 * 1000);
}

TEST_P(RunCompressorTest, adaptiveSkipsPerStream) {
  // A SEPARATE stream that does not shrink is tried 1 in 10 frames: counted
  // through the codec time, the other stream being too small to compress.
  RunCompressor compressor(
      options_, cellOptions(/*adaptive=*/true), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(17);
  int tried = 0;
  for (int frame = 0; frame < 25; ++frame) {
    const std::vector<std::string> streams = {
        randomBytes(100 << 10, rng), repetitive(200)};
    const uint64_t before = compressTimeNs_;
    const auto decoded =
        decodeRun(compressRun(compressor, streams), 2, compressor.codec());
    EXPECT_EQ(decoded.streams, streams);
    EXPECT_FALSE(decoded.compressed[0]);
    tried += compressTimeNs_ > before;
  }
  EXPECT_EQ(tried, 3); // frames 0, 10 and 20
  const auto& stats = compressor.stats();
  EXPECT_EQ(stats.unshrunkFrames, 3);
  EXPECT_EQ(stats.skippedFrames, 22);
  EXPECT_EQ(stats.skippedBytes, 22ull * (100 << 10));
  EXPECT_EQ(stats.tinyFrames, 25); // the 200-byte stream
  ASSERT_GE(compressor.streamStats().size(), 1);
  EXPECT_EQ(compressor.streamStats()[0].tried, 3);
  EXPECT_EQ(compressor.streamStats()[0].skipped, 22);
  EXPECT_EQ(compressor.streamStats()[0].compressed, 0);
  // Another number of streams: the hints start over.
  const std::vector<std::string> three = {
      randomBytes(100 << 10, rng), repetitive(200), repetitive(200)};
  const uint64_t before = compressTimeNs_;
  compressRun(compressor, three);
  EXPECT_GT(compressTimeNs_, before);
}

INSTANTIATE_TEST_SUITE_P(
    Codecs,
    RunCompressorTest,
    testing::Combine(
        testing::Values(
            arrow::Compression::LZ4_FRAME,
            arrow::Compression::ZSTD,
            arrow::Compression::GZIP),
        testing::Bool()),
    [](const auto& info) {
      const auto codec = std::get<0>(info.param);
      const char* name = codec == arrow::Compression::ZSTD ? "zstd"
          : codec == arrow::Compression::GZIP              ? "gzip"
                                                           : "lz4";
      return std::string(name) +
          (std::get<1>(info.param) ? "Streaming" : "OneShot");
    });

TEST(RunCompressorUncompressedTest, storesEveryRun) {
  memory::MemoryManager::testingSetInstance({});
  auto pool = memory::memoryManager()->addLeafPool();
  PartitionWriterOptions options;
  options.compressionType = arrow::Compression::UNCOMPRESSED;
  uint64_t timeNs = 0;
  RunCompressor compressor(options, CellShuffleOptions{}, pool.get(), &timeNs);
  EXPECT_FALSE(compressor.compresses());
  const std::vector<std::string> streams = {repetitive(200 << 10)};
  bool final = false;
  const auto run = compressRun(compressor, streams, &final);
  EXPECT_TRUE(final);
  EXPECT_EQ(static_cast<RunLayout>(run[0]), RunLayout::kCombinedUncompressed);
  EXPECT_EQ(run.substr(runHeaderBytes(1)), streams[0]);
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
