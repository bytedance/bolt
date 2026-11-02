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

#include <folly/lang/Bits.h>
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

/// A RunSink that appends to a string and patches it in place.
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
  return folly::loadUnaligned<uint64_t>(bytes.data() + at);
}

/// Parses `run` as CellFormat.h lays out a run of `numStreams` streams,
/// inflating them with `codec`.
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

/// Writes `streams` as one run, each handed over in pieces of 1000 bytes.
std::string compressRun(
    RunCompressor& compressor,
    const std::vector<std::string>& streams) {
  constexpr size_t kPiece = 1000;
  StringSink sink;
  compressor.write(
      sink,
      static_cast<uint32_t>(streams.size()),
      [&](uint32_t s) { return streams[s].size(); },
      [&](uint32_t s, RunPieceFn fn) {
        for (size_t at = 0; at < streams[s].size(); at += kPiece) {
          const size_t bytes = std::min(kPiece, streams[s].size() - at);
          fn(streams[s].data() + at, static_cast<uint32_t>(bytes));
        }
      });
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

/// RunCompressor under each codec, streaming and not adaptive.
class RunCompressorTest
    : public testing::TestWithParam<arrow::Compression::type> {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    options_.compressionType = GetParam();
  }

  /// Streaming, not adaptive.
  static CellShuffleOptions cellOptions() {
    CellShuffleOptions options;
    options.streamCompression = true;
    options.adaptiveCompression = false;
    return options;
  }

  PartitionWriterOptions options_;
  uint64_t compressTimeNs_{0};
};

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
    RunCompressor compressor(options_, cellOptions(), &compressTimeNs_);
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

  RunCompressor stalled(options_, cellOptions(), &compressTimeNs_);
  stalled.testingSetStreamCompressor(
      std::make_unique<ShortReadCompressor>(type, realStreamForm(), 0));
  BOLT_ASSERT_THROW(compressRun(stalled, streams), "made no progress");
}

INSTANTIATE_TEST_SUITE_P(
    Codecs,
    RunCompressorTest,
    testing::Values(
        arrow::Compression::LZ4_FRAME,
        arrow::Compression::ZSTD,
        arrow::Compression::GZIP),
    [](const auto& info) {
      switch (info.param) {
        case arrow::Compression::ZSTD:
          return "zstd";
        case arrow::Compression::GZIP:
          return "gzip";
        default:
          return "lz4";
      }
    });

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
