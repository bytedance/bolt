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

#include <cstring>
#include <random>
#include <string>
#include <vector>

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

  std::shared_ptr<memory::MemoryPool> pool_;
  PartitionWriterOptions options_;
  uint64_t compressTimeNs_{0};
};

TEST_P(RunCompressorTest, layoutFollowsTheRunSize) {
  RunCompressor compressor(
      options_, streaming(), pool_.get(), &compressTimeNs_);
  struct Case {
    std::vector<std::string> streams;
    RunLayout layout;
  };
  const std::vector<Case> cases = {
      // Too small to pay for a frame.
      {{repetitive(300), repetitive(200)}, RunLayout::kCombinedUncompressed},
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
}

TEST_P(RunCompressorTest, separateStreamThatDoesNotShrinkIsStored) {
  // Each stream decides alone: the random one is stored, read back as it
  // is, while the repetitive one beside it is compressed.
  RunCompressor compressor(
      options_, streaming(), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(7);
  const std::vector<std::string> streams = {
      randomBytes(600 << 10, rng),
      repetitive(900 << 10),
      randomBytes(100 << 10, rng), // judged on its whole size
      repetitive(512)};
  const auto run = compressRun(compressor, streams);
  const auto decoded = decodeRun(run, 4, compressor.codec());
  ASSERT_EQ(decoded.layout, RunLayout::kSeparate);
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_FALSE(decoded.compressed[0]);
  EXPECT_TRUE(decoded.compressed[1]);
  EXPECT_FALSE(decoded.compressed[2]);
  EXPECT_FALSE(decoded.compressed[3]); // below kMinCompressRunBytes
  EXPECT_LT(readU64(run, 9) * 4, streams[1].size());
}

TEST_P(RunCompressorTest, combinedFrameIsJudgedOnItsWholeSize) {
  // A COMBINED frame spans streams: an incompressible first stream does
  // not decide for the compressible ones after it.
  RunCompressor compressor(
      options_, streaming(), pool_.get(), &compressTimeNs_);
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
      options_, streaming(), pool_.get(), &compressTimeNs_);
  std::mt19937_64 rng(11);
  const std::vector<std::string> streams = {
      repetitive(2 << 20) + randomBytes(2 << 20, rng)};
  const auto run = compressRun(compressor, streams);
  const auto decoded = decodeRun(run, 1, compressor.codec());
  EXPECT_EQ(decoded.streams, streams);
  EXPECT_TRUE(decoded.compressed[0]);
}

TEST_P(RunCompressorTest, unfundedOneShotFrameIsStored) {
  // A pool with no room for the workspace: one-shot stores the frame and
  // says so; streaming needs no workspace and compresses regardless.
  auto root = memory::memoryManager()->addRootPool("runCompressor", 1 << 20);
  auto leaf = root->addLeafChild("compressor");
  RunCompressor compressor(options_, streaming(), leaf.get(), &compressTimeNs_);
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
      options_, streaming(), pool_.get(), &compressTimeNs_);
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

INSTANTIATE_TEST_SUITE_P(
    Codecs,
    RunCompressorTest,
    testing::Combine(
        testing::Values(
            arrow::Compression::LZ4_FRAME,
            arrow::Compression::ZSTD),
        testing::Bool()),
    [](const auto& info) {
      return std::string(
                 std::get<0>(info.param) == arrow::Compression::ZSTD ? "zstd"
                                                                     : "lz4") +
          (std::get<1>(info.param) ? "Streaming" : "OneShot");
    });

TEST(RunCompressorUncompressedTest, storesEveryRun) {
  memory::MemoryManager::testingSetInstance({});
  auto pool = memory::memoryManager()->addLeafPool();
  PartitionWriterOptions options;
  options.compressionType = arrow::Compression::UNCOMPRESSED;
  uint64_t timeNs = 0;
  RunCompressor compressor(options, /*streaming=*/true, pool.get(), &timeNs);
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
