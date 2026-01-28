/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#include <folly/Benchmark.h>
#include <glog/logging.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

using namespace bytedance::bolt::shuffle::sparksql;

constexpr size_t kDataSize = 1 * 1024 * 1024;

std::vector<uint8_t> generateCompressibleData(size_t size) {
  std::vector<uint8_t> data(size);
  for (size_t i = 0; i < size; i++) {
    data[i] = static_cast<uint8_t>((i % 64) ^ ((i / 64) % 256));
  }
  return data;
}

const std::vector<uint8_t>& testData() {
  static const std::vector<uint8_t> data = generateCompressibleData(kDataSize);
  return data;
}

std::string codecTypeName(CodecType type) {
  switch (type) {
    case CodecType::ZSTD:
      return "ZSTD";
    case CodecType::GZIP:
      return "GZIP";
    case CodecType::LZ4:
      return "LZ4";
    case CodecType::LZ4_FRAME:
      return "LZ4_FRAME";
    case CodecType::SNAPPY:
      return "SNAPPY";
    default:
      return "UNKNOWN";
  }
}

void logMetrics(
    const std::string& name,
    int64_t compressedSize,
    int64_t totalCompressTime,
    int64_t totalDecompressTime,
    size_t iterations) {
  if (iterations == 0) {
    return;
  }

  const double dataSize = static_cast<double>(kDataSize);
  const double compressed = static_cast<double>(compressedSize);
  const double compressionRatio = compressed > 0 ? dataSize / compressed : 0.0;

  const double compressThroughput = totalCompressTime > 0
      ? dataSize * iterations / static_cast<double>(totalCompressTime) * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  const double decompressThroughput = totalDecompressTime > 0
      ? dataSize * iterations / static_cast<double>(totalDecompressTime) * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  LOG(INFO) << name << " data_size=" << kDataSize
            << " compressed_size=" << compressedSize
            << " compression_ratio=" << compressionRatio
            << " compress_MBps=" << compressThroughput
            << " decompress_MBps=" << decompressThroughput;
}

void runOneShotBenchmark(CodecType type, bool checksumEnabled, size_t n) {
  folly::BenchmarkSuspender suspender;

  CodecOptions options;
  options.checksumEnabled = checksumEnabled;
  auto codec = Codec::create(type, options);

  const auto& data = testData();
  int64_t maxCompressedSize = codec->maxCompressedLen(data.size());
  std::vector<uint8_t> compressed(maxCompressedSize);
  std::vector<uint8_t> decompressed(data.size());

  suspender.dismiss();

  int64_t totalCompressTime = 0;
  int64_t totalDecompressTime = 0;
  int64_t compressedSize = 0;

  for (size_t i = 0; i < n; ++i) {
    auto compressStart = std::chrono::steady_clock::now();
    compressedSize = codec->compress(
        data.data(), data.size(), compressed.data(), compressed.size());
    auto compressEnd = std::chrono::steady_clock::now();
    totalCompressTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
                             compressEnd - compressStart)
                             .count();

    auto decompressStart = std::chrono::steady_clock::now();
    codec->decompress(
        compressed.data(),
        compressedSize,
        decompressed.data(),
        decompressed.size());
    auto decompressEnd = std::chrono::steady_clock::now();
    totalDecompressTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
                               decompressEnd - decompressStart)
                               .count();
  }

  suspender.rehire();

  std::string name = "OneShot_" + codecTypeName(type) +
      (checksumEnabled ? "_Checksum" : "_NoChecksum");
  logMetrics(name, compressedSize, totalCompressTime, totalDecompressTime, n);
}

void runStreamBenchmark(CodecType type, size_t n) {
  folly::BenchmarkSuspender suspender;

  CodecOptions options;
  options.checksumEnabled = false;

  const auto& data = testData();

  suspender.dismiss();

  int64_t totalCompressTime = 0;
  int64_t totalDecompressTime = 0;
  int64_t compressedSize = 0;

  for (size_t i = 0; i < n; ++i) {
    auto compressor = StreamCompressor::create(type, options);

    int64_t outputSize = compressor->recommendedOutputSize(data.size());
    std::vector<uint8_t> compressed(outputSize * 2);

    auto compressStart = std::chrono::steady_clock::now();
    auto compressResult = compressor->compress(
        data.data(), data.size(), compressed.data(), compressed.size());
    CHECK_EQ(compressResult.bytesRead, static_cast<int64_t>(data.size()));
    int64_t totalWritten = compressResult.bytesWritten;

    auto endResult = compressor->end(
        compressed.data() + totalWritten, compressed.size() - totalWritten);
    while (endResult.shouldRetry) {
      totalWritten += endResult.bytesWritten;
      endResult = compressor->end(
          compressed.data() + totalWritten, compressed.size() - totalWritten);
    }
    totalWritten += endResult.bytesWritten;
    compressedSize = totalWritten;

    auto compressEnd = std::chrono::steady_clock::now();
    totalCompressTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
                             compressEnd - compressStart)
                             .count();

    auto decompressor = StreamDecompressor::create(type, options);
    std::vector<uint8_t> decompressed(data.size());

    auto decompressStart = std::chrono::steady_clock::now();
    auto decompressResult = decompressor->decompress(
        compressed.data(),
        compressedSize,
        decompressed.data(),
        decompressed.size());
    auto decompressEnd = std::chrono::steady_clock::now();
    totalDecompressTime += std::chrono::duration_cast<std::chrono::nanoseconds>(
                               decompressEnd - decompressStart)
                               .count();

    CHECK_EQ(decompressResult.bytesWritten, static_cast<int64_t>(data.size()));
  }

  suspender.rehire();

  std::string name = "Stream_" + codecTypeName(type);
  logMetrics(name, compressedSize, totalCompressTime, totalDecompressTime, n);
}

BENCHMARK(OneShot_ZSTD_NoChecksum, n) {
  runOneShotBenchmark(CodecType::ZSTD, false, n);
}

BENCHMARK(OneShot_ZSTD_Checksum, n) {
  runOneShotBenchmark(CodecType::ZSTD, true, n);
}

BENCHMARK(OneShot_GZIP_NoChecksum, n) {
  runOneShotBenchmark(CodecType::GZIP, false, n);
}

BENCHMARK(OneShot_GZIP_Checksum, n) {
  runOneShotBenchmark(CodecType::GZIP, true, n);
}

BENCHMARK(OneShot_LZ4_NoChecksum, n) {
  runOneShotBenchmark(CodecType::LZ4, false, n);
}

BENCHMARK(OneShot_LZ4_FRAME_NoChecksum, n) {
  runOneShotBenchmark(CodecType::LZ4_FRAME, false, n);
}

BENCHMARK(OneShot_SNAPPY_NoChecksum, n) {
  runOneShotBenchmark(CodecType::SNAPPY, false, n);
}

BENCHMARK(Stream_ZSTD, n) {
  runStreamBenchmark(CodecType::ZSTD, n);
}

BENCHMARK(Stream_GZIP, n) {
  runStreamBenchmark(CodecType::GZIP, n);
}

BENCHMARK(Stream_LZ4_FRAME, n) {
  runStreamBenchmark(CodecType::LZ4_FRAME, n);
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  folly::runBenchmarks();
  return 0;
}
