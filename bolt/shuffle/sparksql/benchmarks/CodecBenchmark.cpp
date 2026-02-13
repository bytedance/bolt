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
#include <folly/Benchmark.h>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

using namespace bytedance::bolt::shuffle::sparksql;

constexpr size_t kDataSize = 1 * 1024 * 1024;

// Store final metrics for each benchmark
struct BenchmarkMetrics {
  int64_t compressedSize{0};
  double compressMBps{0};
  double decompressMBps{0};
};

std::map<std::string, BenchmarkMetrics>& getMetricsMap() {
  static std::map<std::string, BenchmarkMetrics> metricsMap;
  return metricsMap;
}

// Generate realistic data that simulates shuffle payload:
// - Mix of sequential IDs (like row IDs)
// - Random values (like hash keys, timestamps)
// - Repeated values (like partition keys, status codes)
// - Some null markers
// Target compression ratio: 2-5x (typical for real data)
std::vector<uint8_t> generateRealisticData(size_t size) {
  std::vector<uint8_t> data(size);
  std::mt19937 rng(42); // Fixed seed for reproducibility

  size_t offset = 0;
  while (offset < size) {
    // Simulate different column types in a row
    size_t rowSize = std::min(size - offset, size_t(64));

    // 8 bytes: Sequential ID (highly compressible)
    if (rowSize >= 8) {
      uint64_t id = offset / 64;
      memcpy(&data[offset], &id, 8);
      offset += 8;
      rowSize -= 8;
    }

    // 8 bytes: Random timestamp-like value (low compressibility)
    if (rowSize >= 8) {
      uint64_t ts = rng();
      memcpy(&data[offset], &ts, 8);
      offset += 8;
      rowSize -= 8;
    }

    // 4 bytes: Partition key (repeated, medium compressibility)
    if (rowSize >= 4) {
      uint32_t partKey = (offset / 1024) % 100;
      memcpy(&data[offset], &partKey, 4);
      offset += 4;
      rowSize -= 4;
    }

    // 4 bytes: Random int (low compressibility)
    if (rowSize >= 4) {
      uint32_t val = rng();
      memcpy(&data[offset], &val, 4);
      offset += 4;
      rowSize -= 4;
    }

    // 8 bytes: Amount/price with some patterns (medium compressibility)
    if (rowSize >= 8) {
      double amount = (rng() % 10000) / 100.0;
      memcpy(&data[offset], &amount, 8);
      offset += 8;
      rowSize -= 8;
    }

    // Remaining: Status codes and flags (high compressibility)
    while (rowSize > 0) {
      data[offset] = static_cast<uint8_t>(rng() % 10); // Values 0-9
      offset++;
      rowSize--;
    }
  }

  return data;
}

const std::vector<uint8_t>& testData() {
  static const std::vector<uint8_t> data = generateRealisticData(kDataSize);
  return data;
}

void storeMetrics(
    const std::string& name,
    int64_t compressedSize,
    int64_t totalCompressTime,
    int64_t totalDecompressTime,
    size_t iterations) {
  if (iterations == 0) {
    return;
  }

  const double dataSize = static_cast<double>(kDataSize);

  const double compressThroughput = totalCompressTime > 0
      ? dataSize * iterations / static_cast<double>(totalCompressTime) * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  const double decompressThroughput = totalDecompressTime > 0
      ? dataSize * iterations / static_cast<double>(totalDecompressTime) * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  // Store/update metrics (keeps the latest values)
  auto& metrics = getMetricsMap()[name];
  metrics.compressedSize = compressedSize;
  metrics.compressMBps = compressThroughput;
  metrics.decompressMBps = decompressThroughput;
}

void printAllMetrics() {
  std::cout << "\n=== Codec Benchmark Metrics ===" << std::endl;
  std::cout << "Data size: " << kDataSize << " bytes (" << kDataSize / 1024
            << " KB)" << std::endl;
  std::cout << std::endl;

  printf(
      "%-35s %12s %15s %15s\n",
      "Benchmark",
      "Compressed",
      "Compress",
      "Decompress");
  printf("%-35s %12s %15s %15s\n", "", "(bytes)", "(MB/s)", "(MB/s)");
  printf("%s\n", std::string(80, '-').c_str());

  for (const auto& [name, metrics] : getMetricsMap()) {
    printf(
        "%-35s %12ld %15.1f %15.1f\n",
        name.c_str(),
        metrics.compressedSize,
        metrics.compressMBps,
        metrics.decompressMBps);
  }
  std::cout << std::endl;
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

  std::string name = "OneShot_" + std::string(Codec::codecTypeName(type)) +
      (checksumEnabled ? "_Checksum" : "_NoChecksum");
  storeMetrics(name, compressedSize, totalCompressTime, totalDecompressTime, n);
}

void runStreamBenchmark(CodecType type, bool checksumEnabled, size_t n) {
  folly::BenchmarkSuspender suspender;

  CodecOptions options;
  options.checksumEnabled = checksumEnabled;

  const auto& data = testData();

  suspender.dismiss();

  int64_t totalCompressTime = 0;
  int64_t totalDecompressTime = 0;
  int64_t compressedSize = 0;

  auto compressor = StreamCompressor::create(type, options);
  auto decompressor = StreamDecompressor::create(type, options);
  for (size_t i = 0; i < n; ++i) {
    int64_t outputSize = compressor->recommendedOutputSize(data.size());
    std::vector<uint8_t> compressed(outputSize * 2);

    auto compressStart = std::chrono::steady_clock::now();
    auto compressResult = compressor->compress(
        data.data(), data.size(), compressed.data(), compressed.size());
    CHECK_EQ(compressResult.bytesRead, static_cast<int64_t>(data.size()));
    int64_t totalWritten = compressResult.bytesWritten;

    auto endResult = compressor->end(
        compressed.data() + totalWritten, compressed.size() - totalWritten);
    while (!endResult.noMoreOutput) {
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
    compressor->reset();

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
    decompressor->reset();
  }

  suspender.rehire();

  std::string name = "Stream_" + std::string(Codec::codecTypeName(type)) +
      (checksumEnabled ? "_Checksum" : "_NoChecksum");
  storeMetrics(name, compressedSize, totalCompressTime, totalDecompressTime, n);
}

BENCHMARK(OneShot_ZSTD_NoChecksum, n) {
  runOneShotBenchmark(CodecType::ZSTD, false, n);
}

BENCHMARK(OneShot_ZSTD_Checksum, n) {
  runOneShotBenchmark(CodecType::ZSTD, true, n);
}

// GZIP has built-in CRC32 checksum (always enabled), so no separate checksum
// benchmark
BENCHMARK(OneShot_GZIP, n) {
  runOneShotBenchmark(CodecType::GZIP, false, n);
}

BENCHMARK(OneShot_LZ4_NoChecksum, n) {
  runOneShotBenchmark(CodecType::LZ4, false, n);
}

BENCHMARK(OneShot_LZ4_FRAME_NoChecksum, n) {
  runOneShotBenchmark(CodecType::LZ4_FRAME, false, n);
}

BENCHMARK(OneShot_LZ4_FRAME_Checksum, n) {
  runOneShotBenchmark(CodecType::LZ4_FRAME, true, n);
}

BENCHMARK(OneShot_SNAPPY_NoChecksum, n) {
  runOneShotBenchmark(CodecType::SNAPPY, false, n);
}

BENCHMARK(Stream_ZSTD_NoChecksum, n) {
  runStreamBenchmark(CodecType::ZSTD, false, n);
}

BENCHMARK(Stream_ZSTD_Checksum, n) {
  runStreamBenchmark(CodecType::ZSTD, true, n);
}

// GZIP has built-in CRC32 checksum (always enabled), so no separate checksum
// benchmark
BENCHMARK(Stream_GZIP, n) {
  runStreamBenchmark(CodecType::GZIP, false, n);
}

BENCHMARK(Stream_LZ4_FRAME_NoChecksum, n) {
  runStreamBenchmark(CodecType::LZ4_FRAME, false, n);
}

BENCHMARK(Stream_LZ4_FRAME_Checksum, n) {
  runStreamBenchmark(CodecType::LZ4_FRAME, true, n);
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  folly::runBenchmarks();
  printAllMetrics();
  return 0;
}
