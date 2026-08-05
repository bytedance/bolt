/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/common/SeekableInputStream.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

#include <arrow/util/rle_encoding.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <zstd.h>

#include <random>

using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;

namespace {

constexpr int32_t kNumPages = 128;
constexpr int32_t kValuesPerPage = 1024;
constexpr int32_t kValuePayloadBytes = 4 * 1024 * 1024;
constexpr uint32_t kSeed = 20260722;

struct SyntheticPage {
  thrift::PageHeader header;
  std::string serializedHeader;
  std::string compressed;
  int32_t uncompressedSize;
};

void appendInt32(std::string& out, int32_t value) {
  out.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

std::string encodeLevels(const std::vector<int16_t>& levels, int bitWidth) {
  std::string encoded(levels.size() * sizeof(int16_t) + 64, '\0');
  ::arrow::util::RleEncoder encoder(
      reinterpret_cast<uint8_t*>(encoded.data()), encoded.size(), bitWidth);
  for (auto level : levels) {
    encoder.Put(level);
  }
  encoded.resize(encoder.Flush());
  return encoded;
}

std::string zstdCompress(const std::string& data) {
  std::string compressed(ZSTD_compressBound(data.size()), '\0');
  const auto compressedSize = ZSTD_compress(
      compressed.data(), compressed.size(), data.data(), data.size(), 1);
  BOLT_CHECK(!ZSTD_isError(compressedSize), ZSTD_getErrorName(compressedSize));
  compressed.resize(compressedSize);
  return compressed;
}

ParquetTypeWithIdPtr makeNestedInt64Type() {
  return std::make_shared<ParquetTypeWithId>(
      BIGINT(),
      std::vector<std::shared_ptr<const dwio::common::TypeWithId>>{},
      0,
      0,
      0,
      "value",
      thrift::Type::INT64,
      std::nullopt,
      std::nullopt,
      1,
      2,
      true,
      true);
}

std::string serializePageHeader(const thrift::PageHeader& pageHeader) {
  auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
  apache::thrift::protocol::TCompactProtocolT<
      apache::thrift::transport::TMemoryBuffer>
      protocol(buffer);
  pageHeader.write(&protocol);

  uint8_t* data = nullptr;
  uint32_t size = 0;
  buffer->getBuffer(&data, &size);
  return std::string(reinterpret_cast<const char*>(data), size);
}

std::string makePageBody(std::mt19937& rng) {
  std::uniform_int_distribution<int32_t> runLengthDist(1, 6);
  std::uniform_int_distribution<int32_t> pct(0, 99);
  std::uniform_int_distribution<int32_t> byteDist(0, 255);

  std::vector<int16_t> repetitionLevels;
  std::vector<int16_t> definitionLevels;
  repetitionLevels.reserve(kValuesPerPage);
  definitionLevels.reserve(kValuesPerPage);

  while (repetitionLevels.size() < kValuesPerPage) {
    const auto runLength = std::min<int32_t>(
        runLengthDist(rng), kValuesPerPage - repetitionLevels.size());
    repetitionLevels.push_back(0);
    definitionLevels.push_back(pct(rng) < 10 ? 1 : 2);
    for (int32_t i = 1; i < runLength; ++i) {
      repetitionLevels.push_back(1);
      const auto roll = pct(rng);
      definitionLevels.push_back(roll < 8 ? 0 : (roll < 20 ? 1 : 2));
    }
  }

  auto repetitionBytes = encodeLevels(repetitionLevels, 1);
  auto definitionBytes = encodeLevels(definitionLevels, 2);

  std::string body;
  appendInt32(body, repetitionBytes.size());
  body.append(repetitionBytes);
  appendInt32(body, definitionBytes.size());
  body.append(definitionBytes);

  for (int32_t i = 0; i < kValuePayloadBytes; ++i) {
    body.push_back(static_cast<char>(byteDist(rng)));
  }
  return body;
}

std::vector<SyntheticPage> makePages() {
  std::mt19937 rng(kSeed);
  std::vector<SyntheticPage> pages;
  pages.reserve(kNumPages);

  for (int32_t page = 0; page < kNumPages; ++page) {
    auto body = makePageBody(rng);
    SyntheticPage syntheticPage;
    syntheticPage.compressed = zstdCompress(body);

    thrift::DataPageHeader dataPageHeader;
    dataPageHeader.__set_num_values(kValuesPerPage);
    dataPageHeader.__set_encoding(thrift::Encoding::PLAIN);
    dataPageHeader.__set_definition_level_encoding(thrift::Encoding::RLE);
    dataPageHeader.__set_repetition_level_encoding(thrift::Encoding::RLE);

    thrift::PageHeader pageHeader;
    pageHeader.__set_type(thrift::PageType::DATA_PAGE);
    pageHeader.__set_uncompressed_page_size(body.size());
    pageHeader.__set_compressed_page_size(syntheticPage.compressed.size());
    pageHeader.__set_data_page_header(dataPageHeader);
    syntheticPage.uncompressedSize = body.size();
    syntheticPage.header = std::move(pageHeader);
    syntheticPage.serializedHeader = serializePageHeader(syntheticPage.header);
    pages.push_back(std::move(syntheticPage));
  }

  return pages;
}

} // namespace

namespace bytedance::bolt::parquet {

class PageReaderPreloadBenchmark {
 public:
  PageReaderPreloadBenchmark() {
    rootPool_ =
        memory::memoryManager()->addRootPool("PageReaderPreloadBenchmark");
    leafPool_ = rootPool_->addLeafChild("PageReaderPreloadBenchmark");
    pages_ = ::makePages();
    for (const auto& page : pages_) {
      columnChunk_.append(page.serializedHeader);
      columnChunk_.append(page.compressed);
    }
  }

  uint64_t run(bool usePrefixFastPath) {
    if (!usePrefixFastPath) {
      return runFullDecompressBaseline();
    }

    auto stream = std::make_unique<SeekableArrayInputStream>(
        columnChunk_.data(), columnChunk_.size());
    PageReader reader(
        std::move(stream),
        *leafPool_,
        makeNestedInt64Type(),
        thrift::CompressionCodec::ZSTD,
        columnChunk_.size(),
        nullptr);
    reader.decodeRepDefs(kNumPages * kValuesPerPage);
    return reader.repDefRange().second;
  }

 private:
  uint64_t runFullDecompressBaseline() {
    constexpr int32_t kLenSize = sizeof(int32_t);
    std::string decompressed;
    uint64_t repDefBytes = 0;
    for (const auto& page : pages_) {
      decompressed.resize(page.uncompressedSize);
      const auto result = ZSTD_decompress(
          decompressed.data(),
          decompressed.size(),
          page.compressed.data(),
          page.compressed.size());
      BOLT_CHECK(!ZSTD_isError(result), ZSTD_getErrorName(result));
      BOLT_CHECK_EQ(result, page.uncompressedSize);

      const char* data = decompressed.data();
      const auto repeatLength = *reinterpret_cast<const int32_t*>(data);
      data += kLenSize + repeatLength;
      const auto defineLength = *reinterpret_cast<const uint32_t*>(data);
      repDefBytes += kLenSize + repeatLength + kLenSize + defineLength;
    }
    return repDefBytes;
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::vector<::SyntheticPage> pages_;
  std::string columnChunk_;
};

} // namespace bytedance::bolt::parquet

namespace {

PageReaderPreloadBenchmark& benchmark() {
  static PageReaderPreloadBenchmark instance;
  return instance;
}

void runPreloadDecode(uint32_t iters, bool usePrefixFastPath) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark();
  suspender.dismiss();
  while (iters--) {
    auto repDefBytes = instance.run(usePrefixFastPath);
    folly::doNotOptimizeAway(repDefBytes);
  }
}

} // namespace

BENCHMARK(prefix_off, iters) {
  runPreloadDecode(iters, false);
}

BENCHMARK_RELATIVE(prefix_on, iters) {
  runPreloadDecode(iters, true);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
