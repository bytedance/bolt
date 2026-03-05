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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <sched.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <random>

#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/Payload.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "shuffle/sparksql/compression/Codec.h"

DEFINE_string(file, "", "Path to parquet file for benchmark");
DEFINE_string(codec, "lz4", "Codec type: lz4 or zstd");
DEFINE_int32(cpu_offset, 0, "CPU offset for affinity");
DEFINE_bool(checksum, false, "Enable checksum");
DEFINE_bool(test_corruption, false, "Run corruption detection test");

using arrow::RecordBatchReader;
using namespace bytedance::bolt::shuffle::sparksql;

// Global configuration set from command line.
struct BenchmarkConfig {
  std::string datafile;
  CodecType codec = CodecType::LZ4_FRAME;
  int32_t cpuOffset = 0;
  bool checksumEnabled = false;
  bool testCorruption = false;
};

BenchmarkConfig& getConfig() {
  static BenchmarkConfig config;
  return config;
}

// Helper to create validity buffer flags from schema.
std::vector<bool> createIsValidityBuffer(
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<bool> isValidityBuffer;
  for (const auto& field : schema->fields()) {
    auto fieldType = field->type()->id();
    switch (fieldType) {
      case arrow::NullType::type_id:
        break;
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
        isValidityBuffer.push_back(true); // validity
        isValidityBuffer.push_back(false); // offsets
        isValidityBuffer.push_back(false); // data
        break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
        // Complex types handled specially.
        break;
      default:
        isValidityBuffer.push_back(true); // validity
        isValidityBuffer.push_back(false); // data
        break;
    }
  }
  return isValidityBuffer;
}

// Extract buffers from RecordBatch for BlockPayload.
std::vector<std::shared_ptr<arrow::Buffer>> extractBuffers(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < batch->num_columns(); ++i) {
    auto column = batch->column(i);
    auto fieldType = batch->schema()->field(i)->type()->id();

    switch (fieldType) {
      case arrow::NullType::type_id:
        break;
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        // Binary/String: validity, offsets, data.
        for (const auto& buffer : column->data()->buffers) {
          buffers.push_back(buffer);
        }
        break;
      }
      default: {
        // Default: validity, data.
        for (const auto& buffer : column->data()->buffers) {
          buffers.push_back(buffer);
        }
        break;
      }
    }
  }
  return buffers;
}

int64_t setCpu(uint32_t cpuindex) {
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpuindex, &cs);
  return sched_setaffinity(0, sizeof(cs), &cs);
}

void logMetrics(
    const std::string& name,
    int64_t numPayloads,
    int64_t numRows,
    int64_t serializeTime,
    int64_t deserializeTime,
    int64_t totalSerializedSize,
    int64_t totalUncompressedSize,
    size_t iterations) {
  if (iterations == 0) {
    return;
  }

  double compressionRatio = totalUncompressedSize > 0
      ? static_cast<double>(totalSerializedSize) / totalUncompressedSize
      : 0.0;

  double serializeThroughput = serializeTime > 0
      ? static_cast<double>(totalUncompressedSize) / serializeTime * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  double deserializeThroughput = deserializeTime > 0
      ? static_cast<double>(totalUncompressedSize) / deserializeTime * 1e9 /
          (1024.0 * 1024.0)
      : 0.0;

  LOG(INFO) << name << " num_payloads=" << numPayloads
            << " num_rows=" << numRows
            << " uncompressed_size=" << totalUncompressedSize
            << " serialized_size=" << totalSerializedSize
            << " compression_ratio=" << compressionRatio
            << " serialize_MBps=" << serializeThroughput
            << " deserialize_MBps=" << deserializeThroughput << std::endl;
}

void logCorruptionMetrics(
    const std::string& name,
    int64_t totalTests,
    int64_t detectedErrors,
    int64_t undetectedDataChanged,
    int64_t dataUnchanged) {
  double detectionRate =
      totalTests > 0 ? 100.0 * detectedErrors / totalTests : 0;
  double dataIntegrityRate = totalTests > 0
      ? 100.0 * (detectedErrors + dataUnchanged) / totalTests
      : 0;

  LOG(INFO) << name << " total_tests=" << totalTests
            << " detected_errors=" << detectedErrors
            << " undetected_data_changed=" << undetectedDataChanged
            << " data_unchanged=" << dataUnchanged
            << " detection_rate_pct=" << detectionRate
            << " data_integrity_rate_pct=" << dataIntegrityRate << std::endl;
}

// Benchmark data holder.
struct BenchmarkData {
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::shared_ptr<arrow::Schema> schema;
  ::parquet::ArrowReaderProperties properties;
  std::vector<int> rowGroupIndices;
  std::vector<int> columnIndices;
  std::vector<bool> isValidityBuffer;
};

void loadParquetFile(const std::string& inputFile, BenchmarkData& data) {
  std::shared_ptr<arrow::fs::FileSystem> fs;
  std::string fileName;
  auto fsResult = arrow::fs::FileSystemFromUriOrPath(inputFile, &fileName);
  BOLT_CHECK(
      fsResult.ok(),
      "Failed to get filesystem: {}",
      fsResult.status().ToString());
  fs = fsResult.MoveValueUnsafe();

  auto fileResult = fs->OpenInputFile(fileName);
  BOLT_CHECK(
      fileResult.ok(),
      "Failed to open file: {}",
      fileResult.status().ToString());
  data.file = fileResult.MoveValueUnsafe();

  data.properties.set_batch_size(32768);
  data.properties.set_pre_buffer(false);
  data.properties.set_use_threads(false);

  std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
  auto makeResult = ::parquet::arrow::FileReader::Make(
      arrow::default_memory_pool(),
      ::parquet::ParquetFileReader::Open(data.file),
      data.properties,
      &parquetReader);
  BOLT_CHECK(
      makeResult.ok(),
      "Failed to make parquet reader: {}",
      makeResult.ToString());

  auto schemaResult = parquetReader->GetSchema(&data.schema);
  BOLT_CHECK(
      schemaResult.ok(), "Failed to get schema: {}", schemaResult.ToString());

  auto numRowgroups = parquetReader->num_row_groups();
  for (int i = 0; i < numRowgroups; ++i) {
    data.rowGroupIndices.push_back(i);
  }

  auto numColumns = data.schema->num_fields();
  for (int i = 0; i < numColumns; ++i) {
    data.columnIndices.push_back(i);
  }

  data.isValidityBuffer = createIsValidityBuffer(data.schema);
}

void runSerializeBenchmark(bool checksumEnabled, size_t iterations) {
  folly::BenchmarkSuspender suspender;

  const auto& config = getConfig();
  setCpu(config.cpuOffset);

  if (config.datafile.empty()) {
    std::cerr << "No datafile specified. Use --file=<path>" << std::endl;
    return;
  }

  BenchmarkData data;
  loadParquetFile(config.datafile, data);

  auto codec = Codec::create(
      config.codec,
      CodecOptions{
          CodecBackend::NONE, kDefaultCompressionLevel, checksumEnabled});

  suspender.dismiss();

  int64_t serializeTime = 0;
  int64_t deserializeTime = 0;
  int64_t totalSerializedSize = 0;
  int64_t totalUncompressedSize = 0;
  int64_t numPayloads = 0;
  int64_t numRows = 0;

  for (size_t iter = 0; iter < iterations; ++iter) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    auto makeStatus = ::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        ::parquet::ParquetFileReader::Open(data.file),
        data.properties,
        &parquetReader);
    BOLT_CHECK(
        makeStatus.ok(), "Failed to make reader: {}", makeStatus.ToString());

    auto readerStatus = parquetReader->GetRecordBatchReader(
        data.rowGroupIndices, data.columnIndices, &recordBatchReader);
    BOLT_CHECK(
        readerStatus.ok(),
        "Failed to get batch reader: {}",
        readerStatus.ToString());

    std::shared_ptr<arrow::RecordBatch> recordBatch;
    auto readStatus = recordBatchReader->ReadNext(&recordBatch);
    BOLT_CHECK(
        readStatus.ok(), "Failed to read batch: {}", readStatus.ToString());

    while (recordBatch) {
      numPayloads++;
      numRows += recordBatch->num_rows();

      auto buffers = extractBuffers(recordBatch);

      for (const auto& buffer : buffers) {
        if (buffer) {
          totalUncompressedSize += buffer->size();
        }
      }

      auto payloadResult = BlockPayload::fromBuffers(
          Payload::Type::kToBeCompressed,
          recordBatch->num_rows(),
          std::move(buffers),
          &data.isValidityBuffer,
          arrow::default_memory_pool(),
          codec.get(),
          Payload::Mode::kBuffer,
          false);
      BOLT_CHECK(
          payloadResult.ok(),
          "Failed to create payload: {}",
          payloadResult.status().ToString());
      auto payload = std::move(payloadResult).ValueOrDie();

      auto outputStream = arrow::io::BufferOutputStream::Create(
          1024 * 1024, arrow::default_memory_pool());
      BOLT_CHECK(
          outputStream.ok(),
          "Failed to create output stream: {}",
          outputStream.status().ToString());

      auto serializeStatus =
          payload->serialize(outputStream.ValueOrDie().get());
      BOLT_CHECK(
          serializeStatus.ok(),
          "Failed to serialize: {}",
          serializeStatus.ToString());

      serializeTime += payload->getCompressTime() + payload->getWriteTime();

      auto serializedBuffer = outputStream.ValueOrDie()->Finish();
      BOLT_CHECK(
          serializedBuffer.ok(),
          "Failed to finish buffer: {}",
          serializedBuffer.status().ToString());
      totalSerializedSize += serializedBuffer.ValueOrDie()->size();

      readStatus = recordBatchReader->ReadNext(&recordBatch);
      BOLT_CHECK(
          readStatus.ok(),
          "Failed to read next batch: {}",
          readStatus.ToString());
    }
  }

  suspender.rehire();

  std::string name = checksumEnabled ? "BlockPayload_Serialize_Checksum"
                                     : "BlockPayload_Serialize_NoChecksum";
  logMetrics(
      name,
      numPayloads,
      numRows,
      serializeTime,
      deserializeTime,
      totalSerializedSize,
      totalUncompressedSize,
      iterations);
}

void runDeserializeBenchmark(bool checksumEnabled, size_t iterations) {
  folly::BenchmarkSuspender suspender;

  const auto& config = getConfig();
  setCpu(config.cpuOffset);

  if (config.datafile.empty()) {
    std::cerr << "No datafile specified. Use --file=<path>" << std::endl;
    return;
  }

  BenchmarkData data;
  loadParquetFile(config.datafile, data);

  auto codec = Codec::create(
      config.codec,
      CodecOptions{
          CodecBackend::NONE, kDefaultCompressionLevel, checksumEnabled});

  // First, serialize all payloads to memory.
  std::vector<std::shared_ptr<arrow::Buffer>> serializedBuffers;
  std::vector<uint32_t> expectedRows;
  int64_t serializeTime = 0;
  int64_t totalSerializedSize = 0;
  int64_t totalUncompressedSize = 0;
  int64_t numPayloads = 0;

  {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    auto makeStatus = ::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        ::parquet::ParquetFileReader::Open(data.file),
        data.properties,
        &parquetReader);
    BOLT_CHECK(makeStatus.ok(), "Failed to make reader");

    auto readerStatus = parquetReader->GetRecordBatchReader(
        data.rowGroupIndices, data.columnIndices, &recordBatchReader);
    BOLT_CHECK(readerStatus.ok(), "Failed to get batch reader");

    std::shared_ptr<arrow::RecordBatch> recordBatch;
    auto readStatus = recordBatchReader->ReadNext(&recordBatch);
    BOLT_CHECK(readStatus.ok(), "Failed to read batch");

    while (recordBatch) {
      auto buffers = extractBuffers(recordBatch);

      for (const auto& buffer : buffers) {
        if (buffer) {
          totalUncompressedSize += buffer->size();
        }
      }

      auto payloadResult = BlockPayload::fromBuffers(
          Payload::Type::kToBeCompressed,
          recordBatch->num_rows(),
          std::move(buffers),
          &data.isValidityBuffer,
          arrow::default_memory_pool(),
          codec.get(),
          Payload::Mode::kBuffer,
          false);
      BOLT_CHECK(payloadResult.ok(), "Failed to create payload");
      auto payload = std::move(payloadResult).ValueOrDie();

      auto outputStream = arrow::io::BufferOutputStream::Create(
          1024 * 1024, arrow::default_memory_pool());
      BOLT_CHECK(outputStream.ok(), "Failed to create output stream");

      auto serializeStatus =
          payload->serialize(outputStream.ValueOrDie().get());
      BOLT_CHECK(serializeStatus.ok(), "Failed to serialize");
      serializeTime += payload->getCompressTime() + payload->getWriteTime();

      auto serializedBuffer = outputStream.ValueOrDie()->Finish();
      BOLT_CHECK(serializedBuffer.ok(), "Failed to finish buffer");

      serializedBuffers.push_back(serializedBuffer.ValueOrDie());
      expectedRows.push_back(recordBatch->num_rows());
      totalSerializedSize += serializedBuffer.ValueOrDie()->size();
      numPayloads++;

      readStatus = recordBatchReader->ReadNext(&recordBatch);
      BOLT_CHECK(readStatus.ok(), "Failed to read next batch");
    }
  }

  std::shared_ptr<Codec> sharedCodec = std::move(Codec::create(
      config.codec,
      CodecOptions{
          CodecBackend::NONE, kDefaultCompressionLevel, checksumEnabled}));

  suspender.dismiss();

  int64_t deserializeTime = 0;
  int64_t numRows = 0;

  for (size_t iter = 0; iter < iterations; ++iter) {
    for (size_t i = 0; i < serializedBuffers.size(); ++i) {
      auto inputStream =
          std::make_shared<arrow::io::BufferReader>(serializedBuffers[i]);

      uint8_t payloadTypeByte;
      auto readStatus = inputStream->Read(sizeof(uint8_t), &payloadTypeByte);
      BOLT_CHECK(readStatus.ok(), "Failed to read payload type");
      std::optional<uint8_t> payloadType = payloadTypeByte;

      uint32_t deserializedNumRows = 0;
      uint64_t decompressTimeLocal = 0;

      auto start = std::chrono::steady_clock::now();
      auto result = BlockPayload::deserialize(
          inputStream.get(),
          data.schema,
          sharedCodec,
          arrow::default_memory_pool(),
          deserializedNumRows,
          decompressTimeLocal,
          payloadType,
          nullptr);
      auto end = std::chrono::steady_clock::now();

      BOLT_CHECK(result.ok(), "Failed to deserialize payload");
      deserializeTime += static_cast<int64_t>((end - start).count());
      numRows += deserializedNumRows;
    }
  }

  suspender.rehire();

  std::string name = checksumEnabled ? "BlockPayload_Deserialize_Checksum"
                                     : "BlockPayload_Deserialize_NoChecksum";
  logMetrics(
      name,
      numPayloads,
      numRows,
      serializeTime,
      deserializeTime,
      totalSerializedSize,
      totalUncompressedSize,
      iterations);
}

void runCorruptionBenchmark(bool checksumEnabled, size_t iterations) {
  folly::BenchmarkSuspender suspender;

  const auto& config = getConfig();
  setCpu(config.cpuOffset);

  if (config.datafile.empty()) {
    std::cerr << "No datafile specified. Use --file=<path>" << std::endl;
    return;
  }

  BenchmarkData data;
  loadParquetFile(config.datafile, data);

  std::shared_ptr<Codec> codec = Codec::create(
      config.codec,
      CodecOptions{
          CodecBackend::NONE, kDefaultCompressionLevel, checksumEnabled});

  suspender.dismiss();

  int64_t totalTests = 0;
  int64_t detectedErrors = 0;
  int64_t undetectedDataChanged = 0;
  int64_t dataUnchanged = 0;

  std::random_device rd;
  std::mt19937 gen(rd());

  for (size_t iter = 0; iter < iterations; ++iter) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    auto makeStatus = ::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        ::parquet::ParquetFileReader::Open(data.file),
        data.properties,
        &parquetReader);
    BOLT_CHECK(makeStatus.ok(), "Failed to make reader");

    auto readerStatus = parquetReader->GetRecordBatchReader(
        data.rowGroupIndices, data.columnIndices, &recordBatchReader);
    BOLT_CHECK(readerStatus.ok(), "Failed to get batch reader");

    std::shared_ptr<arrow::RecordBatch> recordBatch;
    auto readStatus = recordBatchReader->ReadNext(&recordBatch);
    BOLT_CHECK(readStatus.ok(), "Failed to read batch");

    while (recordBatch) {
      // Extract and save original buffers for comparison.
      auto originalBuffers = extractBuffers(recordBatch);
      std::vector<std::shared_ptr<arrow::Buffer>> originalBuffersCopy;
      for (const auto& buf : originalBuffers) {
        if (buf) {
          auto copyResult =
              arrow::AllocateBuffer(buf->size(), arrow::default_memory_pool());
          BOLT_CHECK(copyResult.ok(), "Failed to allocate buffer");
          auto copy = std::move(copyResult).ValueOrDie();
          memcpy(copy->mutable_data(), buf->data(), buf->size());
          originalBuffersCopy.push_back(std::move(copy));
        } else {
          originalBuffersCopy.push_back(nullptr);
        }
      }

      // Create and serialize payload.
      auto buffers = extractBuffers(recordBatch);
      auto payloadResult = BlockPayload::fromBuffers(
          Payload::Type::kToBeCompressed,
          recordBatch->num_rows(),
          std::move(buffers),
          &data.isValidityBuffer,
          arrow::default_memory_pool(),
          codec.get(),
          Payload::Mode::kBuffer,
          false);
      BOLT_CHECK(payloadResult.ok(), "Failed to create payload");
      auto payload = std::move(payloadResult).ValueOrDie();

      auto outputStream = arrow::io::BufferOutputStream::Create(
          1024 * 1024, arrow::default_memory_pool());
      BOLT_CHECK(outputStream.ok(), "Failed to create output stream");

      auto serializeStatus =
          payload->serialize(outputStream.ValueOrDie().get());
      BOLT_CHECK(serializeStatus.ok(), "Failed to serialize");

      auto serializedBuffer = outputStream.ValueOrDie()->Finish();
      BOLT_CHECK(serializedBuffer.ok(), "Failed to finish buffer");
      auto serializedData = serializedBuffer.ValueOrDie();

      // Skip if buffer is too small.
      if (serializedData->size() <= 10) {
        readStatus = recordBatchReader->ReadNext(&recordBatch);
        BOLT_CHECK(readStatus.ok(), "Failed to read next batch");
        continue;
      }

      // Run multiple corruption tests per payload for better statistics
      constexpr int kCorruptionTestsPerPayload = 12;
      for (int testIdx = 0; testIdx < kCorruptionTestsPerPayload; ++testIdx) {
        totalTests++;

        // Create corrupted copy - flip a random bit in the compressed data.
        auto corruptedResult = arrow::AllocateResizableBuffer(
            serializedData->size(), arrow::default_memory_pool());
        BOLT_CHECK(corruptedResult.ok(), "Failed to allocate corrupted buffer");
        auto corruptedBuffer = std::move(corruptedResult).ValueOrDie();
        memcpy(
            corruptedBuffer->mutable_data(),
            serializedData->data(),
            serializedData->size());

        // Corrupt a random bit in the payload data (after header = 6 bytes).
        int64_t corruptOffset = 6;
        int64_t corruptableSize = serializedData->size() - corruptOffset;
        if (corruptableSize > 0) {
          std::uniform_int_distribution<int64_t> byteDist(
              0, corruptableSize - 1);
          std::uniform_int_distribution<int> bitDist(0, 7);
          int64_t bytePos = corruptOffset + byteDist(gen);
          int bitPos = bitDist(gen);
          corruptedBuffer->mutable_data()[bytePos] ^= (1 << bitPos);
        }

        // Try to deserialize corrupted data.
        auto inputStream = std::make_shared<arrow::io::BufferReader>(
            std::move(corruptedBuffer));

        uint8_t payloadTypeByte;
        auto payloadReadStatus =
            inputStream->Read(sizeof(uint8_t), &payloadTypeByte);
        BOLT_CHECK(payloadReadStatus.ok(), "Failed to read payload type");
        std::optional<uint8_t> payloadType = payloadTypeByte;

        uint32_t deserializedNumRows = 0;
        uint64_t decompressTimeLocal = 0;

        try {
          auto result = BlockPayload::deserialize(
              inputStream.get(),
              data.schema,
              codec,
              arrow::default_memory_pool(),
              deserializedNumRows,
              decompressTimeLocal,
              payloadType,
              nullptr);

          if (!result.ok()) {
            // Error detected during decompression.
            detectedErrors++;
          } else {
            // Decompression succeeded - check if data is unchanged.
            auto deserializedBuffers = std::move(result).ValueOrDie();
            bool dataChanged = false;

            size_t minSize = std::min(
                originalBuffersCopy.size(), deserializedBuffers.size());
            for (size_t i = 0; i < minSize; ++i) {
              auto& orig = originalBuffersCopy[i];
              auto& deser = deserializedBuffers[i];

              if ((orig == nullptr) != (deser == nullptr)) {
                dataChanged = true;
                break;
              }
              if (orig && deser) {
                if (orig->size() != deser->size() ||
                    memcmp(orig->data(), deser->data(), orig->size()) != 0) {
                  dataChanged = true;
                  break;
                }
              }
            }

            if (dataChanged) {
              undetectedDataChanged++;
            } else {
              dataUnchanged++;
            }
          }
        } catch (const std::exception& e) {
          // Exception thrown during decompression (corruption detected).
          detectedErrors++;
        }
      }

      readStatus = recordBatchReader->ReadNext(&recordBatch);
      BOLT_CHECK(readStatus.ok(), "Failed to read next batch");
    }
  }

  suspender.rehire();

  std::string name = checksumEnabled ? "BlockPayload_Corruption_Checksum"
                                     : "BlockPayload_Corruption_NoChecksum";
  logCorruptionMetrics(
      name, totalTests, detectedErrors, undetectedDataChanged, dataUnchanged);
}

BENCHMARK(BlockPayload_Serialize_NoChecksum, n) {
  runSerializeBenchmark(false, n);
}

BENCHMARK(BlockPayload_Serialize_Checksum, n) {
  runSerializeBenchmark(true, n);
}

BENCHMARK(BlockPayload_Deserialize_NoChecksum, n) {
  runDeserializeBenchmark(false, n);
}

BENCHMARK(BlockPayload_Deserialize_Checksum, n) {
  runDeserializeBenchmark(true, n);
}

BENCHMARK(BlockPayload_Corruption_NoChecksum, n) {
  runCorruptionBenchmark(false, n);
}

BENCHMARK(BlockPayload_Corruption_Checksum, n) {
  runCorruptionBenchmark(true, n);
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto& config = getConfig();
  config.datafile = FLAGS_file;
  config.cpuOffset = FLAGS_cpu_offset;
  config.checksumEnabled = FLAGS_checksum;
  config.testCorruption = FLAGS_test_corruption;

  // Parse codec type from string
  std::string codecStr = FLAGS_codec;
  std::transform(codecStr.begin(), codecStr.end(), codecStr.begin(), ::tolower);
  if (codecStr == "zstd") {
    config.codec = CodecType::ZSTD;
  } else if (codecStr == "lz4") {
    config.codec = CodecType::LZ4_FRAME;
  } else {
    std::cerr << "Unknown codec: " << FLAGS_codec << ". Use 'lz4' or 'zstd'."
              << std::endl;
    return EXIT_FAILURE;
  }

  if (config.datafile.empty()) {
    std::cerr
        << "No datafile specified. Use --file=<path> to specify a parquet file."
        << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "datafile = " << config.datafile << std::endl;
  std::cout << "codec = " << Codec::codecTypeName(config.codec) << std::endl;
  std::cout << "cpu_offset = " << config.cpuOffset << std::endl;
  std::cout << "checksum = " << config.checksumEnabled << std::endl;
  std::cout << "test_corruption = " << config.testCorruption << std::endl;

  folly::runBenchmarks();
  return 0;
}
